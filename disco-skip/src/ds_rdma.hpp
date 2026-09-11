#pragma once

// Blocking one-sided RDMA, for setup and verification only.
//
// The benchmark path uses the future state machines, which never block. These
// helpers do: each posts one signalled work request and spins on the send
// completion queue until it lands. That is the right shape for bootstrap and
// for --selftest -- both are strictly sequential phases outside the measurement
// window -- and the wrong shape for anything on the hot path.
//
// SINGLE OUTSTANDING OPERATION. Each call drains one completion from the
// connection's send CQ, so it must not run while a future has work in flight on
// the same connection: it would consume that future's completion and the
// future would wait forever. Use these before futures start or after they have
// all drained.

#include <chrono>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

#include <dory/conn/rc.hpp>
#include <dory/extern/ibverbs.hpp>

#include "ds_batch.hpp"
#include "ds_defs.hpp"
#include "ds_node.hpp"
#include "layout.hpp"  // Layout::nodeAddrOf

namespace ds {

/// Work-request id used by the blocking helpers. Distinct from any future id so
/// that a completion turning up on the wrong path is recognisable in a trace
/// rather than silently attributed to future 0.
inline constexpr uint64_t kBlockingWrId = ~uint64_t{0};

namespace detail {

/// Spin until exactly one completion has been reaped, checking its status.
inline void awaitOne(dory::conn::ReliableConnection &rc, char const *what) {
  std::vector<struct ibv_wc> wces;
  while (true) {
    wces.resize(1);
    if (!rc.pollCqIsOk(dory::conn::ReliableConnection::SendCq, wces)) {
      throw std::runtime_error(std::string("polling the send CQ failed during ") +
                               what);
    }
    if (wces.empty()) continue;
    if (wces[0].status != IBV_WC_SUCCESS) {
      // ibv_wc_status is an unsigned enum, and handing it straight to
      // std::to_string picks the int overload -- which -Wsign-promo rejects.
      // Widened explicitly rather than left to promotion.
      throw std::runtime_error(std::string("RDMA ") + what + " failed: status " +
                               std::to_string(static_cast<unsigned>(wces[0].status)));
    }
    return;
  }
}

}  // namespace detail

/// One-sided WRITE, returning once the completion has landed.
///
/// /local/ must lie inside the registered MR -- the HCA reads from it directly,
/// so a stack buffer would not be addressable.
inline void blockingWrite(dory::conn::ReliableConnection &rc, void *local,
                          uint32_t len, uintptr_t remote) {
  if (!rc.postSendSingle(dory::conn::ReliableConnection::RdmaWrite,
                         kBlockingWrId, local, len, remote)) {
    throw std::runtime_error("failed to post a blocking RDMA write");
  }
  detail::awaitOne(rc, "write");
}

/// One-sided READ, returning once /local/ holds the data.
inline void blockingRead(dory::conn::ReliableConnection &rc, void *local,
                         uint32_t len, uintptr_t remote) {
  if (!rc.postSendSingle(dory::conn::ReliableConnection::RdmaRead,
                         kBlockingWrId, local, len, remote)) {
    throw std::runtime_error("failed to post a blocking RDMA read");
  }
  detail::awaitOne(rc, "read");
}

/// One-sided CAS. Returns true if it succeeded, i.e. if the value the HCA found
/// was the one we expected. /swap/ receives the pre-CAS remote value either
/// way, which is why a failed CAS still tells you what is actually there.
inline bool blockingCas(dory::conn::ReliableConnection &rc, uint64_t *swap,
                        uintptr_t remote, uint64_t expected, uint64_t desired) {
  if (!rc.postSendSingleCas(kBlockingWrId, swap, remote, expected, desired)) {
    throw std::runtime_error("failed to post a blocking RDMA CAS");
  }
  detail::awaitOne(rc, "compare-and-swap");
  return *swap == expected;
}

/// Write one node header to every replica.
///
/// /staging/ must be an MR-resident NodeRecord holding the bytes to write. The
/// same bytes go to every replica, since the arena layout is identical on all
/// of them -- that is what makes a RemoteAddr replica-independent.
template <class Conns>
void writeNodeAllReplicas(Conns &conns, NodeRecord *staging, RemoteAddr addr) {
  for (size_t r = 0; r < conns.size(); ++r) {
    auto &rc = *conns[r];
    blockingWrite(rc, staging, kNodeRecordBytes,
                  Layout::nodeAddrOf(rc.remoteBuf(), addr));
  }
}

/// Write one vector to every replica.
template <class Conns>
void writeVecAllReplicas(Conns &conns, Layout const &layout, VecRecord *staging,
                         VecOffset off) {
  for (size_t r = 0; r < conns.size(); ++r) {
    auto &rc = *conns[r];
    blockingWrite(rc, staging, kVecRecordBytes,
                  layout.vecAddrOf(rc.remoteBuf(), off));
  }
}

/// Post a batch as one chained doorbell on one connection.
///
/// This is the whole point of ds_batch.hpp reaching the wire. Each operation
/// becomes a work request, the requests are linked through `wr.next`, and a
/// single `postSend` hands the chain to the HCA -- so a sequence that would
/// cost one round trip per operation costs one round trip in total. The idiom
/// is swarm-kv/src/unreliable_maxreg.hpp's, and like that code it keeps a
/// per-request fallback so the batching can be turned off and measured rather
/// than assumed.
///
/// Three details that are load-bearing:
///
///   * ONLY THE LAST REQUEST IS SIGNALLED, so the chain costs one completion
///     rather than one per operation. That is safe because an error on an
///     unsignalled request still produces an error completion, so a successful
///     completion for the last request means every earlier one succeeded.
///
///   * THE PUBLISHING CAS CARRIES IBV_SEND_FENCE. F3 requires the staged writes
///     to be visible at the remote HCA before the CAS that publishes them. With
///     blocking helpers that came free, since each waited for its own
///     completion; in a chain nothing waits, so the fence is what supplies it.
///     dory never sets that flag -- nothing in conn/, swarm-kv/, chimera/ or
///     fusee/ does -- and `prepareSingleCas` *assigns* send_flags rather than
///     OR-ing, so the flag has to be added after preparing, not before.
///
///   * A CAS REPORTS ITS PRE-CAS VALUE into its own swapback slot, which is why
///     they cannot share one. Those slots are only meaningful once the chain's
///     completion has landed.
///
/// Payloads must already be staged by stageBatchPayloads(); this only refers to
/// them. That split matters: the same bytes go to every replica, so they are
/// copied into the registered MR ONCE and every replica's work requests read
/// from the same source buffer -- which is what writeVecAllReplicas has always
/// done. Copying per replica would also mean writing over a buffer whose bytes
/// another replica's HCA might still be reading.
///
/// @param stage_vecs  kMaxBatchVecWrites slots. Distinct *within* a chain,
///                    because every write in it is in flight at once; shared
///                    *across* replicas, because the bytes are identical.
/// @param cas_bufs    kMaxBatchOps slots for THIS replica -- a CAS reports its
///                    pre-CAS value into its own slot, so these cannot be
///                    shared across replicas the way the staging can
/// @return the number of signalled requests to drain, or 0 if nothing was posted
template <class Conn>
size_t postBatchChain(Conn &rc, Layout const &layout, Batch const &b,
                      NodeRecord *stage_nodes, VecRecord *stage_vecs,
                      uint64_t *cas_bufs, bool doorbell) {
  if (b.size() == 0 || !b.wellFormed()) return 0;

  struct ibv_send_wr wr[kMaxBatchOps];
  struct ibv_sge sg[kMaxBatchOps];
  size_t vec_slot = 0, node_slot = 0;
  size_t const last = b.size() - 1;

  for (size_t i = 0; i < b.size(); ++i) {
    BatchOp const &o = b[i];
    bool const signaled = (i == last);
    switch (o.kind) {
      case BatchKind::WriteVec: {
        VecRecord *slot = &stage_vecs[vec_slot++];
        rc.prepareSingle(wr[i], sg[i],
                         dory::conn::ReliableConnection::RdmaWrite,
                         kBlockingWrId, slot, kVecRecordBytes,
                         layout.vecAddrOf(rc.remoteBuf(), o.off), signaled);
        break;
      }
      case BatchKind::WriteNode: {
        NodeRecord *slot = &stage_nodes[node_slot++];
        rc.prepareSingle(wr[i], sg[i],
                         dory::conn::ReliableConnection::RdmaWrite,
                         kBlockingWrId, slot, kNodeRecordBytes,
                         Layout::nodeAddrOf(rc.remoteBuf(), o.addr), signaled);
        break;
      }
      case BatchKind::CasHandle:
        rc.prepareSingleCas(wr[i], sg[i], kBlockingWrId, &cas_bufs[i],
                            Layout::nodeAddrOf(rc.remoteBuf(), o.addr),
                            o.expected, o.desired, signaled);
        // F3. Must come after prepareSingleCas, which assigns send_flags.
        wr[i].send_flags |= IBV_SEND_FENCE;
        break;
      case BatchKind::CasTs:
        rc.prepareSingleCas(wr[i], sg[i], kBlockingWrId, &cas_bufs[i],
                            layout.vecTsAddrOf(rc.remoteBuf(), o.off),
                            o.expected, o.desired, signaled);
        break;
      case BatchKind::CasNextId:
        rc.prepareSingleCas(wr[i], sg[i], kBlockingWrId, &cas_bufs[i],
                            Layout::nextIdAddrOf(rc.remoteBuf(), o.addr),
                            o.expected, o.desired, signaled);
        break;
      case BatchKind::CasNextKMin:
        rc.prepareSingleCas(wr[i], sg[i], kBlockingWrId, &cas_bufs[i],
                            Layout::nextKMinAddrOf(rc.remoteBuf(), o.addr),
                            o.expected, o.desired, signaled);
        break;
      case BatchKind::CasTailWord:
        rc.prepareSingleCas(wr[i], sg[i], kBlockingWrId, &cas_bufs[i],
                            Layout::tailWordAddrOf(rc.remoteBuf(), o.addr),
                            o.expected, o.desired, signaled);
        break;
    }
  }

  if (doorbell) {
    for (size_t i = 1; i < b.size(); ++i) wr[i - 1].next = &wr[i];
    if (!rc.postSend(wr[0])) {
      throw std::runtime_error("failed to post a chained batch");
    }
    return 1;  // only the last request is signalled
  }

  // Unbatched fallback: one post per request, so the chain's cost can be
  // measured against itself. Every request is signalled here, since the
  // ordering guarantee a chain provides is gone and each must be awaited.
  for (size_t i = 0; i < b.size(); ++i) {
    wr[i].next = nullptr;
    wr[i].send_flags |= IBV_SEND_SIGNALED;
    if (!rc.postSend(wr[i])) {
      throw std::runtime_error("failed to post a batch request");
    }
  }
  return b.size();
}

/// Copy a batch's write payloads into the registered staging slots.
///
/// Called once per batch, before any replica's chain is posted. The HCA reads
/// the source buffer directly, so the payload has to be MR-resident -- a
/// Writer's stack-local staged record is not. Getting this wrong shows up as
/// IBV_WC_LOC_PROT_ERR (status 4) and only on the cluster, since a fake arena
/// has no memory region to be outside of.
inline void stageBatchPayloads(Batch const &b, NodeRecord *stage_nodes,
                               VecRecord *stage_vecs) {
  size_t vec_slot = 0, node_slot = 0;
  for (size_t i = 0; i < b.size(); ++i) {
    BatchOp const &o = b[i];
    if (o.kind == BatchKind::WriteVec) {
      stage_vecs[vec_slot++] = *o.vec;
    } else if (o.kind == BatchKind::WriteNode) {
      stage_nodes[node_slot++] = *o.node;
    }
  }
}

/// Did the publishing CAS in /b/ take, given the swapbacks it wrote?
///
/// A CAS succeeded exactly when the value it found was the one it expected,
/// which is what the swapback holds.
inline bool batchCommitted(Batch const &b, uint64_t const *cas_bufs) {
  for (size_t i = 0; i < b.size(); ++i) {
    if (b[i].kind == BatchKind::CasHandle) {
      return cas_bufs[i] == b[i].expected;
    }
  }
  return true;  // no publishing CAS: nothing to lose
}

/// A node reader satisfying StructureVerifier's Reader concept, backed by real
/// RDMA reads.
///
/// Vectors live out of line, so a node read is two regions: the 64-byte header,
/// then the vector its handle names. Those are ordinarily serialised -- the
/// offset is not known until the header lands.
///
/// The offset hint breaks that dependency. When it has a guess, both reads go
/// out in one doorbell batch and the vector is validated against the handle
/// afterwards; a wrong guess costs a wasted 320-byte read and a second,
/// correct one, landing exactly where the unspeculated read would have. So the
/// hint trades bandwidth for latency, never the reverse, which is why it is a
/// toggle rather than always on: under write-heavy load every write moves the
/// vector and the guess is usually wrong.
///
/// A header whose bookends disagree means a split is mid-propagation. This
/// reader retries rather than helping, because it is used for --selftest over a
/// quiescent structure, where a mismatch that will not settle is real
/// corruption rather than a race. The operation state machines help instead;
/// that is what makes their reads non-blocking.
template <class Conns>
class RdmaNodeReader {
 public:
  /// @param node_buf  MR-resident NodeRecord to read headers into
  /// @param vec_buf   MR-resident VecRecord to read vectors into
  RdmaNodeReader(Conns &conns, Layout const &layout, NodeRecord *node_buf,
                 VecRecord *vec_buf, VecOffsetHint *hint = nullptr,
                 size_t replica = 0)
      : conns_(conns),
        layout_(layout),
        node_buf_(node_buf),
        vec_buf_(vec_buf),
        hint_(hint),
        replica_(replica) {}

  /// A 64-byte header read, raw: no stability retry, because a descent needs
  /// to *see* that a node is mid-propagation in order to route around it or
  /// help finish it. The combined read() below is the settled variant, for the
  /// verifier.
  bool readNode(RemoteAddr a, NodeRecord &node) {
    if (a.isNull()) return false;
    auto &rc = *conns_[replica_];
    blockingRead(rc, node_buf_, kNodeRecordBytes,
                 Layout::nodeAddrOf(rc.remoteBuf(), a));
    ++node_reads_;
    // Learn the true offset even on a raw read: it costs nothing and keeps the
    // hint warm for whoever does speculate.
    if (hint_ != nullptr) hint_->record(a, node_buf_->handle.offset(), kNullVec);
    node = *node_buf_;
    return true;
  }

  bool read(RemoteAddr a, NodeRecord &node, VecRecord &vec) {
    if (a.isNull()) return false;
    auto &rc = *conns_[replica_];
    uintptr_t const node_remote = Layout::nodeAddrOf(rc.remoteBuf(), a);

    for (int attempt = 0; attempt < kRetries; ++attempt) {
      VecOffset const guess = hint_ != nullptr ? hint_->guess(a) : kNullVec;

      // With a guess, fetch both regions in one batch; the vector may turn out
      // to be the wrong version, which the check below catches.
      if (guess != kNullVec) {
        blockingRead(rc, node_buf_, kNodeRecordBytes, node_remote);
        blockingRead(rc, vec_buf_, kVecRecordBytes,
                     layout_.vecAddrOf(rc.remoteBuf(), guess));
        ++node_reads_;
        ++vec_reads_;
      } else {
        blockingRead(rc, node_buf_, kNodeRecordBytes, node_remote);
        ++node_reads_;
      }

      if (!node_buf_->isStable()) {
        ++unstable_;
        continue;  // a split is propagating; re-read
      }

      VecOffset const truth = node_buf_->handle.offset();
      if (hint_ != nullptr) hint_->record(a, truth, guess);

      if (guess != truth) {
        blockingRead(rc, vec_buf_, kVecRecordBytes,
                     layout_.vecAddrOf(rc.remoteBuf(), truth));
        ++vec_reads_;
      }

      if (!vectorIsCurrent(*node_buf_, *vec_buf_, truth)) {
        ++stale_;
        continue;  // the node moved on between the two reads
      }

      node = *node_buf_;
      vec = *vec_buf_;
      return true;
    }
    return false;
  }

  /// Fetch a vector by offset. No validation: the caller may be walking the
  /// old_ver chain, where a superseded version is exactly what it asked for.
  bool readVec(VecOffset off, VecRecord &vec) {
    if (off == kNullVec) return false;
    auto &rc = *conns_[replica_];
    blockingRead(rc, vec_buf_, kVecRecordBytes,
                 layout_.vecAddrOf(rc.remoteBuf(), off));
    ++vec_reads_;
    vec = *vec_buf_;
    return true;
  }

  [[nodiscard]] uint64_t reads() const { return node_reads_ + vec_reads_; }
  [[nodiscard]] uint64_t nodeReads() const { return node_reads_; }
  [[nodiscard]] uint64_t vecReads() const { return vec_reads_; }
  /// Bytes off the wire, which is the number that matters: a header is 64 B
  /// and a vector 320 B, so counting reads alone hides the difference the
  /// layout exists to create.
  [[nodiscard]] uint64_t bytesRead() const {
    return node_reads_ * kNodeRecordBytes + vec_reads_ * kVecRecordBytes;
  }
  [[nodiscard]] uint64_t unstableRetries() const { return unstable_; }
  [[nodiscard]] uint64_t staleRetries() const { return stale_; }

 private:
  static constexpr int kRetries = 16;

  Conns &conns_;
  Layout const &layout_;
  NodeRecord *node_buf_;
  VecRecord *vec_buf_;
  VecOffsetHint *hint_;
  size_t replica_;
  uint64_t node_reads_ = 0;
  uint64_t vec_reads_ = 0;
  uint64_t unstable_ = 0;
  uint64_t stale_ = 0;
};

/// The full Ops surface ds_descend.hpp expects, over real RDMA.
///
/// Extends the reader with the four compare-and-swaps the helping path needs.
/// Each returns whether it succeeded; every caller in the descent ignores that,
/// because a failure means another thread already performed the step.
///
/// Blocking, like everything else in this header, so it belongs to bootstrap,
/// --selftest and the sequential phases -- not to a pipelined benchmark loop.
template <class Conns>
class RdmaOps : public RdmaNodeReader<Conns> {
  using Base = RdmaNodeReader<Conns>;

 public:
  /// @param stage_node,stage_vec  MR-resident staging buffers the write path
  ///        copies into before handing the bytes to the HCA. Only needed for
  ///        F1/F2; a read-only or verification-only user may pass nullptr.
  /// @param nodes,vecs  this client's bump allocators. Also optional, for the
  ///        same reason.
  RdmaOps(Conns &conns, Layout const &layout, NodeRecord *node_buf,
          VecRecord *vec_buf, uint64_t *cas_buf, VecOffsetHint *hint = nullptr,
          size_t replica = 0, NodeRecord *stage_node = nullptr,
          VecRecord *stage_vec = nullptr, NodeAllocator *nodes = nullptr,
          VecAllocator *vecs = nullptr)
      : Base(conns, layout, node_buf, vec_buf, hint, replica),
        conns_(conns),
        layout_(layout),
        cas_buf_(cas_buf),
        replica_(replica),
        stage_node_(stage_node),
        stage_vec_(stage_vec),
        nodes_(nodes),
        vecs_(vecs) {}

  bool casTs(VecOffset off, uint64_t expected, uint64_t desired) {
    auto &rc = *conns_[replica_];
    ++cas_;
    return blockingCas(rc, cas_buf_, layout_.vecTsAddrOf(rc.remoteBuf(), off),
                       expected, desired);
  }

  bool casNextId(RemoteAddr a, uint64_t expected, uint64_t desired) {
    auto &rc = *conns_[replica_];
    ++cas_;
    return blockingCas(rc, cas_buf_,
                       Layout::nextIdAddrOf(rc.remoteBuf(), a), expected,
                       desired);
  }

  bool casNextKMin(RemoteAddr a, Key expected, Key desired) {
    auto &rc = *conns_[replica_];
    ++cas_;
    return blockingCas(rc, cas_buf_,
                       Layout::nextKMinAddrOf(rc.remoteBuf(), a), expected,
                       desired);
  }

  /// tail_struct_ver is 4 bytes but RDMA CAS is 8, so it is swapped as the word
  /// it shares with `level` -- which never changes after creation, and whose
  /// inclusion makes the CAS fail if the node is not the one we read.
  bool casTailWord(RemoteAddr a, uint64_t expected, uint64_t desired) {
    auto &rc = *conns_[replica_];
    ++cas_;
    return blockingCas(rc, cas_buf_,
                       Layout::tailWordAddrOf(rc.remoteBuf(), a), expected,
                       desired);
  }

  /// A helper's timestamp source.
  ///
  /// steady_clock is a placeholder, and deliberately not good enough for A10:
  /// a real snapshot timestamp has to be comparable across machines, which
  /// means a disciplined clock read and a measured epsilon. Until that is
  /// decided this is monotonic per process, which is enough to keep the
  /// old_ver chain ordered locally and not enough to order anything across
  /// clients -- its epoch is boot time, so two nodes disagree by their uptime
  /// difference. Flagged rather than hidden, since a wrong clock here produces
  /// wrong range-query results rather than a crash.
  ///
  /// Measured on this testbed (docs/clock-measurements.md): relative TSC
  /// frequency error across the 12 nodes is 14.31 ppm, so a one-shot reset
  /// accumulates 143 us of skew over a 10 s run against ~2 us operations. A
  /// disciplined CLOCK_REALTIME read costs 31.4 ns against rdtscp's 20.1 ns,
  /// so the fix is to keep rdtsc as the source and let the kernel correct the
  /// rate -- not to hand-roll the counter. Nothing on the point-read, F1 or F2
  /// paths compares timestamps, so that change is not a prerequisite for them.
  uint64_t now() {
    return static_cast<uint64_t>(
        std::chrono::steady_clock::now().time_since_epoch().count());
  }

  // ── The write half of the Ops surface (F1/F2) ─────────────────────────────

  /// Stage a vector and write it to every replica.
  ///
  /// The bytes must pass through the registered MR, because the HCA reads the
  /// source buffer directly -- so the caller's VecRecord (which lives in
  /// ordinary client memory) is copied into the staging slot first. One 320-byte
  /// write per replica.
  bool writeVec(VecOffset off, VecRecord const &vec) {
    if (stage_vec_ == nullptr) return false;
    *stage_vec_ = vec;
    writeVecAllReplicas(conns_, layout_, stage_vec_, off);
    ++vec_writes_;
    return true;
  }

  /// Stage a node header and write it to every replica.
  bool writeNode(RemoteAddr a, NodeRecord const &node) {
    if (stage_node_ == nullptr) return false;
    *stage_node_ = node;
    writeNodeAllReplicas(conns_, stage_node_, a);
    ++node_writes_;
    return true;
  }

  /// The publishing CAS: the operation's linearization point (L1).
  ///
  /// The handle sits at offset 0 in the node, so its address is the node's
  /// address -- which is why nodeAddrOf doubles as the CAS target.
  bool casHandle(RemoteAddr a, uint64_t expected, uint64_t desired) {
    auto &rc = *conns_[replica_];
    ++cas_;
    return blockingCas(rc, cas_buf_, Layout::nodeAddrOf(rc.remoteBuf(), a),
                       expected, desired);
  }

  /// Issue a chained batch on this Ops' single replica.
  ///
  /// The N=1 direct path. Shares postBatchChain with the replicated path, so
  /// there is one implementation of "a batch on the wire" rather than two to
  /// keep in step -- the difference between N=1 and N=3 is how many chains get
  /// posted before anything is drained, not how a chain is built.
  BatchResult submit(Batch const &b) {
    BatchResult out;
    if (!b.wellFormed()) return out;
    auto &rc = *conns_[replica_];
    uint64_t *cas = cas_buf_;
    stageBatchPayloads(b, stage_node_, stage_vec_);
    size_t const to_drain =
        postBatchChain(rc, layout_, b, stage_node_, stage_vec_, cas, doorbell_);
    if (to_drain == 0) return out;
    for (size_t i = 0; i < to_drain; ++i) detail::awaitOne(rc, "chained batch");
    ++batches_;
    vec_writes_ += b.vecWrites();
    node_writes_ += b.nodeWrites();
    cas_ += b.size() - b.vecWrites() - b.nodeWrites();
    out.submitted = true;
    out.committed = batchCommitted(b, cas);
    return out;
  }

  /// A fresh vector offset from this client's stripe, or kNullVec when spent.
  ///
  /// Exhaustion is a hard error rather than a retry: with no reclamation, it
  /// means the run outlasted the arena it was sized for, and the fix is a larger
  /// --vecs-per-client.
  VecOffset allocVec() { return vecs_ == nullptr ? kNullVec : vecs_->allocate(); }
  RemoteAddr allocNode() {
    return nodes_ == nullptr ? RemoteAddr{} : nodes_->allocate();
  }

  [[nodiscard]] uint64_t casCount() const { return cas_; }
  /// Chained submissions, i.e. round trips spent writing.
  [[nodiscard]] uint64_t batches() const { return batches_; }
  void setDoorbell(bool on) { doorbell_ = on; }
  [[nodiscard]] uint64_t nodeWrites() const { return node_writes_; }
  [[nodiscard]] uint64_t vecWrites() const { return vec_writes_; }
  [[nodiscard]] uint64_t bytesWritten() const {
    return node_writes_ * kNodeRecordBytes + vec_writes_ * kVecRecordBytes;
  }

 private:
  Conns &conns_;
  Layout const &layout_;
  uint64_t *cas_buf_;
  size_t replica_;
  NodeRecord *stage_node_ = nullptr;
  VecRecord *stage_vec_ = nullptr;
  NodeAllocator *nodes_ = nullptr;
  VecAllocator *vecs_ = nullptr;
  bool doorbell_ = true;
  uint64_t node_writes_ = 0;
  uint64_t vec_writes_ = 0;
  uint64_t batches_ = 0;
  uint64_t cas_ = 0;
};


/// The per-replica primitive surface ds_quorum.hpp builds CAS-ABD on.
///
/// Deliberately thin: every method is one RDMA against one named replica, with
/// no quorum logic at all. All the interesting decisions -- which value wins a
/// read, what counts as committed, when to write back -- live in QuorumOps,
/// where they are testable against a fake replica set instead of a fabric.
///
/// Buffers are indexed by replica, which the layout already provisions for:
/// nodeBufsSize() and friends are num_servers wide, because a quorum read has
/// three headers in flight and they cannot share a landing slot.
///
/// Blocking, like everything else in this header, so it belongs to bootstrap
/// and --selftest rather than a pipelined benchmark loop. A quorum read here is
/// three *serialised* round trips; the future-based path is what makes them
/// concurrent, and until that lands the replication factor multiplies latency
/// rather than just bandwidth. Worth knowing before reading any N=3 timing as
/// representative.
template <class Conns>
class RdmaReplicaSet {
 public:
  RdmaReplicaSet(Conns &conns, Layout const &layout, NodeRecord *node_bufs,
                 VecRecord *vec_bufs, uint64_t *cas_bufs,
                 NodeRecord *stage_node, VecRecord *stage_vec,
                 NodeAllocator *nodes, VecAllocator *vecs,
                 VecOffsetHint *hint = nullptr)
      : conns_(conns),
        layout_(layout),
        node_bufs_(node_bufs),
        vec_bufs_(vec_bufs),
        cas_bufs_(cas_bufs),
        stage_node_(stage_node),
        stage_vec_(stage_vec),
        nodes_(nodes),
        vecs_(vecs),
        hint_(hint) {}

  size_t replicas() const { return conns_.size(); }

  bool readNodeFrom(size_t r, RemoteAddr a, NodeRecord &node) {
    if (a.isNull()) return false;
    auto &rc = *conns_[r];
    blockingRead(rc, &node_bufs_[r], kNodeRecordBytes,
                 Layout::nodeAddrOf(rc.remoteBuf(), a));
    ++reads_;
    // Only replica 0 feeds the hint. The hint is a guess at the *current*
    // offset, and a lagged replica's offset would poison it -- a wrong guess
    // costs a wasted 320-byte read, so it is worth keeping honest.
    if (hint_ != nullptr && r == 0) {
      hint_->record(a, node_bufs_[r].handle.offset(), kNullVec);
    }
    node = node_bufs_[r];
    return true;
  }

  bool readVecFrom(size_t r, VecOffset off, VecRecord &vec) {
    if (off == kNullVec) return false;
    auto &rc = *conns_[r];
    blockingRead(rc, &vec_bufs_[r], kVecRecordBytes,
                 layout_.vecAddrOf(rc.remoteBuf(), off));
    ++reads_;
    vec = vec_bufs_[r];
    return true;
  }

  bool casHandleOn(size_t r, RemoteAddr a, uint64_t expected, uint64_t desired) {
    auto &rc = *conns_[r];
    ++cas_;
    return blockingCas(rc, &cas_bufs_[r * kMaxBatchOps],
                       Layout::nodeAddrOf(rc.remoteBuf(), a), expected,
                       desired);
  }

  /// Issue a chained batch on every replica.
  ///
  /// THE ORDER HERE IS THE WHOLE POINT. Every replica's chain is posted before
  /// any of them is drained, so three replicas cost one round trip of latency
  /// rather than three. Posting and draining one replica at a time -- which is
  /// what a loop of blocking helpers does, and what the first version of this
  /// file did -- makes replication multiply latency instead of bandwidth. This
  /// is chimera's put_future.hpp pattern: post to all servers, then poll.
  void submitAll(Batch const &b, bool *submitted, bool *committed) {
    size_t const n = conns_.size();
    size_t to_drain[kMaxReplicaFanout] = {};

    // Stage the payloads once. Every replica's work requests read from these
    // same buffers, because the bytes are identical on all of them -- which is
    // also why the staging region is sized for one chain rather than for one
    // chain per replica.
    stageBatchPayloads(b, stage_node_, stage_vec_);

    // Phase 1: post everywhere. Nothing is awaited yet.
    for (size_t r = 0; r < n; ++r) {
      submitted[r] = false;
      committed[r] = false;
      to_drain[r] = postBatchChain(*conns_[r], layout_, b, stage_node_,
                                   stage_vec_, &cas_bufs_[r * kMaxBatchOps],
                                   doorbell_);
      if (to_drain[r] > 0) {
        writes_ += b.vecWrites() + b.nodeWrites();
        cas_ += b.size() - b.vecWrites() - b.nodeWrites();
      }
    }
    ++batches_;

    // Phase 2: drain. The completions have been overlapping since phase 1.
    for (size_t r = 0; r < n; ++r) {
      if (to_drain[r] == 0) continue;
      for (size_t i = 0; i < to_drain[r]; ++i) {
        detail::awaitOne(*conns_[r], "chained batch");
      }
      submitted[r] = true;
      // Only now are the swapbacks meaningful.
      committed[r] = batchCommitted(b, &cas_bufs_[r * kMaxBatchOps]);
    }
  }

  uint64_t now() {
    return static_cast<uint64_t>(
        std::chrono::steady_clock::now().time_since_epoch().count());
  }

  // Client-local, so one of each however many replicas there are: a RemoteAddr
  // and a VecOffset mean the same thing on every replica, which is what lets
  // the same bytes go to all of them.
  VecOffset allocVec() { return vecs_ == nullptr ? kNullVec : vecs_->allocate(); }
  RemoteAddr allocNode() {
    return nodes_ == nullptr ? RemoteAddr{} : nodes_->allocate();
  }

  [[nodiscard]] uint64_t replicaReads() const { return reads_; }
  [[nodiscard]] uint64_t replicaWrites() const { return writes_; }
  [[nodiscard]] uint64_t replicaCas() const { return cas_; }
  /// Chained submissions, i.e. round trips spent writing. Independent of the
  /// replica count, which is the property the chaining exists to create.
  [[nodiscard]] uint64_t batches() const { return batches_; }

  /// Turn the doorbell batching off, so its benefit can be measured rather
  /// than asserted -- the same reasoning as --offset-hint.
  void setDoorbell(bool on) { doorbell_ = on; }

 private:
  /// Bound on the per-replica scratch arrays. invariants.md §9 restricts the
  /// toggle to 1 or 3.
  static constexpr size_t kMaxReplicaFanout = 8;

  Conns &conns_;
  Layout const &layout_;
  NodeRecord *node_bufs_;
  VecRecord *vec_bufs_;
  uint64_t *cas_bufs_;
  NodeRecord *stage_node_;
  VecRecord *stage_vec_;
  NodeAllocator *nodes_;
  VecAllocator *vecs_;
  VecOffsetHint *hint_;
  bool doorbell_ = true;
  uint64_t reads_ = 0, writes_ = 0, cas_ = 0, batches_ = 0;
};

}  // namespace ds
