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
  RdmaOps(Conns &conns, Layout const &layout, NodeRecord *node_buf,
          VecRecord *vec_buf, uint64_t *cas_buf, VecOffsetHint *hint = nullptr,
          size_t replica = 0)
      : Base(conns, layout, node_buf, vec_buf, hint, replica),
        conns_(conns),
        layout_(layout),
        cas_buf_(cas_buf),
        replica_(replica) {}

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
  /// means a PTP-disciplined clock read and a measured epsilon. Until that is
  /// decided this is monotonic per process, which is enough to keep the
  /// old_ver chain ordered locally and not enough to order anything across
  /// clients. Flagged rather than hidden, since a wrong clock here produces
  /// wrong range-query results rather than a crash.
  uint64_t now() {
    return static_cast<uint64_t>(
        std::chrono::steady_clock::now().time_since_epoch().count());
  }

  [[nodiscard]] uint64_t casCount() const { return cas_; }

 private:
  Conns &conns_;
  Layout const &layout_;
  uint64_t *cas_buf_;
  size_t replica_;
  uint64_t cas_ = 0;
};

}  // namespace ds
