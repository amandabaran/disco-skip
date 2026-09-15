#pragma once

// The AsyncOps surface over real RDMA: post, return, resolve on completion.
//
// This is what lets TraversalFuture run on the cluster. Everything here posts
// and returns; nothing waits. The driver (DsClient::tickRdma) reaps completions,
// routes them by wr_id to the owning future, and calls tryStepForward once that
// future's outstanding count reaches zero -- at which point the resolve half
// reads the results out of the per-future buffers.
//
// Contrast ds_rdma.hpp, whose helpers each post one work request and spin. Those
// are correct for bootstrap and --selftest and fundamentally wrong for a
// benchmark: a client that blocks can have one operation in flight, so its
// throughput is one operation per operation-latency however fast the fabric is.
//
// WR IDS. Every request carries the future's id, because that is how the driver
// knows whose completion it is. DsClient reserves the top bit to distinguish the
// legacy range futures, so ids must stay below 2^63 -- which they trivially do,
// being indices into async_parallelism.
//
// BUFFERS ARE PER FUTURE, and reused across the nodes of one traversal. That is
// safe because a traversal consumes each result before posting the next: it is
// sequential *within* an operation and concurrent only *across* them. The
// concurrency that matters for throughput is between futures, and each has its
// own slice of the registered region.

#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>

#include <dory/conn/rc.hpp>
#include <dory/extern/ibverbs.hpp>

#include "ds_batch.hpp"
#include "ds_defs.hpp"
#include "ds_node.hpp"
#include "ds_quorum.hpp"
#include "ds_rdma.hpp"  // postBatchChain, stageBatchPayloads, kBlockingWrId
#include "ds_ts.hpp"
#include "layout.hpp"

namespace ds {

/// Non-blocking quorum operations for one in-flight future.
///
/// @param Conns    the server connection container
/// @param Counters something with `operator[](size_t) -> int64_t&`, the driver's
///                 per-server outstanding-completion tally
template <class Conns, class Counters>
class RdmaAsyncOps {
 public:
  /// @param to_poll  the driver's per-server outstanding tally
  /// @param ongoing  THIS future's per-server tally
  ///
  /// Both are bumped on every post, and the driver decrements both on every
  /// completion. Two counters rather than one because they answer different
  /// questions: the driver polls a server while `to_poll` is positive, and a
  /// future may only step once its own `ongoing` is zero. Bumping only one
  /// would either stop a server being polled or let a future step on partial
  /// results.
  RdmaAsyncOps(Conns &conns, Layout const &layout, Counters &to_poll,
               Counters &ongoing, uint64_t future_id, QuorumStats &stats,
               VecOffsetHint *hint = nullptr, NodeAllocator *nodes = nullptr,
               VecAllocator *vecs = nullptr)
      : conns_(conns),
        layout_(layout),
        to_poll_(to_poll),
        ongoing_(ongoing),
        future_id_(future_id),
        stats_(stats),
        hint_(hint),
        nodes_(nodes),
        vecs_(vecs) {}

  [[nodiscard]] size_t replicas() const { return conns_.size(); }
  [[nodiscard]] size_t majority() const { return conns_.size() / 2 + 1; }

  VecOffset guess(RemoteAddr a) {
    return hint_ != nullptr ? hint_->guess(a) : kNullVec;
  }

  // ── Headers: the quorum read, optionally carrying a speculation ───────────

  /// Post a header read to every replica, and the guessed vector read alongside.
  ///
  /// Replica 0's header and the speculation are ONE chain on one queue pair, so
  /// a correct guess removes the serialisation between a header and the vector
  /// its handle names. The other replicas are separate queue pairs: nothing to
  /// chain them to, only to overlap with.
  ///
  /// @return completions the driver will see for this post
  size_t postHeaders(RemoteAddr a, VecOffset speculate) {
    ++stats_.round_trips;  // one post/resolve pair = one round trip
    size_t const n = conns_.size();
    bumped_ = 0;
    spec_pending_ = speculate;
    NodeRecord *const hdrs = layout_.getNodeBufs(future_id_);
    VecRecord *const spec = layout_.getVecBufs(future_id_);
    size_t completions = 0;

    // WHICH REPLICA THE SPECULATIVE VECTOR READ GOES TO.
    //
    // It was hardcoded to replica 0, and that -- not the winner selection --
    // is where the load imbalance actually lived. The offset hint runs at a
    // 0.964 hit rate on workload E, so ~96% of vector reads are this
    // speculative one, issued BEFORE any winner is known; the winner-based
    // spreading in resolveWalkHeader only decides anything on a spec MISS.
    //
    // Measured: with only the winner path spread, w1 still carried 4.07x the
    // bytes of w2/w3 (down from 4.68) -- just 650 MB of 11,804 MB of vector
    // traffic moved, 5.5%, which is the miss rate and nothing more.
    //
    // Same spread key as the winner path (offset % n) so a walk's consecutive
    // nodes speculate against different replicas.
    // ── WHICH REPLICAS TO ASK (--read-quorum) ─────────────────────────────
    //
    // CAS-ABD's read phase needs the max tag over a QUORUM, not over every
    // replica: the CAS that follows is ABD's write-back phase, and it is the
    // CAS reaching a majority that makes the write durable. So asking all n is
    // a latency choice, not a correctness one.
    //
    // Headers are 45% of this workload's bytes (measured: 3 x 3204 MB of
    // headers against 11804 MB of vectors), so dropping to a quorum removes
    // about 15% of all traffic and a third of the header MESSAGES.
    //
    // WHICH quorum rotates with the node address, so the header load spreads
    // instead of every replica serving every header -- the same idea as
    // vecSourceFor, applied to the read phase.
    //
    // THE COST IS REAL. With exactly majority() replicas asked, ALL of them
    // must agree for a handle to reach a majority. Under write contention they
    // often do not -- the code already records "under sustained write
    // contention on one key the replicas are essentially never in agreement",
    // 76,311 failed reads on workload D at 8 clients -- and each disagreement
    // then costs a retry that asking all n would have avoided. Expect this to
    // win on read-heavy workloads and lose on write-contended ones; that is
    // why it is a toggle and not a default.
    size_t const fanout =
        (layout_.read_quorum && !header_retry_) ? majority() : n;
    // Retry asks EVERYONE: a quorum that disagreed will disagree again, so
    // repeating the same subset is a livelock.
    header_retry_ = false;
    size_t const first = layout_.read_quorum
                             ? static_cast<size_t>(a.id % n)
                             : 0;
    read_mask_ = 0;
    for (size_t k = 0; k < fanout; ++k) read_mask_ |= 1u << ((first + k) % n);

    // The speculative vector read must go to a replica we ACTUALLY READ THE
    // HEADER FROM, because resolveHeaders validates it against that header.
    size_t spec_r = first;
    if (layout_.spread_reads && speculate != kNullVec) {
      size_t const want = static_cast<size_t>(speculate % n);
      spec_r = (read_mask_ & (1u << want)) ? want : first;
    }
    spec_replica_ = spec_r;

    for (size_t r = 0; r < n; ++r) {
      if (!(read_mask_ & (1u << r))) continue;   // not asked this round
      auto &rc = *conns_[r];
      uintptr_t const remote = Layout::nodeAddrOf(rc.remoteBuf(), a);
      if (r == spec_r && speculate != kNullVec) {
        struct ibv_send_wr wr[2];
        struct ibv_sge sg[2];
        rc.prepareSingle(wr[0], sg[0],
                         dory::conn::ReliableConnection::RdmaRead, future_id_,
                         &hdrs[0], kNodeRecordBytes, remote, /*signaled=*/false);
        rc.prepareSingle(wr[1], sg[1],
                         dory::conn::ReliableConnection::RdmaRead, future_id_,
                         &spec[0], kVecRecordBytes,
                         layout_.vecAddrOf(rc.remoteBuf(), speculate),
                         /*signaled=*/true);
        wr[0].next = &wr[1];
        if (!rc.postSend(wr[0])) {
          throw std::runtime_error("failed to post a speculative header chain");
        }
        // One chain, one signalled request, one completion.
        completions += 1;
        bump(r, 1);
      } else {
        if (!rc.postSendSingle(dory::conn::ReliableConnection::RdmaRead,
                               future_id_, &hdrs[r], kNodeRecordBytes, remote)) {
          throw std::runtime_error("failed to post a quorum header read");
        }
        completions += 1;
        bump(r, 1);
      }
    }
    ++stats_.node_reads;
    stats_.replica_reads += fanout + (speculate != kNullVec ? 1u : 0u);
    if (speculate != kNullVec) ++stats_.speculated;
    return postedExactly(completions, "postHeaders");
  }

  /// Resolve the quorum over the buffered headers.
  ///
  /// Highest tag with majority support, exactly as the blocking QuorumOps does
  /// and for the same reason: a partially applied commit puts two distinct
  /// handles on the same tag, so counting votes per value rather than per tag
  /// is what keeps an uncommitted version from being returned.
  ///
  /// @return false if no value has majority support yet; the caller re-posts
  bool resolveHeaders(RemoteAddr a, NodeRecord &node, bool &have_vec,
                      VecRecord &vec) {
    size_t const n = conns_.size();
    NodeRecord const *const hdrs = layout_.getNodeBufs(future_id_);

    size_t best = n;
    uint64_t best_raw = 0;
    for (size_t r = 0; r < n; ++r) {
      // Skip replicas we did not ask: their slot still holds an EARLIER
      // operation's header (the node buffers are reused per future), so
      // counting it would fabricate agreement from stale bytes. read_mask_ is
      // all-ones except under --read-quorum in postHeaders.
      if (!(read_mask_ & (1u << r))) continue;
      size_t votes = 0;
      for (size_t q = 0; q < n; ++q) {
        if (!(read_mask_ & (1u << q))) continue;
        if (hdrs[q].handle == hdrs[r].handle) ++votes;
      }
      if (votes < majority()) continue;
      // MAX RAW, NOT MAX TAG -- the same rule the repair paths use.
      //
      // The offset occupies the LOW 32 bits, so raw order is tag order refined
      // by offset: a bigger offset can never let a staler tag win. The offset
      // is allocated from a per-client stripe by a monotonic bump allocator
      // (layout.hpp VecAllocator: next_++, no reclamation), so raw sorts as
      // (struct_ver, content_ver, writer, attempt) -- a total order every
      // reader computes identically.
      //
      // At n=3 this cannot change WHICH handle is chosen, because of the
      // majority filter above: two distinct handles can never both clear a
      // majority (2*(n/2+1) > n for every n), so exactly one distinct handle
      // survives the filter and there is nothing left for the refinement to
      // order. It matters in repairRead/resolveRepair, which deliberately adopt
      // a handle NO majority holds. Kept identical here so the two paths cannot
      // drift apart, and so the next reader does not have to rediscover why one
      // compares tags and the other compares words.
      uint64_t const raw = hdrs[r].handle.raw;
      if (best == n || raw > best_raw) {
        best = r;
        best_raw = raw;
      }
    }
    if (best == n) {
      ++stats_.read_retries;
      // Ask EVERY replica next time: re-asking the quorum that just disagreed
      // returns the same answer forever.
      header_retry_ = true;
      return false;
    }


    // ── LOAD-SPREAD THE VECTOR READ (--spread-reads) ──────────────────────
    //
    // A node's HEADER is read from every replica to form the quorum, but its
    // VECTOR -- the 320-byte payload, the expensive half -- is read from one.
    // Which one was decided by the loop above, and that loop uses a strict `>`:
    // when the replicas AGREE, which is the normal case, every candidate has an
    // equal raw handle, so r=0 sets `best` and nothing ever displaces it.
    //
    // Replica 0 therefore served EVERY vector read and the other two served
    // only 64-byte headers. Measured on the cluster with IB port counters
    // during a workload-E run:
    //
    //     w1   179 MB/s   avg packet 213 B   <- headers AND all vectors
    //     w2    38 MB/s   avg packet  87 B   <- headers only
    //     w3    38 MB/s   avg packet  87 B   <- headers only
    //
    // The average packet sizes are the proof: 87 B is a 64-byte header read and
    // nothing else, 213 B is that mixed with 320-byte vectors.
    //
    // WHY THIS IS SAFE FOR LINEARIZABILITY. The candidates considered here all
    // satisfy `handle == hdrs[best].handle`, i.e. the SAME 64-bit word: same
    // (struct_ver, content_ver) and, because the offset is the low 32 bits, the
    // same vector offset. They are byte-identical copies of one version, and
    // each voted with the winner (L4). Choosing among them changes WHICH COPY
    // is fetched, never WHAT is fetched, so no reader can observe a different
    // value and no order is affected. A replica that merely reached majority
    // with a DIFFERENT handle is not a candidate.
    //
    // WHY offset % n AND NOT round-robin OR client id. It must be stateless and
    // it must vary WITHIN one range walk -- a walk touches ~10.9 nodes, and the
    // point is that consecutive nodes land on different replicas. Client id
    // pins a client to one replica and spreads nothing within a walk;
    // round-robin needs mutable state on a hot path. The offset is already in
    // the winning handle, needs no extra argument, and is dense per client
    // stripe so consecutive allocations spread evenly.
    //
    // It does NOT spread the speculative vector read in resolveHeaders, which
    // is pinned to replica 0 separately; that path serves point gets, not the
    // range walk this targets.
    // SPREAD THE VECTOR SOURCE, NOT THE NODE RECORD. Moving `best` itself was
    // a livelock: see the note above vecSourceFor.
    node = hdrs[best];
    winner_ = vecSourceFor(hdrs, best, n);
    VecOffset const truth = node.handle.offset();

    have_vec = false;
    if (spec_pending_ != kNullVec) {
      // L4, strictly: usable only if replica 0 voted with the winner -- which,
      // since a handle carries its offset, means the speculation read the
      // version that won.
      // MUST be the replica the speculative read was ISSUED TO, not replica 0.
      // The speculated bytes are usable iff THAT replica holds the winning
      // handle -- same version, same offset -- and the guessed offset is the
      // winner's. Testing replica 0 while having read from another would
      // accept bytes from a replica that never voted with the winner, which is
      // a linearizability violation, not an optimisation.
      if (hdrs[spec_replica_].handle == hdrs[best].handle &&
          spec_pending_ == truth) {
        vec = layout_.getVecBufs(future_id_)[0];
        have_vec = true;
        ++stats_.spec_hits;
      } else {
        ++stats_.spec_misses;
      }
    }
    if (hint_ != nullptr) hint_->record(a, truth, spec_pending_);
    spec_pending_ = kNullVec;
    return true;
  }

  // ── A vector read, from a replica that voted with the winner (L4) ────────

  /// ── A10: the batched range walk ────────────────────────────────────────
  ///
  /// Fetch up to Layout::kWalkFanout data nodes in ONE round trip instead of
  /// one round trip each.
  ///
  /// WHY THIS EXISTS. A range walks data nodes left to right by chasing
  /// next_id, and each hop is a round trip because the next address is not
  /// known until the current node is read. Measured on workload E: 10.9 nodes
  /// per range at scan length 100, rising to 26.2 at 255 -- so the walk cost
  /// grows linearly with scan length and caps our throughput at ~0.41x of a
  /// local LSM no matter how long the scan (measured 8 through 255; the ratio
  /// plateaus).
  ///
  /// The index breaks the dependency. A level-0 index node's entries name up to
  /// kNodeCapacity CONSECUTIVE data nodes, so their addresses are all known
  /// after one vector read and can be fetched together. 26 serial round trips
  /// become 2. A local LSM has no equivalent because it has no round trips to
  /// remove.
  ///
  /// Headers are grouped BY NODE -- walkNodeBufs + i * replicas is node i's
  /// replica set -- because a node read is a quorum read and the majority rule
  /// applies per node over its own replicas.
  size_t postWalkHeaders(RemoteAddr const *addrs, size_t n) {
    ++stats_.round_trips;  // one post/resolve pair = one round trip
    // This path asks EVERY replica for every node, so the vote loops in
    // resolveWalkHeader must consider all of them. read_mask_ is shared with
    // postHeaders, which narrows it under --read-quorum, so it has to be reset
    // here or a walk would silently ignore replicas it did read.
    read_mask_ = ~0u;
    size_t const r_count = conns_.size();
    bumped_ = 0;
    walk_n_ = n;
    NodeRecord *const bufs = layout_.getWalkNodeBufs(future_id_);
    size_t completions = 0;
    for (size_t i = 0; i < n; ++i) walk_addr_[i] = addrs[i];
    if (n == 0) return postedExactly(0, "postWalkHeaders");

    // ONE ibv_post_send PER REPLICA, not one per node per replica.
    //
    // This used to call postSendSingle n * replicas times -- 24 verbs calls for
    // an 8-node backbone across 3 replicas. Every ibv_post_send takes a
    // per-queue-pair spinlock, and perf put pthread_spin_lock at 15.9% of this
    // client's CPU on workload E: the cost scales with the NUMBER OF CALLS, not
    // with bytes or round trips. That is why batching reads into fewer round
    // trips measured flat -- it left the call count alone.
    //
    // Chaining through wr.next posts the whole backbone for one replica in a
    // single call, so 24 becomes 3.
    for (size_t r = 0; r < r_count; ++r) {
      auto &rc = *conns_[r];
      struct ibv_send_wr wr[Layout::kWalkFanout];
      struct ibv_sge sg[Layout::kWalkFanout];
      for (size_t i = 0; i < n; ++i) {
        rc.prepareSingle(wr[i], sg[i],
                         dory::conn::ReliableConnection::RdmaRead, future_id_,
                         &bufs[i * r_count + r], kNodeRecordBytes,
                         Layout::nodeAddrOf(rc.remoteBuf(), addrs[i]),
                         /*signaled=*/true);
        wr[i].next = (i + 1 < n) ? &wr[i + 1] : nullptr;
      }
      if (!rc.postSend(wr[0])) {
        throw std::runtime_error("failed to post a batched header read");
      }
      bump(r, n);
      completions += n;
    }
    stats_.node_reads += n;
    stats_.replica_reads += completions;
    return postedExactly(completions, "postWalkHeaders");
  }

  /// Resolve node /i/ of the batch. Same majority rule as resolveHeaders.
  ///
  /// @return false when no handle has majority support for that node -- the
  ///         caller falls back to the one-at-a-time path for it rather than
  ///         failing the whole batch, since the others are fine.
  /// Which replica to FETCH THE VECTOR FROM, given the node's quorum winner.
  ///
  /// ── WHY THIS IS SEPARATE FROM `best` ──────────────────────────────────
  ///
  /// The first attempt at load-spreading moved `best` itself, so the caller
  /// then took its whole NodeRecord -- handle, k_min, next_id, next_k_min --
  /// from whichever replica was chosen. That LIVELOCKED: two clients tripped
  /// "future N is stuck after 4097 steps without finishing", every cell at 32
  /// and 64 clients timed out, and 8 clients failed intermittently.
  ///
  /// The reason is in ds_node.hpp. `next_id` is "mutated in place by a split
  /// ... the CAS on the handle and the write of this field are SEPARATE
  /// operations, so a reader can land between them", and `next_k_min` is "only
  /// trustworthy while the node is stable". So two replicas can carry the SAME
  /// handle and DIFFERENT successor fields: the handle CAS has landed on both,
  /// the next_id write has landed on only one. Taking the node record from an
  /// arbitrary agreeing replica can therefore hand the walk a stale successor,
  /// and a range that chases the wrong next_id does not terminate. Contention
  /// dependent, which is why it barely showed at 8 clients and always at 32.
  ///
  /// THE VECTOR HAS NO SUCH HAZARD. Versions are copy-on-write, so an offset
  /// names one immutable payload; every replica holding the winning handle
  /// holds the same offset and therefore byte-identical vector contents. There
  /// is no second write to race with. Spreading the FETCH is safe; spreading
  /// which record you believe is not.
  ///
  /// Candidates are restricted to `handle == hdrs[best].handle` -- the same
  /// 64-bit word, so the same version AND the same offset (offset is the low
  /// 32 bits) -- which is also the L4 requirement that the replica voted with
  /// the winner. Falls back to `best` when the preferred replica disagrees.
  size_t vecSourceFor(NodeRecord const *hdrs, size_t best, size_t n) const {
    if (!layout_.spread_reads || best >= n) return best;
    size_t const pref = static_cast<size_t>(hdrs[best].handle.offset() % n);
    for (size_t k = 0; k < n; ++k) {
      size_t const r = (pref + k) % n;
      // Only a replica whose header we READ can be known to hold the winning
      // handle; an unasked slot holds an earlier operation's bytes.
      if (!(read_mask_ & (1u << r))) continue;
      if (hdrs[r].handle == hdrs[best].handle) return r;
    }
    return best;
  }

  bool resolveWalkHeader(size_t i, NodeRecord &node, size_t &winner) {
    size_t const n = conns_.size();
    NodeRecord const *const hdrs =
        layout_.getWalkNodeBufs(future_id_) + i * n;
    size_t best = n;
    uint64_t best_raw = 0;
    for (size_t r = 0; r < n; ++r) {
      // Skip replicas we did not ask: their slot still holds an EARLIER
      // operation's header (the node buffers are reused per future), so
      // counting it would fabricate agreement from stale bytes. read_mask_ is
      // all-ones except under --read-quorum in postHeaders.
      if (!(read_mask_ & (1u << r))) continue;
      size_t votes = 0;
      for (size_t q = 0; q < n; ++q) {
        if (!(read_mask_ & (1u << q))) continue;
        if (hdrs[q].handle == hdrs[r].handle) ++votes;
      }
      if (votes < majority()) continue;
      // Max raw, not max tag -- see the note in resolveHeaders.
      uint64_t const raw = hdrs[r].handle.raw;
      if (best == n || raw > best_raw) { best = r; best_raw = raw; }
    }
    if (best == n) {
      ++stats_.read_retries;
      // Ask EVERY replica next time: re-asking the quorum that just disagreed
      // returns the same answer forever.
      header_retry_ = true;
      return false;
    }
    node = hdrs[best];
    winner = vecSourceFor(hdrs, best, n);
    return true;
  }

  /// Fetch the vectors for /n/ nodes in one round trip, each from the replica
  /// that supplied its winning header (L4).
  size_t postWalkVecs(VecOffset const *offs, size_t const *winners, size_t n) {
    ++stats_.round_trips;  // one post/resolve pair = one round trip
    bumped_ = 0;
    walk_n_ = n;
    VecRecord *const bufs = layout_.getWalkVecBufs(future_id_);
    size_t completions = 0;
    if (n == 0) return postedExactly(0, "postWalkVecs");

    // GROUPED BY REPLICA, one ibv_post_send each. Same reasoning as
    // postWalkHeaders: the per-call spinlock is the cost, so n calls become at
    // most one per replica. Unlike the headers, the vectors come from whichever
    // replica won each node's quorum, so the chain is built per replica over
    // the nodes that chose it.
    size_t const r_count = conns_.size();
    for (size_t r = 0; r < r_count; ++r) {
      struct ibv_send_wr wr[Layout::kWalkFanout];
      struct ibv_sge sg[Layout::kWalkFanout];
      size_t k = 0;
      auto &rc = *conns_[r];
      for (size_t i = 0; i < n; ++i) {
        if (winners[i] != r) continue;
        rc.prepareSingle(wr[k], sg[k],
                         dory::conn::ReliableConnection::RdmaRead, future_id_,
                         &bufs[i], kVecRecordBytes,
                         layout_.vecAddrOf(rc.remoteBuf(), offs[i]),
                         /*signaled=*/true);
        if (k > 0) wr[k - 1].next = &wr[k];
        ++k;
      }
      if (k == 0) continue;
      wr[k - 1].next = nullptr;
      if (!rc.postSend(wr[0])) {
        throw std::runtime_error("failed to post a batched vector read");
      }
      bump(r, k);
      completions += k;
    }
    stats_.vec_reads += n;
    stats_.replica_reads += completions;
    return postedExactly(completions, "postWalkVecs");
  }

  VecRecord const &walkVec(size_t i) const {
    return layout_.getWalkVecBufs(future_id_)[i];
  }

  static constexpr size_t walkFanout() { return Layout::kWalkFanout; }

  /// ── Snapshot acquisition ───────────────────────────────────────────────
  ///
  /// READ the replicated counter on every replica; never fetch-and-add it. A
  /// writer claims a slot and must FAA; a reader only needs to know how far the
  /// counter has got. That difference is why concurrent range queries do not
  /// contend with each other at all and burn no counter values (ds_range.hpp).
  ///
  /// One round trip whatever the replica count: every read is posted before any
  /// is drained, exactly as postHeaders does.
  ///
  /// A replica that does not answer simply does not vote. The maximum over
  /// those that did is still a valid snapshot -- it sits further behind, which
  /// costs freshness rather than correctness, and is the same trade the write
  /// path makes when it stamps from fewer than all replicas.
  size_t postTsCounter() {
    ++stats_.round_trips;  // one post/resolve pair = one round trip
    size_t const n = conns_.size();
    bumped_ = 0;
    for (size_t r = 0; r < n; ++r) {
      auto &rc = *conns_[r];
      // One 8-byte slot per replica out of this future's CAS scratch. Safe
      // because a snapshot read never overlaps a batch submit on the same
      // future: the range takes its snapshot before it touches any node.
      if (!rc.postSendSingle(dory::conn::ReliableConnection::RdmaRead,
                             future_id_, layout_.casBufsFor(future_id_, r),
                             sizeof(uint64_t),
                             layout_.tsCounterAddrOf(rc.remoteBuf()))) {
        throw std::runtime_error("failed to post a snapshot counter read");
      }
      bump(r, 1);
    }
    stats_.replica_reads += n;
    return postedExactly(n, "postTsCounter");
  }

  /// The maximum counter value over the replicas that answered.
  uint64_t resolveTsCounter() {
    size_t const n = conns_.size();
    uint64_t best = 0;
    for (size_t r = 0; r < n; ++r) {
      uint64_t const v = *layout_.casBufsFor(future_id_, r);
      if (v > best) best = v;
    }
    return best;
  }

  /// ── L2 read repair ────────────────────────────────────────────────────
  ///
  /// Pick a handle deterministically from the buffered headers and CAS it onto
  /// every replica holding something older. This is ABD's read phase: a reader
  /// that finds a partially applied write COMPLETES it rather than waiting for
  /// the writer to, which is what makes the read terminate.
  ///
  /// WHY THIS IS NEEDED. resolveHeaders requires a handle with MAJORITY SUPPORT
  /// -- deliberately stronger than L2, which only requires hearing from a
  /// majority. Under write contention on one key the three replicas are
  /// essentially never in agreement: each write CASes them in a chain, so a
  /// reader sampling all three lands mid-flight, and re-polling does not help
  /// because the next write is already arriving. Workload D at 8 clients failed
  /// 76,311 reads this way, every single one of them here and none at any other
  /// guard.
  ///
  /// WHY MAX-RAW AND NOT MAX-TAG. tag() is (struct_ver, content_ver) and is NOT
  /// unique per write: a writer that loses its CAS re-stages the same logical
  /// update at the same versions with a different offset, so two distinct
  /// handles can share a tag -- the tag_ties case. Adopting "a" max-tag handle
  /// would let two readers adopt DIFFERENT ones and fight. The offset is
  /// allocated from a per-client stripe and so is unique per write, and it
  /// occupies the low 32 bits, so comparing the full raw handle is tag order
  /// refined by offset: a total order every reader computes identically.
  ///
  /// WHY ADOPTING A MINORITY HANDLE IS SAFE. The vector it names was WRITTEN to
  /// every replica before the CAS -- the write is unconditional, only the
  /// publish is contended -- so its contents exist and are self-consistent. Its
  /// writer may consider that attempt abandoned, but completing it is a valid
  /// linearization: the write did happen, and the repair is what fixes its
  /// point. That is precisely what ABD does with a partial write.
  ///
  /// The repair must reach a majority BEFORE the read returns, or a later read
  /// hearing from the other replicas could still see an older value and invert.
  /// resolveRepair reports whether it did.
  ///
  /// @return completions to await; 0 when nothing needs repairing
  size_t postRepair(RemoteAddr a) {
    ++stats_.round_trips;  // one post/resolve pair = one round trip
    size_t const n = conns_.size();
    NodeRecord const *const hdrs = layout_.getNodeBufs(future_id_);
    bumped_ = 0;
    repair_posted_ = 0;

    // Deterministic choice: the largest raw handle any replica showed.
    size_t best = 0;
    for (size_t r = 1; r < n; ++r) {
      if (hdrs[r].handle.raw > hdrs[best].handle.raw) best = r;
    }
    repair_desired_ = hdrs[best].handle.raw;
    repair_addr_ = a;
    repair_holders_ = 0;

    size_t completions = 0;
    for (size_t r = 0; r < n; ++r) {
      if (hdrs[r].handle.raw == repair_desired_) {
        ++repair_holders_;   // already correct; counts toward the majority
        continue;
      }
      // Only ever move a replica FORWARD. One that has raced ahead of our
      // chosen handle is left alone -- clobbering it would undo a newer commit.
      if (hdrs[r].handle.raw > repair_desired_) continue;
      auto &rc = *conns_[r];
      uint64_t *const buf = layout_.casBufsFor(future_id_, r);
      if (!rc.postSendSingleCas(future_id_, buf,
                                Layout::nodeAddrOf(rc.remoteBuf(), a),
                                hdrs[r].handle.raw, repair_desired_)) {
        throw std::runtime_error("failed to post a read-repair CAS");
      }
      repair_replica_[repair_posted_++] = r;
      completions += 1;
      bump(r, 1);
    }
    ++stats_.repairs;
    return postedExactly(completions, "postRepair");
  }

  /// Did the repair leave the chosen handle at a majority?
  ///
  /// A CAS that failed is not an error: the replica moved under us, which means
  /// somebody else is making progress. The read simply re-polls.
  bool resolveRepair(NodeRecord &node) {
    uint64_t const *const bufs_base = layout_.casBufsFor(future_id_, 0);
    (void)bufs_base;
    size_t landed = repair_holders_;
    for (size_t i = 0; i < repair_posted_; ++i) {
      size_t const r = repair_replica_[i];
      uint64_t const *const buf = layout_.casBufsFor(future_id_, r);
      // An RDMA CAS returns the PRE-value; equal to expected means it took.
      if (*buf == layout_.getNodeBufs(future_id_)[r].handle.raw) ++landed;
    }
    if (landed < majority()) return false;
    node = layout_.getNodeBufs(future_id_)[0];
    node.handle = Handle{repair_desired_};
    ++stats_.repairs_landed;
    return true;
  }

  size_t postVec(VecOffset off) {
    ++stats_.round_trips;  // one post/resolve pair = one round trip
    bumped_ = 0;
    auto &rc = *conns_[winner_];
    if (!rc.postSendSingle(dory::conn::ReliableConnection::RdmaRead, future_id_,
                           layout_.getDataVecBufs(future_id_), kVecRecordBytes,
                           layout_.vecAddrOf(rc.remoteBuf(), off))) {
      throw std::runtime_error("failed to post a vector read");
    }
    bump(winner_, 1);
    ++stats_.vec_reads;
    ++stats_.replica_reads;
    return postedExactly(1, "postVec");
  }

  bool resolveVec(VecRecord &vec) {
    vec = *layout_.getDataVecBufs(future_id_);
    return true;
  }

  // ── A chained batch, fanned out ──────────────────────────────────────────

  /// Post a batch on every replica. Nothing is drained here, so all the
  /// replicas' chains overlap -- and the caller's operation stays suspended
  /// rather than spinning, which is the whole point.
  size_t postBatch(Batch const &b) {
    ++stats_.round_trips;  // one post/resolve pair = one round trip
    if (!b.wellFormed()) return 0;
    bumped_ = 0;
    size_t const n = conns_.size();
    batch_ = &b;
    stageBatchPayloads(b, layout_.getStageNode(future_id_),
                       layout_.getStageVec(future_id_));
    size_t completions = 0;
    for (size_t r = 0; r < n; ++r) {
      size_t const c = postBatchChain(
          *conns_[r], layout_, b, layout_.getStageNode(future_id_),
          layout_.getStageVec(future_id_),
          layout_.casBufsFor(future_id_, r), /*doorbell=*/true,
          /*wr_id=*/future_id_);
      completions += c;
      bump(r, static_cast<int64_t>(c));
    }
    ++stats_.batches;
    return postedExactly(completions, "postBatch");
  }

  /// The batch's Faa timestamp: the MAXIMUM pre-value over the replicas that
  /// answered, plus this writer's index.
  ///
  /// The counter is replicated on every server and the FAA already rides this
  /// same chain to all of them, so the maximum costs no extra round trip -- and
  /// it is what stops any one server's counter being a point of failure. The
  /// client index is the tiebreak that keeps two writers computing the same
  /// maximum from claiming the same stamp.
  ///
  /// Real-time ordering across writers needs the maximum over ALL replicas; a
  /// majority is not enough, because the replica that decided another writer's
  /// maximum need not be in the intersection. ts_partial counts when that
  /// condition did not hold. The derivation is in ds_ts.hpp.
  uint64_t faaTimestamp(Batch const &b) {
    uint64_t best = kNoFaa;
    size_t answered = 0;
    size_t const n = conns_.size();
    for (size_t r = 0; r < n; ++r) {
      uint64_t const pre = batchPreFaa(b, layout_.casBufsFor(future_id_, r));
      if (pre == kNoFaa) continue;
      ++answered;
      if (best == kNoFaa || pre > best) best = pre;
    }
    if (best == kNoFaa) return kNullTs;
    // A QUORUM IS REQUIRED, NOT "WHOEVER ANSWERED".
    //
    // This used to return a stamp whenever at least ONE replica answered. With
    // one of three, the round has no intersection with any other writer's
    // quorum, so the stamp carries NO ordering property -- not even the
    // weakened one ts_partial describes. Two writers landing on disjoint single
    // replicas could stamp in any order at all.
    //
    // Refusing is safe and uses a path that already exists: kNullTs is a
    // first-class protocol state (ds_node.hpp -- "published but unstamped",
    // isPending()), and a reader resolves a pending version with a CAS from
    // kNullTs rather than waiting. So the write stays visible and gets its
    // order fixed by whoever reads it next.
    if (answered < majority()) {
      ++stats_.ts_short_of_quorum;
      return kNullTs;
    }
    if (answered < n) ++stats_.ts_partial;
    return tsFromFaa(best, client_idx_);
  }

  /// Did the batch's publishing CAS reach a majority (L1)?
  BatchResult resolveBatch(Batch const &b) {
    BatchResult out;
    out.submitted = true;
    out.ts = faaTimestamp(b);
    if (!b.hasCommit()) {
      out.committed = true;
      return out;
    }
    size_t took = 0;
    for (size_t r = 0; r < conns_.size(); ++r) {
      if (batchCommitted(b, layout_.casBufsFor(future_id_, r))) ++took;
    }
    if (took >= majority()) {
      ++stats_.commits;
      out.committed = true;
    } else {
      ++stats_.commits_lost;
      if (took > 0) ++stats_.partial_commits;
    }
    return out;
  }

  [[nodiscard]] TsMode tsMode() const { return ts_mode_; }
  void setTsMode(TsMode m) { ts_mode_ = m; }
  /// The tiebreak that makes a replicated-counter stamp unique.
  void setClientIdx(uint64_t i) { client_idx_ = i; }

  /// The local timestamp for this run's mode. Faa has none -- its value comes
  /// from the counter after the publish; see ds_ts.hpp.
  uint64_t now() { return localNow(ts_mode_); }

  VecOffset allocVec() { return vecs_ == nullptr ? kNullVec : vecs_->allocate(); }
  RemoteAddr allocNode() {
    return nodes_ == nullptr ? RemoteAddr{} : nodes_->allocate();
  }

 private:
  void bump(size_t r, int64_t n) {
    to_poll_[r] += n;
    ongoing_[r] += n;
    bumped_ += static_cast<size_t>(n);
  }

  /// Check that what we told the caller to await is what we actually queued.
  ///
  /// This is the one arithmetic error in the whole path that fails *silently*.
  /// Return too few and the future steps on partial results -- a torn read it
  /// will not notice. Return too many and it waits for a completion that never
  /// comes, which on the cluster looks like a hung client with no error
  /// anywhere. Neither is something a test off-cluster can catch, because the
  /// fake applies posts immediately and never has an outstanding count.
  ///
  /// The value at risk is specifically the speculation chain: its first request
  /// is unsignalled, so a post that speculates yields one completion for
  /// replica 0's pair rather than two. It would be very easy to write
  /// `replicas + 1`.
  ///
  /// Throws rather than asserts, because the release build defines NDEBUG and
  /// this is a correctness invariant rather than a debugging aid. One integer
  /// compare per post.
  size_t postedExactly(size_t claimed, char const *what) {
    if (claimed != bumped_) {
      throw std::runtime_error(
          std::string("completion accounting is wrong in ") + what +
          ": told the driver to await " + std::to_string(claimed) +
          " but queued " + std::to_string(bumped_));
    }
    return claimed;
  }

  Conns &conns_;
  Layout const &layout_;
  Counters &to_poll_;
  Counters &ongoing_;
  uint64_t future_id_;
  QuorumStats &stats_;
  VecOffsetHint *hint_;
  NodeAllocator *nodes_;
  VecAllocator *vecs_;

  TsMode ts_mode_ = TsMode::Clock;
  uint64_t client_idx_ = 0;
  size_t bumped_ = 0;  ///< completions queued by the post in progress
  VecOffset spec_pending_ = kNullVec;
  /// Bitmask of replicas whose header was actually READ this round.
  ///
  /// NOT decoration. getNodeBufs() is a per-future buffer REUSED across
  /// operations, so a slot belonging to a replica we did not ask still holds
  /// the PREVIOUS operation's header. Counting it as a vote would manufacture
  /// agreement out of stale bytes -- a wrong answer, not a slow one. Every
  /// vote loop must skip replicas outside this mask.
  uint32_t read_mask_ = ~0u;
  /// Set when a resolve found no majority, so the next attempt asks everyone.
  bool header_retry_ = false;
  /// Replica the pending speculative vector read was issued to. 0 unless
  /// --spread-reads. resolveHeaders validates against THIS replica's header.
  size_t spec_replica_ = 0;
  size_t walk_n_ = 0;
  RemoteAddr walk_addr_[Layout::kWalkFanout]{};
  uint64_t repair_desired_ = 0;
  RemoteAddr repair_addr_{};
  size_t repair_posted_ = 0;
  size_t repair_holders_ = 0;
  size_t repair_replica_[8]{};
  size_t winner_ = 0;
  Batch const *batch_ = nullptr;
};

}  // namespace ds
