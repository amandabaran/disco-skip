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
    size_t const n = conns_.size();
    bumped_ = 0;
    spec_pending_ = speculate;
    NodeRecord *const hdrs = layout_.getNodeBufs(future_id_);
    VecRecord *const spec = layout_.getVecBufs(future_id_);
    size_t completions = 0;

    for (size_t r = 0; r < n; ++r) {
      auto &rc = *conns_[r];
      uintptr_t const remote = Layout::nodeAddrOf(rc.remoteBuf(), a);
      if (r == 0 && speculate != kNullVec) {
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
    stats_.replica_reads += n + (speculate != kNullVec ? 1u : 0u);
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
    uint64_t best_tag = 0;
    for (size_t r = 0; r < n; ++r) {
      size_t votes = 0;
      for (size_t q = 0; q < n; ++q) {
        if (hdrs[q].handle == hdrs[r].handle) ++votes;
      }
      if (votes < majority()) continue;
      uint64_t const tag = hdrs[r].handle.tag();
      if (best == n || tag > best_tag) {
        best = r;
        best_tag = tag;
      }
    }
    if (best == n) {
      ++stats_.read_retries;
      return false;
    }

    node = hdrs[best];
    winner_ = best;
    VecOffset const truth = node.handle.offset();

    have_vec = false;
    if (spec_pending_ != kNullVec) {
      // L4, strictly: usable only if replica 0 voted with the winner -- which,
      // since a handle carries its offset, means the speculation read the
      // version that won.
      if (hdrs[0].handle == hdrs[best].handle && spec_pending_ == truth) {
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

  size_t postVec(VecOffset off) {
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

  /// Did the batch's publishing CAS reach a majority (L1)?
  BatchResult resolveBatch(Batch const &b) {
    BatchResult out;
    out.submitted = true;
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

  uint64_t now() {
    return static_cast<uint64_t>(
        std::chrono::steady_clock::now().time_since_epoch().count());
  }

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

  size_t bumped_ = 0;  ///< completions queued by the post in progress
  VecOffset spec_pending_ = kNullVec;
  size_t winner_ = 0;
  Batch const *batch_ = nullptr;
};

}  // namespace ds
