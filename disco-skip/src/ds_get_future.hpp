#pragma once

// A Get as a resumable operation: the same shape as ds_get.hpp, yieldable.
//
// GetOperation is the state-machine twin of Getter. It is transport-agnostic --
// templated over the same AsyncOps surface TraversalFuture uses -- so it runs
// against a fake locally and against RDMA on the cluster, and the driver glue
// that binds it to DsState is separate and thin.
//
// It MIRRORS Getter's decisions deliberately, including one asymmetry worth
// understanding before changing either:
//
//   THE FAST PATH DOES NOT SETTLE THE NODE IT ANSWERS FROM. A traversal settles
//   before using a node's entries; a cache hit does not. That is not an
//   oversight in either. Settling fixes a pending `ts` and closes the split
//   propagation window, and a *point* read needs neither: point operations
//   linearize on the CAS and the version tags (L1-L3, and
//   cache-remote-interface.md §9 is explicit that only ranges need a
//   timestamp), and the range check is already sound because the path checks
//   isStable() and otherwise prefers the vector's bound via covers().
//
//   So the slow path helps and the fast path does not, because helping on a
//   cache hit would double the cost of the thing the cache exists to make
//   cheap. The traversal is paying L round trips already, so being a good
//   citizen there is nearly free. Writers stamp their own versions before
//   returning, so a pending version means a writer still in flight and is rare.
//
// Mirroring matters for a second reason: the blocking Getter is the oracle the
// differential tests measure this against, so a deliberate difference would
// show up as a disagreement and cost time to rediscover.

#include <cstdint>

#include "ds_async.hpp"
#include "ds_defs.hpp"
#include "ds_get.hpp"  // GetStats, GetResult, NullCache
#include "ds_node.hpp"
#include "ds_traverse.hpp"

namespace ds {

/// Where a resumable Get is up to.
enum class GetStep : uint8_t {
  Idle,
  AwaitHintHeader,  ///< reading the node the cache pointed at
  AwaitHintVec,     ///< that node looked right, so its entries are needed
  Traversing,       ///< the hint missed or was stale; resolving remotely
  Done,
};

/// @param Ops    the AsyncOps surface
/// @param Cache  NullCache, or the CacheAdapter over SkipVec
template <class Ops, class Cache>
class GetOperation {
 public:
  GetOperation(Ops &ops, Cache &cache, uint32_t layers, GetStats &stats)
      : ops_(ops), cache_(cache), layers_(layers), stats_(stats), trav_(ops) {}

  /// @return completions to await before the first step()
  size_t start(Key k) {
    k_ = k;
    out_ = GetResult{};
    hops_ = 0;


    RemoteAddr const hinted = cache_.locateData(k);
    if (hinted.isNull()) {
      ++stats_.cache_misses;
      return beginTraversal();
    }
    hinted_ = hinted;
    step_ = GetStep::AwaitHintHeader;
    // Speculate the vector: on a hit the whole Get is one round trip, which is
    // the best case the cache can produce.
    ++stats_.rt_hint;
    return ops_.postHeaders(hinted, ops_.guess(hinted));
  }

  size_t step() {
    switch (step_) {
      case GetStep::AwaitHintHeader: return onHintHeader();
      case GetStep::AwaitHintVec:    return onHintVec();
      case GetStep::Traversing:      return onTraversing();
      case GetStep::Idle:
      case GetStep::Done:
        break;
    }
    return 0;
  }

  [[nodiscard]] bool finished() const { return step_ == GetStep::Done; }
  [[nodiscard]] GetResult const &result() const { return out_; }

 private:
  size_t beginTraversal() {
    step_ = GetStep::Traversing;
    ++stats_.traversals;
    return trav_.start(k_, layers_, path_);
  }

  size_t onHintHeader() {
    bool have_vec = false;
    if (!ops_.resolveHeaders(hinted_, node_, have_vec, vec_)) {
      // No majority-supported handle: a commit is in flight. Treat it as a
      // miss and resolve the hard way rather than spinning on the fast path.
      ++stats_.cache_misses;
      return beginTraversal();
    }
    ++stats_.nodes_read;
    have_vec_ = have_vec;
    if (have_vec_) ++stats_.vec_reads;

    bool const stable = node_.isStable();
    if (!stable && !have_vec_) {
      // The header's bound cannot be trusted mid-propagation, so the vector's
      // descriptor decides whether this node still covers k.
      step_ = GetStep::AwaitHintVec;
      ++stats_.rt_hint;
      return ops_.postVec(node_.handle.offset());
    }
    bool const in_range = have_vec_ ? covers(node_, vec_, k_)
                                    : coversByHeader(node_, k_);
    if (in_range && !have_vec_) {
      step_ = GetStep::AwaitHintVec;
      ++stats_.rt_hint;
      return ops_.postVec(node_.handle.offset());
    }
    if (in_range) return answerFromHint();

    // C4: the address was real and the read consistent, but this is no longer
    // the right node. A detected bad hint, not a wrong answer.
    return onStaleHint();
  }

  /// The hint named a node that does not cover k: hop sideways if the budget
  /// allows, otherwise descend.
  ///
  /// ONE HANDLER, TWO DETECTION SITES. A mismatch is found either at the header
  /// (stable node, next_k_min decides) or after the follow-up vector read
  /// (mid-propagation, the split descriptor decides). The hop logic and the
  /// per-operation counting were on the HEADER path only, so a get whose
  /// staleness was detected at the VECTOR neither hopped nor counted as stale
  /// -- silently exempting exactly the contended, mid-split nodes the
  /// experiment is about.
  size_t onStaleHint() {
    ++stats_.kmin_mismatch;
    // Counted once per OPERATION, not per detection, so it is invariant to the
    // hop budget -- see GetStats::hint_stale_ops for why the original
    // experiment could not compare its own arms without it.
    if (hops_ == 0) ++stats_.hint_stale_ops;

    // ── OPTIONAL: FOLLOW `next` INSTEAD OF DESCENDING (--hint-hops) ────────
    //
    // A mismatch means this node split, so k is probably in the sibling one
    // hop away. The successor comes from the vector when we hold it, because
    // the vector's split descriptor is authoritative mid-propagation and the
    // header's next_id is not -- the same rule the traversal's right-walk
    // follows, and getting it backwards here would hop to a node the split has
    // already superseded.
    if (hops_ < ops_.hintHops()) {
      RemoteAddr const next = have_vec_ ? nextNode(node_, vec_)
                                        : RemoteAddr{node_.next_id};
      if (!next.isNull()) {
        ++hops_;
        ++stats_.hops_taken;
        hinted_ = next;
        have_vec_ = false;
        step_ = GetStep::AwaitHintHeader;
        ++stats_.rt_hint;
        return ops_.postHeaders(next, ops_.guess(next));
      }
      // No successor to hop to: this is the tail, so descending is the only
      // option and the budget is irrelevant. Not counted as exhausted -- that
      // bucket is for budgets actually spent.
    } else if (ops_.hintHops() != 0) {
      // Spent the whole budget and still has to descend: paid the hops AND the
      // traversal, which is the case that makes the bet lose.
      ++stats_.hops_exhausted;
    }
    // The budget is spent or there is nowhere to hop: descend.
    //
    // This used to carry a table concluding that hopping loses. It was
    // measured WITHOUT the directory repair in answerFromHint, which is the
    // thing that makes hopping pay -- so the verdict was overturned by
    // changing the mechanism, not by re-reading the numbers. Worth
    // remembering before trusting any "we measured it and it lost".
    return beginTraversal();
  }

  size_t onHintVec() {
    if (!ops_.resolveVec(vec_)) {
      ++stats_.cache_misses;
      return beginTraversal();
    }
    ++stats_.vec_reads;
    have_vec_ = true;
    if (covers(node_, vec_, k_)) return answerFromHint();
    return onStaleHint();
  }

  size_t answerFromHint() {
    ++stats_.cache_hits;
    // A RECOVERY IS COUNTED HERE, not at the range check, because there are
    // TWO ways to reach an answer on the hint path: the header alone when the
    // node is stable, or a follow-up vector read when it is not. The counter
    // started at the header check and so missed every recovery that needed the
    // vector -- which the one-split test caught at once, reporting
    // `1 hop, 0 recovered, 0 traversals`: a hop that plainly worked, since no
    // descent was paid, scored as a failure. That is the same inference error
    // the original hop experiment made, reproduced in its instrumentation.
    if (hops_ != 0) {
      ++stats_.hops_recovered;
      // REPAIR THE DIRECTORY, which is the whole reason hopping used to lose.
      // Reconciliation was a side effect of descending, so a hop that answered
      // the get left the stale entry in place and the next get on this key
      // paid the mismatch again -- compounding staleness and pushing the write
      // path's hint rejections up with it.
      //
      // No path is needed and none is available: mirror_reconcile treats
      // levels = 0 as "entry repair only", and the data routing entry is
      // documented as always safe with a repeat call a no-op. node_.k_min is
      // immutable once the node exists, and `hinted_` is the address we just
      // read and confirmed covers k, so this is exactly the pair the contract
      // asks for.
      cache_.reconcile(node_.k_min, hinted_, nullptr, 0);
      ++stats_.hop_reconciles;
      ++stats_.reconciles;
    }
    int const idx = findLte(vec_, k_);
    out_.resolved = true;
    if (idx >= 0 && vec_.keyAt(idx) == k_) {
      out_.found = true;
      out_.value = vec_.valAt(idx);
    } else {
      ++stats_.not_found;
    }
    step_ = GetStep::Done;
    return 0;
  }

  size_t onTraversing() {
    size_t const await = trav_.step();
    if (!trav_.finished()) return await;

    TraversalResult const &r = trav_.result();
    stats_.nodes_read += r.nodes_read;
    stats_.vec_reads += r.vec_reads;
    stats_.right_hops += r.right_hops;
    // Taken from the traversal's OWN counter rather than derived from the two
    // above: a speculation hit bumps vec_reads without a round trip, and the
    // repair and helping posts are round trips neither of them sees. See
    // TraversalResult::round_trips.
    stats_.rt_traverse += r.round_trips;
    stats_.helped += r.helped_ts + r.helped_splits;

    if (!r.ok()) {
      if (r.status == TraversalStatus::Miss) {
        // Nothing routes to k yet, which for a read is simply "not present".
        // No node to install, so no reconcile either.
        out_.resolved = true;
        ++stats_.not_found;
      } else {
        // Attribute it. "The operation failed" was previously all the log
        // said, which made a large failed-get count on a write-contended
        // workload impossible to explain -- and the three causes want
        // different fixes:
        // NoMajority is quorum-read churn on a hot handle, SettleStuck is a
        // reader helping faster than writers re-dirty, TooManyHops is a
        // runaway right-walk.
        ++stats_.failures;  // resolved stays false: the caller retries
        switch (r.gave_up) {
          case TraversalGaveUp::NoMajority:  ++stats_.gave_up_no_majority; break;
          case TraversalGaveUp::SettleStuck: ++stats_.gave_up_settle_stuck; break;
          case TraversalGaveUp::TooManyHops: ++stats_.gave_up_too_many_hops; break;
          case TraversalGaveUp::No: break;
        }
      }
      step_ = GetStep::Done;
      return 0;
    }

    // A5: hand the path back so the cache splits at the boundaries the
    // traversal actually saw. Costs no extra RDMA -- we already hold them.
    cache_.reconcile(r.data_k_min, r.data_addr, path_, r.levels);
    ++stats_.reconciles;

    out_.resolved = true;
    out_.found = r.found;
    out_.value = r.value;
    if (!r.found) ++stats_.not_found;
    step_ = GetStep::Done;
    return 0;
  }

  Ops &ops_;
  Cache &cache_;
  uint32_t layers_;
  GetStats &stats_;
  TraversalFuture<Ops> trav_;

  Key k_ = 0;
  GetStep step_ = GetStep::Idle;
  /// Sideways hops spent on THIS operation. Also the flag for "we got here by
  /// hopping", which is what makes hops_recovered and hint_stale_ops correct.
  uint32_t hops_ = 0;
  RemoteAddr hinted_{};
  NodeRecord node_{};
  VecRecord vec_{};
  bool have_vec_ = false;
  GetResult out_{};
  PathStep path_[kMaxLayers]{};
};

}  // namespace ds
