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

    RemoteAddr const hinted = cache_.locateData(k);
    if (hinted.isNull()) {
      ++stats_.cache_misses;
      return beginTraversal();
    }
    hinted_ = hinted;
    step_ = GetStep::AwaitHintHeader;
    // Speculate the vector: on a hit the whole Get is one round trip, which is
    // the best case the cache can produce.
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
      return ops_.postVec(node_.handle.offset());
    }
    bool const in_range = have_vec_ ? covers(node_, vec_, k_)
                                    : coversByHeader(node_, k_);
    if (in_range && !have_vec_) {
      step_ = GetStep::AwaitHintVec;
      return ops_.postVec(node_.handle.offset());
    }
    if (in_range) return answerFromHint();

    // C4: the address was real and the read consistent, but this is no longer
    // the right node. A detected bad hint, not a wrong answer.
    ++stats_.kmin_mismatch;
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
    ++stats_.kmin_mismatch;
    return beginTraversal();
  }

  size_t answerFromHint() {
    ++stats_.cache_hits;
    int const idx = findLte(vec_, k_);
    out_.resolved = true;
    if (idx >= 0 && vec_.e[idx].key == k_) {
      out_.found = true;
      out_.value = vec_.e[idx].val;
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
    stats_.helped += r.helped_ts + r.helped_splits;

    if (!r.ok()) {
      if (r.status == TraversalStatus::Miss) {
        // Nothing routes to k yet, which for a read is simply "not present".
        // No node to install, so no reconcile either.
        out_.resolved = true;
        ++stats_.not_found;
      } else {
        // Attribute it. "The operation failed" was previously all the log
        // said, which made 61,019 failed gets on workload D at 8 clients
        // impossible to explain -- and the three causes want different fixes:
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
  RemoteAddr hinted_{};
  NodeRecord node_{};
  VecRecord vec_{};
  bool have_vec_ = false;
  GetResult out_{};
  PathStep path_[kMaxLayers]{};
};

}  // namespace ds
