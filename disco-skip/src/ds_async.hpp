#pragma once

// The non-blocking traversal: the same decisions, resumable.
//
// WHY A STATE MACHINE AT ALL. The blocking Traversal is one function that reads,
// decides and reads again. That shape cannot have more than one operation in
// flight, so a client's throughput is bounded by a single traversal's latency --
// about L round trips. The point of the future path is not to make one traversal
// faster (level L+1 genuinely depends on level L) but to have many traversals in
// flight at once, which is what turns ~2 us per round trip into a throughput
// number rather than a latency ceiling.
//
// WHAT IS AND IS NOT DUPLICATED. Only the *sequencing* lives here. Every
// decision -- rangeEnd, nextNode, covers, findLte, isStable, isPending, and
// what a settle batch should contain -- is the same pure function the blocking
// path calls, in ds_node.hpp and ds_traverse.hpp. That matters because those
// decisions are what the existing tests establish, and a second copy of them
// would be a second thing to keep correct.
//
// The sequencing itself IS new code, so it is tested differentially: traverse the
// same arena with both this and the blocking Traversal and require identical
// results, over randomised structures including mid-split ones. That is a
// stronger check than asserting properties of either alone.
//
// AsyncOps must provide, splitting each operation into post and resolve so the
// blocking path can be post-drain-resolve over the same logic:
//
//     size_t replicas() const;
//     size_t postHeaders(RemoteAddr a, VecOffset speculate);
//     bool   resolveHeaders(RemoteAddr a, NodeRecord &node,
//                           bool &have_vec, VecRecord &vec);
//     size_t postVec(VecOffset off);
//     bool   resolveVec(VecRecord &vec);
//     size_t postBatch(Batch const &b);
//     VecOffset guess(RemoteAddr a);
//     uint64_t now();
//
// `postX` returns the number of completions to await; the driver decrements as
// they land and calls step() when the count reaches zero.

#include <cstdint>

#include "ds_batch.hpp"
#include "ds_defs.hpp"
#include "ds_traverse.hpp"
#include "ds_node.hpp"

namespace ds {

/// Where a resumable traversal is up to.
enum class TraversalStep : uint8_t {
  Idle,           ///< not started, or finished
  AwaitHeaders,   ///< a quorum header read is outstanding
  AwaitVec,       ///< a vector read is outstanding
  AwaitHelp,      ///< a helping batch is outstanding
  Done,
};

/// A traversal that yields instead of blocking.
///
/// One instance per in-flight operation. The caller posts by calling start(),
/// then step() each time all of this traversal's outstanding completions have
/// landed, until finished().
template <class AsyncOps>
class TraversalFuture {
 public:
  explicit TraversalFuture(AsyncOps &ops) : ops_(ops) {}

  /// Begin traversing to the data node covering /k/.
  ///
  /// @param path receives one PathStep per level; must hold /layers/ entries
  /// @return completions to await before the first step()
  size_t start(Key k, uint32_t layers, PathStep *path) {
    k_ = k;
    layers_ = layers;
    path_ = path;
    for (uint32_t i = 0; i < layers; ++i) path[i] = PathStep{};

    res_ = TraversalResult{};
    level_ = layers - 1;
    at_data_ = false;
    cur_ = headAddr(level_);
    have_vec_ = false;
    hops_ = 0;
    settle_tries_ = 0;
    return postHeaders();
  }

  /// Advance. Returns the number of new completions to await; 0 means either
  /// finished or nothing further to wait for, so check finished().
  size_t step() {
    switch (step_) {
      case TraversalStep::AwaitHeaders: return onHeaders();
      case TraversalStep::AwaitVec:     return onVec();
      case TraversalStep::AwaitHelp:    return onHelp();
      case TraversalStep::Idle:
      case TraversalStep::Done:
        break;
    }
    return 0;
  }

  [[nodiscard]] bool finished() const { return step_ == TraversalStep::Done; }
  [[nodiscard]] TraversalResult const &result() const { return res_; }

 private:
  // ── Posting ───────────────────────────────────────────────────────────────

  size_t postHeaders() {
    step_ = TraversalStep::AwaitHeaders;
    // Speculate the vector alongside the headers. On a hit the vector arrives
    // with the header and this node costs one round trip instead of two, which
    // is where the offset hint actually pays.
    size_t const n = ops_.postHeaders(cur_, ops_.guess(cur_));
    ++res_.nodes_read;
    return n;
  }

  size_t postVec(VecOffset off) {
    step_ = TraversalStep::AwaitVec;
    ++res_.vec_reads;
    return ops_.postVec(off);
  }

  size_t fail(TraversalStatus s) {
    res_.status = s;
    step_ = TraversalStep::Done;
    return 0;
  }

  // ── Completions ───────────────────────────────────────────────────────────

  size_t onHeaders() {
    if (!ops_.resolveHeaders(cur_, node_, have_vec_, vec_)) {
      // No majority-supported handle yet: a commit is in flight. Re-poll
      // rather than guess, exactly as the blocking quorum read does.
      if (++settle_tries_ > static_cast<uint32_t>(detail::kMaxSettleAttempts)) {
        res_.gave_up = TraversalGaveUp::NoMajority;
        return fail(TraversalStatus::ReadFailed);
      }
      return postHeaders();
    }
    settle_tries_ = 0;
    if (have_vec_) ++res_.vec_reads;  // the speculation hit
    return decide();
  }

  size_t onVec() {
    if (!ops_.resolveVec(vec_)) return fail(TraversalStatus::ReadFailed);
    have_vec_ = true;
    return decide();
  }

  size_t onHelp() {
    // A helping batch changes the node, so re-read rather than trusting what we
    // had. Same as the blocking settleNode's loop.
    have_vec_ = false;
    return postHeaders();
  }

  // ── The decisions, all borrowed from the blocking path ────────────────────

  /// Right-walk or traverse, given whatever we currently hold for `cur_`.
  size_t decide() {
    // A node mid-propagation cannot be judged from its header: the link fields
    // may not have landed. The vector's descriptor is authoritative, so fetch
    // it. This is the one case where hopping needs the 320-byte read.
    if (!node_.isStable() && !have_vec_) {
      return postVec(node_.handle.offset());
    }

    Key const end = have_vec_ ? rangeEnd(node_, vec_) : node_.next_k_min;
    RemoteAddr const next =
        have_vec_ ? nextNode(node_, vec_) : RemoteAddr{node_.next_id};

    if (k_ < end || next.isNull()) {
      // This node covers k, so its entries are needed.
      if (!have_vec_) return postVec(node_.handle.offset());
      return useEntries();
    }

    if (++hops_ > detail::kMaxHopsPerLevel) {
      res_.gave_up = TraversalGaveUp::TooManyHops;
      return fail(TraversalStatus::Exhausted);
    }
    cur_ = next;
    ++res_.right_hops;
    have_vec_ = false;
    return postHeaders();
  }

  /// We hold the covering node and its vector. Settle it if need be, then route.
  size_t useEntries() {
    bool const pending = vec_.isPending();
    bool const unstable = !node_.isStable();
    if (pending || unstable) {
      if (++settle_tries_ > static_cast<uint32_t>(detail::kMaxSettleAttempts)) {
        res_.gave_up = TraversalGaveUp::SettleStuck;
        return fail(TraversalStatus::ReadFailed);
      }
      // The same batch settleNode() would build, in the same order -- the tail
      // word last, which the chain then delivers last.
      Batch b;
      if (pending) {
        b.casTs(node_.handle.offset(), kNullTs, ops_.now());
        ++res_.helped_ts;
      }
      if (unstable && vec_.hasSplitDescriptor()) {
        if (node_.next_k_min != vec_.k_min_next) {
          b.casNextKMin(cur_, node_.next_k_min, vec_.k_min_next);
        }
        if (node_.next_id != vec_.next_id) {
          b.casNextId(cur_, node_.next_id, vec_.next_id);
        }
      }
      if (unstable) {
        b.casTailWord(cur_, packTailWord(node_.level, node_.tail_struct_ver),
                      packTailWord(node_.level, node_.handle.structVer()));
        ++res_.helped_splits;
      }
      step_ = TraversalStep::AwaitHelp;
      return ops_.postBatch(b);
    }
    settle_tries_ = 0;

    if (at_data_) {
      // The data node. Report it, and whether k is actually here.
      res_.data_addr = cur_;
      res_.data_k_min = node_.k_min;
      int const idx = findLte(vec_, k_);
      if (idx >= 0 && vec_.e[idx].key == k_) {
        res_.found = true;
        res_.value = vec_.e[idx].val;
      }
      res_.status = TraversalStatus::Ok;
      step_ = TraversalStep::Done;
      return 0;
    }

    // An index level. Record what it saw, then step down.
    uint32_t const L = level_;
    path_[L].k_min = node_.k_min;
    path_[L].addr = cur_;
    path_[L].first_down =
        (L == 0 && vec_.size > 0) ? RemoteAddr{vec_.e[0].val} : RemoteAddr{};
    res_.levels = layers_;

    int const idx = findLte(vec_, k_);
    if (idx < 0) return fail(TraversalStatus::Miss);

    cur_ = RemoteAddr{vec_.e[idx].val};
    // Unsigned cursor plus an explicit at-data flag, rather than letting the
    // level go to -1. A signed decrement followed by `level_ < 0` is the
    // `X - C1 cmp C2` shape the dory toolchain rejects at
    // -Wstrict-overflow=5 -- the same diagnostic that cost a cluster build in
    // findLte. There is no unsigned equivalent of that warning.
    if (L == 0) {
      at_data_ = true;
    } else {
      --level_;
    }
    have_vec_ = false;
    hops_ = 0;
    return postHeaders();
  }

  AsyncOps &ops_;
  Key k_ = 0;
  uint32_t layers_ = 0;
  PathStep *path_ = nullptr;

  TraversalStep step_ = TraversalStep::Idle;
  uint32_t level_ = 0;       ///< the index level being traversed
  bool at_data_ = false;     ///< past level 0: `cur_` is a data node
  RemoteAddr cur_{};
  NodeRecord node_{};
  VecRecord vec_{};
  bool have_vec_ = false;
  uint32_t hops_ = 0;
  uint32_t settle_tries_ = 0;  ///< unsigned: `++x > C` on an int is the
                              ///< shape -Wstrict-overflow=5 rejects
  TraversalResult res_{};
};

}  // namespace ds
