#pragma once

// The non-blocking descent: the same decisions, resumable.
//
// WHY A STATE MACHINE AT ALL. The blocking Descender is one function that reads,
// decides and reads again. That shape cannot have more than one operation in
// flight, so a client's throughput is bounded by a single descent's latency --
// about L round trips. The point of the future path is not to make one descent
// faster (level L+1 genuinely depends on level L) but to have many descents in
// flight at once, which is what turns ~2 us per round trip into a throughput
// number rather than a latency ceiling.
//
// WHAT IS AND IS NOT DUPLICATED. Only the *sequencing* lives here. Every
// decision -- rangeEnd, nextNode, covers, findLte, isStable, isPending, and
// what a settle batch should contain -- is the same pure function the blocking
// path calls, in ds_node.hpp and ds_descend.hpp. That matters because those
// decisions are what the existing tests establish, and a second copy of them
// would be a second thing to keep correct.
//
// The sequencing itself IS new code, so it is tested differentially: descend the
// same arena with both this and the blocking Descender and require identical
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
#include "ds_descend.hpp"
#include "ds_node.hpp"

namespace ds {

/// Where a resumable descent is up to.
enum class DescentStep : uint8_t {
  Idle,           ///< not started, or finished
  AwaitHeaders,   ///< a quorum header read is outstanding
  AwaitVec,       ///< a vector read is outstanding
  AwaitHelp,      ///< a helping batch is outstanding
  Done,
};

/// A descent that yields instead of blocking.
///
/// One instance per in-flight operation. The caller posts by calling start(),
/// then step() each time all of this descent's outstanding completions have
/// landed, until finished().
template <class AsyncOps>
class DescentFuture {
 public:
  explicit DescentFuture(AsyncOps &ops) : ops_(ops) {}

  /// Begin descending to the data node covering /k/.
  ///
  /// @param path receives one PathStep per level; must hold /layers/ entries
  /// @return completions to await before the first step()
  size_t start(Key k, uint32_t layers, PathStep *path) {
    k_ = k;
    layers_ = layers;
    path_ = path;
    for (uint32_t i = 0; i < layers; ++i) path[i] = PathStep{};

    res_ = DescentResult{};
    level_ = static_cast<int>(layers) - 1;
    cur_ = headAddr(static_cast<uint32_t>(level_));
    have_vec_ = false;
    hops_ = 0;
    settle_tries_ = 0;
    return postHeaders();
  }

  /// Advance. Returns the number of new completions to await; 0 means either
  /// finished or nothing further to wait for, so check finished().
  size_t step() {
    switch (step_) {
      case DescentStep::AwaitHeaders: return onHeaders();
      case DescentStep::AwaitVec:     return onVec();
      case DescentStep::AwaitHelp:    return onHelp();
      case DescentStep::Idle:
      case DescentStep::Done:
        break;
    }
    return 0;
  }

  [[nodiscard]] bool finished() const { return step_ == DescentStep::Done; }
  [[nodiscard]] DescentResult const &result() const { return res_; }

 private:
  // ── Posting ───────────────────────────────────────────────────────────────

  size_t postHeaders() {
    step_ = DescentStep::AwaitHeaders;
    // Speculate the vector alongside the headers. On a hit the vector arrives
    // with the header and this node costs one round trip instead of two, which
    // is where the offset hint actually pays.
    size_t const n = ops_.postHeaders(cur_, ops_.guess(cur_));
    ++res_.nodes_read;
    return n;
  }

  size_t postVec(VecOffset off) {
    step_ = DescentStep::AwaitVec;
    ++res_.vec_reads;
    return ops_.postVec(off);
  }

  size_t fail(DescentStatus s) {
    res_.status = s;
    step_ = DescentStep::Done;
    return 0;
  }

  // ── Completions ───────────────────────────────────────────────────────────

  size_t onHeaders() {
    if (!ops_.resolveHeaders(cur_, node_, have_vec_, vec_)) {
      // No majority-supported handle yet: a commit is in flight. Re-poll
      // rather than guess, exactly as the blocking quorum read does.
      if (++settle_tries_ > detail::kMaxSettleAttempts) {
        return fail(DescentStatus::ReadFailed);
      }
      return postHeaders();
    }
    settle_tries_ = 0;
    if (have_vec_) ++res_.vec_reads;  // the speculation hit
    return decide();
  }

  size_t onVec() {
    if (!ops_.resolveVec(vec_)) return fail(DescentStatus::ReadFailed);
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

  /// Right-walk or descend, given whatever we currently hold for `cur_`.
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
      return fail(DescentStatus::Exhausted);
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
      if (++settle_tries_ > detail::kMaxSettleAttempts) {
        return fail(DescentStatus::ReadFailed);
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
      step_ = DescentStep::AwaitHelp;
      return ops_.postBatch(b);
    }
    settle_tries_ = 0;

    if (level_ < 0) {
      // The data node. Report it, and whether k is actually here.
      res_.data_addr = cur_;
      res_.data_k_min = node_.k_min;
      int const idx = findLte(vec_, k_);
      if (idx >= 0 && vec_.e[idx].key == k_) {
        res_.found = true;
        res_.value = vec_.e[idx].val;
      }
      res_.status = DescentStatus::Ok;
      step_ = DescentStep::Done;
      return 0;
    }

    // An index level. Record what it saw, then step down.
    uint32_t const L = static_cast<uint32_t>(level_);
    path_[L].k_min = node_.k_min;
    path_[L].addr = cur_;
    path_[L].first_down =
        (L == 0 && vec_.size > 0) ? RemoteAddr{vec_.e[0].val} : RemoteAddr{};
    res_.levels = layers_;

    int const idx = findLte(vec_, k_);
    if (idx < 0) return fail(DescentStatus::Miss);

    cur_ = RemoteAddr{vec_.e[idx].val};
    --level_;
    have_vec_ = false;
    hops_ = 0;
    return postHeaders();
  }

  AsyncOps &ops_;
  Key k_ = 0;
  uint32_t layers_ = 0;
  PathStep *path_ = nullptr;

  DescentStep step_ = DescentStep::Idle;
  int level_ = 0;            ///< >= 0 is an index level; -1 is the data node
  RemoteAddr cur_{};
  NodeRecord node_{};
  VecRecord vec_{};
  bool have_vec_ = false;
  uint32_t hops_ = 0;
  int settle_tries_ = 0;
  DescentResult res_{};
};

}  // namespace ds
