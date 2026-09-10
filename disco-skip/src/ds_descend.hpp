#pragma once

// Remote descent, and the helping that makes reads non-blocking.
//
// Templated over an Ops type so the same logic runs against real RDMA on the
// cluster and against a fake arena in disco-skip/tests. That matters more here
// than anywhere else so far: helping only triggers on states a concurrent
// writer produces, and those are trivial to construct by hand locally and
// nearly impossible to provoke deliberately on a cluster.
//
// Ops must provide:
//
//     bool read(RemoteAddr a, NodeRecord &node, VecRecord &vec);
//     bool readVec(VecOffset off, VecRecord &vec);
//     bool casTs(VecOffset off, uint64_t expected, uint64_t desired);
//     bool casNextId(RemoteAddr a, uint64_t expected, uint64_t desired);
//     bool casNextKMin(RemoteAddr a, Key expected, Key desired);
//     bool casTailWord(RemoteAddr a, uint64_t expected, uint64_t desired);
//     uint64_t now();
//
// Every CAS returns whether it succeeded, and every caller here ignores that:
// a failure means another thread already performed the step, which is exactly
// as good as doing it ourselves.

#include <cstdint>

#include "ds_defs.hpp"
#include "ds_node.hpp"

namespace ds {

/// Why a descent stopped.
enum class DescentStatus {
  Ok,          ///< reached the data node covering k
  Miss,        ///< a level had no entry <= k, so nothing routes to k yet
  ReadFailed,  ///< a node could not be read after retries
  Exhausted,   ///< too many hops; treated as a live-lock guard, not a result
};

struct DescentResult {
  DescentStatus status = DescentStatus::ReadFailed;

  /// The data node covering k, and where its range starts. These are exactly
  /// mirror_reconcile()'s first two arguments.
  RemoteAddr data_addr{};
  Key data_k_min = 0;

  /// Was k actually present, and its payload if so. A descent that reaches the
  /// right node and finds no key is a successful descent with found == false --
  /// the distinction between "absent" and "we could not tell" is the whole
  /// point of the range check.
  bool found = false;
  Value value = 0;

  /// Valid PathStep entries, filled from index 0 (the directory) upward.
  uint32_t levels = 0;

  /// Counters, so the cost of a descent is measurable rather than inferred.
  uint32_t nodes_read = 0;
  uint32_t right_hops = 0;
  uint32_t helped_ts = 0;
  uint32_t helped_splits = 0;

  [[nodiscard]] bool ok() const { return status == DescentStatus::Ok; }
};

namespace detail {

/// Guard against an unbounded right-walk. A level's chain is finite, so
/// exceeding this means the structure is cyclic or a writer is producing nodes
/// faster than we traverse them; either way, giving up and retrying beats
/// spinning.
inline constexpr uint32_t kMaxHopsPerLevel = 1u << 20;

/// Guard against livelocking on a node whose in-flight operation we keep
/// failing to settle.
inline constexpr int kMaxSettleAttempts = 8;

}  // namespace detail

template <class Ops>
class Descender {
 public:
  explicit Descender(Ops &ops) : ops_(ops) {}

  /// Descend to the data node covering /k/, recording what each level saw.
  ///
  /// @param path  receives one PathStep per level, indexed by level; must have
  ///              room for /layers/ entries
  DescentResult descend(Key k, uint32_t layers, PathStep *path) {
    DescentResult res;
    for (uint32_t i = 0; i < layers; ++i) path[i] = PathStep{};

    RemoteAddr cur = headAddr(layers - 1);
    NodeRecord node;
    VecRecord vec;

    for (uint32_t level = layers; level-- > 0;) {
      // Walk right to the node covering k at this level.
      if (!seek(cur, k, node, vec, res)) return res;

      // Record what this level saw. `first_down` is only meaningful at the
      // directory: it is the value of the covering node's first entry, i.e.
      // the data node whose k_min is this node's k_min, which the cache needs
      // in order to create a local directory node with that minimum. Above
      // level 0 the down value is a local pointer the cache resolves itself.
      path[level].k_min = node.k_min;
      path[level].addr = cur;
      path[level].first_down =
          (level == 0 && vec.size > 0) ? RemoteAddr{vec.e[0].val} : RemoteAddr{};
      res.levels = layers;

      // We are about to read this node's entries, so its version must be
      // settled: using contents whose timestamp is unfixed would leave the
      // write unordered with respect to this read. Merely hopping past a node
      // needs no such thing, which is why seek() does not settle.
      if (!settle(cur, node, vec, res)) {
        res.status = DescentStatus::ReadFailed;
        return res;
      }

      int const idx = findLte(vec, k);
      if (idx < 0) {
        // No entry at or below k. Nothing routes to k from here yet -- the
        // structure simply does not cover it. A real miss, not a failure.
        res.status = DescentStatus::Miss;
        return res;
      }
      cur = RemoteAddr{vec.e[idx].val};
    }

    // `cur` now names a data node. Walk right there too: a data-level split
    // the directory has not learned about leaves the covering node one or more
    // hops along, which is exactly interface-doc §7a's third case.
    if (!seek(cur, k, node, vec, res)) return res;
    if (!settle(cur, node, vec, res)) {
      res.status = DescentStatus::ReadFailed;
      return res;
    }

    res.data_addr = cur;
    res.data_k_min = node.k_min;

    int const idx = findLte(vec, k);
    if (idx >= 0 && vec.e[idx].key == k) {
      res.found = true;
      res.value = vec.e[idx].val;
    }
    res.status = DescentStatus::Ok;
    return res;
  }

 private:
  Ops &ops_;

  /// Advance /cur/ along its level until the node covering k is reached.
  ///
  /// Uses the vector's split descriptor in preference to the header, so a node
  /// mid-propagation routes correctly without being settled first. That is what
  /// lets a passing reader avoid helping: it never consults the entries, only
  /// where the range ends and what comes next.
  bool seek(RemoteAddr &cur, Key k, NodeRecord &node, VecRecord &vec,
            DescentResult &res) {
    for (uint32_t hop = 0;; ++hop) {
      if (hop > detail::kMaxHopsPerLevel) {
        res.status = DescentStatus::Exhausted;
        return false;
      }
      if (!ops_.read(cur, node, vec)) {
        res.status = DescentStatus::ReadFailed;
        return false;
      }
      ++res.nodes_read;

      if (k < rangeEnd(node, vec)) return true;

      RemoteAddr const next = nextNode(node, vec);
      if (next.isNull()) {
        // The chain ends here, so this node owns everything above its k_min.
        // Reaching this with k past the range end means next_k_min lied, which
        // the verifier would reject in a quiescent structure.
        return true;
      }
      cur = next;
      ++res.right_hops;
    }
  }

  /// Complete any operation outstanding on this node, then re-read it.
  ///
  /// Returns with /node/ and /vec/ settled, or false if it could not get there.
  bool settle(RemoteAddr addr, NodeRecord &node, VecRecord &vec,
              DescentResult &res) {
    for (int attempt = 0; attempt < detail::kMaxSettleAttempts; ++attempt) {
      bool const pending = vec.isPending();
      bool const unstable = !node.isStable();
      if (!pending && !unstable) return true;

      // 1. Fix the timestamp first. A helper stamps with its own clock, which
      //    orders the write at the moment somebody first needed it -- that is
      //    the point of leaving it unfixed until visible, since it lets the
      //    write be ordered after readers that did not see it.
      if (pending) {
        ops_.casTs(node.handle.offset(), kNullTs, ops_.now());
        ++res.helped_ts;
      }

      // 2. Propagate the split descriptor into the header. Either order is
      //    safe: a reader that checks the bookends never trusts these fields
      //    while the window is open, and one holding the vector prefers its
      //    copies anyway.
      if (unstable && vec.hasSplitDescriptor()) {
        if (node.next_k_min != vec.k_min_next) {
          ops_.casNextKMin(addr, node.next_k_min, vec.k_min_next);
        }
        if (node.next_id != vec.next_id) {
          ops_.casNextId(addr, node.next_id, vec.next_id);
        }
      }

      // 3. Close the window last. Until this lands, another writer knows the
      //    node is unsettled and will help rather than start its own split.
      if (unstable) {
        ops_.casTailWord(addr, packTailWord(node.level, node.tail_struct_ver),
                         packTailWord(node.level, node.handle.structVer()));
        ++res.helped_splits;
      }

      if (!ops_.read(addr, node, vec)) return false;
      ++res.nodes_read;
    }
    return false;
  }
};

}  // namespace ds
