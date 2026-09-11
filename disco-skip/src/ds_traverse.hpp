#pragma once

// Remote traversal, and the helping that makes reads non-blocking.
//
// Templated over an Ops type so the same logic runs against real RDMA on the
// cluster and against a fake arena in disco-skip/tests. That matters more here
// than anywhere else so far: helping only triggers on states a concurrent
// writer produces, and those are trivial to construct by hand locally and
// nearly impossible to provoke deliberately on a cluster.
//
// Ops must provide:
//
//     bool readNode(RemoteAddr a, NodeRecord &node);
//     bool readVec(VecOffset off, VecRecord &vec);
//     BatchResult submit(Batch const &b);
//     uint64_t now();
//
// Helping issues a BATCH rather than individual CASes. Settling a node can need
// up to four of them -- the timestamp, two header fields and the tail word --
// and one at a time that is four round trips on the *read* path. Chained, it is
// one. See ds_batch.hpp.
//
// Nothing here inspects the result: a step that failed is a step another thread
// already performed, which is exactly as good as doing it ourselves. Chaining
// also makes the tail word's "last" requirement structural, since same-QP RC
// ordering delivers the chain in order.

#include <cstdint>

#include "ds_batch.hpp"
#include "ds_defs.hpp"
#include "ds_node.hpp"

namespace ds {

/// Why a traversal stopped.
enum class TraversalStatus {
  Ok,          ///< reached the data node covering k
  Miss,        ///< a level had no entry <= k, so nothing routes to k yet
  ReadFailed,  ///< a node could not be read after retries
  Exhausted,   ///< too many hops; treated as a live-lock guard, not a result
};

struct TraversalResult {
  TraversalStatus status = TraversalStatus::ReadFailed;

  /// The data node covering k, and where its range starts. These are exactly
  /// mirror_reconcile()'s first two arguments.
  RemoteAddr data_addr{};
  Key data_k_min = 0;

  /// Was k actually present, and its payload if so. A traversal that reaches the
  /// right node and finds no key is a successful traversal with found == false --
  /// the distinction between "absent" and "we could not tell" is the whole
  /// point of the range check.
  bool found = false;
  Value value = 0;

  /// Valid PathStep entries, filled from index 0 (the directory) upward.
  uint32_t levels = 0;

  /// Counters, so the cost of a traversal is measurable rather than inferred.
  uint32_t nodes_read = 0;   ///< 64-byte header reads
  uint32_t vec_reads = 0;    ///< 320-byte vector reads -- the expensive kind
  uint32_t right_hops = 0;
  uint32_t helped_ts = 0;
  uint32_t helped_splits = 0;

  [[nodiscard]] bool ok() const { return status == TraversalStatus::Ok; }
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

/// What settleNode() cost, so callers with different stat structs can each
/// fold it into their own counters.
struct HelpCounters {
  uint32_t nodes_read = 0;
  uint32_t vec_reads = 0;
  uint32_t helped_ts = 0;
  uint32_t helped_splits = 0;
  uint32_t batches = 0;  ///< chained submissions, i.e. round trips spent helping
};

/// Complete any operation outstanding on this node, then re-read it.
///
/// Returns with /node/ and /vec/ settled, or false if it could not get there.
///
/// Shared by the traversal and by the write path rather than duplicated: this is
/// the helping protocol of remote-design.md §2, and two copies of it would be
/// two things to keep in step. Every CAS here ignores its result, because a
/// failure means another thread already performed that step.
template <class Ops>
bool settleNode(Ops &ops, RemoteAddr addr, NodeRecord &node, VecRecord &vec,
                HelpCounters &c) {
  for (int attempt = 0; attempt < detail::kMaxSettleAttempts; ++attempt) {
    bool const pending = vec.isPending();
    bool const unstable = !node.isStable();
    if (!pending && !unstable) return true;

    // One chain, in protocol order.
    Batch b;

    // 1. Fix the timestamp first. A helper stamps with its own clock, which
    //    orders the write at the moment somebody first needed it -- that is
    //    the point of leaving it unfixed until visible, since it lets the
    //    write be ordered after readers that did not see it.
    if (pending) {
      b.casTs(node.handle.offset(), kNullTs, ops.now());
      ++c.helped_ts;
    }

    // 2. Propagate the split descriptor into the header. Either order is
    //    safe: a reader that checks the bookends never trusts these fields
    //    while the window is open, and one holding the vector prefers its
    //    copies anyway.
    if (unstable && vec.hasSplitDescriptor()) {
      if (node.next_k_min != vec.k_min_next) {
        b.casNextKMin(addr, node.next_k_min, vec.k_min_next);
      }
      if (node.next_id != vec.next_id) {
        b.casNextId(addr, node.next_id, vec.next_id);
      }
    }

    // 3. Close the window last -- and now structurally last, because the chain
    //    is delivered in order. Until this lands, another writer knows the node
    //    is unsettled and will help rather than start its own split.
    if (unstable) {
      b.casTailWord(addr, packTailWord(node.level, node.tail_struct_ver),
                    packTailWord(node.level, node.handle.structVer()));
      ++c.helped_splits;
    }

    if (b.size() > 0) {
      ++c.batches;
      ops.submit(b);
    }

    if (!ops.readNode(addr, node)) return false;
    ++c.nodes_read;
    if (!ops.readVec(node.handle.offset(), vec)) return false;
    ++c.vec_reads;
  }
  return false;
}

template <class Ops>
class Traversal {
 public:
  explicit Traversal(Ops &ops) : ops_(ops) {}

  /// Traverse to the data node covering /k/, recording what each level saw.
  ///
  /// @param path  receives one PathStep per level, indexed by level; must have
  ///              room for /layers/ entries
  TraversalResult traverse(Key k, uint32_t layers, PathStep *path) {
    TraversalResult res;
    for (uint32_t i = 0; i < layers; ++i) path[i] = PathStep{};

    RemoteAddr cur = headAddr(layers - 1);
    NodeRecord node;
    VecRecord vec;

    for (uint32_t level = layers; level-- > 0;) {
      // Walk right to the node covering k at this level.
      bool have_vec = false;
      if (!seek(cur, k, node, vec, have_vec, res)) return res;

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
        res.status = TraversalStatus::ReadFailed;
        return res;
      }

      int const idx = findLte(vec, k);
      if (idx < 0) {
        // No entry at or below k. Nothing routes to k from here yet -- the
        // structure simply does not cover it. A real miss, not a failure.
        res.status = TraversalStatus::Miss;
        return res;
      }
      cur = RemoteAddr{vec.e[idx].val};
    }

    // `cur` now names a data node. Walk right there too: a data-level split
    // the directory has not learned about leaves the covering node one or more
    // hops along, which is exactly interface-doc §7a's third case.
    bool have_vec = false;
    if (!seek(cur, k, node, vec, have_vec, res)) return res;
    if (!settle(cur, node, vec, res)) {
      res.status = TraversalStatus::ReadFailed;
      return res;
    }

    res.data_addr = cur;
    res.data_k_min = node.k_min;

    int const idx = findLte(vec, k);
    if (idx >= 0 && vec.e[idx].key == k) {
      res.found = true;
      res.value = vec.e[idx].val;
    }
    res.status = TraversalStatus::Ok;
    return res;
  }

 private:
  Ops &ops_;

  /// Advance /cur/ along its level until the node covering k is reached.
  ///
  /// Hops on the 64-byte header alone. That is the whole reason next_id and
  /// next_k_min live in the header rather than the vector: a node we are only
  /// passing over never needs its 320-byte vector fetched, and passing over is
  /// the common case while splitting is rare.
  ///
  /// The vector is consulted in exactly two situations:
  ///
  ///   - the node is unstable, so the header's next_id / next_k_min are
  ///     mid-propagation and the vector's descriptor is authoritative
  ///   - the node covers k, so we are about to read its entries anyway
  ///
  /// On return, /have_vec/ says whether /vec/ was already fetched, so the
  /// caller does not re-read it.
  bool seek(RemoteAddr &cur, Key k, NodeRecord &node, VecRecord &vec,
            bool &have_vec, TraversalResult &res) {
    have_vec = false;
    for (uint32_t hop = 0;; ++hop) {
      if (hop > detail::kMaxHopsPerLevel) {
        res.status = TraversalStatus::Exhausted;
        return false;
      }
      if (!ops_.readNode(cur, node)) {
        res.status = TraversalStatus::ReadFailed;
        return false;
      }
      ++res.nodes_read;

      Key end;
      RemoteAddr next;
      if (node.isStable()) {
        // The common case: the header is settled, so it can be trusted, and
        // no vector read happens unless this node turns out to cover k.
        end = node.next_k_min;
        next = RemoteAddr{node.next_id};
      } else {
        // Mid-propagation. The header's link fields may not have landed yet,
        // so fall back to the descriptor the writer left in the new vector.
        if (!ops_.readVec(node.handle.offset(), vec)) {
          res.status = TraversalStatus::ReadFailed;
          return false;
        }
        ++res.vec_reads;
        have_vec = true;
        end = rangeEnd(node, vec);
        next = nextNode(node, vec);
      }

      if (k < end) {
        // This node covers k, so its entries are needed. Fetch the vector if
        // the unstable path has not already done so.
        if (!have_vec) {
          if (!ops_.readVec(node.handle.offset(), vec)) {
            res.status = TraversalStatus::ReadFailed;
            return false;
          }
          ++res.vec_reads;
          have_vec = true;
        }
        return true;
      }

      if (next.isNull()) {
        // The chain ends here, so this node owns everything above its k_min.
        // Reaching this with k past the range end means next_k_min lied, which
        // the verifier would reject in a quiescent structure.
        if (!have_vec) {
          if (!ops_.readVec(node.handle.offset(), vec)) {
            res.status = TraversalStatus::ReadFailed;
            return false;
          }
          ++res.vec_reads;
          have_vec = true;
        }
        return true;
      }
      cur = next;
      ++res.right_hops;
      have_vec = false;
    }
  }

  /// Complete any operation outstanding on this node, then re-read it.
  ///
  /// Thin wrapper over settleNode(), which the write path shares.
  bool settle(RemoteAddr addr, NodeRecord &node, VecRecord &vec,
              TraversalResult &res) {
    HelpCounters c;
    bool const ok = settleNode(ops_, addr, node, vec, c);
    res.nodes_read += c.nodes_read;
    res.vec_reads += c.vec_reads;
    res.helped_ts += c.helped_ts;
    res.helped_splits += c.helped_splits;
    return ok;
  }
};

}  // namespace ds
