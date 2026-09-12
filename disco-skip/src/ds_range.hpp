#pragma once

// A10: snapshot range queries.
//
// Fix a snapshot T, then walk the data level left to right from the start key,
// taking from each node the newest version with ts <= T. That is the whole
// algorithm; the subtleties are in what "the newest version with ts <= T" means
// when the structure has been splitting underneath.
//
// ── Where T comes from, and why a reader does not FAA ───────────────────────
//
// In Faa mode the counter is replicated on every server (ds_ts.hpp). A WRITER
// fetch-and-adds it; a READER only needs to observe it, so it READs the word on
// each replica and takes the maximum, C. That is the whole difference between
// the write path's cost and the read path's -- a snapshot costs one round trip
// and no mutation, so concurrent range queries do not contend with each other
// at all.
//
// The snapshot is then T = (C << kFaaClientBits) | kFaaClientMask, and that
// exact form is load-bearing:
//
//   * A write W that COMPLETED before the read began FAA'd every replica, so on
//     replica r its pre-value p_r satisfies c_r >= p_r + 1. W's stamp uses
//     max_r p_r <= C - 1, so its stamp is at most (C << 16) | 0xFFFF = T.
//     Included, as it must be.
//   * A write that BEGINS after the read has every pre-value >= c_r, so its
//     maximum is >= C and its stamp is at least (C+1) << 16 > T. Excluded, as
//     it must be.
//   * Writes concurrent with the read may fall either side, which is exactly
//     what linearizability permits.
//
// The low bits being all-ones is what admits every client index at counter
// value C; masking them to zero instead would silently drop writes by client
// id, which is a wrong answer rather than a stale one.
//
// In Clock mode T is just a clock read, and the guarantee weakens to
// "linearizable within epsilon" -- measured at p99 ~56 us on this testbed
// (clock-measurements.md §8), which is why Faa is the mode to run ranges in.
//
// ── Reading as of T while the structure moves ──────────────────────────────
//
// The walk follows the CURRENT chain, but reads each node's version AS OF T.
// Those are two different times, and mixing them is only sound because of two
// structural facts:
//
//   1. A NODE'S k_min NEVER CHANGES. A split at key s on node N creates a NEW
//      node M with k_min = s; N keeps its own k_min. So a node's left edge is
//      immutable, which is what lets the walk terminate on the current k_min
//      (below) and what keeps as-of-T versions from overlapping.
//
//   2. NOTHING IS EVER REMOVED. There is no delete and no merge (invariants.md
//      A7/A9), so every node that existed at T still exists now. The nodes
//      present at T are a subset of the nodes the walk visits.
//
// From those: the as-of-T versions of the nodes that existed at T partition the
// key space as it was at T, so unioning them yields each key exactly once. A
// node created AFTER T has no version with ts <= T -- its oldest is newer --
// and is simply skipped; the keys it now holds were, at T, in the node it split
// from, whose as-of-T version is wider and still on the walk. No loss, no
// duplication, and no need to reason about split descriptors at all.
//
// TERMINATION is on the current k_min: once a node's k_min exceeds the query's
// upper bound, no node further right can hold a key in range, because k_min is
// immutable and non-decreasing along the chain.
//
// ── Pending versions ───────────────────────────────────────────────────────
//
// A version with ts == kNullTs is published but unstamped, so it cannot be
// compared with T at all. The reader SETTLES it -- the same helping the
// traversal does -- rather than guessing. Walking past it to old_ver would be
// wrong: its eventual stamp may well be <= T, and skipping it would drop a
// committed write from the snapshot.

#include <cstdint>
#include <vector>

#include "ds_defs.hpp"
#include "ds_node.hpp"
#include "ds_traverse.hpp"
#include "ds_ts.hpp"

namespace ds {

struct RangeStats {
  uint64_t ranges = 0;
  uint64_t nodes_walked = 0;     ///< data nodes visited on the current chain
  uint64_t versions_walked = 0;  ///< old_ver hops taken to reach as-of-T
  uint64_t nodes_skipped = 0;    ///< created after T, so absent from the snapshot
  uint64_t entries = 0;
  uint64_t helped = 0;           ///< pending versions settled on the way
  uint64_t vec_reads = 0;
  uint64_t nodes_read = 0;
  uint64_t truncated = 0;        ///< hit the caller's cap
  uint64_t failures = 0;
};

struct RangeResult {
  bool resolved = false;
  uint64_t snapshot = kNullTs;
  bool truncated = false;
};

namespace detail {
/// A node's old_ver chain is bounded by how many times it has been written, so
/// this is a livelock guard rather than a limit on history: reaching it means
/// the chain is cyclic or a writer is producing versions faster than we walk
/// them, and both are better retried than spun on.
inline constexpr uint32_t kMaxVersionHops = 1u << 16;
}  // namespace detail

/// The snapshot to read at, for /mode/.
///
/// Faa: READ the replicated counter (never FAA -- see the header note) and
/// admit every client index at that counter value. Clock: read the clock.
template <class Ops>
[[nodiscard]] uint64_t takeSnapshot(Ops &ops) {
  if (ops.tsMode() != TsMode::Faa) return ops.now();
  return (ops.readTsCounter() << kFaaClientBits) | kFaaClientMask;
}

/// Blocking snapshot range.
///
/// The oracle the resumable RangeOperation is differentially tested against,
/// and the simpler place to read the algorithm.
template <class Ops>
class Ranger {
 public:
  Ranger(Ops &ops, RangeStats &stats) : ops_(ops), stats_(stats) {}

  /// Collect entries with lo <= key <= hi, in key order, as of one snapshot.
  ///
  /// @param cap  stop after this many entries and report truncated
  RangeResult range(Key lo, Key hi, uint32_t layers, size_t cap,
                    std::vector<Entry> &out) {
    return rangeAt(lo, hi, layers, cap, takeSnapshot(ops_), out);
  }

  /// The same walk at a CALLER-SUPPLIED snapshot.
  ///
  /// Exposed because snapshot semantics cannot be tested without pinning T:
  /// "a write after the snapshot is invisible" means reading at an OLD T after
  /// that write has landed, which is not expressible if T is always taken
  /// fresh. It is also what a multi-range transaction would want, should one
  /// ever exist.
  RangeResult rangeAt(Key lo, Key hi, uint32_t layers, size_t cap,
                      uint64_t snapshot, std::vector<Entry> &out) {
    RangeResult res;
    ++stats_.ranges;
    res.snapshot = snapshot;
    if (hi < lo) {                     // empty interval, not an error
      res.resolved = true;
      return res;
    }

    // Route to the data node covering lo. The traversal is as-of-now, which is
    // right: it finds where lo lives in the CURRENT chain, and the walk then
    // reads each node as of T.
    PathStep path[kMaxLayers];
    Traversal<Ops> t(ops_);
    TraversalResult const tr = t.traverse(lo, layers, path);
    stats_.nodes_read += tr.nodes_read;
    stats_.vec_reads += tr.vec_reads;
    if (!tr.ok()) {
      // Miss means nothing routes to lo, i.e. the structure is empty below it.
      if (tr.status != TraversalStatus::Miss) {
        ++stats_.failures;
        return res;
      }
      res.resolved = true;
      return res;
    }

    RemoteAddr cur = tr.data_addr;
    while (!cur.isNull()) {
      NodeRecord node;
      VecRecord vec;
      if (!readSettled(cur, node, vec)) {
        ++stats_.failures;
        return res;
      }
      ++stats_.nodes_walked;

      // k_min is immutable, so this is a sound stop even though the version we
      // are about to read is older than the header we just read.
      if (node.k_min > hi) break;

      VecRecord asof;
      if (versionAsOf(vec, res.snapshot, asof)) {
        for (uint32_t i = 0; i < asof.size; ++i) {
          Key const k = asof.e[i].key;
          if (k < lo) continue;
          if (k > hi) break;           // entries are sorted
          if (out.size() >= cap) {
            res.truncated = true;
            ++stats_.truncated;
            res.resolved = true;
            return res;
          }
          out.push_back(asof.e[i]);
          ++stats_.entries;
        }
      } else {
        // No version at or before T: this node was created after the snapshot,
        // so at T its keys were in the node we have already read.
        ++stats_.nodes_skipped;
      }

      cur = nextNode(node, vec);
    }

    res.resolved = true;
    return res;
  }

 private:
  /// Read a node, settling it first if its version is unstamped.
  bool readSettled(RemoteAddr a, NodeRecord &node, VecRecord &vec) {
    for (int attempt = 0; attempt < detail::kMaxSettleAttempts; ++attempt) {
      if (!ops_.readNode(a, node)) return false;
      ++stats_.nodes_read;
      if (!ops_.readVec(node.handle.offset(), vec)) return false;
      ++stats_.vec_reads;
      if (!vec.isPending() && node.isStable()) return true;
      // Unstamped or mid-split: complete it rather than guess. A pending
      // version's eventual stamp may be <= T, so skipping it would drop a
      // committed write from the snapshot.
      HelpCounters hc;
      settleNode(ops_, a, node, vec, hc);
      stats_.nodes_read += hc.nodes_read;
      stats_.vec_reads += hc.vec_reads;
      stats_.helped += hc.helped_ts + hc.helped_splits;
    }
    return false;
  }

  /// The newest version of /vec/'s chain with ts <= T.
  ///
  /// @return false when the whole chain is newer than T -- the node did not
  ///         exist at the snapshot
  bool versionAsOf(VecRecord const &current, uint64_t snapshot,
                   VecRecord &out) {
    VecRecord v = current;
    for (uint32_t hop = 0; hop < detail::kMaxVersionHops; ++hop) {
      if (v.ts != kNullTs && v.ts <= snapshot) {
        out = v;
        return true;
      }
      if (v.old_ver == kNullVec) return false;
      VecOffset const older = static_cast<VecOffset>(v.old_ver);
      if (!ops_.readVec(older, v)) return false;
      ++stats_.vec_reads;
      ++stats_.versions_walked;
    }
    return false;
  }

  Ops &ops_;
  RangeStats &stats_;
};

}  // namespace ds
