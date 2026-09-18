#pragma once

// The point-read orchestration: where the cache and the wire meet.
//
// The cache and the remote layer never call each other (interface doc §1);
// this is the code that composes them. Templated over Ops so it can be driven
// against a fake arena in tests with the *real* cache attached, which is the
// only way to exercise the miss-and-reconcile loop without a cluster.
//
// Shape of a Get:
//
//   1. Ask the cache which data node may hold k. A null answer is a miss.
//   2. Read that node. Three outcomes (interface doc §7a), and only the third
//      costs another round trip -- which co-locating next_k_min removes:
//        - k is in the vector                         -> hit
//        - k absent and k < the node's range end      -> definitively absent
//        - k at or past the range end                 -> wrong node (C4)
//   3. On a miss or a C4 mismatch, traverse remotely and feed the path back so
//      the cache splits at the boundaries the traversal saw.
//
// Step 3 is the whole reason a stale cache costs round trips rather than wrong
// answers: the range check is what turns a bad hint into a detected one.

#include <array>
#include <cstdint>

#include "ds_defs.hpp"
#include "ds_traverse.hpp"
#include "ds_node.hpp"

namespace ds {

/// Why a Get took the path it did. Counted rather than logged, since the
/// interesting quantity is the distribution over a workload.
struct GetStats {
  uint64_t cache_hits = 0;      ///< cache named a node that answered
  uint64_t cache_misses = 0;    ///< cache had no entry for k
  uint64_t kmin_mismatch = 0;   ///< C4: named node no longer covers k

  uint64_t traversals = 0;        ///< full remote traversals

  // ── WHAT A CACHE MISMATCH COSTS, which was not measurable ────────────────
  //
  // The cache's HIT RATE was already answerable and is not the interesting
  // number: the directory almost always names a node, and outright misses are
  // negligible. What costs is naming a STALE one -- a C4 k_min mismatch, where
  // the named node no longer covers k. So "cache miss" is the wrong thing to
  // think about here and the mismatch rate is the whole cost.
  //
  // What that cost IS could not be stated. The client prints one pooled
  // `round trips` figure over gets, puts and ranges -- 5.81 per op on that
  // run -- with no way to split it, and GetStats::nodes_read and vec_reads
  // were accumulated and then never printed. So the largest identified lever
  // on the read path had no price attached.
  //
  // These two split it. Both count TIMES THE OPERATION WAITED FOR THE
  // NETWORK, not records fetched, which is why they do not come from
  // nodes_read + vec_reads: a speculation hit bumps vec_reads without a round
  // trip, and 9.55 M of 18.2 M speculations hit on that run.
  //
  /// Round trips spent on the hint path -- the fast path, whether or not it
  /// ended up answering. Divide by (cache_hits + kmin_mismatch) for the cost
  /// of consulting the cache at all.
  uint64_t rt_hint = 0;
  /// Round trips spent inside a full remote traversal. Divide by `traversals`
  /// for what one costs. THIS IS THE NUMBER the mismatch rate multiplies.
  uint64_t rt_traverse = 0;

  // ── RECOVERING A STALE HINT BY HOPPING SIDEWAYS (--hint-hops) ───────────
  //
  // A k_min mismatch means the named node SPLIT, so k is probably in a sibling
  // one `next` hop away, and following it should cost one round trip where a
  // full descent costs four or five.
  //
  // IT WAS MEASURED ONCE AND LOST, and the measurement's provenance is now
  // ambiguous, which is why the mechanism is back behind a flag rather than
  // gone. The recorded arms were hops 0/2/4 at 8 and 16 clients -- so budget
  // ONE was never measured at all, and its arithmetic is different: the losing
  // case was "~2 trips saved against ~2.4 spent", and at a budget of 1 at most
  // one can be spent. The commit that recorded those numbers ALSO fixed a
  // preload bug under which every client inserted the whole key set, and its
  // two tables disagree with each other about 16-client throughput, so which
  // preload the hop arms ran on cannot be established from the history.
  //
  /// Gets whose hint went stale, counted ONCE PER OPERATION.
  ///
  /// THE COUNTER THE ORIGINAL TEST NEEDED AND DID NOT HAVE. kmin_mismatch
  /// counts DETECTIONS, and each failed hop lands on another node that also
  /// fails to cover k -- so it inflates with the hop budget and made the
  /// staleness rate look worse at higher budgets instead of holding constant.
  /// The arms then could not be compared on the quantity the experiment was
  /// about. This one is invariant to the hop budget by construction.
  uint64_t hint_stale_ops = 0;
  /// Sideways hops actually taken, and how many landed on a node covering k.
  /// The ratio is the hit rate the bet turns on, which used to be inferred
  /// from arm-to-arm differences rather than counted.
  uint64_t hops_taken = 0;
  uint64_t hops_recovered = 0;
  /// Gets that spent their whole hop budget and still had to descend. These
  /// paid the hops AND the traversal, which is the losing case.
  uint64_t hops_exhausted = 0;
  /// Directory repairs performed by a RECOVERED HOP rather than by a descent.
  ///
  /// THE FIX FOR WHY HOPPING LOST. Reconciliation used to happen only after a
  /// traversal -- `reconciles` tracked `traversals` exactly -- so a hop that
  /// answered the get left the stale directory entry in place and the next get
  /// on that key was stale again. Staleness compounded, and the damage reached
  /// the write path too, because writes consult the same directory.
  ///
  /// So the hop was winning its own bet on round trips and losing throughput
  /// anyway, because it skipped a repair worth more than the trips it saved.
  /// A traversal's real product is the CACHE REPAIR, not the answer.
  ///
  /// Repairing on a recovered hop needs no path: mirror_reconcile's contract
  /// documents `levels = 0` as "entry repair only" and the data routing entry
  /// as "always safe, always useful", with a repeat call a no-op. What is given
  /// up is the partition alignment of step 2, which genuinely needs a descent
  /// -- a lost optimisation, not a correctness gap.
  uint64_t hop_reconciles = 0;
  uint64_t reconciles = 0;      ///< paths fed back to the cache
  uint64_t nodes_read = 0;   ///< 64-byte header reads
  uint64_t vec_reads = 0;    ///< 320-byte vector reads
  uint64_t right_hops = 0;
  uint64_t helped = 0;          ///< in-flight operations completed for a writer
  uint64_t not_found = 0;       ///< resolved, and k genuinely does not exist
  uint64_t failures = 0;        ///< could not resolve; caller should retry
  /// Breakdown of `failures` by which livelock guard fired. Every one of these
  /// is starvation under contention, not an error -- see TraversalGaveUp.
  uint64_t gave_up_no_majority = 0;
  uint64_t gave_up_settle_stuck = 0;
  uint64_t gave_up_too_many_hops = 0;
};

struct GetResult {
  bool resolved = false;  ///< false means retry, not absent
  bool found = false;
  Value value = 0;
};

/// @param Ops    the RDMA (or fake) operation surface, as ds_traverse.hpp defines
/// @param Cache  a null-cache stand-in, or the CacheAdapter over SkipVec
template <class Ops, class Cache>
class Getter {
 public:
  Getter(Ops &ops, Cache &cache, uint32_t layers, GetStats &stats)
      : ops_(ops), cache_(cache), layers_(layers), stats_(stats) {}

  GetResult get(Key k) {
    GetResult out;

    // ── 1. The cache's hint ────────────────────────────────────────────────
    RemoteAddr const hinted = cache_.locateData(k);
    if (!hinted.isNull()) {
      NodeRecord node;
      VecRecord vec;
      // The header alone answers the range check on a stable node, so a bad
      // hint is detected for 64 bytes rather than 384. Only a hint that looks
      // good, or a node mid-propagation, pays for the vector.
      if (ops_.readNode(hinted, node)) {
        ++stats_.nodes_read;
        bool const stable = node.isStable();
        bool have_vec = false;
        if (!stable) {
          if (ops_.readVec(node.handle.offset(), vec)) {
            ++stats_.vec_reads;
            have_vec = true;
          }
        }
        bool const in_range =
            stable ? coversByHeader(node, k) : (have_vec && covers(node, vec, k));
        if (in_range) {
          if (!have_vec) {
            if (ops_.readVec(node.handle.offset(), vec)) {
              ++stats_.vec_reads;
              have_vec = true;
            }
          }
        }
        if (in_range && have_vec) {
          // ── 2. The hint was good. Answer from this node alone. ──────────
          ++stats_.cache_hits;
          int const idx = findLte(vec, k);
          out.resolved = true;
          if (idx >= 0 && vec.keyAt(idx) == k) {
            out.found = true;
            out.value = vec.valAt(idx);
          } else {
            ++stats_.not_found;
          }
          return out;
        }
        // C4: the address was real and the read consistent, but this is no
        // longer the right node. Not a wrong answer -- a detected bad hint.
        ++stats_.kmin_mismatch;
      } else {
        // Unreadable, so treat it as a miss and resolve the hard way.
        ++stats_.cache_misses;
      }
    } else {
      ++stats_.cache_misses;
    }

    // ── 3. Resolve remotely, and teach the cache what we saw ───────────────
    PathStep path[kMaxLayers];
    ++stats_.traversals;
    Traversal<Ops> d(ops_);
    TraversalResult const r = d.traverse(k, layers_, path);

    stats_.nodes_read += r.nodes_read;
    stats_.vec_reads += r.vec_reads;
    stats_.right_hops += r.right_hops;
    stats_.helped += r.helped_ts + r.helped_splits;

    if (!r.ok()) {
      // A Miss here means nothing routes to k yet, which for a read is simply
      // "not present": there is no node to install, so no reconcile either.
      if (r.status == TraversalStatus::Miss) {
        out.resolved = true;
        ++stats_.not_found;
        return out;
      }
      ++stats_.failures;
      return out;  // resolved == false: the caller retries
    }

    // The traversal cost nothing extra to record, so hand the whole path back.
    // A5: this is what lets the cache split at the boundaries the traversal
    // actually saw, which is what stops a read-mostly client degenerating the
    // directory into a linked list.
    cache_.reconcile(r.data_k_min, r.data_addr, path, r.levels);
    ++stats_.reconciles;

    out.resolved = true;
    out.found = r.found;
    out.value = r.value;
    if (!r.found) ++stats_.not_found;
    return out;
  }

 private:
  Ops &ops_;
  Cache &cache_;
  uint32_t layers_;
  GetStats &stats_;
};

/// A cache that never hints and never learns, for the DS_CACHE_ENABLED=0
/// baseline. Every Get then costs a full traversal, which is the number the
/// cache has to beat.
/// Satisfies the WHOLE operation surface -- both halves -- because a single
/// future runs either a Get or a Put and is constructed with one cache type.
/// ds_put.hpp's NullPutCache remains the write-only subset, for the blocking
/// Putter's existing call sites.
struct NullCache {
  [[nodiscard]] RemoteAddr locateData(Key) const { return RemoteAddr{}; }
  /// Always a MISS, so a cache-less future falls back to the remote descent.
  /// Returning 0 is not a degenerate stub: RangeOperation::fillFromCache
  /// treats 0 as "the cache knows nothing about this range" and walks the
  /// backbone remotely, which is exactly the no-cache behaviour.
  [[nodiscard]] size_t locateDataRange(Key, Key, Key *, RemoteAddr *,
                                       size_t) const {
    return 0;
  }
  void reconcile(Key, RemoteAddr, PathStep const *, uint32_t) {}
  void mirrorInsert(Key, uint32_t, RemoteAddr,
                    std::array<RemoteAddr, kMaxLayers> const &) {}
};

}  // namespace ds
