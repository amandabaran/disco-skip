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
//   3. On a miss or a C4 mismatch, descend remotely and feed the path back so
//      the cache splits at the boundaries the descent saw.
//
// Step 3 is the whole reason a stale cache costs round trips rather than wrong
// answers: the range check is what turns a bad hint into a detected one.

#include <cstdint>

#include "ds_defs.hpp"
#include "ds_descend.hpp"
#include "ds_node.hpp"

namespace ds {

/// Why a Get took the path it did. Counted rather than logged, since the
/// interesting quantity is the distribution over a workload.
struct GetStats {
  uint64_t cache_hits = 0;      ///< cache named a node that answered
  uint64_t cache_misses = 0;    ///< cache had no entry for k
  uint64_t kmin_mismatch = 0;   ///< C4: named node no longer covers k
  uint64_t descents = 0;        ///< full remote traversals
  uint64_t reconciles = 0;      ///< paths fed back to the cache
  uint64_t nodes_read = 0;
  uint64_t right_hops = 0;
  uint64_t helped = 0;          ///< in-flight operations completed for a writer
  uint64_t not_found = 0;       ///< resolved, and k genuinely does not exist
  uint64_t failures = 0;        ///< could not resolve; caller should retry
};

struct GetResult {
  bool resolved = false;  ///< false means retry, not absent
  bool found = false;
  Value value = 0;
};

/// @param Ops    the RDMA (or fake) operation surface, as ds_descend.hpp defines
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
      if (ops_.read(hinted, node, vec)) {
        ++stats_.nodes_read;
        if (covers(node, vec, k)) {
          // ── 2. The hint was good. Answer from this node alone. ──────────
          ++stats_.cache_hits;
          int const idx = findLte(vec, k);
          out.resolved = true;
          if (idx >= 0 && vec.e[idx].key == k) {
            out.found = true;
            out.value = vec.e[idx].val;
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
    ++stats_.descents;
    Descender<Ops> d(ops_);
    DescentResult const r = d.descend(k, layers_, path);

    stats_.nodes_read += r.nodes_read;
    stats_.right_hops += r.right_hops;
    stats_.helped += r.helped_ts + r.helped_splits;

    if (!r.ok()) {
      // A Miss here means nothing routes to k yet, which for a read is simply
      // "not present": there is no node to install, so no reconcile either.
      if (r.status == DescentStatus::Miss) {
        out.resolved = true;
        ++stats_.not_found;
        return out;
      }
      ++stats_.failures;
      return out;  // resolved == false: the caller retries
    }

    // The descent cost nothing extra to record, so hand the whole path back.
    // A5: this is what lets the cache split at the boundaries the descent
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
/// baseline. Every Get then costs a full descent, which is the number the
/// cache has to beat.
struct NullCache {
  [[nodiscard]] RemoteAddr locateData(Key) const { return RemoteAddr{}; }
  void reconcile(Key, RemoteAddr, PathStep const *, uint32_t) {}
};

}  // namespace ds
