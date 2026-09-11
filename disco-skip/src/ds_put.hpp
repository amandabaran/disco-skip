#pragma once

// The write orchestration, and Update_Index. Symmetric with ds_get.hpp: the
// cache and the remote layer never call each other (interface doc §1), and this
// is the code that composes them.
//
// Shape of a Put of key k with height h:
//
//   1. Descend to the data node covering k, keeping the path. It costs no extra
//      RDMA and is exactly what Update_Index needs at every level.
//   2. h == 0: insert into that data node and stop. A data-layer-only change
//      alters no index structure, so there is no cache call at all.
//   3. h >= 1: k becomes the minimum of a newly created node at the data level
//      and at levels 0 .. h-2, and an ordinary entry at level h-1. That is the
//      skip-vector invariant in interface doc §2, and it is what mirror_insert
//      is shaped to receive.
//   4. Report the structural change to the cache once, with the addresses the
//      splits created.
//
// Splits here are HEIGHT-driven, not capacity-driven (remote-design.md §5): a
// key of height h is split in whether or not the nodes are full. Capacity
// overflow is a separate mechanism, living inside insertWithOverflow, and it
// produces an orphan with no parent rather than a parented boundary.
//
// Every step is idempotent under retry, which is what makes a partially applied
// structural change safe to abandon and redo: splitting at a key that is
// already a node's k_min is a no-op returning that node, so a retry after the
// data split succeeded but a level-1 split lost its CAS re-derives the same
// structure rather than building a second copy of it.

#include <array>
#include <cstdint>

#include "ds_defs.hpp"
#include "ds_descend.hpp"
#include "ds_insert.hpp"
#include "ds_node.hpp"

namespace ds {

struct PutStats {
  uint64_t puts = 0;          ///< resolved operations
  uint64_t height0 = 0;       ///< data-layer-only, no index change, no cache call
  uint64_t structural = 0;    ///< height >= 1, so Update_Index ran
  uint64_t index_splits = 0;  ///< height-driven splits at index levels
  uint64_t data_splits = 0;   ///< height-driven splits at the data level
  uint64_t top_orphans = 0;   ///< top-level inserts that overflowed into an orphan
  uint64_t descents = 0;
  uint64_t restarts = 0;      ///< a step lost its race, so the whole put redid
  uint64_t mirror_calls = 0;
  uint64_t not_covered = 0;   ///< descent reported Miss: nothing routes to k
  uint64_t failures = 0;
  uint64_t nodes_read = 0;
  uint64_t vec_reads = 0;
  uint64_t right_hops = 0;
  uint64_t helped = 0;
};

struct PutResult {
  bool resolved = false;  ///< false means retry, not rejected
  uint32_t height = 0;    ///< the height actually applied, after clamping
  RemoteAddr data_addr{}; ///< the data node holding k afterwards
};

namespace detail {

/// A put is a sequence of independently CAS'd steps, so losing one means
/// redoing the sequence. Bounded because each restart implies somebody else
/// made progress; this guards our own starvation, not a livelock.
inline constexpr int kMaxPutAttempts = 8;

}  // namespace detail

/// Draw a height for /k/ the way the cache's geometry expects.
///
/// Geometric with p = 1/kLevelRatio, clamped to [0, layers]. Height 0 is the
/// common case by design -- 7 keys in 8 alter no index structure, which is what
/// keeps Update_Index off the hot path.
///
/// Takes the RNG by reference rather than owning one, so a test can make a
/// sequence of heights reproducible.
template <class Rng>
uint32_t drawHeight(Rng &rng, uint32_t layers) {
  uint32_t h = 0;
  while (h < layers && (rng() % kLevelRatio) == 0) ++h;
  return h;
}

/// @param Ops    the RDMA (or fake) operation surface
/// @param Cache  a null-cache stand-in, or the CacheAdapter over SkipVec
template <class Ops, class Cache>
class Putter {
 public:
  Putter(Ops &ops, Cache &cache, uint32_t layers, PutStats &stats,
         WriteStats &wstats)
      : ops_(ops),
        cache_(cache),
        layers_(layers),
        stats_(stats),
        writer_(ops, wstats) {}

  PutResult put(Key k, Value v, uint32_t height) {
    PutResult out;
    if (height > layers_) height = layers_;
    out.height = height;

    for (int attempt = 0; attempt < detail::kMaxPutAttempts; ++attempt) {
      PathStep path[kMaxLayers];
      Descender<Ops> d(ops_);
      ++stats_.descents;
      DescentResult const r = d.descend(k, layers_, path);
      stats_.nodes_read += r.nodes_read;
      stats_.vec_reads += r.vec_reads;
      stats_.right_hops += r.right_hops;
      stats_.helped += r.helped_ts + r.helped_splits;

      if (!r.ok()) {
        // Miss means no level had an entry at or below k. Every head covers
        // from key 0, so on a well-formed structure this cannot happen -- it is
        // reported rather than retried so a real structural fault is visible.
        if (r.status == DescentStatus::Miss) ++stats_.not_covered;
        ++stats_.failures;
        return out;
      }

      // ── Height 0: the data layer only ──────────────────────────────────────
      if (height == 0) {
        WriteOutcome const o =
            writer_.insertWithOverflow(r.data_addr, k, v, nullptr);
        if (o == WriteOutcome::Retry) {
          ++stats_.restarts;
          continue;
        }
        if (o != WriteOutcome::Published) {
          ++stats_.failures;
          return out;
        }
        ++stats_.puts;
        ++stats_.height0;
        out.resolved = true;
        out.data_addr = r.data_addr;
        return out;
      }

      // ── Height >= 1: k becomes a boundary from the data layer upward ───────
      //
      // The data node splits at k too, so k names a data node of its own --
      // that is the address mirror_insert wants, since a level-0 entry points
      // at a data node rather than at another index node.
      Entry const data_seed{k, v};
      SplitResult const ds_split =
          writer_.splitAt(r.data_addr, k, /*orphan=*/false, &data_seed);
      if (ds_split.outcome == WriteOutcome::Retry) {
        ++stats_.restarts;
        continue;
      }
      if (ds_split.outcome != WriteOutcome::Published) {
        ++stats_.failures;
        return out;
      }
      if (ds_split.created != r.data_addr) ++stats_.data_splits;

      // The value inserted at the data level is the payload; at every level
      // above it is the id of the node one level down.
      std::array<RemoteAddr, kMaxLayers> index{};
      RemoteAddr down = ds_split.created;
      bool restart = false;

      // Levels 0 .. h-2: k is the minimum of a node this split creates.
      for (uint32_t level = 0; level + 2 <= height; ++level) {
        Entry const seed{k, down.id};
        SplitResult const s =
            writer_.splitAt(path[level].addr, k, /*orphan=*/false, &seed);
        if (s.outcome == WriteOutcome::Retry) {
          restart = true;
          break;
        }
        if (s.outcome != WriteOutcome::Published) {
          ++stats_.failures;
          return out;
        }
        if (s.created != path[level].addr) ++stats_.index_splits;
        index[level] = s.created;
        down = s.created;
      }
      if (restart) {
        ++stats_.restarts;
        continue;
      }

      // Level h-1: k is an ordinary entry in the node that already covers it.
      // This is the one level that may overflow, and A3 says the slot carries
      // the resulting orphan's address, or null if there was none.
      RemoteAddr orphan{};
      WriteOutcome const top = writer_.insertWithOverflow(
          path[height - 1].addr, k, down.id, &orphan);
      if (top == WriteOutcome::Retry) {
        ++stats_.restarts;
        continue;
      }
      if (top != WriteOutcome::Published) {
        ++stats_.failures;
        return out;
      }
      index[height - 1] = orphan;
      if (!orphan.isNull()) ++stats_.top_orphans;

      // ── Tell the cache, once, after the remote change has committed ────────
      cache_.mirrorInsert(k, height, ds_split.created, index);
      ++stats_.mirror_calls;

      ++stats_.puts;
      ++stats_.structural;
      out.resolved = true;
      out.data_addr = ds_split.created;
      return out;
    }
    ++stats_.failures;
    return out;
  }

 private:
  Ops &ops_;
  Cache &cache_;
  uint32_t layers_;
  PutStats &stats_;
  Writer<Ops> writer_;
};

/// A cache that records nothing, for the DS_CACHE_ENABLED=0 baseline. Mirrors
/// ds_get.hpp's NullCache on the write side.
struct NullPutCache {
  void mirrorInsert(Key, uint32_t, RemoteAddr,
                    std::array<RemoteAddr, kMaxLayers> const &) {}
};

}  // namespace ds
