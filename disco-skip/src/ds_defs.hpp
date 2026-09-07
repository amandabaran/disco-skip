#pragma once

// Constants and types shared by both halves of the remote side, with no
// dependency on the cache headers. The remote node layout needs the node
// capacity whether or not DS_CACHE_ENABLED is set (A4 settled that the two
// sides' capacities match), so it cannot live behind that toggle.

#include <cstddef>
#include <cstdint>

#include "ds_remote_addr.hpp"

// Toggles are set by src/CMakeLists.txt. The defaults keep every header here
// usable in a standalone compile check, and keep DS_MAX_LAYERS available to
// main.cpp's argument validation without it needing the cache.
#ifndef DS_MAX_LAYERS
#define DS_MAX_LAYERS 8
#endif
#ifndef DS_IDX_EXP
#define DS_IDX_EXP 3
#endif
#ifndef DS_N_REPLICAS
#define DS_N_REPLICAS 1
#endif

// These two are used as plain expressions and not only in #if, so an undefined
// macro is a hard error rather than an implicit 0. CMake always defines them;
// the defaults are here so a standalone compile of main.cpp does not depend on
// that.
#ifndef DS_CACHE_ENABLED
#define DS_CACHE_ENABLED 1
#endif
#ifndef DS_REG_CACHE_ENABLED
#define DS_REG_CACHE_ENABLED 1
#endif
#ifndef DS_REG_WRITEBACK_ENABLED
#define DS_REG_WRITEBACK_ENABLED 0
#endif

namespace ds {

/// Compile-time cap on the number of levels. This is passed straight through
/// as the cache's MAX_LAYERS template argument in ds_cache.hpp, so the two are
/// equal by construction rather than by convention.
inline constexpr size_t kMaxLayers = DS_MAX_LAYERS;

/// log2 of the target chunk size. The docs are written throughout for 3.
inline constexpr int64_t kIdxExp = DS_IDX_EXP;

/// Replica count. invariants.md §9 restricts this to 1 or 3: at 1 the quorum is
/// 1 and the single-replica path is taken.
inline constexpr size_t kNumReplicas = DS_N_REPLICAS;
static_assert(kNumReplicas == 1 || kNumReplicas == 3,
              "DS_N_REPLICAS must be 1 or 3 (invariants.md §9)");

/// Entries per node vector, on both sides. The cache computes this as
/// `2 << IDX_EXP` (skipvector_disco.h, node_t::get_vector_size), and A4 settled
/// that the remote capacity matches. The remote node layout asserts against
/// this, so the two cannot drift apart silently.
inline constexpr size_t kNodeCapacity = size_t{2} << kIdxExp;
static_assert(kNodeCapacity == 16, "IDX_EXP=3 was assumed throughout the docs");

/// Expected fan-out between adjacent levels: the cache's private
/// TARGET_IDX_RATIO. Interface doc §8's staleness analysis is written for
/// r = 8, and it is the height distribution we draw from, so it is worth
/// naming rather than spelling 8 in three places.
inline constexpr size_t kLevelRatio = size_t{1} << kIdxExp;

using Key = uint64_t;
using Value = uint64_t;

/// Reserved by the cache's vectors as an EMPTY sentinel
/// (vector_sfra.h uses static_cast<K>(-1)), so it must never be a live key.
inline constexpr Key kReservedKey = static_cast<Key>(-1);

/// What one level of a remote descent saw. Handed back to the cache after a
/// miss or a k_min mismatch so it can split its own nodes at the boundaries the
/// descent actually observed (interface doc §5, A5).
///
/// Collecting this costs zero extra RDMA: with passive memory servers the
/// client performs the descent itself, so it already holds every node on the
/// path. The only question was ever whether we keep them.
struct PathStep {
  /// k_min of the remote node covering the sought key at this level.
  Key k_min;
  /// That node's remote address.
  RemoteAddr addr;
  /// LEVEL 0 ONLY: value of that node's first entry, i.e. the data node whose
  /// k_min is this k_min. The cache needs it to create a local directory node
  /// with that minimum. At levels >= 1 the down value is a local pointer the
  /// cache resolves itself, so this stays null.
  RemoteAddr first_down;
};

}  // namespace ds
