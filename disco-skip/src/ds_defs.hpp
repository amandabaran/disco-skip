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

/// Upper bound on the replicas one client fans out to, for stack arrays sized
/// at compile time while the live count is dynamic (`conns_.size()`, from -s).
///
/// At namespace scope rather than private to RdmaReplicaSet, which is where it
/// used to live: the async ops need the same bound for its per-replica FAA
/// pre-value arrays, and two copies of a bound are two things to keep in step.
inline constexpr size_t kMaxReplicaFanout = 8;

/// Entries per node vector, on both sides. The cache computes this as
/// `2 << IDX_EXP` (skipvector_disco.h, node_t::get_vector_size), and A4 settled
/// that the remote capacity matches. The remote node layout asserts against
/// this, so the two cannot drift apart silently.
/// log2 of the DATA node's chunk size, separate from the index's.
///
/// WHY THIS IS ITS OWN KNOB. kNodeCapacity used to derive from kIdxExp, which
/// tied the data vector's entry count to the INDEX fan-out -- two unrelated
/// concerns. Bigger data nodes cut round trips on a range walk (fewer nodes to
/// cover the same key span); the index fan-out governs descent depth. Sweeping
/// one should not move the other.
///
/// The cache already separates them: ds_cache.hpp instantiates the skip vector
/// with IDX_EXP and DATA_EXP as distinct template arguments and we were simply
/// passing kIdxExp for both, so this needs no change on the cache side.
///
/// Defaults to kIdxExp, so an unset DS_DATA_EXP reproduces the old geometry
/// exactly.
#ifndef DS_DATA_EXP
#define DS_DATA_EXP DS_IDX_EXP
#endif
inline constexpr int64_t kDataExp = DS_DATA_EXP;

/// Entries per DATA node vector, on both sides.
///
/// 2 << kDataExp: 16, 32, 64, 128 at DS_DATA_EXP = 3, 4, 5, 6. VecRecord stays
/// a multiple of 64 bytes at every one of those (64 B header + cap*8 keys +
/// cap*8 values), so the cache-line assert below holds without special cases.
inline constexpr size_t kNodeCapacity = size_t{2} << kDataExp;
static_assert(kNodeCapacity >= 16 && kNodeCapacity <= 128,
              "DS_DATA_EXP must be 3..6: below 16 the docs' geometry breaks, "
              "above 128 a VecRecord stops fitting a sensible RDMA read");
static_assert((kNodeCapacity & (kNodeCapacity - 1)) == 0,
              "capacity must be a power of two for the SIMD key scan's tail "
              "handling to be a whole number of lanes");

/// Expected fan-out between adjacent levels: the cache's private
/// TARGET_IDX_RATIO. Interface doc §8's staleness analysis is written for
/// r = 8, and it is the height distribution we draw from, so it is worth
/// naming rather than spelling 8 in three places.
inline constexpr size_t kLevelRatio = size_t{1} << kIdxExp;

// ── TARGET SIZE vs MAX CAPACITY ──────────────────────────────────────────
//
//   target size  = kLevelRatio   = 1 << kIdxExp    mean entries per node
//   max capacity = kNodeCapacity = 2 << kDataExp   the node's ceiling
//
// With DS_DATA_EXP defaulting to DS_IDX_EXP these coincide as
// capacity == 2 * target, which is the intended geometry and worth stating
// because it is load-bearing rather than arbitrary. drawHeight makes a key a
// node boundary with probability 1/kLevelRatio, so runs of height-0 keys are
// geometric with mean (1-p)/p ~= kLevelRatio. A ceiling of TWICE that mean
// leaves room for the tail of the distribution, which is what keeps capacity
// splits rare -- height-driven splits outnumber capacity-driven ones by
// roughly an order of magnitude at ratio 8.
//
// SEPARATING THE TWO EXPONENTS MADE THIS BREAKABLE, hence the assert. A
// capacity below 2 * target inverts the regime -- capacity splits become the
// common case and occupancy is clamped by the ceiling instead of being set by
// the height distribution, which is the one thing the split statistics say
// governs it. Going the other way (capacity well above 2 * target) is merely
// wasteful: it was measured inert, since 8x the capacity at a fixed ratio left
// the node and entry counts essentially unchanged.
static_assert(kNodeCapacity >= 2 * kLevelRatio,
              "max capacity must be at least 2 << kIdxExp, i.e. twice the "
              "target size, or capacity splits dominate and occupancy is "
              "clamped by the ceiling rather than by drawHeight");

using Key = uint64_t;
using Value = uint64_t;

/// Reserved by the cache's vectors as an EMPTY sentinel
/// (vector_sfra.h uses static_cast<K>(-1)), so it must never be a live key.
inline constexpr Key kReservedKey = static_cast<Key>(-1);

/// What one level of a remote traversal saw. Handed back to the cache after a
/// miss or a k_min mismatch so it can split its own nodes at the boundaries the
/// traversal actually observed (interface doc §5, A5).
///
/// Collecting this costs zero extra RDMA: with passive memory servers the
/// client performs the traversal itself, so it already holds every node on the
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
