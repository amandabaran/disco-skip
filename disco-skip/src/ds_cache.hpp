#pragma once

// The compute-local half of disco-skip: a mirror of the remote *index*, owned
// by the other author (disco-skip/include/skipvector_disco.h). This header is
// the only place the remote side touches it.
//
// The two halves never call each other (interface doc §1). Everything here is
// orchestrator glue: it adapts our RemoteAddr and our traversal output to the
// cache's call surface.
//
// Only included when DS_CACHE_ENABLED; constants shared with the remote node
// layout live in ds_defs.hpp so the no-cache baseline does not depend on the
// cache half at all.

#include <array>
#include <cstdint>
#include <type_traits>
#include <utility>

#include "ds_defs.hpp"
#include "ds_remote_addr.hpp"

// The cache's own headers. skipvector_disco.h includes "include-cache/..."
// relative to disco-skip/include, which src/CMakeLists.txt puts on the include
// path along with the generated machine_defines.h.
#include "include-cache/common/config.h"
#include "include-cache/common/machine_defines.h"
#include "include-cache/hp/hp_manager.h"
#include "include-cache/vector/vector_sfra.h"
#include "include-cache/vector/vector_umfra.h"

#include "skipvector_disco.h"

namespace ds {

using SkipVec = skipvector<Key, Value, RemoteAddr,
                           vector_sfra, vector_umfra,
                           /*IDX_EXP=*/kIdxExp, /*DATA_EXP=*/kIdxExp,
                           /*MAX_LAYERS=*/kMaxLayers,
                           /*HP=*/hp_manager<MAX_THREADS>>;

namespace detail {

// Detectors for the API added by "cache: reconcile by splitting at observed
// remote boundaries" (A5). They exist to turn a build against an older cache
// into a clear error rather than a silent behaviour change -- see the
// static_asserts below.

template <class SV, class = void>
struct HasPathReconcile : std::false_type {};
template <class SV>
struct HasPathReconcile<
    SV, std::void_t<decltype(std::declval<SV &>().mirror_reconcile(
            std::declval<Key const &>(), std::declval<RemoteAddr const &>(),
            std::declval<typename SV::path_step const *>(), uint32_t{}))>>
    : std::true_type {};

template <class SV, class = void>
struct HasHeadsBootstrapped : std::false_type {};
template <class SV>
struct HasHeadsBootstrapped<
    SV, std::void_t<decltype(std::declval<SV const &>().heads_bootstrapped())>>
    : std::true_type {};

template <class SV, class = void>
struct HasNodeCount : std::false_type {};
template <class SV>
struct HasNodeCount<
    SV, std::void_t<decltype(std::declval<SV const &>().node_count(int{}))>>
    : std::true_type {};

}  // namespace detail

// A5 is what makes gather_prevs self-healing and stops a read-mostly client
// degenerating the directory layer into a linked list. Falling back to
// entry-only repair would still be *correct*, but A5's own measurement puts it
// at "worse than having no cache at all" -- so this is a build error rather
// than a runtime degradation nobody notices until the numbers look wrong.
static_assert(detail::HasPathReconcile<SkipVec>::value,
              "The cache is missing the path-taking mirror_reconcile(). Update "
              "disco-skip/include to a revision at or after 'cache: reconcile "
              "by splitting at observed remote boundaries'.");
static_assert(detail::HasHeadsBootstrapped<SkipVec>::value,
              "The cache is missing heads_bootstrapped().");
static_assert(detail::HasNodeCount<SkipVec>::value,
              "The cache is missing node_count(), which metric 2 (local-to-"
              "remote node ratio) is measured from.");

/// Install the remote leftmost node address for each level.
///
/// A1: there is a stable leftmost node per level whose address is fixed for the
/// lifetime of the structure, so this is write-once. Interface doc §5:
/// SEQUENTIAL-ONLY, and must happen before any concurrent use. Leaving it
/// uncalled is safe but silently turns every lookup that lands left of the
/// first boundary into a per-level miss, which is why the caller should assert
/// headsBootstrapped() afterwards rather than infer it from a miss rate.
///
/// @param heads  heads[L] is the remote leftmost node at level L, for
///               L in 0..layers-1. Entries at and above /layers/ are ignored.
inline void bootstrapHeads(SkipVec &sv,
                           std::array<RemoteAddr, kMaxLayers> const &heads) {
  sv.set_head_remote_addrs(heads);
}

/// Whether bootstrapHeads() has run.
inline bool headsBootstrapped(SkipVec const &sv) {
  return sv.heads_bootstrapped();
}

/// Feed back what a remote traversal saw, after locate_data() missed or a read
/// showed a k_min mismatch (C4).
///
/// Installs the level-0 routing entry, then splits the local structure at the
/// remote boundaries the traversal actually observed, which is what repairs the
/// *coarse* case: a local node that recorded a boundary as an ordinary entry
/// because some other client created it. Idempotent.
///
/// @param data_k_min  k_min of the remote data node covering the sought key
/// @param data_addr   that node's remote address
/// @param path        what the traversal saw, path[L] describing level L
/// @param levels      valid entries in /path/; 0 (or a null path) repairs the
///                    entry only, and leaves promoted nodes null-addressed
inline void reconcile(SkipVec &sv, Key data_k_min, RemoteAddr data_addr,
                      PathStep const *path, uint32_t levels) {
  if (path == nullptr || levels == 0) {
    sv.mirror_reconcile(data_k_min, data_addr, nullptr, 0);
    return;
  }

  // A path can never be longer than the level cap, so this needs no allocation.
  // The conversion is field-by-field rather than a reinterpret_cast: the two
  // layouts happen to agree today, but a cast would break silently if the cache
  // reordered its fields, whereas this stops compiling.
  if (levels > kMaxLayers) {
    levels = kMaxLayers;
  }
  std::array<typename SkipVec::path_step, kMaxLayers> steps{};
  for (uint32_t i = 0; i < levels; ++i) {
    steps[i].k_min = path[i].k_min;
    steps[i].addr = path[i].addr;
    steps[i].first_down = path[i].first_down;
  }
  sv.mirror_reconcile(data_k_min, data_addr, steps.data(), levels);
}

/// Gather the remote address of the node covering k at each level a key of
/// /height/ will occupy, i.e. levels 0..height-1.
///
/// Any entry may come back null, meaning the covering node at that level has no
/// known remote counterpart; the caller must treat that as a per-level miss and
/// resolve it remotely.
///
/// @param out  must have room for /height/ entries
inline bool gatherPrevs(SkipVec &sv, Key k, uint32_t height, RemoteAddr *out) {
  return sv.gather_prevs(k, height, out);
}

/// Record a remote structural change that has already committed.
///
/// A key of height h occupies levels 0..h-1: at levels 0..h-2 it becomes the
/// minimum of a newly created node, and at h-1 it is inserted into the existing
/// node covering it. So index_addrs[L] for L < h-1 is the address of the node
/// the remote split *created* at level L, while index_addrs[h-1] is an orphan
/// address or null.
///
/// Height 0 needs no cache update at all -- a data-layer-only change does not
/// alter index structure -- so this rejects it rather than asserting inside the
/// cache.
///
/// @param layers  the runtime level count, to clamp /height/ against; the cache
///                asserts internally if a height exceeds it
inline void mirrorInsert(SkipVec &sv, Key k, uint32_t height,
                         RemoteAddr data_addr,
                         std::array<RemoteAddr, kMaxLayers> const &index_addrs,
                         uint32_t layers) {
  if (height == 0) {
    return;
  }
  if (height > layers) {
    height = layers;
  }
  sv.mirror_insert(k, static_cast<int>(height), data_addr, index_addrs);
}

/// Local node count at each level, 0 being the directory layer.
///
/// SEQUENTIAL-ONLY: walks the level chains without taking the sequence locks,
/// so only call it while the cache is quiescent. This is the numerator of
/// metric 2 (local nodes / remote nodes per level), which is what detects the
/// bloat failure mode in A9.
inline std::array<size_t, kMaxLayers> nodeCounts(SkipVec const &sv,
                                                 uint32_t layers) {
  std::array<size_t, kMaxLayers> counts{};
  for (uint32_t L = 0; L < layers && L < kMaxLayers; ++L) {
    counts[L] = sv.node_count(static_cast<int>(L));
  }
  return counts;
}

/// Orphan count and total entries at /level/.
///
/// SEQUENTIAL-ONLY, as node_count is. Orphans are ordinary skip-vector
/// structure produced by the capacity-versus-promotion geometry, not
/// divergence: with matched capacities (A4) the remote generates them at the
/// same rate. Reported so the ~21% steady-state figure in A9 can be confirmed
/// rather than assumed.
struct LevelStats {
  size_t nodes = 0;
  size_t orphans = 0;
  size_t entries = 0;
};

inline LevelStats levelStats(SkipVec const &sv, uint32_t level) {
  LevelStats s;
  sv.for_each_node(static_cast<int>(level),
                   [&s](Key, bool is_orphan, size_t entry_count) {
                     ++s.nodes;
                     s.orphans += is_orphan ? 1 : 0;
                     s.entries += entry_count;
                   });
  return s;
}

/// Adapts SkipVec to the surface Getter expects, so the orchestration in
/// ds_get.hpp can be driven with either the real cache or NullCache without
/// knowing which.
///
/// Deliberately thin: it converts types and nothing else. Anything that needs
/// to reason about the cache's semantics belongs in the free functions above,
/// where it can be read next to the interface doc.
class CacheAdapter {
 public:
  explicit CacheAdapter(SkipVec &sv) : sv_(sv) {}

  [[nodiscard]] RemoteAddr locateData(Key k) { return sv_.locate_data(k); }

  void reconcile(Key data_k_min, RemoteAddr data_addr, PathStep const *path,
                 uint32_t levels) {
    ds::reconcile(sv_, data_k_min, data_addr, path, levels);
  }

  SkipVec &sv() { return sv_; }

 private:
  SkipVec &sv_;
};

}  // namespace ds
