#pragma once

// The compute-local half of disco-skip: a mirror of the remote *index*, owned
// by the other author (disco-skip/include/skipvector_disco.h). This header is
// the only place the remote side touches it.
//
// The two halves never call each other (interface doc §1). Everything here is
// orchestrator glue: it adapts our RemoteAddr and our traversal output to the
// cache's call surface, and isolates the parts of that surface still in flux.
//
// Only included when DS_CACHE_ENABLED; shared constants live in ds_defs.hpp so
// the no-cache baseline does not depend on the cache half at all.

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

// ── Feature detection for the parts of the cache API that are documented but
// ── not yet pushed.
//
// Interface doc §5 lists a path-taking mirror_reconcile, heads_bootstrapped(),
// node_count() and for_each_node(), and A5 describes them as built and
// measured. None of those symbols exist on main or origin/put_perf_testing as
// of this writing. Rather than guess, detect each one and degrade.
//
// When the real header lands, every `if constexpr` on these flips to the
// first branch with no edit here.

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

inline constexpr bool kCacheHasPathReconcile =
    detail::HasPathReconcile<SkipVec>::value;
inline constexpr bool kCacheHasHeadsBootstrapped =
    detail::HasHeadsBootstrapped<SkipVec>::value;
inline constexpr bool kCacheHasNodeCount = detail::HasNodeCount<SkipVec>::value;

/// Assert that the cache was told its head addresses, when it can tell us.
/// Interface doc §5: leaving bootstrap uncalled is safe but silently turns
/// every lookup that lands on a head into a per-level miss, so this exists to
/// be asserted rather than inferred from a high miss rate.
///
/// A template on purpose: `if constexpr` only discards the untaken branch
/// inside a template, so a plain function here would still be checked against
/// the current header and fail to compile.
template <class SV>
inline bool headsBootstrapped(SV const &sv) {
  if constexpr (detail::HasHeadsBootstrapped<SV>::value) {
    return sv.heads_bootstrapped();
  } else {
    (void)sv;
    return true;  // unknowable with the current header; do not block on it
  }
}

}  // namespace ds
