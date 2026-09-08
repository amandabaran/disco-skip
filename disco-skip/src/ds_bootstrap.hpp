#pragma once

// Building the initial (empty) skip vector.
//
// The memory servers run no logic, so nobody on the remote side can initialise
// the structure -- one client has to write it before anyone traverses. Zeroing
// the arena is not enough: a zeroed node has next_k_min == 0 and so covers no
// key at all.
//
// This header only *builds* the records. Writing them out is the caller's job
// (see the bootstrap block in main.cpp), which keeps this dory-free and lets
// disco-skip/tests build the same structure into a local arena and run the
// invariant checker over it.

#include <array>
#include <cstdint>

#include "ds_defs.hpp"
#include "ds_node.hpp"

namespace ds {

/// The nodes the initial structure consists of, in the order they should be
/// written: one data node, then one head per level from the directory up.
///
/// Shape of the empty structure, for `layers = 3`:
///
///     level 2   H2 [ 0 ]          k_min 0, next none
///                    |
///     level 1   H1 [ 0 ]          k_min 0, next none
///                    |
///     level 0   H0 [ 0 ]          k_min 0, next none   (the directory)
///                    |
///     data      D0 [ ]            k_min 0, empty
///
/// Every head carries exactly one entry, keyed 0, pointing one level down. That
/// single entry is what makes the structure navigable from the first operation:
/// a descent does find_lte(k) at each level, and with an entry at key 0 it
/// always has somewhere to go, for any key. An empty directory head would leave
/// a descent with no down pointer to follow and nothing to report.
///
/// The entry is also *faithful* rather than a special case: a head's entries are
/// the boundaries of the level below, and the level below really does have a
/// node whose minimum is 0. So the same rule that governs every other index
/// entry -- entry key equals child k_min -- holds here too, which is what lets
/// the invariant checker treat heads like anything else.
///
/// Heads are not orphans. I4's is_orphan means "reachable only by walking
/// next", and every head except the topmost is the target of a down pointer
/// from the level above. The topmost is the structure's entry point, reached by
/// its well-known id rather than by a pointer, which is a different thing from
/// being unreachable. (Note this differs from the cache side, where heads *must*
/// be flagged orphan because verify_index() only tolerates an unreferenced node
/// in a layer if it is one -- interface doc §3. The two flags answer different
/// questions.)
struct InitialNode {
  RemoteAddr addr;
  NodeRecord record;
};

/// How many nodes the initial structure has: one data node plus one head per
/// level.
inline constexpr uint32_t initialNodeCount(uint32_t layers) {
  return layers + 1;
}

/// Build the initial structure into /out/, returning how many entries were
/// filled. /out/ must have room for initialNodeCount(layers).
///
/// @param layers  level count, counting the directory as 0. Must be > 1 and
///                <= kMaxLayers, which the caller validates.
inline uint32_t buildInitialStructure(uint32_t layers, InitialNode *out) {
  uint32_t n = 0;

  // The one data node. Empty, and covering [0, inf) since it has no successor.
  {
    InitialNode &in = out[n++];
    in.addr = RemoteAddr{kInitialDataId};
    initNode(in.record, /*k_min=*/0, kDataLevel, /*is_orphan=*/false);
  }

  // Heads, bottom-up, each pointing at the one below.
  for (uint32_t level = 0; level < layers; ++level) {
    InitialNode &in = out[n++];
    in.addr = RemoteAddr{kHeadIdBase + level};
    initNode(in.record, /*k_min=*/0, level, /*is_orphan=*/false);

    uint64_t const child = (level == 0) ? kInitialDataId
                                        : kHeadIdBase + (level - 1);
    VecSlot &s = in.record.current();
    s.size = 1;
    s.e[0] = Entry{/*key=*/0, /*val=*/child};
    stampSlot(s, in.record.handle);
  }

  return n;
}

/// The head addresses to hand the cache, in the layout
/// set_head_remote_addrs() expects: addrs[L] is the remote leftmost node at
/// level L. Slots at and above /layers/ stay null and are ignored by the cache.
inline std::array<RemoteAddr, kMaxLayers> headAddrs(uint32_t layers) {
  std::array<RemoteAddr, kMaxLayers> a{};
  for (uint32_t L = 0; L < layers && L < kMaxLayers; ++L) {
    a[L] = RemoteAddr{kHeadIdBase + L};
  }
  return a;
}

}  // namespace ds
