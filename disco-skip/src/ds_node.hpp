#pragma once

// The remote node layout: what actually lives in memory-server memory.
//
// Memory servers run no logic (invariants.md §8 / interface doc §0), so this
// header is the whole contract between clients. Every field's placement is load
// bearing -- it decides how many round trips an operation costs and which
// concurrent interleavings are observable -- so the reasoning is recorded
// inline rather than left to be re-derived.
//
// Nothing here depends on dory or on the cache half, so it is exercised by
// disco-skip/tests off-cluster.

#include <cstddef>
#include <cstdint>
#include <type_traits>

#include "ds_defs.hpp"

namespace ds {

/// One key/value pair inside a node's vector.
///
/// `val` is read three ways depending on the node's level, which is why there
/// is one entry type and not three:
///   - data node        -> the payload for `key`
///   - level-0 (directory) node -> RemoteAddr::id of the data node whose k_min is `key`
///   - level->=1 index node     -> RemoteAddr::id of the node one level down
///
/// The cache mirrors only the index levels, and stores local pointers rather
/// than addresses above level 0 -- but on the wire every level looks like this.
struct Entry {
  Key key;
  uint64_t val;
};
static_assert(sizeof(Entry) == 16);
static_assert(std::is_trivially_copyable_v<Entry>);

/// The 8-byte CAS unit, and the authoritative version pair (V4).
///
/// Bit layout, MSB first:
///
///     63..48  struct_ver   (16 bits)
///     47..16  content_ver  (32 bits)
///     15..8   flags        (8 bits)
///      7..0   slot         (8 bits)  which VecSlot is current
///
/// Two deliberate choices:
///
/// 1. Explicit shifts and masks over a single uint64_t, not a bitfield or a
///    union. Bitfield layout is implementation-defined, and this value is
///    compared and CAS'd across machines -- it has to mean the same thing on
///    both ends regardless of what the compiler would have chosen.
///
/// 2. struct_ver above content_ver, with flags and slot below both. L3 defines
///    tag order as `(struct_ver, content_ver)` lexicographic, so with this
///    layout `tag()` -- the raw value shifted down past flags and slot -- is a
///    plain unsigned integer whose natural order *is* that lexicographic
///    order. Max-tag selection across replicas (L2) becomes one integer
///    compare, with no risk of a slot or flag difference perturbing the
///    comparison.
///
/// On field widths: the interface doc §0 sketched 14 and 18 bits. Widened here
/// because 18 bits of content_ver wraps after 262144 updates to a single node,
/// and a 10-second run at RDMA rates is on the order of 10^8 cluster-wide
/// operations -- close enough to be worth removing rather than reasoning about.
/// A wrap would silently invert tag order and break L2/L3. 32 bits of
/// content_ver gives 4.3e9 updates per node; 16 bits of struct_ver gives 65536
/// splits or merges of one node, and since each split roughly halves a node's
/// occupancy that is unreachable.
struct Handle {
  uint64_t raw = 0;

  static constexpr int kSlotBits = 8;
  static constexpr int kFlagBits = 8;
  static constexpr int kContentBits = 32;
  static constexpr int kStructBits = 16;
  static_assert(kSlotBits + kFlagBits + kContentBits + kStructBits == 64);

  static constexpr int kSlotShift = 0;
  static constexpr int kFlagShift = kSlotShift + kSlotBits;
  static constexpr int kContentShift = kFlagShift + kFlagBits;
  static constexpr int kStructShift = kContentShift + kContentBits;

  static constexpr uint64_t kSlotMask = (uint64_t{1} << kSlotBits) - 1;
  static constexpr uint64_t kFlagMask = (uint64_t{1} << kFlagBits) - 1;
  static constexpr uint64_t kContentMask = (uint64_t{1} << kContentBits) - 1;
  static constexpr uint64_t kStructMask = (uint64_t{1} << kStructBits) - 1;

  /// Flag bits. `kFlagOrphan` records I4: a node nothing above points at,
  /// reachable only by walking `next`. The remote side sets it so a structural
  /// walk can check I4 without reconstructing the parent relation.
  static constexpr uint64_t kFlagOrphan = 1u << 0;

  constexpr Handle() = default;
  explicit constexpr Handle(uint64_t r) noexcept : raw(r) {}

  static constexpr Handle make(uint32_t struct_ver, uint32_t content_ver,
                               uint8_t slot, uint8_t flags = 0) noexcept {
    return Handle{((uint64_t{struct_ver} & kStructMask) << kStructShift) |
                  ((uint64_t{content_ver} & kContentMask) << kContentShift) |
                  ((uint64_t{flags} & kFlagMask) << kFlagShift) |
                  ((uint64_t{slot} & kSlotMask) << kSlotShift)};
  }

  [[nodiscard]] constexpr uint32_t structVer() const noexcept {
    return static_cast<uint32_t>((raw >> kStructShift) & kStructMask);
  }
  [[nodiscard]] constexpr uint32_t contentVer() const noexcept {
    return static_cast<uint32_t>((raw >> kContentShift) & kContentMask);
  }
  [[nodiscard]] constexpr uint8_t flags() const noexcept {
    return static_cast<uint8_t>((raw >> kFlagShift) & kFlagMask);
  }
  [[nodiscard]] constexpr uint8_t slot() const noexcept {
    return static_cast<uint8_t>((raw >> kSlotShift) & kSlotMask);
  }
  [[nodiscard]] constexpr bool isOrphan() const noexcept {
    return (flags() & kFlagOrphan) != 0;
  }

  /// The (struct_ver, content_ver) tag, as one integer ordered lexicographically
  /// by construction. This is what L2's max-tag quorum read compares and what
  /// L3 defines the order of.
  [[nodiscard]] constexpr uint64_t tag() const noexcept {
    return raw >> kContentShift;
  }

  /// V1: a content_ver bump means a KV was added or removed and k_min did not
  /// change, so the cache's structural picture stays valid. V3 allows only one
  /// of the two to move per operation, which is why these are separate.
  [[nodiscard]] constexpr Handle bumpContent(uint8_t new_slot) const noexcept {
    return make(structVer(), contentVer() + 1, new_slot, flags());
  }

  /// V2: a struct_ver bump means a split, a merge, or a k_min change -- exactly
  /// when the cache must refresh (C3, as amended by A8).
  [[nodiscard]] constexpr Handle bumpStruct(uint8_t new_slot,
                                            uint8_t new_flags) const noexcept {
    return make(structVer() + 1, contentVer(), new_slot, new_flags);
  }

  friend constexpr bool operator==(Handle a, Handle b) noexcept {
    return a.raw == b.raw;
  }
  friend constexpr bool operator!=(Handle a, Handle b) noexcept {
    return a.raw != b.raw;
  }
};
static_assert(sizeof(Handle) == 8, "the CAS unit must be exactly 8 bytes");
static_assert(std::is_trivially_copyable_v<Handle>);

/// One copy-on-write version of a node's mutable state.
///
/// A node holds two of these and the handle's `slot` field says which is
/// current; the other is the immediately previous version, which is what C2's
/// `old_ver*` chase reads. Two rather than one because F1 and F2 both need to
/// publish a *new* vector without destroying the one concurrent readers are
/// still reading.
///
/// TWO SLOTS BOUND THE VERSION HISTORY AT ONE STEP, and that is a real
/// constraint rather than a tuning choice. It is sufficient for C2: a Get that
/// raced a single write looks one version back and is done. It is NOT obviously
/// sufficient for A10's range queries, which fix a snapshot T and need, per
/// node, the version whose ts <= T. A node written twice since T has no such
/// version in either slot, and the range query cannot be served -- so under
/// sustained writes a long range would fail repeatedly. The doc's model avoids
/// this by allocating vectors separately and keeping arbitrarily many linked by
/// `old_ver*` until epoch GC reclaims them (invariants.md §5 E3).
///
/// `VecSlot::old_ver` is the escape hatch, reserved now and unused: an older
/// version written out of line into the writing client's own arena stripe, with
/// the inline ring kept as the fast path. That keeps point operations at one
/// RDMA per node while letting a range query pay extra hops only when it
/// actually needs history. It costs nothing to reserve the field and would cost
/// a re-layout of every deployed structure to add later, which is why it is
/// here before A10 is decided.
///
/// `next_id` and `next_k_min` live *here*, inside the versioned region, rather
/// than in the node header. This deviates from invariants.md's F2, which writes
/// `next_dn` after the CAS, and it is a deliberate improvement: putting them in
/// the CoW'd region means the single CAS that publishes a split publishes the
/// new link along with the updated entries, atomically. The doc's ordering
/// leaves a window where a reader sees the bumped struct_ver but the old
/// `next`, and would walk straight past the node the split just created. It
/// also saves the post-CAS round trip. Cost is 16 duplicated bytes per slot.
///
/// `next_k_min` is the interface doc's own §7a recommendation: it makes the
/// range check `k in [k_min, next_k_min)` answerable from a single node read,
/// which removes the one case in three that otherwise needs a further hop.
struct VecSlot {
  /// Leading bookend. V5: these must equal the handle's pair for the read to be
  /// consistent.
  uint32_t lead_struct_ver;
  uint32_t lead_content_ver;

  /// Entries in use, in `e[0 .. size-1]`, sorted ascending by key.
  uint32_t size;
  /// I4. Mirrors the handle's orphan flag; kept here too so a slot validates
  /// standalone.
  uint32_t is_orphan;

  /// The timestamp this version was committed at -- F1's and F2's WRITE(ts).
  ///
  /// This is what makes a linearizable range query possible: a range fixes a
  /// snapshot T and, per node, needs the version whose ts <= T, walking back
  /// through older versions until it finds one (interface doc §0, A10).
  ///
  /// It lives inside the bookended region, and is written *before* the CAS
  /// rather than after it as F1 and F2 have it. Both follow from the same
  /// argument as next_id below: the CAS is the linearization point, so a
  /// timestamp taken before it and published by it is exactly what commit-wait
  /// wants (take T, wait epsilon, then make visible). Writing ts after the CAS
  /// instead leaves a window where a committed version carries a stale
  /// timestamp, which is precisely the thing a snapshot read must not see.
  ///
  /// Zero on a node that has never been written by a timestamped operation.
  uint64_t ts;

  /// The previous version, when it no longer fits in this node's slot ring.
  ///
  /// NOT YET USED -- reserved, and zero everywhere today. See the note on the
  /// slot ring in NodeRecord for why it has to exist in the layout now even
  /// though A10 is deferred: adding a field later would change the record size
  /// and invalidate every already-written structure.
  uint64_t old_ver;

  /// The next node at this level, and where its range starts. 0 == end of
  /// chain, in which case next_k_min is kReservedKey (an upper bound above
  /// every real key).
  uint64_t next_id;
  Key next_k_min;

  Entry e[kNodeCapacity];

  /// Trailing bookend, reversed so a memset or a partially-landed write is less
  /// likely to produce a matching pair by accident.
  uint32_t trail_content_ver;
  uint32_t trail_struct_ver;

  /// Pads the slot to a 64B multiple so each one starts 64B-aligned (F4).
  /// Sized to the fields above -- the static_asserts below fail if it drifts,
  /// which is how adding ts and old_ver got caught.
  uint64_t _pad[1];
};

/// A remote node: stable identity plus its CoW versions.
///
/// The header is 64 bytes so that F4 applies to it -- PCIe writes to 64B-aligned
/// regions are atomic on this hardware -- and the handle sits at offset 0 so the
/// CAS target address is just the node's address.
///
/// The whole record is read in ONE RDMA. That is the reason the slots are inline
/// rather than separately allocated: with the vector out of line, a reader must
/// read the handle to learn the offset and then read the vector, which is two
/// round trips on the hot path of every single operation. Reading 704 bytes
/// instead of 8 costs bandwidth we have and saves a round trip we do not.
struct NodeRecord {
  /// THE CAS TARGET. Must stay at offset 0.
  Handle handle;

  /// Where this node's range starts.
  ///
  /// Immutable for the node's lifetime in the current design: a split leaves the
  /// existing node's k_min alone (it keeps its lower range) and gives the new
  /// node its own, and merges -- the only other thing V2 says can change a
  /// k_min -- are not implemented (A7/A9). That immutability is what makes it
  /// safe for k_min to sit outside the bookended region: there is no write for a
  /// read to tear against. If merges are ever added, this has to move into
  /// VecSlot.
  Key k_min;

  /// Which level this node lives at, 0 being the directory level and
  /// kDataLevel the payload level. Not needed by any operation -- the descent
  /// always knows what level it is on -- but it makes a structural walk over
  /// the arena self-describing, which is what the I1-I4 selftest needs.
  uint32_t level;
  uint32_t _rsv;

  uint64_t _pad[5];

  VecSlot slot[2];

  [[nodiscard]] VecSlot const &current() const noexcept {
    return slot[handle.slot() & 1u];
  }
  [[nodiscard]] VecSlot &current() noexcept { return slot[handle.slot() & 1u]; }

  /// The slot to stage the next version into: the one that is not current.
  [[nodiscard]] uint8_t freeSlot() const noexcept {
    return static_cast<uint8_t>((handle.slot() & 1u) ^ 1u);
  }

  /// The previous version, i.e. C2's one-step `old_ver*`.
  [[nodiscard]] VecSlot const &previous() const noexcept {
    return slot[freeSlot()];
  }
};

inline constexpr size_t kNodeHeaderSize = 64;

/// The record size as a uint32_t, which is what ibverbs takes for a
/// scatter-gather length. Named so the narrowing happens once, next to the
/// static_assert that proves it cannot truncate, rather than implicitly at
/// every call site. Cosmetic rather than a fix -- sizeof is a constant that
/// provably fits, so no compiler warns about it -- but it documents the bound.
inline constexpr uint32_t kNodeRecordBytes =
    static_cast<uint32_t>(sizeof(NodeRecord));
static_assert(sizeof(NodeRecord) <= 0xFFFFFFFFu,
              "a node record must fit in an ibverbs sge length");
static_assert(offsetof(NodeRecord, handle) == 0,
              "the CAS target must be at offset 0 so a node's address is its "
              "handle's address");
static_assert(offsetof(NodeRecord, slot) == kNodeHeaderSize,
              "the header must be exactly 64 bytes so F4's atomicity applies");
static_assert(sizeof(VecSlot) % 64 == 0,
              "slots must be 64B multiples so each starts 64B-aligned");
static_assert(sizeof(VecSlot) == 320);
static_assert(sizeof(NodeRecord) == 704);
static_assert(sizeof(NodeRecord) % 64 == 0);
static_assert(std::is_trivially_copyable_v<NodeRecord>);
static_assert(std::is_trivially_copyable_v<VecSlot>);

// ── Reserved node ids ───────────────────────────────────────────────────────
//
// Node ids are laid out as:
//
//     0                     null -- never allocated (see ds_remote_addr.hpp)
//     1 .. kMaxLayers       the per-level head nodes, in level order
//     kMaxLayers + 1        the initial data node
//     kFirstDynamicId ...   per-client stripes (see Layout / NodeAllocator)
//
// Fixing the heads at known ids is what makes A1 cheap: the leftmost node at
// each level has a stable address for the lifetime of the structure, so
// set_head_remote_addrs() needs no discovery step and no memstore round trip --
// only a barrier until the initialising client has written the records.
//
// These live here rather than in Layout because they are facts about node
// identity, which both the arena layout and the bootstrap need to agree on.

inline constexpr uint64_t kNullId = 0;
inline constexpr uint64_t kHeadIdBase = 1;
inline constexpr uint64_t kInitialDataId = kHeadIdBase + kMaxLayers;
inline constexpr uint64_t kFirstDynamicId = kInitialDataId + 1;

/// The head (leftmost) node at /level/, level 0 being the directory.
inline constexpr RemoteAddr headAddr(uint32_t level) {
  return RemoteAddr{kHeadIdBase + level};
}

/// The level a data (payload) node lives at.
///
/// Levels 0..layers-1 are index levels, level 0 being the directory whose
/// entries point at data nodes (interface doc §2). Data nodes are below all of
/// them and are not part of the index, so they get a sentinel rather than -1:
/// `level` is unsigned, and a structural walk wants to print it.
inline constexpr uint32_t kDataLevel = 0xFFFFFFFFu;

/// Was this read internally consistent?
///
/// V5: the slot's version pair must match the handle's. Because the whole
/// record arrives in one RDMA but the remote HCA may have been writing it
/// concurrently, a read can land with the handle from after a write and the
/// slot from before it, or with a half-written slot. Checking both bookends
/// against the handle catches both: the leading pair rules out a slot written
/// before the handle we saw, and the trailing pair rules out a slot whose write
/// had not finished landing.
///
/// A false result means "re-read", never "the cache was wrong" -- interface doc
/// §7 is explicit that a torn read triggers no cache refresh.
[[nodiscard]] inline bool slotIsConsistent(NodeRecord const &n) noexcept {
  VecSlot const &s = n.current();
  return s.lead_struct_ver == n.handle.structVer() &&
         s.lead_content_ver == n.handle.contentVer() &&
         s.trail_struct_ver == n.handle.structVer() &&
         s.trail_content_ver == n.handle.contentVer() &&
         s.size <= kNodeCapacity;
}

/// Stamp a slot's bookends to match a handle. Call this last when staging, so
/// the versions describe the entries actually present.
inline void stampSlot(VecSlot &s, Handle h) noexcept {
  s.lead_struct_ver = h.structVer();
  s.lead_content_ver = h.contentVer();
  s.trail_struct_ver = h.structVer();
  s.trail_content_ver = h.contentVer();
}

/// Index of the last entry with `key <= k`, or -1 if there is none.
///
/// Binary search: entries are kept sorted, so this is the remote-side twin of
/// the cache's vector_sfra::find_lte, and it is what turns one node read into
/// one step of the descent.
[[nodiscard]] inline int findLte(VecSlot const &s, Key k) noexcept {
  int lo = 0;
  int hi = static_cast<int>(s.size) - 1;
  int found = -1;
  while (lo <= hi) {
    int const mid = lo + (hi - lo) / 2;
    if (s.e[mid].key <= k) {
      found = mid;
      lo = mid + 1;
    } else {
      hi = mid - 1;
    }
  }
  return found;
}

/// Does this node's range cover k?
///
/// The definitive form of C4's check, answerable from a single node read
/// because next_k_min travels with the node. A false result is a k_min
/// mismatch: the address was real and the read consistent, but this is no
/// longer the right node, so the caller re-traverses and reconciles.
[[nodiscard]] inline bool covers(NodeRecord const &n, Key k) noexcept {
  return k >= n.k_min && k < n.current().next_k_min;
}

/// Initialise a record in place as an empty node. Used for bootstrap and for
/// every node a split creates, and always into a client-local staging buffer
/// that is then written out -- never against remote memory directly.
inline void initNode(NodeRecord &n, Key k_min, uint32_t level,
                     bool is_orphan) noexcept {
  // Value-initialisation, not memset: Handle has a user-provided default
  // constructor, which makes NodeRecord non-trivially-default-constructible,
  // and the dory build's -Werror=class-memaccess rejects memset over such a
  // type. Aggregate init is also simply better here -- it zeroes the padding
  // and the whole entry array without depending on sizeof.
  n = NodeRecord{};
  n.k_min = k_min;
  n.level = level;
  n.handle = Handle::make(/*struct_ver=*/0, /*content_ver=*/0, /*slot=*/0,
                          is_orphan ? Handle::kFlagOrphan : 0);
  for (auto &s : n.slot) {
    s.size = 0;
    s.is_orphan = is_orphan ? 1u : 0u;
    s.next_id = 0;
    s.next_k_min = kReservedKey;
    s.ts = 0;        // never written by a timestamped operation
    s.old_ver = 0;   // no out-of-line history
    stampSlot(s, n.handle);
  }
}

}  // namespace ds
