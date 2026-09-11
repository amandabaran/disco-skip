#pragma once

// The remote node layout: what actually lives in memory-server memory.
//
// Memory servers run no logic (invariants.md §8 / interface doc §0), so this
// header is the whole contract between clients. Every field's placement decides
// how many round trips an operation costs and which concurrent interleavings
// are observable, so the reasoning is recorded inline rather than left to be
// re-derived.
//
// Nothing here depends on dory or on the cache half, so it is exercised by
// disco-skip/tests off-cluster.
//
// ── Shape, and why ──────────────────────────────────────────────────────────
//
// A node is a 64-byte header holding stable identity plus the *offset* of its
// current vector; the vector itself lives out of line. Reads therefore cost a
// header read and then a vector read, and that is not an accident to be
// optimised away:
//
//   - Range queries need arbitrarily deep version history. A snapshot at T
//     needs, per node, the version whose ts <= T, so old versions must survive
//     until epoch GC reclaims them. Inline versions bound the history at
//     however many slots fit, which is not enough.
//   - Timestamps must be written *after* a version is visible, never published
//     with it. A timestamp fixed before visibility lets a reader with a larger
//     snapshot miss a write it should have seen, and commit-wait does not close
//     that -- it only guarantees the timestamp is past by the time the write is
//     visible, which says nothing about a reader that started earlier. So the
//     write path is inherently two steps.
//   - With the vector out of line, a right-walk along a level only needs
//     headers. Putting `next` in the vector would make every hop cost both.
//     Splits are rare (a node must fill first); traversal is not.
//
// ── Non-blocking reads by helping ───────────────────────────────────────────
//
// A writer's work does not finish at the CAS, so a reader can arrive mid
// operation. Rather than wait, it *completes* the operation -- the new vector
// carries everything needed, so it doubles as an operation descriptor. Two
// markers say what is outstanding:
//
//   handle.struct_ver != tail_struct_ver   a split is mid-propagation, so the
//                                          header's next_id/next_k_min are not
//                                          yet trustworthy. Visible from a
//                                          header-only read, which is what lets
//                                          a passing reader skip the vector for
//                                          the overwhelmingly common stable
//                                          node.
//   vec.ts == kNullTs                      the version is visible but its
//                                          timestamp is not yet fixed. Only
//                                          matters to a reader that needs this
//                                          node's range, which is reading the
//                                          vector anyway.
//
// Every completion step is a CAS, so any number of helpers race harmlessly:
// whoever loses simply observes that the step is already done.

#include <cstddef>
#include <cstdint>
#include <type_traits>

#include "ds_defs.hpp"

namespace ds {

/// One key/value pair inside a node's vector.
///
/// `val` is read three ways depending on the node's level, which is why there
/// is one entry type and not three:
///   - data node                -> the payload for `key`
///   - level-0 (directory) node -> RemoteAddr::id of the data node whose k_min
///                                 is `key`
///   - level->=1 index node     -> RemoteAddr::id of the node one level down
struct Entry {
  Key key;
  uint64_t val;
};
static_assert(sizeof(Entry) == 16);
static_assert(std::is_trivially_copyable_v<Entry>);

/// Index of a vector within the vector arena. 0 is null, so it doubles as
/// "no older version" in VecRecord::old_ver.
using VecOffset = uint32_t;
inline constexpr VecOffset kNullVec = 0;

/// The null node id. Declared up here because NodeRecord and VecRecord both
/// need it; the rest of the reserved-id map is further down, next to headAddr().
inline constexpr uint64_t kNullId = 0;

/// A timestamp of 0 means "not yet fixed" -- semantically pending. A reader
/// that needs the version resolves it by CAS rather than waiting.
inline constexpr uint64_t kNullTs = 0;

/// The 8-byte CAS unit.
///
/// Bit layout, MSB first:
///
///     63..50  struct_ver   (14 bits)
///     49..32  content_ver  (18 bits)
///     31..0   offset       (32 bits)  index of the current vector
///
/// Three deliberate choices:
///
/// 1. Explicit shifts and masks over a single uint64_t, not a bitfield or a
///    union. Bitfield layout is implementation-defined, and this value is
///    compared and CAS'd across machines -- it has to mean the same thing on
///    both ends regardless of what the compiler would have chosen.
///
/// 2. struct_ver above content_ver above offset. L3 defines tag order as
///    `(struct_ver, content_ver)` lexicographic, so with this layout `tag()` --
///    the raw value shifted past the offset -- is a plain unsigned integer
///    whose natural order *is* that order. The max-tag quorum read of L2 is one
///    integer compare, and a differing offset cannot perturb it.
///
/// 3. `content_ver` exists only here. There is no `tail_content_ver` in the
///    node, because content_ver's only job is to order two content states of
///    the same node for the quorum read. Split propagation is tracked by
///    struct_ver against its tail copy, and an unfixed timestamp by ts itself;
///    neither needs content_ver, so bookending it would be dead weight.
///
/// On widths: `14/18/32` is the interface doc's split, kept deliberately. Wrap
/// is not expected to bind at the evaluation's run length and is a paper
/// discussion rather than a mechanism. Worth recording for that discussion:
/// content_ver wraps roughly 16x sooner than struct_ver, since a node takes
/// about a vector's worth of inserts to earn one split -- so if these widths
/// are ever re-cut, the bits belong to content_ver rather than being split
/// evenly.
struct Handle {
  uint64_t raw = 0;

  static constexpr int kOffsetBits = 32;
  static constexpr int kContentBits = 18;
  static constexpr int kStructBits = 14;
  static_assert(kOffsetBits + kContentBits + kStructBits == 64);

  static constexpr int kOffsetShift = 0;
  static constexpr int kContentShift = kOffsetShift + kOffsetBits;
  static constexpr int kStructShift = kContentShift + kContentBits;

  static constexpr uint64_t kOffsetMask = (uint64_t{1} << kOffsetBits) - 1;
  static constexpr uint64_t kContentMask = (uint64_t{1} << kContentBits) - 1;
  static constexpr uint64_t kStructMask = (uint64_t{1} << kStructBits) - 1;

  static constexpr uint32_t kMaxStructVer = static_cast<uint32_t>(kStructMask);
  static constexpr uint32_t kMaxContentVer = static_cast<uint32_t>(kContentMask);

  constexpr Handle() = default;
  explicit constexpr Handle(uint64_t r) noexcept : raw(r) {}

  static constexpr Handle make(uint32_t struct_ver, uint32_t content_ver,
                               VecOffset offset) noexcept {
    return Handle{((uint64_t{struct_ver} & kStructMask) << kStructShift) |
                  ((uint64_t{content_ver} & kContentMask) << kContentShift) |
                  ((uint64_t{offset} & kOffsetMask) << kOffsetShift)};
  }

  [[nodiscard]] constexpr uint32_t structVer() const noexcept {
    return static_cast<uint32_t>((raw >> kStructShift) & kStructMask);
  }
  [[nodiscard]] constexpr uint32_t contentVer() const noexcept {
    return static_cast<uint32_t>((raw >> kContentShift) & kContentMask);
  }
  [[nodiscard]] constexpr VecOffset offset() const noexcept {
    return static_cast<VecOffset>((raw >> kOffsetShift) & kOffsetMask);
  }

  /// The (struct_ver, content_ver) tag, as one integer ordered
  /// lexicographically by construction. This is what L2's max-tag quorum read
  /// compares and what L3 defines the order of.
  [[nodiscard]] constexpr uint64_t tag() const noexcept {
    return raw >> kContentShift;
  }

  /// V1: a content_ver bump means a KV was added or removed with k_min
  /// unchanged, so the cache's structural picture stays valid. The new content
  /// lives at a new offset, since versions are copy-on-write.
  [[nodiscard]] constexpr Handle withContent(VecOffset new_offset) const noexcept {
    return make(structVer(), contentVer() + 1, new_offset);
  }

  /// V2: a struct_ver bump means a split, a merge, or a k_min change -- exactly
  /// when the cache must refresh (C3, as amended by A8). It also opens the
  /// propagation window, since tail_struct_ver still holds the old value until
  /// the operation completes.
  [[nodiscard]] constexpr Handle withStruct(VecOffset new_offset) const noexcept {
    return make(structVer() + 1, contentVer(), new_offset);
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

/// A remote node: stable identity, plus the offset of its current vector.
///
/// One 64-byte cache line, so F4 applies to it, and the handle sits at offset 0
/// so the CAS target address is just the node's address.
struct NodeRecord {
  /// THE CAS TARGET. Must stay at offset 0.
  Handle handle;

  /// Where this node's range starts.
  ///
  /// Immutable for the node's lifetime in the current design: a split leaves
  /// the existing node's k_min alone (it keeps its lower range) and gives the
  /// new node its own, and merges -- the only other thing V2 lets change a
  /// k_min -- are not implemented (A7/A9). If merges are ever added, a k_min
  /// change becomes a mutation of this field and it needs bookend protection
  /// like next_id does.
  Key k_min;

  /// The next node at this level; kNullId ends the chain.
  ///
  /// Mutated in place by a split, which is why the node is bookended: the CAS
  /// on the handle and the write of this field are separate operations, so a
  /// reader can land between them.
  uint64_t next_id;

  /// Where the next node's range starts, so `k in [k_min, next_k_min)` is
  /// answerable from a single header read (interface doc §7a). kReservedKey
  /// when this node ends the chain.
  ///
  /// Only trustworthy while the node is stable -- see isStable(). A stale value
  /// here is the dangerous kind of stale, because it turns "I need another hop"
  /// into "definitively absent" and the node then answers for a range it no
  /// longer covers.
  Key next_k_min;

  /// 0 being the directory level, kDataLevel the payload level. No operation
  /// needs it -- a descent always knows its own level -- but it makes a walk
  /// over the arena self-describing, which is what the I1-I4 selftest needs.
  uint32_t level;

  /// Bookend partner for handle.struct_ver, and the in-progress marker.
  ///
  /// A split CASes the handle (bumping struct_ver) and only later CASes this to
  /// match, so while they differ the fields above are mid-propagation. That
  /// makes "is this node stable?" answerable from a header-only read, which is
  /// what keeps a passing reader from having to fetch the vector.
  ///
  /// There is deliberately no tail_content_ver: see Handle's note 3.
  uint32_t tail_struct_ver;

  uint64_t _pad[3];

  /// Is every field above settled, or is a split still propagating?
  [[nodiscard]] bool isStable() const noexcept {
    return handle.structVer() == tail_struct_ver;
  }
};

/// One version of a node's contents, allocated out of line.
///
/// Write-once, with exactly one exception. F2 writes it in full before the
/// fence, the CAS then publishes it, and a later version goes to a *new* offset
/// rather than overwriting -- so once reachable, its bytes never change and
/// there is nothing for a reader to tear against. No bookends are needed here;
/// that asymmetry with NodeRecord is the whole reason the node has them and
/// this does not.
///
/// The exception is `ts`, which is fixed after the version is visible. That is
/// a single 8-byte aligned write, atomic under F4, and its intermediate value
/// (kNullTs) is a first-class protocol state rather than a torn read.
struct VecRecord {
  /// Which version this is. Today the offset alone identifies a version --
  /// offsets are bump-allocated and never reused, so a speculative read is
  /// validated by comparing the offset it assumed against the handle's. These
  /// fields are kept because that stops being true the moment epoch GC lands:
  /// once offsets are recycled, offset equality admits an ABA and the version
  /// is what disambiguates. Cheaper to carry now than to add later.
  uint32_t struct_ver;
  uint32_t content_ver;

  /// Entries in use, in `e[0 .. size-1]`, sorted ascending by key.
  uint32_t size;

  /// I4: a vector with is_orphan set is reachable only by walking next.
  uint32_t is_orphan;

  /// When this version linearized, or kNullTs while unfixed.
  ///
  /// A range query fixes a snapshot T and needs, per node, the version with
  /// `ts <= T`, walking old_ver until it finds one. A reader that needs a
  /// pending version resolves it with a CAS from kNullTs rather than waiting,
  /// which is what makes reads non-blocking.
  uint64_t ts;

  /// The previous version of this node, or kNullVec at the end of the chain.
  /// This is C2's `old_ver*`, and it is what a snapshot read walks.
  uint64_t old_ver;

  /// Split descriptor: the new node this version's split created, and where its
  /// range starts. Both kNullId / kReservedKey outside a split.
  ///
  /// These make the vector an operation descriptor. A reader that finds the
  /// node unstable can complete the split from them, and -- since it already
  /// holds the vector -- can also just use them directly in preference to the
  /// header's copies, which makes it immune to the propagation window without
  /// depending on any ordering between the two CASes.
  uint64_t next_id;
  Key k_min_next;

  Entry e[kNodeCapacity];

  uint64_t _pad[2];

  [[nodiscard]] bool isPending() const noexcept { return ts == kNullTs; }

  /// Does this version describe a split whose propagation may be outstanding?
  [[nodiscard]] bool hasSplitDescriptor() const noexcept {
    return next_id != kNullId || k_min_next != kReservedKey;
  }
};

inline constexpr size_t kNodeHeaderSize = 64;
inline constexpr uint32_t kNodeRecordBytes =
    static_cast<uint32_t>(sizeof(NodeRecord));
inline constexpr uint32_t kVecRecordBytes =
    static_cast<uint32_t>(sizeof(VecRecord));

static_assert(offsetof(NodeRecord, handle) == 0,
              "the CAS target must be at offset 0 so a node's address is its "
              "handle's address");
static_assert(sizeof(NodeRecord) == kNodeHeaderSize,
              "a node must be exactly one 64B cache line so F4 applies");
static_assert(sizeof(VecRecord) % 64 == 0,
              "vectors must be 64B multiples so each starts 64B-aligned");
static_assert(sizeof(VecRecord) == 320);
static_assert(std::is_trivially_copyable_v<NodeRecord>);
static_assert(std::is_trivially_copyable_v<VecRecord>);

/// The level a data (payload) node lives at.
///
/// Levels 0..layers-1 are index levels, level 0 being the directory whose
/// entries point at data nodes (interface doc §2). Data nodes sit below all of
/// them and are not part of the index, so they get a sentinel rather than -1:
/// `level` is unsigned, and a structural walk wants to print it.
inline constexpr uint32_t kDataLevel = 0xFFFFFFFFu;

// ── Reserved node ids and vector offsets ────────────────────────────────────
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
// Vector offsets mirror the node id space over the reserved range, so the
// initial vector for node N is at offset N. Beyond that the two spaces are
// independent, since a node acquires a new vector on every write.

inline constexpr uint64_t kHeadIdBase = 1;
inline constexpr uint64_t kInitialDataId = kHeadIdBase + kMaxLayers;
inline constexpr uint64_t kFirstDynamicId = kInitialDataId + 1;
inline constexpr VecOffset kFirstDynamicVec =
    static_cast<VecOffset>(kFirstDynamicId);

/// The head (leftmost) node at /level/, level 0 being the directory.
inline constexpr RemoteAddr headAddr(uint32_t level) {
  return RemoteAddr{kHeadIdBase + level};
}

/// Does /vec/ hold the version /node/'s handle currently names?
///
/// The offset comparison is the real check: offsets are unique per version, so
/// a vector fetched from the offset the handle names is that version by
/// construction. `assumed_offset` is what the caller read from, which differs
/// from the handle's only when the read was speculative -- see the offset hint
/// in ds_layout.
[[nodiscard]] inline bool vectorIsCurrent(NodeRecord const &node,
                                          VecRecord const &vec,
                                          VecOffset assumed_offset) noexcept {
  return assumed_offset == node.handle.offset() &&
         vec.struct_ver == node.handle.structVer() &&
         vec.content_ver == node.handle.contentVer();
}

/// Index of the last entry with `key <= k`, or -1 if there is none.
///
/// Binary search: entries are kept sorted, so this is the remote-side twin of
/// the cache's vector_sfra::find_lte, and it is what turns one vector read into
/// one step of the descent.
/// The search runs over a half-open unsigned range. A closed signed range needs
/// `mid - 1`, and at `-Wstrict-overflow=5` the dory toolchain rejects the
/// `lo <= hi` comparison that follows it -- the optimiser wants to rewrite
/// `X +- C1 cmp C2`, which is only valid if signed overflow cannot occur. There
/// is no unsigned equivalent of that diagnostic, and `hi = mid` cannot underflow
/// the way `hi = mid - 1` can, so this formulation sidesteps it rather than
/// suppressing it. Costs one cluster build; see the gate note in tests/Makefile.
[[nodiscard]] inline int findLte(VecRecord const &v, Key k) noexcept {
  uint32_t lo = 0;
  uint32_t hi = v.size; // candidates live in [lo, hi)
  int found = -1;
  while (lo < hi) {
    uint32_t const mid = lo + (hi - lo) / 2;
    if (v.e[mid].key <= k) {
      found = static_cast<int>(mid);
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }
  return found;
}

/// Where does this node's range end, as best the reader can tell?
///
/// The vector's copy wins whenever a split descriptor is present. A reader
/// holding the vector is then immune to the propagation window without relying
/// on any ordering between the header's two CASes -- which matters because a
/// stale header bound is the one kind of staleness that produces a wrong
/// answer rather than a wasted hop: too large a bound turns "I need another
/// hop" into "definitively absent".
[[nodiscard]] inline Key rangeEnd(NodeRecord const &node,
                                  VecRecord const &vec) noexcept {
  return vec.hasSplitDescriptor() ? vec.k_min_next : node.next_k_min;
}

/// The next node at this level, likewise preferring the vector's copy.
[[nodiscard]] inline RemoteAddr nextNode(NodeRecord const &node,
                                         VecRecord const &vec) noexcept {
  return RemoteAddr{vec.hasSplitDescriptor() ? vec.next_id : node.next_id};
}

/// Does this node's range cover k, judged from the header alone?
///
/// Only sound on a stable node; callers must check isStable() first, or use the
/// vector-aware overload.
[[nodiscard]] inline bool coversByHeader(NodeRecord const &n, Key k) noexcept {
  return k >= n.k_min && k < n.next_k_min;
}

/// Does this node's range cover k, using the vector where it is authoritative?
[[nodiscard]] inline bool covers(NodeRecord const &n, VecRecord const &v,
                                 Key k) noexcept {
  return k >= n.k_min && k < rangeEnd(n, v);
}

/// The 8-byte word holding `level` and `tail_struct_ver`.
///
/// RDMA CAS is an 8-byte operation but tail_struct_ver is 4, so closing the
/// propagation window CASes the pair as one word. `level` never changes after a
/// node is created, so carrying it through the swap disturbs nothing -- and
/// including it makes the CAS fail if the node is not the one we read, which is
/// a free extra check.
///
/// Little-endian assumed, as everywhere else that reads these bytes across the
/// wire: `level` sits at the lower offset, so it occupies the low half.
[[nodiscard]] inline uint64_t packTailWord(uint32_t level,
                                           uint32_t tail_struct_ver) noexcept {
  return static_cast<uint64_t>(level) |
         (static_cast<uint64_t>(tail_struct_ver) << 32);
}

/// Initialise a node header in place. Always into a client-local staging
/// buffer that is then written out -- never against remote memory directly.
inline void initNode(NodeRecord &n, Key k_min, uint32_t level,
                     VecOffset vec) noexcept {
  // Value-initialisation, not memset: Handle has a user-provided default
  // constructor, which makes NodeRecord non-trivially-default-constructible and
  // so -Werror=class-memaccess rejects memset over it. Aggregate init also
  // zeroes the padding without depending on sizeof.
  n = NodeRecord{};
  n.handle = Handle::make(/*struct_ver=*/0, /*content_ver=*/0, vec);
  n.k_min = k_min;
  n.next_id = kNullId;
  n.next_k_min = kReservedKey;
  n.level = level;
  n.tail_struct_ver = 0;  // matches handle.struct_ver: stable
}

/// Initialise a vector in place, empty and already timestamped.
///
/// Bootstrap writes a settled structure rather than a pending one, so `ts` is
/// non-null from the start: there is no in-flight operation for a reader to
/// help with, and leaving it null would make every reader try.
inline void initVec(VecRecord &v, bool is_orphan, uint64_t ts) noexcept {
  v = VecRecord{};
  v.struct_ver = 0;
  v.content_ver = 0;
  v.size = 0;
  v.is_orphan = is_orphan ? 1u : 0u;
  v.ts = ts;
  v.old_ver = kNullVec;
  v.next_id = kNullId;
  v.k_min_next = kReservedKey;
}

}  // namespace ds
