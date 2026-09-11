#pragma once

// F1 (plain write) and F2 (split), and the Update_Index climb a height-driven
// insert performs.
//
// Templated over Ops for the same reason the descent is: the interesting states
// here are the ones a *concurrent* writer produces, and those are constructible
// by hand in disco-skip/tests and nearly impossible to provoke deliberately on
// a cluster. This is the code remote-design.md §2 specifies, so that document is
// the reference for every ordering decision below.
//
// Beyond the read/CAS surface ds_descend.hpp lists, Ops must provide:
//
//     bool writeVec(VecOffset off, VecRecord const &vec);
//     bool writeNode(RemoteAddr a, NodeRecord const &node);
//     void fence();
//     bool casHandle(RemoteAddr a, uint64_t expected, uint64_t desired);
//     VecOffset allocVec();
//     RemoteAddr allocNode();
//
// `casHandle` is the one CAS whose result is NOT ignored. Every other CAS in
// this file and in the descent is a step somebody else may already have taken,
// so failure is as good as success. The handle CAS is the operation's
// linearization point (L1): losing it means another writer got there first and
// our staged version describes a state that never existed, so the only correct
// response is to re-read and start over.

#include <cstdint>

#include "ds_defs.hpp"
#include "ds_descend.hpp"
#include "ds_node.hpp"

namespace ds {

/// Why a write attempt ended.
enum class WriteOutcome {
  Published,  ///< the handle CAS landed; the operation has linearized
  Retry,      ///< lost a race, or the node no longer covers the key; re-descend
  Full,       ///< the key is absent and the vector has no room (capacity split)
  Exhausted,  ///< the arena is spent -- a hard error, not a retry
  Failed,     ///< a read could not be completed
};

struct WriteStats {
  uint64_t published = 0;
  uint64_t updates = 0;        ///< key was present, value replaced
  uint64_t creates = 0;        ///< key was absent, entry added
  uint64_t splits = 0;         ///< F2 completions
  uint64_t capacity_splits = 0;///< of those, driven by a full vector
  uint64_t boundary_noops = 0; ///< split_key was already the node's k_min
  uint64_t cas_lost = 0;       ///< handle CAS lost to a concurrent writer
  uint64_t retries = 0;
  uint64_t nodes_read = 0;
  uint64_t vec_reads = 0;
  uint64_t node_writes = 0;
  uint64_t vec_writes = 0;
  uint64_t helped_ts = 0;
  uint64_t helped_splits = 0;
  uint64_t exhausted = 0;
  uint64_t failures = 0;
};

struct SplitResult {
  WriteOutcome outcome = WriteOutcome::Failed;
  /// The node whose k_min is the split key. On a boundary no-op this is the
  /// node that already had that k_min, so callers can treat both alike.
  RemoteAddr created{};
};

namespace detail {

/// Bound on how many times one logical write re-reads and retries before giving
/// up and letting the caller re-descend. A lost handle CAS means real progress
/// by somebody, so this bounds our own starvation rather than guarding a
/// livelock.
inline constexpr int kMaxWriteAttempts = 8;

}  // namespace detail

template <class Ops>
class Writer {
 public:
  Writer(Ops &ops, WriteStats &stats) : ops_(ops), stats_(stats) {}

  // ── F1 ────────────────────────────────────────────────────────────────────

  /// Insert or update `k -> v` in /addr/, publishing a new version.
  ///
  /// Copy-on-write: the edit is applied to a staged copy at a fresh offset and
  /// published by one CAS on the handle, bumping content_ver and leaving
  /// struct_ver alone (V1, V3). Because struct_ver does not move, the bookends
  /// stay matched throughout and no propagation window opens -- the only
  /// outstanding step afterwards is the timestamp.
  WriteOutcome insertEntry(RemoteAddr addr, Key k, Value v) {
    for (int attempt = 0; attempt < detail::kMaxWriteAttempts; ++attempt) {
      NodeRecord node;
      VecRecord vec;
      if (!load(addr, node, vec)) return WriteOutcome::Failed;

      // The entries are about to be read and rewritten, so the version has to
      // be settled first -- and, more than that, the node has to be *stable*
      // at the handle we are about to CAS from. That is what makes it safe for
      // the staged vector to carry no split descriptor: a stable node has
      // already had any descriptor propagated into its header, so dropping it
      // from the new version loses nothing. Publishing a descriptor-free
      // version over a node whose window is still open would strand the old
      // bound in the header and hand readers a range end that is too large --
      // which remote-design.md §1 calls out as the one staleness that produces
      // a wrong answer rather than a wasted hop.
      if (!settle(addr, node, vec)) return WriteOutcome::Failed;

      VecRecord staged = vec;
      int const idx = findLte(vec, k);
      bool const present = idx >= 0 && vec.e[idx].key == k;

      if (present) {
        staged.e[idx].val = v;
      } else {
        if (vec.size >= kNodeCapacity) return WriteOutcome::Full;
        // Entries are sorted, so the new one lands just after the last key
        // below k. findLte returning -1 means k sorts first.
        uint32_t const at = static_cast<uint32_t>(idx + 1);
        for (uint32_t i = staged.size; i > at; --i) staged.e[i] = staged.e[i - 1];
        staged.e[at] = Entry{k, v};
        ++staged.size;
      }

      VecOffset const off = ops_.allocVec();
      if (off == kNullVec) {
        ++stats_.exhausted;
        return WriteOutcome::Exhausted;
      }

      staged.struct_ver = node.handle.structVer();
      staged.content_ver = node.handle.contentVer() + 1;
      staged.ts = kNullTs;                  // pending until stamped below
      staged.old_ver = node.handle.offset(); // C2's chain
      staged.next_id = kNullId;              // no split: no descriptor
      staged.k_min_next = kReservedKey;
      staged.is_orphan = vec.is_orphan;

      if (!ops_.writeVec(off, staged)) return WriteOutcome::Failed;
      ++stats_.vec_writes;

      // F3. The staged bytes must be visible at the remote HCA before the CAS
      // that publishes them, or a reader following the new handle can reach a
      // vector that has not landed.
      ops_.fence();

      if (!ops_.casHandle(addr, node.handle.raw,
                          node.handle.withContent(off).raw)) {
        // Somebody else published first, so `staged` describes a state that
        // never existed. Its offset is simply abandoned -- there is no
        // reclamation (invariants.md §5), so a lost race costs one vector of
        // arena for the run.
        ++stats_.cas_lost;
        ++stats_.retries;
        continue;
      }

      stamp(off);
      ++stats_.published;
      if (present) ++stats_.updates; else ++stats_.creates;
      return WriteOutcome::Published;
    }
    return WriteOutcome::Retry;
  }

  // ── F2 ────────────────────────────────────────────────────────────────────

  /// Split /addr/ at /split_key/: entries at or above it move to a newly
  /// created node whose k_min is /split_key/.
  ///
  /// @param orphan marks the new node I4-orphan, i.e. reachable only by walking
  ///        next. True for a capacity overflow, which produces a node with no
  ///        parent; false for a height-driven boundary, which Update_Index is
  ///        about to parent.
  /// @param seed  if non-null, an entry to place in the new node -- used when
  ///        /split_key/ is a fresh boundary that is not yet an entry. Its key
  ///        must equal /split_key/.
  SplitResult splitAt(RemoteAddr addr, Key split_key, bool orphan,
                      Entry const *seed) {
    SplitResult out;
    for (int attempt = 0; attempt < detail::kMaxWriteAttempts; ++attempt) {
      NodeRecord node;
      VecRecord vec;
      if (!load(addr, node, vec)) return fail(out);
      if (!settle(addr, node, vec)) return fail(out);

      // Already a boundary. Idempotent rather than an error: a height-driven
      // insert of a key that is some node's k_min already has the node it
      // wanted, and re-splitting would produce an empty node sharing a k_min,
      // which breaks the one-node-per-boundary correspondence the cache's
      // fidelity property rests on.
      //
      // The seed still has to be applied. Skipping it was a real bug: a
      // repeated height-driven put of the same key took this path, reported
      // success, and never wrote the value -- so the structure was right and
      // the payload was silently a version old. The node begins at the split
      // key, so the seed belongs in it as an ordinary entry.
      if (split_key <= node.k_min) {
        ++stats_.boundary_noops;
        if (seed != nullptr) {
          WriteOutcome const o =
              insertWithOverflow(addr, seed->key, seed->val, nullptr);
          if (o != WriteOutcome::Published) {
            out.outcome = o;
            return out;
          }
        }
        out.outcome = WriteOutcome::Published;
        out.created = addr;
        return out;
      }
      // Past this node's range: it split under us, so the node covering
      // split_key is further along. Re-descending is the caller's job.
      if (split_key >= rangeEnd(node, vec)) {
        ++stats_.retries;
        out.outcome = WriteOutcome::Retry;
        return out;
      }

      uint32_t keep = 0;
      while (keep < vec.size && vec.e[keep].key < split_key) ++keep;

      RemoteAddr const created = ops_.allocNode();
      VecOffset const created_vec = ops_.allocVec();
      VecOffset const new_vec = ops_.allocVec();
      if (created.isNull() || created_vec == kNullVec || new_vec == kNullVec) {
        ++stats_.exhausted;
        out.outcome = WriteOutcome::Exhausted;
        return out;
      }

      // 1. The created node: takes the upper range and inherits the successor,
      //    so the level's chain stays linked through it. Its header is complete
      //    before anything can reach it, because nothing points at it until the
      //    handle CAS publishes the descriptor below.
      NodeRecord cnode;
      initNode(cnode, split_key, node.level, created_vec);
      cnode.next_id = nextNode(node, vec).id;
      cnode.next_k_min = rangeEnd(node, vec);

      VecRecord cvec;
      initVec(cvec, orphan, kNullTs);
      for (uint32_t i = keep; i < vec.size; ++i) cvec.e[cvec.size++] = vec.e[i];
      if (seed != nullptr && !insertSorted(cvec, *seed)) return fail(out);

      // 2. The existing node's new version: keeps the lower range, and carries
      //    the split descriptor. That descriptor is what makes the vector an
      //    operation descriptor -- a reader finding the node unstable completes
      //    the split from it, and a reader merely hopping past prefers it over
      //    the header without depending on any ordering between the two header
      //    CASes.
      VecRecord nvec = vec;
      nvec.size = keep;
      for (uint32_t i = keep; i < kNodeCapacity; ++i) nvec.e[i] = Entry{};
      nvec.struct_ver = node.handle.structVer() + 1;
      nvec.content_ver = node.handle.contentVer();  // V3: only one moves
      nvec.ts = kNullTs;
      nvec.old_ver = node.handle.offset();
      nvec.next_id = created.id;
      nvec.k_min_next = split_key;
      nvec.is_orphan = vec.is_orphan;

      if (!ops_.writeVec(created_vec, cvec)) return fail(out);
      ++stats_.vec_writes;
      if (!ops_.writeNode(created, cnode)) return fail(out);
      ++stats_.node_writes;
      if (!ops_.writeVec(new_vec, nvec)) return fail(out);
      ++stats_.vec_writes;

      ops_.fence();

      // 3. Publish. struct_ver + 1 opens the propagation window: from here
      //    until the tail word matches, every reader can see that this node's
      //    header link fields are not yet trustworthy, and either routes around
      //    them via the vector or helps finish.
      if (!ops_.casHandle(addr, node.handle.raw,
                          node.handle.withStruct(new_vec).raw)) {
        ++stats_.cas_lost;
        ++stats_.retries;
        continue;
      }

      // 4. Finish. Any order among these three; the tail word must be last.
      stamp(new_vec);
      stamp(created_vec);
      ops_.casNextKMin(addr, node.next_k_min, split_key);
      ops_.casNextId(addr, node.next_id, created.id);

      // 5. Close the window. Last, unconditionally: while it is open another
      //    writer helps rather than starting its own split on this node, which
      //    is how concurrent splits serialise.
      ops_.casTailWord(addr, packTailWord(node.level, node.tail_struct_ver),
                       packTailWord(node.level, node.handle.structVer() + 1));

      ++stats_.splits;
      if (orphan) ++stats_.capacity_splits;
      out.outcome = WriteOutcome::Published;
      out.created = created;
      return out;
    }
    out.outcome = WriteOutcome::Retry;
    return out;
  }

  /// Insert `k -> v`, splitting for capacity if the node is full.
  ///
  /// The capacity split is a *separate* mechanism from a height-driven one
  /// (remote-design.md §5): it produces an orphan with no parent rather than a
  /// parented boundary, and its split key is whatever the median entry happens
  /// to be rather than a key whose height earned a boundary.
  WriteOutcome insertWithOverflow(RemoteAddr addr, Key k, Value v,
                                  RemoteAddr *orphan_out) {
    if (orphan_out != nullptr) *orphan_out = RemoteAddr{};
    for (int attempt = 0; attempt < detail::kMaxWriteAttempts; ++attempt) {
      WriteOutcome const o = insertEntry(addr, k, v);
      if (o != WriteOutcome::Full) return o;

      // Full. Split at the median so both halves keep room, then retry; the
      // retry re-reads, so it naturally lands in whichever half covers k.
      NodeRecord node;
      VecRecord vec;
      if (!load(addr, node, vec)) return WriteOutcome::Failed;
      if (!settle(addr, node, vec)) return WriteOutcome::Failed;
      if (vec.size < 2) return WriteOutcome::Failed;  // cannot split usefully

      Key const median = vec.e[vec.size / 2].key;
      SplitResult const s = splitAt(addr, median, /*orphan=*/true, nullptr);
      if (s.outcome != WriteOutcome::Published) return s.outcome;
      if (orphan_out != nullptr) *orphan_out = s.created;

      // k may belong to either half now. Route by the split key rather than
      // re-descending, since we know exactly where the boundary fell.
      if (k >= median) addr = s.created;
      ++stats_.retries;
    }
    return WriteOutcome::Retry;
  }

 private:
  Ops &ops_;
  WriteStats &stats_;

  bool load(RemoteAddr addr, NodeRecord &node, VecRecord &vec) {
    if (!ops_.readNode(addr, node)) return false;
    ++stats_.nodes_read;
    if (!ops_.readVec(node.handle.offset(), vec)) return false;
    ++stats_.vec_reads;
    return true;
  }

  bool settle(RemoteAddr addr, NodeRecord &node, VecRecord &vec) {
    HelpCounters c;
    bool const ok = settleNode(ops_, addr, node, vec, c);
    stats_.nodes_read += c.nodes_read;
    stats_.vec_reads += c.vec_reads;
    stats_.helped_ts += c.helped_ts;
    stats_.helped_splits += c.helped_splits;
    return ok;
  }

  /// Fix a version's timestamp.
  ///
  /// Done by the writer *before* it returns to its caller, not left for a
  /// helper. That is what keeps the ordering honest: "the write completed"
  /// then includes its stamp, so any reader starting afterwards sees a
  /// timestamp at or below its own snapshot. A helper stamping on the writer's
  /// behalf only happens while the writer is still in flight, and such a write
  /// is genuinely concurrent with the reader, so either order is legal.
  void stamp(VecOffset off) { ops_.casTs(off, kNullTs, ops_.now()); }

  static bool insertSorted(VecRecord &v, Entry e) {
    if (v.size >= kNodeCapacity) return false;
    uint32_t at = 0;
    while (at < v.size && v.e[at].key < e.key) ++at;
    if (at < v.size && v.e[at].key == e.key) {
      v.e[at].val = e.val;
      return true;
    }
    for (uint32_t i = v.size; i > at; --i) v.e[i] = v.e[i - 1];
    v.e[at] = e;
    ++v.size;
    return true;
  }

  SplitResult &fail(SplitResult &out) {
    ++stats_.failures;
    out.outcome = WriteOutcome::Failed;
    return out;
  }
};

}  // namespace ds
