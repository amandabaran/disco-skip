#pragma once

// F1 (plain write) and F2 (split), and the Update_Index climb a height-driven
// insert performs.
//
// Templated over Ops for the same reason the traversal is: the interesting states
// here are the ones a *concurrent* writer produces, and those are constructible
// by hand in disco-skip/tests and nearly impossible to provoke deliberately on
// a cluster. This is the code remote-design.md §2 specifies, so that document is
// the reference for every ordering decision below.
//
// Beyond the read/CAS surface ds_traverse.hpp lists, Ops must provide:
//
//     BatchResult submit(Batch const &b);
//     VecOffset allocVec();
//     RemoteAddr allocNode();
//
// WRITES GO OUT AS BATCHES, not as individual operations. Issued one at a time,
// F1 costs five round trips and F2 eleven, and at three replicas a per-replica
// loop multiplies both. A batch is a linked chain of work requests posted with
// one doorbell, so each of the sequences below is one round trip -- see
// ds_batch.hpp. F1 becomes two (a read, then one chain) and F2 three.
//
// The batch also carries the protocol's ordering requirements structurally
// rather than by convention: same-QP RC ordering is what puts the staged writes
// before the publishing CAS, and the tail word after everything else.
//
// `BatchResult::committed` reports the publishing CAS, and it is the one result
// that is NOT ignored. Every other CAS in this file and in the traversal is a step
// somebody else may already have taken, so failure is as good as success. The
// handle CAS is the operation's linearization point (L1): losing it means
// another writer got there first and our staged version describes a state that
// never existed, so the only correct response is to re-read and start over.

#include <cstdint>

#include "ds_batch.hpp"
#include "ds_defs.hpp"
#include "ds_ts.hpp"
#include "ds_traverse.hpp"
#include "ds_node.hpp"

namespace ds {

/// Why a write attempt ended.
enum class WriteOutcome {
  Published,  ///< the handle CAS landed; the operation has linearized
  Retry,      ///< lost a race, or the node no longer covers the key; re-traverse
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
  uint64_t not_covered = 0;    ///< the target node does not own the key
  uint64_t cas_lost = 0;       ///< handle CAS lost to a concurrent writer
  uint64_t retries = 0;
  uint64_t batches = 0;      ///< chained submissions, i.e. round trips
  uint64_t faa_stamps = 0;   ///< extra round trips spent stamping in Faa mode
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
/// up and letting the caller re-traverse. A lost handle CAS means real progress
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

      // Does this node actually own k?
      //
      // Nothing upstream guarantees it: the caller found this address by
      // traversing, or from a cache hint, or by routing across a split it just
      // performed, and any of those can be stale by the time we get here. Left
      // unchecked, a write to the wrong node lands a key outside the node's
      // range and breaks I1 -- silently, since a vector is still internally
      // sorted afterwards and only the verifier would ever notice. splitAt
      // already rejects an out-of-range split key; this is the same check on
      // the same reasoning, and it keeps a bad address a detected bad address
      // rather than a wrong answer.
      if (!covers(node, vec, k)) {
        ++stats_.not_covered;
        return WriteOutcome::Retry;
      }

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

      // One chain: stage the vector, publish it, and claim a timestamp.
      //
      // F3 lives on the CasHandle: the backend fences it, so the staged bytes
      // are visible at the remote HCA before the CAS that publishes them.
      //
      // How the stamp is obtained depends on the mode, and the difference is
      // the whole cost of correctness across machines (ds_ts.hpp):
      //
      //   Tsc -- the value is known locally, so the CAS that writes `ts` rides
      //   in the same chain. Safe to issue before knowing whether the publish
      //   won, because it CASes an offset this client just allocated and nobody
      //   else can reach: on a lost race it settles a version that is simply
      //   abandoned.
      //
      //   Faa -- the value comes from the counter, and the counter must not be
      //   touched until the version is visible, or a reader with a larger
      //   snapshot can miss a write whose stamp is already below it. So the FAA
      //   is appended after the CAS and the `ts` write becomes a second round
      //   trip, below.
      Batch b;
      b.writeVec(off, staged);
      b.casHandle(addr, node.handle.raw, node.handle.withContent(off).raw);
      if (ops_.tsMode() == TsMode::Faa) {
        b.faaTs();
      } else {
        // max(clock, predecessor + 1): the chain is strictly decreasing by
        // construction rather than by the clock being good enough, and it is
        // free because `vec` IS the version being superseded. See ds_ts.hpp.
        b.casTs(off, kNullTs, stampFor(ops_.tsMode(), ops_.now(), vec.ts));
      }
      BatchResult const r = ops_.submit(b);
      if (!r.submitted) {
        ++stats_.failures;
        return WriteOutcome::Failed;
      }
      ++stats_.vec_writes;
      ++stats_.batches;

      if (!r.committed) {
        // Somebody else published first, so `staged` describes a state that
        // never existed. Its offset is simply abandoned -- there is no
        // reclamation (invariants.md §5), so a lost race costs one vector of
        // arena for the run.
        ++stats_.cas_lost;
        ++stats_.retries;
        continue;
      }

      // Faa mode: the version is visible but unstamped. Fix it before
      // returning, so that "the write completed" includes its timestamp and a
      // reader starting afterwards cannot be ordered before it. A helper
      // stamping instead only happens while this writer is still in flight,
      // and such a write is genuinely concurrent with the reader.
      if (ops_.tsMode() == TsMode::Faa) {
        Batch stamp;
        // stampFor, not stampOver: in Faa mode the counter value is used RAW.
        // Flooring it at the predecessor can invert the counter's own global
        // order, which is the one thing this mode exists to provide -- see
        // stampFor() in ds_ts.hpp for the worked counterexample.
        stamp.casTs(off, kNullTs, stampFor(ops_.tsMode(), r.ts, vec.ts));
        if (!ops_.submit(stamp).submitted) return WriteOutcome::Failed;
        ++stats_.batches;
        ++stats_.faa_stamps;
      }

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
      // split_key is further along. Re-traversing is the caller's job.
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

      // 3. One chain: everything the split needs staged, then the CAS that
      //    publishes it. struct_ver + 1 opens the propagation window -- from
      //    here until the tail word matches, every reader can see that this
      //    node's header link fields are not yet trustworthy, and either routes
      //    around them via the vector or helps finish.
      //
      //    The created node and both vectors must be on the fabric before the
      //    CAS makes any of them reachable, which the backend guarantees by
      //    fencing the CasHandle.
      Batch stage;
      stage.writeVec(created_vec, cvec);
      stage.writeNode(created, cnode);
      stage.writeVec(new_vec, nvec);
      stage.casHandle(addr, node.handle.raw,
                      node.handle.withStruct(new_vec).raw);
      if (ops_.tsMode() == TsMode::Faa) stage.faaTs();
      BatchResult const r = ops_.submit(stage);
      if (!r.submitted) return fail(out);
      stats_.vec_writes += 2;
      ++stats_.node_writes;
      ++stats_.batches;

      if (!r.committed) {
        ++stats_.cas_lost;
        ++stats_.retries;
        continue;
      }

      // 4. One more chain to finish the operation. The order within it is the
      //    protocol's: the timestamps and the two header fields in any order,
      //    and the tail word LAST -- which same-QP RC ordering now enforces
      //    structurally rather than leaving it to the order of these calls.
      //
      //    Closing the window last is what makes concurrent splits serialise:
      //    while it is open, another writer helps rather than starting its own.
      // Both vectors get the SAME timestamp. They are one operation -- the
      // split published them together -- so giving them different stamps would
      // claim an order between two versions that became visible at once.
      uint64_t const ts = stampFor(
          ops_.tsMode(), ops_.tsMode() == TsMode::Faa ? r.ts : ops_.now(),
          vec.ts);
      Batch finish;
      finish.casTs(new_vec, kNullTs, ts);
      finish.casTs(created_vec, kNullTs, ts);
      finish.casNextKMin(addr, node.next_k_min, split_key);
      finish.casNextId(addr, node.next_id, created.id);
      finish.casTailWord(addr, packTailWord(node.level, node.tail_struct_ver),
                         packTailWord(node.level, node.handle.structVer() + 1));
      if (!ops_.submit(finish).submitted) return fail(out);
      ++stats_.batches;

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
      // re-traversing, since we know exactly where the boundary fell.
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

  // The timestamp is fixed by the writer inside its own batch, not left for a
  // helper. That is what keeps the ordering honest: "the write completed" then
  // includes its stamp, so any reader starting afterwards sees a timestamp at
  // or below its own snapshot. A helper stamping on the writer's behalf only
  // happens while the writer is still in flight, and such a write is genuinely
  // concurrent with the reader, so either order is legal.

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
