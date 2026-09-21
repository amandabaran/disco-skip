#pragma once

// A10: snapshot range queries.
//
// Fix a snapshot T, then walk the data level left to right from the start key,
// taking from each node the newest version with ts <= T. That is the whole
// algorithm; the subtleties are in what "the newest version with ts <= T" means
// when the structure has been splitting underneath.
//
// ── Where T comes from, and why a reader does not FAA ───────────────────────
//
// In Faa mode the counter is replicated on every server (ds_ts.hpp). A WRITER
// fetch-and-adds it; a READER only needs to observe it, so it READs the word on
// each replica and takes the maximum, C. That is the whole difference between
// the write path's cost and the read path's -- a snapshot costs one round trip
// and no mutation, so concurrent range queries do not contend with each other
// at all.
//
// The snapshot is then T = (C << kFaaClientBits) | kFaaClientMask, and that
// exact form is load-bearing:
//
//   * A write W that COMPLETED before the read began FAA'd every replica, so on
//     replica r its pre-value p_r satisfies c_r >= p_r + 1. W's stamp uses
//     max_r p_r <= C - 1, so its stamp is at most (C << 16) | 0xFFFF = T.
//     Included, as it must be.
//   * A write that BEGINS after the read has every pre-value >= c_r, so its
//     maximum is >= C and its stamp is at least (C+1) << 16 > T. Excluded, as
//     it must be.
//   * Writes concurrent with the read may fall either side, which is exactly
//     what linearizability permits.
//
// The low bits being all-ones is what admits every client index at counter
// value C; masking them to zero instead would silently drop writes by client
// id, which is a wrong answer rather than a stale one.
//
// In Clock mode T is just a clock read, and the guarantee weakens to
// "linearizable within epsilon" -- measured at p99 ~56 us on this testbed
// (clock-measurements.md §8), which is why Faa is the mode to run ranges in.
//
// ── Reading as of T while the structure moves ──────────────────────────────
//
// The walk follows the CURRENT chain, but reads each node's version AS OF T.
// Those are two different times, and mixing them is only sound because of two
// structural facts:
//
//   1. A NODE'S k_min NEVER CHANGES. A split at key s on node N creates a NEW
//      node M with k_min = s; N keeps its own k_min. So a node's left edge is
//      immutable, which is what lets the walk terminate on the current k_min
//      (below) and what keeps as-of-T versions from overlapping.
//
//   2. NOTHING IS EVER REMOVED. There is no delete and no merge (invariants.md
//      A7/A9), so every node that existed at T still exists now. The nodes
//      present at T are a subset of the nodes the walk visits.
//
// From those: the as-of-T versions of the nodes that existed at T partition the
// key space as it was at T, so unioning them yields each key exactly once. A
// node created AFTER T has no version with ts <= T -- its oldest is newer --
// and is simply skipped; the keys it now holds were, at T, in the node it split
// from, whose as-of-T version is wider and still on the walk. No loss, no
// duplication, and no need to reason about split descriptors at all.
//
// TERMINATION is on the current k_min: once a node's k_min exceeds the query's
// upper bound, no node further right can hold a key in range, because k_min is
// immutable and non-decreasing along the chain.
//
// ── Pending versions ───────────────────────────────────────────────────────
//
// A version with ts == kNullTs is published but unstamped, so it cannot be
// compared with T at all. The reader SETTLES it -- the same helping the
// traversal does -- rather than guessing. Walking past it to old_ver would be
// wrong: its eventual stamp may well be <= T, and skipping it would drop a
// committed write from the snapshot.

#include <cstdint>
#include <vector>

#include "ds_batch.hpp"
#include "ds_defs.hpp"
#include "ds_node.hpp"
#include "ds_traverse.hpp"
#include "ds_ts.hpp"

namespace ds {

/// An upper bound meaning "no upper bound": use the entry cap instead.
///
/// One below kReservedKey, which is the sentinel a node's k_min_next carries
/// when it has no successor -- a scan whose hi equalled it would compare
/// against a value that is deliberately not a key.
///
/// This is what a count-bounded scan wants. YCSB gives a start key and a COUNT,
/// and for an ordered structure a count bounds the entries returned, not the
/// key distance: a fixed key width would return wildly different numbers of
/// entries depending on how dense the keyspace happens to be there. dLSM's
/// iterator does the same, which is what makes the two comparable on workload E.
inline constexpr Key kUnboundedKey = kReservedKey - 1;

struct RangeStats {
  uint64_t ranges = 0;
  uint64_t nodes_walked = 0;     ///< data nodes visited on the current chain
  uint64_t versions_walked = 0;  ///< old_ver hops taken to reach as-of-T
  uint64_t nodes_skipped = 0;    ///< created after T, so absent from the snapshot
  uint64_t entries = 0;
  uint64_t helped = 0;           ///< pending versions settled on the way
  uint64_t vec_reads = 0;
  uint64_t nodes_read = 0;
  /// Ranges that stopped because they reached the caller's entry cap.
  ///
  /// NOT an anomaly. A YCSB scan is COUNT-bounded, so the upper key bound is
  /// open and reaching the cap is how a scan that found enough entries is
  /// SUPPOSED to end -- on a dense keyspace that is very nearly all of them.
  /// It reads as a warning word, so do not read it as one: what it means is
  /// "there were more entries in range than were asked for".
  uint64_t capped = 0;
  uint64_t failures = 0;

  // ── WHY A FAILURE COUNT WITHOUT A REASON IS NOT ENOUGH ───────────────────
  //
  // `failures` alone says a range gave up and nothing else, and the walk has
  // eight distinct done(false) exits. On the hot scan workload a handful of
  // ranges failed with no snapshot violations and no refused FAA round -- so
  // the cause was one of the bounded-retry exits and there was no way to say
  // which without guessing. Same principle the
  // violation check is here for: finding none is a result, not looking is a
  // gap. These sum to `failures` by construction.
  //
  /// resolveHeaders never found a majority-supported handle for one node
  /// within kMaxSettleAttempts. A LIVENESS limit under contention, not a
  /// correctness fault: the range holds no locks and has published nothing,
  /// so the caller may simply retry.
  uint64_t fail_no_majority = 0;
  /// A node stayed pending or mid-split for kMaxSettleAttempts. Same shape:
  /// the helping protocol could not settle it fast enough against the writers
  /// hitting it.
  uint64_t fail_settle = 0;
  /// The old_ver chain ran past kMaxVersionHops. Either it is cyclic or a
  /// writer is producing versions faster than the walk consumes them.
  uint64_t fail_hops = 0;
  /// The operation re-descended kMaxRangeDescents times without finishing.
  /// Distinct from fail_hops: each individual walk was within its cap, and it
  /// is the OUTER loop that did not terminate. Non-zero here means a range
  /// livelocked and was abandoned rather than crashing the process.
  uint64_t fail_descents = 0;
  /// A vector or index read did not resolve to a quorum.
  uint64_t fail_read = 0;
  /// The traversal could not route to `lo`, and not because nothing is there
  /// (a Miss is a legitimate empty answer, not a failure).
  uint64_t fail_traverse = 0;
  /// TsMode::RangeTs only: the claiming FAA fell short of a majority, so the
  /// cut would carry no ordering. Distinct from the FAA rounds the backend
  /// refuses, because this is the RANGE giving up rather than a write.
  uint64_t fail_snapshot = 0;

  /// A10's violation check, always on.
  ///
  /// cache-remote-interface.md A10 asks that "the range-query path should be
  /// checked for violations during the evaluation. Finding none is a result;
  /// not looking is a gap." This is that check, and it is left enabled in
  /// release builds because a check that only runs under a debug flag is not
  /// looking during the evaluation.
  ///
  /// The invariant: every entry returned comes from a version that is stamped
  /// (ts != kNullTs) and no newer than the snapshot. One comparison per NODE,
  /// not per entry, so the cost is nil against the round trip that fetched it.
  ///
  /// A non-zero count means the walk selected a version it should not have --
  /// the snapshot returned a state that never existed. It is reported, and the
  /// range is failed rather than returned, because a silently wrong range is
  /// the exact failure mode A10 exists to rule out.
  uint64_t snapshot_violations = 0;

  /// A10 batched walk (ds_range_future.hpp). Zero on the serial path.
  uint64_t batches = 0;          ///< index-driven fetches of up to K nodes
  uint64_t batch_fallbacks = 0;  ///< a contended index or node sent us serial
  uint64_t batch_misses = 0;     ///< node needed settling or an old_ver walk
  /// Ranges that gave up on batching entirely and finished serially: either no
  /// index level existed to take a chain from, or the walk ran off the end
  /// of the index with nodes still to come. Distinct from batch_fallbacks,
  /// which is a CONTENDED read -- this one is structural, and without it a run
  /// that batched almost nothing still reported "0 fallbacks".
  uint64_t batch_abandoned = 0;
  /// Nodes reached by the next_id chain that the INDEX DOES NOT NAME. Capacity
  /// splits produce parentless nodes -- 2535 against 16783 height-driven splits
  /// on a measured workload-E run -- so a walk taking its node list from the
  /// index alone would drop roughly one node in eight. This counter is the
  /// evidence that the chain check is doing something.
  uint64_t orphans_walked = 0;

  // ── The cache-sourced chain (--cache-walk) ────────────────────────────
  //
  // Same batched fetch as above, but the addresses come from the LOCAL cache
  // instead of a remote index node. That removes the last round trip from the
  // dependency chain: the index-sourced version still had to read the index
  // node before it knew what to fetch, and it also could not see
  // capacity-split orphans, which the cache's directories do hold.
  uint64_t cache_chains = 0;   ///< ranges that got a chain from the cache
  uint64_t cache_addrs = 0;       ///< addresses it supplied, in total
  uint64_t cache_misses = 0;      ///< lookups that returned nothing (traversed)
  /// A cache address whose fetched node did NOT have the k_min the cache
  /// claimed. The structure moved under us, so the chain is abandoned and
  /// the rest of the range walks the next_id chain -- which cannot skip a node.
  /// A wrong answer here would be silent, so it is counted, not assumed absent.
  uint64_t cache_stale = 0;
};

struct RangeResult {
  bool resolved = false;
  uint64_t snapshot = kNullTs;
  bool capped = false;   ///< stopped at the entry cap; see RangeStats::capped
  /// The run was started with --ts none, so no version in the arena was ever
  /// stamped and no snapshot exists to answer at. Distinguished from an
  /// ordinary failure because it is a CONFIGURATION error, not a race: no
  /// number of retries will help, and the caller should be told to pick a
  /// timestamp mode rather than to try again.
  bool gave_up_no_timestamps = false;
};

namespace detail {
/// A node's old_ver chain is bounded by how many times it has been written, so
/// this is a livelock guard rather than a limit on history: reaching it means
/// the chain is cyclic or a writer is producing versions faster than we walk
/// them, and both are better retried than spun on.
inline constexpr uint32_t kMaxVersionHops = 1u << 16;

/// How many times one range operation may RE-DESCEND before giving up.
///
/// kMaxVersionHops bounds a SINGLE old_ver walk, because hops_ is reset at the
/// head of each one. That makes it useless against an outer loop that keeps
/// returning to the reset: the counter never accumulates. A 32-client e10 cell
/// died exactly there -- 4097 future steps at hops=31, so roughly a hundred
/// descents of ~31 hops each, none of them individually near the cap.
///
/// The only backstop was the generic kMaxFutureSteps guard, which THROWS. An
/// uncaught throw kills every client thread in the process and leaves the other
/// nodes waiting at the barrier forever, so one stuck range took down a whole
/// 32-client cell. A bounded descent count turns that into an ordinary
/// unresolved range: the operation fails, the client prints DID NOT RESOLVE,
/// and the harness marks the cell UNRESOLVED-OPS instead of CRASH.
///
/// 64 is far above anything legitimate -- a count-bounded scan of <=10 keys
/// re-descends only when a writer splits a node under it -- and far below the
/// ~100 descents the step guard allowed, so this fires first and cleanly.
inline constexpr uint32_t kMaxRangeDescents = 64;
}  // namespace detail

/// The snapshot to read at, for /mode/. kNullTs means NO SNAPSHOT COULD BE
/// TAKEN, which callers must treat as a failure rather than as an empty answer.
///
///   Clock/Tsc  read the clock.
///
///   Faa        READ the replicated counter, never FAA it -- writes are what
///              advance it, and a range that advanced it would burn values and
///              push concurrent writes out of its own cut. Admit every client
///              index at the value observed.
///
///   RangeTs    FETCH-AND-ADD it, because here the range is the only thing
///              that advances it at all, and then WRITE BACK before returning.
///
/// WHY RangeTs CANNOT JUST READ, which is the whole asymmetry. In Faa mode the
/// counter is pushed forward by every write, so reading it is enough to sit
/// above everything already stamped. In RangeTs nothing advances it unless a
/// range does, so two ranges that merely read would take the SAME cut -- and
/// every write between them would be stamped at a value inside that cut, so
/// the second range would return writes the first had already excluded at the
/// same T. Advancing it is what separates one range's cut from the next's.
///
/// AND WHY THE WRITE-BACK IS NOT OPTIONAL. The maximum can come from replica A
/// while a later writer's read quorum excludes A; the intersecting replica sits
/// at v_B + 1, which can still be <= this maximum, so that writer would stamp
/// BELOW a snapshot that has already been returned. Raising every replica in
/// the quorum to the maximum first is what makes any later quorum observe it --
/// the same faaCatchUpAddends() write-back the Faa write path performs, moved
/// to the range path. It is skipped entirely when the replicas agreed, which is
/// the failure-free case.
template <class Ops>
[[nodiscard]] uint64_t takeSnapshot(Ops &ops) {
  // TsMode::None has no snapshot to take; Ranger::range refuses before
  // reaching here, and this returns kNullTs so a caller that somehow did
  // cannot mistake a stale clock reading for a valid T.
  if (!tsStamps(ops.tsMode())) return kNullTs;
  if (ops.tsMode() == TsMode::RangeTs) {
    Batch claim;
    claim.faaTs();
    BatchResult const r = ops.submit(claim);
    // kNullTs from a claiming round means it fell short of a majority, so the
    // value carries no ordering at all -- exactly the case a write refuses to
    // stamp with. A range cannot leave itself pending, so it gives up.
    if (!r.submitted || r.ts == kNullTs) return kNullTs;
    if (ops.tsNeedsWriteBack()) {
      Batch back;
      back.faaTsCatchUp();
      if (!ops.submit(back).submitted) return kNullTs;
    }
    return snapshotFromClaim(r.ts);
  }
  if (ops.tsMode() != TsMode::Faa) return ops.now();
  return (ops.readTsCounter() << kFaaClientBits) | kFaaClientMask;
}

/// A10's invariant: a version handed to a caller must be stamped and no newer
/// than the snapshot it was requested at.
///
/// A free function, not a member, so the blocking and resumable ranges apply
/// literally the same test. A checker that differed between the two paths would
/// leave exactly the gap it exists to close -- and those two paths drifting is
/// what produced the unbounded-retry bug in ds_put_future.hpp.
[[nodiscard]] inline bool versionIsWithin(VecRecord const &v,
                                          uint64_t snapshot) noexcept {
  return v.ts != kNullTs && v.ts <= snapshot;
}

/// Blocking snapshot range.
///
/// The oracle the resumable RangeOperation is differentially tested against,
/// and the simpler place to read the algorithm.
template <class Ops>
class Ranger {
 public:
  Ranger(Ops &ops, RangeStats &stats) : ops_(ops), stats_(stats) {}

  /// Collect entries with lo <= key <= hi, in key order, as of one snapshot.
  ///
  /// @param cap  stop after this many entries and report capped
  RangeResult range(Key lo, Key hi, uint32_t layers, size_t cap,
                    std::vector<Entry> &out) {
    // Same refusal as RangeOperation::start: no stamps, no snapshot, no
    // answer. The two paths are differentially tested, so they must agree.
    if (!tsStamps(ops_.tsMode())) {
      RangeResult res;
      res.gave_up_no_timestamps = true;
      ++stats_.failures;
      return res;
    }
    uint64_t const snapshot = takeSnapshot(ops_);
    // A CUT THAT COULD NOT BE TAKEN IS NOT AN EMPTY RANGE. In RangeTs mode the
    // claiming round can fall short of a majority, and kNullTs is what comes
    // back; walking at kNullTs would find no version within it and report a
    // RESOLVED, EMPTY interval -- a wrong answer that looks legitimate, which
    // is the failure mode gave_up_no_timestamps exists to avoid elsewhere.
    if (snapshot == kNullTs) {
      RangeResult res;
      ++stats_.failures;
      ++stats_.fail_snapshot;
      return res;
    }
    return rangeAt(lo, hi, layers, cap, snapshot, out);
  }

  /// The same walk at a CALLER-SUPPLIED snapshot.
  ///
  /// Exposed because snapshot semantics cannot be tested without pinning T:
  /// "a write after the snapshot is invisible" means reading at an OLD T after
  /// that write has landed, which is not expressible if T is always taken
  /// fresh. It is also what a multi-range transaction would want, should one
  /// ever exist.
  RangeResult rangeAt(Key lo, Key hi, uint32_t layers, size_t cap,
                      uint64_t snapshot, std::vector<Entry> &out) {
    RangeResult res;
    ++stats_.ranges;
    res.snapshot = snapshot;
    if (hi < lo) {                     // empty interval, not an error
      res.resolved = true;
      return res;
    }

    // Route to the data node covering lo. The traversal is as-of-now, which is
    // right: it finds where lo lives in the CURRENT chain, and the walk then
    // reads each node as of T.
    PathStep path[kMaxLayers];
    Traversal<Ops> t(ops_);
    TraversalResult const tr = t.traverse(lo, layers, path);
    stats_.nodes_read += tr.nodes_read;
    stats_.vec_reads += tr.vec_reads;
    if (!tr.ok()) {
      // Miss means nothing routes to lo, i.e. the structure is empty below it.
      if (tr.status != TraversalStatus::Miss) {
        ++stats_.failures;
        ++stats_.fail_traverse;
        return res;
      }
      res.resolved = true;
      return res;
    }

    RemoteAddr cur = tr.data_addr;
    while (!cur.isNull()) {
      NodeRecord node;
      VecRecord vec;
      if (!readSettled(cur, node, vec)) {
        ++stats_.failures;
        // readSettled returns false for BOTH "never saw a majority-supported
        // handle" and "would not settle within the budget", and does not say
        // which. Attributed to fail_settle rather than split on a guess; the
        // resumable path, which has the two exits separately, is the one to
        // read for the breakdown. Noted so the two paths' counters are not
        // taken as more comparable than they are.
        ++stats_.fail_settle;
        return res;
      }
      ++stats_.nodes_walked;

      // k_min is immutable, so this is a sound stop even though the version we
      // are about to read is older than the header we just read.
      if (node.k_min > hi) break;

      VecRecord asof;
      if (versionAsOf(vec, res.snapshot, asof)) {
        if (!versionIsWithin(asof, res.snapshot)) {
          ++stats_.snapshot_violations;
          ++stats_.failures;
          return res;                  // resolved stays false
        }
        for (uint32_t i = 0; i < asof.size; ++i) {
          Key const k = asof.keyAt(i);
          if (k < lo) continue;
          if (k > hi) break;           // entries are sorted
          if (out.size() >= cap) {
            res.capped = true;
            ++stats_.capped;
            res.resolved = true;
            return res;
          }
          out.push_back(asof.entryAt(i));
          ++stats_.entries;
        }
      } else {
        // No version at or before T: this node was created after the snapshot,
        // so at T its keys were in the node we have already read.
        ++stats_.nodes_skipped;
      }

      cur = nextNode(node, vec);
    }

    res.resolved = true;
    return res;
  }

 private:
  /// Read a node, settling it first if its version is unstamped.
  bool readSettled(RemoteAddr a, NodeRecord &node, VecRecord &vec) {
    for (int attempt = 0; attempt < detail::kMaxSettleAttempts; ++attempt) {
      if (!ops_.readNode(a, node)) return false;
      ++stats_.nodes_read;
      if (!ops_.readVec(node.handle.offset(), vec)) return false;
      ++stats_.vec_reads;
      if (!tsIsPending(vec, ops_.tsMode()) && node.isStable()) return true;
      // Unstamped or mid-split: complete it rather than guess. A pending
      // version's eventual stamp may be <= T, so skipping it would drop a
      // committed write from the snapshot.
      HelpCounters hc;
      settleNode(ops_, a, node, vec, hc);
      stats_.nodes_read += hc.nodes_read;
      stats_.vec_reads += hc.vec_reads;
      stats_.helped += hc.helped_ts + hc.helped_splits;
    }
    return false;
  }

  /// The newest version of /vec/'s chain with ts <= T.
  ///
  /// @return false when the whole chain is newer than T -- the node did not
  ///         exist at the snapshot
  bool versionAsOf(VecRecord const &current, uint64_t snapshot,
                   VecRecord &out) {
    VecRecord v = current;
    for (uint32_t hop = 0; hop < detail::kMaxVersionHops; ++hop) {
      if (v.ts != kNullTs && v.ts <= snapshot) {
        out = v;
        return true;
      }
      if (v.old_ver == kNullVec) return false;
      VecOffset const older = static_cast<VecOffset>(v.old_ver);
      if (!ops_.readVec(older, v)) return false;
      ++stats_.vec_reads;
      ++stats_.versions_walked;
    }
    return false;
  }

  Ops &ops_;
  RangeStats &stats_;
};

}  // namespace ds
