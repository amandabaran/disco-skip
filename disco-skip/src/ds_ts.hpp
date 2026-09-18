#pragma once

// Where a version's timestamp comes from. One seam, two implementations.
//
// There were three identical copies of `now()` -- RdmaOps, RdmaReplicaSet,
// RdmaAsyncOps -- all returning steady_clock. The clock is meant to be one
// decision, so it is one file.
//
// ── What the timestamp is for ────────────────────────────────────────────────
//
// Nothing on the point-read or write path compares timestamps. `ts` exists so a
// range query can fix a snapshot T and take, per node, the version with
// ts <= T, walking old_ver until it finds one. Point operations linearize on the
// CAS and the version tags (L1-L3), and cache-remote-interface.md §9 is explicit
// that only ranges need a clock at all.
//
// It is also asserted by the verifier: ds_verify.hpp requires the old_ver chain
// to be strictly decreasing in ts. That is the check that turns a bad clock into
// a visible failure before range queries exist -- and it becomes a CROSS-MACHINE
// clock assertion the moment two client machines write the same node.
//
// ── Three modes, and why the default is a clock ─────────────────────────────
//
// Clock is the default. Faa and Tsc exist so that its cost and its correctness
// can each be priced against an alternative, which is the same reason
// --offset-hint is a toggle: the trade is then a measurement rather than an
// argument.
//
//   Clock  CLOCK_REALTIME through the vDSO. Comparable across machines once the
//          clocks are disciplined, and the only mode that is both correct at
//          more than one client AND keeps the fault tolerance replication is
//          there to provide. THE DEFAULT.
//
//   Tsc    Raw rdtscp. The cheapest possible source, and the baseline the other
//          two are priced against. NOT correct across machines.
//
//   Faa    RDMA fetch-and-add on a counter REPLICATED ON EVERY SERVER. No
//          timing assumption at all, at the cost of a round trip per write.
//          This is what a range query needs: see below for why the replicated
//          form is unique and real-time ordered, and for the one condition
//          (FAA all replicas, not a majority) that it rests on.
//
// ── The replicated counter: why it is unique AND real-time ordered ──────────
//
// A single counter would be a single point of failure: the rest of the
// structure tolerates any one replica failing -- that is what the 2-of-3 commit
// of invariants.md §4 buys -- but a write could not claim a timestamp if the
// counter's server were down. So the counter lives on EVERY server
// (Layout::tsCounterOffset is part of serverSize), and the FAA already rides
// the publish chain to all of them, which means taking the maximum costs NO
// EXTRA ROUND TRIP over using one.
//
// UNIQUENESS needs a tiebreak, because two writers can compute the same
// maximum -- the counterexample below. (max_pre, client_idx) is unique because
// client indices are; tsFromFaa packs 48 bits of counter over 16 of client.
//
// REAL-TIME ORDER holds, on one condition: a writer must FAA ALL replicas.
// Suppose W1 completes before W2 begins. W1 incremented every replica, so when
// W2 starts each sits at least one above the value W1 saw there, hence W2's
// maximum M2 >= M1 + 1 > M1 -- strictly greater. The tiebreak therefore never
// engages between non-overlapping writers; it engages only for CONCURRENT
// ones, where any order is a valid linearization anyway.
//
// WITH ONLY A MAJORITY IT BREAKS, and this is the part that is easy to miss.
// Let W1's maximum come from replica A, and let W2's quorum exclude A. W2 does
// touch some B in Q1 ∩ Q2, whose value after W1 is v_B + 1 -- but v_B + 1 can
// still be <= M1, because M1 came from A, not B. So a later write can take a
// SMALLER timestamp than an earlier one. Quorum intersection guarantees W2 sees
// some replica W1 touched; it does not guarantee W2 sees the one that decided
// M1, and that is the whole difference.
//
// So submit() takes the maximum over every replica that ANSWERED and counts the
// runs where that was not all of them (QuorumStats::ts_partial). A partial FAA
// still yields a unique timestamp that is monotone for its own writer; what it
// loses is the cross-writer real-time guarantee. That is precisely the case
// worth counting rather than assuming away.
//
// THE TIEBREAK IS NOT OPTIONAL, which is worth recording so the naive form is
// not re-proposed. "FAA all three and take the max" alone fails uniqueness. Two writers,
// two replicas, interleaved in opposite order on each:
//
//     W1 FAAs R0 -> pre 0      W2 FAAs R0 -> pre 1
//     W2 FAAs R1 -> pre 0      W1 FAAs R1 -> pre 1
//
// W1's pre-values are {0,1}, W2's are {1,0}; both maxima are 1, so both writers
// claim the same timestamp. It generalises to three replicas with a majority.
// The client-id tiebreak is what fixes it, and the real-time argument above is
// the re-derivation this comment previously said was still owed.
//
// ── What makes stamping-after-publish safe, which is not obvious ────────────
//
// Both Clock and Faa fix a version's timestamp AFTER it is visible, which
// remote-design.md §2 argues is necessary: publishing `ts` with the data lets a
// reader holding a larger snapshot miss a write whose stamp is already below
// its own. But it opens a different hazard. Two writers can publish in order
// W1 then W2 -- W2 having CAS'd from W1's handle -- and yet stamp in the order
// W2 then W1, giving the OLDER version the HIGHER timestamp and inverting the
// old_ver chain. That is not clock skew; it would happen with a single perfect
// counter, because the gap between publishing and stamping is schedulable.
//
// It is saved by a rule already in the code: A WRITER MUST SETTLE A NODE BEFORE
// WRITING IT. W2 cannot publish over W1's unstamped version -- it settles it
// first, stamping it with a value W2's own later stamp exceeds. So
// settle-before-write is load-bearing for timestamp ordering and not only for
// the split descriptor, which nobody had written down.
//
// stampOver() below adds the belt to that brace, IN THE CLOCK MODES ONLY: a
// writer holds the version it is superseding, so it can stamp with
// max(source, predecessor + 1) and make the chain monotonic by construction at
// any skew, for free.
//
// In Faa mode that same floor is a BUG -- it can invert the counter's own global
// order and hand a range query a state that never existed. stampFor() is the
// mode-aware entry point every call site uses, and carries the worked
// counterexample. Do not reach for stampOver() directly.
//
// A HELPER is covered in neither mode -- it has not read the predecessor -- so a
// helper's stamp still relies on epsilon being smaller than the gap between two
// successive versions of one node, which is at least a write's latency. The
// verifier's chain check is what would catch a violation.
//
// FAA: an RDMA fetch-and-add on one well-known word in memory-server memory.
// Gives a genuine total order with no clock assumption whatsoever. Measured on
// this testbed (docs/clock-measurements.md): an uncontended FAA costs about the
// same as an 8-byte read, and the word's throughput ceiling is flat from one to
// eight client machines and equally flat with each QP on its own cache line, so
// that ceiling is the responder NIC's atomic unit rather than contention.
//
//   ORDERING IS THE WHOLE POINT, AND IT IS EASY TO GET BACKWARDS. The FAA must
//   happen AFTER the publishing CAS. Allocating a timestamp before the version
//   is visible lets a reader holding a larger snapshot miss a write it should
//   have seen -- the write's stamp is already below the reader's while its bytes
//   are not yet reachable. That is remote-design.md §4's superseded "publish ts
//   atomically with the data" in another guise, and it is why the counter is
//   appended to the publish chain rather than consulted before it.
//
//   The cost is one extra round trip per write. The FAA rides in the publish
//   chain (same QP, so RC ordering puts it after the CAS), but the CAS that
//   writes `ts` cannot: its value is the FAA's result, which is not known until
//   the chain completes. So F1 becomes two round trips instead of one. A lost
//   publish still burns a counter value, which is harmless -- gaps in a sequence
//   order just as well.
//
// TSC: a local counter read, no round trip. rdtscp is serialising, so it is not
// reordered out of the operation, and costs tens of nanoseconds.
//
//   NOT CORRECT ACROSS MACHINES, and not pretending to be. Relative TSC
//   frequency error across these 12 nodes is 14.31 ppm, which is 143 us of
//   accumulated skew over a 10 s run against ~2 us operations. At one client
//   that does not matter, because one clock orders its own writes perfectly. At
//   more than one it does, and the verifier's chain check is where it shows.
//
//   Making this mode correct is a one-line change in tscNow(): read
//   CLOCK_REALTIME through the vDSO instead, which costs a few nanoseconds
//   more than a raw rdtscp -- negligible against an operation -- and is
//   continuously
//   rate-corrected by the kernel once ptp4l and phc2sys are running. The
//   clocksource on these nodes is already `tsc`, so that read IS an rdtsc plus
//   a kernel-maintained scale and offset. Left as raw rdtscp because this mode's
//   job is to be the cheapest possible baseline.

#include <cstdint>
#include <ctime>
#include <string>
#include <x86intrin.h>

#include "ds_defs.hpp"
#include "ds_node.hpp"

namespace ds {

/// Which timestamp source a run uses.
enum class TsMode : uint8_t {
  Clock,  ///< disciplined CLOCK_REALTIME via the vDSO -- the default
  Tsc,    ///< raw rdtscp: cheapest, and not comparable across machines
  Faa,    ///< the global counter: no timing assumption, one point of failure
  /// NO TIMESTAMPS AT ALL. Point operations do not need one -- they linearize
  /// on the publishing CAS and the version tags (L1-L3, and
  /// cache-remote-interface.md §9 is explicit that only ranges need a
  /// timestamp) -- so a run with no scans is paying for a capability it never
  /// uses. YCSB A, B, C and D contain no scans whatsoever.
  ///
  /// WHAT IT SAVES, and the round trip is the large part. In Faa mode a write
  /// cannot fold its stamp into the publish chain, because the value is not
  /// known until the FAA returns; so every write is publish-then-stamp, two
  /// submissions. With no timestamp a write is writeVec + casHandle in ONE
  /// chain: one round trip instead of two, and one atomic per server instead
  /// of three (casHandle, faaTs, casTs). On this hardware atomics measure
  /// 2.705 Mpps against 7.859 for reads, and a CAS stream drags concurrent
  /// reads down to 2.43, so those two atomics are not a rounding cost.
  ///
  /// THE CATCH, AND WHY THIS IS A MODE RATHER THAN AN ABSENT FIELD.
  /// `ts == kNullTs` already means "published but not yet stamped -- go help
  /// it", and isPending() drives the helping path in every state machine. If
  /// writes simply stopped stamping, every version would look pending forever
  /// and every reader would try to help one, turning a saving into unbounded
  /// helper traffic. So readers have to KNOW the mode: see tsIsPending().
  ///
  /// IT IS A PROPERTY OF THE RUN, NOT OF AN OPERATION. A range arriving in a
  /// run that never stamped cannot reconstruct an order after the fact, so
  /// this cannot be "skip the stamp unless somebody scans". A range in this
  /// mode is refused outright rather than answered wrongly.
  None,
  /// THE COUNTER IS ADVANCED BY RANGES, AND ONLY BY RANGES. A write READS it.
  ///
  /// Faa has it the other way round: every write fetch-and-adds, every range
  /// reads. That puts an atomic on the path taken by 50% of workload A's
  /// operations in order to serve a capability workload A never uses, and it
  /// makes the counter diverge under partial writes (see the write-back
  /// derivation above). Inverting it puts the atomic on the rare path.
  ///
  /// ── THE INVARIANT THAT MAKES IT CORRECT ──────────────────────────────
  ///
  /// The moment a range advances the counter from C to C+1, EVERY write that
  /// will stamp at or below f(C) must already be VISIBLE.
  ///
  /// That is what lets a range at T = f(C) trust its own result: any writer
  /// holding C published before the range's FAA, so the range must encounter
  /// that version, find it pending, and help it -- and a helped version is
  /// accounted for rather than missed.
  ///
  /// ── WHICH FORCES THE ORDER, AND IT IS NOT NEGOTIABLE ─────────────────
  ///
  ///     1. writeVec + casHandle        publish, one chain
  ///     2. read the counter            legal ONLY now
  ///     3. casTs(off, kNullTs, f(C))   stamp
  ///
  /// Reading the counter BEFORE publishing breaks the invariant, and the
  /// counterexample is short. Counter C. Writer W reads C at locate time and
  /// has not published. Range R FAAs, gets C, T = f(C), walks W's node, sees
  /// only the old version, and COMPLETES. W then publishes and stamps f(C).
  /// Now ts_W <= T, so W is inside R's snapshot -- but R already returned
  /// without it, and a second range at the same T would include it. Two
  /// ranges at one snapshot disagreeing is not a linearizability quibble; it
  /// breaks snapshot semantics outright, and concurrency does not excuse it
  /// because the timestamp is the externally visible ordering claim.
  ///
  /// So this mode costs the same TWO submissions per write as Faa: step 2's
  /// value arrives with step 1's completion, and step 3 needs it. What it
  /// saves is one ATOMIC of three per write per server -- casHandle and casTs
  /// remain. Measured reference points: Faa -> None removed two atomics AND
  /// the round trip for +17-21%, while removing every atomic at a fixed round
  /// trip count was +6%. So expect single digits, not the full 17-21%: the
  /// ordering rule above is exactly what keeps the round trip.
  ///
  /// ── THE WRITE-BACK DOES NOT GO AWAY, IT MOVES ────────────────────────
  ///
  /// The "with only a majority it breaks" derivation above mirrors precisely.
  /// A range's maximum can come from replica A; a later writer's read quorum
  /// can exclude A; the intersecting replica's value after the range is
  /// v_B + 1, which can still be <= the range's maximum. The writer then
  /// stamps BELOW a snapshot that has already completed. So a range must raise
  /// every replica in its quorum to M + 1 before it returns -- the same
  /// faaCatchUpAddends() write-back, applied on the range path instead of the
  /// write path.
  ///
  /// ── ONE WINDOW IS STILL OPEN, AND IT IS SHARED WITH Faa ──────────────
  ///
  /// A WRITER THAT LOSES ITS STAMPING CAS TO A HELPER RETURNS ON THE HELPER'S
  /// TIMESTAMP WITHOUT WAITING FOR THE HELPER'S WRITE-BACK. PutOperation's
  /// onStamp() does not even resolve its own stamp batch -- there is nothing
  /// to do about a lost casTs, since the version is stamped either way -- so
  /// the write completes as soon as its own chain drains, while the helper's
  /// catch-up is still in flight on other queue pairs.
  ///
  /// Worked through with real values. Three replicas, RangeTs.
  ///
  ///   counters (17, 16, 16). Reachable: a range's FAA landed on r0, answered
  ///   below a majority, and that range gave up -- but the increment stuck.
  ///
  ///   writer W publishes v_new at node 7, reads {r1, r2} -> max 16, so W
  ///   intends band 17, and owes no write-back (r1 and r2 agree).
  ///
  ///   helper H reads {r0, r2} -> max 17, so H stamps band 18 and DOES owe a
  ///   write-back: addend 1 for r2, 0 for r0, nothing for r1 (never read).
  ///   H's chain per replica is casTs then catch-up.
  ///
  ///   H's casTs reaches r1. W's casTs there finds band 18 instead of kNullTs
  ///   and fails; W drains its own chain and RETURNS. ts(W) = band 18.
  ///
  ///   H's catch-up on r2 has not landed. Counters are still (17, 16, 16), so
  ///   band 18 is supported by r0 alone. A range R3 now FAAs {r1, r2}: pre
  ///   (16, 16), P = 16, cut = band 17 -- and EXCLUDES W, which completed
  ///   before R3 began.
  ///
  /// Three coincidences deep and every one of them failure-related, but real,
  /// and NOT introduced by this mode: substitute "H's FAA" for "H's read" and
  /// the same sequence runs in Faa mode, where a helper likewise claims, wins
  /// the casTs, and owes a write-back the losing writer does not await.
  ///
  /// THE FIX, NOT YET IMPLEMENTED. A writer whose casTs loses must not return
  /// until the band of the WINNING stamp is on a majority of counters. That
  /// needs the losing casTs's swapback surfaced -- BatchResult carries the
  /// publishing CAS's result, not casTs's -- and then one conditional extra
  /// round: read the counter, and write back if the maximum is below the
  /// winner's band minus one. Rare path, one round trip, no change to the
  /// common case.
  ///
  /// ── WHAT IT BUYS BEYOND SPEED ────────────────────────────────────────
  ///
  /// No FAA on the write path means no counter divergence from partial
  /// writes, so QuorumStats::ts_partial stops being a caveat on every
  /// write-heavy run -- it becomes a caveat on scan-heavy ones instead.
  ///
  /// ONE OPTIMISATION IS AVAILABLE AND IS NOT TAKEN. A helper could fold its
  /// counter read into the fan-out that read the header, because the version
  /// it is about to stamp is BY DEFINITION already visible -- it found it --
  /// so the ordering rule that forces the write path's second submission does
  /// not bind here. Helping would then cost one submission where Faa forces
  /// two. It is not implemented: the helping sites are shared with Faa mode
  /// through tsClaimOn(), and forking them is how three of them came to stamp
  /// clockNow() into a counter-valued field. Helping is also rare enough that
  /// the saving is unmeasurable on the workloads we run. Recorded so the
  /// possibility is not rediscovered as a bug report.
  RangeTs,
};

/// Parse the --ts option.
[[nodiscard]] inline bool parseTsMode(std::string const &s, TsMode &out) {
  if (s == "clock") { out = TsMode::Clock; return true; }
  if (s == "tsc")   { out = TsMode::Tsc;   return true; }
  if (s == "faa")   { out = TsMode::Faa;   return true; }
  if (s == "none")  { out = TsMode::None;  return true; }
  if (s == "rangets") { out = TsMode::RangeTs; return true; }
  return false;
}

/// Does this mode stamp versions at all?
[[nodiscard]] inline constexpr bool tsStamps(TsMode m) noexcept {
  return m != TsMode::None;
}

/// Is /v/ a version somebody still owes a timestamp to?
///
/// THE MODE IS PART OF THE QUESTION, which is why this is not a method on
/// VecRecord. `ts == kNullTs` means "published, unstamped, help it" in every
/// mode that stamps -- and means nothing at all in TsMode::None, where no
/// version is ever stamped. Asking the record alone would make every version
/// look pending forever the moment stamping was switched off, and every reader
/// would queue a helping batch for a write that was never going to be stamped.
template <class Vec>
[[nodiscard]] inline bool tsIsPending(Vec const &v, TsMode m) noexcept {
  return tsStamps(m) && v.isPending();
}

/// Does the stamp come from the REMOTE counter in this mode?
///
/// THIS IS THE PREDICATE EVERY CALL SITE ACTUALLY WANTS, and it used to be
/// spelled `tsMode() == TsMode::Faa` in eleven places. What those eleven
/// branches are really asking is not "which verb" but "is my stamp's value
/// still unknown when I publish" -- because if it is, the write cannot fold
/// casTs into the publish chain and must split into two submissions. Faa and
/// RangeTs both answer yes, for the same reason and with the same shape; the
/// clock modes answer no because ops_.now() is available before the post.
///
/// Adding RangeTs as a second `== TsMode::Faa` at each site is exactly the
/// mistake that made three helping sites stamp clockNow() into a counter-valued
/// field: the sites that matter are easy to find and the ones that matter most
/// are easy to miss. One predicate, so a third counter mode cannot reintroduce
/// it.
[[nodiscard]] inline constexpr bool tsIsRemote(TsMode m) noexcept {
  return m == TsMode::Faa || m == TsMode::RangeTs;
}

/// Claim a timestamp on /b/ with whichever verb /m/ calls for.
///
/// Templated on the batch type purely to keep ds_ts.hpp free of ds_batch.hpp:
/// the timestamp rules are the lower layer and should not depend on the batch
/// representation.
///
/// Faa ADVANCES the counter -- the writer is taking a slot, so nobody else may
/// have it. RangeTs only OBSERVES it: in that mode the counter moves when a
/// RANGE fetch-and-adds it, and a write that advanced it would push every
/// subsequent write out of the snapshot of a range that had not started.
template <class B>
inline void tsClaimOn(B &b, TsMode m) {
  if (m == TsMode::Faa) {
    b.faaTs();
  } else {
    b.readTs();
  }
}

[[nodiscard]] inline char const *tsModeName(TsMode m) {
  switch (m) {
    case TsMode::Clock: return "clock (disciplined CLOCK_REALTIME)";
    case TsMode::Tsc:   return "tsc (raw rdtscp, not cross-machine)";
    case TsMode::Faa:   return "faa (global counter, one point of failure)";
    case TsMode::None:  return "none (NO SNAPSHOT RANGES -- point operations only)";
    case TsMode::RangeTs:
      return "rangets (ranges advance the counter; writes read it)";
  }
  return "?";
}

/// A local timestamp.
///
/// rdtscp rather than rdtsc: it serialises, so the read cannot be hoisted out
/// of the operation it is timing. Never returns kNullTs -- 0 is the pending
/// marker, and a timestamp of 0 would read as "not yet stamped".
[[nodiscard]] inline uint64_t tscNow() noexcept {
  unsigned aux = 0;
  uint64_t const t = __rdtscp(&aux);
  return t == kNullTs ? 1 : t;
}

/// A disciplined timestamp, in nanoseconds since the epoch.
///
/// CLOCK_REALTIME rather than CLOCK_MONOTONIC, and that is the entire point:
/// MONOTONIC's epoch is boot time, so two machines' values differ by their
/// uptime difference -- hours, not microseconds -- which is why the
/// steady_clock this replaces "ordered nothing across clients".
///
/// The clocksource on these nodes is already `tsc`, so this read IS an rdtsc
/// plus a scale and offset maintained by kernel timekeeping, costing a few
/// nanoseconds more than rdtscp -- negligible against an operation. What
/// the 11 ns buys is continuous RATE correction -- the thing a one-shot reset
/// cannot give, and without which the 14.31 ppm measured between these nodes
/// accumulates to 143 us over a 10 s run.
///
/// Correct across machines only to within epsilon, and epsilon is whatever the
/// discipline achieves: NTP today, ~1 us or better with ptp4l and phc2sys and
/// the hardware timestamping these NICs have. It must be MEASURED and reported,
/// not assumed -- see docs/clock-measurements.md.
[[nodiscard]] inline uint64_t clockNow() noexcept {
  struct timespec t;
  clock_gettime(CLOCK_REALTIME, &t);
  uint64_t const ns = static_cast<uint64_t>(t.tv_sec) * 1000000000ull +
                      static_cast<uint64_t>(t.tv_nsec);
  return ns == kNullTs ? 1 : ns;
}

/// The local timestamp for /mode/. Faa has none -- its value comes from the
/// counter and cannot be read locally, which is why it changes the shape of a
/// write rather than only its value.
[[nodiscard]] inline uint64_t localNow(TsMode mode) noexcept {
  return mode == TsMode::Tsc ? tscNow() : clockNow();
}

/// The timestamp to stamp a version with, given the version it supersedes.
///
/// max(source, predecessor + 1), so the old_ver chain is strictly decreasing BY
/// CONSTRUCTION rather than by the clock being good enough. Free: the writer has
/// already read the version it is superseding.
///
/// This is what makes a Clock-mode run safe against the 14.31 ppm of skew
/// measured between these nodes, for the writer's own stamps. A helper's stamps
/// are not covered -- it has not read the predecessor -- so they still depend on
/// epsilon being below the gap between successive versions of one node.
[[nodiscard]] inline uint64_t stampOver(uint64_t source,
                                        uint64_t predecessor_ts) noexcept {
  uint64_t const floor =
      predecessor_ts == kNullTs ? kBootstrapTs : predecessor_ts + 1;
  return source > floor ? source : floor;
}

/// The timestamp to stamp with in /mode/ -- which is stampOver() for the two
/// clock modes and the RAW COUNTER VALUE for Faa.
///
/// APPLYING THE FLOOR IN FAA MODE IS A BUG, and it was one: it can invert the
/// very global order the counter exists to provide. Worked through with real
/// values, at the bootstrap timestamp of 1:
///
///   node id 1, long history, predecessor ts 50
///     writer W_A claims counter pre-value 10 -> source 11
///     stampOver(11, 50) = 51
///   node id 2, fresh from bootstrap, predecessor ts 1
///     writer W_B claims counter pre-value 11 -> source 12
///     stampOver(12, 1)  = 12
///
/// W_A is globally EARLIER -- it took counter value 10, W_B took 11 -- and ends
/// up with the HIGHER stamp. A range query fixing T = 20 then takes W_B and
/// rejects W_A, returning a mixed state that never existed. That is a wrong
/// answer, not an extra round trip and not something a retry detects.
///
/// The floor is right for the clock modes for a reason that does NOT transfer.
/// There, the predecessor's ts is itself a clock reading, so the floor can only
/// lift a stamp by as much as the two clocks disagree: it converts an unbounded
/// structural violation (a non-monotonic chain, which breaks the snapshot walk
/// outright) into an ordering error already bounded by epsilon, which is the
/// trade Clock mode is making anyway. In Faa mode the counter is exact, so the
/// floor can only introduce error where there was none.
///
/// What keeps the Faa chain monotonic without it is settle-before-write: a
/// writer settles the version it supersedes before publishing over it, so the
/// predecessor's counter value is claimed strictly before this one's. Same
/// argument as the one in the header comment above, and the reason that rule is
/// load-bearing for timestamps and not only for split descriptors.
///
/// Found by ts_test.cc: with stampOver deliberately broken, Faa mode failed
/// too, which it should not have if the counter were really in charge.
[[nodiscard]] inline uint64_t stampFor(TsMode mode, uint64_t source,
                                       uint64_t predecessor_ts) noexcept {
  // RangeTs is counter-sourced exactly like Faa, so the floor is wrong for the
  // same reason: it can invert the global order the counter exists to provide.
  return (mode == TsMode::Faa || mode == TsMode::RangeTs)
             ? source
             : stampOver(source, predecessor_ts);
}

/// Turn an FAA's returned value into a timestamp.
///
/// RDMA fetch-and-add returns the value it found, so the counter starting at 0
/// makes the first writer's pre-value 0 -- which is kNullTs and would read as
/// unstamped. Shifting by one both avoids that and keeps the order.
///
/// No byteswap, and that was checked rather than assumed: the IB spec describes
/// atomic operands in big-endian, but mlx4 performs the read-modify-write in
/// host byte order on x86. Measured three ways -- the client's own count, the
/// server CPU's read of the word, and an RDMA READ of it -- all agreeing
/// (experiments/rdma-counter/RESULTS.md). It is a device property, not a
/// guarantee, so re-check it if the adapter generation changes.
/// Bits of a Faa timestamp reserved for the writer's client index.
///
/// The tiebreak that makes a REPLICATED counter's timestamp unique. See
/// tsFromFaa. 16 bits is 65536 clients; the remaining 48 bits of counter at the
/// counter's measured throughput ceiling is many centuries, so neither field
/// is tight.
inline constexpr unsigned kFaaClientBits = 16;
inline constexpr uint64_t kFaaClientMask = (1ull << kFaaClientBits) - 1;

[[nodiscard]] inline constexpr uint64_t tsFromFaa(uint64_t max_pre_value,
                                                  uint64_t client_idx) noexcept {
  return ((max_pre_value + 1) << kFaaClientBits) |
         (client_idx & kFaaClientMask);
}

/// The counter's first value must clear the bootstrap's, or the first write to
/// a node ties with the vector bootstrap wrote and the chain stops being
/// STRICTLY decreasing. Checked here rather than trusted, because the two
/// constants live in different files.
static_assert(tsFromFaa(0, 0) > kBootstrapTs,
              "the first FAA timestamp must exceed the bootstrap timestamp");
static_assert(tsFromFaa(0, 0) != kNullTs,
              "no FAA timestamp may collide with the pending marker");
static_assert(tsFromFaa(5, 1) != tsFromFaa(5, 2),
              "two writers computing the same counter maximum must still differ");
static_assert(tsFromFaa(5, 9) < tsFromFaa(6, 0),
              "a larger counter maximum outranks any client tiebreak");

/// The stamp for a RangeTs writer, from the counter value it READ.
///
/// SAME PACKING AS tsFromFaa, AND THAT IS THE POINT: one counter value maps to
/// one band of 2^16 stamps whichever mode produced it, so the +1 that clears
/// kBootstrapTs and the client tiebreak in the low bits are shared rather than
/// re-derived. What differs is only WHICH value is fed in -- tsFromFaa is given
/// a pre-value by the writer that incremented the counter itself, this is given
/// a value the writer merely OBSERVED.
///
/// The inclusion test a range applies, with the range at FAA pre-value P and
/// T = snapshotFromCounterFaa(P) = ((P + 1) << 16) | kFaaClientMask:
///
///   writer read V = P      ((P+1) << 16) | client <= T     INCLUDED, and it
///                          MUST be: it published before that range's FAA, so
///                          the range either saw its version or helped it.
///
///   writer read V = P + 1  ((P+2) << 16) | client  > T     EXCLUDED, and it
///                          MUST be: it read the counter only after the range
///                          had advanced past P, so it is not in the cut.
///
/// The band is what makes that boundary exact: every client index at V = P
/// lands at or below T and every client index at V = P+1 lands strictly above
/// it, so where the cut falls never depends on WHO wrote.
[[nodiscard]] inline constexpr uint64_t tsFromCounterRead(
    uint64_t read_value, uint64_t client_idx) noexcept {
  return tsFromFaa(read_value, client_idx);
}

/// The snapshot a RangeTs range reads at, from the PRE-value its FAA returned.
///
/// kFaaClientMask in the low bits admits EVERY client index at that counter
/// value, which is a correctness requirement and not a convenience. Two writes
/// ordered W1 -> W2 in real time can read the SAME counter value V -- nothing
/// advances it between them unless a range runs -- so their stamps differ only
/// in the client field, and the client field carries no order. A cut falling
/// inside the band would then include W2 and exclude W1 on a tiebreak neither
/// writer agreed to, which is a torn snapshot. Taking the whole band keeps
/// them together: either both are in or both are out.
[[nodiscard]] inline constexpr uint64_t snapshotFromCounterFaa(
    uint64_t pre_value) noexcept {
  return (((pre_value + 1) << kFaaClientBits) | kFaaClientMask);
}

static_assert(tsFromCounterRead(0, 0) > kBootstrapTs,
              "the first RangeTs stamp must clear the bootstrap timestamp");
static_assert(tsFromCounterRead(0, 0) != kNullTs,
              "no RangeTs stamp may collide with the pending marker");
// A writer that read P is INSIDE the snapshot of a range whose FAA returned P,
// whatever its client index -- the whole band, per the comment above.
static_assert(tsFromCounterRead(7, 0) <= snapshotFromCounterFaa(7) &&
                  tsFromCounterRead(7, kFaaClientMask) <=
                      snapshotFromCounterFaa(7),
              "a writer holding the range's pre-value must be INCLUDED");
// A writer that read P+1 is OUTSIDE it, whatever its client index.
static_assert(tsFromCounterRead(8, 0) > snapshotFromCounterFaa(7),
              "a writer that read past the range's FAA must be EXCLUDED");
static_assert(tsFromCounterRead(5, 1) != tsFromCounterRead(5, 2),
              "two writers reading the same value must still differ");

/// The snapshot from the TIMESTAMP a claiming round produced, rather than from
/// the raw pre-value.
///
/// A range in RangeTs mode claims its cut with exactly the round a Faa-mode
/// WRITE uses -- one FaaTs on every replica, maximum over the answers, majority
/// required -- so what comes back is already tsFromFaa(max_pre, client). Widening
/// the client field to the whole band turns that into the snapshot. Going
/// through the pre-value instead would mean plumbing BatchResult::pre_faa
/// through the replicated backend, which sets only `ts`.
[[nodiscard]] inline constexpr uint64_t snapshotFromClaim(
    uint64_t claim_ts) noexcept {
  return claim_ts | kFaaClientMask;
}

static_assert(snapshotFromClaim(tsFromFaa(7, 3)) == snapshotFromCounterFaa(7),
              "a claim's stamp and its pre-value must give the same snapshot");
static_assert(snapshotFromClaim(tsFromFaa(7, kFaaClientMask)) ==
                  snapshotFromCounterFaa(7),
              "the claiming client's index must not move the cut");


/// Per-replica addends for the counter write-back, and whether it is needed.
///
/// THE FIX FOR THE "WITH ONLY A MAJORITY IT BREAKS" CASE DERIVED ABOVE. That
/// derivation shows real-time order needs W2 to observe the replica that
/// decided W1's maximum, which quorum intersection does not provide. Raising
/// every replica in W1's quorum to M1 + 1 before W1 returns does provide it:
/// any later quorum intersects that set, so M2 >= M1 + 1.
///
/// The counter holds raw counts, and a replica that answered with pre-value p
/// sits at p + 1 after its own FAA. Target p_max + 1, so its shortfall is
/// p_max - p. A replica that did NOT answer gets 0: its value is unknown and
/// it is outside the quorum being established.
///
/// @param pre       per-replica pre-values from the claiming FAA round
/// @param answered  whether that replica replied at all
/// @param max_pre   the maximum over the replicas that answered
/// @param addends   out, one per replica
/// @return whether any replica lags, i.e. whether the write-back must be
///         posted at all. False in the failure-free case -- every replica
///         answers with the same pre-value -- so the common path adds no
///         atomic and the chain length stays uniform across replicas.
[[nodiscard]] inline bool faaCatchUpAddends(uint64_t const *pre,
                                            bool const *answered, size_t n,
                                            uint64_t max_pre,
                                            uint64_t *addends) noexcept {
  bool needed = false;
  for (size_t r = 0; r < n; ++r) {
    if (!answered[r] || pre[r] >= max_pre) {
      addends[r] = 0;
      continue;
    }
    addends[r] = max_pre - pre[r];
    needed = true;
  }
  return needed;
}

}  // namespace ds
