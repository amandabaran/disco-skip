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
//   Faa    RDMA fetch-and-add on a counter. No timing assumption at all, at the
//          cost of a round trip per write and -- as things stand -- of the
//          system's fault tolerance. See the note on that below; it is the
//          reason this is not the default.
//
// ── Why Faa is not the default: it costs fault tolerance ────────────────────
//
// A total order from a counter needs ONE counter, and one counter lives on one
// memory server. The rest of the structure tolerates any single replica failing
// -- that is what the 2-of-3 commit of invariants.md §4 buys -- but a write
// cannot claim a timestamp if the counter's server is down. So enabling Faa
// silently reduces the failure model from "tolerates one memory node" to
// "tolerates one memory node unless it is the counter's".
//
// THE OBVIOUS FIX DOES NOT WORK, which is worth recording so it is not
// re-proposed. "FAA all three and take the max" fails uniqueness. Two writers,
// two replicas, interleaved in opposite order on each:
//
//     W1 FAAs R0 -> pre 0      W2 FAAs R0 -> pre 1
//     W2 FAAs R1 -> pre 0      W1 FAAs R1 -> pre 1
//
// W1's pre-values are {0,1}, W2's are {1,0}; both maxima are 1, so both writers
// claim the same timestamp. It generalises to three replicas with a majority. A
// client-id tiebreak restores uniqueness -- 48 bits of counter and 16 of client
// id fit one word -- but the real-time ordering guarantee then has to be
// re-derived, and that is left as a paper discussion rather than built. See
// remote-design.md.
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
// this testbed (docs/clock-measurements.md): an uncontended FAA costs 1.89 us,
// the same as an 8-byte read, and the word sustains 2.70 Mops/s -- flat from one
// to eight client machines, and equally flat with each QP on its own cache line,
// so that ceiling is the responder NIC's atomic unit rather than contention.
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
// reordered out of the operation, and costs 20.1 ns measured.
//
//   NOT CORRECT ACROSS MACHINES, and not pretending to be. Relative TSC
//   frequency error across these 12 nodes is 14.31 ppm, which is 143 us of
//   accumulated skew over a 10 s run against ~2 us operations. At one client
//   that does not matter, because one clock orders its own writes perfectly. At
//   more than one it does, and the verifier's chain check is where it shows.
//
//   Making this mode correct is a one-line change in tscNow(): read
//   CLOCK_REALTIME through the vDSO instead, which costs 31.4 ns rather than
//   20.1 ns -- 11 ns more, 0.6% of an operation -- and is continuously
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
};

/// Parse the --ts option.
[[nodiscard]] inline bool parseTsMode(std::string const &s, TsMode &out) {
  if (s == "clock") { out = TsMode::Clock; return true; }
  if (s == "tsc")   { out = TsMode::Tsc;   return true; }
  if (s == "faa")   { out = TsMode::Faa;   return true; }
  return false;
}

[[nodiscard]] inline char const *tsModeName(TsMode m) {
  switch (m) {
    case TsMode::Clock: return "clock (disciplined CLOCK_REALTIME)";
    case TsMode::Tsc:   return "tsc (raw rdtscp, not cross-machine)";
    case TsMode::Faa:   return "faa (global counter, one point of failure)";
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
/// plus a scale and offset maintained by kernel timekeeping: 31.4 ns measured
/// against rdtscp's 20.1 ns, so 11 ns more, or 0.6% of a 2 us operation. What
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
  return mode == TsMode::Faa ? source : stampOver(source, predecessor_ts);
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
[[nodiscard]] inline constexpr uint64_t tsFromFaa(uint64_t pre_value) noexcept {
  return pre_value + kBootstrapTs + 1;
}

/// The counter's first value must clear the bootstrap's, or the first write to
/// a node ties with the vector bootstrap wrote and the chain stops being
/// STRICTLY decreasing. Checked here rather than trusted, because the two
/// constants live in different files.
static_assert(tsFromFaa(0) > kBootstrapTs,
              "the first FAA timestamp must exceed the bootstrap timestamp");
static_assert(tsFromFaa(0) != kNullTs,
              "no FAA timestamp may collide with the pending marker");

}  // namespace ds
