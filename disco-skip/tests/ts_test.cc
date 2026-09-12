// Where a version's timestamp comes from: the three modes and the stampOver
// guard (ds_ts.hpp).
//
// The property that matters is not "the clock is good". It is the one the
// verifier already checks: a node's old_ver chain must be STRICTLY DECREASING
// in ts, because a snapshot read walks back to the first version with ts <= T
// and stops there (ds_verify.hpp, and cache-remote-interface.md §9). So most of
// this drives real writes through a mode and then asks the verifier, rather
// than asserting on timestamps directly.
//
// The interesting test is checkChainSurvivesABackwardClock. stampOver()'s claim
// is that a WRITER's stamps are monotonic BY CONSTRUCTION at any skew, because
// the writer has already read the version it supersedes. That is a claim about
// adversarial clocks, so it is tested with one: a clock that runs backwards
// faster than writes arrive. Without the guard those chains invert; the test
// pins that the guard is what prevents it, by checking both ways round.

#include <cstdio>
#include <set>
#include <string>
#include <vector>

#include "ds_put.hpp"
#include "ds_ts.hpp"
#include "ds_verify.hpp"
#include "fake_ops.hpp"

static int g_failures = 0;

#define CHECK(cond, msg)                                                      \
  do {                                                                        \
    if (!(cond)) {                                                            \
      std::printf("FAIL: %s  [%s] line %d\n", (msg), #cond, __LINE__);        \
      ++g_failures;                                                           \
    }                                                                         \
  } while (0)

static uint32_t const kLayers = 4;

// ── The pure functions ───────────────────────────────────────────────────────

static void checkModeNamesRoundTrip() {
  ds::TsMode m = ds::TsMode::Faa;
  CHECK(ds::parseTsMode("clock", m) && m == ds::TsMode::Clock, "clock parses");
  CHECK(ds::parseTsMode("tsc", m) && m == ds::TsMode::Tsc, "tsc parses");
  CHECK(ds::parseTsMode("faa", m) && m == ds::TsMode::Faa, "faa parses");

  // A rejected parse must leave the mode ALONE, not half-set. main.cpp exits on
  // false, but a caller that ignored the return would otherwise silently run on
  // whatever the last successful parse left behind.
  ds::TsMode keep = ds::TsMode::Clock;
  CHECK(!ds::parseTsMode("Clock", keep), "parse is case-sensitive");
  CHECK(!ds::parseTsMode("rdtsc", keep), "unknown mode rejected");
  CHECK(!ds::parseTsMode("", keep), "empty mode rejected");
  CHECK(keep == ds::TsMode::Clock, "a failed parse does not clobber the mode");

  for (ds::TsMode x : {ds::TsMode::Clock, ds::TsMode::Tsc, ds::TsMode::Faa}) {
    CHECK(ds::tsModeName(x) != nullptr && ds::tsModeName(x)[0] != '?',
          "every mode has a name");
  }
}

static void checkNoSourceEverReturnsTheNullMarker() {
  // ts == 0 means "published but not yet stamped" and is load-bearing: the
  // stamping CAS uses kNullTs as its expected value, and the verifier reports a
  // null ts in a quiescent structure as an error. A clock that can return 0
  // would therefore look like an unstamped version.
  CHECK(ds::tscNow() != ds::kNullTs, "tscNow avoids the null marker");
  CHECK(ds::clockNow() != ds::kNullTs, "clockNow avoids the null marker");
  CHECK(ds::tsFromFaa(0, /*client=*/0) != ds::kNullTs, "the first FAA value is not null");
  // The counter is REPLICATED, so two writers can compute the same maximum --
  // that is the uniqueness failure the client index exists to fix. See the
  // derivation in ds_ts.hpp.
  CHECK(ds::tsFromFaa(5, 1) != ds::tsFromFaa(5, 2),
        "the same maximum from two writers gives different timestamps");
  CHECK(ds::tsFromFaa(5, 99) < ds::tsFromFaa(6, 0),
        "a larger maximum outranks any client tiebreak");
  // Must CLEAR the bootstrap timestamp, not merely dodge kNullTs: bootstrap
  // writes every vector at kBootstrapTs, so a first stamp equal to it ties and
  // the chain stops being strictly decreasing. ds_ts.hpp static_asserts this
  // too; asserted here as well so the reason is stated where it is tested.
  CHECK(ds::tsFromFaa(0, /*client=*/0) > ds::kBootstrapTs,
        "the first FAA timestamp clears the bootstrap timestamp");
  CHECK(ds::localNow(ds::TsMode::Tsc) != ds::kNullTs, "localNow(Tsc) not null");
  CHECK(ds::localNow(ds::TsMode::Clock) != ds::kNullTs, "localNow(Clock) not null");

  // stampOver's floor is what guarantees this for the composed value, at any
  // source. Checked with the worst source there is.
  CHECK(ds::stampOver(0, ds::kNullTs) != ds::kNullTs,
        "a zero source still stamps non-null");
}

static void checkClockModeIsNanosecondsAndCrossMachineComparable() {
  // The whole point of CLOCK_REALTIME over steady_clock: the epoch is shared,
  // so the value is comparable between machines. A steady_clock/CLOCK_MONOTONIC
  // value would be time-since-boot, i.e. small and machine-relative.
  //
  // 1.7e18 ns is 2023-11; any real clock is past it and no uptime is.
  uint64_t const t = ds::clockNow();
  CHECK(t > 1700000000000000000ull,
        "clockNow is ns since the UNIX epoch, not since boot");

  uint64_t const t2 = ds::clockNow();
  CHECK(t2 >= t, "clockNow does not go backwards within one thread");
}

static void checkStampOverAlgebra() {
  // Ahead of the predecessor: the source is used unchanged, so a good clock
  // costs nothing and the timestamps stay meaningful as times.
  CHECK(ds::stampOver(500, 100) == 500, "a source ahead of the floor is kept");

  // Behind it: pulled to predecessor + 1. Strictly greater, never equal --
  // the verifier requires the chain to be strictly decreasing, so ties fail.
  CHECK(ds::stampOver(100, 500) == 501, "a source behind the floor is lifted");
  CHECK(ds::stampOver(500, 500) == 501, "equal to the predecessor is lifted");
  CHECK(ds::stampOver(500, 499) == 500, "one ahead is already enough");

  // No predecessor (the first version of a node): the floor is 1, not 0.
  CHECK(ds::stampOver(0, ds::kNullTs) == ds::kBootstrapTs,
        "null predecessor floors at the bootstrap timestamp");
  CHECK(ds::stampOver(9, ds::kNullTs) == 9, "null predecessor keeps a live source");

  // Monotonic in both arguments, which is what makes repeated application over
  // a chain safe regardless of how the two interleave.
  for (uint64_t pred = 0; pred < 40; ++pred) {
    uint64_t prev = 0;
    for (uint64_t src = 0; src < 40; ++src) {
      uint64_t const s = ds::stampOver(src, pred);
      CHECK(s >= prev, "stampOver is non-decreasing in the source");
      CHECK(pred == ds::kNullTs || s > pred,
            "stampOver always exceeds its predecessor");
      prev = s;
    }
  }
}

static void checkTheFloorIsNotAppliedInFaaMode() {
  // stampFor is where the floor becomes mode-dependent, and getting that wrong
  // inverts the global order the counter exists to provide. The clock modes
  // take the floor; Faa takes the counter value RAW.
  CHECK(ds::stampFor(ds::TsMode::Clock, 12, 500) == 501, "Clock takes the floor");
  CHECK(ds::stampFor(ds::TsMode::Tsc, 12, 500) == 501, "Tsc takes the floor");
  CHECK(ds::stampFor(ds::TsMode::Faa, 12, 500) == 12, "Faa ignores the floor");
  CHECK(ds::stampFor(ds::TsMode::Faa, 12, ds::kNullTs) == 12,
        "Faa ignores the floor with no predecessor too");

  // The worked counterexample from ds_ts.hpp, asserted rather than only
  // described, so that reinstating the floor in Faa mode fails here.
  //
  //   node id 1, long history: its predecessor was stamped at counter max 50
  //   node id 2, fresh from bootstrap: predecessor is kBootstrapTs
  //   W_A claims counter maximum 10, W_B claims 11
  //
  // W_A is globally earlier (10 < 11) so it must get the lower stamp.
  //
  // The predecessor is written as a REAL Faa timestamp rather than a bare
  // integer. It used to be the literal 50, which stopped being a plausible
  // stamp once tsFromFaa began packing 16 bits of client index underneath the
  // counter -- a bare 50 is smaller than every stamp the encoding can produce,
  // so the floor could never bind and the test's own premise evaporated. It
  // failed loudly when that changed, which is what it is for.
  uint64_t const a_pred = ds::tsFromFaa(50, /*client=*/0);
  uint64_t const a_src = ds::tsFromFaa(10, /*client=*/0);
  uint64_t const b_src = ds::tsFromFaa(11, /*client=*/0);
  uint64_t const a = ds::stampFor(ds::TsMode::Faa, a_src, a_pred);
  uint64_t const b = ds::stampFor(ds::TsMode::Faa, b_src, ds::kBootstrapTs);
  CHECK(a < b, "the earlier FAA claim keeps the lower timestamp across nodes");

  // And show the floor really would have inverted it, so this test cannot pass
  // for an unrelated reason.
  CHECK(ds::stampOver(a_src, a_pred) > ds::stampOver(b_src, ds::kBootstrapTs),
        "the floor really would have inverted the counter's global order");
}

// ── The modes, driven through real writes ───────────────────────────────────

/// A clock the test controls, so "skew" is a parameter rather than a wait.
struct Rig {
  FakeOps ops = buildInitialArena(kLayers, 8192);
  ds::PutStats ps;
  ds::WriteStats ws;
  ds::NullPutCache cache;

  ds::PutResult put(ds::Key k, ds::Value v, uint32_t h) {
    ds::Putter<FakeOps, ds::NullPutCache> p(ops, cache, kLayers, ps, ws);
    return p.put(k, v, h);
  }
  bool verify(char const *when) {
    ds::VerifyReport const r = ds::verifyStructure(ops, kLayers);
    if (!r.ok()) {
      std::printf("  verifier failed %s:\n", when);
      for (auto const &e : r.errors) std::printf("    %s\n", e.c_str());
    }
    return r.ok();
  }
};

/// Rewrite one key many times so ONE node accumulates a long old_ver chain.
/// Height 0 on purpose: it keeps every write in the same data node, which is
/// what makes the chain long instead of wide.
static void hammerOneKey(Rig &r, ds::Key k, int times) {
  for (int i = 0; i < times; ++i) {
    ds::PutResult const res = r.put(k, static_cast<ds::Value>(1000 + i), 0);
    CHECK(res.resolved, "rewrite resolves");
  }
}

static void checkEveryModeProducesADecreasingChain() {
  for (ds::TsMode m : {ds::TsMode::Clock, ds::TsMode::Tsc, ds::TsMode::Faa}) {
    Rig r;
    r.ops.setTsMode(m);
    hammerOneKey(r, 42, 30);
    // A mixed-height workload too, so the split paths (F2, which stamps BOTH
    // vectors with one timestamp) are exercised and not just F1.
    for (int i = 0; i < 24; ++i) {
      ds::Key const k = static_cast<ds::Key>(100 + i * 7);
      CHECK(r.put(k, static_cast<ds::Value>(i), i % 4).resolved, "put resolves");
    }
    std::string const when = std::string("in mode ") + ds::tsModeName(m);
    CHECK(r.verify(when.c_str()), "chain decreasing in every mode");
  }
}

static void checkFaaConsumesOneCounterValuePerStampAndNeverRepeats() {
  Rig r;
  r.ops.setTsMode(ds::TsMode::Faa);
  CHECK(r.ops.tsCounter() == 0, "the counter starts at zero");

  hammerOneKey(r, 7, 20);
  CHECK(r.verify("after Faa rewrites"), "Faa chain verifies");

  // Every stamp came from the counter, so the counter must have advanced at
  // least once per resolved write. It can advance MORE -- a lost publish still
  // burns a value, and gaps order just as well -- so this is a lower bound.
  CHECK(r.ops.tsCounter() >= 20, "each write claimed a counter value");

  // And the stamps actually in the arena must BE counter values -- i.e. within
  // the range tsFromFaa can produce -- rather than floored ones. If the floor
  // were applied, a hammered node's stamps would drift above the counter's
  // reach, which is precisely how the inversion above arises.
  uint64_t const ceiling = ds::tsFromFaa(r.ops.tsCounter(), /*client=*/0);
  uint64_t chain = 0;
  for (uint64_t off = 0; off < r.ops.vecCount(); ++off) {
    uint64_t const ts = r.ops.vecAt(static_cast<ds::VecOffset>(off)).ts;
    if (ts == ds::kNullTs || ts == ds::kBootstrapTs) continue;
    ++chain;
    CHECK(ts <= ceiling, "a Faa stamp never exceeds what the counter reached");
  }
  CHECK(chain >= 20, "the rewrites really did leave a chain to check");

  // Uniqueness is the property a total order rests on, and it is the one the
  // "FAA all replicas and take the max" alternative loses (see ds_ts.hpp).
  // Here there is one counter, so it must hold exactly.
  std::set<uint64_t> seen;
  for (uint64_t pre = 0; pre < r.ops.tsCounter(); ++pre) {
    uint64_t const ts = ds::tsFromFaa(pre, /*client=*/0);
    CHECK(seen.insert(ts).second, "no counter value maps to a repeated ts");
    CHECK(ts != ds::kNullTs, "no counter value maps to the null marker");
  }
}

static void checkChainSurvivesABackwardClock() {
  // The adversarial case stampOver exists for. FakeOps::now() normally counts
  // up; a negative step makes it count DOWN by more per call than a write
  // takes, which is a harsher version of two machines whose clocks disagree by
  // more than the gap between successive versions of one node.
  Rig r;
  r.ops.setTsMode(ds::TsMode::Clock);
  r.ops.setClockStep(-1000);

  hammerOneKey(r, 42, 30);
  CHECK(r.verify("with a backward clock"),
        "stampOver keeps the chain decreasing even as the clock runs back");

  // And show the guard is what did it: the same sequence of raw clock values
  // really would have inverted. Asserting the negative is what stops the test
  // passing for an unrelated reason -- if the chain were only ever one deep it
  // would verify with or without stampOver.
  bool inverts = false;
  uint64_t src = 1000;
  uint64_t pred = ds::kNullTs;
  for (int i = 0; i < 30; ++i) {
    uint64_t const raw = (src -= 1000);   // what the clock alone would give
    if (pred != ds::kNullTs && raw <= pred) inverts = true;
    pred = ds::stampOver(raw, pred);
  }
  CHECK(inverts, "the unguarded clock really would have inverted the chain");

  // Faa is immune by construction -- its source is a counter, not a clock -- so
  // the same abuse must not perturb it. This is the comparison that says what
  // Faa buys: correctness with no timing assumption at all.
  Rig f;
  f.ops.setTsMode(ds::TsMode::Faa);
  f.ops.setClockStep(-1000);
  hammerOneKey(f, 42, 30);
  CHECK(f.verify("Faa with a backward local clock"),
        "Faa does not depend on the local clock at all");
}

static void checkHelperStampsAreTheDocumentedGap() {
  // ds_ts.hpp is explicit that a HELPER's stamp is not covered by stampOver:
  // it has not read the version being superseded, so it passes kNullTs as the
  // predecessor and the raw source comes through. This pins that behaviour so
  // the limitation stays visible rather than being quietly assumed away.
  uint64_t const raw = 12345;
  CHECK(ds::stampOver(raw, ds::kNullTs) == raw,
        "a helper's stamp is the unguarded source");
}

int main() {
  checkModeNamesRoundTrip();
  checkNoSourceEverReturnsTheNullMarker();
  checkClockModeIsNanosecondsAndCrossMachineComparable();
  checkStampOverAlgebra();
  checkTheFloorIsNotAppliedInFaaMode();
  checkEveryModeProducesADecreasingChain();
  checkFaaConsumesOneCounterValuePerStampAndNeverRepeats();
  checkChainSurvivesABackwardClock();
  checkHelperStampsAreTheDocumentedGap();

  if (g_failures == 0) {
    std::printf("ts: all checks pass\n");
    return 0;
  }
  std::printf("ts: %d check(s) failed\n", g_failures);
  return 1;
}
