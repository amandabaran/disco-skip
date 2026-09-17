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
#include <random>
#include <string>
#include <vector>

#include "ds_put.hpp"
#include "ds_range.hpp"
#include "ds_ts.hpp"
#include "ds_verify.hpp"
#include "fake_ops.hpp"
#include "fake_replicas.hpp"

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
  for (ds::TsMode m : {ds::TsMode::Clock, ds::TsMode::Tsc, ds::TsMode::Faa,
                       ds::TsMode::RangeTs}) {
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


/// The counterexample ds_ts.hpp derives, run for real.
///
/// "WITH ONLY A MAJORITY IT BREAKS": let W1's maximum come from replica A and
/// let W2's quorum exclude A. W2 does touch some B in Q1 n Q2, whose value
/// after W1 is v_B + 1 -- but v_B + 1 can still be <= M1, because M1 came from
/// A, not B. So a write that STARTS AFTER another FINISHED can claim a
/// timestamp that is not larger.
///
/// TWO THINGS THIS TEST HAS TO KEEP APART, and the first draft did not.
/// Downing a replica to make its counter fall behind also makes its DATA fall
/// behind, and then the next quorum containing it holds two disagreeing
/// handles -- no majority, so the write under test never resolves and the test
/// measures nothing. The counters are therefore diverged directly
/// (advanceCounterForTest) while every replica stays current on data.
///
/// The two writers also touch DIFFERENT data nodes. W_A leaves replica 0 one
/// version stale on its own node; if W_B wrote the same node, its quorum
/// {0,2} could not form a majority there either. The counter is global, so
/// ordering across two different keys is exactly the cross-writer property at
/// issue.
///
/// With counters (C+10, C+10, C) and all data in agreement:
///
///   W_A, quorum {1,2}: pre (C+10, C), M_A = C+10 from REPLICA 1
///                      ts_A = C+11, write-back raises replica 2 to C+11
///   W_A COMPLETES. W_B, quorum {0,2} -- never reads replica 1:
///     with the write-back:  pre (C+10, C+11), M_B = C+11, ts_B = C+12 > ts_A
///     without it, replica 2 would sit at C+1:
///                           pre (C+10, C+1),  M_B = C+10, ts_B = C+11 == ts_A
///
/// Equal is already a violation -- two distinct writes cannot share a
/// timestamp and still give the old_ver chain a strict order -- so ts_B > ts_A
/// catches both the tie and the inversion the client tiebreak turns it into.
static void checkTheMajorityFaaCounterexampleIsRepaired() {
  FakeReplicaSet set(3, kLayers, 16384);
  set.setTsMode(ds::TsMode::Faa);
  ds::QuorumStats qs;
  ds::PutStats ps;
  ds::WriteStats ws;
  ds::NullPutCache cache;

  auto put = [&](ds::Key k, ds::Value v) {
    ds::QuorumOps<FakeReplicaSet> ops(set, qs, nullptr);
    ds::Putter<ds::QuorumOps<FakeReplicaSet>, ds::NullPutCache> p(
        ops, cache, kLayers, ps, ws);
    return p.put(k, v, 0).resolved;
  };
  auto dataAddr = [&](ds::Key k) {
    ds::QuorumOps<FakeReplicaSet> ops(set, qs, nullptr);
    ds::PathStep path[kLayers];
    ds::Traversal<ds::QuorumOps<FakeReplicaSet>> t(ops);
    return t.traverse(k, kLayers, path).data_addr;
  };
  // The ts now on the node covering k, from any replica that is up and current.
  auto tsOf = [&](ds::RemoteAddr addr) -> uint64_t {
    uint64_t best = ds::kNullTs;
    for (size_t r = 0; r < 3; ++r) {
      if (set.isDown(r)) continue;
      ds::NodeRecord node{};
      if (!set.readNodeFrom(r, addr, node)) continue;
      ds::VecRecord vec{};
      if (!set.readVecFrom(r, node.handle.offset(), vec)) continue;
      if (vec.ts != ds::kNullTs && (best == ds::kNullTs || vec.ts > best)) {
        best = vec.ts;
      }
    }
    return best;
  };

  // Seed a structure with at least two data nodes, every replica current.
  std::mt19937_64 rng(0x7A5A11);
  for (int i = 0; i < 300; ++i) {
    (void)put(static_cast<ds::Key>(rng() % 2000),
              static_cast<ds::Value>(i + 1));
  }
  ds::Key const k_a = 100;
  ds::Key const k_b = 1900;
  CHECK(put(k_a, 1), "seed k_a");
  CHECK(put(k_b, 2), "seed k_b");
  ds::RemoteAddr const addr_a = dataAddr(k_a);
  ds::RemoteAddr const addr_b = dataAddr(k_b);
  CHECK(addr_a != addr_b, "the two writers touch different data nodes");

  // Diverge the COUNTERS only. Replica 1 alone runs ahead, and that is the
  // point: W_A's maximum must be held by exactly the replica W_B will not
  // read. Diverging replicas 0 and 1 together does not discriminate, because
  // W_B reads replica 0 and would see the maximum there anyway.
  set.advanceCounterForTest(1, 10);

  uint64_t const wb_before = set.tsWriteBacks();

  // W_A runs with EVERY REPLICA UP, so it leaves no replica stale on data --
  // which is what lets W_B's quorum form a majority afterwards. Its maximum
  // still comes from replica 1 alone.
  CHECK(put(k_a, 900), "W_A resolves with all replicas up");
  uint64_t const ts_a = tsOf(addr_a);

  // W_A has COMPLETED. W_B's quorum is {0,2}; it never reads replica 1.
  //
  //   with the write-back:  replicas 0 and 2 were raised to C+11
  //                         pre (C+11, C+11), M_B = C+11, ts_B = C+12 > ts_A
  //   without it, both sat at C+1 (their own increment only)
  //                         pre (C+1, C+1),   M_B = C+1,  ts_B = C+2  < ts_A
  //
  // so the unrepaired protocol inverts the order outright here, not merely
  // ties it.
  set.setDown(1, true);
  CHECK(put(k_b, 901), "W_B resolves on {0,2}");
  uint64_t const ts_b = tsOf(addr_b);
  set.setDown(1, false);

  std::printf("  majority-FAA counterexample: ts_A=%llu ts_B=%llu"
              "  counters (%llu, %llu, %llu)  write-backs=%llu\n",
              static_cast<unsigned long long>(ts_a),
              static_cast<unsigned long long>(ts_b),
              static_cast<unsigned long long>(set.counterOf(0)),
              static_cast<unsigned long long>(set.counterOf(1)),
              static_cast<unsigned long long>(set.counterOf(2)),
              static_cast<unsigned long long>(set.tsWriteBacks() - wb_before));

  CHECK(ts_a != ds::kNullTs && ts_b != ds::kNullTs, "both writes were stamped");
  CHECK(set.tsWriteBacks() > wb_before,
        "the diverged round actually posted a counter write-back");
  CHECK(ts_b > ts_a,
        "a write that began after another finished takes a LARGER timestamp");
}


/// --ts none: no stamps, no helping, no ranges -- and one submission per write.
///
/// The mode exists because point operations never need a timestamp (they
/// linearize on the publishing CAS and the version tags) while workloads A-D
/// contain no scans at all, so those runs were paying for a capability they
/// never used. What they were paying, per write per server: one atomic to
/// claim or read a timestamp, one atomic to CAS it into the vector, and -- in
/// Faa mode -- a whole extra ROUND TRIP, because the value is not known until
/// the FAA returns so the stamp cannot ride the publish chain.
///
/// Three properties, and the second is the one that makes this a MODE rather
/// than simply an absent field:
///
///   1. Nothing is stamped. Every version keeps ts == kNullTs.
///   2. NOBODY TRIES TO HELP. kNullTs already means "published but unstamped,
///      go help it", so if writes just stopped stamping then every version
///      would look pending forever and every reader would queue a helping
///      batch for a write nobody was ever going to stamp -- turning a saving
///      into unbounded helper traffic. tsIsPending() takes the mode.
///   3. A range REFUSES rather than answering. Nothing was stamped, so no
///      order can be reconstructed after the fact and any interval returned
///      would mix states that never coexisted. Reported as a failure the
///      caller can see, not as an empty result, which would look like a
///      legitimate answer.
static void checkTsNoneSkipsStampingWithoutStrandingReaders() {
  Rig r;
  r.ops.setTsMode(ds::TsMode::None);

  uint64_t const batches_before = r.ops.batches();
  for (int i = 0; i < 40; ++i) {
    ds::Key const k = static_cast<ds::Key>(100 + (i % 10));
    CHECK(r.put(k, static_cast<ds::Value>(i + 1), i % 3).resolved,
          "a put resolves with no timestamp mode");
  }
  uint64_t const batches_after = r.ops.batches();

  // 1. Nothing stamped, anywhere in the arena.
  uint64_t stamped = 0, total = 0;
  for (uint64_t off = 0; off < r.ops.vecCount(); ++off) {
    uint64_t const ts = r.ops.vecAt(static_cast<ds::VecOffset>(off)).ts;
    if (ts == ds::kBootstrapTs) continue;   // bootstrap writes a settled tree
    ++total;
    if (ts != ds::kNullTs) ++stamped;
  }
  CHECK(total > 0, "there were versions to inspect");
  CHECK(stamped == 0, "no version is stamped in TsMode::None");

  // 2. No helping. helped_ts counts timestamps a reader fixed for a writer;
  //    in this mode there is nothing to fix and asking would be wasted work.
  CHECK(r.ws.helped_ts == 0, "no reader helped a timestamp in TsMode::None");

  // And a traversal over that arena must not decide to help either -- this is
  // the path that would storm.
  {
    ds::PathStep path[kLayers];
    ds::Traversal<FakeOps> t(r.ops);
    ds::TraversalResult const tr = t.traverse(105, kLayers, path);
    CHECK(tr.status == ds::TraversalStatus::Ok, "a traversal still resolves");
    CHECK(tr.helped_ts == 0, "a traversal helps no timestamp in TsMode::None");
  }

  // 3. A range refuses, and says why.
  {
    ds::RangeStats rs;
    std::vector<ds::Entry> got;
    ds::Ranger<FakeOps> ranger(r.ops, rs);
    ds::RangeResult const res = ranger.range(0, 1000, kLayers, 1u << 20, got);
    CHECK(!res.resolved, "a range does not resolve in TsMode::None");
    CHECK(res.gave_up_no_timestamps,
          "and reports it as a configuration error, not a race");
    CHECK(rs.failures == 1, "counted as a failure the caller can see");
    CHECK(got.empty(), "and returns nothing rather than a plausible interval");
  }

  // The structure itself is still sound -- this mode changes what is recorded
  // about ORDER, not what is stored.
  CHECK(r.verify("with no timestamps"), "the structure verifies in TsMode::None");

  std::printf("  ts none: %llu writes in %llu batches (%.2f per write), "
              "%llu of %llu versions stamped\n",
              40ULL,
              static_cast<unsigned long long>(batches_after - batches_before),
              static_cast<double>(batches_after - batches_before) / 40.0,
              static_cast<unsigned long long>(stamped),
              static_cast<unsigned long long>(total));
}

/// The saving, stated as a comparison rather than asserted in the abstract.
///
/// Faa needs a SECOND submission per write: the counter value is not known
/// until the first chain completes, and a timestamp may not be allocated
/// before the version it stamps is visible. None needs no second submission at
/// all. On the cluster that second submission is a round trip; here it is a
/// batch count, which is the same quantity the fake can see.
static void checkTsNoneCostsFewerSubmissionsThanFaa() {
  uint64_t counts[2];
  ds::TsMode const modes[2] = {ds::TsMode::Faa, ds::TsMode::None};
  for (int i = 0; i < 2; ++i) {
    Rig r;
    r.ops.setTsMode(modes[i]);
    uint64_t const before = r.ops.batches();
    for (int j = 0; j < 30; ++j) {
      CHECK(r.put(static_cast<ds::Key>(200 + (j % 8)),
                  static_cast<ds::Value>(j + 1), 0).resolved, "put resolves");
    }
    counts[i] = r.ops.batches() - before;
  }
  std::printf("  submissions for 30 writes: faa=%llu none=%llu\n",
              static_cast<unsigned long long>(counts[0]),
              static_cast<unsigned long long>(counts[1]));
  CHECK(counts[1] < counts[0],
        "TsMode::None issues strictly fewer submissions per write than Faa");
}


// ── TsMode::RangeTs ─────────────────────────────────────────────────────────

static void checkRangeTsWritesReadTheCounterAndNeverAdvanceIt() {
  // THE MODE'S ENTIRE CLAIM, stated as the only thing that can verify it: the
  // counter. In Faa mode every write fetch-and-adds, so 30 writes advance it
  // at least 30. Here a write only READS, so 30 writes must leave it exactly
  // where they found it -- and a RANGE must be what moves it.
  //
  // Asserted on the counter rather than on a stat, because a stat can be
  // right while the wire is wrong: BatchKind::ReadTsCounter differs from
  // FaaTs only in the verb posted, and a missed substitution would show up
  // here and nowhere else.
  Rig r;
  r.ops.setTsMode(ds::TsMode::RangeTs);
  CHECK(r.ops.tsCounter() == 0, "the counter starts at zero");

  hammerOneKey(r, 11, 30);
  for (int i = 0; i < 16; ++i) {
    CHECK(r.put(static_cast<ds::Key>(300 + i * 5),
                static_cast<ds::Value>(i + 1), i % 3).resolved,
          "mixed-height put resolves");
  }
  CHECK(r.ops.tsCounter() == 0,
        "46 writes advanced the counter by ZERO in RangeTs mode");
  CHECK(r.verify("after RangeTs writes"), "the arena verifies");

  // Every stamp must be in band 1 -- the band for a writer that read 0 --
  // which is what "nothing advanced it" looks like from the vectors' side.
  uint64_t stamped = 0;
  for (uint64_t off = 0; off < r.ops.vecCount(); ++off) {
    uint64_t const ts = r.ops.vecAt(static_cast<ds::VecOffset>(off)).ts;
    if (ts == ds::kNullTs || ts == ds::kBootstrapTs) continue;
    ++stamped;
    CHECK((ts >> ds::kFaaClientBits) == 1,
          "a RangeTs stamp sits in the band of the value it read");
  }
  CHECK(stamped >= 30, "the rewrites really did leave stamps to check");

  // And now a range, which is the only thing that may advance it.
  ds::RangeStats rs;
  std::vector<ds::Entry> got;
  ds::Ranger<FakeOps> rr(r.ops, rs);
  ds::RangeResult const res = rr.range(0, 4000, kLayers, 1u << 20, got);
  CHECK(res.resolved, "the range resolved");
  CHECK(r.ops.tsCounter() == 1, "the RANGE advanced the counter, by exactly 1");
  CHECK(res.snapshot == ds::snapshotFromCounterFaa(0),
        "and its cut is the band of the pre-value its FAA returned");
  // Every write above completed before this range started, so every one of
  // them is inside its cut. Band 1 <= band 1.
  CHECK(res.snapshot >= ds::tsFromCounterRead(0, ds::kFaaClientMask),
        "a write that read 0 is inside the cut of a range that FAA'd 0");
}

static void checkRangeTsSeparatesSuccessiveCuts() {
  // WHY A RANGE MUST FAA RATHER THAN READ, which is the one place RangeTs is
  // not just Faa with the verbs swapped. Nothing else advances the counter, so
  // two ranges that merely read would take the SAME cut -- and then a write
  // landing between them would be stamped inside that shared cut, so the
  // second range would return a value the first had already excluded AT THE
  // SAME T. Two ranges at one snapshot disagreeing is not a staleness
  // quibble; it breaks the snapshot semantics outright.
  //
  // Driven with real values. Counter 0.
  //   range A FAAs 0 -> 1, cut = band 1
  //   write   reads 1, stamps band 2
  //   range B FAAs 1 -> 2, cut = band 2
  // So A must NOT see the write and B must, and a re-read at A's own cut must
  // still not see it however long afterwards it runs.
  Rig r;
  r.ops.setTsMode(ds::TsMode::RangeTs);
  ds::Key const k = 500;
  CHECK(r.put(k, 1111, 0).resolved, "the pre-existing value is written");

  ds::RangeStats rs;
  std::vector<ds::Entry> a;
  ds::Ranger<FakeOps> rr(r.ops, rs);
  ds::RangeResult const ra = rr.range(k, k, kLayers, 1u << 20, a);
  CHECK(ra.resolved && a.size() == 1 && a[0].val == 1111,
        "range A sees the pre-existing value");
  CHECK(ra.snapshot == ds::snapshotFromCounterFaa(0), "A's cut is band 1");

  CHECK(r.put(k, 2222, 0).resolved, "the write after A's cut lands");
  {
    ds::VecRecord cur{};
    ds::NodeRecord n{};
    (void)n;
    uint64_t newest = 0;
    for (uint64_t off = 0; off < r.ops.vecCount(); ++off) {
      cur = r.ops.vecAt(static_cast<ds::VecOffset>(off));
      int const i = ds::findLte(cur, k);
      if (i >= 0 && cur.keyAt(i) == k && cur.valAt(static_cast<uint32_t>(i)) == 2222) {
        newest = cur.ts;
      }
    }
    CHECK((newest >> ds::kFaaClientBits) == 2,
          "a write after a range's FAA lands in a STRICTLY HIGHER band");
    CHECK(newest > ra.snapshot, "so it is outside the cut A already returned");
  }

  // A's cut re-read, now that the write has landed and been stamped. Same
  // answer, which is what "the cut A returned is stable" means.
  std::vector<ds::Entry> again;
  ds::RangeResult const rag =
      rr.rangeAt(k, k, kLayers, 1u << 20, ra.snapshot, again);
  CHECK(rag.resolved && again.size() == 1 && again[0].val == 1111,
        "A's cut STILL excludes the later write");

  std::vector<ds::Entry> b;
  ds::RangeResult const rb = rr.range(k, k, kLayers, 1u << 20, b);
  CHECK(rb.resolved && b.size() == 1 && b[0].val == 2222,
        "range B, which FAA'd after the write, sees it");
  CHECK(rb.snapshot > ra.snapshot, "and B's cut is strictly above A's");
}

static void checkRangeTsToleratesTiedStampsWithinABand() {
  // THE PRICE OF NOT FLOORING, and the reason ds_verify.hpp checks bands in
  // this mode rather than strict order. Nothing advances the counter between
  // two writes, so both read the same value and their stamps differ ONLY in
  // the 16-bit client field -- which carries no order. Pick the indices so the
  // NEWER version gets the SMALLER stamp and the chain genuinely inverts:
  //
  //   client 9 writes -> ts = (1 << 16) | 9 = 65545
  //   client 2 writes -> ts = (1 << 16) | 2 = 65538     <- newer, smaller
  //
  // This must not be papered over with stampOver's floor. A floored stamp
  // could cross a band boundary, and then a range that FAA'd the earlier value
  // would EXCLUDE a write that completed before it started -- a wrong answer,
  // where a tie is merely an order we never claimed.
  Rig r;
  r.ops.setTsMode(ds::TsMode::RangeTs);
  ds::Key const k = 620;
  r.ops.setClientIdx(9);
  CHECK(r.put(k, 111, 0).resolved, "client 9's write lands");
  r.ops.setClientIdx(2);
  CHECK(r.put(k, 222, 0).resolved, "client 2's write lands");

  // Find the two versions and confirm the inversion is real rather than
  // hypothetical -- otherwise this test passes while testing nothing.
  uint64_t ts_111 = 0, ts_222 = 0;
  for (uint64_t off = 0; off < r.ops.vecCount(); ++off) {
    ds::VecRecord const v = r.ops.vecAt(static_cast<ds::VecOffset>(off));
    int const i = ds::findLte(v, k);
    if (i < 0 || v.keyAt(i) != k) continue;
    ds::Value const val = v.valAt(static_cast<uint32_t>(i));
    if (val == 111) ts_111 = v.ts;
    if (val == 222) ts_222 = v.ts;
  }
  std::printf("  tied band: older ts %llu, newer ts %llu\n",
              static_cast<unsigned long long>(ts_111),
              static_cast<unsigned long long>(ts_222));
  CHECK(ts_111 != 0 && ts_222 != 0, "both versions are stamped");
  CHECK((ts_111 >> ds::kFaaClientBits) == (ts_222 >> ds::kFaaClientBits),
        "the two writes share a band, because no range ran between them");
  CHECK(ts_222 < ts_111,
        "and the NEWER version really has the SMALLER stamp");

  // The verifier must accept it -- there is no total order here to assert.
  CHECK(r.verify("with a tied band"), "banded chains verify in RangeTs mode");

  // And the walk must still answer with the NEWER version. "ts <= T" reduces
  // to "band(ts) <= band(T)" because every cut is band-aligned, so the
  // predicate is monotone along the chain and the first hit is the right one
  // -- the within-band inversion is invisible to it.
  ds::RangeStats rs;
  std::vector<ds::Entry> got;
  ds::Ranger<FakeOps> rr(r.ops, rs);
  ds::RangeResult const res = rr.range(k, k, kLayers, 1u << 20, got);
  CHECK(res.resolved && got.size() == 1 && got[0].val == 222,
        "a range over a tied band returns the NEWER version");

  ds::PutStats gps;
  ds::WriteStats gws;
  (void)gps; (void)gws;
}

static void checkRangeTsCostsTheSameTwoSubmissionsAsFaa() {
  // THE CLAIM IN ds_ts.hpp, PRICED. RangeTs does NOT remove Faa's second
  // submission: the counter may not be read until the version is visible, so
  // the value arrives with the publish's completion and the stamping CAS needs
  // another round. What it removes is one ATOMIC of three per write per
  // server. Stating that here stops the mode being sold as something it is
  // not -- and TsMode::None, which DOES fold the write into one submission,
  // is the row that shows what the round trip is worth.
  uint64_t counts[3];
  ds::TsMode const modes[3] = {ds::TsMode::Faa, ds::TsMode::RangeTs,
                               ds::TsMode::None};
  for (int i = 0; i < 3; ++i) {
    Rig r;
    r.ops.setTsMode(modes[i]);
    uint64_t const before = r.ops.batches();
    for (int j = 0; j < 30; ++j) {
      CHECK(r.put(static_cast<ds::Key>(700 + (j % 8)),
                  static_cast<ds::Value>(j + 1), 0).resolved, "put resolves");
    }
    counts[i] = r.ops.batches() - before;
  }
  std::printf("  submissions for 30 writes: faa=%llu rangets=%llu none=%llu\n",
              static_cast<unsigned long long>(counts[0]),
              static_cast<unsigned long long>(counts[1]),
              static_cast<unsigned long long>(counts[2]));
  CHECK(counts[1] == counts[0],
        "RangeTs costs the SAME submissions per write as Faa, not fewer");
  CHECK(counts[2] < counts[1],
        "and TsMode::None is what actually removes the second submission");
}


/// The majority counterexample, MOVED TO THE RANGE PATH -- both directions.
///
/// ds_ts.hpp derives that a replicated counter orders two operations only if
/// the later one observes the replica that decided the earlier one's maximum,
/// which quorum intersection does NOT provide: the intersecting replica's own
/// value can still be below that maximum. In Faa mode the claim is on the
/// write, so the write owes the write-back. In RangeTs BOTH sides claim -- a
/// range by fetch-and-add, a write by read -- so BOTH owe one, and each guards
/// a different half of snapshot correctness.
///
/// Worked with real values. Three replicas, counter C on all, then replica 1
/// alone advanced by 10 (C+10). Replica 1 is the one the survivor quorum
/// {0, 2} will never see, which is the whole construction: diverging 0 and 1
/// together would not discriminate, because {0,2} reads 0 and would find the
/// maximum there anyway.
///
///   HALF A -- a write that FINISHED must be INSIDE a later range's cut.
///     W reads all three: (C, C+10, C), max C+10, stamps band C+11.
///     W's write-back raises 0 and 2 to C+10.
///     Replica 1 goes down. Range R FAAs {0,2}: (C+10, C+10), max C+10,
///     cut = band C+11. W is band C+11 <= C+11, so R SEES IT.
///   without W's write-back, 0 and 2 still sit at C, so R's max is C and its
///   cut is band C+1 -- and W, which completed before R began, is OUTSIDE it.
///   A range losing a completed write is a wrong answer, not staleness.
///
///   HALF B -- a write that BEGAN must be OUTSIDE a finished range's cut.
///     Range R FAAs all three: (C', C'+10, C'), max C'+10, cut band C'+11.
///     R's write-back raises 0 and 2 to C'+11.
///     Replica 1 goes down. Write W reads {0,2}: (C'+11, C'+11), max C'+11,
///     stamps band C'+12 > the cut. R correctly never had it.
///   without R's write-back, 0 and 2 sit at C'+1, so W reads C'+1 and stamps
///   band C'+2 -- INSIDE a cut R has already returned without it. A second
///   range at R's own T would then include it, and two ranges disagreeing at
///   one snapshot breaks snapshot semantics outright.
static void checkTheMajorityCounterexampleIsRepairedOnBothSides() {
  FakeReplicaSet set(3, kLayers, 16384);
  set.setTsMode(ds::TsMode::RangeTs);
  ds::QuorumStats qs;
  ds::PutStats ps;
  ds::WriteStats ws;
  ds::NullPutCache cache;
  using Ops = ds::QuorumOps<FakeReplicaSet>;

  auto put = [&](ds::Key k, ds::Value v) {
    Ops ops(set, qs, nullptr);
    ds::Putter<Ops, ds::NullPutCache> p(ops, cache, kLayers, ps, ws);
    return p.put(k, v, 0).resolved;
  };
  // The ts now on the node covering k, over the replicas that are up.
  auto tsOf = [&](ds::Key k) -> uint64_t {
    Ops ops(set, qs, nullptr);
    ds::PathStep path[kLayers];
    ds::Traversal<Ops> t(ops);
    ds::RemoteAddr const addr = t.traverse(k, kLayers, path).data_addr;
    uint64_t best = ds::kNullTs;
    for (size_t r = 0; r < 3; ++r) {
      if (set.isDown(r)) continue;
      ds::NodeRecord node{};
      if (!set.readNodeFrom(r, addr, node)) continue;
      ds::VecRecord vec{};
      if (!set.readVecFrom(r, node.handle.offset(), vec)) continue;
      if (vec.ts != ds::kNullTs && (best == ds::kNullTs || vec.ts > best)) {
        best = vec.ts;
      }
    }
    return best;
  };
  auto rangeOver = [&](ds::Key lo, ds::Key hi, std::vector<ds::Entry> &out) {
    Ops ops(set, qs, nullptr);
    ds::RangeStats rs;
    ds::Ranger<Ops> rr(ops, rs);
    return rr.range(lo, hi, kLayers, 1u << 20, out);
  };

  std::mt19937_64 rng(0x5EED5);
  for (int i = 0; i < 300; ++i) {
    (void)put(static_cast<ds::Key>(rng() % 2000),
              static_cast<ds::Value>(i + 1));
  }
  ds::Key const k_a = 100;
  CHECK(put(k_a, 1), "seed k_a on every replica");

  // ── HALF A: the write's write-back keeps it inside a later cut ───────────
  set.advanceCounterForTest(1, 10);
  uint64_t wb = set.tsWriteBacks();
  CHECK(put(k_a, 900), "W resolves with every replica up");
  CHECK(set.tsWriteBacks() > wb,
        "the write's diverged READ round posted a counter write-back");
  uint64_t const ts_w = tsOf(k_a);

  set.setDown(1, true);
  std::vector<ds::Entry> got;
  ds::RangeResult const res = rangeOver(k_a, k_a, got);
  set.setDown(1, false);
  CHECK(res.resolved, "the range on {0,2} resolved");
  bool found = false;
  for (auto const &e : got) {
    if (e.key == k_a) { found = true; CHECK(e.val == 900, "and with W's value"); }
  }
  std::printf("  half A: ts_W=%llu cut=%llu  counters (%llu, %llu, %llu)\n",
              static_cast<unsigned long long>(ts_w),
              static_cast<unsigned long long>(res.snapshot),
              static_cast<unsigned long long>(set.counterOf(0)),
              static_cast<unsigned long long>(set.counterOf(1)),
              static_cast<unsigned long long>(set.counterOf(2)));
  CHECK(ts_w != ds::kNullTs, "W was stamped");
  CHECK(ts_w <= res.snapshot,
        "a write that FINISHED is inside the cut of a range that began after");
  CHECK(found, "so the range returns it");

  // ── HALF B: the range's write-back keeps a later write outside its cut ───
  ds::Key const k_b = 1900;
  CHECK(put(k_b, 2), "seed k_b on every replica");
  set.advanceCounterForTest(1, 10);
  wb = set.tsWriteBacks();
  std::vector<ds::Entry> got_b;
  ds::RangeResult const res_b = rangeOver(k_b, k_b, got_b);
  CHECK(res_b.resolved, "the range with every replica up resolved");
  CHECK(set.tsWriteBacks() > wb,
        "the range's diverged FAA round posted a counter write-back");

  set.setDown(1, true);
  CHECK(put(k_b, 902), "W' resolves on {0,2} after the range finished");
  uint64_t const ts_w2 = tsOf(k_b);
  set.setDown(1, false);
  std::printf("  half B: cut=%llu ts_W'=%llu  counters (%llu, %llu, %llu)\n",
              static_cast<unsigned long long>(res_b.snapshot),
              static_cast<unsigned long long>(ts_w2),
              static_cast<unsigned long long>(set.counterOf(0)),
              static_cast<unsigned long long>(set.counterOf(1)),
              static_cast<unsigned long long>(set.counterOf(2)));
  CHECK(ts_w2 != ds::kNullTs, "W' was stamped");
  CHECK(ts_w2 > res_b.snapshot,
        "a write that BEGAN after a range finished is outside its cut");

  // And the cut really is stable: re-reading at R's own T must still not see
  // W', which is the property "two ranges at one snapshot agree" reduces to.
  {
    Ops ops(set, qs, nullptr);
    ds::RangeStats rs;
    ds::Ranger<Ops> rr(ops, rs);
    std::vector<ds::Entry> again;
    ds::RangeResult const r2 =
        rr.rangeAt(k_b, k_b, kLayers, 1u << 20, res_b.snapshot, again);
    CHECK(r2.resolved, "R's cut re-reads");
    for (auto const &e : again) {
      if (e.key == k_b) CHECK(e.val == 2, "R's cut still excludes W'");
    }
  }
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
  checkTheMajorityFaaCounterexampleIsRepaired();
  checkTsNoneSkipsStampingWithoutStrandingReaders();
  checkTsNoneCostsFewerSubmissionsThanFaa();
  checkRangeTsWritesReadTheCounterAndNeverAdvanceIt();
  checkRangeTsSeparatesSuccessiveCuts();
  checkRangeTsToleratesTiedStampsWithinABand();
  checkRangeTsCostsTheSameTwoSubmissionsAsFaa();
  checkTheMajorityCounterexampleIsRepairedOnBothSides();

  if (g_failures == 0) {
    std::printf("ts: all checks pass\n");
    return 0;
  }
  std::printf("ts: %d check(s) failed\n", g_failures);
  return 1;
}
