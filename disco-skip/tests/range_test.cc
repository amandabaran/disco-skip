// A10: snapshot range queries (ds_range.hpp).
//
// The easy property -- "a range over a quiescent structure returns the keys in
// it" -- is checked against a std::map oracle. The properties worth the file
// are the two that involve TIME:
//
//   * a write that lands AFTER the snapshot must not appear in it, and
//   * a node that was CREATED after the snapshot must be skipped, with the keys
//     it now holds served from the node it split from.
//
// The second is the one the algorithm is actually delicate about: the walk
// follows the CURRENT chain while reading each node AS OF T, and those are two
// different times. It is sound only because a node's k_min never changes and
// nothing is ever removed -- so the test constructs exactly that situation
// rather than trusting the argument.

#include <cstdio>
#include <map>
#include <random>
#include <vector>

// For the cache-sourced backbone: a real SkipVec mirroring the fake arena.
#include "ds_cache.hpp"

#include "ds_get.hpp"
#include "ds_put.hpp"
#include "ds_range.hpp"
#include "ds_verify.hpp"
#include "ds_range_future.hpp"
#include "ds_put_future.hpp"
#include "fake_async.hpp"
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

struct Rig {
  FakeOps ops = buildInitialArena(kLayers, 16384);
  ds::PutStats ps;
  ds::WriteStats ws;
  ds::RangeStats rs;
  ds::NullPutCache cache;

  Rig() { ops.setTsMode(ds::TsMode::Faa); }

  bool put(ds::Key k, ds::Value v, uint32_t h) {
    ds::Putter<FakeOps, ds::NullPutCache> p(ops, cache, kLayers, ps, ws);
    return p.put(k, v, h).resolved;
  }
  ds::RangeResult range(ds::Key lo, ds::Key hi, std::vector<ds::Entry> &out,
                        size_t cap = 1u << 20) {
    ds::Ranger<FakeOps> r(ops, rs);
    return r.range(lo, hi, kLayers, cap, out);
  }
};

static void checkRangeMatchesAnOracle() {
  Rig r;
  std::map<ds::Key, ds::Value> oracle;
  std::mt19937_64 rng(0x5A9E01);
  for (int i = 0; i < 400; ++i) {
    ds::Key const k = static_cast<ds::Key>(rng() % 5000);
    ds::Value const v = static_cast<ds::Value>(i + 1);
    uint32_t const h = ds::drawHeight(rng, kLayers);
    if (r.put(k, v, h)) oracle[k] = v;
  }

  for (int trial = 0; trial < 40; ++trial) {
    ds::Key lo = static_cast<ds::Key>(rng() % 5000);
    ds::Key hi = static_cast<ds::Key>(rng() % 5000);
    if (hi < lo) std::swap(lo, hi);

    std::vector<ds::Entry> got;
    CHECK(r.range(lo, hi, got).resolved, "the range resolves");

    std::vector<ds::Entry> want;
    for (auto const &kv : oracle) {
      if (kv.first >= lo && kv.first <= hi) want.push_back({kv.first, kv.second});
    }
    CHECK(got.size() == want.size(), "the range returns the right count");
    bool same = got.size() == want.size();
    for (size_t i = 0; same && i < got.size(); ++i) {
      if (got[i].key != want[i].key || got[i].val != want[i].val) same = false;
    }
    CHECK(same, "and the right keys and values, in key order");
    if (!same) {
      std::printf("  [%llu,%llu] got %zu want %zu\n",
                  static_cast<unsigned long long>(lo),
                  static_cast<unsigned long long>(hi), got.size(), want.size());
      break;
    }
  }
  std::printf("  oracle: %zu keys, %llu ranges, %llu entries\n", oracle.size(),
              static_cast<unsigned long long>(r.rs.ranges),
              static_cast<unsigned long long>(r.rs.entries));
}

static void checkAWriteAfterTheSnapshotIsInvisible() {
  // The defining property of a snapshot. The counter is read, not advanced, so
  // taking one twice with a write between must show the write only in the
  // second -- and the FIRST snapshot must still be readable afterwards.
  Rig r;
  for (ds::Key k = 100; k <= 500; k += 100) CHECK(r.put(k, k, 0), "seed");

  uint64_t const before = ds::takeSnapshot(r.ops);
  CHECK(r.put(250, 2500, 0), "a write lands after the snapshot is taken");

  // Re-read at the OLD snapshot by hand: the newest version <= `before`.
  std::vector<ds::Entry> now;
  CHECK(r.range(0, 1000, now).resolved, "a fresh range resolves");
  bool sees_new = false;
  for (auto const &e : now) if (e.key == 250) sees_new = true;
  CHECK(sees_new, "a range taken NOW sees the new key");

  // And the snapshot taken before it must not.
  ds::RangeStats rs2;
  ds::Ranger<FakeOps> ranger(r.ops, rs2);
  std::vector<ds::Entry> old_view;
  // Drive the same walk at the earlier snapshot by asking for a range whose
  // snapshot we pin -- exposed for exactly this test.
  ds::RangeResult const res = ranger.rangeAt(0, 1000, kLayers, 1u << 20,
                                             before, old_view);
  CHECK(res.resolved, "the earlier snapshot still resolves");
  bool sees_old = false;
  for (auto const &e : old_view) if (e.key == 250) sees_old = true;
  CHECK(!sees_old, "and does NOT contain the write that followed it");
}

static void checkANodeCreatedAfterTheSnapshotIsSkipped() {
  // The delicate case. Take a snapshot, then force a SPLIT, which creates a
  // node whose every version is newer than T. The walk visits it on the current
  // chain and must skip it -- serving its keys from the node it split from,
  // whose as-of-T version is wider.
  Rig r;
  for (ds::Key k = 10; k <= 90; k += 10) CHECK(r.put(k, k, 0), "seed");

  uint64_t const before = ds::takeSnapshot(r.ops);

  // A height-1 put splits the data node at 50, creating a new node with
  // k_min = 50 that did not exist at `before`.
  CHECK(r.put(50, 5000, 1), "a height-1 put splits the data level");

  ds::RangeStats rs2;
  ds::Ranger<FakeOps> ranger(r.ops, rs2);
  std::vector<ds::Entry> got;
  CHECK(ranger.rangeAt(0, 100, kLayers, 1u << 20, before, got).resolved,
        "the earlier snapshot resolves across the split");

  // Every seeded key must still be there exactly once, with its OLD value.
  CHECK(got.size() == 9, "all nine seeded keys, none lost and none duplicated");
  for (size_t i = 1; i < got.size(); ++i) {
    CHECK(got[i - 1].key < got[i].key, "still in key order, no duplicates");
  }
  bool old_value_at_50 = false;
  for (auto const &e : got) if (e.key == 50 && e.val == 50) old_value_at_50 = true;
  CHECK(old_value_at_50, "and key 50 reads its pre-split value");
  CHECK(rs2.nodes_skipped > 0, "a node created after the snapshot was skipped");
}

static void checkTruncationIsReportedNotSilent() {
  Rig r;
  for (ds::Key k = 1; k <= 50; ++k) CHECK(r.put(k, k, 0), "seed");
  std::vector<ds::Entry> got;
  ds::RangeResult const res = r.range(0, 1000, got, /*cap=*/10);
  CHECK(res.resolved, "a capped range resolves");
  CHECK(res.capped, "and says it stopped at the cap");
  CHECK(got.size() == 10, "returning exactly the cap");
}

static void checkEmptyAndInvertedRanges() {
  Rig r;
  for (ds::Key k = 100; k <= 300; k += 100) CHECK(r.put(k, k, 0), "seed");
  std::vector<ds::Entry> got;
  CHECK(r.range(400, 500, got).resolved, "a range past the end resolves");
  CHECK(got.empty(), "and is empty");
  got.clear();
  CHECK(r.range(300, 100, got).resolved, "an inverted range resolves");
  CHECK(got.empty(), "and is empty rather than an error");
}

/// Drive the resumable range to completion.
template <class Ops>
static ds::RangeResult runAsyncRange(Ops &ops, ds::Key lo, ds::Key hi,
                                     size_t cap, ds::RangeStats &rs,
                                     std::vector<ds::Entry> &out,
                                     bool batched = false) {
  ds::RangeOperation<Ops> r(ops, kLayers, rs, batched);
  size_t await = r.start(lo, hi, cap, out);
  uint64_t guard = 0;
  while (!r.finished()) {
    (void)await;
    await = r.step();
    if (++guard > 200000) break;
  }
  return r.result();
}

/// Drive the CACHE-sourced walk to completion.
template <class Ops, class Cache>
static ds::RangeResult runCacheRange(Ops &ops, Cache &cache, ds::Key lo,
                                     ds::Key hi, size_t cap,
                                     ds::RangeStats &rs,
                                     std::vector<ds::Entry> &out) {
  ds::RangeOperation<Ops, Cache> r(ops, kLayers, rs, /*batched=*/false, &cache,
                                   /*cache_walk=*/true);
  size_t await = r.start(lo, hi, cap, out);
  uint64_t guard = 0;
  while (!r.finished()) {
    (void)await;
    await = r.step();
    if (++guard > 200000) break;
  }
  return r.result();
}

static void checkCacheWalkAgreesWithTheSerialWalk() {
  // The cache-sourced backbone: locateDataRange hands over the data-node
  // addresses from LOCAL memory, so the walk issues one batched read instead of
  // ~11 dependent round trips. It is an optimisation, so it is not asserted
  // correct on its own terms -- it is pinned to the serial walk, which the
  // differential above pins to the blocking oracle.
  //
  // The cache is attached to the PUTS, so it mirrors the arena the way it does
  // in a real run. A cache populated by hand would test a structure the
  // orchestrator never builds.
  FakeReplicaSet set(3, kLayers, 16384);
  set.setTsMode(ds::TsMode::Faa);
  ds::QuorumStats qs;
  ds::PutStats ps;
  ds::WriteStats ws;

  config cfg("disco-skip", "range cache walk", {"normal"}, "");
  cfg.merge_threshold = 1.0;
  cfg.layers = static_cast<int>(kLayers);
  ds::SkipVec sv(&cfg);
  ds::bootstrapHeads(sv, ds::headAddrs(kLayers));
  ds::CacheAdapter cache(sv, kLayers);

  std::mt19937_64 rng(0xCA11);
  {
    ds::QuorumOps<FakeReplicaSet> ops(set, qs, nullptr);
    for (int i = 0; i < 400; ++i) {
      ds::Key const k = static_cast<ds::Key>(rng() % 4000);
      uint32_t const h = ds::drawHeight(rng, kLayers);
      ds::Putter<ds::QuorumOps<FakeReplicaSet>, ds::CacheAdapter> p(
          ops, cache, kLayers, ps, ws);
      p.put(k, static_cast<ds::Value>(i + 1), h);
    }
  }

  size_t compared = 0, entries = 0;
  uint64_t backbones = 0, addrs = 0, misses = 0, stale = 0;
  for (int trial = 0; trial < 40; ++trial) {
    ds::Key lo = static_cast<ds::Key>(rng() % 4000);
    ds::Key hi = static_cast<ds::Key>(rng() % 4000);
    if (hi < lo) std::swap(lo, hi);

    FakeAsyncOps aops(set, qs, nullptr);
    ds::RangeStats ars;
    std::vector<ds::Entry> agot;
    ds::RangeResult const ares =
        runAsyncRange(aops, lo, hi, 1u << 20, ars, agot);

    FakeAsyncOps cops(set, qs, nullptr);
    ds::RangeStats crs;
    std::vector<ds::Entry> cgot;
    ds::RangeResult const cres =
        runCacheRange(cops, cache, lo, hi, 1u << 20, crs, cgot);

    CHECK(ares.resolved && cres.resolved, "both paths resolve");
    bool same = agot.size() == cgot.size();
    for (size_t i = 0; same && i < agot.size(); ++i) {
      if (agot[i].key != cgot[i].key || agot[i].val != cgot[i].val) same = false;
    }
    CHECK(same, "the cache walk returns exactly what the serial walk did");
    if (!same) {
      std::printf("  [%llu,%llu] serial %zu vs cache %zu "
                  "(backbones=%llu addrs=%llu stale=%llu)\n",
                  static_cast<unsigned long long>(lo),
                  static_cast<unsigned long long>(hi), agot.size(), cgot.size(),
                  static_cast<unsigned long long>(crs.cache_backbones),
                  static_cast<unsigned long long>(crs.cache_addrs),
                  static_cast<unsigned long long>(crs.cache_stale));
      break;
    }
    backbones += crs.cache_backbones;
    addrs += crs.cache_addrs;
    misses += crs.cache_misses;
    stale += crs.cache_stale;
    ++compared;
    entries += cgot.size();
  }

  std::printf("  cache walk: %zu ranges agree, %zu entries\n", compared,
              entries);
  std::printf("  backbones %llu (%llu addrs), %llu misses, %llu stale\n",
              static_cast<unsigned long long>(backbones),
              static_cast<unsigned long long>(addrs),
              static_cast<unsigned long long>(misses),
              static_cast<unsigned long long>(stale));
  // A differential that never took the path it covers passes for the wrong
  // reason: every cache lookup could have missed and the walk been serial.
  CHECK(backbones > 0, "and the cache actually supplied a backbone");
  CHECK(addrs > 0, "and supplied addresses");
}

static void checkAsyncRangeAgreesWithTheBlockingOne() {
  // The differential test, and the reason the state machine is written at all
  // rather than trusted. Two implementations of one algorithm over the same
  // arena must return byte-identical results; a divergence between a blocking
  // path and its async twin is what produced the unbounded-retry bug in
  // ds_put_future.hpp, so the two are pinned together rather than separately
  // asserted correct.
  FakeReplicaSet set(3, kLayers, 16384);
  set.setTsMode(ds::TsMode::Faa);
  ds::QuorumStats qs;
  ds::PutStats ps;
  ds::WriteStats ws;
  ds::NullPutCache cache;

  std::mt19937_64 rng(0xD1FF);
  {
    ds::QuorumOps<FakeReplicaSet> ops(set, qs, nullptr);
    for (int i = 0; i < 300; ++i) {
      ds::Key const k = static_cast<ds::Key>(rng() % 4000);
      uint32_t const h = ds::drawHeight(rng, kLayers);
      ds::Putter<ds::QuorumOps<FakeReplicaSet>, ds::NullPutCache> p(
          ops, cache, kLayers, ps, ws);
      p.put(k, static_cast<ds::Value>(i + 1), h);
    }
  }

  size_t compared = 0, entries = 0;
  uint64_t batch_hits = 0, batch_backs = 0, orphans = 0;
  for (int trial = 0; trial < 30; ++trial) {
    ds::Key lo = static_cast<ds::Key>(rng() % 4000);
    ds::Key hi = static_cast<ds::Key>(rng() % 4000);
    if (hi < lo) std::swap(lo, hi);

    // One snapshot, both paths, so any difference is the walk and not the
    // clock. Blocking first; the arena is quiescent, so order cannot matter.
    ds::QuorumOps<FakeReplicaSet> bops(set, qs, nullptr);
    uint64_t const T = ds::takeSnapshot(bops);

    ds::RangeStats brs;
    std::vector<ds::Entry> bgot;
    ds::Ranger<ds::QuorumOps<FakeReplicaSet>> blocking(bops, brs);
    ds::RangeResult const bres =
        blocking.rangeAt(lo, hi, kLayers, 1u << 20, T, bgot);

    FakeAsyncOps aops(set, qs, nullptr);
    ds::RangeStats ars;
    std::vector<ds::Entry> agot;
    ds::RangeResult const ares =
        runAsyncRange(aops, lo, hi, 1u << 20, ars, agot);

    // The batched walk is the same state machine with --batched-walk on: it
    // takes its backbone from a level-0 index node and fetches up to K data
    // nodes at once. It is an OPTIMISATION, so it is not asserted correct on
    // its own terms -- it is pinned to the serial path, which is pinned to the
    // blocking one. Three implementations, one answer, or the build fails.
    FakeAsyncOps bwops(set, qs, nullptr);
    ds::RangeStats brs2;
    std::vector<ds::Entry> bwgot;
    ds::RangeResult const bwres =
        runAsyncRange(bwops, lo, hi, 1u << 20, brs2, bwgot, /*batched=*/true);

    CHECK(bres.resolved && ares.resolved && bwres.resolved, "all paths resolve");
    CHECK(bgot.size() == agot.size(), "both return the same number of entries");
    bool same = bgot.size() == agot.size();
    for (size_t i = 0; same && i < bgot.size(); ++i) {
      if (bgot[i].key != agot[i].key || bgot[i].val != agot[i].val) same = false;
    }
    CHECK(same, "and the same keys and values in the same order");
    if (!same) {
      std::printf("  [%llu,%llu] blocking %zu vs async %zu\n",
                  static_cast<unsigned long long>(lo),
                  static_cast<unsigned long long>(hi), bgot.size(), agot.size());
      break;
    }

    bool bsame = bwgot.size() == agot.size();
    for (size_t i = 0; bsame && i < agot.size(); ++i) {
      if (bwgot[i].key != agot[i].key || bwgot[i].val != agot[i].val) {
        bsame = false;
      }
    }
    CHECK(bsame, "and the batched walk returns exactly what the serial one did");
    if (!bsame) {
      std::printf("  [%llu,%llu] serial %zu vs batched %zu\n",
                  static_cast<unsigned long long>(lo),
                  static_cast<unsigned long long>(hi), agot.size(),
                  bwgot.size());
      std::printf("    serial : nodes_walked=%llu vec_reads=%llu\n",
                  (unsigned long long)ars.nodes_walked,
                  (unsigned long long)ars.vec_reads);
      std::printf("    batched: nodes_walked=%llu batches=%llu fallbacks=%llu"
                  " misses=%llu orphans=%llu resolved=%d\n",
                  (unsigned long long)brs2.nodes_walked,
                  (unsigned long long)brs2.batches,
                  (unsigned long long)brs2.batch_fallbacks,
                  (unsigned long long)brs2.batch_misses,
                  (unsigned long long)brs2.orphans_walked,
                  (int)bwres.resolved);
      break;
    }
    batch_hits += brs2.batches;
    batch_backs += brs2.batch_fallbacks;
    orphans += brs2.orphans_walked;
    ++compared;
    entries += agot.size();
  }
  std::printf("  differential: %zu ranges agree, %zu entries\n", compared,
              entries);
  std::printf("  batched walk: %llu batches, %llu fallbacks, %llu orphans\n",
              static_cast<unsigned long long>(batch_hits),
              static_cast<unsigned long long>(batch_backs),
              static_cast<unsigned long long>(orphans));
  // A differential test that never took the path it is meant to cover passes
  // for the wrong reason. If the index walk never fired, the two paths agree
  // only because they were the same path.
  CHECK(batch_hits > 0, "and the batched path actually ran");
}

static void checkAStaleCacheStillGivesTheRightAnswer() {
  // A STALE CACHE MUST STILL GIVE THE EXACT ANSWER. The cache learns the
  // structure, then 400 more puts run with a NULL cache, so the arena splits
  // and grows while the cache keeps its old view -- which is what another
  // client doing inserts looks like from here.
  //
  // WHAT GOING STALE DOES, precisely. A node's k_min never changes
  // (ds_range.hpp, fact 1), so every address the cache holds still points at
  // the node it claimed. What it misses is nodes created BETWEEN the entries it
  // holds. So the protection is not the k_min assertion in drainBatch -- that
  // cannot fire, and an earlier version of this test wrongly required it to --
  // but the SUCCESSOR CHECK: a backbone node whose next_id is not the following
  // backbone address has an orphan chain between them, which gets walked.
  //
  // The requirement is therefore the answer itself, plus evidence that the
  // orphan path carried it.
  FakeReplicaSet set(3, kLayers, 16384);
  set.setTsMode(ds::TsMode::Faa);
  ds::QuorumStats qs;
  ds::PutStats ps;
  ds::WriteStats ws;

  config cfg("disco-skip", "stale cache walk", {"normal"}, "");
  cfg.merge_threshold = 1.0;
  cfg.layers = static_cast<int>(kLayers);
  ds::SkipVec sv(&cfg);
  ds::bootstrapHeads(sv, ds::headAddrs(kLayers));
  ds::CacheAdapter cache(sv, kLayers);

  std::mt19937_64 rng(0x57A1E);
  {
    // Phase 1: the cache learns the structure.
    ds::QuorumOps<FakeReplicaSet> ops(set, qs, nullptr);
    for (int i = 0; i < 250; ++i) {
      ds::Key const k = static_cast<ds::Key>(rng() % 4000);
      uint32_t const h = ds::drawHeight(rng, kLayers);
      ds::Putter<ds::QuorumOps<FakeReplicaSet>, ds::CacheAdapter> p(
          ops, cache, kLayers, ps, ws);
      p.put(k, static_cast<ds::Value>(i + 1), h);
    }
  }
  {
    // Phase 2: the arena moves on WITHOUT telling the cache.
    ds::QuorumOps<FakeReplicaSet> ops(set, qs, nullptr);
    ds::NullPutCache blind;
    for (int i = 0; i < 400; ++i) {
      ds::Key const k = static_cast<ds::Key>(rng() % 4000);
      uint32_t const h = ds::drawHeight(rng, kLayers);
      ds::Putter<ds::QuorumOps<FakeReplicaSet>, ds::NullPutCache> p(
          ops, blind, kLayers, ps, ws);
      p.put(k, static_cast<ds::Value>(1000 + i), h);
    }
  }

  size_t compared = 0;
  uint64_t stale = 0, backbones = 0, orphans = 0;
  for (int trial = 0; trial < 40; ++trial) {
    ds::Key lo = static_cast<ds::Key>(rng() % 4000);
    ds::Key hi = static_cast<ds::Key>(rng() % 4000);
    if (hi < lo) std::swap(lo, hi);

    FakeAsyncOps aops(set, qs, nullptr);
    ds::RangeStats ars;
    std::vector<ds::Entry> agot;
    ds::RangeResult const ares =
        runAsyncRange(aops, lo, hi, 1u << 20, ars, agot);

    FakeAsyncOps cops(set, qs, nullptr);
    ds::RangeStats crs;
    std::vector<ds::Entry> cgot;
    ds::RangeResult const cres =
        runCacheRange(cops, cache, lo, hi, 1u << 20, crs, cgot);

    CHECK(ares.resolved && cres.resolved, "both paths resolve over a stale cache");
    bool same = agot.size() == cgot.size();
    for (size_t i = 0; same && i < agot.size(); ++i) {
      if (agot[i].key != cgot[i].key || agot[i].val != cgot[i].val) same = false;
    }
    CHECK(same, "a stale cache still yields exactly the serial answer");
    if (!same) {
      std::printf("  [%llu,%llu] serial %zu vs cache %zu (stale=%llu)\n",
                  static_cast<unsigned long long>(lo),
                  static_cast<unsigned long long>(hi), agot.size(), cgot.size(),
                  static_cast<unsigned long long>(crs.cache_stale));
      break;
    }
    stale += crs.cache_stale;
    backbones += crs.cache_backbones;
    orphans += crs.orphans_walked;
    ++compared;
  }

  std::printf("  stale cache: %zu ranges agree, %llu backbones, "
              "%llu orphan detours, %llu k_min mismatches\n", compared,
              static_cast<unsigned long long>(backbones),
              static_cast<unsigned long long>(orphans),
              static_cast<unsigned long long>(stale));
  // The evidence that the stale case was actually reached: the cache supplied
  // backbones, and the successor check had to detour around nodes it did not
  // know about. Without the second, this is the previous differential with
  // extra puts.
  CHECK(backbones > 0, "the cache supplied backbones despite being stale");
  CHECK(orphans > 0, "and the successor check detoured around unknown nodes");
}

static void checkAsyncSkipsANodeCreatedAfterTheSnapshot() {
  // THE CASE THE DIFFERENTIAL TEST DOES NOT REACH, and the reason this exists
  // separately. That test compares the two paths over a QUIESCENT arena, so no
  // node is created after the snapshot and the as-of-T successor happens to
  // equal the current one. Reintroducing the bug of taking next_id from the
  // as-of-T version instead of the current header therefore passed it cleanly.
  //
  // Here a split lands AFTER the snapshot, so the two successors differ: the
  // as-of-T version's next_id points past the node the split created, and a
  // walk that followed it would skip that node entirely -- losing every key
  // that existed at T and now lives there.
  FakeReplicaSet set(3, kLayers, 16384);
  set.setTsMode(ds::TsMode::Faa);
  ds::QuorumStats qs;
  ds::PutStats ps;
  ds::WriteStats ws;
  ds::NullPutCache cache;

  {
    ds::QuorumOps<FakeReplicaSet> ops(set, qs, nullptr);
    for (ds::Key k = 10; k <= 90; k += 10) {
      ds::Putter<ds::QuorumOps<FakeReplicaSet>, ds::NullPutCache> p(
          ops, cache, kLayers, ps, ws);
      CHECK(p.put(k, k, 0).resolved, "seed");
    }
  }

  ds::QuorumOps<FakeReplicaSet> sops(set, qs, nullptr);
  uint64_t const before = ds::takeSnapshot(sops);

  {
    // Height 1 splits the data node at 50, creating a node newer than `before`.
    ds::QuorumOps<FakeReplicaSet> ops(set, qs, nullptr);
    ds::Putter<ds::QuorumOps<FakeReplicaSet>, ds::NullPutCache> p(
        ops, cache, kLayers, ps, ws);
    CHECK(p.put(50, 5000, 1).resolved, "a height-1 put splits after the snapshot");
  }

  // Blocking, at the old snapshot.
  ds::RangeStats brs;
  std::vector<ds::Entry> bgot;
  ds::Ranger<ds::QuorumOps<FakeReplicaSet>> blocking(sops, brs);
  CHECK(blocking.rangeAt(0, 100, kLayers, 1u << 20, before, bgot).resolved,
        "the blocking path resolves at the old snapshot");

  // Async AT THE SAME OLD SNAPSHOT. A fresh snapshot would be newer than the
  // split, so the old_ver walk would never run and the successor choice could
  // not matter -- which is precisely why startAt exists.
  FakeAsyncOps aops(set, qs, nullptr);
  ds::RangeStats ars;
  std::vector<ds::Entry> agot;
  {
    ds::RangeOperation<FakeAsyncOps> r(aops, kLayers, ars);
    size_t await = r.startAt(0, 100, 1u << 20, before, agot);
    uint64_t guard = 0;
    while (!r.finished()) {
      (void)await;
      await = r.step();
      if (++guard > 200000) break;
    }
    CHECK(r.finished() && r.result().resolved,
          "the async path resolves at the old snapshot across the split");
  }

  // And the BATCHED walk, at that same old snapshot. This is the only place the
  // batch-miss detour runs: the differential test's arena is quiescent, so no
  // node there is ever newer than T or mid-split, and batch_misses stayed 0.
  // Here the post-snapshot split makes a backbone node unanswerable from the
  // batch, which must hand off to the serial path and resume -- not drop it.
  FakeAsyncOps wops(set, qs, nullptr);
  ds::RangeStats wrs;
  std::vector<ds::Entry> wgot;
  {
    ds::RangeOperation<FakeAsyncOps> r(wops, kLayers, wrs, /*batched=*/true);
    size_t await = r.startAt(0, 100, 1u << 20, before, wgot);
    uint64_t guard = 0;
    while (!r.finished()) {
      (void)await;
      await = r.step();
      if (++guard > 200000) break;
    }
    CHECK(r.finished() && r.result().resolved,
          "the batched path resolves at the old snapshot across the split");
  }
  CHECK(wrs.batches > 0, "the batched path actually ran here");
  CHECK(wrs.batch_misses > 0,
        "and the split node fell out of the batch to the serial walk");
  bool wsame = wgot.size() == agot.size();
  for (size_t i = 0; wsame && i < agot.size(); ++i) {
    if (wgot[i].key != agot[i].key || wgot[i].val != agot[i].val) wsame = false;
  }
  CHECK(wsame, "and the batched walk agrees with the serial one across a split");
  if (!wsame) {
    std::printf("    serial %zu vs batched %zu (misses=%llu skipped=%llu)\n",
                agot.size(), wgot.size(),
                (unsigned long long)wrs.batch_misses,
                (unsigned long long)wrs.nodes_skipped);
  }

  CHECK(bgot.size() == 9, "blocking: nine keys, none lost or duplicated");
  CHECK(agot.size() == 9, "async: nine keys, none lost or duplicated");
  for (size_t i = 1; i < agot.size(); ++i) {
    CHECK(agot[i - 1].key < agot[i].key, "async: key order, no duplicates");
  }
  CHECK(brs.nodes_skipped > 0, "blocking skipped the post-snapshot node");

  CHECK(ars.nodes_skipped > 0, "async skipped the post-snapshot node too");

  // Both are at the SAME old snapshot, so they must agree exactly -- and both
  // must read the pre-split value, since the split is newer than T.
  bool same = bgot.size() == agot.size();
  for (size_t i = 0; same && i < bgot.size(); ++i) {
    if (bgot[i].key != agot[i].key || bgot[i].val != agot[i].val) same = false;
  }
  CHECK(same, "blocking and async agree at the same old snapshot");
  bool old_value = false;
  for (auto const &e : agot) if (e.key == 50 && e.val == 50) old_value = true;
  CHECK(old_value, "and key 50 reads its pre-split value in both");
}

static void checkTheViolationCheckerFires() {
  // A10 asks that the range path be checked for violations during the
  // evaluation, and that check is always on. A check nobody has ever seen fail
  // is indistinguishable from a check that cannot fail, so this makes it fail
  // on purpose.
  //
  // The violation is manufactured directly: a snapshot BELOW every stamp in the
  // arena. versionAsOf then finds no version at or before T for any node and
  // skips them all, which is the correct behaviour and yields an empty range --
  // so that alone proves nothing. What does is asking for a snapshot that sits
  // below the bootstrap stamp but above kNullTs, so the walk reaches a node
  // whose only version is newer, and then confirming the entries returned are
  // none rather than wrong.
  Rig r;
  for (ds::Key k = 10; k <= 50; k += 10) CHECK(r.put(k, k, 0), "seed");

  ds::RangeStats rs;
  ds::Ranger<FakeOps> ranger(r.ops, rs);
  std::vector<ds::Entry> got;
  // kBootstrapTs is the lowest stamp any real version carries, so a snapshot
  // one below it can match nothing.
  ds::RangeResult const res =
      ranger.rangeAt(0, 100, kLayers, 1u << 20, ds::kBootstrapTs - 1, got);
  CHECK(res.resolved, "a range below every stamp still resolves");
  CHECK(got.empty(), "and returns nothing rather than something newer");
  CHECK(rs.snapshot_violations == 0,
        "skipping is correct behaviour, not a violation");
  CHECK(rs.nodes_skipped > 0, "every node was skipped as newer than T");

  // Now the real test: hand the predicate a version that is newer than the
  // snapshot and require it to say so. This is the exact call both range
  // implementations make, so a change that weakened it would fail here.
  ds::VecRecord newer{};
  newer.ts = 500;
  CHECK(!ds::versionIsWithin(newer, 499),
        "a version newer than the snapshot is a violation");
  CHECK(ds::versionIsWithin(newer, 500),
        "a version exactly at the snapshot is not");
  ds::VecRecord pending{};
  pending.ts = ds::kNullTs;
  CHECK(!ds::versionIsWithin(pending, 1000),
        "an unstamped version is a violation whatever the snapshot");
}


/// Manufacture a writer that published a version and died before stamping it.
///
/// Uses only the wire operations a real writer uses -- writeVec plus the
/// handle CAS, with NO casTs/faaTs -- so the arena is left in exactly the
/// state a crash between publish and stamp produces. Returns the offset of
/// the pending version and the value it carries.
static ds::VecOffset publishWithoutStamping(FakeReplicaSet &set,
                                            ds::QuorumStats &qs,
                                            ds::Key k, ds::Value v) {
  ds::QuorumOps<FakeReplicaSet> ops(set, qs, nullptr);
  ds::TraversalResult tres;
  {
    ds::RangeStats throwaway;
    (void)throwaway;
    ds::PathStep path[kLayers];
    ds::Traversal<ds::QuorumOps<FakeReplicaSet>> t(ops);
    tres = t.traverse(k, kLayers, path);
  }
  if (tres.status != ds::TraversalStatus::Ok) return ds::kNullVec;

  ds::NodeRecord node{};
  if (!ops.readNode(tres.data_addr, node)) return ds::kNullVec;
  ds::VecRecord cur{};
  if (!ops.readVec(node.handle.offset(), cur)) return ds::kNullVec;

  ds::VecOffset const off = set.allocVec();
  if (off == ds::kNullVec) return ds::kNullVec;

  ds::VecRecord staged = cur;
  staged.struct_ver = node.handle.structVer();
  staged.content_ver = node.handle.contentVer() + 1;
  staged.ts = ds::kNullTs;              // published, not yet stamped
  staged.old_ver = node.handle.offset();
  staged.next_id = ds::kNullId;
  staged.k_min_next = ds::kReservedKey;
  int const idx = ds::findLte(staged, k);
  if (idx >= 0 && staged.keyAt(idx) == k) {
    staged.setValAt(static_cast<uint32_t>(idx), v);
  } else {
    return ds::kNullVec;                // want an update, not a structural put
  }

  ds::Batch b;
  b.writeVec(off, staged);
  b.casHandle(tres.data_addr, node.handle.raw,
              node.handle.withContent(off).raw);
  ds::BatchResult const r = ops.submit(b);
  if (!r.submitted || !r.committed) return ds::kNullVec;
  return off;
}

static void checkFaaHelpersClaimACounterValueNotAClockReading() {
  // THE HELPING PROPERTY, which is what makes reads and ranges lock-free: a
  // reader that finds a published-but-unstamped version fixes the timestamp
  // itself rather than waiting for the writer, so a writer that stalls or dies
  // between its publish and its stamp blocks nobody.
  //
  // In Faa mode a helper MUST claim its value from the counter. It cannot be
  // read locally -- that is the whole shape of Faa mode -- so the help batch
  // carries an faaTs() and a second submission writes back what it claimed.
  // settleNode() (the blocking path) always did this. The three ASYNC helping
  // sites did not: they called ops_.now() in every mode, which in Faa mode is
  // clockNow(), i.e. CLOCK_REALTIME nanoseconds written into a field the
  // snapshot walk compares against counter values.
  //
  // TWO CASES, because the range's own help site is SHADOWED for the first
  // node: RangeOperation routes to `lo` through the traversal, which helps the
  // node it lands on. A node reached by walking RIGHT is handled by
  // RangeOperation::useVersion() instead, and that is the only way to reach
  // the range's copy. Finding this cost one wrong version of this test, which
  // passed against a deliberately broken range site.
  //
  // THE ASSERTION IS THE CHAIN, not the stamp's absolute value: a counter
  // value claimed now exceeds every value claimed before it, so the helped
  // version must outrank the version it supersedes. The fake's now() is
  // `clock_ += 10`, so a clock reading lands around 1e3 while real stamps sit
  // around 1.4e7 -- a clock-stamped helper INVERTS the chain, which is the
  // corruption, and is what this catches.
  for (int site = 0; site < 2; ++site) {
    bool const walk_right = (site == 1);
    char const *what = walk_right ? "range's own site (walked right)"
                                  : "traversal site (node under lo)";
    FakeReplicaSet set(3, kLayers, 16384);
    set.setTsMode(ds::TsMode::Faa);
    ds::QuorumStats qs;
    ds::PutStats ps;
    ds::WriteStats ws;
    ds::NullPutCache cache;

    {
      ds::QuorumOps<FakeReplicaSet> ops(set, qs, nullptr);
      std::mt19937_64 rng(0xBE1F0);
      for (int i = 0; i < 400; ++i) {
        ds::Key const kk = static_cast<ds::Key>(rng() % 2000);
        ds::Putter<ds::QuorumOps<FakeReplicaSet>, ds::NullPutCache> p(
            ops, cache, kLayers, ps, ws);
        p.put(kk, static_cast<ds::Value>(i + 1), ds::drawHeight(rng, kLayers));
      }
      for (ds::Key kk = 100; kk <= 1900; kk += 100) {
        ds::Putter<ds::QuorumOps<FakeReplicaSet>, ds::NullPutCache> p(
            ops, cache, kLayers, ps, ws);
        p.put(kk, kk, 0);
      }
    }

    ds::Key const lo = walk_right ? 100 : 1500;
    ds::Key const target = 1500;
    ds::Value const marooned = 777777;

    // For the walk-right case, require that `lo` and `target` really are in
    // different data nodes -- otherwise the traversal shadows this site again
    // and the test silently stops testing anything.
    if (walk_right) {
      ds::QuorumOps<FakeReplicaSet> ops(set, qs, nullptr);
      ds::PathStep pa[kLayers];
      ds::PathStep pb[kLayers];
      ds::Traversal<ds::QuorumOps<FakeReplicaSet>> t(ops);
      ds::TraversalResult const ra = t.traverse(lo, kLayers, pa);
      ds::TraversalResult const rb = t.traverse(target, kLayers, pb);
      CHECK(ra.status == ds::TraversalStatus::Ok &&
            rb.status == ds::TraversalStatus::Ok,
            "both traversals resolved");
      CHECK(ra.data_addr != rb.data_addr,
            "lo and the pending node are different data nodes");
    }

    ds::VecOffset const pending =
        publishWithoutStamping(set, qs, target, marooned);
    CHECK(pending != ds::kNullVec, "the pending version was published");
    if (pending == ds::kNullVec) continue;

    ds::VecRecord before{};
    CHECK(set.readVecFrom(0, pending, before) && before.isPending(),
          "and really is unstamped");
    uint64_t predecessor_ts = 0;
    {
      ds::VecRecord ov{};
      CHECK(before.old_ver != ds::kNullVec &&
            set.readVecFrom(0, static_cast<ds::VecOffset>(before.old_ver), ov),
            "the pending version supersedes a stamped one");
      predecessor_ts = ov.ts;
    }

    FakeAsyncOps aops(set, qs, nullptr);
    ds::RangeStats ars;
    std::vector<ds::Entry> got;
    ds::RangeResult const res =
        runAsyncRange(aops, lo, target, 1u << 20, ars, got);
    CHECK(res.resolved, "the range resolved");
    CHECK(ars.helped > 0, "and helped the pending version");

    ds::VecRecord after{};
    CHECK(set.readVecFrom(0, pending, after), "the helped version reads back");
    CHECK(!after.isPending(), "the helper fixed the timestamp");
    std::printf("  %s:\n"
                "    ts %llu -> %llu   predecessor %llu   counter %llu\n",
                what,
                static_cast<unsigned long long>(before.ts),
                static_cast<unsigned long long>(after.ts),
                static_cast<unsigned long long>(predecessor_ts),
                static_cast<unsigned long long>(set.readTsCounter()));

    // The corruption, stated as the invariant it breaks.
    CHECK(after.ts > predecessor_ts,
          "a helper's stamp outranks the version it supersedes");
    // And it must be a value the counter could have produced.
    CHECK(after.ts <= ds::tsFromFaa(set.readTsCounter(), ds::kFaaClientMask),
          "a Faa helper's stamp is within the counter's reach");

    // The write was in limbo and its writer never came back. Helping must take
    // it out of limbo: a LATER snapshot sees it. The range that did the helping
    // is NOT required to -- it fixed its snapshot before the value was claimed,
    // so ordering the write after that reader is correct. Asserting otherwise
    // was this test's other wrong version.
    {
      FakeAsyncOps later(set, qs, nullptr);
      ds::RangeStats lrs;
      std::vector<ds::Entry> lgot;
      ds::RangeResult const lres =
          runAsyncRange(later, target, target, 1u << 20, lrs, lgot);
      CHECK(lres.resolved, "a later range resolved");
      CHECK(lrs.helped == 0, "and had nothing left to help");
      bool found = false;
      for (auto const &e : lgot) {
        if (e.key == target) {
          found = true;
          CHECK(e.val == marooned,
                "a range after the help sees the abandoned writer's value");
        }
      }
      CHECK(found, "the helped key is visible to a later range");
    }
  }
}


/// Drive an async put to completion, as async_test.cc's runAsyncPut does.
template <class Ops, class Cache>
static ds::PutResult runAsyncPutHere(Ops &ops, Cache &cache, ds::Key k,
                                     ds::Value v, uint32_t h,
                                     ds::PutStats &ps, ds::WriteStats &ws) {
  ds::PutOperation<Ops, Cache> p(ops, cache, kLayers, ps, ws);
  size_t await = p.start(k, v, h);
  uint64_t guard = 0;
  while (!p.finished()) {
    (void)await;
    await = p.step();
    if (++guard > 200000) break;
  }
  return p.result();
}

/// The put path has its OWN copy of the helping code (ds_put_future.hpp's
/// afterFetch), and it is reached when a write targets a node some other
/// writer left pending. Covered separately because breaking it was caught by
/// nothing: the range test above never drives a put.
static void checkAFaaPutHelpsWithACounterValue() {
  FakeReplicaSet set(3, kLayers, 16384);
  set.setTsMode(ds::TsMode::Faa);
  ds::QuorumStats qs;
  ds::PutStats ps;
  ds::WriteStats ws;
  ds::NullPutCache cache;

  {
    ds::QuorumOps<FakeReplicaSet> ops(set, qs, nullptr);
    std::mt19937_64 rng(0xC0FFEE);
    for (int i = 0; i < 300; ++i) {
      ds::Key const kk = static_cast<ds::Key>(rng() % 2000);
      ds::Putter<ds::QuorumOps<FakeReplicaSet>, ds::NullPutCache> p(
          ops, cache, kLayers, ps, ws);
      p.put(kk, static_cast<ds::Value>(i + 1), ds::drawHeight(rng, kLayers));
    }
    for (ds::Key kk = 100; kk <= 1900; kk += 100) {
      ds::Putter<ds::QuorumOps<FakeReplicaSet>, ds::NullPutCache> p(
          ops, cache, kLayers, ps, ws);
      p.put(kk, kk, 0);
    }
  }

  ds::Key const target = 1500;
  ds::VecOffset const pending = publishWithoutStamping(set, qs, target, 555555);
  CHECK(pending != ds::kNullVec, "the pending version was published");
  if (pending == ds::kNullVec) return;

  ds::VecRecord before{};
  CHECK(set.readVecFrom(0, pending, before) && before.isPending(),
        "and really is unstamped");
  uint64_t predecessor_ts = 0;
  {
    ds::VecRecord ov{};
    CHECK(before.old_ver != ds::kNullVec &&
          set.readVecFrom(0, static_cast<ds::VecOffset>(before.old_ver), ov),
          "it supersedes a stamped version");
    predecessor_ts = ov.ts;
  }

  FakeAsyncOps aops(set, qs, nullptr);
  ds::PutStats aps;
  ds::WriteStats aws;
  ds::NullCache apc;  // PutOperation needs locateData, which NullPutCache lacks
  ds::PutResult const r =
      runAsyncPutHere(aops, apc, target, 4242, 0, aps, aws);
  CHECK(r.resolved, "the put resolved over a pending version");
  // NOT asserting that the PUT's own site did the helping. Like the range's,
  // ds_put_future.hpp's afterFetch() site is shadowed whenever the put
  // traverses: PutStep::Traversing runs the same TraversalFuture, which helps
  // the node it lands on, so afterFetch() finds it already settled. The put's
  // copy is reached when the put skips the traversal -- the cache-hint path,
  // which is ON in every deployed run (0.487 hint hit rate measured) -- or on
  // a cas_lost retry. Driving the hint path needs a populated SkipVec cache
  // rather than NullCache, so this test pins the stamp invariant for whichever
  // site helps and the put's own site is covered only by inspection.
  (void)aws;

  ds::VecRecord after{};
  CHECK(set.readVecFrom(0, pending, after), "the helped version reads back");
  CHECK(!after.isPending(), "the helper fixed the timestamp");
  std::printf("  async put help: ts %llu -> %llu   predecessor %llu\n",
              static_cast<unsigned long long>(before.ts),
              static_cast<unsigned long long>(after.ts),
              static_cast<unsigned long long>(predecessor_ts));
  CHECK(after.ts > predecessor_ts,
        "a put helper's stamp outranks the version it supersedes");
  CHECK(after.ts <= ds::tsFromFaa(set.readTsCounter(), ds::kFaaClientMask),
        "and is within the counter's reach");
}

int main() {
  std::printf("range_test: layers=%u\n", kLayers);
  checkRangeMatchesAnOracle();
  checkAWriteAfterTheSnapshotIsInvisible();
  checkANodeCreatedAfterTheSnapshotIsSkipped();
  checkTruncationIsReportedNotSilent();
  checkEmptyAndInvertedRanges();
  checkAsyncRangeAgreesWithTheBlockingOne();
  checkCacheWalkAgreesWithTheSerialWalk();
  checkAStaleCacheStillGivesTheRightAnswer();
  checkAsyncSkipsANodeCreatedAfterTheSnapshot();
  checkTheViolationCheckerFires();
  checkFaaHelpersClaimACounterValueNotAClockReading();
  checkAFaaPutHelpsWithACounterValue();

  if (g_failures != 0) {
    std::printf("%d FAILURE(S)\n", g_failures);
    return 1;
  }
  std::printf("ALL PASS\n");
  return 0;
}
