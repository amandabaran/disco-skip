// Point reads, with the real cache attached to a fake arena.
//
// This is the first test where both halves of the project run together: the
// actual skipvector from include/, driven by the actual Get orchestration, over
// a stand-in for the memory servers. The miss-and-reconcile loop cannot be
// exercised any other way without a cluster, and it is the loop that decides
// whether the cache pays for itself.

#include <cstdio>
#include <cstdlib>
#include <map>
#include <random>
#include <vector>

#include "ds_cache.hpp"
#include "ds_get.hpp"
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

/// Build a data-level chain of /nodes/ nodes, each holding /per_node/ keys,
/// with the directory pointing only at the first. That is deliberately the
/// coarse case: the cache and the remote structure start maximally out of step,
/// so every early Get has to descend and reconcile, and we can watch the cost
/// come down.
static std::map<ds::Key, ds::Value> seedChain(FakeOps &ops, int nodes,
                                              int per_node) {
  std::map<ds::Key, ds::Value> oracle;
  uint64_t id = ds::kFirstDynamicId;
  ds::VecOffset vo = ds::kFirstDynamicVec;
  ds::RemoteAddr prev{ds::kInitialDataId};

  for (int n = 0; n < nodes; ++n) {
    ds::Key const k_min = static_cast<ds::Key>(n * 1000);
    ds::RemoteAddr cur =
        (n == 0) ? ds::RemoteAddr{ds::kInitialDataId} : ds::RemoteAddr{id++};
    ds::VecOffset const off =
        (n == 0) ? ops.node(cur).handle.offset() : vo++;
    if (n != 0) {
      ds::initNode(ops.node(cur), k_min, ds::kDataLevel, off);
      ds::initVec(ops.vecAt(off), /*is_orphan=*/false, /*ts=*/1000);
      ops.node(prev).next_id = cur.id;
      ops.node(prev).next_k_min = k_min;
    }
    ds::VecRecord &v = ops.vecAt(off);
    v.size = 0;
    for (int j = 0; j < per_node; ++j) {
      ds::Key const k = k_min + static_cast<ds::Key>(j * 7 + 1);
      v.e[v.size++] = ds::Entry{k, k * 3};
      oracle[k] = k * 3;
    }
    prev = cur;
  }
  return oracle;
}

static void checkGetAgainstOracleWithCache() {
  FakeOps ops = buildInitialArena(kLayers);
  auto const oracle = seedChain(ops, /*nodes=*/24, /*per_node=*/6);

  config cfg("disco-skip", "get test", {"normal"}, "");
  cfg.merge_threshold = 1.0;
  cfg.layers = static_cast<int>(kLayers);
  ds::SkipVec sv(&cfg);
  ds::bootstrapHeads(sv, ds::headAddrs(kLayers));
  ds::CacheAdapter cache(sv);

  ds::GetStats stats;
  ds::Getter<FakeOps, ds::CacheAdapter> g(ops, cache, kLayers, stats);

  // Every present key must be found, and every absent key must be reported
  // absent rather than as a failure.
  for (auto const &kv : oracle) {
    ds::GetResult const r = g.get(kv.first);
    CHECK(r.resolved, "a Get over a settled structure resolves");
    CHECK(r.found, "an existing key is found");
    CHECK(r.value == kv.second, "with the right value");
  }
  size_t absent_checked = 0;
  for (ds::Key k = 0; k < 24000; k += 13) {
    if (oracle.count(k)) continue;
    ds::GetResult const r = g.get(k);
    CHECK(r.resolved, "a Get for an absent key still resolves");
    CHECK(!r.found, "and reports it absent");
    ++absent_checked;
  }
  CHECK(stats.failures == 0, "no Get failed to resolve");
  std::printf("  %zu present + %zu absent keys, all correct\n", oracle.size(),
              absent_checked);
}

static void checkCacheActuallyReducesWork() {
  // The point of the cache is fewer RDMAs per operation. Measure it: run the
  // same key sequence with the real cache and with NullCache, and compare.
  auto run = [](bool with_cache, ds::GetStats &stats) {
    FakeOps ops = buildInitialArena(kLayers);
    auto const oracle = seedChain(ops, /*nodes=*/24, /*per_node=*/6);

    config cfg("disco-skip", "get test", {"normal"}, "");
    cfg.merge_threshold = 1.0;
    cfg.layers = static_cast<int>(kLayers);
    ds::SkipVec sv(&cfg);
    ds::bootstrapHeads(sv, ds::headAddrs(kLayers));
    ds::CacheAdapter cache(sv);
    ds::NullCache null_cache;

    std::vector<ds::Key> keys;
    for (auto const &kv : oracle) keys.push_back(kv.first);
    std::mt19937_64 rng(4242);

    // Several passes, so a warmed cache gets to show its effect.
    for (int pass = 0; pass < 4; ++pass) {
      std::shuffle(keys.begin(), keys.end(), rng);
      for (ds::Key k : keys) {
        if (with_cache) {
          ds::Getter<FakeOps, ds::CacheAdapter> g(ops, cache, kLayers, stats);
          (void)g.get(k);
        } else {
          ds::Getter<FakeOps, ds::NullCache> g(ops, null_cache, kLayers, stats);
          (void)g.get(k);
        }
      }
    }
    return keys.size() * 4;
  };

  ds::GetStats cached, uncached;
  size_t const ops_cached = run(true, cached);
  size_t const ops_uncached = run(false, uncached);

  double const per_op_cached = double(cached.nodes_read) / double(ops_cached);
  double const per_op_uncached = double(uncached.nodes_read) / double(ops_uncached);

  std::printf("  nodes read per Get: %.2f with cache, %.2f without (%.1fx)\n",
              per_op_cached, per_op_uncached,
              per_op_cached > 0 ? per_op_uncached / per_op_cached : 0.0);
  std::printf("  cache: %llu hits, %llu misses, %llu C4 mismatches, %llu reconciles\n",
              (unsigned long long)cached.cache_hits,
              (unsigned long long)cached.cache_misses,
              (unsigned long long)cached.kmin_mismatch,
              (unsigned long long)cached.reconciles);

  CHECK(uncached.cache_hits == 0, "the baseline never hits");
  // Note reconciles is a count of paths handed back, not of cache work done:
  // NullCache receives and ignores them, so the baseline's count matches its
  // descent count rather than being zero. What distinguishes the baseline is
  // that it descends on *every* operation.
  CHECK(uncached.descents == ops_uncached, "the baseline descends on every Get");
  CHECK(cached.descents < ops_cached, "the cache avoids descending on most Gets");
  CHECK(cached.cache_hits > 0, "the cache does hit once warm");
  CHECK(per_op_cached < per_op_uncached,
        "the cache reduces nodes read per Get");
  // Reconciles must taper: the cache is meant to converge on a static
  // structure, not reconcile on every operation forever.
  CHECK(cached.reconciles < ops_cached,
        "reconciles are fewer than operations, so the cache is converging");
}

static void checkC4IsDetectedNotMisanswered() {
  // Point the cache at a node that no longer covers the key, which is exactly
  // what a split by another client leaves behind. The Get must detect it and
  // re-resolve -- never answer "absent" from the wrong node.
  FakeOps ops = buildInitialArena(kLayers);
  auto const oracle = seedChain(ops, /*nodes=*/8, /*per_node=*/6);

  config cfg("disco-skip", "c4", {"normal"}, "");
  cfg.merge_threshold = 1.0;
  cfg.layers = static_cast<int>(kLayers);
  ds::SkipVec sv(&cfg);
  ds::bootstrapHeads(sv, ds::headAddrs(kLayers));
  ds::CacheAdapter cache(sv);

  // Install a deliberately wrong hint: every key in the range claims to live
  // in the first data node.
  for (auto const &kv : oracle) {
    ds::reconcile(sv, kv.first, ds::RemoteAddr{ds::kInitialDataId}, nullptr, 0);
  }

  ds::GetStats stats;
  ds::Getter<FakeOps, ds::CacheAdapter> g(ops, cache, kLayers, stats);
  size_t wrong = 0;
  for (auto const &kv : oracle) {
    ds::GetResult const r = g.get(kv.first);
    if (!r.resolved || !r.found || r.value != kv.second) ++wrong;
  }
  CHECK(wrong == 0, "every key resolves correctly despite a wrong hint");
  CHECK(stats.kmin_mismatch > 0, "and the bad hints were detected as C4");
  std::printf("  %llu C4 mismatches detected, 0 wrong answers\n",
              (unsigned long long)stats.kmin_mismatch);
}

static void checkGetHelpsInFlightWrites() {
  // A Get that lands on a node whose write is still in flight must complete it
  // rather than wait, and must still return the right answer.
  FakeOps ops = buildInitialArena(kLayers);
  (void)seedChain(ops, /*nodes=*/4, /*per_node=*/4);
  SplitFixture const f = stageMidSplitOn(ops, ds::RemoteAddr{ds::kInitialDataId},
                                          /*pending_ts=*/true);

  config cfg("disco-skip", "help", {"normal"}, "");
  cfg.merge_threshold = 1.0;
  cfg.layers = static_cast<int>(kLayers);
  ds::SkipVec sv(&cfg);
  ds::bootstrapHeads(sv, ds::headAddrs(kLayers));
  ds::CacheAdapter cache(sv);

  ds::GetStats stats;
  ds::Getter<FakeOps, ds::CacheAdapter> g(ops, cache, kLayers, stats);
  ds::GetResult const r = g.get(f.stay_key);
  CHECK(r.resolved && r.found, "a Get through an in-flight write resolves");
  CHECK(stats.helped > 0, "having helped the writer finish");
  CHECK(ops.node(f.existing).isStable(), "and left the node settled");
  CHECK(!ops.vecOf(f.existing).isPending(), "with its timestamp fixed");
  std::printf("  a Get completes an in-flight write rather than waiting\n");
}

int main() {
  std::printf("get_test: layers=%u\n", kLayers);
  checkGetAgainstOracleWithCache();
  checkCacheActuallyReducesWork();
  checkC4IsDetectedNotMisanswered();
  checkGetHelpsInFlightWrites();
  std::printf("%s\n", g_failures == 0 ? "ALL PASS" : "FAILURES");
  return g_failures == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
