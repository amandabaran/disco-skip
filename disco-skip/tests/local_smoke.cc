// Local smoke test for the remote side's cluster-independent pieces.
//
// The RDMA half needs libibverbs and a real fabric, so it can only be built and
// run on the cluster. But the parts that decide correctness of the *interface*
// -- the RemoteAddr contract, the cache instantiation, the node layout -- are
// plain C++ and can be checked on a laptop in a second. This is that check.
//
// Build and run every toggle combination with `make -C disco-skip/tests`.
//
// It deliberately does NOT include disco_skip_state.hpp: that pulls in dory,
// which is not available off-cluster. It includes the same headers in the same
// order that disco_skip_state.hpp does, so an ordering or guard mistake still
// shows up here.

#include <cstdio>
#include <cstdlib>
#include <map>
#include <random>
#include <set>

#include "ds_defs.hpp"
#if DS_CACHE_ENABLED
#include "ds_cache.hpp"
#endif

static int g_failures = 0;

#define CHECK(cond, msg)                                                      \
  do {                                                                        \
    if (!(cond)) {                                                            \
      std::printf("FAIL: %s  [%s] line %d\n", (msg), #cond, __LINE__);        \
      ++g_failures;                                                           \
    }                                                                         \
  } while (0)

static void checkSharedDefs() {
  // A4: the two sides' node capacities must match. kNodeCapacity is derived
  // from DS_IDX_EXP the same way the cache derives its own, so this asserts the
  // derivation rather than a hardcoded number.
  CHECK(ds::kNodeCapacity == (size_t{2} << ds::kIdxExp), "capacity derivation");
  CHECK(ds::kLevelRatio == (size_t{1} << ds::kIdxExp), "level ratio derivation");
  CHECK(ds::kNumReplicas == 1 || ds::kNumReplicas == 3, "replica count is 1 or 3");

  // The null-address contract the cache depends on (interface doc §4).
  CHECK(ds::RemoteAddr{}.isNull(), "default-constructed address is null");
  CHECK(!ds::RemoteAddr{1}.isNull(), "id 1 is not null");
  CHECK(ds::kNullAddr == ds::RemoteAddr{}, "kNullAddr is the default");
  CHECK(!static_cast<bool>(ds::RemoteAddr{}), "null is falsy");
  CHECK(static_cast<bool>(ds::RemoteAddr{5}), "non-null is truthy");
}

#if DS_CACHE_ENABLED
// The fidelity property A5 asserts: every node that something points *down to*
// has a minimum that is a real remote boundary. Orphans are exempt -- nothing
// points at them, so their minimum makes no routing claim.
//
// We check the weaker, locally-checkable half of it: no non-orphan node claims
// a minimum we never told the cache about. The full property needs the remote
// structure to compare against, which is the selftest's job once the remote
// side exists.
static size_t countInventedBoundaries(ds::SkipVec const &sv, uint32_t layers,
                                      std::set<ds::Key> const &known) {
  size_t invented = 0;
  for (uint32_t L = 0; L < layers; ++L) {
    sv.for_each_node(static_cast<int>(L),
                     [&](ds::Key k_min, bool is_orphan, size_t entries) {
                       if (is_orphan || entries == 0) return;  // exempt
                       if (known.find(k_min) == known.end()) ++invented;
                     });
  }
  return invented;
}

// A stand-in for a remote descent, in the shape the real one will hand back.
// Boundaries get sparser by kLevelRatio per level, which is the geometry the
// interface doc's §8 analysis and the cache's TARGET_IDX_RATIO both assume.
static uint32_t fakeDescent(ds::Key k, uint32_t layers, ds::PathStep *out) {
  ds::Key stride = ds::kLevelRatio;
  for (uint32_t L = 0; L < layers; ++L) {
    ds::Key const k_min = (k / stride) * stride;
    out[L].k_min = k_min;
    // Distinct, stable, non-null addresses. Stability is the property
    // mirror_reconcile's additivity argument rests on, so the fake must have
    // it: the same k_min at the same level always yields the same address.
    out[L].addr = ds::RemoteAddr{1 + (L * 1000000) + k_min};
    out[L].first_down = (L == 0) ? ds::RemoteAddr{500000000 + k_min}
                                 : ds::RemoteAddr{};
    stride *= ds::kLevelRatio;
  }
  return layers;
}

static void checkReconcilePath() {
  config cfg("disco-skip", "reconcile path", {"normal"}, "");
  cfg.merge_threshold = 1.0;
  cfg.layers = 4;
  uint32_t const layers = static_cast<uint32_t>(cfg.layers);
  ds::SkipVec sv(&cfg);

  std::array<ds::RemoteAddr, ds::kMaxLayers> heads{};
  for (uint32_t L = 0; L < layers; ++L) heads[L] = ds::RemoteAddr{1 + L};
  ds::bootstrapHeads(sv, heads);
  CHECK(ds::headsBootstrapped(sv), "heads report as bootstrapped");

  // Drive the miss -> descend -> reconcile loop the orchestrator will run, and
  // keep an oracle of what locate_data must return for each key.
  std::map<ds::Key, ds::RemoteAddr> oracle;
  std::set<ds::Key> known_boundaries;
  std::mt19937_64 rng(20260907);
  std::uniform_int_distribution<ds::Key> key_dist(1000, 200000);

  for (int i = 0; i < 4000; ++i) {
    ds::Key const k = key_dist(rng);
    if (sv.locate_data(k).isNull() || oracle.find(k) == oracle.end()) {
      ds::PathStep path[ds::kMaxLayers];
      uint32_t const n = fakeDescent(k, layers, path);
      for (uint32_t L = 0; L < n; ++L) known_boundaries.insert(path[L].k_min);

      ds::RemoteAddr const data_addr{900000000 + k};
      ds::reconcile(sv, k, data_addr, path, n);

      // Mirror what reconcile installs at level 0, in its order. It installs
      // the data entry first, then splits the directory at path[0].k_min --
      // and that split creates a node whose first entry is
      // (path[0].k_min -> path[0].first_down). So the boundary is a level-0
      // entry too, and the oracle has to know about it or it will predict the
      // wrong predecessor for keys in between. Inserts are additive (never
      // overwriting), so first writer wins on both.
      if (oracle.find(k) == oracle.end()) oracle[k] = data_addr;
      if (oracle.find(path[0].k_min) == oracle.end()) {
        oracle[path[0].k_min] = path[0].first_down;
      }

      // The data entry the descent installed is a boundary the cache may split
      // at, so it counts as known.
      known_boundaries.insert(k);
    }
  }

  // Every key we reconciled must now resolve to exactly its own data address.
  size_t checked = 0;
  for (auto const &kv : oracle) {
    CHECK(sv.locate_data(kv.first) == kv.second, "reconciled key resolves");
    ++checked;
  }
  std::printf("  reconciled %zu keys, all resolve\n", checked);

  // Gap keys must resolve to the covering (largest <= k) entry.
  for (int i = 0; i < 5000; ++i) {
    ds::Key const k = key_dist(rng);
    auto it = oracle.upper_bound(k);
    ds::RemoteAddr const want =
        (it == oracle.begin()) ? ds::RemoteAddr{} : std::prev(it)->second;
    CHECK(sv.locate_data(k) == want, "gap key resolves to its predecessor");
  }

  // The fidelity property: the cache must not have invented a boundary.
  size_t const invented =
      countInventedBoundaries(sv, layers, known_boundaries);
  CHECK(invented == 0, "no invented boundaries");
  std::printf("  invented boundaries: %zu\n", invented);

  // gather_prevs must now find real addresses rather than nulls, since
  // reconcile has been installing them. Not every level can be non-null (a
  // level whose covering node the descent never reached stays null), but the
  // directory level always has one after a reconcile in range.
  ds::Key const probe = oracle.begin()->first;
  ds::RemoteAddr prevs[ds::kMaxLayers]{};
  ds::gatherPrevs(sv, probe, layers, prevs);
  CHECK(!prevs[0].isNull(), "gather_prevs finds a level-0 address");

  // Metric 2's numerator, and the orphan rate A9 quotes at ~21%.
  auto const counts = ds::nodeCounts(sv, layers);
  for (uint32_t L = 0; L < layers; ++L) {
    ds::LevelStats const st = ds::levelStats(sv, L);
    CHECK(st.nodes == counts[L], "node_count agrees with for_each_node");
    std::printf("  level %u: %zu nodes, %zu orphans (%.0f%%), %zu entries\n", L,
                st.nodes, st.orphans,
                st.nodes ? 100.0 * double(st.orphans) / double(st.nodes) : 0.0,
                st.entries);
  }

  // Fan-out should settle near kLevelRatio; wildly off means the reconcile
  // climb is not behaving like the structure's own geometry.
  if (counts[0] > 0 && counts[1] > 0) {
    double const fanout = double(counts[0]) / double(counts[1]);
    CHECK(fanout > 2.0 && fanout < 32.0, "level 0:1 fan-out is plausible");
    std::printf("  level 0:1 fan-out: %.1f (kLevelRatio = %zu)\n", fanout,
                ds::kLevelRatio);
  }

  // Const-correctness of exactly what DsState::reportCache() const does. That
  // file needs dory and cannot be compiled off-cluster, so the call shapes it
  // uses are pinned here instead.
  {
    ds::SkipVec const &csv = sv;
    CHECK(ds::headsBootstrapped(csv), "headsBootstrapped on a const cache");
    auto const ccounts = ds::nodeCounts(csv, layers);
    for (uint32_t L = 0; L < layers; ++L) {
      ds::LevelStats const st = ds::levelStats(csv, L);
      CHECK(st.nodes == ccounts[L], "levelStats on a const cache");
    }
  }

  CHECK(sv.verify(), "cache verify() after the reconcile loop");
}

static void checkCacheContract() {
  config cfg("disco-skip", "compute-local skip-vector cache", {"normal"}, "");
  cfg.merge_threshold = 1.0;
  cfg.layers = 4;
  ds::SkipVec sv(&cfg);

  // An empty cache must report a miss, not a stale answer. This is the
  // behaviour every fallback path in the orchestrator is built on.
  CHECK(sv.locate_data(42).isNull(), "empty cache reports a miss");

  // Our RemoteAddr must survive a round trip through the cache. Installing an
  // entry and reading it back is the narrowest test of that: it exercises the
  // std::atomic<RemoteAddr> storage and vector_sfra's memcpy relocation, which
  // is what the static_asserts in ds_remote_addr.hpp exist to protect.
  ds::RemoteAddr const data_addr{4242};
  sv.mirror_reconcile(1000, data_addr);
  CHECK(sv.locate_data(1000) == data_addr, "reconciled entry reads back");
  CHECK(sv.locate_data(1007) == data_addr, "entry covers keys above it");
  CHECK(sv.locate_data(999).isNull(), "key below the entry still misses");

  // Additivity: reconcile must never overwrite (interface doc §5).
  sv.mirror_reconcile(1000, ds::RemoteAddr{9999});
  CHECK(sv.locate_data(1000) == data_addr, "reconcile does not overwrite");

  // Bootstrap installs the head addresses. Before it, anything left of the
  // first boundary is a guaranteed miss; that is the behaviour we assert on.
  std::array<ds::RemoteAddr, ds::kMaxLayers> heads{};
  for (size_t i = 0; i < static_cast<size_t>(cfg.layers); ++i) {
    heads[i] = ds::RemoteAddr{i + 1};
  }
  sv.set_head_remote_addrs(heads);
  CHECK(ds::headsBootstrapped(sv), "heads report as bootstrapped");

  CHECK(sv.verify(), "cache verify() after the round trip");
}
#endif

int main() {
  std::printf("DS_CACHE_ENABLED=%d DS_N_REPLICAS=%zu layers_cap=%zu capacity=%zu\n",
              DS_CACHE_ENABLED, ds::kNumReplicas, ds::kMaxLayers, ds::kNodeCapacity);
#if DS_CACHE_ENABLED
  // The A5 API is now a static_assert in ds_cache.hpp rather than a runtime
  // capability, so there is nothing to report here: if this compiled, it exists.
  std::printf("cache API: A5 reconcile path present (asserted at compile time)\n");
#endif

  checkSharedDefs();
#if DS_CACHE_ENABLED
  checkCacheContract();
  checkReconcilePath();
#endif

  std::printf("%s\n", g_failures == 0 ? "ALL PASS" : "FAILURES");
  return g_failures == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
