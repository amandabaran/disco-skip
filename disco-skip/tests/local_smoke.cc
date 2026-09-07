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
  std::printf("cache API: path_reconcile=%s heads_bootstrapped=%s node_count=%s\n",
              ds::kCacheHasPathReconcile ? "yes" : "no",
              ds::kCacheHasHeadsBootstrapped ? "yes" : "no",
              ds::kCacheHasNodeCount ? "yes" : "no");
#endif

  checkSharedDefs();
#if DS_CACHE_ENABLED
  checkCacheContract();
#endif

  std::printf("%s\n", g_failures == 0 ? "ALL PASS" : "FAILURES");
  return g_failures == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
