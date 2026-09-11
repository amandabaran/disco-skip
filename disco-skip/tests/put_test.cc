// The write orchestration and Update_Index: height-driven splits across levels.
//
// F1 and F2 are covered in insert_test. What is new here is the *climb* -- that
// a key of height h becomes the minimum of a new node at the data level and at
// levels 0..h-2, an ordinary entry at h-1, and that the down-pointers chain
// correctly between them. I3 is the invariant that says so, and the verifier
// checks it, so most of these assert structural correctness through
// verifyStructure rather than by walking the arena by hand.

#include <cstdio>
#include <map>
#include <random>
#include <set>
#include <string>
#include <vector>

#include "ds_put.hpp"
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

static bool verifyOk(FakeOps &ops, char const *when) {
  ds::VerifyReport const r = ds::verifyStructure(ops, kLayers);
  if (!r.ok()) {
    std::printf("  verifier failed %s:\n", when);
    for (auto const &e : r.errors) std::printf("    %s\n", e.c_str());
  }
  return r.ok();
}

/// Everything a put needs, bundled so each test is three lines of setup.
struct Rig {
  // Sized for the longest workload below: no reclamation, so every write
  // costs a vector and every split costs three.
  FakeOps ops = buildInitialArena(kLayers, 8192);
  ds::PutStats ps;
  ds::WriteStats ws;
  ds::NullPutCache cache;

  ds::PutResult put(ds::Key k, ds::Value v, uint32_t h) {
    ds::Putter<FakeOps, ds::NullPutCache> p(ops, cache, kLayers, ps, ws);
    return p.put(k, v, h);
  }

  bool readsBack(ds::Key k, ds::Value want) {
    ds::Traversal<FakeOps> d(ops);
    ds::PathStep path[ds::kMaxLayers];
    ds::TraversalResult const r = d.traverse(k, kLayers, path);
    return r.ok() && r.found && r.value == want;
  }

  /// How many nodes exist at /level/, by walking the chain from the head.
  size_t nodesAt(uint32_t level) {
    size_t n = 0;
    ds::RemoteAddr a = ds::headAddr(level);
    while (!a.isNull()) {
      ++n;
      a = ds::RemoteAddr{ops.node(a).next_id};
      if (n > 10000) break;
    }
    return n;
  }
};

static void checkHeight0TouchesNoIndexStructure() {
  Rig r;
  size_t const before0 = r.nodesAt(0);
  size_t const before1 = r.nodesAt(1);

  for (ds::Key k : {ds::Key{10}, ds::Key{20}, ds::Key{30}}) {
    ds::PutResult const res = r.put(k, k * 7, /*height=*/0);
    CHECK(res.resolved, "a height-0 put resolves");
  }

  CHECK(r.nodesAt(0) == before0 && r.nodesAt(1) == before1,
        "height 0 creates no index nodes");
  CHECK(r.ps.mirror_calls == 0,
        "and makes no cache call at all -- there is nothing structural to report");
  CHECK(r.ps.height0 == 3, "all three counted as data-layer-only");
  for (ds::Key k : {ds::Key{10}, ds::Key{20}, ds::Key{30}})
    CHECK(r.readsBack(k, k * 7), "and the keys are readable");
  CHECK(verifyOk(r.ops, "after height-0 puts"), "I1-I4 hold");
}

static void checkHeight1MakesADataBoundaryAndADirectoryEntry() {
  Rig r;
  r.put(10, 70, 0);
  r.put(30, 210, 0);

  size_t const idx_before = r.nodesAt(0);
  ds::PutResult const res = r.put(20, 140, /*height=*/1);
  CHECK(res.resolved, "a height-1 put resolves");

  // h == 1 means levels 0..h-2 is empty, so no index node is created; k is
  // simply an entry in the directory pointing at its own new data node.
  CHECK(r.nodesAt(0) == idx_before, "height 1 creates no level-0 node");
  CHECK(r.ps.data_splits == 1, "but does split the data level at k");
  CHECK(r.ps.index_splits == 0, "and performs no index split");
  CHECK(!res.data_addr.isNull(), "reporting the data node k now begins");
  CHECK(r.ops.node(res.data_addr).k_min == 20,
        "whose k_min is k -- that is what makes k a boundary");
  CHECK(r.ps.mirror_calls == 1, "and the change is reported to the cache once");

  for (ds::Key k : {ds::Key{10}, ds::Key{20}, ds::Key{30}})
    CHECK(r.readsBack(k, k * 7), "every key still reads back");
  CHECK(verifyOk(r.ops, "after a height-1 put"), "I1-I4 hold");
}

static void checkHeight2BuildsTheDownChain() {
  Rig r;
  r.put(10, 70, 0);
  r.put(40, 280, 0);

  ds::PutResult const res = r.put(25, 175, /*height=*/2);
  CHECK(res.resolved, "a height-2 put resolves");
  CHECK(r.ps.data_splits == 1, "the data level splits at k");
  CHECK(r.ps.index_splits == 1, "and so does level 0 (levels 0..h-2)");

  // The chain has to be: a level-1 entry keyed k -> a level-0 node with
  // k_min == k -> a data node with k_min == k. I3 is exactly this property,
  // so the verifier is the real check; these assertions localise a failure.
  ds::VecRecord const &l1 = r.ops.vecOf(ds::headAddr(1));
  int const at = ds::findLte(l1, 25);
  CHECK(at >= 0 && l1.e[at].key == 25,
        "level 1 gained an ordinary entry keyed k (level h-1)");
  if (at >= 0 && l1.e[at].key == 25) {
    ds::RemoteAddr const l0{l1.e[at].val};
    CHECK(r.ops.node(l0).k_min == 25, "pointing at a level-0 node whose k_min is k");
    ds::VecRecord const &l0v = r.ops.vecOf(l0);
    CHECK(l0v.size >= 1 && l0v.e[0].key == 25,
          "whose first entry is keyed k as well");
    if (l0v.size >= 1) {
      ds::RemoteAddr const dn{l0v.e[0].val};
      CHECK(r.ops.node(dn).k_min == 25, "and points at the data node k begins");
      CHECK(dn == res.data_addr, "which is the address reported to the cache");
    }
  }

  for (ds::Key k : {ds::Key{10}, ds::Key{25}, ds::Key{40}})
    CHECK(r.readsBack(k, k * 7), "every key reads back");
  CHECK(verifyOk(r.ops, "after a height-2 put"), "I1-I4 hold");
}

static void checkFullHeightClimbsToTheTop() {
  Rig r;
  ds::PutResult const res = r.put(500, 3500, /*height=*/kLayers);
  CHECK(res.resolved, "a full-height put resolves");
  CHECK(r.ps.index_splits == kLayers - 1,
        "creating a node at every level from 0 to layers-2");
  CHECK(r.readsBack(500, 3500), "and the key reads back");
  CHECK(verifyOk(r.ops, "after a full-height put"), "I1-I4 hold");

  // A height above the level count must be clamped rather than running off the
  // end of the path array.
  Rig r2;
  ds::PutResult const res2 = r2.put(500, 3500, /*height=*/kLayers + 5);
  CHECK(res2.resolved, "an over-tall height is clamped, not rejected");
  CHECK(res2.height == kLayers, "to the level count");
  CHECK(verifyOk(r2.ops, "after a clamped put"), "I1-I4 hold");
}

static void checkRepeatedBoundaryIsIdempotent() {
  // The property that makes a restart safe: splitting at a key that is already
  // a node's k_min returns that node instead of creating a second one sharing
  // the same minimum. Without it, a retry after a partially applied climb
  // would build a duplicate structure.
  Rig r;
  CHECK(r.put(300, 2100, 2).resolved, "the first structural put resolves");
  size_t const l0 = r.nodesAt(0);
  size_t const l1 = r.nodesAt(1);
  uint64_t const splits = r.ps.index_splits + r.ps.data_splits;

  for (int i = 0; i < 4; ++i) {
    CHECK(r.put(300, 2100 + i, 2).resolved, "and so does re-putting the same key");
  }

  CHECK(r.nodesAt(0) == l0, "no new level-0 nodes appear");
  CHECK(r.nodesAt(1) == l1, "nor level-1 nodes");
  CHECK(r.ps.index_splits + r.ps.data_splits == splits,
        "and no further splits are performed");
  CHECK(r.ws.boundary_noops >= 4, "the repeats are recognised as no-ops");
  CHECK(r.readsBack(300, 2103), "the last value written is the one readable");
  CHECK(verifyOk(r.ops, "after repeated identical puts"), "I1-I4 hold");
}

static void checkPutOntoAMidSplitNodeHelpsFirst() {
  // A structural put arriving at a node whose previous split has not finished
  // propagating must help it to completion before starting its own.
  FakeOps ops = buildInitialArena(kLayers);
  for (ds::Key k : {ds::Key{100}, ds::Key{200}, ds::Key{300}, ds::Key{400}})
    seedDataKey(ops, k, k * 7);
  SplitFixture const f =
      stageMidSplitOn(ops, ds::RemoteAddr{ds::kInitialDataId}, /*pending_ts=*/true);
  ops.seedAllocators(ds::kFirstDynamicId + 300, ds::kFirstDynamicVec + 300);

  ds::PutStats ps;
  ds::WriteStats ws;
  ds::NullPutCache cache;
  ds::Putter<FakeOps, ds::NullPutCache> p(ops, cache, kLayers, ps, ws);

  CHECK(p.put(150, 1050, /*height=*/1).resolved,
        "a structural put resolves over a mid-split node");
  // The traversal settles a node before it reads its entries, so on this path
  // the helping is charged to PutStats, not to the writer. Either is a pass;
  // what matters is that somebody finished the outstanding operation.
  CHECK(ps.helped >= 1 || ws.helped_ts >= 1 || ws.helped_splits >= 1,
        "having helped the outstanding operation");
  CHECK(ops.node(f.existing).isStable(), "which leaves the node settled");

  ds::Traversal<FakeOps> d(ops);
  ds::PathStep path[ds::kMaxLayers];
  for (ds::Key k : {f.stay_key, f.split_key, f.moved_key}) {
    ds::TraversalResult const r = d.traverse(k, kLayers, path);
    CHECK(r.ok() && r.found, "and every pre-existing key survives");
  }
  ds::TraversalResult const r = d.traverse(150, kLayers, path);
  CHECK(r.ok() && r.found && r.value == 1050, "along with the newly written one");
  CHECK(verifyOk(ops, "after a put over a mid-split node"), "I1-I4 hold");
}

static void checkMixedHeightWorkloadAgainstAnOracle() {
  // The end-to-end check, and the one that exercises the interaction between
  // height-driven splits and capacity overflow. Heights are drawn from the real
  // geometry, so most puts are height 0 and the index grows at roughly the
  // level ratio.
  Rig r;
  std::mt19937_64 rng(20260910);
  std::map<ds::Key, ds::Value> oracle;
  uint64_t heights[ds::kMaxLayers + 1] = {};

  for (int i = 0; i < 600; ++i) {
    ds::Key const k = 1 + (rng() % 900);
    ds::Value const v = 1 + (rng() % 1000000);
    uint32_t const h = ds::drawHeight(rng, kLayers);
    ++heights[h];

    ds::PutResult const res = r.put(k, v, h);
    if (!res.resolved) {
      CHECK(false, "every put should resolve");
      continue;
    }
    oracle[k] = v;
  }

  CHECK(verifyOk(r.ops, "after a mixed-height workload"), "I1-I4 hold");

  size_t found = 0;
  for (auto const &kv : oracle) {
    if (r.readsBack(kv.first, kv.second)) {
      ++found;
    } else {
      CHECK(false, "every written key reads back with its last value");
    }
  }
  CHECK(found == oracle.size(), "all of them");

  for (ds::Key k = 901; k <= 950; ++k) {
    ds::Traversal<FakeOps> d(r.ops);
    ds::PathStep path[ds::kMaxLayers];
    ds::TraversalResult const res = d.traverse(k, kLayers, path);
    CHECK(res.ok() && !res.found, "keys never written read as absent");
  }

  std::printf("  oracle: %zu keys, heights", oracle.size());
  for (uint32_t h = 0; h <= kLayers; ++h)
    std::printf(" h%u=%llu", h, (unsigned long long)heights[h]);
  std::printf("\n  nodes per level:");
  for (uint32_t L = 0; L < kLayers; ++L)
    std::printf(" L%u=%zu", L, r.nodesAt(L));
  std::printf("\n  splits: %llu data, %llu index, %llu capacity; %llu restarts\n",
              (unsigned long long)r.ps.data_splits,
              (unsigned long long)r.ps.index_splits,
              (unsigned long long)r.ws.capacity_splits,
              (unsigned long long)r.ps.restarts);
}

int main() {
  std::printf("put_test: layers=%u capacity=%zu ratio=%zu\n", kLayers,
              ds::kNodeCapacity, ds::kLevelRatio);
  checkHeight0TouchesNoIndexStructure();
  checkHeight1MakesADataBoundaryAndADirectoryEntry();
  checkHeight2BuildsTheDownChain();
  checkFullHeightClimbsToTheTop();
  checkRepeatedBoundaryIsIdempotent();
  checkPutOntoAMidSplitNodeHelpsFirst();
  checkMixedHeightWorkloadAgainstAnOracle();

  if (g_failures != 0) {
    std::printf("%d FAILURE(S)\n", g_failures);
    return 1;
  }
  std::printf("ALL PASS\n");
  return 0;
}
