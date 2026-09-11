// The resumable traversal, checked against the blocking one.
//
// The decisions here are shared with the blocking path -- rangeEnd, covers,
// findLte, the settle batch -- so what is new and untested is the SEQUENCING:
// which read is issued when, and what is done with each result. Asserting
// properties of the async path alone would not catch a sequencing bug that
// happens to produce a plausible answer.
//
// So the main tool is differential: traverse the same arena with both, and
// require identical results. The blocking Traversal is already validated by
// traverse_test and get_test, which makes it a usable oracle -- and any
// disagreement is a real bug in one of them rather than an argument about what
// the right answer is.

#include <cstdio>
#include <map>
#include <random>
#include <vector>

#include "ds_put.hpp"
#include "ds_verify.hpp"
#include "fake_async.hpp"

static int g_failures = 0;

#define CHECK(cond, msg)                                                      \
  do {                                                                        \
    if (!(cond)) {                                                            \
      std::printf("FAIL: %s  [%s] line %d\n", (msg), #cond, __LINE__);        \
      ++g_failures;                                                           \
    }                                                                         \
  } while (0)

static uint32_t const kLayers = 4;
static ds::RemoteAddr const kData{ds::kInitialDataId};

/// Compare an async traversal against a blocking one over the same arena.
static bool agrees(FakeReplicaSet &set, ds::QuorumStats &qs,
                   ds::VecOffsetHint *hint, ds::Key k, char const *what) {
  ds::PathStep pa[ds::kMaxLayers], pb[ds::kMaxLayers];

  FakeAsyncOps aops(set, qs, hint);
  ds::TraversalResult const a = runAsyncTraversal(aops, k, kLayers, pa);

  ds::QuorumStats qs2;
  ds::QuorumOps<FakeReplicaSet> bops(set, qs2, nullptr);
  ds::Traversal<ds::QuorumOps<FakeReplicaSet>> d(bops);
  ds::TraversalResult const b = d.traverse(k, kLayers, pb);

  bool ok = a.status == b.status && a.found == b.found && a.value == b.value &&
            a.data_addr == b.data_addr && a.data_k_min == b.data_k_min;
  if (ok) {
    for (uint32_t L = 0; L < kLayers; ++L) {
      if (pa[L].k_min != pb[L].k_min || pa[L].addr != pb[L].addr ||
          pa[L].first_down != pb[L].first_down) {
        ok = false;
      }
    }
  }
  if (!ok) {
    std::printf("  DISAGREE on key %llu (%s):\n", (unsigned long long)k, what);
    std::printf("    async:    status=%d found=%d value=%llu data=%llu kmin=%llu\n",
                static_cast<int>(a.status), a.found ? 1 : 0,
                (unsigned long long)a.value, (unsigned long long)a.data_addr.id,
                (unsigned long long)a.data_k_min);
    std::printf("    blocking: status=%d found=%d value=%llu data=%llu kmin=%llu\n",
                static_cast<int>(b.status), b.found ? 1 : 0,
                (unsigned long long)b.value, (unsigned long long)b.data_addr.id,
                (unsigned long long)b.data_k_min);
  }
  return ok;
}

static void checkEmptyStructure() {
  FakeReplicaSet set(1, kLayers);
  ds::QuorumStats qs;
  for (ds::Key k : {ds::Key{0}, ds::Key{1}, ds::Key{42}, ds::Key{1u << 20}}) {
    CHECK(agrees(set, qs, nullptr, k, "empty"),
          "the async traversal agrees on an empty structure");
  }
  // And it must reach the data node and report absent, not fail or miss.
  FakeAsyncOps aops(set, qs, nullptr);
  ds::PathStep path[ds::kMaxLayers];
  ds::TraversalResult const r = runAsyncTraversal(aops, 42, kLayers, path);
  CHECK(r.ok() && !r.found, "reaching the data node and reporting absent");
  CHECK(r.data_addr == kData, "which is the initial data node");
}

static void checkAgreementAcrossAPopulatedStructure() {
  FakeReplicaSet set(3, kLayers);
  ds::QuorumStats qs;
  ds::QuorumOps<FakeReplicaSet> ops(set, qs, nullptr);
  ds::PutStats ps;
  ds::WriteStats ws;
  ds::NullPutCache cache;
  ds::Putter<ds::QuorumOps<FakeReplicaSet>, ds::NullPutCache> p(ops, cache,
                                                                kLayers, ps, ws);
  std::mt19937_64 rng(20260911);
  std::map<ds::Key, ds::Value> oracle;
  for (int i = 0; i < 250; ++i) {
    ds::Key const k = 1 + (rng() % 400);
    ds::Value const v = 1 + (rng() % 100000);
    uint32_t const h = ds::drawHeight(rng, kLayers);
    if (p.put(k, v, h).resolved) oracle[k] = v;
  }
  ds::VerifyReport const rep = ds::verifyStructure(ops, kLayers);
  CHECK(rep.ok(), "the structure is valid before comparing traversals");

  size_t compared = 0;
  for (auto const &kv : oracle) {
    if (!agrees(set, qs, nullptr, kv.first, "present")) break;
    ++compared;
  }
  CHECK(compared == oracle.size(), "both traversals agree on every present key");

  size_t absent = 0;
  for (ds::Key k = 401; k <= 460; ++k) {
    if (!agrees(set, qs, nullptr, k, "absent")) break;
    ++absent;
  }
  CHECK(absent == 60, "and on every absent key");

  // And the async path's answers are actually right, not merely equal.
  FakeAsyncOps aops(set, qs, nullptr);
  size_t found = 0;
  for (auto const &kv : oracle) {
    ds::PathStep path[ds::kMaxLayers];
    ds::TraversalResult const r = runAsyncTraversal(aops, kv.first, kLayers, path);
    if (r.ok() && r.found && r.value == kv.second) ++found;
  }
  CHECK(found == oracle.size(), "and every key reads back its written value");

  std::printf("  differential: %zu present + %zu absent keys agree, "
              "%zu index nodes\n",
              compared, absent, rep.nodes_visited);
}

static void checkAgreementMidSplit() {
  // The states the helping path exists for. Both traversals must settle the node
  // and return the same answer, and the async one must do it across a
  // suspension rather than in a straight line.
  for (bool pending_ts : {false, true}) {
    FakeReplicaSet set(1, kLayers);
    ds::QuorumStats qs;
    for (ds::Key k : {ds::Key{100}, ds::Key{200}, ds::Key{300}, ds::Key{400}}) {
      seedDataKey(set.arena(0), k, k * 7);
    }
    SplitFixture const f = stageMidSplitOn(set.arena(0), kData, pending_ts);

    // Async first, so it is the one that does the helping.
    FakeAsyncOps aops(set, qs, nullptr);
    ds::PathStep path[ds::kMaxLayers];
    for (ds::Key k : {f.stay_key, f.split_key, f.moved_key}) {
      ds::TraversalResult const r = runAsyncTraversal(aops, k, kLayers, path);
      CHECK(r.ok() && r.found && r.value == k * 7,
            "the async traversal resolves keys across an unpropagated split");
    }
    CHECK(set.arena(0).node(f.existing).isStable(),
          "having closed the propagation window");
    CHECK(set.arena(0).node(f.existing).next_id == f.created.id,
          "and propagated the link into the header");
    CHECK(!set.arena(0).vecOf(f.existing).isPending(),
          "and fixed the timestamp");
  }

  // Then the same fixture, comparing both paths on a node that is still split.
  for (bool pending_ts : {false, true}) {
    FakeReplicaSet set(1, kLayers);
    ds::QuorumStats qs;
    for (ds::Key k : {ds::Key{100}, ds::Key{200}, ds::Key{300}, ds::Key{400}}) {
      seedDataKey(set.arena(0), k, k * 7);
    }
    SplitFixture const f = stageMidSplitOn(set.arena(0), kData, pending_ts);
    CHECK(agrees(set, qs, nullptr, f.moved_key, "mid-split"),
          "both paths agree on a key that moved across a live split");
  }
}

static void checkSpeculationCollapsesANodeToOneRoundTrip() {
  // The reason the hint is worth wiring into the async path: with a hit, a node
  // costs one post instead of two, because the vector arrives with the headers.
  FakeReplicaSet set(3, kLayers);
  ds::QuorumStats qs;
  ds::VecOffsetHint hint(ds::kFirstDynamicId + 4096, /*enabled=*/true);

  FakeAsyncOps cold(set, qs, &hint);
  ds::PathStep path[ds::kMaxLayers];
  cold.resetPosts();
  (void)runAsyncTraversal(cold, 42, kLayers, path);
  uint64_t const cold_posts = cold.posts();

  FakeAsyncOps warm(set, qs, &hint);
  warm.resetPosts();
  ds::TraversalResult const r = runAsyncTraversal(warm, 42, kLayers, path);
  uint64_t const warm_posts = warm.posts();

  CHECK(r.ok(), "the warm traversal still resolves");
  CHECK(warm_posts < cold_posts,
        "a warm hint makes the traversal issue strictly fewer posts");
  CHECK(qs.spec_hits > 0, "because the speculations hit");

  // Five nodes on the path (4 index levels + the data node). Cold: header and
  // vector per node. Warm: one post per node.
  CHECK(warm_posts == kLayers + 1,
        "one post per node on the path when every guess hits");
  std::printf("  posts per traversal: %llu cold -> %llu warm (%u nodes)\n",
              (unsigned long long)cold_posts, (unsigned long long)warm_posts,
              kLayers + 1);
}

static void checkAgreementUnderAStaleHint() {
  // A hint that is a version behind must change no answer -- the speculation is
  // rejected and the real vector read happens. This is the case where a
  // sequencing bug would return stale bytes.
  FakeReplicaSet set(3, kLayers);
  ds::QuorumStats qs;
  ds::VecOffsetHint hint(ds::kFirstDynamicId + 4096, /*enabled=*/true);

  ds::QuorumOps<FakeReplicaSet> ops(set, qs, nullptr);
  ds::WriteStats ws;
  ds::Writer<ds::QuorumOps<FakeReplicaSet>> w(ops, ws);

  ds::PathStep path[ds::kMaxLayers];
  for (int i = 0; i < 12; ++i) {
    // Warm the hint, then move the vector under it, then traverse again.
    FakeAsyncOps aops(set, qs, &hint);
    (void)runAsyncTraversal(aops, 100, kLayers, path);
    CHECK(w.insertEntry(kData, 100, static_cast<ds::Value>(700 + i)) == ds::WriteOutcome::Published,
          "a write publishes a new version");
    FakeAsyncOps aops2(set, qs, &hint);
    ds::TraversalResult const r = runAsyncTraversal(aops2, 100, kLayers, path);
    CHECK(r.ok() && r.found && r.value == static_cast<ds::Value>(700 + i),
          "and the traversal returns the new value, never the stale speculation");
  }
  CHECK(qs.spec_misses > 0, "the stale guesses were scored as misses");
}

int main() {
  std::printf("async_test: layers=%u\n", kLayers);
  checkEmptyStructure();
  checkAgreementAcrossAPopulatedStructure();
  checkAgreementMidSplit();
  checkSpeculationCollapsesANodeToOneRoundTrip();
  checkAgreementUnderAStaleHint();

  if (g_failures != 0) {
    std::printf("%d FAILURE(S)\n", g_failures);
    return 1;
  }
  std::printf("ALL PASS\n");
  return 0;
}
