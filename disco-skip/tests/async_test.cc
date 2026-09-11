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

#include "ds_get_future.hpp"
#include "ds_put.hpp"
#include "ds_put_future.hpp"
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
  ds::NullCache cache;
  ds::Putter<ds::QuorumOps<FakeReplicaSet>, ds::NullCache> p(ops, cache,
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

// ── A Get as a resumable operation, against the blocking Getter ────────────

/// Drive a resumable Get to completion, as the real driver would.
template <class Ops, class Cache>
static ds::GetResult runAsyncGet(Ops &ops, Cache &cache, ds::Key k,
                                 ds::GetStats &stats) {
  ds::GetOperation<Ops, Cache> g(ops, cache, kLayers, stats);
  size_t await = g.start(k);
  uint64_t guard = 0;
  while (!g.finished()) {
    (void)await;
    await = g.step();
    if (++guard > 100000) break;
  }
  return g.result();
}

static void checkAsyncGetAgreesWithTheBlockingGetter() {
  // Both over a NullCache first, so the comparison is of the traversal path and
  // the answer, with no cache state to diverge.
  FakeReplicaSet set(3, kLayers);
  ds::QuorumStats qs;
  ds::QuorumOps<FakeReplicaSet> ops(set, qs, nullptr);
  ds::PutStats ps;
  ds::WriteStats ws;
  ds::NullCache pcache;
  ds::Putter<ds::QuorumOps<FakeReplicaSet>, ds::NullCache> p(ops, pcache,
                                                                kLayers, ps, ws);
  std::mt19937_64 rng(20260911);
  std::map<ds::Key, ds::Value> oracle;
  for (int i = 0; i < 200; ++i) {
    ds::Key const k = 1 + (rng() % 300);
    ds::Value const v = 1 + (rng() % 100000);
    if (p.put(k, v, ds::drawHeight(rng, kLayers)).resolved) oracle[k] = v;
  }

  size_t agreed = 0, checked = 0;
  for (ds::Key k = 1; k <= 340; ++k) {
    ds::NullCache ca, cb;
    ds::GetStats sa, sb;
    FakeAsyncOps aops(set, qs, nullptr);
    ds::GetResult const a = runAsyncGet(aops, ca, k, sa);

    ds::QuorumStats qs2;
    ds::QuorumOps<FakeReplicaSet> bops(set, qs2, nullptr);
    ds::Getter<ds::QuorumOps<FakeReplicaSet>, ds::NullCache> g(bops, cb, kLayers,
                                                               sb);
    ds::GetResult const b = g.get(k);

    ++checked;
    if (a.resolved == b.resolved && a.found == b.found && a.value == b.value) {
      ++agreed;
    } else {
      std::printf("  DISAGREE on key %llu: async(%d,%d,%llu) blocking(%d,%d,%llu)\n",
                  (unsigned long long)k, a.resolved ? 1 : 0, a.found ? 1 : 0,
                  (unsigned long long)a.value, b.resolved ? 1 : 0,
                  b.found ? 1 : 0, (unsigned long long)b.value);
      break;
    }
  }
  CHECK(agreed == checked, "the async Get agrees with the blocking Getter");

  // And the answers are right, not merely equal.
  size_t right = 0;
  for (auto const &kv : oracle) {
    ds::NullCache c;
    ds::GetStats st;
    FakeAsyncOps aops(set, qs, nullptr);
    ds::GetResult const r = runAsyncGet(aops, c, kv.first, st);
    if (r.resolved && r.found && r.value == kv.second) ++right;
  }
  CHECK(right == oracle.size(), "and returns every written value");
  std::printf("  async Get: %zu keys agree with the blocking path, "
              "%zu values correct\n", agreed, right);
}

static void checkAsyncGetHitsTheCacheAndRepairsIt() {
  // With the real cache attached the fast path matters: a warm cache should
  // answer from one node, and a miss should reconcile so the next Get hits.
  FakeReplicaSet set(1, kLayers);
  ds::QuorumStats qs;
  ds::QuorumOps<FakeReplicaSet> ops(set, qs, nullptr);
  ds::PutStats ps;
  ds::WriteStats ws;
  ds::NullCache pcache;
  ds::Putter<ds::QuorumOps<FakeReplicaSet>, ds::NullCache> p(ops, pcache,
                                                                kLayers, ps, ws);
  std::mt19937_64 rng(4242);
  std::map<ds::Key, ds::Value> oracle;
  for (int i = 0; i < 150; ++i) {
    ds::Key const k = 1 + (rng() % 250);
    ds::Value const v = 1 + (rng() % 100000);
    if (p.put(k, v, ds::drawHeight(rng, kLayers)).resolved) oracle[k] = v;
  }

  ds::NullCache cache;  // stands in where the real cache is not compiled
  ds::GetStats st;
  // Two passes: the second must not be more expensive than the first.
  uint64_t reads_pass1 = 0;
  for (int pass = 0; pass < 2; ++pass) {
    ds::GetStats pass_st;
    for (auto const &kv : oracle) {
      FakeAsyncOps aops(set, qs, nullptr);
      ds::GetResult const r = runAsyncGet(aops, cache, kv.first, pass_st);
      CHECK(r.resolved && r.found && r.value == kv.second,
            "every key resolves on both passes");
    }
    if (pass == 0) reads_pass1 = pass_st.nodes_read + pass_st.vec_reads;
    else {
      CHECK(pass_st.nodes_read + pass_st.vec_reads <= reads_pass1,
            "a second pass costs no more reads than the first");
    }
    st = pass_st;
  }
  CHECK(st.traversals > 0, "the null cache forces a traversal every time");
  CHECK(st.failures == 0, "and nothing fails");
}

// ── A Put as a resumable operation, against the blocking Putter ────────────

template <class Ops, class Cache>
static ds::PutResult runAsyncPut(Ops &ops, Cache &cache, ds::Key k,
                                 ds::Value v, uint32_t h, ds::PutStats &ps,
                                 ds::WriteStats &ws) {
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

/// Build the same structure twice -- once with the async put, once with the
/// blocking one -- and require the two arenas to be structurally identical.
///
/// Stronger than comparing return values: two different structures can both
/// answer every key correctly while disagreeing about where the boundaries
/// fell, and a wrong transition in the climb is exactly that kind of bug.
static void checkAsyncPutBuildsTheSameStructure() {
  FakeReplicaSet a(1, kLayers, 16384);
  FakeReplicaSet b(1, kLayers, 16384);
  ds::QuorumStats qa, qb;
  ds::PutStats psa, psb;
  ds::WriteStats wsa, wsb;
  ds::NullCache ca, cb;

  ds::QuorumOps<FakeReplicaSet> bops(b, qb, nullptr);
  ds::Putter<ds::QuorumOps<FakeReplicaSet>, ds::NullCache> blocking(
      bops, cb, kLayers, psb, wsb);

  std::mt19937_64 rng(777);
  std::map<ds::Key, ds::Value> oracle;
  for (int i = 0; i < 300; ++i) {
    ds::Key const k = 1 + (rng() % 400);
    ds::Value const v = 1 + (rng() % 100000);
    uint32_t const h = ds::drawHeight(rng, kLayers);

    FakeAsyncOps aops(a, qa, nullptr);
    ds::PutResult const ra = runAsyncPut(aops, ca, k, v, h, psa, wsa);
    ds::PutResult const rb = blocking.put(k, v, h);

    CHECK(ra.resolved == rb.resolved, "both paths resolve or fail together");
    CHECK(ra.height == rb.height, "and agree on the applied height");
    if (!ra.resolved) break;
    oracle[k] = v;
  }

  // Both structures must be valid...
  ds::QuorumOps<FakeReplicaSet> aops_v(a, qa, nullptr);
  ds::VerifyReport const va = ds::verifyStructure(aops_v, kLayers);
  ds::VerifyReport const vb = ds::verifyStructure(bops, kLayers);
  if (!va.ok()) {
    for (auto const &e : va.errors) std::printf("  async verifier: %s\n", e.c_str());
  }
  CHECK(va.ok(), "the async-built structure satisfies I1-I4");
  CHECK(vb.ok(), "as does the blocking-built one");

  // ...and identical in shape.
  CHECK(va.nodes_visited == vb.nodes_visited, "same index node count");
  CHECK(va.orphans == vb.orphans, "same orphan count");
  CHECK(va.entries == vb.entries, "same index entry count");
  CHECK(va.data_nodes == vb.data_nodes, "same data node count");
  CHECK(va.data_entries == vb.data_entries, "same data entry count");
  CHECK(va.nodes_per_level == vb.nodes_per_level, "same nodes at every level");

  // And every key reads back its last written value from the async arena.
  size_t right = 0;
  ds::Traversal<ds::QuorumOps<FakeReplicaSet>> t(aops_v);
  ds::PathStep path[ds::kMaxLayers];
  for (auto const &kv : oracle) {
    ds::TraversalResult const r = t.traverse(kv.first, kLayers, path);
    if (r.ok() && r.found && r.value == kv.second) ++right;
  }
  CHECK(right == oracle.size(), "and every key reads back correctly");

  std::printf("  async put: %zu keys, structures identical "
              "(%zu index / %zu data nodes), %llu splits\n",
              oracle.size(), va.nodes_visited, va.data_nodes,
              (unsigned long long)wsa.splits);
}

static void checkAsyncPutHandlesCapacityOverflow() {
  // The one path with a mid-operation detour: a full node splits at its median
  // and the put then resumes against whichever half covers the key.
  FakeReplicaSet set(3, kLayers, 16384);
  ds::QuorumStats qs;
  ds::PutStats ps;
  ds::WriteStats ws;
  ds::NullCache cache;

  for (uint32_t i = 0; i < ds::kNodeCapacity; ++i) {
    FakeAsyncOps aops(set, qs, nullptr);
    CHECK(runAsyncPut(aops, cache, (i + 1) * 10, i, 0, ps, ws).resolved,
          "the data node fills to capacity");
  }
  FakeAsyncOps aops(set, qs, nullptr);
  CHECK(runAsyncPut(aops, cache, 175, 4242, 0, ps, ws).resolved,
        "and one more key still resolves, via a capacity split");
  CHECK(ws.capacity_splits >= 1, "which is counted as a capacity split");

  ds::QuorumOps<FakeReplicaSet> ops(set, qs, nullptr);
  ds::VerifyReport const rep = ds::verifyStructure(ops, kLayers);
  if (!rep.ok()) {
    for (auto const &e : rep.errors) std::printf("  verifier: %s\n", e.c_str());
  }
  CHECK(rep.ok(), "I1-I4 hold after an async capacity split");

  ds::Traversal<ds::QuorumOps<FakeReplicaSet>> t(ops);
  ds::PathStep path[ds::kMaxLayers];
  ds::TraversalResult const r = t.traverse(175, kLayers, path);
  CHECK(r.ok() && r.found && r.value == 4242, "and the key is readable");
}

static void checkAsyncPutIsIdempotentOnARepeatedBoundary() {
  // The property that makes a restart safe. Re-putting the same key at the same
  // height must not create a second node sharing a k_min, and must update the
  // value -- the bug the blocking path had.
  FakeReplicaSet set(3, kLayers, 16384);
  ds::QuorumStats qs;
  ds::PutStats ps;
  ds::WriteStats ws;
  ds::NullCache cache;

  for (int i = 0; i < 5; ++i) {
    FakeAsyncOps aops(set, qs, nullptr);
    CHECK(runAsyncPut(aops, cache, 300, static_cast<ds::Value>(2100 + i), 2, ps, ws)
              .resolved,
          "each repeated structural put resolves");
  }
  ds::QuorumOps<FakeReplicaSet> ops(set, qs, nullptr);
  ds::VerifyReport const rep = ds::verifyStructure(ops, kLayers);
  CHECK(rep.ok(), "I1-I4 hold after repeats");
  CHECK(ws.boundary_noops >= 4, "the repeats are recognised as boundary no-ops");

  ds::Traversal<ds::QuorumOps<FakeReplicaSet>> t(ops);
  ds::PathStep path[ds::kMaxLayers];
  ds::TraversalResult const r = t.traverse(300, kLayers, path);
  CHECK(r.ok() && r.found && r.value == 2104,
        "and the last value written is the one readable");
}

int main() {
  std::printf("async_test: layers=%u\n", kLayers);
  checkEmptyStructure();
  checkAgreementAcrossAPopulatedStructure();
  checkAgreementMidSplit();
  checkSpeculationCollapsesANodeToOneRoundTrip();
  checkAgreementUnderAStaleHint();
  checkAsyncGetAgreesWithTheBlockingGetter();
  checkAsyncGetHitsTheCacheAndRepairsIt();
  checkAsyncPutBuildsTheSameStructure();
  checkAsyncPutHandlesCapacityOverflow();
  checkAsyncPutIsIdempotentOnARepeatedBoundary();

  if (g_failures != 0) {
    std::printf("%d FAILURE(S)\n", g_failures);
    return 1;
  }
  std::printf("ALL PASS\n");
  return 0;
}
