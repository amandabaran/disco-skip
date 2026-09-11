// F1 (plain write) and F2 (split): the write path.
//
// This is where the split descriptor gets *written* rather than only consumed,
// so remote-design.md §2's protocol is what these tests check -- particularly
// the parts a cluster run cannot show: that the propagation window is opened
// and closed in the right order, that a lost handle CAS abandons its staged
// version rather than publishing it, and that a reader arriving mid-split still
// gets the right answer.
//
// The structure verifier runs after most of these, because the cheapest way to
// state "the split left the structure correct" is I1-I4.

#include <cstdio>
#include <map>
#include <random>
#include <set>
#include <string>
#include <vector>

#include "ds_insert.hpp"
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
static ds::RemoteAddr const kData{ds::kInitialDataId};

/// Read a node's live entries, so a test can assert on contents without
/// reaching through the arena by hand.
static std::vector<std::pair<ds::Key, ds::Value>> entriesOf(FakeOps &ops,
                                                            ds::RemoteAddr a) {
  std::vector<std::pair<ds::Key, ds::Value>> out;
  ds::VecRecord const &v = ops.vecOf(a);
  for (uint32_t i = 0; i < v.size; ++i) out.push_back({v.e[i].key, v.e[i].val});
  return out;
}

static bool verifyOk(FakeOps &ops) {
  ds::VerifyReport const r = ds::verifyStructure(ops, kLayers);
  if (!r.ok()) {
    for (auto const &e : r.errors) std::printf("  verifier: %s\n", e.c_str());
  }
  return r.ok();
}

// ── F1 ──────────────────────────────────────────────────────────────────────

static void checkF1InsertsAndUpdates() {
  FakeOps ops = buildInitialArena(kLayers);
  ds::WriteStats st;
  ds::Writer<FakeOps> w(ops, st);

  // Out of order, to exercise the sorted insert rather than an append.
  for (ds::Key k : {ds::Key{300}, ds::Key{100}, ds::Key{500}, ds::Key{200}}) {
    CHECK(w.insertEntry(kData, k, k * 7) == ds::WriteOutcome::Published,
          "F1 publishes");
  }

  auto const e = entriesOf(ops, kData);
  CHECK(e.size() == 4, "four entries land");
  bool sorted = true;
  for (size_t i = 1; i < e.size(); ++i)
    if (e[i - 1].first >= e[i].first) sorted = false;
  CHECK(sorted, "and the vector stays sorted");
  CHECK(st.creates == 4 && st.updates == 0, "all four were new keys");

  // An update must replace in place rather than add a second entry.
  CHECK(w.insertEntry(kData, 300, 999) == ds::WriteOutcome::Published,
        "F1 updates an existing key");
  CHECK(entriesOf(ops, kData).size() == 4, "without growing the vector");
  CHECK(st.updates == 1, "and is counted as an update");
  ds::VecRecord const &v = ops.vecOf(kData);
  int const idx = ds::findLte(v, 300);
  CHECK(idx >= 0 && v.e[idx].key == 300 && v.e[idx].val == 999,
        "and the new value is the one readable");

  CHECK(verifyOk(ops), "I1-I4 hold after F1 writes");
  std::printf("  F1: %llu published (%llu new, %llu updated)\n",
              (unsigned long long)st.published, (unsigned long long)st.creates,
              (unsigned long long)st.updates);
}

static void checkF1IsCopyOnWriteAndChains() {
  FakeOps ops = buildInitialArena(kLayers);
  ds::WriteStats st;
  ds::Writer<FakeOps> w(ops, st);

  ds::VecOffset const first = ops.node(kData).handle.offset();
  uint32_t const content0 = ops.node(kData).handle.contentVer();
  uint32_t const struct0 = ops.node(kData).handle.structVer();

  w.insertEntry(kData, 100, 700);
  ds::VecOffset const second = ops.node(kData).handle.offset();

  CHECK(second != first, "a write moves the vector to a fresh offset");
  CHECK(ops.vecAt(second).old_ver == first, "and chains old_ver to its predecessor");
  CHECK(ops.node(kData).handle.contentVer() == content0 + 1, "content_ver bumps (V1)");
  CHECK(ops.node(kData).handle.structVer() == struct0,
        "struct_ver does not (V3), so the bookends never part");
  CHECK(ops.node(kData).isStable(), "so the node is stable throughout");
  CHECK(ops.vecAt(first).size == 0, "the old version is untouched -- write-once");

  // The descriptor must not be inherited: the old vector had none here, but a
  // version that carried one and has since propagated must not keep it, or
  // rangeEnd() would prefer a stale vector copy forever.
  CHECK(!ops.vecAt(second).hasSplitDescriptor(),
        "a plain write publishes no split descriptor");

  CHECK(st.vec_writes == 1, "one vector written");
  // The round-trip claim: staging, publishing and stamping go out as ONE
  // chained batch rather than three separate operations. The fence is no longer
  // a call of its own -- it is a property the backend puts on the publishing
  // CAS inside the chain (ds_batch.hpp).
  CHECK(ops.batches() == 1, "issued as a single chained batch");
  CHECK(st.batches == 1, "and counted as one");
  CHECK(verifyOk(ops), "I1-I4 hold");
}

static void checkF1StampsBeforeReturning() {
  FakeOps ops = buildInitialArena(kLayers);
  ds::WriteStats st;
  ds::Writer<FakeOps> w(ops, st);

  w.insertEntry(kData, 100, 700);
  ds::VecOffset const off = ops.node(kData).handle.offset();

  // The writer settles its own timestamp rather than leaving it for a helper.
  // This is what makes "the write completed" include its stamp, so a reader
  // starting afterwards cannot be ordered before it.
  CHECK(!ops.vecAt(off).isPending(),
        "the writer stamps its own version before returning");
  CHECK(ops.vecAt(off).ts != ds::kNullTs, "with a real timestamp");
  CHECK(st.helped_ts == 0, "and needed no helping to do it");
}

static void checkF1ReportsFullRatherThanOverflowing() {
  FakeOps ops = buildInitialArena(kLayers);
  ds::WriteStats st;
  ds::Writer<FakeOps> w(ops, st);

  for (uint32_t i = 0; i < ds::kNodeCapacity; ++i) {
    CHECK(w.insertEntry(kData, (i + 1) * 10, i) == ds::WriteOutcome::Published,
          "the node fills to capacity");
  }
  CHECK(ops.vecOf(kData).size == ds::kNodeCapacity, "exactly full");

  // A 17th distinct key has nowhere to go, and must be reported rather than
  // silently dropped or written past the array.
  CHECK(w.insertEntry(kData, 99999, 1) == ds::WriteOutcome::Full,
        "a full node reports Full instead of overflowing");
  CHECK(ops.vecOf(kData).size == ds::kNodeCapacity, "and nothing was written");

  // But an *update* of an existing key still fits, since it adds no entry.
  CHECK(w.insertEntry(kData, 10, 4242) == ds::WriteOutcome::Published,
        "a full node still accepts an update");
  CHECK(verifyOk(ops), "I1-I4 hold at capacity");
}

// ── F2 ──────────────────────────────────────────────────────────────────────

static void checkF2SplitLeavesBothHalvesCorrect() {
  FakeOps ops = buildInitialArena(kLayers);
  ds::WriteStats st;
  ds::Writer<FakeOps> w(ops, st);
  for (ds::Key k : {ds::Key{100}, ds::Key{200}, ds::Key{300}, ds::Key{400}})
    w.insertEntry(kData, k, k * 7);

  ds::NodeRecord const before = ops.node(kData);
  ds::SplitResult const s = w.splitAt(kData, 300, /*orphan=*/true, nullptr);
  CHECK(s.outcome == ds::WriteOutcome::Published, "F2 publishes");
  CHECK(!s.created.isNull(), "and names the node it created");

  auto const lo = entriesOf(ops, kData);
  auto const hi = entriesOf(ops, s.created);
  CHECK(lo.size() == 2 && lo[0].first == 100 && lo[1].first == 200,
        "keys below the split key stay put");
  CHECK(hi.size() == 2 && hi[0].first == 300 && hi[1].first == 400,
        "keys at or above it move across");

  // The partition must be a partition: nothing lost, nothing duplicated.
  CHECK(lo.size() + hi.size() == 4, "the split conserves entries");

  CHECK(ops.node(s.created).k_min == 300, "the new node's range starts at the split key");
  CHECK(ops.node(kData).k_min == before.k_min,
        "and the existing node keeps its own k_min (V2: no k_min change here)");
  CHECK(ops.node(kData).next_id == s.created.id, "the chain now runs through it");
  CHECK(ops.node(kData).next_k_min == 300, "with the bound propagated");
  CHECK(ops.node(s.created).next_id == before.next_id,
        "and the new node inherits the old successor");

  CHECK(ops.node(kData).isStable(), "the window is closed when F2 returns");
  CHECK(ops.node(kData).handle.structVer() == before.handle.structVer() + 1,
        "struct_ver bumped (V2)");
  CHECK(ops.node(kData).handle.contentVer() == before.handle.contentVer(),
        "and content_ver did not (V3)");
  CHECK(!ops.vecOf(kData).isPending() && !ops.vecOf(s.created).isPending(),
        "both vectors are stamped");
  CHECK(verifyOk(ops), "I1-I4 hold after a split");

  std::printf("  F2: split into %zu + %zu entries, %llu node + %llu vec writes\n",
              lo.size(), hi.size(), (unsigned long long)st.node_writes,
              (unsigned long long)st.vec_writes);
}

static void checkF2WritesEverythingBeforePublishing() {
  // The ordering F3 requires: the created node, both vectors, then a fence,
  // and only then the CAS that makes any of it reachable. Checked by counting
  // rather than by intercepting, since the fake cannot reorder.
  FakeOps ops = buildInitialArena(kLayers);
  ds::WriteStats st;
  ds::Writer<FakeOps> w(ops, st);
  for (ds::Key k : {ds::Key{100}, ds::Key{200}, ds::Key{300}}) w.insertEntry(kData, k, k);

  uint64_t const batches_before = ops.batches();
  uint64_t const handle_cas_before = ops.casHandleCalls();
  ds::SplitResult const s = w.splitAt(kData, 200, /*orphan=*/true, nullptr);

  CHECK(s.outcome == ds::WriteOutcome::Published, "split publishes");
  // Two chains, not eleven operations: everything staged plus the publishing
  // CAS, then the five completion CASes. That is the round-trip count F2 costs.
  CHECK(ops.batches() == batches_before + 2, "exactly two chained batches per split");
  CHECK(ops.casHandleCalls() == handle_cas_before + 1, "and one publishing CAS");
  // 3 writes: the created node's vector, its header, and the existing node's
  // new version. All before the CAS.
  CHECK(st.node_writes == 1, "one node header written");
  CHECK(verifyOk(ops), "I1-I4 hold");
}

static void checkF2SeedsAFreshBoundary() {
  // The height-driven case: the split key is not yet an entry, so it has to be
  // placed in the new node -- otherwise the node's k_min would be below every
  // key it holds, and I1 would be the only thing to notice.
  FakeOps ops = buildInitialArena(kLayers);
  ds::WriteStats st;
  ds::Writer<FakeOps> w(ops, st);
  for (ds::Key k : {ds::Key{100}, ds::Key{200}, ds::Key{400}}) w.insertEntry(kData, k, k * 7);

  ds::Entry const seed{300, 2100};
  ds::SplitResult const s = w.splitAt(kData, 300, /*orphan=*/false, &seed);
  CHECK(s.outcome == ds::WriteOutcome::Published, "a seeded split publishes");

  auto const hi = entriesOf(ops, s.created);
  CHECK(hi.size() == 2 && hi[0].first == 300 && hi[1].first == 400,
        "the seed lands as the new node's first entry, in sorted position");
  CHECK(hi[0].second == 2100, "with its value");
  CHECK(ops.node(s.created).k_min == 300, "and is the node's k_min (I1)");
  CHECK(verifyOk(ops), "I1-I4 hold");
}

static void checkF2OnAnExistingBoundaryIsANoop() {
  FakeOps ops = buildInitialArena(kLayers);
  ds::WriteStats st;
  ds::Writer<FakeOps> w(ops, st);
  for (ds::Key k : {ds::Key{100}, ds::Key{200}}) w.insertEntry(kData, k, k);

  ds::NodeRecord const before = ops.node(kData);
  // The data node's k_min is 0, so split at 0 is "already a boundary".
  ds::SplitResult const s = w.splitAt(kData, before.k_min, /*orphan=*/false, nullptr);

  CHECK(s.outcome == ds::WriteOutcome::Published, "reported as done");
  CHECK(s.created == kData, "naming the node that already has that k_min");
  CHECK(st.boundary_noops == 1, "counted as a no-op");
  CHECK(ops.node(kData).handle == before.handle, "and nothing was published");
  CHECK(st.splits == 0, "no split happened");
  CHECK(verifyOk(ops), "I1-I4 hold");
}

static void checkCapacityOverflowProducesAnOrphan() {
  FakeOps ops = buildInitialArena(kLayers);
  ds::WriteStats st;
  ds::Writer<FakeOps> w(ops, st);

  // Fill, then force one more key through the overflow path.
  for (uint32_t i = 0; i < ds::kNodeCapacity; ++i) w.insertEntry(kData, (i + 1) * 10, i);
  ds::RemoteAddr orphan{};
  CHECK(w.insertWithOverflow(kData, 175, 4242, &orphan) == ds::WriteOutcome::Published,
        "overflow splits and then inserts");
  CHECK(!orphan.isNull(), "reporting the orphan it created");
  CHECK(ops.vecOf(orphan).is_orphan == 1,
        "which is flagged I4-orphan, since nothing parents it");

  // The key must be findable afterwards, in whichever half it landed.
  ds::Traversal<FakeOps> d(ops);
  ds::PathStep path[ds::kMaxLayers];
  ds::TraversalResult const r = d.traverse(175, kLayers, path);
  CHECK(r.ok() && r.found && r.value == 4242,
        "and the inserted key is readable through a normal traversal");
  CHECK(verifyOk(ops), "I1-I4 hold after an overflow split");

  std::printf("  overflow: %llu capacity split(s), orphan id %llu\n",
              (unsigned long long)st.capacity_splits,
              (unsigned long long)orphan.id);
}

// ── Concurrency: the states only a hand-built arena can produce ─────────────

static void checkWriteHelpsAnOutstandingSplit() {
  // A writer arriving at a node mid-split must finish the previous operation
  // before starting its own, or it would CAS from a handle whose header links
  // have not landed and publish a version with no descriptor -- stranding the
  // old bound in the header.
  FakeOps ops = buildInitialArena(kLayers);
  for (ds::Key k : {ds::Key{100}, ds::Key{200}, ds::Key{300}, ds::Key{400}})
    seedDataKey(ops, k, k * 7);
  SplitFixture const f = stageMidSplitOn(ops, kData, /*pending_ts=*/true);
  ops.seedAllocators(ds::kFirstDynamicId + 300, ds::kFirstDynamicVec + 300);

  CHECK(!ops.node(f.existing).isStable(), "precondition: the window is open");
  CHECK(ops.vecOf(f.existing).isPending(), "and the timestamp is unfixed");

  ds::WriteStats st;
  ds::Writer<FakeOps> w(ops, st);
  CHECK(w.insertEntry(f.existing, 150, 1050) == ds::WriteOutcome::Published,
        "the write succeeds");

  CHECK(st.helped_ts >= 1, "having stamped the previous writer's version");
  CHECK(st.helped_splits >= 1, "and closed its propagation window");
  CHECK(ops.node(f.existing).isStable(), "so the node ends stable");
  CHECK(ops.node(f.existing).next_id == f.created.id,
        "with the interrupted split's link propagated into the header");
  CHECK(ops.node(f.existing).next_k_min == f.split_key,
        "and its bound too -- the staleness that would otherwise be a wrong answer");

  auto const e = entriesOf(ops, f.existing);
  CHECK(e.size() == 3, "the new key joins the two that stayed behind");
  CHECK(verifyOk(ops), "I1-I4 hold after helping then writing");
}

static void checkLostHandleCasPublishesNothing() {
  // Simulate losing the race: another writer publishes between our read and
  // our CAS. The staged version must be abandoned, not published.
  FakeOps ops = buildInitialArena(kLayers);
  ds::WriteStats st;
  ds::Writer<FakeOps> w(ops, st);
  w.insertEntry(kData, 100, 700);

  // A concurrent writer moves the handle on. Our Writer cannot observe this
  // mid-call in a single thread, so instead check the mechanism directly: a
  // CAS from a stale handle must fail and change nothing.
  ds::Handle const stale = ops.node(kData).handle;
  w.insertEntry(kData, 200, 1400);  // the "other writer"
  ds::Handle const current = ops.node(kData).handle;
  CHECK(stale != current, "the handle moved");

  ds::VecOffset const before = ops.node(kData).handle.offset();
  CHECK(!ops.casHandle(kData, stale.raw, ds::Handle::make(0, 0, 12345).raw),
        "a CAS from the stale handle is rejected");
  CHECK(ops.node(kData).handle.offset() == before,
        "and the published version is unchanged");

  // And a write that retries after losing still converges.
  CHECK(w.insertEntry(kData, 300, 2100) == ds::WriteOutcome::Published,
        "a subsequent write still publishes");
  CHECK(entriesOf(ops, kData).size() == 3, "with all three keys present");
  CHECK(verifyOk(ops), "I1-I4 hold");
}

static void checkReaderMidSplitSeesEveryKey() {
  // The property that matters to a reader: while a split is propagating, every
  // key is still findable -- through the vector's descriptor if the header has
  // not caught up.
  for (bool pending_ts : {false, true}) {
    FakeOps ops = buildInitialArena(kLayers);
    for (ds::Key k : {ds::Key{100}, ds::Key{200}, ds::Key{300}, ds::Key{400}})
      seedDataKey(ops, k, k * 7);
    SplitFixture const f = stageMidSplitOn(ops, kData, pending_ts);

    ds::Traversal<FakeOps> d(ops);
    ds::PathStep path[ds::kMaxLayers];
    for (ds::Key k : {f.stay_key, f.split_key, f.moved_key}) {
      ds::TraversalResult const r = d.traverse(k, kLayers, path);
      CHECK(r.ok(), "a mid-split traversal resolves");
      CHECK(r.found && r.value == k * 7,
            "and finds keys on both sides of an unpropagated split");
    }
  }
}

static void checkManyWritesAgainstAnOracle() {
  // The end-to-end check: drive a long mixed sequence of inserts and updates
  // through the overflow path, and compare every key against a std::map.
  FakeOps ops = buildInitialArena(kLayers, 8192);
  ds::WriteStats st;
  ds::Writer<FakeOps> w(ops, st);
  std::mt19937_64 rng(20260910);
  std::map<ds::Key, ds::Value> oracle;

  // Keys are drawn from a small space so updates and re-splits both happen.
  for (int i = 0; i < 400; ++i) {
    ds::Key const k = 1 + (rng() % 600);
    ds::Value const v = rng() % 1000000;

    ds::Traversal<FakeOps> d(ops);
    ds::PathStep path[ds::kMaxLayers];
    ds::TraversalResult const r = d.traverse(k, kLayers, path);
    if (!r.ok()) {
      CHECK(false, "traversal before an insert should resolve");
      continue;
    }
    ds::WriteOutcome const o = w.insertWithOverflow(r.data_addr, k, v, nullptr);
    if (o != ds::WriteOutcome::Published) {
      CHECK(false, "every insert should publish");
      continue;
    }
    oracle[k] = v;
  }

  CHECK(verifyOk(ops), "I1-I4 hold after a long write sequence");

  size_t checked = 0;
  for (auto const &kv : oracle) {
    ds::Traversal<FakeOps> d(ops);
    ds::PathStep path[ds::kMaxLayers];
    ds::TraversalResult const r = d.traverse(kv.first, kLayers, path);
    CHECK(r.ok(), "every written key resolves");
    CHECK(r.found, "and is found");
    if (r.found) CHECK(r.value == kv.second, "with the value last written");
    ++checked;
  }

  // Absent keys must read as absent, not as a failure or a stale neighbour.
  size_t absent = 0;
  for (ds::Key k = 601; k <= 640; ++k) {
    ds::Traversal<FakeOps> d(ops);
    ds::PathStep path[ds::kMaxLayers];
    ds::TraversalResult const r = d.traverse(k, kLayers, path);
    CHECK(r.ok() && !r.found, "keys never written read as absent");
    ++absent;
  }

  std::printf("  oracle: %zu distinct keys verified, %zu absent probes, "
              "%llu splits (%llu capacity)\n",
              checked, absent, (unsigned long long)st.splits,
              (unsigned long long)st.capacity_splits);
}

int main() {
  std::printf("insert_test: layers=%u capacity=%zu\n", kLayers, ds::kNodeCapacity);
  checkF1InsertsAndUpdates();
  checkF1IsCopyOnWriteAndChains();
  checkF1StampsBeforeReturning();
  checkF1ReportsFullRatherThanOverflowing();
  checkF2SplitLeavesBothHalvesCorrect();
  checkF2WritesEverythingBeforePublishing();
  checkF2SeedsAFreshBoundary();
  checkF2OnAnExistingBoundaryIsANoop();
  checkCapacityOverflowProducesAnOrphan();
  checkWriteHelpsAnOutstandingSplit();
  checkLostHandleCasPublishesNothing();
  checkReaderMidSplitSeesEveryKey();
  checkManyWritesAgainstAnOracle();

  if (g_failures != 0) {
    std::printf("%d FAILURE(S)\n", g_failures);
    return 1;
  }
  std::printf("ALL PASS\n");
  return 0;
}
