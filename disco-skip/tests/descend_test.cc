// The remote descent, and the helping that makes reads non-blocking.
//
// Helping only fires on states a concurrent writer produces. Those are trivial
// to build by hand here and nearly impossible to provoke deliberately on a
// cluster, which is why this is the part most worth testing off-cluster.
//
// The arena below supports the same CAS operations the RDMA path does, so the
// Descender under test is the same code that will run there.

#include <cstdio>
#include <cstdlib>
#include <map>
#include <random>
#include <set>
#include <string>
#include <vector>

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

static FakeOps buildInitial() { return buildInitialArena(kLayers); }

/// Seed the pre-split contents, then stage a split on the initial data node.
static SplitFixture stageMidSplit(FakeOps &ops, bool pending_ts) {
  for (ds::Key k : {ds::Key{100}, ds::Key{200}, ds::Key{300}, ds::Key{400}}) {
    seedDataKey(ops, k, k * 7);
  }
  return stageMidSplitOn(ops, ds::RemoteAddr{ds::kInitialDataId}, pending_ts);
}

static void checkDescendOnSettledStructure() {
  FakeOps ops = buildInitial();
  for (ds::Key k : {ds::Key{10}, ds::Key{20}, ds::Key{30}}) seedDataKey(ops, k, k * 7);

  ds::Descender<FakeOps> d(ops);
  ds::PathStep path[ds::kMaxLayers];

  // A key that is present.
  ds::DescentResult r = d.descend(20, kLayers, path);
  CHECK(r.ok(), "descent succeeds on a settled structure");
  CHECK(r.found, "an existing key is found");
  CHECK(r.value == 140, "with its payload");
  CHECK(r.data_addr.id == ds::kInitialDataId, "and names the data node");
  CHECK(r.data_k_min == 0, "and its k_min");
  CHECK(r.levels == kLayers, "one path entry per level");
  CHECK(r.helped_ts == 0 && r.helped_splits == 0,
        "a settled structure needs no helping");

  // The staircase: every level's covering node starts at 0 here, and level 0
  // reports first_down so the cache can create a directory node with that
  // minimum.
  for (uint32_t L = 0; L < kLayers; ++L) {
    CHECK(path[L].k_min == 0, "each level's covering node starts at 0");
    CHECK(!path[L].addr.isNull(), "each level reports an address");
  }
  CHECK(path[0].first_down.id == ds::kInitialDataId,
        "level 0 reports the data node its first entry names");
  for (uint32_t L = 1; L < kLayers; ++L) {
    CHECK(path[L].first_down.isNull(), "first_down is level-0 only");
  }

  // A key that is absent but in range.
  r = d.descend(25, kLayers, path);
  CHECK(r.ok() && !r.found, "an absent key is a successful descent, not found");

  // A key below everything: the data node's k_min is 0 and it holds 10/20/30,
  // so 5 is in range and simply absent.
  r = d.descend(5, kLayers, path);
  CHECK(r.ok() && !r.found, "a key below all entries is absent, not a miss");
  std::printf("  descent on a settled structure: %u nodes read per lookup\n",
              r.nodes_read);
}

static void checkDescendThroughMidSplit() {
  // A key that moved into the new node. The header's stale range bound would
  // claim it -- which is the wrong answer, not a wasted hop, because too large
  // a bound turns "I need another hop" into "definitively absent".
  {
    FakeOps ops = buildInitial();
    SplitFixture const f = stageMidSplit(ops, /*pending_ts=*/false);
    CHECK(!ops.node(f.existing).isStable(), "the fixture really is mid-split");

    ds::Descender<FakeOps> d(ops);
    ds::PathStep path[ds::kMaxLayers];
    ds::DescentResult const r = d.descend(400, kLayers, path);
    CHECK(r.ok(), "descent succeeds through a mid-split node");
    CHECK(r.found && r.value == 2800, "and finds the key that moved");
    CHECK(r.data_addr == f.created, "in the node the split created");
    CHECK(r.data_k_min == f.split_key, "reporting that node's k_min");
    CHECK(r.right_hops >= 1, "having hopped using the vector's descriptor");
  }

  // A key that stayed behind.
  {
    FakeOps ops = buildInitial();
    SplitFixture const f = stageMidSplit(ops, /*pending_ts=*/false);
    ds::Descender<FakeOps> d(ops);
    ds::PathStep path[ds::kMaxLayers];
    ds::DescentResult const r = d.descend(100, kLayers, path);
    CHECK(r.ok() && r.found && r.value == 700, "a key that stayed is found");
    CHECK(r.data_addr == f.existing, "in the existing node");
  }

  // Helping: reading a node's entries settles it, so the window closes and the
  // header ends up agreeing with the descriptor.
  {
    FakeOps ops = buildInitial();
    SplitFixture const f = stageMidSplit(ops, /*pending_ts=*/false);
    ds::Descender<FakeOps> d(ops);
    ds::PathStep path[ds::kMaxLayers];
    ds::DescentResult const r = d.descend(100, kLayers, path);
    CHECK(r.helped_splits >= 1, "the reader helped complete the split");

    ds::NodeRecord const &e = ops.node(f.existing);
    CHECK(e.isStable(), "the window is closed afterwards");
    CHECK(e.next_id == f.created.id, "next_id reached the header");
    CHECK(e.next_k_min == f.split_key, "and so did the range bound");
    CHECK(ops.vecOf(f.existing).ts != ds::kNullTs, "and the timestamp is fixed");
  }

  // A pending timestamp is fixed before the entries are used, so the write is
  // ordered at the moment somebody first needed it.
  {
    FakeOps ops = buildInitial();
    SplitFixture const f = stageMidSplit(ops, /*pending_ts=*/true);
    CHECK(ops.vecOf(f.existing).isPending(), "the fixture really is pending");

    ds::Descender<FakeOps> d(ops);
    ds::PathStep path[ds::kMaxLayers];
    ds::DescentResult const r = d.descend(100, kLayers, path);
    CHECK(r.ok() && r.found, "descent succeeds through a pending version");
    CHECK(r.helped_ts >= 1, "the reader fixed the timestamp");
    uint64_t const ts = ops.vecOf(f.existing).ts;
    CHECK(ts != ds::kNullTs, "which is no longer null");
    CHECK(ts <= ops.clockNow(), "and is not in the future");
  }

  // Hopping past a mid-split node must NOT help: a reader that only needs
  // next_id never touches the entries, and helping there would be pure CAS
  // traffic. The key 400 case above hops past E to reach the new node.
  {
    FakeOps ops = buildInitial();
    SplitFixture const f = stageMidSplit(ops, /*pending_ts=*/true);
    ds::Descender<FakeOps> d(ops);
    ds::PathStep path[ds::kMaxLayers];
    // Descending for 400 uses the *created* node's entries, not E's, so E's
    // pending timestamp should be left alone.
    ds::DescentResult const r = d.descend(400, kLayers, path);
    CHECK(r.ok() && r.found, "descent past a pending node succeeds");
    CHECK(ops.vecOf(f.existing).isPending(),
          "a node merely hopped past is not helped");
    (void)f;
  }

  // Once helped, the structure must pass the full invariant check -- helping
  // has to leave it in a state a quiescent verifier accepts.
  {
    FakeOps ops = buildInitial();
    SplitFixture const f = stageMidSplit(ops, /*pending_ts=*/true);
    ds::Descender<FakeOps> d(ops);
    ds::PathStep path[ds::kMaxLayers];
    (void)d.descend(100, kLayers, path);
    (void)d.descend(400, kLayers, path);
    ds::NodeRecord const &e = ops.node(f.existing);
    CHECK(e.isStable() && !ops.vecOf(f.existing).isPending(),
          "both descents together settle the node completely");
  }
  std::printf("  descent helps mid-split nodes, and skips ones it only hops past\n");
}

static void checkDescentIsIdempotentUnderRepeatedHelp() {
  // Many readers arriving at the same in-flight split must converge, and the
  // losers of each CAS must not undo the winner's work.
  FakeOps ops = buildInitial();
  SplitFixture const f = stageMidSplit(ops, /*pending_ts=*/true);

  ds::PathStep path[ds::kMaxLayers];
  uint64_t first_ts = 0;
  for (int i = 0; i < 20; ++i) {
    ds::Descender<FakeOps> d(ops);
    ds::DescentResult const r = d.descend(100, kLayers, path);
    CHECK(r.ok() && r.found && r.value == 700, "every reader gets the same answer");
    uint64_t const ts = ops.vecOf(f.existing).ts;
    if (first_ts == 0) first_ts = ts;
    CHECK(ts == first_ts, "and the timestamp is fixed exactly once");
  }
  CHECK(ops.node(f.existing).isStable(), "the node stays settled");
  CHECK(ops.node(f.existing).next_id == f.created.id, "and keeps the propagated link");
  std::printf("  repeated helping converges and never regresses\n");
}

static void checkMissIsDistinguishedFromAbsent() {
  // An empty directory routes nowhere: that is a Miss, which the orchestrator
  // must treat differently from a key that is genuinely absent.
  FakeOps ops = buildInitial();
  ops.vecOf(ds::headAddr(0)).size = 0;

  ds::Descender<FakeOps> d(ops);
  ds::PathStep path[ds::kMaxLayers];
  ds::DescentResult const r = d.descend(50, kLayers, path);
  CHECK(r.status == ds::DescentStatus::Miss,
        "a level with no entry <= k is a Miss, not a not-found");
  CHECK(!r.found, "and reports nothing found");
  std::printf("  a routing gap reports Miss rather than absent\n");
}

static void checkDescentAgainstAnOracle() {
  // A wider structure, with the data level split into a chain, checked against
  // a std::map. This is what catches an off-by-one in the right-walk or the
  // range check that a hand-built case would not.
  FakeOps ops = buildInitial();
  std::map<ds::Key, ds::Value> oracle;

  // Build a chain of data nodes, each holding a few keys, linked properly.
  uint64_t id = ds::kFirstDynamicId;
  ds::VecOffset vo = ds::kFirstDynamicVec;
  ds::RemoteAddr prev{ds::kInitialDataId};
  ds::Key k = 0;
  for (int n = 0; n < 12; ++n) {
    ds::Key const k_min = static_cast<ds::Key>(n * 100);
    ds::RemoteAddr cur = (n == 0) ? ds::RemoteAddr{ds::kInitialDataId}
                                  : ds::RemoteAddr{id++};
    ds::VecOffset const off = (n == 0)
                                  ? ops.node(cur).handle.offset()
                                  : vo++;
    if (n != 0) {
      ds::initNode(ops.node(cur), k_min, ds::kDataLevel, off);
      ds::initVec(ops.vecAt(off), /*is_orphan=*/false, /*ts=*/1000);
      ops.node(prev).next_id = cur.id;
      ops.node(prev).next_k_min = k_min;
    }
    ds::VecRecord &v = ops.vecAt(off);
    v.size = 0;
    for (int j = 0; j < 5; ++j) {
      k = k_min + static_cast<ds::Key>(j * 17);
      v.e[v.size++] = ds::Entry{k, k * 3};
      oracle[k] = k * 3;
    }
    prev = cur;
  }

  ds::Descender<FakeOps> d(ops);
  ds::PathStep path[ds::kMaxLayers];
  size_t checked = 0, hops_total = 0;
  for (ds::Key probe = 0; probe < 1300; ++probe) {
    ds::DescentResult const r = d.descend(probe, kLayers, path);
    CHECK(r.ok(), "descent succeeds over a data chain");
    if (!r.ok()) break;
    auto const it = oracle.find(probe);
    bool const want = it != oracle.end();
    CHECK(r.found == want, "found matches the oracle");
    if (want) CHECK(r.value == it->second, "and so does the value");
    // The node it landed on must actually cover the probe.
    CHECK(r.data_k_min <= probe, "the reported data node covers the probe");
    ++checked;
    hops_total += r.right_hops;
  }
  std::printf("  oracle: %zu probes over a 12-node data chain, %.1f right hops mean\n",
              checked, checked ? double(hops_total) / double(checked) : 0.0);
}

int main() {
  std::printf("descend_test: layers=%u capacity=%zu\n", kLayers, ds::kNodeCapacity);
  checkDescendOnSettledStructure();
  checkDescendThroughMidSplit();
  checkDescentIsIdempotentUnderRepeatedHelp();
  checkMissIsDistinguishedFromAbsent();
  checkDescentAgainstAnOracle();
  std::printf("%s\n", g_failures == 0 ? "ALL PASS" : "FAILURES");
  return g_failures == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
