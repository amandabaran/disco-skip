// CAS-ABD over three replicas: the quorum read, the 2-of-3 commit, writeback.
//
// Every case here is one replica disagreeing with two others. That is the whole
// content of replication and none of it is reachable on a healthy cluster, so
// this file is where the correctness actually gets established -- the cluster
// can only confirm that three connections work.
//
// The last two tests are the ones that matter most: the full Traversal/Writer/
// Putter stack running unchanged on top of QuorumOps, because the entire design
// claim is that replication sits behind the Ops surface and the algorithms above
// it neither know nor care.

#include <cstdio>
#include <map>
#include <random>
#include <string>
#include <vector>

#include "ds_put.hpp"
#include "ds_quorum.hpp"
#include "ds_verify.hpp"
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
static ds::RemoteAddr const kData{ds::kInitialDataId};

struct Rig {
  FakeReplicaSet set;
  ds::QuorumStats qs;
  ds::QuorumOps<FakeReplicaSet> ops;

  explicit Rig(size_t replicas = 3)
      : set(replicas, kLayers), ops(set, qs) {}

  bool verifyOk(char const *when) {
    ds::VerifyReport const r = ds::verifyStructure(ops, kLayers);
    if (!r.ok()) {
      std::printf("  verifier failed %s:\n", when);
      for (auto const &e : r.errors) std::printf("    %s\n", e.c_str());
    }
    return r.ok();
  }
};

// ── The quorum read: L2, L3, L4 ─────────────────────────────────────────────

static void checkMajorityIsComputedFromTheReplicaCount() {
  Rig r3(3);
  CHECK(r3.ops.majority() == 2, "3 replicas need 2 to commit");
  Rig r1(1);
  CHECK(r1.ops.majority() == 1, "and 1 replica needs 1, so N=1 is the same path");
}

static void checkReadPrefersTheHigherTagWhenAMajorityHoldsIt() {
  Rig r;
  // Commit a write the normal way, then knock one replica back to the old
  // handle -- the state a commit that reached exactly 2 of 3 leaves behind.
  ds::WriteStats ws;
  ds::Writer<ds::QuorumOps<FakeReplicaSet>> w(r.ops, ws);
  CHECK(w.insertEntry(kData, 100, 700) == ds::WriteOutcome::Published,
        "the write commits");

  ds::Handle const committed = r.set.arena(0).node(kData).handle;
  ds::Handle const older = ds::Handle::make(committed.structVer(),
                                            committed.contentVer() - 1,
                                            /*offset=*/kData.id);
  r.set.forceHandle(2, kData, older);
  CHECK(r.set.votesFor(kData, committed) == 2, "two replicas hold the new handle");

  ds::NodeRecord node;
  CHECK(r.ops.readNode(kData, node), "the quorum read succeeds");
  CHECK(node.handle == committed,
        "and returns the committed handle, not the lagged one");
  CHECK(r.qs.stale_votes >= 1, "counting the replica that voted below it");
}

static void checkReadIgnoresAHigherTagWithoutMajoritySupport() {
  // A partially applied commit: one replica holds a *higher* tag that no
  // majority ever accepted. Taking the max tag would return contents that were
  // never committed, so it must be ignored in favour of the majority value.
  Rig r;
  ds::Handle const original = r.set.arena(0).node(kData).handle;
  ds::Handle const uncommitted =
      ds::Handle::make(original.structVer(), original.contentVer() + 5, 999);
  r.set.forceHandle(1, kData, uncommitted);

  CHECK(r.set.votesFor(kData, original) == 2, "two replicas hold the original");
  CHECK(uncommitted.tag() > original.tag(), "and the odd one out has a higher tag");

  ds::NodeRecord node;
  CHECK(r.ops.readNode(kData, node), "the read still succeeds");
  CHECK(node.handle == original,
        "returning the majority-held handle rather than the highest tag");
}

static void checkReadRetriesWhenNoValueHasMajority() {
  // Three different handles, so nothing has majority support. That is a
  // transient mid-commit state, and the read must report failure for the caller
  // to retry rather than guess.
  Rig r;
  ds::Handle const base = r.set.arena(0).node(kData).handle;
  r.set.forceHandle(1, kData, ds::Handle::make(base.structVer(), 7, 111));
  r.set.forceHandle(2, kData, ds::Handle::make(base.structVer(), 9, 222));

  ds::NodeRecord node;
  CHECK(!r.ops.readNode(kData, node),
        "a state with no majority-supported handle is reported, not resolved");
  CHECK(r.qs.read_retries >= 1, "having re-polled first");
  CHECK(r.qs.read_failures >= 1, "and then given up for the caller to retry");
}

static void checkReadSurvivesOneDownReplica() {
  Rig r;
  r.set.setDown(2, true);

  ds::NodeRecord node;
  CHECK(r.ops.readNode(kData, node),
        "a majority read tolerates one fail-stopped replica");

  ds::VecRecord vec;
  CHECK(r.ops.readVec(node.handle.offset(), vec), "and so does the vector read");

  // Two down is short of majority and must fail rather than return something.
  r.set.setDown(1, true);
  CHECK(!r.ops.readNode(kData, node),
        "but two down is short of majority and fails");
}

static void checkVectorReadHonoursL4() {
  // L4: the vector is read from a replica that voted with the winning handle.
  // Staged by giving replica 0 a handle no majority holds, so the winner must
  // be one of the other two -- and then breaking replica 0's vector so that
  // reading it would be detectable.
  Rig r;
  ds::Handle const base = r.set.arena(0).node(kData).handle;
  r.set.forceHandle(0, kData, ds::Handle::make(base.structVer(), 7, 111));

  ds::NodeRecord node;
  CHECK(r.ops.readNode(kData, node), "the read succeeds");
  CHECK(node.handle == base, "picking the majority handle");

  // Corrupt the loser's copy of the winning vector. A read that honours L4
  // never touches it.
  r.set.arena(0).vecAt(base.offset()).size = 99;

  ds::VecRecord vec;
  CHECK(r.ops.readVec(node.handle.offset(), vec), "the vector read succeeds");
  CHECK(vec.size != 99, "reading from a replica that voted with the winner");
}

// ── The commit: L1 and L5 ───────────────────────────────────────────────────

static void checkCommitNeedsMajorityAndRepairsTheRest() {
  Rig r;
  // Commit a couple of real writes first, so there is a genuinely *older*
  // handle to lag at. Bootstrap starts at content_ver 0, so staging a lagged
  // replica before any write means inventing a handle that is really ahead --
  // which writeback correctly refuses to touch, as the next test asserts.
  ds::WriteStats ws0;
  ds::Writer<ds::QuorumOps<FakeReplicaSet>> w0(r.ops, ws0);
  CHECK(w0.insertEntry(kData, 10, 70) == ds::WriteOutcome::Published, "first write");
  ds::Handle const older = r.set.arena(0).node(kData).handle;
  CHECK(w0.insertEntry(kData, 20, 140) == ds::WriteOutcome::Published, "second write");

  ds::NodeRecord node;
  CHECK(r.ops.readNode(kData, node), "read the current handle");
  ds::Handle const from = node.handle;
  ds::Handle const to = from.withContent(ds::kFirstDynamicVec + 1);
  CHECK(older.tag() < to.tag(), "the staged lag is genuinely behind");

  // Knock one replica back to that older handle, so its CAS must fail.
  r.set.forceHandle(2, kData, older);
  uint64_t const commits_before = r.qs.commits;

  ds::Batch b;
  b.casHandle(kData, from.raw, to.raw);
  CHECK(r.ops.submit(b).committed, "two of three is a commit (L1)");
  CHECK(r.qs.commits == commits_before + 1, "counted as committed");

  // L5: the lagged replica is repaired, so the next commit does not depend on
  // the other two alone.
  CHECK(r.set.votesFor(kData, to) == 3,
        "and the lagged replica is written back (L5)");
  CHECK(r.qs.writebacks >= 1, "counted as a writeback");
  CHECK(r.set.agreeOn(kData), "so all three agree afterwards");
}

static void checkCommitFailsBelowMajorityAndIsRecoverable() {
  Rig r;
  ds::NodeRecord node;
  CHECK(r.ops.readNode(kData, node), "read the current handle");
  ds::Handle const from = node.handle;

  // Two replicas moved on, so only one can accept our expected value.
  ds::Handle const elsewhere = from.withContent(4242);
  r.set.forceHandle(1, kData, elsewhere);
  r.set.forceHandle(2, kData, elsewhere);

  ds::Handle const to = from.withContent(ds::kFirstDynamicVec + 1);
  ds::Batch b;
  b.casHandle(kData, from.raw, to.raw);
  CHECK(!r.ops.submit(b).committed, "one of three is not a commit");
  CHECK(r.qs.commits_lost == 1, "counted as lost");
  CHECK(r.qs.partial_commits == 1, "and as partial, since one replica took it");

  // The partial write is not rolled back, but it is recoverable: a majority
  // still holds `elsewhere`, so a read resolves to that and a writer can
  // proceed from it.
  ds::NodeRecord after;
  CHECK(r.ops.readNode(kData, after), "a read still resolves");
  CHECK(after.handle == elsewhere,
        "to the value a majority holds, not the partial one");
}

static void checkCommitDoesNotClobberANewerReplica() {
  // Writeback must not overwrite a replica that has moved *ahead* of the value
  // being committed, or a slow committer would undo a newer write.
  Rig r;
  ds::NodeRecord node;
  CHECK(r.ops.readNode(kData, node), "read the current handle");
  ds::Handle const from = node.handle;
  ds::Handle const to = from.withContent(ds::kFirstDynamicVec + 1);
  ds::Handle const newer = ds::Handle::make(from.structVer(), 500, 888);

  r.set.forceHandle(2, kData, newer);
  ds::Batch b;
  b.casHandle(kData, from.raw, to.raw);
  CHECK(r.ops.submit(b).committed, "the commit reaches majority");
  CHECK(r.set.arena(2).node(kData).handle == newer,
        "and writeback leaves the newer replica alone");
}

static void checkHelperCasesReachEveryReplica() {
  // The helping CASes are not the linearization point, so there is no majority
  // to reach -- but the field has to arrive everywhere or the replicas never
  // converge.
  Rig r;
  ds::NodeRecord node;
  CHECK(r.ops.readNode(kData, node), "read the node");
  ds::VecOffset const off = node.handle.offset();

  // Make the vector pending on every replica, then settle it.
  for (size_t i = 0; i < 3; ++i) r.set.arena(i).vecAt(off).ts = ds::kNullTs;
  ds::Batch b;
  b.casTs(off, ds::kNullTs, 4242);
  CHECK(r.ops.submit(b).submitted, "a helping batch submits");
  for (size_t i = 0; i < 3; ++i) {
    CHECK(r.set.arena(i).vecAt(off).ts == 4242,
          "and lands on every replica, not just a majority");
  }
}

// ── The whole stack, unchanged, over three replicas ─────────────────────────

static void checkTheWritePathRunsUnchangedOverThreeReplicas() {
  // The design claim: replication is behind the Ops surface, so Writer and
  // Putter are the same code. If this needed any change to them, the claim
  // would be false.
  Rig r;
  ds::PutStats ps;
  ds::WriteStats ws;
  ds::NullPutCache cache;
  ds::Putter<ds::QuorumOps<FakeReplicaSet>, ds::NullPutCache> p(
      r.ops, cache, kLayers, ps, ws);

  struct W { ds::Key k; ds::Value v; uint32_t h; };
  W const writes[] = {{100, 1100, 0}, {300, 1300, 0}, {400, 1400, 1},
                      {500, 1500, 2}, {200, 1200, 0}, {400, 9400, 1}};
  for (W const &w : writes) {
    CHECK(p.put(w.k, w.v, w.h).resolved, "every put resolves over 3 replicas");
  }

  CHECK(r.verifyOk("after replicated puts"), "I1-I4 hold");
  CHECK(r.set.agreeOn(kData), "and the replicas agree on the data node");

  // Read back through the quorum, and independently confirm each replica
  // reached the same state -- convergence is the property replication owes us.
  ds::Traversal<ds::QuorumOps<FakeReplicaSet>> d(r.ops);
  ds::PathStep path[ds::kMaxLayers];
  for (W const &w : writes) {
    ds::Value want = w.v;
    for (W const &later : writes) {
      if (later.k == w.k) want = later.v;
    }
    ds::TraversalResult const res = d.traverse(w.k, kLayers, path);
    CHECK(res.ok() && res.found && res.value == want,
          "every key reads back through the quorum");
  }

  for (size_t i = 0; i < 3; ++i) {
    ds::VerifyReport const rep = ds::verifyStructure(r.set.arena(i), kLayers);
    CHECK(rep.ok(), "and each replica is independently a valid structure");
  }

  std::printf("  3-replica puts: %llu commits, %llu writebacks, "
              "%llu replica reads for %llu logical\n",
              (unsigned long long)r.qs.commits,
              (unsigned long long)r.qs.writebacks,
              (unsigned long long)r.qs.replica_reads,
              (unsigned long long)r.qs.node_reads);
}

static void checkConvergenceWithAPersistentlyLaggingReplica() {
  // One replica is knocked back after every commit, which is the worst case
  // writeback exists for. The structure must still be correct and every key
  // still readable, and the lagging replica must keep being repaired rather
  // than drifting further behind.
  Rig r;
  ds::WriteStats ws;
  ds::Writer<ds::QuorumOps<FakeReplicaSet>> w(r.ops, ws);
  std::mt19937_64 rng(20260910);
  std::map<ds::Key, ds::Value> oracle;

  for (int i = 0; i < 120; ++i) {
    ds::Key const k = 1 + (rng() % 200);
    ds::Value const v = 1 + (rng() % 100000);
    // Traverse first. Writing into a node that does not cover k is exactly what
    // insertEntry now rejects, and relying on it being accepted is how the
    // first version of this test corrupted I1.
    ds::Traversal<ds::QuorumOps<FakeReplicaSet>> d(r.ops);
    ds::PathStep path[ds::kMaxLayers];
    ds::TraversalResult const res = d.traverse(k, kLayers, path);
    if (!res.ok()) {
      CHECK(false, "the traversal before an insert should resolve");
      continue;
    }
    ds::RemoteAddr orphan{};
    if (w.insertWithOverflow(res.data_addr, k, v, &orphan) !=
        ds::WriteOutcome::Published) {
      CHECK(false, "every insert should publish");
      continue;
    }
    oracle[k] = v;

    // Knock replica 2 back to this node's previous version, as a straggler
    // would be. Taken from the old_ver chain rather than synthesised, so the
    // handle names a vector that really is the predecessor -- a decremented
    // content_ver pointing at the *current* offset is a state no writer can
    // produce, and staging impossible states tests nothing.
    ds::NodeRecord const cur = r.set.arena(0).node(res.data_addr);
    ds::VecRecord const &cv = r.set.arena(0).vecAt(cur.handle.offset());
    if (cv.old_ver != ds::kNullVec && cur.handle.contentVer() > 0) {
      r.set.forceHandle(2, res.data_addr,
                        ds::Handle::make(cur.handle.structVer(),
                                         cur.handle.contentVer() - 1,
                                         static_cast<ds::VecOffset>(cv.old_ver)));
    }
  }

  CHECK(r.qs.writebacks >= 1, "writeback fires against a lagging replica");
  CHECK(r.verifyOk("with a lagging replica"), "I1-I4 still hold");

  size_t found = 0;
  ds::Traversal<ds::QuorumOps<FakeReplicaSet>> d(r.ops);
  ds::PathStep path[ds::kMaxLayers];
  for (auto const &kv : oracle) {
    ds::TraversalResult const res = d.traverse(kv.first, kLayers, path);
    if (res.ok() && res.found && res.value == kv.second) ++found;
  }
  CHECK(found == oracle.size(),
        "and every key still reads back its last written value");

  std::printf("  lagging replica: %zu keys, %llu commits, %llu writebacks, "
              "%llu stale votes\n",
              oracle.size(), (unsigned long long)r.qs.commits,
              (unsigned long long)r.qs.writebacks,
              (unsigned long long)r.qs.stale_votes);
}

static void checkSingleReplicaBehavesLikeTheDirectPath() {
  // N=1 must be the same behaviour at quorum 1, so the toggle is a real toggle
  // and not a second implementation. invariants.md §9 asks for zero overhead
  // versus the direct path; this checks the RDMA *counts*, which is the part
  // that can be checked here.
  Rig r(1);
  ds::PutStats ps;
  ds::WriteStats ws;
  ds::NullPutCache cache;
  ds::Putter<ds::QuorumOps<FakeReplicaSet>, ds::NullPutCache> p(
      r.ops, cache, kLayers, ps, ws);
  CHECK(p.put(100, 700, 1).resolved, "a put resolves at N=1");
  CHECK(r.verifyOk("at N=1"), "I1-I4 hold");
  CHECK(r.qs.replica_reads == r.qs.node_reads + r.qs.vec_reads,
        "one replica read per logical read -- no quorum overhead at N=1");
  CHECK(r.qs.writebacks == 0, "and nothing to write back");
}

static void checkReplicationCostsBandwidthNotRoundTrips() {
  // The claim the batch builder exists to make good on. A write's round trips
  // are its batches, and chaining plus fan-out means that count is the SAME at
  // one replica and at three -- only the bytes multiply.
  //
  // Counted at the replica-set level, which is where a round trip is: one
  // submitAll posts every replica's chain before draining any of them.
  uint64_t batches[2] = {};
  uint64_t replica_reads[2] = {};
  size_t const counts[2] = {1, 3};

  for (int i = 0; i < 2; ++i) {
    Rig r(counts[i]);
    ds::WriteStats ws;
    ds::Writer<ds::QuorumOps<FakeReplicaSet>> w(r.ops, ws);
    CHECK(w.insertEntry(kData, 100, 700) == ds::WriteOutcome::Published,
          "the write publishes");
    batches[i] = r.set.batches();
    replica_reads[i] = r.qs.replica_reads;
  }

  CHECK(batches[0] == 1, "F1 issues exactly one chained batch at N=1");
  CHECK(batches[1] == 1, "and exactly one at N=3 -- the fan-out is not a loop");
  CHECK(batches[0] == batches[1],
        "so replication costs no additional round trips on the write");
  CHECK(replica_reads[1] > replica_reads[0],
        "while the per-replica reads do multiply, which is the real cost");

  // F2 is two chains: everything staged plus the publishing CAS, then the five
  // completion CASes. Eleven operations, two round trips.
  Rig r3(3);
  ds::WriteStats ws;
  ds::Writer<ds::QuorumOps<FakeReplicaSet>> w(r3.ops, ws);
  for (ds::Key k : {ds::Key{100}, ds::Key{200}, ds::Key{300}}) {
    CHECK(w.insertEntry(kData, k, k * 7) == ds::WriteOutcome::Published, "seed");
  }
  uint64_t const before = r3.set.batches();
  ds::SplitResult const sp = w.splitAt(kData, 200, /*orphan=*/true, nullptr);
  CHECK(sp.outcome == ds::WriteOutcome::Published, "the split publishes");
  CHECK(r3.set.batches() == before + 2,
        "F2 is two chained batches at N=3, not eleven operations times three");

  std::printf("  round trips: F1 = %llu batch at N=1 and %llu at N=3; "
              "F2 = 2 batches\n",
              (unsigned long long)batches[0], (unsigned long long)batches[1]);
}

// ── The offset hint, speculated inside the quorum read ─────────────────────

/// A rig whose quorum ops carry a warm offset hint.
struct HintRig {
  FakeReplicaSet set;
  ds::QuorumStats qs;
  ds::VecOffsetHint hint;
  ds::QuorumOps<FakeReplicaSet> ops;

  explicit HintRig(size_t replicas = 3)
      : set(replicas, kLayers),
        hint(ds::kFirstDynamicId + 4096, /*enabled=*/true),
        ops(set, qs, &hint) {}
};

static void checkSpeculationSavesTheSerialisedVectorRead() {
  HintRig r;
  ds::NodeRecord node;
  ds::VecRecord vec;

  // First read: the hint is cold, so nothing is speculated -- but the true
  // offset is learned.
  CHECK(r.ops.readNode(kData, node), "the cold read succeeds");
  CHECK(r.ops.readVec(node.handle.offset(), vec), "and its vector read succeeds");
  CHECK(r.qs.speculated == 0, "a cold hint speculates nothing");
  CHECK(r.hint.guess(kData) == node.handle.offset(),
        "but it now holds the true offset");

  uint64_t const reads_before = r.qs.replica_reads;

  // Second read: the guess rides along with the headers, and the vector read is
  // answered from it with no further round trip.
  CHECK(r.ops.readNode(kData, node), "the warm read succeeds");
  CHECK(r.qs.speculated == 1, "having speculated the vector");
  CHECK(r.qs.spec_hits == 1, "and guessed right");

  CHECK(r.ops.readVec(node.handle.offset(), vec), "the vector read succeeds");
  CHECK(r.qs.vec_reads_served == 1,
        "served from the speculation, so it cost no round trip");

  // The bytes are the same either way: a hit costs 3 headers + 1 vector, which
  // is exactly what the unspeculated pair costs. What it saves is the
  // serialisation between them, not bandwidth.
  CHECK(r.qs.replica_reads - reads_before == r.set.replicas() + 1,
        "a hit moves the same bytes as the serialised pair");
  CHECK(vec.size == 0, "and the vector really is this node's");
}

static void checkAStaleGuessNeverServesOldBytes() {
  // The case that matters. A write moves the vector, so the hint's offset is a
  // version behind. The speculation must be rejected and the CURRENT vector
  // returned -- serving the stale one would be a wrong answer, not a slow one.
  HintRig r;
  ds::NodeRecord node;
  ds::VecRecord vec;
  CHECK(r.ops.readNode(kData, node), "warm the hint");
  CHECK(r.ops.readVec(node.handle.offset(), vec), "and read the vector");
  ds::VecOffset const old_off = node.handle.offset();

  ds::WriteStats ws;
  ds::Writer<ds::QuorumOps<FakeReplicaSet>> w(r.ops, ws);
  CHECK(w.insertEntry(kData, 100, 700) == ds::WriteOutcome::Published,
        "a write publishes a new version");

  // The hint still names the superseded offset.
  CHECK(r.hint.guess(kData) == old_off, "the hint is now a version behind");

  uint64_t const misses_before = r.qs.spec_misses;
  uint64_t const served_before = r.qs.vec_reads_served;
  CHECK(r.ops.readNode(kData, node), "the read succeeds");
  CHECK(node.handle.offset() != old_off, "the node has moved on");
  CHECK(r.qs.spec_misses == misses_before + 1, "the guess is scored as a miss");

  CHECK(r.ops.readVec(node.handle.offset(), vec), "the vector read succeeds");
  CHECK(r.qs.vec_reads_served == served_before,
        "NOT served from the stale speculation");
  int const idx = ds::findLte(vec, 100);
  CHECK(idx >= 0 && vec.e[idx].key == 100 && vec.e[idx].val == 700,
        "and it returns the version the write published, not the stale one");
}

static void checkSpeculationIsRejectedWhenReplicaZeroDissents() {
  // L4, strictly: the speculation is read from replica 0, so it is only usable
  // if replica 0 voted with the winning handle. Here it does not, and the
  // guess must be discarded even though the offset would have matched.
  HintRig r;
  ds::NodeRecord node;
  ds::VecRecord vec;
  CHECK(r.ops.readNode(kData, node), "warm the hint");
  CHECK(r.ops.readVec(node.handle.offset(), vec), "and read the vector");

  // Replica 0 becomes the odd one out; the other two still hold the truth.
  ds::Handle const base = r.set.arena(1).node(kData).handle;
  r.set.forceHandle(0, kData, ds::Handle::make(base.structVer(), 7, 111));

  uint64_t const served_before = r.qs.vec_reads_served;
  CHECK(r.ops.readNode(kData, node), "the read succeeds");
  CHECK(node.handle == base, "picking the majority handle");
  CHECK(r.qs.spec_misses >= 1, "and rejecting the speculation");
  CHECK(r.ops.readVec(node.handle.offset(), vec), "the vector read succeeds");
  CHECK(r.qs.vec_reads_served == served_before,
        "from a replica that voted with the winner, not from the speculation");
}

static void checkHintHelpsATraversalAndCostsNothingWrong() {
  // End to end: a traversal with a warm hint must return the same answers as one
  // without, and serve some of its vector reads from speculation.
  HintRig warm;
  Rig cold;
  ds::WriteStats ws_w, ws_c;
  ds::Writer<ds::QuorumOps<FakeReplicaSet>> w_warm(warm.ops, ws_w);
  ds::Writer<ds::QuorumOps<FakeReplicaSet>> w_cold(cold.ops, ws_c);
  for (ds::Key k : {ds::Key{100}, ds::Key{200}, ds::Key{300}}) {
    CHECK(w_warm.insertEntry(kData, k, k * 7) == ds::WriteOutcome::Published, "w");
    CHECK(w_cold.insertEntry(kData, k, k * 7) == ds::WriteOutcome::Published, "c");
  }

  ds::PathStep path[ds::kMaxLayers];
  for (int pass = 0; pass < 3; ++pass) {
    for (ds::Key k : {ds::Key{100}, ds::Key{200}, ds::Key{300}, ds::Key{999}}) {
      ds::Traversal<ds::QuorumOps<FakeReplicaSet>> dw(warm.ops);
      ds::Traversal<ds::QuorumOps<FakeReplicaSet>> dc(cold.ops);
      ds::TraversalResult const rw = dw.traverse(k, kLayers, path);
      ds::TraversalResult const rc = dc.traverse(k, kLayers, path);
      CHECK(rw.ok() == rc.ok() && rw.found == rc.found && rw.value == rc.value,
            "a warm hint changes no answer a traversal gives");
    }
  }

  CHECK(warm.qs.spec_hits > 0, "and by the later passes it is hitting");
  CHECK(warm.qs.vec_reads_served > 0, "serving vector reads without a round trip");
  std::printf("  hint: %llu speculated, %llu hit / %llu miss, "
              "%llu vector reads served\n",
              (unsigned long long)warm.qs.speculated,
              (unsigned long long)warm.qs.spec_hits,
              (unsigned long long)warm.qs.spec_misses,
              (unsigned long long)warm.qs.vec_reads_served);
}

int main() {
  std::printf("quorum_test: layers=%u\n", kLayers);
  checkMajorityIsComputedFromTheReplicaCount();
  checkReadPrefersTheHigherTagWhenAMajorityHoldsIt();
  checkReadIgnoresAHigherTagWithoutMajoritySupport();
  checkReadRetriesWhenNoValueHasMajority();
  checkReadSurvivesOneDownReplica();
  checkVectorReadHonoursL4();
  checkCommitNeedsMajorityAndRepairsTheRest();
  checkCommitFailsBelowMajorityAndIsRecoverable();
  checkCommitDoesNotClobberANewerReplica();
  checkHelperCasesReachEveryReplica();
  checkTheWritePathRunsUnchangedOverThreeReplicas();
  checkConvergenceWithAPersistentlyLaggingReplica();
  checkSingleReplicaBehavesLikeTheDirectPath();
  checkReplicationCostsBandwidthNotRoundTrips();
  checkSpeculationSavesTheSerialisedVectorRead();
  checkAStaleGuessNeverServesOldBytes();
  checkSpeculationIsRejectedWhenReplicaZeroDissents();
  checkHintHelpsATraversalAndCostsNothingWrong();

  if (g_failures != 0) {
    std::printf("%d FAILURE(S)\n", g_failures);
    return 1;
  }
  std::printf("ALL PASS\n");
  return 0;
}
