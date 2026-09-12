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

#include "ds_put.hpp"
#include "ds_range.hpp"
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
  CHECK(res.truncated, "and says it truncated");
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

int main() {
  std::printf("range_test: layers=%u\n", kLayers);
  checkRangeMatchesAnOracle();
  checkAWriteAfterTheSnapshotIsInvisible();
  checkANodeCreatedAfterTheSnapshotIsSkipped();
  checkTruncationIsReportedNotSilent();
  checkEmptyAndInvertedRanges();

  if (g_failures != 0) {
    std::printf("%d FAILURE(S)\n", g_failures);
    return 1;
  }
  std::printf("ALL PASS\n");
  return 0;
}
