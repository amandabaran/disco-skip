// Exercises the structure verifier and the bootstrap builder against a fake
// in-memory arena.
//
// This is the part of the remote side most worth testing off-cluster: the walk
// logic is fiddly, and a checker that silently passes everything is worse than
// no checker. So each invariant gets a deliberately broken structure and the
// test asserts the checker *rejects* it -- not just that it accepts a good one.

#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>

#include "ds_bootstrap.hpp"
#include "ds_defs.hpp"
#include "ds_node.hpp"
#include "ds_verify.hpp"

static int g_failures = 0;

#define CHECK(cond, msg)                                                      \
  do {                                                                        \
    if (!(cond)) {                                                            \
      std::printf("FAIL: %s  [%s] line %d\n", (msg), #cond, __LINE__);        \
      ++g_failures;                                                           \
    }                                                                         \
  } while (0)

/// A local stand-in for the memory servers' arena. The RDMA reader on the
/// cluster has the same interface, so the verifier under test here is the same
/// code that runs there.
class FakeArena {
 public:
  explicit FakeArena(size_t nodes) : nodes_(nodes) {
    for (auto &n : nodes_) ds::initNode(n, 0, 0, false);
  }

  bool read(ds::RemoteAddr a, ds::NodeRecord &out) {
    if (a.isNull() || a.id >= nodes_.size()) return false;
    out = nodes_[a.id];
    // The real reader validates bookends and retries; here a stamped-wrong node
    // is a genuine corruption we want reported rather than retried.
    return ds::slotIsConsistent(out);
  }

  ds::NodeRecord &at(ds::RemoteAddr a) { return nodes_[a.id]; }
  ds::NodeRecord &at(uint64_t id) { return nodes_[id]; }
  size_t size() const { return nodes_.size(); }

  /// Re-stamp after mutating, so a test that means to break one invariant does
  /// not accidentally trip the torn-read check instead.
  void restamp(uint64_t id) {
    ds::stampSlot(nodes_[id].current(), nodes_[id].handle);
  }

 private:
  std::vector<ds::NodeRecord> nodes_;
};

static FakeArena buildInitial(uint32_t layers) {
  FakeArena arena(ds::kFirstDynamicId + 256);
  ds::InitialNode built[ds::kMaxLayers + 1];
  uint32_t const n = ds::buildInitialStructure(layers, built);
  for (uint32_t i = 0; i < n; ++i) arena.at(built[i].addr) = built[i].record;
  return arena;
}

static bool rejects(ds::VerifyReport const &r, char const *what) {
  if (r.ok()) {
    std::printf("FAIL: verifier accepted a structure with %s\n", what);
    ++g_failures;
    return false;
  }
  return true;
}

static void checkInitialStructureIsValid() {
  for (uint32_t layers = 2; layers <= 6; ++layers) {
    FakeArena arena = buildInitial(layers);
    ds::VerifyReport const r = ds::verifyStructure(arena, layers);
    if (!r.ok()) {
      std::printf("FAIL: initial structure at layers=%u is invalid:\n", layers);
      for (auto const &e : r.errors) std::printf("    %s\n", e.c_str());
      ++g_failures;
      continue;
    }
    CHECK(r.nodes_visited == layers, "one index node per level");
    CHECK(r.data_nodes == 1, "exactly one data node");
    CHECK(r.entries == layers, "one entry per head");
    CHECK(r.orphans == 0, "no orphans in the initial structure");
    CHECK(ds::initialNodeCount(layers) == layers + 1, "node count matches");
  }
  std::printf("  initial structure valid for layers 2..6\n");
}

static void checkVerifierCatchesBreakage() {
  uint32_t const layers = 4;

  // I1: k_min must not exceed the smallest key held.
  {
    FakeArena a = buildInitial(layers);
    a.at(ds::headAddr(1)).k_min = 5;  // its entry is keyed 0
    a.restamp(ds::headAddr(1).id);
    rejects(ds::verifyStructure(a, layers), "k_min past its first key (I1)");
  }

  // A down pointer must land on a node whose k_min equals the routing key.
  {
    FakeArena a = buildInitial(layers);
    a.at(ds::headAddr(0)).k_min = 7;  // parent routes to it under key 0
    a.restamp(ds::headAddr(0).id);
    rejects(ds::verifyStructure(a, layers), "a down pointer whose target k_min disagrees");
  }

  // A child must sit exactly one level below its parent.
  {
    FakeArena a = buildInitial(layers);
    a.at(ds::headAddr(1)).level = 3;
    a.restamp(ds::headAddr(1).id);
    rejects(ds::verifyStructure(a, layers), "a level that is not one below its parent");
  }

  // I2: a cycle in a next chain.
  {
    FakeArena a = buildInitial(layers);
    a.at(ds::headAddr(2)).current().next_id = ds::headAddr(2).id;
    a.at(ds::headAddr(2)).current().next_k_min = 0;
    a.restamp(ds::headAddr(2).id);
    rejects(ds::verifyStructure(a, layers), "a self-cycle in a next chain (I2)");
  }

  // next_k_min must agree with the successor's k_min, which is the whole point
  // of co-locating it.
  {
    FakeArena a = buildInitial(layers);
    uint64_t const extra = ds::kFirstDynamicId;
    ds::initNode(a.at(extra), /*k_min=*/100, /*level=*/1, /*is_orphan=*/false);
    ds::VecSlot &h = a.at(ds::headAddr(1)).current();
    h.next_id = extra;
    h.next_k_min = 999;  // lies about where the successor starts
    a.restamp(ds::headAddr(1).id);
    rejects(ds::verifyStructure(a, layers), "next_k_min disagreeing with the successor");
  }

  // I4: an orphan must not be the target of a down pointer.
  {
    FakeArena a = buildInitial(layers);
    ds::NodeRecord &dir = a.at(ds::headAddr(0));
    dir.handle = ds::Handle::make(dir.handle.structVer(), dir.handle.contentVer(),
                                  dir.handle.slot(), ds::Handle::kFlagOrphan);
    a.restamp(ds::headAddr(0).id);
    rejects(ds::verifyStructure(a, layers), "an orphan that something routes to (I4)");
  }

  // I3: a non-orphan that nothing routes to. Splice a node into level 1's
  // chain without giving it a parent -- exactly the shape the cache's
  // verify_index() also rejects.
  {
    FakeArena a = buildInitial(layers);
    uint64_t const extra = ds::kFirstDynamicId;
    ds::initNode(a.at(extra), /*k_min=*/100, /*level=*/1, /*is_orphan=*/false);
    ds::VecSlot &h = a.at(ds::headAddr(1)).current();
    h.next_id = extra;
    h.next_k_min = 100;
    a.restamp(ds::headAddr(1).id);
    rejects(ds::verifyStructure(a, layers), "an unparented non-orphan (I3)");
  }

  // The same splice, but flagged orphan, is legitimate: capacity-overflow
  // splits produce exactly this, and it must NOT be reported.
  {
    FakeArena a = buildInitial(layers);
    uint64_t const extra = ds::kFirstDynamicId;
    ds::initNode(a.at(extra), /*k_min=*/100, /*level=*/1, /*is_orphan=*/true);
    ds::VecSlot &h = a.at(ds::headAddr(1)).current();
    h.next_id = extra;
    h.next_k_min = 100;
    a.restamp(ds::headAddr(1).id);
    ds::VerifyReport const r = ds::verifyStructure(a, layers);
    if (!r.ok()) {
      std::printf("FAIL: verifier rejected a legitimate overflow orphan:\n");
      for (auto const &e : r.errors) std::printf("    %s\n", e.c_str());
      ++g_failures;
    }
    CHECK(r.orphans == 1, "the orphan is counted");
  }

  // Unsorted entries: findLte binary-searches, so this returns wrong answers
  // rather than merely looking odd.
  {
    FakeArena a = buildInitial(layers);
    ds::VecSlot &s = a.at(ds::headAddr(2)).current();
    s.size = 2;
    s.e[0] = ds::Entry{50, ds::headAddr(1).id};
    s.e[1] = ds::Entry{10, ds::headAddr(1).id};
    a.restamp(ds::headAddr(2).id);
    rejects(ds::verifyStructure(a, layers), "entries out of order");
  }

  // Two parents for one child: the index must be a tree.
  {
    FakeArena a = buildInitial(layers);
    ds::VecSlot &s = a.at(ds::headAddr(2)).current();
    s.size = 2;
    s.e[0] = ds::Entry{0, ds::headAddr(1).id};
    s.e[1] = ds::Entry{100, ds::headAddr(1).id};  // same child, twice
    s.next_k_min = ds::kReservedKey;
    a.restamp(ds::headAddr(2).id);
    rejects(ds::verifyStructure(a, layers), "one child with two parents");
  }

  // Timestamps must not go backwards across the slot ring, or a snapshot read
  // walking back for the version with ts <= T stops at the wrong one.
  {
    FakeArena a = buildInitial(layers);
    ds::NodeRecord &n = a.at(ds::headAddr(1));
    n.current().ts = 100;
    n.slot[n.freeSlot()].ts = 500;  // previous version is somehow newer
    a.restamp(ds::headAddr(1).id);
    rejects(ds::verifyStructure(a, layers), "a timestamp inversion across the ring");
  }

  // A plain forward timestamp is fine and must not be reported.
  {
    FakeArena a = buildInitial(layers);
    ds::NodeRecord &n = a.at(ds::headAddr(1));
    n.slot[n.freeSlot()].ts = 100;
    n.current().ts = 500;
    a.restamp(ds::headAddr(1).id);
    ds::VerifyReport const r = ds::verifyStructure(a, layers);
    if (!r.ok()) {
      std::printf("FAIL: verifier rejected a valid timestamp ordering:\n");
      for (auto const &e : r.errors) std::printf("    %s\n", e.c_str());
      ++g_failures;
    }
  }

  // old_ver is reserved: a non-zero value means something wrote a version chain
  // that nothing can read.
  {
    FakeArena a = buildInitial(layers);
    a.at(ds::headAddr(1)).current().old_ver = 7;
    a.restamp(ds::headAddr(1).id);
    rejects(ds::verifyStructure(a, layers), "old_ver set while the chain is unimplemented");
  }

  // A torn node must be reported, not silently walked past.
  {
    FakeArena a = buildInitial(layers);
    a.at(ds::headAddr(1)).current().trail_content_ver ^= 1u;  // no restamp
    rejects(ds::verifyStructure(a, layers), "a torn node");
  }

  std::printf("  verifier rejects every deliberately broken structure\n");
}

int main() {
  std::printf("verify_test: layers cap=%zu capacity=%zu\n", ds::kMaxLayers,
              ds::kNodeCapacity);
  checkInitialStructureIsValid();
  checkVerifierCatchesBreakage();
  std::printf("%s\n", g_failures == 0 ? "ALL PASS" : "FAILURES");
  return g_failures == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
