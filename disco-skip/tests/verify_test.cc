// Exercises the structure verifier and the bootstrap builder against a fake
// in-memory arena.
//
// This is the part of the remote side most worth testing off-cluster: the walk
// logic is fiddly, and a checker that silently passes everything is worse than
// no checker. So each invariant gets a deliberately broken structure and the
// test asserts the checker *rejects* it -- not just that it accepts a good one.

#include <cstdio>
#include <cstdlib>
#include <set>
#include <string>
#include <vector>

#include "ds_bootstrap.hpp"
#include "ds_defs.hpp"
#include "ds_node.hpp"
#include "ds_verify.hpp"
#include "layout.hpp"

static int g_failures = 0;

#define CHECK(cond, msg)                                                      \
  do {                                                                        \
    if (!(cond)) {                                                            \
      std::printf("FAIL: %s  [%s] line %d\n", (msg), #cond, __LINE__);        \
      ++g_failures;                                                           \
    }                                                                         \
  } while (0)

/// A local stand-in for the memory servers' two arenas. The RDMA reader on the
/// cluster has the same interface, so the verifier under test here is the same
/// code that runs there.
class FakeArena {
 public:
  FakeArena(size_t nodes, size_t vecs) : nodes_(nodes), vecs_(vecs) {
    for (auto &n : nodes_) ds::initNode(n, 0, 0, ds::kNullVec);
    for (auto &v : vecs_) ds::initVec(v, false, /*ts=*/1);
  }

  bool read(ds::RemoteAddr a, ds::NodeRecord &node, ds::VecRecord &vec) {
    if (a.isNull() || a.id >= nodes_.size()) return false;
    node = nodes_[a.id];
    ds::VecOffset const off = node.handle.offset();
    if (off == ds::kNullVec || off >= vecs_.size()) return false;
    vec = vecs_[off];
    return true;
  }

  bool readVec(ds::VecOffset off, ds::VecRecord &vec) {
    if (off == ds::kNullVec || off >= vecs_.size()) return false;
    vec = vecs_[off];
    return true;
  }

  ds::NodeRecord &node(ds::RemoteAddr a) { return nodes_[a.id]; }
  ds::NodeRecord &node(uint64_t id) { return nodes_[id]; }
  /// The vector the given node currently points at.
  ds::VecRecord &vec(ds::RemoteAddr a) {
    return vecs_[nodes_[a.id].handle.offset()];
  }
  ds::VecRecord &vecAt(ds::VecOffset off) { return vecs_[off]; }

 private:
  std::vector<ds::NodeRecord> nodes_;
  std::vector<ds::VecRecord> vecs_;
};

static FakeArena buildInitial(uint32_t layers) {
  FakeArena arena(ds::kFirstDynamicId + 256, ds::kFirstDynamicVec + 256);
  ds::InitialNode built[ds::kMaxLayers + 1];
  uint32_t const n = ds::buildInitialStructure(layers, /*ts=*/1000, built);
  for (uint32_t i = 0; i < n; ++i) {
    arena.node(built[i].addr) = built[i].node;
    arena.vecAt(built[i].vec_offset) = built[i].vec;
  }
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
    a.node(ds::headAddr(1)).k_min = 5;  // its entry is keyed 0
    rejects(ds::verifyStructure(a, layers), "k_min past its first key (I1)");
  }

  // A down pointer must land on a node whose k_min equals the routing key.
  {
    FakeArena a = buildInitial(layers);
    a.node(ds::headAddr(0)).k_min = 7;  // parent routes to it under key 0
    rejects(ds::verifyStructure(a, layers), "a down pointer whose target k_min disagrees");
  }

  // A child must sit exactly one level below its parent.
  {
    FakeArena a = buildInitial(layers);
    a.node(ds::headAddr(1)).level = 3;
    rejects(ds::verifyStructure(a, layers), "a level that is not one below its parent");
  }

  // I2: a cycle in a next chain.
  {
    FakeArena a = buildInitial(layers);
    a.node(ds::headAddr(2)).next_id = ds::headAddr(2).id;
    a.node(ds::headAddr(2)).next_k_min = 0;
    rejects(ds::verifyStructure(a, layers), "a self-cycle in a next chain (I2)");
  }

  // The range end must agree with the successor's k_min.
  {
    FakeArena a = buildInitial(layers);
    uint64_t const extra = ds::kFirstDynamicId;
    ds::VecOffset const extra_vec = ds::kFirstDynamicVec;
    ds::initNode(a.node(extra), /*k_min=*/100, /*level=*/1, extra_vec);
    ds::initVec(a.vecAt(extra_vec), /*is_orphan=*/true, /*ts=*/1000);
    a.node(ds::headAddr(1)).next_id = extra;
    a.node(ds::headAddr(1)).next_k_min = 999;  // lies about where it starts
    rejects(ds::verifyStructure(a, layers), "a range end disagreeing with the successor");
  }

  // I4: an orphan must not be the target of a down pointer.
  {
    FakeArena a = buildInitial(layers);
    a.vec(ds::headAddr(0)).is_orphan = 1;
    rejects(ds::verifyStructure(a, layers), "an orphan that something routes to (I4)");
  }

  // I3: a non-orphan that nothing routes to.
  {
    FakeArena a = buildInitial(layers);
    uint64_t const extra = ds::kFirstDynamicId;
    ds::VecOffset const extra_vec = ds::kFirstDynamicVec;
    ds::initNode(a.node(extra), /*k_min=*/100, /*level=*/1, extra_vec);
    ds::initVec(a.vecAt(extra_vec), /*is_orphan=*/false, /*ts=*/1000);
    a.node(ds::headAddr(1)).next_id = extra;
    a.node(ds::headAddr(1)).next_k_min = 100;
    rejects(ds::verifyStructure(a, layers), "an unparented non-orphan (I3)");
  }

  // The same splice, flagged orphan, is legitimate: capacity-overflow splits
  // produce exactly this, and it must NOT be reported.
  {
    FakeArena a = buildInitial(layers);
    uint64_t const extra = ds::kFirstDynamicId;
    ds::VecOffset const extra_vec = ds::kFirstDynamicVec;
    ds::initNode(a.node(extra), /*k_min=*/100, /*level=*/1, extra_vec);
    ds::initVec(a.vecAt(extra_vec), /*is_orphan=*/true, /*ts=*/1000);
    a.node(ds::headAddr(1)).next_id = extra;
    a.node(ds::headAddr(1)).next_k_min = 100;
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
    ds::VecRecord &v = a.vec(ds::headAddr(2));
    v.size = 2;
    v.e[0] = ds::Entry{50, ds::headAddr(1).id};
    v.e[1] = ds::Entry{10, ds::headAddr(1).id};
    rejects(ds::verifyStructure(a, layers), "entries out of order");
  }

  // Two parents for one child: the index must be a tree.
  {
    FakeArena a = buildInitial(layers);
    ds::VecRecord &v = a.vec(ds::headAddr(2));
    v.size = 2;
    v.e[0] = ds::Entry{0, ds::headAddr(1).id};
    v.e[1] = ds::Entry{100, ds::headAddr(1).id};
    rejects(ds::verifyStructure(a, layers), "one child with two parents");
  }

  // ── The new markers ─────────────────────────────────────────────────────

  // An unstable node in a quiescent structure means a writer died
  // mid-propagation and nobody helped it finish -- which livelocks readers.
  {
    FakeArena a = buildInitial(layers);
    ds::NodeRecord &n = a.node(ds::headAddr(1));
    n.handle = n.handle.withStruct(n.handle.offset());  // bumps struct_ver only
    rejects(ds::verifyStructure(a, layers), "a node left mid-split (bookend mismatch)");
  }

  // The vector must be the version the handle names.
  {
    FakeArena a = buildInitial(layers);
    a.vec(ds::headAddr(1)).content_ver = 9;
    rejects(ds::verifyStructure(a, layers), "a vector whose version is not the handle's");
  }

  // A pending version in a quiescent structure: nobody is going to resolve it.
  {
    FakeArena a = buildInitial(layers);
    a.vec(ds::headAddr(1)).ts = ds::kNullTs;
    rejects(ds::verifyStructure(a, layers), "a version left pending");
  }

  // A split descriptor that outlived propagation must agree with the header,
  // or readers preferring the vector and readers using the header disagree
  // about where the node's range ends.
  {
    FakeArena a = buildInitial(layers);
    ds::VecRecord &v = a.vec(ds::headAddr(1));
    v.next_id = 777;
    v.k_min_next = 4242;
    rejects(ds::verifyStructure(a, layers), "a split descriptor disagreeing with the header");
  }

  // old_ver must decrease in ts: a snapshot read stops at the first version
  // with ts <= T, so an inversion makes it stop early and serve the wrong one.
  {
    FakeArena a = buildInitial(layers);
    ds::VecOffset const old = ds::kFirstDynamicVec;
    ds::initVec(a.vecAt(old), /*is_orphan=*/false, /*ts=*/5000);  // newer!
    a.vec(ds::headAddr(1)).old_ver = old;   // current ts is 1000
    rejects(ds::verifyStructure(a, layers), "an old_ver chain that is not decreasing in ts");
  }

  // A correctly ordered chain must be accepted, and counted.
  {
    FakeArena a = buildInitial(layers);
    ds::VecOffset const old = ds::kFirstDynamicVec;
    ds::initVec(a.vecAt(old), /*is_orphan=*/false, /*ts=*/500);
    a.vecAt(old).content_ver = 0;
    ds::VecRecord &cur = a.vec(ds::headAddr(1));
    cur.content_ver = 1;
    a.node(ds::headAddr(1)).handle =
        ds::Handle::make(0, 1, a.node(ds::headAddr(1)).handle.offset());
    cur.old_ver = old;
    ds::VerifyReport const r = ds::verifyStructure(a, layers);
    if (!r.ok()) {
      std::printf("FAIL: verifier rejected a valid old_ver chain:\n");
      for (auto const &e : r.errors) std::printf("    %s\n", e.c_str());
      ++g_failures;
    }
    CHECK(r.old_versions == 1, "the superseded version is counted");
  }

  // A cycle in the old_ver chain must be bounded, not spun on.
  {
    FakeArena a = buildInitial(layers);
    ds::VecOffset const cur_off = a.node(ds::headAddr(1)).handle.offset();
    a.vecAt(cur_off).old_ver = cur_off;  // points at itself
    rejects(ds::verifyStructure(a, layers), "a self-cycle in the old_ver chain");
  }

  // A superseded version that is still pending can never satisfy a snapshot.
  {
    FakeArena a = buildInitial(layers);
    ds::VecOffset const old = ds::kFirstDynamicVec;
    ds::initVec(a.vecAt(old), /*is_orphan=*/false, ds::kNullTs);
    a.vec(ds::headAddr(1)).old_ver = old;
    rejects(ds::verifyStructure(a, layers), "a superseded version left pending");
  }

  std::printf("  verifier rejects every deliberately broken structure\n");
}

static void checkAllocatorsAndHint() {
  // The two arenas are consumed at different rates, so they are separate
  // stripes; check they hand out disjoint, non-reserved ranges.
  std::set<uint64_t> vecs;
  for (uint64_t c = 0; c < 4; ++c) {
    ds::VecAllocator alloc(c, 50);
    CHECK(alloc.capacity() == 50, "vector stripe capacity");
    CHECK(alloc.firstOffset() >= ds::kFirstDynamicVec,
          "a vector stripe never overlaps the reserved offsets");
    for (uint64_t i = 0; i < 50; ++i) {
      ds::VecOffset const o = alloc.allocate();
      CHECK(o != ds::kNullVec, "allocation inside capacity succeeds");
      CHECK(alloc.owns(o), "an allocated offset belongs to its own stripe");
      CHECK(vecs.insert(o).second, "no offset is handed out twice");
    }
    CHECK(alloc.exhausted() && alloc.allocate() == ds::kNullVec,
          "exhaustion reports null rather than wrapping");
  }
  CHECK(vecs.size() == 200, "all vector stripes together cover every offset once");

  // The offset hint: disabled is inert, enabled learns and scores.
  {
    ds::VecOffsetHint off(1024, /*enabled=*/false);
    CHECK(!off.enabled(), "disabled hint reports disabled");
    CHECK(off.guess(ds::RemoteAddr{5}) == ds::kNullVec, "disabled hint never guesses");
    off.record(ds::RemoteAddr{5}, 42, ds::kNullVec);
    CHECK(off.guess(ds::RemoteAddr{5}) == ds::kNullVec, "disabled hint learns nothing");
  }
  {
    ds::VecOffsetHint on(1024, /*enabled=*/true);
    ds::RemoteAddr const a{5};
    CHECK(on.guess(a) == ds::kNullVec, "no guess before anything is learned");
    // A first read with no guess must not be scored either way.
    on.record(a, 42, ds::kNullVec);
    CHECK(on.hits() == 0 && on.misses() == 0, "an unguessed read is not scored");
    CHECK(on.guess(a) == 42, "the hint learned the offset");
    // A correct guess.
    on.record(a, 42, on.guess(a));
    CHECK(on.hits() == 1 && on.misses() == 0, "a correct guess scores a hit");
    // The vector moved: the guess is now wrong, and the hint updates.
    on.record(a, 77, 42);
    CHECK(on.misses() == 1, "a stale guess scores a miss");
    CHECK(on.guess(a) == 77, "and the hint follows the move");
    CHECK(on.hitRate() > 0.4 && on.hitRate() < 0.6, "hit rate is 1 of 2");
    // Out of range must be inert, not out-of-bounds.
    CHECK(on.guess(ds::RemoteAddr{99999}) == ds::kNullVec,
          "an id past the arena guesses nothing");
    on.record(ds::RemoteAddr{99999}, 1, 1);
  }
  std::printf("  allocators and offset hint behave\n");
}

int main() {
  std::printf("verify_test: layers cap=%zu capacity=%zu\n", ds::kMaxLayers,
              ds::kNodeCapacity);
  checkInitialStructureIsValid();
  checkVerifierCatchesBreakage();
  checkAllocatorsAndHint();
  std::printf("%s\n", g_failures == 0 ? "ALL PASS" : "FAILURES");
  return g_failures == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
