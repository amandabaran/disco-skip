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
#include <map>
#include <random>
#include <set>
#include <utility>
#include <vector>

#include "ds_defs.hpp"
#include "ds_node.hpp"
#include "layout.hpp"
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

// ── The remote node layout ───────────────────────────────────────────────────
//
// Sizes and offsets are static_asserted in ds_node.hpp, so what is worth
// testing here is the behaviour: that the version encoding round-trips, that
// tag order really is lexicographic, that the two in-progress markers work, and
// that findLte agrees with a brute-force scan.

static void checkHandleEncoding() {
  ds::Handle const h = ds::Handle::make(0x2ABC, 0x1DEAD, 0xC0FFEEu);
  CHECK(h.structVer() == 0x2ABC, "struct_ver round-trips");
  CHECK(h.contentVer() == 0x1DEAD, "content_ver round-trips");
  CHECK(h.offset() == 0xC0FFEEu, "offset round-trips");

  // Max values must not bleed into neighbouring fields.
  ds::Handle const full = ds::Handle::make(ds::Handle::kMaxStructVer,
                                           ds::Handle::kMaxContentVer, 0xFFFFFFFFu);
  CHECK(full.structVer() == ds::Handle::kMaxStructVer, "max struct_ver");
  CHECK(full.contentVer() == ds::Handle::kMaxContentVer, "max content_ver");
  CHECK(full.offset() == 0xFFFFFFFFu, "max offset");
  CHECK(full.raw == ~uint64_t{0}, "all fields at max fill the word exactly");

  // V3: one operation moves at most one version, and both move the content to
  // a new offset, since versions are copy-on-write.
  ds::Handle const base = ds::Handle::make(5, 9, 100);
  ds::Handle const c = base.withContent(200);
  CHECK(c.contentVer() == 10 && c.structVer() == 5, "withContent moves only content_ver");
  CHECK(c.offset() == 200, "withContent takes the new offset");
  ds::Handle const st = base.withStruct(300);
  CHECK(st.structVer() == 6 && st.contentVer() == 9, "withStruct moves only struct_ver");

  // L3: tag order is (struct_ver, content_ver) lexicographic, and the bit
  // layout exists so a plain unsigned compare on tag() reproduces it -- making
  // L2's max-tag quorum read one integer comparison. Checked as a property
  // with the offset varied, to prove it cannot perturb the comparison.
  std::mt19937_64 rng(7);
  for (int i = 0; i < 20000; ++i) {
    uint32_t const s1 = static_cast<uint32_t>(rng()) & ds::Handle::kMaxStructVer;
    uint32_t const c1 = static_cast<uint32_t>(rng()) & ds::Handle::kMaxContentVer;
    uint32_t const s2 = static_cast<uint32_t>(rng()) & ds::Handle::kMaxStructVer;
    uint32_t const c2 = static_cast<uint32_t>(rng()) & ds::Handle::kMaxContentVer;
    ds::Handle const a = ds::Handle::make(s1, c1, static_cast<uint32_t>(rng()));
    ds::Handle const b = ds::Handle::make(s2, c2, static_cast<uint32_t>(rng()));
    if (s1 == s2 && c1 == c2) {
      CHECK(a.tag() == b.tag(), "equal version pairs compare equal whatever the offset");
    } else {
      bool const want = (s1 != s2) ? (s1 < s2) : (c1 < c2);
      CHECK((a.tag() < b.tag()) == want,
            "tag() order is lexicographic on (struct_ver, content_ver)");
    }
  }
  CHECK(ds::Handle::make(1, 0, 0xFFFFFFFFu).tag() >
            ds::Handle::make(0, ds::Handle::kMaxContentVer, 0).tag(),
        "a struct_ver bump outranks any content_ver, whatever the offsets");
}

static void checkInProgressMarkers() {
  ds::NodeRecord n;
  ds::initNode(n, /*k_min=*/100, /*level=*/0, /*vec=*/7);
  CHECK(n.isStable(), "a freshly initialised node is stable");
  CHECK(n.handle.offset() == 7, "and points at its vector");
  CHECK(n.next_k_min == ds::kReservedKey, "with an open-ended range");

  // A split's handle CAS bumps struct_ver, opening the propagation window: the
  // tail still holds the old value, so a header-only read can tell that
  // next_id / next_k_min are not yet trustworthy. That is what lets a passing
  // reader skip the vector on a stable node and help on an unstable one.
  uint32_t const before = n.handle.structVer();
  n.handle = n.handle.withStruct(/*new_offset=*/8);
  CHECK(!n.isStable(), "bumping struct_ver opens the window");
  CHECK(n.tail_struct_ver == before, "the tail still holds the old value");
  n.tail_struct_ver = n.handle.structVer();
  CHECK(n.isStable(), "matching the tail closes the window");

  // A plain content write never opens that window -- for that case the pending
  // marker is the vector's null timestamp instead.
  n.handle = n.handle.withContent(/*new_offset=*/9);
  CHECK(n.isStable(), "a content write never opens the split window");

  ds::VecRecord v;
  ds::initVec(v, /*is_orphan=*/false, ds::kNullTs);
  CHECK(v.isPending(), "a null timestamp reads as pending");
  ds::initVec(v, /*is_orphan=*/false, /*ts=*/1234);
  CHECK(!v.isPending(), "a fixed timestamp does not");
  CHECK(!v.hasSplitDescriptor(), "a plain vector carries no split descriptor");
  v.next_id = 42;
  CHECK(v.hasSplitDescriptor(), "setting next_id makes it a descriptor");

  // Version identity: the offset is what really identifies a version, so a
  // read validated against the wrong offset is rejected even when the version
  // fields happen to agree.
  ds::NodeRecord m;
  ds::initNode(m, 0, 0, /*vec=*/5);
  ds::VecRecord mv;
  ds::initVec(mv, false, 1);
  CHECK(ds::vectorIsCurrent(m, mv, 5), "the named vector is current");
  CHECK(!ds::vectorIsCurrent(m, mv, 6), "a vector read from another offset is not");
  mv.content_ver = 3;
  CHECK(!ds::vectorIsCurrent(m, mv, 5), "nor one whose version disagrees");
}

static void checkFindLteAndRange() {
  ds::VecRecord v;
  ds::initVec(v, false, 1);

  for (uint32_t size = 0; size <= ds::kNodeCapacity; ++size) {
    std::vector<ds::Key> keys;
    for (uint32_t i = 0; i < size; ++i) keys.push_back(10 * (i + 1));
    v.size = size;
    for (uint32_t i = 0; i < size; ++i) v.e[i] = {keys[i], 1000 + keys[i]};
    for (ds::Key probe = 0; probe <= 10 * (ds::kNodeCapacity + 2); ++probe) {
      int want = -1;
      for (uint32_t i = 0; i < size; ++i) {
        if (keys[i] <= probe) want = static_cast<int>(i);
      }
      int const got = ds::findLte(v, probe);
      CHECK(got == want, "findLte agrees with a brute-force scan");
      if (got != want) return;
    }
  }

  // The range check, and the rule that the vector's split descriptor wins.
  // A stale header bound is the dangerous kind: too large, and it turns "I
  // need another hop" into "definitively absent", so the node answers for a
  // range it no longer covers.
  ds::NodeRecord n;
  ds::initNode(n, /*k_min=*/100, 0, /*vec=*/1);
  n.next_k_min = 500;
  ds::VecRecord cur;
  ds::initVec(cur, false, 1);

  CHECK(ds::rangeEnd(n, cur) == 500, "with no descriptor the header's bound is used");
  CHECK(!ds::covers(n, cur, 99), "below k_min is not covered");
  CHECK(ds::covers(n, cur, 100), "k_min itself is covered");
  CHECK(ds::covers(n, cur, 499), "just below the end is covered");
  CHECK(!ds::covers(n, cur, 500), "the end itself is not");

  cur.next_id = 91;
  cur.k_min_next = 300;
  CHECK(ds::rangeEnd(n, cur) == 300, "the descriptor's bound wins over the header's");
  CHECK(ds::covers(n, cur, 299), "299 is still ours");
  CHECK(!ds::covers(n, cur, 350),
        "350 is not, though the stale header bound would have claimed it");
  CHECK(ds::nextNode(n, cur).id == 91, "and the hop follows the descriptor");

  ds::NodeRecord last;
  ds::initNode(last, /*k_min=*/100, 0, 1);
  ds::VecRecord lv;
  ds::initVec(lv, false, 1);
  CHECK(ds::covers(last, lv, ds::kReservedKey - 1),
        "a tail node covers arbitrarily high keys");
  CHECK(ds::nextNode(last, lv).isNull(), "and has no successor");
}

// ── Arena layout and the client-partitioned allocator ───────────────────────

static ds::Layout makeLayout(uint64_t clients, uint64_t servers,
                             uint64_t nodes_per_client, uint64_t futures) {
  ds::Layout l{};
  l.num_clients = clients;
  l.num_servers = servers;
  l.async_parallelism = futures;
  l.num_registers = 1024;
  l.max_range = 10;
  l.majority = 0;
  l.cache_layers = 4;
  l.nodes_per_client = nodes_per_client;
  l.vecs_per_client = nodes_per_client * 4;
  l.offset_hint = true;
  l.client_local_region = 0;
  return l;
}

static void checkArenaLayout() {
  ds::Layout const l = makeLayout(8, 3, 1000, 4);

  CHECK(ds::kNullId == 0, "id 0 is null");
  CHECK(ds::kNullVec == 0, "vector offset 0 is null");
  std::set<uint64_t> reserved;
  for (uint32_t L = 0; L < ds::kMaxLayers; ++L) {
    ds::RemoteAddr const h = ds::headAddr(L);
    CHECK(!h.isNull(), "a head address is never null");
    CHECK(h.id < ds::kInitialDataId, "heads sit below the initial data node");
    CHECK(reserved.insert(h.id).second, "head ids are distinct");
  }
  CHECK(ds::kInitialDataId == ds::kMaxLayers + 1, "initial data node follows the heads");
  CHECK(ds::kFirstDynamicId == ds::kInitialDataId + 1, "dynamic ids follow the reserved");

  // Both arenas cover reserved plus every stripe, and must not overlap within
  // the server region.
  CHECK(l.nodeArenaNodes() == ds::kFirstDynamicId + 8 * 1000, "node arena covers all stripes");
  CHECK(l.vecArenaVecs() == ds::kFirstDynamicVec + 8 * 4000, "vector arena covers all stripes");
  CHECK(l.vecArenaOffset() >= l.nodeArenaSize(), "the vector arena starts after the node arena");
  CHECK(l.serverSize() == l.vecArenaOffset() + l.vecArenaSize(),
        "the server region is exactly the two arenas");
  CHECK(l.vecArenaSize() > l.nodeArenaSize(),
        "the vector arena is larger, since every write consumes one");

  // A node's address is also its handle's address, because the handle is at
  // offset 0 -- the CAS target depends on that.
  uintptr_t const base = 0x100000;
  CHECK(ds::Layout::nodeAddrOf(base, ds::RemoteAddr{7}) == base + 7 * sizeof(ds::NodeRecord),
        "node addressing is linear in the id");
  CHECK(ds::Layout::nodeAddrOf(base, ds::RemoteAddr{7}) % 64 == 0,
        "every node is 64B-aligned (F4)");
  CHECK(l.vecAddrOf(base, 3) == base + l.vecArenaOffset() + 3 * sizeof(ds::VecRecord),
        "vector addressing is linear in the offset");
  CHECK(l.vecAddrOf(base, 3) % 64 == 0, "every vector is 64B-aligned (F4)");

  // The in-place CAS targets a split's propagation needs. RDMA CAS is an
  // 8-byte operation, so each must be 8B-aligned.
  CHECK(ds::Layout::nextIdAddrOf(base, ds::RemoteAddr{2}) ==
            ds::Layout::nodeAddrOf(base, ds::RemoteAddr{2}) +
                offsetof(ds::NodeRecord, next_id),
        "next_id CAS target");
  CHECK(ds::Layout::nextKMinAddrOf(base, ds::RemoteAddr{2}) ==
            ds::Layout::nodeAddrOf(base, ds::RemoteAddr{2}) +
                offsetof(ds::NodeRecord, next_k_min),
        "next_k_min CAS target");
  CHECK(ds::Layout::nextIdAddrOf(base, ds::RemoteAddr{2}) % 8 == 0, "next_id is 8B-aligned");
  CHECK(ds::Layout::nextKMinAddrOf(base, ds::RemoteAddr{2}) % 8 == 0, "next_k_min is 8B-aligned");
  CHECK(ds::Layout::tailWordAddrOf(base, ds::RemoteAddr{2}) % 8 == 0,
        "the tail word is 8B-aligned");
  CHECK(offsetof(ds::NodeRecord, tail_struct_ver) ==
            offsetof(ds::NodeRecord, level) + sizeof(uint32_t),
        "tail_struct_ver shares its 8-byte word with level, which never changes");
  CHECK(l.vecTsAddrOf(base, 3) % 8 == 0, "the ts CAS target is 8B-aligned");

  // Per-future scratchpad regions must not overlap: overlap would have one
  // in-flight RDMA silently corrupting another's buffer.
  ds::Layout ml = l;
  static std::vector<unsigned char> backing;
  backing.assign(ml.totalClientSize() + 64, 0);
  ml.client_local_region = reinterpret_cast<uintptr_t>(backing.data());

  std::vector<std::pair<uintptr_t, size_t>> regions;
  for (uint64_t f = 0; f < ml.async_parallelism; ++f) {
    regions.emplace_back(reinterpret_cast<uintptr_t>(ml.getNodeBufs(f)), ml.nodeBufsSize());
    regions.emplace_back(reinterpret_cast<uintptr_t>(ml.getVecBufs(f)), ml.vecBufsSize());
    regions.emplace_back(reinterpret_cast<uintptr_t>(ml.getDataNodeBufs(f)), ml.nodeBufsSize());
    regions.emplace_back(reinterpret_cast<uintptr_t>(ml.getDataVecBufs(f)), ml.vecBufsSize());
    regions.emplace_back(reinterpret_cast<uintptr_t>(ml.getStageNode(f)), ml.stageNodeSize());
    regions.emplace_back(reinterpret_cast<uintptr_t>(ml.getStageVec(f)), ml.stageVecSize());
    regions.emplace_back(reinterpret_cast<uintptr_t>(ml.getCasBufs(f)), ml.casBufsSize());
  }
  size_t overlaps = 0, out_of_bounds = 0;
  uintptr_t const lo = ml.client_local_region;
  uintptr_t const hi = lo + ml.totalClientSize();
  for (size_t i = 0; i < regions.size(); ++i) {
    if (regions[i].first < lo || regions[i].first + regions[i].second > hi) ++out_of_bounds;
    for (size_t j = i + 1; j < regions.size(); ++j) {
      bool const disjoint = regions[i].first + regions[i].second <= regions[j].first ||
                            regions[j].first + regions[j].second <= regions[i].first;
      if (!disjoint) ++overlaps;
    }
  }
  CHECK(overlaps == 0, "no two per-future scratchpad regions overlap");
  CHECK(out_of_bounds == 0, "every scratchpad region is inside the client MR");
  CHECK(ml.nodeRegionOffset() >= ml.clientSize(),
        "the node region starts after the legacy register region");
}

static void checkAllocator() {
  uint64_t const per_client = 100;

  // Stripes must be disjoint and must all sit above the reserved region.
  std::set<uint64_t> seen;
  for (uint64_t c = 0; c < 8; ++c) {
    ds::NodeAllocator alloc(c, per_client);
    CHECK(alloc.capacity() == per_client, "stripe capacity is nodes_per_client");
    CHECK(alloc.allocated() == 0, "a fresh allocator has handed out nothing");
    CHECK(alloc.firstId() >= ds::Layout::kFirstDynamicId,
          "a stripe never overlaps the reserved ids");
    for (uint64_t i = 0; i < per_client; ++i) {
      ds::RemoteAddr const a = alloc.allocate();
      CHECK(!a.isNull(), "allocation inside capacity succeeds");
      CHECK(alloc.owns(a), "an allocated id belongs to its own stripe");
      CHECK(seen.insert(a.id).second, "no id is ever handed out twice, across clients");
    }
    CHECK(alloc.exhausted(), "the stripe is spent after capacity allocations");
    CHECK(alloc.remaining() == 0, "nothing remains");
    // Exhaustion must report, not wrap into the next client's stripe.
    CHECK(alloc.allocate().isNull(), "allocation past capacity returns null");
    CHECK(alloc.allocate().isNull(), "and keeps returning null");
    CHECK(alloc.allocated() == per_client, "the count does not run past capacity");
  }
  CHECK(seen.size() == 8 * per_client, "all stripes together cover every id once");

  // Cross-stripe isolation, stated directly: client 0 must not own client 1's ids.
  ds::NodeAllocator a0(0, per_client), a1(1, per_client);
  ds::RemoteAddr const id1 = a1.allocate();
  CHECK(!a0.owns(id1), "one client does not own another's ids");
  CHECK(a0.firstId() + per_client == a1.firstId(), "stripes are contiguous and non-overlapping");
}

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
// The fidelity property A5 asserts: every node that something points *down to*
// has a minimum that is a real remote boundary. Orphans are exempt -- nothing
// points at them, so their minimum makes no routing claim.
//
// We check the weaker, locally-checkable half of it: no non-orphan node claims
// a minimum we never told the cache about. The full property needs the remote
// structure to compare against, which is the selftest's job once the remote
// side exists.
static size_t countInventedBoundaries(ds::SkipVec const &sv, uint32_t layers,
                                      std::set<ds::Key> const &known) {
  size_t invented = 0;
  for (uint32_t L = 0; L < layers; ++L) {
    sv.for_each_node(static_cast<int>(L),
                     [&](ds::Key k_min, bool is_orphan, size_t entries) {
                       if (is_orphan || entries == 0) return;  // exempt
                       if (known.find(k_min) == known.end()) ++invented;
                     });
  }
  return invented;
}

// A stand-in for a remote descent, in the shape the real one will hand back.
// Boundaries get sparser by kLevelRatio per level, which is the geometry the
// interface doc's §8 analysis and the cache's TARGET_IDX_RATIO both assume.
static uint32_t fakeDescent(ds::Key k, uint32_t layers, ds::PathStep *out) {
  ds::Key stride = ds::kLevelRatio;
  for (uint32_t L = 0; L < layers; ++L) {
    ds::Key const k_min = (k / stride) * stride;
    out[L].k_min = k_min;
    // Distinct, stable, non-null addresses. Stability is the property
    // mirror_reconcile's additivity argument rests on, so the fake must have
    // it: the same k_min at the same level always yields the same address.
    out[L].addr = ds::RemoteAddr{1 + (L * 1000000) + k_min};
    out[L].first_down = (L == 0) ? ds::RemoteAddr{500000000 + k_min}
                                 : ds::RemoteAddr{};
    stride *= ds::kLevelRatio;
  }
  return layers;
}

static void checkReconcilePath() {
  config cfg("disco-skip", "reconcile path", {"normal"}, "");
  cfg.merge_threshold = 1.0;
  cfg.layers = 4;
  uint32_t const layers = static_cast<uint32_t>(cfg.layers);
  ds::SkipVec sv(&cfg);

  std::array<ds::RemoteAddr, ds::kMaxLayers> heads{};
  for (uint32_t L = 0; L < layers; ++L) heads[L] = ds::RemoteAddr{1 + L};
  ds::bootstrapHeads(sv, heads);
  CHECK(ds::headsBootstrapped(sv), "heads report as bootstrapped");

  // Drive the miss -> descend -> reconcile loop the orchestrator will run, and
  // keep an oracle of what locate_data must return for each key.
  std::map<ds::Key, ds::RemoteAddr> oracle;
  std::set<ds::Key> known_boundaries;
  std::mt19937_64 rng(20260907);
  std::uniform_int_distribution<ds::Key> key_dist(1000, 200000);

  for (int i = 0; i < 4000; ++i) {
    ds::Key const k = key_dist(rng);
    if (sv.locate_data(k).isNull() || oracle.find(k) == oracle.end()) {
      ds::PathStep path[ds::kMaxLayers];
      uint32_t const n = fakeDescent(k, layers, path);
      for (uint32_t L = 0; L < n; ++L) known_boundaries.insert(path[L].k_min);

      ds::RemoteAddr const data_addr{900000000 + k};
      ds::reconcile(sv, k, data_addr, path, n);

      // Mirror what reconcile installs at level 0, in its order. It installs
      // the data entry first, then splits the directory at path[0].k_min --
      // and that split creates a node whose first entry is
      // (path[0].k_min -> path[0].first_down). So the boundary is a level-0
      // entry too, and the oracle has to know about it or it will predict the
      // wrong predecessor for keys in between. Inserts are additive (never
      // overwriting), so first writer wins on both.
      if (oracle.find(k) == oracle.end()) oracle[k] = data_addr;
      if (oracle.find(path[0].k_min) == oracle.end()) {
        oracle[path[0].k_min] = path[0].first_down;
      }

      // The data entry the descent installed is a boundary the cache may split
      // at, so it counts as known.
      known_boundaries.insert(k);
    }
  }

  // Every key we reconciled must now resolve to exactly its own data address.
  size_t checked = 0;
  for (auto const &kv : oracle) {
    CHECK(sv.locate_data(kv.first) == kv.second, "reconciled key resolves");
    ++checked;
  }
  std::printf("  reconciled %zu keys, all resolve\n", checked);

  // Gap keys must resolve to the covering (largest <= k) entry.
  for (int i = 0; i < 5000; ++i) {
    ds::Key const k = key_dist(rng);
    auto it = oracle.upper_bound(k);
    ds::RemoteAddr const want =
        (it == oracle.begin()) ? ds::RemoteAddr{} : std::prev(it)->second;
    CHECK(sv.locate_data(k) == want, "gap key resolves to its predecessor");
  }

  // The fidelity property: the cache must not have invented a boundary.
  size_t const invented =
      countInventedBoundaries(sv, layers, known_boundaries);
  CHECK(invented == 0, "no invented boundaries");
  std::printf("  invented boundaries: %zu\n", invented);

  // gather_prevs must now find real addresses rather than nulls, since
  // reconcile has been installing them. Not every level can be non-null (a
  // level whose covering node the descent never reached stays null), but the
  // directory level always has one after a reconcile in range.
  ds::Key const probe = oracle.begin()->first;
  ds::RemoteAddr prevs[ds::kMaxLayers]{};
  ds::gatherPrevs(sv, probe, layers, prevs);
  CHECK(!prevs[0].isNull(), "gather_prevs finds a level-0 address");

  // Metric 2's numerator, and the orphan rate A9 quotes at ~21%.
  auto const counts = ds::nodeCounts(sv, layers);
  for (uint32_t L = 0; L < layers; ++L) {
    ds::LevelStats const st = ds::levelStats(sv, L);
    CHECK(st.nodes == counts[L], "node_count agrees with for_each_node");
    std::printf("  level %u: %zu nodes, %zu orphans (%.0f%%), %zu entries\n", L,
                st.nodes, st.orphans,
                st.nodes ? 100.0 * double(st.orphans) / double(st.nodes) : 0.0,
                st.entries);
  }

  // Fan-out should settle near kLevelRatio; wildly off means the reconcile
  // climb is not behaving like the structure's own geometry.
  if (counts[0] > 0 && counts[1] > 0) {
    double const fanout = double(counts[0]) / double(counts[1]);
    CHECK(fanout > 2.0 && fanout < 32.0, "level 0:1 fan-out is plausible");
    std::printf("  level 0:1 fan-out: %.1f (kLevelRatio = %zu)\n", fanout,
                ds::kLevelRatio);
  }

  // Const-correctness of exactly what DsState::reportCache() const does. That
  // file needs dory and cannot be compiled off-cluster, so the call shapes it
  // uses are pinned here instead.
  {
    ds::SkipVec const &csv = sv;
    CHECK(ds::headsBootstrapped(csv), "headsBootstrapped on a const cache");
    auto const ccounts = ds::nodeCounts(csv, layers);
    for (uint32_t L = 0; L < layers; ++L) {
      ds::LevelStats const st = ds::levelStats(csv, L);
      CHECK(st.nodes == ccounts[L], "levelStats on a const cache");
    }
  }

  CHECK(sv.verify(), "cache verify() after the reconcile loop");
}

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
  // The A5 API is now a static_assert in ds_cache.hpp rather than a runtime
  // capability, so there is nothing to report here: if this compiled, it exists.
  std::printf("cache API: A5 reconcile path present (asserted at compile time)\n");
#endif

  checkSharedDefs();
  checkHandleEncoding();
  checkInProgressMarkers();
  checkFindLteAndRange();
  checkArenaLayout();
  checkAllocator();
#if DS_CACHE_ENABLED
  checkCacheContract();
  checkReconcilePath();
#endif

  std::printf("%s\n", g_failures == 0 ? "ALL PASS" : "FAILURES");
  return g_failures == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
