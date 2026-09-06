// Single-threaded, assertion-driven unit coverage for the parts of the cache
// the concurrent harness in test.cc never touches:
//
//   * locate_data() and mirror_reconcile(), checked against an independent
//     std::map oracle of the key -> data-address mapping
//   * set_head_remote_addrs() and gather_prevs(), checked by routing a key
//     that lands on the head at every level and confirming the bootstrapped
//     addresses come back

#include <cassert>
#include <cstdint>
#include <iostream>
#include <map>
#include <random>

#include "include-cache/common/config.h"
#include "include-cache/common/machine_defines.h"
#include "include-cache/hp/hp_manager.h"
#include "include-cache/vector/vector_sfra.h"
#include "include-cache/vector/vector_umfra.h"

#include "skipvector_disco.h"

struct mock_remote_addr {
  uint64_t id = 0;
  bool operator==(mock_remote_addr const &o) const { return id == o.id; }
  bool operator!=(mock_remote_addr const &o) const { return id != o.id; }
};

static size_t const MAX_LAYERS = 8;
static int64_t const IDX_EXP = 3;

using SkipVec = skipvector<uint64_t, uint64_t, mock_remote_addr,
                           vector_sfra, vector_umfra, IDX_EXP, IDX_EXP,
                           MAX_LAYERS, hp_manager<MAX_THREADS>>;

static uint64_t g_next = 1;
static mock_remote_addr fresh() { return {g_next++}; }

static int g_failures = 0;
#define CHECK(cond, msg)                                                       \
  do {                                                                         \
    if (!(cond)) {                                                             \
      std::cout << "FAIL: " << (msg) << "  [" #cond "] line " << __LINE__      \
                << "\n";                                                       \
      ++g_failures;                                                            \
    }                                                                          \
  } while (0)

// The oracle: key -> data address, for every level-0 entry we installed.
static std::map<uint64_t, uint64_t> oracle;

// What locate_data(k) should return: the value of the largest entry <= k,
// or 0 (null) if there is none.
static uint64_t expected(uint64_t k) {
  auto it = oracle.upper_bound(k);
  if (it == oracle.begin())
    return 0;
  return std::prev(it)->second;
}

int main() {
  config cfg("test", "locate_data / mirror_reconcile", {"normal"}, "");
  cfg.merge_threshold = 1.0;
  cfg.layers = 4;
  SkipVec sv(&cfg);

  std::mt19937_64 rng(12345);
  std::uniform_int_distribution<uint64_t> key_dist(1000, 100000);

  // ---- populate via mirror_insert -----------------------------------------
  // Heights >= 1 only; height 0 needs no cache update.
  for (int i = 0; i < 3000; ++i) {
    uint64_t const k = key_dist(rng);
    int height = 1;
    while (height < 4 && (rng() % 8) == 0)
      ++height;

    mock_remote_addr const data_addr = fresh();
    std::array<mock_remote_addr, MAX_LAYERS> idx{};
    for (int L = 0; L < height; ++L)
      idx[L] = fresh();

    // A duplicate key is a no-op in the cache, so only the first wins.
    bool const is_new = (oracle.find(k) == oracle.end());
    sv.mirror_insert(k, height, data_addr, idx);
    if (is_new)
      oracle[k] = data_addr.id;
  }
  std::cout << "installed " << oracle.size() << " distinct keys\n";

  // ---- locate_data returns the right data address -------------------------
  int exact_checked = 0;
  for (auto const &kv : oracle) {
    mock_remote_addr const got = sv.locate_data(kv.first);
    CHECK(got.id == kv.second, "locate_data on an installed key");
    ++exact_checked;
  }
  std::cout << "checked " << exact_checked << " exact keys\n";

  // ---- locate_data on gaps returns the covering (largest <= k) entry ------
  for (int i = 0; i < 20000; ++i) {
    uint64_t const k = key_dist(rng);
    mock_remote_addr const got = sv.locate_data(k);
    CHECK(got.id == expected(k), "locate_data on a gap key");
  }
  std::cout << "checked 20000 gap keys\n";

  // ---- below every key must be a miss (null) ------------------------------
  CHECK(sv.locate_data(1).id == 0, "key below all entries must miss");
  CHECK(sv.locate_data(oracle.begin()->first - 1).id == 0,
        "key just below the minimum must miss");

  // ---- mirror_reconcile installs a new routing entry ----------------------
  uint64_t const new_k = 500000;  // above everything installed so far
  mock_remote_addr const new_addr = fresh();
  CHECK(sv.locate_data(new_k).id == expected(new_k),
        "before reconcile, new_k routes to its predecessor");

  sv.mirror_reconcile(new_k, new_addr);
  oracle[new_k] = new_addr.id;
  CHECK(sv.locate_data(new_k).id == new_addr.id, "reconcile installed entry");
  CHECK(sv.locate_data(new_k + 7).id == new_addr.id,
        "reconciled entry covers keys above it");

  // ---- reconcile is idempotent -------------------------------------------
  sv.mirror_reconcile(new_k, new_addr);
  sv.mirror_reconcile(new_k, new_addr);
  CHECK(sv.locate_data(new_k).id == new_addr.id, "reconcile is idempotent");

  // ---- reconcile is additive: an existing entry is never overwritten ------
  mock_remote_addr const other = fresh();
  sv.mirror_reconcile(new_k, other);
  CHECK(sv.locate_data(new_k).id == new_addr.id,
        "reconcile must not overwrite an existing entry");

  // ---- reconcile into the middle of the key range, many times -------------
  for (int i = 0; i < 3000; ++i) {
    uint64_t const k = key_dist(rng);
    if (oracle.find(k) != oracle.end())
      continue;
    mock_remote_addr const a = fresh();
    sv.mirror_reconcile(k, a);
    oracle[k] = a.id;
  }
  std::cout << "reconciled up to " << oracle.size() << " distinct keys\n";

  // Full re-check of the mapping after all that reconciling.
  for (auto const &kv : oracle)
    CHECK(sv.locate_data(kv.first).id == kv.second,
          "locate_data after bulk reconcile");
  for (int i = 0; i < 20000; ++i) {
    uint64_t const k = key_dist(rng);
    CHECK(sv.locate_data(k).id == expected(k),
          "gap key after bulk reconcile");
  }

  // ---- structural invariants still hold -----------------------------------
  bool const ok = sv.verify();
  CHECK(ok, "verify() after locate_data / mirror_reconcile");

  // ========================================================================
  // set_head_remote_addrs() / gather_prevs()
  // ========================================================================
  //
  // Uses a second, separately bootstrapped skipvector, because
  // set_head_remote_addrs() is write-once and the one above was deliberately
  // left unbootstrapped so the "before" state could be observed.

  int const LAYERS = 4;

  // A key below every boundary lands on the head at every level, so its
  // per-level prev addresses are exactly the head addresses.
  uint64_t const low_key = 1;

  {
    config c2("test", "bootstrap", {"normal"}, "");
    c2.merge_threshold = 1.0;
    c2.layers = LAYERS;
    SkipVec sv2(&c2);

    CHECK(!sv2.heads_bootstrapped(), "fresh cache is not bootstrapped");

    // Populate first, so the descent has real nodes to route past.
    std::mt19937_64 r2(999);
    std::uniform_int_distribution<uint64_t> kd(1000, 100000);
    for (int i = 0; i < 2000; ++i) {
      int height = 1;
      while (height < LAYERS && (r2() % 8) == 0)
        ++height;
      std::array<mock_remote_addr, MAX_LAYERS> idx{};
      for (int L = 0; L < height; ++L)
        idx[L] = fresh();
      sv2.mirror_insert(kd(r2), height, fresh(), idx);
    }

    // Before bootstrap every level reports null for a head-routed key.
    std::array<mock_remote_addr, MAX_LAYERS> prevs{};
    CHECK(sv2.gather_prevs(low_key, LAYERS, prevs.data()),
          "gather_prevs succeeds before bootstrap");
    for (int L = 0; L < LAYERS; ++L)
      CHECK(prevs[L].id == 0, "unbootstrapped head reports null prev");

    // Bootstrap with distinguishable addresses, one per level.
    std::array<mock_remote_addr, MAX_LAYERS> heads{};
    for (int L = 0; L < LAYERS; ++L)
      heads[L] = fresh();
    sv2.set_head_remote_addrs(heads);
    CHECK(sv2.heads_bootstrapped(), "cache reports bootstrapped");

    // Now the same key must report the head address at every level, and each
    // level must report *its own* head, not a neighbour's.
    prevs = {};
    CHECK(sv2.gather_prevs(low_key, LAYERS, prevs.data()),
          "gather_prevs succeeds after bootstrap");
    for (int L = 0; L < LAYERS; ++L)
      CHECK(prevs[L].id == heads[L].id, "head address returned for its level");

    // A key routed into real structure must not report head addresses. Pick
    // the largest installed key, which is far right of every head.
    uint64_t big = 0;
    for (int i = 0; i < 5000; ++i)
      big = std::max(big, kd(r2));
    std::array<mock_remote_addr, MAX_LAYERS> deep{};
    CHECK(sv2.gather_prevs(90000, LAYERS, deep.data()),
          "gather_prevs succeeds for a deep key");
    CHECK(deep[0].id != 0 && deep[0].id != heads[0].id,
          "deep key routes past the level-0 head to a real node");

    // gather_prevs must only write the levels the height covers.
    std::array<mock_remote_addr, MAX_LAYERS> partial{};
    CHECK(sv2.gather_prevs(low_key, 2, partial.data()),
          "gather_prevs succeeds with height 2");
    CHECK(partial[0].id == heads[0].id && partial[1].id == heads[1].id,
          "height 2 fills levels 0 and 1");
    for (int L = 2; L < static_cast<int>(MAX_LAYERS); ++L)
      CHECK(partial[L].id == 0, "gather_prevs does not write above the height");

    // Bootstrap must not disturb the routing entries.
    CHECK(sv2.verify(), "verify() after bootstrap");
  }

  // ========================================================================
  // Faithful reconcile: structure gets built, and no boundary is invented
  // ========================================================================
  //
  // mirror_reconcile() installs level-0 *entries*; node *boundaries* would
  // otherwise arrive only via mirror_insert(). Without the boundary split a
  // read-mostly client fills the directory with unindexed overflow nodes and
  // the layer degenerates -- measured at 1771 directory nodes, all orphans,
  // every index layer empty. This checks both that the structure now forms and
  // that it forms out of REAL remote boundaries only.
  //
  // The remote is modelled with boundaries 8x sparser per level, matching the
  // structure's own geometry, and addresses derived from the boundary key
  // because real remote node addresses are stable.

  {
    int const L4 = 4;
    config c3("test", "faithful reconcile", {"normal"}, "");
    c3.merge_threshold = 1.0;
    c3.layers = L4;
    SkipVec sv3(&c3);

    // Data-node boundaries are height >= 1 keys; level-L INDEX boundaries are
    // height >= L+2 keys, i.e. one ratio sparser again. Keeping those distinct
    // matters: if the model gave them the same spacing, data_k_min would equal
    // path[0].k_min and the test could not tell a faithful split from one that
    // invents a boundary at the data key.
    uint64_t const DATA_SPAN = 8;
    auto span_of = [](int L) -> uint64_t { return 64ull << (3 * L); };
    auto bound_of = [&](int L, uint64_t k) { return (k / span_of(L)) * span_of(L); };
    auto data_bound_of = [&](uint64_t k) { return (k / DATA_SPAN) * DATA_SPAN; };

    // The remote has orphans too: a node that overflows splits off a successor
    // that nothing in the layer above points at, reachable only by walking
    // /next/. A descent therefore sometimes *lands* on one, and its k_min ends
    // up in the path. Model them so that case is actually covered.
    //
    // An orphan's minimum is a boundary of the layer below that is not itself a
    // routing boundary of this layer -- which is exactly what an overflow split
    // produces.
    auto lower_span = [&](int L) -> uint64_t {
      return L == 0 ? DATA_SPAN : span_of(L - 1);
    };
    auto is_orphan_min = [&](int L, uint64_t m) {
      if (m == 0 || m % span_of(L) == 0) return false;   // a routing boundary
      if (m % lower_span(L) != 0) return false;          // not a boundary below
      uint64_t h = m * 0x9E3779B97F4A7C15ull +
                   static_cast<uint64_t>(L) * 0x632BE59Bull;
      h ^= h >> 31;
      return (h & 3) == 0;                               // roughly 1 in 4
    };
    // Minimum of the node covering k at level L, orphans included.
    auto covering_min = [&](int L, uint64_t k) {
      uint64_t best = bound_of(L, k);
      uint64_t const ls = lower_span(L);
      for (uint64_t m = best + ls; m <= k; m += ls)
        if (is_orphan_min(L, m)) best = m;
      return best;
    };
    auto addr_of = [](int level, uint64_t kmin) {
      return mock_remote_addr{(static_cast<uint64_t>(level) + 1) * 0x100000000ull + kmin};
    };

    std::mt19937_64 r3(4242);
    std::uniform_int_distribution<uint64_t> kd3(1, 2000000);

    for (int i = 0; i < 60000; ++i) {
      uint64_t const k = kd3(r3);
      std::array<SkipVec::path_step, MAX_LAYERS> path{};
      uint32_t lv = 0;
      for (int L = 0; L < L4; ++L) {
        uint64_t const b = covering_min(L, k);
        if (b == 0) break;
        path[L] = {b, addr_of(L + 1, b),
                   L == 0 ? addr_of(0, b) : mock_remote_addr{}};
        ++lv;
      }
      uint64_t const dmin = data_bound_of(k);
      if (dmin != 0)
        sv3.mirror_reconcile(dmin, addr_of(0, dmin), path.data(), lv);
    }

    size_t const d = sv3.node_count(0);
    size_t const l1 = sv3.node_count(1);
    size_t const l2 = sv3.node_count(2);

    // The directory must be indexed, not a flat chain. Before the boundary
    // split this was exactly 1 (the empty head) however many keys went in.
    CHECK(l1 > d / 40, "level 1 indexes the directory");
    CHECK(l2 > 1, "level 2 is populated");
    CHECK(d > l1 && l1 > l2, "layers shrink going up");
    CHECK(d / l1 < 40 && l1 / l2 < 40, "fanout per layer stays bounded");

    // THE fidelity property: every node something points down to must have a
    // minimum that is a real remote boundary. Orphans are exempt -- nothing
    // points at them, so they claim to be no one's boundary.
    size_t invented = 0, orphans = 0, checked = 0;
    for (int L = 0; L < L4; ++L) {
      bool first = true;
      sv3.for_each_node(L, [&](uint64_t m, bool is_orphan, size_t sz) {
        if (first) { first = false; return; }   // the head
        if (is_orphan || sz == 0) { ++orphans; return; }
        ++checked;
        // A legitimate local node minimum is any REAL remote node minimum --
        // a routing boundary, or the minimum of a remote orphan. The property
        // is about node minimums, not about which of them have parents: a node
        // mirroring a remote orphan is a faithful mirror, and giving it a local
        // parent just means the cache routes to it directly where the remote
        // has to walk.
        if (bound_of(L, m) != m && !is_orphan_min(L, m))
          ++invented;
      });
    }
    CHECK(checked > 0, "there are parented nodes to check");
    CHECK(invented == 0, "no node minimum is an invented boundary");

    CHECK(sv3.verify(), "verify() after a reconcile-only workload");

    std::cout << "faithful reconcile: directory=" << d << " level1=" << l1
              << " level2=" << l2 << "  parented=" << checked
              << " orphans=" << orphans << " invented=" << invented << "\n";
  }

  std::cout << (g_failures == 0 ? "\nALL PASS\n" : "\nFAILURES\n");
  return g_failures == 0 ? 0 : 1;
}
