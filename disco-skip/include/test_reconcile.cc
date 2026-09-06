// Exercises locate_data() and mirror_reconcile(), which the existing harness
// never touches. Single-threaded and assertion-driven: it checks the actual
// key -> data-address mapping against an independent std::map oracle.

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

  std::cout << (g_failures == 0 ? "\nALL PASS\n" : "\nFAILURES\n");
  return g_failures == 0 ? 0 : 1;
}
