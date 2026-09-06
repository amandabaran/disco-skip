// test.cc
//
// A concurrent test for the skipvector local cache's mirror_insert API.
// Spawns N threads, each performing a configurable number of mirror_insert
// operations with random keys and heights, then verifies invariants.
//
// Usage: ./test [num_threads] [ops_per_thread] [key_range] [num_layers]

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <random>
#include <string>
#include <thread>
#include <vector>

// NB: deliberately not including common/tests.h. Nothing here uses it, and it
// pulls in x86intrin.h and Linux-only affinity APIs, which makes this test
// unbuildable off an x86 Linux box.
#include "include-cache/common/config.h"
#include "include-cache/common/machine_defines.h"
#include "include-cache/hp/hp_manager.h"
#include "include-cache/hp/hp_manager_leaky.h"
#include "include-cache/vector/vector_sfra.h"
#include "include-cache/vector/vector_umfra.h"

#include "skipvector_disco.h"

// ============================================================================
// Configuration
// ============================================================================

struct test_config {
  size_t num_threads   = 4;
  size_t ops_per_thread = 10000;
  uint64_t key_range   = 100000;
  int num_layers       = 4;
  unsigned base_seed   = std::random_device{}();;
};

size_t const MAX_LAYERS = 8;
size_t const IDX_EXP = 3; // 2^3 = 8 (capacity 16)

static test_config parse_args(int argc, char** argv) {
  test_config cfg;
  if (argc > 1) cfg.num_threads    = std::stoul(argv[1]);
  if (argc > 2) cfg.ops_per_thread = std::stoul(argv[2]);
  if (argc > 3) cfg.key_range      = std::stoull(argv[3]);
  if (argc > 4) cfg.num_layers     = std::stoi(argv[4]);
  return cfg;
}

// ============================================================================
// A mock REMOTE_ADDR type
// ============================================================================
//
// The skipvector stores REMOTE_ADDR structs in local nodes. For testing, we
// use a simple struct with a unique identifier. Real deployments would use
// something like {node_id, offset, rkey}.

struct mock_remote_addr {
  uint64_t id;

  bool operator==(mock_remote_addr const& o) const { return id == o.id; }
  bool operator!=(mock_remote_addr const& o) const { return id != o.id; }
  bool is_null() const { return id == 0; }
};

// A global monotonic counter used to hand out unique remote addresses.
// In a real system, addresses come from the remote layer's allocator.
static std::atomic<uint64_t> g_next_remote_id{1};

static mock_remote_addr fresh_addr() {
  return {g_next_remote_id.fetch_add(1, std::memory_order_relaxed)};
}

// A remote node's address is stable for its lifetime -- CoW replaces the
// vector and updates the node's offset, and a split leaves the original node
// holding its address. So a simulated address must be a function of which node
// it is, not a fresh allocation per descent.
static mock_remote_addr stable_addr(int level, uint64_t k_min) {
  return {(static_cast<uint64_t>(level) + 1) * 0x100000000ull + k_min};
}

// ============================================================================
// Type aliases (adjust to match your skipvector template signature)
// ============================================================================

using KeyType   = uint64_t;
using ValueType = uint64_t;  // unused since values live remotely, but the template may require it

// The skipvector template you're testing. Adjust template arguments to match.
// This is a placeholder — replace with your actual instantiation.
//
using SkipVec = skipvector<KeyType, ValueType, mock_remote_addr,
                           vector_sfra, vector_umfra,
                           /*IDX_EXP=*/IDX_EXP, /*DATA_EXP=*/IDX_EXP,
                           /*MAX_LAYERS=*/MAX_LAYERS, /*HP=*/hp_manager<MAX_THREADS>>;

// For this test file, we assume SkipVec is defined. Fill in the type alias
// according to your project.

// ============================================================================
// Height generation
// ============================================================================

// Geometric distribution height, capped at num_layers.
// P(height=0) = 1 - 1/T
// P(height=h) = (1/T)^h * (1 - 1/T)
//
// NB: 1/T must match the skipvector's own random_height(), which promotes with
// probability 1/TARGET_DATA_RATIO then 1/TARGET_IDX_RATIO per level. Both come
// from exp_to_ratio(EXP) = 1 << EXP, so with IDX_EXP = DATA_EXP = 3 that is
// 1/8. Using a larger probability here inflates index churn and skews the
// per-layer node counts that verbose_analysis() reports, which makes them
// useless as a check on vertical balance.
static double const PROMOTE_P = 1.0 / (1 << IDX_EXP);

static int random_height(std::mt19937_64& rng, int num_layers) {
  std::uniform_real_distribution<double> uniform(0.0, 1.0);
  int h = 0;
  while (h < num_layers && uniform(rng) < PROMOTE_P) {
    ++h;
  }
  return h;
}

// ============================================================================
// Worker thread
// ============================================================================

struct thread_stats {
  size_t inserts_attempted = 0;
  size_t inserts_succeeded = 0;  // if your API returns success/failure info
  size_t reconciles = 0;
  size_t heights[16] = {0};      // count by height
};

static void worker(SkipVec* sv, test_config cfg, unsigned thread_id,
                   thread_stats* stats) {
  std::mt19937_64 rng(cfg.base_seed + thread_id);
  std::uniform_int_distribution<uint64_t> key_dist(1, cfg.key_range);

  for (size_t i = 0; i < cfg.ops_per_thread; ++i) {
    KeyType k = key_dist(rng);
    int height = random_height(rng, cfg.num_layers);

    // Fabricate the "remote result" that would normally come from the remote
    // layer. In this test, we allocate fresh addresses for each new node that
    // mirror_insert will install.

    mock_remote_addr new_remote_data_addr = fresh_addr();
    std::array<mock_remote_addr, /*MAX_LAYERS+1=*/MAX_LAYERS> new_remote_index_addrs{};

    // For levels 0..height-2, a new remote index node is created (split-at-K).
    // For level height-1 (top), an orphan address is provided only if the top
    // level's insertion overflows. Since we can't predict that from the client
    // side, always provide a fresh address; mirror_insert will ignore it if
    // not needed.
    for (int level = 0; level < height; ++level) {
      new_remote_index_addrs[level] = fresh_addr();
    }

    stats->inserts_attempted++;
    stats->heights[height]++;

    // A height-0 remote insert makes no structural change to the index, so
    // there is nothing to mirror -- but the client did learn which data node
    // covers the key, which is exactly what mirror_reconcile() installs. This
    // is also what exercises the orphan-promotion path under contention.
    if (height == 0) {
      // Fabricate a plausible descent path. Addresses are derived from the
      // boundary key rather than allocated fresh, because real remote node
      // addresses are stable.
      //
      // Data boundaries every 8 keys; INDEX boundaries one ratio sparser again
      // at each level (64, 512, ...). Keeping those distinct matters -- with
      // the same spacing, path[0].k_min would always equal data_k_min and the
      // level-0 boundary split would never be exercised.
      //
      // Some node minimums are remote orphans: drawn from the layer below, not
      // routing boundaries of this layer, and nothing above points at them. A
      // descent lands on one whenever the key falls in its range, so the path
      // carries its k_min -- which is what makes the cache take the split_at
      // branch rather than split_insert.
      std::array<SkipVec::path_step, MAX_LAYERS> path{};
      uint32_t lv = 0;
      for (int L = 0; L < cfg.num_layers; ++L) {
        uint64_t const span = 64ull << (3 * L);
        uint64_t const lower = (L == 0) ? 8ull : (64ull << (3 * (L - 1)));
        uint64_t b = (k / span) * span;
        for (uint64_t m = b + lower; m <= k; m += lower) {
          uint64_t h = m * 0x9E3779B97F4A7C15ull + static_cast<uint64_t>(L);
          h ^= h >> 31;
          if (m % span != 0 && (h & 3) == 0)
            b = m; // a remote orphan
        }
        if (b == 0) break;
        path[L] = {b, stable_addr(L + 1, b),
                   L == 0 ? stable_addr(0, b) : mock_remote_addr{}};
        ++lv;
      }
      uint64_t const dmin = (k / 8) * 8;
      if (dmin != 0) {
        sv->mirror_reconcile(dmin, stable_addr(0, dmin), path.data(), lv);
        stats->reconciles++;
      }
      continue;
    }

    // NB: no per-op logging here. Unsynchronised cout from every thread
    // interleaves into unreadable output and dominates the runtime, which
    // also hides real contention.
    sv->mirror_insert(k, height, new_remote_data_addr, new_remote_index_addrs);
    stats->inserts_succeeded++;
  }
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char** argv) {
  test_config cfg = parse_args(argc, argv);

  std::cout << "Config:\n"
            << "  threads:        " << cfg.num_threads << "\n"
            << "  ops per thread: " << cfg.ops_per_thread << "\n"
            << "  key range:      " << cfg.key_range << "\n"
            << "  max height:     " << cfg.num_layers << "\n"
            << "  base seed:      " << cfg.base_seed << "\n\n";

  // Construct the skipvector. The constructor signature you have takes a
  // config* — adjust to match. Here we assume a default or dummy config.
  //
  // For example:
  //   config sv_cfg;
  //   sv_cfg.layers = 6;
  //   sv_cfg.merge_threshold = 1.67;
  //   SkipVec sv(&sv_cfg);
  //
  // Fill in as appropriate.

  config cfg_sv = config("bench", "skipvector tests *with iteration*",
                      {"normal"}, "");
  cfg_sv.merge_threshold = 1.0;
  cfg_sv.layers = cfg.num_layers;

  SkipVec sv(&cfg_sv);

  std::vector<thread_stats> stats(cfg.num_threads);
  std::vector<std::thread> threads;
  threads.reserve(cfg.num_threads);

  auto start = std::chrono::steady_clock::now();

  for (size_t t = 0; t < cfg.num_threads; ++t) {
    threads.emplace_back(worker, &sv, cfg, t, &stats[t]);
  }

  for (auto& th : threads) th.join();

  auto end = std::chrono::steady_clock::now();
  auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                        end - start).count();

  std::cout << "Verifying index...\n";
  sv.verify();
  std::cout << "Done verifying index.\n\n";

  sv.verbose_analysis();

  // Aggregate stats.
  size_t total_attempted = 0;
  size_t total_succeeded = 0;
  size_t total_reconciles = 0;
  size_t height_totals[16] = {0};
  for (auto const& s : stats) {
    total_attempted += s.inserts_attempted;
    total_succeeded += s.inserts_succeeded;
    total_reconciles += s.reconciles;
    for (int h = 0; h < 16; ++h) height_totals[h] += s.heights[h];
  }

  std::cout << "Results:\n"
            << "  elapsed:         " << elapsed_ms << " ms\n"
            << "  ops attempted:   " << total_attempted << "\n"
            << "  ops succeeded:   " << total_succeeded << "\n"
            << "  reconciles:      " << total_reconciles << "\n"
            << "  throughput:      "
            << (elapsed_ms > 0
                    ? (total_attempted * 1000 / elapsed_ms)
                    : 0)
            << " ops/sec\n\n";

  std::cout << "Height distribution:\n";
  for (int h = 0; h <= cfg.num_layers; ++h) {
    std::cout << "  height " << h << ": " << height_totals[h] << "\n";
  }

  // Optionally, run verify() to check invariants.
  // if (!sv.verify()) {
  //   std::cerr << "verify() failed!\n";
  //   return 1;
  // }
  // std::cout << "\nverify() passed.\n";

  return 0;
}