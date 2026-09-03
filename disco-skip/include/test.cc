// test_mirror_insert.cc
//
// A concurrent test for the skipvector local cache's mirror_insert API.
// Spawns N threads, each performing a configurable number of mirror_insert
// operations with random keys and heights, then verifies invariants.
//
// Usage: ./test_mirror_insert [num_threads] [ops_per_thread] [key_range] [max_height]

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

#include "include-cache/common/config.h"
#include "include-cache/common/machine_defines.h"
#include "include-cache/common/tests.h"
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
  int max_height       = 4;
  unsigned base_seed   = 42;
};

static test_config parse_args(int argc, char** argv) {
  test_config cfg;
  if (argc > 1) cfg.num_threads    = std::stoul(argv[1]);
  if (argc > 2) cfg.ops_per_thread = std::stoul(argv[2]);
  if (argc > 3) cfg.key_range      = std::stoull(argv[3]);
  if (argc > 4) cfg.max_height     = std::stoi(argv[4]);
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
                           /*IDX_EXP=*/5, /*DATA_EXP=*/5,
                           /*MAX_LAYERS=*/8, /*HP=*/hp_manager<MAX_THREADS>>;

// For this test file, we assume SkipVec is defined. Fill in the type alias
// according to your project.

// ============================================================================
// Height generation
// ============================================================================

// Geometric distribution height, capped at max_height.
// P(height=0) = 1 - 1/T
// P(height=h) = (1/T)^h * (1 - 1/T)
static int random_height(std::mt19937_64& rng, int max_height) {
  // Simple: uniform 0..max_height with bias toward 0.
  // For a real test, use a geometric distribution matching your skipvector's
  // TARGET_DATA_RATIO / TARGET_IDX_RATIO.
  std::uniform_real_distribution<double> uniform(0.0, 1.0);
  int h = 0;
  while (h < max_height && uniform(rng) < 0.25) {
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
  size_t heights[16] = {0};      // count by height
};

static void worker(SkipVec* sv, test_config cfg, unsigned thread_id,
                   thread_stats* stats) {
  std::mt19937_64 rng(cfg.base_seed + thread_id);
  std::uniform_int_distribution<uint64_t> key_dist(1, cfg.key_range);

  for (size_t i = 0; i < cfg.ops_per_thread; ++i) {
    KeyType k = key_dist(rng);
    int height = random_height(rng, cfg.max_height);

    // Fabricate the "remote result" that would normally come from the remote
    // layer. In this test, we allocate fresh addresses for each new node that
    // mirror_insert will install.

    mock_remote_addr new_remote_data_addr = fresh_addr();
    std::array<mock_remote_addr, /*MAX_LAYERS+1=*/16> new_remote_index_addrs{};

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

    // Skip if height is 0 (no mirror update needed).
    if (height == 0) continue;

    // Call the API under test.
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
            << "  max height:     " << cfg.max_height << "\n"
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

  config cfg = config("bench", "skipvector tests *with iteration*",
                      {"normal"}, "");
  cfg.merge_threshold = 1.0;
  cfg.layers = cfg.max_height;

  SkipVec sv(&cfg);

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

  sv.verify();

  // Aggregate stats.
  size_t total_attempted = 0;
  size_t total_succeeded = 0;
  size_t height_totals[16] = {0};
  for (auto const& s : stats) {
    total_attempted += s.inserts_attempted;
    total_succeeded += s.inserts_succeeded;
    for (int h = 0; h < 16; ++h) height_totals[h] += s.heights[h];
  }

  std::cout << "Results:\n"
            << "  elapsed:         " << elapsed_ms << " ms\n"
            << "  ops attempted:   " << total_attempted << "\n"
            << "  ops succeeded:   " << total_succeeded << "\n"
            << "  throughput:      "
            << (elapsed_ms > 0
                    ? (total_attempted * 1000 / elapsed_ms)
                    : 0)
            << " ops/sec\n\n";

  std::cout << "Height distribution:\n";
  for (int h = 0; h <= cfg.max_height; ++h) {
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