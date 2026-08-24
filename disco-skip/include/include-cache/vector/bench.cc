#include <iostream>

#include "../common/config.h"
#include "../common/entry.h"
#include "../common/machine_defines.h"
#include "../common/static_initialization.h"
#include "../common/tests.h"
#include "../hp/hp_manager.h"
#include "../skipvector-pq/skipvector_pq.h"
#include "../skipvector-pq/skipvector_pq_teams.h"
#include "multivector_asr.h"
#include "multivector_cfra.h"

using K = size_t;
using V = long;
constexpr size_t EMPTY = static_cast<K>(-1);
using ENTRY = entry<K, V, EMPTY>;
constexpr std::string_view DS_NAME = "vector";
using HP = hp_manager<MAX_THREADS>;

// Launch the appropriate test
template <int DATA_SIZE> int run(config *cfg) {
  // Setting IDX_EXP to 0 starts skiparray simulation mode

  if (cfg->bench_name == "cfra") {
    using pq = skipvector_pq<ENTRY, SKIPARRAY_SIM_MODE, DATA_SIZE, HP>;
    priority_queue_test<pq>(cfg);
    pq::tear_down();
  } else if (cfg->bench_name == "asr") {
    using pq = skipvector_pq_teams<ENTRY, SKIPARRAY_SIM_MODE, DATA_SIZE,
                                   MAX_THREADS, HP>;
    priority_queue_test<pq>(cfg);
    pq::tear_down();
  } else {
    std::cout << "Invalid benchmark name" << std::endl;
    return 2;
  }

  return 0;
}

/// main routine: parse the command-line arguments and then launch the
/// appropriate instantiation of the benchmark template
int main(int argc, char **argv) {
  config cfg = config("bench", "vector-pq tests", {"cfra", "asr"}, "");

  // Set defaults more appropriate to this priority queue benchmark.
  cfg.lookup = 98.0; // overloaded to mean extract_min() percentage

  try {
    cfg.init_from_args(DS_NAME, argc, argv);
  } catch (const std::exception &e) {
    std::cout << "Error parsing arguments: " << e.what() << std::endl;
    return -1;
  }

  cfg.report();

  switch (cfg.chunk_size) {
  case 16:
    return run<16>(&cfg);

#if TEMPLATE_INITIALIZATION >= 1
  case 32:
    return run<32>(&cfg);
  case 64:
    return run<64>(&cfg);
  case 128:
    return run<128>(&cfg);
  case 256:
    return run<256>(&cfg);
  case 512:
    return run<512>(&cfg);
  case 1024:
    return run<1024>(&cfg);
#endif

#if TEMPLATE_INITIALIZATION >= 2
    // Very small sizes
  case 1:
    return run<1>(&cfg);
  case 2:
    return run<2>(&cfg);
  case 4:
    return run<4>(&cfg);
  case 8:
    return run<8>(&cfg);

  // Very big sizes
  case 2048:
    return run<2048>(&cfg);
  case 4096:
    return run<4096>(&cfg);
#endif
  default:
    std::cout << "Invalid data size " << cfg.chunk_size << std::endl;
    return -2;
  }
}
