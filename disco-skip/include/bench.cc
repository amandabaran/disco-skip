#include <iostream>

#include "include-cache/common/config.h"
#include "include-cache/common/machine_defines.h"
#include "include-cache/common/tests.h"
#include "include-cache/hp/hp_manager.h"
#include "include-cache/hp/hp_manager_leaky.h"
#include "include-cache/vector/vector_sfra.h"
#include "include-cache/vector/vector_umfra.h"
#include "skipvector.h"

constexpr size_t MAX_LAYERS = 32;
constexpr std::string_view DS_NAME = "skipvector";

// Launch the appropriate test
template <int IDX_EXP, int DATA_EXP, typename HP> int run(config *cfg) {
  if (cfg->bench_name == "normal") {
    // 2PL-Late skipvector in the default configuration: sorted vectors in the
    // index layer, and unsorted vectors in the data layer.
    using ii_map = skipvector<size_t, long, vector_sfra, vector_umfra, IDX_EXP,
                              DATA_EXP, MAX_LAYERS, HP>;
    ordered_map_test<ii_map>(cfg);
    ii_map::tear_down();
#if 0
  } else if (cfg->bench_name == "sorted") {
    // 2PL-Late skipvector with Sorted vectors everywhere
    typedef skipvector<size_t, long, vector_sfra, vector_sfra, IDX_EXP,
                       DATA_EXP, MAX_LAYERS, HP>
        ii_map;
    ordered_map_test<ii_map>(cfg);
    ii_map::tear_down();
  } else if (cfg->bench_name == "unsorted") {
    // 2PL-Late skipvector with Unsorted vectors everywhere
    typedef skipvector<size_t, long, vector_umfra, vector_umfra, IDX_EXP,
                       DATA_EXP, MAX_LAYERS, HP>
        ii_map;
    ordered_map_test<ii_map>(cfg);
    ii_map::tear_down();
  } else if (cfg->bench_name == "reverse") {
    // 2PL-Late skipvector in the Reverse configuration: unsorted vectors in the
    // index layer, and sorted vectors in the data layer.
    typedef skipvector<size_t, long, vector_umfra, vector_sfra, IDX_EXP,
                       DATA_EXP, MAX_LAYERS, HP>
        ii_map;
    ordered_map_test<ii_map>(cfg);
    ii_map::tear_down();
#endif
  } else {
    std::cout << "Invalid benchmark name" << std::endl;
    return 2;
  }

  return 0;
}

template <int IDX_EXP, int DATA_EXP> int run(config *cfg) {
  if (cfg->reclaim_memory) {
    return run<IDX_EXP, DATA_EXP, hp_manager<MAX_THREADS>>(cfg);
  } else {
    return run<IDX_EXP, DATA_EXP, hp_manager_leaky>(cfg);
  }
}

template <int DATA_EXP> int run(config *cfg) {
  if (cfg->index_size == SKIPLIST_SIM_MODE) {
    return run<SKIPLIST_SIM_MODE, DATA_EXP>(cfg);
  } else if (cfg->index_size == SKIPARRAY_SIM_MODE) {
    return run<SKIPARRAY_SIM_MODE, DATA_EXP>(cfg);
  } else if (cfg->index_size == 4) {
    return run<2, DATA_EXP>(cfg);
  } else if (cfg->index_size == 8) {
    return run<3, DATA_EXP>(cfg);
  } else if (cfg->index_size == 16) {
    return run<4, DATA_EXP>(cfg);
  } else if (cfg->index_size == 32) {
    return run<5, DATA_EXP>(cfg);
  } else if (cfg->index_size == 64) {
    return run<6, DATA_EXP>(cfg);
  } else if (cfg->index_size == 128) {
    return run<7, DATA_EXP>(cfg);
  } else if (cfg->index_size == 256) {
    return run<8, DATA_EXP>(cfg);
  } else {
    std::cout << "Invalid data/index size counts " << cfg->chunk_size << ","
              << cfg->index_size << std::endl;
    return 5;
  }
}

/// main routine: parse the command-line arguments and then launch the
/// appropriate instantiation of the benchmark template
int main(int argc, char **argv) {
  config cfg = config("bench", "skipvector tests *with iteration*",
                      {"normal", "reverse", "sorted", "unsorted"}, "");

  try {
    cfg.init_from_args(DS_NAME, argc, argv);
  } catch (const std::exception &e) {
    std::cout << "Error parsing arguments: " << e.what() << std::endl;
    return -1;
  }

  cfg.report();

  switch (cfg.chunk_size) {
  case SKIPLIST_SIM_MODE:
    return run<SKIPLIST_SIM_MODE>(&cfg);
  case 4:
    return run<2>(&cfg);
  case 8:
    return run<3>(&cfg);
  case 16:
    return run<4>(&cfg);
  case 32:
    return run<5>(&cfg);
  case 64:
    return run<6>(&cfg);
  case 128:
    return run<7>(&cfg);
  case 256:
    return run<8>(&cfg);
  default:
    std::cout << "Invalid chunk size count " << cfg.chunk_size << std::endl;
    return 4;
  }
}
