#pragma once

#include "unistd.h"

#include <cmath>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "cpu_policy.h"
#include "cpu_policy_tools.h"
#include "machine_defines.h"
#include "special_values.h"

/// config encapsulates all of the configuration behaviors that we require of
/// our benchmarks.  It standardizes the format of command-line arguments,
/// parsing of command-line arguments, and reporting of command-line arguments.
///
/// config is a superset of everything that our individual benchmarks need.  For
/// example, it has a chunk　size, even though our linked list benchmark doesn't
/// need it.  The price of such generality is small, and the code savings is
/// large.
///
/// The purpose of config is not to hide information, but to reduce boilerplate
/// code.  We aren't concerned about good object-oriented design, so everything
/// is public.
struct config {
  /// Interval of time the test should run for in seconds
  int interval = 5;

  /// Our benchmark harness uses the data structures as integer sets or
  /// integer-integer maps.  This is the range for keys in the maps, and for
  /// elements in the sets.
  int64_t key_range = 65536;

  /// Number of threads that should execute the benchmark code
  int nthreads = 1;

  /// Lookup ratio as a percentage of elemental operations. For a map, lookups
  /// are contains() operations. For a priority queue, "lookups" are
  /// extract_min() operations, and the insertion batch size WILL be chosen as a
  /// consequence of this number in order to keep the size of the data structure
  /// roughly static over time.
  float lookup = 80.0;

  /// The size of index layer vectors.
  int64_t index_size = 16;

  /// If the data structure uses chunks, this is the size of each chunk
  int64_t chunk_size = 16;

  /// This is the proportion of threads that will be dedicated to traversals,
  /// represented as a percentage. This fraction will be rounded up to the next
  /// whole number of threads.
  /// If set to a negative number, uses exactly as many threads as the absolute
  /// value.
  float traversal_pctg = 0.0;

  /// The percentage of traversals that are ragne() operations, for benchmarks
  /// that have range queries.
  /// This uses an RNG out of 100, so float values are not allowed.
  float range_pctg = 0.0;

  /// Percentage of for_each() and range() operations that will exit early.
  /// This uses an RNG out of 100, so float values are not allowed.
  float early_exit_pctg = 0.0;

  /// Percentage of for_each() and range() operations that will be read-only.
  /// This uses an RNG out of 100, so float values are not allowed.
  float readonly_traversal_pctg = 0.0;

  /// Should the benchmark output a lot of data, or just a little?
  bool verbose = false;

  /// Should the benchmark forgo pretty printing entirely, and output as a
  /// comma-separated list for the sake of data processing?
  bool output_raw = false;

  /// The merge threshold of a skipvector
  float merge_threshold = 1.0;

  /// The number of teams (skipvector priority queue only)
  int nteams = 0;

  /// Number of index layers (skipvector-map only). 0 index layers is not
  /// allowed. There is always at least one index layer. If AUTOMATIC, that
  /// automatically computes the ideal number of layers from the key range and
  /// vector sizes.
  int layers = 0;

  /// The difference between start and end keys in a range query... We will
  /// randomly choose a start, and then end will be this far away from it.
  /// Note: If AUTOMATIC, then rather than using a fixed range size, we choose a
  /// start and an end point uniformly at random, such that end >= start,
  /// allowing variable range lengths.
  size_t range_dist = 0;

  /// The target number of elements in each "node" of a "phantom" layer in
  /// between the data layer and the bottommost index layer. If this is set to
  /// 1, then the skipvector priority queue behaves as though this layer is
  /// entirely absent. If this exponent is set to greater value, then the
  /// priority queue will try to have phantom_cap data nodes per key in the
  /// bottommost index layer. This may reduce contention for extract_min().
  int phantom_cap = 1;

  /// The initial population of the data structure as a percentage of key range.
  float initial_pop_pctg = 50.0;

  /// The name of the specific data structure to test
  std::string data_structure_name;

  /// The name of the specific data structure variant to test
  std::string bench_name = "normal";

  /// Whether or not to use hazard pointers to reclaim memory precisely.
  /// If false, leak memory for safety.
  bool reclaim_memory = false;

  // An enum representing the CPU allocation policy.
  cpu_policy policy = OPERATING_SYSTEM;

  /// The name of the executable
  std::string program_name;

  /// A description of the program
  std::string bench_description;

  /// All of the possible data structure implementations that can be run
  std::vector<std::string> ds_options;

  /// A statement about any command-line options that don't pertain to a
  /// particular program
  std::string unused_options_statement;

  /// Blank constructor.
  config()
      : program_name("SSSP"), bench_description("Runs SSSP"), ds_options({}) {}

  /// Initialize the program's configuration by setting the strings that are not
  /// dependent on the command-line
  config(std::string prog_name, std::string bench_desc,
         std::vector<std::string> ds_opts, std::string unused_stmt)
      : program_name(std::move(prog_name)),
        bench_description(std::move(bench_desc)),
        ds_options(std::move(ds_opts)),
        unused_options_statement(std::move(unused_stmt)) {}

  /// Usage() reports on the command-line options for the benchmark
  void usage() const {
    using std::cout;
    using std::endl;

    cout << program_name << ":" << bench_description << endl
         << " -b: benchmark                                   "
            "(default varies)"
         << endl
         << " -c: vector capacity                             "
            "(default 16)"
         << endl
         << "  (" << SKIPLIST_SIM_MODE
         << ": special value that means skiplist simulation mode)" << endl
#ifndef PQ_BENCHMARK
         << " -d: merge threshold                             "
            "(default 1.0)"
         << endl
         << " -e: percentage of traversals that exit early    "
            "(default 0.0%)"
         << endl
         << " -f: % of threads doing traversals               "
            "(default 0%)"
         << endl
         << "  (if negative: special value, use exactly |f| threads)" << endl
         << " -g: range() ops (as a percentage of traversals) "
            "(default 0%)"
         << endl
#else
         << " -d: number of teams                             "
            "(default "
         << AUTOMATIC << ")" << endl
         << "  (" << AUTOMATIC
         << ": special value that means same as number of NUMA zones." << endl
         << "  If > nthreads, is reduced to nthreads.)" << endl
#endif
         << " -h: print this message                          "
            "(default false)"
         << endl
         << " -i: test interval in seconds                    "
            "(default 5)"
         << endl
         << " -k: key range                                   "
            "(default 65536)"
         << endl
#ifdef PQ_BENCHMARK
         << "  (if nonpositive: special value, prefill |k| and then use "
            "RDTSCP)"
         << endl
         << " -l: %age of pops (dictates insert batch size)   "
            "(default 98%)"
         << endl
#else
         << " -l: lookups as a % of elementals                "
            "(default 80%)"
         << endl
#endif
         << " -m: use hazard pointers to reclaim memory       "
            "(default false)"
         << endl
         << " -n: CPU allocation policy                       "
            "(default os)"
         << endl
         << "  (os: allow the OS to decide)" << endl
         << "  (f1: fill one NUMA zone at a time, including hyperthreading)"
         << endl
         << "  (f1hl: fill one NUMA zone at a time, but hyperthread last)"
         << endl
         << "  (rr: round-robin NUMA zones)" << endl
         << " -o: output raw (CSV)                            "
            "(default false)"
         << endl
         << " -p: initial population as % of key range        "
            "(default 50%)"
         << endl
#ifndef PQ_BENCHMARK
         << " -r: percentage of traversals that are read-only "
            "(default 0%)"
         << endl
#endif
         << " -s: index layer node capacity                   "
            "(default: "
         << AUTOMATIC << ")" << endl
         << "  (" << AUTOMATIC
         << ": special value that sets -s to the same as -c)        " << endl
#ifndef PQ_BENCHMARK
         << "  (" << SKIPLIST_SIM_MODE
         << ": special value that means skiplist simulation mode)" << endl
         << "  (" << SKIPARRAY_SIM_MODE
         << ": special value that means skiparray simulation mode)" << endl
#else
         << "  (any negative value: use skiparray simulation mode)" << endl
#endif
         << " -t: # threads                                   "
            "(default 1)"
         << endl
         << " -v: be verbose?                                 "
            "(default false)"
         << endl
#ifndef PQ_BENCHMARK
         << " -x: number of index layers                      "
            "(default "
         << AUTOMATIC << ")" << endl
         << "  (" << AUTOMATIC
         << ": automatically calculate ideal value)                  " << endl
         << " -z: distance of range()                         "
            "(default "
         << AUTOMATIC << ")" << endl
         << "  (" << AUTOMATIC
         << ": special value that means choose range uniformly at "
            "random)"
         << endl
#else
         << " -z: capacity for phantom layer nodes            "
            "(default 1)"
         << endl
#endif
         << "      [ ";
    for (auto const &i : ds_options) {
      cout << i << " ";
    }
    cout << "]" << endl;
    if (!unused_options_statement.empty()) {
      cout << unused_options_statement << endl;
    }
  }

  /// Parse the command-line options to initialize fields of the config object
  void init_from_args(const std::string_view ds, int argc, char **argv) {
    using std::stof;
    using std::stoi;
    using std::stoll;
    using std::stoull;
    using std::string;

    data_structure_name = ds;
    long opt = 0;
    while ((opt = getopt(argc, argv, "b:c:d:e:f:g:hi:k:l:mn:op:r:s:t:vx:z:")) !=
           -1) {
      switch (opt) {
      case 'b':
        bench_name = string(optarg);
        break;
      case 'c':
        chunk_size = stoi(optarg);
        break;
      case 'd':
        merge_threshold = stof(optarg);
        nteams = stoi(optarg);
        break;
      case 'e':
        early_exit_pctg = stof(optarg);
        break;
      case 'f':
        traversal_pctg = stof(optarg);
        break;
      case 'g':
        range_pctg = stof(optarg);
        break;
      case 'h':
        usage();
        exit(0);
      case 'i':
        interval = stoi(optarg);
        break;
      case 'k':
        key_range = stoll(optarg);
        break;
      case 'l':
        lookup = stof(optarg);
        break;
      case 'm':
        reclaim_memory = !reclaim_memory;
        break;
      case 'n':
        policy = cpu_policy_tools::parse(optarg);
        break;
      case 'o':
        output_raw = !output_raw;
        break;
      case 'p':
        initial_pop_pctg = stof(optarg);
        break;
      case 'r':
        readonly_traversal_pctg = stof(optarg);
        break;
      case 's':
        index_size = stoi(optarg);
        break;
      case 't':
        nthreads = stoi(optarg);
        break;
      case 'v':
        verbose = !verbose;
        break;
      case 'x':
        layers = stoi(optarg);
        break;
      case 'z':
        range_dist = stoull(optarg);
        phantom_cap = stoi(optarg);
        break;
      default:
        throw std::logic_error("Unrecognized option: -" + std::to_string(opt));
      }
    }

    validate_args();
  }

  // Validate the arguments, and auto-populate arguments set to special values.
  void validate_args() {
    using std::string;

    // If index layer vector size is AUTOMATIC,
    // set it to the same as the data layer size.
    if (index_size == AUTOMATIC &&
        (data_structure_name.find("svpq") != string::npos ||
         data_structure_name.find("skipvector") != string::npos)) {
      index_size = chunk_size;
    }

#ifndef PQ_BENCHMARK
    if (index_size == SKIPARRAY_SIM_MODE) {
      // The skipvector is not coded to support 0 index layers,
      // so set this to the minimum, 1.
      layers = 1;
    } else if (layers == AUTOMATIC) {
      // If layers is set to AUTOMATIC, automatically compute ideal
      // value: ceil(log base s of (e/c)), where e is the number of elements.

      // If the index or data layer is set to a special value, use 2 instead.
      int64_t const data_ratio = chunk_size <= 0 ? 2 : chunk_size;
      int64_t const idx_ratio = index_size <= 0 ? 2 : index_size;

      double const data_node_count_target =
          static_cast<double>(std::abs(key_range)) *
          (initial_pop_pctg / 100.0) / static_cast<double>(data_ratio);

      double first_idx_node_count = data_node_count_target /
                                    static_cast<double>(phantom_cap) /
                                    static_cast<double>(idx_ratio);

      // Increase the number of layers one at a time until the uppermost layer
      // is at a good size; between 1 and the square root of idx_ratio.
      for (layers = 1; first_idx_node_count > std::sqrt(idx_ratio); ++layers) {
        first_idx_node_count /= static_cast<double>(idx_ratio);
      }
    }
#endif

    // If nteams is AUTOMATIC, set it to the number of NUMA zones.
    if (nteams == AUTOMATIC) {
      nteams = NUMA_ZONES;
    }

    // We can't have teams without threads, so if nteams exceeds nthreads,
    // reduce it to nthreads.
    if (nteams > nthreads) {
      nteams = nthreads;
    }
  }

  /// Report the current values of the configuration object
  void report() const {
    using std::cout;
    using std::endl;

    if (output_raw)
      return;

#ifndef NDEBUG
    // Clearly mark any results where NDEBUG is not set.
    cout << "NDEBUG is undefined, so certain implementations may run slower! "
         << endl;
#endif

    cout << "configuration ";
    print_config(std::string(", "));
    cout << endl;
  }

  /// Report the current values of the configuration object as a comma-separated
  /// line
  void report_raw() const {
    using std::cout;

#ifndef NDEBUG
    // Clearly mark any results where NDEBUG is not set.
    cout << "DEBUG-";
#endif

    cout << data_structure_name << ",";
    print_config(std::string(","));
  }

  /// s is the separator
  void print_config(const std::string &s) const {
    using std::cout;

    // Legend, -bc
    cout << "(bcdefgiklmnprstxz)" << s << bench_name << s << chunk_size << s;

    // -d
#ifdef PQ_BENCHMARK
    cout << nteams;
#else
    cout << merge_threshold;
#endif

    // All other parameters
    cout << s << early_exit_pctg << s << traversal_pctg << s << range_pctg << s
         << interval << s << key_range << s << lookup << s << reclaim_memory
         << s << policy << s << initial_pop_pctg << s << readonly_traversal_pctg
         << s << index_size << s << nthreads << s << layers << s;

    // -p
#ifdef PQ_BENCHMARK
    cout << phantom_cap;
#else
    cout << range_dist;
#endif
  }
};
