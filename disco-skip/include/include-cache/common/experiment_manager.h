#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <csignal>
#include <unistd.h>

#include "config.h"
#include "defs.h"

/// experiment_manager keeps track of all data that we measure during an
/// experiment, and any data we use to manage the execution of the experiment
struct experiment_manager {
  /// Barriers for controlling the execution of the program
  std::array<std::atomic<int>, 3> barriers;

  /// Start time of the experiment
  std::chrono::high_resolution_clock::time_point start_time;

  /// End time of the experiment
  std::chrono::high_resolution_clock::time_point end_time;

  /// These are the actual global statistics
  std::array<std::atomic<uint64_t>, event_types::COUNT> stats;

  /// The flag used to stop the experiment
  std::atomic<bool> running;

  /// Static reference to singleton instance of this struct
  static experiment_manager *instance;

  /// Construct the global context by initializing the barriers and zeroing the
  /// counters
  experiment_manager() {
    running.store(true);
    experiment_manager::instance = this;
    for (auto &barrier : barriers)
      barrier = 0;
    for (auto &stat : stats)
      stat = 0;
  }

  /// Report all of the statistics that we counted
  void report(config *cfg) {
    if (cfg->output_raw) {
      report_raw(cfg);
    } else {
      report_pretty(cfg);
    }
  }

  /// Report all of the configuration settings and statistics that we counted as
  /// a comma separated line
  void report_raw(config *cfg) {
    using std::cout;
    using std::endl;
    using namespace std::chrono;

    cfg->report_raw();

    // Print a short marker indicating what the next two fields are that won't
    // interfere with the ability to parse this output as a CSV
    cout << ",(time e-thruput e-count t-thruput t-count),";

    // Report throughput, execution time, and operations completed
    uint64_t const e_ops = count_elementals();
    uint64_t const t_ops = count_iterations();
    auto dur = duration_cast<duration<double>>(end_time - start_time).count();
    double const e_thruput = static_cast<double>(e_ops) / dur;
    double const t_thruput = static_cast<double>(t_ops) / dur;
    cout << dur << "," << e_thruput << "," << e_ops << "," << t_thruput << ","
         << t_ops << endl;
  }

  /// Report all of the statistics that we counted for human readability
  void report_pretty(config *cfg) {
    using std::cout;
    using std::endl;
    using namespace std::chrono;

    // Report throughput, execution time, and operations completed
    uint64_t const ops = count_operations();
    auto dur = duration_cast<duration<double>>(end_time - start_time).count();
    double const thruput = static_cast<double>(ops) / dur;
    cout << "Throughput: " << thruput << endl;
    cout << "Execution Time: " << dur << endl;
    cout << "Operations: " << ops << endl;

    if (!cfg->verbose)
      return;
    uint64_t const e_ops = count_elementals();
    uint64_t const t_ops = count_iterations();
    double const e_thruput = static_cast<double>(e_ops) / dur;
    double const t_thruput = static_cast<double>(t_ops) / dur;
    cout << "Elemental Throughput: " << e_thruput << endl;
    cout << "Iteration Throughput: " << t_thruput << endl;

    constexpr std::array<std::string_view, COUNT> titles = {
        "lookup hit",   "lookup miss", "insert hit",  "insert quantity",
        "insert miss",  "remove hit",  "remove miss", "for_each",
        "for_each sum", "range",       "range sum",   "early exits",
        "readonly"};
    for (size_t i = 0; i < COUNT; ++i) {
      cout << "  " << titles[i] << " : " << stats.at(i) << endl;
    }
  }

  /// Before launching experiments, use this to ensure that the threads start at
  /// the same time.  This uses two barriers internally, with a timer read
  /// between the first and second, so that we don't read the time while threads
  /// are still being configured, but we do ensure we read it before any work is
  /// done
  void sync_before_launch(size_t id, config *cfg) {
    // Barrier #1: ensure everyone is initialized
    synchronize(0, cfg->nthreads);
    // Now get the time
    if (id == 0) {
      start_time = std::chrono::high_resolution_clock::now();
      signal(SIGALRM, experiment_manager::stop_running);

      if (cfg->interval == 0) {
        // If interval is set to 0, stop immediately.
        // (This can be used to test filling.)
        stop_running(0);
      } else {
        // Otherwise, set up the alarm as normal.
        alarm(cfg->interval);
      }
    }
    // Barrier #2: ensure we have the start time before work begins
    synchronize(1, cfg->nthreads);
  }

  /// Method used to stop test execution.
  static void stop_running(int) {
    experiment_manager::instance->running.store(false);
  }

  /// After threads finish the experiments, use this to have them all wait
  /// before getting the stop time.
  void sync_after_launch(size_t id, int nthreads) {
    // wait for all threads
    synchronize(2, nthreads);
    // now get the time
    if (id == 0)
      end_time = std::chrono::high_resolution_clock::now();
  }

  /// Arrive at one of the barriers.
  void synchronize(size_t i, int nthreads) {
    barriers.at(i)++;
    while (barriers.at(i) < nthreads)
      ;
  }

  /// Get a count of the number of operations that were completed.
  /// Note: this is brittle, be sure to update this when introducing a new
  /// operation type.
  uint64_t count_operations() {
    return count_elementals() + count_iterations();
  }

  /// Get a count of the number of elementals that were completed.
  uint64_t count_elementals() {
    return stats[LOOKUP_HIT] + stats[LOOKUP_MISS] + stats[INSERT_HIT] +
           stats[INSERT_MISS] + stats[REMOVE_HIT] + stats[REMOVE_MISS];
  }

  /// Get a count of the number of iterations that were completed.
  uint64_t count_iterations() { return stats[FOREACH] + stats[RANGE]; }
};

// Static field declaration
experiment_manager *experiment_manager::instance;
