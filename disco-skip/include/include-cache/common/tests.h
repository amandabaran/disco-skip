/// tests.h defines any tempalted functions that we use as microbenchmarks on
/// data structures.  Currently there are two tests, for data structures that
/// present a map interface or a queue interface

#pragma once

#include "unistd.h"
#include "x86intrin.h"

#include <algorithm>
#include <csignal>
#include <random>
#include <thread>

#include "config.h"
#include "defs.h"
#include "entry.h"
#include "experiment_manager.h"
#include "thread_context.h"

inline uint64_t get_time() {
  unsigned int _ = 0;
  return static_cast<uint64_t>(__rdtscp(&_));
}

// Fill the data structure before the test.
template <class ds_t> void fill(ds_t &DS, int id, config *cfg) {
  using K = typename ds_t::KEY_TYPE;
  using V = typename ds_t::VAL_TYPE;

  auto nthreads = cfg->nthreads;
  K const key_range = std::abs(cfg->key_range);

  if (cfg->initial_pop_pctg == 0.0)
    // There are no elements to fill, and this check avoids a divide-by-zero.
    return;

  // Work out the increment and fraction to skip.
  // TODO: This works but could use some revision and explanation...
  double const p = static_cast<double>(cfg->initial_pop_pctg) / 100.0;
  double const p_inverse = 1.0 / p;
  int increment = (int)std::floor(p_inverse);
  double const total_fraction_to_skip = 1.0 - p;
  double const fraction_filled_by_increment =
      1.0 / static_cast<double>(increment);
  double const fraction_skipped_by_increment =
      1.0 - fraction_filled_by_increment;
  double to_skip = total_fraction_to_skip - fraction_skipped_by_increment;
  to_skip *= increment;
  increment *= cfg->nthreads;

  if (nthreads == 1) {
    // If thread count is 1, use the fast sequential fill method to speed it
    // up moderately. Fill backwards, so inserts are O(1) instead of O(log n).
    double skip_accumulator = 0;
    for (int64_t i = 0; i < cfg->key_range; i += increment) {

      // If we need to skip this element to respect the fullness requirements,
      // do so.
      skip_accumulator += to_skip;
      if (skip_accumulator >= 1.0) {
        skip_accumulator -= 1.0;
        continue;
      }

      // Otherwise, proceed with the insert.
      K const k = key_range - i;
      V const v = static_cast<V>(k);
      DS.insert_seq({k, v});
    }
    return;
  }

  // If there are multiple threads, we have to use the normal insert method,
  // and do something more complicated to save time and ensure allocations are
  // probabilistically fair across NUMA zones.

  // start_offset indicates where to begin inserting. To reduce contention, each
  // thread starts insertion at a different place. Once the end is reached, the
  // thread will wrap around and start filling from the start.

  // First, calculate approximately where this thread should start.
  double start_offset_double = (((double)id) / nthreads) * key_range;

  // We now have to round down to a multiple of nthreads. We do this by dividing
  // by nthreads, casting to int, and multiplying by nthreads again.
  start_offset_double /= nthreads;
  K start_offset = start_offset_double;
  start_offset *= nthreads;

  // Finally, add thread id to determine the key to start on.
  start_offset += id;

  // Start the skip_accumulator at a different value for each thread, to try to
  // distribute the skipped elements more evenly and to better approximate the
  // number of skips the user wants.
  double skip_accumulator = static_cast<double>(id) / nthreads;

  // Insert keys from the start point to the end.
  K k;
  for (k = start_offset; k < key_range; k += increment) {
    skip_accumulator += to_skip;
    if (skip_accumulator >= 1.0) {
      skip_accumulator -= 1.0;
      continue;
    }

    V const v = static_cast<V>(k);
    DS.insert({k, v});
  }

  // Calculate overshoot. This is the amount we overshot the end of key_range
  // by, rounded down to a multiple of nthreads. This ensures that we do not
  // lose skipped elements when we wrap around to the start point.
  int overshoot = k - key_range;
  overshoot /= nthreads;
  overshoot *= nthreads;

  // Now, insert keys from the beginning of the map to the start point.
  for (k = id + overshoot; k < start_offset; k += increment) {
    skip_accumulator += to_skip;
    if (skip_accumulator >= 1.0) {
      skip_accumulator -= 1.0;
      continue;
    }

    V const v = static_cast<V>(k);
    DS.insert({k, v});
  }
}

/// Run tests on data structures that implement an ordered map interface. This
/// requires map_t to have insert, lookup, remove, for_each, and range
/// operations, and to operate on key / value pairs
template <class map_t> void ordered_map_test(config *cfg) {
  using std::uniform_int_distribution;
  using std::uniform_real_distribution;
  using K = typename map_t::KEY_TYPE;
  using V = typename map_t::VAL_TYPE;

  // Create a global stats object for managing this experiment
  experiment_manager exp;

  // Create a map and initialize it to 50% full
  map_t MAP(cfg);

  // This is the benchmark task for threads doing traversals
  auto traversal_task = [&](int id) {
    std::mt19937 mt(id * LARGE_PRIME);
    std::array<unsigned int, event_types::COUNT> stats = {0};

    // Do this thread's part to populate the data structure before the first
    // thread barrier.
    fill<map_t>(MAP, id, cfg);

    // set up distributions for our PRNG
    uniform_int_distribution<K> key_dist(0, cfg->key_range - 1);
    uniform_int_distribution<K> early_exit_dist(0, cfg->key_range / 2);
    uniform_real_distribution<float> percent_dist(0, 100.0);

    // Synchronize threads and get time
    exp.sync_before_launch(id, cfg);

    // Run randomly-chosen operations for a fixed interval
    while (exp.running.load()) {
      // Check if we should be testing for_each() or range()
      float const iteration_type = percent_dist(mt);

      V sum = 0;
      bool const readonly = percent_dist(mt) < cfg->readonly_traversal_pctg;
      bool const exit_early = percent_dist(mt) < cfg->early_exit_pctg;

      if (iteration_type >= cfg->range_pctg) {
        // Perform a sum operation utilizing for_each().
        if (exit_early) {
          // TODO: Since the skipvector is ordered we can get a MUCH more
          // uniform early exit distribution by simply choosing a key
          // uniformly at random and stopping once we reach a key >= it.
          // This is a holdover from the iteration project.
          size_t exit_count = early_exit_dist(mt);
          size_t visit_count = 0;
          MAP.for_each([&](K, V const &v, bool &exit) {
            ++visit_count;
            sum += v;
            if (visit_count >= exit_count)
              exit = true;
          });
          stats[EARLY_EXIT]++;
        } else
          // Just do a regular end-to-end for_each.
          MAP.for_each([&](K, V const &v, bool &) { sum += v; });

        stats[FOREACH]++;
        stats[FOREACH_SUM] += sum;
      } else {
        // Else, we are doing a range query

        K const start = key_dist(mt);
        K end;

        if (cfg->range_dist == AUTOMATIC) {
          // If range_dist is set to AUTOMATIC,
          // then choose start and end uniformly at random,
          // resulting in variable length range operations.
          uniform_int_distribution<K> end_dist(start, cfg->key_range);
          end = end_dist(mt);
        } else
          // Otherwise, set an end that gives the desired range length.
          end = start + cfg->range_dist;

        if (exit_early) {
          // Pick a random point during the range to bail.
          // NB: Unordered maps don't implement range() at all, so, here we
          // can implement early exit in the sensible way without worrying
          // about fair comparisons to unordered maps.
          uniform_int_distribution<K> early_exit_range_dist(start, end);
          K key_exit = early_exit_range_dist(mt);
          MAP.range(start, end, [&](K k, V const &v, bool &exit) {
            sum += v;
            if (k >= key_exit)
              exit = true;
          });
          stats[EARLY_EXIT]++;
        } else
          // Just do a normal range operation
          MAP.range(start, end, [&sum](K, V const &v, bool &) { sum += v; });

        stats[RANGE]++;
        stats[RANGE_SUM] += sum;
      }

      // Increment readonly count (for either type of iteration)
      if (readonly)
        stats[READONLY]++;
    }

    // arrive at the last barrier, then get the timer again
    exp.sync_after_launch(id, cfg->nthreads);

    // merge stats into global
    for (size_t i = 0; i < event_types::COUNT; ++i)
      exp.stats.at(i).fetch_add(stats.at(i));
  };

  // This is the benchmark task for threads doing elementals
  auto elemental_task = [&](int id) {
    std::mt19937 mt(id * LARGE_PRIME);
    std::array<unsigned int, event_types::COUNT> stats = {0};

    // Do this thread's part to populate the data structure before the first
    // thread barrier.
    fill<map_t>(MAP, id, cfg);

    // set up distributions for our PRNG
    uniform_int_distribution<K> key_dist(0, cfg->key_range - 1);
    uniform_real_distribution<float> percent_dist(0, 100.0);

    // Synchronize threads and get time
    exp.sync_before_launch(id, cfg);

    // Run randomly-chosen operations for a fixed interval
    while (exp.running.load()) {
      // Do a random mix of lookup/insert/remove
      float const action = percent_dist(mt);
      K const key = key_dist(mt);

      // Lookup
      if (action < cfg->lookup) {
        V val = static_cast<V>(key);
        if (MAP.contains(key, val))
          stats[LOOKUP_HIT]++;
        else
          stats[LOOKUP_MISS]++;
      }

      // Insert
      else if (action < cfg->lookup + (100.0 - cfg->lookup) / 2.0) {
        V const val = static_cast<V>(key);
        if (MAP.insert({key, val}))
          stats[INSERT_HIT]++;
        else
          stats[INSERT_MISS]++;
      }

      // Remove
      else {
        if (MAP.remove(key))
          stats[REMOVE_HIT]++;
        else
          stats[REMOVE_MISS]++;
      }
    }

    // arrive at the last barrier, then get the timer again
    exp.sync_after_launch(id, cfg->nthreads);

    // merge stats into global
    for (size_t i = 0; i < event_types::COUNT; ++i)
      exp.stats.at(i).fetch_add(stats.at(i));
  };

  // Launch more worker threads as needed.
  int const nthreads = cfg->nthreads;
  std::vector<std::thread> threads;
  size_t traversal_threads = 0;

  // If a negative value is set for traversal_pctg, contrive a fraction that
  // will ensure the number of threads doing traversals is exactly the
  // absolute value of that.
  float traversal_pctg = cfg->traversal_pctg;
  if (traversal_pctg < 0.0F) {
    float const tthreads = -std::round(traversal_pctg);

    // NB: tthreads / nthreads gives us the proportion as a fraction. We
    // multiply by 100 to convert to percentage. We subtract 50% from the
    // denominator to ensure that when we round up, we round up to the desired
    // value. (Otherwise, a small episilon resulting from the integer division
    // may result in an extra thread running traversals.)
    traversal_pctg = (tthreads - 0.5F) / static_cast<float>(nthreads);
    traversal_pctg *= 100.0F;
  }

  // NB: This setup thread becomes thread ID 0,
  // so start creating worker threads at thread ID 1.
  for (int i = 1; i < nthreads; ++i) {
    // If adding this thread as an elemental will cause the actual traversal
    // thread ratio to fall below the user's desired ratio, make it a traversal
    // thread.
    // NB: We do this in this somewhat complicated manner to ensure an even
    // spread of traversal and elemental threads across chips and cores.
    auto curr_pctg = static_cast<float>(traversal_threads);
    curr_pctg /= static_cast<float>(i);
    curr_pctg *= 100.0F;
    if (curr_pctg < traversal_pctg) {
      threads.emplace_back(traversal_task, i);
      ++traversal_threads;
    } else
      threads.emplace_back(elemental_task, i);
  }

  // Finally, put this thread to work (with ID 0).
  auto curr_pctg = static_cast<float>(traversal_threads);
  curr_pctg /= static_cast<float>(nthreads);
  curr_pctg *= 100.0F;
  if (curr_pctg < traversal_pctg) {
    ++traversal_threads;
    traversal_task(0);
  } else
    elemental_task(0);

  // Once the this thread is done working, join the other threads.
  for (int i = 0; i < nthreads - 1; ++i)
    threads[i].join();

  if (cfg->verbose)
    MAP.verbose_analysis();

  // Report statistics from the experiment
  exp.report(cfg);

  if (!MAP.verify())
    std::cout << "Warning: map failed final self-validation!" << std::endl;
}

/// Run tests on data structures that implement a priority queue interface.
/// This requires queue_t to have insert, contains, and extract_min operations.
template <class queue_t> void priority_queue_test(config *cfg) {
  using std::uniform_int_distribution;
  using std::uniform_real_distribution;
  using ENTRY = typename queue_t::ENTRY_TYPE;
  using K = typename queue_t::KEY_TYPE;
  using V = typename queue_t::VAL_TYPE;

  // Create a global stats object for managing this experiment
  experiment_manager exp;

  // Create a queue and initialize it to 50% full
  queue_t QUEUE(cfg);

  // This is the benchmark task for worker threads
  auto task = [&](int id) {
    std::mt19937 mt(id * LARGE_PRIME);
    K current_min = 0;
    int64_t my_delta = 0; // (total count of removed minus inserted elements)
    std::vector<ENTRY> batch = std::vector<ENTRY>();

    std::array<unsigned int, event_types::COUNT> stats = {0};
    if constexpr (queue_t::NUMA_AWARE)
      thread_context::create_context(id, cfg->policy);

    // Do this thread's part to populate the data structure before the first
    // thread barrier.
    fill<queue_t>(QUEUE, id, cfg);

    uniform_real_distribution<float> percent_dist(0, 100.0);

    // Synchronize threads and get time
    exp.sync_before_launch(id, cfg);

    // Run randomly-chosen operations for a fixed interval
    while (exp.running.load()) {

      // Do a random mix of insert/extract_min
      float const action = percent_dist(mt);

      // Insert
      if (action >= cfg->lookup) {
        K key;

        // If I can reset my delta to 0 by inserting more than one element,
        // do so; otherwise, just insert one element.
        int64_t const batch_size = my_delta >= 1 ? my_delta : 1;

        if (cfg->key_range > 0) {
          // key_range is positive (normal), so use RNG.
          // If we simply used a distribution between 0 and key_range, the
          // inserts would skew heavily towards the head of the priority queue,
          // which would be terrible for concurrency. Thus, we offset the key
          // distribution by the largest key we've extracted.
          uniform_int_distribution<K> key_dist(current_min,
                                               current_min + cfg->key_range);

          for (int64_t i = 0; i < batch_size; ++i) {
            key = key_dist(mt);
            V const val = static_cast<V>(key);
            batch.emplace_back(key, val);
          }

        } else {
          // key_range is nonpositive, so use RDTSCP.
          for (int64_t i = 0; i < batch_size; ++i) {
            key = get_time();
            V const val = static_cast<V>(key);
            batch.emplace_back(key, val);
          }
        }

        std::sort(batch.begin(), batch.end());

        auto b = batch.cbegin();
        auto e = batch.cend();
        QUEUE.insert(b, e);
        assert(b == e);
        stats[INSERT_HIT]++;
        stats[INSERT_QUANTITY] += batch.size();
        batch.clear();
        my_delta -= batch_size;
      }

      // Extract_min
      else {

        // We don't care about k or v, but extract_min() returns it via
        // reference parameter, so we need to give it variables to write into.
        ENTRY entry;
        if (QUEUE.extract_min(entry)) {
          // Just consider a PQ's extract to be equivalent to a map's remove.
          stats[REMOVE_HIT]++;
          ++my_delta;

          K const &k = entry.get_k();
          if (k > current_min)
            current_min = k;

        } else {
          stats[REMOVE_MISS]++;

          // If the PQ is empty, and the benchmark is set to 100% extractions,
          // just stop. For some PQ implementations, we may have some leftover
          // elements in some thread- or team-private nodes, but this check
          // stops the performance from varying wildly depending on how much of
          // the benchmark was spent polling an empty PQ repeatedly.
          if (cfg->lookup >= 100.0) {
            experiment_manager::stop_running(0);
            break;
          }
        }
      }
    }

    // arrive at the last barrier, then get the timer again
    exp.sync_after_launch(id, cfg->nthreads);

    // merge stats into global
    for (size_t i = 0; i < event_types::COUNT; ++i)
      exp.stats.at(i).fetch_add(stats.at(i));

    if constexpr (queue_t::NUMA_AWARE)
      thread_context::destroy_context();
  };

  // Launch more worker threads as needed.
  int const nthreads = cfg->nthreads;
  std::vector<std::thread> threads;

  // NB: This setup thread becomes thread ID 0,
  // so start creating worker threads at thread ID 1.
  for (int i = 1; i < nthreads; ++i)
    threads.emplace_back(task, i);

  // Finally, put this thread to work (with ID 0).
  task(0);

  // Once the this thread is done working, join the other threads.
  for (int i = 0; i < nthreads - 1; ++i)
    threads[i].join();

  if (cfg->verbose) {
    cpu_policy_tools::dump(cfg->policy, cfg->nthreads, cfg->nteams);
    QUEUE.verbose_analysis();
  }

  // Report statistics from the experiment
  exp.report(cfg);

  if (!QUEUE.verify())
    std::cout << "Warning: queue failed final self-validation!" << std::endl;
}
