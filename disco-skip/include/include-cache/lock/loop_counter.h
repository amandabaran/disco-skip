#pragma once

#include <cstdint>
#include <pthread.h>

/// A struct used to count the number of iterations a loop has spent spinning
/// waiting to acquire a spinlock. If a certain threshold is reached, yield.
struct loop_counter {
  static constexpr uint64_t MAX = 1024ULL;

  uint64_t iterations = 0;

  void count() {
    ++iterations;
    if (iterations > MAX) {
      sched_yield();
      iterations = 0;
    }
  }
};
