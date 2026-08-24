#pragma once

#include "sched.h"

#include <cassert>

#include "cpu_policy_tools.h"
#include "machine_defines.h"

/// This class keeps track of this thread's ID, which core it is pinned to,
/// and which NUMA zone it is in. There is one instance per thread.
class thread_context {
  static thread_local thread_context *this_ctx;

  /// Construct a thread's context by storing its ID and creating its PRNG
  explicit thread_context(size_t _id, cpu_policy policy) : id(_id) {
    this_ctx = this;
    size_t pin_to = 0;
    cpu_policy_tools::pin(policy, id, pin_to, numa_zone);

    // For policies other than OS, "numa_zone" must be
    // initialized, and set within a valid range.
    assert(policy == OPERATING_SYSTEM || numa_zone < NUMA_ZONES);

    // Finally, we do the pinning.
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(pin_to, &cpuset);
    sched_setaffinity(0, sizeof(cpu_set_t), &cpuset);
  }

  ~thread_context() = default;

public:
  /// The thread's id
  size_t const id;

  /// The NUMA zone this thread lives in.
  size_t numa_zone = static_cast<size_t>(-1);

  static thread_context *get_ctx() {
    assert(this_ctx != nullptr);
    assert(static_cast<size_t>(this_ctx->numa_zone) < NUMA_ZONES);
    return this_ctx;
  }

  static size_t get_numa_zone() {
    assert(this_ctx != nullptr);
    assert(static_cast<size_t>(this_ctx->numa_zone) < NUMA_ZONES);
    return this_ctx->numa_zone;
  }

  size_t get_team(cpu_policy policy, size_t nteams) const {
    return cpu_policy_tools::get_team(policy, id, nteams);
  }

  static thread_context *create_context(int64_t id, cpu_policy policy) {
    assert(this_ctx == nullptr);
    this_ctx = new thread_context(id, policy);
    return this_ctx;
  }

  static void destroy_context() {
    assert(this_ctx != nullptr);
    delete this_ctx;
    this_ctx = nullptr;
  }
};

// Note: Be sure to include the following line of code in your main file, or in
// a file only included once from it!

// thread_local thread_context *thread_context::this_ctx = nullptr;
