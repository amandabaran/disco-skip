#pragma once

#include <array>
#include <iostream>
#include <stdexcept>
#include <string>

#include "cpu_policy.h"
#include "machine_defines.h"

/// Utility functions for interacting conveniently with the CPU thread layout
/// policy, NUMA zones, teams, and so on.
class cpu_policy_tools {

public:
  // The number of valid CPU policies.
  static constexpr int VALID_POLICIES = 4;

  // The total number of values in the cpu_policy enum,
  // including ones that do not represent valid CPU policies.
  static constexpr int TOTAL_POLICIES = 5;

  // Abbreviations of the valid policies.
  static constexpr std::array<std::string_view, VALID_POLICIES> ABBREVIATIONS =
      {"os", "f1", "f1hl", "rr"};

  // Pretty names for all the policies.
  static constexpr std::array<std::string_view, TOTAL_POLICIES> PRETTY_NAMES = {
      "Operating System", "Fill One NUMA Zone (including hyperthreading)",
      "Fill One NUMA Zone (hyperthread last)", "Round Robin", "Invalid"};

  // Parse a char* as a cpu_policy.
  static cpu_policy parse(const std::string_view &arg) {
    // If the arg is of the format \d\0, parse it as a number.
    if (arg.size() == 1) {
      char const c = arg[0];
      if (c >= '0' && c <= '3') {
        return static_cast<cpu_policy>(c - '0');
      }
    }

    // Else, see if it matches an abbreviation string.
    for (int i = 0; i < VALID_POLICIES; ++i) {
      if (arg == ABBREVIATIONS[i]) {
        return static_cast<cpu_policy>(i);
      }
    }

    // Indicate that the char could not be parsed.
    return cpu_policy::INVALID;
  }

  // Computes the appropriate core ID and numa zone to pin a given thread ID to
  // in accordance with the FILL_ONE policy.
  // NB: This method makes a lot of assumptions about how the core IDs are laid
  // out...
  static void pin_fill_one(size_t id, size_t &pin_to, size_t &numa_zone) {
    constexpr size_t THREADS_PER_ZONE = MAX_THREADS / NUMA_ZONES;

    // The index of the NUMA zone this thread is in.
    numa_zone = id / THREADS_PER_ZONE;

    // The remainder of the above division.
    size_t const remainder = id % THREADS_PER_ZONE;

    // The core ID to pin to.
    // We start by setting it to the appropriate NUMA zone.
    pin_to = numa_zone;

    // Next, we push it to the correct core in the NUMA zone.
    pin_to += remainder * NUMA_ZONES;
  }

  // Computes the appropriate core ID and numa zone to pin a given thread ID to
  // in accordance with the FILL_ONE_HYPERTHREAD_LAST policy.
  // NB: This method makes a lot of assumptions about how the core IDs are laid
  // out...
  static void pin_fill_one_hyperthread_last(size_t id, size_t &pin_to,
                                            size_t &numa_zone) {
    constexpr size_t NUM_CORES = MAX_THREADS / THREADS_PER_CORE;
    constexpr size_t CORES_PER_ZONE = NUM_CORES / NUMA_ZONES;

    // If this variable is n, then this thread will be the nth hyperthread added
    // to this specific core, zero-indexed.
    size_t const hyperthread_id = id / NUM_CORES;

    // The remainder of the above division.
    size_t remainder = id % NUM_CORES;

    // The index of the NUMA zone this thread is in.
    numa_zone = remainder / CORES_PER_ZONE;

    // Again, the remainder of the above division.
    remainder = remainder % CORES_PER_ZONE;

    // The core ID to pin to.
    // We start by setting it to the appropriate NUMA zone.
    pin_to = numa_zone;

    // Next, we push it to the correct core in the NUMA zone.
    pin_to += remainder * NUMA_ZONES;

    // Then, we push it to the correct hyperthread for the core.
    pin_to += (MAX_THREADS / THREADS_PER_CORE) * hyperthread_id;
  }

  // Computes the appropriate core ID and numa zone to pin a given thread ID to
  // in accordance with the ROUND_ROBIN policy.
  // NB: This method makes a lot of assumptions about how the core IDs are laid
  // out...
  static void pin_round_robin(size_t id, size_t &pin_to, size_t &numa_zone) {
    // This is super easy; just pin in linear order.
    numa_zone = id % NUMA_ZONES;
    pin_to = id;
  }

  static void pin(cpu_policy policy, size_t id, size_t &pin_to,
                  size_t &numa_zone) {
    switch (policy) {
    case OPERATING_SYSTEM:
      // TODO: We need to figure out how to find the NUMA zone we're in...
      break;

    case FILL_ONE:
      pin_fill_one(id, pin_to, numa_zone);
      break;

    case FILL_ONE_HYPERTHREAD_LAST:
      pin_fill_one_hyperthread_last(id, pin_to, numa_zone);
      break;

    case ROUND_ROBIN:
      pin_round_robin(id, pin_to, numa_zone);
      break;

    default:
      throw std::logic_error("Invalid CPU policy: " + std::to_string(policy));
    }
  }

  // Computes the appropriate team to assign a given thread ID to in accordance
  // with the FILL_ONE policy.
  // NB: This method makes a lot of assumptions about how the core IDs are laid
  // out...
  static size_t get_team_fill_one(size_t id, size_t nteams) {
    size_t const threads_per_team = MAX_THREADS / nteams;
    return id / threads_per_team;
  }

  // Computes the appropriate team to assign a given thread ID to in accordance
  // with the FILL_ONE_HYPERTHREAD_LAST policy.
  // NB: This method makes a lot of assumptions about how the core IDs are laid
  // out...
  static size_t get_team_fill_one_hyperthread_last(size_t id, size_t nteams) {
    constexpr size_t NUM_CORES = MAX_THREADS / THREADS_PER_CORE;
    if (NUM_CORES >= nteams) {
      // Case 1: CORES == teams, so we have a bijection between cores and teams.
      // Case 2: CORES > teams, so we have teams that span multiple cores.
      size_t const cores_per_team = NUM_CORES / nteams;
      size_t const remainder = id % NUM_CORES;
      return remainder / cores_per_team;
    }

    // Case 3: teams > CORES, so we have cores with multiple teams.
    return id % nteams;
  }

  // Computes the appropriate team to assign a given thread ID to in accordance
  // with the ROUND_ROBIN policy.
  // NB: This method makes a lot of assumptions about how the core IDs are laid
  // out...
  static size_t get_team_round_robin(size_t id, size_t nteams) {
    return id % nteams;
  }

  // Determine which team to put this thread onto, given the number of teams and
  // the policy used to map thread IDs to NUMA zones. If the number of teams is
  // equal to the number of NUMA zones, then each NUMA zone will constitute one
  // team. Otherwise, it tries to assign teams as intelligently as it can.
  // (TODO: define this more rigorously.)
  static size_t get_team(cpu_policy policy, size_t id, size_t nteams) {
    switch (policy) {

    case FILL_ONE:
      return get_team_fill_one(id, nteams);

    case FILL_ONE_HYPERTHREAD_LAST:
      return get_team_fill_one_hyperthread_last(id, nteams);

    case ROUND_ROBIN:
      return get_team_round_robin(id, nteams);

    default:
      throw std::logic_error("Unsupported CPU policy: " +
                             std::to_string(policy));
    }

    throw std::logic_error("Invalid CPU policy: " + std::to_string(policy));
  }

  static void dump(cpu_policy policy, size_t thread_count, size_t nteams) {
    using std::cout;
    using std::endl;

    if (policy == OPERATING_SYSTEM) {
      // This policy is unsupported, so...
      return;
    }

    if (policy < 0 || policy >= TOTAL_POLICIES) {
      cout << "Warning: invalid value for cpu_policy: " << policy << "!"
           << endl;
    } else {
      cout << "Policy: " << PRETTY_NAMES[policy] << endl;
    }

    for (size_t i = 0; i < thread_count; ++i) {
      size_t pin_to = -1;
      size_t numa_zone = -1;
      pin(policy, i, pin_to, numa_zone);
      size_t const team = get_team(policy, i, nteams);
      cout << "Thread " << i << " pinned to core " << pin_to << " on team "
           << team << " in numa zone " << numa_zone << endl;
    }
  }
};
