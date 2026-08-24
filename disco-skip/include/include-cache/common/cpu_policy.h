#pragma once

/// An enumerated type which indicates the CPU allocation policy.
enum cpu_policy {
  /// Do no pinning. Allow the operating system to decide.
  OPERATING_SYSTEM = 0,

  /// Fill one NUMA zone completely, including hyperthreads, before moving to
  /// the next NUMA zone.
  FILL_ONE = 1,

  /// As above, but save hyperthreading for last.
  FILL_ONE_HYPERTHREAD_LAST = 2,

  /// Rotate between NUMA zones for each and every new thread.
  ROUND_ROBIN = 3,

  /// Dummy value indicating an invalid CPU policy.
  INVALID = 4
};
