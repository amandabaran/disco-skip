#pragma once

#include <cstdint>

// The two rules that decide the range table lock's correctness, with NO
// dependencies -- not on dory, not on a Layout, not on a NIC.
//
// They live apart from range_table_lock.hpp so they can be unit-tested for
// what they are: pure predicates. Including the lock itself in a test drags in
// dory/conn -> dory/ctrl -> dory/memory -> spdlog -> fmt, and the test then
// fails on a missing SPDLOG_ACTIVE_LEVEL or an unresolved fmt symbol, which
// reads as a broken toolchain rather than a broken rule.
//
// Both failures these guard against are SILENT. A conflict rule that is too
// permissive lets a writer run inside a scan and the scan tears; a tie-break
// that is not a total order lets both sides conclude they won, with the same
// result. Neither shows up in a throughput number.

namespace dory {
namespace rangelock {

enum State : uint64_t { kFree = 0, kScan = 1, kWrite = 2 };

/// Do two published entries conflict?
///
/// Scan/scan does NOT: two scans only read, and reads commute. Making them
/// conflict would serialise most of a scan-heavy workload against itself for
/// no correctness gain. Write/write does not either -- per-key atomicity is
/// the store's own business, and this lock exists only to close the gap
/// between a multi-key scan and the writes that would land inside it.
inline bool conflicts(uint64_t a_state, uint64_t a_lo, uint64_t a_hi,
                      uint64_t b_state, uint64_t b_lo, uint64_t b_hi) {
  if (a_state == kFree || b_state == kFree) return false;
  if (a_state == kScan && b_state == kScan) return false;
  if (a_state == kWrite && b_state == kWrite) return false;
  return a_lo <= b_hi && b_lo <= a_hi;          // the intervals meet
}

/// Does the entry stamped (my_stamp, my_owner) lose to (their_stamp,
/// their_owner)?
///
/// Earliest publisher wins, owner id breaks an exact tie. Owner ids are
/// unique, so the pair is a TOTAL ORDER and exactly one of any conflicting
/// pair loses -- if both could lose the pair deadlocks, and if neither could,
/// mutual exclusion is gone.
///
/// The stamp leads rather than the id because id-ordering is systematically
/// unfair: client 1 would never wait and client 64 would always lose. This
/// harness sums per-client throughputs, and it already documents what happens
/// when clients finish at very different rates -- the measurement windows stop
/// overlapping and the sum overstates a total the system never delivered.
/// Correctness needs only SOME total order; fairness is why it is the clock.
inline bool losesTo(uint64_t my_stamp, uint64_t my_owner,
                    uint64_t their_stamp, uint64_t their_owner) {
  if (their_stamp != my_stamp) return their_stamp < my_stamp;
  return their_owner < my_owner;
}

}  // namespace rangelock
}  // namespace dory
