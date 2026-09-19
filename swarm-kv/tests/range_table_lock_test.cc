// The two rules that decide correctness, tested without a NIC.
//
// conflicts() and losesTo() are the whole protocol: everything else is RDMA
// plumbing. Both are pure statics precisely so they can be checked here, and
// the failure they guard against is silent -- a lock that lets both sides
// proceed produces a plausible throughput number and a torn scan, which no
// benchmark output would reveal.
//
//   make -C swarm-kv/tests && ./swarm-kv/tests/obj/range_table_lock_test
#include <cstdint>
#include <cstdio>

// Only the rules, which have no dependencies. Including the lock itself
// pulls dory/conn -> ctrl -> memory -> spdlog -> fmt and the test then fails
// on a missing SPDLOG_ACTIVE_LEVEL or an unresolved fmt symbol -- a broken
// toolchain, apparently, rather than a broken rule.
#include "../src/range_lock_rules.hpp"

namespace Lock = dory::rangelock;
static int failures = 0;

static void check(bool ok, char const* what) {
  if (!ok) { std::printf("FAIL: %s\n", what); ++failures; }
}

int main() {
  constexpr uint64_t SCAN = Lock::kScan, WRITE = Lock::kWrite, FREE = Lock::kFree;

  // ── conflicts ───────────────────────────────────────────────────────────
  check(!Lock::conflicts(SCAN, 3, 103, FREE, 0, 0), "free never conflicts");
  check(!Lock::conflicts(FREE, 0, 0, WRITE, 50, 50), "free never conflicts (rhs)");

  // THE CASE THE DESIGN TURNS ON: two scans must NOT conflict. Reads commute,
  // and on a scan-heavy workload making them conflict would serialise most of
  // the operations against each other for no correctness gain.
  check(!Lock::conflicts(SCAN, 3, 103, SCAN, 50, 150), "scan/scan never conflicts");
  check(!Lock::conflicts(SCAN, 3, 103, SCAN, 3, 103), "identical scans do not conflict");

  // Write/write is the store's own per-key business, not this lock's.
  check(!Lock::conflicts(WRITE, 50, 50, WRITE, 50, 50), "write/write not ours");

  // Scan vs write: overlap is the whole question.
  check(Lock::conflicts(SCAN, 3, 103, WRITE, 50, 50), "write inside the scan");
  check(Lock::conflicts(SCAN, 3, 103, WRITE, 3, 3), "write on the low edge");
  check(Lock::conflicts(SCAN, 3, 103, WRITE, 103, 103), "write on the high edge");
  check(!Lock::conflicts(SCAN, 3, 103, WRITE, 2, 2), "just below the range");
  check(!Lock::conflicts(SCAN, 3, 103, WRITE, 104, 104), "just above the range");
  check(Lock::conflicts(WRITE, 50, 50, SCAN, 3, 103), "and it is symmetric");

  // A single-key scan is still a scan.
  check(Lock::conflicts(SCAN, 7, 7, WRITE, 7, 7), "degenerate 1-key scan");

  // ── losesTo: a TOTAL order, so exactly one of any pair loses ────────────
  check(Lock::losesTo(200, 5, 100, 9), "later stamp loses to earlier");
  check(!Lock::losesTo(100, 9, 200, 5), "earlier stamp wins");
  check(Lock::losesTo(100, 9, 100, 5), "equal stamps: higher id loses");
  check(!Lock::losesTo(100, 5, 100, 9), "equal stamps: lower id wins");
  check(!Lock::losesTo(100, 5, 100, 5), "an entry never loses to itself");

  // EXACTLY ONE LOSES, over a grid. If both sides could lose the pair
  // deadlocks; if neither could, mutual exclusion is broken and the scan
  // tears. This is the property, checked rather than argued.
  for (uint64_t sa = 98; sa <= 102; ++sa) {
    for (uint64_t sb = 98; sb <= 102; ++sb) {
      for (uint64_t oa = 1; oa <= 3; ++oa) {
        for (uint64_t ob = 1; ob <= 3; ++ob) {
          if (oa == ob) continue;           // one slot per client
          bool const a = Lock::losesTo(sa, oa, sb, ob);
          bool const b = Lock::losesTo(sb, ob, sa, oa);
          check(a != b, "exactly one of any conflicting pair loses");
        }
      }
    }
  }

  // FAIRNESS, which is why the stamp leads. Under id-ordering client 1 would
  // never wait; here an earlier publisher beats a lower id.
  check(Lock::losesTo(500, 1, 400, 64), "a late client 1 loses to an early 64");

  if (failures) { std::printf("%d FAILURE(S)\n", failures); return 1; }
  std::printf("ALL PASS\n");
  return 0;
}
