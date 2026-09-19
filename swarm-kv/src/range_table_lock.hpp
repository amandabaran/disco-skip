#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <ctime>
#include <stdexcept>
#include <string>
#include <vector>

#include <dory/conn/rc-exchanger.hpp>

#include "range_lock_rules.hpp"

namespace dory {

/// (Templated on Layout ONLY so this file can be byte-identical in swarm-kv and
/// fusee, which are separate conan packages with separate Layout types and no
/// shared include path. `diff swarm-kv/src/range_table_lock.hpp
/// fusee/src/range_table_lock.hpp` must be empty, for the same reason it must
/// be empty for range_lock.hpp: a lock that differs between two competitor
/// arms makes their numbers incomparable.)
///
/// A PRECISE range lock, as the alternative to range_lock.hpp's striped one.
///
/// ── WHAT IT IS ────────────────────────────────────────────────────────────
///
/// A table with one slot per client. A scan publishes the interval it is about
/// to read, [lo, hi]; a writer publishes the single key it is about to write,
/// as [k, k]. Readers publish nothing and never wait -- a point read is atomic
/// on its own, and making it take the lock would serialise the read-only
/// workloads against nothing and overstate the cost rather than measure it.
///
/// The win over striping is precision. A striped lock rounds a 100-key scan up
/// to whole 1024-key stripes, so it blocks writers to as many as 2048 keys that
/// the scan never touches. This blocks exactly the keys the scan reads, so the
/// false-conflict rate goes to zero and what is left is the true conflict rate.
///
/// ── WHY PUBLISH-THEN-VALIDATE, AND NOT CHECK-THEN-PUBLISH ─────────────────
///
/// The obvious protocol -- read the table, see no conflict, write my entry --
/// is two round trips and is WRONG, because both sides can pass the check:
///
///     t0  scan S reads the table            -> empty
///     t1  writer W reads the table          -> empty
///     t2  S publishes [3,103], starts reading
///     t3  W publishes [50,50], writes key 50
///
/// S read keys 3..49 before t3 and 51..103 after it, so its result mixes the
/// state before W's write with the state after, and that is precisely the
/// non-linearizable outcome the lock exists to prevent. No ordering of those
/// two round trips fixes it; the check and the publish are not atomic.
///
/// So: PUBLISH FIRST, then read the table and look for conflicts. Each client
/// owns a fixed slot, so publishing is a single RDMA write that cannot
/// contend, and the ordering argument is:
///
///   for A to miss B, B must have published AFTER A's read; but then B's read
///   comes after A's publish, so B sees A.
///
/// At least one of any conflicting pair sees the other. A total order on the
/// entries then decides which proceeds, and the loser withdraws and retries.
///
/// ── WHY SCANS DO NOT CONFLICT WITH SCANS ──────────────────────────────────
///
/// Two scans only read, and reads commute. Making them conflict would cost
/// concurrency for nothing, and on a scan-heavy workload that is most of the
/// operations. Only scan-vs-write conflicts. Write-vs-write is left to the
/// store's own per-key atomicity, which it already has.
///
/// ── WHY WRITERS PUBLISH TOO ───────────────────────────────────────────────
///
/// If only scans published, a write already in flight when the scan starts is
/// invisible to it:
///
///     t0  W checks (nothing), begins writing key 50
///     t1  S publishes [3,103], begins scanning
///     t2  W's write lands on key 50
///
/// S straddles the write again. So a scan has to be able to see in-flight
/// writers and wait for them to drain, which means writers publish before
/// writing. That is the half of the protocol that is easy to leave out.
///
/// ── THE TIE-BREAK IS A TIMESTAMP, NOT THE CLIENT ID ───────────────────────
///
/// Ordering conflicts by owner id is simpler and correct, but SYSTEMATICALLY
/// UNFAIR: client 1 would never wait and client 64 would always lose. That is
/// not merely unkind. This harness sums per-client throughputs, and it already
/// documents what happens when clients finish at widely different rates -- the
/// measurement windows stop overlapping, each client measures a system
/// carrying only part of the load, and the sum overstates a total the system
/// never delivered. An unfair lock walks straight into that.
///
/// So entries carry a publish timestamp and the earliest publisher wins, with
/// the owner id breaking exact ties. CLOCK_REALTIME on these nodes is
/// disciplined by PTP to well under a microsecond against ~2 us operations
/// (experiments/compare/setup-ptp.sh), so the comparison is meaningful. If PTP
/// is not running the clocks spread by ~100 us and this degenerates toward
/// id-order -- still CORRECT, because the tie-break only has to be a total
/// order, but no longer fair. Correctness does not depend on the clock;
/// fairness does.
///
/// ── WHAT IT STILL IS NOT ──────────────────────────────────────────────────
///
/// No lease and no revocation: a client that dies holding an entry wedges
/// every writer to that range, exactly as the striped lock does. Acceptable
/// for a benchmark whose clients do not fail, and another thing the locked arm
/// gets for free that a real deployment would have to pay for.
///
/// And as with the striped lock, a scan here still walks the KEY SPACE rather
/// than returning the next N live keys, so it is an atomic snapshot of N
/// key-space slots and not an ordered range query. The lock closes the
/// atomicity gap, not the semantic one.
template <class Layout>
class RangeTableLock {
 public:
  /// A slot is one cacheline so two clients' entries never share one, which
  /// would put their publishes in the same coherence line and the same NIC
  /// atomic unit.
  static constexpr uint64_t kSlotBytes = 64;
  /// state, lo, hi, stamp -- the four words a slot actually carries.
  static constexpr uint64_t kSlotWords = 8;

  static constexpr uint64_t kFree = rangelock::kFree;
  static constexpr uint64_t kScan = rangelock::kScan;
  static constexpr uint64_t kWrite = rangelock::kWrite;

  RangeTableLock(Layout const& layout, conn::ReliableConnection& rc,
                 uint64_t owner_id, uint64_t* scratch, uint64_t slots)
      : layout_{layout},
        rc_{rc},
        owner_{owner_id},
        scratch_{scratch},
        slots_{slots} {
    if (slots_ == 0) {
      throw std::runtime_error("RangeTableLock: slots must be >= 1");
    }
    if (owner_ == 0) {
      // 0 is the free marker in `state`, so it can never be an owner. Clients
      // are numbered from 1; an off-by-one here would make a held slot read as
      // free, which is the one bug this class exists not to have.
      throw std::runtime_error("RangeTableLock: owner id 0 is the free marker");
    }
    // SLOTS ARE INDEXED FROM THE FIRST CLIENT, NOT FROM 1. proc ids are
    // global: servers take 1..num_servers and clients follow, so with 3
    // servers client 1 is proc 4. Subtracting only 1 shifted every slot up by
    // num_servers, left slots 0..num_servers-1 permanently unused, and pushed
    // the LAST num_servers clients off the end of the table -- which is
    // exactly how this first failed on the cluster: clients 14, 15 and 16 of
    // 16 died with "Lock stripe out of range: 16/17/18" while 1..13 ran, and
    // the survivors then hung forever at the barrier waiting for the dead.
    slot_ = ownerSlot(owner_, layout_.num_servers);
    if (slot_ >= slots_) {
      throw std::runtime_error(
          "RangeTableLock: owner " + std::to_string(owner_) + " maps to slot " +
          std::to_string(slot_) + " but the table has only " +
          std::to_string(slots_) +
          " slots. The table needs one slot per client; check that "
          "--lock-mode 1 sized it from num_clients.");
    }
    // The table lands in REGISTERED memory immediately after the staging
    // cacheline. A std::vector here is ordinary heap, and an RDMA read into
    // it fails the work completion -- see Layout::lockScratchSize.
    table_ = scratch_ + kSlotWords;
  }

  /// Lock [start_key, start_key + count - 1] for a scan.
  void acquireRange(uint64_t start_key, uint64_t count) {
    uint64_t const hi = count == 0 ? start_key : start_key + count - 1;
    acquire(kScan, start_key, hi);
  }

  /// Lock the single key a write is about to touch.
  void acquireKey(uint64_t key) { acquire(kWrite, key, key); }

  void release() {
    if (!held_) return;
    publish(kFree, 0, 0, 0);
    held_ = false;
  }

  uint64_t acquires() const { return acquires_; }
  uint64_t retries() const { return retries_; }
  /// Round trips spent validating, which is the cost this lock adds beyond the
  /// striped one and the number to quote when it is slower.
  uint64_t validations() const { return validations_; }

  /// The protocol's two rules live in range_lock_rules.hpp, with no
  /// dependencies, so they can be unit-tested without dragging in dory.
  using Rules = rangelock::State;
  static bool conflicts(uint64_t a_state, uint64_t a_lo, uint64_t a_hi,
                        uint64_t b_state, uint64_t b_lo, uint64_t b_hi) {
    return rangelock::conflicts(a_state, a_lo, a_hi, b_state, b_lo, b_hi);
  }
  static bool losesTo(uint64_t my_stamp, uint64_t my_owner,
                      uint64_t their_stamp, uint64_t their_owner) {
    return rangelock::losesTo(my_stamp, my_owner, their_stamp, their_owner);
  }

 private:
  static uint64_t ownerSlot(uint64_t owner, uint64_t num_servers) {
    return owner - (num_servers + 1);
  }

  /// Monotonic-ish wall clock, in nanoseconds, for the fairness tie-break.
  static uint64_t stampNow() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1000000000ull +
           static_cast<uint64_t>(ts.tv_nsec);
  }

  void acquire(uint64_t state, uint64_t lo, uint64_t hi) {
    if (held_) throw std::runtime_error("RangeTableLock: already held");
    uint64_t const stamp = stampNow();
    for (uint64_t attempt = 0;; ++attempt) {
      publish(state, lo, hi, stamp);
      if (validate(state, lo, hi, stamp)) {
        ++acquires_;
        held_ = true;
        return;
      }
      // Lost: withdraw so the winner is not blocked by an entry we are not
      // going to honour, then back off. Withdrawing matters -- leaving it
      // published would make every later arrival conflict with a client that
      // is itself waiting.
      publish(kFree, 0, 0, 0);
      ++retries_;
      for (uint64_t i = 0; i < (attempt < 10 ? (1ull << attempt) : 1024); ++i) {
        __builtin_ia32_pause();
      }
    }
  }

  void publish(uint64_t state, uint64_t lo, uint64_t hi, uint64_t stamp) {
    scratch_[0] = state;
    scratch_[1] = lo;
    scratch_[2] = hi;
    scratch_[3] = stamp;
    uintptr_t const addr = layout_.getLockAddress(rc_.remoteBuf(), slot_);
    postBlocking(conn::ReliableConnection::RdmaWrite, scratch_,
                 4 * sizeof(uint64_t), addr);
  }

  /// Read the whole table and decide whether we won. True means we hold it.
  bool validate(uint64_t state, uint64_t lo, uint64_t hi, uint64_t stamp) {
    ++validations_;
    uintptr_t const base = layout_.getLockAddress(rc_.remoteBuf(), 0);
    postBlocking(conn::ReliableConnection::RdmaRead, table_,
                 static_cast<uint32_t>(slots_ * kSlotBytes), base);
    for (uint64_t s = 0; s < slots_; ++s) {
      if (s == slot_) continue;
      uint64_t const* e = &table_[s * kSlotWords];
      if (!conflicts(state, lo, hi, e[0], e[1], e[2])) continue;
      // Earliest publisher wins; owner id breaks an exact tie. Both sides
      // apply the same rule to the same two entries, so exactly one of them
      // concludes that it won.
      if (losesTo(stamp, owner_, e[3], s + 1)) return false;
    }
    return true;
  }

  /// Post one operation and drain it. Safe ONLY where no other RDMA is in
  /// flight on this connection: the send CQ is shared with the future
  /// machinery and this drains it blindly. Callers take the lock at operation
  /// boundaries, after finishAllFutures(), for exactly that reason -- the same
  /// contract range_lock.hpp's casBlocking has.
  void postBlocking(conn::ReliableConnection::RdmaReq req, void* buf,
                    uint32_t len, uintptr_t addr) {
    if (!rc_.postSendSingle(req, kLockWrId, buf, len, addr)) {
      throw std::runtime_error("RangeTableLock: failed to post");
    }
    std::vector<struct ibv_wc> wces(1);
    while (true) {
      wces.resize(1);
      if (!rc_.pollCqIsOk(conn::ReliableConnection::SendCq, wces)) {
        throw std::runtime_error("RangeTableLock: error polling cq");
      }
      if (wces.empty()) continue;
      if (wces[0].status != IBV_WC_SUCCESS) {
        throw std::runtime_error(
            std::string("RangeTableLock: work completion failed: ") +
            ibv_wc_status_str(wces[0].status) + " (status " +
            std::to_string(wces[0].status) + ")");
      }
      if (wces[0].wr_id != kLockWrId) {
        throw std::runtime_error(
            "RangeTableLock: drained a completion belonging to a future -- the "
            "lock was taken while other RDMA was in flight");
      }
      break;
    }
  }

  // Outside the future id range, so a completion belonging to a future is
  // detectable rather than silently eaten.
  static constexpr uint64_t kLockWrId = 1ull << 20;

  Layout const& layout_;
  conn::ReliableConnection& rc_;
  uint64_t owner_;
  uint64_t* scratch_;
  uint64_t slots_;
  uint64_t slot_ = 0;
  bool held_ = false;
  uint64_t *table_ = nullptr;   ///< registered, inside the lock scratch
  uint64_t acquires_ = 0;
  uint64_t retries_ = 0;
  uint64_t validations_ = 0;
};

}  // namespace dory
