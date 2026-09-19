#pragma once

#include <algorithm>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

#include <dory/conn/rc-exchanger.hpp>


namespace dory {

/// (Templated on Layout ONLY so this file can be byte-identical in swarm-kv and
/// fusee, which are separate conan packages with separate Layout types and no
/// shared include path. Two hand-maintained copies of a lock would drift, and a
/// lock that differs between two competitor arms makes their numbers
/// incomparable. `diff swarm-kv/src/range_lock.hpp fusee/src/range_lock.hpp`
/// must be empty.)
///
/// An RDMA lock that makes a SCAN linearizable in a system with no snapshot.
///
/// ── WHY THIS EXISTS ───────────────────────────────────────────────────────
///
/// swarm-kv's SCAN (main.cpp, OpType::SCAN) is N INDEPENDENT POINT READS over
/// lexicographically successive keys. Nothing makes them mutually atomic, so an
/// update landing between two of the reads is visible in the result and the
/// scan can return a state that never existed at any instant. It is a loop of
/// reads, not a range query.
///
/// So a comparison of DiSCO-Skip's snapshot range against swarm-kv's scan is
/// not a comparison of two answers to one question -- it is a linearizable
/// operation against a non-linearizable one, and the faster side is faster
/// partly by doing less. This adds the cheapest thing that closes that gap, a
/// lock, so that the competitor answers the same question before its cost is
/// quoted.
///
/// ── WHY WRITERS TAKE IT TOO ───────────────────────────────────────────────
///
/// A scan that locks while writers do not is not linearizable: the writer
/// simply ignores the lock and mutates mid-scan. So the UPDATE path acquires it
/// as well, and on a write-heavy workload that is where most of the cost lands.
/// This is inherent to the approach, not a pessimisation chosen here.
///
/// ── WHAT IT STILL IS NOT ──────────────────────────────────────────────────
///
/// Even locked, this is NOT the same operation as an ordered range scan. It
/// walks the KEY SPACE, reading successive key values whether or not they
/// exist, rather than returning the next N keys that do. An atomic snapshot of
/// N key-space slots and an ordered scan of N live keys are different queries
/// on a hash-based store, and any figure putting them on one axis has to say
/// so.
///
/// ── CRASH BEHAVIOUR ───────────────────────────────────────────────────────
///
/// A client that dies holding the lock wedges every other client: there is no
/// lease and no revocation. That is acceptable for a benchmark whose clients do
/// not fail and unacceptable for anything else, and it is another thing the
/// locked arm gets for free that a real deployment would have to pay for.
template <class Layout>
class RangeLock {
 public:
  /// Keys per stripe. Chosen so a 100k-key workload spreads across the stripe
  /// count rather than collapsing onto one.
  static constexpr uint64_t kKeysPerStripe = 1024;

  /// `stripes` is the NUMBER OF STRIPES and must be >= 1. One stripe is the
  /// global lock, which is then not a special case anywhere below -- stripeOf
  /// simply always returns 0. An earlier version made 0 mean "global" while
  /// Layout::lock_stripes used 0 to mean "disabled"; two meanings for one value
  /// across an interface is how a disabled arm reports itself as locked.
  RangeLock(Layout const& layout, conn::ReliableConnection& rc,
            uint64_t owner_id, uint64_t* scratch, uint64_t stripes)
      : layout_{layout},
        rc_{rc},
        owner_{owner_id},
        scratch_{scratch},
        stripes_{stripes} {
    if (stripes_ == 0) {
      throw std::runtime_error("RangeLock: stripes must be >= 1");
    }
    if (owner_ == 0) {
      // 0 is the free marker, so it can never be an owner id. Clients are
      // numbered from firstClientId() (>= 1), but an off-by-one here would
      // produce a lock that looks free while held, which is exactly the bug
      // this class exists to not have.
      throw std::runtime_error("RangeLock: owner id 0 is the free marker");
    }
  }

  /// Lock everything a scan of [start, start + count) touches.
  ///
  /// Stripes are acquired in ASCENDING order, which is what makes this
  /// deadlock-free: every acquirer orders its requests the same way, so a cycle
  /// cannot form. A scan's stripes are contiguous and ascending already.
  /// The DISTINCT stripes a scan of [start_key, start_key + count) touches, in
  /// ascending stripe order. Pure and static so it can be tested without a NIC.
  ///
  /// Not simply `for (s = stripeOf(start); s <= stripeOf(end); ++s)`. Stripe
  /// ids are taken modulo the stripe count, so a range that wraps past the last
  /// stripe yields lo > hi and that loop yields NOTHING -- a scan that silently
  /// holds no lock at all, which is worse than not having the arm.
  static std::vector<uint64_t> stripesFor(uint64_t start_key, uint64_t count,
                                          uint64_t stripes) {
    uint64_t const first = start_key / kKeysPerStripe;
    uint64_t const last = (start_key + count) / kKeysPerStripe;
    uint64_t const span = last - first + 1;
    std::vector<uint64_t> want;
    if (span >= stripes) {
      // The range covers every stripe; listing them directly also avoids
      // building a span-sized vector for a huge count.
      want.resize(stripes);
      for (uint64_t i = 0; i < stripes; ++i) want[i] = i;
      return want;
    }
    want.reserve(span);
    for (uint64_t r = first; r <= last; ++r) want.push_back(r % stripes);
    std::sort(want.begin(), want.end());
    want.erase(std::unique(want.begin(), want.end()), want.end());
    return want;
  }

  void acquireRange(uint64_t start_key, uint64_t count) {
    held_.clear();
    for (uint64_t s : stripesFor(start_key, count, stripes_)) acquireOne(s);
  }

  /// Lock the single stripe a point write falls in.
  void acquireKey(uint64_t key) {
    held_.clear();
    acquireOne(stripeOf(key));
  }

  /// Release in DESCENDING order, the reverse of acquisition. Not required for
  /// correctness -- releases cannot deadlock -- but it keeps the held_ stack
  /// discipline obvious and makes a leaked stripe visible as a mismatch.
  void release() {
    while (!held_.empty()) {
      releaseOne(held_.back());
      held_.pop_back();
    }
  }

  uint64_t acquires() const { return acquires_; }
  uint64_t retries() const { return retries_; }

 private:
  uint64_t stripeOf(uint64_t key) const {
    return (key / kKeysPerStripe) % stripes_;
  }

  void acquireOne(uint64_t stripe) {
    uintptr_t const addr = layout_.getLockAddress(rc_.remoteBuf(), stripe);
    for (uint64_t attempt = 0;; ++attempt) {
      if (casBlocking(addr, 0, owner_)) {
        ++acquires_;
        held_.push_back(stripe);
        return;
      }
      ++retries_;
      // Bounded spin with a widening pause. No fairness and no queueing: a
      // contended global lock here degenerates to a scramble, which is part of
      // what the measurement is showing.
      for (uint64_t i = 0; i < (attempt < 10 ? (1ull << attempt) : 1024); ++i) {
        __builtin_ia32_pause();
      }
    }
  }

  void releaseOne(uint64_t stripe) {
    uintptr_t const addr = layout_.getLockAddress(rc_.remoteBuf(), stripe);
    // CAS rather than a plain write, so releasing a lock this client does not
    // own fails loudly instead of stomping another client's ownership.
    if (!casBlocking(addr, owner_, 0)) {
      throw std::runtime_error("RangeLock: released a lock we did not hold");
    }
  }

  /// Post one CAS and drain it. Safe ONLY at a point where no other RDMA is in
  /// flight on this connection, because the send CQ is shared with the future
  /// machinery and this drains it blindly. Callers take the lock at operation
  /// boundaries, after finishAllFutures(), for exactly that reason.
  bool casBlocking(uintptr_t addr, uint64_t expected, uint64_t desired) {
    *scratch_ = 0;
    if (!rc_.postSendSingleCas(kLockWrId, scratch_, addr, expected, desired)) {
      throw std::runtime_error("RangeLock: failed to post CAS");
    }
    std::vector<struct ibv_wc> wces(1);
    while (true) {
      wces.resize(1);
      if (!rc_.pollCqIsOk(conn::ReliableConnection::SendCq, wces)) {
        throw std::runtime_error("RangeLock: error polling cq");
      }
      if (wces.empty()) continue;
      if (wces[0].status != IBV_WC_SUCCESS) {
        throw std::runtime_error(
            std::string("RangeLock: work completion failed: ") +
            ibv_wc_status_str(wces[0].status) + " (status " +
            std::to_string(wces[0].status) + ")");
      }
      if (wces[0].wr_id != kLockWrId) {
        throw std::runtime_error(
            "RangeLock: drained a completion belonging to a future -- the lock "
            "was taken while other RDMA was in flight");
      }
      break;
    }
    return *scratch_ == expected;   // CAS returns the PRE-image
  }

  // Outside the future id range (futures use 0..async_parallelism-1), so a
  // completion belonging to a future is detectable rather than silently eaten.
  static constexpr uint64_t kLockWrId = 1ull << 20;

  Layout const& layout_;
  conn::ReliableConnection& rc_;
  uint64_t owner_;
  uint64_t* scratch_;
  uint64_t stripes_;
  std::vector<uint64_t> held_;
  uint64_t acquires_ = 0;
  uint64_t retries_ = 0;
};

}  // namespace dory
