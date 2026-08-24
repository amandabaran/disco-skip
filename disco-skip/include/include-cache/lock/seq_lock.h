#pragma once

#include <atomic>
#include <cassert>

#include "loop_counter.h"

/// An ordinary sequence lock with no bits stolen to represent special values.
class seq_lock {
  std::atomic<uint64_t> lock;

  // Represents the lock bit,
  // as well as the amount the lock is incremented by on acquire and release.
  static constexpr uint64_t LOCK_BIT = 0x0000000000000001ULL;

  // A special value indicating that this lock, and the object it belongs to,
  // are dead. Attempts to acquire this lock will return false.
  // It is technically possible but unfeasible for the lock to be incremented to
  // this value if it is locked and unlocked many, many times.
  static constexpr uint64_t DEAD_VALUE = 0xFFFFFFFFFFFFFFFFULL;

public:
  /// Static helper method.
  /// Determine if a given value represents an unlocked node.
  static bool is_unlocked(uint64_t v) { return (v & LOCK_BIT) == 0; }

  /// Static helper method.
  /// Determine if a given value represents a locked node.
  static bool is_locked(uint64_t v) { return (v & LOCK_BIT) == LOCK_BIT; }

  /// Static helper method.
  /// Determine if a given value represents an dead node.
  static bool is_dead(uint64_t v) { return v == DEAD_VALUE; }

  /// Constructor; begin unlocked.
  seq_lock() : lock(0) {}
  ~seq_lock() = default;

  /// Determine if the node owning this lock is an orphan.
  /// May only be called while lock is held.
  [[nodiscard]] bool is_unlocked() const { return is_unlocked(lock); }
  [[nodiscard]] bool is_locked() const { return is_locked(lock); }
  [[nodiscard]] bool is_dead() const { return is_dead(lock); }

  /// Acquire the sequence lock as a writer. Busywait until unlocked, then
  /// (atomically) increment the counter to the next locked value.
  bool acquire() {
    uint64_t read_value = lock.load();
    bool success = false;
    loop_counter ctr;
    while (!success) {
      ctr.count();
      if (is_unlocked(read_value)) {
        // Node is unlocked, so try to lock it.
        uint64_t const new_value = read_value + LOCK_BIT;
        success = lock.compare_exchange_weak(read_value, new_value);
      } else if (is_dead(read_value)) {
        // Node is dead, so return false.
        return false;
      } else {
        // Node is locked, so keep waiting.
        read_value = lock.load();
      }
    }
    return true;
  }

  /// Try to upgrade from reader to writer.
  /// Succeeds only if the lock's freeze bit and lock bit are both clear, and
  /// lock's value is unchanged. Otherwise, fails.
  bool try_upgrade(uint64_t v) {
    // We expect that this method is only ever called by threads that think this
    // node is in the unlocked state.
    assert(is_unlocked(v));

    // NB: If this method is called when this lock is dead, this CAS will fail
    // because the dead value has the lock bit set.
    return lock.compare_exchange_strong(v, v + LOCK_BIT);
  }

  /// Release the sequence lock after making changes to the protected data. This
  /// increments the counter to the next unlocked value and clears the freeze
  /// bit. Should only be called by the thread that acquired.
  uint64_t release() {
#ifndef NDEBUG
    // Assertion: nodes must be locked to be unlocked.
    uint64_t const read_val = lock.load();
    assert(read_val != DEAD_VALUE);
    assert(is_locked(read_val));
#endif

    // NB: This addition will have the effect of clearing the lock bit and
    // incrementing the counter. The addition of LOCK_BIT seems to be done
    // twice because fetch_add() returns the old value.
    return lock.fetch_add(LOCK_BIT) + LOCK_BIT;
  }

  /// Release the sequence lock after making no changes to the protected data.
  /// This decrements the counter down to the previous unlocked value.
  /// Should only be called by the thread that acquired.
  void release_unchanged() {
#ifndef NDEBUG
    // Assertion: nodes must be locked to be unlocked.
    uint64_t const read_val = lock.load();
    assert(read_val != DEAD_VALUE);
    assert(is_locked(read_val));
#endif

    lock -= LOCK_BIT;
  }

  /// Releases normally if bool b is true.
  /// Releases unchanged if it is false.
  void release_changed_if(bool b) {
    if (b) {
      release();
    } else {
      release_unchanged();
    }
  }

  /// Puts a lock into the dead state.
  /// Should only be done by a thread that has first acquired the lock.
  void die() {
#ifndef NDEBUG
    // Assertion: nodes must be locked to be killed.
    uint64_t const read_val = lock.load();
    assert(read_val != DEAD_VALUE);
    assert(is_locked(read_val));
#endif

    lock.store(DEAD_VALUE);
  }

  /// "Acquire" the lock as a reader.
  /// Returns the value of the lock as though it were in the unlocked state.
  /// This is for two reasons:
  /// 1) There is a nonzero chance the writer will release_unchanged() and the
  /// read will actually succeed.
  /// 2) This will let it prefetch the memory it will need to work with.
  /// Should only be done when the shared data allows for safe concurrent reads
  /// and writes, and any unsound results from the access can easily be
  /// discarded, reversed, or repaired.
  [[nodiscard]] uint64_t begin_read() const { return lock.load() & ~LOCK_BIT; }

  /// "Release" the lock as a reader.
  /// Basically just checks if the lock has changed.
  /// Reader must abort and try again if it has.
  [[nodiscard]] bool confirm_read(uint64_t v) const {
    // NB: If this method is called when this lock is dead, this CAS will fail
    // because the dead value has the lock bit set.

    std::atomic_thread_fence(std::memory_order_acquire);
    uint64_t const new_v = lock.load(std::memory_order_relaxed);
    return v == new_v;
  }

  /// Just get the value of the sequence lock directly.
  /// For debug purposes only.
  [[nodiscard]] uint64_t get_value() const { return lock.load(); }

  void dump() const {
    using std::cout;

    uint64_t const read_value = lock.load();

    if (read_value == DEAD_VALUE) {
      cout << "DEAD";
      return;
    }

    if ((read_value & LOCK_BIT) == LOCK_BIT) {
      cout << "L"; // locked
    } else {
      cout << "U"; // unlocked
    }

    cout << +(read_value / LOCK_BIT);
  }
};
