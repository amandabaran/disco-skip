#pragma once

#include <atomic>
#include <cassert>
#include <stdexcept>

#include "loop_counter.h"

// [bll] Will be easier if we separate dead bit and orphan bit from the seqlock,
//       and make them fields of some sort.

/// An implementation of a sequence lock to meet the specific needs of the
/// concurrent skipvector. Steals two flag bits to represents the states a
/// skipvector node can be in.
class sv_lock {
  std::atomic<uint64_t> lock;

  // A bit mask representing the orphan bit.
  // Indicates if this node has a parent in the layer above.
  static constexpr uint64_t ORPHAN_BIT = 0x0000000000000001ULL;

  // A bit mask indicating if this lock is frozen. While a lock is frozen,
  // readers may continue to access it, but threads other than the freezer may
  // neither freeze nor acquire it. This state is intended to reduce lock
  // contention.
  static constexpr uint64_t FREEZE_BIT = 0x0000000000000002ULL;

  // Represents the lock bit,
  // as well as the amount the lock is incremented by on acquire and release.
  // If the lock and freeze bits are both set, that represents a node in the
  // "dead" state.
  static constexpr uint64_t LOCK_BIT = 0x0000000000000004ULL;

  // The bits which indicate the state of the lock.
  // The orphan bit is excluded as it represents the state of the node, not the
  // state of the lock.
  static constexpr uint64_t STATE_MASK = FREEZE_BIT + LOCK_BIT;

  // If both bits are set, the node is in the dead state.
  // The same as the state mask, but two names are used for clarity.
  static constexpr uint64_t DEAD_MASK = STATE_MASK;

public:
  /// Static helper method. Determine if a given lock value's orphan bit is set.
  static bool is_orphan(uint64_t v) { return (v & ORPHAN_BIT) == ORPHAN_BIT; }

  /// Static helper method.
  /// Determine if a given value represents an unlocked node.
  static bool is_unlocked(uint64_t v) { return (v & STATE_MASK) == 0; }

  /// Static helper method.
  /// Determine if a given value represents a frozen node.
  static bool is_frozen(uint64_t v) { return (v & STATE_MASK) == FREEZE_BIT; }

  /// Static helper method.
  /// Determine if a given value represents a locked node.
  static bool is_locked(uint64_t v) { return (v & STATE_MASK) == LOCK_BIT; }

  /// Static helper method.
  /// Determine if a given value represents an dead node.
  static bool is_dead(uint64_t v) { return (v & STATE_MASK) == DEAD_MASK; }

  /// Default constructor; begin unlocked and with all flag bits clear.
  sv_lock() : lock(0) {}

  /// Default constructor; begin in the unlocked state.
  /// Set the orphan bit according to the provided bool argument.
  explicit sv_lock(bool orphan) : lock(orphan ? ORPHAN_BIT : 0) {}

  ~sv_lock() = default;

  /// Initializes a node that was constructed with the default constructor.
  void initialize(void *, bool _orphan) {
    if (_orphan) {
      // We assume initialize is called before anything other operations,
      // so it's OK that this resets the state of the lock.
      lock.store(ORPHAN_BIT);
    }
  }

  /// Determine if the node owning this lock is an orphan.
  /// May only be called while lock is held.
  [[nodiscard]] bool is_orphan() const { return is_orphan(lock); }
  bool is_unlocked() { return is_unlocked(lock); }
  bool is_frozen() { return is_frozen(lock); }
  [[nodiscard]] bool is_locked() const { return is_locked(lock); }
  [[nodiscard]] bool is_dead() const { return is_dead(lock); }

  /// Acquire the sequence lock as a writer. Busywait until unlocked and
  /// unfrozen, then (atomically) increment the counter to the next locked
  /// value.
  /// Returns false if the lock is dead.
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
        // Node is locked or frozen, so keep waiting.
        read_value = lock.load();
      }
    }
    return true;
  }

  /// Acquire a lock frozen by this thread.
  void acquire_frozen() {
    uint64_t const read_value = lock;

    // Assert that the freeze bit is set and the lock bit is clear.
    assert(is_frozen(read_value));

    // Since the lock was frozen by this thread, it shouldn't be possible for
    // any other thread to modify the lock concurrently.
    // So, just take it by setting the lock bit (and clearing the freeze bit.)
    lock = read_value + (LOCK_BIT - FREEZE_BIT);
  }

  /// Try to upgrade from reader to writer.
  /// Succeeds only if the lock's freeze bit and lock bit are both clear, and
  /// lock's value is unchanged. Otherwise, fails.
  bool try_upgrade(uint64_t v) {
    // We expect that this method is only ever called by threads that think this
    // node is in the unlocked state.
    assert(is_unlocked(v));

    return lock.compare_exchange_strong(v, v + LOCK_BIT);
  }

  /// Release the sequence lock after making changes to the protected data. This
  /// increments the counter to the next unlocked value and clears the freeze
  /// bit. Should only be called by the thread that acquired.
  uint64_t release() {
    assert(is_locked(lock.load()));

    // NB: This addition will have the effect of clearing the lock bit and
    // incrementing the counter. The addition of LOCK_BIT seems to be done
    // twice because fetch_add() returns the old value.
    return lock.fetch_add(LOCK_BIT) + LOCK_BIT;
  }

  /// Release the sequence lock after making changes to the protected data,
  /// and mark this node as an orphan. Orphaning is irreversible.
  /// This increments the counter to the next unlocked value.
  /// Should only be called by the thread that acquired.
  /// Should ALSO only be called if the node was not an orphan before this.
  uint64_t release_as_orphan() {

#ifndef NDEBUG
    uint64_t const val = lock.load();
    assert(is_locked(val));
    assert(!is_orphan(val));
#endif

    // NB: This addition will have the effect of clearing the lock bit and
    // incrementing the counter. The addition of LOCK_BIT and ORPHAN_BIT seems
    // to be done twice because fetch_add() returns the old value.
    return lock.fetch_add(LOCK_BIT + ORPHAN_BIT) + LOCK_BIT + ORPHAN_BIT;
  }

  /// Atomically set the freeze bit.
  /// If lock is already locked or frozen, spin until it is unlocked and
  /// unfrozen.
  bool freeze() {
    uint64_t read_value = lock;
    bool success = false;
    loop_counter ctr;
    while (!success) {
      ctr.count();
      if (is_unlocked(read_value)) {
        // Node is unlocked, so go ahead and try to freeze it.
        uint64_t const new_value = read_value + FREEZE_BIT;
        success = lock.compare_exchange_weak(read_value, new_value);
      } else if (is_dead(read_value)) {
        // Node is dead, so give up and return false.
        return false;
      } else {
        // Otherwise, node is locked or frozen, so keep trying.
        read_value = lock;
      }
    }
    return true;
  }

  /// Clear the freeze bit.
  /// Should only be called by the thread that set it.
  void thaw() {
    assert(is_frozen(lock.load()));
    lock -= FREEZE_BIT;
  }

  /// Try to upgrade from reader to freezer.
  /// Succeeds only if the lock's freeze bit and lock bit are both clear, and
  /// lock's value is unchanged. Otherwise, fails.
  bool try_freeze(uint64_t v) {
    // We expect that this method is only ever called by threads that think this
    // node is in the unlocked state.
    assert(is_unlocked(v));
    return lock.compare_exchange_strong(v, v + FREEZE_BIT);
  }

  /// Release the sequence lock after making no changes to the protected data.
  /// This decrements the counter down to the previous unlocked value.
  /// Should only be called by the thread that acquired.
  void release_unchanged() {
    assert(is_locked(lock.load()));
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
    assert(is_locked(lock.load()));
    lock += FREEZE_BIT;
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
  /// The freeze bit is also cleared as it is irrelevant to a reader.
  /// Work will be wasted if the node is dead, but that is rare (only happens to
  /// the data head in the priority queue implementation.)
  [[nodiscard]] uint64_t begin_read() const { return lock.load() & ~STATE_MASK; }

  /// "Release" the lock as a reader.
  /// Basically just checks if the lock has changed.
  /// Reader must abort and try again if it has.
  [[nodiscard]] bool confirm_read(uint64_t v) const {
    std::atomic_thread_fence(std::memory_order_acquire);
    uint64_t new_v = lock.load(std::memory_order_relaxed);

    // Clear the freeze bit, as it is irrelevant to a reader.
    // We assume the freeze bit on the v was cleared by begin_read().
    // NB: Even if the lock moves to the dead state,
    // the confirm will still fail because the lock bit will be set.
    new_v = new_v & ~FREEZE_BIT;
    return v == new_v;
  }

  /// Conclude a read with no regard for whether it was successful.
  /// In this implementation, this is a no-op.
  void abort_read() {}

  /// Just get the value of the sequence lock directly.
  /// For debug purposes only.
  [[nodiscard]] uint64_t get_value() const { return lock.load(); }

  void dump() const {
    using std::cout;

    uint64_t const read_value = lock.load();

    if (is_orphan(read_value)) {
      cout << "o"; // orphan
    } else {
      cout << "C"; // child
    }

    switch (read_value & STATE_MASK) {
    case DEAD_MASK:
      cout << "D"; // dead
      break;
    case FREEZE_BIT:
      cout << "F"; // Frozen
      break;
    case LOCK_BIT:
      cout << "L"; // locked
      break;
    case 0:
      cout << "U"; // unlocked
      break;
    default:
      cout << "X"; // This shouldn't be possible
      throw std::logic_error("Unexpected state: " + std::to_string(read_value));
    }

    cout << +(read_value / LOCK_BIT);
  }
};
