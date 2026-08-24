#include <array>
#include <atomic>
#include <cassert>
#include <unordered_set>

#include "hp_deletable.h"

/// A thread hazard pointer context. This specific context type assumes we
/// are doing hand-over-hand traversals, and thus only ever need two
/// hazard pointers. The two hazard pointers are called "curr" and "next."
///
/// hp_thread_context tracks two things.  First, it has a small number (2)
/// of hazard pointers: shared single-writer, multi-reader locations where a
/// thread can store the locations it is protecting.  Second, it has a large
/// segment of memory that stores retired pointers that are not yet reclaimed
/// (presumably because of a product of laziness and the hazard pointers held by
/// other threads).
///
/// @param ZOMBIE_CAP The capacity of the zombie array.
template <size_t ZOMBIE_CAP> class hp_thread_context {

  static constexpr size_t MAX_HP_COUNT = 2;

  // Returns the index of the slot that is not curr_idx.
  // If curr_idx is 0, returns 1. If curr_idx is 1, returns 0.
  // Unless there is a bug in this class,
  // curr_idx should never take any other value.
  int8_t next_idx() {
    assert(curr_idx == 0 || curr_idx == 1);
    return 1 - curr_idx;
  }

  /// This variable indicates which of the two hazard pointer slots contains
  /// "curr." This makes the other "next," naturally.
  int8_t curr_idx = 0;

public:
  /// The current size of the zombies array.
  size_t count = 0;

  /// The set of objects to be reclaimed.
  std::array<hp_deletable *, ZOMBIE_CAP> zombies = {nullptr};

  /// The hazard pointers held by this thread.
  /// It is hard-coded at two because that meets the needs of this use case.
  std::array<std::atomic<hp_deletable *>, MAX_HP_COUNT> hazP;

  /// The threads' HP contexts form a linked list. The order in the list is
  /// not important, but we need a next pointer to create the list.
  hp_thread_context *next = nullptr;

  /// Create a thread's hazard pointer context.
  hp_thread_context() {
    hazP[0].store(nullptr, std::memory_order_relaxed);
    hazP[1].store(nullptr, std::memory_order_relaxed);
  }

  /// When destroying the thread hazard pointer context, we assume that the
  /// program has shifted to a new phase (possibly sequential), and thus
  /// all zombies can now safely be reclaimed, so we do just that.
  ~hp_thread_context() {
    assert(hazP[0].load(std::memory_order_relaxed) == nullptr);
    assert(hazP[1].load(std::memory_order_relaxed) == nullptr);
    for (; count > 0; --count) {
      hp_deletable *zombie = zombies.at(count - 1);
      delete zombie;
    }
  }

  /// Protect a location by taking a hazard pointer on it.
  /// This method has to check if any hazard pointers are currently held,
  /// so it is slower than either take_first() or take_next().
  /// Avoid using it unless you have to.
  void take(hp_deletable *p) {
    if (hazP[0].load(std::memory_order_relaxed) == nullptr &&
        hazP[1].load(std::memory_order_relaxed) == nullptr)
      take_first(p);
    else
      take_next(p);
  }

  /// Protect a location by taking a hazard pointer on it.
  /// Assumes no hazard pointers are held by the thread yet.
  void take_first(hp_deletable *p) {
    assert(p != nullptr);
    assert(hazP[0].load(std::memory_order_relaxed) == nullptr);
    assert(hazP[1].load(std::memory_order_relaxed) == nullptr);
    hazP.at(curr_idx).store(p, std::memory_order_relaxed);
  }

  /// Protect a location by taking a hazard pointer on it. Assumes there is
  /// exactly one other hazard pointer currently held. If none are held, use
  /// take_first() instead. If two are held, no more can be taken.
  void take_next(hp_deletable *p) {
    assert(p != nullptr);
    int8_t const next_index = next_idx();
    assert(hazP.at(curr_idx).load(std::memory_order_relaxed) != nullptr);
    assert(hazP.at(next_index).load(std::memory_order_relaxed) == nullptr);
    hazP.at(next_index).store(p, std::memory_order_relaxed);
  }

  /// Count how many hazard pointers are reserved by this thread.
  int8_t count_reserved() {
    int8_t result = 0;

    for (auto &i : hazP)
      if (i.load(std::memory_order_relaxed) != nullptr)
        ++result;

    return result;
  }

  /// If two hazard pointers have been taken, drops "next."
  /// If only one has been taken, drops "curr."
  /// This method has to check how many hazard pointers are currently held,
  /// so it is slower than either drop_curr() or drop_next().
  /// Avoid using it unless you have to.
  void drop() {
    if (hazP.at(next_idx()).load(std::memory_order_relaxed) != nullptr)
      drop_next();
    else
      drop_curr();
  }

  /// Drop the current hazard pointer. If two hazard pointers are held, this
  /// causes the next hazard pointer to become the current hazard pointer.
  void drop_curr() {
    assert(hazP.at(curr_idx).load(std::memory_order_relaxed) != nullptr);
    hazP.at(curr_idx).store(nullptr, std::memory_order_relaxed);

    // Switch curr_slot to the other slot. If there is a node in the other slot,
    // this is necessary; if there isn't, this is harmless.
    // It's cheaper than an if statement, so just do it.
    curr_idx = next_idx();
  }

  /// Drop the hazard pointer on next.
  /// This is only valid when two hazard pointers are held.
  void drop_next() {
    int8_t const next_index = next_idx();
    assert(hazP.at(curr_idx).load(std::memory_order_relaxed) != nullptr);
    assert(hazP.at(next_index).load(std::memory_order_relaxed) != nullptr);
    hazP.at(next_index).store(nullptr, std::memory_order_relaxed);
  }

  /// Drop all hazard pointers held by this thread.
  void drop_all() {
    assert(hazP[0].load(std::memory_order_relaxed) != nullptr);
    assert(hazP[1].load(std::memory_order_relaxed) != nullptr);
    hazP[0].store(nullptr, std::memory_order_relaxed);
    hazP[1].store(nullptr, std::memory_order_relaxed);
  }

  /// Reclaim all zombies that aren't in use.
  /// The provided unordered_set indicates which ones are in use.
  void sweep(const std::unordered_set<hp_deletable *> &in_use) {
    auto const end = in_use.end();
    size_t i = 0;
    while (i < count) {
      hp_deletable *zombie = zombies.at(i);
      assert(zombie != nullptr);
      auto it = in_use.find(zombie);

      if (it != end) {
        // Someone else has a hazard pointer on this zombie, so don't delete it
        // yet. Just move onto the next item.
        ++i;
        continue;
      }

      // No one else has a hazard pointer on this zombie, so delete it, and
      // move the last element into this empty slot to maintain compactness.
      delete zombie;
      --count;
      hp_deletable *last_zombie = zombies.at(count);
      assert(last_zombie != nullptr);
      zombies.at(i) = last_zombie;
      zombies.at(count) = nullptr;
    }
  }
};
