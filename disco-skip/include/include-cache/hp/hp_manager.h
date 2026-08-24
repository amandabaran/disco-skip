#pragma once

#include <atomic>
#include <string>
#include <unordered_set>

#include "hp_deletable.h"
#include "hp_thread_context.h"

/// A class that manages the use of hazard pointers.
/// This class sets up each thread's hp_thread_context, and handles interactions
/// between the user and said class.
///
/// @param THREADS The maximum number of threads that will use this class.
/// If more are used, this class will still work, but the tuning may be
/// suboptimal.
template <size_t THREADS> class hp_manager {
  // The cap on the global number of held HPs. Each thread can hold
  // MAX_HP_COUNT, so it's the product of thread count by that number.
  static constexpr size_t MAX_HP_COUNT = THREADS * 2;

  static constexpr size_t SWEEP_PROPORTION = 10;

  // An efficient implementation of ceil(log2(x))
  // with the caveat that it returns an invalid answer for x = 0, 1.
  // That drawback is irrelevant for this use case.
  static constexpr uint32_t log2(const uint32_t x) {
    // __builtin_clz() is a GCC built-in function that counts the number of
    // zeroes on the left side of a 32-bit binary number.
    return 32 - __builtin_clz(x - 1);
  }

  // Calculate the size of the zombies array from the number of threads.
  static constexpr size_t zombies_capacity() {
    // We want to choose a capacity big enough to ensure that any sweep cleans
    // up at least 90% of the array. Since each thread can have two pointers
    // reserved, there can be MAX_HP_COUNT pointers held at any time, and so we
    // need ten times that amount to ensure our 90% target is reached.
    // After that, just round up to the nearest power of two.
    return 2 << log2(MAX_HP_COUNT * SWEEP_PROPORTION);
  }

  static constexpr size_t ZOMBIE_CAP = zombies_capacity();

  // Shorthand for long and common type names.
  using context_t = hp_thread_context<ZOMBIE_CAP>;
  using ptr_set_t = std::unordered_set<hp_deletable *>;

  /// A thread-local pointer to each thread's hazard pointer context.
  static thread_local context_t *thread_ctx;

  // The head of the global linked list of contexts.
  static std::atomic<context_t *> contexts_head;

public:
  /// Create this thread's context object if it does not exist yet.
  static void init_context() {

    if (thread_ctx == nullptr) {
      // Create the context.
      thread_ctx = new context_t();

      // Repeatedly attempt to CAS the current context into the global linked
      // list of contexts until successful.
      context_t *read_head = nullptr;
      do {
        read_head = contexts_head.load();
        thread_ctx->next = read_head;
      } while (!contexts_head.compare_exchange_strong(read_head, thread_ctx));
    }
  }

  /// Count the number of hazard pointers held by this thread.
  static size_t reserved_by_thread() { return thread_ctx->count_reserved(); }

  /// Count the number of hazard pointers held by all threads.
  static size_t count_reserved() {
    size_t reserved_count = 0;
    context_t *curr = contexts_head.load(std::memory_order_relaxed);
    while (curr) {
      reserved_count += curr->count_reserved();
      curr = curr->next;
    }
    return reserved_count;
  }

  /// Mark a node for reclamation by adding it to the zombies array for this
  /// thread.
  static void reclaim(hp_deletable *zombie) {
    size_t index = thread_ctx->count;

    // If index is out of array bounds, sweep and try again.
    if (index >= zombies_capacity()) {
      sweep();
      index = thread_ctx->count;
    }

    // Don't overwrite another pointer awaiting reclamation!
    assert(thread_ctx->zombies.at(index) == nullptr);

    thread_ctx->zombies.at(index) = zombie;
    ++thread_ctx->count;
  }

  /// Populate the provided set with all in-use hazard pointers.
  /// This method does not lock anything, so results may be stale,
  /// but that is fine for the use case.
  static void get_in_use(ptr_set_t &result) {
    context_t *curr = contexts_head.load(std::memory_order_relaxed);

    while (curr != nullptr) {
      for (size_t i = 0; i < 2; ++i) {
        hp_deletable *p = curr->hazP.at(i);
        if (p != nullptr) {
          result.insert(p);
        }
      }
      curr = curr->next;
    }
  }

  /// Go through this thread's zombies, and try to reclaim each pointer.
  static void sweep() {
    ptr_set_t in_use(MAX_HP_COUNT);
    get_in_use(in_use);
    thread_ctx->sweep(in_use);
  }

  /// As part of final cleanup, free all pointers.
  static void sweep_all() {
    // Check that no threads still hold HPs
    ptr_set_t in_use(MAX_HP_COUNT);
    get_in_use(in_use);
    assert(in_use.empty());

    context_t *curr = contexts_head.load(std::memory_order_relaxed);

    while (curr != nullptr) {
      curr->sweep(in_use);
      curr = curr->next;
    }
  }

  // Sequential-only teardown method. Destroys ALL hazard pointer context,
  // for ALL threads, globally!
  // Teardown is IRREVOCABLE: NO class that uses this hazard pointer class will
  // work after this is called, as the thread_local pointers to their own
  // contexts cannot be nulled after the context objects are deleted!
  static void tear_down() {
    context_t *curr = contexts_head.exchange(nullptr);

    ptr_set_t const empty(0);

    while (curr != nullptr) {
      // Destroy each thread's context.
      // Contexts' destructor will reclaim all remaining unlinked nodes.
      context_t *next = curr->next;
      assert(curr->count_reserved() == 0);
      curr->sweep(empty);
      delete curr;
      curr = next;
    }
  }

  // The next several methods are static entry-points to non-static methods for
  // the current thread's context.

  /// Protect a location by taking a hazard pointer on it.
  /// This method has to check if any hazard pointers are currently held,
  /// so it is slower than either take_first() or take_next().
  /// Avoid using it unless you have to.
  static void take(hp_deletable *p) { thread_ctx->take(p); }

  /// Protect a location by taking a hazard pointer on it.  Assumes no hazard
  /// pointers are held by the thread yet.
  static void take_first(hp_deletable *p) { thread_ctx->take_first(p); }

  /// Protect a location by taking a hazard pointer on it.  Assumes there is
  /// another hazard pointer currently held.
  static void take_next(hp_deletable *p) { thread_ctx->take_next(p); }

  /// If two hazard pointers have been taken, drops "next."
  /// If only one has been taken, drops "curr."
  /// This method has to check how many hazard pointers are currently held,
  /// so it is slower than either drop_curr() or drop_next().
  /// Avoid using it unless you have to.
  static void drop() { thread_ctx->drop(); }

  /// Drop the "oldest" hazard pointer that we currently hold
  static void drop_curr() { thread_ctx->drop_curr(); }

  /// Drop the hazard pointer on next.
  static void drop_next() { thread_ctx->drop_next(); }

  /// Drop all hazard pointers held by this thread.
  static void drop_all() { thread_ctx->drop_all(); }

  static std::string_view get_name() { return "Hazard Pointers"; }
};

// Define the static fields of the class that was declared above
//
// NB: we take care to ensure that our benchmarks never use this file from two
// separate translation units, and hence it is safe to declare and define
// these static and thread-local variables within a header file

template <size_t THREADS>
thread_local hp_thread_context<hp_manager<THREADS>::ZOMBIE_CAP>
    *hp_manager<THREADS>::thread_ctx = nullptr;

template <size_t THREADS>
std::atomic<hp_thread_context<hp_manager<THREADS>::ZOMBIE_CAP> *>
    hp_manager<THREADS>::contexts_head(nullptr);
