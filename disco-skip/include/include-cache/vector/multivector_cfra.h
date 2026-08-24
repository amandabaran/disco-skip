#pragma once

#include <cassert>
#include <cstring>
#include <functional>
#include <iostream>
#include <stdexcept>

#include "../common/rlx_atomic.h"
#include "../lock/seq_lock.h"
#include "atomic_kv.h"
#include "insert_result.h"

/// A data structure which uses a sorted vector to implement a multikey map
/// interface. Terrible asymptotes on insertion and deletion (O(n)), but fast
/// lookup, and fast at small sizes.
/// Though sorted, it uses a dynamic mapping of logical and physical slots to
/// enable constant-time extract-min(), by simply shifting the "start" position
/// over one.
/// Non-concurrent. Non-resizable.
/// CFRA: Circular, Fixed capacity, Relaxed Atomic
///
/// @param ENTRY    - The type representing k/v entrys.
/// @param CAPACITY The total number of key/value entrys that can be held.
template <typename ENTRY, size_t CAPACITY> class multivector_cfra {

  using K = typename ENTRY::KEY_TYPE;
  using V = typename ENTRY::VAL_TYPE;

  using entry_t = atomic_kv<ENTRY, rlx_atomic, rlx_atomic>;

  /// List of key/value entrys.
  /// We use a C-style array here so we can "cheat" and use memcpy().
  entry_t list[CAPACITY];

  // The size of the above type.
  static constexpr size_t ENTRY_SIZE = sizeof(entry_t);

  /// The lock.
  seq_lock lock;

  /// Current number of elements in the list.
  rlx_atomic<size_t> size;

  /// The current index at which the minimum index is slotted.
  rlx_atomic<size_t> min_index;

  // Increment min_index.
  // Takes the old value to save a read on an atomic variable.
  // Takes it as a reference so it can be updated.
  inline void inc_min_index(size_t &m) {
    m = (m + 1) % CAPACITY;
    min_index.store(m);
  }

  // Decrement min_index.
  // Takes the old value to save a read on an atomic variable.
  // Takes it as a reference so it can be updated.
  inline void dec_min_index(size_t &m) {
    m = (m + CAPACITY - 1) % CAPACITY;
    min_index.store(m);
  }

  // Convert from logical index to physical index.
  // Calls min_index.load(), which is slow, so if you need to call this more
  // than once, read min_index and call the variant below.
  [[nodiscard]] inline size_t phys(size_t idx) const {
    return phys(idx, min_index.load());
  }

  // Convert from logical index to physical index.
  // Faster variant which uses an already-loaded copy of min_index.
  [[nodiscard]] static inline size_t phys(size_t idx, size_t min_idx) {
    return (idx + min_idx) % CAPACITY;
  }

  /// Internal function used to find a key in the vector.
  /// A binary search that takes lg(n) time.
  /// If multiple copies of the key are present, this may find ANY instance, so
  /// it is not suitable when the first or last instance is needed; in that
  /// case, use find_first() or find_last() instead.
  /// If no copies of the key are present, returns the location into which it
  /// should be inserted. May return the out-of-bounds value [size] to indicate
  /// it should be inserted at the end.
  [[nodiscard]] size_t find_any(K const &k, size_t min_idx) const {
    int64_t left = 0;         // Left-hand bound on key's position (inclusive.)
    int64_t right = size - 1; // Right-hand bound on key's position (inclusive.)

    while (right >= left) {
      // NB: This expression is logically equivalent to
      // "size_t mid = (left + right)/2", but does not overflow.
      int64_t mid = left + ((right - left) / 2);
      K read_k = list[phys(mid, min_idx)].key;

      if (read_k == k)
        return mid;

      if (read_k > k)
        right = mid - 1;
      else
        left = mid + 1;
    }

    // key not found, but return the position where it would be
    assert(left >= 0);
    return static_cast<size_t>(left);
  }

  // Internal function used to find the position of the first instance of a key.
  // Uses a modified binary search. If the key is absent, this returns the
  // first key that is greater than it. Returns the out-of-bounds value [size]
  // if all keys in the vector are strictly less than k.
  [[nodiscard]] size_t find_first(K const &k, size_t min_idx) const {
    int64_t left = 0;
    int64_t right = size - 1;

    while (right > left) {
      // NB: The first case of the branch below may assign RIGHT to mid.
      // If this expression assigns mid to RIGHT, this loop will never
      // terminate. This can only happen if the range is two elements wide and
      // this expression rounds UP when computing the median position.
      // Thus, this expression rounds DOWN.
      int64_t const mid = left + ((right - left) / 2);
      K const read_k = list[phys(mid, min_idx)].key;

      if (read_k == k) {
        // list[mid] is equal to the key. While we can't be sure if it's the
        // first instance of the key or not, we CAN successfully rule out
        // everything to the right.
        right = mid;
      } else if (read_k > k) {
        // list[mid] is strictly greater than the key,
        // so we can rule out it and everything to its right.
        right = mid - 1;
      } else {
        // list[mid] is strictly greater than the key,
        // so we can rule out it and everything to its left.
        left = mid + 1;
      }
    }

    assert(left >= 0);

    if (left > right)
      // At this point, if left is strictly greater than right, then k is absent
      // from the vector, and so we return left, the smallest key that is > k.
      return static_cast<size_t>(left);

    // If we have arrived here, we know left == right, so the only possible
    // location for the first instance of k is here. If the key in this slot
    // equals k, then it's here; if it's greater, then it's the smallest key
    // greater than k. Either way, it's the desired result.
    if (list[phys(left, min_idx)].key >= k)
      return static_cast<size_t>(left);

    // Otherwise, the next slot over is the smallest key greater than k.
    return static_cast<size_t>(left + 1);
  }

  // Internal function used to find the position of the last instance of a key.
  // Uses a modified binary search. If the key is absent, this returns the
  // last key that is smaller than it. Returns the out-of-bounds value -1 if
  // all keys in the vector are strictly greater than k.
  [[nodiscard]] int64_t find_last(K const &k, size_t min_idx) const {
    int64_t left = 0;
    int64_t right = size - 1;

    while (right > left) {
      // NB: The first case of the branch below may assign LEFT to mid.
      // If this expression assigns mid to LEFT, this loop will never
      // terminate. This can only happen if the range is two elements wide and
      // this expression rounds DOWN when computing the median position.
      // Thus, this expression rounds UP.
      int64_t const mid = left + ((right - left + 1) / 2);
      K const read_k = list[phys(mid, min_idx)].key;

      if (read_k > k) {
        right = mid - 1;
      } else if (read_k < k) {
        left = mid + 1;
      } else {
        // list[mid] is equal to the key. While we can't be sure if it's the
        // last instance of the key or not, we CAN successfully rule out
        // everything to the left.
        // Optimization note: this is the least likely case, so it is last. (?)
        left = mid;
      }
    }

    if (left > right)
      // At this point, if left is strictly greater than right, then k is absent
      // from the vector, and so we return right, the largest key that is < k.
      return right;

    // If we have arrived here, we know left == right, so the only possible
    // location for the last instance of k is here. If the key in this slot
    // equals k, then it's here; if it's less, then it's the largest key
    // smaller than k. Either way, it's the desired result.
    if (list[phys(right, min_idx)].key <= k)
      return right;

    // Otherwise, the next slot over is the largest key smaller than k.
    return right - 1;
  }

  inline void initialize() {
    // Do not allow synthesis of vector with zero or negative capacity
    static_assert(CAPACITY >= 1);
  }

public:
  using KEY_TYPE = K;
  using VAL_TYPE = V;

  /// Indicates whether this vector class permits concurrent insertions.
  static constexpr bool SUPPORTS_CONCURRENT_INSERT = false;

  /// Constructors
  multivector_cfra() : size(0), min_index(0) { initialize(); }
  explicit multivector_cfra(config *) : size(0), min_index(0) { initialize(); }

  /// Destructor
  ~multivector_cfra() = default;

  /// Populate by stealing elements from another chunk.
  /// Insert (k,v) as the first element, and then take all entries STRICTLY
  /// greater than k from a given other vector.
  /// This method overwrites the contents of the current vector with the stolen
  /// elements; it is assumed it is called when the current vector is empty.
  /// Returns true if successful;
  /// returns false if it cannot be done as it would make this vector too full.
  bool split_insert(multivector_cfra *victim, ENTRY const &entry) {
    // First, determine how many entries to steal, and initialize key_list and
    // val_list with the appropriate sizes.
    K const &k = entry.get_k();
    size_t const victim_min_idx = victim->min_index;
    size_t const start_pos = victim->find_last(k, victim_min_idx) + 1;
    size_t const entries_to_steal = victim->size - start_pos;

    assert(entries_to_steal + 1 <= CAPACITY);

    // Add the inserted element as the minimum.
    list[0] = entry;

    // Copy over the other entries.
    size_t const phys_start_point = (victim_min_idx + start_pos) % CAPACITY;

    if (phys_start_point + entries_to_steal > CAPACITY) {
      // The range we are copying from the victim DOES loop around the end
      // of the physical array. Thus, we must do two memcpy()s.
      size_t const first_batch_size = CAPACITY - phys_start_point;
      size_t const second_batch_size = entries_to_steal - first_batch_size;

      asm volatile("" ::: "memory"); // ensure everything is flushed to RAM
      std::memcpy(list + 1, victim->list + phys_start_point,
                  first_batch_size * ENTRY_SIZE);
      std::memcpy(list + first_batch_size + 1, victim->list,
                  second_batch_size * ENTRY_SIZE);
      asm volatile("" ::: "memory"); // ensure everything is flushed to RAM
    } else {
      // The range we are copying from the victim DOES NOT loop around the end
      // of the physical array. Thus, we can do a straightforward memcpy().

      asm volatile("" ::: "memory"); // ensure everything is flushed to RAM
      std::memcpy(list + 1, victim->list + phys_start_point,
                  entries_to_steal * ENTRY_SIZE);
      asm volatile("" ::: "memory"); // ensure everything is flushed to RAM
    }

    // Correct the sizes of the two vectors.
    size = 1 + entries_to_steal;
    victim->size -= entries_to_steal;

    verify_locked();
    victim->verify_locked();

    // Return true to indicate success.
    return true;
  }

  /// Populate by stealing the last, greater half of the elements from another
  /// chunk. If victim starts with n elements, victim will end with floor(n/2)
  /// elements, and new vector will end with ceil(n/2) elements.
  /// This method overwrites the contents of the current vector with the stolen
  /// elements; it is assumed it is called when the current vector is empty.
  void steal_half(multivector_cfra *victim) {
    size_t const victim_size = victim->size;

    assert(victim_size <= CAPACITY);

    size_t const entries_to_steal = (victim_size + 1) / 2;
    size_t const start_pos = victim_size - entries_to_steal;

    // Copy over the other entries.
    size_t const phys_start_point = (victim->min_index + start_pos) % CAPACITY;

    if (phys_start_point + entries_to_steal > CAPACITY) {
      // The range we are copying from the victim DOES loop around the end
      // of the physical array. Thus, we must do two memcpy()s.
      size_t const first_batch_size = CAPACITY - phys_start_point;
      size_t const second_batch_size = entries_to_steal - first_batch_size;

      asm volatile("" ::: "memory"); // ensure everything is flushed to RAM
      std::memcpy(list, victim->list + phys_start_point,
                  first_batch_size * ENTRY_SIZE);
      std::memcpy(list + first_batch_size, victim->list,
                  second_batch_size * ENTRY_SIZE);
      asm volatile("" ::: "memory"); // ensure everything is flushed to RAM
    } else {
      // The range we are copying from the victim DOES NOT loop around the end
      // of the physical array. Thus, we can do a straightforward memcpy().

      asm volatile("" ::: "memory"); // ensure everything is flushed to RAM
      std::memcpy(list, victim->list + phys_start_point,
                  entries_to_steal * ENTRY_SIZE);
      asm volatile("" ::: "memory"); // ensure everything is flushed to RAM
    }

    // Correct the sizes of the two vectors.
    size = entries_to_steal;
    victim->size -= entries_to_steal;

    verify_locked();
    victim->verify_locked();
  }

  void steal_max(multivector_cfra *victim) {
    if (CAPACITY == 2) {
      // Edge case: if capacity is 2,
      // then stealing half is equivalent to stealing the max.
      steal_half(victim);
    } else {
      // acquire_for_split() below never returns 1, so this should never get
      // called. Wouldn't be hard to implement, but there's currently no need.
      throw std::logic_error("Unimplemented");
    }
  }

  /// We may be able to get better perf out of this by being very clever, but
  /// it'd be hard, and we don't care about batch insertion performance in this
  /// implementation. So, just insert one at a time in a loop.
  template <typename Iter> insert_result insert(Iter &b, Iter const e) {
    for (; std::distance(b, e) > 0; ++b) {
      insert_result const res = insert(*b);
      if (res != SUCCESS)
        return res;
    }
    return SUCCESS;
  }

  /// Insert a new element into the list. The insert is "stable": newly inserted
  /// items are always inserted after items with the same key. Returns true if
  /// successful; false if failed due to overfull.
  insert_result insert(ENTRY const &entry) {
    // Prevent vector from becoming overfull
    if (size == CAPACITY) {
      return MUST_SPLIT;
    }

    K const &k = entry.get_k();

    // This index will eventually represent the insertion position.
    // It defaults to 0, the position used when the vector is empty.
    size_t i = 0;
    size_t min_idx = min_index.load();

    // If the vector is non-empty, a median exists, so we sample it to
    // determine (in constant time) if the new element should be inserted at
    // the right end or the left end.
    if (size > 0 && k >= list[phys(size / 2, min_idx)].key) {
      // The inserted key is greater than or equal to the median and therefore
      // needs to go into the right end. So, insert the normal way (as
      // multivector_sfra), by shifting elements from the end to make room.
      for (i = size; list[phys(i - 1, min_idx)].key > k; --i)
        list[phys(i, min_idx)] = list[phys(i - 1, min_idx)];
    } else if (size > 0) {
      // The inserted element is less than the median and therefore needs to go
      // into the left end. So, insert the reverse way, by decrementing
      // min_index and shifting elements from the start to make room.
      dec_min_index(min_idx);
      for (; list[phys(i + 1, min_idx)].key <= k; ++i)
        list[phys(i, min_idx)] = list[phys(i + 1, min_idx)];
    }

    list[phys(i, min_idx)] = entry;
    ++size;
    verify_locked();
    return SUCCESS;
  }

  /// This class does not support concurrent insert, so just delegate to the
  /// regular insert method.
  insert_result insert_concurrent(ENTRY const &entry, int64_t) {
    return insert(entry);
  }

  /// This class does not support concurrent insert, so just delegate to the
  /// regular insert method.
  template <typename Iter>
  insert_result insert_concurrent(Iter &b, Iter const e, int64_t) {
    return insert(b, e);
  }

  // This class is already sequential-only, so just call insert().
  bool insert_seq(ENTRY const &entry) { return insert(entry); }

  // For vectors that do not support concurrent insert, this is a no-op.
  bool wait_until_split() { return true; }

  /// Remove an element from the list and fetch its value. Return true if
  /// successful, false if didn't exist.
  /// The removal is "stable": if multiple elements with the same key are
  /// present, the removed one is always the one that is first.
  bool remove(K const &k, V &v) {
    size_t min_idx = min_index.load();
    size_t const i = find_first(k, min_idx);

    if (i >= size || list[phys(i, min_idx)].key > k) {
      // Element not found.
      return false;
    }

    v = list[phys(i, min_idx)].val;

    if (i >= size / 2) {
      // Element is on "right end" (or dead center),
      // so do a normal shift (as multivector_sfra)
      --size;
      for (size_t j = i; j < size; ++j)
        list[phys(j, min_idx)] = list[phys(j + 1, min_idx)];
    } else {
      // Element is on "left end," so do a reverse shift and move start index
      --size;
      for (size_t j = i; j > 0; --j)
        list[phys(j, min_idx)] = list[phys(j - 1, min_idx)];
      inc_min_index(min_idx);
    }

    verify_locked();
    return true;
  }

  /// As above, but caller doesn't care about found value.
  bool remove(K const &k) {
    V _; // Dummy argument
    return remove(k, _);
  }

  /// Find a given key in the list.
  /// This contains operation is "stable":
  /// it always finds the first instance of a key if multiple are present.
  /// Return false if not found.
  bool contains(K const &k, V &v) const {
    size_t min_idx = min_index.load();
    size_t i = find_first(k, min_idx);
    size_t phys_idx = phys(i, min_idx);

    if (i >= size || list[phys_idx].key != k)
      // Element not found.
      return false;

    v = list[phys_idx].val;
    return true;
  }

  /// As above, but caller doesn't care about found value.
  [[nodiscard]] bool contains(K const &k) const {
    size_t min_idx = min_index.load();
    size_t pos = find_any(k, min_idx);
    return pos < size && list[phys(pos, min_idx)].key == k;
  }

  /// Removes the minimum element, and assigns the reference parameter to its
  /// key and value. Returns false if no elements are available.
  bool extract_min(ENTRY &entry) {
    if (size <= 0)
      // No elements left to return.
      return false;

    size_t min_idx = min_index.load();
    entry = list[phys(0, min_idx)].unwrap();

    --size;
    inc_min_index(min_idx);

    return true;
  }

  /// For this implementation, extracting the minimum is just as fast as
  /// extracting an arbitrary element, so do that.
  bool extract(ENTRY &entry) { return extract_min(entry); }

  /// Return the key in slot 0, which is guaranteed to be the minimum.
  [[nodiscard]] K first() const {
    assert(size >= 1);
    assert(size <= CAPACITY);
    return list[phys(0)].key;
  }

  /// Return the maximum key via the parameter k.
  /// Do nothing and return false if empty.
  bool last(K &k) const {
    if (size > 0) {
      k = list[phys(size - 1)].key;
      return true;
    }

    return false;
  }

  /// Find the biggest key that is Less Than or Equal to k (hence "lte").
  /// The value is assigned to v.
  /// Returns false if there is no such element.
  /// If multiple instances of the key are present, finds the last one.
  bool find_lte(K const &k, V &v) const {
    size_t const min_idx = min_index.load();
    int64_t const pos = find_last(k, min_idx);

    if (pos < 0) {
      // If find_last goes left of the start, there is no suitable element.
      return false;
    }

    size_t const idx = phys(pos, min_idx);
    v = list[idx].val;
    return true;
  }

  /// In debug mode, dump the state of the data structure and throw an error to
  /// end immediately. In "release" mode, no op.
  [[nodiscard]] bool fail() const {
#ifndef NDEBUG
    dump();
    std::cout << std::endl;
    throw std::logic_error("verify failure");
#endif
    return false;
  }

  bool verify() const {
    // Verify the lock.
    if (lock.is_locked()) {
      std::cout << "Lock unexpectedly held at verify time." << std::endl;
      return fail();
    }

    return verify_locked();
  }

  bool verify_locked() const {
#ifndef NDEBUG
    using std::cout;
    using std::endl;

    if (size > CAPACITY) {
      cout << "Size greater than capacity at verify time!" << endl;
      return fail();
    }

    if (lock.is_dead()) {
      cout << "Lock unexpectedly dead at verify time." << endl;
      return fail();
    }

    // Check that keys are monotonically increasing
    size_t const min_idx = min_index.load();

    for (size_t i = 1; i < size; ++i) {
      size_t const this_idx = phys(i, min_idx);
      size_t const last_idx = phys(i - 1, min_idx);
      if (list[this_idx].key < list[last_idx].key) {
        cout << "Verification failed! Key " << +list[last_idx].key
             << " is at position " << i - 1 << ", but key "
             << +list[this_idx].key << " is at position " << i << "!" << endl;
        cout << "Current size: " << size << endl;
        return fail();
      }
    }
#endif
    return true;
  }

  bool verify_nontail_node(K const &max) const {
    size_t const min_idx = min_index.load();

    // Verify all elements are <= max
    for (size_t i = 0; i < size; ++i) {
      K const &k = list[phys(i, min_idx)].key;
      if (k > max) {
        std::cout << "Key " << k << " at index " << i
                  << " exceeds maximum value of " << max << std::endl;
        return fail();
      }
    }

    return true;
  }

  /// Sort this vector.
  /// This implementation is already sorted, so this is a no-op.
  void sort() const {}

  /// Return the key at a specific index.
  /// Assertion failure if index out of range.
  [[nodiscard]] K at(size_t index) const {
    assert(index < size);
    return list[phys(index)].key;
  }

  /// Return the value at a specific index.
  /// Assertion failure if index out of range.
  [[nodiscard]] V value_at(size_t index) const {
    assert(index < size);
    return list[phys(index)].val;
  }

  void verbose_analysis() const {
    using std::cout;

    cout << "[";

    size_t min_idx = min_index.load();

    // Print first element
    if (size > 0)
      cout << +list[phys(0, min_idx)].key;

    // Print last element if distinct from first element
    if (size > 1)
      cout << "-" << +list[phys(size - 1, min_idx)].key;

    // Print size/capacity
    cout << "](" << size << "/" << CAPACITY << ")" << std::endl;
  }

  void dump() const {
    using std::cout;
    size_t const min_idx = min_index.load();

    lock.dump();

    cout << std::hex << "[ ";
    for (size_t i = 0; i < size; ++i) {
      size_t const phys_idx = phys(i, min_idx);
      cout << +list[phys_idx].key << ":" << list[phys_idx].val << " ";
    }

    // Print size/capacity
    cout << "](" << size << "/" << CAPACITY << ")" << std::endl;
    cout << "Minimum is at index: " << min_index << std::endl;
  }

  // Get the maximum key
  [[nodiscard]] K max_key() const {
    // Assert that there's at least one element
    // (the maximum of the empty set is undefined)
    if (size == 0)
      throw std::out_of_range(
          "max_key(): maximum of empty vector is undefined");

    assert(size <= CAPACITY);

    return list[phys(size - 1)].key;
  }

  [[nodiscard]] std::string_view get_name() const {
    return "Sorted Vector, Relaxed Atomic";
  }
  [[nodiscard]] size_t get_capacity() const { return CAPACITY; }
  [[nodiscard]] size_t get_size() const { return size; }

  // No op
  static void tear_down() {}

  // Lock passthrough methods
  bool acquire() { return lock.acquire(); }
  size_t acquire_for_split() { return CAPACITY / 2; }
  uint64_t release_after_split() { return 0; };
  bool try_upgrade(uint64_t v) { return lock.try_upgrade(v); }
  uint64_t release() {
    verify_locked();
    return lock.release();
  }
  void release_unchanged() {
    verify_locked();
    lock.release_unchanged();
  }
  void die() {
    verify_locked();
    return lock.die();
  }
  [[nodiscard]] bool is_dead() const { return lock.is_dead(); }
  [[nodiscard]] uint64_t begin_read() const { return lock.begin_read(); }
  [[nodiscard]] bool confirm_read(uint64_t v) const {
    return lock.confirm_read(v);
  }
};
