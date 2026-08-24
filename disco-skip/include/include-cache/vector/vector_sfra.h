#pragma once

#include <cassert>
#include <cstring>
#include <functional>
#include <iostream>
#include <stdexcept>

#include "../common/config.h"
#include "../common/entry.h"
#include "../common/rlx_atomic.h"
#include "atomic_kv.h"

/// A data structure which uses a sorted vector to implement a map interface.
/// Terrible asymptotes on insertion and deletion (O(n)), but fast lookup,
/// and fast at small sizes.
/// Non-concurrent. Non-resizable.
/// SFRA: Sorted, Fixed capacity, Relaxed Atomic
/// @param K The key type.
/// @param V The value type.
/// @param CAPACITY The total number of key/value pairs that can be held.
template <typename K, typename V, size_t CAPACITY> class vector_sfra {

  static constexpr K EMPTY = static_cast<K>(-1);
  using ENTRY = entry<K, V, EMPTY>;
  using entry_t = atomic_kv<ENTRY, rlx_atomic, rlx_atomic>;

  /// List of key/value pairs.
  /// We use a C-style array here so we can "cheat" and use memcpy().
  entry_t list[CAPACITY];

  // The size of the above type.
  static constexpr size_t ENTRY_SIZE = sizeof(entry_t);

  /// Current number of elements in the list.
  rlx_atomic<size_t> size;

  /// Internal function used to find a key in the vector.
  /// If it exists, it returns the exact position;
  /// Otherwise, the position it should be inserted into.
  /// A binary search that takes lg(n) time.
  /// May return the out-of-bounds value [size] to indicate it should be
  /// inserted at the end.
  [[nodiscard]] size_t find(K const &k) const {
    // TODO: add SIMD vectorization here
    int64_t left = 0;
    int64_t right = size - 1;

    while (right >= left) {
      // NB: logically equivalent to "int mid = (left + right)/2",
      // but does not overflow.
      int64_t const mid = left + ((right - left) / 2);

      if (list[mid].key == k)
        return mid;
      else if (list[mid].key > k)
        right = mid - 1;
      else
        left = mid + 1;
    }

    // key not found, but return the position where it would be
    assert(left >= 0);
    return static_cast<size_t>(left);
  }

  void initialize() {
    // Do not allow synthesis of vector with zero or negative capacity
    static_assert(CAPACITY >= 1);
  }

public:
  using KEY_TYPE = K;
  using VAL_TYPE = V;

  /// insert() takes a reference to a k/v pair, so we expose the type here
  using value_type = std::pair<const K, V>;

  /// Constructor
  vector_sfra() : size(0) { initialize(); }
  explicit vector_sfra(config *) : size(0) { initialize(); }

  /// Destructor
  ~vector_sfra() = default;

  /// Populate by stealing elements from another chunk.
  /// Insert (k,v) as the first element, and then take all entries greater than
  /// k from a given other vector.
  /// This method overwrites the contents of the current vector with the stolen
  /// elements; it is assumed it is called when the current vector is empty.
  /// Returns true if successful;
  /// returns false if it cannot be done as it would make this vector too full.
  bool split_insert(vector_sfra *victim, value_type const &pair) {

    // First, determine how many entries to steal,
    // and initialize list with the appropriate size.
    K const &k = pair.first;
    size_t const start_pos = victim->find(k);

    // Assert that k is not in the victim vector.
    assert(start_pos >= victim->size || victim->list[start_pos].key != k);

    size_t const entries_to_steal = victim->size - start_pos;

    assert(entries_to_steal + 1 <= CAPACITY);

    // Initialize the first entry.
    list[0] = pair;

    // Copy over the other entries.
    asm volatile("" ::: "memory"); // ensure everything is flushed to RAM
    std::memcpy(list + 1, victim->list + start_pos,
                entries_to_steal * ENTRY_SIZE);
    asm volatile("" ::: "memory"); // ensure everything is flushed to RAM

    // Finally, correct the sizes of the two vectors.
    size = 1 + entries_to_steal;
    victim->size -= entries_to_steal;

    // Return true to indicate success.
    return true;
  }

  /// Construct and populate by stealing the latter half of the elements from
  /// another chunk. Also insert (k,v) into either the victim or the newly
  /// constructed vector as appropriate.
  /// If victim starts with n elements, victim will end with ceil((n+1)/2)
  /// elements, and this vector will end with floor((n+1)/2) elements.
  /// This method overwrites the contents of the current vector with the stolen
  /// elements; it is assumed it is called when the current vector is empty.
  void steal_half_and_insert(vector_sfra *victim, value_type const &pair) {

    K const &k = pair.first;

    // First, determine where the inserted element should go.
    size_t const insert_pos = victim->find(k);

    // Assert that k is not in the victim vector.
    assert(insert_pos >= victim->size || victim->list[insert_pos].key != k);

    // NB: We slightly abuse the term "median" here. Normally, if the number of
    // elements is even, the median is the average of the two middle elements.
    // Here we simply take the greater of the two.
    size_t const median_pos = victim->size / 2;

    if (insert_pos <= median_pos) {
      // Case 1: (k,v) will be inserted into victim.
      // In this case, we simply split the vector and then insert the new
      // element, as there isn't a more efficient way to do it.

      // In this case, we steal the median and all elements that follow.
      size_t const first_to_steal = median_pos;

      // First, copy elements from victim.
      // NB: If victim->size is even, then first_to_steal == entries_to_steal,
      // but if it's odd, then entries_to_steal ends up being 1 greater.
      size_t const entries_to_steal = victim->size - first_to_steal;

      asm volatile("" ::: "memory"); // ensure everything is flushed to RAM
      std::memcpy(list, victim->list + first_to_steal,
                  entries_to_steal * ENTRY_SIZE);
      asm volatile("" ::: "memory"); // ensure everything is flushed to RAM

      // Correct the sizes of the two vectors.
      size = entries_to_steal;
      victim->size = first_to_steal;

      // Finally, insert the new element into the victim.
      victim->insert(pair);
    } else {
      // Case 2: (k,v) will be inserted into this vector.
      // We combine the insert and copy procedures to avoid wastefully moving
      // elements twice, which would be the result of the naive approach (split
      // then insert).

      // In this case, we allow the victim to keep the median,
      // and so the first element we steal is the one after that.
      size_t const first_to_steal = median_pos + 1;
      size_t const entries_to_steal = victim->size - first_to_steal;

      size_t const first_batch_size = insert_pos - first_to_steal;
      size_t const second_batch_size = entries_to_steal - first_batch_size;

      // Copy the first batch, the elements between the start point and the
      // inserted element (may be zero.)

      // we don't have P1478R1 yet, so we do the fences by hand:
      asm volatile("" ::: "memory"); // ensure everything is flushed to RAM
      std::memcpy(list, victim->list + first_to_steal,
                  first_batch_size * ENTRY_SIZE);
      asm volatile("" ::: "memory"); // ensure everything is flushed to RAM

      // Write the inserted element.
      list[first_batch_size] = pair;

      // Copy the second batch, the elements between the inserted element and
      // the end (may be zero.)
      asm volatile("" ::: "memory"); // ensure everything is flushed to RAM
      std::memcpy(list + first_batch_size + 1,
                  victim->list + first_to_steal + first_batch_size,
                  second_batch_size * ENTRY_SIZE);
      asm volatile("" ::: "memory"); // ensure everything is flushed to RAM

      // Correct the sizes of the two vectors.
      size = entries_to_steal + 1;
      victim->size -= entries_to_steal;
    }
  }

  /// Populate by stealing the latter half of the elements from another chunk.
  /// If victim starts with n elements, victim will end with floor(n/2)
  /// elements, and new vector will end with ceil(n/2) elements.
  /// This method overwrites the contents of the current vector with the stolen
  /// elements; it is assumed it is called when the current vector is empty.
  void steal_half(vector_sfra *victim) {
    size_t const median_pos = victim->size / 2;

    // Steal the median and all elements that follow.
    size_t const first_to_steal = median_pos;

    // Copy elements from victim.
    size_t const entries_to_steal = victim->size - first_to_steal;

    asm volatile("" ::: "memory"); // ensure everything is flushed to RAM
    std::memcpy(list, victim->list + first_to_steal,
                entries_to_steal * ENTRY_SIZE);
    asm volatile("" ::: "memory"); // ensure everything is flushed to RAM

    // Correct the sizes of the two vectors.
    size = entries_to_steal;
    victim->size = first_to_steal;
  }

  /// Insert a new element into the list.
  /// If already exists, do nothing and return false.
  /// If it does not exist, but there isn't room to insert it,
  /// mark the overfull parameter true and return false.
  bool insert(value_type const &pair, bool &overfull) {
    K const &k = pair.first;

    size_t const pos = find(k);

    if (pos < size && list[pos].key == k) {
      // Already exists
      overfull = false;
      return false;
    }

    // Prevent vector from becoming overfull
    if (size == CAPACITY) {
      overfull = true;
      return false;
    }

    // Shift other elements to make room
    for (size_t i = size; i > pos; --i) {
      list[i] = list[i - 1];
    }

    // Insert new element
    list[pos] = pair;

    ++size;

    overfull = false;
    return true;
  }

  /// Insert a new element into the list.
  /// If already exists, do nothing and return false.
  /// If it does not exist, but there isn't room to insert it,
  /// an assert will fail.
  bool insert(value_type const &pair) {
    bool overfull = false;
    bool const result = insert(pair, overfull);
    assert(!overfull);
    return result;
  }

  // This class is already sequential-only, so just call insert().
  bool insert_seq(value_type const &pair) { return insert(pair); }

  /// Remove an element from the list and fetch its value.
  /// Return true if successful, false if didn't exist.
  bool remove(K const &k, V &v) {
    size_t const pos = find(k);

    if (pos >= size || list[pos].key != k)
      // Didn't exist
      return false;

    v = list[pos].val;
    --size;

    // Fill gap by shifting other elements
    for (size_t i = pos; i < size; ++i)
      list[i] = list[i + 1];

    return true;
  }

  /// As above, but caller doesn't care about found value.
  bool remove(K const &k) {
    V _; // Dummy argument
    return remove(k, _);
  }

  /// Find a given key in the list.
  /// Return false if not found.
  bool contains(K const &k, V &v) const {
    size_t const pos = find(k);

    if (pos < size && list[pos].key == k) {
      v = list[pos].val;
      return true;
    }

    return false;
  }

  /// As above, but caller doesn't care about found value.
  [[nodiscard]] bool contains(K const &k) const {
    size_t const pos = find(k);
    return pos < size && list[pos].key == k;
  }

  /// Return the minimum key.
  /// CAVEAT: If vector is empty, may return junk data!
  [[nodiscard]] K first() const { return list[0].key; }

  /// Return the last key via the parameter k.
  /// Do nothing and return false if empty.
  bool last(K &k) const {
    if (size > 0) {
      k = list[size - 1].key;
      return true;
    }

    return false;
  }

  /// Find the biggest key that is Less Than or Equal to sought_k (hence "lte").
  /// The found key is assigned to found_k, and the value is assigned to v.
  /// Returns false if there is no such element.
  bool find_lte(K const &sought_k, K &found_k, V &v) const {
    // Edge case: if entirely empty, just return false.
    if (size == 0)
      return false;

    // First, call find.
    size_t pos = find(sought_k);

    // find() has "GTE" behavior; it finds the smallest element Greater Than or
    // Equal to sought_k. If sought_k is in the vector, this is what we want;
    // otherwise, we subtract 1 to find the largest element less than sought_k.
    if (pos >= size || list[pos].key > sought_k) {
      if (pos == 0)
        // No element in entire vector is <= k
        return false;

      --pos;
    }

    // Otherwise, pos is the position of the element we want.
    found_k = list[pos].key;
    v = list[pos].val;
    return true;
  }

  /// As above, but caller doesn't care about the found k
  bool find_lte(K const &sought_k, V &v) const {
    K _ = sought_k;
    return find_lte(sought_k, _, v);
  }

  /// Consume another vector_sfra, stealing all of its elements. This
  /// method assumes the other vector's minimum element > this vector's maximum
  /// element.
  void merge(vector_sfra *victim) {
    // Check against overfull
    assert(size + victim->size <= CAPACITY);

    // Copy the elements over
    asm volatile("" ::: "memory"); // ensure everything is flushed to RAM
    std::memcpy(list + size, victim->list, victim->size * ENTRY_SIZE);
    asm volatile("" ::: "memory"); // ensure everything is flushed to RAM

    size += victim->size;
    victim->size = 0;
  }

  /// In debug mode, dump the state of the data structure and throw an error to
  /// end immediately. In "release" mode, no op.
  [[nodiscard]] bool fail() const {
#ifndef NDEBUG
    dump();
    throw std::logic_error("verify failure");
#endif
    return false;
  }

  bool verify() const {
    using std::cout;
    using std::endl;

    if (CAPACITY < size) {
      cout << "Verification failed! Vector is " << size << "/" << CAPACITY
           << endl;
      return fail();
    }

    // Check that keys are monotonically increasing
    for (size_t i = 1; i < size; ++i) {
      if (list[i].key <= list[i - 1].key) {
        cout << "Verification failed! Key " << +list[i - 1].key
             << " is at position " << i - 1 << ", but key " << +list[i].key
             << " is at position " << i << "!" << endl;
        cout << "Current size: " << size << endl;
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
    // Bounds check
    assert(index < size);
    return list[index].key;
  }

  void verbose_analysis() const {
    using std::cout;

    cout << "[";

    // Print first element
    if (size > 0)
      cout << +list[0].key;

    // Print last element if distinct from first element
    if (size > 1)
      cout << "-" << +list[size - 1].key;

    // Print size out of capacity
    cout << "](" << size << "/" << CAPACITY << ")" << std::endl;
  }

  void dump() const {
    using std::cout;

    // Print all elements
    cout << "[ ";
    for (size_t i = 0; i < size; ++i)
      cout << +list[i].key << " ";

    // Print size/capacity
    cout << "](" << size << "/" << CAPACITY << ")" << std::endl;
  }

  // Get the maximum key
  [[nodiscard]] K max_key() const {
    // Assert that there's at least one element
    // (the maximum of the empty set is undefined)
    if (size == 0)
      throw std::out_of_range(
          "max_key(): maximum of empty vector is undefined");

    return list[size - 1].key;
  }

  [[nodiscard]] std::string_view get_name() const {
    return "Sorted Vector, Relaxed Atomic";
  }
  [[nodiscard]] size_t get_capacity() const { return CAPACITY; }
  [[nodiscard]] size_t get_size() const { return size; }

  /// Apply a function f() to all key/value pairs in this vector.
  /// Reports to a parent for_each() function if it should stop via the
  /// exit_flag parameter.
  void for_each(std::function<void(const K &, V &, bool &)> f,
                bool &exit_flag) {
    for (size_t i = 0; i < size && !exit_flag; ++i) {
      // NB: This is necessary because val_list is atomic,
      // and f() wants a non-atomic T.
      // Copying v twice will be expensive if it's big...
      // Application of f() isn't atomic, but we trust the caller (in this
      // project, the skipvector) to provide true mutex with any other
      // modifying operations while for_each() is running.
      V v = list[i].val.load();
      f(list[i].key.load(), v, exit_flag);
      list[i].val.store(v);
    }
  }

  /// Apply a function f() to all key/value pairs in this vector.
  void for_each(std::function<void(const K &, V &, bool &)> f) {
    bool exit_flag = false;
    for_each(f, exit_flag);
  }

  /// Apply a function f() to all key/value pairs in the intersection of this
  /// vector and the given range [from, to].
  /// Returns true if end of range is reached, false otherwise.
  bool range(K const &from, K const &to,
             std::function<void(const K &, V &, bool &)> f, bool &exit_flag) {

    size_t i = 0;

    // Skip over any elements less than the start.
    while (i < size && list[i].key < from) {
      ++i;
    }

    // Apply f() to elements in the range.
    while (i < size && !exit_flag) {
      if (list[i].key > to) {
        // If we encounter an element after the end of the range, return true.
        return true;
      }

      // NB: Same note as in foreach().
      V v = list[i].val.load();
      f(list[i].key, v, exit_flag);
      list[i].val.store(v);
      ++i;
    }

    return false;
  }

  /// Apply a function f() to all key/value pairs in the intersection of this
  /// vector and the given range [from, to].
  /// Returns true if end of range is reached, false otherwise.
  bool range(K const &from, K const &to,
             std::function<void(const K &, V &, bool &)> f) {
    bool exit_flag = false;
    return range(from, to, f, exit_flag);
  }

  // No op
  static void tear_down() {}
};
