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

/// A data structure which uses an unsorted vector to implement a map interface.
/// Terrible asymptotes (O(n)), but fast at small sizes.
/// Non-concurrent. Non-resizable.
/// UMFRA: Unsorted, track Maximum, Fixed capacity, Relaxed Atomic
/// INVARIANT: If vector has at least 2 elements, the minimum element is at
/// index 0 and the maximum is at index 1.
///
/// @param K The key type.
/// @param V The value type.
/// @param CAPACITY The total number of key/value pairs that can be held.
template <typename K, typename V, size_t CAPACITY> class vector_umfra {

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
  /// If it exists, it returns true, and sets pos.
  /// Otherwise, it returns false.
  bool find(K const &k, size_t &pos) {
    // TODO: add SIMD vectorization here
    for (size_t i = 0; i < size; i++) {
      if (list[i].key == k) {
        pos = i;
        return true;
      }
    }
    return false;
  }

  /// As above, but we do not care about the index.
  bool find(K const &k) {
    size_t _ = k;
    return find(k, _);
  }

  // Swap key and value at indices n and m.
  void swap(int n, int m) {
    if (n != m)
      list[n].swap(list[m]);
  }

  // Performs a quickselect to take the median.
  void quickselect_median() {
    if (CAPACITY > 2 && size > 2) {
      // Move the maximum to the end.
      swap(1, size - 1);

      // Quickselect for the median element, taking the entire list minus the
      // already-sorted min and max elements.
      std::nth_element(list + 1, list + (size / 2), list + size - 1);
    }
  }

  // Restore the invariant when max is at the final position.
  void restore_invariant() {
    if (CAPACITY > 2 && size > 2) {
      swap(1, size - 1);
    }
  }

  // Finds the minimum element and swaps it to its correct position
  // after the invariant was ruined by some method.
  // This method assumes max is in its correct position.
  void find_new_min() {
    if (CAPACITY <= 2 || size <= 2)
      // min should already be in place.
      return;

    size_t min_pos = 0;
    K min_k = list[0].key;

    for (size_t i = 2; i < size; ++i) {
      if (list[i].key < min_k) {
        min_k = list[i].key;
        min_pos = i;
      }
    }

    if (min_pos != 0)
      swap(0, min_pos);
  }

  // Finds the maximum element and swaps it to its correct position
  // after the invariant was ruined by some method.
  // This method assumes min is in its correct position.
  void find_new_max() {
    if (CAPACITY <= 2 || size <= 2)
      // max should already be in place.
      return;

    size_t max_pos = 1;
    K max_k = list[1].key;

    for (size_t i = 2; i < size; ++i) {
      if (list[i].key > max_k) {
        max_k = list[i].key;
        max_pos = i;
      }
    }

    if (max_pos != 1)
      swap(1, max_pos);
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

  /// Constructors
  vector_umfra() : size(0) { initialize(); }
  explicit vector_umfra(config *) : size(0) { initialize(); }

  /// Destructor
  ~vector_umfra() = default;

  /// Populate by stealing elements from another chunk.
  /// Insert (k,v) as the first element, and then take all entries greater than
  /// k from a given other vector.
  /// This method overwrites the contents of the current vector with the stolen
  /// elements; it is assumed it is called when the current vector is empty.
  /// Returns true if successful;
  /// returns false if it cannot be done as it would make this vector too full.
  bool split_insert(vector_umfra *victim, value_type const &pair) {
    K const &k = pair.first;

    // First, determine the needed capacity.
    // Also, scan for the element that will become victim's new max.
    size_t elements_to_steal = 0;
    size_t victims_new_max_pos = 0;
    K victims_new_max_key = victim->list[0].key;

    // We want to start looking for victims_new_max_key at element 2,
    // so unroll the first two iterations of the loop.
    if (victim->size > 0 && victim->list[0].key > k)
      ++elements_to_steal;

    if (CAPACITY >= 2 && victim->size > 1 && victim->list[1].key > k)
      ++elements_to_steal;

    for (size_t i = 2; i < victim->size; ++i) {
      // Count elements to steal
      if (victim->list[i].key > k)
        ++elements_to_steal;

      // Find victim's new max element
      if (victim->list[i].key < k &&
          victim->list[i].key > victims_new_max_key) {
        victims_new_max_key = victim->list[i].key;
        victims_new_max_pos = i;
      }

      // Assert that k is not in the victim vector.
      assert(victim->list[i].key != k);
    }

    size_t const needed_capacity = elements_to_steal + 1;

    assert(needed_capacity <= CAPACITY);

    // Initialize the first entry.
    list[0] = pair;

    // Edge case 1: there are no elements to be stolen from victim.
    // NB: If the caller respects the invariant that this method is never
    // invoked when it would result in this vector becoming overfull,
    // and capacity is 1, then there can not be any elements to steal.
    if (CAPACITY == 1 || elements_to_steal == 0) {
      // We're already done!
      size = 1;
      return true;
    }

    // Edge case 2: steal ALL elements from victim.
    if (elements_to_steal == victim->size) {

      // Edge case 2A: victim has just one element
      if (elements_to_steal == 1) {
        // Move victim's only element to our maximum position.
        list[1] = victim->list[0];
      } else {
        // Edge case 2B: victim has at least two elements
        // Move element in victim's maximum position to ours.
        list[1] = victim->list[1];

        // Steal element in victim's minimum position.
        list[2] = victim->list[0];

        // Steal all other elements.
        elements_to_steal -= 2;
        asm volatile("" ::: "memory"); // ensure everything is flushed to RAM
        std::memcpy(list + 3, victim->list + 2, elements_to_steal * ENTRY_SIZE);
        asm volatile("" ::: "memory");
      }

      victim->size = 0;
      size = needed_capacity;
      return true;
    }

    // Edge case 3: Steal all elements from victim except its minimum.
    if (victims_new_max_key == victim->list[0].key) {
      // NB: Element in victim's max position will be moved to our max position.
      asm volatile("" ::: "memory"); // ensure everything is flushed to RAM
      std::memcpy(list + 1, victim->list + 1, elements_to_steal * ENTRY_SIZE);
      asm volatile("" ::: "memory");

      victim->size = 1;
      size = needed_capacity;
      return true;
    }

    // NB: Due to the edge cases ruled out above, we know for certain that we
    // will steal at least one of the victim's elements, and that the victim
    // will retain at least two elements. Since we will steal at least one
    // element, we know we will steal victim's maximum. Since we will leave
    // at least two elements, we know we will not steal the victim's minimum,
    // and that the victim's maximum position will be occupied after we steal.

    // Steal the victim's maximum element.
    list[1] = victim->list[1];
    size = 2;

    // Move the victim's new maximum element to its proper position.
    victim->list[1] = victim->list[victims_new_max_pos];
    --victim->size;

    // Move victim's last element into the gap left by this move.
    if (victims_new_max_pos != victim->size)
      victim->list[victims_new_max_pos] = victim->list[victim->size];

    // Now, steal all the elements > k from the victim vector.
    // This loop carefully avoids moving any element more than once.
    size_t i = 2;
    while (i < victim->size) {
      if (victim->list[i].key < k) {
        // Skip over any elements that are < k.
        ++i;

      } else if (victim->list[victim->size - 1].key > k) {
        // Move any elements > k from the end of the victim.
        --victim->size;
        list[size] = victim->list[victim->size];
        ++size;

      } else {
        // At this point we know victim[i] > k, victim[size - 1] < k, and
        // i != size - 1.  Thus, move victim[i] to this vector,
        // and victim[size - 1] to victim[i].
        list[size] = victim->list[i];
        ++size;
        --victim->size;
        victim->list[i] = victim->list[victim->size];
        ++i;
      }
    }

    return true;
  }

  /// Construct and populate by stealing the latter half of the elements from
  /// another chunk. Also insert (k,v) into either the victim or the newly
  /// constructed vector as appropriate.
  /// If victim starts with n elements, victim will end with ceil((n+1)/2)
  /// elements, and this vector will end with floor((n+1)/2) elements.
  /// This method overwrites the contents of the current vector with the stolen
  /// elements; it is assumed it is called when the current vector is empty.
  void steal_half_and_insert(vector_umfra *victim, value_type const &pair) {
    victim->quickselect_median();

    K const &k = pair.first;

    // NB: We slightly abuse the term "median" here. Normally, if the number of
    // elements is even, the median is the average of the two middle elements.
    // Here we simply take the greater of the two.
    size_t const median_pos = victim->size / 2;
    K median_k = victim->list[median_pos].key;
    size_t first_to_steal = 0;

    if (k < median_k)
      // Case 1: (k,v) will be inserted into victim.
      // In this case, we steal the median and all elements that follow.
      first_to_steal = median_pos;
    else
      // Case 2: (k,v) will be inserted into this vector.
      // In this case, we allow the victim to keep the median,
      // and so the first element we steal is the one after that.
      first_to_steal = median_pos + 1;

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

    // Our quickselect algorithm places the max at the end,
    // so this will restore this vector's maximum element.
    restore_invariant();

    // At this point, victim's min is correct, as it was never disturbed.
    // All that remains is to correct our min and victim's max.
    if (k < median_k) {
      // If we took the median, it ended up as our min,
      // so no op there.
      // Find victim's new max.
      victim->find_new_max();

      // Finally, insert element into victim.
      victim->insert(pair);
    } else {
      // If we let the victim keep the median, it ended up in its last position.
      // Therefore, this will restore it to position 1.
      victim->restore_invariant();

      // Find our new min.
      find_new_min();

      // Finally, insert element into this vector.
      insert(pair);
    }
  }

  /// Populate by stealing the latter half of the elements from another chunk.
  /// If victim starts with n elements, victim will end with floor(n/2)
  /// elements, and new vector will end with ceil(n/2) elements.
  /// This method overwrites the contents of the current vector with the stolen
  /// elements; it is assumed it is called when the current vector is empty.
  void steal_half(vector_umfra *victim) {
    victim->quickselect_median();

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

    // Our quickselect algorithm places the max at the end,
    // so this will restore this vector's maximum element.
    restore_invariant();

    // At this point, victim's min is correct, as it was never disturbed.
    // Our min is correct, as it is the median.
    // All that remains is to correct victim's max.
    victim->find_new_max();
  }

  /// Insert a new element into the list.
  /// If already exists, do nothing and return false.
  bool insert(value_type const &pair, bool &overfull) {
    K const &k = pair.first;

    if (find(k))
      // Already exists
      return false;

    // Prevent vector from becoming overfull
    if (size == CAPACITY) {
      overfull = true;
      return false;
    }

    // Insert new element

    if (CAPACITY >= 1 && size >= 1 && k < list[0].key) {
      // Case 1: element is smaller than existing minimum
      // Move previous min to end of list
      list[size] = list[0];

      // Insert new element as new minimum
      list[0] = pair;
    } else if (CAPACITY >= 2 && size >= 2 && k > list[1].key) {
      // Case 2: element is larger than existing maximum
      // Move previous max to end of list
      list[size] = list[1];

      // Insert new element as new maximum
      list[1] = pair;
    } else {
      // Normal case: new element is between existing max and min
      // (Or, correctly inserting new element as new minimum or maximum
      // because that slot happens to be at the end of the vector.)
      list[size] = pair;
    }

    ++size;
    return true;
  }

  /// Insert a new element into the list.
  /// If already exists, do nothing and return false.
  /// If it does not exist, but there isn't room to insert it,
  /// throw overfull.
  bool insert(value_type const &pair) {
    bool overfull = false;
    bool const result = insert(pair, overfull);
    assert(!overfull);
    return result;
  }

  // This class is already sequential-only, so just call insert().
  bool insert_seq(value_type const &pair) { return insert(pair); }

  /// Remove an element from the list.
  /// Return true if successful, false if didn't exist.
  bool remove(K const &k, V &v) {
    // Edge case 1: Removing minimum
    if (size >= 1 && k == list[0].key) {
      v = list[0].val;

      // Edge case 1A: Removing sole element
      if (size == 1) {
        size = 0;
      } else {
        // Edge case 1B: Need to find new minimum
        K new_min = list[1].key;
        size_t new_min_pos = 1;
        for (size_t i = 2; i < size; i++) {
          if (list[i].key < new_min) {
            new_min = list[i].key;
            new_min_pos = i;
          }
        }

        // Move new minimum to minimum slot
        list[0] = list[new_min_pos];

        // Move final element into gap
        --size;
        if (new_min_pos != size)
          list[new_min_pos] = list[size];
      }
      return true;
    } else if (CAPACITY >= 2 && size >= 2 && k == list[1].key) {
      // Edge case 2: Removing maximum
      v = list[1].val;

      // Edge case 2A: Maximum position will be vacant
      if (size == 2) {
        size = 1;
      } else {
        // Edge case 2B: Need to find new maximum
        K new_max = list[2].key;
        size_t new_max_pos = 2;
        for (size_t i = 3; i < size; i++) {
          if (list[i].key > new_max) {
            new_max = list[i].key;
            new_max_pos = i;
          }
        }

        // Move new maximum to maximum slot
        list[1] = list[new_max_pos];

        // Move final element into gap
        --size;
        if (new_max_pos != size)
          list[new_max_pos] = list[size];
      }
      return true;
    }

    // Normal case: Removing neither maximum nor minimum.
    size_t pos = 0;

    if (!find(k, pos)) {
      // Didn't exist
      return false;
    }

    v = list[pos].val;

    // Move the last element into the gap left by this removal
    // (unless removed element was the last element)
    --size;
    if (pos != size) {
      list[pos] = list[size];
    }

    return true;
  }

  /// As above, but caller does not care about value.
  bool remove(K const &k) {
    V _;
    return remove(k, _);
  }

  /// Find a given key in the list.
  /// Return false if not found.
  bool contains(K const &k, V &v) {
    size_t pos = 0;

    bool const found = find(k, pos);
    if (found)
      v = list[pos].val.load();
    return found;
  }

  /// As above, but caller doesn't care about found value.
  bool contains(K const &k) {
    size_t _ = k; // Dummy argument
    return find(k, _);
  }

  /// Return the minimum key.
  /// CAVEAT: If vector is empty, may return junk data!
  [[nodiscard]] K first() const { return list[0].key; }

  /// Return the last key via the parameter k.
  /// Do nothing and return false if empty.
  bool last(K &k) {
    if (size >= 2) {
      k = list[1].key;
      return true;
    }

    if (size == 1) {
      k = list[0].key;
      return true;
    }

    return false;
  }

  /// Find the biggest key that is Less Than or Equal to sought_k (hence "lte").
  /// The found key is assigned to found_k, and the value is assigned to v.
  /// Returns false if there is no such element.
  bool find_lte(K const &sought_k, K &found_k, V &v) {
    size_t found_pos = CAPACITY;

    // Search until a single element <= sought_k is found
    size_t i = 0;
    for (; i < size; i++) {
      if (list[i].key <= sought_k) {
        found_pos = i;
        found_k = list[i].key;
        break;
      }
    }

    // Search rest of array for maximum element <= sought_k
    for (; i < size; i++) {
      if (found_k < list[i].key && list[i].key <= sought_k) {
        found_pos = i;
        found_k = list[i].key;
      }
    }

    // If one was found,
    // return the found key and value via parameter refs and return true.
    bool const found = found_pos < CAPACITY;
    if (found)
      v = list[found_pos].val;
    return found;
  }

  /// As above, but caller doesn't care about the found k
  bool find_lte(K const &sought_k, V &v) {
    K _ = sought_k;
    return find_lte(sought_k, _, v);
  }

  /// Consume another vector_umfra, stealing all of its elements. This
  /// method assumes the other vector's minimum element > this vector's maximum
  /// element.
  void merge(vector_umfra *victim) {
    // NOTE: Implemented slightly inefficiently.
    // Has a few swaps more than an optimal solution, but is much simpler.

    if (victim->size == 0)
      // Case 1: victim has no elements to steal.
      return;

    // Check against overfull
    assert(size + victim->size <= CAPACITY);

    if (CAPACITY == 1 || size == 0) {
      // Case 2: we are empty. Copy the entire victim vector.
      assert(size == 0);

      asm volatile("" ::: "memory"); // ensure everything is flushed to RAM
      std::memcpy(list, victim->list, victim->size * ENTRY_SIZE);
      asm volatile("" ::: "memory");
      size = victim->size.load();
      victim->size = 0;
      return;
    }

    // Case 3: Victim has exactly one element to steal.
    // It becomes our new maximum.
    if (victim->size == 1) {
      // Move our max element to end
      list[size] = list[1];

      // Move victim's sole element to our max position
      list[1] = victim->list[0];

      victim->size = 0;
      ++size;
      return;
    }

    // General case: Due to the edge cases ruled out above, we know we are
    // stealing at least two elements, and so victim's max must become curr's
    // max. curr has at least one element, and thus its minimum position is
    // filled.

    // Steal victim's minimum.
    list[size] = victim->list[0];
    ++size;

    if (size >= 2)
      // We have at least two elements,
      // so our maximum must be displaced to make room.
      list[size] = list[1];

    // victim's maximum becomes our new maximum.
    list[1] = victim->list[1];

    ++size;

    // Copy all elements from victim other than min and max.
    size_t const elements_to_steal = victim->size - 2;
    asm volatile("" ::: "memory"); // ensure everything is flushed to RAM
    std::memcpy(list + size, victim->list + 2, elements_to_steal * ENTRY_SIZE);
    asm volatile("" ::: "memory");
    victim->size = 0;
    size += elements_to_steal;
    return;
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
    using std::cout;
    using std::endl;

    // Assert size does not exceed CAPACITY
    if (size > CAPACITY) {
      cout << "Somehow, size of " << size << " has exceeded capacity of "
           << CAPACITY << endl;
      return fail();
    }

    // The following checks are nonsensical for a vector of capacity 1, so...
    if (CAPACITY == 1)
      return true;

    // Check for duplicated keys
    for (size_t i = 1; i < size; i++) {
      for (size_t j = 0; j < i; j++) {
        if (list[i].key.load() == list[j].key.load()) {
          cout << "Found duplicated key " << list[i].key << " at indices " << i
               << "," << j << endl;

          return fail();
        }
      }
    }

    // Verify max > min.
    if (size >= 2 && list[1].key < list[0].key) {
      cout << "min > max: " << list[0].key << "," << list[1].key << endl;
      return fail();
    }

    // Verify elements in range [2:size-1] are between max and min.
    for (size_t i = 2; i < size; i++) {
      // Verify element is not > max or < min
      if (list[i].key < list[0].key || list[i].key > list[1].key) {
        cout << "Found key " << list[i].key << " at index " << i
             << " not between min " << list[0].key << " and max " << list[1].key
             << endl;
        return fail();
      }
    }

    return true;
  }

  /// Sort this vector map (EXCEPT for max element.)
  /// Only to be used in total isolation.
  void sort() {
    // No-op if vector is too small to possibly be unsorted.
    // Checking both CAPACITY and size is redundant, but because CAPACITY is a
    // templated parameter, that check will be optimized out by the compiler.
    if (CAPACITY > 3 && size > 3)
      // By the invariant, slot 0 contains the minimum element, and slot 1
      // contains the maximum element, so they are excluded from the sort.
      std::sort(list + 2, list + size);
  }

  /// Return the key at a given order in the vector.
  /// Note: Only works if vector is sorted! Call sort() first!
  /// @throws out_of_range
  K at(size_t index) {
    if (index >= size)
      // Error case: caller wants out-of-bounds element.
      throw std::out_of_range("at(" + std::to_string(index) + ")");

    if (index == 0)
      // Case 1: caller wants minimum element.
      return list[0].key;
    else if (index == size - 1)
      // Case 2: caller wants maximum element.
      return list[1].key;

    // Common case: caller wants element between min and max.
    // Offset by 1 to account for max at position 1.
    return list[index + 1].key;
  }

  void verbose_analysis() const {
    using std::cout;

    cout << "[";

    // Print first element
    if (size > 0)
      cout << +list[0].key;

    // Print last element if distinct from first element
    if (size > 1)
      cout << "-" << +list[1].key;

    // Print size/capacity
    cout << "](" << size << "/" << CAPACITY << ")" << std::endl;
  }

  // Get the maximum key
  [[nodiscard]] K max_key() const {
    if (size == 0)
      throw std::out_of_range(
          "max_key(): maximum of empty vector is undefined");

    if (size == 1)
      return list[0].key;

    return list[1].key;
  }

  void dump() const {
    using std::cout;

    // Print all elements
    cout << "[ ";
    for (size_t i = 0; i < size; i++)
      cout << +list[i].key << " ";

    // Print size/capacity
    cout << "](" << size << "/" << CAPACITY << ")" << std::endl;
  }

  [[nodiscard]] std::string_view get_name() const {
    return "Unsorted Vector, Relaxed Atomic";
  }
  [[nodiscard]] size_t get_capacity() const { return CAPACITY; }
  [[nodiscard]] size_t get_size() const { return size; }

  /// Process an element for a range or for_each operation.
  void iterate_element(std::function<void(const K &, V &, bool &)> f, int i,
                       bool &exit_flag) {
    // NB: same note as sfra for_each()
    V v = list[i].val.load();
    f(list[i].key.load(), v, exit_flag);
    list[i].val.store(v);
  }

  // Process an element for a range operation,
  // when it is unknown if the element is less than from.
  bool iterate_element_range1(std::function<void(const K &, V &, bool &)> f,
                              int i, K const &from, K const &to,
                              bool &exit_flag) {
    // Skip elements below from.
    if (list[i].key < from)
      return false;

    // If we find an element in excess of to,
    // set exit_flag to end the iteration.
    if (list[i].key > to) {
      exit_flag = true;
      return true;
    }

    // Element is neither below from nor above to, so process it.
    iterate_element(f, i, exit_flag);

    // If we process to itself, set exit_flag.
    if (list[i].key == to)
      exit_flag = true;

    return true;
  }

  // Process an element for a range operation,
  // when it is known that the element exceeds from.
  void iterate_element_range2(std::function<void(const K &, V &, bool &)> f,
                              int i, K const &to, bool &exit_flag) {
    // If we find an element in excess of to,
    // set exit_flag to end the iteration.
    if (list[i].key > to)
      exit_flag = true;

    // Element is neither below from nor above to, so process it.
    iterate_element(f, i, exit_flag);

    // If we process to itself, set exit_flag.
    if (list[i].key == to)
      exit_flag = true;
  }

  /// Apply a function f() to all key/value pairs in this vector.
  /// NOTE: As this vector is unsorted, elements are not processed in order.
  void for_each(std::function<void(const K &, V &, bool &)> f,
                bool &exit_flag) {

    // The easiest way to process the elements in order is to sort them.
    sort();

    // Process first element.
    if (size > 0 && !exit_flag)
      iterate_element(f, 0, exit_flag);

    // Process sorted elements.
    for (size_t i = 2; i < size && !exit_flag; ++i)
      iterate_element(f, i, exit_flag);

    // Process maximum element.
    if (size > 1 && !exit_flag)
      iterate_element(f, 1, exit_flag);
  }

  /// Apply a function f() to all key/value pairs in this vector.
  void for_each(std::function<void(const K &, V &, bool &)> f) {
    bool exit_flag = false;
    for_each(f, exit_flag);
  }

  /// Apply a function f() to all key/value pairs in the intersection of this
  /// vector and the given range [from, to].
  /// Returns true if exit_flag is set or end of range is reached, else false.
  /// NOTE: As this vector is unsorted, elements are not processed in order.
  bool range(K const &from, K const &to,
             std::function<void(const K &, V &, bool &)> f, bool &exit_flag) {

    bool from_reached = false;

    // The easiest way to process the elements in order is to sort them.
    sort();

    // Process minimum element.
    if (size > 0 && !exit_flag)
      from_reached = iterate_element_range1(f, 0, from, to, exit_flag);

    // Process sorted elements.
    size_t i = 2;
    for (; i < size && !exit_flag && !from_reached; ++i)
      from_reached = iterate_element_range1(f, i, from, to, exit_flag);

    for (; i < size && !exit_flag; ++i)
      iterate_element_range2(f, i, to, exit_flag);

    // Process maximum element.
    if (size > 1 && !exit_flag)
      // NB: It is not guaranteed that from_reached is set at this point,
      // so we use iterate_element_range1 instead of iterate_element_range2.
      iterate_element_range1(f, 1, from, to, exit_flag);

    return exit_flag;
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
