#pragma once

#include <cassert>
#include <cstring>
#include <functional>
#include <iostream>
#include <stdexcept>
#include <vector>

#include "../common/config.h"
#include "../common/rlx_atomic.h"
#include "../lock/loop_counter.h"
#include "atomic_kv.h"
#include "insert_result.h"

/// A data structure which uses an unsorted vector to implement a multikey queue
/// interface.
///
/// Has three phases. The first phase is FAA insertion, in which FAA operations
/// are performed on a counter to quickly reserve insertion slots in a way that
/// minimizes contention. Once this counter reaches FAA_CAPACITY or this node is
/// otherwise split, we transition to the CAS insertion phase, in which a
/// different counter is CAS'd to reserve insertion slots and also track a
/// version number. Finally, once this node reaches the head of the data layer,
/// it is extracted from the data structure and put into an extraction phase.
///
/// Insertions and extractions may only occur during their respective phases.
/// This is only suitable for use in the data layer of a priority queue--it is
/// inapplicable to a map, and cannot be used in the index layer.
////
/// INVARIANT: The first element inserted is the minimum. Insertions below the
/// minimum are not accepted. The only way the minimum can change is in the
/// LOCKED_EXCLUSIVE state, and the minimum must be moved to slot 0.
///
/// @param ENTRY    - The type representing k/v entrys.
/// @param CAPACITY - The total number of key/value entrys that can be held.
template <typename ENTRY, size_t CAPACITY> class multivector_asr {

  using K = typename ENTRY::KEY_TYPE;
  using V = typename ENTRY::VAL_TYPE;
  static constexpr K EMPTY = ENTRY::EMPTY;

  using entry_t = atomic_kv<ENTRY, std::atomic, rlx_atomic>;

  /// List of key/value entrys.
  /// We use a C-style array here so we can "cheat" and use memcpy().
  entry_t list[CAPACITY];

  // This counter is used during the FAA insertion phase, described above.
  // If this counter is less than or equal to FAA_CAPACITY, we are in the FAA
  // insertion phase, and this counter represents the number of reserved
  // insertions. If this counter is greater than FAA_CAPACITY, we are no
  // longer in the FAA insertion phase, and this counter's value is meaningless
  // apart from indicating that fact.
  std::atomic<size_t> faa_counter;

  // This counter is used during the CAS insertion phase, described above.
  // During the FAA insertion phase, this counter is not used. Afterwards, it
  // represents multiple values packed into one. The bottom 32 bits are used to
  // represent the number of reserved insertions, like with the FAA version of
  // the counter. The next bit represents whether the lock is held in exclusive
  // mode by some thread or not. The top 31 bits represent a version number. If
  // set to the special value 0xFFFFFFFFFFFFFFFF, the vector is permanently
  // transitioned to the extraction state.
  // The number of reserved insertions IS ALLOWED to exceed CAPACITY. If it
  // does, that means some thread has "reserved" more slots than actually exist;
  // that thread must then make sure to split the node.
  std::atomic<size_t> cas_counter;

  // The number of elements that were in the data structure when the phase
  // changed from insertion to extraction. Represents the total number of
  // elements that may be extracted during the extraction phase.
  rlx_atomic<size_t> elements_at_phase_change;

  // The counter of how many extractions have been reserved.
  std::atomic<size_t> extraction_counter;

  // A mask for the bits that represent the insertion counter.
  static constexpr size_t INSERT_MASK = 0x00000000FFFFFFFFULL;

  // A mask for the bit that represents the lock.
  static constexpr size_t LOCK_BIT = 0x0000000100000000ULL;

  // A mask for the bits that represent the version number counter.
  static constexpr size_t VERSION_MASK = 0xFFFFFFFE00000000ULL;

  // The lowest bit which represents versions.
  static constexpr size_t VERSION_LOWEST_BIT = 0x0000000200000000ULL;

  // A special value for the counter that represents the extraction state.
  static constexpr size_t EXTRACTION_STATE_VALUE = 0xFFFFFFFFFFFFFFFFULL;

  // A special value that can be returned by reserve_for_insertion() that
  // indicates that the reservation failed because this object is in
  // extract-only mode.
  static constexpr size_t EXTRACT_ONLY = 0xFFFFFFFFFFFFFFFFULL;

  // A special value that can be returned by reserve_for_insertion() that
  // indicates that the reservation failed because the version number increased
  // past the provided value.
  static constexpr size_t VERSION_CHECK_FAIL = 0xFFFFFFFFFFFFFFFEULL;

  // This variable indicates how many slots may be reserved by FAA insertion.
  // Performing a partition and splitting 50/50 takes a long time and we want to
  // minimize how long we keep the tail locked.
  // Thus, if CAPACITY = TARGET_SIZE*2, we permit TARGET_SIZE+1 elements in
  // FAA insertion mode, so that when we split, we just take one element, the
  // maximum, leaving this node with TARGET_SIZE elements afterwards. Finding
  // the maximum is much faster partitioning for a 50/50 split, and leaves more
  // room in the new tail for more fast FAA insertions before it needs to be
  // split.
  static constexpr int FAA_CAPACITY = (CAPACITY / 2) + 1;

  enum state_t { FAA_INSERTION, CAS_INSERTION, LOCKED, EXTRACTION };

  static state_t get_state(size_t counter) {
    if (counter == 0)
      return FAA_INSERTION;
    if (counter == EXTRACTION_STATE_VALUE)
      return EXTRACTION;
    if ((counter & LOCK_BIT) == LOCK_BIT)
      return LOCKED;

    return CAS_INSERTION;
  }

  static inline bool is_insertion_state(state_t state) {
    return state == CAS_INSERTION || state == FAA_INSERTION;
  }

  static inline bool is_insertion_state(size_t counter) {
    return is_insertion_state(get_state(counter));
  }

  // Extract the number of reserved insertions from an insertion counter.
  // Is permitted to exceed CAPACITY.
  static inline size_t get_insert_counter(size_t counter) {
    return counter & INSERT_MASK;
  }

  // Extract the actual number of valid reserved insertions from an insertion
  // counter. If it exceeds CAPACITY, return CAPACITY.
  static inline size_t get_valid_reservations(size_t counter) {
    size_t const result = get_insert_counter(counter);
    return result > CAPACITY ? CAPACITY : result;
  }

  // Extract the actual number of reserved insertions from an insertion counter.
  static inline size_t get_version_counter(size_t counter) {
    return counter / VERSION_LOWEST_BIT;
  }

  // Change the actual number of insertions
  static inline void set_insert_counter(size_t &counter, size_t count) {
    assert(count <= INSERT_MASK);
    counter = counter & ~INSERT_MASK; // Clears all the insert bits
    counter += count;
  }

  /// Wait for all reserved insertions to complete. If we want to do something
  /// that will operate on all of the elements, we must do this first.
  void wait_for_insertions(size_t reservations) {
    for (size_t i = 0; i < reservations; ++i) {
      // Spin until the key in slot i is not EMPTY.
      while (list[i].key.load() == EMPTY)
        ;
    }
  }

  /// Reserve slots for insertion. Increment the ingress counter,
  /// and return the value. Returns a unique reservation ID if successful,
  /// or -1 if the insert cannot proceed.
  /// If version_number is negative, ignore it.
  size_t reserve_for_insertion(int64_t &expected_version, size_t elts) {

    // Attempt to reserve a slot by incrementing the FAA insertion counter.
    // By incrementing the counter from n to n+1, slot n is reserved.
    size_t const read_faa = faa_counter.fetch_add(elts);

    // If this vector is in FAA insertion mode, we know it is the tail.
    if (read_faa < FAA_CAPACITY) {
      // We reserved at least one slot using the FAA insertion counter.
      // Fast path succeeded!
      assert(expected_version <= 0);

      // If caller provided the special value -1 for the version number, this
      // assignment informs the caller that the version number is 0, hence,
      // we're in the FAA insertion state; if the caller provided 0, this is a
      // no-op. (This path isn't reachable otherwise.)
      expected_version = 0;
      return read_faa;
    }

    // If faa_insertion_counter >= FAA_CAPACITY,
    // we must use cas_insertion_counter instead.
    loop_counter ctr;
    while (true) {
      ctr.count();

      // Read and unpack cas_insertion_counter.
      size_t read_val = cas_counter.load();
      state_t const state = get_state(read_val);
      size_t const read_version = get_version_counter(read_val);
      size_t const insertions = get_insert_counter(read_val);

      switch (state) {
      case EXTRACTION:
        // The data structure is in extract-only mode. This vector will never
        // permit an insert again. So, return special value EXTRACT_ONLY.
        return EXTRACT_ONLY;

      case LOCKED:
        // If the data structure is locked, then spin until it is unlocked.
        continue;

      case FAA_INSERTION:
        // The FAA insertion counter is over FAA_CAPACITY, but the CAS insertion
        // counter is uninitialized. We return read_faa, which is >=
        // FAA_CAPACITY. The caller then determines whether or not they have
        // inherited the responsibility to split (true if read_faa ==
        // FAA_CAPACITY, false if it is > FAA_CAPACITY.)
        expected_version = 0;
        return read_faa;

      case CAS_INSERTION:
        // If expected_version is specified, check to make sure the read version
        // number matches it. If not, return a special value indicating a
        // version check failure.
        if (expected_version >= 0 &&
            read_version != static_cast<size_t>(expected_version))
          return VERSION_CHECK_FAIL;

        // If insertions is strictly greater than CAPACITY, then all slots are
        // reserved, AND some thread has assumed responsibility for splitting.
        // Return a value over CAPACITY to indicate that we are waiting on
        // another thread to split.
        if (insertions > CAPACITY) {
          expected_version = static_cast<int64_t>(read_version);
          return insertions;
        }

        // This assert trips if the +1 below would make it overflow the
        // designated bits for insertions.
        assert((read_val & INSERT_MASK) != INSERT_MASK);

        // Otherwise, the insert can proceed normally.
        if (cas_counter.compare_exchange_weak(read_val, read_val + elts)) {
          expected_version = static_cast<int64_t>(read_version);
          return insertions;
        }

        break;

      default:
        throw std::logic_error("Unrecognized state");
      }
    }
  }

  void initialize() {
    // Do not allow synthesis of vector with zero or negative capacity
    static_assert(CAPACITY >= 1);

    for (size_t i = 0; i < CAPACITY; ++i)
      list[i].key = EMPTY;

    faa_counter = 0;
    cas_counter = 0;
    elements_at_phase_change = 0;
    extraction_counter = 0;
  }

  // Swap key and value at indices n and m. Should only be called when locked
  // exclusive, and verified insertions have completed on both slots.
  inline void swap(int n, int m) {
    if (n != m)
      list[n].swap(list[m]);
  }

  // Three-way partition function.
  // From "A Discipline of Programming" by Edsger Dijkstra.
  // Implemented based on the pseudocode at:
  // https://en.wikipedia.org/wiki/Dutch_national_flag_problem
  // Before returning, sets lo to the index of the first key >= pivot,
  // and hi to the index of the last key <= pivot.
  void partition3(int &lo, int &hi) {
    // There are four regions.
    // "start" and "end" represent the initial values of lo and hi.
    // [start, lo) is keys less than the pivot (Region L for less.)
    // [lo, u) is keys equal to the pivot. (Region E for equal.)
    // [u, hi] is unsorted region of the array. (Region U for unsorted.)
    // (hi, end] is keys greater than the pivot. (Region G for greater.)

    // Initially, the entire region is unsorted.
    int u = lo;

    // Choose the element in the middle as the pivot. This works well when the
    // vector is already mostly sorted, which is likely for this use case.
    K const pivot_k = list[(hi + lo) / 2].key;

    // The loop continues so long as U's size is > 0.
    while (u <= hi) {
      K const ku = list[u].key;
      if (ku < pivot_k) {
        // Is the first element of U less than the pivot?
        // Move it to the end of L. (E gets shifted by 1.)
        swap(lo++, u++);
      } else if (ku > pivot_k) {
        // Is the first element of U greater than the pivot?
        // Move it to the start of G.
        // But first, see if G can be expanded without performing any swaps.
        while (hi >= u && list[hi].key > pivot_k)
          --hi;

        if (u > hi)
          // Edge case: the loop above may have caused u and hi to cross.
          // If so, we're done!
          return;

        swap(u, hi--);
      } else {
        // Is the first element of U equal to the pivot?
        // Leave it where it is.
        ++u;
      }
    }
  }

  // Returns the k-th smallest element of list within left..right inclusive
  // (i.e. left <= k <= right).
  // Because this vector supports duplicate keys, this quickselect is based on a
  // three-way partition instead of a two-way partition.
  // NB: This implementation has a special consideration: if the element in slot
  // 0 is the min, it must not be moved. std::nth_element() does not do this, so
  // it cannot be used here.
  void quickselect(int const left, int const right, int const k) {
    assert(right >= k);
    assert(k >= left);
    assert(left >= 0);

    // If the list contains only one element, return that element.
    if (left == right)
      return;

    int lo = left;
    int hi = right;
    partition3(lo, hi);

    // The next step depends on which of the three regions index k happens to be
    // in after the three-way partition.
    if (lo <= k && k <= hi)
      // Region E: we're done.
      return;

    if (k < lo)
      // Region L: Recurse on region L.
      quickselect(left, lo - 1, k);
    else
      // Region R: Recurse on region R.
      quickselect(hi + 1, right, k);
  }

  // Performs a quickselect to take the median.
  void quickselect_median(int size) {
    if (CAPACITY >= 2 && size >= 2)
      // Quickselect for the median element.
      quickselect(0, size - 1, size / 2);
  }

public:
  using KEY_TYPE = K;
  using VAL_TYPE = V;

  /// Indicates whether this vector class permits concurrent insertions.
  static constexpr bool SUPPORTS_CONCURRENT_INSERT = true;

  /// Default constructor. Initializes EMPTY to the default value, -1.
  multivector_asr() { initialize(); }

  /// Benchmark constructor. Initializes EMPTY to the default value, -1.
  /// config argument is ignored, but its presence helps compatibility.
  explicit multivector_asr(config *) { initialize(); }

  /// Destructor
  ~multivector_asr() = default;

  /// Populate by stealing elements from another chunk.
  /// Insert (k,v) as the first element, and then take all entries STRICTLY
  /// greater than k from a given other vector.
  /// This method overwrites the contents of the current vector with the
  /// stolen elements; it is assumed it is called when the current vector is
  /// empty. Returns true if successful; returns false if it cannot be done as
  /// it would make this vector too full.
  /// This vector will end up in the FAA insertion state or CAS insertion state
  /// depending on how many elements it steals.
  bool split_insert(multivector_asr *victim, ENTRY const &entry) {
    K const &k = entry.get_k();
    assert(k != EMPTY); // insertion of EMPTY is not allowed
    assert(EMPTY == victim->EMPTY);

    size_t victim_counter = victim->cas_counter.load();
    assert(get_state(victim_counter) == LOCKED);
    size_t victim_size = get_valid_reservations(victim_counter);

    assert(victim_size <= CAPACITY);

    victim->wait_for_insertions(victim_size);

    // First, determine the needed capacity.
    size_t elements_to_steal = 0;
    for (size_t i = 0; i < victim_size; ++i)
      if (victim->list[i].key > k)
        // Count elements to steal
        ++elements_to_steal;

    assert(elements_to_steal < CAPACITY);

    // Initialize the first entry.
    list[0] = entry;
    size_t my_size = 1;

    // Edge case: there are no elements to be stolen from victim.
    // NB: If the caller respects the invariant that this method is never
    // invoked when it would result in this vector becoming overfull,
    // and capacity is 1, then there can not be any elements to steal.
    if (CAPACITY == 1 || elements_to_steal == 0) {
      // We're already done!
      faa_counter.store(1);
      return true;
    }

    // Steal all the elements > k from the victim vector.
    // This loop carefully avoids moving any element more than once.
    size_t i = 0;
    while (i < victim_size) {
      if (victim->list[i].key <= k) {
        // Skip over any elements that are <= k.
        ++i;

      } else if (victim->list[victim_size - 1].key > k) {
        // Move any elements > k from the end of the victim.
        --victim_size;
        list[my_size].rob(victim->list[victim_size]);
        ++my_size;

      } else {
        // At this point we know victim[i] > k, victim[size - 1] <= k, and
        // i != size - 1.  Thus, move victim[i] to this vector,
        // and victim[size - 1] to victim[i].
        list[my_size] = victim->list[i];
        ++my_size;
        --victim_size;
        victim->list[i].rob(victim->list[victim_size]);
        ++i;
      }
    }

    if (my_size <= FAA_CAPACITY) {
      // If we have enough elements to start in the FAA insertion phase, do so.
      faa_counter.store(my_size);
      verify_locked(my_size);
    } else {
      // If we have so many elements we must start in the CAS phase, do so.
      faa_counter.store(FAA_CAPACITY + 1);
      size_t my_counter = VERSION_LOWEST_BIT;
      set_insert_counter(my_counter, my_size);
      cas_counter.store(my_counter);
      verify_locked(my_size);
    }

    set_insert_counter(victim_counter, victim_size);
    victim->cas_counter.store(victim_counter);
    victim->verify_locked(victim_size);
    return true;
  }

  /// Populate by stealing the maximum element from another chunk. This method
  /// overwrites the contents of the current vector with the stolen element; it
  /// is assumed it is called when the current vector is empty.
  void steal_max(multivector_asr *victim) {
    size_t victim_counter = victim->cas_counter.load();
    assert(get_state(victim_counter) == LOCKED);
    assert(EMPTY == victim->EMPTY);

    size_t victim_size = get_valid_reservations(victim_counter);
    assert(victim_size > 0);
    assert(victim_size <= CAPACITY);

    // Make sure all interleaving inserts into victim are complete, and also,
    // scan for maximum element's location.
    size_t max_location = victim_size;
    K max_key = EMPTY;

    for (size_t i = 0; i < victim_size; ++i) {
      // Spin until the key in slot i is not EMPTY.
      K read_key;
      do {
        read_key = victim->list[i].key.load();
      } while (read_key == EMPTY);

      if (max_key == EMPTY || read_key >= max_key) {
        max_key = read_key;
        max_location = i;
      }
    }

    // Move the element over.
    list[0].rob(victim->list[max_location]);

    // Restore compactness to victim.
    size_t const last_location = victim_size - 1;
    if (max_location != last_location) {
      victim->list[max_location].rob(victim->list[last_location]);
    }

    // At this point, our min is correct, as it is the only element.
    // As for victim's min, there are two possibilities.
    // (1) The victim had at least two elements, thus the min and max are
    // distinct, and thus we didn't disturb its minimum.
    // (2) The victim had one element, and we took it, and thus it is empty and
    // has no need to respect the minimum invariant.
    // Either way, there is nothing left to do.

    faa_counter.store(1);
    --victim_size;
    set_insert_counter(victim_counter, victim_size);
    victim->cas_counter.store(victim_counter);
    verify_locked(1);
    victim->verify_locked(victim_size);
  }

  /// Populate by stealing the latter half of the elements from another chunk.
  /// If victim starts with n elements, victim will end with floor(n/2)
  /// elements, and new vector will end with ceil(n/2) elements.
  /// This method overwrites the contents of the current vector with the
  /// stolen elements; it is assumed it is called when the current vector is
  /// empty.
  void steal_half(multivector_asr *victim) {
    size_t victim_counter = victim->cas_counter.load();
    assert(get_state(victim_counter) == LOCKED);
    assert(EMPTY == victim->EMPTY);

    size_t victim_size = get_valid_reservations(victim_counter);
    victim->wait_for_insertions(victim_size);
    victim->quickselect_median(victim_size);

    assert(victim_size <= CAPACITY);

    size_t const median_pos = victim_size / 2;

    // Steal the median and all elements that follow.
    size_t const first_to_steal = median_pos;

    // Copy elements from victim.
    size_t const entries_to_steal = victim_size - first_to_steal;

    for (size_t i = 0; i < entries_to_steal; ++i)
      list[i].rob(victim->list[first_to_steal + i]);

    // Correct the sizes of the two vectors.
    size_t const my_size = entries_to_steal;
    victim_size = first_to_steal;

    // At this point, our min is correct, as it is the median.
    // As for victim's min, there are two possibilities.
    // (1) The minimum was correct before the method and is still correct now,
    // as it was never disturbed. (quickselect() won't move it if it's correct.)
    // (2) The victim is the data head node, so it does not respect the
    // slot-0-is-minimum invariant, and so there is no need to restore its
    // minimum.
    // Either way, there is nothing left to do.

    faa_counter.store(my_size);
    set_insert_counter(victim_counter, victim_size);
    victim->cas_counter.store(victim_counter);
    verify_locked(my_size);
    victim->verify_locked(victim_size);
  }

  /// Insert a new element into the vector.
  insert_result insert(ENTRY const &entry) {
    const ENTRY *b = &entry; // Treat entry as an array of one element.
    const ENTRY *e = b + 1;  // Pointer to one-past-end of array is OK.
    return insert(b, e);     // Delegate to batch-insertion method.
  }

  /// Try to insert elements in the range [b, e).
  /// b will be incremented past every inserted element, thus informing the
  /// caller how many elements were inserted.
  template <typename Iter> insert_result insert(Iter &b, Iter const e) {
    // Delegate to the concurrent insert() method with the version number set to
    // the special value -1, indicating that the version check should be elided.
    return insert_concurrent(b, e, -1);
  }

  /// Try to insert elements in the range [b, e).
  /// b will be incremented past every inserted element, thus informing the
  /// caller how many elements were inserted.
  /// The caller provides the version number they expect us to be in; if we have
  /// updated since then, return VERSION_CHECK_FAIL. The special value -1 can be
  /// used to indicate the caller does not care what the version number is and
  /// wants the insertion to proceed regardless.
  template <typename Iter>
  insert_result insert_concurrent(Iter &b, Iter const e,
                                  int64_t version_number) {
    size_t const to_insert = std::distance(b, e);

    size_t slot = reserve_for_insertion(version_number, to_insert);

    // This variable tells us how many slots are available to us. If the version
    // number is 0, then we're in FAA insertion mode, and thus limit is
    // determined by FAA_CAPACITY. Otherwise, we're in CAS insertion mode, and
    // the limit is set by CAPACITY. (If the caller provides the special value
    // -1 for the version number, then reserve_for_insertion() will initialize
    // this variable for us!)
    size_t const limit = (version_number == 0) ? FAA_CAPACITY : CAPACITY;

    if (slot == EXTRACT_ONLY)
      return DEAD_VECTOR;
    if (slot == VERSION_CHECK_FAIL)
      return VERSION_CHANGED;
    if (slot > limit)
      return SPLIT_PENDING;

    // Assert that it is not smaller than the minimum, which would violate the
    // invariant. (If this insert races with the insert of the minimum and it
    // is not set yet, this assert might fail to catch an invariant
    // violation.)

    // assert(list[0].key == EMPTY || k >= list[0].key);

    // NB: The assert above fails, because the leftmost vector IS permitted to
    // insert keys less than the minimum. Luckily, the minimum of the leftmost
    // node does not matter. For now, let's just skip the assert and see if it
    // still works.

    while (std::distance(b, e) > 0 && slot < limit) {
      // NB: The overloaded assignment operator writes val first, key second.
      // This is necessary for this class's correctness!
      ENTRY const &entry = *b;
      assert(entry.get_k() != EMPTY); // insertion of EMPTY is not allowed
      list[slot] = entry;
      ++b;
      ++slot;
    }

    assert(b <= e); // Check against overshoot

    if (b != e) {
      // If we filled the vector to capacity and there are still elements to
      // insert, we must split the vector to insert further elements.
      return MUST_SPLIT;
    }

    // If b == e, all elements were inserted with no issue.
    // (If we fill it to capacity exactly, it's the next guy's problem.)
    return SUCCESS;
  }

  // Currently, this implementation does not offer a faster sequential variant
  // of insert. It just uses the concurrent one.
  bool insert_seq(ENTRY const &entry) { return insert(entry); }

  /// Switch from insertion to extraction phase.
  /// Return true if successful, false if failed (already in extraction
  /// phase.)
  void die() {
    size_t const read_val = cas_counter.load();
    assert(get_state(read_val) == LOCKED);
    cas_counter.store(EXTRACTION_STATE_VALUE);

    // Determine the number of valid insertion reservations
    // that occured before the phase transition.
    elements_at_phase_change = get_valid_reservations(read_val);
  }

  /// We sacrifice ordering for the sake of parallelism, and thus extract an
  /// abritrary element instead of min. Returns false if no elements are
  /// available. However, we DO guarantee that the first-extracted element is
  /// the minimum.
  bool extract(ENTRY &entry) {
    size_t const slot = extraction_counter++;

    if (slot >= elements_at_phase_change)
      // There are no more elements to take.
      return false;

    // There is a SLIGHT possibility we are racing with an unfinished insert.
    // Spin until the slot is not EMPTY.
    loop_counter ctr;
    while (list[slot].key == EMPTY)
      ctr.count();

    entry = list[slot].unwrap();
    return true;
  }

  /// Return the minimum key. This method assumes this is not the head node,
  /// and the min-in-slot-0 invariant is respected. CAVEAT: If vector is
  /// empty, may return junk data!
  [[nodiscard]] K first() const { return list[0].key; }

  // Return true if ANY key in this multivector is <= k.
  // Assumes this is called only when lock is held,
  // and when CAPACITY is totally full.
  [[nodiscard]] bool any_lte(K const &k) const {
    for (size_t i = 0; i < CAPACITY; ++i) {
      // Spin until the key in slot i is not EMPTY.
      K read_key;
      do {
        read_key = list[i].key.load();
      } while (read_key == EMPTY);

      if (read_key <= k)
        return true;
    }

    return false;
  }

  /// In debug mode, dump the state of the data structure and throw an error
  /// to end immediately. In "release" mode, no op.
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

    // If verify() is called, we assume the data structure is quiescent.
    // Therefore, we can run stricter verification than if we assumed other
    // threads were running on this object.

    size_t const read_ic = cas_counter.load();
    state_t const state = get_state(read_ic);
    size_t const size = get_size();

    if (is_insertion_state(state)) {
      size_t i = 0;
      if (size > 0) {
        // The minimum exists, so check it.
        K const min = list[0].key.load();
        if (min == EMPTY) {
          cout << "VQ Verification: minimum reserved but empty" << endl;
          return fail();
        }

        // For all other keys, make sure they are inserted,
        // and are >= the minimum
        for (i = 1; i < size; ++i) {
          K const curr = list[i].key.load();
          if (curr == EMPTY) {
            cout << "VQ Verification: slot " << i << " reserved but empty"
                 << endl;
            return fail();
          }
          // Note: we allow this invariant to be violated by the leftmost data
          // node.
          // else if (curr < min) {
          //   cout << "VQ Verification: slot " << i
          //             << " not less than minimum" << endl;
          //   return fail();
          // }
        }
      }

      // Make sure all keys through the end of the list are EMPTY.
      for (; i < CAPACITY; ++i) {
        K const curr = list[i].key.load();
        if (curr != EMPTY) {
          cout << "VQ Verification: slot " << i << " occupied but not reserved"
               << endl;
          return fail();
        }
      }

    } else if (state == EXTRACTION) {
      // Can't think of any way things can go wrong during the extraction phase
    } else if (state == LOCKED) {
      // We shouldn't be in the locked state at verify time...
      cout << "VQ Verification: unexpectedly in locked state: " << endl;
      return fail();
    } else {
      // If state is not equal to a valid state, something is very wrong...
      cout << "VQ Verification: unexpectedly in invalid state: " << +state
           << endl;
      return fail();
    }

    return true;
  }

  bool verify_nontail_node(K const &max) const {
    using std::cout;
    using std::endl;

    size_t const read_faa = faa_counter.load();
    size_t const read_cas = cas_counter.load();

    if (read_faa <= FAA_CAPACITY) {
      cout << "FAA counter must be above FAA_CAPACITY for non-tail node when "
              "data structure is quiescent, but is: "
           << read_faa << endl;
      return fail();
    }

    if (get_state(read_cas) != CAS_INSERTION) {
      cout << "CAS counter is inconsistent with CAS insertion state: "
           << read_cas << endl;
      return fail();
    }

    // Verify all elements are <= max
    size_t const size = get_size();
    for (size_t i = 0; i < size; ++i) {
      K const &k = list[i].key;
      if (k > max) {
        cout << "Key " << k << " at index " << i << " exceeds maximum value of "
             << max << endl;
        return fail();
      }
    }

    return true;
  }

  // A method for verifying a node in the extraction state.
  bool verify_extraction_node() const {
    using std::cout;
    using std::endl;

    size_t read_faa = faa_counter.load();
    size_t read_cas = cas_counter.load();

    if (read_faa <= FAA_CAPACITY) {
      cout << "FAA counter must be above FAA_CAPACITY for extraction node, but "
              "is: "
           << read_faa << endl;
      return fail();
    }

    if (get_state(read_cas) != EXTRACTION) {
      cout << "CAS counter is inconsistent with extraction state: " << read_cas
           << endl;
      return fail();
    }

    return true;
  }

  // A method for verifying locked nodes during execution.
  // Obviously, should only be done in debug mode.
  // This method checks to make sure all elements are populated, and so it
  // should only be invoked after a function that waits for all interleaving
  // inserts to complete (steal_half(), split_insert(), steal_max().)
  bool verify_locked([[maybe_unused]] size_t size) const {
#ifndef NDEBUG
    using std::cout;
    using std::endl;

    size_t i = 0;

    if (size > CAPACITY) {
      cout << "VQ Locked Verification: size exceeds capacity" << endl;
      return fail();
    }

    // Make sure keys up through size are populated.
    for (i = 0; i < size; ++i) {
      K const curr = list[i].key.load();
      if (curr == EMPTY) {
        cout << "VQ Locked Verification: slot " << i << " reserved but empty"
             << endl;
        return fail();
      }
    }

    // Make sure all keys through the end of the list are EMPTY.
    for (; i < CAPACITY; ++i) {
      K const curr = list[i].key.load();
      if (curr != EMPTY) {
        cout << "VQ Locked Verification: slot " << i
             << " occupied but not reserved" << endl;
        return fail();
      }
    }
#endif
    return true;
  }

  void verbose_analysis() const {
    using std::cout;

    cout << "[";

    // Print first element
    K _first = list[0].key.load();
    if (_first != EMPTY)
      cout << +_first;

    // Print size/capacity.
    cout << "](" << get_size() << "/" << CAPACITY << ")";
  }

  // Get the maximum key.
  // This is costly, so don't do this if you can avoid it.
  [[nodiscard]] K max_key() const {
    K max_key = EMPTY;

    // First, scan for any valid key.
    size_t i = 0;
    for (; i < CAPACITY; ++i) {
      if (list[i].key != EMPTY) {
        max_key = list[i].key;
        break;
      }
    }

    if (max_key == EMPTY)
      throw std::out_of_range(
          "max_key(): maximum of empty vector is undefined");

    // Now that max_key is initialized,
    // scan the rest of the vector for a bigger key.
    for (; i < CAPACITY; ++i)
      if (list[i].key != EMPTY && list[i].key > max_key)
        max_key = list[i].key;

    return max_key;
  }

  void dump() const {
    using std::cout;

    // Print all elements
    cout << "[ ";
    size_t i = 0;
    size_t empty_count = 0;
    for (; i < CAPACITY; ++i) {
      if (list[i].key != EMPTY) {
        if (empty_count > 1) {
          cout << "(" << empty_count << " EMPTYs) ";
          empty_count = 0;
        } else if (empty_count == 1) {
          cout << "EMPTY ";
          empty_count = 0;
        }
        cout << +list[i].key << " ";
      } else {
        ++empty_count;
      }
    }

    if (empty_count > 1)
      cout << "(" << empty_count << " EMPTYs) ";
    else if (empty_count == 1)
      cout << "EMPTY ";

    // Print size/capacity
    cout << "](" << get_size() << "/" << CAPACITY << ")";

    // Print counters
    size_t const faa_val = faa_counter.load();
    size_t const cas_val = cas_counter.load();
    state_t const state = get_state(cas_val);

    if (state == EXTRACTION) {
      cout << ", Extractions: " << extraction_counter << "/"
           << elements_at_phase_change;
    } else {
      if (state == LOCKED)
        cout << ", Locked";

      if (faa_val > FAA_CAPACITY) {
        cout << ", CAS, counter: " << get_insert_counter(cas_val);
        cout << ", version " << get_version_counter(cas_val);
      } else {
        cout << ", FAA, counter: " << faa_val;
      }
    }

    cout << std::endl;
  }

  [[nodiscard]] std::string_view get_name() const {
    return "Vector with Atomic Slot Reservation";
  }
  [[nodiscard]] size_t get_capacity() const { return CAPACITY; }

  /// Estimate the current size by returning the number of insertion
  /// reservations.
  [[nodiscard]] size_t get_size() const {
    size_t const faa_val = faa_counter.load();

    if (faa_val <= FAA_CAPACITY) {
      // faa_insertion_counter has a value that indicates it is still in use,
      // so return its value.
      return faa_val;
    }

    size_t const cas_val = cas_counter.load();
    if (cas_val == 0) {
      // faa_insertion_counter reports that FAA insertion is over, but
      // cas_insertion_counter reports that CAS insertion has not yet
      // started, so we know for sure that the number of successful
      // reservations is FAA_CAPACITY, the maximum amount permitted by FAA
      // insertion.
      return FAA_CAPACITY;
    }

    state_t const state = get_state(cas_val);

    switch (state) {
    case CAS_INSERTION:
    case LOCKED:
      // Normal case: just read it from cas_insertion_counter.
      return get_valid_reservations(cas_val);

    case EXTRACTION:
      // Return the number of remaining elements, floored at zero.
      if (extraction_counter > elements_at_phase_change)
        return 0;
      else
        return elements_at_phase_change - extraction_counter;

    case FAA_INSERTION:
      throw std::logic_error("Unexpectedly in FAA_INSERTION state.");

    default:
      throw std::logic_error("Unrecognized state: " + std::to_string(state));
    }
  }

  // No op
  static void tear_down() {}

  // Public acquire and release methods.
  // We handle the interactions with the insertion counter for them.

  bool acquire() {
    // Busywait until unlocked, then CAS to locked.
    size_t read_val = cas_counter.load();
    bool success = false;
    loop_counter ctr;
    while (!success) {
      ctr.count();
      state_t const state = get_state(read_val);
      if (is_insertion_state(state)) {
        size_t const new_val = read_val | LOCK_BIT;
        success = cas_counter.compare_exchange_weak(read_val, new_val);
      } else if (state == EXTRACTION) {
        // Dead lock; can never be taken again.
        return false;
      } else {
        read_val = cas_counter.load();
      }
    }

    if (get_version_counter(read_val) == 0) {
      // If this is the first time the lock is acquired, then there is a
      // chance this VQ is still in FAA insertion mode. If it is, then we do
      // not actually have an exclusive lock yet, despite acquiring
      // cas_insertion_counter. We must also increase faa_insertion_counter to
      // a value that indicates FAA insertions are closed, and record the
      // value. Since we can conflict with concurrent FAAs, we need to either
      // wait until faa_insertion_counter exceeds FAA_CAPACITY, or CAS it.
      faa_to_cas_transition();
    }

    return true;
  }

  /// Acquire variant to be used when a split is needed.
  /// Handles the following cases:
  /// (a) Normal-case (50/50) split, due to being at CAPACITY.
  /// (b) Special-case (n-and-1) split, due to being at FAA_CAPACITY and being
  /// the thread that transitions this vector from version number 0 to 1.
  /// (c) No split needed at all; some interleaving thread performed a split
  /// on this node due to inserting a tall element into the middle of it. This
  /// method will return the number of elements that need to be split off:
  /// CAPACITY/2 in case (a), 1 in case (b), and 0 in case (c).
  /// In case (a) and (b), we take the lock; in case (c), we do not.
  size_t acquire_for_split() {

    // Busywait until unlocked, then check...
    size_t read_val = cas_counter.load();
    state_t state = get_state(read_val);
    size_t read_version = get_version_counter(read_val);
    size_t insertions = get_insert_counter(read_val);

    bool success = false;
    loop_counter ctr;

    while (!success) {
      ctr.count();

      if (is_insertion_state(state)) {
        if (read_version > 0 && insertions <= CAPACITY) {
          // If read_version is greater than zero, we are in CAS insertion
          // mode, and thus this vector can accommodate CAPACITY elements. If
          // the number of reserved insertions is <= CAPACITY, we can conclude
          // that some interleaving thread split this node due to a tall
          // insert, and thus our split is no longer needed. Thus, we are in
          // case (c), and return 0 without locking.
          return 0;
        }

        // Otherwise, let's try to take the lock.
        size_t const new_val = read_val | LOCK_BIT;
        success = cas_counter.compare_exchange_weak(read_val, new_val);
      } else if (state == EXTRACTION) {
        // The lock is dead and can never be taken again;
        // thus, we are in case (c) and return 0 without locking.
        return 0;
      } else {
        read_val = cas_counter.load();
      }

      read_version = get_version_counter(read_val);
      insertions = get_insert_counter(read_val);
      state = get_state(read_val);
    }

    if (read_version == 0) {
      // Transitioning from FAA to CAS insertion, so, n-and-1 split
      assert(insertions == 0);
      faa_to_cas_transition();
      return 1;
    }

    // Already in CAS insertion state, so, 50/50 split
    assert(insertions > CAPACITY);
    return CAPACITY / 2;
  }

  size_t release_after_split() { return release(); }

  bool try_upgrade(size_t version_number) {
    size_t read_val = cas_counter.load();
    size_t const read_version = get_version_counter(read_val);

    if (read_version != version_number || !is_insertion_state(read_val))
      return false;

    bool const result =
        cas_counter.compare_exchange_strong(read_val, read_val + LOCK_BIT);

    if (result && get_version_counter(read_val) == 0)
      // If the CAS was successful, and we are in the FAA insertion state, do
      // the state transition.
      faa_to_cas_transition();

    return result;
  }

  /// Transition from the FAA insertion state to the CAS insertion state.
  void faa_to_cas_transition() {
    size_t insertions = FAA_CAPACITY;
    size_t read_faa = faa_counter.load();

    while (read_faa <= FAA_CAPACITY) {
      // If the number of FAA insertions has not yet reached FAA_CAPACITY+1,
      // then we need to update faa_insertion_counter to end FAA insertion
      // mode. We also need to record the actual number of insertions from it.
      if (faa_counter.compare_exchange_weak(read_faa, FAA_CAPACITY + 1)) {
        insertions = read_faa;
        break;
      }

      read_faa = faa_counter.load();
    }

    // Update the CAS insertion counter with the value.
    cas_counter.store(LOCK_BIT + insertions);
  }

  size_t release() {
    size_t const read_val = cas_counter.load();
    assert(get_state(read_val) == LOCKED);

    // Version number overflow check
    assert(read_val < 0xFFFFFFFF00000000);

    size_t const new_val = read_val + LOCK_BIT;
    cas_counter.store(new_val);
    return get_version_counter(new_val);
  }

  void release_unchanged() {
    size_t const read_val = cas_counter.load();
    size_t const insertions = get_insert_counter(read_val);
    assert(get_state(read_val) == LOCKED);

    if (get_version_counter(read_val) >= 1) {
      // In the normal case,
      // clear the lock bit without changing the version number.
      size_t const new_val = read_val & ~LOCK_BIT;
      cas_counter.store(new_val);
    } else {
      // A version number of 0 indicates we were in FAA insertion mode.
      // Special code is required to roll back to that mode.
      assert(insertions <= FAA_CAPACITY);

      // Write the number of insertions into the FAA counter.
      // This immediately allows other threads to resume FAA insertions.
      faa_counter.store(insertions);

      // Clear the CAS counter completely,
      // which is the expectation during the FAA insertion state.
      cas_counter.store(0);
    }
  }

  [[nodiscard]] bool is_dead() const {
    return get_state(cas_counter.load()) == EXTRACTION;
  }

  [[nodiscard]] size_t begin_read() const {
    return get_version_counter(cas_counter.load());
  }

  [[nodiscard]] bool confirm_read(size_t v) const {
    size_t const read_val = cas_counter.load();
    return is_insertion_state(read_val) && get_version_counter(read_val) == v;
  }
};
