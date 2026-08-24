#pragma once

#include <atomic>
#include <cassert>
#include <cstddef>
#include <functional>
#include <stdexcept>

#include "include-cache/common/config.h"
#include "include-cache/common/rlx_atomic.h"
#include "include-cache/common/special_values.h"
#include "include-cache/hp/hp_deletable.h"
#include "include-cache/lock/sv_lock.h"
#include "include-cache/rng/lehmer64.h"

/// This class is the default implementation of the skip vector. It supports
/// for_each and range operations using lazy two-phase locking for concurrency
/// control.  That is, it does not lock its entire working right away.  Instead,
/// it begins traversing the SkipVector and locking elements.  As it locks
/// elements, it operates on them.  Finally, when it has finished locking
/// elements and operating on them, then it releases all of its locks.
///
/// skipvector lazily merges orphans and uses non-resizable vectors.
///
/// Template Parameters:
/// @param K          - The type of the key for k/v pairs.
/// @param V          - The type of the value for k/v pairs.
/// @param IDX_VEC    - A vector type that can hold pairs for the index layer.
/// @param DATA_VEC   - A vector type that can hold pairs for the data layer.
/// @param IDX_EXP    - The log_2 of the target chunk size for index vectors.
/// @param DATA_EXP   - The log_2 of the target chunk size for data vectors.
/// @param MAX_LAYERS - The maximum number of index layers.
/// @param HP         - The class responsible for managing hazard pointers.
template <typename K, typename V,
          template <typename, typename, size_t> typename IDX_VEC,
          template <typename, typename, size_t> typename DATA_VEC,
          int64_t IDX_EXP, int64_t DATA_EXP, size_t MAX_LAYERS, typename HP>
class skipvector {

  static constexpr size_t exp_to_ratio(int64_t exp) {
    return exp <= 0 ? 2 : (1 << exp);
  }

  /// TARGET_IDX_RATIO represents the expected ratio between the number of
  /// elements in any given index layer and the index layer above it. In the
  /// normal case, this is 2^IDX_EXP (which we calculate with a left shift),
  /// but if IDX_EXP is set to a special value (<=0), then we set it to 2.
  static constexpr size_t TARGET_IDX_RATIO = exp_to_ratio(IDX_EXP);

  /// TARGET_DATA_RATIO represents the expected ratio between the number of
  /// elements in the data layer and  index layer 0. As with
  /// TARGET_IDX_RATIO, we use 2 when a special value is provided.
  static constexpr size_t TARGET_DATA_RATIO = exp_to_ratio(DATA_EXP);

  using addr_t = uint64_t;

  /// node_t is used for both the data layer and the index layer(s)
  struct node_t : public hp_deletable {
    /// A lock to protect this node.  sv_lock is a sequence lock with a
    /// few stolen bits
    sv_lock lock;

    /// Identifies the remote node this local node mirrors
    const addr_t remote_addr; // todo: do I want to store struct ver too to detect when structural changes occur? or unnec.?

    /// A pointer to the next node in this layer.
    rlx_atomic<node_t *> next;

    /// Default constructor; creates node as orphan. This constructor is only
    /// ever used to create leftmost nodes, which are always orphans.
    node_t(uint64_t r_addr) :
            remote_addr(r_addr), lock(true), v(), next(nullptr) {}
    /// Constructor; creates node, and stitches it in after prev.
    /// Requires that prev is locked.
    node_t(node_t *prev, bool orphan, uint64_t r_addr) :
            remote_addr(r_addr), lock(orphan), v(), next(prev->next.load()) {
      prev->next = this;
    }

    ~node_t() override = default;

    /// Sequential code for checking if a node is an orphan
    ///
    /// NB: Concurrent methods should read the orphan bit from the seqlock
    [[nodiscard]] bool is_orphan_seq() const {
      return sv_lock::is_orphan(lock.get_value());
    }

    void dump() const {
      lock.dump();
      v.dump();
    }
  };

  template <typename T, int64_t EXP,
            template <typename, typename, size_t> typename VEC>
  struct index_node_t : public node_t {

    static constexpr size_t get_vector_size() {
      static_assert(EXP <= 64);
      if (EXP > 0) {
        // General case: choose a size that can hold 2 * 2^EXP elements.
        return 2 << EXP;
      } else {
        // Special case: if EXP <= 0, set the capacity to 1.
        return 1;
      }
    }

    /// A vector of key/value pairs.
    VEC<K, T, get_vector_size()> v;

    /// Default constructor; creates node as orphan. This constructor is only
    /// ever used to create leftmost nodes, which are always orphans.
    index_node_t(uint64_t r_addr) : node_t(r_addr) {}
    
    /// Constructor; creates node, and stitches it in after prev.
    /// Requires that prev is locked.
    index_node_t(node_t *prev, bool orphan, uint64_t r_addr) : node_t(prev, orphan, r_addr) {
      prev->next = this;
    }

    ~index_node_t() override = default;

    /// Checks to see if this node's successor should be merged into it.
    /// This is the case if next is an orphan and the sum of the sizes is under
    /// merge_threshold, or if next is totally empty.
    ///
    /// For sequence lock safety reasons, next and next_is_orphan must be passed
    /// into this method, even though it may seem like it is redundant to do so.
    template <bool CLEANUP>
    bool should_merge(double merge_threshold, node_t *next,
                      bool next_is_orphan) {
      // Non-orphans can never be merged.
      if (!next_is_orphan)
        return false;

      int const nextsize = next->v.get_size();

      if (nextsize == 0)
        // If next is totally empty, always merge.
        return true;

      if (!CLEANUP || EXP <= 0)
        // If the CLEANUP flag is disabled, do not merge. Also, merges of
        // nonempty nodes should never happen in skiplist/skiparray simulation
        // mode. These are both templated parameters, so this check gets
        // optimized out by the compiler.
        return false;
      else
        // In the normal case, we compare the  using the actual merge
        // threshold parameter.
        return (v.get_size() + nextsize) < (merge_threshold * (1 << EXP));
    }

    /// Merge the next node into this node, and unlink next node
    ///
    /// NB: The caller is expected to handle reclamation of unlinked node
    void merge() {
      node_t *zombie = next;
      v.merge(&(zombie->v));
      next = zombie->next.load();
      zombie->lock.die();
    }

    /// Insert a K/V pair into this node
    ///
    /// NB: May split this node if it is full
    bool insert(const std::pair<const K, T> &pair) {
      bool overfull = false;
      bool const result = v.insert(pair, overfull);
      if (overfull) {
        // Insert failed because the current node was too big,
        // so split it and make an orphan.
        // Note: the orphan's constructor will stitch itself in.
        auto *new_orphan = new node_t(this, true);
        new_orphan->v.steal_half_and_insert(&v, pair);
        return true;
      }
      return result;
    }

  };

  struct data_node_t : public node_t {
    /// Minimum key stored in this data node
    K k_min; // todo: const?

    /// Default constructor; creates node as orphan. This constructor is only
    /// ever used to create leftmost nodes, which are always orphans.
    data_node_t(uint64_t r_addr, K k_min_) : node_t(r_addr), k_min(k_min_) {}

    /// Constructor; creates node, and stitches it in after prev.
    /// Requires that prev is locked.
    data_node_t(node_t *prev, bool orphan, uint64_t r_addr, K k_min_) : node_t(prev, orphan, r_addr), k_min(k_min_) {
      prev->next = this;
    }

    ~data_node_t() override = default;

    /// Merge the next node into this node, and unlink next node
    ///
    /// NB: The caller is expected to handle reclamation of unlinked node
    void merge() {
      node_t *zombie = next;
      next = zombie->next.load();
      zombie->lock.die();
    }
  };

  /// Represents fresh remote state that the orchestrator has observed and wants the cache to install
  /// Used by refresh and structural-update calls.
  template <int64_t EXP, template <typename, typename, size_t> typename VEC>
  struct remote_node_snapshot {
    static constexpr size_t get_vector_size() {
      static_assert(EXP <= 64);
      if (EXP > 0) {
        // General case: choose a size that can hold 2 * 2^EXP elements.
        return 2 << EXP;
      } else {
        // Special case: if EXP <= 0, set the capacity to 1.
        return 1;
      }
    }

    addr_t remote_addr;
    K k_min;
    addr_t next_remote_addr;  // remote address of the sibling
    // Contents of the vector (for cache to install)
    VEC<K, addr_t, get_vector_size()> entries;  // (key, down_addr) // TODO: should this be the type of one of the vector classes provided, or "converted" to that later?
  };

  /// type of index nodes.  Since an index node can reference either another
  /// index node, or a data node, we use a generic void*.  Thus the map holds
  /// K/ptr pairs
  using index_t = index_node_t<void *, IDX_EXP, IDX_VEC>;

  /// type of data nodes.  A data node's vector holds k/v pairs
  using data_t = data_node_t;

  /// The threshold at which to merge chunks of the skipvector
  const double merge_threshold;

  /// The number of index layers in the skipvector.
  /// This does not include the data layer.
  size_t const layers;

  /// Array of leftmost index nodes.
  std::array<index_t, MAX_LAYERS> index_head;

  /// Leftmost data vector.
  data_t data_head;

  /// Create a context for the thread, if one doesn't exist
  void init_context() const { HP::init_context(); }

  /// Generate height using a geometric distribution from 0 to layers.
  /// A height of n means it exists in the bottommost n index layers, and also
  /// the data layer. A height of zero means it exists solely in the data layer.
  [[nodiscard]] size_t random_height() const {
    // Special case: if IDX_EXP is set to　SKIPARRAY_SIM_MODE, we
    // always return 0 so no key is ever inserted into the index layer.
    if (IDX_EXP == SKIPARRAY_SIM_MODE) {
      return 0;
    }

    static thread_local __uint128_t g_lehmer64_state =
        lehmer64_seed(pthread_self());
    uint64_t r = lehmer64(g_lehmer64_state);
    size_t h = 0;

    // The probability that r is divisible by TARGET_DATA_RATIO is exactly
    // 1 / TARGET_DATA_RATIO (assuming the size of the range is a power of 2.)
    //
    // The first iteration of the loop is unrolled to specially handle the data
    // layer.
    //
    // NB: The trick here is that we can look for a series of low 0 bits, and
    //     use that to both (a) check many bits in parallel, and (b)
    //     short-circuit the search when we find any non-zero bits.
    if (r % TARGET_DATA_RATIO == 0) {
      // Remove the used bits.
      r /= TARGET_DATA_RATIO;

      for (h = 1; r % TARGET_IDX_RATIO == 0 && h < layers; ++h) {
        // Remove the used bits.
        r /= TARGET_IDX_RATIO;
      }
    }

    return h;
  }

  /// Helper function that determines if a search should continue to the next
  /// chunk, and if so, it advances curr repeatedly until no more advancing is
  /// necessary.
  ///
  /// If parameter "cleanup" is set to true, this function will merge orphans
  /// as necessary when they are found. If it is not, will only clean up empty
  /// orphans. The cleanup flag is set to true by insert() and remove();
  /// contains() sets it to false, to keep contains() fast.
  ///
  /// INVARIANT: This thread holds curr and only curr when this method is
  /// called, and when it returns.
  ///
  /// @returns true if successful, false on a seqlock verification failure
  template <bool CLEANUP, typename T>
  bool check_next(T *&curr, uint64_t &curr_lock, K const &k) {
    // The fastest way out of this loop is when next is nullptr or curr's last
    // element is >= k.  Finding these early avoids taking a hazard pointer on
    // next or reading its seqlock.  If the /while/ condition fails, we will
    // return true.
    T *next = curr->next;
    K last = k;
    while (next != nullptr && (!curr->v.last(last) || k > last)) {
      // Take a hazard pointer on next, then make sure curr hasn't changed
      HP::take_next(next);
      if (!curr->lock.confirm_read(curr_lock)) {
        HP::drop_next();
        return false;
      }

      uint64_t next_lock = next->lock.begin_read();

      // TODO: move logic to remove empty orphan to update path (currently even if CLEANUP is False, merge if next is empty)
      // TODO: can we encounter an empty orphan once ^ applied?
      // Check if /next/ needs to be removed (and possibly merged first)
      // - remove if it's an empty orphan
      // - merge+remove if it's an orphan and cleanup == should_merge() == true
      //
      // [mfs] The guard for this /if/ is the same as the guard for the
      //       do/while.  It's probably possible to refactor into a single
      //       /while/ loop
      if (curr->template should_merge<CLEANUP>(merge_threshold, next,
                                               next->lock.is_orphan())) {

        // [mfs] This logic should be very infrequently needed.  I think we'd be
        //       better having it in a separate function, so that hopefully it
        //       doesn't get inlined

        // Get write lock on curr, since we'll modify curr->next
        if (!curr->lock.try_upgrade(curr_lock)) {
          HP::drop_next();
          return false;
        }

        // We don't need an HP on next anymore; it can't be unlinked without
        // modifying curr, which we have locked. (We don't need curr's either
        // for the moment, but we'll need it later so keep it.)
        HP::drop_next();

        // We unlink in a loop, since there may be multiple orphans
        bool changed = false;
        do {
          // Acquire next, so we can mark it deleted
          if (!next->lock.try_upgrade(next_lock)) {
            curr->lock.release_changed_if(changed); // may downgrade curr->lock
            return false;
          }

          // Unlink it, and mark it for reclamation.
          curr->merge();
          HP::reclaim(next);
          next = curr->next;
          changed = true;

          // We may need to keep looping.  Next==null is the easy exit case
          if (next == nullptr) {
            curr_lock = curr->lock.release();
            return true;
          }

          // NB: We don't have to take a hazard pointer on next because we have
          //     its predecessor locked as a writer.  Even if it's not an
          //     orphan, it can't be deleted without holding a lock on its
          //     predecessor, and we have that lock.
          next_lock = next->lock.begin_read();
        } while (curr->template should_merge<CLEANUP>(
            merge_threshold, next, sv_lock::is_orphan(next_lock)));

        if (CLEANUP) {
          // If the CLEANUP flag is enabled, merging may have eliminated the
          // need to check next, so start again from the top.
          //
          // NB: curr->lock.release() still gives us a read lock on curr
          curr_lock = curr->lock.release();
          continue;
        } else {
          // Before we release the write lock on curr, we need to take a hazard
          // pointer on next, so that we can continue to access it safely after
          // the release.
          HP::take_next(next);
          curr_lock = curr->lock.release();
        }
      }

      // At this point we know that we have a nonempty next.
      if (k < next->v.first()) {
        // Next's first element is after k, so we have ruled out next.
        // Now we just need to check its sequence lock.
        // Return true if the check succeeds, false if it fails.
        bool const result = next->lock.confirm_read(next_lock);
        HP::drop_next();
        return result;
      }

      // Next's first element is before (or equal to) the sought key,
      // so we to go to next and repeat from there. We're done with curr,
      // so we just need to confirm its sequence lock hasn't changed.
      if (!curr->lock.confirm_read(curr_lock)) {
        HP::drop_next();
        return false;
      }

      curr = next;
      curr_lock = next_lock;
      next = curr->next;
      HP::drop_curr();
    }

    // We ruled out next, so return true.
    return true;
  }

  template <typename T>
  bool check_next_dl(T *&curr, uint64_t &curr_lock, K const &k) {
    // The fastest way out of this loop is when next is nullptr or curr's last
    // element is >= k.  Finding these early avoids taking a hazard pointer on
    // next or reading its seqlock.  If the /while/ condition fails, we will
    // return true.
    T *next = curr->next;
    K last = k;
    while (next != nullptr) { // todo - why would next ever be a nullptr?
      // Take a hazard pointer on next, then make sure curr hasn't changed
      HP::take_next(next);
      if (!curr->lock.confirm_read(curr_lock)) {
        HP::drop_next();
        return false;
      }

      uint64_t next_lock = next->lock.begin_read();

      // At this point we know that we have a nonempty next.
      if (k < next->k_min) {
        // Next's min element is after k, so we have ruled out next.
        // Now we just need to check its sequence lock.
        // Return true if the check succeeds, false if it fails.
        bool const result = next->lock.confirm_read(next_lock);
        HP::drop_next();
        return result;
      }

      // Next's first element is before (or equal to) the sought key,
      // so we to go to next and repeat from there. We're done with curr,
      // so we just need to confirm its sequence lock hasn't changed.
      if (!curr->lock.confirm_read(curr_lock)) {
        HP::drop_next();
        return false;
      }

      curr = next;
      curr_lock = next_lock;
      next = curr->next;
      HP::drop_curr();
    }

    // We ruled out next, so return true.
    return true;
  }

  /// Sequential-only variant of check_next.
  template <typename T> void check_next_sequential(T *&curr, K const &k) {
    T *next = curr->next;
    K last = k;
    while (next != nullptr && (!curr->v.last(last) || k > last)) {
      while (curr->template should_merge<true>(merge_threshold, next,
                                               next->lock.is_orphan())) {
        // Unlink and immediately reclaim next
        // (no need for hazard pointers when running in isolation.)
        // NB: merge() expects next's lock to be held, so we have to take it,
        // even though this method is sequential-only.
        next->lock.acquire();
        curr->merge();
        delete next;
        next = curr->next;

        if (next == nullptr)
          return;
      }

      if (k < next->v.first())
        return;

      curr = next;
      next = curr->next;
    }
  }

  /// Given a node /curr/ that is read locked, give up that lock, and replace it
  /// with a read lock on new_node.  Also drops HP on curr, takes HP on new_node
  ///
  /// @returns true if successful, false if failed.
  template <typename T>
  bool reader_swap(index_t *curr, uint64_t &curr_lock, T *new_node) const {
    // Take a hazard pointer on new_node, make sure curr hasn't changed
    HP::take_next(new_node);
    if (!curr->lock.confirm_read(curr_lock)) {
      HP::drop_next();
      return false;
    }

    // read-lock new_node, then check curr hasn't changed
    //
    // NB: This double-check is necessary. It is possible that new_node's
    // minimum element will be removed in the meantime, and then reinserted at
    // a lower height.
    uint64_t const new_lock = new_node->lock.begin_read();
    if (!curr->lock.confirm_read(curr_lock)) {
      HP::drop_next();
      return false;
    }

    curr_lock = new_lock;
    HP::drop_curr();
    return true;
  }

  /// follow() is used by contains to find the correct down pointer from curr.
  /// follow() also swaps the lock on curr for a lock on the new down node
  template <typename T>
  bool follow(index_t *curr, uint64_t &curr_lock, K const &k, T *&down) {
    // if check_next() fails, start over
    if (!check_next<false>(curr, curr_lock, k))
      return false;

    // Find down pointer in curr, confirm curr's sequence lock (and next's, if
    // next was read), and take a seqlock on down.
    void *down_void = nullptr;
    if (curr->v.find_lte(k, down_void))
      down = static_cast<T *>(down_void);

    return reader_swap<T>(curr, curr_lock, down);
  }

  /// skip_to is used by range operations to find the correct starting point in
  /// the data layer
  data_t *skip_to(K const &k) {
    // [mfs] Instead of goto, can we use recursion?
  top:
    // Start from the head node (leftmost node in topmost layer.)
    int layer = layers - 1;
    index_t *curr = &(index_head.at(layer));

    // Read head node's sequence lock.
    HP::take_first(curr);
    uint64_t curr_lock = curr->lock.begin_read();

    // Skip through all index layers but the last.
    for (; layer >= 1; --layer) {
      // If follow() doesn't find a suitable down pointer,
      // default to next index layer's head.
      index_t *down = &index_head.at(layer - 1);
      if (!follow(curr, curr_lock, k, down)) {
        // Sequence lock check failed
        HP::drop_curr();
        goto top;
      }
      curr = down;
    }

    // Skip through the last index layer.
    data_t *curr_dl = &data_head;
    if (!follow(curr, curr_lock, k, curr_dl)) {
      // Sequence lock check failed
      HP::drop_curr();
      goto top;
    }

    // Upgrade curr_dl's lock from reader to writer.
    if (!curr_dl->lock.try_upgrade(curr_lock)) {
      // Sequence lock check failed
      HP::drop_curr();
      goto top;
    }

    // We now have a true mutex on curr_dl,
    // so we can drop all of our hazard pointers.
    HP::drop_curr();

    // curr_dl is now locked by the traversal.
    return curr_dl;
  }

public:
  using KEY_TYPE = K;
  using VAL_TYPE = V;

  /// insert() takes a reference to a k/v pair, so we expose the type here
  using value_type = std::pair<const K, V>;

  /// Benchmark constructor
  explicit skipvector(config *cfg)
      : merge_threshold(cfg->merge_threshold), layers(cfg->layers) {

    // Make sure number of layers is valid.
    assert(layers > 0 && layers <= MAX_LAYERS);

    // We use a single 64-bit random number on insert(), so make sure that's
    // enough for the chosen configuration.
    assert(DATA_EXP + (cfg->layers * IDX_EXP) <= 64);
  }

  /// Sequential-only destructor
  ~skipvector() {
    // First, free all index layer nodes BUT the leftmost ones.
    for (size_t i = 0; i < layers; ++i) {
      index_t *curr = index_head.at(i).next;
      while (curr != nullptr) {
        index_t *next = curr->next;
        delete curr;
        curr = next;
      }
    }

    // Free each node in data layer but the leftmost,
    // which was statically allocated
    data_t *data_curr = data_head.next;
    while (data_curr != nullptr) {
      data_t *next = data_curr->next;
      delete data_curr;
      data_curr = next;
    }

    // Thoroughly sweep the hazard pointer lists to clean up any remaining
    // unreclaimed nodes from this skip vector.
    HP::sweep_all();
  }

  // Sequential-only teardown method.
  static void tear_down() { HP::tear_down(); }

  /// Search for a key in the skipvector
  /// Returns remote address of node which may contain k
  addr_t locate_data(K const &k) {
    // TODO: once figure out merging, update check_next()

    init_context(); // hazard pointers

  top:
    // Start from the head node (leftmost node in topmost layer.)
    int layer = layers - 1;
    index_t *curr = &(index_head.at(layer));

    // Read head node's sequence lock.
    HP::take_first(curr);
    uint64_t curr_lock = curr->lock.begin_read();

    // Skip through all index layers but the last.
    for (; layer >= 1; --layer) {
      // If follow() doesn't find a suitable down pointer,
      // default to next index layer's head.
      index_t *down = &index_head.at(layer - 1);
      if (!follow(curr, curr_lock, k, down)) {
        // Sequence lock check failed
        HP::drop_curr();
        goto top;
      }
      curr = down;
    }

    // Skip through the last index layer.
    data_t *curr_dl = &data_head;
    if (!follow(curr, curr_lock, k, curr_dl)) {
      // Sequence lock check failed
      HP::drop_curr();
      goto top;
    }

    // Finally, read the data layer.
    if (!check_next_dl(curr_dl, curr_lock, k)) {
      HP::drop_curr();
      goto top;
    }

    // Scan curr_dl for the sought value.
    // NB: There is a chance that this contains() will find a value,
    // but the final sequence lock checks will fail, making us start over,
    // and the element will be gone by the time we get back here.
    // This scenario would yield the somewhat undesirable result that this
    // method returns false but overwrites v with an outdated value.
    // To prevent this, we use a temporary intermediate variable, tmp.
    addr_t const r_addr = curr_dl->remote_addr;

    // Confirm curr's sequence lock.
    if (!curr_dl->lock.confirm_read(curr_lock)) {
      HP::drop_curr();
      goto top;
    }

    HP::drop_curr();

    return r_addr;
  }

  /// Gather prev info for every level <= height
  bool gather_prevs(K const &k, uint32_t const height, addr_t*& prev_addrs) {

    init_context(); // hazard pointers

  top:
    // Start from the head node (leftmost node in topmost layer.)
    int layer = layers - 1;
    index_t *curr = &(index_head.at(layer));

    // Read head node's sequence lock.
    HP::take_first(curr);
    uint64_t curr_lock = curr->lock.begin_read();

    // Skip through all index layers but the last.
    for (; layer >= 1; --layer) {
      // If follow() doesn't find a suitable down pointer,
      // default to next index layer's head.
      index_t *down = &index_head.at(layer - 1);
      if (!follow(curr, curr_lock, k, down)) {
        // Sequence lock check failed
        HP::drop_curr();
        goto top;
      }
      curr = down;
      if (layer <= height) {
        prev_addrs[]
      }
    }

    // Skip through the last index layer.
    data_t *curr_dl = &data_head;
    if (!follow(curr, curr_lock, k, curr_dl)) {
      // Sequence lock check failed
      HP::drop_curr();
      goto top;
    }

    // Finally, read the data layer.
    if (!check_next_dl(curr_dl, curr_lock, k)) {
      HP::drop_curr();
      goto top;
    }

    // Scan curr_dl for the sought value.
    // NB: There is a chance that this contains() will find a value,
    // but the final sequence lock checks will fail, making us start over,
    // and the element will be gone by the time we get back here.
    // This scenario would yield the somewhat undesirable result that this
    // method returns false but overwrites v with an outdated value.
    // To prevent this, we use a temporary intermediate variable, tmp.
    addr_t const r_addr = curr_dl->remote_addr;

    // Confirm curr's sequence lock.
    if (!curr_dl->lock.confirm_read(curr_lock)) {
      HP::drop_curr();
      goto top;
    }

    HP::drop_curr();

    return r_addr;
  }

  /// Insert a new element into the map
  bool insert(value_type const &pair) {
    init_context();

    K const &k = pair.first;

    // Pre-generate a new height for the node
    int const new_height = random_height();

    // Do a lookup as though doing a contains() operation, but save references
    // to index nodes we'll need later in an array.
    std::array<index_t *, MAX_LAYERS> frozen_nodes = {nullptr};

  top:

    // Start at the topmost index layer.
    int layer = layers - 1;
    index_t *curr = &(index_head.at(layer));
    HP::take_first(curr);
    uint64_t curr_lock = curr->lock.begin_read();
    data_t *curr_dl = &data_head;

    // checkpoint is a "safe node" that we know won't be deleted, because either
    // we hold the lock on its parent, or it is the head node of its layer.
    // If we have a sequence lock check fail during execution, and we have a
    // checkpoint, we can jump back to it rather than start all over.
    index_t *checkpoint = nullptr;
    data_t *checkpoint_dl = nullptr;
    bool load_checkpoint = false;

    // Skip through index layers.
    while (layer >= 0) {

      // Load the checkpoint if the appropriate flag is set.
      if (load_checkpoint) {
        HP::drop_curr();
        load_checkpoint = false;
        if (checkpoint == nullptr) {
          // No checkpoint was set, so retry from start.
          goto top;
        }
        curr = checkpoint;

        // NB: We know the checkpoint won't be deleted, so we do not need to
        // double-check the hazard pointer we take on it.
        HP::take_first(curr);
        curr_lock = curr->lock.begin_read();
      }

      // Check next, as it may need to be maintained or followed.
      if (!check_next<true>(curr, curr_lock, k)) {
        load_checkpoint = true;
        continue;
      }

      // If the inserted node is tall enough, lock this node and save it.
      if (layer < new_height) {
        if (!curr->lock.try_freeze(curr_lock)) {
          load_checkpoint = true;
          continue;
        }

        frozen_nodes.at(layer) = curr;
      }

      // Now, search the vector we arrived at for the right down pointer.
      void *down = nullptr;
      index_t *down_idx = nullptr;
      K found_k = k;

      if (curr->v.find_lte(k, found_k, down)) {
        if (found_k == k) {
          // If we find k in the index layer, stop and return false.
          if (layer < new_height) {
            // If we froze any nodes, we must thaw them before returning.
            for (int i = layer; i < new_height; ++i) {
              frozen_nodes.at(i)->lock.thaw();
            }
          } else {
            // We don't have curr locked,
            // so we must validate its sequence lock before returning.
            if (!curr->lock.confirm_read(curr_lock)) {
              load_checkpoint = true;
              continue;
            }
          }
          HP::drop_curr();
          return false;
        }

        // Otherwise, we found an appropriate down pointer, so follow it.
        if (layer > 0) {
          down_idx = static_cast<index_t *>(down);
        } else {
          curr_dl = static_cast<data_t *>(down);
        }
      } else {
        // No appropriate down pointer was found,
        // so start at the leftmost node at the next layer.
        if (layer > 0) {
          down_idx = &(index_head.at(layer - 1));
        } else {
          curr_dl = &data_head;
        }
      }

      if (layer < new_height) {
        // If we have locked curr, then we can use down as a checkpoint.
        if (layer > 0) {
          HP::take_next(down_idx);
          HP::drop_curr();
          curr = down_idx;
          curr_lock = down_idx->lock.begin_read();
          checkpoint = down_idx;
        } else {
          HP::take_next(curr_dl);
          HP::drop_curr();
          curr_lock = curr_dl->lock.begin_read();
          checkpoint_dl = curr_dl;
        }
      } else {
        // If we have not locked curr,
        // we must safely exchange locks and hazard pointers.
        if (layer > 0) {
          if (!reader_swap(curr, curr_lock, down_idx)) {
            load_checkpoint = true;
            continue;
          }
          curr = down_idx;
        } else {
          if (!reader_swap(curr, curr_lock, curr_dl)) {
            load_checkpoint = true;
            continue;
          }
        }
      }

      --layer;
    }

  retry_dl:
    // At this point we should be at the data layer.

    // Check if we have to follow any next pointers.
    bool const skip_success = check_next<true>(curr_dl, curr_lock, k);

    // Now, acquire curr_dl as a writer.
    if (!skip_success || !curr_dl->lock.try_upgrade(curr_lock)) {
      // Go back to checkpoint_dl if it exists, or start over from top.
      HP::drop_curr();

      if (checkpoint_dl != nullptr) {
        curr_dl = checkpoint_dl;
        curr_lock = curr_dl->lock.begin_read();
        HP::take_first(curr_dl);
        goto retry_dl;
      } else {
        goto top;
      }
    }

    // Common case: generated height is 0,
    // so simply attempt to insert it into the data layer.
    if (new_height == 0) {
      bool const result = curr_dl->insert(pair);
      curr_dl->lock.release_changed_if(result);
      HP::drop_curr();
      return result;
    }

    // Generated height is at least 1, so we need to partition the data node.
    // First we must manually check if the key is present in the data node.
    if (curr_dl->v.contains(k)) {
      // If key is present, thaw everything and just return false.
      for (int i = 0; i < new_height; ++i) {
        frozen_nodes.at(i)->lock.thaw();
      }
      curr_dl->lock.release_unchanged();
      HP::drop_curr();
      return false;
    }

    // Key isn't present, so do the partition.

    // Edge case: curr_dl may be full and the inserted key may be less than
    // its minimum. (This can only happen if it is leftmost.)
    // If this is the case, we must first partition curr_dl.
    if (curr_dl->v.get_size() == curr_dl->v.get_capacity() &&
        k < curr_dl->v.first()) {
      auto *new_orphan = new data_t(curr_dl, true);
      new_orphan->v.steal_half(&(curr_dl->v));
    }

    auto *new_data_node = new data_t(curr_dl, false);
    new_data_node->lock.acquire();
    new_data_node->v.split_insert(&(curr_dl->v), pair);
    new_data_node->lock.release();
    curr_dl->lock.release();

    void *down_ptr = new_data_node;

    // Partition any index layers that need partitioning,
    // and insert down pointers.
    // NB: This loop's range excludes the top layer
    // because we do not partition at the top layer
    for (int i = 0; i + 1 < new_height; ++i) {
      index_t *victim = frozen_nodes.at(i);

      victim->lock.acquire_frozen();

      // Same edge case as above, just for index layer nodes
      if (victim->v.get_size() == victim->v.get_capacity() &&
          k < victim->v.first()) {
        auto *new_index_orphan = new index_t(victim, true);
        new_index_orphan->v.steal_half(&(victim->v));
      }

      auto *new_index_node = new index_t(victim, false);
      new_index_node->v.split_insert(&(victim->v), std::make_pair(k, down_ptr));

      victim->lock.release();

      down_ptr = new_index_node;
    }

    // Finally, at the pre-generated height,
    // simply insert the appropriate down pointer into the appropriate vector.
    index_t *top_node = frozen_nodes.at(new_height - 1);
    top_node->lock.acquire_frozen();
    frozen_nodes.at(new_height - 1)->insert(std::make_pair(k, down_ptr));
    top_node->lock.release();

    HP::drop_curr();
    return true;
  }

  /// Insert a new element into the map, in isolation for setup
  bool insert_seq(value_type const &pair) {
    K const &k = pair.first;

    // Pre-generate a new height for the node
    int const new_height = random_height();

    // Do a lookup as though doing a contains() operation, but save references
    // to index nodes we'll need later in an array.
    std::array<index_t *, MAX_LAYERS> prev_nodes = {nullptr};

    // Start at the topmost index layer.
    int layer = layers - 1;
    index_t *curr = &(index_head.at(layer));
    data_t *curr_dl = &data_head;

    // Skip through index layers.
    while (layer >= 0) {
      check_next_sequential(curr, k);

      // If the inserted node is tall enough, lock this node and save it.
      if (layer < new_height)
        prev_nodes.at(layer) = curr;

      // Now, search the vector we arrived at for the right down pointer.
      void *down = nullptr;
      index_t *down_idx = nullptr;
      K found_k = k;

      if (curr->v.find_lte(k, found_k, down)) {
        if (found_k == k)
          return false;

        // Otherwise, we found an appropriate down pointer, so follow it.
        if (layer > 0)
          down_idx = static_cast<index_t *>(down);
        else
          curr_dl = static_cast<data_t *>(down);

      } else {
        // No appropriate down pointer was found,
        // so start at the leftmost node at the next layer.
        if (layer > 0)
          down_idx = &(index_head.at(layer - 1));
        else
          curr_dl = &data_head;
      }

      if (layer > 0)
        curr = down_idx;

      --layer;
    }

    // Check if we have to follow any next pointers.
    check_next_sequential(curr_dl, k);

    // Common case: generated height is 0,
    // so simply attempt to insert it into the data layer.
    if (new_height == 0)
      return curr_dl->insert(pair);

    // Generated height is at least 1, so we need to partition the data node.
    // First we must manually check if the key is present in the data node.
    if (curr_dl->v.contains(k))
      // If key is present, just return false.
      return false;

    // Key isn't present, so do the partition.

    // Edge case: curr_dl may be full and the inserted key may be less than
    // its minimum. (This can only happen if it is leftmost.)
    // If this is the case, we must first partition curr_dl.
    if (curr_dl->v.get_size() == curr_dl->v.get_capacity() &&
        k < curr_dl->v.first()) {
      auto *new_orphan = new data_t(curr_dl, true);
      new_orphan->v.steal_half(&(curr_dl->v));
    }

    auto *new_data_node = new data_t(curr_dl, false);
    new_data_node->v.split_insert(&(curr_dl->v), pair);

    void *down_ptr = new_data_node;

    // Partition any index layers that need partitioning,
    // and insert down pointers.
    // NB: This loop's range excludes the top layer
    // because we do not partition at the top layer
    for (int i = 0; i + 1 < new_height; ++i) {
      index_t *victim = prev_nodes.at(i);

      // Same edge case as above, just for index layer nodes
      if (victim->v.get_size() == victim->v.get_capacity() &&
          k < victim->v.first()) {
        auto *new_index_orphan = new index_t(victim, true);
        new_index_orphan->v.steal_half(&(victim->v));
      }

      auto *new_index_node = new index_t(victim, false);
      new_index_node->v.split_insert(&(victim->v), std::make_pair(k, down_ptr));

      down_ptr = new_index_node;
    }

    // Finally, at the pre-generated height,
    // simply insert the appropriate down pointer into the appropriate vector.
    prev_nodes.at(new_height - 1)->insert(std::make_pair(k, down_ptr));

    return true;
  }

  /// Remove an element from the map
  bool remove(K const &k) {
    init_context();

    // First, search for the uppermost instance of k in the data structure.
    // Clean up after other lazy removes along the way.

  top:

    // Start at the topmost index layer.
    int layer = layers - 1;
    index_t *curr = &(index_head.at(layer));
    uint64_t curr_lock = curr->lock.begin_read();
    HP::take_first(curr);
    data_t *curr_dl = &data_head;

    // Skip through index layers.
    while (layer >= 0) {
      // Check next, as it may need to be maintained or followed.
      if (!check_next<true>(curr, curr_lock, k)) {
        HP::drop_curr();
        goto top;
      }

      // Now, search the vector we arrived at for the right down pointer.
      void *down = nullptr;
      index_t *down_idx = nullptr;
      K found_k = k;

      if (curr->v.find_lte(k, found_k, down)) {
        if (found_k == k) {
          // If we find the uppermost instance of k in the skipvector, then lock
          // it and proceed to remove it.

          // NB: Here we must confirm that this is the uppermost instance of k.
          // If curr is not an orphan, and k is the first element in this list,
          // then this is NOT the uppermost instance of k, so start over.
          // Otherwise, it is safe to proceed. This can happen if this remove()
          // call interleaves with an insert() call on the same k.
          if (!sv_lock::is_orphan(curr_lock) && curr->v.first() == k) {
            HP::drop_curr();
            goto top;
          }

          if (!curr->lock.try_upgrade(curr_lock)) {
            HP::drop_curr();
            goto top;
          }

          // We have a write lock on curr now, so we don't need the hazard
          // pointer anymore.
          HP::drop_curr();
          break;
        }

        // Otherwise, we found an appropriate down pointer, so follow it.
        if (layer > 0) {
          down_idx = static_cast<index_t *>(down);
        } else {
          curr_dl = static_cast<data_t *>(down);
        }
      } else {
        // No appropriate down pointer was found,
        // so start at the leftmost node at the next layer.
        if (layer > 0) {
          down_idx = &(index_head.at(layer - 1));
        } else {
          curr_dl = &data_head;
        }
      }

      // Exchange curr's lock for down's lock.
      if (layer > 0) {
        if (!reader_swap(curr, curr_lock, down_idx)) {
          HP::drop_curr();
          goto top;
        }
        curr = down_idx;
      } else {
        if (!reader_swap(curr, curr_lock, curr_dl)) {
          HP::drop_curr();
          goto top;
        }
      }

      --layer;
    }

    // Normal case: k wasn't found in upper levels, so check data layer.
    if (layer == -1) {
      // Check if we have to follow any next pointers.
      bool const skip_success = check_next<true>(curr_dl, curr_lock, k);

      // Now, acquire curr_dl as a writer.
      if (!skip_success || !curr_dl->lock.try_upgrade(curr_lock)) {
        HP::drop_curr();
        goto top;
      }

      // Edge case: Same as above; this may not be the uppermost instance of k,
      // if this call interleaves with an insert() call on k.
      // Double-check that it is.
      if (!sv_lock::is_orphan(curr_lock) && curr_dl->v.first() == k) {
        curr_dl->lock.release_unchanged();
        HP::drop_curr();
        goto top;
      }

      // At this point, simply try to remove from the data node we arrived at.
      bool const result = curr_dl->v.remove(k);
      curr_dl->lock.release_changed_if(result);
      HP::drop_curr();
      return result;
    }

    // remove() starts being lazy here.
    // We broke out of loop early, so lock all the way down.
    // NB: For each new node we access here, we have its parent locked as a
    // writer, so there is no need to take a hazard pointer on them.

    for (int i = layer; i > 0; --i) {
      void *down_void = nullptr;
      curr->v.remove(k, down_void);
      auto *down_idx = static_cast<index_t *>(down_void);
      down_idx->lock.acquire();

      // Release first node normally, subsequent nodes as orphans.
      if (i == layer) {
        curr->lock.release();
      } else {
        curr->lock.release_as_orphan();
      }

      curr = down_idx;
    }

    // And do it once more for the last index layer.
    void *down_void = nullptr;
    curr->v.remove(k, down_void);
    curr_dl = static_cast<data_t *>(down_void);
    curr_dl->lock.acquire();
    if (layer == 0) {
      // NB: This check covers the edge case where
      // the loop above iterates zero times.
      curr->lock.release();
    } else {
      curr->lock.release_as_orphan();
    }

    // Finally, remove the element from the data layer.
    curr_dl->v.remove(k);
    curr_dl->lock.release_as_orphan();

    return true;
  }

  /// for_each() applies an elemental function f() to each element in the map.
  /// This implementation is linearizable.
  void for_each(std::function<void(const K &, V &, bool &)> f) {
    data_t *curr = &data_head;
    bool exit_flag = false;

    // Acquiring locks and traversing
    while (curr != nullptr && !exit_flag) {
      curr->lock.acquire();
      curr->v.for_each(f, exit_flag);
      curr = curr->next;
    }

    // Lock-releasing phase
    data_t *last = curr;
    curr = &data_head;
    while (curr != last) {
      data_t *next = curr->next;
      curr->lock.release();
      curr = next;
    }
  }

  /// Perform a range operation by applying f to the keys between from and to,
  /// inclusive
  void range(K const &from, K const &to,
             std::function<void(const K &, V &, bool &)> f) {
    init_context();

    // Validate input parameters
    if (from > to)
      return;

    data_t *first_locked = skip_to(from);
    data_t *curr = first_locked;
    bool done = false;
    bool exit_flag = false;

    // Process the first node.
    done = curr->v.range(from, to, f, exit_flag);
    curr = curr->next;

    // Process the nodes.
    while (curr != nullptr && !done && !exit_flag) {
      curr->lock.acquire();
      done = curr->v.range(from, to, f, exit_flag);
      curr = curr->next;
    }

    // Lock-releasing phase
    data_t *last = curr;
    curr = first_locked;
    while (curr != last) {
      data_t *next = curr->next;
      curr->lock.release();
      curr = next;
    }
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

  // Verify internal structure of skipvector. For debug purposes.
  bool verify() {
    using std::cout;
    using std::endl;

    // Verify that no hazard pointers are held (not allowed while quiescent.)
    int const hps = HP::count_reserved();
    if (hps != 0) {
      cout << "Verify failure: " << hps
           << " hazard pointer(s) held at verify time!" << endl;
      return fail();
    }

    // Verify each index layer against the next layer down.
    for (int layer = layers - 1; layer > 0; --layer) {
      verify_index<index_t>(layer, &(index_head.at(layer - 1)));
    }

    // Verify the last index layer against the data layer.
    verify_index<data_t>(0, &data_head);

    // Verify the data layer.
    const data_t *curr = &data_head;

    while (curr != nullptr) {
      int const curr_size = curr->v.get_size();
      data_t *next = curr->next;
      verify_lock<data_t>(curr);

      // Verify the node's vector, delegating to its own verify function.
      if (!curr->v.verify()) {
        return fail();
      }

      // Empty orphans are allowed, so skip any empty orphans until a proper
      // next is found.
      while (next != nullptr && next->v.get_size() == 0) {
        // But still check the sequence lock.
        verify_lock<data_t>(next);
        if (!next->is_orphan_seq()) {
          cout << "Verify failure: empty node not orphan!" << endl;
          return fail();
        }
        next = next->next;
      }

      // If next exists, make sure its first element is greater than our last
      if (next != nullptr) {
        // If curr is empty, make sure it is an orphan.
        // If it is not empty, then make sure curr and next are properly
        // ordered.
        if (curr_size == 0) {
          if (!curr->is_orphan_seq()) {
            cout << "Verify failure: curr is a empty non-orphan at data layer"
                 << endl;
            return fail();
          }
        } else if (curr->v.max_key() > next->v.first()) {
          cout << "Verify failure: nodes are improperly ordered in data layer."
               << endl;
          return fail();
        }
      }

      // Advance to the next node
      curr = next;
    }

    return true;
  }

  // SEQUENTIAL-ONLY size function
  [[nodiscard]] size_t get_size() const {
    size_t result = 0;
    data_t *curr = &data_head;

    while (curr != nullptr) {
      result += curr->v.get_size();
      curr = curr->next;
    }

    return result;
  }

  void verbose_analysis() const {
    using std::cout;
    using std::endl;

    cout << "Index vector type: " << index_head[0].v.get_name() << endl;
    cout << "Index vector exponent: " << IDX_EXP << endl;
    cout << "Index vector size: " << index_head[0].v.get_capacity() << endl;

    cout << "Data vector type: " << data_head.v.get_name() << endl;
    cout << "Data vector exponent: " << DATA_EXP << endl;
    cout << "Data vector size: " << data_head.v.get_capacity() << endl;

    cout << "Layers: " << layers << endl;
    cout << "Layer Array Capacity: " << MAX_LAYERS << endl;
    cout << "Merge threshold: " << merge_threshold << endl;
    cout << "Memory strategy: " << HP::get_name() << endl;

    // Print the number of vectors at each layer to check vertical balance
    cout << "Number of nodes at each layer: " << endl;

    // Number of elements on previous layer.
    size_t last_elts = 0;

    for (int i = layers - 1; i >= 0; --i) {
      size_t count = 0;
      size_t elements = 0;
      const index_t *curr = &(index_head.at(i));
      while (curr != nullptr) {
        elements += curr->v.get_size();
        ++count;
        curr = curr->next;
      }
      size_t const orphans = count - last_elts;
      cout << i << ": " << count << " nodes (" << orphans << " orphans)"
           << endl;
      last_elts = elements;
    }

    size_t count = 0;
    size_t elements = 0;
    const data_t *curr = &data_head;
    while (curr != nullptr) {
      elements += curr->v.get_size();
      ++count;
      curr = curr->next;
    }
    size_t const orphans = count - last_elts;
    cout << "D: " << count << " nodes (" << orphans << " orphans)" << endl;
    cout << "Elements: " << elements << endl;
  }

  // Dump the entire state of the skipvector for debug
  void dump() const {
    using std::cout;
    using std::endl;

    // Print contents of all index layers
    for (int i = layers - 1; i >= 0; --i) {
      cout << "Index " << std::hex << i << ": ";
      const index_t *curr = &(index_head.at(i));
      while (curr != nullptr) {
        curr->dump();
        curr = curr->next;
      }
      cout << endl;
    }

    cout << "Data: ";
    const data_t *curr = &data_head;
    while (curr != nullptr) {
      curr->dump();
      curr = curr->next;
    }
    cout << endl;
  }

  // Helper method for verify(). Makes sure the sequence lock is not locked
  // (which is not allowed while quiescent,) and returns a bool indicating if it
  // is an orphan (which is allowed.)
  template <typename T> bool verify_lock(const T *curr) const {
    uint64_t const lock = curr->lock.get_value();
    if (sv_lock::is_locked(lock)) {
      std::cout << "node " << curr << " had lock value of " << lock
                << " at verify time!" << std::endl;
      return fail();
    }
    return true;
  }

  // Helper method to verify(). Verifies nodes in the index layer.
  // The layer below may consist of index_t nodes or data_t nodes, so this
  // method is templated.
  template <typename T> bool verify_index(int layer, const T *trace) {
    using std::cout;
    using std::endl;

    index_t *curr = &(index_head.at(layer));

    while (curr != nullptr) {
      int const curr_size = curr->v.get_size();
      index_t *next = curr->next;
      verify_lock<index_t>(curr);

      // Verify the node's vector, delegating to its own verify function.
      if (!curr->v.verify()) {
        return fail();
      }

      curr->v.sort();

      // Trace along next layer to verify structural consistency with the down
      // pointers in this layer
      for (int j = 0; j < curr_size; ++j) {
        K key = curr->v.at(j);
        void *down_void = nullptr;
        curr->v.contains(key, down_void);
        T *down = static_cast<T *>(down_void);

        // Advance trace until current down pointer is found
        while (trace != down) {
          if (trace == nullptr) {
            cout << "Verify failure: Trace reached nullptr before finding key "
                 << +key << " at index layer " << layer << endl;
            return fail();
          }

          // If trace doesn't match the down pointer, it must be an orphan
          if (!trace->is_orphan_seq()) {
            cout << "Verify failure: Trace found non-orphan with start key "
                 << +trace->v.first() << " while looking for key " << +key
                 << " at index layer " << layer << endl;
            return fail();
          }

          trace = trace->next;
        }

        // Currently, trace == down, so advance it once more in preparation
        // for next loop
        trace = trace->next;

        // Down pointer must not point to an orphan
        if (down->is_orphan_seq()) {
          cout << "Verify failure: down pointer for key " << +key
               << " pointing to orphan in layer: " << layer << endl;
          return fail();
        }

        // Down pointer must not point to empty vector
        if (down->v.get_size() == 0) {
          cout << "Verify failure: down pointer for key " << +key
               << " pointing to empty vector in layer: " << layer << endl;

          return fail();
        }

        // Down pointer's first element must be sought key
        if (key != down->v.first()) {
          cout << "Verify failure: down pointer at layer " << layer
               << " with key " << +key
               << " points to vector with first element " << down->v.first()
               << endl;
          return fail();
        }
      }

      // Empty orphans are allowed, so skip any empty orphans until a proper
      // next is found.
      while (next != nullptr && next->v.get_size() == 0) {
        // But still check the sequence lock.
        verify_lock<index_t>(next);
        if (!next->is_orphan_seq()) {
          cout << "Verify failure: empty node not orphan!" << endl;
          return fail();
        }
        next = next->next;
      }

      // If next exists, make sure its first element is greater than our last
      if (next != nullptr) {
        // If curr is empty, make sure it is an orphan.
        if (curr_size == 0) {
          if (!curr->is_orphan_seq()) {
            cout << "Verify failure: curr is a empty non-orphan at " << layer
                 << endl;
            return fail();
          }
        } else if (curr->v.max_key() > next->v.first()) {
          // If curr is not empty, then make sure curr and next are properly
          // ordered.
          cout << "Verify failure: nodes are improperly ordered at layer "
               << layer << endl;
          return fail();
        }
      }

      // Advance to the next node
      curr = next;
    }

    // If we get here, the layer verified successfully.
    return true;
  }
};
