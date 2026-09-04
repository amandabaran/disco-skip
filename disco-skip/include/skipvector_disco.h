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
/// @param REMOTE_ADDR - The type of the remote address to store in each node
/// @param IDX_VEC    - A vector type that can hold pairs for the index layer.
/// @param DATA_VEC   - A vector type that can hold pairs for the data layer.
/// @param IDX_EXP    - The log_2 of the target chunk size for index vectors.
/// @param DATA_EXP   - The log_2 of the target chunk size for data vectors.
/// @param MAX_LAYERS - The maximum number of index layers.
/// @param HP         - The class responsible for managing hazard pointers.
template <typename K, typename V, typename REMOTE_ADDR,
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

  /// node_t is used for both the data layer and the index layer(s)
  template <typename T, int64_t EXP,
            template <typename, typename, size_t> typename VEC>
  struct node_t : public hp_deletable {
    /// A lock to protect this node.  sv_lock is a sequence lock with a
    /// few stolen bits
    sv_lock lock;

    /// Identifies the remote node this local node mirrors
    const REMOTE_ADDR remote_addr; // todo: do I want to store struct ver too to detect when structural changes occur? or unnec.?
    uint32_t cached_struct_ver;

    /// A pointer to the next node in this layer.
    rlx_atomic<node_t *> next;

    /// Minimum key stored in this data node
    K k_min; // todo: const? //! should I even have this field still?

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
    node_t() : remote_addr{}, cached_struct_ver(0), lock(false), next(nullptr) {}
    /// Constructor; creates node, and stitches it in after prev.
    /// Requires that prev is locked.
    node_t(node_t *prev, bool orphan, REMOTE_ADDR const& r_addr /*, uint32_t cached_struct_ver_*/) : 
            remote_addr(r_addr),  lock(orphan), v(), next(prev->next.load()) { //cached_struct_ver(cached_struct_ver_),
      prev->next = this;
    }

    //~node_t() override = default;
    virtual ~node_t() = default;

    /// Sequential code for checking if a node is an orphan
    ///
    /// NB: Concurrent methods should read the orphan bit from the seqlock
    [[nodiscard]] bool is_orphan_seq() const {
      return sv_lock::is_orphan(lock.get_value());
    }

    void dump() const {
      lock.dump();
    }

        template <bool CLEANUP>
    bool should_merge(double merge_threshold, node_t *next,
                      bool next_is_orphan) {
      // Non-orphans can never be merged.
      if (!next_is_orphan)
        return false;

      int const nextsize = static_cast<index_t*>(next)->v.get_size();

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
      index_t *zombie = static_cast<index_t*>(next.load());
      v.merge(&(zombie->v));
      next = zombie->next.load();
      zombie->lock.die();
    }

    /// Insert a K/V pair into this node
    ///
    /// NB: May split this node if it is full
    bool insert(const std::pair<const K, T> &pair, REMOTE_ADDR r_addr) {
      bool overfull = false;
      bool const result = v.insert(pair, overfull);
      if (overfull) {
        // Insert failed because the current node was too big,
        // so split it and make an orphan.
        // Note: the orphan's constructor will stitch itself in.
        auto *new_orphan = new node_t(this, true, r_addr);
        new_orphan->v.steal_half_and_insert(&v, pair);
        return true;
      }
      return result;
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

    REMOTE_ADDR remote_addr;
    K k_min;
    REMOTE_ADDR next_remote_addr;  // remote address of the sibling
    // Contents of the vector (for cache to install)
    VEC<K, REMOTE_ADDR, get_vector_size()> entries;  // (key, down_addr) // TODO: should this be the type of one of the vector classes provided, or "converted" to that later?
  };

  /// type of index nodes.  Since an index node can reference either another
  /// index node, or a data node, we use a generic void*.  Thus the map holds
  /// K/ptr pairs
  using index_t = node_t<void *, IDX_EXP, IDX_VEC>;

  /// type of data nodes.  A data node's vector holds k/v pairs
  using directory_t = node_t<REMOTE_ADDR, IDX_EXP, IDX_VEC>;

  /// The threshold at which to merge chunks of the skipvector
  const double merge_threshold;

  /// The number of index layers in the skipvector.
  /// This does not include the data layer.
  size_t const layers;

  /// Array of leftmost index nodes.
  /// layer_n at index n-1 (bc index level 0 has diff type)
  std::array<index_t, MAX_LAYERS - 1> index_head{};

  /// Leftmost data vector.
  directory_t directory_head{};

  /// Create a context for the thread, if one doesn't exist
  void init_context() const { HP::init_context(); }

  // layer is the "proper layer" (not the )
  bool is_head(void* node, int layer) {
    if (layer == 0) { 
      return static_cast<directory_t*>(node) == &directory_head;
    }
    return static_cast<index_t*>(node) == &index_head.at(layer-1);
  }

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
  //template <bool CLEANUP, typename T>
  template <typename T>
  bool check_next(T *&curr, uint64_t &curr_lock, K const &k, size_t layer) {
    // The fastest way out of this loop is when next is nullptr or curr's last
    // element is >= k.  Finding these early avoids taking a hazard pointer on
    // next or reading its seqlock.  If the /while/ condition fails, we will
    // return true.
    T *next = curr->next;
    K last = k;
    while (next != nullptr && (is_head(curr, layer) || !curr->v.last(last) || k > last)) {
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

      //!
      /*
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
      */

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
    while (next != nullptr) {
      // Take a hazard pointer on next, then make sure curr hasn't changed
      HP::take_next(next);
      if (!curr->lock.confirm_read(curr_lock)) {
        HP::drop_next();
        return false;
      }

      uint64_t next_lock = next->lock.begin_read();

      // At this point we know that we have a nonempty next. // todo: could we see an empty (now that not merging) or no?
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
      //! I don't think this if statement is necessary anymore since we do not attempt to merge anything
      // if (!curr->lock.confirm_read(curr_lock)) {
      //   HP::drop_next();
      //   return false;
      // }

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
  bool follow(index_t *curr, uint64_t &curr_lock, K const &k, T *&down, uint32_t layer) {
    // if check_next() fails, start over
    if (!check_next(curr, curr_lock, k, layer))
      return false;

    // Find down pointer in curr, confirm curr's sequence lock (and next's, if
    // next was read), and take a seqlock on down.
    void *down_void = nullptr;
    if (!is_head(curr, layer) && curr->v.find_lte(k, down_void)) {
      down = static_cast<T *>(down_void);
    }

    return reader_swap<T>(curr, curr_lock, down);
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
    assert(layers > 1 && layers <= MAX_LAYERS);

    // We use a single 64-bit random number on insert(), so make sure that's
    // enough for the chosen configuration.
    assert(DATA_EXP + (cfg->layers * IDX_EXP) <= 64);
  }

  /// Sequential-only destructor
  ~skipvector() {
    // First, free all index layer nodes BUT the leftmost ones.
    for (size_t i = 1; i < layers; ++i) {
      index_t *curr = index_head.at(i-1).next;
      while (curr != nullptr) {
        index_t *next = curr->next;
        delete curr;
        curr = next;
      }
    }

    // Free each node in data layer but the leftmost,
    // which was statically allocated
    directory_t *data_curr = directory_head.next;
    while (data_curr != nullptr) {
      directory_t *next = data_curr->next;
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
  REMOTE_ADDR locate_data(K const &k) {
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
      if (!follow(curr, curr_lock, k, down, layer)) {
        // Sequence lock check failed
        HP::drop_curr();
        goto top;
      }
      curr = down;
    }

    // Skip through the last index layer.
    directory_t *curr_dl = &directory_head;
    if (!follow(curr, curr_lock, k, curr_dl, 0)) {
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
    REMOTE_ADDR const r_addr = curr_dl->remote_addr;

    // Confirm curr's sequence lock.
    if (!curr_dl->lock.confirm_read(curr_lock)) {
      HP::drop_curr();
      goto top;
    }

    HP::drop_curr();

    return r_addr;
  }

  /// Gather prev info for every level <= height
  /// prev_addrs[] capacity is (height + 1), which covers index layers and data layer
  bool gather_prevs(K const &k, uint32_t const height, REMOTE_ADDR*& prev_addrs) {
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
      if (!follow(curr, curr_lock, k, down, layer)) {
        // Sequence lock check failed
        HP::drop_curr();
        goto top;
      }
      if (layer < height) {
        prev_addrs[layer+1] = curr->remote_addr;
      }
      curr = down;
    }

    // Skip through the last index layer.
    directory_t *curr_dl = &directory_head;
    if (!follow(curr, curr_lock, k, curr_dl, 0)) {
      // Sequence lock check failed
      HP::drop_curr();
      goto top;
    }

    if (layer < height) {
      prev_addrs[layer+1] = curr->remote_addr;
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
    prev_addrs[0] = curr_dl->remote_addr;

    // Confirm curr's sequence lock.
    if (!curr_dl->lock.confirm_read(curr_lock)) {
      HP::drop_curr();
      goto top;
    }

    HP::drop_curr();

    return true;
  }

  // Returns a node at `target_level` covering key k.
  // The node is protected by an HP in the curr slot.
  // On any sequence-lock verification failure, restarts internally.
  // //! HERE 9/1/26 - todo: apply the following function to mirror_insert_at_lvl, and then mirror_insert
  directory_t* descend_to_directory(uint64_t &target_lock, K const& k) {
    assert(layers >= 2);
  top:
    int layer = layers - 1;
    index_t* curr = &(index_head.at(layer-1)); // -1 again bc layer n corresponds to index n-1, since directory_head is not included in index_head array
    HP::take_first(curr);
    uint64_t curr_lock = curr->lock.begin_read();

    // Descend through index levels above target_level
    for (; layer > 1; --layer) {
      // At each level, find the correct node and follow the down pointer.
      index_t* down = &index_head.at(layer - 2);
      if (!follow(curr, curr_lock, k, down, layer)) {
        HP::drop_curr();
        goto top;
      }
      curr = down;
    }

    // From index level 1, descend to directory level 0
    directory_t* curr_0 = &directory_head;
    if (!follow(curr, curr_lock, k, curr_0, 0)) {
      HP::drop_curr();
      goto top;
    }

    // At level 0, walk right until finding the directory node covering k
    if (!check_next(curr_0, curr_lock, k, layer)) {
      HP::drop_curr();
      goto top;
    }

    target_lock = curr_lock;
    return curr_0;
    // Caller inherits the HP on curr, and the read lock context (curr_lock)
  }

  index_t* descend_to_index_level(uint64_t &target_lock, K const& k, uint target_level) { // target_level > 0
    assert(layers >= 2);
    assert(target_level >= 1);
    assert(target_level < layers);
  top:
    int layer = layers - 1;
    index_t* curr = &(index_head.at(layer-1)); // -1 again bc layer n corresponds to index n-1, since directory_head is not included in index_head array
    HP::take_first(curr);
    uint64_t curr_lock = curr->lock.begin_read();

    // Descend through index levels above target_level
    for (; layer > target_level; --layer) {
      // At each level, find the correct node and follow the down pointer.
      index_t* down = &index_head.at(layer - 2);
      if (!follow(curr, curr_lock, k, down, layer)) {
        HP::drop_curr();
        goto top;
      }
      curr = down;
    }

    // At target_level, walk right until we find the node covering k
    if (!check_next(curr, curr_lock, k, target_level)) {
      HP::drop_curr();
      goto top;
    }
    target_lock = curr_lock;
    return curr;
    // Caller inherits the HP on curr, and the read lock context (curr_lock)
  }

  /// Gather prev info for every level <= height
  /// prev_addrs[] capacity is (height + 1), which covers index layers and data layer
  /*
  std::vector<REMOTE_ADDR> collect_range_addrs(K const &k_from, K const &k_to) {
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
      if (!follow(curr, curr_lock, k_from, down)) {
        // Sequence lock check failed
        HP::drop_curr();
        goto top;
      }
      curr = down;
    }

    // Skip through the last index layer.
    directory_t *curr_dl = &directory_head;
    if (!follow(curr, curr_lock, k_from, curr_dl)) {
      // Sequence lock check failed
      HP::drop_curr();
      goto top;
    }

    // Finally, read the data layer, find node for k_from
    if (!check_next_dl(curr_dl, curr_lock, k_from)) {
      HP::drop_curr();
      goto top;
    }

    std::vector<REMOTE_ADDR> remote_addrs = {curr_dl->remote_addr};

    // staring with curr_dl, collect remote addresses in range of [k_from, k_to]
    directory_t *next_dl = curr_dl->next;
    while (next_dl != nullptr) {
      // Take a hazard pointer on next, then make sure curr hasn't changed
      HP::take_next(next_dl);
      if (!curr_dl->lock.confirm_read(curr_lock)) {
        HP::drop_all();
        goto top;
      }

      uint64_t next_lock = next_dl->lock.begin_read();
      if (k_to < next_dl->k_min) {
        // Next's min element is after k, so we have ruled out next.
        // Now we just need to check its sequence lock.
        // Return true if the check succeeds, false if it fails.
        if (!next_dl->lock.confirm_read(next_lock)) {
          HP::drop_all();
          goto top;
        }
        HP::drop_all();
        return remote_addrs;
      }

      remote_addrs.push_back(next_dl->remote_addr);
      if (!next_dl->lock.confirm_read(next_lock)) {
        HP::drop_all();
        goto top;
      }
      curr_dl = next_dl;
      curr_lock = next_lock;
      next_dl = curr_dl->next;
      HP::drop_curr();
    }

    HP::drop_curr();

    return remote_addrs;
  }
  */

  // Descends to the local node at `level` covering k, then splits it at k.
  // - The current local node keeps entries < k.
  // - A new local node is created holding K and entries > k.
  // - The new local node is stitched in as `curr->next`.
  // - The new local node's remote_addr is set to `new_remote_addr`.
  // - Returns the new local node (which will be the down_ptr for level+1).
  template <typename NodeT, typename ValueT>
  NodeT* mirror_split_at_level(
      K const& k, int level,
      ValueT const& down_ptr_k,      // remote_addr for level 0, local ptr for level ≥ 1
      REMOTE_ADDR const& new_remote_addr) {

  retry:
    // Descend read-only to find the local node covering k at this level.
    uint64_t curr_lock;
    NodeT* curr;
    if constexpr (std::is_same_v<NodeT, directory_t>) {
      curr = descend_to_directory(curr_lock, k);
    } else {
      curr = descend_to_index_level(curr_lock, k, level);
    }

    // Try to upgrade to write lock. On failure, restart.
    if (!curr->lock.try_upgrade(curr_lock)) {
      HP::drop_curr();
      goto retry;
    }

    // Idempotency check: has another thread already installed this split?
    // If a local node whose remote_addr matches new_remote_addr
    // already exists as curr->next (or somewhere), skip.
    // Do not need hp because I have lock on curr
    NodeT* existing_next = curr->next;
    if (existing_next != nullptr && 
        existing_next->remote_addr == new_remote_addr) {
      // Already installed by someone else. Nothing to do here.
      curr->lock.release_unchanged();
      HP::drop_curr();
      return existing_next;
    }

    // Create the new local node
    NodeT* new_local = new NodeT(curr, false, new_remote_addr);
    new_local->lock.acquire();
    // Move entries > k from curr to new_local
    new_local->v.split_insert(&curr->v, {k, down_ptr_k});
    new_local->lock.release();
    curr->lock.release();
    HP::drop_curr();

    return new_local;
  }

  // Descends to the local node at `level` covering k, and inserts (k, down_ptr).
  // If the insert causes overflow, splits into an orphan.
  // - orphan_remote_addr is used only if a split occurs; if no split, ignored.
  // - down_ptr_k: what K's entry's value should be.
  template <typename NodeT, typename ValueT>
  void mirror_insert_at_top_level(
      K const& k, int level,
      ValueT const& down_ptr,
      REMOTE_ADDR const& orphan_remote_addr) {  // may be null if no remote split expected

  retry:
    uint64_t curr_lock;
    NodeT* curr;
    if constexpr (std::is_same_v<NodeT, directory_t>) {
      curr = descend_to_directory(curr_lock, k);
    } else {
      curr = descend_to_index_level(curr_lock, k, level);
    }

    // todo: if curr is head, don't want to insert to it
    if (is_head())

    if (!curr->lock.try_upgrade(curr_lock)) {
      HP::drop_curr();
      goto retry;
    }

    // Insert (k, down_ptr_k) into curr
    if (!curr->insert({k, down_ptr}, orphan_remote_addr)) { // todo: is it possible that orphan_remote_addr is null, but a split happens anyway?
      // False if already exists
      // Someone else already updated the cache, safe to return
      curr->lock.release_unchanged();
      HP::drop_curr();
      return;
    }

    curr->lock.release();
    HP::drop_curr();
  }

  // todo: consider how height correlates to level here, update below (level < height in for loop, and elsewhere) as needed
  void mirror_insert(K const& k, int height,
                    REMOTE_ADDR &new_remote_data_addr,
                    std::array<REMOTE_ADDR, MAX_LAYERS> const& new_remote_index_addrs) {
    init_context();
    assert(height > 0);

    int const top_level = height - 1;
    index_t* new_local_below = nullptr;  // unused at level 0
    directory_t* new_local_below_dir = nullptr;
    
    // Levels 0..top_level-1: split-at-K at each level
    for (int level = 0; level < top_level; ++level) {
      if (level == 0) {
        new_local_below_dir = mirror_split_at_level<directory_t>(k, 0, new_remote_data_addr, new_remote_index_addrs[0]);
      } else if (level == 1) {
        new_local_below = mirror_split_at_level<index_t>(k, level, new_local_below_dir, new_remote_index_addrs[level]);
      } else {
        new_local_below = mirror_split_at_level<index_t>(k, level, new_local_below, new_remote_index_addrs[level]);
      }
    }

    // Top level: insert into existing
    if (top_level == 0) {
      // Height 1: no splits happened; top-level insert with remote data addr
      mirror_insert_at_top_level<directory_t>(k, 0, new_remote_data_addr, new_remote_index_addrs[0]);
    } else if (top_level == 1) {
      // Height 2: pass the newly created directory node from level 0
      mirror_insert_at_top_level<index_t>(k, top_level, new_local_below_dir, new_remote_index_addrs[top_level]);
    } else {
      // Height >= 3: pass the newly created index node from the previous level
      mirror_insert_at_top_level<index_t>(k, top_level, new_local_below, new_remote_index_addrs[top_level]);
    }
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
    directory_t *curr_dl = &directory_head;

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
          curr_dl = static_cast<directory_t *>(down);

      } else {
        // No appropriate down pointer was found,
        // so start at the leftmost node at the next layer.
        if (layer > 0)
          down_idx = &(index_head.at(layer - 1));
        else
          curr_dl = &directory_head;
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
      auto *new_orphan = new directory_t(curr_dl, true);
      new_orphan->v.steal_half(&(curr_dl->v));
    }

    auto *new_data_node = new directory_t(curr_dl, false);
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
      if (layer == 1) {
        verify_index<directory_t>(layer-1, &directory_head);
      } else {
        verify_index<index_t>(layer-1, &(index_head.at(layer - 2)));
      }
    }

    // Verify the data layer.
    const directory_t *curr = &directory_head;
    curr = curr->next;

    while (curr != nullptr) {
      int const curr_size = curr->v.get_size();
      directory_t *next = curr->next;
      verify_lock<directory_t>(curr);

      // Verify the node's vector, delegating to its own verify function.
      if (!curr->v.verify()) {
        return fail();
      }

      // Empty orphans are allowed, so skip any empty orphans until a proper
      // next is found.
      while (next != nullptr && next->v.get_size() == 0) {
        // But still check the sequence lock.
        verify_lock<directory_t>(next);
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
    directory_t *curr = &directory_head;

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

    cout << "Data vector type: " << directory_head.v.get_name() << endl;
    cout << "Data vector exponent: " << DATA_EXP << endl;
    cout << "Data vector size: " << directory_head.v.get_capacity() << endl;

    cout << "Layers: " << layers << endl;
    cout << "Layer Array Capacity: " << MAX_LAYERS << endl;
    cout << "Merge threshold: " << merge_threshold << endl;
    cout << "Memory strategy: " << HP::get_name() << endl;

    // Print the number of vectors at each layer to check vertical balance
    cout << "Number of nodes at each layer: " << endl;

    // Number of elements on previous layer.
    size_t last_elts = 0;

    for (int i = layers - 2; i >= 0; --i) {
      size_t count = 0;
      size_t elements = 0;
      const index_t *curr = &(index_head.at(i));
      while (curr != nullptr) {
        elements += curr->v.get_size();
        ++count;
        curr = curr->next;
      }
      size_t const orphans = count - last_elts;
      cout << i+1 << ": " << count << " nodes (" << orphans << " orphans)"
           << endl;
      last_elts = elements;
    }

    size_t count = 0;
    size_t elements = 0;
    const directory_t *curr = &directory_head;
    while (curr != nullptr) {
      elements += curr->v.get_size();
      ++count;
      curr = curr->next;
    }
    size_t const orphans = count - last_elts;
    cout << "D: " << count << " nodes (" << orphans << " orphans) \t (Directory level)" << endl;
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
    const directory_t *curr = &directory_head;
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
  // The layer below may consist of index_t nodes or directory_t nodes, so this
  // method is templated.
  template <typename T> bool verify_index(int layer_idx, const T *trace) {
    using std::cout;
    using std::endl;

    index_t *curr = &(index_head.at(layer_idx)); // layer n's index into index_head is at n-1
    curr = curr->next;

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
                 << +key << " at index layer " << layer_idx+1 << endl;
            return fail();
          }

          // If trace doesn't match the down pointer, it must be an orphan
          if (!trace->is_orphan_seq()) {
            cout << "Verify failure: Trace found non-orphan with start key "
                 << +trace->v.first() << " while looking for key " << +key
                 << " at index layer " << layer_idx+1 << endl;
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
               << " pointing to orphan in layer: " << layer_idx+1 << endl;
          return fail();
        }

        // Down pointer must not point to empty vector
        if (down->v.get_size() == 0) {
          cout << "Verify failure: down pointer for key " << +key
               << " pointing to empty vector in layer: " << layer_idx+1 << endl;

          return fail();
        }

        // Down pointer's first element must be sought key
        if (key != down->v.first()) {
          cout << "Verify failure: down pointer at layer " << layer_idx+1
               << " with key " << +key
               << " points to vector with first element " << down->v.first()
               << endl;
          return fail();
        }
      }
      /// At this point, the vector's down pointers have been verified

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
            cout << "Verify failure: curr is a empty non-orphan at " << layer_idx+1
                 << endl;
            return fail();
          }
        } else if (curr->v.max_key() > next->v.first()) {
          // If curr is not empty, then make sure curr and next are properly
          // ordered.
          cout << "Verify failure: nodes are improperly ordered at layer "
               << layer_idx+1 << endl;
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
