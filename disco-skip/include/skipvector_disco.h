#pragma once

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstring>
#include <functional>
#include <pthread.h>
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

    /// Identifies the remote node this local node mirrors.
    ///
    /// NB: This is write-once. Nodes created during a mirror update receive
    /// their address at construction and never change it. The only exception
    /// is a leftmost (head) node: heads are constructed before the remote
    /// leftmost addresses are known, so they are filled in exactly once by
    /// set_head_remote_addrs() during bootstrap, before any concurrent use.
    /// (Hence not const.)
    ///
    /// A default-constructed (null) address means "this local node has no
    /// known remote counterpart" -- either a head that has not been bootstrapped
    /// or a local-only orphan created because a local vector hit its capacity
    /// when the remote had not split. Callers must treat null as a cache miss.
    REMOTE_ADDR remote_addr;
    uint32_t cached_struct_ver;

    /// A pointer to the next node in this layer.
    rlx_atomic<node_t *> next;

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
    /// ever used to create leftmost (head) nodes, which are always orphans.
    ///
    /// NB: Heads MUST be orphans. A head is never the target of a down pointer
    /// from the layer above, and verify_index() only tolerates an unreferenced
    /// node in a layer if that node is an orphan.
    node_t() : lock(true), remote_addr{}, cached_struct_ver(0), next(nullptr) {}

    /// Constructor; creates node, and stitches it in after prev.
    /// Requires that prev is locked.
    node_t(node_t *prev, bool orphan, REMOTE_ADDR const &r_addr)
        : lock(orphan), remote_addr(r_addr), cached_struct_ver(0),
          next(prev->next.load()), v() {
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
      node_t *zombie = next.load();
      v.merge(&(zombie->v));
      next = zombie->next.load();
      zombie->lock.die();
    }

    /// Insert a K/V pair into this node
    ///
    /// NB: May split this node if it is full
    ///
    /// r_addr is used only if a split occurs, as the remote address of the
    /// newly created orphan. It may legitimately be null: local vector
    /// capacity is a purely local property, so a local vector can overflow at
    /// a point where the remote did not split and therefore reported no new
    /// remote address. The resulting orphan is then a local-only node with no
    /// remote counterpart, which is sound -- it is still reachable by walking
    /// /next/, and any caller that reads its null remote_addr must treat it as
    /// a cache miss. We deliberately still perform the split rather than
    /// skipping it, because skipping would leave the down pointers created at
    /// the levels below this one without a parent.
    ///
    /// @returns true if the pair was installed (with or without a split),
    ///          false if this node already contained the key
    ///
    /// If a split occurs and new_orphan_out is non-null, the newly created
    /// orphan is reported through it. Callers that can promote the orphan into
    /// the layer above should do so -- an unpromoted orphan is reachable only
    /// by walking /next/, and letting them accumulate is what turns a layer
    /// into a linked list.
    bool insert(const std::pair<const K, T> &pair, REMOTE_ADDR const &r_addr,
                node_t **new_orphan_out) {
      bool overfull = false;
      bool const result = v.insert(pair, overfull);
      if (overfull) {
        // Insert failed because the current node was too big,
        // so split it and make an orphan.
        // Note: the orphan's constructor will stitch itself in.
        auto *new_orphan = new node_t(this, true, r_addr);
        new_orphan->v.steal_half_and_insert(&v, pair);
        if (new_orphan_out != nullptr)
          *new_orphan_out = new_orphan;
        return true;
      }
      return result;
    }

    bool insert(const std::pair<const K, T> &pair, REMOTE_ADDR const &r_addr) {
      return insert(pair, r_addr, nullptr);
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

  /// Set once set_head_remote_addrs() has run. Until then the heads carry null
  /// remote addresses, which is safe but silently degrades every gather_prevs()
  /// that lands on a head into a per-level miss -- so it is worth being able to
  /// ask, rather than discovering it as unexplained remote traffic.
  bool heads_bootstrapped_ = false;

  /// Create a context for the thread, if one doesn't exist
  void init_context() const { HP::init_context(); }

  /// Is /node/ the leftmost (head) node of /layer/?
  ///
  /// /layer/ is the "proper layer": 0 is the directory layer, and index layer
  /// n lives at index_head[n-1].
  ///
  /// NB: Heads participate in traversal like any other node -- they hold
  /// entries and are searched normally -- so the traversal path does not need
  /// this. It is here for callers that need to distinguish "the region left of
  /// every remote boundary" from an ordinary node, e.g. to decide whether a
  /// remote_addr is meaningful.
  bool is_head(void *node, int layer) const {
    if (layer == 0) {
      return static_cast<directory_t *>(node) == &directory_head;
    }
    return static_cast<index_t *>(node) == &index_head.at(layer - 1);
  }

  /// A thread-identity value usable as a PRNG seed.
  ///
  /// NB: pthread_t is an integer on Linux but an opaque pointer on macOS, so
  /// copy its representation rather than assuming it converts to an integer.
  static uint64_t thread_seed() {
    pthread_t const self = pthread_self();
    static_assert(sizeof(self) <= sizeof(uint64_t),
                  "pthread_t does not fit in a uint64_t seed");
    uint64_t bits = 0;
    std::memcpy(&bits, &self, sizeof(self));
    return bits;
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
        lehmer64_seed(thread_seed());
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
  bool check_next(T *&curr, uint64_t &curr_lock, K const &k) {
    // The fastest way out of this loop is when next is nullptr or curr's last
    // element is >= k.  Finding these early avoids taking a hazard pointer on
    // next or reading its seqlock.  If the /while/ condition fails, we will
    // return true.
    //
    // NB: An empty node (including an as-yet-unpopulated head) makes
    // v.last() return false, which forces us to advance -- so heads need no
    // special case here.
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

      // NB: The merge block below stays disabled. It was evaluated (2026-09) and
      // left off deliberately; if you re-enable it, read this first.
      //
      //  - It does not solve the local-bloat problem it looks like it solves.
      //    should_merge() bails on non-orphans, but structure the remote side
      //    removed and the cache never heard about leaves stale *non-orphans*.
      //  - It is mildly divergent as written. An orphan created by the overflow
      //    path in node_t::insert can carry a real remote address, and absorbing
      //    it discards the knowledge that a second remote node exists. That
      //    degrades gather_prevs(), though not locate_data(), which reads entry
      //    values rather than node identities. A safe version must merge only
      //    orphans whose remote_addr is null, plus empty ones -- which would
      //    newly require REMOTE_ADDR to be null-testable, something the cache
      //    otherwise never does.
      //  - check_next() is the shared read path. Merging here makes every reader
      //    a writer, bumping seqlocks and forcing concurrent descenders to
      //    restart. That is what the original CLEANUP template parameter existed
      //    to avoid. It belongs on the update paths, which already hold write
      //    locks and run far less often.
      //  - The payoff is small: the directory-level orphan fraction measures a
      //    stable ~21% across a 64x range of workload size, i.e. a bounded
      //    constant, and a few extra cache lines is nothing against an RDMA.
      //
      // NB: check_next_sequential() below still merges, so insert_seq() does
      // compact. Only the concurrent path is disabled.
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
  ///
  /// If prev_addr_out is non-null, the remote address of the node we descend
  /// *from* is written there. This has to happen inside follow(): after
  /// follow() returns, reader_swap() has already dropped the hazard pointer on
  /// curr, so the caller can no longer safely dereference it.
  template <typename T>
  bool follow(index_t *curr, uint64_t &curr_lock, K const &k, T *&down,
              REMOTE_ADDR *prev_addr_out = nullptr) {
    // if check_next() fails, start over
    if (!check_next(curr, curr_lock, k))
      return false;

    // Find down pointer in curr, confirm curr's sequence lock (and next's, if
    // next was read), and take a seqlock on down.
    //
    // NB: find_lte() returns false on an empty vector and when no entry is
    // <= k, in which case /down/ keeps the caller-supplied default: the head
    // of the layer below. So a key that is left of every boundary at this
    // layer descends via heads, which is exactly right.
    void *down_void = nullptr;
    if (curr->v.find_lte(k, down_void)) {
      down = static_cast<T *>(down_void);
    }

    // Snapshot curr's remote address if the caller asked for it. reader_swap()
    // confirms curr's sequence lock, which validates this read along with the
    // down pointer.
    if (prev_addr_out != nullptr)
      *prev_addr_out = curr->remote_addr;

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

  /// Bootstrap the head nodes' remote addresses.
  ///
  /// A local head node mirrors the remote leftmost node at its layer: the node
  /// covering [-inf, first remote boundary). Those remote nodes are fixed for
  /// the lifetime of the structure -- a head is never unlinked, and a remote
  /// split of the leftmost node maps to "head keeps entries < K, a new local
  /// node after head takes entries >= K", which preserves the correspondence.
  /// So their addresses can be installed once, here.
  ///
  /// addrs[0] is the remote directory-layer leftmost node; addrs[n] is the
  /// remote leftmost node of index layer n.
  ///
  /// SEQUENTIAL-ONLY: must be called before any concurrent use. Until it is
  /// called, heads carry null remote addresses, which callers reading
  /// remote_addr must treat as a cache miss.
  ///
  /// Leaving this uncalled is safe but makes any gather_prevs() that lands on a
  /// head report a miss for that level, so call it as part of bootstrap.
  ///
  /// Write-once: calling it twice is a bug, since a head's correspondence to
  /// its remote leftmost node is fixed for the life of the structure.
  void set_head_remote_addrs(std::array<REMOTE_ADDR, MAX_LAYERS> const &addrs) {
    assert(!heads_bootstrapped_ &&
           "set_head_remote_addrs() is write-once; heads never move");
    directory_head.remote_addr = addrs.at(0);
    for (size_t n = 1; n < layers; ++n) {
      index_head.at(n - 1).remote_addr = addrs.at(n);
    }
    heads_bootstrapped_ = true;
  }

  /// Have the head remote addresses been installed? Lets the orchestrator (or a
  /// test) confirm bootstrap actually ran, instead of inferring it from a
  /// higher-than-expected miss rate.
  [[nodiscard]] bool heads_bootstrapped() const { return heads_bootstrapped_; }

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

  /// Search for a key in the skipvector.
  ///
  /// NB: A directory node carries two unrelated remote addresses. Its own
  /// remote_addr identifies the remote level-0 *index* node it mirrors; its
  /// vector entries map keys to remote *data* node addresses. This returns the
  /// latter, so the caller can read the data node directly instead of reading
  /// the remote index node and resolving k against its entries.
  ///
  /// @returns the remote address of the data node that may contain k, or a
  ///          null address if no covering entry is cached, which the caller
  ///          must treat as a miss and resolve by remote traversal (then feed
  ///          back through mirror_reconcile).
  REMOTE_ADDR locate_data(K const &k) {
    init_context(); // hazard pointers

    while (true) {
      uint64_t curr_lock = 0;
      directory_t *curr_dl = descend_to_directory(curr_lock, k);

      // Read into a local before the final verification: if the verification
      // fails we must not have already handed a torn value to the caller.
      //
      // NB: find_lte() leaves r_addr untouched when the vector is empty or
      // holds no entry <= k, so a miss falls through as the null address.
      // This read is optimistic, exactly as in follow(); the confirm_read()
      // below is what makes it meaningful.
      REMOTE_ADDR r_addr{};
      curr_dl->v.find_lte(k, r_addr);

      bool const ok = curr_dl->lock.confirm_read(curr_lock);
      HP::drop_curr();
      if (ok)
        return r_addr;
    }
  }

  /// One level of what a remote descent saw: the node covering the sought key
  /// at that level, and its remote address. The orchestrator already holds this
  /// for every level it descended through -- with passive memory servers the
  /// client performs the descent itself and must read each node to follow the
  /// pointer down -- so collecting it costs no extra RDMA.
  struct path_step {
    K k_min{};             ///< k_min of the remote node covering the sought key
    REMOTE_ADDR addr{};    ///< that node's remote address

    /// LEVEL 0 ONLY: the value of that node's first entry, i.e. the remote
    /// address of the data node whose k_min is /k_min/.
    ///
    /// Needed because creating a local directory node with minimum /k_min/
    /// requires a value for that entry, and at level 0 the value is a remote
    /// address only the descent has seen. At levels >= 1 the value is a local
    /// node pointer, which the cache resolves for itself, so this is unused
    /// there.
    REMOTE_ADDR first_down{};
  };

private:
  /// Whether an index-layer node that overflows during reconcile has its
  /// split-off orphan promoted into the layer above.
  ///
  /// Off by default. Promotion invents a boundary at a key whose remote height
  /// may be too low to deserve one, which is a deviation from mirroring that
  /// the faithful path below does not need. Left compiled in so the residual
  /// index-layer orphan rate can be measured with it on and off.
  static constexpr bool PROMOTE_INDEX_ORPHANS = false;


  /// Promote one orphan into the layer above, and report any orphan that
  /// promotion created there in turn.
  ///
  /// @returns the new orphan at parent_level if the parent overflowed, else
  ///          nullptr. Returns nullptr without acting if there is no layer
  ///          above, if the child is empty, or if the parent layer already
  ///          routes the child's key.
  template <typename ChildT>
  index_t *promote_one(ChildT *child, int child_level, path_step const *path,
                       uint32_t levels) {
    int const parent_level = child_level + 1;
    if (parent_level >= static_cast<int>(layers))
      return nullptr; // nothing above the top layer to route from

    // Read the child's minimum -- the key that will route to it.
    //
    // NB: safe without holding the child's lock, because a node's minimum never
    // decreases. A descent only lands on a node whose first key is <= the sought
    // key, so no smaller key can be inserted; and a split leaves the lower part
    // behind. It can only grow by losing its upper half, which does not move the
    // minimum.
    K child_min{};
    while (true) {
      uint64_t const l = child->lock.begin_read();
      if (child->v.get_size() == 0)
        return nullptr; // nothing to route to yet
      child_min = child->v.first();
      if (child->lock.confirm_read(l))
        break;
    }

    // A node created by overflow at parent_level covers a sub-range of the node
    // it split from, so the remote node covering the sought key at that level
    // also covers it. Null above the path we were given.
    REMOTE_ADDR const parent_orphan_addr =
        (static_cast<uint32_t>(parent_level) < levels) ? path[parent_level].addr
                                                       : REMOTE_ADDR{};

    index_t *made = nullptr;
    bool installed = false;
    while (true) {
      uint64_t curr_lock = 0;
      index_t *curr = descend_to_index_level(curr_lock, child_min, parent_level);
      if (!curr->lock.try_upgrade(curr_lock)) {
        HP::drop_curr();
        continue;
      }
      installed = curr->insert({child_min, static_cast<void *>(child)},
                               parent_orphan_addr, &made);
      curr->lock.release_changed_if(installed);
      HP::drop_curr();
      break;
    }

    if (!installed) {
      // Some other node at parent_level already routes this key. Leave the
      // child an orphan rather than creating a second down pointer for it.
      return nullptr;
    }

    // The child now has a parent, so it must stop being an orphan --
    // verify_index() rejects a down pointer that targets one.
    child->lock.acquire();
    if (child->lock.is_orphan())
      child->lock.release_and_adopt();
    else
      child->lock.release_unchanged();

    return made;
  }

  /// Make a newly split-off orphan reachable by descent rather than only by
  /// walking /next/, recursing while each promotion overflows the layer above.
  ///
  /// The first hop may cross node types (directory -> index); everything above
  /// is index-to-index, which is why this is a recurse-then-loop rather than a
  /// single loop.
  template <typename ChildT>
  void promote_orphan(ChildT *child, int child_level, path_step const *path,
                      uint32_t levels) {
    index_t *next_orphan = promote_one(child, child_level, path, levels);
    int level = child_level + 1;
    while (next_orphan != nullptr) {
      next_orphan = promote_one(next_orphan, level, path, levels);
      ++level;
    }
  }

public:
  /// Repair the cache after locate_data() reported a miss, or after a remote
  /// read showed a k_min mismatch (C4).
  ///
  /// Does two things, and both matter:
  ///
  /// 1. Installs the routing entry for the data node. Level-0 entries are
  ///    purely additive -- remote node addresses are stable, since CoW replaces
  ///    a node's vector and updates its offset while a split leaves the
  ///    original node holding its address and its lower range -- so a cached
  ///    entry never becomes wrong, only missing. A bare insert suffices; no
  ///    invalidation, no upsert, and repeating the call is a no-op.
  ///
  /// 2. Splits the local structure at the remote boundaries the descent
  ///    actually saw. This is what keeps the cache from degenerating: entries
  ///    accumulate at level 0 while node *boundaries* would otherwise arrive
  ///    only via mirror_insert(), so a read-mostly client would fill the
  ///    directory with unindexed overflow nodes -- measured at 1771 directory
  ///    nodes, all orphans, every index layer still empty, which is worse than
  ///    having no cache at all.
  ///
  /// Step 2 installs only boundaries the remote really has, so the local
  /// partition stays a coarsening of the remote one and never invents a
  /// boundary of its own. It also repairs the coarse case: a local node that
  /// recorded a boundary as an ordinary entry, because some other client
  /// created it, gets split there and stops spanning two remote nodes.
  ///
  /// How far up it can go is limited by what one descent sees. The covering
  /// boundary is coarser at each level -- for k = 350 the level-0 node may
  /// start at 300 while the level-1 node starts at 100 -- so the chain hanging
  /// below a higher boundary passes through nodes this descent never read. We
  /// therefore climb only while the same key is a boundary at the next level
  /// up, and otherwise install the entry and stop. Later reconciles fill in the
  /// rest: path[0].k_min equals path[1].k_min whenever the key falls in the
  /// first sub-node, roughly one lookup in TARGET_IDX_RATIO.
  ///
  /// @param data_k_min  k_min of the remote data node covering the sought key
  /// @param data_addr   that node's remote address
  /// @param path        what the descent saw, path[L] describing level L
  /// @param levels      number of valid entries in path; 0 for entry repair only
  void mirror_reconcile(K const &data_k_min, REMOTE_ADDR const &data_addr,
                        path_step const *path, uint32_t levels) {
    init_context();

    REMOTE_ADDR const dir_orphan_addr =
        (levels > 0) ? path[0].addr : REMOTE_ADDR{};

    // 1. The data routing entry. Always safe, always useful.
    install_data_entry(data_k_min, data_addr, dir_orphan_addr);

    if (levels == 0 || path == nullptr)
      return;

    // 2. Line the local partition up with the remote one, bottom-up.
    K const b = path[0].k_min;
    directory_t *d = mirror_split_at_level<directory_t>(b, 0, path[0].first_down,
                                                        path[0].addr);
    if (d == nullptr)
      return;

    // Climb while b is a boundary at the next level too. Creating the node with
    // minimum b at level L installs (b -> child) as its first entry, which is
    // what gives /child/ its parent.
    void *child = d;
    uint32_t L = 1;
    uint32_t const top =
        std::min<uint32_t>(levels, static_cast<uint32_t>(layers));
    while (L < top && path[L].k_min == b) {
      index_t *n = mirror_split_at_level<index_t>(b, static_cast<int>(L), child,
                                                  path[L].addr);
      if (n == nullptr)
        break; // could not build this level; child still needs a parent
      child = n;
      ++L;
    }

    // Whatever we stopped on is a non-orphan with no parent yet, and
    // verify_index() requires every non-orphan to be the target of a down
    // pointer. Install its entry one level up. That is still faithful: b is a
    // boundary at level L-1, so the remote node covering b at level L really
    // does hold an entry keyed b.
    //
    // Nothing is needed above the top layer -- there is no layer to route from,
    // and verify() does not require parents there.
    if (L < layers) {
      install_index_entry(static_cast<int>(L), b, child,
                          (L < levels) ? path[L].addr : REMOTE_ADDR{});
    }
  }

private:
  /// Insert one (key -> data address) routing entry at level 0.
  void install_data_entry(K const &key, REMOTE_ADDR const &addr,
                          REMOTE_ADDR const &orphan_addr) {
    while (true) {
      uint64_t curr_lock = 0;
      directory_t *curr = descend_to_directory(curr_lock, key);
      if (!curr->lock.try_upgrade(curr_lock)) {
        HP::drop_curr();
        continue;
      }
      directory_t *made = nullptr;
      bool const changed = curr->insert({key, addr}, orphan_addr, &made);
      curr->lock.release_changed_if(changed);
      HP::drop_curr();
      if (made != nullptr && PROMOTE_INDEX_ORPHANS)
        promote_orphan(made, 0, nullptr, 0);
      return;
    }
  }

  /// Insert one (key -> local node) routing entry at an index level.
  void install_index_entry(int level, K const &key, void *down,
                           REMOTE_ADDR const &orphan_addr) {
    while (true) {
      uint64_t curr_lock = 0;
      index_t *curr = descend_to_index_level(curr_lock, key, level);
      if (!curr->lock.try_upgrade(curr_lock)) {
        HP::drop_curr();
        continue;
      }
      index_t *made = nullptr;
      bool const changed = curr->insert({key, down}, orphan_addr, &made);
      curr->lock.release_changed_if(changed);
      HP::drop_curr();
      if (made != nullptr && PROMOTE_INDEX_ORPHANS)
        promote_orphan(made, level, nullptr, 0);
      return;
    }
  }

public:
  /// Convenience overload for callers with no descent path. Repairs the level-0
  /// entry but cannot give promoted nodes a remote address, so they come out
  /// null and read as cache misses.
  void mirror_reconcile(K const &data_k_min, REMOTE_ADDR const &data_addr) {
    mirror_reconcile(data_k_min, data_addr, nullptr, 0);
  }

  /// Visit every node at /level/, head first, calling
  /// f(minimum_key, is_orphan, entry_count). The head is reported with a
  /// default-constructed key when it is empty.
  ///
  /// SEQUENTIAL-ONLY. Exposed so a test can check the property that matters for
  /// mirroring fidelity: every node that something points down to must have a
  /// minimum that is a real remote boundary.
  template <typename F> void for_each_node(int level, F &&f) const {
    auto visit = [&f](auto const *c) {
      K const m = c->v.get_size() > 0 ? c->v.first() : K{};
      f(m, c->is_orphan_seq(), c->v.get_size());
    };
    if (level == 0) {
      for (const directory_t *c = &directory_head; c != nullptr; c = c->next)
        visit(c);
    } else {
      for (const index_t *c = &index_head.at(level - 1); c != nullptr;
           c = c->next)
        visit(c);
    }
  }

  /// Number of local nodes at /level/, 0 being the directory layer.
  /// SEQUENTIAL-ONLY. Exposed for tests and for the local-to-remote node ratio
  /// metric, which is what detects cache bloat.
  [[nodiscard]] size_t node_count(int level) const {
    size_t n = 0;
    if (level == 0) {
      for (const directory_t *c = &directory_head; c != nullptr; c = c->next)
        ++n;
    } else {
      for (const index_t *c = &index_head.at(level - 1); c != nullptr;
           c = c->next)
        ++n;
    }
    return n;
  }

  /// Gather the remote address of the node covering k at each level the key
  /// will occupy.
  ///
  /// A key of height h occupies levels 0..h-1 (level 0 being the directory
  /// layer), matching mirror_insert(). So prev_addrs[] must have /height/
  /// entries, and prev_addrs[L] receives the address for level L.
  ///
  /// Any entry may come back null, meaning the covering node at that level has
  /// no known remote counterpart; the caller must treat that as a cache miss
  /// for that level.
  bool gather_prevs(K const &k, uint32_t const height, REMOTE_ADDR *prev_addrs) {
    init_context(); // hazard pointers

    while (true) {
      // Start from the head node (leftmost node in the topmost index layer.)
      // NB: index layer n lives at index_head[n-1].
      int layer = static_cast<int>(layers) - 1;
      index_t *curr = &index_head.at(layer - 1);

      // Read head node's sequence lock.
      HP::take_first(curr);
      uint64_t curr_lock = curr->lock.begin_read();

      // Descend index layers layers-1 .. 2, recording prevs on the way.
      bool restart = false;
      for (; layer >= 2; --layer) {
        // If follow() doesn't find a suitable down pointer, default to the
        // next index layer's head.
        index_t *down = &index_head.at(layer - 2);
        REMOTE_ADDR *out = (static_cast<uint32_t>(layer) < height)
                               ? &prev_addrs[layer]
                               : nullptr;
        if (!follow(curr, curr_lock, k, down, out)) {
          // Sequence lock check failed
          HP::drop_curr();
          restart = true;
          break;
        }
        curr = down;
      }
      if (restart)
        continue;

      // From index layer 1, descend to the directory layer.
      directory_t *curr_dl = &directory_head;
      REMOTE_ADDR *out = (1u < height) ? &prev_addrs[1] : nullptr;
      if (!follow(curr, curr_lock, k, curr_dl, out)) {
        HP::drop_curr();
        continue;
      }

      // Walk right at the directory layer to the node covering k.
      if (!check_next(curr_dl, curr_lock, k)) {
        HP::drop_curr();
        continue;
      }

      prev_addrs[0] = curr_dl->remote_addr;

      bool const ok = curr_dl->lock.confirm_read(curr_lock);
      HP::drop_curr();
      if (ok)
        return true;
    }
  }

  /// Returns the directory-layer (level 0) node covering key k.
  /// The node is protected by an HP in the curr slot, and target_lock receives
  /// its read-lock context. On any sequence-lock verification failure, this
  /// restarts internally.
  directory_t *descend_to_directory(uint64_t &target_lock, K const &k) {
    assert(layers >= 2);

    while (true) {
      // NB: index layer n lives at index_head[n-1], because directory_head is
      // not part of the index_head array.
      int layer = static_cast<int>(layers) - 1;
      index_t *curr = &index_head.at(layer - 1);
      HP::take_first(curr);
      uint64_t curr_lock = curr->lock.begin_read();

      // Descend index layers layers-1 .. 2.
      bool restart = false;
      for (; layer >= 2; --layer) {
        index_t *down = &index_head.at(layer - 2);
        if (!follow(curr, curr_lock, k, down)) {
          HP::drop_curr();
          restart = true;
          break;
        }
        curr = down;
      }
      if (restart)
        continue;

      // From index layer 1, descend to directory layer 0.
      directory_t *curr_0 = &directory_head;
      if (!follow(curr, curr_lock, k, curr_0)) {
        HP::drop_curr();
        continue;
      }

      // At layer 0, walk right until we find the directory node covering k.
      if (!check_next(curr_0, curr_lock, k)) {
        HP::drop_curr();
        continue;
      }

      target_lock = curr_lock;
      return curr_0;
      // Caller inherits the HP on curr, and the read lock context.
    }
  }

  /// Returns the index-layer node at target_level covering key k, with the
  /// same HP / lock-context contract as descend_to_directory().
  index_t *descend_to_index_level(uint64_t &target_lock, K const &k,
                                  int target_level) {
    assert(layers >= 2);
    assert(target_level >= 1);
    assert(target_level < static_cast<int>(layers));

    while (true) {
      int layer = static_cast<int>(layers) - 1;
      index_t *curr = &index_head.at(layer - 1);
      HP::take_first(curr);
      uint64_t curr_lock = curr->lock.begin_read();

      // Descend index layers above target_level.
      bool restart = false;
      for (; layer > target_level; --layer) {
        index_t *down = &index_head.at(layer - 2);
        if (!follow(curr, curr_lock, k, down)) {
          HP::drop_curr();
          restart = true;
          break;
        }
        curr = down;
      }
      if (restart)
        continue;

      // At target_level, walk right until we find the node covering k.
      if (!check_next(curr, curr_lock, k)) {
        HP::drop_curr();
        continue;
      }

      target_lock = curr_lock;
      return curr;
      // Caller inherits the HP on curr, and the read lock context.
    }
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

  /// Descend to the local node at `level` covering k, then split it at k.
  /// - The current local node keeps entries < k.
  /// - A new local node is created holding k and entries > k.
  /// - The new local node is stitched in as `curr->next`.
  /// - The new local node's remote_addr is set to `new_remote_addr`.
  ///
  /// This implements the skip vector invariant for every level *below* a key's
  /// height: at those levels, k becomes the minimum of a newly created node.
  ///
  /// @returns the new local node (the down pointer for level+1), or nullptr if
  ///          the local mirror is in a state where this split does not apply,
  ///          in which case the caller must abandon the whole mirror update.
  template <typename NodeT, typename ValueT>
  NodeT *mirror_split_at_level(
      K const &k, int level,
      ValueT const &down_ptr_k, // remote_addr for level 0, local ptr for level >= 1
      REMOTE_ADDR const &new_remote_addr) {

    while (true) {
      // Descend read-only to find the local node covering k at this level.
      uint64_t curr_lock = 0;
      NodeT *curr;
      if constexpr (std::is_same_v<NodeT, directory_t>) {
        curr = descend_to_directory(curr_lock, k);
      } else {
        curr = descend_to_index_level(curr_lock, k, level);
      }

      // Try to upgrade to write lock. On failure, restart.
      if (!curr->lock.try_upgrade(curr_lock)) {
        HP::drop_curr();
        continue;
      }

      // Idempotency: if k is already curr's minimum, this split has already
      // been installed -- by a duplicate mirror update, or by another thread
      // that got here first. Note that we detect it by landing *on* the
      // already-created node, since a node whose minimum is k is exactly what
      // the descent for k finds.
      //
      // We can only hand that node up as a down pointer if it is a legal
      // down-pointer target, which orphans (heads included) are not. A node
      // with minimum k can be an orphan two ways: k was previously inserted
      // at the top level into a head, making k the head's minimum; or an
      // overflow split at this level left k as the minimum of the new orphan.
      // Both mean the mirror does not currently have a representable chain for
      // k, so abandon the update and let a refresh reconcile it.
      if (curr->v.get_size() > 0 && curr->v.first() == k) {
        // Safe to read the orphan bit directly: we hold the write lock.
        bool const usable = !curr->lock.is_orphan();
        // Adopt the caller's remote address: this local node and the remote
        // node share a minimum, so they are the same logical node.
        bool changed = false;
        if (usable) {
          curr->remote_addr = new_remote_addr;
          changed = true;
        }
        curr->lock.release_changed_if(changed);
        HP::drop_curr();
        return usable ? curr : nullptr;
      }

      // k is present at this level but is not a node minimum. That happens when
      // the local node is *coarser* than the remote one -- the cache recorded k
      // as an ordinary entry without ever learning it was a node boundary,
      // which is the normal state for a boundary some other client created.
      // Split the node at the entry it already holds, so it lines up with the
      // remote partition again.
      if (curr->v.contains(k)) {
        NodeT *split = new NodeT(curr, false, new_remote_addr);
        if (!split->v.split_at(&curr->v, k)) {
          // Nothing at or above k after all; undo and give up rather than
          // leave an empty non-orphan linked in.
          curr->next = split->next.load();
          delete split;
          curr->lock.release_unchanged();
          HP::drop_curr();
          return nullptr;
        }
        curr->lock.release();
        HP::drop_curr();
        return split;
      }

      // Edge case: curr is full and k is below its minimum, so the new node
      // would have to take all of curr's entries plus k -- one too many.
      // Shed the upper half into an orphan first. (This mirrors the same case
      // in insert_seq(). It can only arise when curr is a head: any node
      // reached through a down pointer has a minimum <= k.)
      //
      // NB: The orphan is a local-only node -- local vector capacity is not a
      // remote property -- so it gets a null remote address.
      if (curr->v.get_size() == curr->v.get_capacity() &&
          k < curr->v.first()) {
        auto *shed = new NodeT(curr, true, REMOTE_ADDR{});
        shed->v.steal_half(&curr->v);
      }

      // Create the new local node and move entries > k into it.
      //
      // NB: The constructor stitches new_local in while we hold curr's write
      // lock, so a reader can briefly see it with an empty vector -- but such
      // a reader reached it through curr and will fail its confirm_read() on
      // curr, so it discards whatever it saw.
      NodeT *new_local = new NodeT(curr, false, new_remote_addr);
      new_local->v.split_insert(&curr->v, {k, down_ptr_k});
      curr->lock.release();
      HP::drop_curr();

      return new_local;
    }
  }

  /// Descend to the local node at `level` covering k, and insert (k, down_ptr)
  /// into it. If the insert overflows the node, it splits into an orphan.
  ///
  /// This implements the skip vector invariant at the *top* level of a key's
  /// height: there, k joins an existing node rather than becoming a new node's
  /// minimum. Head nodes hold entries like any other node, so the node the
  /// descent lands on is always a valid target -- including the head, which is
  /// the covering node for any key left of every boundary at this level.
  ///
  /// orphan_remote_addr is used only if the insert overflows; see
  /// node_t::insert() for why it may legitimately be null.
  template <typename NodeT, typename ValueT>
  void mirror_insert_at_top_level(K const &k, int level, ValueT const &down_ptr,
                                  REMOTE_ADDR const &orphan_remote_addr) {

    while (true) {
      uint64_t curr_lock = 0;
      NodeT *curr;
      if constexpr (std::is_same_v<NodeT, directory_t>) {
        curr = descend_to_directory(curr_lock, k);
      } else {
        curr = descend_to_index_level(curr_lock, k, level);
      }

      if (!curr->lock.try_upgrade(curr_lock)) {
        HP::drop_curr();
        continue;
      }

      // Insert (k, down_ptr) into curr. A false return means the key was
      // already there, i.e. someone else already applied this update.
      bool const changed = curr->insert({k, down_ptr}, orphan_remote_addr);
      curr->lock.release_changed_if(changed);
      HP::drop_curr();
      return;
    }
  }

  // todo: consider how height correlates to level here, update below (level < height in for loop, and elsewhere) as needed
  void mirror_insert(K const& k, int height,
                    REMOTE_ADDR const &new_remote_data_addr,
                    std::array<REMOTE_ADDR, MAX_LAYERS> const& new_remote_index_addrs) {
    init_context();
    assert(height > 0);

    int const top_level = height - 1;

    // Height 1: nothing to split. k simply joins the directory node covering it.
    if (top_level == 0) {
      mirror_insert_at_top_level<directory_t>(k, 0, new_remote_data_addr,
                                              new_remote_index_addrs[0]);
      return;
    }

    // Level 0: k becomes the minimum of a new directory node.
    directory_t *d = mirror_split_at_level<directory_t>(
        k, 0, new_remote_data_addr, new_remote_index_addrs[0]);
    if (d == nullptr)
      return; // nothing was created, so there is nothing to parent

    // Levels 1..top_level-1: the same, each pointing at the level below. Each
    // node created here installs (k -> the level below) as its first entry,
    // which is what parents the node beneath it.
    void *child = d;
    int level = 1;
    for (; level < top_level; ++level) {
      index_t *n = mirror_split_at_level<index_t>(k, level, child,
                                                  new_remote_index_addrs[level]);
      if (n == nullptr)
        break; // could not build this level; child still needs a parent
      child = n;
    }

    // Whatever we stopped on is a non-orphan with no parent yet: at top_level
    // in the normal case, or at the level we broke out on. verify_index()
    // requires every non-orphan to be the target of a down pointer, so install
    // its entry one level up either way.
    //
    // NB: the boundary-nesting invariant -- a key that is a boundary at level L
    // is a boundary at every level below -- probably makes the break case
    // unreachable, since it would need k to be a level-L boundary while not
    // being a level-0 one. That reasoning is delicate under concurrency and the
    // guard is free, so we do not rely on it.
    if (level < static_cast<int>(layers)) {
      mirror_insert_at_top_level<index_t>(k, level, child,
                                          new_remote_index_addrs[level]);
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
    // NB: Start at the head, not head->next: heads hold entries, so their
    // contents and ordering have to be checked like any other node's.
    const directory_t *curr = &directory_head;

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

    // Print contents of all index layers.
    // NB: index layer n lives at index_head[n-1], and layers counts the
    // directory layer, so the index layers are 1 .. layers-1.
    for (int n = layers - 1; n >= 1; --n) {
      cout << "Index " << n << ": ";
      const index_t *curr = &(index_head.at(n - 1));
      while (curr != nullptr) {
        curr->dump();
        curr = curr->next;
      }
      cout << endl;
    }

    cout << "Directory: ";
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

    // NB: layer n's slot in index_head is n-1. Start at the head, not
    // head->next: heads hold entries, so their down pointers have to be
    // traced like any other node's.
    index_t *curr = &(index_head.at(layer_idx));

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
               << " pointing to orphan in layer: " << layer_idx + 1
               << " (target is_head=" << is_head(down, layer_idx)
               << " size=" << down->v.get_size();
          if (down->v.get_size() > 0)
            cout << " min=" << +down->v.first();
          cout << ")" << endl;
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
