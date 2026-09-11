#pragma once

#include <cstdint>
#include <cstddef>
#include <vector>
#include "ds_batch.hpp"
#include "ds_defs.hpp"
#include "ds_node.hpp"
#include "register.hpp"

namespace ds {

using ProcId = uint64_t;

struct Layout {
    // Configurable via CLI (set in main.cpp)
    uint64_t num_clients;
    uint64_t num_servers;
    uint64_t async_parallelism;
    uint64_t num_registers;
    uint64_t max_range;
    uint64_t majority;       // 0 = auto-compute (num_servers/2 + 1)

    // Number of levels in the skip vector, counting the directory level as 0.
    // Must be in (1, DS_MAX_LAYERS]; the cache asserts that range.
    uint64_t cache_layers;

    // Size of each client's private stripe of the node arena. Nothing is ever
    // reclaimed (invariants.md §5 puts epoch GC out of scope), so this has to
    // cover every node the client will create over the whole run, and the
    // server allocation scales with num_clients * this.
    uint64_t nodes_per_client;

    // Likewise for the vector arena -- but this is the one that binds, because
    // every write allocates a new vector (copy-on-write) while only a split
    // allocates a node. Budget one per write, not one per node.
    uint64_t vecs_per_client;

    // Whether to speculate the current vector offset from a local hint so the
    // header and vector reads can be issued in one doorbell batch. A wrong
    // guess costs a wasted vector read -- bandwidth, a work request and a
    // completion -- but not latency, since the fallback read lands where the
    // unspeculated second read would have. So this is a bandwidth trade: worth
    // it for read-heavy workloads, where the hint is usually right, and against
    // for write-heavy ones, where every write moves the vector.
    bool offset_hint;

    // Runtime arms of the two toggles. invariants.md §9 specifies
    // DS_CACHE_ENABLED as compile-time, and it has to be: the cache is a member
    // of DsState, so whether it exists at all is decided at build time. But the
    // *no-cache baseline* wants to be a runtime arm, so one binary can produce
    // both halves of the headline "RDMAs per op, cache on vs off" measurement
    // without a rebuild. So: DS_CACHE_ENABLED decides whether the cache is
    // compiled in, and consult_cache decides whether we ask it.
    //
    // Setting consult_cache with DS_CACHE_ENABLED=0 is a user error rather than
    // a silent no-op; main.cpp rejects it.
    bool consult_cache;
    bool writeback;

    // Set by client at runtime after MR is allocated.
    // (Same pattern as swarm-kv: see Layout::client_local_region)
    uintptr_t client_local_region;

    // helper to pad sizes to 64-byte boundaries
    static constexpr size_t align64(size_t size) {
        // ~size_t{63}, not ~63: the latter is an int, and sign-extending it to
        // size_t happens to produce the right mask on this platform while
        // tripping -Wsign-conversion. Spelling the width out removes both the
        // warning and the dependence on that accident.
        return (size + 63) & ~size_t{63};
    }

    uint64_t firstClientId() const { return num_servers + 1; }

    uint64_t effectiveMajority() const {
        return majority == 0 ? (num_servers / 2 + 1) : majority;
    }

    // ─── Per-future scratchpad layout (in client's MR) ─────────────────
    // Each future owns a contiguous region:
    //   [read_bufs : num_servers * sizeof(Register)]
    //   [swap_bufs : num_servers * sizeof(Register)]
    //   [bulk_bufs : num_servers * max_range * sizeof(Register)]

    size_t readBufsSize() const { return static_cast<size_t>(num_servers) * sizeof(Register); }
    size_t swapBufsSize() const { return static_cast<size_t>(num_servers) * sizeof(Register); }
    size_t bulkBufsSize() const { return static_cast<size_t>(num_servers) * static_cast<size_t>(max_range) * sizeof(Register); }
    size_t perFutureSize() const {
        return align64(readBufsSize() + swapBufsSize() + bulkBufsSize());
    }

    size_t clientSize() const { return async_parallelism * perFutureSize(); }
    /// Legacy flat-register region, kept while that path still exists.
    size_t registerRegionSize() const { return num_registers * sizeof(Register); }

    // ─── Pointer accessors (use client_local_region) ───────────────────
    Register* getReadBufs(uint64_t future_id) const {
        return reinterpret_cast<Register*>(
            client_local_region + future_id * perFutureSize());
    }
    Register* getSwapBufs(uint64_t future_id) const {
        return reinterpret_cast<Register*>(
            client_local_region + future_id * perFutureSize() + readBufsSize());
    }
    Register* getBulkBufs(uint64_t future_id) const {
        return reinterpret_cast<Register*>(
            client_local_region + future_id * perFutureSize()
            + readBufsSize() + swapBufsSize());
    }

    // ─── Remote address calculation (legacy register path) ─────────────
    static uintptr_t remoteAddrOf(uintptr_t remote_base, uint64_t key) {
        return remote_base + key * sizeof(Register);
    }

    // ═══════════════════════════════════════════════════════════════════
    // Skip-vector arenas
    // ═══════════════════════════════════════════════════════════════════
    //
    // The memory servers hold two flat arrays, laid out identically on every
    // replica:
    //
    //     [ node arena   : NodeRecord, 64 B each  ]
    //     [ vector arena : VecRecord, 320 B each  ]
    //
    // A node's identity is its index into the first -- a RemoteAddr -- which is
    // why a RemoteAddr is replica-independent and has to be resolved against a
    // particular connection's remoteBuf() to become an address. A version's
    // identity is its index into the second, which is what the handle's 32-bit
    // offset field holds.
    //
    // Both are per-client bump allocators over private stripes, with no free
    // list: invariants.md §5 puts epoch GC explicitly out of scope. For nodes
    // that is a mild constraint. For vectors it is the binding one, because
    // *every write allocates a vector* -- so the vector arena must hold one
    // per write for the whole run. At 320 B that is ~32 GB cluster-wide for
    // 1e8 writes, which means arena size bounds run length rather than the
    // other way round. Worth knowing before a long run, and worth reporting,
    // which is why vecArenaSize() is printed at startup.

    /// Reserved ids and offsets live in ds_node.hpp, next to headAddr(), since
    /// they are facts about node identity the bootstrap needs too. Re-exported
    /// so call sites holding a Layout need not reach past it.
    static constexpr uint64_t kNullId = ds::kNullId;
    static constexpr uint64_t kHeadIdBase = ds::kHeadIdBase;
    static constexpr uint64_t kInitialDataId = ds::kInitialDataId;
    static constexpr uint64_t kFirstDynamicId = ds::kFirstDynamicId;
    static constexpr VecOffset kFirstDynamicVec = ds::kFirstDynamicVec;
    static constexpr RemoteAddr headAddr(uint32_t level) {
        return ds::headAddr(level);
    }

    uint64_t nodeArenaNodes() const {
        return kFirstDynamicId + num_clients * nodes_per_client;
    }
    size_t nodeArenaSize() const {
        return static_cast<size_t>(nodeArenaNodes()) * sizeof(NodeRecord);
    }

    uint64_t vecArenaVecs() const {
        return kFirstDynamicVec + num_clients * vecs_per_client;
    }
    size_t vecArenaSize() const {
        return static_cast<size_t>(vecArenaVecs()) * sizeof(VecRecord);
    }

    size_t vecArenaOffset() const { return align64(nodeArenaSize()); }
    size_t serverSize() const { return vecArenaOffset() + vecArenaSize(); }

    /// Address of a node on one replica. Also its handle's address, since the
    /// handle sits at offset 0 -- so this doubles as the CAS target.
    static uintptr_t nodeAddrOf(uintptr_t remote_base, RemoteAddr a) {
        return remote_base + a.id * sizeof(NodeRecord);
    }

    /// Address of the node's next_id field, which a split CASes in place.
    static uintptr_t nextIdAddrOf(uintptr_t remote_base, RemoteAddr a) {
        return nodeAddrOf(remote_base, a) + offsetof(NodeRecord, next_id);
    }
    static uintptr_t nextKMinAddrOf(uintptr_t remote_base, RemoteAddr a) {
        return nodeAddrOf(remote_base, a) + offsetof(NodeRecord, next_k_min);
    }
    /// tail_struct_ver is 4 bytes, but RDMA CAS is 8. It is CAS'd as the
    /// 8-byte word it shares with `level`, which never changes after creation,
    /// so the pair can be swapped atomically without disturbing it.
    static uintptr_t tailWordAddrOf(uintptr_t remote_base, RemoteAddr a) {
        return nodeAddrOf(remote_base, a) + offsetof(NodeRecord, level);
    }

    /// Address of one vector on one replica.
    uintptr_t vecAddrOf(uintptr_t remote_base, VecOffset off) const {
        return remote_base + vecArenaOffset() +
               static_cast<size_t>(off) * sizeof(VecRecord);
    }
    /// Address of a vector's ts field, which readers CAS to resolve a pending
    /// version.
    uintptr_t vecTsAddrOf(uintptr_t remote_base, VecOffset off) const {
        return vecAddrOf(remote_base, off) + offsetof(VecRecord, ts);
    }

    // ─── Per-future scratchpad for the skip-vector path ────────────────
    //
    // Only buffers RDMA reads into or writes out of need to live in the
    // registered MR. The traversal path itself (PathStep[]) is ordinary client
    // memory and lives in the future object.
    //
    // The traversal reuses one node/vector buffer pair per replica across levels:
    // it is strictly sequential -- read a header, maybe its vector, traverse --
    // so per-level buffers would be dead space. The data node gets its own pair
    // because a Get holds both at once. Staging buffers are not per-replica,
    // since the same bytes go to every replica.

    size_t nodeBufsSize() const { return static_cast<size_t>(num_servers) * sizeof(NodeRecord); }
    size_t vecBufsSize() const { return static_cast<size_t>(num_servers) * sizeof(VecRecord); }

    // Staging is sized for the longest CHAIN, not for one write at a time.
    // Every write in a chained batch is posted before any of them completes, so
    // their source buffers must all be live simultaneously -- F2 stages two
    // vectors and one node header, and sharing a slot would have the second
    // write overwrite the first's bytes while the HCA was still reading them.
    size_t stageNodeSize() const { return kMaxBatchNodeWrites * sizeof(NodeRecord); }
    size_t stageVecSize() const { return kMaxBatchVecWrites * sizeof(VecRecord); }

    // One swapback slot per operation per replica. A CAS reports the pre-CAS
    // value into its own buffer, and a chain can hold several, so they cannot
    // share either -- and the publishing CAS's result is the one that decides
    // whether the operation committed.
    size_t casBufsSize() const {
        return static_cast<size_t>(num_servers) * kMaxBatchOps * sizeof(uint64_t);
    }

    /// This replica's slice of the swapback buffers, indexed by batch position.
    uint64_t* casBufsFor(uint64_t future_id, size_t replica) const {
        return getCasBufs(future_id) + replica * kMaxBatchOps;
    }

    size_t nodePerFutureSize() const {
        return align64(2 * nodeBufsSize() + 2 * vecBufsSize() +
                       stageNodeSize() + stageVecSize() + casBufsSize());
    }
    size_t nodeClientSize() const { return async_parallelism * nodePerFutureSize(); }

    /// Offset of the skip-vector scratchpad within the client region. The
    /// legacy register scratchpad keeps the front so both paths can coexist
    /// while the skip-vector operations are built out; this becomes 0 when the
    /// register path goes.
    size_t nodeRegionOffset() const { return align64(clientSize()); }

    uintptr_t nodeFutureBase(uint64_t future_id) const {
        return client_local_region + nodeRegionOffset() +
               future_id * nodePerFutureSize();
    }

    // Layout within a future's slice, in order:
    //   [idx nodes][idx vecs][data nodes][data vecs][stage node][stage vec][cas]
    NodeRecord* getNodeBufs(uint64_t f) const {
        return reinterpret_cast<NodeRecord*>(nodeFutureBase(f));
    }
    VecRecord* getVecBufs(uint64_t f) const {
        return reinterpret_cast<VecRecord*>(nodeFutureBase(f) + nodeBufsSize());
    }
    NodeRecord* getDataNodeBufs(uint64_t f) const {
        return reinterpret_cast<NodeRecord*>(nodeFutureBase(f) + nodeBufsSize() +
                                             vecBufsSize());
    }
    VecRecord* getDataVecBufs(uint64_t f) const {
        return reinterpret_cast<VecRecord*>(nodeFutureBase(f) + 2 * nodeBufsSize() +
                                            vecBufsSize());
    }
    NodeRecord* getStageNode(uint64_t f) const {
        return reinterpret_cast<NodeRecord*>(nodeFutureBase(f) + 2 * nodeBufsSize() +
                                             2 * vecBufsSize());
    }
    VecRecord* getStageVec(uint64_t f) const {
        return reinterpret_cast<VecRecord*>(nodeFutureBase(f) + 2 * nodeBufsSize() +
                                            2 * vecBufsSize() + stageNodeSize());
    }
    uint64_t* getCasBufs(uint64_t f) const {
        return reinterpret_cast<uint64_t*>(nodeFutureBase(f) + 2 * nodeBufsSize() +
                                           2 * vecBufsSize() + stageNodeSize() +
                                           stageVecSize());
    }

    size_t totalClientSize() const { return nodeRegionOffset() + nodeClientSize(); }
};

/// Per-client bump allocator over that client's private stripe of the arena.
///
/// Client-partitioned so allocation needs no coordination, which it cannot have
/// -- the memory servers run no logic, so there is nowhere to put a shared
/// allocator short of a CAS loop on a global counter, and E5 already calls for
/// per-client slabs.
///
/// Exhaustion returns the null address rather than throwing or wrapping. A
/// caller that ignores it would write over another client's nodes, so the
/// remote side treats a null allocation as a hard error at the call site; the
/// fix is a bigger --nodes-per-client, not a retry.
class NodeAllocator {
    uint64_t first_ = Layout::kFirstDynamicId;  // inclusive
    uint64_t next_ = Layout::kFirstDynamicId;   // next id to hand out
    uint64_t end_ = Layout::kFirstDynamicId;    // exclusive

public:
    NodeAllocator() = default;

    NodeAllocator(uint64_t client_idx, uint64_t nodes_per_client)
        : first_(Layout::kFirstDynamicId + client_idx * nodes_per_client),
          next_(first_),
          end_(first_ + nodes_per_client) {}

    /// Returns a fresh node id, or the null address when the stripe is spent.
    RemoteAddr allocate() {
        if (next_ >= end_) return RemoteAddr{};
        return RemoteAddr{next_++};
    }

    [[nodiscard]] uint64_t allocated() const { return next_ - first_; }
    [[nodiscard]] uint64_t remaining() const { return end_ - next_; }
    [[nodiscard]] uint64_t capacity() const { return end_ - first_; }
    [[nodiscard]] uint64_t firstId() const { return first_; }
    [[nodiscard]] bool exhausted() const { return next_ >= end_; }

    /// Does /a/ fall in this client's stripe? Only this client writes nodes
    /// here, so a node address that fails this came from somewhere else -- which
    /// the selftest uses to check stripe isolation.
    [[nodiscard]] bool owns(RemoteAddr a) const {
        return a.id >= first_ && a.id < end_;
    }
};


/// Per-client bump allocator over that client's private stripe of the vector
/// arena.
///
/// Separate from NodeAllocator because the two are consumed at wildly
/// different rates: a node is allocated only by a split, a vector by every
/// single write. Sharing one stripe would let write traffic starve splits.
class VecAllocator {
    VecOffset first_ = Layout::kFirstDynamicVec;
    VecOffset next_ = Layout::kFirstDynamicVec;
    VecOffset end_ = Layout::kFirstDynamicVec;

public:
    VecAllocator() = default;

    VecAllocator(uint64_t client_idx, uint64_t vecs_per_client)
        : first_(static_cast<VecOffset>(Layout::kFirstDynamicVec +
                                        client_idx * vecs_per_client)),
          next_(first_),
          end_(static_cast<VecOffset>(first_ + vecs_per_client)) {}

    /// Returns a fresh vector offset, or kNullVec when the stripe is spent.
    ///
    /// Exhaustion is not recoverable: with no reclamation, running out means
    /// the run is longer than the arena was sized for. Callers treat it as a
    /// hard error rather than retrying.
    VecOffset allocate() {
        if (next_ >= end_) return kNullVec;
        return next_++;
    }

    [[nodiscard]] uint64_t allocated() const { return next_ - first_; }
    [[nodiscard]] uint64_t remaining() const { return end_ - next_; }
    [[nodiscard]] uint64_t capacity() const { return end_ - first_; }
    [[nodiscard]] VecOffset firstOffset() const { return first_; }
    [[nodiscard]] bool exhausted() const { return next_ >= end_; }
    [[nodiscard]] bool owns(VecOffset o) const {
        return o >= first_ && o < end_;
    }
};

/// Last-seen vector offset per node, so the header read and the vector read can
/// be issued in one doorbell batch instead of serialised on the header's
/// answer.
///
/// Direct-mapped and indexed by node id, because node ids are dense indices
/// into a bounded arena -- so this is a flat array with no hashing, no
/// eviction, and O(1) lookup. It is purely client-local and never RDMA'd, which
/// is why it lives here rather than in the registered MR.
///
/// It deliberately does NOT live in the cache. Cache entries are write-once:
/// install_data_entry() ignores insert()'s "already present" return, so a hint
/// packed into a REMOTE_ADDR could never be refreshed -- installed on first
/// touch and stale forever after the first write. It would also undercut the
/// §5 additivity argument, which rests on node addresses being stable.
///
/// A wrong hint costs a wasted vector read, not a round trip: the real read
/// lands where the unspeculated one would have. So the toggle trades rNIC and
/// PCIe bandwidth, which is the scarce resource under write-heavy load, and
/// hitRate() is instrumented so the crossover is measured rather than assumed.
class VecOffsetHint {
    std::vector<VecOffset> hint_;
    bool enabled_ = false;
    uint64_t hits_ = 0;
    uint64_t misses_ = 0;

public:
    VecOffsetHint() = default;
    VecOffsetHint(size_t arena_nodes, bool enabled)
        : hint_(enabled ? arena_nodes : 0, kNullVec), enabled_(enabled) {}

    [[nodiscard]] bool enabled() const { return enabled_; }

    /// The offset to speculate for /a/, or kNullVec for "no guess" -- in which
    /// case the caller must serialise the two reads.
    [[nodiscard]] VecOffset guess(RemoteAddr a) const {
        if (!enabled_ || a.id >= hint_.size()) return kNullVec;
        return hint_[a.id];
    }

    /// Record the true offset, learned from a header read. Also scores the
    /// guess that preceded it, which is what hitRate() reports.
    void record(RemoteAddr a, VecOffset truth, VecOffset guessed) {
        if (!enabled_ || a.id >= hint_.size()) return;
        if (guessed != kNullVec) {
            if (guessed == truth) ++hits_; else ++misses_;
        }
        hint_[a.id] = truth;
    }

    [[nodiscard]] uint64_t hits() const { return hits_; }
    [[nodiscard]] uint64_t misses() const { return misses_; }
    [[nodiscard]] double hitRate() const {
        uint64_t const n = hits_ + misses_;
        return n == 0 ? 0.0 : static_cast<double>(hits_) / static_cast<double>(n);
    }
};

} // namespace ds
