#pragma once

#include <cstdint>
#include <cstddef>
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
    size_t serverSize() const { return num_registers * sizeof(Register); }

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
    // Skip-vector node arena
    // ═══════════════════════════════════════════════════════════════════
    //
    // The memory servers hold one flat array of NodeRecords, laid out
    // identically on every replica. A node's identity is its index into that
    // array -- a RemoteAddr -- which is why a RemoteAddr is replica-independent
    // and has to be resolved against a particular connection's remoteBuf() to
    // become an address. See ds_remote_addr.hpp for why it must not become a
    // virtual address instead.
    //
    // There is no free list. invariants.md §5 puts epoch GC explicitly out of
    // scope, so allocation is a per-client bump pointer over a private stripe
    // and nothing is ever reclaimed. That is a capacity limit, not a leak: the
    // arena has to be sized for the whole run, which nodeArenaNodes() does.

    /// Node ids are laid out as:
    ///
    ///     0                        null (never allocated -- see RemoteAddr)
    ///     1 .. kMaxLayers          the per-level head nodes, in level order
    ///     kMaxLayers + 1           the initial data node
    ///     kFirstDynamicId ...      per-client stripes, nodes_per_client each
    ///
    /// Fixing the heads at known ids is what makes A1 cheap: the leftmost node
    /// at each level has a stable address for the lifetime of the structure, so
    /// set_head_remote_addrs() needs no discovery step and no memstore round
    /// trip -- only a barrier to wait until the initialising client has written
    /// the records.
    static constexpr uint64_t kNullId = 0;
    static constexpr uint64_t kHeadIdBase = 1;
    static constexpr uint64_t kInitialDataId = kHeadIdBase + kMaxLayers;
    static constexpr uint64_t kFirstDynamicId = kInitialDataId + 1;

    /// The head (leftmost) node at /level/, level 0 being the directory.
    static constexpr RemoteAddr headAddr(uint32_t level) {
        return RemoteAddr{kHeadIdBase + level};
    }

    /// Total nodes the arena must hold, and hence the server-side allocation.
    uint64_t nodeArenaNodes() const {
        return kFirstDynamicId + num_clients * nodes_per_client;
    }

    size_t nodeArenaSize() const {
        return static_cast<size_t>(nodeArenaNodes()) * sizeof(NodeRecord);
    }

    /// Address of a node on one replica. A node's address is also its handle's
    /// address, since the handle sits at offset 0 -- so this doubles as the CAS
    /// target.
    static uintptr_t nodeAddrOf(uintptr_t remote_base, RemoteAddr a) {
        return remote_base + a.id * sizeof(NodeRecord);
    }

    /// Address of one CoW slot within a node, for staging a new version without
    /// rewriting the header.
    static uintptr_t slotAddrOf(uintptr_t remote_base, RemoteAddr a,
                                uint8_t slot) {
        return nodeAddrOf(remote_base, a) + offsetof(NodeRecord, slot) +
               static_cast<size_t>(slot & 1u) * sizeof(VecSlot);
    }

    // ─── Per-future scratchpad for the skip-vector path ────────────────
    //
    // Only buffers that RDMA reads into or writes out of need to live in the
    // registered MR. The descent path itself (PathStep[]) is ordinary client
    // memory and lives in the future object.
    //
    // The descent reuses one node buffer per replica across levels: it is
    // strictly sequential -- read level L, extract the PathStep, read level
    // L-1 -- so per-level buffers would be dead space. The data node gets its
    // own buffer because a Get holds both at once.
    //
    // Staging buffers are not per-replica: the same bytes go to every replica,
    // so one copy is read by all of them.

    size_t nodeBufsSize() const { return static_cast<size_t>(num_servers) * sizeof(NodeRecord); }
    size_t dataBufsSize() const { return static_cast<size_t>(num_servers) * sizeof(NodeRecord); }
    size_t stageNodeSize() const { return sizeof(NodeRecord); }
    size_t stageSlotSize() const { return sizeof(VecSlot); }
    size_t casBufsSize() const { return static_cast<size_t>(num_servers) * sizeof(uint64_t); }

    size_t nodePerFutureSize() const {
        return align64(nodeBufsSize() + dataBufsSize() + stageNodeSize() +
                       stageSlotSize() + casBufsSize());
    }

    size_t nodeClientSize() const { return async_parallelism * nodePerFutureSize(); }

    /// Offset of the skip-vector scratchpad within the client region. The
    /// legacy register scratchpad keeps the front of the region so both paths
    /// can coexist while the skip-vector operations are built out; when the
    /// register path is deleted this becomes 0.
    size_t nodeRegionOffset() const { return align64(clientSize()); }

    uintptr_t nodeFutureBase(uint64_t future_id) const {
        return client_local_region + nodeRegionOffset() +
               future_id * nodePerFutureSize();
    }

    NodeRecord* getNodeBufs(uint64_t future_id) const {
        return reinterpret_cast<NodeRecord*>(nodeFutureBase(future_id));
    }
    NodeRecord* getDataBufs(uint64_t future_id) const {
        return reinterpret_cast<NodeRecord*>(nodeFutureBase(future_id) + nodeBufsSize());
    }
    NodeRecord* getStageNode(uint64_t future_id) const {
        return reinterpret_cast<NodeRecord*>(nodeFutureBase(future_id) +
                                             nodeBufsSize() + dataBufsSize());
    }
    VecSlot* getStageSlot(uint64_t future_id) const {
        return reinterpret_cast<VecSlot*>(nodeFutureBase(future_id) + nodeBufsSize() +
                                          dataBufsSize() + stageNodeSize());
    }
    uint64_t* getCasBufs(uint64_t future_id) const {
        return reinterpret_cast<uint64_t*>(nodeFutureBase(future_id) + nodeBufsSize() +
                                           dataBufsSize() + stageNodeSize() +
                                           stageSlotSize());
    }

    /// Total client-side registered memory: the legacy register scratchpad
    /// followed by the skip-vector one.
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

} // namespace ds
