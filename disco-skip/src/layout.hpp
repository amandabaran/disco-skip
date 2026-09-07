#pragma once

#include <cstdint>
#include <cstddef>
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
        return (size + 63) & ~63;
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

    // ─── Remote address calculation ────────────────────────────────────
    static uintptr_t remoteAddrOf(uintptr_t remote_base, uint64_t key) {
        return remote_base + key * sizeof(Register);
    }
};

} // namespace ds
