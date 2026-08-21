#pragma once

#include <cstdint>
#include <vector>
#include <chrono>

#include <fmt/core.h>

#include <dory/conn/rc-exchanger.hpp>
#include <dory/extern/ibverbs.hpp>

#include "cache.hpp"
#include "layout.hpp"
#include "latency.hpp"   // Swarm's LatencyProfiler

namespace chimera {

using timepoint = std::chrono::steady_clock::time_point;
using duration  = std::chrono::steady_clock::duration;

class ChimeraState {
public:
    Layout layout;
    ProcId proc_id;
    uint8_t client_idx;
    uint64_t quorum;

    std::vector<dory::conn::ReliableConnection*> server_conns;
    std::vector<int64_t> to_poll_per_server;
    std::vector<struct ibv_wc> wces;
    std::vector<size_t> quorum_indices;

    // ─── Profilers (mirroring OopsState) ────────────────────────────
    LatencyProfiler get_profiler;
    LatencyProfiler put_profiler;
    LatencyProfiler range_profiler;

    // ─── Counters (small, useful for sanity / debugging) ───────────
    uint64_t rdma_reads            = 0;
    uint64_t rdma_cas_attempts     = 0;
    uint64_t rdma_cas_failures     = 0;
    uint64_t gets_with_writeback   = 0;
    uint64_t gets_without_writeback= 0;
    uint64_t put_retries           = 0;

    #if CHIMERA_CACHE_ENABLED
        Cache cache;   // your existing chimera::Cache
        uint64_t cache_hits = 0;
        uint64_t cache_misses = 0;
        void metrics_cache_hit()  { cache_hits++; }
        void metrics_cache_miss() { cache_misses++; }
    #endif

    ChimeraState(Layout _layout,
                 dory::conn::RcConnectionExchanger<ProcId>& rcx,
                 ProcId _proc_id)
        : layout{_layout},
          proc_id{_proc_id}
        #if CHIMERA_CACHE_ENABLED
          ,cache{_layout.num_registers}
        #endif 
    {
        // 1. Compute client_idx explicitly from incoming parameter block to avoid initialization-order bugs
        client_idx = static_cast<uint8_t>(_proc_id - _layout.firstClientId());

        // 2. Compute majority thresholds safely
        if (layout.majority == 0) {
            layout.majority = layout.num_servers / 2 + 1;
        }
        quorum = layout.majority;

        server_conns.reserve(layout.num_servers);
        for (size_t i = 0; i < layout.num_servers; ++i) {
            server_conns.push_back(&rcx.connections().at(i + 1));
        }
        to_poll_per_server.assign(layout.num_servers, 0);
        wces.resize(128);

        quorum_indices.clear();
        quorum_indices.reserve(quorum);
        for (size_t i = 0; i < quorum; ++i) {
            quorum_indices.push_back((client_idx + i) % layout.num_servers);
        }
    }

    // ─── Counter helpers ───────────────────────────────────────────
    inline void countRead()                  { rdma_reads++; }
    inline void countCasAttempt()            { rdma_cas_attempts++; }
    inline void countCasFail()               { rdma_cas_failures++; }
    inline void countWriteback(bool yes)     {
        if (yes) gets_with_writeback++;
        else     gets_without_writeback++;
    }

    // ─── Latency recording ─────────────────────────────────────────
    void addGetMeasurement(timepoint start, timepoint end) {
        get_profiler.addMeasurement(end - start);
    }
    void addPutMeasurement(timepoint start, timepoint end) {
        put_profiler.addMeasurement(end - start);
    }
    void addRangeMeasurement(timepoint start, timepoint end) {
        range_profiler.addMeasurement(end - start);
    }

    // ─── Reporting (mirroring OopsState::reportStats) ──────────────
    void reportStats(bool detailed = false) {
        fmt::print("\n################ Counters:\n");
        fmt::print("rdma_reads:              {}\n", rdma_reads);
        fmt::print("rdma_cas_attempts:       {}\n", rdma_cas_attempts);
        fmt::print("rdma_cas_failures:       {}\n", rdma_cas_failures);
        fmt::print("gets_with_writeback:     {}\n", gets_with_writeback);
        fmt::print("gets_without_writeback:  {}\n", gets_without_writeback);
        fmt::print("put_retries:             {}\n", put_retries);

        fmt::print("\n################ Main stats:\n");

        if (get_profiler.getMeasurementCount() > 0) {
            fmt::print("######## GET stats:\n");
            get_profiler.report(detailed);
        }
        if (put_profiler.getMeasurementCount() > 0) {
            fmt::print("######## PUT stats:\n");
            put_profiler.report(detailed);
        }
        if (range_profiler.getMeasurementCount() > 0) {
            fmt::print("######## RANGE stats:\n");
            range_profiler.report(detailed);
        }
        fmt::print("\n");
    }
};

class BasicFuture {
public:
    BasicFuture(ChimeraState& s, uint64_t id) : state{s}, future_id{id} {}
    BasicFuture(BasicFuture const&) = delete;
    BasicFuture& operator=(BasicFuture const&) = delete;
    BasicFuture(BasicFuture&&) noexcept = default;
    BasicFuture& operator=(BasicFuture&&) noexcept = default;

    uint64_t futureId() const { return future_id; }

protected:
    ChimeraState& state;
    uint64_t future_id;
};

} // namespace chimera
