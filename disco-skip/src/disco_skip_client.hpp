#pragma once

#include <cstdint>
#include <vector>

#include <dory/conn/rc-exchanger.hpp>
#include <dory/extern/ibverbs.hpp>

#include "disco_skip_state.hpp"
#include "ds_futures.hpp"
#include "range_future.hpp"

namespace ds {

// The skip-vector client.
//
// `futures` are skip-vector operations -- SvFuture over GetOperation and
// PutOperation. The register GetFuture/PutFuture this used to hold are retired:
// they operated on a flat array of registers, which is not the data structure
// any more.
//
// RangeFuture stays, and is still the register path. It is the ONLY scan
// capability in the tree -- there is no skip-vector range, A10 being deferred --
// so retiring it would make workload E unrunnable rather than merely
// unrepresentative. It should go when A10 lands.
class DsClient {
private:
    DsState state;
    QuorumStats qstats;
    GetStats gstats;
    PutStats pstats;
    WriteStats wstats;
#if DS_CACHE_ENABLED
    ClientCache cache;
#else
    ClientCache cache;
#endif
    std::vector<SvFuture<ClientCache>> futures;
    std::vector<bool> progress;
    std::vector<RangeFuture> range_futures;
    std::vector<bool> range_progress;
public:
    DsClient(Layout layout,
              dory::conn::RcConnectionExchanger<ProcId>& rcx,
              ProcId proc_id)
    : state{layout, rcx, proc_id}
#if DS_CACHE_ENABLED
    , cache{state.cache_sv,
            static_cast<uint32_t>(state.layout.cache_layers),
            state.layout.consult_cache}
#else
    , cache{}
#endif
    {
        progress.resize(state.layout.async_parallelism, false);
        futures.reserve(state.layout.async_parallelism);

        range_progress.resize(state.layout.async_parallelism, false);
        range_futures.reserve(state.layout.async_parallelism);

        for (size_t i = 0; i < state.layout.async_parallelism; ++i) {
            futures.emplace_back(state, i, cache, qstats, gstats, pstats,
                                 wstats);
            range_futures.emplace_back(state, i);
        }
    }

    // Drain the send CQs across all server connections. Routes each completion
    // to its owning future via wr_id, and flips `progress[i]` so the user loop
    // knows to call tryStepForward on it.
    bool tickRdma() {
        bool any_progress = false;
        for (size_t s = 0; s < state.layout.num_servers; ++s) {
            auto& rc = *state.server_conns[s];
            auto& tp = state.to_poll_per_server[s];
            if (tp <= 0) continue;

            // Poll at most as many completions as we actually have outstanding on
            // this server. Resizing to a blind 128 lets one poll reap more CQEs
            // than `tp` accounted for, and the decrement below then drives `tp`
            // negative -- after which `tp == 0` is never true again and this
            // server stops being polled. swarm-kv bounds it the same way
            // (oops_client.hpp: wces.resize(to_poll)).
            state.wces.resize(static_cast<size_t>(tp));

            // Dory shrinks state.wces.size() inside this function to match the real event count
            if (!rc.pollCqIsOk(dory::conn::ReliableConnection::SendCq, state.wces)) {
                throw std::runtime_error("Error polling CQ");
            }

            // Loop strictly over entries reaped by Dory
            for (auto const& wc : state.wces) {
                if (wc.status != IBV_WC_SUCCESS) {
                    throw std::runtime_error("WC unsuccessful");
                }
                
                uint64_t raw_id = wc.wr_id;
                bool is_range_op = (raw_id >> 63) != 0;
                uint64_t clean_id = raw_id & ~(1ULL << 63);

                if (is_range_op) {
                    range_futures.at(clean_id).addToOngoingRDMA(s, -1);
                    range_progress.at(clean_id) = true;
                } else {
                    futures.at(clean_id).addToOngoingRDMA(s, -1);
                    progress.at(clean_id) = true;
                }
                any_progress = true;
            }
            
            // Safe decrement using Dory's clean post-poll vector size
            tp -= static_cast<int64_t>(state.wces.size());
        }
        return any_progress;
    }

    // Returns the first future that is done.  Spins on tickRdma() until one is.
    //
    // This is what bounds how many operations are in flight: it hands back a
    // slot only when that slot's operation has completed, so with
    // async_parallelism slots there are at most that many outstanding. The
    // caller must NOT drain between operations -- see the note on
    // finishAllFutures.
    SvFuture<ClientCache>& getFreeFuture() {
        while (true) {
            for (uint64_t i = 0; i < state.layout.async_parallelism; ++i) {
                auto& f = futures[i];
                if (progress[i]) {
                    f.tryStepForward();
                    progress[i] = false;
                }
                if (f.isDone()) return f;
            }
            tickRdma();
        }
    }

    RangeFuture& getFreeRangeFuture() {
        while (true) {
            for (uint64_t i = 0; i < state.layout.async_parallelism; ++i) {
                auto& f = range_futures[i];
                if (range_progress[i]) {
                    f.tryStepForward();
                    range_progress[i] = false;
                }
                if (f.isDone()) return f;
            }
            tickRdma();
        }
    }

    // Returns a specific future by index (used for round-robin or hashing).
    SvFuture<ClientCache>& getFuture(uint64_t i) {
        return futures.at(i % state.layout.async_parallelism);
    }

    [[nodiscard]] QuorumStats const& quorumStats() const { return qstats; }
    [[nodiscard]] GetStats const& getStats() const { return gstats; }
    [[nodiscard]] PutStats const& putStats() const { return pstats; }
    [[nodiscard]] WriteStats const& writeStats() const { return wstats; }

    // Wait for all futures to drain.
    //
    // CALL THIS AT MEASUREMENT BOUNDARIES AND AT THE END, NOT BETWEEN
    // OPERATIONS. Calling it per operation is what the benchmark loop used to
    // do, and it makes the client synchronous: every operation is drained
    // before the next is issued, so exactly one is ever in flight and
    // async_parallelism has no effect whatsoever. Pipelining is the whole point
    // of this machinery, and one misplaced drain removes it silently -- the
    // throughput is simply lower, with nothing to indicate why.
    void finishAllFutures() {
        for (size_t idx = 0; idx < state.layout.async_parallelism; ++idx) {
            while (!futures[idx].isDone() || !range_futures[idx].isDone()) {
                tickRdma();
                if (progress[idx]) {
                    futures[idx].tryStepForward();
                    progress[idx] = false;
                }
                if (range_progress[idx]) {
                    range_futures[idx].tryStepForward();
                    range_progress[idx] = false;
                }
            }
        }
    }

    DsState& getState() { return state; }

    void reportStats(bool detailed = false) {
        state.reportStats(detailed);
    }
};

} // namespace ds
