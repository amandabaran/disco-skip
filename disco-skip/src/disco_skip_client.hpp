#pragma once

#include <cstdint>
#include <vector>

#include <dory/conn/rc-exchanger.hpp>
#include <dory/extern/ibverbs.hpp>

#include "chimera_state.hpp"
#include "op_future.hpp"

namespace chimera {

class ChimeraClient {
private:
    ChimeraState state;
    std::vector<OpFuture> futures;
    std::vector<bool> progress;
    std::vector<RangeFuture> range_futures;
    std::vector<bool> range_progress;
public:
    ChimeraClient(Layout layout,
              dory::conn::RcConnectionExchanger<ProcId>& rcx,
              ProcId proc_id)
    : state{layout, rcx, proc_id} {
        progress.resize(state.layout.async_parallelism, false);
        futures.reserve(state.layout.async_parallelism);
        
        range_progress.resize(state.layout.async_parallelism, false);
        range_futures.reserve(state.layout.async_parallelism);

        for (size_t i = 0; i < state.layout.async_parallelism; ++i) {
            futures.emplace_back(state, i);
            range_futures.emplace_back(state, i);
        }
    }

    // Drain CQs across all server connections.  Routes each completion to
    // its owning future via wr_id, and flips `progress[i]` so the user loop
    // knows to call tryStepForward on it.
    // bool tickRdma() {
    //     bool any_progress = false;
    //     for (size_t s = 0; s < state.layout.num_servers; ++s) {
    //         auto& rc = *state.server_conns[s];
    //         auto& tp = state.to_poll_per_server[s];
    //         if (tp == 0) continue;

    //         state.wces.resize(static_cast<size_t>(tp));
    //         if (!rc.pollCqIsOk(dory::conn::ReliableConnection::SendCq, state.wces)) {
    //             throw std::runtime_error("Error polling CQ");
    //         }
    //         for (auto const& wc : state.wces) {
    //             if (wc.status != IBV_WC_SUCCESS) {
    //                 throw std::runtime_error("WC unsuccessful");
    //             }
    //             uint64_t raw_id = wc.wr_id;
    //             bool is_range_op = (raw_id >> 63) != 0;
    //             uint64_t clean_id = raw_id & ~(1ULL << 63);

    //             if (is_range_op) {
    //                 range_futures.at(clean_id).addToOngoingRDMA(s, -1); // Or appropriate hook
    //                 range_progress.at(clean_id) = true;
    //             } else {
    //                 futures.at(clean_id).addToOngoingRDMA(s, -1);
    //                 progress.at(clean_id) = true;
    //             }
    //             any_progress = true;
    //         }
    //         tp -= static_cast<int64_t>(state.wces.size());
    //     }
    //     return any_progress;

        
    // }

    bool tickRdma() {
        bool any_progress = false;
        for (size_t s = 0; s < state.layout.num_servers; ++s) {
            auto& rc = *state.server_conns[s];
            auto& tp = state.to_poll_per_server[s];
            if (tp == 0) continue;

            // Set maximum burst capacity step for the hardware poll step
            state.wces.resize(128); 

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
    OpFuture& getFreeFuture() {
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
    OpFuture& getFuture(uint64_t i) {
        return futures.at(i % state.layout.async_parallelism);
    }

    // Wait for all futures to drain — call this at end of measurement.
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

    ChimeraState& getState() { return state; }

    void reportStats(bool detailed = false) {
        state.reportStats(detailed);
    }
};

} // namespace chimera
