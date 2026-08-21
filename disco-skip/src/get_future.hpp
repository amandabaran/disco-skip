#pragma once

#include <chrono>
#include <cstdint>
#include <vector>

#include "chimera_state.hpp"
#include "register.hpp"

namespace chimera {

class GetFuture : public BasicFuture {
public:
    enum Step {
        Reading,
        WritingBack,
        Done
    };

private:
    Step step = Done;
    uint64_t key;

    Register* read_bufs;
    Register* swap_bufs;

    std::vector<int64_t> ongoing_per_server;

    Register max_reg;
    uint64_t max_count = 0;
    uint64_t expected[8];

    uint32_t result_value = 0;
    bool measuring = false;
    timepoint start;

    void postWriteback() {
        for (size_t i = 0; i < state.quorum; ++i) {
            if (expected[i] == max_reg.raw) continue;       // already has max
            size_t r = state.quorum_indices[i];
            auto& rc = *state.server_conns[r];

            rc.postSendSingleCas(
                future_id,                                   // wr_id
                &swap_bufs[r],
                Layout::remoteAddrOf(rc.remoteBuf(), key),
                expected[i],
                max_reg.raw);

            ongoing_per_server[r] += 1;
            state.to_poll_per_server[r] += 1;
            state.countCasAttempt();
        }
    }

public:
    GetFuture(ChimeraState& s, uint64_t id) : BasicFuture{s, id} {
        ongoing_per_server.assign(state.layout.num_servers, 0);
        read_bufs = state.layout.getReadBufs(future_id);
        swap_bufs = state.layout.getSwapBufs(future_id);
    }

    void doGet(uint64_t k, bool _measuring = false) {
        key = k;
        measuring = _measuring;
        if (measuring) start = std::chrono::steady_clock::now();

        max_reg = Register(0);
        max_count = 0;

        for (size_t i = 0; i < state.quorum; ++i) {
            size_t r = state.quorum_indices[i];
            auto& rc = *state.server_conns[r];

            rc.postSendSingle(
                dory::conn::ReliableConnection::RdmaRead,
                future_id,
                &read_bufs[r],
                sizeof(Register),
                Layout::remoteAddrOf(rc.remoteBuf(), key));

            ongoing_per_server[r] += 1;
            state.to_poll_per_server[r] += 1;
        }
        step = Reading;
    }

    bool isDone() const { return step == Done; }

    uint64_t getValue() const { return result_value; }

    timepoint getStart() const { return start; }
    bool isMeasuring() const { return measuring; }

    void addToOngoingRDMA(size_t server_idx, int64_t n) {
        ongoing_per_server[server_idx] += n;
    }

    bool tryStepForward() {
        // Wait until all RDMA we issued has completed
        for (auto x : ongoing_per_server) if (x > 0) return false;

        switch (step) {
            case Reading: {
                for (size_t i = 0; i < state.quorum; ++i) {
                    size_t r = state.quorum_indices[i];
                    Register reg = read_bufs[r];
                    expected[i] = reg.raw;
                    state.countRead();
                    if (max_reg < reg)        { max_reg = reg; max_count = 1; }
                    else if (reg == max_reg)  { max_count++; }
                }
                #if CHIMERA_CACHE_ENABLED
                state.cache.put(key, max_reg);
                #endif
                result_value = max_reg.fields.value;

#if CHIMERA_WRITEBACK_ENABLED
                if (max_count < state.quorum) {
                    state.countWriteback(true);
                    postWriteback();
                    step = WritingBack;
                    return true;
                }
#endif
                state.countWriteback(false);
                if (measuring) {
                    state.addGetMeasurement(start, std::chrono::steady_clock::now());
                }
                step = Done;
                break;
            }
            case WritingBack: {
                // Inspect CAS swapbacks. We accept the result either way:
                // ABD writeback only needs to *try* to propagate; if the slot
                // already moved past us, the new writer's value is also valid.
                for (size_t i = 0; i < state.quorum; ++i) {
                    if (expected[i] == max_reg.raw) continue;
                    size_t r = state.quorum_indices[i];
                    Register observed = swap_bufs[r];
                    if (observed.raw != expected[i]) {
                        state.countCasFail();
                    }
                }
                if (measuring) {
                    state.addGetMeasurement(start, std::chrono::steady_clock::now());
                }
                step = Done;
                break;
            }
            case Done:
                break;
            default:
                break;
        }
        return true;
    }
};

} // namespace chimera
