#pragma once

#include <cstdint>
#include "get_future.hpp"
#include "put_future.hpp"
#include "range_future.hpp"

namespace chimera {

class OpFuture : public BasicFuture {
public:
    enum class Kind { None, Get, Put, Range };

private:
    Kind kind = Kind::None;
    GetFuture   get_f;
    PutFuture   put_f;
    RangeFuture range_f;

public:
    OpFuture(ChimeraState& s, uint64_t id)
        : BasicFuture{s, id}, get_f{s, id}, put_f{s, id}, range_f{s, id} {}

    // Dispatchers
    void doGet(uint64_t key, bool measuring = false) {
        kind = Kind::Get;
        get_f.doGet(key, measuring);
    }
    void doPut(uint64_t key, uint64_t val, bool measuring = false) {
        kind = Kind::Put;
        put_f.doPut(key, val, measuring);
    }
    void doRange(uint64_t sk, uint64_t ek, bool measuring = false) {
        kind = Kind::Range;
        range_f.doRange(sk, ek, measuring);
    }

    // Result accessors (caller must know which kind)
    uint64_t getValue() const  { return get_f.getValue(); }
    std::vector<uint64_t> const& getRangeResults() const {
        return range_f.getResults();
    }
    Kind getKind() const { return kind; }

    bool isDone() const {
        switch (kind) {
            case Kind::Get:   return get_f.isDone();
            case Kind::Put:   return put_f.isDone();
            case Kind::Range: return range_f.isDone();
            case Kind::None:  return true;
            default:
                break;
        }
        return true;
    }

    bool isMeasuring() const {
        switch (kind) {
            case Kind::Get:   return get_f.isMeasuring();
            case Kind::Put:   return put_f.isMeasuring();
            case Kind::Range: return range_f.isMeasuring();
            case Kind::None:  return false;
            default:
                break;
        }
        return false;
    }

    timepoint getStart() const {
        switch (kind) {
            case Kind::Get:   return get_f.getStart();
            case Kind::Put:   return put_f.getStart();
            case Kind::Range: return range_f.getStart();
            default: return timepoint{};
        }
    }

    void addToOngoingRDMA(size_t server_idx, int64_t n) {
        // Forward to whichever sub-future is active
        switch (kind) {
            case Kind::Get:   get_f.addToOngoingRDMA(server_idx, n); break;
            case Kind::Put:   put_f.addToOngoingRDMA(server_idx, n); break;
            case Kind::Range: range_f.addToOngoingRDMA(server_idx, n); break;
            case Kind::None:  break;
            default:
                break;
        }
    }

    bool tryStepForward() {
        switch (kind) {
            case Kind::Get:   return get_f.tryStepForward();
            case Kind::Put:   return put_f.tryStepForward();
            case Kind::Range: return range_f.tryStepForward();
            case Kind::None:  return true;
            default:
                break;
        }
        return true;
    }
};

} // namespace chimera
