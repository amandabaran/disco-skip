#pragma once

// The DsState glue: skip-vector operations in the driver's future contract.
//
// GetOperation and PutOperation are transport-agnostic state machines. These
// bind them to one future slot -- its id, its buffers, its per-server
// outstanding tally -- and present the interface DsClient drives:
//
//     void doGet(k) / doPut(k, v, height)
//     void addToOngoingRDMA(server, n)
//     bool tryStepForward()
//     bool isDone()
//
// The contract, from chimera's futures: a future may only advance once ALL of
// its outstanding completions have landed. tryStepForward therefore returns
// early while any server still owes one, which is what makes it safe for the
// driver to call on every completion rather than tracking which was the last.
//
// TWO CHECKS THAT TURN SILENT FAILURE INTO A MESSAGE. Both exist because the
// completion accounting is the one thing no off-cluster test can exercise --
// the fake applies posts immediately and never has anything outstanding.
//
//   * A negative tally means MORE completions landed than we said to expect,
//     so an earlier step ran on partial results. Caught here rather than
//     surfacing later as a torn read.
//   * A future that is stepped many times without finishing or posting
//     anything new is stuck. Without this it is an idle client with no error
//     anywhere, which is the worst thing to debug on a cluster.

#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

#include "disco_skip_state.hpp"
#include "ds_get_future.hpp"
#include "ds_put_future.hpp"
#include "ds_rdma_async.hpp"

namespace ds {

/// The cache type the client's futures are built over.
///
/// One type, not two: DS_CACHE_ENABLED decides whether a cache is compiled in,
/// and CacheAdapter's `consult` flag decides at runtime whether it is asked.
/// So the no-cache baseline is a command-line arm of the same binary rather
/// than a second set of template instantiations.
#if DS_CACHE_ENABLED
using ClientCache = CacheAdapter;
#else
using ClientCache = NullCache;
#endif

namespace detail {

/// How many times a future may be stepped without finishing before we call it
/// stuck. Generous: a traversal is a handful of steps per level, a put a
/// handful per level per phase, and a retry restarts. Anything past this is a
/// transition that does not terminate.
inline constexpr uint64_t kMaxFutureSteps = 4096;

}  // namespace detail

/// One in-flight skip-vector operation.
///
/// Holds both a Get and a Put machine rather than a union, because a future
/// slot is reused across operations of either kind and the machines are cheap
/// next to the registered buffers they point at.
template <class Cache>
class SvFuture : public BasicFuture {
 public:
  enum class Kind : uint8_t { None, Get, Put, Range };

  using Conns = std::vector<dory::conn::ReliableConnection *>;
  using Tally = std::vector<int64_t>;
  using AsyncOps = RdmaAsyncOps<Conns, Tally>;

  SvFuture(DsState &s, uint64_t id, Cache &cache, QuorumStats &qstats,
           GetStats &gstats, PutStats &pstats, WriteStats &wstats,
           RangeStats &rstats)
      : BasicFuture{s, id},
        ongoing_(s.layout.num_servers, 0),
        ops_(s.server_conns, s.layout, s.to_poll_per_server, ongoing_, id,
             qstats, &s.vec_hint, &s.node_alloc, &s.vec_alloc),
        get_(ops_, cache, static_cast<uint32_t>(s.layout.cache_layers), gstats),
        put_(ops_, cache, static_cast<uint32_t>(s.layout.cache_layers), pstats,
             wstats),
        range_(ops_, static_cast<uint32_t>(s.layout.cache_layers), rstats) {
    // The timestamp source is a per-run decision carried on Layout, not a
    // per-future one -- every future on a client must agree, or the old_ver
    // chains they write interleave two incomparable clocks.
    ops_.setTsMode(s.layout.ts_mode);
    // The tiebreak for the replicated counter. Without this every client
    // stamps with index 0 and two writers computing the same maximum collide
    // -- which is the uniqueness failure the index exists to prevent, so a
    // missing setter here would silently undo it.
    ops_.setClientIdx(s.client_idx);
  }

  void doGet(Key k, bool measuring = false) {
    begin(Kind::Get, measuring);
    awaiting_ = get_.start(k);
    settleIfImmediate();
    recordIfDone();
  }

  void doPut(Key k, Value v, uint32_t height, bool measuring = false) {
    begin(Kind::Put, measuring);
    awaiting_ = put_.start(k, v, height);
    settleIfImmediate();
    recordIfDone();
  }

  /// A10. The entries land in this future's own buffer, readable through
  /// rangeEntries() until the next doRange on the same future.
  ///
  /// Owned rather than caller-supplied because the future outlives any single
  /// call site here: the benchmark issues a range and comes back to it several
  /// steps later, and a caller's vector would have to stay alive across that.
  void doRange(Key lo, Key hi, size_t cap, bool measuring = false) {
    begin(Kind::Range, measuring);
    range_out_.clear();
    awaiting_ = range_.start(lo, hi, cap, range_out_);
    settleIfImmediate();
    recordIfDone();
  }

  void addToOngoingRDMA(size_t server_idx, int64_t n) {
    ongoing_[server_idx] += n;
    if (ongoing_[server_idx] < 0) {
      throw std::runtime_error(
          "future " + std::to_string(future_id) + " on server " +
          std::to_string(server_idx) +
          " reaped more completions than it awaited: a post under-reported its "
          "completion count, so an earlier step ran on partial results");
    }
  }

  bool tryStepForward() {
    if (kind_ == Kind::None) return true;
    for (int64_t x : ongoing_) {
      if (x > 0) return false;  // not all of this future's reads have landed
    }
    if (++steps_ > detail::kMaxFutureSteps) {
      throw std::runtime_error(
          "future " + std::to_string(future_id) + " is stuck after " +
          std::to_string(steps_) +
          " steps without finishing: a state transition is not terminating");
    }
    awaiting_ = stepActive();
    settleIfImmediate();
    recordIfDone();
    return true;
  }

  [[nodiscard]] bool isDone() const {
    switch (kind_) {
      case Kind::Get:   return get_.finished();
      case Kind::Put:   return put_.finished();
      case Kind::Range: return range_.finished();
      case Kind::None:  break;
    }
    return true;
  }

  [[nodiscard]] bool isMeasuring() const { return measuring_; }
  [[nodiscard]] timepoint getStart() const { return start_; }
  [[nodiscard]] Kind kind() const { return kind_; }
  [[nodiscard]] GetResult const &getResult() const { return get_.result(); }
  [[nodiscard]] PutResult const &putResult() const { return put_.result(); }
  [[nodiscard]] RangeResult const &rangeResult() const {
    return range_.result();
  }
  [[nodiscard]] std::vector<Entry> const &rangeEntries() const {
    return range_out_;
  }

 private:
  void begin(Kind k, bool measuring) {
    kind_ = k;
    // Gated HERE rather than in recordIfDone() so that --latency 0 also skips
    // the start_ timestamp: gating only the recording would leave one of the
    // two clock_gettime calls in place and quietly halve the saving.
    measuring_ = measuring && state.layout.measure_latency;
    steps_ = 0;
    recorded_ = false;
    if (measuring) start_ = std::chrono::steady_clock::now();
  }

  /// Feed the latency profilers when the operation completes.
  ///
  /// THIS WAS MISSING, and silently. begin() stamped start_ and exposed it
  /// through isMeasuring()/getStart() for a caller to use, and no caller ever
  /// did -- only RangeFuture recorded anything. So get_profiler and
  /// put_profiler stayed empty, reportStats() skipped both (it prints a section
  /// only when getMeasurementCount() > 0), and a benchmark log contained no
  /// per-operation latency at all. The failure mode was a MISSING SECTION
  /// rather than a zero, which is why it survived: nothing looked wrong, the
  /// latency panel of a figure was simply blank for this system while the
  /// comparison systems filled theirs in.
  ///
  /// Recorded here rather than in the driver because "the operation finished"
  /// is a fact this class owns; asking every call site to notice it is how it
  /// came to be missed. Guarded by recorded_ so a future that is stepped again
  /// after completing cannot double-count.
  /// Step whichever operation is active. One place, so a new Kind cannot be
  /// half-wired: the switch has no default, so omitting an arm fails to build.
  size_t stepActive() {
    switch (kind_) {
      case Kind::Get:   return get_.step();
      case Kind::Put:   return put_.step();
      case Kind::Range: return range_.step();
      case Kind::None:  break;
    }
    return 0;
  }

  void recordIfDone() {
    if (!measuring_ || recorded_ || !isDone()) return;
    recorded_ = true;
    timepoint const end = std::chrono::steady_clock::now();
    switch (kind_) {
      case Kind::Get:   state.addGetMeasurement(start_, end); break;
      case Kind::Put:   state.addPutMeasurement(start_, end); break;
      case Kind::Range: state.addRangeMeasurement(start_, end); break;
      case Kind::None:  break;
    }
  }

  /// An operation can finish without ever going to the fabric -- a cache miss
  /// on an empty structure, say, or a malformed batch. Nothing will then arrive
  /// to trigger a step, so drive it here until it either blocks on a real
  /// completion or completes.
  void settleIfImmediate() {
    while (awaiting_ == 0 && !isDone()) {
      if (++steps_ > detail::kMaxFutureSteps) {
        throw std::runtime_error(
            "future " + std::to_string(future_id) +
            " made no progress and posted nothing: a transition returned zero "
            "completions without finishing");
      }
      awaiting_ = stepActive();
    }
  }

  Tally ongoing_;
  AsyncOps ops_;
  GetOperation<AsyncOps, Cache> get_;
  PutOperation<AsyncOps, Cache> put_;
  RangeOperation<AsyncOps> range_;
  std::vector<Entry> range_out_;

  Kind kind_ = Kind::None;
  bool measuring_ = false;
  bool recorded_ = false;
  timepoint start_{};
  size_t awaiting_ = 0;
  uint64_t steps_ = 0;
};

}  // namespace ds
