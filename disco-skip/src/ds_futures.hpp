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
  enum class Kind : uint8_t { None, Get, Put };

  using Conns = std::vector<dory::conn::ReliableConnection *>;
  using Tally = std::vector<int64_t>;
  using AsyncOps = RdmaAsyncOps<Conns, Tally>;

  SvFuture(DsState &s, uint64_t id, Cache &cache, QuorumStats &qstats,
           GetStats &gstats, PutStats &pstats, WriteStats &wstats)
      : BasicFuture{s, id},
        ongoing_(s.layout.num_servers, 0),
        ops_(s.server_conns, s.layout, s.to_poll_per_server, ongoing_, id,
             qstats, &s.vec_hint, &s.node_alloc, &s.vec_alloc),
        get_(ops_, cache, static_cast<uint32_t>(s.layout.cache_layers), gstats),
        put_(ops_, cache, static_cast<uint32_t>(s.layout.cache_layers), pstats,
             wstats) {
    // The timestamp source is a per-run decision carried on Layout, not a
    // per-future one -- every future on a client must agree, or the old_ver
    // chains they write interleave two incomparable clocks.
    ops_.setTsMode(s.layout.ts_mode);
  }

  void doGet(Key k, bool measuring = false) {
    begin(Kind::Get, measuring);
    awaiting_ = get_.start(k);
    settleIfImmediate();
  }

  void doPut(Key k, Value v, uint32_t height, bool measuring = false) {
    begin(Kind::Put, measuring);
    awaiting_ = put_.start(k, v, height);
    settleIfImmediate();
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
    awaiting_ = (kind_ == Kind::Get) ? get_.step() : put_.step();
    settleIfImmediate();
    return true;
  }

  [[nodiscard]] bool isDone() const {
    if (kind_ == Kind::None) return true;
    return (kind_ == Kind::Get) ? get_.finished() : put_.finished();
  }

  [[nodiscard]] bool isMeasuring() const { return measuring_; }
  [[nodiscard]] timepoint getStart() const { return start_; }
  [[nodiscard]] Kind kind() const { return kind_; }
  [[nodiscard]] GetResult const &getResult() const { return get_.result(); }
  [[nodiscard]] PutResult const &putResult() const { return put_.result(); }

 private:
  void begin(Kind k, bool measuring) {
    kind_ = k;
    measuring_ = measuring;
    steps_ = 0;
    if (measuring) start_ = std::chrono::steady_clock::now();
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
      awaiting_ = (kind_ == Kind::Get) ? get_.step() : put_.step();
    }
  }

  Tally ongoing_;
  AsyncOps ops_;
  GetOperation<AsyncOps, Cache> get_;
  PutOperation<AsyncOps, Cache> put_;

  Kind kind_ = Kind::None;
  bool measuring_ = false;
  timepoint start_{};
  size_t awaiting_ = 0;
  uint64_t steps_ = 0;
};

}  // namespace ds
