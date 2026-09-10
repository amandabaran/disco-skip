#pragma once

// Blocking one-sided RDMA, for setup and verification only.
//
// The benchmark path uses the future state machines, which never block. These
// helpers do: each posts one signalled work request and spins on the send
// completion queue until it lands. That is the right shape for bootstrap and
// for --selftest -- both are strictly sequential phases outside the measurement
// window -- and the wrong shape for anything on the hot path.
//
// SINGLE OUTSTANDING OPERATION. Each call drains one completion from the
// connection's send CQ, so it must not run while a future has work in flight on
// the same connection: it would consume that future's completion and the
// future would wait forever. Use these before futures start or after they have
// all drained.

#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

#include <dory/conn/rc.hpp>
#include <dory/extern/ibverbs.hpp>

#include "ds_defs.hpp"
#include "ds_node.hpp"
#include "layout.hpp"  // Layout::nodeAddrOf

namespace ds {

/// Work-request id used by the blocking helpers. Distinct from any future id so
/// that a completion turning up on the wrong path is recognisable in a trace
/// rather than silently attributed to future 0.
inline constexpr uint64_t kBlockingWrId = ~uint64_t{0};

namespace detail {

/// Spin until exactly one completion has been reaped, checking its status.
inline void awaitOne(dory::conn::ReliableConnection &rc, char const *what) {
  std::vector<struct ibv_wc> wces;
  while (true) {
    wces.resize(1);
    if (!rc.pollCqIsOk(dory::conn::ReliableConnection::SendCq, wces)) {
      throw std::runtime_error(std::string("polling the send CQ failed during ") +
                               what);
    }
    if (wces.empty()) continue;
    if (wces[0].status != IBV_WC_SUCCESS) {
      // ibv_wc_status is an unsigned enum, and handing it straight to
      // std::to_string picks the int overload -- which -Wsign-promo rejects.
      // Widened explicitly rather than left to promotion.
      throw std::runtime_error(std::string("RDMA ") + what + " failed: status " +
                               std::to_string(static_cast<unsigned>(wces[0].status)));
    }
    return;
  }
}

}  // namespace detail

/// One-sided WRITE, returning once the completion has landed.
///
/// /local/ must lie inside the registered MR -- the HCA reads from it directly,
/// so a stack buffer would not be addressable.
inline void blockingWrite(dory::conn::ReliableConnection &rc, void *local,
                          uint32_t len, uintptr_t remote) {
  if (!rc.postSendSingle(dory::conn::ReliableConnection::RdmaWrite,
                         kBlockingWrId, local, len, remote)) {
    throw std::runtime_error("failed to post a blocking RDMA write");
  }
  detail::awaitOne(rc, "write");
}

/// One-sided READ, returning once /local/ holds the data.
inline void blockingRead(dory::conn::ReliableConnection &rc, void *local,
                         uint32_t len, uintptr_t remote) {
  if (!rc.postSendSingle(dory::conn::ReliableConnection::RdmaRead,
                         kBlockingWrId, local, len, remote)) {
    throw std::runtime_error("failed to post a blocking RDMA read");
  }
  detail::awaitOne(rc, "read");
}

/// One-sided CAS. Returns true if it succeeded, i.e. if the value the HCA found
/// was the one we expected. /swap/ receives the pre-CAS remote value either
/// way, which is why a failed CAS still tells you what is actually there.
inline bool blockingCas(dory::conn::ReliableConnection &rc, uint64_t *swap,
                        uintptr_t remote, uint64_t expected, uint64_t desired) {
  if (!rc.postSendSingleCas(kBlockingWrId, swap, remote, expected, desired)) {
    throw std::runtime_error("failed to post a blocking RDMA CAS");
  }
  detail::awaitOne(rc, "compare-and-swap");
  return *swap == expected;
}

/// Write one node record to every replica.
///
/// /staging/ must be an MR-resident NodeRecord holding the bytes to write. The
/// same bytes go to every replica, since the arena layout is identical on all
/// of them -- that is what makes a RemoteAddr replica-independent.
template <class Conns>
void writeNodeAllReplicas(Conns &conns, NodeRecord *staging, RemoteAddr addr) {
  for (size_t r = 0; r < conns.size(); ++r) {
    auto &rc = *conns[r];
    blockingWrite(rc, staging, kNodeRecordBytes,
                  Layout::nodeAddrOf(rc.remoteBuf(), addr));
  }
}

/// A node reader satisfying StructureVerifier's Reader concept, backed by real
/// RDMA reads.
///
/// Retries a torn read rather than reporting it: interface doc §7 is explicit
/// that a torn read means "re-read" and says nothing about the cache being
/// wrong. Only a read that will not settle is reported, since during --selftest
/// the structure is quiescent and so a persistent mismatch is real corruption,
/// not a race.
template <class Conns>
class RdmaNodeReader {
 public:
  /// @param buf  an MR-resident NodeRecord to read into
  RdmaNodeReader(Conns &conns, NodeRecord *buf, size_t replica = 0)
      : conns_(conns), buf_(buf), replica_(replica) {}

  bool read(RemoteAddr a, NodeRecord &out) {
    if (a.isNull()) return false;
    auto &rc = *conns_[replica_];
    uintptr_t const remote = Layout::nodeAddrOf(rc.remoteBuf(), a);

    for (int attempt = 0; attempt < kTornReadRetries; ++attempt) {
      blockingRead(rc, buf_, kNodeRecordBytes, remote);
      ++reads_;
      if (slotIsConsistent(*buf_)) {
        out = *buf_;
        return true;
      }
      ++torn_;
    }
    return false;
  }

  [[nodiscard]] uint64_t reads() const { return reads_; }
  [[nodiscard]] uint64_t tornReads() const { return torn_; }

 private:
  static constexpr int kTornReadRetries = 8;

  Conns &conns_;
  NodeRecord *buf_;
  size_t replica_;
  uint64_t reads_ = 0;
  uint64_t torn_ = 0;
};

}  // namespace ds
