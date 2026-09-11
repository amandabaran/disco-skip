#pragma once

// A chained sequence of RDMA operations, issued as one doorbell per replica.
//
// WHY THIS EXISTS. The operations of remote-design.md §2 are sequences of
// writes and CASes on one node, and issued one at a time each costs a round
// trip. F1 is five, F2 is eleven, and at three replicas a per-replica loop
// multiplies both -- so replication would cost latency rather than only
// bandwidth. Same-QP RC ordering means a linked list of work requests posted
// with a single `postSend` costs *one* round trip for the whole sequence, which
// is the idiom swarm-kv/src/unreliable_maxreg.hpp uses and chimera's
// put_future.hpp applies across servers.
//
// So the Ops surface takes batches rather than individual operations at the
// points where a sequence is issued. `Writer` builds one and submits it; the
// backend decides whether that becomes a chain, a fan-out, or a plain loop.
//
// TWO PROPERTIES THE CHAIN BUYS BEYOND SPEED.
//
//   * Ordering becomes structural. F2 requires `tail_struct_ver` to be CAS'd
//     last, and today that holds because the calls happen in the right order in
//     the source. In a chain, same-QP RC ordering enforces it -- the requirement
//     is expressed in the data rather than trusted to the code.
//
//   * The fence has somewhere to live. F3 needs the staged writes visible at
//     the remote HCA before the CAS that publishes them. With blocking helpers
//     that came free, because each waited for its own completion. In a chain
//     nothing waits, so the publishing CAS must carry IBV_SEND_FENCE -- see
//     the note in ds_rdma.hpp, since dory never sets that flag itself.
//
// A batch is plain data and holds no RDMA concepts, so the fake backends in
// disco-skip/tests apply one by walking it in order -- which is what keeps the
// write path testable off-cluster now that it issues chains.

#include <cstddef>
#include <cstdint>

#include "ds_defs.hpp"
#include "ds_node.hpp"

namespace ds {

enum class BatchKind : uint8_t {
  WriteVec,
  WriteNode,
  CasHandle,  ///< the publishing CAS: fenced, and the one whose result matters
  CasTs,
  CasNextId,
  CasNextKMin,
  CasTailWord,
  /// Fetch-and-add the global timestamp counter.
  ///
  /// Appended AFTER the publishing CAS, never before -- see ds_ts.hpp. Its
  /// returned value becomes the version's timestamp, which is why it cannot be
  /// chained with the CAS that writes `ts`: that CAS's value is this one's
  /// result, and is not known until the chain completes.
  FaaTs,
};

/// One operation in a batch.
///
/// `vec` and `node` are BORROWED. The records must stay alive until submit()
/// returns, which is why Writer keeps its staged copies as locals across the
/// call rather than building them inline. Copying them here instead would mean
/// carrying 320 bytes per entry through a path that is about to copy them into
/// the registered MR anyway.
struct BatchOp {
  BatchKind kind = BatchKind::WriteVec;
  RemoteAddr addr{};
  VecOffset off = kNullVec;
  uint64_t expected = 0;
  uint64_t desired = 0;
  VecRecord const *vec = nullptr;
  NodeRecord const *node = nullptr;
};

/// Longest sequence any operation issues. F2's completion is the biggest at
/// five (two timestamps, two header fields, the tail word); its stage-and-
/// publish is four. Eight leaves room without making this dynamic, which
/// matters because a Batch is a stack local on the write path.
inline constexpr size_t kMaxBatchOps = 8;

/// Longest run of vector writes in one batch -- F2 stages two, the new node's
/// and the existing node's. The RDMA backend needs this many staging slots live
/// at once, since every write in a chain is posted before any completes.
inline constexpr size_t kMaxBatchVecWrites = 2;
inline constexpr size_t kMaxBatchNodeWrites = 1;

class Batch {
 public:
  void writeVec(VecOffset off, VecRecord const &v) {
    if (BatchOp *o = push(BatchKind::WriteVec)) {
      o->off = off;
      o->vec = &v;
      ++vec_writes_;
    }
  }
  void writeNode(RemoteAddr a, NodeRecord const &n) {
    if (BatchOp *o = push(BatchKind::WriteNode)) {
      o->addr = a;
      o->node = &n;
      ++node_writes_;
    }
  }
  /// The publishing CAS. At most one per batch: it is the linearization point,
  /// and two in one chain would make "did this batch commit" ambiguous.
  void casHandle(RemoteAddr a, uint64_t expected, uint64_t desired) {
    if (BatchOp *o = push(BatchKind::CasHandle)) {
      o->addr = a;
      o->expected = expected;
      o->desired = desired;
      ++commits_;
    }
  }
  void casTs(VecOffset off, uint64_t expected, uint64_t desired) {
    if (BatchOp *o = push(BatchKind::CasTs)) {
      o->off = off;
      o->expected = expected;
      o->desired = desired;
    }
  }
  void casNextId(RemoteAddr a, uint64_t expected, uint64_t desired) {
    if (BatchOp *o = push(BatchKind::CasNextId)) {
      o->addr = a;
      o->expected = expected;
      o->desired = desired;
    }
  }
  void casNextKMin(RemoteAddr a, Key expected, Key desired) {
    if (BatchOp *o = push(BatchKind::CasNextKMin)) {
      o->addr = a;
      o->expected = expected;
      o->desired = desired;
    }
  }
  void casTailWord(RemoteAddr a, uint64_t expected, uint64_t desired) {
    if (BatchOp *o = push(BatchKind::CasTailWord)) {
      o->addr = a;
      o->expected = expected;
      o->desired = desired;
    }
  }
  /// Claim a timestamp. At most one per batch: two would make "which value is
  /// this operation's timestamp" ambiguous.
  void faaTs() {
    if (push(BatchKind::FaaTs) != nullptr) ++faas_;
  }

  [[nodiscard]] size_t size() const { return n_; }
  [[nodiscard]] BatchOp const &operator[](size_t i) const { return ops_[i]; }

  /// Did a push fall off the end? A truncated batch would silently skip a step
  /// of the protocol, so backends must refuse to submit one.
  [[nodiscard]] bool overflowed() const { return overflow_; }

  /// Does this batch contain the publishing CAS? A batch without one cannot
  /// fail to commit, so submit() reports it as committed.
  [[nodiscard]] bool hasCommit() const { return commits_ > 0; }
  [[nodiscard]] size_t vecWrites() const { return vec_writes_; }
  [[nodiscard]] size_t nodeWrites() const { return node_writes_; }
  [[nodiscard]] bool hasFaa() const { return faas_ > 0; }

  /// Is this batch issuable as written?
  ///
  /// Checks the two things a backend cannot recover from: a truncated batch,
  /// and more staged records than there are staging slots to hold them
  /// simultaneously. Both are programming errors rather than runtime
  /// conditions, so they are caught here rather than surfacing as a corrupt
  /// write.
  [[nodiscard]] bool wellFormed() const {
    return !overflow_ && commits_ <= 1 && faas_ <= 1 &&
           vec_writes_ <= kMaxBatchVecWrites &&
           node_writes_ <= kMaxBatchNodeWrites;
  }

 private:
  BatchOp *push(BatchKind k) {
    if (n_ >= kMaxBatchOps) {
      overflow_ = true;
      return nullptr;
    }
    ops_[n_] = BatchOp{};
    ops_[n_].kind = k;
    return &ops_[n_++];
  }

  BatchOp ops_[kMaxBatchOps];
  size_t n_ = 0;
  size_t commits_ = 0;
  size_t faas_ = 0;
  size_t vec_writes_ = 0;
  size_t node_writes_ = 0;
  bool overflow_ = false;
};

struct BatchResult {
  /// The timestamp the batch's FaaTs claimed, or kNullTs if it carried none.
  uint64_t ts = kNullTs;
  /// Did the batch reach the fabric at all? False means a malformed batch or a
  /// write that missed its quorum -- the caller cannot assume anything landed.
  bool submitted = false;
  /// Did the publishing CAS commit? True for a batch with no CasHandle.
  bool committed = false;
};

}  // namespace ds
