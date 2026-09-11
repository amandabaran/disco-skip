#pragma once

// A fake AsyncOps: posts are applied immediately and the results buffered, so a
// test can drive the resumable traversal to completion in a loop.
//
// The point is that the state machine under test is EXACTLY the one that runs on
// the cluster -- only the transport differs. What this cannot model is
// concurrency between futures or real completion ordering; what it can model,
// and what the sequencing bugs live in, is the order a single operation issues
// its reads and what it does with each result.
//
// Wraps a FakeReplicaSet so the quorum rules apply here too: a lagged replica
// or a partial commit reaches the async path the same way it reaches the
// blocking one.

#include <cstddef>

#include "ds_async.hpp"
#include "ds_quorum.hpp"
#include "fake_replicas.hpp"

class FakeAsyncOps {
 public:
  FakeAsyncOps(FakeReplicaSet &set, ds::QuorumStats &stats,
               ds::VecOffsetHint *hint = nullptr)
      : set_(set), stats_(stats), hint_(hint), quorum_(set, stats, hint) {}

  size_t replicas() const { return set_.replicas(); }

  ds::VecOffset guess(ds::RemoteAddr a) {
    return hint_ != nullptr ? hint_->guess(a) : ds::kNullVec;
  }

  /// Post a quorum header read, optionally carrying a speculative vector read.
  ///
  /// Applied immediately here. The return value is the completion count a real
  /// driver would await: one per replica, plus one if a vector was speculated.
  size_t postHeaders(ds::RemoteAddr a, ds::VecOffset speculate) {
    pending_addr_ = a;
    pending_spec_ = speculate;
    set_.readNodeAll(a, seen_, ok_, speculate, &spec_, &spec_ok_);
    ++posts_;
    return set_.replicas() + (speculate != ds::kNullVec ? 1u : 0u);
  }

  /// Resolve the quorum over the buffered headers.
  ///
  /// Deliberately the same rules QuorumOps applies -- highest tag with majority
  /// support, and a speculation accepted only when replica 0 voted with the
  /// winner. Reusing the blocking path's readNode here would hide the async
  /// sequencing rather than test it, so the resolve is expressed over the
  /// buffers and the *decisions* are the shared ones.
  bool resolveHeaders(ds::RemoteAddr a, ds::NodeRecord &node, bool &have_vec,
                      ds::VecRecord &vec) {
    size_t const n = set_.replicas();
    size_t got = 0;
    for (size_t r = 0; r < n; ++r) {
      if (ok_[r]) ++got;
    }
    if (got < majority()) return false;

    size_t best = n;
    uint64_t best_tag = 0;
    for (size_t r = 0; r < n; ++r) {
      if (!ok_[r]) continue;
      size_t votes = 0;
      for (size_t q = 0; q < n; ++q) {
        if (ok_[q] && seen_[q].handle == seen_[r].handle) ++votes;
      }
      if (votes < majority()) continue;
      uint64_t const tag = seen_[r].handle.tag();
      if (best == n || tag > best_tag) {
        best = r;
        best_tag = tag;
      }
    }
    if (best == n) return false;  // no majority-supported value yet

    node = seen_[best];
    winner_ = best;
    ds::VecOffset const truth = node.handle.offset();

    have_vec = false;
    if (pending_spec_ != ds::kNullVec) {
      if (spec_ok_ && ok_[0] && seen_[0].handle == seen_[best].handle &&
          pending_spec_ == truth) {
        vec = spec_;
        have_vec = true;
        ++stats_.spec_hits;
      } else {
        ++stats_.spec_misses;
      }
      ++stats_.speculated;
    }
    if (hint_ != nullptr) hint_->record(a, truth, pending_spec_);
    pending_spec_ = ds::kNullVec;
    return true;
  }

  size_t postVec(ds::VecOffset off) {
    // L4: from a replica that voted with the winning handle.
    vec_ok_ = set_.readVecFrom(winner_, off, vec_buf_);
    ++posts_;
    return 1;
  }
  bool resolveVec(ds::VecRecord &vec) {
    if (!vec_ok_) return false;
    vec = vec_buf_;
    return true;
  }

  size_t postBatch(ds::Batch const &b) {
    // Applied immediately, and the outcome stashed for resolveBatch. On the
    // wire the two halves are genuinely separate: the chains are posted, the
    // future suspends, and the swapbacks only become meaningful once the
    // completions land.
    last_batch_ = quorum_.submit(b);
    ++posts_;
    return set_.replicas();
  }

  /// Whether the batch's publishing CAS reached a majority (L1).
  ds::BatchResult resolveBatch(ds::Batch const &) { return last_batch_; }

  uint64_t now() { return set_.now(); }

  // Allocation is client-local, so it comes from the replica set's single
  // allocator pair rather than per replica -- a RemoteAddr and a VecOffset mean
  // the same thing on every replica, which is what lets one batch serve all.
  ds::VecOffset allocVec() { return set_.allocVec(); }
  ds::RemoteAddr allocNode() { return set_.allocNode(); }

  /// How many times this operation went to the fabric. The quantity the future
  /// path exists to overlap across operations, so a test can compare it against
  /// the blocking path's.
  uint64_t posts() const { return posts_; }
  void resetPosts() { posts_ = 0; }

 private:
  size_t majority() const { return set_.replicas() / 2 + 1; }

  FakeReplicaSet &set_;
  ds::QuorumStats &stats_;
  ds::VecOffsetHint *hint_;
  ds::QuorumOps<FakeReplicaSet> quorum_;

  ds::NodeRecord seen_[8]{};
  bool ok_[8] = {};
  ds::RemoteAddr pending_addr_{};
  ds::VecOffset pending_spec_ = ds::kNullVec;
  ds::VecRecord spec_{};
  bool spec_ok_ = false;
  size_t winner_ = 0;
  ds::VecRecord vec_buf_{};
  bool vec_ok_ = false;
  ds::BatchResult last_batch_{};
  uint64_t posts_ = 0;
};

/// Drive a resumable traversal to completion, as the real driver would.
template <class Ops>
inline ds::TraversalResult runAsyncTraversal(Ops &ops, ds::Key k, uint32_t layers,
                                         ds::PathStep *path,
                                         uint64_t *steps_out = nullptr) {
  ds::TraversalFuture<Ops> f(ops);
  uint64_t steps = 0;
  size_t await = f.start(k, layers, path);
  // A real driver waits for `await` completions before each step; here they are
  // already applied, so stepping immediately is the same sequence of decisions.
  while (!f.finished()) {
    (void)await;
    await = f.step();
    if (++steps > 100000) break;  // guard a sequencing bug rather than hang
  }
  if (steps_out != nullptr) *steps_out = steps;
  return f.result();
}
