#pragma once

// A replica set of independent fake arenas, so the states CAS-ABD exists for
// can be built by hand.
//
// Every interesting case in ds_quorum.hpp is one replica disagreeing with two
// others: a lagged replica, a partially applied commit, two handles sharing a
// tag, a replica that is down. None of those can be provoked deliberately on a
// healthy cluster, and all of them are three lines here.
//
// Each replica is a whole FakeOps, which keeps the per-replica primitives
// trivial -- they forward. The allocators and the clock are deliberately NOT
// per-replica: they are client-local state, and a real client has exactly one
// of each regardless of how many replicas it talks to.

#include <cstddef>
#include <vector>

#include "ds_node.hpp"
#include "ds_quorum.hpp"
#include "fake_ops.hpp"

class FakeReplicaSet {
 public:
  FakeReplicaSet(size_t replicas, uint32_t layers, size_t slots = 4096)
      : down_(replicas, false) {
    arenas_.reserve(replicas);
    for (size_t i = 0; i < replicas; ++i) {
      arenas_.push_back(buildInitialArena(layers, slots));
    }
  }

  // ── The ReplicaSet concept ───────────────────────────────────────────────

  size_t replicas() const { return arenas_.size(); }

  /// Read the header from every replica, optionally carrying a speculative
  /// vector read alongside.
  ///
  /// On the wire every request is posted before any is drained, and replica 0's
  /// header plus the speculation are one chain on one QP -- so the whole thing
  /// is a single round trip regardless of replica count. Here it is a loop,
  /// which models the *cost* faithfully (the reads are all counted) without the
  /// latency there is nothing to overlap.
  void readNodeAll(ds::RemoteAddr a, ds::NodeRecord *out, bool *ok,
                   ds::VecOffset speculate, ds::VecRecord *spec,
                   bool *spec_ok) {
    for (size_t r = 0; r < arenas_.size(); ++r) {
      ok[r] = readNodeFrom(r, a, out[r]);
    }
    *spec_ok = false;
    if (speculate != ds::kNullVec && !down_[0]) {
      *spec_ok = arenas_[0].readVec(speculate, *spec);
    }
  }

  bool readNodeFrom(size_t r, ds::RemoteAddr a, ds::NodeRecord &node) {
    if (down_[r]) return false;
    return arenas_[r].readNode(a, node);
  }
  bool readVecFrom(size_t r, ds::VecOffset off, ds::VecRecord &vec) {
    if (down_[r]) return false;
    return arenas_[r].readVec(off, vec);
  }
  bool casHandleOn(size_t r, ds::RemoteAddr a, uint64_t e, uint64_t d) {
    if (down_[r]) return false;
    return arenas_[r].casHandle(a, e, d);
  }

  /// Issue a chained batch on every replica.
  ///
  /// On the wire this posts each replica's chain before draining any of them,
  /// so N replicas cost one round trip rather than N. Here it is a loop, which
  /// is the right fake: applying each replica's chain in order reproduces what
  /// same-QP RC ordering guarantees, and there is no latency to overlap.
  ///
  /// The batch count is what a test asserts on, because it is the round-trip
  /// count -- the thing the chaining exists to reduce.
  void submitAll(ds::Batch const &b, bool *submitted, bool *committed) {
    ++batches_;
    uint64_t best = ds::kNullTs;
    size_t answered = 0;
    for (size_t r = 0; r < arenas_.size(); ++r) {
      if (down_[r]) {
        submitted[r] = false;
        committed[r] = false;
        continue;
      }
      ds::BatchResult const res = arenas_[r].submit(b);
      submitted[r] = res.submitted;
      committed[r] = res.committed;
      // THE COUNTER IS REPLICATED, so the stamp is the maximum over the
      // replicas that answered -- not replica 0's. Each arena has its own
      // counter and they advance independently (a replica that was down misses
      // increments), which is exactly the divergence the maximum exists to
      // absorb. Modelled here rather than left as replica 0, or the tests would
      // pass against a single-counter fake while the cluster ran a replicated
      // one.
      if (res.ts != ds::kNullTs) {
        ++answered;
        if (best == ds::kNullTs || res.ts > best) best = res.ts;
      }
    }
    if (best != ds::kNullTs) {
      last_ts_ = best;
      if (answered < arenas_.size()) ++ts_partial_;
    }
  }

  // Client-local, so one of each rather than one per replica.
  uint64_t now() { return clock_ += 10; }

  [[nodiscard]] ds::TsMode tsMode() const { return arenas_[0].tsMode(); }
  void setTsMode(ds::TsMode m) {
    for (auto &a : arenas_) a.setTsMode(m);
  }
  /// The maximum stamp over the replicas that answered -- the counter is
  /// replicated on every server, so no one of them is authoritative.
  [[nodiscard]] uint64_t lastTs() const { return last_ts_; }
  /// Stamps taken from fewer than all replicas: unique and monotone for their
  /// own writer, but without the cross-writer real-time guarantee. ds_ts.hpp.
  [[nodiscard]] uint64_t tsPartial() const { return ts_partial_; }

  /// Observe the replicated counter without incrementing it: read every
  /// replica, take the maximum. A replica that is down simply does not vote --
  /// the maximum over those that answered is still a valid snapshot, it just
  /// may sit further behind.
  [[nodiscard]] uint64_t readTsCounter() const {
    uint64_t best = 0;
    for (size_t r = 0; r < arenas_.size(); ++r) {
      if (down_[r]) continue;
      uint64_t const c = arenas_[r].readTsCounter();
      if (c > best) best = c;
    }
    return best;
  }
  ds::VecOffset allocVec() {
    // Offsets must mean the same thing on every replica, so allocation is a
    // client-side decision handed to all of them -- which is exactly why a
    // RemoteAddr and a VecOffset are replica-independent.
    return arenas_[0].allocVec();
  }
  ds::RemoteAddr allocNode() { return arenas_[0].allocNode(); }

  // ── Test-side manipulation ───────────────────────────────────────────────

  FakeOps &arena(size_t r) { return arenas_[r]; }

  /// Take a replica offline: every operation on it fails, as a fail-stop
  /// memory node does (§8).
  void setDown(size_t r, bool down) { down_[r] = down; }
  bool isDown(size_t r) const { return down_[r]; }

  /// Apply a handle to one replica only, which is how a lagged replica or a
  /// partially applied commit is staged.
  void forceHandle(size_t r, ds::RemoteAddr a, ds::Handle h) {
    arenas_[r].node(a).handle = h;
  }

  /// Do the replicas agree on this node's handle?
  bool agreeOn(ds::RemoteAddr a) {
    for (size_t r = 1; r < arenas_.size(); ++r) {
      if (arenas_[r].node(a).handle != arenas_[0].node(a).handle) return false;
    }
    return true;
  }

  /// How many replicas hold exactly this handle.
  size_t votesFor(ds::RemoteAddr a, ds::Handle h) {
    size_t n = 0;
    for (auto &ar : arenas_) {
      if (ar.node(a).handle == h) ++n;
    }
    return n;
  }

  /// Chained submissions, i.e. round trips spent writing.
  uint64_t batches() const { return batches_; }

  /// Per-replica RDMA totals, summed. The interesting comparison against the
  /// single-replica path, since replication multiplies reads and CASes but not
  /// the logical operation count.
  uint64_t totalReads() {
    uint64_t n = 0;
    for (auto &a : arenas_) n += a.reads();
    return n;
  }
  uint64_t totalCas() {
    uint64_t n = 0;
    for (auto &a : arenas_) n += a.casCount();
    return n;
  }

 private:
  std::vector<FakeOps> arenas_;
  std::vector<bool> down_;
  uint64_t clock_ = 1000;
  uint64_t last_ts_ = ds::kNullTs;
  uint64_t ts_partial_ = 0;
  uint64_t batches_ = 0;
};
