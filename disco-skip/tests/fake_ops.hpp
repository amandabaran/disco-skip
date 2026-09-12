#pragma once

// A local stand-in for the memory servers, plus the fixtures that stage the
// concurrent states the helping paths exist for.
//
// Shared by traverse_test and get_test: both need an arena that supports the
// same reads and CAS operations the RDMA path does, so that the Traversal and
// Getter under test are the same code that runs on the cluster.

#include <cstdint>
#include <vector>

#include "ds_batch.hpp"
#include "ds_ts.hpp"
#include "ds_bootstrap.hpp"
#include "ds_traverse.hpp"
#include "ds_node.hpp"
#include "layout.hpp"

/// A local stand-in for the memory servers, supporting reads and the four CAS
/// operations the helping path needs.
class FakeOps {
 public:
  FakeOps(size_t nodes, size_t vecs) : nodes_(nodes), vecs_(vecs) {
    for (auto &n : nodes_) ds::initNode(n, 0, 0, ds::kNullVec);
    for (auto &v : vecs_) ds::initVec(v, false, ds::kBootstrapTs);
  }

  // ── The Ops concept ──────────────────────────────────────────────────────

  /// A 64-byte header read.
  bool readNode(ds::RemoteAddr a, ds::NodeRecord &node) {
    if (a.isNull() || a.id >= nodes_.size()) return false;
    node = nodes_[a.id];
    ++node_reads_;
    return true;
  }

  /// A 320-byte vector read -- five times the bytes, and the one the layout
  /// exists to avoid on a pass-through hop.
  bool readVec(ds::VecOffset off, ds::VecRecord &vec) {
    if (off == ds::kNullVec || off >= vecs_.size()) return false;
    vec = vecs_[off];
    ++vec_reads_;
    return true;
  }

  /// Both, for callers that always want the pair (the verifier).
  bool read(ds::RemoteAddr a, ds::NodeRecord &node, ds::VecRecord &vec) {
    if (!readNode(a, node)) return false;
    return readVec(node.handle.offset(), vec);
  }

  bool casTs(ds::VecOffset off, uint64_t expected, uint64_t desired) {
    ++cas_ts_;
    if (off >= vecs_.size()) return false;
    if (vecs_[off].ts != expected) return false;
    vecs_[off].ts = desired;
    return true;
  }

  bool casNextId(ds::RemoteAddr a, uint64_t expected, uint64_t desired) {
    ++cas_next_id_;
    if (a.id >= nodes_.size()) return false;
    if (nodes_[a.id].next_id != expected) return false;
    nodes_[a.id].next_id = desired;
    return true;
  }

  bool casNextKMin(ds::RemoteAddr a, ds::Key expected, ds::Key desired) {
    ++cas_next_k_min_;
    if (a.id >= nodes_.size()) return false;
    if (nodes_[a.id].next_k_min != expected) return false;
    nodes_[a.id].next_k_min = desired;
    return true;
  }

  bool casTailWord(ds::RemoteAddr a, uint64_t expected, uint64_t desired) {
    ++cas_tail_;
    if (a.id >= nodes_.size()) return false;
    ds::NodeRecord &n = nodes_[a.id];
    uint64_t const actual = ds::packTailWord(n.level, n.tail_struct_ver);
    if (actual != expected) return false;
    n.level = static_cast<uint32_t>(desired & 0xFFFFFFFFu);
    n.tail_struct_ver = static_cast<uint32_t>(desired >> 32);
    return true;
  }

  /// The local-clock source (Clock and Tsc modes both read it). A counter, not
  /// a real clock: tests need monotonicity and reproducibility, and neither
  /// rdtscp nor CLOCK_REALTIME gives either.
  ///
  /// The step is settable, and may be NEGATIVE, so a test can make this clock
  /// run backwards faster than writes arrive. That is the adversarial case
  /// stampOver() exists for -- a harsher version of two machines disagreeing by
  /// more than the gap between successive versions of one node -- and it cannot
  /// be produced by waiting. See ts_test.cc.
  uint64_t now() {
    clock_ = static_cast<uint64_t>(static_cast<int64_t>(clock_) + clock_step_);
    return clock_ == ds::kNullTs ? 1 : clock_;
  }
  void setClockStep(int64_t step) { clock_step_ = step; }

  [[nodiscard]] ds::TsMode tsMode() const { return ts_mode_; }
  void setTsMode(ds::TsMode m) { ts_mode_ = m; }

  /// The global timestamp counter this arena holds, fetched-and-added by
  /// FaaTs. Starts at 0, so the first claimed timestamp is 1 -- which is what
  /// keeps it clear of kNullTs.
  uint64_t faaTs() { return ts_counter_++; }

  /// Observe the counter WITHOUT incrementing it. A range query needs a
  /// snapshot, not a slot: readers that FAA'd would contend with each other and
  /// burn counter values for nothing. See ds_range.hpp.
  [[nodiscard]] uint64_t readTsCounter() const { return ts_counter_; }
  void setClientIdx(uint64_t i) { client_idx_ = i; }
  [[nodiscard]] uint64_t clientIdx() const { return client_idx_; }
  [[nodiscard]] uint64_t tsCounter() const { return ts_counter_; }

  // ── The write half of the Ops concept (F1/F2) ────────────────────────────
  //
  // Writes are plain stores here. On the wire they are one-sided RDMA WRITEs to
  // every replica, so nothing may assume they land atomically as a unit: the
  // fence plus the publishing CAS is what makes them visible together. A test
  // that wants to observe a half-finished operation constructs that state
  // directly (see stageMidSplitOn) rather than interleaving these.

  bool writeVec(ds::VecOffset off, ds::VecRecord const &vec) {
    if (off == ds::kNullVec || off >= vecs_.size()) return false;
    vecs_[off] = vec;
    ++vec_writes_;
    return true;
  }

  bool writeNode(ds::RemoteAddr a, ds::NodeRecord const &node) {
    if (a.isNull() || a.id >= nodes_.size()) return false;
    nodes_[a.id] = node;
    ++node_writes_;
    return true;
  }

  /// F3's SEND_FENCE. A no-op locally: there is no reordering to guard against
  /// in a single-threaded fake. Counted so a test can assert that a fence was
  /// actually issued before the publishing CAS, which is the property that does
  /// not survive being got wrong on real hardware.
  void fence() { ++fences_; }

  /// The publishing CAS. Distinct from the others because this is the one that
  /// linearizes the operation (L1) -- and the only one whose failure means
  /// "start over" rather than "somebody else already did it".
  bool casHandle(ds::RemoteAddr a, uint64_t expected, uint64_t desired) {
    ++cas_handle_;
    if (a.id >= nodes_.size()) return false;
    if (nodes_[a.id].handle.raw != expected) return false;
    nodes_[a.id].handle = ds::Handle{desired};
    return true;
  }

  /// Apply a batch in order.
  ///
  /// The whole point of a batch on the wire is that it is one chained doorbell;
  /// here it is simply a loop, which is exactly right -- a fake that moves no
  /// data has nothing to gain from chaining, and applying in order reproduces
  /// what same-QP RC ordering guarantees. Counted so a test can assert that an
  /// operation issued ONE batch rather than several, which is the property the
  /// round-trip count depends on.
  ds::BatchResult submit(ds::Batch const &b) {
    ds::BatchResult out;
    if (!b.wellFormed()) return out;  // submitted == false
    ++batches_;
    out.submitted = true;
    out.committed = !b.hasCommit();
    for (size_t i = 0; i < b.size(); ++i) {
      ds::BatchOp const &o = b[i];
      switch (o.kind) {
        case ds::BatchKind::WriteVec:
          if (!writeVec(o.off, *o.vec)) out.submitted = false;
          break;
        case ds::BatchKind::WriteNode:
          if (!writeNode(o.addr, *o.node)) out.submitted = false;
          break;
        case ds::BatchKind::CasHandle:
          out.committed = casHandle(o.addr, o.expected, o.desired);
          break;
        case ds::BatchKind::CasTs:
          (void)casTs(o.off, o.expected, o.desired);
          break;
        case ds::BatchKind::CasNextId:
          (void)casNextId(o.addr, o.expected, o.desired);
          break;
        case ds::BatchKind::CasNextKMin:
          (void)casNextKMin(o.addr, static_cast<ds::Key>(o.expected),
                            static_cast<ds::Key>(o.desired));
          break;
        case ds::BatchKind::CasTailWord:
          (void)casTailWord(o.addr, o.expected, o.desired);
          break;
        case ds::BatchKind::FaaTs:
          // Claimed here, in chain order -- so it lands after the publishing
          // CAS, which is the property the ordering depends on.
          out.ts = ds::tsFromFaa(faaTs(), client_idx_);
          break;
      }
    }
    return out;
  }

  // Client-local bump allocation, mirroring Vec/NodeAllocator. Exposed through
  // Ops so the F1/F2 logic can allocate without knowing whether it is talking
  // to RDMA or to this.
  ds::VecOffset allocVec() {
    if (next_vec_ >= vecs_.size()) return ds::kNullVec;
    return static_cast<ds::VecOffset>(next_vec_++);
  }
  ds::RemoteAddr allocNode() {
    if (next_node_ >= nodes_.size()) return ds::RemoteAddr{};
    return ds::RemoteAddr{next_node_++};
  }

  // ── Test-side manipulation ───────────────────────────────────────────────

  ds::NodeRecord &node(ds::RemoteAddr a) { return nodes_[a.id]; }
  ds::NodeRecord &node(uint64_t id) { return nodes_[id]; }
  ds::VecRecord &vecAt(ds::VecOffset off) { return vecs_[off]; }
  [[nodiscard]] uint64_t vecCount() const { return vecs_.size(); }
  ds::VecRecord &vecOf(ds::RemoteAddr a) { return vecs_[nodes_[a.id].handle.offset()]; }

  uint64_t nodeReads() const { return node_reads_; }
  uint64_t vecReads() const { return vec_reads_; }
  uint64_t reads() const { return node_reads_ + vec_reads_; }

  // The rest of RdmaNodeReader's and RdmaOps' reporting surface, so this is a
  // complete stand-in and ds_selftest.hpp can be *run* here rather than only
  // type-checked. The two retry counters are always zero: those retries exist
  // because a real read can catch a node mid-write, which cannot happen in a
  // single-threaded fake.
  uint64_t bytesRead() const {
    return node_reads_ * ds::kNodeRecordBytes + vec_reads_ * ds::kVecRecordBytes;
  }
  uint64_t unstableRetries() const { return 0; }
  uint64_t staleRetries() const { return 0; }
  uint64_t casCount() const {
    return cas_ts_ + cas_next_id_ + cas_next_k_min_ + cas_tail_ + cas_handle_;
  }
  uint64_t bytesWritten() const {
    return node_writes_ * ds::kNodeRecordBytes +
           vec_writes_ * ds::kVecRecordBytes;
  }
  uint64_t casTsCalls() const { return cas_ts_; }
  uint64_t casTailCalls() const { return cas_tail_; }
  uint64_t casNextIdCalls() const { return cas_next_id_; }
  uint64_t casNextKMinCalls() const { return cas_next_k_min_; }
  uint64_t casHandleCalls() const { return cas_handle_; }
  uint64_t nodeWrites() const { return node_writes_; }
  uint64_t vecWrites() const { return vec_writes_; }
  uint64_t fences() const { return fences_; }
  uint64_t batches() const { return batches_; }
  uint64_t clockNow() const { return clock_; }

  /// Point the bump allocators somewhere a fixture is not already using.
  void seedAllocators(uint64_t first_node, ds::VecOffset first_vec) {
    next_node_ = first_node;
    next_vec_ = first_vec;
  }

 private:
  std::vector<ds::NodeRecord> nodes_;
  std::vector<ds::VecRecord> vecs_;
  uint64_t clock_ = 1000;
  int64_t clock_step_ = 10;
  uint64_t ts_counter_ = 0;
  /// This writer's tiebreak index. One arena models one replica, so the
  /// maximum is trivially this counter's value; the index is what keeps two
  /// writers that saw the same value from claiming the same stamp.
  uint64_t client_idx_ = 0;
  ds::TsMode ts_mode_ = ds::TsMode::Tsc;
  uint64_t next_node_ = ds::kFirstDynamicId;
  uint64_t next_vec_ = ds::kFirstDynamicVec;
  uint64_t node_reads_ = 0, vec_reads_ = 0, cas_ts_ = 0, cas_next_id_ = 0, cas_next_k_min_ = 0,
           cas_tail_ = 0, cas_handle_ = 0, node_writes_ = 0, vec_writes_ = 0, fences_ = 0,
           batches_ = 0;
};

/// Insert a key into a data node sequentially, as a settled write would leave
/// it. Not the real F1 path -- just enough structure for a traversal to find.
inline void seedDataKey(FakeOps &ops, ds::Key k, ds::Value v) {
  ds::VecRecord &dv = ops.vecOf(ds::RemoteAddr{ds::kInitialDataId});
  uint32_t i = dv.size;
  while (i > 0 && dv.e[i - 1].key > k) {
    dv.e[i] = dv.e[i - 1];
    --i;
  }
  dv.e[i] = ds::Entry{k, v};
  ++dv.size;
}


/// Build the initial structure into a fresh arena.
///
/// /slots/ sizes the dynamic part of both arenas. It has to be generous for
/// write tests: there is no reclamation (invariants.md §5), so *every* write
/// consumes a vector and a split consumes three plus a node. A workload of N
/// puts therefore needs well over N vectors, and running out shows up as
/// Exhausted rather than as a wrong answer -- which is correct behaviour, but
/// makes a test look broken when it is only under-provisioned.
inline FakeOps buildInitialArena(uint32_t layers, size_t slots = 512) {
  FakeOps ops(ds::kFirstDynamicId + slots, ds::kFirstDynamicVec + slots);
  ds::InitialNode built[ds::kMaxLayers + 1];
  uint32_t const n =
      ds::buildInitialStructure(layers, ds::kBootstrapTs, built);
  for (uint32_t i = 0; i < n; ++i) {
    ops.node(built[i].addr) = built[i].node;
    ops.vecAt(built[i].vec_offset) = built[i].vec;
  }
  return ops;
}

/// The mid-split state the design is about: /existing/'s handle has been CAS'd
/// to a new vector carrying the split descriptor, but nothing has propagated
/// into its header yet.
struct SplitFixture {
  ds::RemoteAddr existing{};
  ds::RemoteAddr created{};
  ds::Key split_key = 0;
  ds::Key stay_key = 0;   ///< a key that stayed behind in /existing/
  ds::Key moved_key = 0;  ///< a key that moved into /created/
  ds::VecOffset new_vec = 0;
};

inline SplitFixture stageMidSplitOn(FakeOps &ops, ds::RemoteAddr existing,
                                    bool pending_ts) {
  SplitFixture f;
  f.existing = existing;
  f.created = ds::RemoteAddr{ds::kFirstDynamicId + 256};
  f.split_key = 300;
  f.stay_key = 100;
  f.moved_key = 400;
  f.new_vec = ds::kFirstDynamicVec + 256;
  ds::VecOffset const created_vec = ds::kFirstDynamicVec + 257;

  ds::NodeRecord &e = ops.node(f.existing);

  // The node the split creates: keys >= split_key, inheriting the successor.
  ds::initNode(ops.node(f.created), f.split_key, ds::kDataLevel, created_vec);
  ops.node(f.created).next_id = e.next_id;
  ops.node(f.created).next_k_min = e.next_k_min;
  ds::initVec(ops.vecAt(created_vec), /*is_orphan=*/true, /*ts=*/1000);
  ops.vecAt(created_vec).size = 2;
  ops.vecAt(created_vec).e[0] = ds::Entry{f.split_key, f.split_key * 7};
  ops.vecAt(created_vec).e[1] = ds::Entry{f.moved_key, f.moved_key * 7};

  // The existing node's new vector: keys < split_key, plus the descriptor
  // telling a helper what still has to reach the header.
  ds::VecRecord &nv = ops.vecAt(f.new_vec);
  ds::initVec(nv, /*is_orphan=*/false, pending_ts ? ds::kNullTs : 2000);
  nv.size = 2;
  nv.e[0] = ds::Entry{f.stay_key, f.stay_key * 7};
  nv.e[1] = ds::Entry{200, 1400};
  nv.next_id = f.created.id;
  nv.k_min_next = f.split_key;
  nv.old_ver = e.handle.offset();
  nv.struct_ver = e.handle.structVer() + 1;
  nv.content_ver = e.handle.contentVer();

  // The handle CAS: publishes the new vector and opens the window. The header
  // still carries its pre-split next_id / next_k_min.
  e.handle = e.handle.withStruct(f.new_vec);
  return f;
}
