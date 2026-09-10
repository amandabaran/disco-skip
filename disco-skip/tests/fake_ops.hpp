#pragma once

// A local stand-in for the memory servers, plus the fixtures that stage the
// concurrent states the helping paths exist for.
//
// Shared by descend_test and get_test: both need an arena that supports the
// same reads and CAS operations the RDMA path does, so that the Descender and
// Getter under test are the same code that runs on the cluster.

#include <cstdint>
#include <vector>

#include "ds_bootstrap.hpp"
#include "ds_descend.hpp"
#include "ds_node.hpp"
#include "layout.hpp"

/// A local stand-in for the memory servers, supporting reads and the four CAS
/// operations the helping path needs.
class FakeOps {
 public:
  FakeOps(size_t nodes, size_t vecs) : nodes_(nodes), vecs_(vecs) {
    for (auto &n : nodes_) ds::initNode(n, 0, 0, ds::kNullVec);
    for (auto &v : vecs_) ds::initVec(v, false, /*ts=*/1);
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

  uint64_t now() { return clock_ += 10; }

  // ── Test-side manipulation ───────────────────────────────────────────────

  ds::NodeRecord &node(ds::RemoteAddr a) { return nodes_[a.id]; }
  ds::NodeRecord &node(uint64_t id) { return nodes_[id]; }
  ds::VecRecord &vecAt(ds::VecOffset off) { return vecs_[off]; }
  ds::VecRecord &vecOf(ds::RemoteAddr a) { return vecs_[nodes_[a.id].handle.offset()]; }

  uint64_t nodeReads() const { return node_reads_; }
  uint64_t vecReads() const { return vec_reads_; }
  uint64_t reads() const { return node_reads_ + vec_reads_; }
  uint64_t casTsCalls() const { return cas_ts_; }
  uint64_t casTailCalls() const { return cas_tail_; }
  uint64_t clockNow() const { return clock_; }

 private:
  std::vector<ds::NodeRecord> nodes_;
  std::vector<ds::VecRecord> vecs_;
  uint64_t clock_ = 1000;
  uint64_t node_reads_ = 0, vec_reads_ = 0, cas_ts_ = 0, cas_next_id_ = 0, cas_next_k_min_ = 0,
           cas_tail_ = 0;
};

/// Insert a key into a data node sequentially, as a settled write would leave
/// it. Not the real F1 path -- just enough structure for a descent to find.
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
inline FakeOps buildInitialArena(uint32_t layers) {
  FakeOps ops(ds::kFirstDynamicId + 512, ds::kFirstDynamicVec + 512);
  ds::InitialNode built[ds::kMaxLayers + 1];
  uint32_t const n = ds::buildInitialStructure(layers, /*ts=*/1000, built);
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
