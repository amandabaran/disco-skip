#pragma once

// Structural verification of the remote skip vector: invariants.md §1 (I1-I4)
// plus the layout invariants this side adds.
//
// Templated over a Reader so the same checker runs in two places: against real
// RDMA reads on the cluster (--selftest), and against a fake in-memory arena in
// disco-skip/tests. The walk logic is the part most likely to be subtly wrong,
// and it would otherwise only ever be exercised on the cluster.
//
// A Reader must provide:
//
//     bool read(RemoteAddr a, NodeRecord &node, VecRecord &vec);
//     bool readVec(VecOffset off, VecRecord &vec);
//
// The first fetches a node header and the vector its handle names -- two
// regions, since vectors live out of line. The second fetches a vector by
// offset, which is what walking the old_ver chain needs. Both return false if
// the read could not be completed: on the RDMA side a header whose bookends
// never settle, locally an out-of-range index.
//
// SEQUENTIAL-ONLY. Every check here assumes the structure is quiescent -- it
// reads nodes one at a time and compares them against each other, so a
// concurrent writer would produce spurious failures. Run it before or after a
// workload, never during.

#include <cstdint>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "ds_defs.hpp"
#include "ds_node.hpp"

namespace ds {

struct VerifyReport {
  size_t nodes_visited = 0;
  size_t orphans = 0;
  size_t entries = 0;
  std::vector<size_t> nodes_per_level;  // index 0 = directory
  size_t data_nodes = 0;
  size_t data_entries = 0;
  size_t old_versions = 0;
  std::vector<std::string> errors;

  [[nodiscard]] bool ok() const { return errors.empty(); }
};

namespace detail {

/// Cap on reported errors. A structural break tends to produce one failure per
/// node, and a thousand identical lines is less useful than the first few.
inline constexpr size_t kMaxErrors = 32;

/// Cap on chain length, which is how cycle detection is done: I2 forbids cycles,
/// and a bound is cheaper and more robust than a visited set when the chain is
/// the thing under suspicion.
inline constexpr size_t kMaxChainSteps = 1u << 24;

/// Cap on old_ver chain depth, for the same reason.
inline constexpr size_t kMaxChainDepth = 1u << 20;

}  // namespace detail

template <class Reader>
class StructureVerifier {
 public:
  StructureVerifier(Reader &reader, uint32_t layers)
      : r_(reader), layers_(layers) {
    rep_.nodes_per_level.assign(layers, 0);
  }

  VerifyReport run() {
    // Pass 1: walk each index level's next chain top-down, validating what can
    // be checked from one node plus its successor, and recording every down
    // pointer for pass 2.
    for (uint32_t L = layers_; L-- > 0;) {
      walkIndexLevel(L);
    }

    // Pass 2: every down pointer must land on a node whose k_min equals the
    // entry's key and which lives one level below. This is the property the
    // whole index rests on -- the cache's fidelity property is its local
    // mirror -- so it is checked for every entry, not sampled.
    checkDownPointers();

    // Pass 3: reachability. I4 says an orphan is reachable only by walking
    // next, so an orphan must be nobody's down-pointer target; and a
    // non-orphan must be somebody's, or the index has lost a subtree (which is
    // I3's concern -- a node with entries that nothing routes to).
    checkReachability();

    // Pass 4: the data level, reached through the directory's entries.
    walkDataLevel();

    return rep_;
  }

 private:
  Reader &r_;
  uint32_t layers_;
  VerifyReport rep_;

  /// child id -> (expected k_min, expected level, referencing node)
  struct Expect {
    Key key;
    uint32_t level;
    uint64_t from;
  };
  std::unordered_map<uint64_t, Expect> down_;
  /// Every node seen on an index chain, and whether it was flagged orphan.
  std::unordered_map<uint64_t, bool> seen_orphan_;

  void err(std::string msg) {
    if (rep_.errors.size() < detail::kMaxErrors) {
      rep_.errors.push_back(std::move(msg));
    } else if (rep_.errors.size() == detail::kMaxErrors) {
      rep_.errors.push_back("... further errors suppressed");
    }
  }

  static std::string idStr(uint64_t id) { return "node " + std::to_string(id); }

  /// Checks that hold for one node and its current vector in isolation.
  void checkNodeLocal(NodeRecord const &n, VecRecord const &s, uint64_t id,
                      uint32_t expect_level) {
    // A quiescent structure has no operation in flight, so every node must be
    // settled. An unstable node here means a writer died mid-propagation and
    // nobody helped it finish -- which, since helping is what makes reads
    // non-blocking, would mean readers are livelocked on it.
    if (!n.isStable()) {
      err(idStr(id) + ": handle struct_ver " +
          std::to_string(n.handle.structVer()) +
          " != tail_struct_ver " + std::to_string(n.tail_struct_ver) +
          ", so a split is still propagating in a quiescent structure");
    }

    // The vector must be the version the handle names.
    if (s.struct_ver != n.handle.structVer() ||
        s.content_ver != n.handle.contentVer()) {
      err(idStr(id) + ": vector at offset " +
          std::to_string(n.handle.offset()) + " carries version (" +
          std::to_string(s.struct_ver) + "," + std::to_string(s.content_ver) +
          ") but the handle names (" + std::to_string(n.handle.structVer()) +
          "," + std::to_string(n.handle.contentVer()) + ")");
    }

    // Likewise no version should still be pending.
    if (s.isPending()) {
      err(idStr(id) +
          ": current version's ts is null (pending) in a quiescent structure");
    }

    // A split descriptor that outlives its propagation must agree with the
    // header, or readers preferring the vector's copy and readers using the
    // header's would disagree about where this node's range ends.
    if (s.hasSplitDescriptor()) {
      if (s.next_id != n.next_id) {
        err(idStr(id) + ": vector's split next_id " +
            std::to_string(s.next_id) + " disagrees with the header's " +
            std::to_string(n.next_id));
      }
      if (s.k_min_next != n.next_k_min) {
        err(idStr(id) + ": vector's split k_min_next " +
            std::to_string(s.k_min_next) + " disagrees with the header's " +
            std::to_string(n.next_k_min));
      }
    }

    checkOldVerChain(n, s, id);

    if (n.level != expect_level) {
      err(idStr(id) + ": level is " + std::to_string(n.level) + ", expected " +
          std::to_string(expect_level));
    }
    if (s.size > kNodeCapacity) {
      err(idStr(id) + ": size " + std::to_string(s.size) + " exceeds capacity");
      return;  // anything further would read out of bounds
    }

    // Entries must be sorted and distinct: findLte is a binary search, so an
    // unsorted vector does not merely look odd, it silently returns wrong
    // answers.
    for (uint32_t i = 1; i < s.size; ++i) {
      if (s.e[i].key <= s.e[i - 1].key) {
        err(idStr(id) + ": entries not strictly ascending at index " +
            std::to_string(i));
        break;
      }
    }

    // I1, in its general form: a node's k_min must not exceed the smallest key
    // it holds, or the node claims a range that starts after its own contents.
    if (s.size > 0 && n.k_min > s.e[0].key) {
      err(idStr(id) + ": k_min " + std::to_string(n.k_min) +
          " exceeds first key " + std::to_string(s.e[0].key));
    }

    // Every key must fall inside the range the node advertises, or a descent
    // that trusts next_k_min would skip it.
    Key const end = rangeEnd(n, s);
    if (s.size > 0 && s.e[s.size - 1].key >= end) {
      err(idStr(id) + ": last key " + std::to_string(s.e[s.size - 1].key) +
          " is at or past the end of its range " + std::to_string(end));
    }

  }

  /// Walks the old_ver chain, checking it is finite and ordered.
  ///
  /// A snapshot read walks back for the version with ts <= T and stops at the
  /// first one that qualifies, so the chain must be strictly decreasing in ts.
  /// An inversion would make it stop early and serve the wrong version, which
  /// is the failure mode a range query cannot detect for itself.
  void checkOldVerChain(NodeRecord const &n, VecRecord const &current,
                        uint64_t id) {
    VecOffset off = static_cast<VecOffset>(current.old_ver);
    uint64_t newer_ts = current.ts;
    uint32_t newer_content = current.content_ver;
    VecRecord older;
    size_t depth = 0;

    while (off != kNullVec) {
      if (++depth > detail::kMaxChainDepth) {
        err(idStr(id) + ": old_ver chain exceeded the depth bound, so it "
                        "contains a cycle");
        return;
      }
      if (!r_.readVec(off, older)) {
        err(idStr(id) + ": old_ver chain reaches unreadable offset " +
            std::to_string(off));
        return;
      }
      ++rep_.old_versions;

      if (older.isPending()) {
        err(idStr(id) + ": a superseded version at offset " +
            std::to_string(off) + " is still pending, so no snapshot can ever "
                                  "be resolved against it");
      } else if (older.ts >= newer_ts) {
        err(idStr(id) + ": old_ver chain is not decreasing in ts -- offset " +
            std::to_string(off) + " has ts " + std::to_string(older.ts) +
            " at or above its successor's " + std::to_string(newer_ts));
      }
      if (older.content_ver >= newer_content && older.struct_ver == n.handle.structVer()) {
        err(idStr(id) + ": old_ver chain is not decreasing in content_ver at "
                        "offset " + std::to_string(off));
      }

      newer_ts = older.ts;
      newer_content = older.content_ver;
      off = static_cast<VecOffset>(older.old_ver);
    }
  }

  /// Walks one level's next chain. Returns via rep_/down_/seen_orphan_.
  void walkIndexLevel(uint32_t level) {
    RemoteAddr cur = headAddr(level);
    NodeRecord n;
    VecRecord v;
    Key prev_k_min = 0;
    bool first = true;
    size_t steps = 0;

    while (!cur.isNull()) {
      if (++steps > detail::kMaxChainSteps) {
        err("level " + std::to_string(level) +
            ": next chain exceeded the step bound, so it contains a cycle (I2)");
        return;
      }
      if (!r_.read(cur, n, v)) {
        err(idStr(cur.id) + ": could not be read consistently");
        return;
      }

      ++rep_.nodes_visited;
      ++rep_.nodes_per_level[level];
      rep_.entries += v.size;
      bool const orphan = v.is_orphan != 0;
      if (orphan) ++rep_.orphans;
      seen_orphan_[cur.id] = orphan;

      checkNodeLocal(n, v, cur.id, level);

      // I2: the chain must be sorted strictly ascending by k_min. Equal k_mins
      // would make "the node covering k" ambiguous.
      if (!first && n.k_min <= prev_k_min) {
        err(idStr(cur.id) + ": k_min " + std::to_string(n.k_min) +
            " does not exceed its predecessor's " + std::to_string(prev_k_min) +
            " (I2)");
      }
      prev_k_min = n.k_min;
      first = false;

      // Record this node's down pointers for pass 2.
      VecRecord const &s = v;
      uint32_t const capped = s.size <= kNodeCapacity ? s.size : 0;
      for (uint32_t i = 0; i < capped; ++i) {
        uint64_t const child = s.e[i].val;
        if (child == kNullId) {
          err(idStr(cur.id) + ": entry " + std::to_string(i) + " (key " +
              std::to_string(s.e[i].key) + ") points at the null node");
          continue;
        }
        uint32_t const child_level = (level == 0) ? kDataLevel : level - 1;
        auto const it = down_.find(child);
        if (it != down_.end()) {
          err(idStr(child) + ": referenced by both " + idStr(it->second.from) +
              " and " + idStr(cur.id) + ", so the index is not a tree");
        } else {
          down_.emplace(child, Expect{s.e[i].key, child_level, cur.id});
        }
      }

      // next_k_min must agree with the successor's k_min, or C4's single-read
      // range check gives a wrong answer. This is the invariant that makes
      // co-locating it worthwhile, so it is worth checking directly.
      RemoteAddr const next = nextNode(n, v);
      Key const end = rangeEnd(n, v);
      if (next.isNull()) {
        if (end != kReservedKey) {
          err(idStr(cur.id) +
              ": is last in its chain but its range end is not the sentinel");
        }
      } else {
        NodeRecord succ;
        VecRecord succ_v;
        if (!r_.read(next, succ, succ_v)) {
          err(idStr(next.id) + ": successor could not be read consistently");
          return;
        }
        if (succ.k_min != end) {
          err(idStr(cur.id) + ": range end " + std::to_string(end) +
              " disagrees with " + idStr(next.id) + "'s k_min " +
              std::to_string(succ.k_min));
        }
      }
      cur = next;
    }
  }

  void checkDownPointers() {
    NodeRecord child;
    VecRecord child_v;
    for (auto const &kv : down_) {
      RemoteAddr const a{kv.first};
      Expect const &e = kv.second;
      if (!r_.read(a, child, child_v)) {
        err(idStr(a.id) + ": down-pointer target could not be read");
        continue;
      }
      if (child.k_min != e.key) {
        err(idStr(a.id) + ": k_min " + std::to_string(child.k_min) +
            " does not equal the key " + std::to_string(e.key) +
            " that " + idStr(e.from) + " routes to it under");
      }
      if (child.level != e.level) {
        err(idStr(a.id) + ": level " + std::to_string(child.level) +
            " is not one below its parent " + idStr(e.from));
      }
    }
  }

  void checkReachability() {
    uint64_t const root = headAddr(layers_ - 1).id;
    for (auto const &kv : seen_orphan_) {
      uint64_t const id = kv.first;
      bool const orphan = kv.second;
      bool const referenced = down_.find(id) != down_.end();

      if (orphan && referenced) {
        err(idStr(id) +
            ": is flagged orphan but something routes down to it (I4)");
      }
      if (!orphan && !referenced && id != root) {
        err(idStr(id) +
            ": is not an orphan yet nothing routes down to it, so its range is "
            "reachable only by walking next (I3)");
      }
    }
  }

  /// The data level is not an index level: it has no head at a reserved id, so
  /// it is reached through the directory's entries and then walked by next.
  void walkDataLevel() {
    NodeRecord dir;
    VecRecord dir_v;
    if (!r_.read(headAddr(0), dir, dir_v)) return;  // already reported
    if (dir_v.size == 0) return;                    // empty structure

    RemoteAddr cur{dir_v.e[0].val};
    NodeRecord n;
    VecRecord v;
    Key prev_k_min = 0;
    bool first = true;
    size_t steps = 0;

    while (!cur.isNull()) {
      if (++steps > detail::kMaxChainSteps) {
        err("data level: next chain contains a cycle (I2)");
        return;
      }
      if (!r_.read(cur, n, v)) {
        err(idStr(cur.id) + ": data node could not be read consistently");
        return;
      }
      ++rep_.data_nodes;
      rep_.data_entries += v.size;
      checkNodeLocal(n, v, cur.id, kDataLevel);

      if (!first && n.k_min <= prev_k_min) {
        err(idStr(cur.id) + ": data-level k_min " + std::to_string(n.k_min) +
            " does not exceed its predecessor's " + std::to_string(prev_k_min) +
            " (I2)");
      }
      prev_k_min = n.k_min;
      first = false;
      cur = nextNode(n, v);
    }
  }
};

/// Convenience wrapper.
template <class Reader>
VerifyReport verifyStructure(Reader &reader, uint32_t layers) {
  return StructureVerifier<Reader>(reader, layers).run();
}

}  // namespace ds
