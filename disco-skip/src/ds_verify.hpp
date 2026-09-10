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
//     bool read(RemoteAddr a, NodeRecord &out);
//
// returning false if the node could not be read consistently. On the RDMA side
// that means a torn read that did not settle after retries; locally it means an
// out-of-range id.
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

  /// Checks that hold for any node in isolation.
  void checkNodeLocal(NodeRecord const &n, uint64_t id, uint32_t expect_level) {
    VecSlot const &s = n.current();

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
    if (s.size > 0 && s.e[s.size - 1].key >= s.next_k_min) {
      err(idStr(id) + ": last key " + std::to_string(s.e[s.size - 1].key) +
          " is at or past next_k_min " + std::to_string(s.next_k_min));
    }

    // Timestamps must not go backwards across the slot ring. The ring
    // alternates, so `previous` is exactly one version behind `current`, and a
    // snapshot read walking back for the version with ts <= T relies on that
    // ordering to know when to stop. An inversion would make it stop early and
    // read the wrong version.
    //
    // Trivially satisfied today, since nothing writes a timestamp yet -- it is
    // here so the insert path cannot introduce an inversion unnoticed.
    if (n.current().ts < n.previous().ts) {
      err(idStr(id) + ": current version's ts " + std::to_string(n.current().ts) +
          " is older than the previous version's " +
          std::to_string(n.previous().ts));
    }

    // old_ver is reserved and must be zero until the out-of-line version chain
    // is built (A10). A non-zero value here would mean something wrote a chain
    // that nothing knows how to read.
    if (s.old_ver != 0) {
      err(idStr(id) + ": old_ver is set to " + std::to_string(s.old_ver) +
          " but the out-of-line version chain is not implemented");
    }
  }

  /// Walks one level's next chain. Returns via rep_/down_/seen_orphan_.
  void walkIndexLevel(uint32_t level) {
    RemoteAddr cur = headAddr(level);
    NodeRecord n;
    Key prev_k_min = 0;
    bool first = true;
    size_t steps = 0;

    while (!cur.isNull()) {
      if (++steps > detail::kMaxChainSteps) {
        err("level " + std::to_string(level) +
            ": next chain exceeded the step bound, so it contains a cycle (I2)");
        return;
      }
      if (!r_.read(cur, n)) {
        err(idStr(cur.id) + ": could not be read consistently");
        return;
      }

      ++rep_.nodes_visited;
      ++rep_.nodes_per_level[level];
      rep_.entries += n.current().size;
      bool const orphan = n.handle.isOrphan();
      if (orphan) ++rep_.orphans;
      seen_orphan_[cur.id] = orphan;

      checkNodeLocal(n, cur.id, level);

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
      VecSlot const &s = n.current();
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
      RemoteAddr const next{s.next_id};
      if (next.isNull()) {
        if (s.next_k_min != kReservedKey) {
          err(idStr(cur.id) +
              ": is last in its chain but next_k_min is not the sentinel");
        }
      } else {
        NodeRecord succ;
        if (!r_.read(next, succ)) {
          err(idStr(next.id) + ": successor could not be read consistently");
          return;
        }
        if (succ.k_min != s.next_k_min) {
          err(idStr(cur.id) + ": next_k_min " + std::to_string(s.next_k_min) +
              " disagrees with " + idStr(next.id) + "'s k_min " +
              std::to_string(succ.k_min));
        }
      }
      cur = next;
    }
  }

  void checkDownPointers() {
    NodeRecord child;
    for (auto const &kv : down_) {
      RemoteAddr const a{kv.first};
      Expect const &e = kv.second;
      if (!r_.read(a, child)) {
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
    if (!r_.read(headAddr(0), dir)) return;  // already reported
    if (dir.current().size == 0) return;     // empty structure

    RemoteAddr cur{dir.current().e[0].val};
    NodeRecord n;
    Key prev_k_min = 0;
    bool first = true;
    size_t steps = 0;

    while (!cur.isNull()) {
      if (++steps > detail::kMaxChainSteps) {
        err("data level: next chain contains a cycle (I2)");
        return;
      }
      if (!r_.read(cur, n)) {
        err(idStr(cur.id) + ": data node could not be read consistently");
        return;
      }
      ++rep_.data_nodes;
      rep_.data_entries += n.current().size;
      checkNodeLocal(n, cur.id, kDataLevel);

      if (!first && n.k_min <= prev_k_min) {
        err(idStr(cur.id) + ": data-level k_min " + std::to_string(n.k_min) +
            " does not exceed its predecessor's " + std::to_string(prev_k_min) +
            " (I2)");
      }
      prev_k_min = n.k_min;
      first = false;
      cur = RemoteAddr{n.current().next_id};
    }
  }
};

/// Convenience wrapper.
template <class Reader>
VerifyReport verifyStructure(Reader &reader, uint32_t layers) {
  return StructureVerifier<Reader>(reader, layers).run();
}

}  // namespace ds
