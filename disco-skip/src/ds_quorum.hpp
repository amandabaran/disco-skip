#pragma once

// CAS-ABD over three replicas: the quorum read (L2/L3/L4), the 2-of-3 commit
// (L1) and the async writeback (L5) of invariants.md §4.
//
// This sits BEHIND the Ops surface, which is the point. `QuorumOps` provides
// exactly the surface ds_traverse.hpp and ds_insert.hpp already expect, so the
// traversal, the Writer and the Putter are unchanged and unaware: replication is
// not a parameter of the algorithms, it is a property of the storage under them.
//
// It is templated over a ReplicaSet of per-replica primitives so the whole thing
// runs against a fake in disco-skip/tests. That matters more here than anywhere
// else in the project: every interesting state involves one replica disagreeing
// with two others, which is trivial to construct locally and essentially
// impossible to provoke on a healthy cluster.
//
// ReplicaSet must provide:
//
//     size_t replicas() const;
//     void readNodeAll(RemoteAddr a, NodeRecord *out, bool *ok,
//                      VecOffset speculate, VecRecord *spec, bool *spec_ok);
//     bool readNodeFrom(size_t r, RemoteAddr a, NodeRecord &node);
//     bool readVecFrom(size_t r, VecOffset off, VecRecord &vec);
//     bool casHandleOn(size_t r, RemoteAddr a, uint64_t exp, uint64_t des);
//     void submitAll(Batch const &b, bool *submitted, bool *committed);
//     uint64_t now();
//     VecOffset allocVec();
//     RemoteAddr allocNode();
//
// `readNodeAll` and `submitAll` are where the round trips are won or lost. It must post every
// replica's chain BEFORE draining any of them, so that N replicas cost one
// round trip of latency rather than N -- which is what chimera's put_future
// does across servers and what a per-replica loop of blocking calls does not.
// It reports per-replica outcomes; deciding what a majority of them means is
// this file's job.
//
// ── Two decisions worth reading before changing anything here ────────────────
//
// 1. THE COMMIT ATTEMPTS EVERY REPLICA, not a fixed majority subset.
//
//    `DsState::quorum_indices` picks a fixed majority per client, rotated by
//    client index, and the legacy register path uses it. Doing that here
//    deadlocks. With replicas R0/R1/R2, client A holding {0,1} and client B
//    holding {1,2}:
//
//      - A commits: CAS R0 and R1 both succeed, 2 of 3. R2 is now lagged.
//      - B writes: its majority read of {1,2} sees R1's new handle as the max.
//        It CASes that expected value on {1,2}: R1 succeeds, R2 fails because
//        it still holds the old handle. One success, short of majority.
//      - B retries forever. Nothing in that loop ever repairs R2.
//
//    So L1's "2/3 CAS success" has to mean *attempt three, require two*. That
//    also demotes the writeback of L5 to what §4.1 claims it is -- an
//    accelerant -- rather than something correctness secretly depends on.
//
// 2. THE READ TAKES THE HIGHEST TAG WITH MAJORITY SUPPORT, which is slightly
//    stronger than L2 as written.
//
//    L2 says a read linearizes at a quorum read taking the max tag. That is
//    right whenever every distinct handle value is either committed or absent.
//    A *partially* applied commit breaks that assumption: two writers bumping
//    from the same predecessor produce two different handles with the SAME tag,
//    since both bump content_ver once. Tags then tie and L3's order cannot
//    separate them, and picking the uncommitted one would return contents that
//    no majority ever held.
//
//    Reading every replica makes the ambiguity decidable, so this takes the
//    highest tag that at least a majority actually holds. A state with no
//    majority-supported value is transient -- the writers involved are still
//    retrying -- so it is reported as a failed read for the caller to retry
//    rather than resolved by guessing.
//
//    Flagged rather than silently adopted: it is a deliberate strengthening of
//    L2, and if invariants.md is to stay authoritative it should say so.
//
// ── The offset hint, and why it belongs here ─────────────────────────────────
//
// A node read is a header and then the vector its handle names, and those are
// ordinarily serialised because the offset is not known until the header lands.
// The hint breaks that: with a guess, the vector read is issued *alongside* the
// header reads and validated afterwards.
//
// The policy lives here rather than in the ReplicaSet because validating a
// guess means knowing which handle won the quorum, which is this file's job.
// The ReplicaSet only offers to carry one extra read.
//
// Validation is stricter than "the offset matched", to keep L4 honest. The
// speculation is accepted only when the replica it was read from voted with the
// winning handle -- which, since a handle carries its offset, collapses to:
// replica 0 voted with the winner and the guess equalled that offset. A vector
// is write-once and written to every replica before the handle CAS publishes
// it, so a correct guess would give identical bytes from any replica that has
// it; the strict check costs nothing and does not rely on that.
//
// A wrong guess now costs a wasted 320-byte read and NO latency, because the
// speculation rides in the same doorbell as the headers. That is the trade the
// toggle exists to measure: rNIC and PCIe bandwidth against round trips.

#include <cstddef>
#include <cstdint>

#include "ds_batch.hpp"
#include "ds_defs.hpp"
#include "ds_ts.hpp"
#include "ds_node.hpp"
#include "layout.hpp"  // VecOffsetHint

namespace ds {

struct QuorumStats {
  uint64_t node_reads = 0;      ///< logical reads, not per-replica RDMAs
  uint64_t vec_reads = 0;
  uint64_t replica_reads = 0;   ///< the per-replica RDMAs those cost
  uint64_t read_retries = 0;    ///< no majority-supported value yet
  uint64_t read_failures = 0;
  uint64_t tag_ties = 0;        ///< distinct handles sharing the highest tag
  uint64_t stale_votes = 0;     ///< replicas that voted below the winner
  uint64_t commits = 0;         ///< handle CASes that reached majority
  uint64_t commits_lost = 0;    ///< handle CASes that did not
  uint64_t partial_commits = 0; ///< ... of those, with at least one success
  uint64_t writebacks = 0;      ///< lagged replicas repaired
  /// L2 read repair: a read that could not find a majority-supported handle
  /// after its poll budget, so it adopted the max raw handle and CAS'd it onto
  /// the laggards -- ABD's read phase completing a partial write. See
  /// RdmaAsyncOps::postRepair. `landed` counts those that reached a majority;
  /// the difference is repairs that raced and will be re-polled.
  uint64_t repairs = 0;
  uint64_t repairs_landed = 0;
  uint64_t batches = 0;         ///< chained submissions, i.e. round trips
  uint64_t write_shortfalls = 0;///< a batch landed on fewer than a majority
  uint64_t vec_writes = 0;      ///< logical vector writes, summed over batches
  uint64_t node_writes = 0;
  uint64_t cas_issued = 0;      ///< CASes carried by batches, all replicas
  uint64_t speculated = 0;      ///< reads that carried a speculative vector
  uint64_t spec_hits = 0;       ///< ... whose guess was right, saving a round trip
  uint64_t spec_misses = 0;     ///< ... whose guess was wrong, costing 320 bytes
  uint64_t vec_reads_served = 0;///< vector reads answered from a speculation
};

namespace detail {

/// How many times a read re-polls before reporting failure. A state with no
/// majority-supported handle means writers are mid-commit, so this bounds our
/// own spinning rather than guarding a livelock.
inline constexpr int kMaxQuorumReadAttempts = 8;

}  // namespace detail

/// The Ops surface, over a replica set, with CAS-ABD underneath.
template <class ReplicaSet>
class QuorumOps {
 public:
  /// @param hint  optional; when present, a node read speculates the vector
  ///              read alongside the headers instead of serialising after them
  QuorumOps(ReplicaSet &set, QuorumStats &stats, VecOffsetHint *hint = nullptr)
      : set_(set), stats_(stats), hint_(hint) {}

  /// Replicas that must agree for a value to count as committed.
  [[nodiscard]] size_t majority() const { return set_.replicas() / 2 + 1; }

  // ── Reads: L2, L3, L4 ─────────────────────────────────────────────────────

  /// Quorum header read.
  ///
  /// Reads every replica, groups by exact handle value, and takes the highest
  /// tag that a majority holds. Records which replica supplied it, so the
  /// vector read can honour L4.
  bool readNode(RemoteAddr a, NodeRecord &node) {
    ++stats_.node_reads;
    size_t const n = set_.replicas();
    spec_valid_ = false;  // any previous speculation is stale now

    for (int attempt = 0; attempt < detail::kMaxQuorumReadAttempts; ++attempt) {
      NodeRecord seen[kMaxReplicas];
      bool ok[kMaxReplicas] = {};

      // One fan-out, optionally carrying the speculative vector read. Every
      // request is posted before any is drained, so this is one round trip
      // whatever the replica count -- and the speculation is free in latency.
      VecOffset const guess = hint_ != nullptr ? hint_->guess(a) : kNullVec;
      VecRecord spec;
      bool spec_ok = false;
      set_.readNodeAll(a, seen, ok, guess, &spec, &spec_ok);
      stats_.replica_reads += n;
      if (guess != kNullVec) {
        ++stats_.speculated;
        ++stats_.replica_reads;  // the speculative read is a real read
      }

      size_t got = 0;
      for (size_t r = 0; r < n; ++r) {
        if (ok[r]) ++got;
      }
      if (got < majority()) {
        ++stats_.read_failures;
        return false;
      }

      // Group by exact handle. Distinct handles can share a tag when a commit
      // was partially applied, which is why votes are counted per value rather
      // than per tag.
      size_t best = n;  // index of the winning replica
      uint64_t best_tag = 0;
      size_t best_votes = 0;
      bool tie = false;
      for (size_t r = 0; r < n; ++r) {
        if (!ok[r]) continue;
        size_t votes = 0;
        for (size_t q = 0; q < n; ++q) {
          if (ok[q] && seen[q].handle == seen[r].handle) ++votes;
        }
        if (votes < majority()) continue;
        uint64_t const tag = seen[r].handle.tag();
        if (best == n || tag > best_tag) {
          best = r;
          best_tag = tag;
          best_votes = votes;
        } else if (tag == best_tag && seen[r].handle != seen[best].handle) {
          tie = true;
        }
      }
      if (tie) ++stats_.tag_ties;

      if (best == n) {
        // Every value is short of majority support, so a commit is in flight.
        // Re-poll rather than pick: returning a value no majority holds would
        // publish contents that were never committed.
        //
        // BUT RE-POLLING IS NOT ALWAYS ENOUGH. Under sustained write contention
        // on one key the replicas are essentially never in agreement -- each
        // write CASes them in a chain, so a reader sampling all three lands
        // mid-flight and the next write is already arriving. On the cluster this
        // failed 76,311 reads on workload D at 8 clients, every one of them
        // here. So on the last attempt, fall back to L2 and REPAIR: adopt a
        // handle deterministically and CAS it onto the laggards, which is ABD's
        // read phase completing a partial write.
        //
        // Kept in step with TraversalFuture's AwaitRepair deliberately. The two
        // read paths diverging is what produced today's other bug, where the
        // async put dropped a retry bound its blocking twin had.
        ++stats_.read_retries;
        if (attempt + 1 == detail::kMaxQuorumReadAttempts &&
            repairRead(a, seen, ok, n)) {
          continue;  // repaired; one more poll will now find a majority
        }
        continue;
      }

      for (size_t r = 0; r < n; ++r) {
        if (ok[r] && seen[r].handle != seen[best].handle) ++stats_.stale_votes;
      }

      node = seen[best];
      last_read_addr_ = a;
      last_max_replica_ = best;
      last_max_valid_ = true;
      (void)best_votes;

      // Was the guess right? Accepted only if replica 0 voted with the winning
      // handle, which is the strict reading of L4 -- see the header note.
      VecOffset const truth = seen[best].handle.offset();
      if (guess != kNullVec) {
        if (spec_ok && ok[0] && seen[0].handle == seen[best].handle &&
            guess == truth) {
          spec_valid_ = true;
          spec_off_ = guess;
          spec_vec_ = spec;
          ++stats_.spec_hits;
        } else {
          ++stats_.spec_misses;
        }
      }
      if (hint_ != nullptr) hint_->record(a, truth, guess);
      return true;
    }
    ++stats_.read_failures;
    return false;
  }

  /// Vector read, from a replica that voted with the winning handle (L4).
  ///
  /// The replica is remembered from the preceding readNode. That coupling is
  /// safe because every caller pairs the two -- the traversal reads a header and
  /// then, only if it needs the entries, that node's vector -- but it is a
  /// coupling, so it falls back to replica 0 rather than misbehaving if the
  /// pairing is ever broken.
  ///
  /// Worth knowing why the fallback is harmless in this failure model: a vector
  /// is written to every replica at the same offset before the handle CAS
  /// publishes it, and it is write-once, so all replicas that have offset `off`
  /// have identical bytes there. L4 matters for a replica that missed the write,
  /// which under fail-stop with no partitions (§8) means a replica that is down
  /// and whose read would fail anyway.
  bool readVec(VecOffset off, VecRecord &vec) {
    ++stats_.vec_reads;

    // Already in hand: the preceding readNode speculated this offset and the
    // winning handle confirmed it, so there is no round trip to make. Consumed
    // rather than cached, because the next readNode may publish a new version
    // and a stale speculation is exactly the thing that would return old bytes.
    if (spec_valid_ && off == spec_off_) {
      vec = spec_vec_;
      spec_valid_ = false;
      ++stats_.vec_reads_served;
      return true;
    }

    size_t const start = last_max_valid_ ? last_max_replica_ : 0;
    size_t const n = set_.replicas();
    for (size_t i = 0; i < n; ++i) {
      size_t const r = (start + i) % n;
      ++stats_.replica_reads;
      if (set_.readVecFrom(r, off, vec)) return true;
    }
    ++stats_.read_failures;
    return false;
  }

  /// Both, for the verifier's Reader concept.
  bool read(RemoteAddr a, NodeRecord &node, VecRecord &vec) {
    if (!readNode(a, node)) return false;
    return readVec(node.handle.offset(), vec);
  }

  // ── Batch submission: the fan-out ────────────────────────────────────────

  /// Issue a chained batch on every replica, then decide what happened.
  ///
  /// The publishing CAS follows L1: committed iff a majority of replicas took
  /// it. Everything else in the batch is a helping step -- idempotent, and with
  /// no majority to reach, since the field simply has to arrive everywhere for
  /// the replicas to converge.
  ///
  /// A batch with no publishing CAS is reported committed, so a caller that
  /// only wanted the helping steps does not have to special-case it.
  BatchResult submit(Batch const &b) {
    BatchResult out;
    if (!b.wellFormed()) return out;

    size_t const n = set_.replicas();
    bool submitted[kMaxReplicas] = {};
    bool committed[kMaxReplicas] = {};
    set_.submitAll(b, submitted, committed);

    size_t landed = 0, took = 0;
    for (size_t r = 0; r < n; ++r) {
      if (submitted[r]) ++landed;
      if (submitted[r] && committed[r]) ++took;
    }

    ++stats_.batches;
    stats_.vec_writes += b.vecWrites();
    stats_.node_writes += b.nodeWrites();
    stats_.cas_issued +=
        (b.size() - b.vecWrites() - b.nodeWrites()) * set_.replicas();
    if (landed < majority()) {
      ++stats_.write_shortfalls;
      return out;  // submitted == false: nothing may be assumed to have landed
    }
    out.submitted = true;

    if (!b.hasCommit()) {
      out.committed = true;
      out.ts = set_.lastTs();
      return out;
    }

    if (took < majority()) {
      ++stats_.commits_lost;
      if (took > 0) ++stats_.partial_commits;
      // Deliberately NOT rolled back. There is nothing safer to roll back to:
      // the replicas that took it hold a value with the same tag as any rival
      // winner's, and a later commit from the majority supersedes it. A rollback
      // would need its own CAS that could itself lose.
      return out;
    }

    ++stats_.commits;
    out.committed = true;
    out.ts = set_.lastTs();
    writeBack(b, submitted, committed);
    last_max_valid_ = false;  // the winning replica is no longer meaningful
    return out;
  }

  // ── Reporting, so the same selftest can run over this surface ────────────
  //
  // Deliberately the same names RdmaNodeReader and RdmaOps expose, because
  // ds_selftest.hpp is templated over both and must not care which it has. The
  // unstable-retry counter is always zero here: a read caught mid-write is
  // resolved by the majority rule rather than by re-reading one replica.

  [[nodiscard]] uint64_t nodeReads() const { return stats_.node_reads; }
  [[nodiscard]] uint64_t vecReads() const { return stats_.vec_reads; }
  [[nodiscard]] uint64_t reads() const {
    return stats_.node_reads + stats_.vec_reads;
  }
  /// Bytes off the wire, counted per replica -- the honest number under
  /// replication, since a quorum read really does move N headers.
  [[nodiscard]] uint64_t bytesRead() const {
    return stats_.node_reads * set_.replicas() * kNodeRecordBytes +
           stats_.vec_reads * kVecRecordBytes;
  }
  [[nodiscard]] uint64_t unstableRetries() const { return 0; }
  [[nodiscard]] uint64_t staleRetries() const { return stats_.read_retries; }
  [[nodiscard]] uint64_t casCount() const { return stats_.cas_issued; }
  [[nodiscard]] uint64_t nodeWrites() const { return stats_.node_writes; }
  [[nodiscard]] uint64_t vecWrites() const { return stats_.vec_writes; }
  [[nodiscard]] uint64_t bytesWritten() const {
    return (stats_.node_writes * kNodeRecordBytes +
            stats_.vec_writes * kVecRecordBytes) * set_.replicas();
  }
  /// Round trips spent writing. Independent of the replica count, which is the
  /// property the chaining exists to create.
  [[nodiscard]] uint64_t batches() const { return stats_.batches; }

  uint64_t now() { return set_.now(); }
  [[nodiscard]] TsMode tsMode() const { return set_.tsMode(); }
  VecOffset allocVec() { return set_.allocVec(); }
  RemoteAddr allocNode() { return set_.allocNode(); }

  /// L5's async writeback, for the replicas whose publishing CAS did not take.
  ///
  /// Not required for correctness -- any later majority read intersects the
  /// committed set -- but leaving a replica behind makes every subsequent
  /// commit depend on the other two, turning one slow replica into a single
  /// point of failure.
  ///
  /// Repaired by CAS rather than by write, so a replica that has meanwhile
  /// moved *ahead* of this commit is left alone rather than clobbered.
  /// L2 read repair: adopt the max RAW handle and CAS it onto the laggards.
  ///
  /// Max *raw*, not max tag. tag() is (struct_ver, content_ver) and is not
  /// unique per write -- a writer that loses its CAS re-stages the same logical
  /// update at the same versions with a different offset, so two distinct
  /// handles can share a tag (the tag_ties case counted above). The offset is
  /// allocated from a per-client stripe and occupies the low 32 bits, so the
  /// raw handle is tag order refined by offset: a total order every reader
  /// computes identically, which is what stops two readers adopting different
  /// handles and fighting.
  ///
  /// Adopting a handle no majority holds is safe because the vector it names
  /// was WRITTEN to every replica before the CAS -- the write is unconditional,
  /// only the publish is contended -- so its contents exist and are
  /// self-consistent. Its writer may consider that attempt abandoned;
  /// completing it is still a valid linearization, which is exactly what ABD
  /// does with a partial write.
  ///
  /// @return true if the chosen handle now sits at a majority
  bool repairRead(RemoteAddr a, NodeRecord const *seen, bool const *ok,
                  size_t n) {
    uint64_t desired = 0;
    for (size_t r = 0; r < n; ++r) {
      if (ok[r] && seen[r].handle.raw > desired) desired = seen[r].handle.raw;
    }
    if (desired == 0) return false;

    size_t holders = 0;
    for (size_t r = 0; r < n; ++r) {
      if (!ok[r]) continue;
      if (seen[r].handle.raw == desired) { ++holders; continue; }
      // Never move a replica backwards: one that raced ahead of our choice has
      // a newer commit and clobbering it would undo that.
      if (seen[r].handle.raw > desired) continue;
      if (set_.casHandleOn(r, a, seen[r].handle.raw, desired)) ++holders;
    }
    ++stats_.repairs;
    if (holders < majority()) return false;
    ++stats_.repairs_landed;
    return true;
  }

  void writeBack(Batch const &b, bool const *submitted, bool const *committed) {
    RemoteAddr addr{};
    uint64_t desired = 0;
    for (size_t i = 0; i < b.size(); ++i) {
      if (b[i].kind == BatchKind::CasHandle) {
        addr = b[i].addr;
        desired = b[i].desired;
        break;
      }
    }
    if (addr.isNull()) return;

    for (size_t r = 0; r < set_.replicas(); ++r) {
      if (!submitted[r] || committed[r]) continue;
      NodeRecord cur;
      ++stats_.replica_reads;
      if (!set_.readNodeFrom(r, addr, cur)) continue;
      if (cur.handle.raw == desired) continue;                 // caught up
      if (cur.handle.tag() > Handle{desired}.tag()) continue;  // newer: leave it
      if (set_.casHandleOn(r, addr, cur.handle.raw, desired)) ++stats_.writebacks;
    }
  }

 private:
  /// Bound on the replica arrays above. invariants.md §9 restricts the toggle
  /// to 1 or 3; this leaves room without making the arrays dynamic, since they
  /// are per-call scratch on a path that runs per node read.
  static constexpr size_t kMaxReplicas = 8;

  ReplicaSet &set_;
  QuorumStats &stats_;
  VecOffsetHint *hint_ = nullptr;
  RemoteAddr last_read_addr_{};
  size_t last_max_replica_ = 0;
  bool last_max_valid_ = false;

  /// A validated speculation, waiting for the readVec that wants it.
  bool spec_valid_ = false;
  VecOffset spec_off_ = kNullVec;
  VecRecord spec_vec_{};
};

}  // namespace ds
