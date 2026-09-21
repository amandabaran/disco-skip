#pragma once

#include <string>

// A10: the resumable snapshot range.
//
// The state-machine form of ds_range.hpp's Ranger, for the pipelined client.
// The ALGORITHM lives there, with the argument for why walking the current
// chain while reading each node as of T is sound; this file is that walk turned
// inside out so it can yield between round trips. range_test.cc drives both
// against the same arena and requires identical output, which is what keeps the
// two from drifting -- the divergence between a blocking path and its async
// twin is what produced the unbounded-retry bug in ds_put_future.hpp.
//
// ── Shape of a range ────────────────────────────────────────────────────────
//
//   1. Take the snapshot. In Faa mode that is an RDMA READ of the replicated
//      counter -- never a fetch-and-add, because a reader needs to observe the
//      counter, not claim a slot. One round trip, and concurrent ranges do not
//      contend. In Clock mode it is local and costs nothing.
//   2. Traverse to the data node covering `lo`. As-of-now, deliberately: it
//      finds where lo lives in the CURRENT chain, which is the chain the walk
//      follows.
//   3. Per node: read header (+ speculated vector), settle if pending, then
//      walk old_ver back to the newest version with ts <= T, collect the
//      entries inside [lo, hi], and step right.
//   4. Stop when the node's k_min exceeds hi, or the chain ends, or the
//      caller's cap is reached.
//
// The old_ver walk is the one part with no analogue in Get or Put: it is a
// sequential chase of a linked list in remote memory, one round trip per hop,
// and it is why a range over a hot key costs more than a range over a cold one.
// versions_walked counts the hops so that cost is measurable rather than
// inferred.

#include <cstdint>
#include <vector>

#include "ds_async.hpp"
#include "ds_defs.hpp"
#include "ds_node.hpp"
#include "ds_range.hpp"
#include "ds_ts.hpp"

namespace ds {

enum class RangeStep : uint8_t {
  Idle,
  AwaitSnapshot,  ///< claiming the cut: a counter READ in Faa mode, a
                  ///< fetch-and-add in RangeTs (see takeSnapshot in
                  ///< ds_range.hpp for why the verbs differ by mode)
  AwaitSnapshotWriteBack,  ///< RangeTs only: raising the laggard replicas to
                           ///< the cut before the walk begins
  Traversing,     ///< routing to the node covering lo
  AwaitHeader,    ///< reading the current node's header, maybe with its vector
  AwaitVec,       ///< that header did not carry its vector
  AwaitSettle,    ///< helping a pending or mid-split node
  AwaitSettleStamp, ///< Faa mode only: writing the ts the help batch claimed
  AwaitOldVer,    ///< chasing old_ver back towards the snapshot
  AwaitIdxHeader, ///< batched walk: reading the level-0 index node
  AwaitIdxVec,    ///< batched walk: its vector, which names the chain
  AwaitBatchHdrs, ///< batched walk: K data-node headers in one round trip
  AwaitBatchVecs, ///< batched walk: their vectors, likewise
  Done,
};

/// @param Ops    the async operation surface (RdmaAsyncOps, or a fake)
/// @param Cache  the cache adapter, for --cache-walk. Defaults to NullPutCache
///               so every existing RangeOperation<Ops> still compiles and
///               behaves identically: NullPutCache::locateDataRange returns 0,
///               which is a miss, which traverses.
template <class Ops, class Cache = NullPutCache>
class RangeOperation {
 public:
  RangeOperation(Ops &ops, uint32_t layers, RangeStats &stats,
                 bool batched_walk = false, Cache *cache = nullptr,
                 bool cache_walk = false)
      : ops_(ops), layers_(layers), stats_(stats), trav_(ops),
        batched_(batched_walk), cache_(cache),
        cache_walk_(cache_walk && cache != nullptr) {}

  /// @return completions to await before the first step()
  size_t start(Key lo, Key hi, size_t cap, std::vector<Entry> &out) {
    begin(lo, hi, cap, out);
    if (hi < lo) return done(true);   // empty interval, not an error

    // NO TIMESTAMPS MEANS NO SNAPSHOT, so there is no answer to give. Refusing
    // is the only correct behaviour: the versions in the arena were never
    // stamped, so no ordering can be reconstructed now and any interval we
    // returned would be a mix of states that never coexisted. Reported as a
    // failure the caller can see, not an empty result -- an empty range looks
    // like a legitimate answer.
    if (!tsStamps(ops_.tsMode())) {
      ++stats_.failures;
      res_.gave_up_no_timestamps = true;
      return done(false);
    }

    // RangeTs ADVANCES the counter here, which is the mode's defining move:
    // it is the only thing that advances it at all, so two ranges that merely
    // read would share a cut and disagree about every write between them. The
    // claim is the same round a Faa-mode WRITE issues -- one FaaTs per replica,
    // maximum over the answers, majority required -- so it reuses the batch
    // path rather than postTsCounter's plain read. takeSnapshot() in
    // ds_range.hpp carries the derivation, and the blocking Ranger is
    // differentially tested against this, so the two must agree.
    if (ops_.tsMode() == TsMode::RangeTs) {
      snap_batch_ = Batch{};
      snap_batch_.faaTs();
      step_ = RangeStep::AwaitSnapshot;
      return ops_.postBatch(snap_batch_);
    }
    if (ops_.tsMode() == TsMode::Faa) {
      step_ = RangeStep::AwaitSnapshot;
      return ops_.postTsCounter();
    }
    res_.snapshot = ops_.now();
    return beginTraversal();
  }

  /// The same walk at a CALLER-SUPPLIED snapshot, mirroring Ranger::rangeAt.
  ///
  /// Not a convenience: without it the async path's snapshot behaviour is
  /// UNTESTABLE. A fresh snapshot is always newer than everything in the arena,
  /// so the old_ver walk never runs and the as-of-T version is always the
  /// current one -- which means a bug in which successor the walk follows
  /// cannot manifest. That is not hypothetical: the differential test and a
  /// post-split test both passed with exactly that bug reintroduced, because
  /// neither could reach the path.
  size_t startAt(Key lo, Key hi, size_t cap, uint64_t snapshot,
                 std::vector<Entry> &out) {
    begin(lo, hi, cap, out);
    res_.snapshot = snapshot;
    if (hi < lo) return done(true);
    return beginTraversal();
  }

  size_t step() {
    switch (step_) {
      case RangeStep::AwaitSnapshot: return onSnapshot();
      case RangeStep::AwaitSnapshotWriteBack: return onSnapshotWriteBack();
      case RangeStep::Traversing:    return onTraversal();
      case RangeStep::AwaitHeader:   return onHeader();
      case RangeStep::AwaitVec:      return onVec();
      case RangeStep::AwaitSettle:   return onSettle();
      case RangeStep::AwaitSettleStamp: return onSettleStamp();
      case RangeStep::AwaitOldVer:   return onOldVer();
      case RangeStep::AwaitIdxHeader: return onIdxHeader();
      case RangeStep::AwaitIdxVec:    return onIdxVec();
      case RangeStep::AwaitBatchHdrs: return onBatchHdrs();
      case RangeStep::AwaitBatchVecs: return onBatchVecs();
      case RangeStep::Idle:
      case RangeStep::Done:
        break;
    }
    return 0;
  }

  [[nodiscard]] bool finished() const { return step_ == RangeStep::Done; }

  /// One line of state for the non-termination watchdog in ds_futures.hpp.
  ///
  /// The watchdog only said "a state transition is not terminating", which
  /// names no state -- and two attempts to reason out which one were wrong. A
  /// stuck range is either revisiting one step forever or advancing its cursor
  /// in a cycle, and those need different fixes, so print enough to tell them
  /// apart: the step, the cursor, the boundary key it last accepted, and where
  /// it is in the chain.
  [[nodiscard]] std::string debugState() const {
    char const *st = "?";
    switch (step_) {
      case RangeStep::Idle:           st = "Idle";           break;
      case RangeStep::AwaitSnapshot:  st = "AwaitSnapshot";  break;
      case RangeStep::AwaitSnapshotWriteBack:
        st = "AwaitSnapshotWriteBack"; break;
      case RangeStep::Traversing:     st = "Traversing";     break;
      case RangeStep::AwaitHeader:    st = "AwaitHeader";    break;
      case RangeStep::AwaitVec:       st = "AwaitVec";       break;
      case RangeStep::AwaitSettle:    st = "AwaitSettle";    break;
      case RangeStep::AwaitSettleStamp: st = "AwaitSettleStamp"; break;
      case RangeStep::AwaitOldVer:    st = "AwaitOldVer";    break;
      case RangeStep::AwaitIdxHeader: st = "AwaitIdxHeader"; break;
      case RangeStep::AwaitIdxVec:    st = "AwaitIdxVec";    break;
      case RangeStep::AwaitBatchHdrs: st = "AwaitBatchHdrs"; break;
      case RangeStep::AwaitBatchVecs: st = "AwaitBatchVecs"; break;
      case RangeStep::Done:           st = "Done";           break;
    }
    return std::string("step=") + st +
           " lo=" + std::to_string(lo_) + " hi=" + std::to_string(hi_) +
           " cur.id=" + std::to_string(cur_.id) +
           " last_kmin=" + std::to_string(static_cast<uint64_t>(last_kmin_)) +
           " chain_i=" + std::to_string(chain_i_) +
           "/" + std::to_string(chain_n_) +
           // ── THE FIELDS THAT DECIDE WHICH LOOP IS SPINNING ──────────────
           //
           // A live capture of this guard read
           //   step=AwaitOldVer lo=99837 hi=18446744073709551614
           //   cur.id=650101 last_kmin=111923 chain_i=2/0
           // and that was not enough to name the cycle. hi is the documented
           // kUnboundedKey sentinel for a count-bounded YCSB scan, so it is
           // not the anomaly; chain_i=2 against chain_n=0 is, but the drain
           // loop gates on chain_ok_ rather than chain_n_, and chain_ok_ was
           // not printed -- so whether the cursor is genuinely past the end
           // could not be told from the message.
           //
           // hops_ matters for the opposite reason: the old_ver chase IS
           // bounded by kMaxVersionHops, so a spin that never raises hops_
           // rules that path out rather than implicating it.
           " chain_ok=" + std::to_string(chain_ok_) +
           " batch_resume=" + std::to_string(batch_resume_) +
           " hops=" + std::to_string(hops_) +
           " orphan_until=" + (orphan_until_.isNull() ? "null" : "set") +
           " tail_succ=" + (tail_succ_.isNull() ? "null" : "set") +
           " chain_pending=" + (chain_pending_ ? "1" : "0") +
           " idx_past_hi=" + (idx_past_hi_ ? "1" : "0");
  }
  [[nodiscard]] RangeResult const &result() const { return res_; }

 private:
  void begin(Key lo, Key hi, size_t cap, std::vector<Entry> &out) {
    lo_ = lo;
    hi_ = hi;
    cap_ = cap;
    out_ = &out;
    res_ = RangeResult{};
    hops_ = 0;
    settle_tries_ = 0;
    chain_n_ = chain_ok_ = chain_i_ = 0;
    batch_resume_ = kNoResume;
    orphan_until_ = RemoteAddr{};
    tail_succ_ = RemoteAddr{};
    chain_pending_ = false;
    idx_i_ = 0;
    idx_past_hi_ = false;
    from_cache_ = false;
    cache_rearm_ = false;
    walked_any_ = false;
    last_succ_ = RemoteAddr{};
    last_kmin_ = Key{};
    ++stats_.ranges;
  }

  size_t done(bool resolved) {
    res_.resolved = resolved;
    if (!resolved) ++stats_.failures;
    step_ = RangeStep::Done;
    return 0;
  }

  size_t onSnapshot() {
    if (ops_.tsMode() == TsMode::RangeTs) {
      BatchResult const r = ops_.resolveBatch(snap_batch_);
      // A CUT THAT COULD NOT BE TAKEN IS NOT AN EMPTY RANGE. kNullTs from a
      // claiming round means it fell short of a majority, so the value orders
      // nothing -- the same case a write refuses to stamp with. Walking at
      // kNullTs would find no version within it and report a RESOLVED, EMPTY
      // interval, which is a wrong answer that looks like a legitimate one.
      if (!r.submitted || r.ts == kNullTs) {
        ++stats_.fail_snapshot;
        return done(false);
      }
      res_.snapshot = snapshotFromClaim(r.ts);
      // The write-back, BEFORE the walk rather than before the return: a
      // later writer's read quorum can exclude the replica that decided this
      // maximum, and would then stamp below a cut already handed out. Doing it
      // first is stronger than necessary and much easier to see is correct.
      // Skipped entirely when the replicas agreed, which is the common case.
      if (ops_.tsNeedsWriteBack()) {
        snap_batch_ = Batch{};
        snap_batch_.faaTsCatchUp();
        step_ = RangeStep::AwaitSnapshotWriteBack;
        return ops_.postBatch(snap_batch_);
      }
      return beginTraversal();
    }
    // Faa mode: a plain READ of the replicated counter. The maximum over the
    // replicas that answered, admitting every client index at that counter
    // value. See ds_range.hpp for why the low bits are all ones rather than
    // zero -- masking them down would drop writes by client id, which is a
    // wrong answer rather than a stale one.
    res_.snapshot = (ops_.resolveTsCounter() << kFaaClientBits) |
                    kFaaClientMask;
    return beginTraversal();
  }

  size_t onSnapshotWriteBack() {
    BatchResult const r = ops_.resolveBatch(snap_batch_);
    if (!r.submitted) {
      ++stats_.fail_snapshot;
      return done(false);
    }
    return beginTraversal();
  }

  size_t beginTraversal() {
    // THE CACHE WALK. Try for a chain locally first: the remote descent is
    // ~4 dependent round trips and the next_id walk another ~11, and every
    // address both of them discover is already in a directory this cache
    // maintains. A miss falls through to the descent, so this is strictly an
    // attempt to skip work.
    if (cache_walk_) {
      if (fillFromCache(lo_, /*after=*/false) > 0) {
        ++stats_.cache_chains;
        from_cache_ = true;
        return postChain();
      }
      ++stats_.cache_misses;
    }
    step_ = RangeStep::Traversing;
    return trav_.start(lo_, layers_, path_);
  }

  // ── ROUND-TRIP ACCOUNTING, which is the whole justification ──────────────
  //
  // Measured occupancy: level-0 nodes hold 6.2 entries against a capacity of
  // 16, so a scan of L keys touches ceil(L/6.2) nodes.
  //
  //   SERIAL: 1 round trip per node. The header and a SPECULATED vector read
  //   are chained on one queue pair (postHeaders + guess), and the offset
  //   hint almost always hits on a scan workload, so a node costs one trip
  //   rather than two. That makes serial a stronger baseline than a naive
  //   count suggests.
  //
  //   BATCHED: 2 round trips per batch, and no fewer. The vector offsets are
  //   not known until the headers come back, so the two volleys are dependent
  //   and the speculation the serial path enjoys is unavailable. A batch that
  //   covers the whole scan therefore costs 2, not 1.
  //
  // So the ceiling on the win is ~9/2 and ~17/2, and it is realised only if ONE
  // batch covers the range. Two sizing mistakes each destroyed it:
  //
  //   * asking by hi, which OpScan leaves unbounded (a YCSB scan is
  //     count-bounded, so nearly every range ends on the entry cap). The
  //     batch then always asked for the full fanout and fetched several times
  //     the nodes it used, and measured SLOWER than serial.
  //
  //   * asking by kNodeCapacity, which assumes full nodes. At the observed
  //     occupancy a scan needs about twice the nodes it was asked for, so the
  //     batch covered part of the range and refilled repeatedly -- and again
  //     measured slower than serial.
  //
  // Hence sizing by ceil(remaining / (kNodeCapacity/2)) + 1: simulated against
  // the measured occupancy that is 2 batches, 4 round trips, and 17 nodes
  // fetched for the 17 a 100-key scan needs.
  //
  // NOTE THE OPEN QUESTION. The second round did ~8 round trips against
  // serial's ~11 and still measured 229 against 433 -- FEWER trips and far
  // lower throughput. Round-trip count alone does not account for that, so if
  // the current sizing still loses, the cause is elsewhere: most likely that
  // two dependent volleys pipeline worse under -a 4 than a stream of
  // independent single-node reads. That would point at having the cache store
  // vector offsets too, so the batch could speculate as the serial path does.

  /// Fill chain_ from the cache. @param after skips a leading entry that repeats
  /// the last node already walked.
  ///
  /// locate_data_range starts at the greatest k_min <= from, which is what a
  /// range START needs -- the node containing lo. On a REFILL that same rule
  /// returns the node just finished, because its k_min is still <= the key we
  /// ask from. Hence /after/: ask from the last k_min and drop it if it comes
  /// back. Asking from last_kmin+1 instead would be wrong for a key type where
  /// +1 is not the next possible key.
  size_t fillFromCache(Key from, bool after) {
    size_t max = Ops::walkFanout() < kBoneMax ? Ops::walkFanout() : kBoneMax;

    // BOUND IT BY THE REMAINING ENTRY CAP, not only by hi.
    //
    // A YCSB scan is COUNT-bounded and OpScan passes hi = kUnboundedKey, so hi
    // limits nothing: the cache returns its full width every time, which was
    // several times the nodes a mean scan actually needs, and nearly every
    // range ended on the entry cap rather than on hi. The cache walk then
    // measured SLOWER than the serial walk, entirely from over-fetching.
    //
    // +1 node because the first contributes only its entries >= lo, so a cap
    // of exactly one node's worth can still span two.
    size_t const have = out_->size();
    size_t const want = have >= cap_ ? 0 : cap_ - have;

    // DIVIDE BY OBSERVED OCCUPANCY, NOT CAPACITY. A level-0 node holds
    // kNodeCapacity entries but averages well under half that in practice,
    // because a split leaves both halves partly full. Dividing by the capacity
    // underestimated the nodes a scan needs, so the batch covered only part of
    // the range and refilled repeatedly -- a round trip per refill, and most of
    // the benefit gone.
    //
    // Half capacity is the standard occupancy assumption for a split-on-full
    // structure and matches the measurement closely enough: ceil(50/8)+1 = 8
    // against the ~9 actually needed, so a rare refill rather than four.
    // Over-asking is bounded by the fanout and costs a wasted address, not a
    // wasted fetch -- drainBatch stops at the cap.
    size_t const per_node = kNodeCapacity / 2;
    size_t const need = (want + per_node - 1) / per_node + 1;
    if (need < max) max = need;
    if (max == 0) return 0;
    size_t n = cache_->locateDataRange(from, hi_, chain_kmin_, chain_, max);

    size_t first = 0;
    if (after && n > 0 && chain_kmin_[0] <= last_kmin_) first = 1;

    // Truncate at the first NULL address. Null means the local node has no
    // known remote counterpart (node_t::remote_addr), so it is a miss for that
    // entry -- and everything after it would be reached by guessing.
    size_t good = 0;
    for (size_t i = first; i < n; ++i) {
      if (chain_[i].isNull()) break;
      chain_[good] = chain_[i];
      chain_kmin_[good] = chain_kmin_[i];
      ++good;
    }
    chain_n_ = good;
    stats_.cache_addrs += good;
    return good;
  }

  size_t onTraversal() {
    size_t const await = trav_.step();
    if (await != 0 || !trav_.finished()) return await;

    TraversalResult const &r = trav_.result();
    stats_.nodes_read += r.nodes_read;
    stats_.vec_reads += r.vec_reads;
    stats_.helped += r.helped_ts + r.helped_splits;
    if (!r.ok()) {
      // Miss means nothing routes to lo: the structure holds nothing at or
      // below it, so the range is empty rather than failed.
      if (r.status != TraversalStatus::Miss) ++stats_.fail_traverse;
      return done(r.status == TraversalStatus::Miss);
    }
    cur_ = r.data_addr;
    if (batched_ && r.levels == 0) {
      // Nothing above the data level to take a chain from, so this range is
      // serial no matter what the toggle says. Counted: a measured run where
      // most of the walk was serial reported "0 fallbacks", which read as the
      // batched path running cleanly when it had barely run at all.
      ++stats_.batch_abandoned;
    }
    if (batched_ && r.levels > 0) {
      // path_[0] is the level-0 INDEX node covering lo, already located by the
      // traversal we just paid for. Its entries name up to kNodeCapacity
      // consecutive data nodes, which is what makes a batched fetch possible at
      // all -- their addresses are known without reading them.
      idx_addr_ = path_[0].addr;
      return postIdxHeader();
    }
    return postHeader();
  }

  // ── The batched walk ──────────────────────────────────────────────────────
  //
  // Chain from the index, fetched K at a time; orphans picked up from the
  // next_id chain. See the note on postWalkHeaders in ds_rdma_async.hpp for why
  // the index breaks the round-trip dependency, and RangeStats::orphans_walked
  // for why the chain still has to be checked.

  size_t postIdxHeader() {
    step_ = RangeStep::AwaitIdxHeader;
    return ops_.postHeaders(idx_addr_, ops_.guess(idx_addr_));
  }

  size_t onIdxHeader() {
    bool have = false;
    if (!ops_.resolveHeaders(idx_addr_, idx_node_, have, idx_vec_)) {
      // The index is contended. Fall back rather than spin: the serial walk
      // reaches the same answer, only slower, and correctness is not on the
      // line here.
      ++stats_.batch_fallbacks;
      batched_ = false;
      return postHeader();
    }
    ++stats_.nodes_read;
    if (have) { ++stats_.vec_reads; return useIndex(); }
    step_ = RangeStep::AwaitIdxVec;
    return ops_.postVec(idx_node_.handle.offset());
  }

  size_t onIdxVec() {
    if (!ops_.resolveVec(idx_vec_)) {
      ++stats_.fail_read;
      return done(false);
    }
    ++stats_.vec_reads;
    return useIndex();
  }

  /// A fresh index node: position the cursor and take the first chain.
  size_t useIndex() {
    int const first = findLte(idx_vec_, lo_);
    idx_i_ = first < 0 ? 0 : static_cast<uint32_t>(first);
    // The next index node, captured now for the same reason the data walk
    // captures next_: the vector is about to be reused.
    idx_next_ = nextNode(idx_node_, idx_vec_);
    return fillChainFromIndex();
  }

  /// Take up to K data-node addresses out of the index vector, continuing from
  /// wherever the last batch stopped.
  ///
  /// One index node names more data nodes than one batch can carry, so a
  /// chain that stops because it hit the fanout is NOT a finished index
  /// node -- it is refilled from the same vector. Conflating the two truncated
  /// every range at exactly K nodes; the differential test against the serial
  /// walk is what caught it.
  size_t fillChainFromIndex() {
    chain_n_ = 0;
    for (; idx_i_ < idx_vec_.size && chain_n_ < Ops::walkFanout(); ++idx_i_) {
      if (idx_vec_.keyAt(idx_i_) > hi_) { idx_past_hi_ = true; break; }
      chain_[chain_n_++] = RemoteAddr{idx_vec_.valAt(idx_i_)};
    }
    if (chain_n_ == 0) return nextIndexNode();

    // THE TAIL CHECK. The last node of the previous chain had no chain_[i+1]
    // to compare its successor against, so the judgement was deferred to here,
    // where the next chain's first address is finally known. Guessing null
    // for it instead sent every batch down a bogus orphan detour and truncated
    // the range at one node.
    if (!tail_succ_.isNull() && tail_succ_ != chain_[0]) {
      ++stats_.orphans_walked;
      cur_ = tail_succ_;
      tail_succ_ = RemoteAddr{};
      orphan_until_ = chain_[0];
      batch_resume_ = 0;
      chain_pending_ = true;   // addresses chosen, not yet fetched
      return postHeader();
    }
    tail_succ_ = RemoteAddr{};
    return postChain();
  }

  size_t postChain() {
    chain_pending_ = false;
    ++stats_.batches;
    step_ = RangeStep::AwaitBatchHdrs;
    return ops_.postWalkHeaders(chain_, chain_n_);
  }

  /// This index vector is spent. Step right along level 0 of the index, unless
  /// its keys have already passed hi.
  size_t nextIndexNode() {
    if (!idx_past_hi_ && idx_i_ >= idx_vec_.size && !idx_next_.isNull()) {
      idx_addr_ = idx_next_;
      return postIdxHeader();
    }
    // The index has run out but the last node still had a successor -- nodes
    // past the end of the index, or an orphan chain. The serial walk finishes
    // the range; it stops on its own at hi.
    if (!tail_succ_.isNull()) {
      // Off the end of the index with nodes still to come. Everything from
      // here is serial, and for a long scan that is most of the range -- at
      // scan 1000 this path turned 139k batches into 17.7M nodes walked.
      ++stats_.batch_abandoned;
      batched_ = false;
      cur_ = tail_succ_;
      tail_succ_ = RemoteAddr{};
      return postHeader();
    }
    return done(true);
  }

  size_t onBatchHdrs() {
    // Resolve every node's quorum, collecting the vector offsets to fetch.
    chain_ok_ = 0;
    for (size_t i = 0; i < chain_n_; ++i) {
      size_t winner = 0;
      if (!ops_.resolveWalkHeader(i, chain_node_[i], winner)) {
        // One contended node. Everything before it is still good; walk the rest
        // serially from here rather than discarding the batch.
        ++stats_.batch_fallbacks;
        break;
      }
      chain_win_[chain_ok_] = winner;
      chain_off_[chain_ok_] = chain_node_[i].handle.offset();
      ++chain_ok_;
    }
    if (chain_ok_ == 0) {
      batched_ = false;
      return postHeader();
    }
    step_ = RangeStep::AwaitBatchVecs;
    return ops_.postWalkVecs(chain_off_, chain_win_, chain_ok_);
  }

  size_t onBatchVecs() {
    chain_i_ = 0;
    return drainBatch();
  }

  /// Walk the fetched nodes in key order, collecting entries.
  ///
  /// Anything the batch cannot answer -- a pending or mid-split node, a version
  /// newer than the snapshot, or an ORPHAN sitting between two chain nodes
  /// -- hands off to the serial path for that node and resumes afterwards. The
  /// orphan case is not rare: a capacity split produces a node with no parent,
  /// and a workload-E run produced 2535 of them against 16783 height-driven
  /// splits, so an index-only walk would drop roughly one node in eight.
  size_t drainBatch() {
    while (chain_i_ < chain_ok_) {
      size_t const i = chain_i_;
      NodeRecord const &nd = chain_node_[i];
      VecRecord const &vc = ops_.walkVec(i);

      // A CONSISTENCY ASSERTION ON THE CACHE, not a staleness guard -- the
      // distinction matters and I had it wrong.
      //
      // A node's k_min NEVER CHANGES (ds_range.hpp, fact 1: a split at key s
      // creates a NEW node rather than moving one). So an address the cache
      // hands over always holds the node with the k_min it claimed, however
      // far behind the cache has fallen. This can only fire on a genuine cache
      // BUG -- a wrong address for a key -- which is worth catching but is not
      // what a stale cache does.
      //
      // WHAT A STALE CACHE ACTUALLY DOES is miss a node created BETWEEN two
      // entries it holds. That is caught by the successor check further down:
      // if this node's next_id is not the following chain address, an
      // orphan chain sits between them and is walked. Verified by
      // checkAStaleCacheStillGivesTheRightAnswer, which lets the cache fall
      // 400 puts behind and still requires the serial answer exactly.
      //
      // Recovery is the next_id chain, which cannot skip a node. If anything
      // has been walked already, continue from its recorded successor; if not,
      // nothing is in out_ yet and the remote descent starts over.
      if (from_cache_ && nd.k_min != chain_kmin_[i]) {
        ++stats_.cache_stale;
        from_cache_ = false;
        cache_walk_ = false;
        batch_resume_ = kNoResume;
        orphan_until_ = RemoteAddr{};
        tail_succ_ = RemoteAddr{};
        if (walked_any_ && !last_succ_.isNull()) {
          cur_ = last_succ_;
          return postHeader();
        }
        if (walked_any_) return done(true);   // chain ended here
        return beginTraversal();
      }

      if (nd.k_min > hi_) return done(true);

      if (tsIsPending(vc, ops_.tsMode()) || !nd.isStable() || vc.ts > res_.snapshot) {
        // Not answerable from the batch. Resume the serial path at this node;
        // it settles, walks old_ver, and continues from there.
        ++stats_.batch_misses;
        cur_ = chain_[i];
        batch_resume_ = i + 1;
        // The serial path walks this node; its successor still has to be
        // checked for an orphan, exactly as the batched path would have.
        orphan_until_ = (i + 1 < chain_ok_) ? chain_[i + 1] : RemoteAddr{};
        return postHeader();
      }

      if (!versionIsWithin(vc, res_.snapshot)) {
        ++stats_.snapshot_violations;
        return done(false);
      }
      for (uint32_t e = 0; e < vc.size; ++e) {
        Key const k = vc.keyAt(e);
        if (k < lo_) continue;
        if (k > hi_) break;
        if (out_->size() >= cap_) {
          res_.capped = true;
          ++stats_.capped;
          return done(true);
        }
        out_->push_back(vc.entryAt(e));
        ++stats_.entries;
      }
      ++stats_.nodes_walked;

      // ORPHAN CHECK. The chain comes from the index, which does not name
      // capacity-split nodes. If this node's successor is not the next chain
      // node, an orphan chain sits between them and must be walked.
      RemoteAddr const succ = nextNode(nd, vc);
      // Kept so a later staleness detection can resume from a node that was
      // definitely walked, rather than re-reading from lo and duplicating.
      last_succ_ = succ;
      last_kmin_ = nd.k_min;
      walked_any_ = true;
      ++chain_i_;
      if (i + 1 >= chain_ok_) {
        // Last node of the chain: nothing to compare against yet. Defer to
        // fillChainFromIndex, which will know the next chain's first address.
        tail_succ_ = succ;
        break;
      }
      RemoteAddr const expect = chain_[i + 1];
      if (!succ.isNull() && succ != expect) {
        ++stats_.orphans_walked;
        cur_ = succ;
        batch_resume_ = chain_i_;
        orphan_until_ = expect;
        return postHeader();
      }
    }
    // Chain done. A cache-sourced one refills from the cache; an
    // index-sourced one from the index vector. Both are a miss away from the
    // serial walk, which is always correct.
    if (from_cache_) {
      if (last_kmin_ >= hi_) return done(true);
      if (fillFromCache(last_kmin_, /*after=*/true) > 0) {
        ++stats_.cache_chains;
        // The tail check still applies: the previous chain's last successor
        // must be the new chain's first node, or an orphan sits between.
        if (!tail_succ_.isNull() && tail_succ_ != chain_[0]) {
          ++stats_.orphans_walked;
          cur_ = tail_succ_;
          tail_succ_ = RemoteAddr{};
          orphan_until_ = chain_[0];
          batch_resume_ = 0;
          chain_pending_ = true;
          return postHeader();
        }
        tail_succ_ = RemoteAddr{};
        return postChain();
      }
      // The cache ran out for THIS directory. Walk the chain, and stepRight
      // re-arms from the first serial node -- whose k_min lands in the next
      // directory, which is what locate_data_range needs to advance.
      ++stats_.cache_misses;
      from_cache_ = false;
      cache_rearm_ = true;
      if (!tail_succ_.isNull()) {
        cur_ = tail_succ_;
        tail_succ_ = RemoteAddr{};
        return postHeader();
      }
      return done(true);
    }
    return fillChainFromIndex();
  }

  size_t postHeader() {
    step_ = RangeStep::AwaitHeader;
    have_vec_ = false;
    return ops_.postHeaders(cur_, ops_.guess(cur_));
  }

  size_t onHeader() {
    if (!ops_.resolveHeaders(cur_, node_, have_vec_, vec_)) {
      // No majority-supported handle yet. The traversal has the full L2 repair
      // for this; here a re-poll is enough, because a range that cannot read
      // one node can simply be retried by the caller -- it holds no locks and
      // has published nothing.
      if (++settle_tries_ > static_cast<uint32_t>(detail::kMaxSettleAttempts)) {
        ++stats_.fail_no_majority;
        return done(false);
      }
      return postHeader();
    }
    ++stats_.nodes_read;
    if (have_vec_) ++stats_.vec_reads;

    // k_min is immutable (ds_range.hpp), so this is a sound stop even though
    // the version we are about to read is older than the header.
    if (node_.k_min > hi_) return done(true);

    // Counted HERE, where the node is actually visited -- not in stepRight().
    // stepRight is skipped whenever a range reaches its cap inside collect(),
    // which for a count-bounded scan is the usual way a range ends, so counting
    // there undercounted by roughly one node per range and made nodes_walked
    // smaller than the range count. Ranger counts at the visit for the same
    // reason; the two must agree or the differential test is comparing
    // different quantities.
    ++stats_.nodes_walked;
    // Record it for the cache re-arm in stepRight: a serial node is exactly
    // what tells us a key inside the NEXT directory.
    last_kmin_ = node_.k_min;
    walked_any_ = true;

    if (!have_vec_) {
      step_ = RangeStep::AwaitVec;
      return ops_.postVec(node_.handle.offset());
    }
    return useVersion();
  }

  size_t onVec() {
    if (!ops_.resolveVec(vec_)) {
      ++stats_.fail_read;
      return done(false);
    }
    ++stats_.vec_reads;
    have_vec_ = true;
    return useVersion();
  }

  /// We hold the node and its CURRENT vector. Settle it if need be, then begin
  /// the walk back towards the snapshot.
  size_t useVersion() {
    bool const pending = tsIsPending(vec_, ops_.tsMode());
    bool const unstable = !node_.isStable();
    if (pending || unstable) {
      if (++settle_tries_ > static_cast<uint32_t>(detail::kMaxSettleAttempts)) {
        ++stats_.fail_settle;
        return done(false);
      }
      // A pending version cannot be compared with T at all, and skipping it
      // would drop a committed write from the snapshot -- its eventual stamp
      // may well be <= T. So complete it, exactly as the traversal does.
      Batch b;
      helped_ts_ = pending;
      helped_off_ = node_.handle.offset();
      if (pending) {
        // IN FAA MODE THE VALUE IS NOT AVAILABLE LOCALLY. The batch claims one
        // and a SECOND submission writes it, exactly as settleNode() and the
        // write path do -- a timestamp may not be allocated before the version
        // it stamps is visible.
        //
        // Using ops_.now() here regardless of mode was a silent correctness
        // bug, and this is the path where it bit hardest. localNow() returns
        // CLOCK_REALTIME nanoseconds (~1.79e18) in Faa mode, while a Faa
        // snapshot is (counter << 16 | 0xffff) -- about 6.6e10 after a million
        // writes. So the helped version compared as newer than every possible
        // snapshot, walkToSnapshot() chased old_ver past it, and a COMMITTED
        // write became permanently invisible to range queries: the counter
        // would need ~2.7e13 more values to catch up, which is centuries at
        // the counter's measured throughput ceiling. Point reads never
        // noticed, because they do
        // not compare ts.
        if (tsIsRemote(ops_.tsMode())) {
          tsClaimOn(b, ops_.tsMode());
        } else {
          b.casTs(helped_off_, kNullTs, ops_.now());
        }
        ++stats_.helped;
      }
      if (unstable && vec_.hasSplitDescriptor()) {
        if (node_.next_k_min != vec_.k_min_next) {
          b.casNextKMin(cur_, node_.next_k_min, vec_.k_min_next);
        }
        if (node_.next_id != vec_.next_id) {
          b.casNextId(cur_, node_.next_id, vec_.next_id);
        }
      }
      if (unstable) {
        b.casTailWord(cur_, packTailWord(node_.level, node_.tail_struct_ver),
                      packTailWord(node_.level, node_.handle.structVer()));
        ++stats_.helped;
      }
      last_batch_ = b;   // resolveBatch needs the same batch that was posted
      step_ = RangeStep::AwaitSettle;
      return ops_.postBatch(last_batch_);
    }
    settle_tries_ = 0;
    hops_ = 0;
    // Capture the CURRENT successor now, before the old_ver walk overwrites
    // vec_.
    //
    // TO BE CLEAR ABOUT WHY, because the obvious justification is wrong: the
    // as-of-T version's next_id would ALSO be correct, and is in fact cheaper.
    // At T, D's successor was E; a later split inserts M between them, so the
    // current chain is D -> M -> E while the as-of-T chain is D -> E. Following
    // the as-of-T successor skips M -- but M cannot contribute anything at T,
    // because if it had existed at T then D's as-of-T next_id would name M
    // rather than E. So every node that shortcut skips is one the walk would
    // read and discard anyway.
    //
    // The current successor is used regardless, for one reason: Ranger does the
    // same, and range_test.cc pins the two implementations to identical output.
    // Keeping them structurally identical is worth more than the round trips
    // the shortcut would save, and a blocking path drifting from its async twin
    // is what produced the unbounded-retry bug in ds_put_future.hpp. If the
    // shortcut is ever wanted, both sides should take it together.
    next_ = nextNode(node_, vec_);
    return walkToSnapshot();
  }

  size_t onSettle() {
    BatchResult const r = ops_.resolveBatch(last_batch_);
    if (helped_ts_ && tsIsRemote(ops_.tsMode()) && r.ts != kNullTs) {
      helped_ts_ = false;
      // The claimed counter value, raw: stampFor() is the identity in Faa mode
      // and a helper has not read the predecessor anyway. Targets the offset
      // captured BEFORE the help batch, which is the version we found pending;
      // if a writer has since published over it, stamping the older version is
      // still correct and still what the chain needs.
      Batch stamp;
      stamp.casTs(helped_off_, kNullTs, r.ts);
        // ABD's write-back, applied to the counter: if this FAA round found
        // the replicas' counters disagreeing, raise the laggards to the
        // maximum BEFORE the operation returns. Without it a later writer can
        // claim a smaller timestamp than one that already finished -- the
        // "with only a majority it breaks" case derived in ds_ts.hpp. Rides
        // this batch, so it costs no extra round trip, and is omitted entirely
        // when the round was in agreement.
        if (ops_.tsNeedsWriteBack()) stamp.faaTsCatchUp();
      last_batch_ = stamp;
      step_ = RangeStep::AwaitSettleStamp;
      return ops_.postBatch(last_batch_);
    }
    helped_ts_ = false;
    // Re-read: the settle told us the node is complete, not what it now holds.
    return postHeader();
  }

  /// The Faa stamp landed. Re-read, as onSettle() would have.
  size_t onSettleStamp() {
    // The CAS result is deliberately ignored: a failure means somebody else
    // stamped this version first, which is the helping protocol working.
    (void)ops_.resolveBatch(last_batch_);
    return postHeader();
  }

  /// Is `vec_` at or before the snapshot? If not, chase old_ver.
  size_t walkToSnapshot() {
    if (vec_.ts != kNullTs && vec_.ts <= res_.snapshot) return collect();
    if (vec_.old_ver == kNullVec) {
      // The whole chain is newer than T: this node did not exist at the
      // snapshot. Its keys were, at T, in the node it split from -- which the
      // walk has already read. Skip it rather than inventing entries.
      ++stats_.nodes_skipped;
      return stepRight();
    }
    if (++hops_ > detail::kMaxVersionHops) {
      ++stats_.fail_hops;
      return done(false);
    }
    step_ = RangeStep::AwaitOldVer;
    ++stats_.versions_walked;
    return ops_.postVec(static_cast<VecOffset>(vec_.old_ver));
  }

  size_t onOldVer() {
    if (!ops_.resolveVec(vec_)) {
      ++stats_.fail_read;
      return done(false);
    }
    ++stats_.vec_reads;
    return walkToSnapshot();
  }

  /// `vec_` is the as-of-T version. Take the entries inside [lo, hi].
  size_t collect() {
    // A10's violation check, always on -- see RangeStats::snapshot_violations.
    // Literally the same predicate Ranger applies, so the two paths cannot
    // disagree about what a violation is.
    if (!versionIsWithin(vec_, res_.snapshot)) {
      ++stats_.snapshot_violations;
      return done(false);
    }
    for (uint32_t i = 0; i < vec_.size; ++i) {
      Key const k = vec_.keyAt(i);
      if (k < lo_) continue;
      if (k > hi_) break;            // entries are sorted
      if (out_->size() >= cap_) {
        res_.capped = true;
        ++stats_.capped;
        return done(true);
      }
      out_->push_back(vec_.entryAt(i));
      ++stats_.entries;
    }
    return stepRight();
  }

  size_t stepRight() {
    // A serial detour out of the batched walk ends here. Two ways back:
    //   * an ORPHAN chain -- keep walking until we reach the chain node the
    //     index said comes next, then resume the batch after it;
    //   * a settle or old_ver detour -- that one node is done, so resume at the
    //     next chain entry.
    if (batch_resume_ != kNoResume) {
      RemoteAddr const succ = nextNode(node_, vec_);
      if (!orphan_until_.isNull()) {
        if (succ != orphan_until_ && !succ.isNull()) {
          cur_ = succ;            // still inside the orphan chain
          ++stats_.orphans_walked;
          return postHeader();
        }
        orphan_until_ = RemoteAddr{};
      } else if (batch_resume_ >= chain_ok_) {
        // A detour off the LAST chain node: its successor is the tail the
        // next fillChainFromIndex has to validate.
        tail_succ_ = succ;
      }
      chain_i_ = batch_resume_;
      batch_resume_ = kNoResume;
      if (chain_pending_) return postChain();
      return drainBatch();
    }

    // RE-ARM THE CACHE. locate_data_range reads only the directory containing
    // the key it is asked for, so asking from a key inside the directory just
    // drained returns that directory again and the refill always misses -- one
    // batch per range and serial thereafter (measured: 38 chains over 40
    // ranges, 40 misses). One serial node breaks that: the node just walked has
    // a k_min in the NEXT directory, so the cache can be asked from there.
    //
    // Costs one round trip per directory boundary instead of one per node.
    if (cache_rearm_ && cache_walk_ && walked_any_ && last_kmin_ < hi_) {
      RemoteAddr const serial_succ = nextNode(node_, vec_);
      if (fillFromCache(last_kmin_, /*after=*/true) > 0) {
        ++stats_.cache_chains;
        from_cache_ = true;
        if (!serial_succ.isNull() && serial_succ != chain_[0]) {
          // An orphan sits between the serial node and the new chain.
          ++stats_.orphans_walked;
          cur_ = serial_succ;
          orphan_until_ = chain_[0];
          batch_resume_ = 0;
          chain_pending_ = true;
          return postHeader();
        }
        tail_succ_ = RemoteAddr{};
        return postChain();
      }
      // Still nothing: stop trying, so a range whose keys the cache does not
      // hold does not pay a lookup per node for the rest of the walk.
      cache_rearm_ = false;
    }

    // The CURRENT chain, from the header and vector we read before walking
    // back -- not from the as-of-T version, whose successor may since have
    // been split away. next_ was captured at that point for exactly this.
    cur_ = next_;
    if (cur_.isNull()) return done(true);
    return postHeader();
  }

  Ops &ops_;
  uint32_t layers_;
  RangeStats &stats_;
  TraversalFuture<Ops> trav_;

  Key lo_ = 0, hi_ = 0;
  size_t cap_ = 0;
  std::vector<Entry> *out_ = nullptr;
  RangeResult res_{};

  static constexpr size_t kNoResume = ~size_t{0};

  bool batched_ = false;
  RemoteAddr idx_addr_{};
  RemoteAddr idx_next_{};
  NodeRecord idx_node_{};
  VecRecord idx_vec_{};
  uint32_t idx_i_ = 0;
  bool idx_past_hi_ = false;
  /// The chain: the data nodes the level-0 index names, in key order.
  RemoteAddr chain_[Ops::walkFanout()]{};
  NodeRecord chain_node_[Ops::walkFanout()]{};
  VecOffset chain_off_[Ops::walkFanout()]{};
  size_t chain_win_[Ops::walkFanout()]{};
  size_t chain_n_ = 0;    ///< named by the index
  size_t chain_ok_ = 0;   ///< of those, whose headers resolved to a majority
  size_t chain_i_ = 0;    ///< cursor while draining
  size_t batch_resume_ = kNoResume;
  RemoteAddr orphan_until_{};
  RemoteAddr tail_succ_{};
  bool chain_pending_ = false;
  Cache *cache_ = nullptr;
  bool cache_walk_ = false;
  bool from_cache_ = false;
  bool cache_rearm_ = false;
  bool walked_any_ = false;
  RemoteAddr last_succ_{};
  Key last_kmin_{};
  static constexpr size_t kBoneMax = 16;
  Key chain_kmin_[kBoneMax]{};

  RangeStep step_ = RangeStep::Idle;
  RemoteAddr cur_{};
  RemoteAddr next_{};
  NodeRecord node_{};
  VecRecord vec_{};
  bool have_vec_ = false;
  uint32_t hops_ = 0;
  uint32_t settle_tries_ = 0;
  Batch last_batch_{};
  /// The snapshot claim, and then its write-back. A MEMBER, not a local: the
  /// batch must outlive the suspension, because postBatch() records a pointer
  /// to it and resolveBatch() locates the claim by op position in that same
  /// object. A stack local would be a dangling read at the next step().
  Batch snap_batch_{};
  VecOffset helped_off_ = kNullVec;  ///< the pending version a help batch targeted
  bool helped_ts_ = false;           ///< that batch carried a Faa claim to write back
  PathStep path_[kMaxLayers]{};
};

}  // namespace ds
