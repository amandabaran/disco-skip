#pragma once

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
  AwaitSnapshot,  ///< an RDMA read of the replicated counter (Faa mode)
  Traversing,     ///< routing to the node covering lo
  AwaitHeader,    ///< reading the current node's header, maybe with its vector
  AwaitVec,       ///< that header did not carry its vector
  AwaitSettle,    ///< helping a pending or mid-split node
  AwaitOldVer,    ///< chasing old_ver back towards the snapshot
  AwaitIdxHeader, ///< batched walk: reading the level-0 index node
  AwaitIdxVec,    ///< batched walk: its vector, which names the backbone
  AwaitBatchHdrs, ///< batched walk: K data-node headers in one round trip
  AwaitBatchVecs, ///< batched walk: their vectors, likewise
  Done,
};

/// @param Ops  the async operation surface (RdmaAsyncOps, or a fake)
template <class Ops>
class RangeOperation {
 public:
  RangeOperation(Ops &ops, uint32_t layers, RangeStats &stats,
                 bool batched_walk = false)
      : ops_(ops), layers_(layers), stats_(stats), trav_(ops),
        batched_(batched_walk) {}

  /// @return completions to await before the first step()
  size_t start(Key lo, Key hi, size_t cap, std::vector<Entry> &out) {
    begin(lo, hi, cap, out);
    if (hi < lo) return done(true);   // empty interval, not an error

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
      case RangeStep::Traversing:    return onTraversal();
      case RangeStep::AwaitHeader:   return onHeader();
      case RangeStep::AwaitVec:      return onVec();
      case RangeStep::AwaitSettle:   return onSettle();
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
    bone_n_ = bone_ok_ = bone_i_ = 0;
    batch_resume_ = kNoResume;
    orphan_until_ = RemoteAddr{};
    tail_succ_ = RemoteAddr{};
    bone_pending_ = false;
    idx_i_ = 0;
    idx_past_hi_ = false;
    ++stats_.ranges;
  }

  size_t done(bool resolved) {
    res_.resolved = resolved;
    if (!resolved) ++stats_.failures;
    step_ = RangeStep::Done;
    return 0;
  }

  size_t onSnapshot() {
    // The maximum over the replicas that answered, admitting every client
    // index at that counter value. See ds_range.hpp for why the low bits are
    // all ones rather than zero -- masking them down would drop writes by
    // client id, which is a wrong answer rather than a stale one.
    res_.snapshot = (ops_.resolveTsCounter() << kFaaClientBits) |
                    kFaaClientMask;
    return beginTraversal();
  }

  size_t beginTraversal() {
    step_ = RangeStep::Traversing;
    return trav_.start(lo_, layers_, path_);
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
      return done(r.status == TraversalStatus::Miss);
    }
    cur_ = r.data_addr;
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
  // Backbone from the index, fetched K at a time; orphans picked up from the
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
    if (!ops_.resolveVec(idx_vec_)) return done(false);
    ++stats_.vec_reads;
    return useIndex();
  }

  /// A fresh index node: position the cursor and take the first backbone.
  size_t useIndex() {
    int const first = findLte(idx_vec_, lo_);
    idx_i_ = first < 0 ? 0 : static_cast<uint32_t>(first);
    // The next index node, captured now for the same reason the data walk
    // captures next_: the vector is about to be reused.
    idx_next_ = nextNode(idx_node_, idx_vec_);
    return fillBackbone();
  }

  /// Take up to K data-node addresses out of the index vector, continuing from
  /// wherever the last batch stopped.
  ///
  /// One index node names more data nodes than one batch can carry, so a
  /// backbone that stops because it hit the fanout is NOT a finished index
  /// node -- it is refilled from the same vector. Conflating the two truncated
  /// every range at exactly K nodes; the differential test against the serial
  /// walk is what caught it.
  size_t fillBackbone() {
    bone_n_ = 0;
    for (; idx_i_ < idx_vec_.size && bone_n_ < Ops::walkFanout(); ++idx_i_) {
      if (idx_vec_.e[idx_i_].key > hi_) { idx_past_hi_ = true; break; }
      bone_[bone_n_++] = RemoteAddr{idx_vec_.e[idx_i_].val};
    }
    if (bone_n_ == 0) return nextIndexNode();

    // THE TAIL CHECK. The last node of the previous backbone had no bone_[i+1]
    // to compare its successor against, so the judgement was deferred to here,
    // where the next backbone's first address is finally known. Guessing null
    // for it instead sent every batch down a bogus orphan detour and truncated
    // the range at one node.
    if (!tail_succ_.isNull() && tail_succ_ != bone_[0]) {
      ++stats_.orphans_walked;
      cur_ = tail_succ_;
      tail_succ_ = RemoteAddr{};
      orphan_until_ = bone_[0];
      batch_resume_ = 0;
      bone_pending_ = true;   // addresses chosen, not yet fetched
      return postHeader();
    }
    tail_succ_ = RemoteAddr{};
    return postBackbone();
  }

  size_t postBackbone() {
    bone_pending_ = false;
    ++stats_.batches;
    step_ = RangeStep::AwaitBatchHdrs;
    return ops_.postWalkHeaders(bone_, bone_n_);
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
      batched_ = false;
      cur_ = tail_succ_;
      tail_succ_ = RemoteAddr{};
      return postHeader();
    }
    return done(true);
  }

  size_t onBatchHdrs() {
    // Resolve every node's quorum, collecting the vector offsets to fetch.
    bone_ok_ = 0;
    for (size_t i = 0; i < bone_n_; ++i) {
      size_t winner = 0;
      if (!ops_.resolveWalkHeader(i, bone_node_[i], winner)) {
        // One contended node. Everything before it is still good; walk the rest
        // serially from here rather than discarding the batch.
        ++stats_.batch_fallbacks;
        break;
      }
      bone_win_[bone_ok_] = winner;
      bone_off_[bone_ok_] = bone_node_[i].handle.offset();
      ++bone_ok_;
    }
    if (bone_ok_ == 0) {
      batched_ = false;
      return postHeader();
    }
    step_ = RangeStep::AwaitBatchVecs;
    return ops_.postWalkVecs(bone_off_, bone_win_, bone_ok_);
  }

  size_t onBatchVecs() {
    bone_i_ = 0;
    return drainBatch();
  }

  /// Walk the fetched nodes in key order, collecting entries.
  ///
  /// Anything the batch cannot answer -- a pending or mid-split node, a version
  /// newer than the snapshot, or an ORPHAN sitting between two backbone nodes
  /// -- hands off to the serial path for that node and resumes afterwards. The
  /// orphan case is not rare: a capacity split produces a node with no parent,
  /// and a workload-E run produced 2535 of them against 16783 height-driven
  /// splits, so an index-only walk would drop roughly one node in eight.
  size_t drainBatch() {
    while (bone_i_ < bone_ok_) {
      size_t const i = bone_i_;
      NodeRecord const &nd = bone_node_[i];
      VecRecord const &vc = ops_.walkVec(i);

      if (nd.k_min > hi_) return done(true);

      if (vc.isPending() || !nd.isStable() || vc.ts > res_.snapshot) {
        // Not answerable from the batch. Resume the serial path at this node;
        // it settles, walks old_ver, and continues from there.
        ++stats_.batch_misses;
        cur_ = bone_[i];
        batch_resume_ = i + 1;
        // The serial path walks this node; its successor still has to be
        // checked for an orphan, exactly as the batched path would have.
        orphan_until_ = (i + 1 < bone_ok_) ? bone_[i + 1] : RemoteAddr{};
        return postHeader();
      }

      if (!versionIsWithin(vc, res_.snapshot)) {
        ++stats_.snapshot_violations;
        return done(false);
      }
      for (uint32_t e = 0; e < vc.size; ++e) {
        Key const k = vc.e[e].key;
        if (k < lo_) continue;
        if (k > hi_) break;
        if (out_->size() >= cap_) {
          res_.capped = true;
          ++stats_.capped;
          return done(true);
        }
        out_->push_back(vc.e[e]);
        ++stats_.entries;
      }
      ++stats_.nodes_walked;

      // ORPHAN CHECK. The backbone comes from the index, which does not name
      // capacity-split nodes. If this node's successor is not the next backbone
      // node, an orphan chain sits between them and must be walked.
      RemoteAddr const succ = nextNode(nd, vc);
      ++bone_i_;
      if (i + 1 >= bone_ok_) {
        // Last node of the backbone: nothing to compare against yet. Defer to
        // fillBackbone, which will know the next backbone's first address.
        tail_succ_ = succ;
        break;
      }
      RemoteAddr const expect = bone_[i + 1];
      if (!succ.isNull() && succ != expect) {
        ++stats_.orphans_walked;
        cur_ = succ;
        batch_resume_ = bone_i_;
        orphan_until_ = expect;
        return postHeader();
      }
    }
    // Backbone done. Refill from this index vector if it has more, else step
    // to the next index node, else finished -- fillBackbone decides which.
    return fillBackbone();
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

    if (!have_vec_) {
      step_ = RangeStep::AwaitVec;
      return ops_.postVec(node_.handle.offset());
    }
    return useVersion();
  }

  size_t onVec() {
    if (!ops_.resolveVec(vec_)) return done(false);
    ++stats_.vec_reads;
    have_vec_ = true;
    return useVersion();
  }

  /// We hold the node and its CURRENT vector. Settle it if need be, then begin
  /// the walk back towards the snapshot.
  size_t useVersion() {
    bool const pending = vec_.isPending();
    bool const unstable = !node_.isStable();
    if (pending || unstable) {
      if (++settle_tries_ > static_cast<uint32_t>(detail::kMaxSettleAttempts)) {
        return done(false);
      }
      // A pending version cannot be compared with T at all, and skipping it
      // would drop a committed write from the snapshot -- its eventual stamp
      // may well be <= T. So complete it, exactly as the traversal does.
      Batch b;
      if (pending) {
        b.casTs(node_.handle.offset(), kNullTs, ops_.now());
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
    (void)ops_.resolveBatch(last_batch_);
    // Re-read: the settle told us the node is complete, not what it now holds.
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
    if (++hops_ > detail::kMaxVersionHops) return done(false);
    step_ = RangeStep::AwaitOldVer;
    ++stats_.versions_walked;
    return ops_.postVec(static_cast<VecOffset>(vec_.old_ver));
  }

  size_t onOldVer() {
    if (!ops_.resolveVec(vec_)) return done(false);
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
      Key const k = vec_.e[i].key;
      if (k < lo_) continue;
      if (k > hi_) break;            // entries are sorted
      if (out_->size() >= cap_) {
        res_.capped = true;
        ++stats_.capped;
        return done(true);
      }
      out_->push_back(vec_.e[i]);
      ++stats_.entries;
    }
    return stepRight();
  }

  size_t stepRight() {
    // A serial detour out of the batched walk ends here. Two ways back:
    //   * an ORPHAN chain -- keep walking until we reach the backbone node the
    //     index said comes next, then resume the batch after it;
    //   * a settle or old_ver detour -- that one node is done, so resume at the
    //     next backbone entry.
    if (batch_resume_ != kNoResume) {
      RemoteAddr const succ = nextNode(node_, vec_);
      if (!orphan_until_.isNull()) {
        if (succ != orphan_until_ && !succ.isNull()) {
          cur_ = succ;            // still inside the orphan chain
          ++stats_.orphans_walked;
          return postHeader();
        }
        orphan_until_ = RemoteAddr{};
      } else if (batch_resume_ >= bone_ok_) {
        // A detour off the LAST backbone node: its successor is the tail the
        // next fillBackbone has to validate.
        tail_succ_ = succ;
      }
      bone_i_ = batch_resume_;
      batch_resume_ = kNoResume;
      if (bone_pending_) return postBackbone();
      return drainBatch();
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
  /// The backbone: the data nodes the level-0 index names, in key order.
  RemoteAddr bone_[Ops::walkFanout()]{};
  NodeRecord bone_node_[Ops::walkFanout()]{};
  VecOffset bone_off_[Ops::walkFanout()]{};
  size_t bone_win_[Ops::walkFanout()]{};
  size_t bone_n_ = 0;    ///< named by the index
  size_t bone_ok_ = 0;   ///< of those, whose headers resolved to a majority
  size_t bone_i_ = 0;    ///< cursor while draining
  size_t batch_resume_ = kNoResume;
  RemoteAddr orphan_until_{};
  RemoteAddr tail_succ_{};
  bool bone_pending_ = false;

  RangeStep step_ = RangeStep::Idle;
  RemoteAddr cur_{};
  RemoteAddr next_{};
  NodeRecord node_{};
  VecRecord vec_{};
  bool have_vec_ = false;
  uint32_t hops_ = 0;
  uint32_t settle_tries_ = 0;
  Batch last_batch_{};
  PathStep path_[kMaxLayers]{};
};

}  // namespace ds
