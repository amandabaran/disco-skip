#pragma once

// A Put as a resumable operation: F1, F2 and the Update_Index climb, yieldable.
//
// The state-machine twin of Putter. Harder than the Get side because a put is
// nested: insertEntry and splitAt each have their own fetch-settle-commit-retry
// cycle, and the height-driven climb runs one of those per level. Rather than
// nest three machines, this is ONE flat machine -- a step saying what is
// outstanding, and a phase saying what the put is trying to achieve -- which is
// longer to read but far easier to reason about when a transition is wrong.
//
// Everything structural is borrowed, not restated: the coverage check, the
// median choice, what a split's two vectors and header contain, the batch
// contents and their order. Those are the same expressions the blocking Writer
// uses, and remote-design.md §2 is the specification for all of them.
//
// THE RETRY IS A TRANSITION, NOT A LOOP. Where the blocking Writer re-reads and
// loops, this goes back to FetchHeader for the same target. That is the whole
// structural difference, and it is why a lost handle CAS costs a suspension
// rather than a spin.
//
// Idempotence is what makes that safe, exactly as it does in the blocking path:
// splitting at a key that is already a node's k_min is a no-op returning that
// node, so a restart after a partially applied climb re-derives the same
// structure rather than building a second copy.

#include <array>
#include <cstdint>

#include "ds_async.hpp"
#include "ds_batch.hpp"
#include "ds_defs.hpp"
#include "ds_insert.hpp"
#include "ds_node.hpp"
#include "ds_put.hpp"
#include "ds_traverse.hpp"
#include "ds_ts.hpp"

namespace ds {

/// What this put currently has outstanding.
enum class PutStep : uint8_t {
  Idle,
  Traversing,
  AwaitHeader,       ///< fetching the target node's header
  AwaitVec,          ///< fetching its vector
  AwaitSettle,       ///< helping an operation outstanding on it
  AwaitInsert,       ///< F1's single chained batch
  AwaitSplitStage,   ///< F2's stage-and-publish chain
  AwaitSplitFinish,  ///< F2's completion chain
  AwaitStamp,        ///< Faa mode only: writing the timestamp it claimed
  Done,
};

/// What the put is trying to achieve at its current target.
enum class PutPhase : uint8_t {
  DataInsert,     ///< height 0: the payload goes in the covering data node
  DataSplit,      ///< height >= 1: k becomes a data node's k_min
  ClimbSplit,     ///< levels 0 .. h-2: k becomes an index node's k_min
  TopInsert,      ///< level h-1: k is an ordinary entry
  CapacitySplit,  ///< a node was full; split it, then resume
  SeedInsert,     ///< a split was a boundary no-op, so apply its seed
};

template <class Ops, class Cache>
class PutOperation {
 public:
  PutOperation(Ops &ops, Cache &cache, uint32_t layers, PutStats &stats,
               WriteStats &wstats)
      : ops_(ops),
        cache_(cache),
        layers_(layers),
        stats_(stats),
        wstats_(wstats),
        trav_(ops) {}

  size_t start(Key k, Value v, uint32_t height) {
    k_ = k;
    v_ = v;
    height_ = height > layers_ ? layers_ : height;
    out_ = PutResult{};
    out_.height = height_;
    index_ = {};
    attempts_ = 0;
    fetches_ = 0;

    // ── Ask the cache where to write ──────────────────────────────────────
    //
    // A height-0 put needs exactly one thing: the data node covering k. That
    // is precisely what locateData returns, so a hit skips the whole traversal
    // and the put costs one node fetch plus one chained batch.
    //
    // This was missing, and measurably so. The parallelism sweep put the
    // cache's benefit at 4.7-6.3x on a read-only workload and only 1.3-1.5x on
    // 50/50 -- because the write half never asked. Roughly seven puts in eight
    // are height 0, so this is most of that gap.
    //
    // A BAD HINT IS DETECTED, NOT TRUSTED. actInsert checks covers() before
    // touching anything and returns to a traversal when the node does not own
    // k, which is the same C4 contract the read path uses: a stale hint costs a
    // round trip, never a wrong write.
    //
    // Height >= 1 still traverses, deliberately. Its climb needs the covering
    // node at every level, and while the cache can supply those (gather_prevs,
    // interface doc §5) that is the path which BUILDS the index -- a hint that
    // is merely coarse there would be rejected by the split's range check and
    // cost a retry, so the saving is small and the surface to get wrong is the
    // structural one. Worth measuring separately before adopting.
    if (height_ == 0) {
      RemoteAddr const hinted = cache_.locateData(k);
      if (!hinted.isNull()) {
        ++stats_.hinted_writes;
        phase_ = PutPhase::DataInsert;
        target_ = hinted;
        data_addr_ = hinted;
        from_hint_ = true;
        return fetch();
      }
      ++stats_.hint_misses;
    }
    return beginTraversal();
  }

  size_t step() {
    switch (step_) {
      case PutStep::Traversing:       return onTraversal();
      case PutStep::AwaitHeader:      return onHeader();
      case PutStep::AwaitVec:         return onVec();
      case PutStep::AwaitSettle:      return fetch();  // re-read after helping
      case PutStep::AwaitInsert:      return onInsert();
      case PutStep::AwaitSplitStage:  return onSplitStage();
      case PutStep::AwaitSplitFinish: return onSplitFinish();
      case PutStep::AwaitStamp:       return onStamp();
      case PutStep::Idle:
      case PutStep::Done:
        break;
    }
    return 0;
  }

  [[nodiscard]] bool finished() const { return step_ == PutStep::Done; }
  [[nodiscard]] PutResult const &result() const { return out_; }

 private:
  size_t done(bool resolved) {
    if (!resolved) ++stats_.failures;
    out_.resolved = resolved;
    step_ = PutStep::Done;
    return 0;
  }

  size_t beginTraversal() {
    from_hint_ = false;
    fetches_ = 0;   // a fresh attempt gets a fresh budget; the total is still
                    // bounded by kMaxPutAttempts * kMaxFetchesPerAttempt
    if (++attempts_ > static_cast<uint32_t>(detail::kMaxPutAttempts)) {
      return done(false);
    }
    step_ = PutStep::Traversing;
    ++stats_.traversals;
    return trav_.start(k_, layers_, path_);
  }

  size_t onTraversal() {
    size_t const await = trav_.step();
    if (!trav_.finished()) return await;

    TraversalResult const &r = trav_.result();
    stats_.nodes_read += r.nodes_read;
    stats_.vec_reads += r.vec_reads;
    stats_.right_hops += r.right_hops;
    stats_.helped += r.helped_ts + r.helped_splits;
    if (!r.ok()) {
      if (r.status == TraversalStatus::Miss) ++stats_.not_covered;
      return done(false);
    }

    data_addr_ = r.data_addr;
    if (height_ == 0) {
      phase_ = PutPhase::DataInsert;
      target_ = data_addr_;
    } else {
      // Height >= 1: the data node splits at k too, so k names a data node of
      // its own -- which is the address mirror_insert wants, a level-0 entry
      // pointing at a data node rather than another index node.
      phase_ = PutPhase::DataSplit;
      target_ = data_addr_;
      split_key_ = k_;
      seed_ = Entry{k_, v_};
      have_seed_ = true;
      level_ = 0;
    }
    return fetch();
  }

  // ── Fetch the target node, settling it if need be ─────────────────────────

  /// Re-read the target's header (and speculatively its vector).
  ///
  /// EVERY RETRY EDGE IN THIS CLASS GOES THROUGH HERE, which is why the bound
  /// lives here and not at the call sites. There are nine `return fetch()`
  /// edges: four are forward progress (a new target or phase), and the rest
  /// are retries -- a lost publishing CAS in actInsert and actSplit, and the
  /// re-poll in onHeader when no handle yet has majority support. Those three
  /// were UNBOUNDED. `attempts_`/kMaxPutAttempts only counts beginTraversal(),
  /// so a writer that kept losing its CAS looped here forever and was stopped
  /// only by SvFuture's 4096-step guard, which THROWS and takes the process
  /// down with it.
  ///
  /// That is not hypothetical: workload D (95/5 over `latest`, so the writes
  /// concentrate on a few hot keys) killed all 8 clients at 8 clients with
  /// "future 0 is stuck after 4097 steps". It survived at 4 clients, and it
  /// survived at 8 with the cache OFF -- because without a hint every attempt
  /// pays a full traversal first, and that latency was accidentally acting as
  /// backoff. Adding the write-path hint removed the backoff and made a
  /// pre-existing unbounded loop reachable.
  ///
  /// The blocking twin never had this problem: ds_insert.hpp wraps the same
  /// retry in `for (attempt < kMaxWriteAttempts)`. Converting that loop into a
  /// state machine turned the loop edge into `return fetch()` and dropped the
  /// counter. Guarding fetch() itself rather than the three retry sites is
  /// deliberate: the bug WAS a forgotten counter, so the fix should not rely
  /// on remembering one.
  ///
  /// On exhaustion, escalate to a full traversal rather than failing outright.
  /// A re-traversal is genuinely different work -- it re-derives the target
  /// from the head, so it recovers from a stale hint or a node that split away
  /// -- and it counts against kMaxPutAttempts, so the total is bounded at
  /// kMaxPutAttempts * kMaxFetchesPerAttempt and the operation ends in
  /// done(false) rather than a throw.
  size_t fetch() {
    if (++fetches_ > static_cast<uint32_t>(detail::kMaxFetchesPerAttempt)) {
      ++wstats_.retry_exhausted;
      return beginTraversal();
    }
    step_ = PutStep::AwaitHeader;
    return ops_.postHeaders(target_, ops_.guess(target_));
  }

  size_t onHeader() {
    bool have_vec = false;
    if (!ops_.resolveHeaders(target_, node_, have_vec, vec_)) {
      return fetch();  // no majority-supported handle yet; re-poll
    }
    ++wstats_.nodes_read;
    if (have_vec) {
      ++wstats_.vec_reads;
      return afterFetch();
    }
    step_ = PutStep::AwaitVec;
    return ops_.postVec(node_.handle.offset());
  }

  size_t onVec() {
    if (!ops_.resolveVec(vec_)) return done(false);
    ++wstats_.vec_reads;
    return afterFetch();
  }

  size_t afterFetch() {
    // A write must find the node STABLE at the handle it is about to CAS from,
    // not merely readable. A stable node has had any split descriptor
    // propagated into its header, which is what makes it safe for the new
    // version to carry none -- publishing a descriptor-free version over an
    // open window would strand the old bound in the header.
    bool const pending = vec_.isPending();
    bool const unstable = !node_.isStable();
    if (pending || unstable) {
      Batch b;
      if (pending) {
        b.casTs(node_.handle.offset(), kNullTs, ops_.now());
        ++wstats_.helped_ts;
      }
      if (unstable && vec_.hasSplitDescriptor()) {
        if (node_.next_k_min != vec_.k_min_next) {
          b.casNextKMin(target_, node_.next_k_min, vec_.k_min_next);
        }
        if (node_.next_id != vec_.next_id) {
          b.casNextId(target_, node_.next_id, vec_.next_id);
        }
      }
      if (unstable) {
        b.casTailWord(target_,
                      packTailWord(node_.level, node_.tail_struct_ver),
                      packTailWord(node_.level, node_.handle.structVer()));
        ++wstats_.helped_splits;
      }
      step_ = PutStep::AwaitSettle;
      return ops_.postBatch(b);
    }
    return act();
  }

  // ── Act on a settled node ─────────────────────────────────────────────────

  size_t act() {
    switch (phase_) {
      case PutPhase::DataInsert:
      case PutPhase::TopInsert:
      case PutPhase::SeedInsert:
        return actInsert();
      case PutPhase::DataSplit:
      case PutPhase::ClimbSplit:
      case PutPhase::CapacitySplit:
        return actSplit();
    }
    return done(false);
  }

  /// F1: stage a new version with the entry applied, publish it, stamp it.
  size_t actInsert() {
    Key const key = insertKey();
    Value const val = insertVal();

    if (!covers(node_, vec_, key)) {
      // The target no longer owns this key -- it split under us, or the cache's
      // hint was stale. A detected bad address, not a wrong write. Counted
      // separately from a hint that was simply absent, because the two say
      // different things: misses mean a cold cache, rejections mean a stale one.
      ++wstats_.not_covered;
      if (from_hint_) ++stats_.hint_rejected;
      return beginTraversal();
    }

    int const idx = findLte(vec_, key);
    bool const present = idx >= 0 && vec_.e[idx].key == key;
    if (!present && vec_.size >= kNodeCapacity) {
      // Full. Split at the median so both halves keep room, then come back.
      // A capacity split is a SEPARATE mechanism from a height-driven one: it
      // produces an orphan with no parent rather than a parented boundary.
      if (vec_.size < 2) return done(false);
      resume_phase_ = phase_;
      phase_ = PutPhase::CapacitySplit;
      split_key_ = vec_.e[vec_.size / 2].key;
      have_seed_ = false;
      return actSplit();
    }

    staged_ = vec_;
    if (present) {
      staged_.e[idx].val = val;
    } else {
      uint32_t const at = static_cast<uint32_t>(idx + 1);
      for (uint32_t i = staged_.size; i > at; --i) {
        staged_.e[i] = staged_.e[i - 1];
      }
      staged_.e[at] = Entry{key, val};
      ++staged_.size;
    }

    VecOffset const off = ops_.allocVec();
    if (off == kNullVec) {
      ++wstats_.exhausted;
      return done(false);
    }
    staged_.struct_ver = node_.handle.structVer();
    staged_.content_ver = node_.handle.contentVer() + 1;
    staged_.ts = kNullTs;
    staged_.old_ver = node_.handle.offset();
    staged_.next_id = kNullId;
    staged_.k_min_next = kReservedKey;
    staged_.is_orphan = vec_.is_orphan;

    batch_ = Batch{};
    batch_.writeVec(off, staged_);
    batch_.casHandle(target_, node_.handle.raw,
                     node_.handle.withContent(off).raw);
    // See ds_ts.hpp: Faa claims its value after the publish and writes it in a
    // second round trip; the local modes fold it into this chain, guarded
    // against the version being superseded so the chain cannot invert.
    if (ops_.tsMode() == TsMode::Faa) {
      batch_.faaTs();
    } else {
      batch_.casTs(off, kNullTs,
                   stampFor(ops_.tsMode(), ops_.now(), vec_.ts));
    }
    pred_ts_ = vec_.ts;
    staged_off_ = off;
    was_present_ = present;
    step_ = PutStep::AwaitInsert;
    return ops_.postBatch(batch_);
  }

  size_t onInsert() {
    BatchResult const r = ops_.resolveBatch(batch_);
    if (!r.submitted) return done(false);
    ++wstats_.vec_writes;
    ++wstats_.batches;
    if (!r.committed) {
      // Somebody published first, so the staged version describes a state that
      // never existed. Its offset is abandoned; there is no reclamation.
      ++wstats_.cas_lost;
      ++wstats_.retries;
      return fetch();
    }
    if (ops_.tsMode() == TsMode::Faa) {
      // Visible but unstamped. Fix it before the operation is called complete,
      // so "the write finished" includes its timestamp and a reader starting
      // afterwards cannot be ordered before it.
      Batch stamp;
      // Raw counter value in Faa mode -- see stampFor() in ds_ts.hpp.
      stamp.casTs(staged_off_, kNullTs,
                  stampFor(ops_.tsMode(), r.ts, pred_ts_));
      if (!ops_.postBatch(stamp)) return done(false);
      ++wstats_.faa_stamps;
      ++wstats_.batches;
      step_ = PutStep::AwaitStamp;
      return 1;  // the stamp batch's completions
    }
    ++wstats_.published;
    if (was_present_) ++wstats_.updates; else ++wstats_.creates;
    return advance();
  }

  /// F2: stage the created node and both vectors, then publish.
  size_t actSplit() {
    if (split_key_ <= node_.k_min) {
      // Already a boundary. Idempotent, which is what makes a restart safe --
      // but the seed still has to be applied, or a repeated height-driven put
      // reports success and never writes the value.
      ++wstats_.boundary_noops;
      created_ = target_;
      if (have_seed_) {
        resume_phase_ = phase_;
        phase_ = PutPhase::SeedInsert;
        return actInsert();
      }
      return advance();
    }
    if (split_key_ >= rangeEnd(node_, vec_)) {
      ++wstats_.retries;
      return beginTraversal();
    }

    uint32_t keep = 0;
    while (keep < vec_.size && vec_.e[keep].key < split_key_) ++keep;

    created_ = ops_.allocNode();
    VecOffset const cvec_off = ops_.allocVec();
    VecOffset const nvec_off = ops_.allocVec();
    if (created_.isNull() || cvec_off == kNullVec || nvec_off == kNullVec) {
      ++wstats_.exhausted;
      return done(false);
    }
    created_vec_ = cvec_off;
    new_vec_ = nvec_off;

    bool const orphan = phase_ == PutPhase::CapacitySplit;

    initNode(cnode_, split_key_, node_.level, cvec_off);
    cnode_.next_id = nextNode(node_, vec_).id;
    cnode_.next_k_min = rangeEnd(node_, vec_);

    initVec(cvec_, orphan, kNullTs);
    for (uint32_t i = keep; i < vec_.size; ++i) cvec_.e[cvec_.size++] = vec_.e[i];
    if (have_seed_ && !insertSorted(cvec_, seed_)) return done(false);

    nvec_ = vec_;
    nvec_.size = keep;
    for (uint32_t i = keep; i < kNodeCapacity; ++i) nvec_.e[i] = Entry{};
    nvec_.struct_ver = node_.handle.structVer() + 1;
    nvec_.content_ver = node_.handle.contentVer();
    nvec_.ts = kNullTs;
    nvec_.old_ver = node_.handle.offset();
    nvec_.next_id = created_.id;
    nvec_.k_min_next = split_key_;
    nvec_.is_orphan = vec_.is_orphan;

    batch_ = Batch{};
    batch_.writeVec(cvec_off, cvec_);
    batch_.writeNode(created_, cnode_);
    batch_.writeVec(nvec_off, nvec_);
    batch_.casHandle(target_, node_.handle.raw,
                     node_.handle.withStruct(nvec_off).raw);
    if (ops_.tsMode() == TsMode::Faa) batch_.faaTs();
    pred_ts_ = vec_.ts;
    pre_split_ = node_;
    split_orphan_ = orphan;
    step_ = PutStep::AwaitSplitStage;
    return ops_.postBatch(batch_);
  }

  size_t onSplitStage() {
    BatchResult const r = ops_.resolveBatch(batch_);
    if (!r.submitted) return done(false);
    wstats_.vec_writes += 2;
    ++wstats_.node_writes;
    ++wstats_.batches;
    if (!r.committed) {
      ++wstats_.cas_lost;
      ++wstats_.retries;
      return fetch();
    }

    // The completion chain. Order is the protocol's: timestamps and the two
    // header fields in any order, the tail word LAST -- which same-QP ordering
    // now enforces rather than leaving it to the order of these calls.
    // Both vectors get the SAME timestamp: the split published them together,
    // so distinct stamps would claim an order between two versions that became
    // visible at once. Guarded against the version being superseded, so the
    // chain cannot invert whatever the clock does.
    uint64_t const ts = stampFor(
        ops_.tsMode(), ops_.tsMode() == TsMode::Faa ? r.ts : ops_.now(),
        pred_ts_);
    finish_ = Batch{};
    finish_.casTs(new_vec_, kNullTs, ts);
    finish_.casTs(created_vec_, kNullTs, ts);
    finish_.casNextKMin(target_, pre_split_.next_k_min, split_key_);
    finish_.casNextId(target_, pre_split_.next_id, created_.id);
    finish_.casTailWord(
        target_, packTailWord(pre_split_.level, pre_split_.tail_struct_ver),
        packTailWord(pre_split_.level, pre_split_.handle.structVer() + 1));
    step_ = PutStep::AwaitSplitFinish;
    return ops_.postBatch(finish_);
  }

  size_t onSplitFinish() {
    (void)ops_.resolveBatch(finish_);
    ++wstats_.batches;
    ++wstats_.splits;
    if (split_orphan_) ++wstats_.capacity_splits;

    if (phase_ == PutPhase::CapacitySplit) {
      // Route by the split key rather than re-traversing: we know exactly
      // where the boundary fell.
      phase_ = resume_phase_;
      if (insertKey() >= split_key_) target_ = created_;
      ++wstats_.retries;
      return fetch();
    }
    if (have_seed_) {
      // The seed went into the created node, so the entry is already in place.
      return advance();
    }
    return advance();
  }

  /// Faa mode only: the claimed timestamp has been written, so F1 is complete.
  size_t onStamp() {
    ++wstats_.published;
    if (was_present_) ++wstats_.updates; else ++wstats_.creates;
    return advance();
  }

  // ── Move to the next thing the put has to do ──────────────────────────────

  size_t advance() {
    switch (phase_) {
      case PutPhase::DataInsert:
        ++stats_.puts;
        ++stats_.height0;
        out_.data_addr = target_;
        return done(true);

      case PutPhase::SeedInsert:
        phase_ = resume_phase_;
        return advance();

      case PutPhase::DataSplit:
        if (created_ != data_addr_) ++stats_.data_splits;
        created_data_ = created_;
        down_ = created_;
        level_ = 0;
        return nextClimbOrTop();

      case PutPhase::ClimbSplit:
        if (created_ != path_[level_].addr) ++stats_.index_splits;
        index_[level_] = created_;
        down_ = created_;
        ++level_;
        return nextClimbOrTop();

      case PutPhase::TopInsert: {
        // A3: the top slot carries the orphan a capacity split produced, or
        // null if there was none.
        index_[height_ - 1] = top_orphan_;
        if (!top_orphan_.isNull()) ++stats_.top_orphans;
        cache_.mirrorInsert(k_, height_, created_data_, index_);
        ++stats_.mirror_calls;
        ++stats_.puts;
        ++stats_.structural;
        out_.data_addr = created_data_;
        return done(true);
      }

      case PutPhase::CapacitySplit:
        return done(false);  // handled in onSplitFinish
    }
    return done(false);
  }

  size_t nextClimbOrTop() {
    if (level_ + 2 <= height_) {
      phase_ = PutPhase::ClimbSplit;
      target_ = path_[level_].addr;
      split_key_ = k_;
      seed_ = Entry{k_, down_.id};
      have_seed_ = true;
      return fetch();
    }
    phase_ = PutPhase::TopInsert;
    target_ = path_[height_ - 1].addr;
    have_seed_ = false;
    top_orphan_ = RemoteAddr{};
    return fetch();
  }

  Key insertKey() const {
    return phase_ == PutPhase::SeedInsert ? seed_.key : k_;
  }
  Value insertVal() const {
    if (phase_ == PutPhase::SeedInsert) return seed_.val;
    if (phase_ == PutPhase::TopInsert) return down_.id;
    return v_;
  }

  static bool insertSorted(VecRecord &v, Entry e) {
    if (v.size >= kNodeCapacity) return false;
    uint32_t at = 0;
    while (at < v.size && v.e[at].key < e.key) ++at;
    if (at < v.size && v.e[at].key == e.key) {
      v.e[at].val = e.val;
      return true;
    }
    for (uint32_t i = v.size; i > at; --i) v.e[i] = v.e[i - 1];
    v.e[at] = e;
    ++v.size;
    return true;
  }

  Ops &ops_;
  Cache &cache_;
  uint32_t layers_;
  PutStats &stats_;
  WriteStats &wstats_;
  TraversalFuture<Ops> trav_;

  Key k_ = 0;
  Value v_ = 0;
  uint32_t height_ = 0;
  PutStep step_ = PutStep::Idle;
  PutPhase phase_ = PutPhase::DataInsert;
  PutPhase resume_phase_ = PutPhase::DataInsert;
  uint32_t attempts_ = 0;  ///< unsigned: see the note in ds_async.hpp
  uint32_t fetches_ = 0;   ///< re-reads within the current attempt

  RemoteAddr target_{}, data_addr_{}, created_{}, created_data_{}, down_{};
  RemoteAddr top_orphan_{};
  uint32_t level_ = 0;  ///< unsigned: see the note in ds_async.hpp
  Key split_key_ = 0;
  Entry seed_{};
  bool have_seed_ = false;
  bool split_orphan_ = false;
  bool was_present_ = false;
  bool from_hint_ = false;  ///< this attempt's target came from the cache
  uint64_t pred_ts_ = kNullTs;      ///< ts of the version being superseded
  VecOffset staged_off_ = kNullVec; ///< the offset awaiting its stamp

  NodeRecord node_{}, cnode_{}, pre_split_{};
  VecRecord vec_{}, staged_{}, cvec_{}, nvec_{};
  VecOffset created_vec_ = kNullVec, new_vec_ = kNullVec;
  Batch batch_, finish_;

  PutResult out_{};
  PathStep path_[kMaxLayers]{};
  std::array<RemoteAddr, kMaxLayers> index_{};
};

}  // namespace ds
