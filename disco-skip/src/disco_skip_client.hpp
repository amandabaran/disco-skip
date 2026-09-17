#pragma once

#include <array>
#include <algorithm>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

#include <dory/conn/rc-exchanger.hpp>
#include <dory/extern/ibverbs.hpp>

#include "disco_skip_state.hpp"
#include "ds_futures.hpp"
#include "range_future.hpp"

namespace ds {

// The skip-vector client.
//
// `futures` are skip-vector operations -- SvFuture over GetOperation and
// PutOperation. The register GetFuture/PutFuture this used to hold are retired:
// they operated on a flat array of registers, which is not the data structure
// any more.
//
// RangeFuture stays, and is still the register path. It is the ONLY scan
// capability in the tree -- there is no skip-vector range, A10 being deferred --
// so retiring it would make workload E unrunnable rather than merely
// unrepresentative. It should go when A10 lands.
class DsClient {
private:
    DsState state;
    QuorumStats qstats;
    GetStats gstats;
    PutStats pstats;
    WriteStats wstats;
    RangeStats rstats;
#if DS_CACHE_ENABLED
    ClientCache cache;
#else
    ClientCache cache;
#endif
    std::vector<SvFuture<ClientCache>> futures;
    std::vector<bool> progress;
    std::vector<RangeFuture> range_futures;
    std::vector<bool> range_progress;
public:
    DsClient(Layout layout,
              dory::conn::RcConnectionExchanger<ProcId>& rcx,
              ProcId proc_id)
    : state{layout, rcx, proc_id}
#if DS_CACHE_ENABLED
    , cache{state.cache_sv,
            static_cast<uint32_t>(state.layout.cache_layers),
            state.layout.consult_cache}
#else
    , cache{}
#endif
    {
        progress.resize(state.layout.async_parallelism, false);
        futures.reserve(state.layout.async_parallelism);

        range_progress.resize(state.layout.async_parallelism, false);
        range_futures.reserve(state.layout.async_parallelism);

        for (size_t i = 0; i < state.layout.async_parallelism; ++i) {
            futures.emplace_back(state, i, cache, qstats, gstats, pstats,
                                 wstats, rstats);
            range_futures.emplace_back(state, i);
        }
    }

    // Drain the send CQs across all server connections. Routes each completion
    // to its owning future via wr_id, and flips `progress[i]` so the user loop
    // knows to call tryStepForward on it.
    // Where the last getFreeFuture search stopped; see the note there.
    uint64_t next_future_ = 0;

    // Completion scratch, allocated once. See the note in tickRdma.
    static constexpr size_t kWcBufMax = 256;
    std::array<struct ibv_wc, kWcBufMax> wc_buf_{};

    bool tickRdma() {
        bool any_progress = false;
        for (size_t s = 0; s < state.layout.num_servers; ++s) {
            auto& rc = *state.server_conns[s];
            auto& tp = state.to_poll_per_server[s];
            if (tp <= 0) continue;

            // Poll at most as many completions as we actually have outstanding
            // on this server. The bound matters: polling a blind 128 lets one
            // call reap more CQEs than `tp` accounted for, the decrement below
            // then drives `tp` negative, and `tp == 0` is never true again so
            // this server stops being polled.
            //
            // INTO A FIXED BUFFER, NOT A RESIZED VECTOR. The vector form uses
            // size() as both the input capacity and the output count, so it
            // shrinks to the number polled and has to be regrown before the
            // next tick -- and std::vector::resize value-initialises, zeroing
            // 48-byte ibv_wc structs on every poll of every server. perf put
            // _M_default_append at 9.6% of this client's CPU on workload E.
            int const want = tp < static_cast<int64_t>(kWcBufMax)
                                 ? static_cast<int>(tp)
                                 : static_cast<int>(kWcBufMax);
            int polled = 0;
            if (!rc.pollCqIsOk(dory::conn::ReliableConnection::SendCq,
                               wc_buf_.data(), want, polled)) {
                throw std::runtime_error("Error polling CQ");
            }

            // Loop strictly over entries actually reaped
            for (int wi = 0; wi < polled; ++wi) {
                auto const& wc = wc_buf_[static_cast<size_t>(wi)];
                if (wc.status != IBV_WC_SUCCESS) {
                    throw std::runtime_error("WC unsuccessful");
                }
                
                uint64_t raw_id = wc.wr_id;

                // A completion whose id is not a future's is a routing bug, and
                // it has to be caught here: the id is used as an index, so the
                // alternative is an out_of_range from inside a vector with a
                // number like 9223372036854775807 in it and nothing to say what
                // posted it. That is exactly what happened when postBatchChain
                // hardcoded kBlockingWrId -- every async batch completion
                // routed to range_futures.at(2^63 - 1).
                //
                // kBlockingWrId is all ones, so it survives the top-bit strip
                // as INT64_MAX and cannot be a legal future index.
                if (raw_id == kBlockingWrId) {
                    throw std::runtime_error(
                        "a blocking helper's completion reached the future "
                        "driver: something on the async path posted with "
                        "kBlockingWrId instead of its future id, so its "
                        "completions cannot be routed");
                }

                bool is_range_op = (raw_id >> 63) != 0;
                uint64_t clean_id = raw_id & ~(1ULL << 63);
                uint64_t const slots = state.layout.async_parallelism;
                if (clean_id >= slots) {
                    throw std::runtime_error(
                        "completion for future " + std::to_string(clean_id) +
                        " but only " + std::to_string(slots) +
                        " slots exist: a work-request id was not a future id");
                }

                if (is_range_op) {
                    range_futures.at(clean_id).addToOngoingRDMA(s, -1);
                    range_progress.at(clean_id) = true;
                } else {
                    futures.at(clean_id).addToOngoingRDMA(s, -1);
                    progress.at(clean_id) = true;
                }
                any_progress = true;
            }
            
            // Decrement by what was actually reaped.
            tp -= static_cast<int64_t>(polled);
        }
        return any_progress;
    }

    // Returns the first future that is done.  Spins on tickRdma() until one is.
    //
    // This is what bounds how many operations are in flight: it hands back a
    // slot only when that slot's operation has completed, so with
    // async_parallelism slots there are at most that many outstanding. The
    // caller must NOT drain between operations -- see the note on
    // finishAllFutures.
    SvFuture<ClientCache>& getFreeFuture() {
        // ROUND-ROBIN, not restart-from-zero.
        //
        // perf put this at 13.0% of client CPU on workload E, with tickRdma at
        // a further 12.3%. Most of that is the WAIT itself: when every future is
        // awaiting completions the loop spins, and spinning is how an RDMA
        // client waits -- there is nothing else for it to do. So this is not a
        // 13% saving, and the real reductions are elsewhere (the completion
        // buffer, and chaining work requests so fewer verbs calls are made).
        //
        // What it does remove is the redundant re-scan: starting at 0 every
        // iteration re-tests the same busy futures before reaching the one that
        // finished. Resuming where the last search stopped checks the likely
        // candidate first.
        uint64_t const n = state.layout.async_parallelism;
        while (true) {
            for (uint64_t k = 0; k < n; ++k) {
                uint64_t const i = next_future_ % n;
                next_future_ = i + 1;
                auto& f = futures[i];
                if (progress[i]) {
                    f.tryStepForward();
                    progress[i] = false;
                }
                if (f.isDone()) return f;
            }
            tickRdma();
        }
    }

    RangeFuture& getFreeRangeFuture() {
        while (true) {
            for (uint64_t i = 0; i < state.layout.async_parallelism; ++i) {
                auto& f = range_futures[i];
                if (range_progress[i]) {
                    f.tryStepForward();
                    range_progress[i] = false;
                }
                if (f.isDone()) return f;
            }
            tickRdma();
        }
    }

    // Returns a specific future by index (used for round-robin or hashing).
    SvFuture<ClientCache>& getFuture(uint64_t i) {
        return futures.at(i % state.layout.async_parallelism);
    }

    [[nodiscard]] QuorumStats const& quorumStats() const { return qstats; }
    [[nodiscard]] GetStats const& getStats() const { return gstats; }
    [[nodiscard]] PutStats const& putStats() const { return pstats; }
    [[nodiscard]] WriteStats const& writeStats() const { return wstats; }

    // Wait for all futures to drain.
    //
    // CALL THIS AT MEASUREMENT BOUNDARIES AND AT THE END, NOT BETWEEN
    // OPERATIONS. Calling it per operation is what the benchmark loop used to
    // do, and it makes the client synchronous: every operation is drained
    // before the next is issued, so exactly one is ever in flight and
    // async_parallelism has no effect whatsoever. Pipelining is the whole point
    // of this machinery, and one misplaced drain removes it silently -- the
    // throughput is simply lower, with nothing to indicate why.
    void finishAllFutures() {
        for (size_t idx = 0; idx < state.layout.async_parallelism; ++idx) {
            while (!futures[idx].isDone() || !range_futures[idx].isDone()) {
                tickRdma();
                if (progress[idx]) {
                    futures[idx].tryStepForward();
                    progress[idx] = false;
                }
                if (range_progress[idx]) {
                    range_futures[idx].tryStepForward();
                    range_progress[idx] = false;
                }
            }
        }
    }

    DsState& getState() { return state; }

    void reportStats(bool detailed = false) {
        state.reportStats(detailed);

        // THE OPERATION-LEVEL COUNTERS WERE NEVER PRINTED. GetStats, PutStats
        // and WriteStats are accumulated by every future and then went nowhere:
        // reportStats only showed DsState's own RDMA counters. So a put that
        // gave up -- done(false) increments PutStats::failures -- was completely
        // invisible, and worse, the benchmark loop does not check `resolved`,
        // so the operation still counted toward throughput. A run could report
        // a healthy number while silently resolving nothing.
        //
        // That mattered the moment the unbounded retry in ds_put_future.hpp was
        // bounded: without this, the fix would have converted a loud crash into
        // a quiet wrong number, which is the worse failure.
        fmt::print("\n################ Operations:\n");
        fmt::print("gets:         {} ok, {} not-found, {} FAILED\n",
                   gstats.cache_hits + gstats.traversals, gstats.not_found,
                   gstats.failures);
        fmt::print("              {} cache hits, {} misses, {} kmin mismatch\n",
                   gstats.cache_hits, gstats.cache_misses, gstats.kmin_mismatch);
        // THE CACHE'S HIT RATE IS NOT THE INTERESTING NUMBER, and the line
        // above does not make that obvious. On workload A the directory almost
        // never MISSES -- 67 times in 3.87 M gets -- it names a STALE node in
        // one get in five. So the cost is the mismatch rate multiplied by what
        // a traversal costs, and until now the second factor was unprintable:
        // `round trips` below is pooled over gets, puts and ranges.
        //
        // `traversals` was also never shown, though it is recoverable two ways
        // that agree exactly -- ok minus cache_hits, and mismatch plus misses.
        // Verified equal on 16 of 16 clients. Shown now so it need not be
        // reconstructed.
        {
            uint64_t const consulted = gstats.cache_hits + gstats.kmin_mismatch;
            fmt::print("              {} traversals ({:.1f}% of gets), "
                       "{} reconciles\n",
                       gstats.traversals,
                       100.0 * static_cast<double>(gstats.traversals) /
                           static_cast<double>(std::max<uint64_t>(
                               1, gstats.cache_hits + gstats.traversals +
                                      gstats.not_found)),
                       gstats.reconciles);
            fmt::print("              round trips: {} on the hint path "
                       "({:.2f}/consult), {} in traversals "
                       "({:.2f}/traversal)\n",
                       gstats.rt_hint,
                       static_cast<double>(gstats.rt_hint) /
                           static_cast<double>(std::max<uint64_t>(1, consulted)),
                       gstats.rt_traverse,
                       static_cast<double>(gstats.rt_traverse) /
                           static_cast<double>(
                               std::max<uint64_t>(1, gstats.traversals)));
            // What the staleness actually costs, in the one unit that matters.
            // A correct hit and a mismatch are not the same operation: the
            // mismatch pays the hint path AND the traversal.
            if (gstats.traversals != 0 && gstats.cache_hits != 0) {
                double const hit_rt =
                    static_cast<double>(gstats.rt_hint) /
                    static_cast<double>(std::max<uint64_t>(1, consulted));
                double const trav_rt =
                    static_cast<double>(gstats.rt_traverse) /
                    static_cast<double>(gstats.traversals);
                fmt::print("              so a stale hint costs ~{:.2f} extra "
                           "round trips vs ~{:.2f} for a correct one\n",
                           trav_rt, hit_rt);
            }
            fmt::print("              {} node reads, {} vector reads, "
                       "{} right hops (RECORDS, not trips)\n",
                       gstats.nodes_read, gstats.vec_reads, gstats.right_hops);
            // STALENESS PER OPERATION, which is the rate the hop experiment is
            // actually about. kmin_mismatch above counts DETECTIONS and is
            // inflated by every failed hop, so the two differ exactly when
            // --hint-hops is on -- and comparing arms on the detection count
            // is what made the previous run's staleness look 5x worse instead
            // of constant.
            if (gstats.hint_stale_ops != 0 || gstats.hops_taken != 0) {
                fmt::print("              stale hints: {} operations "
                           "({} detections)\n",
                           gstats.hint_stale_ops, gstats.kmin_mismatch);
            }
            if (gstats.hops_taken != 0) {
                fmt::print("              sideways hops: {} taken, {} "
                           "recovered ({:.1%}), {} budgets exhausted\n",
                           gstats.hops_taken, gstats.hops_recovered,
                           static_cast<double>(gstats.hops_recovered) /
                               static_cast<double>(
                                   std::max<uint64_t>(1, gstats.hops_taken)),
                           gstats.hops_exhausted);
                // THE BET, ARITHMETIC AND ALL. A recovery saves the descent a
                // traversal would have cost; every hop costs one trip whether
                // it works or not. Printed rather than reasoned about after
                // the fact, which is how the previous conclusion came to rest
                // on a hop-hit rate inferred from arm-to-arm differences.
                double const trav_rt =
                    static_cast<double>(gstats.rt_traverse) /
                    static_cast<double>(
                        std::max<uint64_t>(1, gstats.traversals));
                double const saved =
                    static_cast<double>(gstats.hops_recovered) * trav_rt;
                double const spent = static_cast<double>(gstats.hops_taken);
                fmt::print("              the bet: saved ~{:.0f} trips, spent "
                           "{:.0f} -> {}{:.2f} net per hop\n",
                           saved, spent,
                           saved >= spent ? "+" : "",
                           (saved - spent) /
                               static_cast<double>(
                                   std::max<uint64_t>(1, gstats.hops_taken)));
            }
        }
        if (gstats.failures != 0) {
            fmt::print("              gave up: {} no-majority, {} settle-stuck, "
                       "{} too-many-hops\n",
                       gstats.gave_up_no_majority, gstats.gave_up_settle_stuck,
                       gstats.gave_up_too_many_hops);
        }
        fmt::print("puts:         {} resolved, {} FAILED ({} height-0, {} structural)\n",
                   pstats.puts, pstats.failures, pstats.height0,
                   pstats.structural);
        fmt::print("              {} hinted, {} hint misses, {} hint rejected\n",
                   pstats.hinted_writes, pstats.hint_misses,
                   pstats.hint_rejected);
        if (rstats.ranges != 0) {
            fmt::print("ranges:       {} resolved, {} FAILED, {} hit the entry cap\n",
                       rstats.ranges - rstats.failures, rstats.failures,
                       rstats.capped);
            fmt::print("              {} entries, {} nodes walked, {} skipped "
                       "(newer than the snapshot)\n",
                       rstats.entries, rstats.nodes_walked,
                       rstats.nodes_skipped);
            // The old_ver hops are a range's distinctive cost -- one round trip
            // each, chasing a linked list in remote memory -- so they are
            // reported rather than folded into vec_reads.
            fmt::print("              {} old_ver hops, {} vector reads\n",
                       rstats.versions_walked, rstats.vec_reads);
            // A10: "Finding none is a result; not looking is a gap." So it is
            // printed either way -- a zero here is the result, and its absence
            // would mean nobody looked.
            fmt::print("              snapshot violations: {}{}\n",
                       rstats.snapshot_violations,
                       rstats.snapshot_violations == 0 ? " (none)" : "  *** ");
            // WHY A RANGE GAVE UP, not merely that one did. The walk has eight
            // done(false) exits and `failures` alone cannot tell them apart:
            // a hot scan run failed 4 of 1,119,888 ranges with 0 violations
            // and no refused FAA round, and there was no way to name the cause
            // without guessing. Printed only when something failed, so a clean
            // run does not gain a line of zeros.
            if (rstats.failures != 0) {
                uint64_t const attributed =
                    rstats.fail_no_majority + rstats.fail_settle +
                    rstats.fail_hops + rstats.fail_read +
                    rstats.fail_traverse + rstats.fail_snapshot +
                    rstats.snapshot_violations;
                fmt::print("              why: {} no majority, {} would not "
                           "settle, {} version hops, {} read, {} traverse, "
                           "{} snapshot, {} violation\n",
                           rstats.fail_no_majority, rstats.fail_settle,
                           rstats.fail_hops, rstats.fail_read,
                           rstats.fail_traverse, rstats.fail_snapshot,
                           rstats.snapshot_violations);
                // The buckets must account for every failure. An exit added
                // later without a counter would otherwise be invisible -- the
                // total would still be right and the breakdown would silently
                // under-report, which is the failure mode this whole block
                // exists to remove.
                if (attributed != rstats.failures) {
                    fmt::print("              *** {} of {} failures "
                               "UNATTRIBUTED -- a done(false) exit is missing "
                               "its counter\n",
                               rstats.failures - attributed, rstats.failures);
                }
            }
            if (rstats.cache_backbones != 0 || rstats.cache_misses != 0) {
                // What the cache actually bought, in the terms that settle it:
                // addresses per backbone is the batch width, and a miss is a
                // range (or a refill) that fell back to the serial chain. A
                // cache-walk arm whose misses dominate its backbones measured
                // the serial walk with extra lookups.
                fmt::print("              cache walk: {} backbones, {} addrs "
                           "({:.1f}/backbone), {} misses, {} k_min mismatches\n",
                           rstats.cache_backbones, rstats.cache_addrs,
                           rstats.cache_backbones == 0
                               ? 0.0
                               : static_cast<double>(rstats.cache_addrs) /
                                     static_cast<double>(rstats.cache_backbones),
                           rstats.cache_misses, rstats.cache_stale);
            }
            if (rstats.batches != 0) {
                // What the batched walk actually bought, in the only terms
                // that settle it: nodes fetched in parallel per batch, against
                // the serial detours it could not avoid. A fallback rate near
                // the orphan rate means the index bought little.
                fmt::print("              batched walk: {} batches, {} nodes/batch, "
                           "{} fallbacks, {} misses, {} orphan detours, "
                           "{} abandoned\n",
                           rstats.batches,
                           rstats.batches == 0
                               ? 0.0
                               : static_cast<double>(rstats.nodes_walked) /
                                     static_cast<double>(rstats.batches),
                           rstats.batch_fallbacks, rstats.batch_misses,
                           rstats.orphans_walked, rstats.batch_abandoned);
            }
        }
        // Orphans decide whether an index-batched range walk is sound: a
        // capacity split produces a node with NO PARENT, reachable only along
        // the next_id chain, so a walk that took its node list from the index
        // alone would silently skip one. Counted here because the design
        // question is "do these occur in practice", not "can they".
        fmt::print("splits:       {} height-driven, {} capacity (ORPHANS), "
                   "{} boundary no-ops\n",
                   wstats.splits, wstats.capacity_splits,
                   wstats.boundary_noops);
        fmt::print("writes:       {} published, {} cas lost, {} retries, "
                   "{} retry-budget exhausted\n",
                   wstats.published, wstats.cas_lost, wstats.retries,
                   wstats.retry_exhausted);
        fmt::print("quorum:       {} node reads -> {} replica reads, {} re-polls\n",
                   qstats.node_reads, qstats.replica_reads,
                   qstats.read_retries);
        // ts_partial: FAA ROUNDS THAT DID NOT REACH EVERY REPLICA.
        //
        // This was counted and never printed, which meant no run in this repo
        // could substantiate its own linearizability claim. Faa's real-time
        // order between non-overlapping writers holds ONLY if the writer FAA'd
        // all replicas (ds_ts.hpp): quorum intersection guarantees a later
        // writer sees some replica the earlier one touched, but not the one
        // that DECIDED its maximum, so a partial round can hand a later write a
        // smaller timestamp. A partial FAA still gives a unique stamp that is
        // monotone for its own writer -- it loses only the cross-writer
        // guarantee -- so it degrades rather than corrupts, and it must be
        // visible to be reported honestly.
        //
        // NON-ZERO HERE MEANS THE FAA LINEARIZABILITY CLAIM IS WEAKER THAN
        // STATED FOR THAT RUN, and the figure needs the caveat.
        fmt::print("              {} partial FAA rounds (did NOT reach all "
                   "replicas){}\n",
                   qstats.ts_partial,
                   qstats.ts_partial == 0 ? " (none)" : "  *** real-time order "
                                                        "NOT guaranteed ***");
        // Below a quorum there is no ordering property at all, so these rounds
        // are REFUSED a timestamp and left pending rather than stamped. A
        // non-zero count means writes are completing without an order until a
        // reader settles them -- a liveness/latency story, not a correctness
        // one, but it must not be invisible.
        fmt::print("              {} FAA rounds refused (short of quorum){}\n",
                   qstats.ts_short_of_quorum,
                   qstats.ts_short_of_quorum == 0 ? " (none)" : "  ***");
        // HELPING, which is what makes reads and ranges lock-free: a reader
        // that finds a published-but-unstamped version fixes the timestamp
        // itself instead of waiting, so a writer stalled between its publish
        // and its stamp blocks nobody.
        //
        // Printed because it was invisible. These counters have always been
        // accumulated and never reported, which meant there was no way to tell
        // from a run whether the helping path executed at all -- and that is
        // exactly how a Faa-mode bug in three of the async helping sites
        // survived every sweep: they stamped a local clock reading instead of
        // claiming a counter value, and nothing in the output would have shown
        // it. Zero here means the path is untested by this run, not that it
        // works.
        fmt::print("              helped: {} on gets, {} on puts, {} on "
                   "ranges (pending versions settled for a stalled writer)\n",
                   gstats.helped, pstats.helped, rstats.helped);
        // SPECULATION, and specifically the L4 CHECK -- also never printed
        // before, which is how the cost of --spread-reads stayed hidden.
        //
        // The offset hint's own hit rate is reported above and answers a
        // DIFFERENT question: was the guessed OFFSET right. This one answers
        // whether the replica the speculation was issued to turned out to hold
        // the winning handle, because L4 accepts the bytes only then. On
        // workload C at 32 clients the offset hint read 0.987 with and without
        // spreading while server bytes out went 7.1 -> 10.5 GB: the whole
        // difference was here, and invisible.
        {
            uint64_t const sp = qstats.spec_hits + qstats.spec_misses;
            fmt::print("              speculation: {} hit / {} rejected by L4"
                       " ({:.3f}) -- a rejection wastes one vector read\n",
                       qstats.spec_hits, qstats.spec_misses,
                       sp == 0 ? 0.0
                               : static_cast<double>(qstats.spec_hits) /
                                     static_cast<double>(sp));
        }
        // ROUND TRIPS PER OPERATION -- the comparison that explains a slow
        // workload. A point get is 1-2; a scan-100 walks ~10.9 data nodes, so
        // it is one per node unless the batched or cache walk collapses them.
        // At a measured ~3.7 us per round trip, trips/op times 3.7 should
        // account for the operation's latency; where it does not, the cost is
        // NOT round trips and looking for it there is wasted effort.
        // THE DENOMINATOR IS THE WHOLE POINT OF THIS LINE, AND IT WAS WRONG.
        //
        // It used gstats.traversals, which is NOT the number of gets -- it
        // counts gets the cache could NOT answer, so it is a small fraction of
        // them. On workload C at -I 1200000 that was 101,914 against 2,450,000
        // actual gets, and the printed ratio came out 31.34 round trips per
        // "op" when the truth is 1.30. A ratio computed over 4% of the
        // operations is worse than no ratio, because it looks like a finding.
        //
        // A get is counted the same way the `gets:` line above counts it:
        // cache_hits + traversals, the hits plus the ones that went remote.
        // Ranges subtract failures to match the `ranges:` line, so a failed
        // range does not inflate the per-op cost of the ones that resolved.
        uint64_t const ops_resolved = (gstats.cache_hits + gstats.traversals) +
                                      pstats.puts +
                                      (rstats.ranges - rstats.failures);
        fmt::print("              {} round trips ({:.2f} per resolved op, "
                   "over {} gets+puts+ranges)\n",
                   qstats.round_trips,
                   ops_resolved == 0
                       ? 0.0
                       : static_cast<double>(qstats.round_trips) /
                             static_cast<double>(ops_resolved),
                   ops_resolved);
        fmt::print("              {} stale votes, {} tag ties, {} writebacks\n",
                   qstats.stale_votes, qstats.tag_ties, qstats.writebacks);
        // The L2 read repair. Printed unconditionally because "did the repair
        // fire?" was otherwise only inferable from failures going to zero
        // between two runs -- a correlation, not evidence.
        fmt::print("              {} L2 read repairs, {} reached majority\n",
                   qstats.repairs, qstats.repairs_landed);
        if (pstats.failures != 0 || gstats.failures != 0) {
            fmt::print("*** {} put and {} get operations DID NOT RESOLVE. The "
                       "throughput above counts them as completed, so it "
                       "OVERSTATES useful work. ***\n",
                       pstats.failures, gstats.failures);
        }
    }
};

} // namespace ds
