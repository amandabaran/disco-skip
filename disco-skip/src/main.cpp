#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <fstream>
#include <pthread.h>
#include <sched.h>
#include <thread>

#include <fmt/ostream.h>

#include "rdma_device.hpp"
#include "ds_log.hpp"
#include <iostream>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <vector>
#include <chrono>
#include <random>
#include <thread>
#include <cstring>
#include <string>
#include <optional>
#include <stdexcept>
#include <algorithm>
#include <functional>

// Dory headers
#include <lyra/lyra.hpp>
#include <dory/ctrl/block.hpp>
#include <dory/ctrl/device.hpp>
#include <dory/conn/rc.hpp>
#include <dory/memstore/store.hpp>
#include <dory/shared/match.hpp>

// Disco-skip headers
#include "ds.hpp"
#include "disco_skip_client.hpp"
#include "register.hpp"
#include "range_future.hpp"

using namespace dory;
using namespace dory::conn;

const uint64_t default_warmup     = 1'000'000;
const uint64_t default_iter_count = 1'000'000;
const uint64_t default_keepwarm   =   500'000;


enum OpType { OpGet, OpPut, OpScan, OpInsert };

struct YcsbOp {
    uint64_t target_reg;
    OpType type;
    uint64_t scan_len; // Only used if OpScan
};

// NOT A GLOBAL ANY MORE -- it is per client, and as a global it was a heap
// corruption waiting for a second thread. Each client fills its own operation
// list from its own YCSB subprocess, so two client threads push_back into the
// SAME vector concurrently: both reallocate, both free the old buffer, and the
// process dies with "double free or corruption (out)". That is exactly how the
// first two-thread run failed, and it failed the same way for both clients at
// the same line. Declared inside runClient instead; see there.

// A safe custom deleter struct that avoids compiler attribute mismatches
struct PipeDeleter {
    void operator()(FILE* fp) const {
        if (fp) pclose(fp);
    }
};

// Clean forward declarations to satisfy -Wmissing-declarations
std::unique_ptr<FILE, PipeDeleter> exec(const std::string& cmd);
size_t pseudo_hash(const std::string& str);
// Declared to match the definition below exactly. The previous version of this
// declaration had four parameters against the definition's six, so it declared
// an overload that never existed and left the real function undeclared --
// which is what -Wmissing-declarations was reporting.
void bootstrap_structure(ds::DsState& state);
int run_structure_selftest(ds::DsState& state);

/// The largest number of entries one scan may return, read from the YCSB
/// workload file rather than from a flag.
///
/// ── WHY THIS IS DERIVED AND NOT CONFIGURED ────────────────────────────────
///
/// max_range does two jobs and only one of them is a choice. It sizes the
/// registered bulk buffer -- Layout::bulkBufsSize() is
/// num_servers * max_range * sizeof(Register), and that memory is registered
/// before the workload is even parsed, so SOMETHING has to bound it up front.
/// But it is also a hard cap on every scan:
///
///     uint64_t len = op.scan_len;
///     if (len > layout.max_range) len = layout.max_range;
///
/// and the scan length is already a property of the workload file. So the flag
/// was redundant as a control and actively harmful as a default: it was 10, and
/// a scan-100 workload run without the flag returned 9.5 entries per range
/// instead of ~50.5. That is not a slow run, it is a DIFFERENT WORKLOAD, and
/// nothing in the output said so -- the only visible trace was entries per
/// range, which nobody reads unless they already suspect it.
///
/// It bit oops-workloade-hot exactly that way: built with maxscanlength=100 to
/// stress the snapshot walk, capped at 10, and the walk-coverage estimate came
/// out 15x short of prediction for that reason alone.
///
/// ── WHY GREP AND NOT YCSB ─────────────────────────────────────────────────
///
/// The operation stream is packed by shelling out to YCSB, which happens long
/// after the memory region is allocated. The workload file is on disk and is a
/// Java properties file, so the one line needed is readable directly and
/// cheaply at argument-parse time. Parsing more of it than this would be
/// re-implementing YCSB; one integer is not.
///
/// @return the file's maxscanlength, or 0 if the file has none (a workload with
///         no scans, where the buffer still needs a floor and the cap is never
///         consulted).
static uint64_t deriveMaxRange(std::string const &workload_path) {
    std::ifstream f(workload_path);
    if (!f) return 0;
    std::string line;
    while (std::getline(f, line)) {
        // Properties files allow whitespace and comments; take the first
        // uncommented maxscanlength= and stop.
        size_t const first = line.find_first_not_of(" \t");
        if (first == std::string::npos || line[first] == '#') continue;
        std::string const key = "maxscanlength";
        size_t const at = line.find(key, first);
        if (at != first) continue;
        size_t const eq = line.find('=', at);
        if (eq == std::string::npos) continue;
        try {
            long long const v = std::stoll(line.substr(eq + 1));
            if (v > 0) return static_cast<uint64_t>(v);
        } catch (...) {
            return 0;
        }
    }
    return 0;
}
void run_ml_prog_tracker_workload(
    ds::DsClient& client,
    uint64_t global_thread_id,
    uint64_t num_clients,
    uint64_t ops_to_run,
    uint64_t think_time_ms,
    dory::memstore::MemoryStore& store);

// ─── Bootstrap ──────────────────────────────────────────────────────────
//
// The memory servers run no logic, so nobody there can initialise the
// structure: one client writes it before anyone traverses. Zeroing the arena is
// not enough -- a zeroed node has next_k_min == 0 and so covers no key at all.
//
// Called only by the first client, and only before the "initialized" barrier
// that every process already waits on, so no new barrier is needed: any client
// that has passed that barrier is guaranteed to see the structure.
void bootstrap_structure(ds::DsState& state) {
    uint32_t const layers = static_cast<uint32_t>(state.layout.cache_layers);

    // A non-null bootstrap timestamp: the initial structure is settled, not
    // in-flight, so there is no operation for a reader to help complete. A null
    // ts would make every early reader try to resolve a version nobody is
    // writing.
    ds::InitialNode built[ds::kMaxLayers + 1];
    uint32_t const count =
        ds::buildInitialStructure(layers, ds::kBootstrapTs, built);

    // The HCA reads directly out of the source buffer, so it has to be MR
    // resident -- a stack array would not be addressable. Future 0's staging
    // slots are free at this point, since no future has started.
    ds::NodeRecord* stage_node = state.layout.getStageNode(0);
    ds::VecRecord* stage_vec = state.layout.getStageVec(0);

    for (uint32_t i = 0; i < count; ++i) {
        // Vector first, then the header that names it. A reader that catches
        // the header must never find its vector missing -- the same ordering F2
        // uses, contents before the pointer that publishes them.
        *stage_vec = built[i].vec;
        ds::writeVecAllReplicas(state.server_conns, state.layout, stage_vec,
                                built[i].vec_offset);
        *stage_node = built[i].node;
        ds::writeNodeAllReplicas(state.server_conns, stage_node, built[i].addr);
    }

    std::cout << "Bootstrap: wrote " << count << " nodes + " << count
              << " vectors (" << layers << " heads + 1 data node) to "
              << state.server_conns.size() << " replica(s)" << std::endl;
}

// ─── Structure selftest ─────────────────────────────────────────────────
//
// The body lives in src/ds_selftest.hpp, templated over the reader and the Ops
// surface. What stays here is the part that genuinely needs the cluster:
// building those two out of DsState. Everything else is checked off-cluster --
// rdma_compile.cc type-checks it against the real RDMA types and
// selftest_test.cc runs it against a fake arena.
//
// That split exists because this file cannot be compiled off-cluster at all, so
// code in it is unverified until a build. Six builds have now been lost that
// way, the last to a mistyped VerifyReport field.
/// Walk the structure after a WORKLOAD and check invariants.md §1.
///
/// WHY THIS EXISTS SEPARATELY FROM THE SELFTEST. run_structure_selftest is
/// invoked at 1 server and 1 client, and at one client a timestamp mode cannot
/// be caught ordering anything wrongly -- ds_ts.hpp says so directly: "At one
/// client that does not matter, because one clock orders its own writes
/// perfectly." So the existing verifier cannot detect the failure mode that
/// clock skew produces, which is an old_ver chain that does not decrease in ts
/// because two writers on DIFFERENT machines stamped out of order.
///
/// That needs concurrent cross-machine writes with contention -- which is a
/// workload, not a selftest -- followed by a check. Hence this.
///
/// IT MUST RUN QUIESCENT. verifyStructure is a sequential checker: a peer
/// still writing looks exactly like corruption, and the run would report
/// invariant violations that are really just concurrency. The caller runs it
/// after the "finished" rendezvous, when every client has stopped.
///
/// ONE CLIENT RUNS IT. The structure is shared, so a second checker re-reads
/// the whole thing to reach the same answer.
int verify_after_run(ds::DsState& state) {
    uint32_t const layers = static_cast<uint32_t>(state.layout.cache_layers);
    ds::RdmaReplicaSet<decltype(state.server_conns)> replicas(
        state.server_conns, state.layout, state.layout.getNodeBufs(0),
        state.layout.getVecBufs(0), state.layout.getCasBufs(0),
        state.layout.getStageNode(0), state.layout.getStageVec(0),
        &state.node_alloc, &state.vec_alloc, &state.vec_hint);
    replicas.setTsMode(state.layout.ts_mode);
    replicas.setClientIdx(state.client_idx);
    ds::QuorumStats qstats;
    ds::QuorumOps<decltype(replicas)> ops(replicas, qstats, nullptr);

    ds::clientOut() << "\n################ Post-workload verify ("
                    << ds::tsModeName(state.layout.ts_mode) << "):" << std::endl;
    ds::VerifyReport const rep = ds::verifyStructure(ops, layers);
    ds::clientOut() << "index nodes:  " << rep.nodes_visited << " ("
                    << rep.orphans << " orphans, " << rep.entries
                    << " entries)" << std::endl;
    ds::clientOut() << "data nodes:   " << rep.data_nodes << " ("
                    << rep.data_entries << " entries)" << std::endl;
    ds::clientOut() << "old versions: " << rep.old_versions
                    << "   <- the chains the ts check walks" << std::endl;
    if (!rep.ok()) {
        for (auto const& e : rep.errors) ds::clientOut() << "  " << e << std::endl;
        ds::clientOut() << "VERIFY FAIL: I1-I4 do NOT hold after this workload"
                        << std::endl;
        return 1;
    }
    ds::clientOut() << "VERIFY PASS: I1-I4 hold after this workload, and every "
                       "old_ver chain decreases in ts" << std::endl;
    return 0;
}

int run_structure_selftest(ds::DsState& state) {
    uint32_t const layers = static_cast<uint32_t>(state.layout.cache_layers);

    // ONE path, whatever the replica count.
    //
    // The replication factor is the number of memory servers this run connects
    // to, decided at runtime: QuorumOps derives its majority from
    // set.replicas(), so at one server it is a majority of one and every
    // quorum rule degenerates to the direct operation. That is why there is no
    // separate single-replica implementation to pick between here, and why
    // "zero overhead at N=1" is true by construction rather than by keeping two
    // code paths in step. quorum_test.cc asserts it at both counts.
    //
    // NOTE on DS_N_REPLICAS: invariants.md §9 describes it as the toggle that
    // selects a "single-replica code path". In the skip-vector path it selects
    // nothing -- kNumReplicas is a constant with a static_assert and drives no
    // logic. The factor that matters is the server count. Left as a declared
    // expectation rather than wired into an assert, because comparing a 1-server
    // and a 3-server run is exactly the microbench §9 asks for and an assert
    // would forbid it.
    //
    // Passed as BOTH the reader and the ops: at three servers the verifier
    // should check the *committed* structure, which is what a quorum read
    // returns, rather than whatever one replica happens to hold.
    ds::RdmaReplicaSet<decltype(state.server_conns)> replicas(
        state.server_conns, state.layout, state.layout.getNodeBufs(0),
        state.layout.getVecBufs(0), state.layout.getCasBufs(0),
        state.layout.getStageNode(0), state.layout.getStageVec(0),
        &state.node_alloc, &state.vec_alloc, &state.vec_hint);
    replicas.setTsMode(state.layout.ts_mode);
    replicas.setClientIdx(state.client_idx);
    ds::QuorumStats qstats;
    // The hint makes a node read speculate its vector read alongside the
    // headers rather than serialising after them -- see ds_quorum.hpp.
    ds::QuorumOps<decltype(replicas)> ops(replicas, qstats, &state.vec_hint);

    std::cout << "replication:  " << replicas.replicas()
              << " server(s), majority " << ops.majority() << " (DS_N_REPLICAS="
              << ds::kNumReplicas << " declared)" << std::endl;
    std::cout << "timestamps:   " << ds::tsModeName(state.layout.ts_mode)
              << std::endl;

    int const rc = ds::runSelftest(ops, ops, layers, &state.vec_hint, std::cout);

    // Quorum accounting. At one server these are all trivial; at three they are
    // the evidence that CAS-ABD ran at all rather than the code happening to
    // work against replica 0.
    std::cout << "\n################ Quorum:" << std::endl;
    std::cout << "reads:        " << qstats.node_reads << " header quorum reads -> "
              << qstats.replica_reads << " replica reads" << std::endl;
    std::cout << "              " << qstats.stale_votes << " stale votes, "
              << qstats.tag_ties << " tag ties, " << qstats.read_retries
              << " re-polls" << std::endl;
    std::cout << "commits:      " << qstats.commits << " committed, "
              << qstats.commits_lost << " lost (" << qstats.partial_commits
              << " partial)" << std::endl;
    std::cout << "writebacks:   " << qstats.writebacks << " lagged replica(s) repaired"
              << std::endl;
    std::cout << "round trips:  " << qstats.batches << " chained batches"
              << std::endl;
    std::cout << "speculation:  " << qstats.speculated << " reads speculated, "
              << qstats.spec_hits << " hit / " << qstats.spec_misses
              << " miss; " << qstats.vec_reads_served
              << " vector read(s) served without a round trip" << std::endl;
    return rc;
}


std::unique_ptr<FILE, PipeDeleter> exec(const std::string& cmd) {
    auto raw_pipe = popen(cmd.c_str(), "r");
    if (!raw_pipe) {
        throw std::runtime_error("popen() failed!");
    }
    return std::unique_ptr<FILE, PipeDeleter>(raw_pipe);
}

// Simple hash implementation to replicate swarm's key-conflict hazard avoidance checking
size_t pseudo_hash(const std::string& str) {
    size_t hash = 5381;
    for (char const &c : str) {
        hash = ((hash << 5) + hash) + static_cast<size_t>(c);
    }
    return hash;
}

void run_ml_prog_tracker_workload(
    ds::DsClient& client, 
    uint64_t global_thread_id, 
    uint64_t num_clients,        // total client count (was num_registers)
    uint64_t ops_to_run,
    uint64_t think_time_ms,
    dory::memstore::MemoryStore& store) {

    std::cout << "Starting ML Tracker Workload: "
              << "thread_id=" << global_thread_id
              << " num_clients=" << num_clients
              << " ops_to_run=" << ops_to_run
              << " think_time_ms=" << think_time_ms << std::endl;

    // --- WARMUP PHASE (5-second native spin) ---
    auto warmup_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (std::chrono::steady_clock::now() < warmup_deadline) {
        if (global_thread_id == 0) {
            client.getFreeRangeFuture().doRange(0, num_clients - 1, false);
        } else {
            // Height 0: this warmup only needs the key present, and a
            // structural insert would make the warmup's cost depend on the
            // height draw.
            client.getFreeFuture().doPut(global_thread_id, 1, /*height=*/0, false);
        }
        client.finishAllFutures();
    }

    auto start_time = std::chrono::steady_clock::now();
    uint64_t completed_ops = 0;

    if (global_thread_id == 0) {
    // ────────────── TRACKER ──────────────
        const uint64_t num_workers = num_clients - 1;
        uint64_t workers_done = 0;
        uint64_t op_idx = 0;
        std::vector<bool> seen(num_clients, false);

        while (workers_done < num_workers) {
            bool measuring = (op_idx % 1000 == 0);

            // 1. Issue the range scan
            client.getFreeRangeFuture().doRange(0, num_clients - 1, measuring);

            // 2. Flush IMMEDIATELY so the measurement window closes
            //    before we sleep. Latency stats now exclude think time.
            client.finishAllFutures();

            // 3. Poll worker-done flags (cheap, non-blocking memstore lookups).
            //    Done here so it also doesn't pollute latency measurement.
            if (op_idx % 16 == 0) {
                for (uint64_t w = 1; w < num_clients; ++w) {
                    if (seen[w]) continue;
                    std::string val;
                    if (store.get("ml_worker_done_" + std::to_string(w), val)) {
                        seen[w] = true;
                        ++workers_done;
                        if (workers_done >= num_workers) break;
                    }
                }
            }
            if (workers_done >= num_workers) break;

            // 4. NOW sleep — outside the measurement window.
            if (think_time_ms > 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(think_time_ms));
            }

            ++op_idx;
        }
        completed_ops = op_idx;
    } else {
        // ────────────── WORKER ──────────────
        uint64_t progress_counter = 1;

        for (uint64_t op_idx = 0; op_idx < ops_to_run; ++op_idx) {
            bool measuring = (op_idx % 1000 == 0);
            client.getFreeFuture().doPut(global_thread_id, progress_counter++,
                                         /*height=*/0, measuring);
            if (op_idx % 16 == 0) {
                client.finishAllFutures();
            }
        }
        client.finishAllFutures();
        completed_ops = ops_to_run;

        // Signal the tracker that this worker is done.
        store.set("ml_worker_done_" + std::to_string(global_thread_id), "1");
    }

    auto end_time = std::chrono::steady_clock::now();

    uint64_t duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                               end_time - start_time).count();
    double duration_sec = static_cast<double>(duration_ns) / 1'000'000'000.0;
    double tput_kops    = (static_cast<double>(completed_ops) / 1000.0) / duration_sec;

    client.reportStats(true);
    std::cout << "ML-TRACKER-RESULTS: "
              << "thread_id=" << global_thread_id
              << " role="     << (global_thread_id == 0 ? "tracker" : "worker")
              << " ops="      << completed_ops
              << " duration_sec=" << duration_sec
              << " tput_kops=" << tput_kops << std::endl;
}

/// Pin the calling thread to one PHYSICAL core.
///
/// Replaces what `numactl -C $CORE` does per process today: once a node's
/// clients are threads in one process, the process has to place them itself or
/// they float and the measurement is about the scheduler.
///
/// CORE NUMBERING IS THE TRAP AND IT HAS ALREADY BEEN PAID FOR ONCE. On these
/// machines /sys/.../thread_siblings_list reports (0,8), (1,9) ... (7,15): 8
/// physical cores, 16 logical, with each core's second thread numbered +8. So
/// cores 0..7 are eight DISTINCT physical cores and a stride of 1 is correct,
/// while the stride of 2 that looks natural lands on 0,2,4,6,8,10,12,14 --
/// which is physical cores 0,2,4,6 used twice and 1,3,5,7 left idle. run.sh
/// carries the same note for the same reason.
static bool pinThreadToCore(unsigned core) {
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(core, &set);
    int const rc = pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
    if (rc != 0) {
        std::cerr << "WARNING: could not pin thread to core " << core
                  << " (pthread_setaffinity_np: " << rc
                  << "). The run will still produce numbers, but they are "
                     "about the scheduler as much as the system."
                  << std::endl;
        return false;
    }
    return true;
}

/// Collective operations for the client threads in this process.
///
/// WHY THIS EXISTS. dory's MemoryStore is a singleton wrapping ONE
/// libmemcached connection, and libmemcached is not thread-safe: two client
/// threads calling into it concurrently race on the connection's query_id and
/// the process dies on an assertion inside memcached_get. Setup avoids this by
/// running serially on the main thread, but runClient itself is full of
/// collectives -- the "initialized" and "finished" rendezvous and the
/// measure-start barrier -- and those run once the threads are going.
///
/// A PLAIN MUTEX DOES NOT WORK, which is the part worth knowing. MemoryStore's
/// barrier increments once and then POLLS until the count reaches wait_for, and
/// waitReadyAll polls likewise. Holding a lock across either deadlocks: one
/// thread waits for a peer to increment while holding the lock that peer needs
/// in order to do it.
///
/// So collectives are two-level. The threads rendezvous locally; the last one
/// in performs the global operation alone while the others are parked on a
/// condition variable (holding nothing); then everyone is released. Exactly one
/// thread is ever inside libmemcached, and nobody blocks while holding
/// anything.
///
/// At one thread per process the first arrival is immediately the last, so the
/// global operation runs inline and the sequence is what it always was.
class ClientSync {
public:
    ClientSync(size_t threads, dory::memstore::MemoryStore &store,
               std::vector<std::unique_ptr<
                   dory::conn::RcConnectionExchanger<ds::ProcId>>> &ces,
               size_t client_processes)
        : n_{threads}, store_{store}, ces_{ces},
          client_processes_{client_processes} {}

    /// Every identity in this process announces `state`, then ONE wait covers
    /// the process. Waiting on the first exchanger is sufficient: its remotes
    /// are every other participant, our own other identities included, and
    /// those have already announced.
    void rendezvous(char const *state) {
        collective([&] {
            for (auto &c : ces_) c->announceReady(store_, "qp", state);
            ces_[0]->waitReadyAll(store_, "qp", state);
        });
    }

    void unannounce(char const *state) {
        collective([&] {
            for (auto &c : ces_) c->unannounceReady(store_, "qp", state);
        });
    }

    /// A CLIENTS-ONLY barrier, on the memstore counter.
    ///
    /// A CE rendezvous cannot do this: waitReadyAll waits for every remote,
    /// servers included, and a server never announces a client-only state, so
    /// the clients would wait forever. The counter is incremented once per
    /// process, like measureStart, and counts only who arrives.
    void barrierAll(char const *key) {
        collective([&] { store_.barrier(key, client_processes_); });
    }

    /// The measure-start barrier, counted in PROCESSES rather than clients.
    ///
    /// MemoryStore::barrier increments by one per call, so a process that
    /// increments once on behalf of its threads must be counted once. At one
    /// thread per process this is the client count, i.e. exactly the number
    /// the single-threaded path has always passed.
    void measureStart() {
        collective([&] { store_.barrier("measure-start", client_processes_); });
    }

private:
    template <class Op>
    void collective(Op const &op) {
        std::unique_lock<std::mutex> lk(m_);
        size_t const my_gen = gen_;
        if (++arrived_ == n_) {
            // Last in: the others are parked on cv_ and hold nothing, so this
            // is the only thread that can touch the store.
            op();
            arrived_ = 0;
            ++gen_;
            lk.unlock();
            cv_.notify_all();
        } else {
            cv_.wait(lk, [&] { return gen_ != my_gen; });
        }
    }

    size_t n_;
    dory::memstore::MemoryStore &store_;
    std::vector<std::unique_ptr<
        dory::conn::RcConnectionExchanger<ds::ProcId>>> &ces_;
    size_t client_processes_;
    std::mutex m_;
    std::condition_variable cv_;
    size_t arrived_ = 0;
    size_t gen_ = 0;
};

/// Everything one client does, from constructing its DsClient to its last line
/// of output.
///
/// EXTRACTED FROM main SO THAT A NODE'S CLIENTS CAN BE THREADS. It was 500
/// lines inside `if (is_client)`, which made one client per process a
/// structural fact rather than a choice. Nothing about the work changed; it
/// only became callable more than once, with its own proc id, its own queue
/// pairs, its own slice of the registered region and its own log, sharing
/// whatever NodeCache the caller hands it.
///
/// `layout` is BY VALUE deliberately: client_local_region differs per thread,
/// so a shared reference would have every thread reading and writing one
/// another's scratch buffers -- silent corruption, not an error.
///
/// Returns 0 on success. These returns used to exit main directly, so the
/// caller now has to aggregate them: one failing client must still fail the
/// process rather than being swallowed by a thread nobody checked.
static int runClient(ds::Layout layout,
                     ds::ProcId proc_id,
                     dory::conn::RcConnectionExchanger<ds::ProcId> &ce,
                     dory::memstore::MemoryStore &store,
                     ClientSync &sync,
#if DS_CACHE_ENABLED
                     ds::NodeCache &node_cache,
#endif
                     std::string const &workload,
                     std::string const &ycsb_path,
                     uint64_t iter_count,
                     uint64_t warmup,
                     uint64_t keepwarm,
                     uint64_t start_measurements,
                     uint64_t stop_measurements,
                     uint64_t total_iter_count,
                     bool detailed,
                     bool run_selftest,
                     bool verify_after,
                     bool run_ml_workload,
                     uint64_t think_time) {
#if DS_CACHE_ENABLED
    ds::DsClient client{layout, ce, proc_id, node_cache};
#else
    ds::DsClient client{layout, ce, proc_id};
#endif
    // PER CLIENT, not per process. See the note where this used to be a
    // file-scope global.
    std::vector<YcsbOp> operations;
    int verify_rc = 0;
    ds::DsState& state = client.getState();

    // ─── Bootstrap the skip vector ─────────────────────────────
    //
    // The first client writes the initial structure. Everything below this
    // point runs after the "initialized" barrier that every process already
    // waits on, so there is no need for a barrier of our own: no client
    // traverses before that barrier, and the writes complete before we
    // announce.
    bool const is_initializer = (proc_id == layout.firstClientId());
    if (is_initializer) {
        bootstrap_structure(state);
    }

#if DS_CACHE_ENABLED
    // Hand the cache its head addresses. These are reserved constants, so
    // this needs no discovery -- but it does need doing, because a cache
    // that was never told them reports a miss for every key left of the
    // first boundary at every level, which is correct but silently slow
    // (interface doc §5).
    ds::bootstrapHeads(state.cache_sv,
                       ds::headAddrs(static_cast<uint32_t>(layout.cache_layers)));
    if (!ds::headsBootstrapped(state.cache_sv)) {
        std::cerr << "Cache heads were not bootstrapped" << std::endl;
        return 1;
    }
#endif

    // ─── Structure selftest ────────────────────────────────────
    //
    // Its own path, so it needs neither YCSB nor a workload file. It has to
    // run after every client has passed the barrier, since a concurrent
    // bootstrap write would look like corruption to a sequential checker.
    if (run_selftest) {
        sync.rendezvous("initialized");

        int const rc = run_structure_selftest(state);
        state.reportCache();

        sync.rendezvous("finished");
        sync.unannounce("initialized");
        sync.unannounce("connected");
        ds::clientOut() << "###DONE###" << std::endl;
        return rc;
    }

    // SWARM PATTERN: First client triggers initial data loading phase
    if (proc_id == (layout.num_servers + 1)) {
        ds::clientOut() << "Querying YCSB for the set of initial key-pairs... " << std::flush;
        std::vector<std::pair<std::string, std::string>> inserts = {};

        {
            auto fp = exec(ycsb_path + " load basic -P " + workload + " -s 2> /dev/null");
            char buffer[1024];
            while (fgets(buffer, sizeof(buffer), fp.get()) != nullptr) {
                std::string line(buffer);
                if (std::strncmp("INSERT ", line.c_str(), 7) != 0) {
                    continue;
                }
                auto keystart = std::string("INSERT usertable ").length();
                auto keyend = line.find(" [", keystart);
                auto key = line.substr(keystart, keyend - keystart);

                auto start = keyend + std::string(" [ field0=").length();
                auto end = line.length() - std::string(" ]\n").length();
                auto value = line.substr(start, end - start);

                inserts.emplace_back(key, value);
            }
        }
        ds::clientOut() << "Done." << std::endl;

        ds::clientOut() << "Inserting the initial key-pairs... " << std::flush;
        // Same seed rule as the measured phase: reproducible per client,
        // and different between clients so two do not make an identical
        // structural decision for the same key.
        std::mt19937_64 pop_rng(0xB0B ^ proc_id);
        // PARTITION THE PRELOAD ACROSS CLIENTS. It used to be
        // `kvIndex = 0; kvIndex < inserts.size()` on EVERY client, so all
        // of them inserted the whole key set.
        //
        // At high client counts that is millions of put operations to
        // build a key set of a hundred thousand, and it was wrong in three
        // ways at once:
        //
        //  1. WRONG WORKLOAD. Only the first client to reach a key inserts
        //     it; every other client performs an UPDATE. So the "load"
        //     phase ran almost entirely updates, which is not what loading
        //     a structure means, and the split history that comes out of
        //     it -- orphan counts, occupancy, level populations -- is
        //     shaped by those writes rather than by the key set.
        //  2. WRONG ARENA. Every put allocates a vector (copy-on-write, no
        //     reclamation), so the preload alone reserved a vector per key
        //     PER CLIENT. At high client counts the arena approached the
        //     servers' available memory and throughput collapsed -- which
        //     looked like a scaling limit and was memory pressure.
        //  3. WRONG SETUP TIME. Work proportional to the client count
        //     before every run, all of it unnecessary.
        //
        // Striding by client_idx gives each client a disjoint share, so the
        // total is the key set exactly once. finishAllFutures below plus
        // the "initialized" barrier still order the whole load before any
        // client starts measuring, so no client can observe a partially
        // loaded structure.
        uint64_t const preload_stride = layout.num_clients;
        uint64_t const preload_start = state.client_idx;
        for (size_t kvIndex = preload_start; kvIndex < inserts.size();
             kvIndex += preload_stride) {
            
            // Extract numerical representation of key string for the register mapping
            uint64_t target_reg = std::stoull(inserts[kvIndex].first.substr(4)) % layout.num_registers;
            // ds::clientOut() << "Inserting register: " << target_reg << std::endl;
            // Population, so heights are drawn: this is what builds the
            // index the measured phase then traverses. A populated
            // structure with no index would measure a linked list.
            uint32_t const ph = ds::drawHeight(
                pop_rng, static_cast<uint32_t>(layout.cache_layers));
            client.getFreeFuture().doPut(target_reg, 69, ph, false);
        }
        client.finishAllFutures();
        ds::clientOut() << " Done." << std::endl;
    }

    if (run_ml_workload) {
        // Trackers are assigned based on a normalized structural index 
        // sequence. If this is client #1 (proc_id == num_servers + 1), it becomes ID 0 (Tracker).
        uint64_t global_thread_id = proc_id - layout.num_servers - 1;

        ds::clientOut() << "Running single-threaded ML tracker workload. ID=" << global_thread_id  << "Registers=" << layout.num_registers << "Clients=" << layout.num_clients << std::endl;
        
        // Sync with cluster deployment infrastructure
        sync.rendezvous("initialized");

        run_ml_prog_tracker_workload(client, global_thread_id,
                         layout.num_clients, iter_count,
                         think_time, store);

        sync.rendezvous("finished");
        sync.unannounce("initialized");
    } 
    else {
        ds::clientOut() << "Configuring Client " << proc_id << std::endl;
        
        // SWARM PATTERN: Load continuous execution operations
        ds::clientOut() << "Querying YCSB for the list of operations... " << std::flush;

        {
            auto fp = exec(ycsb_path + " run basic -P " + workload + " -s 2> /dev/null");
            char buffer[1024];
            while (fgets(buffer, sizeof(buffer), fp.get()) != nullptr) {
                std::string line(buffer);
                
                if (!(std::strncmp("READ ", line.c_str(), 5))) {
                    auto keystart = std::string("READ usertable ").length();
                    auto keyend = line.find(" [", keystart);
                    auto key = line.substr(keystart, keyend - keystart);

                    uint64_t target_reg = std::stoull(key.substr(4)) % layout.num_registers;
                    operations.push_back({target_reg, OpGet, 0});
                } 
                else if (!(std::strncmp("UPDATE ", line.c_str(), 7))) {
                    auto keystart = std::string("UPDATE usertable ").length();
                    auto keyend = line.find(" [", keystart);
                    auto key = line.substr(keystart, keyend - keystart);

                    uint64_t target_reg = std::stoull(key.substr(4)) % layout.num_registers;
                    operations.push_back({target_reg, OpPut, 0});
                } 
                else if (!(std::strncmp("INSERT ", line.c_str(), 7))) {
                    auto keystart = std::string("INSERT usertable ").length();
                    auto keyend = line.find(" [", keystart);
                    auto key = line.substr(keystart, keyend - keystart);

                    // INSERTS GET THEIR OWN KEY BAND, above everything the
                    // load phase wrote. Not the same mapping as the other ops,
                    // and not the raw key either -- both are wrong here:
                    //
                    //  * `% num_registers`, as READ/UPDATE/SCAN and the load
                    //    phase all use, folds insert keys straight back onto
                    //    loaded keys. Every insert becomes an update and the
                    //    only difference between D or E and B or C disappears.
                    //
                    //  * the raw key does not work either. YCSB's default
                    //    insertorder is `hashed`, so these are not a counter
                    //    from recordcount -- they are hashed 64-bit values
                    //    spanning ~2e16 to ~9.2e18 (checked against the real
                    //    generator). Using them raw scatters inserts across the
                    //    whole key space, nowhere near the [0, num_registers)
                    //    band everything else touches, and brushes up against
                    //    kReservedKey (UINT64_MAX) as a sentinel collision.
                    //
                    // So: hash into a band of 4*num_registers starting at
                    // num_registers. Keys are genuinely new (disjoint from the
                    // loaded range by construction), bounded, adjacent to the
                    // loaded data so the structure stays compact, and clear of
                    // the sentinel. The band is 4x wider than the insert count
                    // a standard run issues (5% of operationcount), which keeps
                    // insert-on-insert collisions low without being sparse.
                    //
                    // BUDGET THE ARENA FOR THESE. An insert allocates a vector
                    // and may split, and nothing is reclaimed, so
                    // --vecs-per-client has to cover recordcount plus the
                    // inserts the run will issue.
                    uint64_t const raw = std::stoull(key.substr(4));
                    uint64_t const band = layout.num_registers * 4;
                    uint64_t const insert_key =
                        layout.num_registers + (raw % band);
                    operations.push_back({insert_key, OpInsert, 0});
                }
                else if (!(std::strncmp("SCAN ", line.c_str(), 5))) {
                    auto keystart = std::string("SCAN usertable ").length();
                    auto keyend = line.find(" ", keystart);
                    auto key = line.substr(keystart, keyend - keystart);

                    auto countstart = keyend + 1;
                    auto countend = line.find(" [", countstart);
                    uint64_t scan_len = std::stoull(line.substr(countstart, countend - countstart));

                    uint64_t target_reg = std::stoull(key.substr(4)) % layout.num_registers;
                    operations.push_back({target_reg, OpScan, scan_len});
                }
            }
        }
        {
            // Report the mix. The run phase used to parse READ/UPDATE/SCAN
            // and drop INSERT on the floor, so a workload with inserts ran
            // silently short and looked like a different workload. Printing
            // the breakdown is how that stays visible.
            size_t n_get = 0, n_put = 0, n_scan = 0, n_ins = 0;
            for (auto const &o : operations) {
                switch (o.type) {
                    case OpGet:    ++n_get;  break;
                    case OpPut:    ++n_put;  break;
                    case OpScan:   ++n_scan; break;
                    case OpInsert: ++n_ins;  break;
                }
            }
            ds::clientOut() << "Done. Packed " << operations.size()
                      << " operations: " << n_get << " read, " << n_put
                      << " update, " << n_scan << " scan, " << n_ins
                      << " insert." << std::endl;

            // --ts none WITH SCANS IS A CONFIGURATION ERROR, and it has to
            // fail HERE rather than per range.
            //
            // Nothing in the arena is stamped in that mode, so a range has
            // no snapshot to answer at and RangeOperation refuses. If the
            // run proceeded, every scan would count as a failure and the
            // throughput number would describe a workload that answered
            // 95% of its operations with an error -- a plausible-looking
            // figure for something that did no useful work. Refusing up
            // front costs one line of output instead of a void sweep.
            //
            // The check lives after the census because that is the first
            // point at which the operation mix is known: the mode is a
            // flag, but whether the workload scans is a property of the
            // YCSB file.
            // THE BACKSTOP. The cap is derived from the workload file
            // before the memory region is sized, and the refusal above
            // compares a PINNED value against that same file -- so both
            // rest on having read the file correctly. This checks the
            // operations that were actually packed, which is the thing
            // that matters and the only place it can be known exactly.
            //
            // Cannot be fixed by resizing here: bulkBufsSize() was
            // registered long before this point. So it refuses, for the
            // same reason the pinned check does -- a truncated scan
            // measures a workload nobody asked for.
            {
                uint64_t widest = 0;
                for (auto const &o : operations) {
                    if (o.type == OpScan && o.scan_len > widest) {
                        widest = o.scan_len;
                    }
                }
                if (widest > state.layout.max_range) {
                    std::cerr << "\n*** the packed workload contains a scan "
                                 "of " << widest << " entries but maxrange "
                                 "is " << state.layout.max_range
                              << ".\n*** Every longer scan would be "
                                 "silently truncated. This is a harness "
                                 "bug:\n*** the cap is derived from the "
                                 "workload file, so the file and the packed "
                                 "\n*** operations disagree."
                              << std::endl;
                    return 1;
                }
            }

            if (!ds::tsStamps(state.layout.ts_mode) && n_scan > 0) {
                std::cerr << "\n*** --ts none cannot run a workload with "
                          << "scans: " << n_scan << " of "
                          << operations.size() << " operations are scans."
                          << "\n*** No version is stamped in that mode, so "
                          << "a range has no snapshot to answer at."
                          << "\n*** Use --ts faa (or clock/tsc) for scan "
                          << "workloads; --ts none is for the point-only "
                          << "workloads (YCSB A-D)." << std::endl;
                return 1;
            }
        }

        ds::clientOut() << "Waiting for the initialization of other clients... " << std::flush;
        sync.rendezvous("initialized");
        ds::clientOut() << "Done." << std::endl;

        ds::clientOut() << "Running the benchmark (YCSB Swarm Engine)... " << std::endl;

        std::chrono::steady_clock::time_point start_time;
        std::chrono::steady_clock::time_point end_time;
        bool measuring = false;
        size_t skipped = 0;

        // Heights are drawn here rather than inside the put, because the
        // cache's geometry expects a geometric distribution with
        // p = 1/kLevelRatio and the drawing has to be reproducible for a
        // given seed. Seeded per client so two clients do not make an
        // identical structural decision for the same key.
        std::mt19937_64 height_rng(0x5EED ^ proc_id);

        for (size_t i = 0; i < total_iter_count; i++) {
            auto& op = operations[(i + skipped) % operations.size()];

            if (i == start_measurements) {
                // Drain before starting the clock: otherwise operations
                // issued during warmup complete inside the measured window
                // and are counted as its throughput.
                client.finishAllFutures();

                // BARRIER, OR THE SUM OVER CLIENTS IS MEANINGLESS.
                //
                // There is a barrier at initialization but there was none
                // here, so each client ran `warmup` operations at its own
                // pace and began measuring whenever it personally arrived.
                // Client start times therefore drifted, and once the drift
                // exceeded keepwarm the per-client windows stopped
                // overlapping: every client measured a system carrying only
                // part of the load, and the sweep summed those rates into a
                // total the system never delivered.
                //
                // OBSERVED on workload E from the same binary with a
                // fixed number of measured operations each: runs whose
                // per-client windows all had the same duration agreed with
                // each other, while a run whose windows differed by several
                // times reported roughly double. In that run one client
                // finished its whole allocation faster than a single
                // UNCONTENDED client can go on this cluster, while its own
                // sibling on the same node took several times as long.
                // Both cannot be right about a shared system unless they
                // measured different stretches of wall-clock time. The
                // excursion is ABOVE the figure that reproduces whenever
                // the windows are tight, so the error flatters us.
                //
                // memstore::barrier is an atomic increment-and-wait and it
                // THROWS if the count passes wait_for, so a counter left
                // over from a previous run fails loudly instead of letting
                // the barrier through. remote-memc.sh restarts memcached
                // per run, so it starts clean. Only clients reach this
                // loop (is_client == proc_id > num_servers), hence
                // num_clients and not num_clients + num_servers -- waiting
                // on the servers here would hang forever, because they
                // never enter the benchmark.
                sync.measureStart();

                measuring = true;
                start_time = std::chrono::steady_clock::now();
            } else if (i == stop_measurements) {
                // And drain before stopping it, so operations issued inside
                // the window are paid for inside it. Without this the tail
                // of the pipeline is free and the number is inflated by
                // roughly async_parallelism operations.
                //
                // NO BARRIER HERE, DELIBERATELY. Every client runs the same
                // iter_count, so a barrier before end_time would park the
                // fast clients until the slow ones caught up and count that
                // idle wait inside their own measured window -- deflating
                // exactly the clients that were working hardest. The starts
                // are what must coincide; enlarged keepwarm (above) keeps
                // the finished clients loading the system through the tail.
                client.finishAllFutures();
                measuring = false;
                end_time = std::chrono::steady_clock::now();
            }
            switch (op.type) {
                case OpGet: {
                    // target_reg is the workload's key. It indexed a flat
                    // register array; it is now a skip-vector key, which is
                    // the same integer used for a different thing -- so
                    // these numbers are a fresh baseline and not comparable
                    // with any register-path measurement.
                    client.getFreeFuture().doGet(op.target_reg, measuring);
                    break;
                }
                case OpScan: {
                    // A10: the SKIP-VECTOR range. This used to go to the
                    // register RangeFuture over the old flat array, which
                    // meant a scan-heavy workload measured the previous
                    // structure -- and, worse, that E's scans and its
                    // inserts touched two disjoint structures, so the
                    // inserts never grew the thing being scanned.
                    //
                    // The interval is a KEY RANGE now, not a register span.
                    // YCSB gives a start key and a count, and the skip
                    // vector is ordered, so the count becomes an upper
                    // bound on the entries returned rather than a bound on
                    // the key distance -- which is what a scan means for an
                    // ordered structure and what dLSM's iterator does too.
                    // The key window is left open at the top and the CAP is
                    // what stops it; a fixed key width would return a
                    // wildly variable number of entries depending on how
                    // dense the keyspace is there.
                    uint64_t len = op.scan_len;
                    if (len > layout.max_range) len = layout.max_range;
                    if (len == 0) len = 1;
                    client.getFreeFuture().doRange(
                        op.target_reg, ds::kUnboundedKey, len, measuring);
                    break;
                }
                case OpPut: {
                    uint32_t const h = ds::drawHeight(
                        height_rng, static_cast<uint32_t>(layout.cache_layers));
                    client.getFreeFuture().doPut(op.target_reg, 69, h, measuring);
                    break;
                }
                case OpInsert: {
                    // Mechanically the same call as OpPut -- a put of an
                    // absent key IS an insert, and F1/F2 decide which by
                    // looking at the node. Kept as its own case so the
                    // counts below can separate "rewrote a loaded key" from
                    // "grew the structure", which are different costs: an
                    // insert is the only op that can force a split.
                    //
                    // CAVEAT ON LONG RUNS. `operations` is cycled when
                    // total_iter_count exceeds its length, so on the second
                    // pass these keys already exist and the op degrades to
                    // an update. The counter below reports issued inserts,
                    // not distinct keys.
                    uint32_t const h = ds::drawHeight(
                        height_rng, static_cast<uint32_t>(layout.cache_layers));
                    client.getFreeFuture().doPut(op.target_reg, 69, h, measuring);
                    break;
                }
                default:
                    break;
            }
            // NO DRAIN HERE. The loop used to call finishAllFutures() every
            // iteration, which made the client synchronous: one operation
            // in flight at a time, so async_parallelism did nothing at all.
            // getFreeFuture() is what bounds concurrency now -- it returns
            // a slot only once that slot's operation has finished.
        }

        client.finishAllFutures();
        ds::clientOut() << "Done. Results:" << std::endl;

        client.reportStats(detailed);
        // THREE DECIMALS, AND THE RESOLUTION IS THE WHOLE REASON.
        //
        // This printed an INTEGER number of kops per client. Per-client
        // throughput falls as clients are added -- workload A at 64
        // clients is a few kops each -- so the smallest representable
        // difference was 1 kops/client, which across all of them is a
        // large share of the total, or
        // a large share of the total. Every client in a cell reported the
        // identical integer and the summed total landed on an exact
        // multiple of the client count.
        //
        // That is not a rounding nuisance, it manufactured findings. A
        // kIdxExp comparison at high client counts came out as a clean
        // doubling when the underlying per-client figures were two
        // quantization steps apart, so the true ratio was anywhere in a
        // wide band. It also masqueraded as run-to-run variance, and an
        // instrumented (--latency 1) run could appear FASTER than an
        // uninstrumented one purely by landing one step up.
        //
        // Computed in double rather than truncating integer division for
        // the same reason. lib.sh's total_kops and the plotter's
        // _TPUT_PATTERNS both accept a decimal now.
        fmt::print(ds::clientOut(), "Local tput: {:.3f} kops\n",
                static_cast<double>(iter_count) * 1e6
                / static_cast<double>((end_time - start_time).count()));
        fmt::print(ds::clientOut(), "Local duration: {}s\n", 
        static_cast<uint64_t>((end_time - start_time).count() / 1000000000));
        ds::clientOut() << std::flush;
        
        // THE VERIFY MUST HAPPEN BEFORE "finished", NOT AFTER.
        //
        // The server branch announces "finished", waits for everyone, and
        // then CLOSES ITS CONNECTION. Verifying after that rendezvous means
        // reading from servers that are tearing down, and the walk hangs on a
        // read that can never complete -- which is exactly what it did, twice,
        // while being misdiagnosed first as too much data and then as an
        // undrained completion queue. A two-second workload followed by
        // fifteen minutes of silence was the tell.
        //
        // So: a clients-only barrier to establish quiescence while the servers
        // are still up, the verify, then a second barrier so the other clients
        // do not announce "finished" and kill the servers underneath it.
        sync.barrierAll("verify-quiesce");
        if (verify_after && proc_id == layout.firstClientId()) {
            // DRAIN FIRST. The verifier posts reads and polls the send CQ
            // blindly, and that CQ is shared with the future machinery -- the
            // same contract range_lock.hpp's casBlocking documents. The
            // selftest gets away without this because no future has ever run
            // in that path; here twenty thousand operations just did, and the
            // verify hung waiting for a completion that was already consumed.
            // It presented as "the walk is too slow", and the run was resized
            // twice before the cause was read off the log: a 2-second
            // workload followed by fifteen minutes of nothing.
            client.finishAllFutures();
            verify_rc = verify_after_run(client.getState());
        }
        if (verify_after) sync.barrierAll("verify-done");

        sync.rendezvous("finished");
        if (verify_rc != 0) {
            sync.unannounce("initialized");
            return verify_rc;
        }
        sync.unannounce("initialized");
    }
    return 0;
}

int main(int argc, char** argv) {
    ds::ProcId proc_id = 0;
    bool show_help = false;

    ds::Layout layout;
    layout.num_clients       = 1;
    layout.num_servers       = 1;
    layout.async_parallelism = 1;
    layout.num_registers     = 100000;
    // 0 MEANS DERIVE IT FROM THE WORKLOAD FILE. See deriveMaxRange below for
    // why this is not simply 10 any more.
    layout.max_range         = 0;
    layout.majority          = 0;
    layout.cache_layers      = 4;
    layout.nodes_per_client  = 1 << 16;
    layout.vecs_per_client   = 1 << 18;
    // OFF BY DEFAULT AS OF 17 SEP 2026, MEASURED. The speculative vector read
    // appends the guessed vector to every header fan-out, and on a
    // write-heavy workload most of those are discarded, because every write
    // moves the vector to a freshly allocated offset. Turning it off does MORE
    // round trips per operation and is faster anyway, because per-trip service
    // time falls: what saturates is priced in WORK PER ROUND TRIP, not in
    // round trips, so inflating every operation to save a trip on some of
    // them is a losing trade, and it compounds with client count.
    //
    // The justification in layout.hpp -- that this trades "rNIC and PCIe
    // bandwidth, which is the scarce resource under write-heavy load" -- had
    // the premise backwards: bandwidth sits mostly idle at the cliff, which is
    // exactly WHY the trade is bad. The hint spends the plentiful resource to
    // save the scarce one.
    //
    // Left as a toggle rather than deleted: on a read-mostly workload the
    // guess is almost always right and the saved trip is real. --offset-hint 1
    // restores it.
    layout.offset_hint       = false;
    layout.batched_walk      = false;
    // ON BY DEFAULT. A range used to walk next_id one node per DEPENDENT round
    // trip; taking the chain from the local directory batches header and
    // vector fetches for several nodes at once, which collapses the per-node
    // speculative vector read that dominated a long scan.
    //
    // IT LOST TWICE BEFORE, BOTH TIMES ON SIZING, NOT ON THE IDEA. It asked by
    // `hi`, which OpScan leaves unbounded, so the directory returned its full
    // width and the batch fetched several times the nodes a scan uses; then it
    // asked by kNodeCapacity, which assumes full nodes, so it was handed about
    // half what it needed and refilled repeatedly. fillFromCache now sizes by
    // OBSERVED occupancy and by the entries still wanted, and the over-fetch
    // that sank both attempts is gone.
    //
    // Inert where there are no ranges: cache_walk_ is read only by
    // RangeOperation, so a scan-free workload cannot be affected by this.
    // Needs the cache compiled in and consulted -- see the guard below, which
    // refuses the combination rather than silently ignoring it.
    layout.cache_walk        = true;
    layout.spread_reads      = false;
    layout.cas_as_write      = false;
    layout.read_quorum       = false;
    layout.consult_cache     = DS_CACHE_ENABLED ? true : false;
    layout.writeback         = DS_REG_WRITEBACK_ENABLED ? true : false;
    // ONE SIDEWAYS HOP BY DEFAULT, ON BOTH PATHS.
    //
    // A k_min mismatch means the cached node SPLIT, so the key is usually in
    // the sibling one `next` hop away. Following it costs one round trip; a
    // full descent from the head costs roughly an order of magnitude more.
    //
    // IT ONLY PAYS IF A RECOVERED HOP ALSO REPAIRS THE DIRECTORY.
    // Reconciliation used to be a side effect of descending, so a hop that
    // answered the operation left the stale entry in place and the next
    // operation on that key paid the mismatch again -- staleness compounded
    // and the write path was dragged down with it. With the repair the loop
    // runs the other way: a fresher directory means each path also cuts the
    // OTHER path's traversals, which is where most of the benefit comes from
    // rather than from the descents this operation itself avoided.
    //
    // --hint-hops covers both paths; --put-hint-hops pins the write path
    // alone and exists only to isolate one path in an experiment.
    //
    // Larger budgets were swept and are not distinguishable from 1: they
    // remove nearly all remaining descents while buying little, because a
    // failed chain pays every hop AND the descent anyway. So 1 takes the
    // available gain at the smallest worst-case latency.
    //
    // MEASURED ON WORKLOAD A ONLY so far. A is the right place to measure it,
    // being write-heavy, so splits are frequent and hints go stale often --
    // but the effect must scale with split rate, so read-mostly workloads
    // should see less and C, which writes nothing, should see nothing. Set
    // --hint-hops 0 to recover the previous behaviour exactly.
    layout.hint_hops         = 1;
    // Follow hint_hops on the write path: one knob, both paths. Pinning it is
    // for isolating one path in an experiment, not for shipping.
    layout.put_hint_hops     = ds::Layout::kPutHopsFollowGet;
    layout.ts_mode           = ds::TsMode::Clock;
    layout.measure_latency   = true;

    // Set by client at runtime after MR is allocated.
    // (Same pattern as swarm-kv: see Layout::client_local_region)
    uintptr_t client_local_region = 0;

    float rq_p = 0.0f, get_p = 0.5f, put_p = 0.5f;
    uint64_t iter_count = default_iter_count;
    uint64_t warmup     = UINT64_MAX;

    bool run_ml_workload = false;
    bool run_selftest = false;
    uint64_t think_time = 0;

    // ─── CLIENT THREADS PER PROCESS ───────────────────────────────────────
    //
    // 1 reproduces today's arrangement exactly: one client per process, one
    // NodeCache per process, so 64 clients keep 64 directories. Above 1 this
    // process hosts that many client threads and they SHARE one NodeCache, so
    // 8 nodes x 8 threads keep 8 directories instead of 64.
    //
    // THE STRIDE IS NOT COSMETIC. run.sh maps client c to machine
    // (FIRST_CLIENT + (c-1) % CLIENT_MACHINES), so the clients co-located on a
    // node are strided by CLIENT_MACHINES -- w4 hosts 1, 9, 17, ... and NOT
    // 1..8. That numbering has to be preserved, because client_idx drives both
    // the per-client arena stripes (node_alloc, vec_alloc) and the quorum
    // rotation (quorum_indices). Renumbering a node's clients contiguously
    // would change which server each one reads first and which arena stripe it
    // writes, and the comparison would then confound cache sharing with both.
    int64_t client_threads = 1;
    int64_t thread_stride = 1;
    // SHARING IS SEPARABLE FROM THREADING, AND HAS TO BE.
    //
    // Hosting a node's clients as threads does two things at once: it lets
    // them share one directory, and it removes seven of every eight
    // per-process ControlBlocks, registered MRs, CQ sets and polling loops. If
    // the threaded arm wins, either could be responsible, and the experiment
    // as designed cannot say which.
    //
    // So --share-cache 0 with --threads > 1 gives each thread its OWN cache:
    // the same launch, the same overhead saving, none of the sharing. Then
    // (threads, own cache) minus (processes) is the consolidation alone, and
    // (threads, shared) minus (threads, own) is the sharing alone. Costs one
    // flag, because NodeCache is already a separate object.
    int64_t share_cache = 1;
    // Check the structure after the workload, on one client, once everyone has
    // stopped writing. Off by default: it costs a full walk of the structure
    // and a throughput run does not want it.
    bool verify_after = false;
    // WHERE EACH CLIENT THREAD WRITES ITS LOG. Empty means "don't", which is
    // right at --threads 1: invoker.sh already tees this process's stdout to
    // exactly that path, and opening the same file here would put two writers
    // on it. Above one thread the binary has to do it, because a process has
    // one stdout and the harness wants one file per client.
    std::string client_log_dir;

    // -1 means "not given". The guard below must distinguish a contradictory
    // EXPLICIT request (--cache-walk 1 --cache 0, which is a mistake worth
    // refusing) from the DEFAULT meeting --cache 0, which is an ordinary
    // baseline arm and must still run. Before this existed, flipping the
    // default made every `--cache 0` arm exit 1.
    int cache_walk_arg = -1;

    // -1 means "not given", so the write path keeps following --hint-hops.
    // Signed because the sentinel has to be distinguishable from a real 0,
    // and 0 is a meaningful value here (pin the write path OFF).
    int64_t put_hint_hops_arg = -1;

    // Parsed into layout.ts_mode below: lyra binds strings, not enums.
    std::string ts_mode_name = "clock";

    std::string ycsb_path = "./YCSB/bin/ycsb.sh";
    std::string workload = "./YCSB/workloads/swarm-workloada";

    bool detailed = true;

    // ─── CLI ────────────────────────────────────────────────────────
    auto cli =
        lyra::cli() |
        lyra::help(show_help) |
        lyra::opt(proc_id, "proc_id")
            .required()["-i"]["-p"]["--id"]["--process"]
            .help("ID of this process.") |
        lyra::opt(layout.num_clients, "num_clients")
            .optional()["-c"]["--clients"] |
        lyra::opt(layout.num_servers, "num_servers")
            .optional()["-s"]["--servers"] |
        lyra::opt(layout.majority, "majority")
            .optional()["-m"]["--majority"] |
        lyra::opt(layout.async_parallelism, "async_parallelism")
            .optional()["-a"]["--async"] |
        lyra::opt(layout.num_registers, "num_registers")
            .optional()["-r"]["--regs"] |
        lyra::opt(layout.max_range, "max_range")
            .optional()["--maxrange"](
                "Upper bound on entries one scan may return, which also sizes "
                "the registered bulk buffer. NORMALLY LEAVE THIS UNSET: it is "
                "derived from the workload file's maxscanlength, and a value "
                "below that TRUNCATES every scan. Pass it only to pin the "
                "buffer deliberately; a pinned value smaller than the workload "
                "asks for is refused rather than applied.") |
        lyra::opt(layout.cache_layers, "cache_layers")
            .optional()["--layers"]("Skip-vector level count, directory = 0 (default 4)") |
        lyra::opt(layout.nodes_per_client, "nodes_per_client")
            .optional()["--nodes-per-client"](
                "Nodes in each client's arena stripe (default 65536). Nothing is "
                "reclaimed, so this must cover the whole run.") |
        lyra::opt(layout.vecs_per_client, "vecs_per_client")
            .optional()["--vecs-per-client"](
                "Vectors in each client's arena stripe (default 262144). Every "
                "write allocates one, so this is the binding limit on run "
                "length -- budget one per write, not one per node.") |
        lyra::opt(layout.offset_hint, "offset_hint")
            .optional()["--offset-hint"](
                "Speculate the vector offset so a node read is one doorbell "
                "batch instead of two serialised reads (1 or 0). Saves a round "
                "trip when right, wastes a vector read when wrong, so it "
                "favours read-heavy workloads.") |
        lyra::opt(layout.read_quorum, "read_quorum")
            .optional()["--read-quorum"](
                "Post header reads to a load-balanced majority() replicas "
                "instead of all of them. -m sets the quorum size. Falls back "
                "to all replicas for the retry when the quorum disagrees.") |
        lyra::opt(layout.cas_as_write, "cas_as_write")
            .optional()["--unsafe-cas-as-write"](
                "DIAGNOSTIC, BREAKS CORRECTNESS: issue every CAS as a plain "
                "RDMA WRITE. Measures what the atomic VERB costs by holding "
                "messages, bytes and round trips fixed. Loses updates.") |
        lyra::opt(layout.spread_reads, "spread_reads")
            .optional()["--spread-reads"](
                "Read each vector from replica (offset % n) rather than always "
                "the first replica that agrees. Off by default so the two arms "
                "stay comparable.") |
        lyra::opt(cache_walk_arg, "cache_walk")
            .optional()["--cache-walk"](
                "Take a range's chain of data nodes from the LOCAL CACHE "
                "instead of walking next_id, so the addresses cost no round trip "
                "and their vectors are fetched in one batch (1 or 0, default "
                "1). Needs the cache compiled in and consulted; with --cache 0 "
                "it turns itself off unless you asked for it explicitly, in "
                "which case the contradiction is refused. Unlike "
                "--batched-walk it also sees capacity-split orphans, which a "
                "remote index node does not name.") |
        lyra::opt(layout.batched_walk, "batched_walk")
            .optional()["--batched-walk"](
                "Take a range's chain of data nodes from a level-0 index node "
                "and fetch up to 16 per round trip instead of one (1 or 0, "
                "default 0). Orphaned nodes still cost a serial detour, and "
                "short ranges pay for the index read, so it favours long "
                "scans.") |
        lyra::opt(rq_p,  "rq_p" ).optional()["--rq"] |
        lyra::opt(get_p, "get_p").optional()["--get"] |
        lyra::opt(put_p, "put_p").optional()["--put"] |
        lyra::opt(workload, "workload").optional()["-w"]["--workload"] |
        lyra::opt(ycsb_path, "ycsb_path").optional()["-y"]["--ycsbpath"] |
        lyra::opt(detailed, "detailed").optional()["-d"]["--detailed"] |
        lyra::opt(iter_count, "iter_count").optional()["-I"]["--iter_count"] |
        lyra::opt(warmup,     "warmup")    .optional()["-W"]["--warmup"] |
        lyra::opt(layout.consult_cache, "cache")
            ["--cache"]("Consult the local skip-vector cache (1 or 0). Requires a "
                        "DS_CACHE_ENABLED build; 0 gives the no-cache baseline.") |
        lyra::opt(layout.writeback, "writeback")
            ["--writeback"]("Enable or disable writeback in the CAS-ABD protocol (1 or 0)") |
        lyra::opt(run_selftest, "selftest").optional()["--selftest"](
            "Bootstrap the structure, verify invariants I1-I4 over RDMA, and "
            "exit (1 or 0). Needs no YCSB and runs on a single server/client "
            "pair. Takes a value, like --cache and --ml: pass --selftest 1.") |
        lyra::opt(layout.measure_latency, "measure_latency")
            .optional()["--latency"](
                "Time individual operations (1 or 0, default 1). 0 removes two "
                "clock_gettime calls and a profiler update per operation, for a "
                "throughput figure that is not paying for latency it will not "
                "plot. NOT COMPARABLE with swarm-kv or fusee when off: both "
                "record unconditionally and neither has a switch. With 0 the "
                "GET/PUT stats sections are absent from the log rather than "
                "zero, so the omission is visible.") |
        lyra::opt(ts_mode_name, "ts_mode").optional()["--ts"](
            "Timestamp source: clock|tsc|faa|none|rangets (default clock). "
            "clock is a disciplined CLOCK_REALTIME and is the only mode "
            "correct at more than one client that also keeps the fault "
            "tolerance replication provides. tsc is raw rdtscp -- cheapest, "
            "single-client only. faa fetch-and-adds a REPLICATED counter on "
            "every write and reads it on every range: no timing assumption, "
            "at one extra round trip per write. rangets inverts that -- a "
            "RANGE fetch-and-adds the counter and a write only READS it -- so "
            "the atomic sits on the rare path; same two submissions per write "
            "as faa, one fewer atomic per write per server. none stamps "
            "nothing at all and REFUSES any workload containing a scan, which "
            "is legal for YCSB A-D. See ds_ts.hpp.") |
        lyra::opt(layout.hint_hops, "hint_hops").optional()["--hint-hops"](
            "Sideways `next` hops a stale cache hint may follow before giving "
            "up and descending from the head (default 1). A k_min mismatch "
            "means the named node SPLIT, so k is probably in the sibling one "
            "hop away: a recovery saves a ~4-trip descent, a failure costs a "
            "trip and still pays it, so it only pays if a RECOVERED HOP ALSO "
            "REPAIRS THE DIRECTORY -- without that it lost 12%, because "
            "reconciliation was a side effect of descending and staleness "
            "compounded. Applies to BOTH the read and the write path (see "
            "--put-hint-hops). Most of the benefit is not the descents this "
            "operation avoids: repairing on a recovered hop leaves the shared "
            "directory fresher, so each path also cuts the OTHER path's "
            "traversals. 0 restores the previous behaviour.") |
        lyra::opt(client_log_dir, "client_log_dir").optional()["--client-log-dir"](
            "Directory in which each CLIENT THREAD writes its own "
            "client<N>.txt, named the way run.sh names them. Required with "
            "--threads > 1, because a process has one stdout and every parser "
            "in experiments/compare reads one log per client. Ignored at "
            "--threads 1, where invoker.sh already tees stdout to that file.") |
        lyra::opt(verify_after, "verify_after").optional()["--verify-after"](
            "After the workload, once every client has stopped writing, have "
            "ONE client walk the structure and check invariants.md I1-I4 -- "
            "including that every old_ver chain DECREASES in ts. This is the "
            "check that can catch a timestamp mode ordering two machines' "
            "writes wrongly; --selftest cannot, because it runs at one client "
            "and one clock orders its own writes perfectly.") |
        lyra::opt(share_cache, "share_cache").optional()["--share-cache"](
            "Whether the client threads in this process SHARE one per-node "
            "cache (1, default) or each keep their own (0). 0 exists to "
            "separate the two effects of threading: with --threads > 1 it "
            "keeps the reduced per-process overhead while removing the "
            "sharing, so the two can be attributed separately. Meaningless at "
            "--threads 1, where there is nothing to share with.") |
        lyra::opt(client_threads, "client_threads").optional()["--threads"](
            "Client THREADS this process hosts, sharing one per-node cache "
            "(default 1, which is one client per process and one cache per "
            "client -- today's arrangement). Thread t takes proc id "
            "(-i) + t * --thread-stride and is pinned to physical core t.") |
        lyra::opt(thread_stride, "thread_stride").optional()["--thread-stride"](
            "Proc-id distance between the client threads this process hosts "
            "(default 1). Must be the number of CLIENT MACHINES, because "
            "run.sh strides co-located clients by that: preserving the exact "
            "proc ids keeps each client's arena stripe and quorum rotation "
            "identical, so only the cache sharing differs.") |
        lyra::opt(put_hint_hops_arg, "put_hint_hops").optional()["--put-hint-hops"](
            "Hop budget for the WRITE path alone. Defaults to whatever "
            "--hint-hops is, which is the shipped behaviour -- both paths "
            "symmetric. Pin it only to isolate one path: --hint-hops moves "
            "BOTH, so the write path's own contribution cannot be separated "
            "from that comparison alone.") |
        lyra::opt(run_ml_workload, "ml").optional()["--ml"] |
        lyra::opt(think_time, "think").optional()["--think"];

    auto result = cli.parse({argc, argv});
    if (!result || show_help ) {
        std::cerr << cli << std::endl;
        if (!result) {
            std::cerr << "Error in command line: " << result.errorMessage() << std::endl;
        }
        return 1;
    }

    if (warmup == UINT64_MAX) {
        warmup = iter_count < default_warmup ? iter_count : default_warmup;
    }
    // KEEPWARM MUST OUTLAST THE SLOWEST CLIENT'S WINDOW, not a quarter of it.
    //
    // It was (iter_count + warmup) / 4. Every client runs the same fixed
    // iter_count, so a FAST client finishes its window early and, once keepwarm
    // runs out, stops issuing entirely -- and the slow clients then measure a
    // system carrying less than the intended load. The per-client spread on
    // this cluster means a fast client must keep loading for a large fraction
    // of its own window after finishing. A quarter does not cover that;
    // iter_count covers up to a 2x spread.
    //
    // Costs more wall clock per cell, since keepwarm is added to warmup and
    // iter. That is the price of the sum over clients meaning anything.
    const uint64_t keepwarm           = iter_count;
    const uint64_t start_measurements = warmup;
    const uint64_t stop_measurements  = start_measurements + iter_count;
    const uint64_t total_iter_count   = stop_measurements + keepwarm;

    if (layout.majority == 0) {
        layout.majority = layout.num_servers / 2 + 1;
    }

    // The cache asserts layers > 1 && layers <= MAX_LAYERS in its constructor,
    // which would abort with no explanation. Fail with one instead.
    if (layout.cache_layers <= 1 || layout.cache_layers > DS_MAX_LAYERS) {
        std::cerr << "--layers must be in (1, " << DS_MAX_LAYERS
                  << "], got " << layout.cache_layers << std::endl;
        return 1;
    }

    if (put_hint_hops_arg >= 0) {
        if (put_hint_hops_arg > 64) {
            std::cerr << "--put-hint-hops must be 0..64, got "
                      << put_hint_hops_arg << std::endl;
            return 1;
        }
        layout.put_hint_hops = static_cast<uint32_t>(put_hint_hops_arg);
        std::cerr << "NOTE: --put-hint-hops pinned to "
                  << layout.put_hint_hops << " while --hint-hops is "
                  << layout.hint_hops
                  << ". The two paths are ASYMMETRIC in this run, which is an "
                     "experiment setting and not the shipped configuration."
                  << std::endl;
    }

    if (!ds::parseTsMode(ts_mode_name, layout.ts_mode)) {
        std::cerr << "--ts must be clock, tsc, faa, none or rangets; got '"
                  << ts_mode_name << "'" << std::endl;
        return 1;
    }
    // Both of these are legal and useful, and both are easy to select by
    // accident and then read the numbers as if they meant something else. Say
    // so on the way past rather than leaving it to the log reader.
    if (layout.ts_mode == ds::TsMode::Tsc && layout.num_clients > 1) {
        std::cerr << "WARNING: --ts tsc with " << layout.num_clients
                  << " clients. Raw rdtscp is not comparable across machines "
                     "(14.31 ppm measured between these nodes, i.e. 143 us of "
                     "skew over a 10 s run), so the old_ver chains this run "
                     "writes may be non-monotonic and ds_verify will say so."
                  << std::endl;
    }
    if (layout.ts_mode == ds::TsMode::RangeTs && layout.num_servers > 1) {
        // THE WRITE-BACK MOVES TO THE RANGE PATH, which changes which
        // operation pays for a divergent counter and which one stalls on a
        // dead replica. Stated here because the faa note below is the run
        // header a reader will otherwise compare against.
        //
        // A range claims its cut with the same round a faa-mode WRITE issues:
        // FaaTs on every replica, maximum over those that answered, REFUSED
        // below a majority, then faaCatchUpAddends raising the laggards before
        // the walk starts. So ranges tolerate one failure here, where in faa
        // mode a range's snapshot READ awaits all N and stalls.
        //
        // A write claims by READING the counter in the same chain, after its
        // publishing CAS -- majority required, same as faa -- so it tolerates
        // one failure too, and no longer advances a counter it may only have
        // reached partially. QuorumStats::ts_partial therefore stops being a
        // caveat on write-heavy runs and becomes one on scan-heavy runs.
        std::cerr << "NOTE: --ts rangets puts the counter's fetch-and-add on "
                     "the RANGE path and a plain read on the write path, both "
                     "over a majority of "
                  << layout.num_servers
                  << " replicas, so writes and ranges each tolerate one "
                     "failure. A write still costs two submissions: the "
                     "counter may not be read until the version is visible."
                  << std::endl;
    }
    if (layout.ts_mode == ds::TsMode::Faa && layout.num_servers > 1) {
        // THIS WARNING USED TO SAY THE COUNTER LIVES ON REPLICA 0 ONLY, and
        // that it therefore tolerated one failure unless it was server 0. That
        // was wrong, and wrong in the pessimistic direction -- it understated
        // our own failure model in every run header printed. The counter is
        // replicated: postTsCounter/faaTs address tsCounterAddrOf(remoteBuf())
        // per connection and post to every replica, and faaTimestamp() takes
        // the MAXIMUM over those that answered while refusing below a
        // majority. Quorum intersection is what makes that ordered: a writer
        // increments every replica in its quorum, any later writer's quorum
        // shares one of them, so the later writer observes a strictly larger
        // maximum. Uniqueness for concurrent writers comes from the 16-bit
        // client tiebreak in tsFromFaa.
        //
        // What IS weaker is the snapshot read, and only for liveness:
        // postTsCounter posts to all N and postedExactly(N) makes the driver
        // await all N completions, so one unreachable replica stalls a range's
        // snapshot rather than being outvoted. Safety is unaffected -- a
        // maximum over all N is a maximum over a majority -- and point
        // operations are unaffected, since only ranges take a snapshot.
        std::cerr << "NOTE: --ts faa replicates the counter on all "
                  << layout.num_servers
                  << " servers and stamps from the maximum over a majority, so"
                     " writes tolerate one failure. Range SNAPSHOTS currently"
                     " await all "
                  << layout.num_servers
                  << " counter reads, so a failed replica stalls ranges"
                     " (liveness only, not safety)."
                  << std::endl;
    }

    // Allocation never reclaims, so a stripe that is too small stops the run
    // partway with no way to recover. Fail now, with the arithmetic shown,
    // rather than mid-benchmark.
    if (layout.nodes_per_client == 0 || layout.vecs_per_client == 0) {
        std::cerr << "--nodes-per-client and --vecs-per-client must be positive"
                  << std::endl;
        return 1;
    }
    {
        auto gib = [](size_t b) {
            return static_cast<double>(b) / (1024.0 * 1024.0 * 1024.0);
        };
        std::cout << "Node arena:   " << layout.nodeArenaNodes() << " nodes, "
                  << gib(layout.nodeArenaSize()) << " GiB per memory server"
                  << std::endl;
        // The vector arena is the one that binds: every write allocates a
        // vector and nothing is reclaimed (invariants.md §5 defers epoch GC),
        // so this caps total writes for the whole run.
        std::cout << "Vector arena: " << layout.vecArenaVecs() << " vectors, "
                  << gib(layout.vecArenaSize()) << " GiB per memory server"
                  << " -- caps writes at " << layout.vecs_per_client
                  << " per client for the whole run" << std::endl;
    }

    // --cache 1 against a DS_CACHE_ENABLED=0 build cannot be honoured: the
    // cache is not compiled in. This used to be accepted and ignored, so every
    // artifact script has been passing a flag that did nothing.
    if (layout.consult_cache && !DS_CACHE_ENABLED) {
        std::cerr << "--cache 1 requires a build with DS_CACHE_ENABLED=1 "
                     "(see src/CMakeLists.txt)" << std::endl;
        return 1;
    }

    // A cache walk with no cache to walk is a user error, not a silent no-op.
    // It would run, fall back to the serial walk on every range, and report a
    // number labelled --cache-walk that measures the thing --cache-walk exists
    // to replace.
    if (client_threads < 1 || client_threads > 64) {
        std::cerr << "--threads must be 1..64, got " << client_threads
                  << std::endl;
        return 1;
    }
    if (thread_stride < 1) {
        std::cerr << "--thread-stride must be >= 1, got " << thread_stride
                  << std::endl;
        return 1;
    }
    if (share_cache != 0 && share_cache != 1) {
        std::cerr << "--share-cache must be 0 or 1, got " << share_cache
                  << std::endl;
        return 1;
    }
    if (client_threads > 1 &&
        layout.num_clients % static_cast<uint64_t>(client_threads) != 0) {
        // REFUSED, because the failure is a HANG. The measure-start barrier
        // counts client PROCESSES, derived as num_clients / threads; if that
        // does not divide, the integer division truncates, the barrier waits
        // for a count no one will reach, and the run sits there until the
        // harness times it out with nothing to say about why.
        std::cerr << "--threads " << client_threads << " does not divide "
                  << layout.num_clients << " clients: the measure-start "
                     "barrier counts processes and would wait forever."
                  << std::endl;
        return 1;
    }
    if (client_threads > 1 && client_log_dir.empty()) {
        // REFUSED, NOT WARNED. Without it the T clients' output interleaves
        // into one stdout, the harness finds one log where it expects T, and
        // every cell reads as PARTIAL -- a configuration mistake that would
        // look like a cluster fault.
        std::cerr << "--threads " << client_threads
                  << " needs --client-log-dir: each client thread writes its "
                     "own client<N>.txt there, and the sweeps read one log "
                     "per client." << std::endl;
        return 1;
    }
    if (client_threads > 1) {
        // Say it out loud: a run that shares a cache is not comparable with one
        // that does not, and the whole point is to tell them apart.
        std::cerr << "NOTE: hosting " << client_threads
                  << " client threads in this process, proc ids "
                  << proc_id << " step " << thread_stride << ", "
                  << (share_cache ? "SHARING one per-node cache"
                                  : "each with its OWN cache (sharing off)")
                  << "." << std::endl;
    } else if (share_cache == 0) {
        std::cerr << "NOTE: --share-cache 0 with --threads 1 changes nothing; "
                     "there is no co-located thread to share with."
                  << std::endl;
    }

    bool const cache_walk_explicit = cache_walk_arg >= 0;
    if (cache_walk_explicit) layout.cache_walk = cache_walk_arg != 0;

    if (layout.cache_walk && (!DS_CACHE_ENABLED || !layout.consult_cache)) {
        if (cache_walk_explicit) {
            // ASKED FOR, AND IMPOSSIBLE. Refuse rather than run something
            // other than what was requested.
            std::cerr << "--cache-walk 1 requires DS_CACHE_ENABLED=1 and "
                         "--cache 1: the chain comes from the cache, so "
                         "without one every range silently falls back to the "
                         "serial walk"
                      << std::endl;
            return 1;
        }
        // NOT ASKED FOR, JUST THE DEFAULT. A --cache 0 arm is a legitimate
        // baseline and must still run; the walk has nothing to walk, so turn
        // it off and say so. Refusing here would have killed every
        // disco-skip-cache0 arm in the A-D sweeps the moment the default
        // flipped, and the sweep would have reported CRASH-OR-BAD-ARGS.
        layout.cache_walk = false;
        std::cerr << "NOTE: --cache-walk defaults on but needs the cache; "
                     "running with it OFF because this arm has --cache 0."
                  << std::endl;
    }

    if(run_ml_workload){
        // The ML workload repurposes max_range as a CLIENT count, not a scan
        // length, so it overrides whatever the derivation produced. Kept after
        // the derivation rather than before it so this reads as the override
        // it is.
        layout.max_range = layout.num_clients; 
    } else if (layout.max_range == 0) {
        uint64_t const derived = deriveMaxRange(workload);
        // A workload with no maxscanlength has no scans, so the cap is never
        // consulted -- but bulkBufsSize() still multiplies by it, and a zero
        // would register a zero-length buffer. 10 is the historical default and
        // is the right floor for exactly the workloads that do not care.
        layout.max_range = derived != 0 ? derived : 10;
        std::cout << "maxrange:     " << layout.max_range
                  << (derived != 0 ? " (from the workload's maxscanlength)"
                                   : " (workload has no scans; buffer floor)")
                  << std::endl;
    } else {
        uint64_t const derived = deriveMaxRange(workload);
        // A PINNED VALUE BELOW WHAT THE WORKLOAD ASKS FOR IS REFUSED, not
        // applied. Truncating every scan produces a plausible throughput
        // number for a workload nobody requested, and the only visible trace is
        // entries-per-range. Same reasoning as the --ts none + scans refusal
        // below: a configuration error should cost one line, not a void sweep.
        if (derived != 0 && layout.max_range < derived) {
            std::cerr << "\n*** --maxrange " << layout.max_range
                      << " is below the workload's maxscanlength of " << derived
                      << ".\n*** Every scan would be truncated and the run "
                         "would measure a workload\n*** that was never asked "
                         "for. Raise it, or leave it unset to derive it."
                      << std::endl;
            return 1;
        }
    }


    auto num_proc  = layout.num_clients + layout.num_servers;
    bool is_client = proc_id > layout.num_servers;

    std::cout << "Experiment: << ";

    if (proc_id > num_proc) {
        std::cerr << "Invalid process id: " << proc_id
                  << " > " << num_proc << std::endl;
        return 1;
    }

    if (is_client) {
        std::cout << "Workload: " << workload << std::endl;
        // RECORD THE CONFIGURATION IN THE LOG, not just in the shell history
        // that launched it. The timestamp mode was printed only by the
        // --selftest path, so a benchmark log carried no record of which clock
        // produced its old_ver chains -- and `clock` vs `tsc` vs `faa` changes
        // both the per-write round-trip count and what the numbers mean. A
        // result whose configuration has to be reconstructed from a script is
        // a result nobody can check.
        if (layout.cas_as_write) {
        std::cerr << "\n*** --unsafe-cas-as-write IS ON. Every CAS is a blind "
                     "RDMA WRITE.\n"
                     "*** THIS STORE IS NOT LINEARIZABLE AND LOSES UPDATES: a "
                     "write cannot detect\n"
                     "*** that another writer published first, so concurrent "
                     "writers all believe\n"
                     "*** they won and versions are silently dropped. Split "
                     "link fields and the\n"
                     "*** tail word are equally unprotected.\n"
                     "*** It exists to measure what the ATOMIC VERB costs, "
                     "holding message count,\n"
                     "*** byte count, round trips and the algorithm fixed. NO "
                     "NUMBER FROM THIS RUN\n"
                     "*** DESCRIBES A CORRECT STORE. Expect failures and a "
                     "verifier that objects.\n"
                  << std::endl;
    }
    uint32_t const ops_put_hops =
        layout.put_hint_hops == ds::Layout::kPutHopsFollowGet
            ? layout.hint_hops
            : layout.put_hint_hops;
    std::cout << "cas-as-write: " << (layout.cas_as_write ? "ON (UNSAFE)" : "off")
              << std::endl;
    std::cout << "timestamps:   " << ds::tsModeName(layout.ts_mode)
                  << std::endl;
        std::cout << "cache:        " << (layout.consult_cache ? "on" : "off")
                  << "  cache-walk: " << (layout.cache_walk ? "on" : "off")
                  << "\n"
                  << "  batched-walk: "
                  << (layout.batched_walk ? "on" : "off") << "\n"
                  << "  offset-hint: " << (layout.offset_hint ? "on" : "off")
                  << "  async: " << layout.async_parallelism
                  << "  maxrange: " << layout.max_range
                  << "  latency: " << (layout.measure_latency ? "on" : "off")
                  << "\n"
                  // BOTH HOP BUDGETS AND THE GEOMETRY, so a log says what it
                  // ran with. put-hint-hops follows --hint-hops unless pinned,
                  // and a run where the two differ is an experiment rather
                  // than the shipped configuration -- which a reader cannot
                  // tell from a banner that prints only one of them. The index
                  // fan-out is compile-time (DS_IDX_EXP), so it is otherwise
                  // invisible in the logs entirely, and a sweep comparing two
                  // builds has no record of which geometry it measured.
                  << "  hint-hops: " << layout.hint_hops
                  << " (get) / " << ops_put_hops
                  << " (put)"
                  << "  idx-exp: " << ds::kIdxExp
                  << "  node-capacity: " << ds::kNodeCapacity
                  << "\n"
                  << "  client-threads: " << client_threads
                  << "  cache: "
                  << (client_threads > 1
                          ? (share_cache ? "shared per node" : "per thread")
                          : "per process")
                  << std::endl;
    }

    // ─── Device + port ─────────────────────────────────────────────
    ctrl::Devices d;
    ctrl::OpenDevice od;
    auto& available_devices = d.list();
    // WAS target_index = 0, under a comment claiming index 0 "corresponds to
    // the 3rd device (uverbs2 or mlx5_2)" -- which it does not. On the r320
    // nodes there is only mlx4_0 so it did not matter; on the r650 nodes
    // index 0 is mlx5_0, the ACTIVE 25G management NIC, while the experiment
    // LAN is mlx5_2 at 100G. That picks a working but four-times-slower
    // fabric and reports nothing wrong. See rdma_device.hpp.
    size_t const target_index = rdmasel::pickDevice(available_devices);
    od = std::move(available_devices[target_index]);

    std::cout << od.name() << " " << od.devName() << " "
            << ctrl::OpenDevice::typeStr(od.nodeType()) << " "
            << ctrl::OpenDevice::typeStr(od.transportType()) << std::endl;

    ctrl::ResolvedPort resolved_port(od);
    auto binded = resolved_port.bindTo(0);
    if (!binded) {
        throw std::runtime_error("Couldn't bind the device.");
    }
    std::cout << "Binded successfully (port_id, port_lid) = ("
                << +resolved_port.portId() << ", " << +resolved_port.portLid()
                << ")" << std::endl;

                
    // ─── Control block & MR ────────────────────────────────────────
    ctrl::ControlBlock cb(resolved_port);
    cb.registerPd("primary");

   {
        // The server region holds both arenas, laid out identically on every
        // replica. The legacy register array shares it while that path still
        // exists, so the server takes whichever is larger rather than assuming
        // the skip vector has displaced it yet.
        size_t const server_size =
            std::max(layout.registerRegionSize(), layout.serverSize());
        // ONE STRIPE PER CLIENT THREAD. Every client's scratch buffers are
        // addressed off Layout::client_local_region, so T threads in one
        // process need T disjoint stripes -- otherwise they read and write one
        // another's read buffers and CAS scratch, which corrupts silently
        // rather than failing. At --threads 1 this is exactly one stripe and
        // the arithmetic is the identity.
        size_t const client_size =
            layout.totalClientSize() * static_cast<size_t>(client_threads);
        size_t const allocated_size = is_client ? client_size : server_size;
        cb.allocateBuffer("shared-buf", allocated_size, 64);
        std::cout << (is_client ? "Client" : "Server") << " region: "
                  << allocated_size << " bytes";
        if (is_client && client_threads > 1) {
            std::cout << " (" << client_threads << " stripes of "
                      << layout.totalClientSize() << "B)";
        }
        if (!is_client) {
            std::cout << " (" << layout.nodeArenaNodes() << " nodes of "
                      << sizeof(ds::NodeRecord) << "B + "
                      << layout.vecArenaVecs() << " vectors of "
                      << sizeof(ds::VecRecord) << "B)";
        }
        std::cout << std::endl;
    }

    cb.registerMr(
        "shared-mr", "primary", "shared-buf",
        ctrl::ControlBlock::LOCAL_READ  | ctrl::ControlBlock::LOCAL_WRITE |
        ctrl::ControlBlock::REMOTE_READ | ctrl::ControlBlock::REMOTE_WRITE |
        ctrl::ControlBlock::REMOTE_ATOMIC);

    // ─── The identities this process owns ─────────────────────────────────
    //
    // One per client thread. run.sh strides a node's clients by the client
    // machine count, so these are proc_id, proc_id + stride, ... and NOT
    // contiguous -- preserving the exact ids keeps each client's arena stripe
    // and quorum rotation identical to the process-per-client arrangement.
    // At --threads 1 this is the single id it has always been.
    std::vector<ds::ProcId> my_ids;
    my_ids.reserve(static_cast<size_t>(client_threads));
    for (int64_t t = 0; t < client_threads; ++t) {
        my_ids.push_back(static_cast<ds::ProcId>(
            proc_id + static_cast<ds::ProcId>(t * thread_stride)));
    }

    // A CQ PER (IDENTITY, REMOTE), not per remote. The name used to be
    // "cq{id}", which with several identities in one process would hand every
    // thread the SAME completion queue for a given remote -- so the threads
    // would silently share completion queues, reintroducing exactly the
    // cross-thread coupling that keeping queue pairs per-thread exists to
    // avoid, and making a result about the shared cache unreadable. At
    // --threads 1 the suffix is "-0" and there is one CQ per remote as before.
    auto cqName = [](ds::ProcId mine, ds::ProcId remote, size_t slot) {
        (void)mine;
        return fmt::format("cq{}-{}", remote, slot);
    };

    std::vector<std::vector<ds::ProcId>> remotes_of(my_ids.size());
    for (size_t t = 0; t < my_ids.size(); ++t) {
        for (ds::ProcId id = 1; id <= num_proc; id++) {
            if (id == my_ids[t]) continue;
            remotes_of[t].push_back(id);
        }
        for (auto const& id : remotes_of[t]) {
            cb.registerCq(cqName(my_ids[t], id, t));
        }
    }
    // Kept for the code below that still speaks of a single identity.
    std::vector<ds::ProcId> const& remote_ids = remotes_of[0];

    auto local_region = cb.mr("shared-mr").addr;
    if (is_client) {
        // Thread 0's stripe. Each client thread is handed its own base below,
        // computed the same way; this keeps the single-threaded path reading
        // exactly the address it always has.
        layout.client_local_region = local_region;
    } else {
        // Zeroing is not by itself a valid empty structure -- a zeroed node has
        // next_k_min = 0 and so covers no key at all. The head records are
        // written properly during bootstrap; this only makes the arena
        // deterministic rather than whatever the allocator handed us.
        std::memset(reinterpret_cast<void*>(local_region), 0,
                    std::max(layout.registerRegionSize(), layout.serverSize()));
    }

    // ─── Connection exchange, PHASE BY PHASE ACROSS ALL IDENTITIES ────────
    //
    // NOT one exchanger at a time. waitReadyAll blocks until EVERY participant
    // has announced, and this process now holds several of them: announcing
    // identity 0 and then waiting would wait for identities 1..T-1 that this
    // same thread has not announced yet, and the run would hang in setup with
    // no output. So every identity announces, then every identity waits, then
    // every identity connects.
    //
    // All of it runs on the main thread before any client thread starts, so
    // nothing here depends on dory's ControlBlock or the memstore being
    // thread-safe -- only the steady-state RDMA path is concurrent, and there
    // each thread touches only its own queue pairs.
    //
    // At --threads 1 the loops run once and the sequence is exactly what it
    // was.
    auto& store = memstore::MemoryStore::getInstance();
    std::vector<std::unique_ptr<dory::conn::RcConnectionExchanger<ds::ProcId>>>
        ces;
    ces.reserve(my_ids.size());
    for (size_t t = 0; t < my_ids.size(); ++t) {
        ces.push_back(
            std::make_unique<dory::conn::RcConnectionExchanger<ds::ProcId>>(
                my_ids[t], remotes_of[t], cb));
        for (auto const& id : remotes_of[t]) {
            auto cq = cqName(my_ids[t], id, t);
            ces[t]->configure(id, "primary", "shared-mr", cq, cq);
        }
    }

    for (auto& c : ces) {
        c->announceAll(store, "qp");
        c->announceReady(store, "qp", "prepared");
    }
    for (auto& c : ces) {
        c->waitReadyAll(store, "qp", "prepared");
        c->unannounceReady(store, "qp", "finished");
    }
    for (auto& c : ces) {
        c->connectAll(
            store, "qp",
            ctrl::ControlBlock::LOCAL_READ  | ctrl::ControlBlock::LOCAL_WRITE |
            ctrl::ControlBlock::REMOTE_READ | ctrl::ControlBlock::REMOTE_WRITE |
            ctrl::ControlBlock::REMOTE_ATOMIC);
    }
    for (auto& c : ces) c->announceReady(store, "qp", "connected");
    for (auto& c : ces) c->waitReadyAll(store, "qp", "connected");
    for (auto& c : ces) {
        c->unannounceAll(store, "qp");
        c->unannounceReady(store, "qp", "prepared");
    }
    auto& ce = *ces[0];

    if (is_client) {
        // ─── One thread per client ────────────────────────────────────────
        //
        // CACHES: one shared per node by default, or one per thread with
        // --share-cache 0. The second exists so the two effects of threading
        // can be told apart -- the launch also removes most of the
        // per-process RDMA setup, and without a per-thread-cache arm a win
        // would be unattributable.
        //
        // REGION: thread t gets its own stripe. Every client addresses its
        // scratch buffers off Layout::client_local_region, so a shared base
        // would have the threads overwriting one another's read buffers and
        // CAS scratch -- silent corruption, not an error.
        //
        // At --threads 1 this runs runClient once, on this thread, with the
        // same id, the same exchanger and the same region base as before.
#if DS_CACHE_ENABLED
        size_t const n_caches =
            share_cache ? 1u : static_cast<size_t>(client_threads);
        std::vector<std::unique_ptr<ds::NodeCache>> caches;
        caches.reserve(n_caches);
        for (size_t i = 0; i < n_caches; ++i) {
            caches.push_back(
                std::make_unique<ds::NodeCache>(layout.cache_layers));
        }
#endif
        std::vector<int> rcs(static_cast<size_t>(client_threads), 0);

        // Collectives go through this, because libmemcached is not
        // thread-safe. client_processes is what the measure-start barrier
        // counts: this process increments once on behalf of its threads, so
        // the total must be the number of client PROCESSES. At one thread per
        // process that is the client count, exactly as before.
        size_t const client_processes =
            static_cast<size_t>(layout.num_clients) /
            static_cast<size_t>(client_threads);
        ClientSync sync{static_cast<size_t>(client_threads), store, ces,
                        client_processes};

        auto one_client = [&](size_t t) {
            // Its own stripe of the registered region.
            ds::Layout my_layout = layout;
            my_layout.client_local_region =
                local_region + t * layout.totalClientSize();

            // Its own log, named exactly as the harness expects, so a thread
            // is indistinguishable from a process to every parser. Only
            // opened when this process hosts more than one client: at
            // --threads 1 invoker.sh still tees stdout to that same path, and
            // opening it here as well would have two writers on one file.
            std::unique_ptr<std::ofstream> log;
            std::unique_ptr<ds::ScopedClientOut> redirect;
            if (client_threads > 1 && !client_log_dir.empty()) {
                log = std::make_unique<std::ofstream>(
                    ds::clientLogPath(client_log_dir, my_ids[t],
                                      layout.num_servers));
                if (log->is_open()) {
                    redirect = std::make_unique<ds::ScopedClientOut>(*log);
                } else {
                    std::cerr << "WARNING: client " << my_ids[t]
                              << " could not open its log under "
                              << client_log_dir
                              << "; its output will be mixed into stdout and "
                                 "the sweep will read this cell as PARTIAL."
                              << std::endl;
                }
            }

            if (client_threads > 1) {
                // Placed by us now, because numactl -C can only pin a whole
                // process. Physical cores are 0..7 here with siblings at +8,
                // so a stride of 1 is right -- see pinThreadToCore.
                pinThreadToCore(static_cast<unsigned>(t));
            }

#if DS_CACHE_ENABLED
            ds::NodeCache& my_cache = *caches[share_cache ? 0 : t];
            rcs[t] = runClient(my_layout, my_ids[t], *ces[t], store, sync, my_cache,
                               workload, ycsb_path, iter_count, warmup,
                               keepwarm, start_measurements, stop_measurements,
                               total_iter_count, detailed, run_selftest,
                               verify_after, run_ml_workload, think_time);
#else
            rcs[t] = runClient(my_layout, my_ids[t], *ces[t], store, sync,
                               workload, ycsb_path, iter_count, warmup,
                               keepwarm, start_measurements, stop_measurements,
                               total_iter_count, detailed, run_selftest,
                               verify_after, run_ml_workload, think_time);
#endif
            if (client_threads > 1) {
                // Each client ends its own log the way the harness expects:
                // wait_for_logs counts the files carrying this marker.
                ds::clientOut() << "###DONE###" << std::endl;
                ds::clientOut().flush();
            }
        };

        if (client_threads == 1) {
            one_client(0);
        } else {
            std::vector<std::thread> threads;
            threads.reserve(static_cast<size_t>(client_threads));
            for (int64_t t = 0; t < client_threads; ++t) {
                threads.emplace_back(one_client, static_cast<size_t>(t));
            }
            for (auto& th : threads) th.join();
        }

        // ONE FAILING CLIENT MUST STILL FAIL THE PROCESS. These returns used
        // to exit main directly; swallowing them in a thread nobody checked
        // would turn a broken run into a quiet one.
        for (size_t t = 0; t < rcs.size(); ++t) {
            if (rcs[t] != 0) {
                std::cerr << "client " << my_ids[t] << " failed with "
                          << rcs[t] << std::endl;
                return rcs[t];
            }
        }
        int const client_rc = 0;
        if (client_rc != 0) return client_rc;
    } else {
        std::cout << "Server " << proc_id << " online." << std::endl;
        ce.announceReady(store, "qp", "initialized");
        ce.announceReady(store, "qp", "finished");
        ce.waitReadyAll(store, "qp", "finished");
        ce.unannounceReady(store, "qp", "initialized");
        std::cout << "Closing server connection." << std::endl;
    }

    ce.unannounceReady(store, "qp", "connected");
    std::cout << "###DONE###" << std::endl;
    return 0;
}