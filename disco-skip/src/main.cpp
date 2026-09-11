#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <iostream>
#include <memory>
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

std::vector<YcsbOp> operations;

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

int main(int argc, char** argv) {
    ds::ProcId proc_id = 0;
    bool show_help = false;

    ds::Layout layout;
    layout.num_clients       = 1;
    layout.num_servers       = 1;
    layout.async_parallelism = 1;
    layout.num_registers     = 100000;
    layout.max_range         = 10;
    layout.majority          = 0;
    layout.cache_layers      = 4;
    layout.nodes_per_client  = 1 << 16;
    layout.vecs_per_client   = 1 << 18;
    layout.offset_hint       = true;
    layout.consult_cache     = DS_CACHE_ENABLED ? true : false;
    layout.writeback         = DS_REG_WRITEBACK_ENABLED ? true : false;
    layout.ts_mode           = ds::TsMode::Clock;

    // Set by client at runtime after MR is allocated.
    // (Same pattern as swarm-kv: see Layout::client_local_region)
    uintptr_t client_local_region = 0;

    float rq_p = 0.0f, get_p = 0.5f, put_p = 0.5f;
    uint64_t iter_count = default_iter_count;
    uint64_t warmup     = UINT64_MAX;

    bool run_ml_workload = false;
    bool run_selftest = false;
    uint64_t think_time = 0;

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
            .optional()["--maxrange"] |
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
        lyra::opt(ts_mode_name, "ts_mode").optional()["--ts"](
            "Timestamp source: clock|tsc|faa (default clock). clock is a "
            "disciplined CLOCK_REALTIME and is the only mode correct at more "
            "than one client that also keeps the fault tolerance replication "
            "provides. tsc is raw rdtscp -- cheapest, single-client only. faa "
            "is an RDMA fetch-and-add on a global counter: no timing "
            "assumption, at one extra round trip per write and one point of "
            "failure. See ds_ts.hpp.") |
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
    const uint64_t keepwarm           = (iter_count + warmup) / 4;
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

    if (!ds::parseTsMode(ts_mode_name, layout.ts_mode)) {
        std::cerr << "--ts must be clock, tsc or faa; got '" << ts_mode_name
                  << "'" << std::endl;
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
    if (layout.ts_mode == ds::TsMode::Faa && layout.num_servers > 1) {
        std::cerr << "WARNING: --ts faa holds the counter on replica 0 only, so "
                     "this run tolerates one memory node failing UNLESS it is "
                     "server 0. That is a weaker failure model than the 2-of-"
                  << layout.num_servers << " commit otherwise gives."
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

    if(run_ml_workload){
        layout.max_range = layout.num_clients; 
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
    }

    // ─── Device + port ─────────────────────────────────────────────
    ctrl::Devices d;
    ctrl::OpenDevice od;
    auto& available_devices = d.list();
    size_t target_index = 0; // This corresponds to the 3rd device (uverbs2 or mlx5_2)

    if (available_devices.size() > target_index) {
        od = std::move(available_devices[target_index]);
        std::cout << "Selected device: " << od.devName() << std::endl;
    } else {
        std::cerr << "Error: Device index " << target_index << " not available." << std::endl;
        std::cerr << "Available devices: ";
        for (auto const& dev : available_devices) std::cerr << dev.devName() << " ";
        std::cerr << std::endl;
        return 1;
    }

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
        size_t const allocated_size = is_client ? layout.totalClientSize() : server_size;
        cb.allocateBuffer("shared-buf", allocated_size, 64);
        std::cout << (is_client ? "Client" : "Server") << " region: "
                  << allocated_size << " bytes";
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

    std::vector<ds::ProcId> remote_ids;
    for (ds::ProcId id = 1; id <= num_proc; id++) {
        if (id == proc_id) continue;
        remote_ids.push_back(id);
    }

    for (auto const& id : remote_ids) {
        cb.registerCq(fmt::format("cq{}", id));
    }

    auto local_region = cb.mr("shared-mr").addr;
    if (is_client) {
        layout.client_local_region = local_region;
    } else {
        // Zeroing is not by itself a valid empty structure -- a zeroed node has
        // next_k_min = 0 and so covers no key at all. The head records are
        // written properly during bootstrap; this only makes the arena
        // deterministic rather than whatever the allocator handed us.
        std::memset(reinterpret_cast<void*>(local_region), 0,
                    std::max(layout.registerRegionSize(), layout.serverSize()));
    }

    // ─── Connection exchange ───────────────────────────────────────
    auto& store = memstore::MemoryStore::getInstance();
    dory::conn::RcConnectionExchanger<ds::ProcId> ce(proc_id, remote_ids, cb);

    for (auto const& id : remote_ids) {
        auto cq = fmt::format("cq{}", id);
        ce.configure(id, "primary", "shared-mr", cq, cq);
    }

    ce.announceAll(store, "qp");
    ce.announceReady(store, "qp", "prepared");
    ce.waitReadyAll(store, "qp", "prepared");
    ce.unannounceReady(store, "qp", "finished");

    ce.connectAll(
        store, "qp",
        ctrl::ControlBlock::LOCAL_READ  | ctrl::ControlBlock::LOCAL_WRITE |
        ctrl::ControlBlock::REMOTE_READ | ctrl::ControlBlock::REMOTE_WRITE |
        ctrl::ControlBlock::REMOTE_ATOMIC);

    ce.announceReady(store, "qp", "connected");
    ce.waitReadyAll(store, "qp", "connected");

    ce.unannounceAll(store, "qp");
    ce.unannounceReady(store, "qp", "prepared");

    if (is_client) {
        ds::DsClient client{layout, ce, proc_id};
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
            ce.announceReady(store, "qp", "initialized");
            ce.waitReadyAll(store, "qp", "initialized");

            int const rc = run_structure_selftest(state);
            state.reportCache();

            ce.announceReady(store, "qp", "finished");
            ce.waitReadyAll(store, "qp", "finished");
            ce.unannounceReady(store, "qp", "initialized");
            ce.unannounceReady(store, "qp", "connected");
            std::cout << "###DONE###" << std::endl;
            return rc;
        }

        // SWARM PATTERN: First client triggers initial data loading phase
        if (proc_id == (layout.num_servers + 1)) {
            std::cout << "Querying YCSB for the set of initial key-pairs... " << std::flush;
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
            std::cout << "Done." << std::endl;

            std::cout << "Inserting the initial key-pairs... " << std::flush;
            // Same seed rule as the measured phase: reproducible per client,
            // and different between clients so two do not make an identical
            // structural decision for the same key.
            std::mt19937_64 pop_rng(0xB0B ^ proc_id);
            for (size_t kvIndex = 0; kvIndex < inserts.size(); kvIndex++) {
                
                // Extract numerical representation of key string for the register mapping
                uint64_t target_reg = std::stoull(inserts[kvIndex].first.substr(4)) % layout.num_registers;
                // std::cout << "Inserting register: " << target_reg << std::endl;
                // Population, so heights are drawn: this is what builds the
                // index the measured phase then traverses. A populated
                // structure with no index would measure a linked list.
                uint32_t const ph = ds::drawHeight(
                    pop_rng, static_cast<uint32_t>(layout.cache_layers));
                client.getFreeFuture().doPut(target_reg, 69, ph, false);
            }
            client.finishAllFutures();
            std::cout << " Done." << std::endl;
        }

        if (run_ml_workload) {
            // Trackers are assigned based on a normalized structural index 
            // sequence. If this is client #1 (proc_id == num_servers + 1), it becomes ID 0 (Tracker).
            uint64_t global_thread_id = proc_id - layout.num_servers - 1;

            std::cout << "Running single-threaded ML tracker workload. ID=" << global_thread_id  << "Registers=" << layout.num_registers << "Clients=" << layout.num_clients << std::endl;
            
            // Sync with cluster deployment infrastructure
            ce.announceReady(store, "qp", "initialized");
            ce.waitReadyAll(store, "qp", "initialized");

            run_ml_prog_tracker_workload(client, global_thread_id,
                             layout.num_clients, iter_count,
                             think_time, store);

            ce.announceReady(store, "qp", "finished");
            ce.waitReadyAll(store, "qp", "finished");
            ce.unannounceReady(store, "qp", "initialized");
        } 
        else {
            std::cout << "Configuring Client " << proc_id << std::endl;
            
            // SWARM PATTERN: Load continuous execution operations
            std::cout << "Querying YCSB for the list of operations... " << std::flush;

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

                        // NO MODULO, unlike every other op here, and that is the
                        // whole point. YCSB draws run-phase insert keys from a
                        // counter that starts at recordcount, so `% num_registers`
                        // would fold them straight back onto keys the load phase
                        // already wrote -- turning every insert into an update and
                        // silently deleting the only difference between workload D
                        // or E and workload B or C. The skip vector takes an
                        // arbitrary uint64 key, so the raw value is used.
                        //
                        // BUDGET THE ARENA FOR THESE. An insert allocates a vector
                        // and may split, and nothing is reclaimed, so
                        // --vecs-per-client has to cover recordcount plus the
                        // inserts the run will issue.
                        uint64_t const insert_key = std::stoull(key.substr(4));
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
                std::cout << "Done. Packed " << operations.size()
                          << " operations: " << n_get << " read, " << n_put
                          << " update, " << n_scan << " scan, " << n_ins
                          << " insert." << std::endl;
            }

            std::cout << "Waiting for the initialization of other clients... " << std::flush;
            ce.announceReady(store, "qp", "initialized");
            ce.waitReadyAll(store, "qp", "initialized");
            std::cout << "Done." << std::endl;

            std::cout << "Running the benchmark (YCSB Swarm Engine)... " << std::endl;

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
                    measuring = true;
                    start_time = std::chrono::steady_clock::now();
                } else if (i == stop_measurements) {
                    // And drain before stopping it, so operations issued inside
                    // the window are paid for inside it. Without this the tail
                    // of the pipeline is free and the number is inflated by
                    // roughly async_parallelism operations.
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
                        // Still the register path: there is no skip-vector
                        // range (A10). A scan here measures the old structure,
                        // so a scan-heavy workload is not yet a disco-skip
                        // measurement.
                        uint64_t len = op.scan_len;
                        if (len > layout.max_range) len = layout.max_range;
                        if (op.target_reg + len > layout.num_registers) len = layout.num_registers - op.target_reg;

                        uint64_t end_reg = op.target_reg + len - 1;
                        client.getFreeRangeFuture().doRange(op.target_reg, end_reg, measuring);
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
            std::cout << "Done. Results:" << std::endl;

            client.reportStats(detailed);
            fmt::print("Local tput: {} kops\n",
                    iter_count * 1'000'000
                    / static_cast<uint64_t>((end_time - start_time).count()));
            fmt::print("Local duration: {}s\n", 
            static_cast<uint64_t>((end_time - start_time).count() / 1000000000));
            std::cout << std::flush;
            
            ce.announceReady(store, "qp", "finished");
            ce.waitReadyAll(store, "qp", "finished");
            ce.unannounceReady(store, "qp", "initialized");
        }
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