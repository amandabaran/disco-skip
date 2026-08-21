#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <iostream>
#include <memory>
#include <vector>
#include <chrono>
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

// Chimera headers
#include "layout.hpp"
#include "chimera_client.hpp"
#include "op_future.hpp"
#include "register.hpp"
#include "range_future.hpp" // Integrated your RangeFuture class definition

using namespace dory;
using namespace dory::conn;

const uint64_t default_warmup     = 1'000'000;
const uint64_t default_iter_count = 1'000'000;
const uint64_t default_keepwarm   =   500'000;

namespace chimera {
    bool cache_enabled = false;
    bool writeback_enabled = false;
}

enum OpType { OpGet, OpPut, OpScan };

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
void run_ml_prog_tracker_workload(
    chimera::ChimeraClient& client, 
    uint64_t global_thread_id, 
    uint64_t num_registers,
    uint64_t ops_to_run);

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
    chimera::ChimeraClient& client, 
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
            client.getFreeFuture().doPut(global_thread_id, 1, false);
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
            client.getFreeFuture().doPut(global_thread_id, progress_counter++, measuring);
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
    chimera::ProcId proc_id = 0;
    bool show_help = false;

    chimera::Layout layout;
    layout.num_clients       = 1;
    layout.num_servers       = 1;
    layout.async_parallelism = 1;
    layout.num_registers     = 100000;
    layout.max_range         = 10;
    layout.majority          = 0;

    // Set by client at runtime after MR is allocated.
    // (Same pattern as swarm-kv: see Layout::client_local_region)
    uintptr_t client_local_region = 0;

    float rq_p = 0.0f, get_p = 0.5f, put_p = 0.5f;
    uint64_t iter_count = default_iter_count;
    uint64_t warmup     = UINT64_MAX;

    bool run_ml_workload = false;
    uint64_t think_time = 0;

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
        lyra::opt(rq_p,  "rq_p" ).optional()["--rq"] |
        lyra::opt(get_p, "get_p").optional()["--get"] |
        lyra::opt(put_p, "put_p").optional()["--put"] |
        lyra::opt(workload, "workload").optional()["-w"]["--workload"] |
        lyra::opt(ycsb_path, "ycsb_path").optional()["-y"]["--ycsbpath"] |
        lyra::opt(detailed, "detailed").optional()["-d"]["--detailed"] |
        lyra::opt(iter_count, "iter_count").optional()["-I"]["--iter_count"] |
        lyra::opt(warmup,     "warmup")    .optional()["-W"]["--warmup"] |
        lyra::opt(chimera::cache_enabled, "cache")
            ["--cache"]("Enable or disable the Chimera cache system (1 or 0)") |
        lyra::opt(chimera::writeback_enabled, "writeback")
            ["--writeback"]("Enable or disable writeback in CAS-ABD protocol (1 or 0)") |
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
        size_t allocated_size = is_client ? layout.clientSize() : layout.serverSize();
        cb.allocateBuffer("shared-buf", allocated_size, 64);
    }

    cb.registerMr(
        "shared-mr", "primary", "shared-buf",
        ctrl::ControlBlock::LOCAL_READ  | ctrl::ControlBlock::LOCAL_WRITE |
        ctrl::ControlBlock::REMOTE_READ | ctrl::ControlBlock::REMOTE_WRITE |
        ctrl::ControlBlock::REMOTE_ATOMIC);

    std::vector<chimera::ProcId> remote_ids;
    for (chimera::ProcId id = 1; id <= num_proc; id++) {
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
        std::memset(reinterpret_cast<void*>(local_region), 0, layout.serverSize());
    }

    // ─── Connection exchange ───────────────────────────────────────
    auto& store = memstore::MemoryStore::getInstance();
    dory::conn::RcConnectionExchanger<chimera::ProcId> ce(proc_id, remote_ids, cb);

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
        chimera::ChimeraClient client{layout, ce, proc_id};

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
            for (size_t kvIndex = 0; kvIndex < inserts.size(); kvIndex++) {
                client.finishAllFutures();
                
                // Extract numerical representation of key string for Chimera's register mapping
                uint64_t target_reg = std::stoull(inserts[kvIndex].first.substr(4)) % layout.num_registers;
                // std::cout << "Inserting register: " << target_reg << std::endl;
                client.getFreeFuture().doPut(target_reg, 69, false);
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
            std::cout << "Done. Packed " << operations.size() << " operations structural states." << std::endl;

            std::cout << "Waiting for the initialization of other clients... " << std::flush;
            ce.announceReady(store, "qp", "initialized");
            ce.waitReadyAll(store, "qp", "initialized");
            std::cout << "Done." << std::endl;

            std::cout << "Running the benchmark (YCSB Swarm Engine)... " << std::endl;

            std::chrono::steady_clock::time_point start_time;
            std::chrono::steady_clock::time_point end_time;
            bool measuring = false;
            size_t skipped = 0;

            for (size_t i = 0; i < total_iter_count; i++) {
                auto& op = operations[(i + skipped) % operations.size()];

                if (i == start_measurements) {
                    measuring = true;
                    start_time = std::chrono::steady_clock::now();
                } else if (i == stop_measurements) {
                    measuring = false;
                    end_time = std::chrono::steady_clock::now();
                }
                switch (op.type) {
                    case OpGet: {
                        client.getFreeFuture().doGet(op.target_reg, measuring);
                        break;
                    }
                    case OpScan: {
                        uint64_t len = op.scan_len;
                        if (len > layout.max_range) len = layout.max_range;
                        if (op.target_reg + len > layout.num_registers) len = layout.num_registers - op.target_reg;

                        uint64_t end_reg = op.target_reg + len - 1;
                        client.getFreeRangeFuture().doRange(op.target_reg, end_reg, measuring);
                        break;
                    }
                    case OpPut: {
                        client.getFreeFuture().doPut(op.target_reg, 69, measuring);
                        break;
                    }
                    default:
                        break;
                }
                client.finishAllFutures();
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