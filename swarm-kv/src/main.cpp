#include "rdma_device.hpp"
#include <memory>
#include <optional>
#include <thread>
#include <chrono>
#include <iostream>
#include <vector>
#include <string>

#include <lyra/lyra.hpp>

#include "race/client_index.hpp"
#include "race/server_index.hpp"

#include "latency.hpp"
#include "layout.hpp"
#include "range_lock.hpp"
#include "range_table_lock.hpp"
#include "main.hpp"
#include "oops_client.hpp"

#include <dory/shared/match.hpp>

/// Either lock, chosen at run time. Both expose acquireRange / acquireKey /
/// release, so the call sites do not branch; only construction does.
/// Virtual dispatch costs nothing measurable against an RDMA round trip.
template <class Layout>
class ScanLock {
 public:
  virtual ~ScanLock() = default;
  virtual void acquireRange(uint64_t start_key, uint64_t count) = 0;
  virtual void acquireKey(uint64_t key) = 0;
  virtual void release() = 0;
  virtual uint64_t acquires() const = 0;
  virtual uint64_t retries() const = 0;
};

template <class Layout>
class StripedScanLock : public ScanLock<Layout> {
 public:
  StripedScanLock(Layout const& l, dory::conn::ReliableConnection& rc,
                  uint64_t owner, uint64_t* scratch, uint64_t stripes)
      : impl_{l, rc, owner, scratch, stripes} {}
  void acquireRange(uint64_t s, uint64_t c) override { impl_.acquireRange(s, c); }
  void acquireKey(uint64_t k) override { impl_.acquireKey(k); }
  void release() override { impl_.release(); }
  uint64_t acquires() const override { return impl_.acquires(); }
  uint64_t retries() const override { return impl_.retries(); }
 private:
  dory::RangeLock<Layout> impl_;
};

template <class Layout>
class TableScanLock : public ScanLock<Layout> {
 public:
  TableScanLock(Layout const& l, dory::conn::ReliableConnection& rc,
                uint64_t owner, uint64_t* scratch, uint64_t slots)
      : impl_{l, rc, owner, scratch, slots} {}
  void acquireRange(uint64_t s, uint64_t c) override { impl_.acquireRange(s, c); }
  void acquireKey(uint64_t k) override { impl_.acquireKey(k); }
  void release() override { impl_.release(); }
  uint64_t acquires() const override { return impl_.acquires(); }
  uint64_t retries() const override { return impl_.retries(); }
  uint64_t validations() const { return impl_.validations(); }
 private:
  dory::RangeTableLock<Layout> impl_;
};

using namespace dory;
using namespace dory::conn;
using namespace conn;

const uint64_t default_warmup = 1'000'000;
const uint64_t default_iter_count = 1'000'000;
const uint64_t default_keepwarm = 500'000;

// Forward declaration to satisfy -Wmissing-declarations
void run_ml_prog_tracker_workload(
    OopsClient& client, 
    uint64_t global_thread_id, 
    uint64_t active_workers, 
    uint64_t ops_to_run);

/// The numeric part of a YCSB key ("user0000123" -> 123).
///
/// MUST agree with IncrementYcsbKey, which is what a SCAN walks: the stripes a
/// scan locks are derived from this, so if the two disagreed about key ordering
/// the lock would cover a different range than the scan reads -- a lock that is
/// held, and protects the wrong thing.
///
/// strtoull, not stoll: with insertorder=hashed YCSB keys run to ~9.2e18, which
/// is at the edge of a signed 64-bit integer.
inline uint64_t ycsbKeyToInt(const std::string& key) {
  size_t const digit = key.find_first_of("0123456789");
  if (digit == std::string::npos) return 0;
  return std::strtoull(key.c_str() + digit, nullptr, 10);
}

inline std::string IncrementYcsbKey(const std::string& key) {
    size_t non_digit = key.find_first_of("0123456789");
    if (non_digit == std::string::npos) return key + "0"; 
    
    std::string prefix = key.substr(0, non_digit);
    long long num = std::stoll(key.substr(non_digit));
    num++;
    
    std::string num_str = std::to_string(num);
    // Pad out zero configurations to match exact structural constraints of the KVS index
    int padding_needed = static_cast<int>(key.length() - prefix.length() - num_str.length());
    if (padding_needed > 0) {
        return prefix + std::string(padding_needed, '0') + num_str;
    }
    return prefix + num_str;
}

void run_ml_prog_tracker_workload(
    OopsClient& client, 
    uint64_t global_thread_id, 
    uint64_t active_workers, // Equal to layout.num_clients
    uint64_t ops_to_run) 
{
    // Lambda to generate key strings
    auto get_worker_key = [](uint64_t id) -> std::string {
        std::string id_str = std::to_string(id);
        return "worker_progress_" + std::string(8 - id_str.length(), '0') + id_str;
    };

    // ──────────────────────────────────────────────────────────────────────────
    // PRE-INSERTION PHASE
    // ──────────────────────────────────────────────────────────────────────────
    if (global_thread_id == 0) {
        std::cout << "[ML-INIT] Pre-inserting tracking keys into Swarm-KV... " << std::flush;
        
        for (uint64_t w = 1; w <= active_workers; ++w) {
            std::string k = get_worker_key(w);
            std::string initial_val = "1";
            std::string val_padded = initial_val + std::string(64 - initial_val.length(), '0');
            
            client.finishAllFutures();
            client.getFreeFuture().doInsert(k, val_padded);
        }
        client.finishAllFutures();
        std::cout << "Done. All keys initialized." << std::endl;
    }

    client.finishAllFutures(); 
    std::this_thread::sleep_for(std::chrono::seconds(2)); 

    // ──────────────────────────────────────────────────────────────────────────
    // --- WARMUP PHASE ---
    // ──────────────────────────────────────────────────────────────────────────
    auto warmup_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (std::chrono::steady_clock::now() < warmup_deadline) {
        if (global_thread_id == 0) {
            for (uint64_t w = 1; w <= active_workers; ++w) {
                try {
                    std::string k = get_worker_key(w);
                    client.getFreeFuture().doRead(k, false);
                } catch (...) {}
            }
            client.finishAllFutures(); 
        } else {
            std::string k = get_worker_key(global_thread_id);
            std::string val_str = "1";
            std::string val_padded = val_str + std::string(64 - val_str.length(), '0');
            client.getFreeFuture().doUpdate(k, val_padded, false);
            client.finishAllFutures();
        }
    }
    
    uint64_t progress_counter = 1;
    auto start_time = std::chrono::steady_clock::now();

    // ──────────────────────────────────────────────────────────────────────────
    // --- MAIN LOOP ---
    // ──────────────────────────────────────────────────────────────────────────
    for (uint64_t op_idx = 0; op_idx < ops_to_run; ++op_idx) {
        bool measuring = (op_idx % 1000 == 0); 

        if (global_thread_id == 0) {
            for (uint64_t w = 1; w <= active_workers; ++w) {
                try {
                    std::string k = get_worker_key(w);
                    client.getFreeFuture().doRead(k, measuring);
                } 
                catch (const std::runtime_error& e) {
                    // Ignore key-not-found states safely
                }
            }
            client.finishAllFutures();
        } else {
            std::string k = get_worker_key(global_thread_id);
            std::string val_str = std::to_string(progress_counter++);
            std::string val_padded = val_str + std::string(64 - val_str.length(), '0');
            
            client.getFreeFuture().doUpdate(k, val_padded, measuring);
            
            if (op_idx % 16 == 0) {
                client.finishAllFutures();
            }
        }
    }
    client.finishAllFutures();
    auto end_time = std::chrono::steady_clock::now();

    uint64_t duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
    double duration_sec = static_cast<double>(duration_ns) / 1'000'000'000.0;
    double tput_kops = (static_cast<double>(ops_to_run) / 1000.0) / duration_sec;

    client.reportStats(true);
    std::cout << "ML-TRACKER-RESULTS: " 
              << "thread_id=" << global_thread_id << " "
              << "duration_sec=" << duration_sec << " "
              << "tput_kops=" << tput_kops 
              << std::endl;
}

int main(int argc, char* argv[]) {
  ProcId proc_id = 0;

  Layout layout;
  layout.doorbell = false;
  layout.in_place = true;
  layout.num_clients = 1;
  layout.num_servers = 1;
  layout.majority = 0;
  layout.guess_ts = true;
  layout.async_parallelism = 1;
  layout.keys_per_server = 100000;
  layout.server_logs_per_client = 0;
  layout.key_size = 24;
  layout.value_size = 64;
  layout.num_tsp = 1;
  // 0 = off: the unlocked baseline, whose SCAN is not linearizable. See
  // range_lock.hpp for why that matters when this is compared against a system
  // with a snapshot.
  layout.lock_stripes = 0;
  layout.lock_mode = 0;
  uint64_t death_point = uint64_t(-1);
  bool measure_batches = false;

  uint64_t iter_count = default_iter_count;
  uint64_t warmup = UINT64_MAX;

  layout.bucket_bits = 18;
  size_t pointer_cache_size = UINT64_MAX;
  bool detailed = true;

  bool run_ml_workload = false;

  std::string ycsb_path = "./YCSB/bin/ycsb.sh";
  std::string workload = "./YCSB/workloads/swarm-workloada";

  auto cli =
      lyra::cli() |
      lyra::opt(proc_id, "proc_id")
          .required()["-i"]["-p"]["--id"]["--process"]
          .help("ID of this process.") |
      lyra::opt(layout.num_clients, "num_clients")
          .optional()["-c"]["--clients"] |
      lyra::opt(layout.num_servers, "num_servers")
          .optional()["-s"]["--servers"] |
      lyra::opt(layout.majority, "majority").optional()["-m"]["--majority"] |
      lyra::opt(layout.async_parallelism, "async_parallelism")
          .optional()["-a"]["--async"] |
      lyra::opt(layout.keys_per_server, "num_keys")
          .optional()["-n"]["--numkeys"] |
      lyra::opt(layout.server_logs_per_client, "server_logs_per_client")
          .optional()["-l"]["--logs"] |
      lyra::opt(layout.key_size, "key_size").optional()["-k"]["--keysize"] |
      lyra::opt(layout.value_size, "value_size").optional()["-v"]["--valuesize"] |
      lyra::opt(layout.bucket_bits, "bucket_bits")
            .optional()["-b"]["--bucketbits"] |
      lyra::opt(pointer_cache_size, "pointer_cache_size")
          .optional()["-t"]["--pointercachesize"] |
      lyra::opt(workload, "workload").optional()["-w"]["--workload"] |
      lyra::opt(ycsb_path, "ycsb_path").optional()["-y"]["--ycsbpath"] |
      lyra::opt(detailed, "detailed").optional()["-d"]["--detailed"] |
      lyra::opt(layout.guess_ts, "guess_ts").optional()["-g"]["--guess"] |
      lyra::opt(layout.doorbell, "use_doorbell")
          .optional()["-o"]["--doorbell"] |
      lyra::opt(layout.in_place, "in_place").optional()["-e"]["--in_place"] |
      lyra::opt(layout.num_tsp, "num_tsp").optional()["-T"]["--num_tsp"] |
      lyra::opt(layout.lock_stripes, "lock_stripes")
          .optional()["--lock-stripes"](
              "Make SCAN linearizable by locking (0 = off, the default and the "
              "non-linearizable baseline; 1 = one global lock; N = the key "
              "space striped N ways). Writers take the lock too, which they "
              "must for a scan to be linearizable at all, so this costs the "
              "write path as well as the scan path.") |
      lyra::opt(layout.lock_mode, "lock_mode")
          .optional()["--lock-mode"](
              "Which lock, when --lock-stripes > 0: 0 striped (default, "
              "range_lock.hpp -- a scan rounds up to whole stripes and blocks "
              "writers to keys it never reads), 1 range table "
              "(range_table_lock.hpp -- one slot per client, a scan blocks "
              "exactly the keys it reads). Same guarantee either way; they "
              "differ in how many false conflicts they manufacture.") |
      lyra::opt(death_point, "death_point").optional()["-D"]["--death_point"] |
      lyra::opt(measure_batches, "measure_batches")
          .optional()["-B"]["--measure_batches"] |
      lyra::opt(iter_count, "iter_count").optional()["-I"]["--iter_count"] |
      lyra::opt(warmup, "warmup").optional()["-W"]["--warmup"] |
      lyra::opt(run_ml_workload, "ml").optional()["--ml"];

  auto result = cli.parse({argc, argv});
  if (!result) {
    std::cerr << "Error in command line: " << result.errorMessage()
              << std::endl;
    return 1;
  }
  if(warmup == UINT64_MAX) {
    warmup = iter_count < default_warmup ? iter_count : default_warmup;
  }
  // KEEPWARM MUST OUTLAST THE SLOWEST CLIENT'S WINDOW. See the long note in
  // disco-skip/src/main.cpp: every client runs the same fixed iter_count, so a
  // fast client finishes early and, once keepwarm runs out, stops issuing --
  // and the slow clients then measure a system carrying less than the intended
  // load. A quarter of the window does not cover the per-client speed spread
  // measured on this cluster (up to 1.7x); iter_count covers a 2x spread.
  const uint64_t keepwarm = iter_count;

  const uint64_t start_measurements = warmup;
  const uint64_t stop_measurements = start_measurements + iter_count;
  const uint64_t total_iter_count = stop_measurements + keepwarm;

  if (layout.majority == 0) {
    layout.majority = layout.num_servers;
  }
  if (layout.num_tsp == 0) {
    layout.num_tsp = layout.num_clients;
  }
  if (layout.server_logs_per_client == 0) {
    layout.server_logs_per_client = layout.keys_per_server + 10'000 + total_iter_count * 11 / 20;
  }

  if(pointer_cache_size != UINT64_MAX) {
    pointer_cache_size =
        (pointer_cache_size * 1024) / (layout.guess_ts ? 32 : 24);
  }

  auto num_proc = layout.num_clients + layout.num_servers;

  if (proc_id > num_proc) {
    std::cerr << "Invalid process id error: " << proc_id << " is bigger than "
              << num_proc << " (number of processes)" << std::endl;
    return 1;
  }
  bool is_client = proc_id > layout.num_servers;
  if (is_client) {
    std::cout << "Workload: " << workload << std::endl;
    std::cout << "Log entry size: " << layout.fullLogEntrySize() << std::endl;
    std::cout << "KV entry size: " << layout.fullKVSize() << std::endl;
  }

  std::vector<ProcId> remote_ids;
  for (ProcId id = 1; id <= num_proc; id++) {
    if (id == proc_id) {
      continue;
    }
    remote_ids.push_back(id);
  }

  using namespace units;

  ctrl::Devices d;
  ctrl::OpenDevice od;

  auto& available_devices = d.list();
  // WAS target_index = 0 with a comment claiming it is "the 3rd device
  // (uverbs2 or mlx5_2)", which index 0 is not. Only ever correct because the
  // r320 nodes have one device. On the r650 nodes index 0 is mlx5_0, the
  // ACTIVE 25G management NIC, while the experiment LAN is mlx5_2 at 100G --
  // so it runs, reports nothing wrong, and measures a 4x slower fabric.
  // See rdma_device.hpp.
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

  ctrl::ControlBlock cb(resolved_port);
  cb.registerPd("primary");

  {
    size_t allocated_size = is_client ? layout.clientSize() : layout.serverSize();
    size_t alignment = 64;
    cb.allocateBuffer("shared-buf", allocated_size, alignment);
  }

  cb.registerMr(
      "shared-mr", "primary", "shared-buf",
      ctrl::ControlBlock::LOCAL_READ | ctrl::ControlBlock::LOCAL_WRITE |
          ctrl::ControlBlock::REMOTE_READ | ctrl::ControlBlock::REMOTE_WRITE |
          ctrl::ControlBlock::REMOTE_ATOMIC);

  for (auto const& id : remote_ids) {
    auto cq = fmt::format("cq{}", id);
    cb.registerCq(cq);
  }

  auto local_region = cb.mr("shared-mr").addr;
  if (is_client) {
    layout.client_local_region = local_region;
  }

  auto& store = memstore::MemoryStore::getInstance();

  RcConnectionExchanger<ProcId> ce(proc_id, remote_ids, cb);
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
      ctrl::ControlBlock::LOCAL_READ | ctrl::ControlBlock::LOCAL_WRITE |
          ctrl::ControlBlock::REMOTE_READ | ctrl::ControlBlock::REMOTE_WRITE |
          ctrl::ControlBlock::REMOTE_ATOMIC);

  ce.announceReady(store, "qp", "connected");
  ce.waitReadyAll(store, "qp", "connected");

  ce.unannounceAll(store, "qp");
  ce.unannounceReady(store, "qp", "prepared");

  if (is_client) {
    OopsClient client{
        layout,          ce,          proc_id,   pointer_cache_size,
        measure_batches, death_point, iter_count};

    std::unique_ptr<ScanLock<Layout>> range_lock;

    if (layout.lock_stripes > 0) {
      if (layout.async_parallelism != 1) {
        throw std::runtime_error(
            "--lock-stripes requires -a 1. The lock's CAS drains the send CQ, "
            "which it shares with the future machinery, so it can only be "
            "taken when nothing else is in flight.");
      }
      // One slot per CLIENT for the table, one per stripe for the striped
      // lock -- see Layout::lockCells.
      uint64_t const cells = layout.lockCells();
      if (layout.lock_mode == 1) {
        range_lock = std::make_unique<TableScanLock<Layout>>(
            layout, ce.connections().at(1), static_cast<uint64_t>(proc_id),
            reinterpret_cast<uint64_t*>(layout.getLockScratchAddress()), cells);
      } else {
        range_lock = std::make_unique<StripedScanLock<Layout>>(
            layout, ce.connections().at(1), static_cast<uint64_t>(proc_id),
            reinterpret_cast<uint64_t*>(layout.getLockScratchAddress()), cells);
      }
      if (layout.lock_mode == 1) {
        std::cout << "Range lock ON: TABLE, " << cells
                  << " slots (one per client), exact intervals" << std::endl;
      } else {
        std::cout << "Range lock ON: STRIPED, " << cells
                  << (cells == 1 ? " stripe (global)" : " stripes") << ", "
                  << RangeLock<Layout>::kKeysPerStripe << " keys each"
                  << std::endl;
      }
    }

    if (proc_id == layout.firstClientId()) {
      std::cout << "Querying YCSB for the set of initial key-pairs... " << std::flush;
      std::vector<std::pair<std::string, std::string>> inserts = {};

      {
        auto output = exec(ycsb_path + " load basic -P " + workload + " -s 2> /dev/null");
        std::string line;
        while (std::getline(output, line)) {
          if (std::strncmp("INSERT ", line.c_str(), std::string("INSERT ").length())) {
            continue;
          }
          auto keystart = std::string("INSERT usertable ").length();
          auto keyend = line.find(" [", keystart);
          auto key = line.substr(keystart, keyend - keystart);

          auto start = keyend + std::string(" [ field0=").length();
          auto end = line.length() - std::string(" ]").length();
          auto value = line.substr(start, end - start);

          inserts.emplace_back(key, value);
        }
      }
      std::cout << "Done." << std::endl;

      std::cout << "Inserting the initial key-pairs... " << std::flush;
      for (size_t kvIndex = 0; kvIndex < inserts.size(); kvIndex++) {
        auto& insert = inserts[kvIndex];
        client.finishAllFutures();
        client.getFuture(0).doInsert(insert.first, insert.second);
      }
      client.finishAllFutures();
      std::cout << " Done." << std::endl;
    }
    
    std::cout << "Querying YCSB for the list of operations... " << std::flush;
    // INSERT IS PART OF THE RUN PHASE, not only the load phase. Without it an
    // "INSERT usertable ..." line matched none of the branches below and was
    // dropped silently -- no else, no warning. Workload E is 95% scan / 5%
    // insert, so this system executed the scans, skipped the inserts, and its
    // tree never grew while the system it is compared against did. Same defect
    // disco-skip had and fixed.
    enum class OpType { READ, UPDATE, SCAN, INSERT };

    struct KvsOp {
      OpType type;
      std::string key;
      std::string value; 
      int scan_count;    
    };
    std::vector<KvsOp> operations = {};

    {
      auto output = exec(ycsb_path + " run basic -P " + workload + " -s 2> /dev/null");
      std::string line;
      while (std::getline(output, line)) {
        if (!(std::strncmp("READ ", line.c_str(), std::string("READ ").length()))) {
          auto keystart = std::string("READ usertable ").length();
          auto keyend = line.find(" [", keystart);
          auto key = line.substr(keystart, keyend - keystart);
          operations.push_back({OpType::READ, key, "", 0});
        } else if (!(std::strncmp("UPDATE ", line.c_str(), std::string("UPDATE ").length()))) {
          auto keystart = std::string("UPDATE usertable ").length();
          auto keyend = line.find(" [", keystart);
          auto key = line.substr(keystart, keyend - keystart);

          auto start = keyend + std::string(" [ field0=").length();
          auto end = line.length() - std::string(" ]").length();
          auto value = line.substr(start, end - start);
          operations.push_back({OpType::UPDATE, key, value, 0});
        } else if (!(std::strncmp("INSERT ", line.c_str(), std::string("INSERT ").length()))) {
          // Byte-identical to the load-phase parse above: same prefix, same
          // " [ field0=" split. Two parsers for one line format would drift.
          auto keystart = std::string("INSERT usertable ").length();
          auto keyend = line.find(" [", keystart);
          auto key = line.substr(keystart, keyend - keystart);

          auto start = keyend + std::string(" [ field0=").length();
          auto end = line.length() - std::string(" ]").length();
          auto value = line.substr(start, end - start);
          operations.push_back({OpType::INSERT, key, value, 0});
        } else if (!(std::strncmp("SCAN ", line.c_str(), std::string("SCAN ").length()))) {
          auto keystart = std::string("SCAN usertable ").length();
          auto keyend = line.find(" ", keystart);
          auto key = line.substr(keystart, keyend - keystart);
          
          auto countstart = keyend + 1;
          auto countend = line.find(" [", countstart);
          int count = std::stoi(line.substr(countstart, countend - countstart));
          operations.push_back({OpType::SCAN, key, "", count});
        }
      }
    }
    std::cout << "Done." << std::endl;

    std::cout << "Waiting for the initialization of other clients... " << std::flush;
    ce.announceReady(store, "qp", "initialized");
    ce.waitReadyAll(store, "qp", "initialized");
    std::cout << "Done." << std::endl;

    std::cout << "Running the benchmark... " << std::endl;
    client.initClock();

    // ──────────────────────────────────────────────────────────────────────────
    // BRANCH A: ML TRACKER WORKLOAD PATH
    // ──────────────────────────────────────────────────────────────────────────
    if (run_ml_workload) {
        uint64_t global_thread_id = proc_id - layout.num_servers - 1;

        std::cout << "Running single-threaded ML tracker workload. ID=" << global_thread_id  
                  << " | Active Workers Checked=" << layout.num_clients << std::endl;

        run_ml_prog_tracker_workload(client, global_thread_id, layout.num_clients, iter_count);

        ce.announceReady(store, "qp", "finished");
        ce.waitReadyAll(store, "qp", "finished");
        ce.unannounceReady(store, "qp", "initialized");
    } 
    // ──────────────────────────────────────────────────────────────────────────
    // BRANCH B: STANDARD YCSB WORKLOAD PATH
    // ──────────────────────────────────────────────────────────────────────────
    else {
      std::chrono::steady_clock::time_point start;
      std::chrono::steady_clock::time_point end;
      bool measuring = false;
      size_t skipped = 0;
      uint64_t total_scan_duration_ns = 0;
      uint64_t total_scans_measured = 0;

      for (size_t i = 0; i < total_iter_count; i++) {
        retry_next_key:
        auto& op = operations[(i + skipped) % operations.size()]; 

        if (layout.async_parallelism > 1) {
          auto hkey = hash(op.key); 
          for (size_t p = 0; p < layout.async_parallelism; p++) {
            auto& future = client.getFuture(p);
            if (future.hkey == hkey && !(future.isDone())) {
              ++skipped;
              goto retry_next_key;
            }
          }
        }

        if (i == start_measurements) {
          // BARRIER, OR THE PER-CLIENT NUMBERS DO NOT SHARE A WINDOW.
          // Same defect and same fix as disco-skip/src/main.cpp -- there was a
          // barrier at "initialized" but none here, so each client began
          // measuring whenever it personally finished warmup, and the windows
          // drifted apart until they no longer overlapped.
          //
          // THIS HAD TO BE FIXED IN ALL THREE BINARIES, NOT JUST OURS.
          // swarm-kv and fusee carry the identical pattern, so correcting only
          // disco-skip would leave the competitors' numbers inflated by the
          // stagger while ours became honest -- biasing the comparison against
          // us and invalidating it either way.
          //
          // Clients only: the memory servers never enter this loop, so waiting
          // on them here would hang forever.
          store.barrier("measure-start", layout.num_clients);
          measuring = true;
          start = std::chrono::steady_clock::now();
          client.startMeasurements(start);
        } else if (i == stop_measurements) {
          measuring = false;
          end = std::chrono::steady_clock::now();
        }

        // THE LOCK. Taken at operation boundaries, never with RDMA in flight:
        // casBlocking drains the send CQ, which the future machinery shares, so
        // a completion belonging to a future would be eaten. finishAllFutures()
        // below and the per-key drain inside SCAN are what make these points
        // quiescent.
        if (range_lock && op.type != OpType::READ) {
          // DRAIN BEFORE ACQUIRING, not just before releasing.
          //
          // casBlocking drains the send CQ, which it shares with the future
          // machinery, so the lock can only be taken with nothing in flight.
          // The release side already drained; the ACQUIRE side did not, and the
          // gap is a READ: a read takes no lock and does not drain, so when the
          // next operation is an update it acquired while that read was still
          // outstanding. The guard in RangeLock caught it as
          // "drained a completion belonging to a future" rather than letting
          // the CQ quietly desynchronise, but the fix is here.
          client.finishAllFutures();
          if (op.type == OpType::SCAN) {
            range_lock->acquireRange(ycsbKeyToInt(op.key),
                                     static_cast<uint64_t>(op.scan_count));
          } else if (op.type == OpType::UPDATE || op.type == OpType::INSERT) {
            // An INSERT is a write. A scan that is atomic against updates but
            // not against inserts is not linearizable -- workload E's inserts
            // are exactly what a concurrent scan would otherwise tear on.
            range_lock->acquireKey(ycsbKeyToInt(op.key));
          }
          // READ is a single point read and is already atomic on its own, so
          // it does not take the lock. A reader that took it would serialise
          // the read-only workloads against nothing, which would overstate the
          // cost rather than measure it.
        }

        if (op.type == OpType::INSERT) {
          auto& future = client.getFreeFuture();
          future.doInsert(op.key, op.value, measuring);
        }
        else if (op.type == OpType::UPDATE) {
          auto& future = client.getFreeFuture();
          future.doUpdate(op.key, op.value, measuring);
        } 
        else if (op.type == OpType::READ) {
          auto& future = client.getFreeFuture();
          future.doRead(op.key, measuring);
        } 
        else if (op.type == OpType::SCAN) {
          std::string target_scan_key = op.key;
          std::chrono::steady_clock::time_point scan_start;
          if (measuring) {
            scan_start = std::chrono::steady_clock::now();
          }

          for (int c = 0; c < op.scan_count; ++c) {
            try {
              auto& future = client.getFreeFuture();
              future.doRead(target_scan_key, false); 
              client.finishAllFutures(); 
            } 
            catch (const std::runtime_error& e) {
              if (std::string(e.what()).find("Key not found") == std::string::npos) {
                  throw; 
              }
            }
            target_scan_key = IncrementYcsbKey(target_scan_key);
          }

          if (measuring) {
            auto scan_end = std::chrono::steady_clock::now();
            total_scan_duration_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(scan_end - scan_start).count();
            total_scans_measured++;
          }
        }

        if (range_lock &&
            (op.type == OpType::SCAN || op.type == OpType::UPDATE ||
             op.type == OpType::INSERT)) {
          // The write must LAND before the lock is dropped, or a scan could
          // take the lock immediately afterwards and miss it. This drain is
          // not an artefact of how the lock is implemented: releasing before
          // the protected write has completed would be incorrect under any
          // implementation.
          client.finishAllFutures();
          range_lock->release();
        }
      }

      client.finishAllFutures();

      std::cout << "Done. Results:" << std::endl;
      client.reportStats(detailed);
      if (range_lock) {
        // Retries per acquire is the contention figure. A locked arm whose
        // retry count is ~0 was not contended, so its throughput says nothing
        // about what locking costs under load.
        fmt::print("range lock:   {} acquires, {} retries ({:.2f} per acquire)\n",
                   range_lock->acquires(), range_lock->retries(),
                   range_lock->acquires() == 0
                       ? 0.0
                       : static_cast<double>(range_lock->retries()) /
                             static_cast<double>(range_lock->acquires()));
      }

      if (total_scans_measured > 0) {
          double avg_scan_latency_ms = static_cast<double>(total_scan_duration_ns) 
                                      / static_cast<double>(total_scans_measured) 
                                      / 1'000'000.0;
          fmt::print("Macro Scan Count: {}\n", total_scans_measured);
          fmt::print("Average Macro Scan Latency: {:.3f} ms\n", avg_scan_latency_ms);
      } else {
          fmt::print("Average Macro Scan Latency: N/A (No scan operations occurred during measurement window)\n");
      }

      // THREE DECIMALS, for the same reason disco-skip's print was changed:
      // this is PER CLIENT, so at 64 clients it is ~4 kops each and an integer
      // could only move in steps of 1 kops/client = 25% of the summed total.
      // fusee needs no such change -- it prints an AGGREGATE, so one kops is a
      // small fraction of it -- but leaving swarm-kv coarse while ours is
      // precise would bias the comparison at exactly the client count where it
      // matters most.
      //
      // The `kpos` is swarm-kv's own typo and is load-bearing: lib.sh and the
      // plotter both match on it, so it is preserved deliberately.
      fmt::print(
          "Local tput: {:.3f}kpos\n",
          static_cast<double>(iter_count) * 1e6
              / static_cast<double>((end - start).count()));
      fmt::print("Local duration: {}s\n", 
          static_cast<uint64_t>((end - start).count() / 1000000000));
      std::cout << std::flush;

      ce.announceReady(store, "qp", "finished");
      ce.waitReadyAll(store, "qp", "finished");
      ce.unannounceReady(store, "qp", "initialized");
    }
  } else {
    // SERVER/REPLICA SETUP
    std::vector<ProcId> client_ids;
    for (ProcId id = layout.firstClientId();
         id < layout.firstClientId() + layout.num_clients; id++) {
      client_ids.push_back(id);
    }
    dory::race::ServerIndex server(proc_id, client_ids, layout.bucket_bits);
    ce.announceReady(store, "qp", "initialized");
    ce.announceReady(store, "qp", "finished");
    ce.waitReadyAll(store, "qp", "finished");
    ce.unannounceReady(store, "qp", "initialized");
    std::cout << "Closing server index connection." << std::endl;
  }

  ce.unannounceReady(store, "qp", "connected");
  std::cout << "###DONE###" << std::endl;
  return 0;
}