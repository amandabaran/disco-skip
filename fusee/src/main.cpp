#include "layout.hpp"
#include "range_lock.hpp"
#include "range_table_lock.hpp"
#include "client.hpp"
#include "latency.hpp"
#include <memory>
#include <optional>
#include "race/server_index.hpp"
#include "race/client_index.hpp"

#include <dory/shared/match.hpp>
#include "lru-cache.hpp"

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

// INSERT IS PART OF THE RUN PHASE, not only the load phase. Without it an
// "INSERT usertable ..." line matched none of the parser's branches and was
// dropped silently -- no else, no warning. Workload E is 95% scan / 5% insert,
// so this system executed the scans, skipped the inserts, and its table never
// grew while the system it is compared against did.
enum class OpType { READ, UPDATE, SCAN, INSERT };

struct KvsOp {
  OpType type;
  std::string key;
  std::string value; // Used for UPDATE
  int scan_count;    // Used for SCAN
};

/// The numeric part of a YCSB key ("user0000123" -> 123).
///
/// MUST agree with IncrementYcsbKey, which is what a SCAN walks: the stripes a
/// scan locks come from this, so if the two disagreed about key ordering the
/// lock would be held and protect the wrong range. Byte-identical to the copy
/// in swarm-kv/src/main.cpp for the same reason range_lock.hpp is shared --
/// two competitor arms must lock the same way to be comparable.
///
/// strtoull, not stoll: with insertorder=hashed YCSB keys reach ~9.2e18, at the
/// edge of a signed 64-bit integer.
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
    int padding_needed = static_cast<int>(key.length() - prefix.length() - num_str.length());
    if (padding_needed > 0) {
        return prefix + std::string(padding_needed, '0') + num_str;
    }
    return prefix + num_str;
}

int main(int argc, char* argv[]) {
  Layout layout;
  layout.num_clients = 1;

  // 0 = off: the unlocked baseline, whose SCAN is not linearizable. See
  // range_lock.hpp.
  layout.lock_stripes = 0;
  layout.lock_mode = 0;
  layout.num_keys = 100'000;
  layout.key_size = 24;
  layout.value_size = 64;

  size_t bucket_bits = 18;
  uint64_t pointer_cache_size = UINT64_MAX;

  layout.kv_read_slot_count = 128;

  std::string ycsb_path = "./YCSB/bin/ycsb";
  std::string workload = "./YCSB/workloads/swarm-workloada";

  size_t iter_count = default_iter_count;
  uint64_t warmup = UINT64_MAX;

  LatencyProfiler get_stat, update_stat, search_stat;

  auto cli =
      lyra::cli() |
      lyra::opt(layout.num_clients, "num_clients")
          .optional()["-c"]["--clients"] |
      lyra::opt(layout.num_servers, "num_servers")
          .optional()["-s"]["--servers"] |
      lyra::opt(layout.proc_id, "proc_id")
          .required()["-i"]["-p"]["--id"]["--process"]
          .help("ID of this process.") |
      lyra::opt(layout.lock_stripes, "lock_stripes")
          .optional()["--lock-stripes"](
              "Make SCAN linearizable by locking (0 = off, the default and the "
              "non-linearizable baseline; 1 = one global lock; N = the key "
              "space striped N ways). Writers take the lock too, which they "
              "must for a scan to be linearizable at all.") |
      lyra::opt(layout.key_size, "key_size").optional()["-k"]["--keysize"] |
      lyra::opt(layout.value_size, "value_size")
          .optional()["-v"]["--valuesize"] |
      lyra::opt(layout.num_keys, "num_keys").optional()["-n"]["--numkeys"] |
      lyra::opt(layout.kv_read_slot_count, "kv_read_slot_count")
          .optional()["-e"]["--kvcacheentrycount"] |
      lyra::opt(bucket_bits, "bucket_bits").optional()["-b"]["--bucketbits"] |
      lyra::opt(pointer_cache_size, "pointer_cache_size")
          .optional()["-t"]["--pointercachesize"] |
      lyra::opt(workload, "workload").optional()["-w"]["--workload"] |
      lyra::opt(ycsb_path, "ycsb_path").optional()["-y"]["--ycsbpath"] |
      lyra::opt(iter_count, "iter_count").optional()["-I"]["--iter_count"] |
      lyra::opt(warmup, "warmup").optional()["-W"]["--warmup"];

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

  layout.max_num_updates = layout.num_keys + 10'000 + total_iter_count * 11 / 20;

  if(pointer_cache_size != UINT64_MAX) {
    pointer_cache_size = (pointer_cache_size * 1024) / 24;
  }

  layout.keys_per_server = (layout.num_keys + layout.max_num_updates) * layout.num_clients;

  auto num_proc = layout.num_clients + layout.num_servers;

  if (layout.proc_id > num_proc) {
    std::cerr << "Invalid process id error: " << layout.proc_id << " is bigger than "
              << num_proc << " (number of processes)" << std::endl;
    return 1;
  }
  bool is_server = layout.proc_id <= layout.num_servers;
  bool is_client = layout.proc_id > layout.num_servers;

  if (is_client) {
    std::cout << "Workload: " << workload << std::endl;
    std::cout << "Log entry size: " << layout.fullEntrySize() << std::endl;
    std::cout << "Read entry size: " << layout.kvSize() << std::endl;
    // pin_main_to_core(0);
  }

  using namespace units;

  ctrl::Devices d;
  ctrl::OpenDevice od;

  // Get the last device
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

  // 2. We configure the control block.
  ctrl::ControlBlock cb(resolved_port);
  cb.registerPd("primary");

  {
    size_t allocated_size =
        is_client ? layout.clientSize() :
            layout.serverSize();
    size_t alignment = 64;
    cb.allocateBuffer("shared-buf", allocated_size, alignment);
  }

  cb.registerMr(
      "shared-mr", "primary", "shared-buf",
      ctrl::ControlBlock::LOCAL_READ | ctrl::ControlBlock::LOCAL_WRITE |
          ctrl::ControlBlock::REMOTE_READ | ctrl::ControlBlock::REMOTE_WRITE |
          ctrl::ControlBlock::REMOTE_ATOMIC);
  cb.registerCq("cq");
  if (is_client) {
    cb.registerCq("cq2");
  }
  
  auto local_region = cb.mr("shared-mr").addr;

  // if (is_server) {
  //   for (uint64_t i = 0; i < layout.keys_per_server; i++) {
  //     *reinterpret_cast<uint64_t*>(layout.getServerKV(local_region, i)->key()) = i;
  //   }
  // }

  // 3. We establish reliable connections.
  auto& store = memstore::MemoryStore::getInstance();

  std::vector<ProcId> remote_ids;
  for (ProcId id = 1; id <= num_proc; id++) {
    if (id == layout.proc_id) {
      continue;
    }
    // if (is_client == (id > layout.num_servers)) {
    //   continue; // Only connect to clients if we are a server or backup and vice versa
    // }
    remote_ids.push_back(id);
  }

  RcConnectionExchanger<ProcId> ce(layout.proc_id, remote_ids, cb);
  for (auto const& id : remote_ids) {
    ce.configure(id, "primary", "shared-mr", "cq", "cq");
  }
  ce.announceAll(store, "qp");

  ce.announceReady(store, "qp", "prepared");
  ce.waitReadyAll(store, "qp", "prepared");

  ce.unannounceReady(store, "qp", "finished"); // Clean up from previous runs

  ce.connectAll(
      store, "qp",
      ctrl::ControlBlock::LOCAL_READ | ctrl::ControlBlock::LOCAL_WRITE |
          ctrl::ControlBlock::REMOTE_READ | ctrl::ControlBlock::REMOTE_WRITE |
          ctrl::ControlBlock::REMOTE_ATOMIC);

  ce.announceReady(store, "qp", "connected");
  ce.waitReadyAll(store, "qp", "connected");

  // Clean up
  ce.unannounceAll(store, "qp");
  ce.unannounceReady(store, "qp", "prepared");

  if (is_client) {
    std::vector<std::shared_ptr<dory::race::ClientIndex>> indexes;
    for (ProcId id = 1; id <= layout.num_servers; id++) {
      auto x = std::make_shared<dory::race::ClientIndex>(
        layout.proc_id, id, bucket_bits, std::to_string(id));
      indexes.emplace_back(x);
    }

    // TODO(zyf): handle multiple servers
    auto client = Client(layout, local_region, ce, layout.num_servers);

    std::unique_ptr<ScanLock<Layout>> range_lock;
    if (layout.lock_stripes > 0) {
      // One slot per CLIENT for the table, one per stripe for the striped
      // lock -- see Layout::lockCells.
      uint64_t const cells = layout.lockCells();
      auto* scratch = reinterpret_cast<uint64_t*>(
          layout.getLockScratchAddress(local_region));
      if (layout.lock_mode == 1) {
        range_lock = std::make_unique<TableScanLock<Layout>>(
            layout, ce.connections().at(1),
            static_cast<uint64_t>(layout.proc_id), scratch, cells);
        std::cout << "Range lock ON: TABLE, " << cells
                  << " slots (one per client), exact intervals" << std::endl;
      } else {
        range_lock = std::make_unique<StripedScanLock<Layout>>(
            layout, ce.connections().at(1),
            static_cast<uint64_t>(layout.proc_id), scratch, cells);
        std::cout << "Range lock ON: STRIPED, " << cells
                  << (cells == 1 ? " stripe (global)" : " stripes") << ", "
                  << RangeLock<Layout>::kKeysPerStripe << " keys each"
                  << std::endl;
      }
    }
    auto pointer_cache = LRUCache<HashedKey, uint64_t>(pointer_cache_size);

    ProcId first_client = layout.num_servers + 1;
    if (layout.proc_id == first_client) {
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

          // std::cout << line << std::endl;
          // std::cout << inserts.back().first << ": ";
          // for (auto s : inserts.back().second) {
          //   std::cout << s << "€";
          // }
          // std::cout << std::endl;
        }
      }
      std::cout << "Done." << std::endl;


      // Initialize the index
      std::vector<dory::race::ClientIndex::TryInsertFuture> futures;
      futures.reserve(layout.num_servers);
      std::cout << "Inserting the initial key-pairs... " << std::flush;
      for (size_t kvIndex = 0; kvIndex < inserts.size(); kvIndex++) {
        auto insert = inserts.at(kvIndex);
        auto key = insert.first;
        auto value = insert.second;

        auto hkey = hash(key);
        auto search = indexes[0]->search(hkey);

        auto* local_log = client.prepareToWriteEntry(key, value);
        auto kv_id = client.write(local_log, 0);

        auto search_res = search.await();
        if (search_res.nb_free == 0) {
          throw std::runtime_error("No free space in the index");
        }
        auto free = search_res.free;

        futures.clear();
        for (size_t s = 0; s < layout.num_servers; s++) {
          auto& index = *(indexes.at(s));
          futures.emplace_back(index.tryInsert(free, hkey, kv_id));
        }

        for (size_t s = 0; s < layout.num_servers; s++) {
          auto res = futures.at(s).await();
          if (res.asUint64() != 0) {
            throw std::runtime_error("Failed to insert key " + key + " in server " + std::to_string(s));
          }
        }


        // pointer_cache.put(hkey, kv_id);
        // fmt::print("Ts: {}, CachedTs : {}\n", local_log->kv.ts, *(pointer_cache.get(hkey)));

        // auto sf = index.search(hkey).await();

        // if (sf.nb_matches == 0) {
        //   std::cout << "No match for key " << fakekey << " even though it was just inserted ???" << std::endl;
        // }
      }
      std::cout << " Done." << std::endl;
    }

    std::cout << "Querying YCSB for the list of operations... " << std::flush;
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

    // std::cout << "Waiting for the initialization of other clients... " << std::flush;

    ce.announceReady(store, "qp", "initialized");
    ce.waitReadyAll(store, "qp", "initialized");

    // std::cout << "Done." << std::endl;
    std::cout << "Running the benchmark... " << std::flush;

    std::vector<dory::race::ClientIndex::TryInsertFuture> futures;
    futures.reserve(layout.num_servers - 1);

    // The same sequence the load phase runs inline: write the entry, then
    // insert the pointer into every server's index. Lifted into a lambda so the
    // run phase executes exactly what the load phase does -- two copies of a
    // multi-step distributed insert would drift, and a run-phase insert that
    // differed from the load-phase one would make the table's contents depend
    // on when a key arrived.
    // BY VALUE, not by const reference: prepareToWriteEntry takes non-const
    // std::string&. The load phase gets away with it by copying into locals
    // first (`auto key = insert.first;`), so this does the same rather than
    // changing a signature in the client.
    auto execute_insert = [&](std::string key, std::string value) {
      auto hkey = hash(key);
      auto search = indexes[0]->search(hkey);

      auto* local_log = client.prepareToWriteEntry(key, value);
      auto kv_id = client.write(local_log, 0);

      auto search_res = search.await();
      if (search_res.nb_free == 0) {
        throw std::runtime_error("No free space in the index");
      }
      auto free = search_res.free;

      futures.clear();
      for (size_t sv = 0; sv < layout.num_servers; sv++) {
        auto& index = *(indexes.at(sv));
        futures.emplace_back(index.tryInsert(free, hkey, kv_id));
      }
      for (size_t sv = 0; sv < layout.num_servers; sv++) {
        auto res = futures.at(sv).await();
        if (res.asUint64() != 0) {
          throw std::runtime_error("Failed to insert key " + key +
                                   " in server " + std::to_string(sv));
        }
      }
    };

    std::chrono::time_point now = std::chrono::steady_clock::now();
    auto start = now;
    auto end = now;
    auto last_time = start;
    auto zero = now - now;
    auto total_search_time = zero;
    auto total_read_time = zero;
    auto total_update_time = zero;
    int32_t total_read_count = 0;
    int32_t total_update_count = 0;
    int32_t total_cache_hit_count = 0;
    int32_t total_true_cache_hit_count = 0;
    // Run-wide, NOT per-operation: execute_point_op is defined inside the
    // operation loop, so a counter declared next to it would reset on every
    // iteration and always report the last scan's misses.
    uint64_t scan_probe_misses = 0;

    // Worklaod Loop
    // WORKLOAD LOOP
    for (size_t i = 0; i < total_iter_count; i++) {
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
        start = std::chrono::steady_clock::now();
      } else if (i == stop_measurements) {
        end = std::chrono::steady_clock::now();
      }
      
      auto operation = operations[i % operations.size()];

      // Native point operation processing lambda for FUSEE
      // Native point operation processing lambda for FUSEE
      // `scan_probe` = this call is one slot of a SCAN, not a point op.
      //
      // WHY IT IS NEEDED. fusee is a HASH store with no ordered iterator, so
      // workload E's scan is emulated by walking the key space: start at the
      // scan's key and read `scan_count` lexicographically successive keys,
      // whether or not they exist. In a uniform YCSB keyspace most of those
      // slots are empty, and both "absent" paths below used to throw -- so
      // EVERY fusee workload-E run aborted with `Key not found` partway
      // through the benchmark. There is no fusee E data in this repo for that
      // reason, not because it was slow.
      //
      // An absent slot is a legitimate outcome of that walk and is counted, not
      // an error. A point READ or UPDATE of a preloaded key that comes back
      // missing IS an error and still throws -- that distinction is the whole
      // point of the flag, and collapsing it would hide real failures in the
      // read-heavy workloads.
      auto execute_point_op = [&](const std::string& target_key, OpType type, const std::string& target_value, bool scan_probe = false) {
        auto hkey = hash(target_key);
        auto random_server = (reinterpret_cast<uint64_t const*>(hkey.data())[0] % (layout.num_servers - 1)) + 1;
        auto main_server = 0UL;
        auto& main_index = *(indexes.at(main_server));

        auto search = main_index.search(hkey);
        uint64_t new_kv_id;
        
        auto cache_entry = pointer_cache.get(hkey);
        if (cache_entry) {
          auto kv_id = *cache_entry;
          now = std::chrono::steady_clock::now();
          auto search_time = now - last_time;
          if (start_measurements <= i && i < stop_measurements) {
            total_cache_hit_count += 1;
          }

          if (type == OpType::UPDATE) {
            // --- FIX: Create mutable string copies to satisfy Client::prepareToWriteEntry ---
            std::string mutable_key = target_key;
            std::string mutable_val = target_value;
            auto* local_log = client.prepareToWriteEntry(mutable_key, mutable_val);
            
            new_kv_id = client.readAndWrite(local_log, kv_id, random_server).second;
            auto matches = search.await();
            
            auto prev_entry = search.entryFor(kv_id);
            auto offset = search.getOffsetOf(prev_entry);
            if (!offset.has_value()) { goto cache_miss_fallback; }

            futures.clear();
            for (size_t s = 1; s < layout.num_servers; s++) {
              auto& index = *(indexes.at((main_server + s) % layout.num_servers));
              futures.emplace_back(index.tryUpdate(*offset, prev_entry, hkey, new_kv_id));
            }

            auto res = futures.at(0).await();
            if (res.asUint64() != prev_entry.asUint64()) {
              auto iter = 0;
              do {
                res = main_index.tryCheck(*offset).await();
              } while(res.asUint64() == prev_entry.asUint64());
              new_kv_id = res.getValue();
              goto cache_hit_up_done;
            }

            for (size_t s = 2; s < layout.num_servers; s++) {
              res = futures.at(s - 1).await();
              if (res.asUint64() != prev_entry.asUint64()) {
                auto& index = *(indexes.at((main_server + s) % layout.num_servers));
                index.tryUpdate(*offset, prev_entry, hkey, new_kv_id).await();
              }
            }

            client.writeLogCommit(main_server, kv_id, new_kv_id);
            main_index.tryUpdate(*offset, prev_entry, hkey, new_kv_id).await();

            cache_hit_up_done:
            pointer_cache.put(hkey, new_kv_id);
            now = std::chrono::steady_clock::now();
            if (start_measurements <= i && i < stop_measurements) {
              auto measure = now - last_time;
              update_stat.addMeasurement(measure);
              total_update_time += measure;
              ++total_update_count;
            }
          } else { // READ
            auto* ignored = client.read(random_server, kv_id);
            auto matches = search.await();

            auto prev_entry = search.entryFor(kv_id);
            auto offset = search.getOffsetOf(prev_entry);
            if (!offset.has_value()) { goto cache_miss_fallback; }

            now = std::chrono::steady_clock::now();
            if (start_measurements <= i && i < stop_measurements) {
              auto measure = now - last_time;
              get_stat.addMeasurement(measure);
              total_read_time += measure;
              ++total_read_count;
            }
          }
          
          if (start_measurements <= i && i < stop_measurements) {
            search_stat.addMeasurement(search_time);
            total_search_time += search_time;
            total_true_cache_hit_count += 1;
          }
          last_time = now;
          return;
        }

        // Cache Miss Pipeline Execution Block
        if (type == OpType::UPDATE) {
          // --- FIX: Create mutable string copies here as well ---
          std::string mutable_key = target_key;
          std::string mutable_val = target_value;
          auto* local_log = client.prepareToWriteEntry(mutable_key, mutable_val);
          
          new_kv_id = client.write(local_log, random_server);
        }

        cache_miss_fallback:
        auto sf = search.await();
        now = std::chrono::steady_clock::now();
        if (start_measurements <= i && i < stop_measurements) {
          auto measure = now - last_time;
          search_stat.addMeasurement(measure);
          total_search_time += measure;
        }

        // No index slot for this key. Empty keyspace slot during a scan walk;
        // a genuinely missing key otherwise.
        if (sf.nb_matches == 0) {
          if (scan_probe) { ++scan_probe_misses; last_time = now; return; }
          throw std::runtime_error("Key not found");
        }

        auto found = false;
        for (size_t j = 0; j < sf.nb_matches; j++) {
          auto kv_id = sf.matches[j].getValue();
          auto entry = client.read(random_server, kv_id);

          if (!(std::strncmp(target_key.c_str(), entry->kv.key(), layout.key_size))) {
            found = true;
            if (type == OpType::UPDATE) {
              auto prev_entry = search.entryFor(kv_id);
              auto offset = search.getOffsetOf(prev_entry);
              
              futures.clear();
              for (size_t s = 1; s < layout.num_servers; s++) {
                auto& index = *(indexes.at((main_server + s) % layout.num_servers));
                futures.emplace_back(index.tryUpdate(*offset, prev_entry, hkey, new_kv_id));
              }

              auto res = futures.at(0).await();
              if (res.asUint64() != prev_entry.asUint64()) {
                do {
                  res = main_index.tryCheck(*offset).await();
                } while(res.asUint64() == prev_entry.asUint64());
                new_kv_id = res.getValue();
                goto cache_miss_up_done;
              }

              for (size_t s = 2; s < layout.num_servers; s++) {
                res = futures.at(s - 1).await();
                if (res.asUint64() != prev_entry.asUint64()) {
                  auto& index = *(indexes.at((main_server + s) % layout.num_servers));
                  index.tryUpdate(*offset, prev_entry, hkey, new_kv_id).await();
                }
              }

              client.writeLogCommit(main_server, kv_id, new_kv_id);
              main_index.tryUpdate(*offset, prev_entry, hkey, new_kv_id).await();

              cache_miss_up_done:
              pointer_cache.put(hkey, new_kv_id);
              now = std::chrono::steady_clock::now();
              if (start_measurements <= i && i < stop_measurements) {
                auto measure = now - last_time;
                update_stat.addMeasurement(measure);
                total_update_time += measure;
                ++total_update_count;
              }
            } else { // READ
              pointer_cache.put(hkey, kv_id);
              now = std::chrono::steady_clock::now();
              if (start_measurements <= i && i < stop_measurements) {
                auto measure = now - last_time;
                get_stat.addMeasurement(measure);
                total_read_time += measure;
                ++total_read_count;
              }
            }
            break;
          }
        }
        // Candidate slots existed but none held this key -- a hash collision
        // against some other key. Same story as nb_matches == 0: during a scan
        // walk that is an empty slot, elsewhere it is a real failure.
        if (!found) {
          if (scan_probe) { ++scan_probe_misses; last_time = now; return; }
          throw std::runtime_error("Match evaluation logic failed.");
        }
        last_time = now;
      };

      // Handle the distinct execution flows
      // Taken at operation boundaries: casBlocking drains the send CQ, so it
      // must not run with other RDMA outstanding. execute_point_op is
      // synchronous here, so these points are quiescent.
      //
      // READ does not lock -- a single point read is already atomic, and making
      // it lock would serialise the read-only workloads against nothing, which
      // overstates the cost rather than measuring it.
      if (range_lock) {
        if (operation.type == OpType::SCAN) {
          range_lock->acquireRange(ycsbKeyToInt(operation.key),
                                   static_cast<uint64_t>(operation.scan_count));
        } else if (operation.type == OpType::UPDATE ||
                   operation.type == OpType::INSERT) {
          // An INSERT is a write. A scan atomic against updates but not against
          // inserts is not linearizable, and workload E's inserts are exactly
          // what a concurrent scan would tear on.
          range_lock->acquireKey(ycsbKeyToInt(operation.key));
        }
      }

      if (operation.type == OpType::INSERT) {
        execute_insert(operation.key, operation.value);
      }
      else if (operation.type == OpType::UPDATE) {
        execute_point_op(operation.key, OpType::UPDATE, operation.value);
      } 
      else if (operation.type == OpType::READ) {
        execute_point_op(operation.key, OpType::READ, "");
      } 
      else if (operation.type == OpType::SCAN) {
        std::string scan_key = operation.key;
        for (int c = 0; c < operation.scan_count; ++c) {
          execute_point_op(scan_key, OpType::READ, "", /*scan_probe=*/true);
          scan_key = IncrementYcsbKey(scan_key);
        }
      }
      if (range_lock) range_lock->release();
      now = std::chrono::steady_clock::now();
    }

    std::cout << "Done. Results:" << std::endl;

    fmt::print("\n");
    // SCAN PROBE MISSES -- REQUIRED TO INTERPRET ANY fusee WORKLOAD-E NUMBER.
    //
    // fusee's scan walks `scan_count` successive keys in a hash store, and an
    // absent slot now costs one index search and NO kv read. So a scan that
    // misses most of its slots is fast precisely because it found nothing, and
    // quoting its throughput against an ordered range scan without this figure
    // overstates it. A high miss rate is not a bug -- it is what emulating an
    // ordered scan on a hash store means -- but it must appear next to the
    // number.
    fmt::print("scan probes:  {} misses (empty keyspace slots)\n",
               scan_probe_misses);
    if (range_lock) {
      // Byte-identical in swarm-kv/src/main.cpp, and the harness greps for this
      // exact text to flag an arm labelled locked that took no locks. Retries
      // per acquire is the contention figure: a locked arm with none was never
      // contended, and its throughput says nothing about what locking costs.
      fmt::print("range lock:   {} acquires, {} retries ({:.2f} per acquire)\n",
                 range_lock->acquires(), range_lock->retries(),
                 range_lock->acquires() == 0
                     ? 0.0
                     : static_cast<double>(range_lock->retries()) /
                           static_cast<double>(range_lock->acquires()));
    }
    fmt::print("################ Main stats:\n");
    fmt::print("######## SEARCH stats:\n");
    fmt::print("cache-hits: {}%\n",
               search_stat.getMeasurementCount() > 0
                   ? static_cast<double>(total_cache_hit_count) * 100.0 /
                         static_cast<double>(search_stat.getMeasurementCount())
                   : 0);
    fmt::print("true-hit: {}% ({}% of hits)\n", (100.0 * total_true_cache_hit_count) / static_cast<double>(iter_count), (100.0 * total_true_cache_hit_count) / total_cache_hit_count);
    search_stat.report(true);
    fmt::print("######## GET stats:\n");
    get_stat.report(true);
    fmt::print("######## UPDATE stats:\n");
    update_stat.report(true);
    fmt::print("global average: {}\n", (end - start) / static_cast<double>(iter_count));
    fmt::print("aggregated tput: {}kops\n", (layout.num_clients * iter_count * 1'000'000) / static_cast<uint64_t>((end - start).count()));
    std::cout << std::flush;

    ce.announceReady(store, "qp", "finished");
    ce.waitReadyAll(store, "qp", "finished");
    ce.unannounceReady(store, "qp", "initialized");
  } else if (is_server) {
    std::vector<ProcId> client_ids;
    for (ProcId id = layout.num_servers + 1; id <= num_proc; id++) {
      client_ids.push_back(id);
    }
    dory::race::ServerIndex server(layout.proc_id, client_ids, bucket_bits, std::to_string(layout.proc_id));
    ce.announceReady(store, "qp", "initialized");
    ce.announceReady(store, "qp", "finished");
    ce.waitReadyAll(store, "qp", "finished");
    ce.unannounceReady(store, "qp", "initialized");
  }

  // 9. Clean up.
  ce.unannounceReady(store, "qp", "connected");
  
  std::cout << "###DONE###" << std::endl;
  return 0;
}
