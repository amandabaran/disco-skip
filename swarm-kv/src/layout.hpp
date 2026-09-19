#pragma once

#include <fmt/core.h>
#include <xxhash.h>
#include <array>
#include <cstdint>
#include <stdexcept>

#include "base.hpp"
#include "tspointers.hpp"

using chsum_t = std::array<uint8_t, 16>;
static thread_local XXH3_state_t* hash_state{XXH3_createState()};

struct KV {
  uint64_t in_place_tsp;
  chsum_t chsum;
};

struct LogEntry {
  uint64_t notif;
  uint64_t from_tsp;
  KV kv;
};

struct Layout {
  bool doorbell;
  bool in_place;
  size_t bucket_bits;

  uint64_t num_clients;
  uint64_t num_servers;
  uint64_t majority;

  uint64_t num_tsp;

  // Range-lock arm. 0 disables locking entirely (the unlocked baseline, which
  // has a non-linearizable scan -- see range_lock.hpp). 1 is a single global
  // lock. Larger values stripe the key space.
  //
  // The region sits at offset 0 of the server and everything else shifts past
  // it, so a binary built with locking disabled and one built with it enabled
  // do NOT share an address space. They are separate runs, never mixed.
  uint64_t lock_stripes;

  // WHICH LOCK, when one is enabled at all.
  //
  // 0 = striped (range_lock.hpp): lock_stripes cells, each covering
  //     kKeysPerStripe of key space, so a scan rounds up to whole stripes and
  //     blocks writers to keys it never reads.
  // 1 = range table (range_table_lock.hpp): ONE CELL PER CLIENT, each holding
  //     that client's published interval, so a scan blocks exactly the keys it
  //     reads.
  //
  // Both are selected by lock_stripes > 0; this only says which. Kept as a
  // separate field rather than overloading lock_stripes, because two meanings
  // for one value across an interface is how a disabled arm reports itself as
  // locked -- the same mistake range_lock.hpp's header already records.
  uint64_t lock_mode;

  /// Cells in the lock region. The striped lock wants one per stripe; the
  /// range table wants one per CLIENT, because each client publishes into a
  /// slot it owns and no two clients ever contend on a cell.
  uint64_t lockCells() const {
    if (lock_stripes == 0) return 0;
    return lock_mode == 1 ? num_clients : lock_stripes;
  }

  bool guess_ts;

  uint64_t firstServerId() { return 1; }
  uint64_t firstClientId() { return num_servers + 1; }

  uint64_t async_parallelism;

  uint64_t keys_per_server;
  uint64_t server_logs_per_client;

  uint32_t key_size;
  uint32_t value_size;

  uint32_t rawKVSize() const { return key_size + value_size; }
  uint32_t tspArraySize() const {
    return static_cast<uint32_t>(sizeof(uint64_t) * num_tsp);
  }
  uint32_t rawDataSize() const { return rawKVSize() + tspArraySize(); }
  
  uint32_t partialKVSize() const {
    return static_cast<uint32_t>(sizeof(KV)) + rawKVSize();
  }
  uint32_t fullKVSize() const {
    return static_cast<uint32_t>(sizeof(KV)) + rawDataSize();
  }

  uint64_t rawKVOffset() const { return sizeof(KV); }
  uint64_t keyOffset() const { return sizeof(KV); }
  uint64_t valueOffset() const { return keyOffset() + key_size; }
  uint64_t tspArrayOffset() const { return valueOffset() + value_size; }
  uint64_t clientTspOffset(uint64_t client_idx) const {
    return tspArrayOffset() + sizeof(uint64_t) * (client_idx % num_tsp);
  }

  uint32_t partialLogEntrySize() const {
    return static_cast<uint32_t>(sizeof(LogEntry)) + rawKVSize();
  }
  uint32_t fullLogEntrySize() const {
    return static_cast<uint32_t>(sizeof(LogEntry)) + rawDataSize();
  }

  uint64_t fullKVSizeWithPadding() const {
    return (((static_cast<uint64_t>(fullKVSize()) - 1) >> 6) + 1) << 6;
  }
  uint64_t fullLogEntrySizeWithPadding() const {
    return (((static_cast<uint64_t>(fullLogEntrySize()) - 1) >> 6) + 1) << 6;
  }
  uint64_t partialLogEntrySizeWithPadding() const {
    return (((static_cast<uint64_t>(partialLogEntrySize()) - 1) >> 6) + 1) << 6;
  }

  uint64_t serverLogSize() const {
    return partialLogEntrySizeWithPadding() * server_logs_per_client *
           num_clients;
  }
  uint64_t clientLogSize() const {
    return fullLogEntrySizeWithPadding() * (1 + num_servers) *
           async_parallelism;
  }

  /// One cacheline per stripe. Two stripes in one line would be one lock as
  /// far as the NIC's atomic unit and the coherence traffic are concerned, and
  /// the striped arm would silently measure the global one.
  static constexpr uint64_t kLockStride = 64;
  uint64_t lockRegionSize() const {
    return kLockStride * lockCells();
  }
  static uint64_t lockRegionOffset() { return 0; }
  uintptr_t getLockAddress(uintptr_t region, uint64_t stripe) const {
    if (lock_stripes == 0 || stripe >= lockCells()) {
      throw std::invalid_argument(
          fmt::format("Lock stripe out of range: {} (num: {})", stripe,
                      lock_stripes));
    }
    return region + lockRegionOffset() + kLockStride * stripe;
  }

  // No longer static: the logs now start past the lock region.
  uint64_t serverLogsOffset() const { return lockRegionSize(); }
  uint64_t serverDataOffset() const {
    return lockRegionSize() + serverLogSize();
  }
  uint64_t serverSize() const {
    return lockRegionSize() + serverLogSize() +
           fullKVSizeWithPadding() * keys_per_server;
  }

  static uint64_t clientLogsOffset() { return 0; }
  /// One cacheline of client-local memory for the lock's CAS result. The CAS
  /// returns the PRE-image into local memory, so it needs a registered landing
  /// slot of its own -- reusing a log entry would corrupt an in-flight write.
  /// One cacheline of client-local memory. The striped lock lands a CAS
  /// pre-image here; the range table stages the four words it publishes and
  /// they must be in registered memory to be the source of an RDMA write.
  uint64_t lockScratchSize() const { return lock_stripes == 0 ? 0 : 64; }
  uint64_t clientSize() const { return clientLogSize() + lockScratchSize(); }
  uintptr_t getLockScratchAddress() const {
    return client_local_region + clientLogSize();
  }

  // Access the remote server's RDMA memory (only used in clients):
  uintptr_t getServerLogAddress(uintptr_t region, uint64_t client_idx,
                                uint64_t log_id) const {
    if (client_idx >= num_clients) {
      throw std::invalid_argument(fmt::format(
          "Client id out of range: {} (num: {})", client_idx, num_clients));
    }
    if (log_id >= server_logs_per_client) {
      throw std::invalid_argument(fmt::format(
          "Log id out of range: {} (num: {})", log_id, server_logs_per_client));
    }
    return region + serverLogsOffset() +
           partialLogEntrySizeWithPadding() *
               (log_id + server_logs_per_client * client_idx);
  }

  uintptr_t getServerLogKVAddress(uintptr_t region, uint64_t client_idx,
                                  uint64_t log_id) const {
    return getServerLogAddress(region, client_idx, log_id) +
           offsetof(LogEntry, kv);
  }

  uintptr_t getServerKVAddress(uintptr_t region, uint64_t kv_id) const {
    if (kv_id >= keys_per_server) {
      throw std::invalid_argument(fmt::format(
          "Key id out of range: {} (num: {})", kv_id, keys_per_server));
    }
    return region + serverDataOffset() + fullKVSizeWithPadding() * kv_id;
  }

  // Access the client's local RDMA memory (only used in the client):
  uintptr_t client_local_region;

 private:
  uintptr_t getClientLogAddress(uint64_t logblock_id,
                                uint64_t sublog_id) const {
    if (sublog_id >= (1 + num_servers)) {
      throw std::invalid_argument(fmt::format(
          "Sub-log id out of range: {} (num: 1 + {})", sublog_id, num_servers));
    }
    if (logblock_id >= async_parallelism) {
      throw std::invalid_argument(
          fmt::format("Log-block id out of range: {} (num: {})", logblock_id,
                      async_parallelism));
    }
    return client_local_region + clientLogsOffset() +
           fullLogEntrySizeWithPadding() *
               (sublog_id + logblock_id * (1 + num_servers));
  }

 public:
  uintptr_t getClientReadLogAddress(uint64_t logblock_id,
                                    uint64_t server_idx) const {
    if (server_idx >= num_servers) {
      throw std::invalid_argument(fmt::format(
          "Server id out of range: {} (num: {})", server_idx, num_servers));
    }
    return getClientLogAddress(logblock_id, 1 + server_idx);
  }

  uintptr_t getClientWriteLogAddress(uint64_t logblock_id) const {
    return getClientLogAddress(logblock_id, 0);
  }

  LogEntry* getClientReadLog(uint64_t logblock_id, uint64_t server_idx) const {
    return reinterpret_cast<LogEntry*>(
        getClientReadLogAddress(logblock_id, server_idx));
  }

  LogEntry* getClientWriteLog(uint64_t logblock_id) const {
    return reinterpret_cast<LogEntry*>(getClientWriteLogAddress(logblock_id));
  }

  // KV accessors:
  char* rawKVOf(KV& kv) {
    return reinterpret_cast<char*>(reinterpret_cast<uint8_t*>(&kv) +
                                   rawKVOffset());
  }

  char* keyOf(KV& kv) {
    return reinterpret_cast<char*>(reinterpret_cast<uint8_t*>(&kv) +
                                   keyOffset());
  }

  char* valueOf(KV& kv) {
    return reinterpret_cast<char*>(reinterpret_cast<uint8_t*>(&kv) + valueOffset());
  }

  uint64_t* tspArrayOf(KV& kv) {
    return reinterpret_cast<uint64_t*>(reinterpret_cast<uint8_t*>(&kv) +
                                       tspArrayOffset());
  }

  uint64_t maxTspOf(KV& kv) {
    auto* tsps = tspArrayOf(kv);
    uint64_t tsp = 0;
    for (uint64_t i = 0; i < num_tsp; i++) {
      tsp = std::max(tsp, tsps[i]);
    }
    return tsp;
  }

  uint64_t& myTspOf(KV& kv, uint64_t client_idx) {
    return tspArrayOf(kv)[client_idx % num_tsp];
  }

  void hashOfTo(KV& kv, chsum_t& out) {
    XXH3_128bits_reset(hash_state);
    uint64_t tsp = kv.in_place_tsp;
    tsp = filterLogIdAndClientProcId(tsp);  // Only hash log_id and client_id
    XXH3_128bits_update(hash_state, &tsp, sizeof(uint64_t));
    XXH3_128bits_update(hash_state, rawKVOf(kv), rawKVSize());
    *reinterpret_cast<XXH128_hash_t*>(out.data()) =
        XXH3_128bits_digest(hash_state);
  }
};
