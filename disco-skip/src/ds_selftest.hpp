#pragma once

// The --selftest body, extracted from main.cpp and templated.
//
// WHY THIS IS A HEADER. main.cpp is the one translation unit that cannot be
// compiled off-cluster -- it needs lyra, fmt and the dory control plane -- so
// anything living in it is unchecked until a cluster build. That has now cost
// six builds, the most recent to a mistyped VerifyReport field name, which is
// about as cheap a mistake as exists and still cost a full round trip.
//
// Templating over Reader and Ops fixes that twice over:
//
//   - tests/rdma_compile.cc instantiates it against the real RdmaNodeReader and
//     RdmaOps, so the body type-checks locally under the cluster's warning set;
//   - tests/selftest_test.cc instantiates it against FakeOps and *runs* it, so
//     the logic is exercised rather than only compiled.
//
// main.cpp keeps only what genuinely needs the cluster: building the reader and
// the Ops surface out of DsState, and calling this.
//
// SEQUENTIAL BY CONSTRUCTION. Runs with no futures in flight, which both the
// blocking RDMA helpers and the verifier require.

#include <cstdint>
#include <ostream>

#include "ds_defs.hpp"
#include "ds_descend.hpp"
#include "ds_insert.hpp"
#include "ds_node.hpp"
#include "ds_put.hpp"
#include "ds_verify.hpp"
#include "layout.hpp"

namespace ds {

/// One scripted write, so the structure the write phase builds is deterministic
/// and the counts it reports are an exact expectation rather than a
/// distribution. If they move, something changed.
struct SelftestWrite {
  Key k;
  Value v;
  uint32_t height;
};

/// The fixed script: ascending, descending and interleaved keys, one repeat to
/// exercise the update path, and one repeated boundary to exercise the split
/// no-op. Heights are fixed rather than drawn for the same reason.
inline constexpr SelftestWrite kSelftestWrites[] = {
    {100, 1100, 0}, {300, 1300, 0}, {200, 1200, 0},
    {400, 1400, 1},  // a boundary: splits the data level
    {150, 1150, 0},  // lands left of that boundary
    {500, 1500, 2},  // climbs to level 0 and enters level 1
    {400, 9400, 1},  // repeat: an update, and a boundary no-op
    {600, 1600, 0},  {50, 1050, 0},
};
inline constexpr size_t kSelftestWriteCount =
    sizeof(kSelftestWrites) / sizeof(kSelftestWrites[0]);

/// A key never written, so the read-back below cannot pass on a structure that
/// simply answers everything.
inline constexpr Key kSelftestAbsentKey = 777;

/// Walk the remote structure and check invariants.md §1.
///
/// @return 0 if every phase passed, 1 otherwise.
template <class Reader>
int selftestStructure(Reader &reader, uint32_t layers, VecOffsetHint const *hint,
                      std::ostream &out) {
  out << "\n################ Structure selftest:" << std::endl;

  VerifyReport const rep = verifyStructure(reader, layers);

  out << "index nodes:  " << rep.nodes_visited << " (" << rep.orphans
      << " orphans, " << rep.entries << " entries)" << std::endl;
  for (size_t L = 0; L < rep.nodes_per_level.size(); ++L) {
    out << "  level " << L << ": " << rep.nodes_per_level[L] << " nodes"
        << std::endl;
  }
  out << "data nodes:   " << rep.data_nodes << " (" << rep.data_entries
      << " entries)" << std::endl;
  out << "old versions: " << rep.old_versions << std::endl;
  out << "rdma reads:   " << reader.reads() << " (" << reader.nodeReads()
      << " headers, " << reader.vecReads() << " vectors, " << reader.bytesRead()
      << " bytes)" << std::endl;
  out << "              " << reader.unstableRetries() << " mid-split retries, "
      << reader.staleRetries() << " stale-vector retries" << std::endl;
  if (hint != nullptr && hint->enabled()) {
    out << "offset hint:  hit rate " << hint->hitRate() << " (" << hint->hits()
        << " hit / " << hint->misses() << " miss)" << std::endl;
  } else {
    out << "offset hint:  disabled" << std::endl;
  }

  if (!rep.ok()) {
    out << "SELFTEST FAIL: " << rep.errors.size() << " problem(s)" << std::endl;
    for (auto const &e : rep.errors) out << "  " << e << std::endl;
    return 1;
  }
  out << "SELFTEST PASS: I1-I4 hold" << std::endl;
  return 0;
}

/// Exercise the descent over the fabric.
///
/// The structure is empty at this point, so every key resolves to the single
/// data node and finds nothing -- which is exactly the interesting assertion: a
/// descent over an empty structure must reach the data node and report absent,
/// never fail and never miss.
template <class Ops>
int selftestDescent(Ops &ops, uint32_t layers, std::ostream &out) {
  out << "\n################ Descent:" << std::endl;
  Descender<Ops> descender(ops);

  PathStep path[kMaxLayers];
  uint32_t ok = 0, absent = 0, bad = 0;
  Key const probes[] = {0, 1, 42, 1000, 1u << 20};
  for (Key k : probes) {
    DescentResult const r = descender.descend(k, layers, path);
    if (!r.ok()) {
      out << "  key " << k << ": descent failed, status "
          << static_cast<int>(r.status) << std::endl;
      ++bad;
      continue;
    }
    ++ok;
    if (!r.found) ++absent;
    // Every level's covering node must start at or below the key, and the
    // staircase must be non-increasing as it climbs.
    for (uint32_t L = 0; L + 1 < layers; ++L) {
      if (path[L + 1].k_min > path[L].k_min) {
        out << "  key " << k << ": staircase inverted between level " << L
            << " and " << (L + 1) << std::endl;
        ++bad;
      }
    }
    if (r.data_k_min > k) {
      out << "  key " << k << ": data node k_min " << r.data_k_min
          << " is above the key" << std::endl;
      ++bad;
    }
  }
  out << "probes:       " << ok << " resolved, " << absent
      << " absent (expected: all, the structure is empty)" << std::endl;
  out << "rdma:         " << ops.nodeReads() << " headers, " << ops.vecReads()
      << " vectors, " << ops.casCount() << " CAS" << std::endl;

  if (bad != 0) {
    out << "DESCENT FAIL: " << bad << " problem(s)" << std::endl;
    return 1;
  }
  if (ok != sizeof(probes) / sizeof(probes[0]) || absent != ok) {
    out << "DESCENT FAIL: expected every probe to resolve and be absent"
        << std::endl;
    return 1;
  }
  out << "DESCENT PASS: every probe reached the data node and reported absent"
      << std::endl;
  return 0;
}

/// Drive F1 and F2 over the fabric, then read every key back.
///
/// F1 and F2 are tested exhaustively off-cluster, where a mid-split state can
/// be built by hand. What only a real fabric can tell us is that the writes,
/// the fence and the CASes actually work against the HCA -- that a staged vector
/// lands, that a handle CAS from the value we read succeeds, and that the
/// structure is still I1-I4 correct afterwards.
template <class Reader, class Ops>
int selftestWrites(Reader &reader, Ops &ops, uint32_t layers,
                   std::ostream &out) {
  out << "\n################ Write path:" << std::endl;

  PutStats pstats;
  WriteStats wstats;
  NullPutCache put_cache;
  Putter<Ops, NullPutCache> putter(ops, put_cache, layers, pstats, wstats);

  uint32_t wrote = 0, wfail = 0;
  for (SelftestWrite const &w : kSelftestWrites) {
    PutResult const r = putter.put(w.k, w.v, w.height);
    if (!r.resolved) {
      out << "  key " << w.k << " (h=" << w.height << "): put did not resolve"
          << std::endl;
      ++wfail;
      continue;
    }
    ++wrote;
  }

  // Read every key back through an ordinary descent. This is the end-to-end
  // assertion: the bytes a write put on the memory server are the bytes a
  // reader that knows nothing about that write finds.
  Descender<Ops> descender(ops);
  PathStep path[kMaxLayers];
  uint32_t readback = 0, mismatch = 0;
  for (SelftestWrite const &w : kSelftestWrites) {
    // The script writes one key twice; the later value is the one that must be
    // readable. Derived from the script rather than hardcoded, so editing the
    // script cannot leave a stale expectation behind.
    Value want = w.v;
    for (SelftestWrite const &later : kSelftestWrites) {
      if (later.k == w.k) want = later.v;
    }
    DescentResult const r = descender.descend(w.k, layers, path);
    if (!r.ok() || !r.found || r.value != want) {
      out << "  key " << w.k << ": expected " << want << ", got ";
      if (!r.ok()) {
        out << "descent failure";
      } else if (!r.found) {
        out << "absent";
      } else {
        out << r.value;
      }
      out << std::endl;
      ++mismatch;
      continue;
    }
    ++readback;
  }

  DescentResult const never =
      descender.descend(kSelftestAbsentKey, layers, path);
  bool const absent_ok = never.ok() && !never.found;
  if (!absent_ok) {
    out << "  key " << kSelftestAbsentKey
        << " was never written but did not read as absent" << std::endl;
  }

  out << "puts:         " << wrote << " resolved, " << wfail << " failed ("
      << pstats.height0 << " data-only, " << pstats.structural << " structural)"
      << std::endl;
  out << "splits:       " << pstats.data_splits << " data, "
      << pstats.index_splits << " index, " << wstats.capacity_splits
      << " capacity; " << wstats.boundary_noops << " boundary no-op(s)"
      << std::endl;
  out << "rdma writes:  " << ops.nodeWrites() << " nodes, " << ops.vecWrites()
      << " vectors, " << ops.bytesWritten() << " bytes; " << ops.casCount()
      << " CAS total" << std::endl;
  // The number the chaining exists to reduce. Every write goes out as one
  // chained batch, so this is round trips -- not the operation count above it,
  // which is unchanged by batching. The ratio between them is the win.
  uint64_t const ops_issued =
      ops.nodeWrites() + ops.vecWrites() + ops.casCount();
  out << "round trips:  " << ops.batches() << " chained batches carrying "
      << ops_issued << " operations" << std::endl;
  out << "readback:     " << readback << "/" << kSelftestWriteCount
      << " keys match" << std::endl;

  // And the structure must still be correct. This is the assertion a read-back
  // alone would not make: a wrongly linked structure can still answer for the
  // keys it happens to reach.
  VerifyReport const rep = verifyStructure(reader, layers);
  out << "index nodes:  " << rep.nodes_visited << " (" << rep.orphans
      << " orphans, " << rep.entries << " entries)" << std::endl;
  out << "data nodes:   " << rep.data_nodes << " (" << rep.data_entries
      << " entries)" << std::endl;
  out << "old versions: " << rep.old_versions << std::endl;

  if (wfail != 0 || mismatch != 0 || !absent_ok || !rep.ok()) {
    for (auto const &e : rep.errors) out << "  " << e << std::endl;
    out << "WRITE FAIL: " << wfail << " unresolved, " << mismatch
        << " mismatched, structure " << (rep.ok() ? "ok" : "BROKEN")
        << std::endl;
    return 1;
  }
  out << "WRITE PASS: F1/F2 committed over RDMA, every key reads back, "
         "I1-I4 hold"
      << std::endl;
  return 0;
}

/// All three phases, in order. Stops at the first failure, because a broken
/// structure makes the later phases' output noise rather than evidence.
template <class Reader, class Ops>
int runSelftest(Reader &reader, Ops &ops, uint32_t layers,
                VecOffsetHint const *hint, std::ostream &out) {
  if (int const rc = selftestStructure(reader, layers, hint, out); rc != 0) {
    return rc;
  }
  if (int const rc = selftestDescent(ops, layers, out); rc != 0) return rc;
  return selftestWrites(reader, ops, layers, out);
}

}  // namespace ds
