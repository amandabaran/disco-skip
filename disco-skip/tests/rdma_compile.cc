// Compiles ds_rdma.hpp against the dory stub, under the strict warning set.
//
// No behaviour is asserted -- the stub moves no data. The point is that the
// header type-checks and is warning-clean before it reaches the cluster, which
// three failed cluster builds argue is worth a file.

// The umbrella header, exactly as main.cpp includes it. That is the point:
// main.cpp is the one translation unit that cannot be compiled off-cluster, so
// checking ds.hpp here is what stops a missing include in it costing a cluster
// round trip -- which it has done once already.
#include "ds.hpp"

#include <iostream>

// Force instantiation of the templates; being templates, nothing above would.
using Conns = std::vector<dory::conn::ReliableConnection *>;

struct Bufs {
  ds::NodeRecord *node;
  ds::VecRecord *vec;
  ds::NodeRecord *stage_node;
  ds::VecRecord *stage_vec;
};

void instantiate_rdma_paths(Conns &conns, ds::Layout const &layout, Bufs b,
                            ds::VecOffsetHint *hint, ds::RemoteAddr addr,
                            uint32_t layers);
void instantiate_rdma_paths(Conns &conns, ds::Layout const &layout, Bufs b,
                            ds::VecOffsetHint *hint, ds::RemoteAddr addr,
                            uint32_t layers) {
  ds::writeNodeAllReplicas(conns, b.stage_node, addr);
  ds::writeVecAllReplicas(conns, layout, b.stage_vec, ds::kFirstDynamicVec);

  ds::RdmaNodeReader<Conns> reader(conns, layout, b.node, b.vec, hint);
  ds::NodeRecord out_node;
  ds::VecRecord out_vec;
  (void)reader.read(addr, out_node, out_vec);
  (void)reader.readVec(ds::kFirstDynamicVec, out_vec);
  (void)reader.reads();
  (void)reader.unstableRetries();
  (void)reader.staleRetries();

  // The verifier, driven by the RDMA reader -- the exact combination that runs
  // in --selftest.
  ds::VerifyReport const rep = ds::verifyStructure(reader, layers);
  (void)rep.ok();
}

// The traversal and Get, over the full RDMA Ops surface. Compile-only: this is
// the combination that runs on the cluster, so it must at least type-check
// here.
void instantiate_ops(Conns &conns, ds::Layout const &layout, Bufs b,
                     uint64_t *cas_buf, ds::VecOffsetHint *hint,
                     uint32_t layers);
void instantiate_ops(Conns &conns, ds::Layout const &layout, Bufs b,
                     uint64_t *cas_buf, ds::VecOffsetHint *hint,
                     uint32_t layers) {
  ds::RdmaOps<Conns> ops(conns, layout, b.node, b.vec, cas_buf, hint);

  ds::PathStep path[ds::kMaxLayers];
  ds::Traversal<ds::RdmaOps<Conns>> d(ops);
  ds::TraversalResult const r = d.traverse(42, layers, path);
  (void)r.ok();
  (void)ops.casCount();

  ds::NullCache null_cache;
  ds::GetStats stats;
  ds::Getter<ds::RdmaOps<Conns>, ds::NullCache> g(ops, null_cache, layers, stats);
  (void)g.get(42);
}

// The write path (F1/F2 and Update_Index) over the full RDMA Ops surface. Same
// argument as above: this is the combination the cluster runs, so it must
// type-check here rather than costing a build.
void instantiate_write_paths(Conns &conns, ds::Layout const &layout, Bufs b,
                             uint64_t *cas_buf, ds::VecOffsetHint *hint,
                             ds::NodeAllocator *nodes, ds::VecAllocator *vecs,
                             uint32_t layers);
void instantiate_write_paths(Conns &conns, ds::Layout const &layout, Bufs b,
                             uint64_t *cas_buf, ds::VecOffsetHint *hint,
                             ds::NodeAllocator *nodes, ds::VecAllocator *vecs,
                             uint32_t layers) {
  ds::RdmaOps<Conns> ops(conns, layout, b.node, b.vec, cas_buf, hint,
                         /*replica=*/0, b.stage_node, b.stage_vec, nodes, vecs);

  ds::WriteStats wstats;
  ds::Writer<ds::RdmaOps<Conns>> w(ops, wstats);
  ds::RemoteAddr const addr{ds::kInitialDataId};
  (void)w.insertEntry(addr, 42, 4242);
  (void)w.insertWithOverflow(addr, 43, 4343, nullptr);
  ds::Entry const seed{44, 4444};
  (void)w.splitAt(addr, 44, /*orphan=*/false, &seed);

  ds::PutStats pstats;
  ds::NullPutCache null_cache;
  ds::Putter<ds::RdmaOps<Conns>, ds::NullPutCache> p(ops, null_cache, layers,
                                                     pstats, wstats);
  (void)p.put(45, 4545, 2);

  (void)ops.bytesWritten();
  (void)ops.batches();
}

// The --selftest body, against the real RDMA types. This is the instantiation
// that closes the gap main.cpp leaves: the selftest logic no longer lives in an
// uncompilable translation unit, so a mistyped field in it fails here in a
// second instead of costing a cluster build.
void instantiate_selftest(Conns &conns, ds::Layout const &layout, Bufs b,
                          uint64_t *cas_buf, ds::VecOffsetHint *hint,
                          ds::NodeAllocator *nodes, ds::VecAllocator *vecs,
                          uint32_t layers);
void instantiate_selftest(Conns &conns, ds::Layout const &layout, Bufs b,
                          uint64_t *cas_buf, ds::VecOffsetHint *hint,
                          ds::NodeAllocator *nodes, ds::VecAllocator *vecs,
                          uint32_t layers) {
  ds::RdmaNodeReader<Conns> reader(conns, layout, b.node, b.vec, hint);
  ds::RdmaOps<Conns> ops(conns, layout, b.node, b.vec, cas_buf, hint,
                         /*replica=*/0, b.stage_node, b.stage_vec, nodes, vecs);
  (void)ds::runSelftest(reader, ops, layers, hint, std::cout);
}

// The bootstrap write loop, in the shape main.cpp uses it.
void instantiate_bootstrap(Conns &conns, ds::Layout const &layout, Bufs b,
                           uint32_t layers);
void instantiate_bootstrap(Conns &conns, ds::Layout const &layout, Bufs b,
                           uint32_t layers) {
  ds::InitialNode built[ds::kMaxLayers + 1];
  uint32_t const count =
      ds::buildInitialStructure(layers, ds::kBootstrapTs, built);
  for (uint32_t i = 0; i < count; ++i) {
    *b.stage_vec = built[i].vec;
    ds::writeVecAllReplicas(conns, layout, b.stage_vec, built[i].vec_offset);
    *b.stage_node = built[i].node;
    ds::writeNodeAllReplicas(conns, b.stage_node, built[i].addr);
  }
}

// The driver glue: a skip-vector future bound to DsState, in the contract
// DsClient drives. This is the piece that was previously unverifiable until a
// cluster build -- it needs DsState, which needs fmt and the connection
// exchanger, both now stubbed for exactly this reason.
void instantiate_future(ds::DsState &state, ds::QuorumStats &q, ds::GetStats &g,
                        ds::PutStats &p, ds::WriteStats &w);
void instantiate_future(ds::DsState &state, ds::QuorumStats &q, ds::GetStats &g,
                        ds::PutStats &p, ds::WriteStats &w) {
  // ClientCache, not NullCache: with DS_CACHE_ENABLED=1 that is CacheAdapter,
  // and the operation templates instantiated over it are different code from
  // the ones over NullCache. main.cpp instantiates exactly this, so this is
  // where it gets checked -- compiling only the NullCache arm left the real
  // one unchecked until a cluster build, which is how a -Wstrict-overflow
  // error in it reached the cluster.
#if DS_CACHE_ENABLED
  // Built from the state's own SkipVec, exactly as DsClient does it.
  ds::ClientCache cache(state.cache_sv,
                        static_cast<uint32_t>(state.layout.cache_layers),
                        state.layout.consult_cache);
#else
  ds::ClientCache cache;
#endif
  ds::RangeStats rq;
  ds::SvFuture<ds::ClientCache> f(state, /*id=*/0, cache, q, g, p, w, rq);
  f.doGet(42);
  f.doPut(43, 4343, 2);
  // A10 through the future, which is how main.cpp issues a scan.
  f.doRange(10, ds::kUnboundedKey, /*cap=*/8);
  (void)f.rangeResult().snapshot;
  (void)f.rangeEntries().size();
  f.addToOngoingRDMA(0, -1);
  (void)f.tryStepForward();
  (void)f.isDone();
  (void)f.isMeasuring();
  (void)f.getStart();
  (void)f.getResult().resolved;
  (void)f.putResult().resolved;

  // A10. Instantiated against the REAL RdmaAsyncOps so that postTsCounter,
  // resolveTsCounter and the walk are type-checked against dory's surface
  // rather than only against the fake -- the class of error this file exists
  // for. It is never stepped here; the stub moves no data.
  {
    using AsyncOps = ds::SvFuture<ds::ClientCache>::AsyncOps;
    std::vector<int64_t> ongoing(state.layout.num_servers, 0);
    AsyncOps aops(state.server_conns, state.layout, state.to_poll_per_server,
                  ongoing, /*future_id=*/0, q, &state.vec_hint,
                  &state.node_alloc, &state.vec_alloc);
    ds::RangeStats rstats;
    ds::RangeOperation<AsyncOps> r(aops, 4, rstats);
    std::vector<ds::Entry> out;
    (void)r.start(10, 20, 64, out);
    (void)r.startAt(10, 20, 64, /*snapshot=*/1, out);
    (void)r.step();
    (void)r.finished();
    (void)r.result().snapshot;
  }
}

int main() { return 0; }
