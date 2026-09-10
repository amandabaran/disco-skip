// Compiles ds_rdma.hpp against the dory stub, under the strict warning set.
//
// No behaviour is asserted -- the stub moves no data. The point is that the
// header type-checks and is warning-clean before it reaches the cluster, which
// three failed cluster builds argue is worth a file.

#include "ds_bootstrap.hpp"
#include "ds_node.hpp"
#include "ds_rdma.hpp"
#include "ds_verify.hpp"
#include "layout.hpp"

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

// The bootstrap write loop, in the shape main.cpp uses it.
void instantiate_bootstrap(Conns &conns, ds::Layout const &layout, Bufs b,
                           uint32_t layers);
void instantiate_bootstrap(Conns &conns, ds::Layout const &layout, Bufs b,
                           uint32_t layers) {
  ds::InitialNode built[ds::kMaxLayers + 1];
  uint32_t const count = ds::buildInitialStructure(layers, /*ts=*/1, built);
  for (uint32_t i = 0; i < count; ++i) {
    *b.stage_vec = built[i].vec;
    ds::writeVecAllReplicas(conns, layout, b.stage_vec, built[i].vec_offset);
    *b.stage_node = built[i].node;
    ds::writeNodeAllReplicas(conns, b.stage_node, built[i].addr);
  }
}

int main() { return 0; }
