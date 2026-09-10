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

void instantiate_rdma_paths(Conns &conns, ds::NodeRecord *staging,
                            ds::RemoteAddr addr, uint32_t layers);
void instantiate_rdma_paths(Conns &conns, ds::NodeRecord *staging,
                            ds::RemoteAddr addr, uint32_t layers) {
  ds::writeNodeAllReplicas(conns, staging, addr);

  ds::RdmaNodeReader<Conns> reader(conns, staging);
  ds::NodeRecord out;
  (void)reader.read(addr, out);
  (void)reader.reads();
  (void)reader.tornReads();

  // The verifier, driven by the RDMA reader -- the exact combination that runs
  // in --selftest.
  ds::VerifyReport const rep = ds::verifyStructure(reader, layers);
  (void)rep.ok();
}

// The bootstrap write loop, in the shape main.cpp uses it.
void instantiate_bootstrap(Conns &conns, ds::NodeRecord *staging, uint32_t layers);
void instantiate_bootstrap(Conns &conns, ds::NodeRecord *staging, uint32_t layers) {
  ds::InitialNode built[ds::kMaxLayers + 1];
  uint32_t const count = ds::buildInitialStructure(layers, built);
  for (uint32_t i = 0; i < count; ++i) {
    *staging = built[i].record;
    ds::writeNodeAllReplicas(conns, staging, built[i].addr);
  }
}

int main() { return 0; }
