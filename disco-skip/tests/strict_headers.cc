// Compiles every dory-independent header under (approximately) the dory
// toolchain's warning set, so that a -Werror failure on the cluster is caught
// here first. The cluster build uses -Werror with -Wconversion, -Wold-style-cast,
// -Wcast-align, -Wsign-promo, -Wclass-memaccess and more; the cache half's own
// Makefile uses only -Wall -Wextra -Wpedantic, which is why the mismatch bit us
// once already (a memset over a type with a user-provided default constructor).
//
// Only headers that do not need libibverbs can be checked this way. main.cpp,
// disco_skip_state.hpp and the futures still need a real cluster build.

#include "ds_remote_addr.hpp"
#include "ds_defs.hpp"
#include "ds_node.hpp"
#include "layout.hpp"
#include "ds_bootstrap.hpp"
#include "ds_verify.hpp"

// The templated paths. Including them here checks their declarations under the
// strict set; their *bodies* are only checked once instantiated, which happens
// under the same flags in rdma_compile.cc over the real RdmaOps -- the
// combination the cluster actually runs.
#include "ds_traverse.hpp"
#include "ds_get.hpp"
#include "ds_get_future.hpp"
#include "ds_insert.hpp"
#include "ds_put.hpp"
#include "ds_async.hpp"
#include "ds_quorum.hpp"

// Reference enough of each header that the compiler cannot skip instantiating
// the parts we care about.
int main() {
  ds::NodeRecord n;
  ds::initNode(n, 42, 0, /*vec=*/1);
  ds::VecRecord v;
  ds::initVec(v, /*is_orphan=*/false, /*ts=*/1);
  v.size = 1;
  v.e[0] = ds::Entry{42, 7};

  ds::Layout l{};
  l.num_clients = 1;
  l.num_servers = 1;
  l.async_parallelism = 1;
  l.num_registers = 8;
  l.max_range = 1;
  l.majority = 0;
  l.cache_layers = 2;
  l.nodes_per_client = 8;
  l.vecs_per_client = 8;
  l.offset_hint = true;
  l.client_local_region = 0;

  ds::NodeAllocator alloc(0, l.nodes_per_client);
  ds::VecAllocator valloc(0, l.vecs_per_client);
  ds::VecOffsetHint hint(16, true);

  ds::InitialNode built[ds::kMaxLayers + 1];
  uint32_t const built_n = ds::buildInitialStructure(2, /*ts=*/1, built);
  auto const heads = ds::headAddrs(2);

  // Instantiate the verifier against a trivial reader so its body is compiled
  // under the strict set too -- it is a template, so nothing else would.
  struct NullReader {
    bool read(ds::RemoteAddr, ds::NodeRecord &, ds::VecRecord &) { return false; }
    bool readVec(ds::VecOffset, ds::VecRecord &) { return false; }
  } null_reader;
  ds::VerifyReport const rep = ds::verifyStructure(null_reader, 2);

  return static_cast<int>(
      static_cast<unsigned>(n.isStable()) +
      static_cast<unsigned>(v.isPending()) +
      static_cast<unsigned>(v.hasSplitDescriptor()) +
      static_cast<unsigned>(ds::vectorIsCurrent(n, v, 1)) +
      static_cast<unsigned>(ds::covers(n, v, 42)) +
      static_cast<unsigned>(ds::rangeEnd(n, v) != 0) +
      static_cast<unsigned>(ds::nextNode(n, v).isNull()) +
      static_cast<unsigned>(ds::findLte(v, 42) >= 0) +
      static_cast<unsigned>(n.handle.tag() != 0) +
      static_cast<unsigned>(l.nodeArenaNodes() != 0) +
      static_cast<unsigned>(l.vecArenaVecs() != 0) +
      static_cast<unsigned>(l.serverSize() != 0) +
      static_cast<unsigned>(ds::headAddr(0).isNull()) +
      static_cast<unsigned>(alloc.allocate().isNull()) +
      static_cast<unsigned>(valloc.allocate() == ds::kNullVec) +
      static_cast<unsigned>(hint.guess(ds::RemoteAddr{1}) == ds::kNullVec) +
      static_cast<unsigned>(built_n != 0) +
      static_cast<unsigned>(heads[0].isNull()) +
      static_cast<unsigned>(rep.ok()));
}
