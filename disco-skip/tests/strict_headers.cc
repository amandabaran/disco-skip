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

// Reference enough of each header that the compiler cannot skip instantiating
// the parts we care about.
int main() {
  ds::NodeRecord n;
  ds::initNode(n, 42, 0, false);
  ds::VecSlot &s = n.current();
  s.size = 1;
  s.e[0] = ds::Entry{42, 7};
  ds::stampSlot(s, n.handle);

  ds::Layout l{};
  l.num_clients = 1;
  l.num_servers = 1;
  l.async_parallelism = 1;
  l.num_registers = 8;
  l.max_range = 1;
  l.majority = 0;
  l.cache_layers = 2;
  l.nodes_per_client = 8;
  l.client_local_region = 0;

  ds::NodeAllocator alloc(0, l.nodes_per_client);

  return static_cast<int>(
      static_cast<unsigned>(ds::slotIsConsistent(n)) +
      static_cast<unsigned>(ds::covers(n, 42)) +
      static_cast<unsigned>(ds::findLte(s, 42) >= 0) +
      static_cast<unsigned>(n.handle.tag() != 0) +
      static_cast<unsigned>(l.nodeArenaNodes() != 0) +
      static_cast<unsigned>(ds::Layout::headAddr(0).isNull()) +
      static_cast<unsigned>(alloc.allocate().isNull()));
}
