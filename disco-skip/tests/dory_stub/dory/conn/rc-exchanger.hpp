#pragma once
// Compile-only stub of the connection exchanger. See dory_stub/README.md.
//
// Only the shape DsState needs: an exchanger whose connections() yields
// something indexable by .at(). Nothing here connects anything.
#include <cstddef>
#include <vector>

#include "rc.hpp"

namespace dory::conn {

template <typename ProcId>
class RcConnectionExchanger {
 public:
  std::vector<ReliableConnection> &connections() { return conns_; }

 private:
  std::vector<ReliableConnection> conns_;
};

}  // namespace dory::conn
