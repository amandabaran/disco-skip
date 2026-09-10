#pragma once
// Compile-only stub of dory::conn::ReliableConnection. See dory_stub/README.md.
//
// Signatures mirror conn/src/rc.hpp so that a call which compiles here compiles
// there. Bodies are absent on purpose: linking is not the point, and a body
// would invite someone to assert on it.
#include <cstdint>
#include <vector>

#include <dory/extern/ibverbs.hpp>

namespace dory::conn {

class ReliableConnection {
 public:
  enum Cq { SendCq, RecvCq };
  enum RdmaReq { RdmaRead = IBV_WR_RDMA_READ, RdmaWrite = IBV_WR_RDMA_WRITE };

  static int constexpr WrDepth = 128;
  static int constexpr MaxInlining = 256;
  static int constexpr CasLength = sizeof(uint64_t);

  bool postSendSingle(RdmaReq req, uint64_t req_id, void *buf, uint32_t len,
                      uintptr_t remote_addr, bool signaled = true);
  bool postSendSingleCas(uint64_t req_id, void *buf, uintptr_t remote_addr,
                         uint64_t expected, uint64_t swap,
                         bool signaled = true);
  void prepareSingle(ibv_send_wr &wr, ibv_sge &sg, RdmaReq req, uint64_t req_id,
                     void *buf, uint32_t len, uintptr_t remote_addr,
                     bool signaled = true);
  void prepareSingleCas(ibv_send_wr &wr, ibv_sge &sg, uint64_t req_id, void *buf,
                        uintptr_t remote_addr, uint64_t expected, uint64_t swap,
                        bool signaled = true);
  bool postSend(ibv_send_wr &wr);
  bool pollCqIsOk(Cq cq, std::vector<struct ibv_wc> &entries) const;

  uintptr_t remoteBuf() const;
  uint64_t remoteSize() const;
};

}  // namespace dory::conn
