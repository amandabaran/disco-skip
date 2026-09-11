#pragma once
// Compile-only stub. See dory_stub/README.md.
#include <cstdint>

enum ibv_wc_status : unsigned { IBV_WC_SUCCESS = 0, IBV_WC_GENERAL_ERR = 1 };
// Send flags. IBV_SEND_FENCE is the one that matters to us: F3 needs the
// staged writes visible at the remote HCA before the publishing CAS, and in a
// chained batch nothing waits, so the fence is what supplies that. dory never
// sets it, so we do -- which means the stub has to declare it.
enum ibv_send_flags : unsigned {
  IBV_SEND_FENCE = 1,
  IBV_SEND_SIGNALED = 2,
  IBV_SEND_INLINE = 8
};

enum ibv_wr_opcode : unsigned {
  IBV_WR_RDMA_WRITE = 0,
  IBV_WR_RDMA_READ = 4,
  IBV_WR_ATOMIC_CMP_AND_SWP = 5
};

struct ibv_wc {
  uint64_t wr_id;
  ibv_wc_status status;
};

struct ibv_sge {
  uint64_t addr;
  uint32_t length;
  uint32_t lkey;
};

struct ibv_send_wr {
  uint64_t wr_id;
  ibv_send_wr *next;
  ibv_sge *sg_list;
  int num_sge;
  ibv_wr_opcode opcode;
  unsigned send_flags;
};
