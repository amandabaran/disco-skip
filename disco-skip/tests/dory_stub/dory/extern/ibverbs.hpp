#pragma once
// Compile-only stub. See dory_stub/README.md.
#include <cstdint>

enum ibv_wc_status : unsigned { IBV_WC_SUCCESS = 0, IBV_WC_GENERAL_ERR = 1 };
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
