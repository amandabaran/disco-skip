#pragma once

#include <cstdint>
#include <string>
#include <format>

// 1. Packed Register. Exactly 8 bytes for RDMA CAS and Read.
struct Register {
    union {
        uint64_t raw;
        struct {
            uint32_t value;      // 32 bits
            uint16_t client_id;  // 16 bits
            uint16_t seq;        // 16 bits
        } fields;
    };

    Register() : raw(0) {}
    Register(uint64_t val) : raw(val) {}
    Register(uint16_t s, uint16_t id, uint32_t v) {
        fields.seq = s;
        fields.client_id = id;
        fields.value = v;
    }

    bool operator<(const Register& other) const {
        if (fields.seq != other.fields.seq) return fields.seq < other.fields.seq;
        return fields.client_id < other.fields.client_id;
    }

    bool operator==(const Register& other) const { return raw == other.raw; }
};

static_assert(sizeof(Register) == 8, "RDMA CAS requires exactly 8 bytes");
static_assert(sizeof(Register) == sizeof(uint64_t), "Register must be CAS-able");
