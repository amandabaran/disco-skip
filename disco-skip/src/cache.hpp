#pragma once
#include <vector>
#include "register.hpp"

class Cache {
    std::vector<Register> data_;
    std::vector<bool> valid_;  // Bit-packed validity tracking since all 0s is a valid register state; Only N/8 bytes needed for N entries 

public:
    Cache(size_t size) : data_(size), valid_(size, false) {}

    void put(size_t offset, Register reg) {
        data_[offset] = reg;
        valid_[offset] = true;
    }

    bool get(size_t offset, Register& out_reg) const {
        if (valid_[offset]) {
            out_reg = data_[offset];
            return true;
        }
        return false;
    }

    bool contains(size_t offset) const {
        return valid_[offset];
    }

    void clear(size_t offset) {
        valid_[offset] = false;
    }

    void clear_all() {
        std::fill(valid_.begin(), valid_.end(), false);
    }

    size_t size() const { return data_.size(); }
};
