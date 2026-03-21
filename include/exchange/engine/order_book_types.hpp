#pragma once

#include "exchange/engine/types.hpp"
#include <cstdint>
#include <limits>

namespace exchange::engine {
struct PriceLevel;

// Intrusive doubly-linked list node stored in a flat MemoryPool slab.
// prev_idx / next_idx are uint32_t offsets into the slab rather than raw
// pointers, halving the per-node pointer footprint and improving cache density.
// NULL_IDX (UINT32_MAX) is the sentinel for "no neighbour".
struct OrderNode {
    static constexpr uint32_t NULL_IDX = std::numeric_limits<uint32_t>::max();

    uint64_t order_id{0};
    uint32_t quantity{0};
    Side side{Side::BUY};
    // 3 bytes implicit padding here to align price_level* to 8 bytes
    PriceLevel *price_level{nullptr};
    uint32_t prev_idx{NULL_IDX};
    uint32_t next_idx{NULL_IDX};

    void reset() {
        order_id = 0;
        quantity = 0;
        price_level = nullptr;
        prev_idx = NULL_IDX;
        next_idx = NULL_IDX;
    }
};
// OrderNode is 32 bytes: fits 2 per 64-byte cache line (vs 1 previously).

// PriceLevel stores the doubly-linked list as slab indices rather than
// pointers, shrinking from 32 bytes to 24 bytes per level entry.
struct PriceLevel {
    uint64_t price{0};
    uint32_t total_quantity{0};
    uint32_t head_idx{OrderNode::NULL_IDX};
    uint32_t tail_idx{OrderNode::NULL_IDX};
};

} // namespace exchange::engine