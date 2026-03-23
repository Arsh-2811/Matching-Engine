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
//
// Layout: first 32 bytes = hot matching data (always accessed),
//         last 16 bytes = cold routing data (only accessed on fill/cancel ACK).
struct OrderNode {
    static constexpr uint32_t NULL_IDX = std::numeric_limits<uint32_t>::max();

    // Hot matching data (first 32 bytes)
    uint64_t order_id{0};
    uint32_t quantity{0};
    Side side{Side::BUY};
    // 3 bytes implicit padding here to align price_level* to 8 bytes
    PriceLevel *price_level{nullptr};
    uint32_t prev_idx{NULL_IDX};
    uint32_t next_idx{NULL_IDX};
    // Cold routing data (last 16 bytes) — used only when emitting maker fill events
    uint64_t cl_ord_id{0};
    int client_fd{-1};
    uint32_t _pad{0};

    void reset() {
        order_id = 0;
        quantity = 0;
        price_level = nullptr;
        prev_idx = NULL_IDX;
        next_idx = NULL_IDX;
        cl_ord_id = 0;
        client_fd = -1;
    }
};
// OrderNode is 48 bytes: hot matching fields in first 32, routing in last 16.

// PriceLevel stores the doubly-linked list as slab indices rather than
// pointers, shrinking from 32 bytes to 24 bytes per level entry.
struct PriceLevel {
    uint64_t price{0};
    uint32_t total_quantity{0};
    uint32_t head_idx{OrderNode::NULL_IDX};
    uint32_t tail_idx{OrderNode::NULL_IDX};
};

} // namespace exchange::engine
