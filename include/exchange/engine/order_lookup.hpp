#pragma once

#include "exchange/engine/order_book_types.hpp"
#include <cstddef>
#include <cstdint>
#include <vector>

namespace exchange::engine {
class OrderLookup {
  private:
    std::vector<uint32_t> table_;
    std::size_t max_orders_;

  public:
    explicit OrderLookup(std::size_t max_orders) : max_orders_(max_orders) {
        table_.resize(max_orders_ + 1, OrderNode::NULL_IDX);
    }

    OrderLookup(const OrderLookup &) = delete;
    OrderLookup &operator=(const OrderLookup &) = delete;

    void add_order(uint64_t order_id, uint32_t node_idx) {
        if (order_id > 0 && order_id <= max_orders_) {
            table_[order_id] = node_idx;
        }
    }

    [[nodiscard]] auto get_order(uint64_t order_id) const -> uint32_t {
        if (order_id > 0 && order_id <= max_orders_) {
            return table_[order_id];
        }
        return OrderNode::NULL_IDX;
    }

    void remove_order(uint64_t order_id) {
        if (order_id > 0 && order_id <= max_orders_) {
            table_[order_id] = OrderNode::NULL_IDX;
        }
    }
};
} // namespace exchange::engine
