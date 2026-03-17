#pragma once

#include "exchange/engine/order_book_types.hpp"
#include <cstddef>
#include <cstdint>
#include <vector>

namespace exchange::engine {
class OrderLookup {
  private:
    std::vector<OrderNode *> table_;
    std::size_t max_orders_;

  public:
    explicit OrderLookup(std::size_t max_orders) : max_orders_(max_orders) {
        table_.resize(max_orders_ + 1, nullptr);
    }

    OrderLookup(const OrderLookup &) = delete;
    OrderLookup &operator=(const OrderLookup &) = delete;

    void add_order(uint64_t order_id, OrderNode *order) {
        if (order_id > 0 && order_id <= max_orders_) {
            table_[order_id] = order;
        }
    }

    [[nodiscard]] auto get_order(uint64_t order_id) const -> OrderNode * {
        if (order_id > 0 && order_id <= max_orders_) {
            return table_[order_id];
        }
        return nullptr;
    }

    void remove_order(uint64_t order_id) {
        if (order_id > 0 && order_id <= max_orders_) {
            table_[order_id] = nullptr;
        }
    }
};
} // namespace exchange::engine