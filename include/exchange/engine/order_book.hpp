#pragma once

#include "exchange/engine/memory_pool.hpp"
#include "exchange/engine/order_book_types.hpp"
#include "exchange/engine/order_lookup.hpp"
#include "exchange/engine/price_level_index.hpp"
#include "exchange/engine/types.hpp"
#include <cstddef>
#include <cstdint>
#include <sys/types.h>

namespace exchange::engine {
class OrderBook {
  private:
    MemoryPool<OrderNode> node_pool_;
    OrderLookup order_lookup_;

    PriceLevelIndex bids_;
    PriceLevelIndex asks_;

    uint64_t min_price_;
    uint64_t max_price_;
    uint64_t tick_size_;

    uint64_t highest_bid_;
    uint64_t lowest_ask_;

    // Returns the total quantity matched during this fill sweep.
    auto match_buy(Order &taker) -> uint32_t {
        uint32_t total_matched = 0;
        for (uint64_t price = lowest_ask_; price <= taker.price; price += tick_size_) {
            PriceLevel *level = asks_.get_level(price);
            if (level == nullptr || level->head == nullptr) {
                if (price == lowest_ask_) {
                    lowest_ask_ = price + tick_size_;
                }
                continue;
            }
            OrderNode *maker = level->head;

            while (maker != nullptr && taker.quantity > 0) {
                uint32_t trade_qty = std::min(taker.quantity, maker->quantity);

                total_matched += trade_qty;
                taker.quantity -= trade_qty;
                maker->quantity -= trade_qty;
                level->total_quantity -= trade_qty;

                if (maker->quantity == 0) {
                    OrderNode *next_maker = maker->next;

                    level->remove_order(maker);
                    order_lookup_.remove_order(maker->order_id);
                    maker->reset();
                    node_pool_.deallocate(maker);

                    maker = next_maker;
                } else {
                    break;
                }
            }

            if (level->total_quantity == 0) {
                lowest_ask_ = price + tick_size_;
            }

            if (taker.quantity == 0) {
                break;
            }
        }
        return total_matched;
    }

    // Returns the total quantity matched during this fill sweep.
    auto match_sell(Order &taker) -> uint32_t {
        uint32_t total_matched = 0;
        for (uint64_t price = highest_bid_; price >= taker.price; price -= tick_size_) {
            PriceLevel *level = bids_.get_level(price);
            if (level == nullptr || level->head == nullptr) {
                if (price == highest_bid_) {
                    highest_bid_ = (price > tick_size_) ? price - tick_size_ : min_price_;
                }
                if (price <= min_price_) {
                    break;
                }
                continue;
            }
            OrderNode *maker = level->head;
            while (maker != nullptr && taker.quantity > 0) {
                uint32_t trade_qty = std::min(taker.quantity, maker->quantity);

                total_matched += trade_qty;
                taker.quantity -= trade_qty;
                maker->quantity -= trade_qty;
                level->total_quantity -= trade_qty;

                if (maker->quantity == 0) {
                    OrderNode *next_maker = maker->next;

                    level->remove_order(maker);
                    order_lookup_.remove_order(maker->order_id);
                    maker->reset();
                    node_pool_.deallocate(maker);

                    maker = next_maker;
                } else {
                    break;
                }
            }

            if (level->total_quantity == 0) {
                highest_bid_ = (price > tick_size_) ? price - tick_size_ : min_price_;
            }

            if (taker.quantity == 0 || price == min_price_) {
                break;
            }
        }
        return total_matched;
    }

    // Returns total matched quantity (0 for a fully resting order).
    auto on_new_order(Order &order) -> uint32_t {
        uint32_t matched = 0;
        if (order.side == Side::BUY) {
            matched = match_buy(order);
        } else {
            matched = match_sell(order);
        }

        if (order.quantity == 0) {
            return matched;
        }

        OrderNode *node = node_pool_.allocate();
        if (node == nullptr) {
            return matched; // In a real system, we would trigger a critical halt here
        }
        node->order_id = order.order_id;
        node->price = order.price;
        node->quantity = order.quantity;
        node->side = order.side;

        PriceLevel *level = nullptr;
        if (order.side == Side::BUY) {
            level = bids_.get_level(order.price);
            if (level != nullptr) {
                level->append_order(node);
                highest_bid_ = std::max(highest_bid_, order.price);
            }

        } else {
            level = asks_.get_level(order.price);
            if (level != nullptr) {
                level->append_order(node);
                lowest_ask_ = std::min(lowest_ask_, order.price);
            }
        }

        if (level == nullptr) {
            node_pool_.deallocate(node);
            return matched;
        }

        order_lookup_.add_order(order.order_id, node);
        return matched;
    }

    void on_cancel_order(const Order &order) {
        OrderNode *node = order_lookup_.get_order(order.order_id);
        if (node == nullptr) {
            return;
        }

        PriceLevel *level = node->price_level;
        if (level != nullptr) {
            level->remove_order(node);
        }

        order_lookup_.remove_order(order.order_id);
        node->reset();
        node_pool_.deallocate(node);
    }

  public:
    OrderBook(std::size_t max_orders, uint64_t min_price, uint64_t max_price, uint64_t tick_size)
        : node_pool_(max_orders), order_lookup_(max_orders), bids_(min_price, max_price, tick_size),
          asks_(min_price, max_price, tick_size), min_price_(min_price), max_price_(max_price),
          tick_size_(tick_size), highest_bid_(min_price), lowest_ask_(max_price) {}

    OrderBook(const OrderBook &) = delete;
    OrderBook &operator=(const OrderBook &) = delete;

    // Returns total quantity matched for this order (0 for fully resting orders).
    auto process_order(const Order &incoming_order) -> uint32_t {
        Order order = incoming_order;
        switch (order.type) {
        case OrderType::NEW:
            return on_new_order(order);
        case OrderType::CANCEL:
            on_cancel_order(order);
            return 0;
        case OrderType::MODIFY:
            on_cancel_order(order);
            return on_new_order(order);
        }
        return 0;
    }

    // Public getters for testing and asserting state
    [[nodiscard]] auto get_bids() -> PriceLevelIndex & { return bids_; }
    [[nodiscard]] auto get_asks() -> PriceLevelIndex & { return asks_; }
    [[nodiscard]] auto get_available_capacity() const -> std::size_t {
        return node_pool_.available();
    }
};
} // namespace exchange::engine
