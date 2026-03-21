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

    // Link node at the tail of level's list. node must already be allocated
    // from node_pool_ and have order_id / quantity / side populated.
    void append_to_level(PriceLevel *level, OrderNode *node) {
        uint32_t node_idx = node_pool_.index_of(node);
        node->price_level = level;
        node->next_idx = OrderNode::NULL_IDX;
        node->prev_idx = level->tail_idx;

        if (level->tail_idx != OrderNode::NULL_IDX) {
            node_pool_.get(level->tail_idx).next_idx = node_idx;
        } else {
            level->head_idx = node_idx; // list was empty
        }

        level->tail_idx = node_idx;
        level->total_quantity += node->quantity;
    }

    // Unlink node from level's list without touching total_quantity.
    // Caller is responsible for adjusting total_quantity if needed.
    void unlink_from_level(PriceLevel *level, OrderNode *node) {
        if (node->prev_idx != OrderNode::NULL_IDX) {
            node_pool_.get(node->prev_idx).next_idx = node->next_idx;
        } else {
            level->head_idx = node->next_idx; // node was head
        }

        if (node->next_idx != OrderNode::NULL_IDX) {
            node_pool_.get(node->next_idx).prev_idx = node->prev_idx;
        } else {
            level->tail_idx = node->prev_idx; // node was tail
        }

        node->prev_idx = OrderNode::NULL_IDX;
        node->next_idx = OrderNode::NULL_IDX;
        node->price_level = nullptr;
    }

    // Returns the total quantity matched during this fill sweep.
    auto match_buy(Order &taker) -> uint32_t {
        uint32_t total_matched = 0;
        for (uint64_t price = lowest_ask_; price <= taker.price; price += tick_size_) {
            PriceLevel *level = asks_.get_level(price);
            if (level == nullptr || level->head_idx == OrderNode::NULL_IDX) {
                if (price == lowest_ask_) {
                    lowest_ask_ = price + tick_size_;
                }
                continue;
            }

            uint32_t maker_idx = level->head_idx;
            while (maker_idx != OrderNode::NULL_IDX && taker.quantity > 0) {
                OrderNode &maker = node_pool_.get(maker_idx);
                // Prefetch the next node into L1 while we compute trade_qty for
                // the current node. Hides the cache-miss latency on deep books.
                if (maker.next_idx != OrderNode::NULL_IDX) {
                    __builtin_prefetch(&node_pool_.get(maker.next_idx), 0, 3);
                }
                uint32_t trade_qty = std::min(taker.quantity, maker.quantity);

                total_matched += trade_qty;
                taker.quantity -= trade_qty;
                maker.quantity -= trade_qty;
                level->total_quantity -= trade_qty;

                if (maker.quantity == 0) {
                    uint32_t next_idx = maker.next_idx;
                    // Matching always consumes from the head — fast-path unlink.
                    level->head_idx = next_idx;
                    if (next_idx != OrderNode::NULL_IDX) {
                        node_pool_.get(next_idx).prev_idx = OrderNode::NULL_IDX;
                    } else {
                        level->tail_idx = OrderNode::NULL_IDX;
                    }
                    maker.price_level = nullptr;
                    order_lookup_.remove_order(maker.order_id);
                    maker.reset();
                    node_pool_.deallocate(&maker);
                    maker_idx = next_idx;
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
            if (level == nullptr || level->head_idx == OrderNode::NULL_IDX) {
                if (price == highest_bid_) {
                    highest_bid_ = (price > tick_size_) ? price - tick_size_ : min_price_;
                }
                if (price <= min_price_) {
                    break;
                }
                continue;
            }

            uint32_t maker_idx = level->head_idx;
            while (maker_idx != OrderNode::NULL_IDX && taker.quantity > 0) {
                OrderNode &maker = node_pool_.get(maker_idx);
                // Prefetch the next node into L1 while we compute trade_qty for
                // the current node. Hides the cache-miss latency on deep books.
                if (maker.next_idx != OrderNode::NULL_IDX) {
                    __builtin_prefetch(&node_pool_.get(maker.next_idx), 0, 3);
                }
                uint32_t trade_qty = std::min(taker.quantity, maker.quantity);

                total_matched += trade_qty;
                taker.quantity -= trade_qty;
                maker.quantity -= trade_qty;
                level->total_quantity -= trade_qty;

                if (maker.quantity == 0) {
                    uint32_t next_idx = maker.next_idx;
                    // Matching always consumes from the head — fast-path unlink.
                    level->head_idx = next_idx;
                    if (next_idx != OrderNode::NULL_IDX) {
                        node_pool_.get(next_idx).prev_idx = OrderNode::NULL_IDX;
                    } else {
                        level->tail_idx = OrderNode::NULL_IDX;
                    }
                    maker.price_level = nullptr;
                    order_lookup_.remove_order(maker.order_id);
                    maker.reset();
                    node_pool_.deallocate(&maker);
                    maker_idx = next_idx;
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

        // FOK: check available liquidity BEFORE touching the book.
        if (order.type == OrderType::FOK) {
            uint32_t avail = 0;
            if (order.side == Side::BUY) {
                for (uint64_t p = lowest_ask_; p <= order.price && avail < order.quantity;
                     p += tick_size_) {
                    PriceLevel *lvl = asks_.get_level(p);
                    if (lvl != nullptr) {
                        avail += lvl->total_quantity;
                    }
                }
            } else {
                for (uint64_t p = highest_bid_; p >= order.price && avail < order.quantity;
                     p -= tick_size_) {
                    PriceLevel *lvl = bids_.get_level(p);
                    if (lvl != nullptr) {
                        avail += lvl->total_quantity;
                    }
                    if (p <= min_price_) {
                        break;
                    }
                }
            }
            if (avail < order.quantity) {
                return 0; // insufficient liquidity, cancel FOK
            }
        }

        if (order.side == Side::BUY) {
            matched = match_buy(order);
        } else {
            matched = match_sell(order);
        }

        // IOC/FOK: discard remainder (never rest)
        if (order.type == OrderType::IOC || order.type == OrderType::FOK) {
            return matched;
        }

        if (order.quantity == 0) {
            return matched;
        }

        OrderNode *node = node_pool_.allocate();
        if (node == nullptr) {
            return matched; // In a real system, we would trigger a critical halt here
        }
        node->order_id = order.order_id;
        node->quantity = order.quantity;
        node->side = order.side;

        PriceLevel *level = nullptr;
        if (order.side == Side::BUY) {
            level = bids_.get_level(order.price);
            if (level != nullptr) {
                append_to_level(level, node);
                highest_bid_ = std::max(highest_bid_, order.price);
            }
        } else {
            level = asks_.get_level(order.price);
            if (level != nullptr) {
                append_to_level(level, node);
                lowest_ask_ = std::min(lowest_ask_, order.price);
            }
        }

        if (level == nullptr) {
            node_pool_.deallocate(node);
            return matched;
        }

        order_lookup_.add_order(order.order_id, node_pool_.index_of(node));
        return matched;
    }

    void on_cancel_order(const Order &order) {
        uint32_t idx = order_lookup_.get_order(order.order_id);
        if (idx == OrderNode::NULL_IDX) {
            return;
        }
        OrderNode &node = node_pool_.get(idx);
        PriceLevel *level = node.price_level;
        if (level != nullptr) {
            level->total_quantity -= node.quantity;
            unlink_from_level(level, &node);
        }
        order_lookup_.remove_order(order.order_id);
        node.reset();
        node_pool_.deallocate(&node);
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
        case OrderType::IOC:
            return on_new_order(order);
        case OrderType::FOK:
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

    // Returns a pointer to the head OrderNode for a given PriceLevel, or
    // nullptr if the level is empty. Used in tests and for book inspection.
    [[nodiscard]] auto get_head_node(const PriceLevel *level) const -> const OrderNode * {
        if (level == nullptr || level->head_idx == OrderNode::NULL_IDX) {
            return nullptr;
        }
        return &node_pool_.get(level->head_idx);
    }
};
} // namespace exchange::engine
