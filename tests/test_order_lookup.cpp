#include "exchange/engine/order_book_types.hpp"
#include "exchange/engine/order_lookup.hpp"
#include <gtest/gtest.h>

using namespace exchange::engine;

TEST(OrderLookupTest, Initialization) {
    constexpr std::size_t MAX_ORDERS = 100;
    OrderLookup lookup(MAX_ORDERS);

    EXPECT_EQ(lookup.get_order(1), OrderNode::NULL_IDX);
    EXPECT_EQ(lookup.get_order(100), OrderNode::NULL_IDX);
}

TEST(OrderLookupTest, AddAndRetrieveOrder) {
    constexpr std::size_t MAX_ORDERS = 100;
    constexpr uint64_t ORDER_ID = 42;
    constexpr uint32_t NODE_IDX = 7;

    OrderLookup lookup(MAX_ORDERS);
    lookup.add_order(ORDER_ID, NODE_IDX);

    uint32_t retrieved = lookup.get_order(ORDER_ID);
    EXPECT_EQ(retrieved, NODE_IDX);
}

TEST(OrderLookupTest, RemoveOrder) {
    constexpr std::size_t MAX_ORDERS = 100;
    constexpr uint64_t ORDER_ID = 10;
    constexpr uint32_t NODE_IDX = 3;

    OrderLookup lookup(MAX_ORDERS);

    lookup.add_order(ORDER_ID, NODE_IDX);
    EXPECT_NE(lookup.get_order(10), OrderNode::NULL_IDX);

    lookup.remove_order(ORDER_ID);
    EXPECT_EQ(lookup.get_order(10), OrderNode::NULL_IDX);
}

TEST(OrderLookupTest, OutOfBoundsSafety) {
    constexpr std::size_t MAX_ORDERS = 50;
    constexpr uint64_t ORDER_ID = 51;
    constexpr uint32_t NODE_IDX = 5;

    OrderLookup lookup(MAX_ORDERS);

    lookup.add_order(ORDER_ID, NODE_IDX);
    EXPECT_EQ(lookup.get_order(51), OrderNode::NULL_IDX);

    EXPECT_EQ(lookup.get_order(0), OrderNode::NULL_IDX);
}
