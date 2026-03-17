#include "exchange/engine/order_book_types.hpp"
#include "exchange/engine/order_lookup.hpp"
#include <gtest/gtest.h>

using namespace exchange::engine;

TEST(OrderLookupTest, Initialization) {
    constexpr std::size_t MAX_ORDERS = 100;
    OrderLookup lookup(MAX_ORDERS);

    EXPECT_EQ(lookup.get_order(1), nullptr);
    EXPECT_EQ(lookup.get_order(100), nullptr);
}

TEST(OrderLookupTest, AddAndRetrieveOrder) {
    constexpr std::size_t MAX_ORDERS = 100;
    constexpr uint64_t ORDER_ID = 42;

    OrderLookup lookup(MAX_ORDERS);
    OrderNode dummy_order;
    dummy_order.order_id = ORDER_ID;

    lookup.add_order(ORDER_ID, &dummy_order);

    OrderNode *retrieved = lookup.get_order(ORDER_ID);
    ASSERT_NE(retrieved, nullptr);
    EXPECT_EQ(retrieved->order_id, 42);
    EXPECT_EQ(retrieved, &dummy_order);
}

TEST(OrderLookupTest, RemoveOrder) {
    constexpr std::size_t MAX_ORDERS = 100;
    constexpr uint64_t ORDER_ID = 10;

    OrderLookup lookup(MAX_ORDERS);
    OrderNode dummy_order;

    lookup.add_order(ORDER_ID, &dummy_order);
    EXPECT_NE(lookup.get_order(10), nullptr);

    lookup.remove_order(ORDER_ID);
    EXPECT_EQ(lookup.get_order(10), nullptr);
}

TEST(OrderLookupTest, OutOfBoundsSafety) {
    constexpr std::size_t MAX_ORDERS = 50;
    constexpr uint64_t ORDER_ID = 51;

    OrderLookup lookup(MAX_ORDERS);
    OrderNode dummy_order;

    lookup.add_order(ORDER_ID, &dummy_order);
    EXPECT_EQ(lookup.get_order(51), nullptr);

    EXPECT_EQ(lookup.get_order(0), nullptr);
}