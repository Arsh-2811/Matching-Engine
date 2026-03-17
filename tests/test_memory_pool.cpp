#include "exchange/engine//memory_pool.hpp"
#include "exchange/engine/order_book_types.hpp"
#include <cstdint>
#include <gtest/gtest.h>

using namespace exchange::engine;

TEST(MemoryPoolTest, InitializationAndCapacity) {
    constexpr std::size_t CAPACITY = 100;
    MemoryPool<OrderNode> pool(CAPACITY);
    EXPECT_EQ(pool.available(), 100);
}

TEST(MemoryPoolTest, AllocateDecreasesAvailability) {
    constexpr std::size_t CAPACITY = 10;
    MemoryPool<OrderNode> pool(CAPACITY);

    OrderNode *order1 = pool.allocate();
    ASSERT_NE(order1, nullptr);

    EXPECT_EQ(pool.available(), 9);
}

TEST(MemoryPoolTest, DeallocateRecyclesMemoryAndRequiresReset) {
    constexpr std::size_t CAPACITY = 5;
    constexpr uint64_t TEST_ORDER_ID = 999;
    constexpr uint64_t TEST_PRICE = 10050;

    MemoryPool<OrderNode> pool(CAPACITY);

    OrderNode *order1 = pool.allocate();
    order1->order_id = TEST_ORDER_ID;
    order1->price = TEST_PRICE;

    pool.deallocate(order1);
    EXPECT_EQ(pool.available(), 5);

    OrderNode *order2 = pool.allocate();
    EXPECT_EQ(order1, order2);

    EXPECT_EQ(order2->order_id, 999);

    order2->reset();
    EXPECT_EQ(order2->order_id, 0);
}

TEST(MemoryPoolTest, ExhaustionReturnsNullptr) {
    MemoryPool<OrderNode> pool(2);

    OrderNode *order1 = pool.allocate();
    OrderNode *order2 = pool.allocate();

    EXPECT_EQ(pool.available(), 0);

    OrderNode *order3 = pool.allocate();
    EXPECT_EQ(order3, nullptr);
}

TEST(MemoryPoolTest, ContiguousMemoryProof) {
    MemoryPool<OrderNode> pool(3);

    OrderNode *order1 = pool.allocate();
    OrderNode *order2 = pool.allocate();

    auto ptr1_address = reinterpret_cast<std::uintptr_t>(order1);
    auto ptr2_address = reinterpret_cast<std::uintptr_t>(order2);

    EXPECT_EQ(ptr2_address - ptr1_address, sizeof(OrderNode));
}