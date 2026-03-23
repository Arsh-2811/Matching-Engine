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
    constexpr uint32_t TEST_QUANTITY = 42;

    MemoryPool<OrderNode> pool(CAPACITY);

    OrderNode *order1 = pool.allocate();
    order1->order_id = TEST_ORDER_ID;
    order1->quantity = TEST_QUANTITY;

    pool.deallocate(order1);
    EXPECT_EQ(pool.available(), 5);

    OrderNode *order2 = pool.allocate();
    EXPECT_EQ(order1, order2);

    // State must be preserved until reset() is explicitly called.
    EXPECT_EQ(order2->order_id, TEST_ORDER_ID);
    EXPECT_EQ(order2->quantity, TEST_QUANTITY);

    order2->reset();
    EXPECT_EQ(order2->order_id, 0);
    EXPECT_EQ(order2->quantity, 0);
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

TEST(MemoryPoolTest, GetByIndexRoundTrips) {
    MemoryPool<OrderNode> pool(4);

    OrderNode *node0 = pool.allocate();
    OrderNode *node1 = pool.allocate();

    uint32_t idx0 = pool.index_of(node0);
    uint32_t idx1 = pool.index_of(node1);

    EXPECT_NE(idx0, idx1);
    EXPECT_EQ(&pool.get(idx0), node0);
    EXPECT_EQ(&pool.get(idx1), node1);
}

TEST(MemoryPoolTest, NodeSizeIs48Bytes) {
    // OrderNode is 48 bytes: 32 bytes of hot matching fields (order_id, quantity,
    // side, price_level*, prev_idx, next_idx) followed by 16 bytes of cold routing
    // fields (cl_ord_id, client_fd, _pad) embedded for maker-fill ACKs.
    EXPECT_EQ(sizeof(OrderNode), 48U);
}