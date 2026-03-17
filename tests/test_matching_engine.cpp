#include "exchange/engine/matching_engine.hpp"
#include "exchange/engine/order_book.hpp"
#include "exchange/engine/types.hpp"
#include "exchange/queue/mpsc_queue.hpp"
#include <chrono>
#include <cstddef>
#include <gtest/gtest.h>
#include <thread>

using namespace exchange::engine;
using namespace exchange::queue;

TEST(MatchingEngineTest, LifeCycleStartAndStop) {
    constexpr std::size_t CAPACITY = 1024;
    constexpr std::size_t SLEEP_DURATION_MS = 10;

    MpscQueue<Order> order_queue(CAPACITY);
    OrderBook book{1000, 100, 200, 1};

    MatchingEngine engine(order_queue, book);

    EXPECT_NO_THROW(engine.start());

    std::this_thread::sleep_for(std::chrono::microseconds(SLEEP_DURATION_MS));

    EXPECT_NO_THROW(engine.stop());
}

TEST(MatchingEngineTest, ConsumeOrdersFromQueue) {
    constexpr std::size_t CAPACITY = 1024;
    constexpr std::size_t PRICE = 10050;
    constexpr std::size_t QUANTITY = 100;
    constexpr std::size_t SLEEP_DURATION_MS = 50;

    MpscQueue<Order> order_queue(CAPACITY);
    OrderBook book{1000, 100, 200, 1};

    Order new_order{.order_id = 1,
                    .price = PRICE,
                    .quantity = QUANTITY,
                    .side = Side::BUY,
                    .type = OrderType::NEW};

    ASSERT_TRUE(order_queue.push(new_order));

    MatchingEngine engine(order_queue, book);
    EXPECT_NO_THROW(engine.start());

    std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_DURATION_MS));

    EXPECT_NO_THROW(engine.stop());

    Order popped_order{};
    EXPECT_FALSE(order_queue.pop(popped_order));
}