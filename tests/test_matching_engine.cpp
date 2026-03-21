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

    constexpr std::size_t MAX_ORDERS = 1000;
    constexpr std::size_t MIN_PRICE = 100;
    constexpr std::size_t MAX_PRICE = 200;
    constexpr std::size_t TICK_SIZE = 1;

    MpscQueue<Order> order_queue(CAPACITY);
    OrderBook book{MAX_ORDERS, MIN_PRICE, MAX_PRICE, TICK_SIZE};

    MatchingEngine engine(order_queue, book);

    EXPECT_NO_THROW(engine.start(1));

    std::this_thread::sleep_for(std::chrono::microseconds(SLEEP_DURATION_MS));

    EXPECT_NO_THROW(engine.stop());
}

TEST(MatchingEngineTest, ConsumeOrdersFromQueue) {
    constexpr std::size_t CAPACITY = 1024;
    constexpr std::size_t PRICE = 10050;
    constexpr std::size_t QUANTITY = 100;
    constexpr std::size_t SLEEP_DURATION_MS = 50;

    constexpr std::size_t MAX_ORDERS = 1000;
    constexpr std::size_t MIN_PRICE = 100;
    constexpr std::size_t MAX_PRICE = 200;
    constexpr std::size_t TICK_SIZE = 1;

    MpscQueue<Order> order_queue(CAPACITY);
    OrderBook book{MAX_ORDERS, MIN_PRICE, MAX_PRICE, TICK_SIZE};

    Order new_order{.order_id = 1,
                    .price = PRICE,
                    .quantity = QUANTITY,
                    .side = Side::BUY,
                    .type = OrderType::NEW};

    ASSERT_TRUE(order_queue.push(new_order));

    MatchingEngine engine(order_queue, book);
    EXPECT_NO_THROW(engine.start(1));

    std::this_thread::sleep_for(std::chrono::milliseconds(SLEEP_DURATION_MS));

    EXPECT_NO_THROW(engine.stop());

    Order popped_order{};
    EXPECT_FALSE(order_queue.pop(popped_order));
}

TEST(MatchingEngineTest, PoisonPillExitJoinsCleanly) {
    constexpr std::size_t QUEUE_CAP = 1024;
    constexpr std::size_t BOOK_MAX = 100;
    constexpr std::size_t BOOK_MIN_P = 100;
    constexpr std::size_t BOOK_MAX_P = 200;
    constexpr std::size_t BOOK_TICK = 1;
    constexpr int WAIT_MS = 100;

    MpscQueue<Order> queue(QUEUE_CAP);
    OrderBook book{BOOK_MAX, BOOK_MIN_P, BOOK_MAX_P, BOOK_TICK};
    {
        MatchingEngine engine(queue, book);
        engine.start(0);
        Order poison{};
        poison.order_id = 0;
        while (!queue.push(poison)) {
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(WAIT_MS));
        // engine goes out of scope here — destructor calls stop()
        // if bug exists, std::terminate() is called
    }
    SUCCEED(); // reached here without crash
}