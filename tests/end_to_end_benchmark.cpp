#include "exchange/engine/matching_engine.hpp"
#include "exchange/engine/order_book.hpp"
#include "exchange/queue/mpsc_queue.hpp"
#include <chrono>
#include <gtest/gtest.h>
#include <iostream>
#include <random>
#include <thread>
#include <vector>

using namespace exchange::engine;
using namespace exchange::queue;

TEST(SystemBenchmark, ThroughputStressTest) {
    constexpr std::size_t NUM_PRODUCERS = 4;
    constexpr std::size_t MESSAGES_PER_PRODUCER = 1'000'000;
    constexpr std::size_t TOTAL_MESSAGES = NUM_PRODUCERS * MESSAGES_PER_PRODUCER;

    constexpr std::size_t QUEUE_CAPACITY = 8388608;
    constexpr std::size_t BOOK_CAPACITY = TOTAL_MESSAGES + 10;

    constexpr std::size_t MIN_PRICE = 100;
    constexpr std::size_t MAX_PRICE = 200;
    constexpr std::size_t TICK_SIZE = 1;

    constexpr std::size_t SEED = 1337;
    constexpr std::size_t NANOS_PER_SECOND = 1'000'000'000;

    constexpr std::size_t PRICE_DISTRIBUTION_MIN = 120;
    constexpr std::size_t PRICE_DISTRIBUTION_Max = 180;

    constexpr std::size_t QUANTITY_DISTRIBUTION_MIN = 10;
    constexpr std::size_t QUANTITY_DISTRIBUTION_MAX = 100;

    MpscQueue<Order> queue(QUEUE_CAPACITY);
    OrderBook book(BOOK_CAPACITY, MIN_PRICE, MAX_PRICE, TICK_SIZE);
    MatchingEngine engine(queue, book);

    // ==========================================
    // 1. PRE-GENERATE DATA (Off the clock)
    // ==========================================
    std::cout << "[*] Pre-generating " << TOTAL_MESSAGES << " random orders..." << std::endl;
    std::vector<std::vector<Order>> producer_loads(NUM_PRODUCERS);

    for (std::size_t i = 0; i < NUM_PRODUCERS; ++i) {
        producer_loads[i].reserve(MESSAGES_PER_PRODUCER);

        std::mt19937 gen(SEED + i);
        std::uniform_int_distribution<uint64_t> price_dist(PRICE_DISTRIBUTION_MIN,
                                                           PRICE_DISTRIBUTION_Max);
        std::uniform_int_distribution<uint32_t> qty_dist(QUANTITY_DISTRIBUTION_MIN,
                                                         QUANTITY_DISTRIBUTION_MAX);
        std::uniform_int_distribution<int> side_dist(0, 1);

        for (std::size_t j = 0; j < MESSAGES_PER_PRODUCER; ++j) {
            uint64_t global_id = (i * MESSAGES_PER_PRODUCER) + j + 1;

            producer_loads[i].push_back({.order_id = global_id,
                                         .price = price_dist(gen),
                                         .quantity = qty_dist(gen),
                                         .side = side_dist(gen) == 0 ? Side::BUY : Side::SELL,
                                         .type = OrderType::NEW});
        }
    }

    // ==========================================
    // 2. THE BENCHMARK (On the clock)
    // ==========================================
    std::atomic<bool> start_flag{false};
    std::vector<std::thread> producers;

    for (std::size_t i = 0; i < NUM_PRODUCERS; ++i) {
        producers.emplace_back([&queue, &producer_loads, i, &start_flag]() {
            while (!start_flag.load(std::memory_order_acquire)) {
                _mm_pause();
            }

            for (const auto &order : producer_loads[i]) {
                while (!queue.push(order)) {
                }
            }
        });
    }

    std::cout << "[*] Starting the engine..." << std::endl;
    engine.start(1);

    auto start_time = std::chrono::high_resolution_clock::now();
    start_flag.store(true, std::memory_order_release);
    for (auto &producer : producers) {
        producer.join();
    }

    // Push the POISON PILL (order_id = 0) to tell the engine to stop
    while (!queue.push({0, 0, 0, Side::BUY, OrderType::NEW})) {
    }

    engine.stop();
    auto end_time = std::chrono::high_resolution_clock::now();

    // ==========================================
    // 3. RESULTS
    // ==========================================
    std::chrono::duration<double> elapsed = end_time - start_time;
    double messages_per_second = TOTAL_MESSAGES / elapsed.count();
    double nanos_per_message = (elapsed.count() * NANOS_PER_SECOND) / TOTAL_MESSAGES;

    std::cout << "\n=========================================\n";
    std::cout << " EXCHANGE CORE BENCHMARK RESULTS\n";
    std::cout << "=========================================\n";
    std::cout << " Orders Processed: " << TOTAL_MESSAGES << "\n";
    std::cout << " Time Elapsed:     " << elapsed.count() << " seconds\n";
    std::cout << " Throughput:       " << static_cast<uint64_t>(messages_per_second)
              << " orders/sec\n";
    std::cout << " Average Latency:  " << nanos_per_message << " ns/order\n";
    std::cout << " Memory Remaining: " << book.get_available_capacity() << " nodes\n";
    std::cout << "=========================================\n\n";

    EXPECT_LT(book.get_available_capacity(), BOOK_CAPACITY);
}