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

    constexpr std::size_t QUEUE_CAPACITY = 8388608; // 2^23 (Must be power of 2)
    constexpr std::size_t BOOK_CAPACITY = TOTAL_MESSAGES + 10;

    MpscQueue<Order> queue(QUEUE_CAPACITY);
    OrderBook book(BOOK_CAPACITY, 100, 200, 1);
    MatchingEngine engine(queue, book);

    // ==========================================
    // 1. PRE-GENERATE DATA (Off the clock)
    // ==========================================
    std::cout << "[*] Pre-generating " << TOTAL_MESSAGES << " random orders..." << std::endl;
    std::vector<std::vector<Order>> producer_loads(NUM_PRODUCERS);

    for (std::size_t i = 0; i < NUM_PRODUCERS; ++i) {
        producer_loads[i].reserve(MESSAGES_PER_PRODUCER);

        // Use a deterministic seed so our benchmarks are comparable run-to-run
        std::mt19937 gen(1337 + i);
        std::uniform_int_distribution<uint64_t> price_dist(120, 180); // High collision area
        std::uniform_int_distribution<uint32_t> qty_dist(10, 100);
        std::uniform_int_distribution<int> side_dist(0, 1);

        for (std::size_t j = 0; j < MESSAGES_PER_PRODUCER; ++j) {
            // Assign globally unique, sequential IDs to avoid OrderLookup collisions
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
            // Spin-wait until the starting gun is fired to ensure all threads start exactly
            // together
            while (!start_flag.load(std::memory_order_acquire)) {
                _mm_pause(); // Or yield
            }

            for (const auto &order : producer_loads[i]) {
                while (!queue.push(order)) {
                    // Spin if queue is full
                }
            }
        });
    }

    std::cout << "[*] Starting the engine..." << std::endl;
    engine.start();

    // FIRE THE STARTING GUN
    auto start_time = std::chrono::high_resolution_clock::now();
    start_flag.store(true, std::memory_order_release);

    // Wait for all producers to finish dumping their loads
    for (auto &producer : producers) {
        producer.join();
    }

    // Push the POISON PILL (order_id = 0) to tell the engine to stop
    while (!queue.push({0, 0, 0, Side::BUY, OrderType::NEW})) {
    }

    // Wait for the engine to hit the poison pill and naturally shut down
    engine.stop();
    auto end_time = std::chrono::high_resolution_clock::now();

    // ==========================================
    // 3. RESULTS
    // ==========================================
    std::chrono::duration<double> elapsed = end_time - start_time;
    double messages_per_second = TOTAL_MESSAGES / elapsed.count();
    double nanos_per_message = (elapsed.count() * 1'000'000'000) / TOTAL_MESSAGES;

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

    // Basic sanity check to ensure the book actually consumed memory
    EXPECT_LT(book.get_available_capacity(), BOOK_CAPACITY);
}