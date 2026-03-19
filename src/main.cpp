#include "exchange/engine/matching_engine.hpp"
#include "exchange/engine/order_book.hpp"
#include "exchange/network/gateway.hpp"
#include "exchange/queue/mpsc_queue.hpp"
#include <cstdint>
#include <iostream>
#include <thread>
#include <vector>

using namespace exchange;

auto main() -> int {
    constexpr std::size_t CAPACITY = 2097152;
    constexpr std::size_t MAX_ORDERS = 1000000;
    constexpr std::size_t MIN_PRICE = 1;
    constexpr std::size_t MAX_PRICE = 10000;
    constexpr std::size_t TICK_SIZE = 1;
    constexpr std::size_t PORT = 9000;

    queue::MpscQueue<engine::Order> incoming_queue(CAPACITY);
    engine::OrderBook book(MAX_ORDERS, MIN_PRICE, MAX_PRICE, TICK_SIZE);
    engine::MatchingEngine engine(incoming_queue, book);

    network::Gateway gateway(incoming_queue, PORT, 2);

    engine.start(1);
    gateway.start({2, 3});

    std::cout << "Exchange is LIVE on port " << PORT << "...\n";

    std::thread stats_thread([&engine]() {
        uint64_t last_count = 0;
        while (true) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            uint64_t current = engine.get_total_processed();
            uint64_t total_lat = engine.get_total_latency();
            uint64_t max_lat = engine.get_max_latency();

            uint64_t delta = current - last_count;
            if (delta > 0) {
                double avg_lat = static_cast<double>(total_lat) / current;
                std::cout << "Throughput: " << delta << " | Avg Latency: " << avg_lat << " ns"
                          << " | Max Jitter: " << max_lat << " ns" << std::endl;
            }
            // std::cout << "Throughput: " << (current - last_count) << " orders/sec" << std::endl;
            last_count = current;
        }
    });
    stats_thread.detach();

    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    return 0;
}