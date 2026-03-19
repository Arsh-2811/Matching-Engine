#include "exchange/common/config.hpp"
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
    namespace cfg = exchange::config;

    queue::MpscQueue<engine::Order> incoming_queue(cfg::engine::QUEUE_CAPACITY);
    engine::OrderBook book(cfg::engine::MAX_ORDERS, cfg::engine::MIN_PRICE, cfg::engine::MAX_PRICE,
                           cfg::engine::TICK_SIZE);
    engine::MatchingEngine engine(incoming_queue, book);

    network::Gateway gateway(incoming_queue, cfg::network::PORT, 2);

    engine.start(cfg::engine::ENGINE_CORE_ID);
    gateway.start({cfg::engine::GATEWAY_CORE_1, cfg::engine::GATEWAY_CORE_2});

    std::cout << "Exchange is LIVE on port " << cfg::network::PORT << "...\n";

    std::thread stats_thread([&engine]() {
        uint64_t last_count = 0;
        uint64_t last_latency = 0;

        while (true) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            uint64_t current_count = engine.get_total_processed();
            uint64_t current_total_lat = engine.get_total_latency();
            uint64_t max_lat = engine.get_max_latency();

            uint64_t delta_count = current_count - last_count;
            uint64_t delta_latency = current_total_lat - last_latency;

            if (delta_count > 0) {
                double avg_lat = static_cast<double>(delta_latency) / delta_count;
                std::cout << "Throughput: " << delta_count << " | Snap Avg Latency: " << avg_lat
                          << " ns" << " | Max Jitter: " << max_lat << " ns" << std::endl;
            }
            last_count = current_count;
            last_latency = current_total_lat;
        }
    });
    stats_thread.detach();

    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    return 0;
}