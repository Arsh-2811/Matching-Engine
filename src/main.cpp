#include "exchange/common/config.hpp"
#include "exchange/engine/ack_thread.hpp"
#include "exchange/engine/market_data_thread.hpp"
#include "exchange/engine/matching_engine.hpp"
#include "exchange/engine/metrics_thread.hpp"
#include "exchange/engine/order_book.hpp"
#include "exchange/engine/types.hpp"
#include "exchange/network/ack_routing_table.hpp"
#include "exchange/network/gateway.hpp"
#include "exchange/queue/mpsc_queue.hpp"
#include "exchange/queue/spmc_broadcast_queue.hpp"
#include <csignal>
#include <cstdio>
#include <thread>

using namespace exchange;

static std::atomic<bool> keep_running{true};
static void sig_handler(int /*signum*/) { keep_running.store(false, std::memory_order_relaxed); }

auto main() -> int {
    namespace cfg = exchange::config;

    signal(SIGINT, sig_handler);
    signal(SIGTERM, sig_handler);

    queue::MpscQueue<engine::Order> incoming_queue(cfg::engine::QUEUE_CAPACITY);

    queue::SpmcBroadcastQueue<engine::OutboundEvent> outbound_queue(
        cfg::engine::OUTBOUND_QUEUE_CAPACITY);

    // Register all consumers before the engine starts pushing events.
    engine::MetricsThread metrics(outbound_queue, &incoming_queue);
    engine::MarketDataThread market_data(outbound_queue);

    network::AckRoutingTable routing_table(cfg::engine::MAX_ORDERS);
    engine::AckThread ack_thread(outbound_queue, routing_table);

    engine::OrderBook book(cfg::engine::MAX_ORDERS, cfg::engine::MIN_PRICE, cfg::engine::MAX_PRICE,
                           cfg::engine::TICK_SIZE);

    engine::MatchingEngine engine(incoming_queue, book, &outbound_queue);

    network::Gateway gateway(incoming_queue, routing_table, cfg::network::PORT, 2);

    engine.start(cfg::engine::ENGINE_CORE_ID);
    metrics.start(cfg::engine::METRICS_CORE_ID);
    market_data.start(cfg::engine::METRICS_CORE_ID);
    ack_thread.start(0); // Core 0 — main thread's core, otherwise idle
    gateway.start({cfg::engine::GATEWAY_CORE_1, cfg::engine::GATEWAY_CORE_2});

    std::printf("Exchange is LIVE on port %d...\n", cfg::network::PORT);
    std::fflush(stdout);

    static constexpr int SHUTDOWN_POLL_MS = 100;
    while (keep_running.load(std::memory_order_relaxed)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(SHUTDOWN_POLL_MS));
    }

    std::printf("Shutting down exchange...\n");
    std::fflush(stdout);

    gateway.stop();

    engine::Order poison{};
    poison.order_id = 0;
    while (!incoming_queue.push(poison)) {
    }
    engine.stop();

    std::printf("Outbound drops: %llu\n", static_cast<unsigned long long>(engine.outbound_drops()));
    std::fflush(stdout);

    metrics.stop();
    market_data.stop();
    ack_thread.stop();

    std::printf("Exchange shut down cleanly.\n");
    std::fflush(stdout);

    return 0;
}
