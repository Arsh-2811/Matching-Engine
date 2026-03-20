#include "exchange/common/config.hpp"
#include "exchange/engine/matching_engine.hpp"
#include "exchange/engine/metrics_thread.hpp"
#include "exchange/engine/order_book.hpp"
#include "exchange/engine/types.hpp"
#include "exchange/network/gateway.hpp"
#include "exchange/queue/mpsc_queue.hpp"
#include "exchange/queue/spmc_broadcast_queue.hpp"
#include <csignal>
#include <cstdio>
#include <thread>

using namespace exchange;

auto main() -> int {
    namespace cfg = exchange::config;

    // ----- Inbound path -----
    queue::MpscQueue<engine::Order> incoming_queue(cfg::engine::QUEUE_CAPACITY);

    // ----- Outbound path (broadcast to all downstream consumers) -----
    queue::SpmcBroadcastQueue<engine::OutboundEvent> outbound_queue(
        cfg::engine::OUTBOUND_QUEUE_CAPACITY);

    // MetricsThread must be constructed first — it registers as a consumer
    // before the engine starts pushing events.
    engine::MetricsThread metrics(outbound_queue);

    engine::OrderBook book(cfg::engine::MAX_ORDERS, cfg::engine::MIN_PRICE, cfg::engine::MAX_PRICE,
                           cfg::engine::TICK_SIZE);

    engine::MatchingEngine engine(incoming_queue, book, &outbound_queue);

    network::Gateway gateway(incoming_queue, cfg::network::PORT, 2);

    // Start threads — engine first so it is ready before clients arrive.
    engine.start(cfg::engine::ENGINE_CORE_ID);
    metrics.start(cfg::engine::METRICS_CORE_ID);
    gateway.start({cfg::engine::GATEWAY_CORE_1, cfg::engine::GATEWAY_CORE_2});

    std::printf("Exchange is LIVE on port %d...\n", cfg::network::PORT);
    std::fflush(stdout);

    // Park the main thread — the exchange runs until the process is killed.
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    return 0;
}
