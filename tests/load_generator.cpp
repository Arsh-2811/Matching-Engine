#include "exchange/common/config.hpp"
#include "exchange/common/tsc.hpp"
#include "exchange/network/protocol.hpp"
#include <arpa/inet.h>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace exchange::network;
using namespace exchange::config;

static std::atomic<bool> keep_running{true};

static void signal_handler(int /*sig*/) { keep_running.store(false, std::memory_order_relaxed); }

static constexpr std::size_t SINGLE_MSG_SIZE = sizeof(MsgHeader) + sizeof(SbeOrder);
static constexpr std::size_t BATCH_BYTES = SINGLE_MSG_SIZE * load_gen::BATCH_SIZE;
static constexpr int CONNECT_MAX_ATTEMPTS = 10;
static constexpr int CONNECT_RETRY_MS = 200;
static constexpr uint32_t ORDER_DEFAULT_QTY = 10;
static constexpr double UNLIMITED_TOKENS_PER_CYCLE = 1.0e18;
static constexpr double NS_PER_SEC_DOUBLE = 1.0e9;
static constexpr int TOKEN_BUCKET_BURST_MULTIPLIER = 4;

// Simple token-bucket rate limiter.
// tokens_per_cycle = target_orders_per_sec / tsc_frequency_hz
class TokenBucket {
  public:
    TokenBucket(uint64_t target_rate_per_sec, double ns_per_cycle)
        : tokens_(static_cast<double>(load_gen::BATCH_SIZE)),
          max_tokens_(static_cast<double>(load_gen::BATCH_SIZE * TOKEN_BUCKET_BURST_MULTIPLIER)),
          last_tsc_(rdtsc()) {
        // tokens_per_cycle = rate / (1e9 / ns_per_cycle)
        if (target_rate_per_sec == 0 || ns_per_cycle <= 0.0) {
            tokens_per_cycle_ = UNLIMITED_TOKENS_PER_CYCLE;
        } else {
            double tsc_hz = NS_PER_SEC_DOUBLE / ns_per_cycle;
            tokens_per_cycle_ = static_cast<double>(target_rate_per_sec) / tsc_hz;
        }
    }

    // Returns true and consumes BATCH_SIZE tokens if available, else false.
    [[nodiscard]] auto try_consume() -> bool {
        auto now = rdtsc();
        double elapsed = static_cast<double>(now - last_tsc_);
        tokens_ = std::min(max_tokens_, tokens_ + elapsed * tokens_per_cycle_);
        last_tsc_ = now;
        if (tokens_ >= static_cast<double>(load_gen::BATCH_SIZE)) {
            tokens_ -= static_cast<double>(load_gen::BATCH_SIZE);
            return true;
        }
        return false;
    }

  private:
    double tokens_;
    double max_tokens_;
    double tokens_per_cycle_;
    uint64_t last_tsc_;
};

namespace {
// Xorshift64 — lock-free per-thread RNG to avoid std::rand() global lock contention.
constexpr uint64_t RNG_FALLBACK_SEED = 0xdeadbeefcafe1234ULL;
constexpr uint64_t RNG_THREAD_ID_SHIFT = 32;
constexpr int XS_SHIFT_A = 13;
constexpr int XS_SHIFT_B = 7;
constexpr int XS_SHIFT_C = 17;

thread_local uint64_t tl_rng_state = 0;

void tl_rng_seed() {
    tl_rng_state =
        rdtsc() ^ (static_cast<uint64_t>(std::hash<std::thread::id>{}(std::this_thread::get_id()))
                   << RNG_THREAD_ID_SHIFT);
    if (tl_rng_state == 0) {
        tl_rng_state = RNG_FALLBACK_SEED;
    }
}

auto tl_rand() -> uint64_t {
    tl_rng_state ^= tl_rng_state << XS_SHIFT_A;
    tl_rng_state ^= tl_rng_state >> XS_SHIFT_B;
    tl_rng_state ^= tl_rng_state << XS_SHIFT_C;
    return tl_rng_state;
}
} // namespace

static void run_client(int client_id, uint64_t target_rate_per_sec, double ns_per_cycle, int port) {
    tl_rng_seed();
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        std::perror("Socket creation failed");
        return;
    }

    int opt = 1;
    setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));

    sockaddr_in serv_addr{};
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(static_cast<uint16_t>(port));
    inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr);

    int attempts = 0;
    while (connect(sock, reinterpret_cast<struct sockaddr *>(&serv_addr), sizeof(serv_addr)) < 0) {
        if (++attempts > CONNECT_MAX_ATTEMPTS) {
            std::fprintf(stderr, "Client %d failed to connect after %d attempts.\n", client_id,
                         CONNECT_MAX_ATTEMPTS);
            close(sock);
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(CONNECT_RETRY_MS));
    }

    // Per-client order ID space, wraps within MAX_ORDERS to stay in lookup table range.
    // With NUM_CLIENTS=4, each client gets 250K IDs.
    const uint64_t id_range = engine::MAX_ORDERS / load_gen::NUM_CLIENTS;
    const uint64_t id_base = static_cast<uint64_t>(client_id) * id_range;
    uint64_t order_counter = 0;

    // Mid price walks randomly each batch (clamped to [MID_PRICE_MIN, MID_PRICE_MAX]).
    uint64_t mid_price = load_gen::MID_PRICE_INIT;

    TokenBucket bucket(target_rate_per_sec, ns_per_cycle);

    // NOLINTNEXTLINE(modernize-avoid-c-arrays)
    char batch_buf[BATCH_BYTES];

    while (keep_running.load(std::memory_order_relaxed)) {
        if (!bucket.try_consume()) {
            CPU_PAUSE();
            continue;
        }

        // Walk the mid price by ±1 each batch.
        int delta = static_cast<int>(tl_rand() % 3) - 1;
        if (delta > 0 && mid_price < load_gen::MID_PRICE_MAX) {
            ++mid_price;
        } else if (delta < 0 && mid_price > load_gen::MID_PRICE_MIN) {
            --mid_price;
        }

        for (int idx = 0; idx < load_gen::BATCH_SIZE; ++idx) {
            uint64_t oid = id_base + (order_counter % id_range) + 1;
            ++order_counter;

            // Alternate aggressive BUY / SELL to generate continuous matching:
            //   BUY  at mid+offset: crosses any resting SELL at or below mid+offset
            //   SELL at mid-offset: crosses any resting BUY  at or above mid-offset
            bool is_buy = (idx % 2 == 0);
            uint64_t price = is_buy ? mid_price + load_gen::AGGRESSIVE_OFFSET
                                    : mid_price - load_gen::AGGRESSIVE_OFFSET;

            // Per-order TSC timestamp for accurate wire-to-match latency measurement.
            uint64_t order_tsc = rdtsc();

            MsgHeader header{static_cast<uint16_t>(SINGLE_MSG_SIZE), MsgType::NEW_ORDER};
            SbeOrder order{.order_id = oid,
                           .price = price,
                           .quantity = ORDER_DEFAULT_QTY,
                           .side = static_cast<uint8_t>(is_buy ? 0 : 1),
                           .type = 0, // NEW
                           .timestamp_tsc = order_tsc};

            char *ptr = batch_buf + static_cast<std::size_t>(idx) * SINGLE_MSG_SIZE;
            std::memcpy(ptr, &header, sizeof(header));
            std::memcpy(ptr + sizeof(header), &order, sizeof(order));
        }

        if (send(sock, batch_buf, BATCH_BYTES, 0) < 0) {
            break;
        }
    }

    close(sock);
}

auto main(int argc, char *argv[]) -> int {
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    int num_clients = (argc > 1) ? std::stoi(argv[1]) : load_gen::NUM_CLIENTS;
    // Rate limit in orders/sec per client. 0 = unlimited.
    uint64_t rate_per_client =
        (argc > 2) ? static_cast<uint64_t>(std::stoll(argv[2])) : load_gen::DEFAULT_RATE_LIMIT;

    std::printf("--- LOAD GENERATOR STARTING ---\n");
    std::printf("Clients: %d | Rate per client: %s orders/s\n", num_clients,
                rate_per_client == 0 ? "unlimited" : std::to_string(rate_per_client).c_str());

    // Calibrate TSC once — shared across all client threads.
    double ns_per_cycle =
        exchange::common::calibrate_tsc_ns_per_cycle(exchange::config::engine::TSC_CALIBRATION_MS);
    std::printf("TSC calibration: %.4f ns/cycle\n", ns_per_cycle);

    auto start_time = std::chrono::high_resolution_clock::now();

    std::vector<std::thread> clients;
    clients.reserve(static_cast<std::size_t>(num_clients));
    for (int i = 0; i < num_clients; ++i) {
        clients.emplace_back(run_client, i, rate_per_client, ns_per_cycle, network::PORT);
    }

    for (auto &client_thread : clients) {
        if (client_thread.joinable()) {
            client_thread.join();
        }
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> diff = end_time - start_time;

    std::printf("--- LOAD GENERATOR STOPPED ---\n");
    std::printf("Total Time: %.3f seconds\n", diff.count());

    return 0;
}
