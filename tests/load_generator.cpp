#include "exchange/common/config.hpp"
#include "exchange/common/tsc.hpp"
#include "exchange/network/protocol.hpp"
#include <arpa/inet.h>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <unordered_map>
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
static constexpr int CANCEL_INTERVAL = 10; // 10% cancel rate by new-order count

// Wire size of an ACK message from the server.
static constexpr std::size_t ACK_MSG_SIZE = sizeof(MsgHeader) + sizeof(AckMsg);
// Large enough to absorb burst ACK data at unlimited send rates.
static constexpr std::size_t RECV_BUF_CAPACITY = 1024 * 1024; // 1 MB

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

// Per-client recv buffer for parsing AckMsg responses.
struct RecvBuffer {
    char data[RECV_BUF_CAPACITY]{};
    size_t read_idx{0};
    size_t write_idx{0};

    [[nodiscard]] auto pending_bytes() const -> size_t { return write_idx - read_idx; }
    [[nodiscard]] auto available_space() const -> size_t { return RECV_BUF_CAPACITY - write_idx; }

    void compact() {
        if (read_idx > 0 && write_idx > read_idx) {
            std::memmove(data, data + read_idx, write_idx - read_idx);
            write_idx -= read_idx;
            read_idx = 0;
        } else if (read_idx == write_idx) {
            read_idx = 0;
            write_idx = 0;
        }
    }
};

// Live-order tracking:
//   live_map  : exchange_order_id → {cl_ord_id, index_in_live_keys}
//               O(1) lookup AND O(1) removal (index lets us swap-erase without scanning)
//   live_keys : flat list of exchange_order_ids for O(1) random cancel pick
//
// Cap: at most LIVE_CAP resting orders tracked. Once full, new entries are
// silently ignored. This prevents unbounded growth when maker fills arrive
// without a corresponding fill-notification (makers currently emit no event).
static constexpr std::size_t LIVE_CAP = 65536;

static void erase_live(std::unordered_map<uint64_t, std::pair<uint64_t, std::size_t>> &live_map,
                       std::vector<uint64_t> &live_keys, uint64_t eid) {
    auto it = live_map.find(eid);
    if (it == live_map.end()) {
        return;
    }
    std::size_t idx = it->second.second; // index in live_keys
    live_map.erase(it);

    if (idx + 1 < live_keys.size()) {
        // Swap with last element and update its index entry.
        uint64_t last_eid = live_keys.back();
        live_keys[idx] = last_eid;
        live_map[last_eid].second = idx;
    }
    live_keys.pop_back();
}

static void insert_live(std::unordered_map<uint64_t, std::pair<uint64_t, std::size_t>> &live_map,
                        std::vector<uint64_t> &live_keys, uint64_t eid, uint64_t cl_ord_id) {
    if (live_keys.size() >= LIVE_CAP) {
        return; // cap hit — discard entry to prevent unbounded growth
    }
    auto [it, inserted] = live_map.emplace(eid, std::make_pair(cl_ord_id, live_keys.size()));
    (void)it;
    if (inserted) {
        live_keys.push_back(eid);
    }
}

// Drain all available ACK messages from the socket (loops until EAGAIN).
// live_map/live_keys are updated for each ACK received.
static void drain_acks(int sock, RecvBuffer &rbuf,
                       std::unordered_map<uint64_t, std::pair<uint64_t, std::size_t>> &live_map,
                       std::vector<uint64_t> &live_keys, uint64_t &cancels_acked,
                       uint64_t &cancel_rejects) {
    // Loop recv() until EAGAIN so we drain the full kernel receive buffer.
    while (true) {
        if (rbuf.available_space() < ACK_MSG_SIZE) {
            rbuf.compact();
            if (rbuf.available_space() < ACK_MSG_SIZE) {
                break; // buffer full even after compaction — parse what we have
            }
        }

        ssize_t bytes_read =
            recv(sock, rbuf.data + rbuf.write_idx, rbuf.available_space(), MSG_DONTWAIT);
        if (bytes_read <= 0) {
            break; // EAGAIN / EWOULDBLOCK or error — stop reading
        }
        rbuf.write_idx += static_cast<size_t>(bytes_read);

        // Parse all complete ACK messages in the buffer.
        while (rbuf.pending_bytes() >= ACK_MSG_SIZE) {
            auto *hdr = reinterpret_cast<MsgHeader *>(rbuf.data + rbuf.read_idx);
            if (hdr->type != MsgType::ORDER_ACK) {
                size_t skip =
                    (hdr->msg_size >= sizeof(MsgHeader) && rbuf.pending_bytes() >= hdr->msg_size)
                        ? hdr->msg_size
                        : 1;
                rbuf.read_idx += skip;
                continue;
            }
            if (rbuf.pending_bytes() < hdr->msg_size) {
                break;
            }

            const auto *ack =
                reinterpret_cast<const AckMsg *>(rbuf.data + rbuf.read_idx + sizeof(MsgHeader));
            uint8_t event_type = ack->event_type;
            uint64_t eid = ack->exchange_order_id;

            // EventType: 0=NEW_ACK, 1=PARTIALLY_FILLED, 2=FILLED, 3=CANCELLED, 4=CANCEL_REJECTED
            if (event_type == 0 || event_type == 1) {
                // Order resting — track as live (capped at LIVE_CAP).
                insert_live(live_map, live_keys, eid, ack->cl_ord_id);
            } else if (event_type == 2 || event_type == 3) {
                // FILLED or CANCELLED — remove from live tracking.
                erase_live(live_map, live_keys, eid);
                if (event_type == 3) {
                    ++cancels_acked;
                }
            } else if (event_type == 4) {
                // CANCEL_REJECTED — remove defensively.
                erase_live(live_map, live_keys, eid);
                ++cancel_rejects;
            }

            rbuf.read_idx += hdr->msg_size;
        }
    }

    rbuf.compact();
}

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

    RecvBuffer rbuf;
    // O(1) lookup and O(1) removal: exchange_id → {cl_ord_id, index_in_live_keys}
    std::unordered_map<uint64_t, std::pair<uint64_t, std::size_t>> live_map;
    live_map.reserve(LIVE_CAP);
    // Flat list for O(1) random pick when selecting an order to cancel.
    std::vector<uint64_t> live_keys;
    live_keys.reserve(LIVE_CAP);

    uint64_t cancels_sent = 0;
    uint64_t cancels_acked = 0;
    uint64_t cancel_rejects = 0;
    int cancel_counter = 0;

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

        // Maybe send a cancel before the new-order batch (10% cancel rate).
        if (cancel_counter >= CANCEL_INTERVAL && !live_keys.empty()) {
            uint64_t cancel_eid = live_keys[static_cast<size_t>(tl_rand() % live_keys.size())];

            // NOLINTNEXTLINE(modernize-avoid-c-arrays)
            char cancel_buf[SINGLE_MSG_SIZE];
            MsgHeader chdr{static_cast<uint16_t>(SINGLE_MSG_SIZE), MsgType::CANCEL_ORDER};
            SbeOrder corder{.order_id = cancel_eid,
                            .price = 0,
                            .quantity = 0,
                            .side = 0,
                            .type = 1, // CANCEL
                            .timestamp_tsc = rdtsc()};
            std::memcpy(cancel_buf, &chdr, sizeof(chdr));
            std::memcpy(cancel_buf + sizeof(chdr), &corder, sizeof(corder));
            send(sock, cancel_buf, SINGLE_MSG_SIZE, 0);

            ++cancels_sent;
            cancel_counter = 0;
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

        ++cancel_counter;

        // Drain ACKs non-blocking (loops until EAGAIN) — don't stall the send loop.
        drain_acks(sock, rbuf, live_map, live_keys, cancels_acked, cancel_rejects);
    }

    std::fprintf(stderr, "Client %d: cancels_sent=%llu cancels_acked=%llu cancel_rejects=%llu\n",
                 client_id, static_cast<unsigned long long>(cancels_sent),
                 static_cast<unsigned long long>(cancels_acked),
                 static_cast<unsigned long long>(cancel_rejects));

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
