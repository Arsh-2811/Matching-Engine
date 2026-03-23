#pragma once
#include "exchange/engine/types.hpp"
#include "exchange/network/ack_routing_table.hpp"
#include "exchange/network/protocol.hpp"
#include "exchange/queue/spmc_broadcast_queue.hpp"
#include <atomic>
#include <pthread.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <vector>

namespace exchange::engine {

// Consumes OutboundEvents and sends AckMsg responses back to clients.
// Registered as a BEST-EFFORT consumer on the SPMC broadcast queue — its
// cursor is never checked for producer backpressure. If it falls more than
// queue capacity behind, it skips ahead (events are dropped) so MetricsThread
// and the engine are never impaired.
//
// Batching: ACKs are accumulated per client_fd and flushed in one send()
// call per client every ACK_BATCH events (or on idle). This reduces syscalls
// from O(events) to O(events/ACK_BATCH * num_clients) — ~64× fewer at
// ACK_BATCH=256, keeping the thread from falling behind the engine.
class AckThread {
  private:
    static constexpr int ACK_BATCH = 256; // flush per-client buffer every N events
    static constexpr std::size_t ACK_MSG_SIZE =
        sizeof(network::MsgHeader) + sizeof(network::AckMsg);

    queue::SpmcBroadcastQueue<OutboundEvent> &outbound_;
    network::AckRoutingTable &routing_table_;
    std::atomic<bool> is_running_{false};
    std::thread thread_;
    int consumer_id_{-1};

    void run(int core_id) {
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(core_id, &cpuset);
        pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);

        // Per-client accumulation buffers. Key = client_fd.
        // Typically ≤ 4 entries (NUM_CLIENTS).
        std::unordered_map<int, std::vector<char>> send_bufs;
        send_bufs.reserve(8);

        int events_since_flush = 0;

        auto flush_all = [&]() {
            for (auto &[fd, buf] : send_bufs) {
                if (!buf.empty()) {
                    ::send(fd, buf.data(), static_cast<int>(buf.size()), MSG_NOSIGNAL);
                    buf.clear();
                }
            }
            events_since_flush = 0;
        };

        while (is_running_.load(std::memory_order_acquire)) {
            OutboundEvent event;
            if (!outbound_.pop_best_effort(consumer_id_, event)) {
                if (events_since_flush > 0) {
                    flush_all();
                }
                std::this_thread::yield();
                continue;
            }

            // Skip poison pill (order_id == 0 used for engine shutdown)
            if (event.order_id == 0) {
                continue;
            }

            const auto &entry = routing_table_.get(event.order_id);
            if (entry.client_fd < 0) {
                // No routing entry — order came from a client that disconnected.
                continue;
            }

            // Append ACK bytes to this client's send buffer.
            auto &buf = send_bufs[entry.client_fd];
            const std::size_t old_size = buf.size();
            buf.resize(old_size + ACK_MSG_SIZE);

            network::MsgHeader hdr;
            hdr.msg_size = static_cast<uint16_t>(ACK_MSG_SIZE);
            hdr.type = network::MsgType::ORDER_ACK;

            network::AckMsg ack{};
            ack.exchange_order_id = event.order_id;
            ack.cl_ord_id = entry.cl_ord_id;
            ack.matched_qty = event.matched_qty;
            ack.leaves_qty = event.leaves_qty;
            ack.event_type = static_cast<uint8_t>(event.event_type);

            __builtin_memcpy(buf.data() + old_size, &hdr, sizeof(hdr));
            __builtin_memcpy(buf.data() + old_size + sizeof(hdr), &ack, sizeof(ack));

            // Free routing slot for fully-resolved orders.
            bool resolved = (event.event_type == EventType::FILLED ||
                             event.event_type == EventType::CANCELLED ||
                             event.event_type == EventType::CANCEL_REJECTED);
            if (resolved) {
                routing_table_.clear(event.order_id);
            }

            if (++events_since_flush >= ACK_BATCH) {
                flush_all();
            }
        }
    }

  public:
    explicit AckThread(queue::SpmcBroadcastQueue<OutboundEvent> &outbound,
                       network::AckRoutingTable &routing_table)
        : outbound_(outbound), routing_table_(routing_table) {
        consumer_id_ = outbound_.register_best_effort_consumer();
    }

    AckThread(const AckThread &) = delete;
    AckThread &operator=(const AckThread &) = delete;
    AckThread(AckThread &&) = delete;
    AckThread &operator=(AckThread &&) = delete;

    ~AckThread() { stop(); }

    void start(int core_id) {
        if (!is_running_.load(std::memory_order_relaxed)) {
            is_running_.store(true, std::memory_order_release);
            thread_ = std::thread(&AckThread::run, this, core_id);
        }
    }

    void stop() {
        is_running_.store(false, std::memory_order_release);
        if (thread_.joinable()) {
            thread_.join();
        }
    }
};

} // namespace exchange::engine
