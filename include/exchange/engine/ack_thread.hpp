#pragma once
#include "exchange/engine/types.hpp"
#include "exchange/network/protocol.hpp"
#include "exchange/queue/mpsc_queue.hpp"
#include "exchange/queue/spmc_broadcast_queue.hpp"
#include <atomic>
#include <functional>
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
// Also drains an optional maker_fill_queue_ that carries MAKER_FILLED events
// on a path that bypasses MetricsThread entirely.
//
// Batching: ACKs are accumulated per client_fd and flushed in one send()
// call per client every ACK_BATCH events (or on idle). Each queue has its own
// independent flush counter — taker ACKs flush every ACK_BATCH taker events,
// maker fill ACKs flush every ACK_BATCH maker fill events. This prevents
// maker fill volume from inflating the effective flush interval for taker ACKs
// (which would inflate round-trip latency at rate-limited operation).
class AckThread {
  private:
    static constexpr int ACK_BATCH = 256; // flush per-client buffer every N events
    static constexpr std::size_t ACK_MSG_SIZE =
        sizeof(network::MsgHeader) + sizeof(network::AckMsg);

    queue::SpmcBroadcastQueue<OutboundEvent> &outbound_;
    // Optional SPMC queue (zero critical consumers) for MAKER_FILLED events.
    queue::SpmcBroadcastQueue<OutboundEvent> *maker_fill_queue_;
    int maker_fill_consumer_id_{-1};
    std::atomic<bool> is_running_{false};
    std::thread thread_;
    int consumer_id_{-1};

    static void append_ack(std::unordered_map<int, std::vector<char>> &send_bufs,
                           const OutboundEvent &ev, int &events_since_flush,
                           const std::function<void()> &flush_all) {
        auto &buf = send_bufs[ev.client_fd];
        const std::size_t old_size = buf.size();
        buf.resize(old_size + ACK_MSG_SIZE);

        network::MsgHeader hdr;
        hdr.msg_size = static_cast<uint16_t>(ACK_MSG_SIZE);
        hdr.type = network::MsgType::ORDER_ACK;

        network::AckMsg ack{};
        ack.exchange_order_id = ev.order_id;
        ack.cl_ord_id = ev.cl_ord_id;
        ack.matched_qty = ev.matched_qty;
        ack.leaves_qty = ev.leaves_qty;
        ack.event_type = static_cast<uint8_t>(ev.event_type);

        __builtin_memcpy(buf.data() + old_size, &hdr, sizeof(hdr));
        __builtin_memcpy(buf.data() + old_size + sizeof(hdr), &ack, sizeof(ack));

        if (++events_since_flush >= ACK_BATCH) {
            flush_all();
        }
    }

    void run(int core_id) {
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(core_id, &cpuset);
        pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);

        // Per-client accumulation buffers. Key = client_fd.
        // Typically ≤ 4 entries (NUM_CLIENTS).
        std::unordered_map<int, std::vector<char>> send_bufs;
        send_bufs.reserve(8);

        // Independent flush counters per queue — prevents maker fill volume from
        // inflating the effective flush interval for taker ACKs.
        int taker_events_since_flush = 0;
        int maker_events_since_flush = 0;

        auto flush_all = [&]() {
            for (auto &[fd, buf] : send_bufs) {
                if (!buf.empty()) {
                    ::send(fd, buf.data(), static_cast<int>(buf.size()), MSG_NOSIGNAL);
                    buf.clear();
                }
            }
            taker_events_since_flush = 0;
            maker_events_since_flush = 0;
        };

        while (is_running_.load(std::memory_order_acquire)) {
            bool got_any = false;

            // Drain taker events from SPMC (best-effort: skips ahead if lapped).
            OutboundEvent event;
            if (outbound_.pop_best_effort(consumer_id_, event)) {
                got_any = true;
                // Skip poison pill (order_id == 0 used for engine shutdown)
                if (event.order_id != 0 && event.client_fd >= 0) {
                    append_ack(send_bufs, event, taker_events_since_flush, flush_all);
                }
            }

            // Drain maker fill events from dedicated SPMC queue (best-effort).
            if (maker_fill_queue_ != nullptr) {
                OutboundEvent mfev;
                while (maker_fill_queue_->pop_best_effort(maker_fill_consumer_id_, mfev)) {
                    got_any = true;
                    if (mfev.order_id != 0 && mfev.client_fd >= 0) {
                        append_ack(send_bufs, mfev, maker_events_since_flush, flush_all);
                    }
                }
            }

            if (!got_any) {
                if (taker_events_since_flush > 0 || maker_events_since_flush > 0) {
                    flush_all();
                }
                std::this_thread::yield();
            }
        }
    }

  public:
    explicit AckThread(queue::SpmcBroadcastQueue<OutboundEvent> &outbound,
                       queue::SpmcBroadcastQueue<OutboundEvent> *maker_fill_queue = nullptr)
        : outbound_(outbound), maker_fill_queue_(maker_fill_queue) {
        consumer_id_ = outbound_.register_best_effort_consumer();
        if (maker_fill_queue_ != nullptr) {
            maker_fill_consumer_id_ = maker_fill_queue_->register_best_effort_consumer();
        }
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
