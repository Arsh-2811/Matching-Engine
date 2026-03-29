#include "exchange/network/worker.hpp"
#include "exchange/network/protocol.hpp"
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <sys/socket.h>
#include <unistd.h>

#if defined(__x86_64__) || defined(_M_X64)
#include <immintrin.h>
#define NET_CPU_PAUSE() _mm_pause()
#else
#define NET_CPU_PAUSE() __asm__ volatile("yield" ::: "memory")
#endif

namespace exchange::network {

namespace {
constexpr int RECV_BUF_SIZE = 4 * 1024 * 1024;
constexpr size_t COMPACTION_THRESH = 4096;
constexpr int EPOLL_MAX_EVENTS = 128;
} // namespace

void GatewayWorker::set_socket_options(int sock_fd) {
    int opt = 1;
    setsockopt(sock_fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));
    int buf_size = RECV_BUF_SIZE;
    setsockopt(sock_fd, SOL_SOCKET, SO_RCVBUF, &buf_size, sizeof(buf_size));
    setsockopt(sock_fd, SOL_SOCKET, SO_SNDBUF, &buf_size, sizeof(buf_size));
}

void GatewayWorker::set_non_blocking(int sock_fd) {
    int flags = fcntl(sock_fd, F_GETFL, 0);
    fcntl(sock_fd, F_SETFL, flags | O_NONBLOCK);
}

void GatewayWorker::accept_new_clients() {
    while (true) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        int client_fd =
            accept(listen_fd_, reinterpret_cast<struct sockaddr *>(&client_addr), &client_len);
        if (client_fd < 0) {
            if (errno != EAGAIN && errno != EWOULDBLOCK) {
                std::cerr << "Accept error" << std::endl;
            }
            break;
        }
        set_non_blocking(client_fd);
        set_socket_options(client_fd);

        struct epoll_event epev;
        epev.events = EPOLLIN | EPOLLET;
        epev.data.fd = client_fd;
        epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, client_fd, &epev);
        sessions_.emplace(client_fd, ClientSession(client_fd));
    }
}

void GatewayWorker::handle_disconnect(int sock_fd) {
    auto iter = sessions_.find(sock_fd);
    if (iter == sessions_.end()) {
        return;
    }
    auto &session = iter->second;

    // Cancel all outstanding orders for this client.
    for (uint64_t exchange_id : session.live_exchange_ids) {
        routing_table_.clear(exchange_id);
        engine::Order cancel{};
        cancel.order_id = exchange_id;
        cancel.type = engine::OrderType::CANCEL;
        cancel.client_fd = -1; // suppress ACK — client already disconnected
        while (!incoming_queue_.push(cancel)) {
            NET_CPU_PAUSE();
        }
    }

    close(sock_fd);
    sessions_.erase(iter);
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity)
void GatewayWorker::handle_client_data(int sock_fd) {
    auto iter = sessions_.find(sock_fd);
    if (iter == sessions_.end()) {
        return;
    }
    auto &session = iter->second;

    while (true) {
        if (session.available_space() < COMPACTION_THRESH) {
            size_t remaining = session.pending_bytes();
            if (remaining > 0 && session.read_idx > 0) {
                std::memmove(session.buffer.data(), session.buffer.data() + session.read_idx,
                             remaining);
            }
            session.read_idx = 0;
            session.write_idx = remaining;
            if (session.available_space() < COMPACTION_THRESH) {
                break;
            }
        }

        ssize_t bytes_read =
            recv(sock_fd, session.buffer.data() + session.write_idx, session.available_space(), 0);

        if (bytes_read < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }
            handle_disconnect(sock_fd);
            return;
        }
        if (bytes_read == 0) {
            handle_disconnect(sock_fd);
            return;
        }

        session.write_idx += static_cast<size_t>(bytes_read);

        while (session.pending_bytes() >= sizeof(MsgHeader)) {
            auto *header = reinterpret_cast<MsgHeader *>(&session.buffer[session.read_idx]);
            if (session.pending_bytes() < header->msg_size) {
                break;
            }

            auto *sbe_order =
                reinterpret_cast<SbeOrder *>(&session.buffer[session.read_idx + sizeof(MsgHeader)]);

            if (header->type == MsgType::NEW_ORDER) {
                uint64_t exchange_id = next_exchange_id();

                // Write routing entry BEFORE pushing to queue.
                // The MpscQueue release-store provides the compiler barrier so
                // this write is visible to AckThread via the acquire-release chain.
                routing_table_.set(exchange_id, sock_fd, sbe_order->order_id);
                session.live_exchange_ids.push_back(exchange_id);

                engine::Order internal_order{.order_id = exchange_id,
                                             .price = sbe_order->price,
                                             .quantity = sbe_order->quantity,
                                             .side = static_cast<engine::Side>(sbe_order->side),
                                             .type = engine::OrderType::NEW,
                                             .timestamp_tsc = sbe_order->timestamp_tsc,
                                             .client_fd = sock_fd,
                                             .cl_ord_id = sbe_order->order_id};

                while (!incoming_queue_.push(internal_order)) {
                    NET_CPU_PAUSE();
                }
            } else if (header->type == MsgType::CANCEL_ORDER) {
                // order_id field carries the ExchangeOrderID for cancel.
                const auto &rte = routing_table_.get(sbe_order->order_id);
                engine::Order cancel_order{.order_id = sbe_order->order_id,
                                           .price = 0,
                                           .quantity = 0,
                                           .side = engine::Side::BUY,
                                           .type = engine::OrderType::CANCEL,
                                           .timestamp_tsc = sbe_order->timestamp_tsc,
                                           .client_fd = sock_fd,
                                           .cl_ord_id = rte.cl_ord_id};

                while (!incoming_queue_.push(cancel_order)) {
                    NET_CPU_PAUSE();
                }
            }

            session.read_idx += header->msg_size;
        }
    }
}

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
void GatewayWorker::run(int core_id, int port) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);

    listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));
    set_non_blocking(listen_fd_);

    struct sockaddr_in addr {};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port));
    addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(listen_fd_, reinterpret_cast<struct sockaddr *>(&addr), sizeof(addr)) < 0) {
        std::cerr << "Bind error" << std::endl;
        return;
    }
    listen(listen_fd_, SOMAXCONN);

    epoll_fd_ = epoll_create1(0);
    struct epoll_event epev;
    epev.events = EPOLLIN | EPOLLET;
    epev.data.fd = listen_fd_;
    epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, listen_fd_, &epev);

    // NOLINTNEXTLINE(modernize-avoid-c-arrays)
    struct epoll_event events[EPOLL_MAX_EVENTS];
    while (!stop_flag_.load(std::memory_order_relaxed)) {
        int nev = epoll_wait(epoll_fd_, events, EPOLL_MAX_EVENTS, 0);
        if (nev == 0) {
            NET_CPU_PAUSE();
            continue;
        }
        for (int idx = 0; idx < nev; ++idx) {
            if (events[idx].data.fd == listen_fd_) {
                accept_new_clients();
            } else {
                handle_client_data(events[idx].data.fd);
            }
        }
    }
    close(listen_fd_);
    close(epoll_fd_);
}

} // namespace exchange::network
