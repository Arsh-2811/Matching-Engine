#include "exchange/network/worker.hpp"
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

void GatewayWorker::set_socket_options(int fd) {
    int opt = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));

    int buf_size = 4 * 1024 * 1024; // 4MB
    setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &buf_size, sizeof(buf_size));
    setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &buf_size, sizeof(buf_size));
}

void GatewayWorker::set_non_blocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

void GatewayWorker::accept_new_clients() {
    while (true) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        int client_fd = accept(listen_fd_, (struct sockaddr *)&client_addr, &client_len);

        if (client_fd < 0) {
            if (errno != EAGAIN && errno != EWOULDBLOCK) {
                std::cerr << "Accept error" << std::endl;
            }
            break;
        }

        set_non_blocking(client_fd);
        set_socket_options(client_fd);

        struct epoll_event ev;
        ev.events = EPOLLIN | EPOLLET; // Edge Triggered
        ev.data.fd = client_fd;
        epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, client_fd, &ev);

        sessions_.emplace(client_fd, ClientSession(client_fd));
    }
}

void GatewayWorker::handle_client_data(int fd) {
    auto it = sessions_.find(fd);
    if (it == sessions_.end())
        return;
    auto &session = it->second;

    uint8_t temp_buf[4096];
    while (true) {
        ssize_t bytes_read = recv(fd, temp_buf, sizeof(temp_buf), 0);
        if (bytes_read < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                break;
            return;
        } else if (bytes_read == 0) {
            close(fd);
            sessions_.erase(it);
            return;
        }

        session.buffer.insert(session.buffer.end(), temp_buf, temp_buf + bytes_read);
    }

    size_t offset = 0;
    while (session.buffer.size() - offset >= sizeof(MsgHeader)) {
        auto *header = reinterpret_cast<MsgHeader *>(&session.buffer[offset]);

        if (session.buffer.size() - offset < header->msg_size)
            break;
        auto *sbe_order = reinterpret_cast<SbeOrder *>(&session.buffer[offset + sizeof(MsgHeader)]);

        engine::Order internal_order{.order_id = sbe_order->order_id,
                                     .price = sbe_order->price,
                                     .quantity = sbe_order->quantity,
                                     .side = static_cast<engine::Side>(sbe_order->side),
                                     .type = static_cast<engine::OrderType>(sbe_order->type),
                                     .timestamp_ns = sbe_order->timestamp_ns};

        while (!incoming_queue_.push(internal_order)) {
            NET_CPU_PAUSE();
        }

        offset += header->msg_size;
    }

    if (offset > 0) {
        session.buffer.erase(session.buffer.begin(), session.buffer.begin() + offset);
    }
}

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
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = INADDR_ANY;

    bind(listen_fd_, (struct sockaddr *)&addr, sizeof(addr));
    listen(listen_fd_, SOMAXCONN);

    epoll_fd_ = epoll_create1(0);
    struct epoll_event ev;
    ev.events = EPOLLIN | EPOLLET;
    ev.data.fd = listen_fd_;
    epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, listen_fd_, &ev);

    struct epoll_event events[128];
    while (true) {
        int n = epoll_wait(epoll_fd_, events, 128, 0);
        if (n == 0) {
            NET_CPU_PAUSE();
            continue;
        }
        for (int i = 0; i < n; ++i) {
            if (events[i].data.fd == listen_fd_) {
                accept_new_clients();
            } else {
                handle_client_data(events[i].data.fd);
            }
        }
    }
}

} // namespace exchange::network