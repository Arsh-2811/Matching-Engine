#pragma once
#include <cstddef>
#include <cstdint>
#include <vector>

namespace exchange::network {

// Maps ExchangeOrderID → {client_fd, cl_ord_id}.
// Written by GatewayWorker at order ingress (before MpscQueue push).
// Read and cleared by AckThread after sending the response.
// Thread safety: relies on the acquire-release chain through the two queues.
// Worker write → MpscQueue release → Engine acquire → SPMC release → AckThread acquire.
// By the time AckThread reads this table, the worker's write is visible.
struct RoutingEntry {
    int client_fd{-1};
    uint64_t cl_ord_id{0};
};

class AckRoutingTable {
  private:
    std::vector<RoutingEntry> table_;
    std::size_t capacity_;

  public:
    explicit AckRoutingTable(std::size_t capacity) : capacity_(capacity) {
        table_.resize(capacity_ + 1); // 1-indexed; slot 0 unused
        for (auto &e : table_) {
            e = {-1, 0};
        }
    }

    AckRoutingTable(const AckRoutingTable &) = delete;
    AckRoutingTable &operator=(const AckRoutingTable &) = delete;

    void set(uint64_t exchange_id, int client_fd, uint64_t cl_ord_id) {
        if (exchange_id > 0 && exchange_id <= capacity_) {
            table_[exchange_id] = {client_fd, cl_ord_id};
        }
    }

    [[nodiscard]] auto get(uint64_t exchange_id) const -> const RoutingEntry & {
        if (exchange_id > 0 && exchange_id <= capacity_) {
            return table_[exchange_id];
        }
        static const RoutingEntry null_entry{-1, 0};
        return null_entry;
    }

    void clear(uint64_t exchange_id) {
        if (exchange_id > 0 && exchange_id <= capacity_) {
            table_[exchange_id] = {-1, 0};
        }
    }
};

} // namespace exchange::network
