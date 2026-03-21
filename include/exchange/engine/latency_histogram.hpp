#pragma once
#include <array>
#include <cstdint>

namespace exchange::engine {

// Power-of-2 × sub-bucket histogram for TSC-cycle latency values.
//
// The 64 MSB buckets are each split into SUB_BUCKETS=8 sub-buckets by
// examining the 3 bits below the MSB, giving ~13% relative resolution
// instead of the previous 2× (100%) per bucket.
//
// TOTAL_BUCKETS = 64 × 8 = 512.
// Memory footprint: 512 × 8 + 8 = 4104 bytes.
// Per-sample cost : 1 __builtin_clzll + 2 arithmetic ops (~3-4 cycles).
// Percentile cost : O(512) prefix-sum scan, done once per print interval.
struct LatencyHistogram {
    static constexpr int LOG2_BUCKETS = 64;
    static constexpr int SUB_BUCKETS = 8;
    static constexpr int TOTAL_BUCKETS = LOG2_BUCKETS * SUB_BUCKETS; // 512
    static constexpr int MSB_MAX = LOG2_BUCKETS - 1;                 // 63
    static constexpr int SUB_SHIFT = 3;                              // log2(SUB_BUCKETS)

    // Keep BUCKET_COUNT as an alias so existing code referencing it still compiles.
    static constexpr int BUCKET_COUNT = TOTAL_BUCKETS;

    std::array<uint64_t, TOTAL_BUCKETS> buckets{};
    uint64_t sample_count{0};

    [[nodiscard]] static auto bucket_of(uint64_t lat) noexcept -> int {
        if (lat == 0) {
            return 0;
        }
        int msb = MSB_MAX - __builtin_clzll(lat);
        int sub = (msb >= SUB_SHIFT)
                      ? static_cast<int>((lat >> (msb - SUB_SHIFT)) & (SUB_BUCKETS - 1))
                      : 0;
        int idx = msb * SUB_BUCKETS + sub;
        return (idx < TOTAL_BUCKETS) ? idx : TOTAL_BUCKETS - 1;
    }

    void record(uint64_t lat) noexcept {
        ++buckets[static_cast<std::size_t>(bucket_of(lat))];
        ++sample_count;
    }

    // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
    [[nodiscard]] auto percentile_ns(double pct, double ns_per_cycle) const noexcept -> double {
        if (sample_count == 0) {
            return 0.0;
        }
        auto target = static_cast<uint64_t>(static_cast<double>(sample_count) * (pct / 100.0));
        uint64_t cumulative = 0;
        for (int i = 0; i < TOTAL_BUCKETS; ++i) {
            cumulative += buckets[static_cast<std::size_t>(i)];
            if (cumulative > target) {
                int msb = i / SUB_BUCKETS;
                int sub = i % SUB_BUCKETS;
                uint64_t lower =
                    (msb == 0) ? static_cast<uint64_t>(sub)
                    : (msb < SUB_SHIFT)
                        ? (uint64_t{1} << msb)
                        : (uint64_t{1} << msb) + (static_cast<uint64_t>(sub) << (msb - SUB_SHIFT));
                return static_cast<double>(lower) * ns_per_cycle;
            }
        }
        // NOLINTNEXTLINE(readability-magic-numbers)
        return static_cast<double>(uint64_t{1} << MSB_MAX) * ns_per_cycle;
    }

    void reset() noexcept {
        buckets.fill(0);
        sample_count = 0;
    }
};

} // namespace exchange::engine
