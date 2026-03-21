#pragma once

#include <array>
#include <cstdint>

namespace exchange::engine {

// Power-of-2 bucket histogram for TSC-cycle latency values.
//
// Bucket i covers the half-open interval [2^i, 2^(i+1)) TSC cycles.
// 64 buckets span from 1 cycle (~0.4 ns at 2.4 GHz) to 2^63 cycles,
// comfortably covering every real measurement.
//
// Per-sample cost  : 1 __builtin_clzll + 1 array increment  (~2-3 cycles).
// Percentile cost  : O(64) prefix-sum scan, done once per print interval.
// Memory footprint : 64 × 8 + 8 = 520 bytes.
//
// This struct is plain data (no heap, no virtual dispatch) and is designed
// to be embedded directly inside MetricsThread without extra synchronisation.
struct LatencyHistogram {
    static constexpr int BUCKET_COUNT = 64;
    static constexpr int MAX_BUCKET_IDX = BUCKET_COUNT - 1;

    std::array<uint64_t, BUCKET_COUNT> buckets{};
    uint64_t sample_count{0};

    // Map a TSC latency value to its bucket index [0, MAX_BUCKET_IDX].
    //
    // For lat_tsc in [2^i, 2^(i+1)):
    //   __builtin_clzll(lat_tsc) == 63 - i
    //   => bucket = 63 - __builtin_clzll(lat_tsc) == i
    //
    // lat_tsc == 0 is defined (returns 0); callers that already guard
    // lat > 0 still get correct behaviour.
    [[nodiscard]] static auto bucket_of(uint64_t lat_tsc) noexcept -> int {
        if (lat_tsc == 0) {
            return 0;
        }
        return MAX_BUCKET_IDX - __builtin_clzll(lat_tsc);
    }

    // Record a single latency sample (TSC cycles).
    void record(uint64_t lat_tsc) noexcept {
        buckets[static_cast<std::size_t>(bucket_of(lat_tsc))]++;
        ++sample_count;
    }

    // Return the approximate latency at the given percentile [0.0, 100.0].
    // ns_per_cycle is the calibrated TSC-to-nanosecond conversion factor.
    //
    // Resolution: ~2× per bucket (each bucket spans a 2× latency range).
    // Reports the lower bound of the containing bucket converted to ns.
    // Returns 0.0 when the histogram is empty.
    [[nodiscard]] auto percentile_ns(double pct, // NOLINT(bugprone-easily-swappable-parameters)
                                     double ns_per_cycle) const noexcept -> double {
        if (sample_count == 0) {
            return 0.0;
        }
        // Floor of (count × pct/100) — the 0-based index of the target sample.
        auto target = static_cast<uint64_t>(static_cast<double>(sample_count) * (pct / 100.0));
        uint64_t cumulative = 0;
        for (int i = 0; i < BUCKET_COUNT; ++i) {
            cumulative += buckets[static_cast<std::size_t>(i)];
            if (cumulative > target) {
                // Lower bound of bucket i = 2^i TSC cycles.
                return static_cast<double>(uint64_t{1} << i) * ns_per_cycle;
            }
        }
        // Unreachable when sample_count > 0, but satisfies the compiler.
        return static_cast<double>(uint64_t{1} << MAX_BUCKET_IDX) * ns_per_cycle;
    }

    // Clear all buckets and reset the sample count.
    void reset() noexcept {
        buckets.fill(0);
        sample_count = 0;
    }
};

} // namespace exchange::engine
