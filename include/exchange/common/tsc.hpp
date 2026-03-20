#pragma once

#include <cstdint>
#include <ctime>

// Platform-specific TSC read and CPU pause
#if defined(__x86_64__) || defined(_M_X64)
#include <x86intrin.h>
inline auto rdtsc() -> uint64_t { return __rdtsc(); }
#define CPU_PAUSE() _mm_pause()
#elif defined(__aarch64__)
inline auto rdtsc() -> uint64_t {
    uint64_t val;
    __asm__ volatile("mrs %0, cntvct_el0" : "=r"(val));
    return val;
}
#define CPU_PAUSE() __asm__ volatile("yield" ::: "memory")
#else
#include <chrono>
inline auto rdtsc() -> uint64_t {
    return static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count());
}
#define CPU_PAUSE()
#endif

namespace exchange::common {

static constexpr uint64_t NS_PER_MS = 1'000'000ULL;
static constexpr uint64_t NS_PER_SEC = 1'000'000'000ULL;

// Calibrates TSC frequency by comparing TSC ticks to CLOCK_MONOTONIC over a
// short sleep. Returns nanoseconds per TSC cycle.
// Call once at startup — safe to call from any thread.
inline auto calibrate_tsc_ns_per_cycle(uint64_t calibration_ms = 50) -> double {
    struct timespec ts1 {};
    struct timespec ts2 {};
    clock_gettime(CLOCK_MONOTONIC, &ts1);
    uint64_t tsc1 = rdtsc();

    struct timespec sleep_ts {
        0, static_cast<long>(calibration_ms *NS_PER_MS)
    };
    nanosleep(&sleep_ts, nullptr);

    uint64_t tsc2 = rdtsc();
    clock_gettime(CLOCK_MONOTONIC, &ts2);

    uint64_t elapsed_ns =
        (static_cast<uint64_t>(ts2.tv_sec) - static_cast<uint64_t>(ts1.tv_sec)) * NS_PER_SEC +
        (static_cast<uint64_t>(ts2.tv_nsec) - static_cast<uint64_t>(ts1.tv_nsec));
    uint64_t elapsed_tsc = tsc2 - tsc1;

    if (elapsed_tsc == 0) {
        return 1.0; // Fallback — should not happen on real hardware
    }
    return static_cast<double>(elapsed_ns) / static_cast<double>(elapsed_tsc);
}

} // namespace exchange::common
