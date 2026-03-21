#include "exchange/engine/latency_histogram.hpp"
#include <gtest/gtest.h>

using exchange::engine::LatencyHistogram;

// ---------------------------------------------------------------------------
// bucket_of() — boundary and interior values
// ---------------------------------------------------------------------------

TEST(LatencyHistogramBucketTest, ZeroMapsToFirstBucket) {
    EXPECT_EQ(LatencyHistogram::bucket_of(0), 0);
}

TEST(LatencyHistogramBucketTest, OneMapsToFirstBucket) {
    // msb=0, sub=0 (msb<3), idx=0*8+0=0
    EXPECT_EQ(LatencyHistogram::bucket_of(1), 0);
}

TEST(LatencyHistogramBucketTest, TwoMapsToSecondBucket) {
    // msb=1, sub=0 (msb<3), idx=1*8+0=8
    EXPECT_EQ(LatencyHistogram::bucket_of(2), 8);
    EXPECT_EQ(LatencyHistogram::bucket_of(3), 8);
}

TEST(LatencyHistogramBucketTest, PowersOfTwoAreLowerBoundsOfTheirBucket) {
    // 2^i → msb=i, sub=0 (the 3 bits below MSB are 0), idx = i * SUB_BUCKETS.
    for (int i = 1; i < LatencyHistogram::LOG2_BUCKETS; ++i) {
        uint64_t lower = uint64_t{1} << i;
        int expected = i * LatencyHistogram::SUB_BUCKETS;
        EXPECT_EQ(LatencyHistogram::bucket_of(lower), expected)
            << "Failed for i=" << i << " lower=" << lower;
    }
}

TEST(LatencyHistogramBucketTest, OneBelowPowerOfTwoIsInPreviousBucket) {
    // 2^i - 1: msb = i-1. For i >= 4, the 3 bits below msb are all set → sub=7.
    // idx = (i-1)*SUB_BUCKETS + (SUB_BUCKETS-1).
    for (int i = 4; i < LatencyHistogram::LOG2_BUCKETS; ++i) {
        uint64_t upper_minus_one = (uint64_t{1} << i) - 1U;
        int expected =
            (i - 1) * LatencyHistogram::SUB_BUCKETS + (LatencyHistogram::SUB_BUCKETS - 1);
        EXPECT_EQ(LatencyHistogram::bucket_of(upper_minus_one), expected) << "Failed for i=" << i;
    }
}

TEST(LatencyHistogramBucketTest, LargeValueMapsToCorrectBucket) {
    // 1 million TSC cycles: msb=19, sub=(1000000>>16)&7 = 15&7 = 7, idx=19*8+7=159
    EXPECT_EQ(LatencyHistogram::bucket_of(1'000'000ULL), 159);
    // 1 billion TSC cycles: msb=29, sub=(1000000000>>26)&7 = 14&7 = 6, idx=29*8+6=238
    EXPECT_EQ(LatencyHistogram::bucket_of(1'000'000'000ULL), 238);
}

// ---------------------------------------------------------------------------
// record() and sample_count
// ---------------------------------------------------------------------------

TEST(LatencyHistogramRecordTest, RecordIncreasesSampleCount) {
    LatencyHistogram hist;
    EXPECT_EQ(hist.sample_count, 0U);
    hist.record(1000);
    EXPECT_EQ(hist.sample_count, 1U);
    hist.record(2000);
    hist.record(3000);
    EXPECT_EQ(hist.sample_count, 3U);
}

TEST(LatencyHistogramRecordTest, RecordIncrementsCorrectBucket) {
    LatencyHistogram hist;
    // 1024 = 2^10: msb=10, sub=0, idx=80
    hist.record(1024);
    EXPECT_EQ(hist.buckets[80], 1U);
    // 2048 = 2^11: msb=11, sub=0, idx=88
    hist.record(2048);
    EXPECT_EQ(hist.buckets[88], 1U);
    // 2049: msb=11, sub=(2049>>8)&7=8&7=0, idx=88
    hist.record(2049);
    EXPECT_EQ(hist.buckets[88], 2U);
}

// ---------------------------------------------------------------------------
// reset()
// ---------------------------------------------------------------------------

TEST(LatencyHistogramResetTest, ResetClearsAllBucketsAndCount) {
    LatencyHistogram hist;
    hist.record(100);
    hist.record(200);
    hist.record(300);

    hist.reset();

    EXPECT_EQ(hist.sample_count, 0U);
    for (int i = 0; i < LatencyHistogram::TOTAL_BUCKETS; ++i) {
        EXPECT_EQ(hist.buckets[static_cast<std::size_t>(i)], 0U)
            << "Bucket " << i << " not cleared";
    }
}

// ---------------------------------------------------------------------------
// percentile_ns() — correctness against known distributions
// ---------------------------------------------------------------------------

TEST(LatencyHistogramPercentileTest, EmptyHistogramReturnsZero) {
    LatencyHistogram hist;
    EXPECT_DOUBLE_EQ(hist.percentile_ns(50.0, 1.0), 0.0);
    EXPECT_DOUBLE_EQ(hist.percentile_ns(99.0, 1.0), 0.0);
}

TEST(LatencyHistogramPercentileTest, AllSamplesInOneBucketAllPercentilesMatch) {
    LatencyHistogram hist;
    // Put all samples at 2^20. bucket_of(2^20): msb=20, sub=0, idx=160.
    // lower bound of bucket 160: msb=20, sub=0 → 2^20.
    constexpr int TARGET_BUCKET = 20;
    for (int i = 0; i < 100; ++i) {
        hist.record(uint64_t{1} << TARGET_BUCKET); // exactly 2^20
    }
    // Every percentile should land in bucket 160 → lower bound = 2^20.
    const double ns_per_cycle = 1.0;
    const double expected_ns = static_cast<double>(uint64_t{1} << TARGET_BUCKET);
    EXPECT_DOUBLE_EQ(hist.percentile_ns(0.0, ns_per_cycle), expected_ns);
    EXPECT_DOUBLE_EQ(hist.percentile_ns(50.0, ns_per_cycle), expected_ns);
    EXPECT_DOUBLE_EQ(hist.percentile_ns(99.0, ns_per_cycle), expected_ns);
    EXPECT_DOUBLE_EQ(hist.percentile_ns(99.9, ns_per_cycle), expected_ns);
}

TEST(LatencyHistogramPercentileTest, P50SplitsTwoEqualBuckets) {
    LatencyHistogram hist;
    // 50 samples at 1024 (bucket 80), 50 samples at 2^20 (bucket 160).
    for (int i = 0; i < 50; ++i) {
        hist.record(1024ULL); // bucket 80
    }
    for (int i = 0; i < 50; ++i) {
        hist.record(1ULL << 20); // bucket 160
    }
    // With 100 samples, target for p50 = floor(100 * 0.50) = 50.
    // cumulative after bucket 80 = 50, which is NOT > 50, so we continue.
    // cumulative after bucket 160 = 100, which IS > 50, so p50 lands in bucket 160.
    // lower bound of bucket 160: msb=20, sub=0 → 2^20.
    const double p50 = hist.percentile_ns(50.0, 1.0);
    EXPECT_DOUBLE_EQ(p50, static_cast<double>(uint64_t{1} << 20));
}

TEST(LatencyHistogramPercentileTest, P99SeparatesOneOutlierFromNinetyNine) {
    LatencyHistogram hist;
    // 99 samples at 1024 (bucket 80), 1 at 2^30 (bucket 240).
    for (int i = 0; i < 99; ++i) {
        hist.record(1024ULL); // bucket 80
    }
    hist.record(uint64_t{1} << 30); // bucket 240 — the outlier

    const double ns_per_cycle = 0.5; // arbitrary scale
    // p99 target = floor(100 * 0.99) = 99.
    // cumulative after bucket 80 = 99, which is NOT > 99.
    // cumulative after bucket 240 = 100, which IS > 99.
    // lower bound of bucket 240: msb=30, sub=0 → 2^30.
    double expected_p99 = static_cast<double>(uint64_t{1} << 30) * ns_per_cycle;
    EXPECT_DOUBLE_EQ(hist.percentile_ns(99.0, ns_per_cycle), expected_p99);

    // p50 target = floor(100 * 0.50) = 50.
    // cumulative after bucket 80 = 99, which IS > 50 → p50 = lower bound of bucket 80.
    // lower bound of bucket 80: msb=10, sub=0 → 2^10 = 1024.
    double expected_p50 = static_cast<double>(uint64_t{1} << 10) * ns_per_cycle;
    EXPECT_DOUBLE_EQ(hist.percentile_ns(50.0, ns_per_cycle), expected_p50);
}

TEST(LatencyHistogramPercentileTest, NsPerCycleScalesResult) {
    LatencyHistogram hist;
    // 1024 → bucket 80, lower bound = 2^10 = 1024.
    hist.record(1024ULL);

    // With ns_per_cycle = 0.4167 (2.4 GHz), 2^10 cycles ≈ 426.7 ns.
    const double ns_per_cycle = 0.4167;
    double result = hist.percentile_ns(50.0, ns_per_cycle);
    EXPECT_NEAR(result, 1024.0 * ns_per_cycle, 1e-9);
}

TEST(LatencyHistogramPercentileTest, UniformDistributionPercentilesMonotonicallyIncrease) {
    LatencyHistogram hist;
    // One sample per log2-bucket (0..63), at 2^i each.
    for (int i = 0; i < LatencyHistogram::LOG2_BUCKETS; ++i) {
        uint64_t val = (i == 0) ? 0 : (uint64_t{1} << i);
        hist.record(val);
    }
    // Percentiles must be non-decreasing.
    double prev = hist.percentile_ns(0.0, 1.0);
    for (double pct = 10.0; pct <= 99.9; pct += 10.0) {
        double cur = hist.percentile_ns(pct, 1.0);
        EXPECT_GE(cur, prev) << "Non-monotonic at pct=" << pct;
        prev = cur;
    }
}
