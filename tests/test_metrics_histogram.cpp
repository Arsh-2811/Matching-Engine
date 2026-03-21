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
    // Bucket 0 covers [2^0, 2^1) = [1, 2)
    EXPECT_EQ(LatencyHistogram::bucket_of(1), 0);
}

TEST(LatencyHistogramBucketTest, TwoMapsToSecondBucket) {
    // Bucket 1 covers [2^1, 2^2) = [2, 4)
    EXPECT_EQ(LatencyHistogram::bucket_of(2), 1);
    EXPECT_EQ(LatencyHistogram::bucket_of(3), 1);
}

TEST(LatencyHistogramBucketTest, PowersOfTwoAreLowerBoundsOfTheirBucket) {
    // 2^i is always the lower bound of bucket i.
    for (int i = 1; i < LatencyHistogram::BUCKET_COUNT; ++i) {
        uint64_t lower = uint64_t{1} << i;
        EXPECT_EQ(LatencyHistogram::bucket_of(lower), i)
            << "Failed for i=" << i << " lower=" << lower;
    }
}

TEST(LatencyHistogramBucketTest, OneBelowPowerOfTwoIsInPreviousBucket) {
    // 2^i - 1 is the last element of bucket i-1.
    for (int i = 2; i < LatencyHistogram::BUCKET_COUNT; ++i) {
        uint64_t upper_minus_one = (uint64_t{1} << i) - 1U;
        EXPECT_EQ(LatencyHistogram::bucket_of(upper_minus_one), i - 1) << "Failed for i=" << i;
    }
}

TEST(LatencyHistogramBucketTest, LargeValueMapsToCorrectBucket) {
    // 1 million TSC cycles: floor(log2(1000000)) = 19
    EXPECT_EQ(LatencyHistogram::bucket_of(1'000'000ULL), 19);
    // 1 billion TSC cycles: floor(log2(10^9)) = 29
    EXPECT_EQ(LatencyHistogram::bucket_of(1'000'000'000ULL), 29);
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
    hist.record(1024); // bucket 10: [1024, 2048)
    EXPECT_EQ(hist.buckets[10], 1U);
    hist.record(2048); // bucket 11: [2048, 4096)
    EXPECT_EQ(hist.buckets[11], 1U);
    hist.record(2049); // bucket 11 again
    EXPECT_EQ(hist.buckets[11], 2U);
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
    for (int i = 0; i < LatencyHistogram::BUCKET_COUNT; ++i) {
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
    // Put all samples in bucket 20 (lower bound = 2^20 = 1048576 cycles).
    constexpr int TARGET_BUCKET = 20;
    for (int i = 0; i < 100; ++i) {
        hist.record(uint64_t{1} << TARGET_BUCKET); // exactly 2^20
    }
    // Every percentile should land in bucket 20.
    const double ns_per_cycle = 1.0;
    const double expected_ns = static_cast<double>(uint64_t{1} << TARGET_BUCKET);
    EXPECT_DOUBLE_EQ(hist.percentile_ns(0.0, ns_per_cycle), expected_ns);
    EXPECT_DOUBLE_EQ(hist.percentile_ns(50.0, ns_per_cycle), expected_ns);
    EXPECT_DOUBLE_EQ(hist.percentile_ns(99.0, ns_per_cycle), expected_ns);
    EXPECT_DOUBLE_EQ(hist.percentile_ns(99.9, ns_per_cycle), expected_ns);
}

TEST(LatencyHistogramPercentileTest, P50SplitsTwoEqualBuckets) {
    LatencyHistogram hist;
    // 50 samples in bucket 10 (lat=1024), 50 samples in bucket 20 (lat=1048576).
    for (int i = 0; i < 50; ++i) {
        hist.record(1024ULL); // bucket 10
    }
    for (int i = 0; i < 50; ++i) {
        hist.record(1ULL << 20); // bucket 20
    }
    // With 100 samples, target for p50 = floor(100 * 0.50) = 50.
    // cumulative after bucket 10 = 50, which is NOT > 50, so we continue.
    // cumulative after bucket 20 = 100, which IS > 50, so p50 lands in bucket 20.
    // (our implementation finds the first bucket where cumulative > target)
    const double p50 = hist.percentile_ns(50.0, 1.0);
    EXPECT_DOUBLE_EQ(p50, static_cast<double>(uint64_t{1} << 20));
}

TEST(LatencyHistogramPercentileTest, P99SeparatesOneOutlierFromNinetyNine) {
    LatencyHistogram hist;
    // 99 samples in bucket 10 (low latency), 1 in bucket 30 (high latency).
    for (int i = 0; i < 99; ++i) {
        hist.record(1024ULL); // bucket 10
    }
    hist.record(uint64_t{1} << 30); // bucket 30 — the outlier

    const double ns_per_cycle = 0.5; // arbitrary scale
    // p99 target = floor(100 * 0.99) = 99.
    // cumulative after bucket 10 = 99, which is NOT > 99.
    // cumulative after bucket 30 = 100, which IS > 99 → p99 = bucket 30 lower bound.
    double expected_p99 = static_cast<double>(uint64_t{1} << 30) * ns_per_cycle;
    EXPECT_DOUBLE_EQ(hist.percentile_ns(99.0, ns_per_cycle), expected_p99);

    // p50 target = floor(100 * 0.50) = 50.
    // cumulative after bucket 10 = 99, which IS > 50 → p50 = bucket 10 lower bound.
    double expected_p50 = static_cast<double>(uint64_t{1} << 10) * ns_per_cycle;
    EXPECT_DOUBLE_EQ(hist.percentile_ns(50.0, ns_per_cycle), expected_p50);
}

TEST(LatencyHistogramPercentileTest, NsPerCycleScalesResult) {
    LatencyHistogram hist;
    hist.record(1024ULL); // bucket 10

    // With ns_per_cycle = 0.4167 (2.4 GHz), 2^10 cycles ≈ 426.7 ns.
    const double ns_per_cycle = 0.4167;
    double result = hist.percentile_ns(50.0, ns_per_cycle);
    EXPECT_NEAR(result, 1024.0 * ns_per_cycle, 1e-9);
}

TEST(LatencyHistogramPercentileTest, UniformDistributionPercentilesMonotonicallyIncrease) {
    LatencyHistogram hist;
    // One sample per bucket (0..63).
    for (int i = 0; i < LatencyHistogram::BUCKET_COUNT; ++i) {
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
