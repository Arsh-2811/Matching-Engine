# Matching Engine

Minimal C++20 project setup with **CMake**, **GoogleTest**, **clang-format**, and **clang-tidy**.

## Build

```bash
cmake -S . -B build -G Ninja
cmake --build build
```

## Run Tests

```bash
ctest --test-dir build
```

## Clean Build

```bash
rm -rf build
cmake -S . -B build -G Ninja
cmake --build build
```

## Release Build

```bash
cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j$(nproc)
```

## Running Benchmark Tests

```bash
./unit_tests --gtest_filter=SystemBenchmark.ThroughputStressTest

./unit_tests --gtest_filter=MpscQueueBenchmark.Throughput
```
