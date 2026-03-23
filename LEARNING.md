# Understanding the Matching Engine — A Complete Guide

This document explains every concept used in this codebase from first principles.
It is organized concept-by-concept. Each section builds on the previous one.
Every concept is tied back to the exact file and line where you can see it in action.

---

## Table of Contents

1. [Processes, Threads, and CPU Cores](#1-processes-threads-and-cpu-cores)
2. [The Memory Hierarchy](#2-the-memory-hierarchy)
3. [Cache Lines, False Sharing, and Prefetching](#3-cache-lines-false-sharing-and-prefetching)
4. [Memory Ordering and Atomics](#4-memory-ordering-and-atomics)
5. [TCP Networking from the Ground Up](#5-tcp-networking-from-the-ground-up)
6. [The SBE Binary Protocol](#6-the-sbe-binary-protocol)
7. [The Time Stamp Counter (TSC)](#7-the-time-stamp-counter-tsc)
8. [Lock-Free Data Structures](#8-lock-free-data-structures)
9. [The Memory Pool](#9-the-memory-pool)
10. [The Order Book — Price-Time Priority Matching](#10-the-order-book--price-time-priority-matching)
11. [The Matching Engine Thread](#11-the-matching-engine-thread)
12. [The Gateway Worker — epoll and Edge-Triggered I/O](#12-the-gateway-worker--epoll-and-edge-triggered-io)
13. [The Metrics Thread and Latency Histogram](#13-the-metrics-thread-and-latency-histogram)
14. [The Load Generator](#14-the-load-generator)
15. [System Startup, Shutdown, and CPU Affinity](#15-system-startup-shutdown-and-cpu-affinity)
16. [The Full Picture — One Order's Journey](#16-the-full-picture--one-orders-journey)

---

## 1. Processes, Threads, and CPU Cores

### What is a process?

When you run a program, the operating system (OS) creates a **process** for it.
A process gets its own block of memory. No other process can touch that memory directly.
It is fully isolated. If it crashes, nothing else is affected.

### What is a thread?

A **thread** is a unit of execution that lives *inside* a process.
A process starts with one thread (called the main thread). You can create more.

The key difference from a process: **all threads in the same process share the same memory**.
Thread A can read and write the same variables as Thread B.
This is powerful — but also dangerous. If both try to write the same variable at the same time, the result is unpredictable. This is called a **data race**.

### What is a CPU core?

Modern CPUs have multiple **cores**. Each core is an independent execution unit.
Your machine (i5-9300H) has 4 cores, numbered 0–3.

A core can run one thread at a time (ignoring hyperthreading for now).
The OS decides which thread runs on which core — this is called **scheduling**.
The OS can move a thread between cores. It can pause it and resume it later.
Each pause/resume is called a **context switch** and takes ~1–10 microseconds.

### Why does this matter for HFT?

Low-latency trading systems use the same pattern: dedicated threads per task with busy-waiting.
The reason is the same here. Context switches are expensive.
If you are trying to process orders in hundreds of nanoseconds, a context switch adds 10,000+ nanoseconds. That is unacceptable.

The solution is to **pin a thread to a core** (CPU affinity) so the OS never moves it.
You will see this everywhere in this codebase. More on that in Section 15.

### In this codebase

The system runs four logical threads, pinned to dedicated cores:

| Thread | Core | File |
| --- | --- | --- |
| MatchingEngine | core 1 | `include/exchange/engine/matching_engine.hpp:41` |
| GatewayWorker 1 | core 2 | `src/network/worker.cpp:141` |
| GatewayWorker 2 | core 3 | `src/network/worker.cpp:141` |
| MetricsThread | core 3 | `include/exchange/engine/metrics_thread.hpp:42` |

### Basic implementation: creating a thread in C++

```cpp
#include <thread>
#include <iostream>

void my_function(int x) {
    std::cout << "Running with x = " << x << std::endl;
}

int main() {
    // Create a thread that runs my_function with argument 5
    std::thread t(my_function, 5);

    // Wait for the thread to finish before exiting
    // (if main() exits first, the thread is killed abruptly)
    t.join();

    return 0;
}
```

`std::thread` — this is C++'s standard library class for threads.
`t.join()` — blocks the calling thread until `t` finishes. Without this, your program might exit before the thread completes.

---

## 2. The Memory Hierarchy

### The problem: RAM is slow

Your CPU can execute billions of instructions per second.
But fetching data from RAM takes ~100 nanoseconds (ns).
If the CPU had to go to RAM for every variable it needed, it would spend 99% of its time waiting.

To solve this, modern CPUs have a hierarchy of smaller, faster storage between the CPU and RAM:

```text
CPU Core
  └── Registers         (~0 ns,   ~32 bytes)
  └── L1 Cache          (~1 ns,   ~32 KB per core)
  └── L2 Cache          (~5 ns,   ~256 KB per core)
  └── L3 Cache (shared) (~20 ns,  ~6 MB shared across all cores)
  └── RAM               (~100 ns, ~16 GB)
  └── SSD               (~100,000 ns)
```

Each level is larger but slower than the one above it.

### How caches work: cache lines

The CPU does not fetch one variable at a time from RAM.
It always fetches a **cache line** — a 64-byte block of contiguous memory.

If you access variable `x` at address 1000, the CPU fetches bytes 960–1023 into L1 cache.
The next time you access `x`, it is already in L1 — 1 ns instead of 100 ns.
This is called a **cache hit**. If the data is not there, it is a **cache miss**.

### What this means for data structure design

If your data is laid out well in memory (elements close together, accessed in order), you get cache hits. The CPU loads one line and uses many elements from it.

If your data is scattered in memory (e.g., pointer-chasing through a linked list where each node is allocated separately at a random address), each node access is likely a cache miss.

This is why the order book in this project uses **slab allocation** (Section 9) instead of calling `new` for each order node individually.

### In this codebase

`include/exchange/queue/mpsc_queue.hpp:18` defines `CACHE_LINE_SIZE = 64`.
This constant is used throughout to align data structures to cache line boundaries — a technique explained in the next section.

---

## 3. Cache Lines, False Sharing, and Prefetching

### False sharing

You have 2 cores. Core 0 writes variable `A`. Core 1 writes variable `B`.
`A` and `B` are different variables, so there is no data race. Seems fine.

But if `A` and `B` happen to sit in the **same 64-byte cache line**, there is a problem.

When Core 0 writes `A`, it marks that cache line as "dirty" (modified).
Before Core 1 can write `B` (in the same line), it must first get the updated line from Core 0.
This requires a **cache coherence protocol** — an inter-core communication that takes ~50 ns.

They are not sharing data, but they are sharing the same cache line.
This is **false sharing**: a performance problem disguised as correct code.

The fix is to ensure that data written by different cores lives on different cache lines.

### `alignas` — C++ syntax explanation

`alignas(64)` is a C++ keyword that tells the compiler:
"Place this variable at a memory address that is a multiple of 64."

Since cache lines are 64 bytes and start at multiples of 64, this guarantees the variable gets its own cache line (assuming it is no larger than 64 bytes).

In `include/exchange/queue/mpsc_queue.hpp:31`:

```cpp
alignas(CACHE_LINE_SIZE) std::atomic<std::size_t> write_index_{0};
alignas(CACHE_LINE_SIZE) std::atomic<std::size_t> read_index_{0};
```

`write_index_` is written by multiple producer threads.
`read_index_` is written by the single consumer thread.
By placing each on its own cache line, writes to one do not invalidate the other's cache line on a different core.

### `struct alignas(N)` — aligning entire structs

In `include/exchange/queue/mpsc_queue.hpp:22`:

```cpp
struct alignas(CACHE_LINE_SIZE) Slot {
    T data;
    std::atomic<bool> turn{false};
};
```

`alignas` on a struct means every instance of `Slot` starts at an address that is a multiple of 64. This ensures that each slot in the ring buffer gets its own cache line, so producers writing to different slots do not cause false sharing.

In `include/exchange/queue/spmc_broadcast_queue.hpp:32`:

```cpp
struct alignas(SPMC_CACHE_LINE_SIZE) ConsumerSeq {
    std::atomic<uint64_t> value{0};
};
```

Each consumer's sequence number lives on its own cache line. Otherwise, when Consumer 0 advances its cursor (writes to `value`), it would invalidate the cache line for Consumer 1's cursor on a different core — causing false sharing between consumers.

### Prefetching

The CPU automatically fetches cache lines when it detects you are reading memory sequentially (hardware prefetcher). But when you traverse a linked list, nodes are at random addresses — the prefetcher cannot predict where the next node is.

**Software prefetching** is manually telling the CPU: "I will need this address soon, start fetching it now while I work on something else."

`__builtin_prefetch(address, rw, locality)` is a GCC/Clang compiler hint:

- `address`: the memory address to prefetch
- `rw`: 0 = will read, 1 = will write
- `locality`: 0–3, where 3 = keep in L1 cache (highest locality)

In `include/exchange/engine/order_book.hpp:83`:

```cpp
if (maker.next_idx != OrderNode::NULL_IDX) {
    __builtin_prefetch(&node_pool_.get(maker.next_idx), 0, 3);
}
uint32_t trade_qty = std::min(taker.quantity, maker.quantity);
```

While the CPU computes `trade_qty` for the current order node, the prefetch instruction is sent in parallel to load the *next* node into L1 cache. By the time the loop iterates to that next node, it is already in cache — hiding the memory latency.

### Basic implementation: demonstrating false sharing vs. padding

```cpp
#include <thread>
#include <atomic>

// BAD: both counters share a cache line
struct BadCounters {
    std::atomic<int> counter_a{0};  // these are 4 bytes each
    std::atomic<int> counter_b{0};  // they sit next to each other in memory
};

// GOOD: each counter on its own cache line
struct GoodCounters {
    alignas(64) std::atomic<int> counter_a{0};  // 64-byte aligned
    alignas(64) std::atomic<int> counter_b{0};  // also 64-byte aligned
};

// Core 0 increments counter_a, Core 1 increments counter_b.
// With BadCounters, they fight over the same cache line.
// With GoodCounters, they operate completely independently.
```

---

## 4. Memory Ordering and Atomics

### The problem: reordering

You might assume that if you write code like:

```cpp
x = 42;
flag = true;
```

then any other thread reading `flag == true` will also see `x == 42`.

This is **not guaranteed**. There are two sources of reordering:

1. **The compiler** can reorder instructions if it believes it will be faster, as long as the observable behavior of the *single-threaded* program does not change.
2. **The CPU** can execute instructions out of order for performance. Modern CPUs are superscalar and aggressively reorder loads and stores.

In a multithreaded program, this reordering can cause other threads to see states that never "logically existed."

### `std::atomic` — what it is

`std::atomic<T>` is a C++ type wrapper that guarantees two things:

1. **Atomicity**: reads and writes to the variable are indivisible. No thread will ever see a partial write.
2. **Visibility + ordering control**: you can specify how the write/read must be ordered relative to surrounding memory operations.

`std::atomic<bool>`, `std::atomic<uint64_t>` etc. — these are types from the C++ standard library (`<atomic>` header).

### Memory order modes

Every `load()` and `store()` on an atomic takes an optional `std::memory_order` argument:

| Mode | What it means |
| --- | --- |
| `memory_order_relaxed` | No ordering guarantees. Only atomicity. Cheapest. |
| `memory_order_acquire` | No loads/stores after this can be reordered to before it. Used on reads. |
| `memory_order_release` | No loads/stores before this can be reordered to after it. Used on writes. |
| `memory_order_seq_cst` | Full sequential consistency. Most expensive. Default. |

### The acquire-release pair — the most important pattern

The most common pattern is: one thread **release-stores** a value, another thread **acquire-loads** it.

This creates a **happens-before** guarantee: everything written before the release-store is visible to the thread that does the acquire-load.

Think of it like a sealed envelope:

- The writer puts data in an envelope, seals it (release-store).
- The reader opens the envelope (acquire-load). Everything that was put in before sealing is now visible.

In `include/exchange/queue/mpsc_queue.hpp:56`:

```cpp
buffer_[pos].data = item;                                    // write the data first
buffer_[pos].turn.store(true, std::memory_order_release);   // then "publish" it
```

And in `mpsc_queue.hpp:63`:

```cpp
if (!buffer_[actual_position].turn.load(std::memory_order_acquire)) {
    return false;
}
item = buffer_[actual_position].data;   // safe to read — acquire paired with release
```

The consumer does an acquire-load of `turn`. If it sees `true`, it is guaranteed to also see the `data` that was written before the release-store of `turn`. Without this pairing, the CPU could let the consumer read stale `data` even after seeing `turn == true`.

### `memory_order_relaxed` — when ordering does not matter

Used when you only need atomicity (no tearing) but not ordering.

In `include/exchange/engine/matching_engine.hpp:75`:

```cpp
outbound_drops_.fetch_add(1, std::memory_order_relaxed);
```

`outbound_drops_` is a counter that is only read at shutdown. Nobody is making decisions based on its value in real-time, so there is no need for ordering. `relaxed` is correct and cheaper.

### `fetch_add` — atomic increment

`fetch_add(n, order)` atomically adds `n` to the variable and returns the old value.
It is equivalent to `old = x; x += n; return old;` but as one indivisible operation.

In `include/exchange/queue/spmc_broadcast_queue.hpp:87`:

```cpp
int consumer_id = num_consumers_.fetch_add(1, std::memory_order_relaxed);
```

Even if two threads call `register_consumer()` simultaneously, each gets a unique `consumer_id`.

### Basic implementation: a simple flag between two threads

```cpp
#include <atomic>
#include <thread>
#include <cassert>

int shared_data = 0;
std::atomic<bool> ready{false};

void producer() {
    shared_data = 42;                           // write data
    ready.store(true, std::memory_order_release); // publish
}

void consumer() {
    while (!ready.load(std::memory_order_acquire)) {
        // spin until ready
    }
    // Guaranteed: shared_data == 42 here
    assert(shared_data == 42);
}
```

---

## 5. TCP Networking from the Ground Up

### What is a network socket?

A **socket** is a communication endpoint — like a telephone jack.
When two programs want to communicate over a network, each creates a socket and they connect to each other.

A **file descriptor** (fd) is just an integer the OS gives you to refer to an open resource (file, socket, pipe, etc.). When you say `int sock_fd = socket(...)`, you get back an integer like `5`. You pass this integer to all subsequent operations on that socket.

### TCP — what it guarantees

**TCP (Transmission Control Protocol)** is a protocol that guarantees:

1. **Ordered delivery**: bytes arrive in the order they were sent.
2. **Reliable delivery**: if a packet is lost, TCP retransmits it automatically.
3. **Connection-based**: before sending data, you establish a connection.

The tradeoff: these guarantees require acknowledgments and retransmissions, adding latency compared to UDP.

For this exchange, we use TCP because we cannot afford to lose orders or have them arrive out of order.

### How a TCP server is set up — step by step

The code for this is in `src/network/worker.cpp:141`. Here is what each step does:

#### Step 1: Create a socket

```cpp
listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
```

- `AF_INET`: use IPv4 addressing
- `SOCK_STREAM`: use a stream (TCP). The alternative `SOCK_DGRAM` would be UDP.
- Returns a file descriptor integer.

#### Step 2: Bind to an address and port

```cpp
struct sockaddr_in addr {};
addr.sin_family = AF_INET;
addr.sin_port = htons(9000);      // port 9000
addr.sin_addr.s_addr = INADDR_ANY; // accept connections on any network interface

bind(listen_fd_, (struct sockaddr*)&addr, sizeof(addr));
```

- `htons()` — "host to network short". TCP uses **big-endian** byte order; x86 CPUs use **little-endian**. `htons` converts a 16-bit port number to the correct byte order for the network.
- `INADDR_ANY` — listen on all network interfaces (localhost, ethernet, wifi, etc.)
- `bind()` associates the socket with a specific port. Without this, clients don't know where to connect.

#### Step 3: Start listening

```cpp
listen(listen_fd_, SOMAXCONN);
```

Tells the OS: "Start accepting incoming connection requests on this socket."
`SOMAXCONN` is the maximum number of pending connections to queue.

#### Step 4: Accept a connection

```cpp
int client_fd = accept(listen_fd_, (struct sockaddr*)&client_addr, &client_len);
```

When a client connects, `accept()` returns a *new* file descriptor for that specific client.
`listen_fd_` is for accepting new connections; `client_fd` is for talking to this specific client.

#### Step 5: Send and receive data

```cpp
recv(client_fd, buffer, buffer_size, 0);  // read bytes from client
send(client_fd, data, data_size, 0);      // write bytes to client
```

### The client side (load generator)

In `tests/load_generator.cpp:99`:

```cpp
int sock = socket(AF_INET, SOCK_STREAM, 0);   // create socket

sockaddr_in serv_addr{};
serv_addr.sin_family = AF_INET;
serv_addr.sin_port = htons(9000);
inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr);  // connect to localhost

connect(sock, (struct sockaddr*)&serv_addr, sizeof(serv_addr));

```text
`inet_pton()` — "presentation to network". Converts the human-readable string "127.0.0.1" into a binary IP address.

### TCP_NODELAY — disabling Nagle's algorithm

In `src/network/worker.cpp:30`:

```cpp
setsockopt(sock_fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));
```

By default, TCP uses **Nagle's algorithm**: it buffers small outgoing messages and waits briefly to see if more data arrives, so it can batch them into one packet. This improves throughput for many small writes but adds latency.

For an HFT system, we want every message sent immediately. `TCP_NODELAY` disables Nagle's algorithm.

### Non-blocking sockets

Normally, `recv()` **blocks** — the thread sits and waits until data arrives.
In a server handling many clients, you cannot block on one client; you need to handle all of them.

The solution is **non-blocking** mode:

```cpp
// src/network/worker.cpp:39
int flags = fcntl(sock_fd, F_GETFL, 0);
fcntl(sock_fd, F_SETFL, flags | O_NONBLOCK);
```

`fcntl` — "file control", a Linux system call for managing file descriptor properties.
`O_NONBLOCK` — sets the socket to non-blocking mode.

Now `recv()` returns immediately:

- If data is available: returns the data.
- If no data: returns `-1` and sets `errno = EAGAIN` (meaning "try again later").

This is where `epoll` comes in.

### epoll — efficient I/O event notification

When you have many clients (say 100), you cannot call `recv()` on each in a loop — most will return `EAGAIN` and you waste CPU checking idle connections.

**epoll** is a Linux kernel mechanism: you register file descriptors with it, then call `epoll_wait()`. The kernel notifies you only when a descriptor *actually has data ready* to read.

In `src/network/worker.cpp:163`:

```cpp
epoll_fd_ = epoll_create1(0);          // create an epoll instance

struct epoll_event epev;
epev.events = EPOLLIN | EPOLLET;       // notify on: data available (IN), edge-triggered (ET)
epev.data.fd = listen_fd_;
epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, listen_fd_, &epev);  // register listen_fd with epoll
```

Then the event loop:

```cpp
// src/network/worker.cpp:171
struct epoll_event events[128];
while (running) {
    int nev = epoll_wait(epoll_fd_, events, 128, 0);  // 0 = don't block, return immediately
    for (int i = 0; i < nev; i++) {
        if (events[i].data.fd == listen_fd_) {
            accept_new_clients();              // new connection request
        } else {
            handle_client_data(events[i].data.fd);  // existing client sent data
        }
    }
}
```

### Edge-triggered vs. level-triggered

`EPOLLET` means **edge-triggered**: epoll notifies you *once* when new data arrives.
If you don't read all the data in that notification, you won't get another notification.
This is why in `handle_client_data`, the code loops with `recv()` until it gets `EAGAIN`:

```cpp
// src/network/worker.cpp:77
while (true) {
    ssize_t bytes_read = recv(sock_fd, buffer + write_idx, available, 0);
    if (bytes_read < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            break;  // no more data right now — stop
        }
        // real error
    }
    // process data...
}
```

The alternative is **level-triggered** (default, no `EPOLLET`): epoll keeps notifying you as long as data is available. Easier to use but slightly less efficient for HFT.

### Basic implementation: minimal TCP echo server

```cpp
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>

int main() {
    // 1. Create socket
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);

    // 2. Bind to port 8080
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(8080);
    addr.sin_addr.s_addr = INADDR_ANY;
    bind(listen_fd, (sockaddr*)&addr, sizeof(addr));

    // 3. Listen
    listen(listen_fd, 10);

    // 4. Accept one client
    int client_fd = accept(listen_fd, nullptr, nullptr);

    // 5. Read data and echo it back
    char buf[1024];
    int n = recv(client_fd, buf, sizeof(buf), 0);
    send(client_fd, buf, n, 0);

    close(client_fd);
    close(listen_fd);
    return 0;
}
```

---

## 6. The SBE Binary Protocol

### Why not send text (like JSON or HTTP)?

Text is human-readable but inefficient for machines:

- `{"order_id":12345,"price":1000,"quantity":10}` = 45 bytes.
- A binary encoding of the same data = 20 bytes.
- Parsing text requires character-by-character scanning. Binary is a direct memory copy.

For an exchange sending millions of orders per second, text is not viable.

### What SBE means

**SBE (Simple Binary Encoding)** is a binary protocol where messages are just raw structs laid out in memory. No parsing needed — you cast the raw bytes directly to a struct pointer.

In `include/exchange/network/protocol.hpp` (used in `src/network/worker.cpp:113`):

```cpp
auto *header = reinterpret_cast<MsgHeader *>(&session.buffer[session.read_idx]);
auto *sbe_order = reinterpret_cast<SbeOrder *>(&session.buffer[session.read_idx + sizeof(MsgHeader)]);
```

`reinterpret_cast<T*>` is a C++ cast that says: "treat this raw memory as a pointer to type T." No parsing — you are just reinterpreting bytes that are already in the right layout.

### `reinterpret_cast` — C++ syntax explanation

C++ has four cast operators:

- `static_cast<T>`: safe conversions between compatible types (int → double, etc.)
- `reinterpret_cast<T*>`: treat raw memory as a different type. No conversion. The bytes are unchanged.
- `const_cast<T>`: add/remove const. Rarely used.
- `dynamic_cast<T>`: runtime-checked downcast for polymorphic types.

`reinterpret_cast` is appropriate here because the network buffer contains bytes that were literally written as a `SbeOrder` struct on the client side. The bytes are already in the right layout.

### The message format

Each message consists of:

1. A fixed-size **header** that tells you the message type and total size.
2. The message **body** — the actual order data.

In `src/network/worker.cpp:112`:

```cpp
while (session.pending_bytes() >= sizeof(MsgHeader)) {
    auto *header = reinterpret_cast<MsgHeader*>(...);
    if (session.pending_bytes() < header->msg_size) {
        break;  // incomplete message — wait for more data
    }
    // process complete message
}
```

This is necessary because TCP is a **byte stream**, not a message stream. TCP gives you bytes; it does not know or care that you think of those bytes as structured messages. A single `recv()` call might give you half a message, or two messages at once. You must buffer bytes and parse out complete messages yourself.

### BATCH_SIZE — why 64 orders per send()

In `include/exchange/common/config.hpp:31`:

```cpp
constexpr int BATCH_SIZE = 64;
```

Each TCP `send()` call has overhead (syscall cost, network packet header overhead).
The load generator batches 64 orders into a single `send()` call.

A single order is ~20 bytes. 64 orders = 1280 bytes — fits in one or two network packets.
Below 64: too many syscalls, latency wasted on overhead.
Above 64: latency increases because early orders in the batch wait for the batch to fill up.
64 is the empirical sweet spot found by the parameter sweep.

---

## 7. The Time Stamp Counter (TSC)

### What is the TSC?

Every modern x86 CPU has a built-in counter called the **Time Stamp Counter (TSC)**.
It is a 64-bit register that increments at a constant rate tied to the CPU's clock frequency.
On an i5-9300H running at ~3.8 GHz base, the TSC increments roughly 3.8 billion times per second.

Reading the TSC is an extremely cheap operation — 1 instruction, ~3–5 CPU cycles.
This is much cheaper than calling the OS for the current time (`clock_gettime` costs ~15–20 ns).

### `rdtsc()` — reading the TSC

In `include/exchange/common/tsc.hpp:9`:

```cpp
inline auto rdtsc() -> uint64_t { return __rdtsc(); }
```

`__rdtsc()` is a compiler intrinsic — a function that maps directly to the x86 `RDTSC` instruction.
`inline` means: replace every call to `rdtsc()` with the body of the function at the call site. No function call overhead.
`-> uint64_t` is C++ trailing return type syntax. Equivalent to `uint64_t rdtsc()`.

### TSC for latency measurement

In `include/exchange/engine/matching_engine.hpp:63`:

```cpp
uint64_t process_tsc = rdtsc();
```

The load generator stamps each order with `timestamp_tsc = rdtsc()` at the moment of creation.
The matching engine records `process_tsc = rdtsc()` when it starts processing.
Latency = `process_tsc - entry_tsc` TSC cycles.

To convert cycles to nanoseconds, we need to know how many nanoseconds one TSC cycle represents.

### TSC calibration

In `include/exchange/common/tsc.hpp:34`:

```cpp
inline auto calibrate_tsc_ns_per_cycle(uint64_t calibration_ms = 50) -> double {
    clock_gettime(CLOCK_MONOTONIC, &ts1);  // OS wall-clock time before
    uint64_t tsc1 = rdtsc();               // TSC value before

    nanosleep(...50ms...);                  // sleep 50ms

    uint64_t tsc2 = rdtsc();               // TSC value after
    clock_gettime(CLOCK_MONOTONIC, &ts2);  // OS wall-clock time after

    uint64_t elapsed_ns = (ts2 - ts1) in nanoseconds;
    uint64_t elapsed_tsc = tsc2 - tsc1;

    return (double)elapsed_ns / (double)elapsed_tsc;  // ns per cycle
}
```

By sleeping a known duration and measuring both wall-clock nanoseconds and TSC ticks, we calculate: "how many nanoseconds does one TSC tick represent?"
For an i5-9300H: approximately 0.26 ns per cycle (1 / 3.84 GHz).

### Why not just use `clock_gettime` everywhere?

`clock_gettime` is a Linux system call — it traps into the kernel.
On modern Linux with vDSO (virtual dynamic shared object), it is partially optimized to ~15–20 ns.
But even 15 ns is 57+ CPU cycles at 3.8 GHz.

The matching engine processes one order in ~155 ns (6.45M orders/sec).
Calling `clock_gettime` on every order would add ~10% overhead just for timestamps.
`rdtsc()` costs ~1 ns — 15–20× cheaper.

### Basic implementation: measuring how long something takes

```cpp
#include <cstdint>
#include <x86intrin.h>   // for __rdtsc()
#include <cstdio>

int main() {
    uint64_t start = __rdtsc();

    // --- work to measure ---
    volatile int sum = 0;
    for (int i = 0; i < 1000; i++) sum += i;
    // ----------------------

    uint64_t end = __rdtsc();
    uint64_t cycles = end - start;

    // Assume ~0.26 ns/cycle for i5-9300H
    double ns = cycles * 0.26;
    printf("Took %llu cycles = %.1f ns\n", (unsigned long long)cycles, ns);
    return 0;
}
```

---

## 8. Lock-Free Data Structures

### Why avoid locks?

A **mutex** (mutual exclusion lock) is the standard way to protect shared data between threads.
When Thread A holds a mutex, Thread B must stop and wait.

The problems with mutexes in HFT:

1. **Blocking**: a waiting thread does nothing useful. At 10M orders/sec, you cannot afford microseconds of waiting.
2. **OS involvement**: when a thread blocks on a mutex, the OS may schedule it off the CPU. Getting it scheduled back takes microseconds.
3. **Priority inversion**: a low-priority thread holds a lock needed by a high-priority thread. The high-priority thread is blocked.

**Lock-free** data structures use atomics instead. Threads never block.
A thread may spin (loop doing nothing), but it always makes progress — there is no external entity that could cause indefinite delay.

### What MPSC and SPMC mean

- **MPSC**: Multiple Producer, Single Consumer. Many threads can push; only one thread pops.
- **SPMC**: Single Producer, Multiple Consumer. Only one thread pushes; many threads can pop.

This constraint lets us design simpler, faster structures than a general multi-producer multi-consumer queue.

### The Ring Buffer (circular buffer)

Both queues in this project are ring buffers.

Imagine an array of N slots arranged in a circle.
A **write index** points to the next slot to write into.
A **read index** points to the next slot to read from.

```text
Slots:  [0][1][2][3][4][5][6][7]   (N=8)
              ^           ^
              read_idx    write_idx

Items in queue: slots 2, 3, 4, 5
```

When `write_index == read_index`, the queue is empty.
When `write_index - read_index == N`, the queue is full.

To get the actual slot position from an index: `index & (N-1)`.
This is equivalent to `index % N` but much faster (a bitwise AND vs. a division).
This is why capacity **must be a power of 2** — the modulo trick only works for powers of 2.

In `include/exchange/queue/mpsc_queue.hpp:50`:

```cpp
std::size_t pos = idx & capacity_mask_;  // capacity_mask_ = capacity - 1
```

### The MPSC Queue — `include/exchange/queue/mpsc_queue.hpp`

**Structure:**

```text
buffer_:    [Slot][Slot][Slot]...[Slot]   (2M slots)
             each Slot has: T data + atomic<bool> turn

write_index_: (on its own cache line, written by producers)
read_index_:  (on its own cache line, written by consumer)
```

Each `Slot` has a `turn` flag (atomic bool). `turn = false` means "empty, producer can write." `turn = true` means "full, consumer can read."

**Push (producer side):**

```cpp
// mpsc_queue.hpp:44
auto push(const T &item) -> bool {
    std::size_t w = write_index_.load(std::memory_order_relaxed);
    // Check if queue is full
    if (w - read_index_.load(std::memory_order_acquire) >= capacity_mask_ + 1) {
        return false;
    }
    // Atomically claim a slot (fetch_add returns the old value)
    std::size_t idx = write_index_.fetch_add(1, std::memory_order_relaxed);
    std::size_t pos = idx & capacity_mask_;
    // Wait if this slot hasn't been consumed yet (extremely rare)
    while (buffer_[pos].turn.load(std::memory_order_acquire)) {
        QUEUE_CPU_PAUSE();
    }
    buffer_[pos].data = item;
    buffer_[pos].turn.store(true, std::memory_order_release);  // publish
    return true;
}
```

Multiple producers each call `fetch_add(1)` to claim a unique slot index atomically.
Producer A gets slot 5, Producer B gets slot 6 — no conflict.
Each then writes to its own claimed slot and sets `turn = true` to say "ready."

**Pop (consumer side):**

```cpp
// mpsc_queue.hpp:60
auto pop(T &item) -> bool {
    std::size_t current_read = read_index_.load(std::memory_order_relaxed);
    size_t actual_position = current_read & capacity_mask_;
    // Check if the slot at current read position is ready
    if (!buffer_[actual_position].turn.load(std::memory_order_acquire)) {
        return false;  // queue empty (or that slot not yet written)
    }
    item = buffer_[actual_position].data;
    buffer_[actual_position].turn.store(false, std::memory_order_relaxed);  // mark empty
    read_index_.store(current_read + 1, std::memory_order_release);
    return true;
}
```

The consumer checks `turn`. If `true`, the data is ready to read. It reads the data, resets the slot, and advances `read_index_`.

**`QUEUE_CPU_PAUSE()` — what it does**

In `mpsc_queue.hpp:10`:

```cpp
#define QUEUE_CPU_PAUSE() _mm_pause()
```

`_mm_pause()` translates to the x86 `PAUSE` instruction. When spinning in a tight loop waiting for a condition, `PAUSE` does two things:

1. Tells the CPU this is a spin-wait, letting it reduce power consumption slightly.
2. Improves performance of the **memory order buffer** — helps the CPU detect the spin loop ending faster when the condition changes.

Without `PAUSE`, tight spin loops can actually slow down the thread that is writing the variable being waited on (due to memory pipeline thrashing).

### The SPMC Broadcast Queue — `include/exchange/queue/spmc_broadcast_queue.hpp`

**Key difference from MPSC**: every consumer sees every item. It is a broadcast, not a round-robin dispatch.

**Structure:**

```text
buffer_:             [T][T][T]...[T]   (1M slots)
producer_seq_:       (on its own cache line) — next slot to write
consumer_seqs_[]:    (each on its own cache line) — per-consumer cursor
cached_min_consumer_seq_: (producer-private, same cache line as producer_seq_)
```

The producer has one sequence number `producer_seq_`. Each consumer has its own sequence number `consumer_seqs_[id]`.

A consumer is "at" position `consumer_seq`. The producer is "at" position `producer_seq`.
If `consumer_seq < producer_seq`, there are unread items. The consumer reads `buffer_[consumer_seq & mask]` and increments its own cursor.

The producer can only advance if the **slowest consumer** is not a full buffer behind:

```cpp
// spmc_broadcast_queue.hpp:104
auto push(const T &item) -> bool {
    uint64_t seq = producer_seq_.load(std::memory_order_relaxed);
    // Fast path: check cached min consumer seq
    if (seq - cached_min_consumer_seq_ >= capacity_) {
        cached_min_consumer_seq_ = get_min_consumer_seq();  // expensive: acquire-load all consumers
        if (seq - cached_min_consumer_seq_ >= capacity_) {
            return false;  // queue full
        }
    }
    buffer_[seq & capacity_mask_] = item;
    producer_seq_.store(seq + 1, std::memory_order_release);  // publish
    return true;
}
```

**The cached min consumer seq optimization:**

`get_min_consumer_seq()` loops over all consumers and does an `acquire` load on each — that means crossing cache lines on other cores, costing ~50 ns each.

If we called this on every `push()`, the overhead would be enormous at 10M+ orders/sec.

Instead, we cache the last known minimum. We only call the expensive `get_min_consumer_seq()` when the cached value suggests we might be full. In the common case (consumers keeping up), the cached value is valid and we skip the expensive check entirely.

---

## 9. The Memory Pool

### Why not use `new` / `delete` (dynamic allocation)?

When you write `new OrderNode()` in C++, the OS must:

1. Find a free block of memory of the right size (search through the heap).
2. Return a pointer to it.

This search is non-deterministic — sometimes fast, sometimes slow depending on heap fragmentation.
It can also cause **cache misses** because the returned block could be at any address.

In HFT, non-deterministic latency is unacceptable. We want every allocation to take the same time.

**Object pooling** (which you already understand from GS) solves this.
Pre-allocate all objects upfront. `allocate()` is just removing one from a free list — O(1) and deterministic.

### The MemoryPool — `include/exchange/engine/memory_pool.hpp`

```cpp
template <typename T> class MemoryPool {
    std::vector<T> buffer_;   // contiguous slab of all objects
    uint32_t free_head_;      // index of the first free object
    std::size_t available_;
```

`template <typename T>` — C++ generics. `T` is a placeholder for the type being stored. When you write `MemoryPool<OrderNode>`, the compiler replaces `T` with `OrderNode` everywhere.

`std::vector<T>` — a dynamically-sized array. Internally just a contiguous block of memory.

**Initialization** (`memory_pool.hpp:22`):

```cpp
for (std::size_t i = capacity; i > 0; --i) {
    buffer_[i - 1].next_idx = (i < capacity) ? static_cast<uint32_t>(i) : T::NULL_IDX;
}
free_head_ = 0;
```

This builds an **intrusive free list**: the `next_idx` field of each `OrderNode` is repurposed to point to the next free node. Node 0 → Node 1 → Node 2 → ... → NULL. No extra memory needed for the free list.

**Allocate** (`memory_pool.hpp:35`):

```cpp
auto allocate() -> T* {
    if (free_head_ == T::NULL_IDX) return nullptr;  // pool exhausted
    T* obj = &buffer_[free_head_];
    free_head_ = obj->next_idx;   // advance free list head
    obj->next_idx = T::NULL_IDX;
    --available_;
    return obj;
}
```

**Deallocate** (`memory_pool.hpp:46`):

```cpp
void deallocate(T* obj) {
    obj->next_idx = free_head_;   // push to front of free list
    free_head_ = index_of(obj);
    ++available_;
}
```

**`index_of`** (`memory_pool.hpp:62`):

```cpp
auto index_of(const T* ptr) const -> uint32_t {
    return static_cast<uint32_t>(ptr - buffer_.data());
}
```

`buffer_.data()` returns a raw pointer to the first element of the vector.
Subtracting two pointers gives you the number of elements between them — i.e., the index.

### Why uint32_t instead of pointers for node links

In `include/exchange/engine/order_book_types.hpp:14`:

```cpp
struct OrderNode {
    // ...
    uint32_t prev_idx{NULL_IDX};
    uint32_t next_idx{NULL_IDX};
```

A pointer on a 64-bit system is 8 bytes. `uint32_t` is 4 bytes.
Using indices instead of pointers halves the size of the link fields.
`OrderNode` is 32 bytes — exactly 2 per 64-byte cache line (vs. 1 previously).
More nodes per cache line = more cache hits when traversing the linked list during matching.

---

## 10. The Order Book — Price-Time Priority Matching

### What is an order book?

An **order book** is a data structure that holds all outstanding (resting) buy and sell orders.
Each order specifies: price, quantity, side (buy or sell).

- **Bids**: buy orders, sorted by price descending (highest bid first).
- **Asks**: sell orders, sorted by price ascending (lowest ask first).

**Price-time priority** means:

1. **Price**: better prices are matched first. For bids, higher price = better. For asks, lower price = better.
2. **Time**: if two orders have the same price, the one that arrived first gets filled first.

A **trade** happens when a new incoming order's price crosses the spread:

- A new **buy** at price 1002, and there are resting **sells** at 1000, 1001, 1002 → fill against the cheapest asks first.
- A new **sell** at price 998, and there are resting **buys** at 1002, 1001, 1000, 999 → fill against the highest bids first.

### Data structures used

**PriceLevelIndex** (`include/exchange/engine/price_level_index.hpp`):

A flat array indexed by price. For prices 1 to 10,000 with tick size 1, this is an array of 10,000 `PriceLevel` elements. Looking up "what is at price 1001?" is O(1): `array[1001 - min_price]`.

**PriceLevel** (`include/exchange/engine/order_book_types.hpp:37`):

```cpp
struct PriceLevel {
    uint64_t price{0};
    uint32_t total_quantity{0};
    uint32_t head_idx{OrderNode::NULL_IDX};  // first order in the FIFO queue
    uint32_t tail_idx{OrderNode::NULL_IDX};  // last order in the FIFO queue
};
```

Each `PriceLevel` has a doubly-linked list of `OrderNode`s in arrival order (time priority). New orders are appended to the tail. Matching consumes from the head.

**OrderLookup** (`include/exchange/engine/order_lookup.hpp`):

A flat array indexed by `order_id`. Maps order_id → pool slot index. Allows O(1) cancellation: given an order_id, find its node immediately without searching the book.

### Order types

From `include/exchange/engine/types.hpp:10`:

```cpp
enum class OrderType : uint8_t { NEW, CANCEL, MODIFY, IOC, FOK };
```

- **NEW**: place a new order. Try to match; rest remainder.
- **CANCEL**: remove a resting order by ID.
- **MODIFY**: cancel the existing order, then place a new one with updated price/quantity.
- **IOC (Immediate or Cancel)**: match as much as possible immediately; cancel any unfilled remainder. Never rests.
- **FOK (Fill or Kill)**: must be completely filled immediately or cancelled entirely. Never partially filled.

### The matching algorithm — `match_buy` (`order_book.hpp:67`)

When a buy order arrives, it sweeps through ask price levels starting from the lowest ask:

```cpp
for (uint64_t price = lowest_ask_; price <= taker.price; price += tick_size_) {
    PriceLevel *level = asks_.get_level(price);
    // ... skip empty levels ...

    uint32_t maker_idx = level->head_idx;
    while (maker_idx != OrderNode::NULL_IDX && taker.quantity > 0) {
        OrderNode &maker = node_pool_.get(maker_idx);
        // Prefetch next node
        if (maker.next_idx != OrderNode::NULL_IDX) {
            __builtin_prefetch(&node_pool_.get(maker.next_idx), 0, 3);
        }
        uint32_t trade_qty = std::min(taker.quantity, maker.quantity);
        taker.quantity -= trade_qty;
        maker.quantity -= trade_qty;
        // If maker is fully filled, remove it from the book
        if (maker.quantity == 0) {
            // fast-path unlink from head of list
            level->head_idx = maker.next_idx;
            order_lookup_.remove_order(maker.order_id);
            maker.reset();
            node_pool_.deallocate(&maker);
            maker_idx = next_idx;
        } else {
            break;  // maker partially filled, stop
        }
    }
}
```

The outer loop sweeps price levels. The inner loop drains orders at each level.
`lowest_ask_` is maintained as a cached value — no need to scan from price 1 every time.

### FOK check — `on_new_order` (`order_book.hpp:184`)

Before touching the book for a FOK order, we check if enough liquidity exists:

```cpp
if (order.type == OrderType::FOK) {
    uint32_t avail = 0;
    for (uint64_t p = lowest_ask_; p <= order.price && avail < order.quantity; p += tick_size_) {
        PriceLevel *lvl = asks_.get_level(p);
        if (lvl != nullptr) avail += lvl->total_quantity;
    }
    if (avail < order.quantity) return 0;  // cancel without touching book
}
```

### Resting an order (when no full match)

If the incoming order has remaining quantity after matching (or no match at all), and it is not IOC/FOK:

```cpp
// order_book.hpp:230
OrderNode *node = node_pool_.allocate();
node->order_id = order.order_id;
node->quantity = order.quantity;
node->side = order.side;

PriceLevel *level = bids_.get_level(order.price);
append_to_level(level, node);  // add to tail of price level's list
highest_bid_ = std::max(highest_bid_, order.price);  // update cached best bid
order_lookup_.add_order(order.order_id, node_pool_.index_of(node));  // register for cancellation
```

---

## 11. The Matching Engine Thread

### Role

The `MatchingEngine` owns one thread (`engine_thread_`) pinned to core 1.
Its only job: drain the MPSC inbound queue, call `process_order()` on each order, push results to the outbound SPMC queue.

It has no other responsibilities. No I/O. No syscalls. No blocking. This is the **hot path**.

### The run loop — `matching_engine.hpp:41`

```cpp
void run(int core_id) {
    // Pin to core_id
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);

    Order buf[DRAIN_BATCH];  // local array of 64 orders
    while (is_running_.load(std::memory_order_acquire)) {
        int num_drained = 0;
        // Drain up to 64 orders at once
        while (num_drained < DRAIN_BATCH && incoming_queue_.pop(buf[num_drained])) {
            ++num_drained;
        }
        if (num_drained == 0) {
            ENGINE_CPU_PAUSE();
            continue;
        }
        for (int i = 0; i < num_drained; ++i) {
            if (buf[i].order_id == 0) {
                is_running_.store(false, std::memory_order_release);
                return;  // poison pill — shut down
            }
            uint64_t process_tsc = rdtsc();
            uint32_t matched_qty = book->process_order(buf[i]);
            // Push event to outbound queue
            OutboundEvent event;
            event.entry_tsc = buf[i].timestamp_tsc;
            event.process_tsc = process_tsc;
            event.matched_qty = matched_qty;
            outbound_->push(event);
        }
    }
}
```

**cpu_set_t / pthread_setaffinity_np** — Linux API for pinning a thread to a core:

- `CPU_ZERO(&cpuset)`: clear the bitmask (no CPUs selected)
- `CPU_SET(1, &cpuset)`: mark core 1 as selected
- `pthread_setaffinity_np(...)`: tell the OS "only run this thread on the CPUs in cpuset"

**`pthread_self()`** — returns the current thread's ID (for use in pthread functions).

**Drain batch**: instead of processing one order at a time and returning, drain up to 64 orders into a local array first. This amortizes the pop overhead and improves instruction cache behavior.

**Poison pill**: when the system is shutting down (`main.cpp:66`), the main thread pushes an order with `order_id == 0`. The engine detects this and exits cleanly.

### `std::atomic<bool> is_running_` and ownership

`is_running_` controls the thread's loop. It is atomic because both the engine thread (reads it) and the main/stop thread (writes it) access it.

```cpp
// start: main thread writes
is_running_.store(true, std::memory_order_release);
engine_thread_ = std::thread(&MatchingEngine::run, this, core_id);

// stop: main thread writes
is_running_.store(false, std::memory_order_release);
engine_thread_.join();  // wait for the engine thread to exit
```

`std::thread(&MatchingEngine::run, this, core_id)`:

- `&MatchingEngine::run` — pointer to the member function `run`
- `this` — the object to call it on
- `core_id` — argument to pass

### `= delete` — C++ syntax explanation

In `matching_engine.hpp:89`:

```cpp
MatchingEngine(const MatchingEngine &) = delete;
MatchingEngine &operator=(const MatchingEngine &) = delete;
```

This explicitly disables the copy constructor and copy assignment operator.
Copying a `MatchingEngine` makes no sense — it has a running thread, references to queues, etc.
`= delete` ensures the compiler gives an error if anyone tries to copy it, rather than generating broken code silently.

---

## 12. The Gateway Worker — epoll and Edge-Triggered I/O

### Role

The `GatewayWorker` runs on cores 2 and 3 (two worker instances).
It:

1. Listens for TCP connections.
2. Receives raw bytes from clients.
3. Parses complete SBE messages from the byte stream.
4. Pushes decoded `Order` objects into the MPSC inbound queue.

### The session buffer — `src/network/worker.cpp:75`

Each connected client has a `ClientSession` with an 8MB buffer.
The buffer acts as a sliding window over the byte stream:

```text
buffer: [....consumed....][pending bytes][..........free space..........][end]
         0          read_idx        write_idx            SESSION_BUFFER_SIZE
```

`read_idx` tracks where the next unprocessed message starts.
`write_idx` tracks where to write incoming bytes.

When `available_space()` drops below a threshold, **compaction** moves the pending bytes to the front of the buffer (`worker.cpp:80`):

```cpp
std::memmove(session.buffer.data(), session.buffer.data() + session.read_idx, remaining);
session.read_idx = 0;
session.write_idx = remaining;
```

`memmove` copies bytes, handling overlapping source/destination correctly.

### Spin-wait on full queue — `worker.cpp:130`

```cpp
while (!incoming_queue_.push(internal_order)) {
    NET_CPU_PAUSE();
}
```

If the inbound queue is full (engine falling behind), the gateway spins rather than returning.
This is intentional: with edge-triggered epoll, if we returned here, the OS would not re-notify us about this client's data. We would lose the event, deadlocking that client. Spinning prevents this at the cost of CPU time.

---

## 13. The Metrics Thread and Latency Histogram

### Role

`MetricsThread` runs on core 3 (sharing with GatewayWorker 2).
It consumes from the SPMC outbound queue (consumer_id 0) and computes/prints statistics every second.

### Time-check throttling — `metrics_thread.hpp:84`

```cpp
++loop_count;
if ((loop_count & (TIME_CHECK_INTERVAL - 1U)) != 0U) {
    continue;  // skip the clock check
}
auto now = std::chrono::steady_clock::now();
```

`TIME_CHECK_INTERVAL = 65536 = 2^16`.

`(loop_count & (65536 - 1)) != 0` is true for 65535 out of every 65536 iterations.
Only on the 65536th iteration (when the low 16 bits are all zero) do we call `steady_clock::now()`.

Why? `steady_clock::now()` costs ~15–20 ns. At 7M events/sec, calling it on every event would cost 105 ms of CPU time per second — 10.5% overhead just for checking the clock. By calling it every 65536 iterations, the overhead drops to ~2 µs/sec.

### The Latency Histogram — `include/exchange/engine/latency_histogram.hpp`

Storing every individual latency sample would require enormous memory.
Instead, we use a histogram: increment a counter for the bucket that the latency falls into.

**Bucket scheme**: log2 buckets × 8 sub-buckets = 512 total buckets.

`__builtin_clzll(x)` — "count leading zeros" for a 64-bit integer. This finds the position of the most significant bit (MSB), which is the floor of log2(x). For example:

- `x = 100` → binary `1100100` → MSB at position 6 → `__builtin_clzll(100) = 57` → `msb = 63 - 57 = 6`

In `latency_histogram.hpp:34`:

```cpp
static auto bucket_of(uint64_t lat) noexcept -> int {
    int msb = MSB_MAX - __builtin_clzll(lat);  // floor(log2(lat))
    int sub = (msb >= SUB_SHIFT)
        ? static_cast<int>((lat >> (msb - SUB_SHIFT)) & (SUB_BUCKETS - 1))
        : 0;
    return msb * SUB_BUCKETS + sub;
}
```

`sub` looks at 3 bits below the MSB, giving 8 sub-buckets per power-of-2 range (~13% resolution).

**Percentile calculation** (`latency_histogram.hpp:48`):
Scan buckets from smallest to largest, accumulating counts.
When the cumulative count exceeds `sample_count * (pct/100)`, we are in the target bucket.
Reconstruct the lower bound of that bucket from its (msb, sub) index, convert to nanoseconds.

### `[[nodiscard]]` — C++ attribute

In many function declarations you see `[[nodiscard]]`:

```cpp
[[nodiscard]] auto size() const -> std::size_t;
```

This tells the compiler: "Warn if the caller ignores the return value."
If you call `queue.size()` without using the result, the compiler will emit a warning.
It prevents bugs where you forget to check a return value (like `push()` returning false when the queue is full).

---

## 14. The Load Generator

### Role

`tests/load_generator.cpp` is a separate process (not part of the exchange).
It spawns 4 client threads, each establishing a TCP connection to the exchange and sending orders at a controlled rate.

### Token Bucket Rate Limiter (`load_generator.cpp:35`)

A token bucket limits the output rate:

- Tokens accumulate over time at a rate proportional to the target rate.
- Sending a batch consumes `BATCH_SIZE` tokens.
- If tokens < BATCH_SIZE, wait.

```cpp
auto try_consume() -> bool {
    auto now = rdtsc();
    double elapsed = (double)(now - last_tsc_);
    tokens_ = min(max_tokens_, tokens_ + elapsed * tokens_per_cycle_);
    last_tsc_ = now;
    if (tokens_ >= BATCH_SIZE) {
        tokens_ -= BATCH_SIZE;
        return true;  // allowed to send
    }
    return false;  // not yet
}
```

`tokens_per_cycle_` = target_rate_per_second / tsc_frequency_hz.
Every TSC cycle, `tokens_per_cycle_` tokens are added. When enough accumulate, send a batch.

### Order ID partitioning

Each client uses a distinct range of order IDs:

- Client 0: IDs 1–250,000
- Client 1: IDs 250,001–500,000
- etc.

This prevents ID collisions in the OrderLookup table, which is indexed by order_id.

### Mid-price random walk (`config.hpp:34`)

```
constexpr uint64_t MID_PRICE_INIT = 1000;
constexpr uint64_t AGGRESSIVE_OFFSET = 2;
```

The mid price drifts randomly within [500, 1500].
Buys are placed at `mid + 2`, sells at `mid - 2`.
Since buy price > sell price (aggressive pricing), ~50% of orders match immediately.
This creates a realistic load with both resting orders and active trades.

### XorShift RNG (`load_generator.cpp:89`)

```cpp
auto tl_rand() -> uint64_t {
    tl_rng_state ^= tl_rng_state << 13;
    tl_rng_state ^= tl_rng_state >> 7;
    tl_rng_state ^= tl_rng_state << 17;
    return tl_rng_state;
}
```

XorShift is an extremely fast pseudo-random number generator — 3 operations vs. the ~100 ns of `std::rand()` which uses a global lock for thread safety.

`thread_local` — each thread has its own copy of `tl_rng_state`. No sharing, no synchronization needed.

`^=` is XOR-assign. `<<` is left bit-shift. This specific combination of shifts (13, 7, 17) produces a sequence with good statistical properties.

---

## 15. System Startup, Shutdown, and CPU Affinity

### Startup order matters — `src/main.cpp`

```cpp
// 1. Create queues
queue::MpscQueue<engine::Order> incoming_queue(QUEUE_CAPACITY);
queue::SpmcBroadcastQueue<engine::OutboundEvent> outbound_queue(OUTBOUND_QUEUE_CAPACITY);

// 2. Register consumers BEFORE starting the producer
engine::MetricsThread metrics(outbound_queue, &incoming_queue);  // registers consumer_id 0
engine::MarketDataThread market_data(outbound_queue);             // registers consumer_id 1

// 3. Create engine and gateway
engine::OrderBook book(...);
engine::MatchingEngine engine(incoming_queue, book, &outbound_queue);
network::Gateway gateway(incoming_queue, PORT, 2);

// 4. Start threads
engine.start(ENGINE_CORE_ID);    // core 1
metrics.start(METRICS_CORE_ID); // core 3
market_data.start(METRICS_CORE_ID);
gateway.start({GATEWAY_CORE_1, GATEWAY_CORE_2}); // cores 2, 3
```

Consumers must be registered before the engine starts. If a consumer registered after the engine started, it would miss all events already pushed to the queue (it starts from the current producer position, not from 0).

### Signal handling — clean shutdown

```cpp
// main.cpp:16
static std::atomic<bool> keep_running{true};
static void sig_handler(int) { keep_running.store(false, std::memory_order_relaxed); }

signal(SIGINT, sig_handler);   // Ctrl+C
signal(SIGTERM, sig_handler);  // kill command
```

`signal(SIGINT, sig_handler)` registers a function to be called when the process receives SIGINT (Ctrl+C). The handler sets `keep_running = false`, which causes the main loop to exit.

### Graceful shutdown sequence — `main.cpp:64`

Order matters:

1. Stop the gateway first — no more new orders accepted.
2. Push a poison pill (`order_id == 0`) to the engine — engine processes all remaining orders then exits.
3. Stop the engine (`engine.stop()` calls `join()`, waiting for the engine thread to finish).
4. Stop metrics.

If you stopped the engine before the gateway, in-flight orders in the queue would be dropped.
If you stopped metrics before the engine, metrics would miss the final orders.

### CPU affinity — `pthread_setaffinity_np`

In every thread's `run()` function (matching engine, gateway, metrics):

```cpp
cpu_set_t cpuset;
CPU_ZERO(&cpuset);    // clear all CPU bits
CPU_SET(core_id, &cpuset);  // set bit for core_id
pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
```

`cpu_set_t` is a bitmask where bit N = 1 means "allowed to run on core N."
After this call, the OS will only schedule this thread on the specified core.

The engine is on core 1. No other thread uses core 1. This means:

- No context switches. The engine runs uninterrupted.
- Warm L1/L2 cache. The engine's working set (order book, queue slots) stays in the core's cache between iterations.

---

## 16. The Full Picture — One Order's Journey

Let us trace a single order from the moment a client sends it to the moment it is matched.

### Step 1: Client sends an order (Load Generator)

The client thread builds a batch of 64 `SbeOrder` structs in a local buffer.
Each order is timestamped: `sbe_order.timestamp_tsc = rdtsc()`.
The entire batch is sent in one `send()` syscall:

```
[MsgHeader][SbeOrder] [MsgHeader][SbeOrder] ... (64 times)
```

The bytes arrive at the server's TCP receive buffer in the kernel.

### Step 2: Gateway receives bytes (GatewayWorker, core 2 or 3)

`epoll_wait()` returns — there is data available on this client's socket file descriptor.
`handle_client_data()` is called. `recv()` copies bytes from kernel TCP buffer into the 8MB session buffer.

The gateway parses complete messages:

```cpp
// src/network/worker.cpp:118
auto *sbe_order = reinterpret_cast<SbeOrder*>(buffer + read_idx + sizeof(MsgHeader));
engine::Order internal_order{
    .order_id = sbe_order->order_id,
    .price = sbe_order->price,
    .quantity = sbe_order->quantity,
    ...
    .timestamp_tsc = sbe_order->timestamp_tsc  // preserved from client
};
incoming_queue_.push(internal_order);  // into the MPSC queue
```

### Step 3: MPSC Queue (shared memory between gateway and engine)

The gateway (on core 2 or 3) writes to the MPSC queue.
The engine (on core 1) reads from it.
The queue's cache-line-aligned atomics ensure the data transfer is coherent across cores without a lock.

### Step 4: Engine processes the order (MatchingEngine, core 1)

The engine drains up to 64 orders from the queue.
For each order:

```cpp
uint64_t process_tsc = rdtsc();  // timestamp when processing begins
uint32_t matched_qty = book->process_order(buf[i]);
```

`process_order` dispatches to `on_new_order` / `on_cancel_order` / etc.
For a matching order, `match_buy` or `match_sell` is called — sweeping through price levels.

### Step 5: Outbound event (SPMC Queue)

After processing, the engine pushes one `OutboundEvent`:

```cpp
OutboundEvent event;
event.entry_tsc = buf[i].timestamp_tsc;  // when client sent it
event.process_tsc = process_tsc;          // when engine processed it
event.matched_qty = matched_qty;
outbound_->push(event);                  // broadcast to all consumers
```

### Step 6: Metrics consumes the event (MetricsThread, core 3)

```cpp
outbound_queue_.pop(consumer_id_, event);
uint64_t lat = event.process_tsc - event.entry_tsc;  // latency in TSC cycles
lat_hist.record(lat);
```

Every second, percentiles are printed:

```text
Throughput: 15,280,000 orders/s | P50: 180ns | P99: 950ns | P99.9: 2100ns
```

### End-to-end latency breakdown

| Stage | Typical time |
| --- | --- |
| Client `rdtsc()` to `send()` | <100 ns |
| TCP transfer (loopback) | ~1–5 µs |
| Gateway `recv()` and parse | ~200 ns |
| MPSC queue push | ~50 ns |
| Engine queue pop | ~50 ns |
| `process_order()` | ~155 ns |
| **Total (measured)** | **~0.5–1 ms at 1.4M/client** |

The large gap between the individual steps and the measured end-to-end latency at rate-limited loads is dominated by **queueing delay** — orders waiting in the inbound queue for the engine to get to them.

---

## 17. Throughput vs Latency — The Rate-Limit Sweet Spot

### The fundamental tradeoff

Throughput and latency measure different things and you cannot maximise both at once.

**Throughput** = how many orders the engine processes per second. This is the engine's raw capacity — a property of the hardware and code, not the input rate.

**Latency** = how long a single order waits from the moment the load gen builds it (`rdtsc()` in `tests/load_generator.cpp:164`) to the moment the engine processes it (`matching_engine.hpp:78`). This is dominated by **queue depth**, not engine speed.

### What happens at unlimited rate

The load gen sends as fast as the TCP socket allows. The engine processes at its ceiling (~14M/s on master). Because both ends run at similar speeds, the MpscQueue (2M slots) fills up permanently.

An order entering the back of a full queue has to wait for ~2M orders ahead of it. At 14M/s: `2,000,000 / 14,000,000 = 143ms`. That is why unlimited P50 latency is ~145ms — it is pure queueing delay, not engine overhead.

GatewayWorker at `src/network/worker.cpp:130`:
```cpp
while (!incoming_queue_.push(internal_order)) { }  // spin-waits when queue full
```

### What happens at rate-limited

The token bucket in `tests/load_generator.cpp:51-61` only releases a batch of tokens every `BATCH_SIZE / rate_per_sec` seconds. The load gen spins on `CPU_PAUSE()` between releases.

With 4 clients at 1.7M/s = 6.8M orders/s total, the engine can do 14M/s. The engine is always faster than the load gen. The queue is almost always empty. Each order arrives, gets processed immediately, and the ACK is measured almost instantly. P50 latency ≈ TCP round trip + processing = ~0.05ms.

### The latency cliff

If you keep increasing the rate, eventually the load gen matches the engine's speed. The queue starts filling. Latency jumps exponentially. This jump is called the **latency cliff**.

From the rate sweep (master branch, BATCH=64, 4 clients):

| Rate/client | Total | Throughput | P50 Latency |
|---|---|---|---|
| 1700k | 6.8M/s | 6.83M/s | 0.05ms |
| **2000k** | **8M/s** | **7.98M/s** | **5.68ms** ← cliff |
| 2500k | 10M/s | 8.13M/s | 14ms |

The **sweet spot** is the highest rate just before the cliff: maximum throughput while keeping the queue empty.

### Why the cliff moves between branches

The `feat/cancel-ack-maker-fills` branch adds overhead per order (cancel/ACK routing, maker fill notifications). The engine's effective capacity drops from ~14M/s to ~7M/s. The queue fills faster at the same input rate. Cliff shifts from 2000k (master) to 1700k/client (feature branch).

The `tests/rate_sweep_plot.py` script automates this analysis — it sweeps rate limits and draws the throughput + latency curves so you can see the cliff visually.

---

## 18. BATCH_SIZE — Syscall Cost vs Latency Floor

### Why batching exists

Every `send()` call is a **syscall** — a request to the operating system to do something on your behalf. Syscalls are expensive: the CPU must switch from user mode to kernel mode and back, costing ~1–2 microseconds each.

If BATCH_SIZE=1, each order triggers a separate `send()`. At 14M orders/s that is 14M syscalls/s × 2µs = 28 seconds of CPU time per second — impossible.

BATCH_SIZE=64 packs 64 × 32 bytes = 2048 bytes into one `send()`. One syscall to deliver 64 orders. Cost = 2µs / 64 = 31ns per order equivalent — negligible.

See `tests/load_generator.cpp:25-26`:
```cpp
static constexpr std::size_t BATCH_BYTES = SINGLE_MSG_SIZE * BATCH_SIZE;
// ... then at line 179:
send(sock, batch_buf, BATCH_BYTES, 0);  // one syscall for all BATCH_SIZE orders
```

### The latency floor

Order #1 in a batch gets its `rdtsc()` timestamp the instant it is built. But it does not get sent until all 64 orders in the batch are built and `send()` is called. Order #1 therefore waits for orders #2–#64 to be built before going on the wire — a batching latency floor of ~4µs at 14M/s.

At 1.7M/client rate-limited, the token bucket pause between batches is `64/1,700,000 ≈ 37.6µs`. The within-batch delay (~4µs) is small on top of that. So BATCH=64 has no measurable latency penalty at this rate.

In a real production system you would not batch at the network layer — you would send each order immediately and use kernel bypass (DPDK/RDMA) to eliminate syscalls entirely.

### Why BATCH=8 collapses

From the param sweep: BATCH=8 at 1.4M/client → only 3.46M/s throughput.

With BATCH=8, the load gen calls `send()` 8× more often. The load gen itself becomes the bottleneck — it cannot generate orders fast enough because it is spending too much time in syscalls.

| BATCH | Rate | Throughput | P50 Lat |
|---|---|---|---|
| 64 | 1700k | 5.63M/s | 0.05ms |
| 16 | 1400k | 4.86M/s | 0.16ms |
| 8  | 1400k | 3.46M/s | 44ms |

---

## 19. Performance Engineering Lessons from This Project

### Lesson 1: Every cross-core atomic access costs ~50ns

An `std::atomic<T>::load(std::memory_order_acquire)` that hits a cache line owned by another core forces a cache coherency protocol round trip. On this i5-9300H, that costs ~50ns.

At 10M operations/s, one cross-core read per operation = 500ms/sec of overhead = the CPU is 50% occupied doing nothing but cache coherency. This is why:
- The SPMC queue caches `min_consumer_seq_` locally (avoids cross-core read on every push in the common case)
- The load gen uses xorshift64 per-thread RNG to avoid `std::rand()` global lock

### Lesson 2: Cache line layout is a first-order concern

A struct that straddles a 64-byte cache line boundary costs 2× cache misses when accessed. With 14M nodes traversed per second, doubling the cache miss rate is a measurable throughput loss.

Rules:
- Keep hot structs at sizes that are powers of 2 (8, 16, 32, 64 bytes)
- 32 bytes = 2 nodes per cache line = optimal for linked list traversal
- Separate hot (frequently accessed) and cold (rarely accessed) fields into different structs or parallel arrays

### Lesson 3: The queue is the engine's clock

At unlimited rate, the throughput you see is the **engine's ceiling** — the fastest it can process orders. The engine never idles.

At rate-limited, throughput = input rate. The engine idles most of the time. To measure the engine's true capacity you must drive it to saturation (unlimited rate).

To measure true end-to-end latency you must keep the queue empty (rate-limit to 70–80% of capacity). An order in a full queue spends most of its measured latency waiting in line — that latency tells you about queue depth, not engine speed.

### Lesson 4: Token bucket rate limiting is TSC-based

The load gen's `TokenBucket` (`tests/load_generator.cpp:35-68`) accumulates tokens proportional to CPU cycles elapsed since last check:

```cpp
double elapsed = static_cast<double>(now - last_tsc_);
tokens_ = std::min(max_tokens_, tokens_ + elapsed * tokens_per_cycle_);
```

`tokens_per_cycle_` = `rate_hz / tsc_frequency_hz`. This is purely arithmetic — no syscalls, no sleeping. The load gen spin-loops on `CPU_PAUSE()` between batches. This gives very precise rate control but burns CPU.

---

*End of LEARNING.md — Version 2.0*

*Added in Version 2.0: Throughput vs Latency and the rate-limit sweet spot (§17), BATCH_SIZE effects on syscall cost and latency floor (§18), Performance engineering lessons (§19).*
