# KVStore — LSM-Tree Key-Value Storage Engine in C++

> A production-style persistent key-value storage engine built from scratch in C++17,
> implementing the **LSM-Tree architecture** used by LevelDB, RocksDB, and Apache Cassandra.

---

## Benchmark Results (Intel Xeon @ 2.20GHz, 13GB RAM)

```
========================================================
  KVStore FAANG-Level Benchmark
  Methodology: warmup + multiple runs + tail latency
========================================================

[1. Bloom Filter]
  add(key)           :  4.5M  ops/sec   (0.22 µs/op)
  check(known)       :  4.6M  ops/sec   (0.22 µs/op) — 100% true positive
  check(unknown)     :  9.2M  ops/sec   (0.11 µs/op) — 1.05% false positive
  Disk I/O saved     :  99.0%

[2. Block Cache — Zipfian Workload]
  get() warm         :  p50=0.22µs   p95=0.43µs   p99=0.61µs
  Throughput         :  4.2M ops/sec
  Hit rate           :  27.3%  at  5% memory ratio  (Zipfian distribution)

[3. KVStore — Full LSM Path]
  Warm reads         :  p50=0.21µs   p95=0.80µs   p99=1.05µs
  Throughput         :  2.97M ops/sec (cache warm)

  Cold reads (disk)  :  p50=1542µs   p95=2077µs   p99=2552µs
  Throughput         :  624 ops/sec  (cache=1, forcing real SSTable reads)

  Writes (WAL+flush) :  p50=2791µs  p95=3567µs  p99=5390µs
  Sustained writes   :  ~352 writes/sec median (5 runs)
                         includes WAL fsync + memtable + SSTable flush to disk

[4. Compaction]
  SSTables before    :  5
  SSTables after     :  1
  Compaction time    :  ~20 ms
  Read amplification :  O(5 files) -> O(1)

[5. Test Suite]
  35 / 35 tests passing across 8 test suites
  Includes: crash recovery, WAL corruption, LRU eviction,
            persistence, regression, bloom filter edge cases
========================================================
```

---

## Architecture

### Write Path
```
put("key", "value")
    │
    ├─► WAL (disk, fsync)              ← crash safe — written first
    │
    ├─► MemTable (RAM, std::map)       ← fast in-memory write O(log n)
    │
    └─► [MemTable full?] ──────────────► SSTable flush → Compaction trigger
```

### Read Path
```
get("key")
    │
    ├─► MemTable check      ← freshest data   O(log n)
    ├─► Tombstone check     ← deleted?        O(1)
    ├─► Block Cache (LRU)   ← hot keys        O(1)
    └─► SSTables (newest first)
            └─► Bloom Filter ── NO ──► skip disk entirely (99% of misses)
                    │
                   MAYBE
                    │
                    └─► SSTable binary search on disk
```

### Component Table

| Component | Role | Key Metric |
|-----------|------|-----------|
| WAL | Crash-safe write log | Append-only, replay on restart |
| MemTable | In-memory write buffer | `std::map`, O(log n) |
| SSTable | Immutable sorted disk file | Binary search read |
| Bloom Filter | Fast existence check | 9.2M ops/sec, 1.05% FPR |
| Block Cache | LRU hot-read accelerator | 4.2M ops/sec, HashMap+DLL |
| Compaction | Merge & tombstone GC | O(n)→O(1) read amplification |

---

## Project Structure

```
kvstore/
├── include/
│   ├── kvstore.h        ← Main engine API
│   ├── wal.h            ← Write-Ahead Log
│   ├── sstable.h        ← Sorted String Table
│   ├── bloom_filter.h   ← Probabilistic filter
│   └── block_cache.h    ← LRU Cache
├── src/
│   ├── kvstore.cpp      ← Engine: put/get/del/flush/compact
│   ├── wal.cpp          ← log_put, log_delete, recover
│   ├── sstable.cpp      ← write, get, read_all
│   ├── bloom_filter.cpp ← multi-hash bloom
│   ├── block_cache.cpp  ← LRU: HashMap + Doubly Linked List
│   └── main.cpp         ← CLI interface
├── bench/
│   └── bench.cpp        ← p50/p95/p99 benchmark, Zipfian workload
├── tests/
│   └── tests.cpp        ← 35 GoogleTest unit + regression tests
└── CMakeLists.txt
```

---

## Build & Run

### Prerequisites
```bash
# Ubuntu/Debian
sudo apt-get install -y cmake g++ build-essential libgtest-dev

# macOS
brew install cmake googletest
```

### Build
```bash
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)
```

### CLI
```bash
./kvstore set name "Rahul Sharma"
./kvstore get name          # → Rahul Sharma
./kvstore get ghost         # → (nil)
./kvstore delete name
./kvstore stats
./kvstore flush             # force MemTable → SSTable
./kvstore compact           # merge all SSTables → 1
```

### Tests
```bash
./run_tests
# [  PASSED  ] 35 tests.
```

### Benchmark
```bash
./bench
```

---


## Bugs Fixed (13 total — documented in kvstore.cpp)

| # | Bug | Impact |
|---|-----|--------|
| 2 | `.tmp` partial files not cleaned on startup | Stale files accumulate |
| 3 | Lexicographic SSTable sort | Stale reads returned |
| 4 | Max SSTable number not tracked correctly | Counter reset bug |
| 5 | `compact()` silently no-op if below threshold | API lied to caller |
| 6 | `close()` ignored SSTable write failure | Data loss on full disk |
| 11 | Explicit `close()` didn't add SSTable to index | Data invisible post-close |
| 12 | Tombstones kept after full compaction | Wasted disk + bloom inflation |
| 13 | WAL recovery error not checked | Silent write failures post-recovery |

---

## Known Limitations

- **Single mutex**: No parallel writes. Upgrade path: sharded `shared_mutex`.
- **Full compaction only**: All SSTables merged at once. No leveled/tiered strategy.
- **No sparse index**: SSTable reads use binary search from file start.
  RocksDB adds a block index — planned improvement.
- **Cold read latency**: ~1.5ms on HDD/shared disk. On NVMe SSD: ~50µs expected.

---

## This Architecture Powers

| System | Uses LSM-Tree |
|--------|--------------|
| LevelDB (Google) | WAL + MemTable + SSTable + Bloom + Compaction |
| RocksDB (Meta) | Same + multi-level compaction |
| Apache Cassandra | LSM for write-heavy distributed storage |
| Azure Cosmos DB | LSM internals for NoSQL backend |
| InfluxDB | LSM for time-series storage |

---


