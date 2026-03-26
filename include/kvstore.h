#pragma once
#include "wal.h"
#include "sstable.h"
#include "block_cache.h"
#include "bloom_filter.h"
#include <string>
#include <map>
#include <vector>
#include <optional>
#include <memory>
#include <mutex>

// KVStore — Main Engine
// Ye sab components ko ek saath jodata hai:
//   WAL + MemTable + SSTables + BloomFilter + BlockCache
//
// Write path:  WAL → MemTable → (flush) → SSTable
// Read path:   MemTable → Cache → BloomFilter → SSTables (newest first)

struct KVConfig {
    std::string  db_path       = "./kvdata";
    size_t       memtable_size = 4096;   // kitni entries ke baad flush karo
    size_t       cache_size    = 1000;   // cache mein max kitni keys
    int          max_sstables  = 8;      // compaction trigger
};

class KVStore {
public:
    explicit KVStore(const KVConfig& config = KVConfig{});
    ~KVStore();

    // Core operations
    bool put(const std::string& key, const std::string& value);
    std::optional<std::string> get(const std::string& key);
    bool del(const std::string& key);

    // Utility
    bool contains(const std::string& key);
    void flush();            // MemTable ko SSTable mein force karo
    void compact();          // SSTables merge karo
    void close();

    // Stats — benchmark ke liye
    struct Stats {
        uint64_t writes      = 0;
        uint64_t reads       = 0;
        uint64_t deletes     = 0;
        uint64_t cache_hits  = 0;
        uint64_t cache_miss  = 0;
        uint64_t bloom_skip  = 0;   // bloom ne kitni baar disk bachaya
        int      sstable_cnt = 0;
    };
    Stats stats() const;

private:
    KVConfig config_;
    std::unique_ptr<WAL> wal_;
    std::map<std::string, std::string> memtable_;
    std::map<std::string, bool> deleted_keys_;  // tombstones
    std::vector<std::unique_ptr<SSTable>> sstables_;
    BlockCache cache_;
    mutable std::mutex mu_;
    mutable Stats stats_;

    void recover_from_wal();
    void maybe_flush();
    void maybe_compact();     // threshold-gated
    void force_compact_internal(); // Bug 5 fix: always runs
    std::string next_sstable_path();
    int sstable_counter_ = 0;
};
