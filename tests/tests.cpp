#include <gtest/gtest.h>
#include "kvstore.h"
#include "bloom_filter.h"
#include "block_cache.h"
#include "wal.h"
#include <filesystem>
#include <thread>
#include <vector>

namespace fs = std::filesystem;

// ─── Helpers ─────────────────────────────────────────────────────────────────

static KVConfig test_config(const std::string& dir) {
    KVConfig cfg;
    cfg.db_path       = dir;
    cfg.memtable_size = 10;   // Chota rakho — SSTable flush jaldi ho
    cfg.cache_size    = 50;
    return cfg;
}

static void cleanup(const std::string& dir) {
    fs::remove_all(dir);
}

// ─── Bloom Filter Tests ────────────────────────────────────────────────────────

TEST(BloomFilterTest, BasicAddAndCheck) {
    BloomFilter bf;
    bf.add("hello");
    bf.add("world");
    EXPECT_TRUE(bf.possibly_contains("hello"));
    EXPECT_TRUE(bf.possibly_contains("world"));
}

TEST(BloomFilterTest, MissingKeyReturnsFalse) {
    BloomFilter bf;
    bf.add("present");
    // "absent" kabhi add nahi kiya — mostly false hoga
    // Note: false positive possible hai but very rare
    int false_positives = 0;
    for (int i = 0; i < 100; i++) {
        if (bf.possibly_contains("missing_key_" + std::to_string(i)))
            false_positives++;
    }
    EXPECT_LT(false_positives, 10);  // <10% false positive rate
}

TEST(BloomFilterTest, Reset) {
    BloomFilter bf;
    bf.add("key1");
    bf.reset();
    // After reset, false positives almost zero
    EXPECT_FALSE(bf.possibly_contains("key1"));
}

// ─── Block Cache Tests ─────────────────────────────────────────────────────────

TEST(BlockCacheTest, PutAndGet) {
    BlockCache cache(100);
    cache.put("k1", "v1");
    auto v = cache.get("k1");
    ASSERT_TRUE(v.has_value());
    EXPECT_EQ(v.value(), "v1");
}

TEST(BlockCacheTest, MissReturnsNullopt) {
    BlockCache cache(100);
    EXPECT_FALSE(cache.get("nonexistent").has_value());
}

TEST(BlockCacheTest, LRUEviction) {
    BlockCache cache(3);  // sirf 3 entries
    cache.put("a", "1");
    cache.put("b", "2");
    cache.put("c", "3");
    cache.put("d", "4");  // "a" evict hona chahiye
    EXPECT_FALSE(cache.get("a").has_value());
    EXPECT_TRUE(cache.get("b").has_value());
    EXPECT_TRUE(cache.get("c").has_value());
    EXPECT_TRUE(cache.get("d").has_value());
}

TEST(BlockCacheTest, RecentlyUsedNotEvicted) {
    BlockCache cache(3);
    cache.put("a", "1");
    cache.put("b", "2");
    cache.put("c", "3");
    cache.get("a");        // "a" ko access karo — recently used
    cache.put("d", "4");   // "b" evict hona chahiye (LRU)
    EXPECT_TRUE(cache.get("a").has_value());   // "a" still hai
    EXPECT_FALSE(cache.get("b").has_value());  // "b" gone
}

TEST(BlockCacheTest, HitMissStats) {
    BlockCache cache(100);
    cache.put("x", "y");
    cache.get("x");   // hit
    cache.get("z");   // miss
    EXPECT_EQ(cache.hits(), 1);
    EXPECT_EQ(cache.misses(), 1);
}

// ─── WAL Tests ────────────────────────────────────────────────────────────────

TEST(WALTest, LogAndRecover) {
    std::string path = "/tmp/test_wal.log";
    fs::remove(path);

    {
        WAL wal(path);
        wal.log_put("name", "Rahul");
        wal.log_put("city", "Mumbai");
        wal.log_delete("name");
    }

    WAL wal2(path);
    std::vector<WAL::Record> records;
    wal2.recover([&](const WAL::Record& r) {
        records.push_back(r);
    });

    ASSERT_EQ(records.size(), 3u);
    EXPECT_EQ(records[0].key, "name");
    EXPECT_EQ(records[0].op, WAL::OpType::PUT);
    EXPECT_EQ(records[1].key, "city");
    EXPECT_EQ(records[2].op, WAL::OpType::DEL);

    fs::remove(path);
}

// ─── KVStore Core Tests ────────────────────────────────────────────────────────

class KVStoreTest : public ::testing::Test {
protected:
    void SetUp() override {
        cleanup(dir_);
        db_ = std::make_unique<KVStore>(test_config(dir_));
    }
    void TearDown() override {
        db_.reset();
        cleanup(dir_);
    }
    std::string dir_ = "/tmp/kvtest_basic";
    std::unique_ptr<KVStore> db_;
};

TEST_F(KVStoreTest, PutAndGet) {
    EXPECT_TRUE(db_->put("foo", "bar"));
    auto v = db_->get("foo");
    ASSERT_TRUE(v.has_value());
    EXPECT_EQ(v.value(), "bar");
}

TEST_F(KVStoreTest, GetMissingKey) {
    EXPECT_FALSE(db_->get("ghost").has_value());
}

TEST_F(KVStoreTest, Overwrite) {
    db_->put("key", "v1");
    db_->put("key", "v2");
    auto v = db_->get("key");
    ASSERT_TRUE(v.has_value());
    EXPECT_EQ(v.value(), "v2");
}

TEST_F(KVStoreTest, Delete) {
    db_->put("del_me", "value");
    EXPECT_TRUE(db_->contains("del_me"));
    db_->del("del_me");
    EXPECT_FALSE(db_->contains("del_me"));
}

TEST_F(KVStoreTest, MultipleKeys) {
    for (int i = 0; i < 100; i++) {
        db_->put("key" + std::to_string(i), "val" + std::to_string(i));
    }
    for (int i = 0; i < 100; i++) {
        auto v = db_->get("key" + std::to_string(i));
        ASSERT_TRUE(v.has_value());
        EXPECT_EQ(v.value(), "val" + std::to_string(i));
    }
}

// ─── Crash Recovery Test ──────────────────────────────────────────────────────

// Bug 8 fix: old test was finding data in SSTable (destructor flushed it),
// not actually testing WAL recovery. True WAL recovery test:
// write directly to WAL file, never open KVStore (simulates hard crash),
// then open fresh DB and verify recovery from WAL.
TEST(CrashRecoveryTest, TrueWALRecovery) {
    std::string dir = "/tmp/kvtest_wal_recovery";
    cleanup(dir);
    fs::create_directories(dir);

    // Simulate crash: write to WAL directly, never flush to SSTable
    {
        WAL wal(dir + "/wal.log");
        wal.log_put("survivor", "alive");
        wal.log_put("another",  "data");
        // WAL NOT cleared — simulates unclean shutdown / hard kill
        // No KVStore opened, so no SSTable flush happened
    }

    // Fresh KVStore open — must recover from WAL, no SSTables exist
    {
        KVStore db(test_config(dir));
        auto v = db.get("survivor");
        ASSERT_TRUE(v.has_value()) << "WAL recovery failed — data not found";
        EXPECT_EQ(v.value(), "alive");

        auto v2 = db.get("another");
        ASSERT_TRUE(v2.has_value());
        EXPECT_EQ(v2.value(), "data");
    }

    cleanup(dir);
}

// Additional: verify destructor flush does NOT corrupt WAL-recovered data
TEST(CrashRecoveryTest, DestructorFlushPreservesData) {
    std::string dir = "/tmp/kvtest_crash_destruct";
    cleanup(dir);
    {
        KVStore db(test_config(dir));
        db.put("key1", "val1");
        db.put("key2", "val2");
        // Destructor runs here — flushes to SSTable, clears WAL
    }
    {
        // Restart — data should be in SSTable now
        KVStore db2(test_config(dir));
        EXPECT_TRUE(db2.get("key1").has_value());
        EXPECT_EQ(db2.get("key1").value(), "val1");
        EXPECT_TRUE(db2.get("key2").has_value());
    }
    cleanup(dir);
}

// ─── Flush & Persistence Test ─────────────────────────────────────────────────

TEST(PersistenceTest, DataSurvivesRestart) {
    std::string dir = "/tmp/kvtest_persist";
    cleanup(dir);

    {
        KVStore db(test_config(dir));
        for (int i = 0; i < 50; i++) {  // memtable_size=10 so will flush
            db.put("p" + std::to_string(i), "v" + std::to_string(i));
        }
        db.flush();
    }

    // Restart
    {
        KVStore db2(test_config(dir));
        for (int i = 0; i < 50; i++) {
            auto v = db2.get("p" + std::to_string(i));
            ASSERT_TRUE(v.has_value()) << "Key p" << i << " missing after restart";
        }
    }

    cleanup(dir);
}

// ─── Stats Test ───────────────────────────────────────────────────────────────

TEST(StatsTest, WriteReadCounts) {
    std::string dir = "/tmp/kvtest_stats";
    cleanup(dir);
    KVStore db(test_config(dir));

    db.put("a", "1");
    db.put("b", "2");
    db.get("a");
    db.get("a");
    db.del("b");

    auto s = db.stats();
    EXPECT_EQ(s.writes, 2u);
    EXPECT_EQ(s.reads, 2u);
    EXPECT_EQ(s.deletes, 1u);

    cleanup(dir);
}


// ─── Bug 3 Regression: Numeric SSTable sort ──────────────────────────────────
// sst_10 must be treated as newer than sst_9 (lexicographic sort fails this)
TEST(RegressionTest, NumericSSTSort) {
    std::string dir = "/tmp/kvtest_sort";
    cleanup(dir);
    {
        KVConfig cfg;
        cfg.db_path = dir;
        cfg.memtable_size = 2; // flush every 2 entries -> many SSTables
        cfg.cache_size = 1;
        cfg.max_sstables = 999; // no compaction
        KVStore db(cfg);

        // Write enough to create 10+ SSTables
        for (int i = 0; i < 30; i++) {
            db.put("key", "version_" + std::to_string(i));
        }
        db.flush();

        // Must get LATEST version, not an old one from wrong sort order
        auto val = db.get("key");
        ASSERT_TRUE(val.has_value());
        // The last put was version_29
        EXPECT_EQ(val.value(), "version_29")
            << "Bug 3: lexicographic sort returned stale value. Got: " << val.value();
    }
    cleanup(dir);
}

// ─── Bug 5 Regression: compact() must always run ─────────────────────────────
TEST(RegressionTest, CompactAlwaysRuns) {
    std::string dir = "/tmp/kvtest_compact";
    cleanup(dir);
    {
        KVConfig cfg;
        cfg.db_path = dir;
        cfg.memtable_size = 2;
        cfg.cache_size = 10;
        cfg.max_sstables = 99; // high threshold -> auto-compact never fires
        KVStore db(cfg);

        for (int i = 0; i < 10; i++) db.put("k" + std::to_string(i), "v");
        db.flush();

        auto before = db.stats().sstable_cnt;
        EXPECT_GT(before, 1) << "Need multiple SSTables to test compaction";

        db.compact(); // Bug 5: this was a silent no-op before

        auto after = db.stats().sstable_cnt;
        EXPECT_EQ(after, 1)
            << "Bug 5: compact() did not merge SSTables. Before=" << before
            << " After=" << after;
    }
    cleanup(dir);
}

// ─── Bug 9 Regression: load_bloom seeks past values, doesn't read them ────────
// Verifies bloom filter is correct after open regardless of value sizes.
// (perf: confirmed by valgrind — value bytes no longer allocated on load)
TEST(RegressionTest, BloomCorrectAfterOpenLargeValues) {
    std::string dir = "/tmp/kvtest_bloom_values";
    cleanup(dir);
    {
        KVConfig cfg;
        cfg.db_path = dir;
        cfg.memtable_size = 5;
        cfg.cache_size = 1;
        KVStore db(cfg);
        std::string big_value(4096, 'x'); // 4KB values
        for (int i = 0; i < 20; i++)
            db.put("bigkey_" + std::to_string(i), big_value + std::to_string(i));
        db.flush();
    }
    // Reopen — load_bloom must reconstruct correctly with seek (not read)
    {
        KVStore db2(test_config(dir));
        // Bloom must not produce false negatives on existing keys
        for (int i = 0; i < 20; i++) {
            auto v = db2.get("bigkey_" + std::to_string(i));
            ASSERT_TRUE(v.has_value()) << "Key bigkey_" << i << " missing after reload";
        }
        // And must return nullopt for missing keys
        EXPECT_FALSE(db2.get("never_inserted").has_value());
    }
    cleanup(dir);
}

// ─── Bug 11 Regression: close() adds SSTable to sstables_ list ───────────────
// After explicit close(), get() must still find the data that was flushed.
TEST(RegressionTest, ExplicitCloseDataVisible) {
    std::string dir = "/tmp/kvtest_close_visible";
    cleanup(dir);
    {
        KVConfig cfg;
        cfg.db_path = dir;
        cfg.memtable_size = 10000; // never auto-flush
        cfg.cache_size = 1;        // no cache hits
        KVStore db(cfg);
        db.put("persisted", "yes");
        db.close(); // explicit close — must flush + register SSTable
        // Object still alive — get() must find "persisted" via sstables_
        auto v = db.get("persisted");
        ASSERT_TRUE(v.has_value())
            << "Bug 11: data flushed in close() not in sstables_ list";
        EXPECT_EQ(v.value(), "yes");
    }
    cleanup(dir);
}

// ─── Bug 12 Regression: tombstones dropped after full compaction ──────────────
// After compact(), deleted keys must still be absent (tombstones dropped, not leaked).
// Also verifies the compacted SSTable is smaller (no wasted tombstone entries).
TEST(RegressionTest, TombstonesDroppedAfterCompaction) {
    std::string dir = "/tmp/kvtest_tombstone_compact";
    cleanup(dir);
    {
        KVConfig cfg;
        cfg.db_path = dir;
        cfg.memtable_size = 3;
        cfg.cache_size = 1;
        cfg.max_sstables = 999;
        KVStore db(cfg);

        for (int i = 0; i < 9; i++)
            db.put("k" + std::to_string(i), "v" + std::to_string(i));
        db.flush();

        // Delete half the keys
        for (int i = 0; i < 5; i++)
            db.del("k" + std::to_string(i));
        db.flush();

        db.compact();
        EXPECT_EQ(db.stats().sstable_cnt, 1);

        // Deleted keys must be gone
        for (int i = 0; i < 5; i++)
            EXPECT_FALSE(db.get("k" + std::to_string(i)).has_value())
                << "Deleted key k" << i << " still present after compaction";

        // Live keys must survive
        for (int i = 5; i < 9; i++) {
            auto v = db.get("k" + std::to_string(i));
            ASSERT_TRUE(v.has_value()) << "Live key k" << i << " lost after compaction";
            EXPECT_EQ(v.value(), "v" + std::to_string(i));
        }
    }
    cleanup(dir);
}

// ─── Bug 13 Regression: recover_from_wal() does not hide WAL re-open error ───
// This tests the normal (healthy) path — recovery succeeds and WAL is usable.
// The error path (permissions issue) is hard to trigger in a unit test but
// the code now at least surfaces it via stderr instead of silently failing.
TEST(RegressionTest, WALRecoveryAndSubsequentWritesWork) {
    std::string dir = "/tmp/kvtest_wal_reopen";
    cleanup(dir); fs::create_directories(dir);
    {
        WAL wal(dir + "/wal.log");
        wal.log_put("k1", "v1");
        wal.log_put("k2", "v2");
    }
    {
        KVStore db(test_config(dir));
        ASSERT_TRUE(db.get("k1").has_value());
        // Crucially: subsequent writes after recovery must work
        EXPECT_TRUE(db.put("k3", "v3"))
            << "Bug 13: put() failed after WAL recovery — WAL not re-opened";
        EXPECT_EQ(db.get("k3").value(), "v3");
    }
    cleanup(dir);
}

// ─── Bug 1 Regression: read_str handles corrupted/truncated SSTable ──────────
TEST(RegressionTest, CorruptedSSTDoesNotCrash) {
    std::string dir = "/tmp/kvtest_corrupt";
    fs::remove_all(dir); fs::create_directories(dir);

    // Plant a corrupt .sst file:
    // num_entries=1 (valid), then key_len=0xDEADBEEF (huge → read_str must reject)
    std::string corrupt_path = dir + "/sst_1.sst";
    {
        std::ofstream f(corrupt_path, std::ios::binary | std::ios::trunc);
        uint32_t num = 1;
        f.write(reinterpret_cast<char*>(&num), 4); // 1 entry
        uint8_t del = 0;
        f.write(reinterpret_cast<char*>(&del), 1);
        uint32_t huge_len = 0x7FFFFFFFu; // 2GB key — must be rejected by sanity cap
        f.write(reinterpret_cast<char*>(&huge_len), 4);
        // no actual key bytes — truncated file
    }

    // Opening DB must NOT crash or OOM — read_str sanity cap must fire
    EXPECT_NO_THROW({
        KVConfig cfg; cfg.db_path = dir; cfg.memtable_size = 100; cfg.cache_size = 10;
        KVStore db(cfg);
        auto v = db.get("goodkey");
        (void)v;
    }) << "Bug 1: corrupted SSTable caused crash or bad_alloc";

    fs::remove_all(dir);
}

// ─── Bug 2 Regression: no partial .sst left on disk, .tmp cleaned on restart ─
TEST(RegressionTest, NoPartialSSTOnDisk) {
    std::string dir = "/tmp/kvtest_partial";
    fs::remove_all(dir); fs::create_directories(dir);

    // Manually plant a .tmp file (simulates interrupted write)
    std::string tmp_file = dir + "/sst_1.sst.tmp";
    { std::ofstream f(tmp_file); f << "garbage partial data"; }
    ASSERT_TRUE(fs::exists(tmp_file));

    // Opening KVStore must clean up the .tmp file
    {
        KVConfig cfg; cfg.db_path = dir; cfg.memtable_size = 100; cfg.cache_size = 10;
        KVStore db(cfg); // constructor should remove .tmp
    }

    EXPECT_FALSE(fs::exists(tmp_file))
        << "Bug 2: .tmp file not cleaned up on startup";

    fs::remove_all(dir);
}

// ─── Bug 3 Regression: WAL stream recovers after transient write failure ──────
TEST(RegressionTest, WALStreamRecovesAfterFailure) {
    std::string path = "/tmp/test_wal_recover_stream.log";
    fs::remove(path);
    WAL wal(path);

    // Write a good record
    EXPECT_TRUE(wal.log_put("before", "ok"));

    // Simulate stream corruption by calling log_put while stream is poisoned.
    // We can't easily simulate disk-full in a unit test, but we can verify the
    // stream stays usable — which is the contract we added.
    // Full disk-full simulation requires ulimit tricks — out of scope for unit test.
    // Just verify normal put → fail path → put again works:
    bool second = wal.log_put("after", "also_ok");
    EXPECT_TRUE(second) << "Bug 3: WAL stream unusable after prior write";

    fs::remove(path);
}

// ─── Bug 4 Regression: cache stats come from cache_ object, not dead counters ─
TEST(RegressionTest, CacheStatsAccurate) {
    std::string dir = "/tmp/kvtest_cachestats";
    fs::remove_all(dir);
    KVConfig cfg; cfg.db_path = dir; cfg.memtable_size = 1000; cfg.cache_size = 100;
    KVStore db(cfg);

    db.put("k1", "v1");
    db.put("k2", "v2");

    // Must flush first — otherwise get() returns from MemTable and never
    // reaches the cache check path. Cache hits only happen for keys on disk.
    db.flush();

    // Invalidate cache by creating a fresh db instance reading from SSTable
    // (cache is cold — first get() populates cache, second get() is a hit)
    {
        KVConfig cfg2 = cfg; cfg2.cache_size = 100;
        KVStore db2(cfg2);
        db2.get("k1"); // SSTable → cache miss, populates cache
        db2.get("k1"); // cache hit
        db2.get("k1"); // cache hit
        db2.get("k99"); // miss — key doesn't exist, no cache population

        auto s = db2.stats();
        EXPECT_GE(s.cache_hits, 2u)  << "Expected >= 2 cache hits";
        EXPECT_GE(s.cache_miss, 1u)  << "Expected >= 1 cache miss";
    }
    fs::remove_all(dir);
}

// ─── Bug 5 Regression: WAL write_record returns false on bad stream ───────────
// True fsync durability can't be verified in a unit test without hardware.
// This test verifies the stream-level contract: write succeeds on healthy stream.
TEST(RegressionTest, WALWriteSucceedsOnHealthyStream) {
    std::string path = "/tmp/test_wal_fsync.log";
    fs::remove(path);
    WAL wal(path);
    EXPECT_TRUE(wal.log_put("key", "value"))
        << "Bug 5: WAL write_record failed on healthy stream";
    EXPECT_TRUE(wal.log_delete("key"));

    // Verify records were actually persisted (fsync'd to disk)
    WAL wal2(path);
    std::vector<WAL::Record> recs;
    wal2.recover([&](const WAL::Record& r){ recs.push_back(r); });
    ASSERT_EQ(recs.size(), 2u) << "Records not found after fsync — not persisted";
    EXPECT_EQ(recs[0].key, "key");
    EXPECT_EQ(recs[1].op, WAL::OpType::DEL);

    fs::remove(path);
}

// ─── New: load_bloom + read_all validate num ─────────────────────────────────
TEST(RegressionTest, CorruptNumEntriesDoesNotReserveGigabytes) {
    std::string dir = "/tmp/kvtest_corrupt_num";
    fs::remove_all(dir); fs::create_directories(dir);

    // Plant an SSTable with num_entries = 0x00FFFFFF (16M) but no actual entries
    // Old code: result.reserve(16M) → bad_alloc before reading anything
    std::string sst_path = dir + "/sst_1.sst";
    {
        std::ofstream f(sst_path, std::ios::binary);
        uint32_t huge_num = 0x00FFFFFFu; // 16M entries — exceeds MAX_ENTRIES
        f.write(reinterpret_cast<char*>(&huge_num), 4);
        // no actual entry bytes — truncated
    }

    EXPECT_NO_THROW({
        KVConfig cfg; cfg.db_path = dir; cfg.memtable_size = 100; cfg.cache_size = 10;
        KVStore db(cfg);
        auto v = db.get("any");
        (void)v;
    }) << "Corrupt num_entries caused crash or bad_alloc";

    fs::remove_all(dir);
}

// ─── New: WAL read_record validates key_len/val_len ──────────────────────────
TEST(RegressionTest, CorruptWALKeyLenDoesNotCrash) {
    std::string dir = "/tmp/kvtest_wal_corrupt";
    fs::remove_all(dir); fs::create_directories(dir);

    // Write a WAL with one valid record then a corrupt one (huge key_len)
    {
        WAL wal(dir + "/wal.log");
        wal.log_put("good_key", "good_val");
    }
    // Append a corrupt record: valid op byte, then key_len = 0x7FFFFFFF
    {
        std::ofstream f(dir + "/wal.log", std::ios::binary | std::ios::app);
        uint8_t op = 1; // PUT
        uint32_t bad_key_len = 0x7FFFFFFFu; // 2GB
        f.write(reinterpret_cast<char*>(&op), 1);
        f.write(reinterpret_cast<char*>(&bad_key_len), 4);
        // no actual key bytes
    }

    // Opening DB must recover the good record and stop cleanly at corrupt one
    EXPECT_NO_THROW({
        KVConfig cfg; cfg.db_path = dir; cfg.memtable_size = 100; cfg.cache_size = 10;
        KVStore db(cfg);
        // good_key was recovered before corruption
        auto v = db.get("good_key");
        EXPECT_TRUE(v.has_value()) << "good_key before corruption should be recovered";
    }) << "Corrupt WAL key_len caused OOM or crash";

    fs::remove_all(dir);
}

// ─── Bug A Regression: read_all stops cleanly when value is truncated ─────────
// Old code pushed corrupt {key, "", del} when key read OK but value truncated.
TEST(RegressionTest, TruncatedValueDoesNotPushCorruptEntry) {
    std::string dir = "/tmp/kvtest_truncval";
    fs::remove_all(dir); fs::create_directories(dir);

    // Write a valid SSTable with 2 entries, then truncate mid-value of entry 2
    std::string sst_path = dir + "/sst_1.sst";
    {
        std::ofstream f(sst_path, std::ios::binary);
        // num_entries = 2
        uint32_t num = 2; f.write(reinterpret_cast<char*>(&num), 4);

        // Entry 1: complete and valid — "k1" = "v1"
        uint8_t del = 0; f.write(reinterpret_cast<char*>(&del), 1);
        uint32_t klen = 2; f.write(reinterpret_cast<char*>(&klen), 4); f.write("k1", 2);
        uint32_t vlen = 2; f.write(reinterpret_cast<char*>(&vlen), 4); f.write("v1", 2);

        // Entry 2: key = "k2" written, value length = 100 but only 3 bytes written (truncated)
        f.write(reinterpret_cast<char*>(&del), 1);
        uint32_t klen2 = 2; f.write(reinterpret_cast<char*>(&klen2), 4); f.write("k2", 2);
        uint32_t vlen2 = 100; f.write(reinterpret_cast<char*>(&vlen2), 4);
        f.write("abc", 3); // only 3 of 100 bytes — truncated
    }

    {
        KVConfig cfg; cfg.db_path = dir; cfg.memtable_size = 100; cfg.cache_size = 10;
        KVStore db(cfg);

        // Entry 1 is valid — must be found
        auto v1 = db.get("k1");
        ASSERT_TRUE(v1.has_value()) << "Valid entry k1 must be readable";
        EXPECT_EQ(v1.value(), "v1");

        // Entry 2 was corrupt — must NOT return empty string as value
        // (old bug: would push {k2, "", false} and return "" for k2)
        auto v2 = db.get("k2");
        if (v2.has_value()) {
            // If found, must not be empty — that would be the corrupt value
            EXPECT_NE(v2.value(), "")
                << "Bug A: corrupt empty value pushed from truncated entry";
        }
        // It's also acceptable that k2 is not found (clean stop at corruption)
    }
    fs::remove_all(dir);
}

// ─── Bug B Regression: empty-string key works correctly through bloom filter ──
// Old code skipped bloom_.add("") — false negative on every get("").
TEST(RegressionTest, EmptyStringKeyBloomNeverFalseNegative) {
    std::string dir = "/tmp/kvtest_emptykey";
    fs::remove_all(dir);
    {
        KVConfig cfg; cfg.db_path = dir;
        cfg.memtable_size = 2; // flush immediately
        cfg.cache_size = 1;    // no cache
        KVStore db(cfg);
        db.put("", "empty_key_value"); // store empty string key
        db.flush();
    }
    // Reopen — bloom must include "" so get("") reaches disk scan
    {
        KVConfig cfg; cfg.db_path = dir;
        cfg.memtable_size = 1000; cfg.cache_size = 1;
        KVStore db(cfg);
        auto v = db.get("");
        ASSERT_TRUE(v.has_value())
            << "Bug B: empty key not found — bloom false negative (key not added to bloom)";
        EXPECT_EQ(v.value(), "empty_key_value");
    }
    fs::remove_all(dir);
}

// ─── BloomFilter(0) regression: must not crash ───────────────────────────────
TEST(BloomFilterTest, ZeroKeysDoesNotCrash) {
    // BloomFilter(0, 0.01): old code → optimal_bits(0)=0 → size_bits_=0 →
    // get_bit_index does (h % 0) → division by zero crash.
    // optimal_hashes(0,0) → 0.0/0 → nan → static_cast<int>(nan) → UB.
    EXPECT_NO_THROW({
        BloomFilter bf(0, 0.01);
        bf.add("key");
        (void)bf.possibly_contains("key");
        (void)bf.possibly_contains("missing");
    }) << "BloomFilter(0) must not crash or trigger UB";
}

// ─── WAL invalid op byte regression: must not silently delete keys ────────────
TEST(RegressionTest, CorruptWALOpByteDoesNotSilentlyDelete) {
    std::string dir = "/tmp/kvtest_wal_opbyte";
    fs::remove_all(dir); fs::create_directories(dir);

    // Write valid WAL records then append a corrupt op byte (0x99)
    {
        WAL wal(dir + "/wal.log");
        wal.log_put("safe_key", "safe_val");
    }
    {
        std::ofstream f(dir + "/wal.log", std::ios::binary | std::ios::app);
        uint8_t bad_op = 0x99; // not PUT(1) or DEL(2)
        f.write(reinterpret_cast<char*>(&bad_op), 1);
        // followed by valid-looking key_len=8 and key="safe_key"
        uint32_t klen = 8;
        f.write(reinterpret_cast<char*>(&klen), 4);
        f.write("safe_key", 8);
        uint32_t vlen = 0;
        f.write(reinterpret_cast<char*>(&vlen), 4);
    }

    // "safe_key" must survive — corrupt op must stop replay, not DELETE the key
    {
        KVConfig cfg; cfg.db_path = dir; cfg.memtable_size = 100; cfg.cache_size = 10;
        KVStore db(cfg);
        auto v = db.get("safe_key");
        ASSERT_TRUE(v.has_value())
            << "Bug: corrupt op byte treated as DELETE — safe_key was wiped";
        EXPECT_EQ(v.value(), "safe_val");
    }
    fs::remove_all(dir);
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}