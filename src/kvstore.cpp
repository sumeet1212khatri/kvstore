#include "kvstore.h"
#include <filesystem>
#include <stdexcept>
#include <algorithm>
#include <iostream>

namespace fs = std::filesystem;

KVStore::KVStore(const KVConfig& config)
    : config_(config), cache_(config.cache_size) {

    // DB directory banao agar nahi hai
    fs::create_directories(config_.db_path);

    // WAL initialize karo
    wal_ = std::make_unique<WAL>(config_.db_path + "/wal.log");

    // Bug 4 fix: track MAX number, not count
    // Bug 3 fix: sort numerically, not lexicographically
    // "sst_9" > "sst_10" lexicographically but 10 > 9 numerically
    auto parse_sst_num = [](const std::string& path) -> int {
        std::string stem = fs::path(path).stem().string(); // "sst_N"
        try { return std::stoi(stem.substr(4)); }           // skip "sst_"
        catch (...) { return 0; }
    };

    for (auto& entry : fs::directory_iterator(config_.db_path)) {
        if (entry.path().extension() == ".sst") {
            sstables_.push_back(std::make_unique<SSTable>(entry.path().string()));
            int n = parse_sst_num(entry.path().string());
            sstable_counter_ = std::max(sstable_counter_, n);
        } else if (entry.path().extension() == ".tmp") {
            // Bug 2 fix: leftover .tmp from a previously interrupted write.
            // These are partial files — safe to delete on startup.
            fs::remove(entry.path());
        }
    }

    // Bug 3 fix: numeric sort so sst_10 is newer than sst_9
    std::sort(sstables_.begin(), sstables_.end(),
        [&parse_sst_num](const auto& a, const auto& b) {
            return parse_sst_num(a->path()) > parse_sst_num(b->path());
        });

    // WAL se crash recovery
    recover_from_wal();
}

KVStore::~KVStore() {
    close();
}

void KVStore::close() {
    std::lock_guard<std::mutex> lock(mu_);
    if (!memtable_.empty() || !deleted_keys_.empty()) {
        // Bug 6 fix: check return value before clearing anything.
        // Bug 11 fix: add flushed SSTable to sstables_ so reads still work
        // if close() is called explicitly (not only via destructor).
        // Old code: wrote SSTable but never added it to sstables_ — data
        // was invisible to any get() call made after an explicit close().
        std::string path = next_sstable_path();
        if (SSTable::write(path, memtable_, deleted_keys_)) {
            sstables_.insert(sstables_.begin(), std::make_unique<SSTable>(path));
            stats_.sstable_cnt = static_cast<int>(sstables_.size());
            memtable_.clear();
            deleted_keys_.clear();
            if (wal_) wal_->clear();
        }
        // else: disk full / I/O error — WAL survives for recovery on next open
    }
}

// ─── WRITE PATH ───────────────────────────────────────────────────────────────

bool KVStore::put(const std::string& key, const std::string& value) {
    std::lock_guard<std::mutex> lock(mu_);

    // Step 1: WAL mein likho (crash safe)
    if (!wal_->log_put(key, value)) return false;

    // Step 2: MemTable mein daalo
    memtable_[key] = value;
    deleted_keys_.erase(key);  // agar pehle delete tha

    // Step 3: Cache update karo
    cache_.put(key, value);

    stats_.writes++;

    // Step 4: Bhar gayi toh flush karo
    maybe_flush();
    return true;
}

bool KVStore::del(const std::string& key) {
    std::lock_guard<std::mutex> lock(mu_);

    // WAL mein likho
    if (!wal_->log_delete(key)) return false;

    // MemTable se hatao, tombstone lagao
    memtable_.erase(key);
    deleted_keys_[key] = true;
    cache_.remove(key);

    stats_.deletes++;
    maybe_flush();
    return true;
}

// ─── READ PATH ────────────────────────────────────────────────────────────────

std::optional<std::string> KVStore::get(const std::string& key) {
    std::lock_guard<std::mutex> lock(mu_);
    stats_.reads++;

    // Step 1: Pehle MemTable mein dekho (sabse fresh data)
    if (deleted_keys_.count(key)) return std::nullopt;  // deleted
    auto it = memtable_.find(key);
    if (it != memtable_.end()) return it->second;

    // Step 2: Block Cache check karo
    auto cached = cache_.get(key);
    if (cached.has_value()) {
        // Bug 4 fix: removed stats_.cache_hits++ here — it was dead code.
        // stats() overwrites cache_hits/miss directly from cache_.hits()/misses(),
        // so manual increments here were accumulated but never returned to callers.
        return cached;
    }
    // Bug 4 fix: removed stats_.cache_miss++ here — same reason as above.

    // Step 3: SSTables mein dhundho (newest first)
    for (auto& sst : sstables_) {
        // Bug 2 fix: check bloom explicitly so we only count TRUE skips
        // (bloom rejected key → disk definitely avoided)
        // vs false positives (bloom said "maybe" but key not found on disk)
        if (!sst->bloom_possibly_contains(key)) {
            stats_.bloom_skip++; // only count real bloom-avoided disk reads
            continue;
        }
        auto result = sst->get_without_bloom(key);
        if (result.has_value()) {
            if (result->is_deleted) return std::nullopt;
            cache_.put(key, result->value);
            return result->value;
        }
        // false positive: bloom said maybe but key absent — don't count as skip
    }

    return std::nullopt;  // key exist hi nahi karti
}

bool KVStore::contains(const std::string& key) {
    return get(key).has_value();
}

// ─── FLUSH ────────────────────────────────────────────────────────────────────

void KVStore::flush() {
    std::lock_guard<std::mutex> lock(mu_);
    if (memtable_.empty() && deleted_keys_.empty()) return;

    std::string sst_path = next_sstable_path();
    if (SSTable::write(sst_path, memtable_, deleted_keys_)) {
        sstables_.insert(sstables_.begin(),
            std::make_unique<SSTable>(sst_path));
        memtable_.clear();
        deleted_keys_.clear();
        wal_->clear();
        stats_.sstable_cnt = static_cast<int>(sstables_.size());
    }
}

void KVStore::maybe_flush() {
    // mutex already held by caller
    if (memtable_.size() + deleted_keys_.size() >= config_.memtable_size) {
        std::string sst_path = next_sstable_path();
        if (SSTable::write(sst_path, memtable_, deleted_keys_)) {
            sstables_.insert(sstables_.begin(),
                std::make_unique<SSTable>(sst_path));
            memtable_.clear();
            deleted_keys_.clear();
            wal_->clear();
            stats_.sstable_cnt = static_cast<int>(sstables_.size());
            maybe_compact();
        }
    }
}

// ─── COMPACTION ───────────────────────────────────────────────────────────────
// Multiple SSTables ko ek badi SSTable mein merge karo
// Duplicate keys mein newest wala jeetega
// Tombstones (deleted keys) hata do

void KVStore::compact() {
    std::lock_guard<std::mutex> lock(mu_);
    // Bug 5 fix: compact() = explicit user call, ALWAYS runs
    // maybe_compact() = internal, only when threshold hit
    // Old code: compact() called maybe_compact() which silently did nothing
    // if sstables_.size() < max_sstables — silent no-op, API lie
    force_compact_internal();
}

void KVStore::force_compact_internal() {
    if (sstables_.empty()) return;
    std::map<std::string, SSTable::Entry> merged;
    for (int i = (int)sstables_.size() - 1; i >= 0; i--) {
        auto entries = sstables_[i]->read_all();
        for (auto& e : entries) merged[e.key] = e;
    }

    std::map<std::string, std::string> live_data;
    // Bug 12 fix: drop tombstones after full compaction.
    // After merging ALL SSTables into one, there are no older SSTables that
    // could still have a live version of a deleted key. Keeping tombstones
    // wastes disk, inflates the bloom filter, and slows reads.
    // (Partial compaction / leveled compaction would still need tombstones.)
    std::map<std::string, bool> dead_data; // intentionally empty
    for (auto& [k, e] : merged) {
        if (!e.is_deleted)
            live_data[k] = e.value;
        // deleted entries silently dropped — correct after full merge
    }

    std::string new_path = next_sstable_path();
    if (!SSTable::write(new_path, live_data, dead_data)) return;

    for (auto& sst : sstables_) { fs::remove(sst->path()); }
    sstables_.clear();
    sstables_.push_back(std::make_unique<SSTable>(new_path));
    stats_.sstable_cnt = 1;
}

void KVStore::maybe_compact() {
    if ((int)sstables_.size() < config_.max_sstables) return;
    force_compact_internal(); // Bug 5: threshold check here, not in force_compact
}

// ─── CRASH RECOVERY ───────────────────────────────────────────────────────────

void KVStore::recover_from_wal() {
    int recovered = 0;
    // Bug 13 fix: check return value of wal_->recover().
    // If the WAL file can't be re-opened after replay, log a clear error.
    // Old code: silently ignored the return — subsequent puts would all
    // return false with no explanation (write_record on a closed fstream).
    bool ok = wal_->recover([&](const WAL::Record& rec) {
        if (rec.op == WAL::OpType::PUT) {
            memtable_[rec.key] = rec.value;
            deleted_keys_.erase(rec.key);
        } else {
            memtable_.erase(rec.key);
            deleted_keys_[rec.key] = true;
        }
        recovered++;
    });
    if (recovered > 0) {
        std::cout << "[KVStore] Recovered " << recovered
                  << " records from WAL\n";
    }
    if (!ok) {
        std::cerr << "[KVStore] ERROR: WAL could not be re-opened after recovery — "
                  << "all subsequent writes will fail. Check disk permissions.\n";
    }
}

// ─── STATS ────────────────────────────────────────────────────────────────────

KVStore::Stats KVStore::stats() const {
    std::lock_guard<std::mutex> lock(mu_);
    Stats s = stats_;
    s.cache_hits  = cache_.hits();
    s.cache_miss  = cache_.misses();
    s.sstable_cnt = static_cast<int>(sstables_.size());
    return s;
}

std::string KVStore::next_sstable_path() {
    return config_.db_path + "/sst_" +
           std::to_string(++sstable_counter_) + ".sst";
}
