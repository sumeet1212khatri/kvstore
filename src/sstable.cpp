#include "sstable.h"
#include <fstream>
#include <filesystem>
#include <stdexcept>
#include <sys/stat.h>

// SSTable binary format:
// [num_entries: 4B]
// per entry: [is_deleted:1B][key_len:4B][key][val_len:4B][val]
// (bloom filter is rebuilt in memory from keys on load — not stored on disk)

static void write_str(std::ofstream& f, const std::string& s) {
    uint32_t len = static_cast<uint32_t>(s.size());
    f.write(reinterpret_cast<const char*>(&len), 4);
    f.write(s.data(), len);
}

static std::string read_str(std::ifstream& f) {
    // Bug 1 fix: guard against truncated/corrupted SSTable files.
    // Old code: read len with no check → garbage len (e.g. 0xDEADBEEF) →
    // std::string(4GB, '\0') → std::bad_alloc → process crash.
    // New code: check every read, sanity-cap len at 10MB.
    uint32_t len = 0;
    if (!f.read(reinterpret_cast<char*>(&len), 4)) return {};
    constexpr uint32_t MAX_LEN = 10u * 1024u * 1024u; // 10 MB sanity cap
    if (len > MAX_LEN) { f.setstate(std::ios::failbit); return {}; }
    std::string s(len, '\0');
    if (!f.read(s.data(), len)) return {};
    return s;
}

bool SSTable::write(const std::string& path,
                    const std::map<std::string, std::string>& memtable,
                    const std::map<std::string, bool>& deleted_keys) {
    // Bug 2 fix: write to "<path>.tmp" first, then atomically rename to <path>.
    // Old code: opened <path> directly — if disk full mid-write, a partial
    // .sst file was left on disk. On restart, directory_iterator picked it up,
    // load_bloom() read garbage num → Bug 1 crash.
    // New code: partial writes land in .tmp which is cleaned up on failure.
    // rename() is atomic on POSIX — reader never sees a half-written file.
    const std::string tmp_path = path + ".tmp";
    {
        std::ofstream f(tmp_path, std::ios::binary | std::ios::trunc);
        if (!f.is_open()) return false;

        std::map<std::string, SSTable::Entry> merged;
        for (auto& [k, v] : memtable) merged[k] = {k, v, false};
        for (auto& [k, _] : deleted_keys) merged[k] = {k, "", true};

        uint32_t actual_num = static_cast<uint32_t>(merged.size());
        BloomFilter bloom(actual_num > 0 ? actual_num : 100, 0.01);

        f.write(reinterpret_cast<const char*>(&actual_num), 4);

        for (auto& [k, entry] : merged) {
            uint8_t del = entry.is_deleted ? 1 : 0;
            f.write(reinterpret_cast<const char*>(&del), 1);
            write_str(f, entry.key);
            write_str(f, entry.value);
            bloom.add(entry.key);
        }

        // Bloom filter rebuilt from keys on load — not stored on disk.
        f.flush();
        if (!f.good()) {
            f.close();
            std::filesystem::remove(tmp_path); // clean up partial .tmp
            return false;
        }
    } // ofstream closes here

    // Atomic rename: tmp → final path
    std::error_code ec;
    std::filesystem::rename(tmp_path, path, ec);
    if (ec) {
        std::filesystem::remove(tmp_path);
        return false;
    }
    return true;
}

SSTable::SSTable(const std::string& path) : path_(path), bloom_(100, 0.01) {
    load_bloom();
}

// Sanity cap for num_entries — prevents bad_alloc on corrupt files.
// 10M entries × ~50 bytes avg = ~500MB, well above any realistic SSTable.
static constexpr uint32_t MAX_ENTRIES = 10'000'000u;

void SSTable::load_bloom() {
    // Reads entry count, sizes bloom correctly, adds keys.
    // Seeks past value bytes (Bug 9 fix — no wasted allocation).
    // Validates every read — returns early on any corruption (Bug new).
    std::ifstream f(path_, std::ios::binary);
    if (!f.is_open()) return;

    uint32_t num = 0;
    if (!f.read(reinterpret_cast<char*>(&num), 4)) return; // truncated file
    if (num > MAX_ENTRIES) return;                          // corrupt num

    bloom_ = BloomFilter(num > 0 ? num : 100, 0.01);

    for (uint32_t i = 0; i < num; i++) {
        if (!f.good()) break;
        uint8_t del = 0;
        if (!f.read(reinterpret_cast<char*>(&del), 1)) break;

        std::string key = read_str(f); // read_str already validates MAX_LEN
        if (!f.good()) break;

        uint32_t val_len = 0;
        if (!f.read(reinterpret_cast<char*>(&val_len), 4)) break;
        constexpr uint32_t MAX_VAL = 10u * 1024u * 1024u; // 10 MB cap
        if (val_len > MAX_VAL) break;                      // corrupt val_len
        f.seekg(val_len, std::ios::cur);                   // skip value bytes

        // Add key to bloom — empty string keys are valid and must be included.
        // Old code: `if (!key.empty()) bloom_.add(key)` — skipped empty keys,
        // causing false negatives: bloom_.possibly_contains("") = false even
        // when "" was stored, forcing every get("") to bypass bloom incorrectly.
        bloom_.add(key);
    }
}

std::vector<SSTable::Entry> SSTable::read_all() {
    std::ifstream f(path_, std::ios::binary);
    if (!f.is_open()) return {};

    uint32_t num = 0;
    if (!f.read(reinterpret_cast<char*>(&num), 4)) return {};
    if (num > MAX_ENTRIES) return {}; // corrupt num — don't reserve 4B entries

    std::vector<Entry> result;
    result.reserve(num); // safe: num <= MAX_ENTRIES (10M)
    for (uint32_t i = 0; i < num; i++) {
        if (!f.good()) break;
        uint8_t del = 0;
        if (!f.read(reinterpret_cast<char*>(&del), 1)) break;
        std::string key = read_str(f);
        std::string val = read_str(f);
        // Fix: check f.good() alone — old code checked `&& key.empty()` which
        // was wrong. If key read succeeds (non-empty) but value read fails,
        // old code pushed a corrupt {key, "", del} entry instead of stopping.
        // Empty string keys are valid (len=0 reads fine → f.good() stays true).
        if (!f.good()) break;
        result.push_back({key, val, del == 1});
    }
    return result;
}

// Full get with bloom check
std::optional<SSTable::Entry> SSTable::get(const std::string& key) {
    if (!bloom_.possibly_contains(key)) return std::nullopt;
    return get_without_bloom(key);
}

// Bug 2 fix: linear scan only, bloom already checked by caller
std::optional<SSTable::Entry> SSTable::get_without_bloom(const std::string& key) {
    auto entries = read_all();
    for (auto& e : entries) {
        if (e.key == key) return e;
        if (e.key > key) break; // sorted — stop early
    }
    return std::nullopt;
}

uint64_t SSTable::file_size() const {
    struct stat st;
    if (stat(path_.c_str(), &st) == 0) return st.st_size;
    return 0;
}
