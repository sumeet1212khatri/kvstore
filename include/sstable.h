#pragma once
#include "bloom_filter.h"
#include <string>
#include <map>
#include <vector>
#include <optional>
#include <cstdint>

class SSTable {
public:
    struct Entry {
        std::string key;
        std::string value;
        bool is_deleted;
    };

    static bool write(const std::string& path,
                      const std::map<std::string, std::string>& memtable,
                      const std::map<std::string, bool>& deleted_keys);

    explicit SSTable(const std::string& path);

    // Full get: bloom check + linear scan
    std::optional<Entry> get(const std::string& key);

    // Bug 2 fix: expose bloom check separately so KVStore tracks skips correctly
    bool bloom_possibly_contains(const std::string& key) const {
        return bloom_.possibly_contains(key);
    }

    // Bug 2 fix: linear scan only (bloom already checked by caller)
    std::optional<Entry> get_without_bloom(const std::string& key);

    std::vector<Entry> read_all();

    const std::string& path() const { return path_; }
    uint64_t file_size() const;

private:
    std::string path_;
    BloomFilter bloom_{1000, 0.01};

    // Bug 1 fix: load bloom correctly sized for actual entry count
    void load_bloom();
};
