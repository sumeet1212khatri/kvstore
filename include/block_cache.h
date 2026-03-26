#pragma once
#include <string>
#include <unordered_map>
#include <list>
#include <optional>
#include <mutex>

// BlockCache — LRU Cache
// Recently padhe keys memory mein rakho
// Agar wahi key fir se maangi jaaye — disk mat jao, seedha yahan se do
// LRU = Least Recently Used eviction policy

class BlockCache {
public:
    explicit BlockCache(size_t capacity = 1000);

    void put(const std::string& key, const std::string& value);
    std::optional<std::string> get(const std::string& key);
    void remove(const std::string& key);
    void clear();

    size_t size() const { return cache_map_.size(); }
    size_t capacity() const { return capacity_; }

    // Stats
    uint64_t hits() const { return hits_; }
    uint64_t misses() const { return misses_; }
    double hit_rate() const;

private:
    size_t capacity_;
    mutable uint64_t hits_   = 0;
    mutable uint64_t misses_ = 0;

    // LRU implementation: doubly linked list + hashmap
    // List front = most recently used
    std::list<std::pair<std::string, std::string>> lru_list_;
    std::unordered_map<std::string,
        std::list<std::pair<std::string,std::string>>::iterator> cache_map_;

    void evict_lru();
};
