#include "block_cache.h"

BlockCache::BlockCache(size_t capacity) : capacity_(capacity) {}

void BlockCache::put(const std::string& key, const std::string& value) {
    auto it = cache_map_.find(key);
    if (it != cache_map_.end()) {
        // Already hai — front pe le aao (most recently used)
        lru_list_.erase(it->second);
        cache_map_.erase(it);
    }
    lru_list_.push_front({key, value});
    cache_map_[key] = lru_list_.begin();

    // Capacity bhar gayi — sabse purana nikalo
    if (cache_map_.size() > capacity_) evict_lru();
}

std::optional<std::string> BlockCache::get(const std::string& key) {
    auto it = cache_map_.find(key);
    if (it == cache_map_.end()) {
        misses_++;
        return std::nullopt;
    }
    hits_++;
    // Front pe le aao — recently used
    lru_list_.splice(lru_list_.begin(), lru_list_, it->second);
    return it->second->second;
}

void BlockCache::remove(const std::string& key) {
    auto it = cache_map_.find(key);
    if (it != cache_map_.end()) {
        lru_list_.erase(it->second);
        cache_map_.erase(it);
    }
}

void BlockCache::clear() {
    lru_list_.clear();
    cache_map_.clear();
    hits_ = 0;
    misses_ = 0;
}

void BlockCache::evict_lru() {
    if (lru_list_.empty()) return;
    auto& lru = lru_list_.back();
    cache_map_.erase(lru.first);
    lru_list_.pop_back();
}

double BlockCache::hit_rate() const {
    uint64_t total = hits_ + misses_;
    if (total == 0) return 0.0;
    return static_cast<double>(hits_) / total * 100.0;
}
