#include "bloom_filter.h"

// Auto-sized constructor (recommended)
BloomFilter::BloomFilter(size_t expected_keys, double fp_rate)
    : fp_rate_(fp_rate) {
    // Guard against expected_keys=0:
    //   optimal_bits(0, p) = 0  → size_bits_=0 → get_bit_index does (h % 0) → crash
    //   optimal_hashes(0, 0) = max(1, round(0.0/0)) = max(1, round(nan)) →
    //   static_cast<int>(nan) = undefined behaviour
    // Fix: treat 0 keys same as 1 key — results in a tiny but non-zero filter.
    size_t safe_n   = (expected_keys == 0) ? 1 : expected_keys;
    size_bits_  = optimal_bits(safe_n, fp_rate);
    num_hashes_ = optimal_hashes(size_bits_, safe_n);
    bits_.assign(size_bits_, false);
}

// Manual constructor (for legacy / testing)
BloomFilter::BloomFilter(size_t size_bits, int num_hashes, bool)
    : bits_(size_bits, false),
      size_bits_(size_bits),
      num_hashes_(num_hashes),
      fp_rate_(0.0) {}

void BloomFilter::add(const std::string& key) {
    for (int i = 0; i < num_hashes_; i++)
        bits_[get_bit_index(key, i)] = true;
}

bool BloomFilter::possibly_contains(const std::string& key) const {
    for (int i = 0; i < num_hashes_; i++)
        if (!bits_[get_bit_index(key, i)]) return false;
    return true;
}

void BloomFilter::reset() {
    std::fill(bits_.begin(), bits_.end(), false);
}

uint64_t BloomFilter::hash1(const std::string& key) const {
    // FNV-1a
    uint64_t h = 14695981039346656037ULL;
    for (unsigned char c : key) { h ^= c; h *= 1099511628211ULL; }
    return h;
}

uint64_t BloomFilter::hash2(const std::string& key) const {
    // djb2 xor
    uint64_t h = 5381;
    for (unsigned char c : key) h = ((h << 5) + h) ^ c;
    return h | 1ULL;  // force odd — coprime with 2^n
}

size_t BloomFilter::get_bit_index(const std::string& key, int i) const {
    uint64_t h1 = hash1(key);
    uint64_t h2 = hash2(key);
    return static_cast<size_t>(
        (h1 + static_cast<uint64_t>(i) * h2) % size_bits_);
}
