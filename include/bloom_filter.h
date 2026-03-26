#pragma once
#include <vector>
#include <string>
#include <cstdint>
#include <cmath>

// BloomFilter — probabilistic existence check
//
// Sizing formula (standard):
//   m = -n * ln(p) / (ln2)^2   (bits needed)
//   k = (m/n) * ln2             (optimal hash count)
//
// bits/key guide:
//   6  bits/key → ~4%  FP
//   8  bits/key → ~2%  FP
//   10 bits/key → ~1%  FP    ← default
//   14 bits/key → ~0.3% FP
//
// Usage:
//   BloomFilter bf(expected_keys, 0.01);  // 1% FP
class BloomFilter {
public:
    // Constructor: auto-size based on expected keys + target FP rate
    // Default: 10 bits/key → ~1% false positive rate
    explicit BloomFilter(size_t expected_keys = 100000,
                         double  fp_rate      = 0.01);

    // Legacy constructor: manual bit size (for testing)
    BloomFilter(size_t size_bits, int num_hashes, bool /*manual*/);

    void   add(const std::string& key);
    bool   possibly_contains(const std::string& key) const;
    void   reset();

    size_t bit_count()   const { return size_bits_; }
    int    hash_count()  const { return num_hashes_; }
    double fp_rate()     const { return fp_rate_; }

private:
    std::vector<bool> bits_;
    size_t            size_bits_;
    int               num_hashes_;
    double            fp_rate_;

    static size_t optimal_bits(size_t n, double p) {
        return static_cast<size_t>(
            std::ceil(-static_cast<double>(n) * std::log(p)
                      / (std::log(2.0) * std::log(2.0))));
    }
    static int optimal_hashes(size_t m, size_t n) {
        return std::max(1, static_cast<int>(
            std::round(static_cast<double>(m) / n * std::log(2.0))));
    }

    uint64_t hash1(const std::string& key) const;
    uint64_t hash2(const std::string& key) const;
    size_t   get_bit_index(const std::string& key, int i) const;
};
