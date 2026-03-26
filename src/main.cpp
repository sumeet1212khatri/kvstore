#include "kvstore.h"
#include <iostream>
#include <string>
#include <iomanip>

// ─── CLI Usage ────────────────────────────────────────────────────────────────
// ./kvstore set <key> <value>
// ./kvstore get <key>
// ./kvstore delete <key>
// ./kvstore stats
// ./kvstore flush
// ./kvstore compact

void print_usage() {
    std::cout << "Usage:\n"
              << "  kvstore set <key> <value>   -- store a key-value pair\n"
              << "  kvstore get <key>            -- retrieve a value\n"
              << "  kvstore delete <key>         -- delete a key\n"
              << "  kvstore stats               -- show engine stats\n"
              << "  kvstore flush               -- flush memtable to disk\n"
              << "  kvstore compact             -- compact all SSTables\n";
}

void print_stats(const KVStore::Stats& s) {
    std::cout << "\n=== KVStore Stats ===\n"
              << "  Writes      : " << s.writes      << "\n"
              << "  Reads       : " << s.reads        << "\n"
              << "  Deletes     : " << s.deletes      << "\n"
              << "  Cache hits  : " << s.cache_hits   << "\n"
              << "  Cache miss  : " << s.cache_miss   << "\n"
              << "  Bloom skips : " << s.bloom_skip   << "\n"
              << "  SSTables    : " << s.sstable_cnt  << "\n";
    uint64_t total = s.cache_hits + s.cache_miss;
    if (total > 0) {
        double rate = 100.0 * s.cache_hits / total;
        std::cout << std::fixed << std::setprecision(1)
                  << "  Cache rate  : " << rate << "%\n";
    }
    std::cout << "====================\n";
}

int main(int argc, char* argv[]) {
    if (argc < 2) { print_usage(); return 1; }

    KVConfig cfg;
    cfg.db_path = "./kvdata";
    KVStore db(cfg);

    std::string cmd = argv[1];

    if (cmd == "set") {
        if (argc < 4) { std::cerr << "Error: set needs <key> <value>\n"; return 1; }
        std::string key   = argv[2];
        std::string value = argv[3];
        if (db.put(key, value)) {
            std::cout << "OK\n";
        } else {
            std::cerr << "Error: write failed\n"; return 1;
        }

    } else if (cmd == "get") {
        if (argc < 3) { std::cerr << "Error: get needs <key>\n"; return 1; }
        auto val = db.get(argv[2]);
        if (val.has_value()) {
            std::cout << val.value() << "\n";
        } else {
            std::cout << "(nil)\n"; return 1;
        }

    } else if (cmd == "delete") {
        if (argc < 3) { std::cerr << "Error: delete needs <key>\n"; return 1; }
        if (db.del(argv[2])) {
            std::cout << "OK\n";
        } else {
            std::cerr << "Error: delete failed\n"; return 1;
        }

    } else if (cmd == "stats") {
        print_stats(db.stats());

    } else if (cmd == "flush") {
        db.flush();
        std::cout << "Flushed to SSTable\n";

    } else if (cmd == "compact") {
        db.compact();
        std::cout << "Compaction done\n";

    } else {
        std::cerr << "Unknown command: " << cmd << "\n";
        print_usage();
        return 1;
    }

    return 0;
}
