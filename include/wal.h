#pragma once
#include <string>
#include <fstream>
#include <functional>
#include <cstdint>

class WAL {
public:
    enum class OpType : uint8_t { PUT = 1, DEL = 2 };

    struct Record {
        OpType      op;
        std::string key;
        std::string value;
    };

    explicit WAL(const std::string& path);
    ~WAL();

    bool log_put(const std::string& key, const std::string& value);
    bool log_delete(const std::string& key);
    bool recover(std::function<void(const Record&)> apply_fn);
    bool clear();
    bool is_open() const { return file_.is_open(); }

private:
    std::string  path_;
    std::fstream file_;

    bool write_record(const Record& rec);
    bool read_record(std::fstream& f, Record& rec);
};
