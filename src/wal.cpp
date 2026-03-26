#include "wal.h"
#include <stdexcept>
#include <fstream>
#include <unistd.h>   // Bug 5 fix: ::fsync(), ::close()
#include <fcntl.h>    // Bug 5 fix: ::open(), O_WRONLY, O_APPEND

WAL::WAL(const std::string& path) : path_(path) {
    file_.open(path, std::ios::in | std::ios::out | std::ios::binary | std::ios::app);
    if (!file_.is_open()) {
        std::ofstream create(path, std::ios::binary); create.close();
        file_.open(path, std::ios::in | std::ios::out | std::ios::binary | std::ios::app);
    }
    if (!file_.is_open()) throw std::runtime_error("WAL: cannot open: " + path);
}

WAL::~WAL() { if (file_.is_open()) file_.close(); }

bool WAL::log_put(const std::string& key, const std::string& value) {
    return write_record({OpType::PUT, key, value});
}

bool WAL::log_delete(const std::string& key) {
    return write_record({OpType::DEL, key, ""});
}

bool WAL::write_record(const Record& rec) {
    uint8_t  op      = static_cast<uint8_t>(rec.op);
    uint32_t key_len = static_cast<uint32_t>(rec.key.size());
    uint32_t val_len = static_cast<uint32_t>(rec.value.size());
    std::string buf;
    buf.reserve(1 + 4 + key_len + 4 + val_len);
    buf.append(reinterpret_cast<const char*>(&op),      1);
    buf.append(reinterpret_cast<const char*>(&key_len), 4);
    buf.append(rec.key);
    buf.append(reinterpret_cast<const char*>(&val_len), 4);
    buf.append(rec.value);
    file_.write(buf.data(), static_cast<std::streamsize>(buf.size()));
    // Bug 5 fix: file_.flush() only pushes C++ buffer → OS kernel buffer.
    // Power failure = WAL record lost. fsync() forces kernel → physical disk.
    file_.flush();
    if (!file_.good()) {
        file_.clear(); // Bug 3 fix: reset error flags so stream can recover
        return false;
    }
    // fsync for true durability. Open a separate fd (POSIX-safe approach:
    // fsync flushes all dirty pages for the inode, not just this fd).
    // If open fails (e.g. fd limit), treat as write failure — do NOT silently
    // claim durability. Bug fix: old code ignored open() failure and returned true.
    int fd = ::open(path_.c_str(), O_WRONLY | O_APPEND);
    if (fd < 0) return false; // can't fsync → durability not guaranteed
    int sync_result = ::fsync(fd);
    ::close(fd);
    if (sync_result != 0) return false; // fsync itself failed
    return true;
}

bool WAL::recover(std::function<void(const Record&)> apply_fn) {
    file_.close();
    std::fstream reader(path_, std::ios::in | std::ios::binary);
    if (!reader.is_open()) return false;
    Record rec;
    while (reader.peek() != EOF) {
        if (!read_record(reader, rec)) break;
        apply_fn(rec);
    }
    reader.close();
    file_.open(path_, std::ios::in | std::ios::out | std::ios::binary | std::ios::app);
    return file_.is_open();
}

bool WAL::read_record(std::fstream& f, Record& rec) {
    // Validate every field — corrupt WAL must not cause OOM or crash.
    uint8_t  op;
    uint32_t key_len, val_len;
    if (!f.read(reinterpret_cast<char*>(&op), 1)) return false;

    // Validate op byte BEFORE casting to enum.
    // Old code: static_cast<OpType>(garbage_byte) → any invalid byte falls into
    // the else-branch in recover_from_wal() and is treated as DELETE — silently
    // deleting keys that were never actually deleted. Casting an out-of-range
    // value to a scoped enum is also undefined behaviour in C++.
    if (op != static_cast<uint8_t>(OpType::PUT) &&
        op != static_cast<uint8_t>(OpType::DEL)) {
        return false; // corrupt record — stop replay here
    }
    rec.op = static_cast<OpType>(op);

    if (!f.read(reinterpret_cast<char*>(&key_len), 4)) return false;
    constexpr uint32_t MAX_LEN = 10u * 1024u * 1024u;
    if (key_len > MAX_LEN) return false;

    rec.key.resize(key_len);
    if (!f.read(rec.key.data(), key_len)) return false;

    if (!f.read(reinterpret_cast<char*>(&val_len), 4)) return false;
    if (val_len > MAX_LEN) return false;

    rec.value.resize(val_len);
    if (val_len > 0 && !f.read(rec.value.data(), val_len)) return false;
    return true;
}

bool WAL::clear() {
    file_.close();
    std::ofstream trunc(path_, std::ios::trunc | std::ios::binary); trunc.close();
    file_.open(path_, std::ios::in | std::ios::out | std::ios::binary | std::ios::app);
    return file_.is_open();
}
