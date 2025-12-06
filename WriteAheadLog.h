//
// Created by K on 2025-12-06.
//

#ifndef KVDB_WRITEAHEADLOG_H
#define KVDB_WRITEAHEADLOG_H

#include <string>
#include <vector>
#include <fstream>
#include <cstdint>

class WriteAheadLog {
public:
    enum class OpType : uint8_t {
        PUT = 0,
        DELETE = 1
    };

    struct LogEntry {
        OpType type;
        std::string key;
        std::string value;

        LogEntry(OpType t, std::string k, std::string v)
            : type(t), key(std::move(k)), value(std::move(v)) {}
    };

    static constexpr uint64_t MAGIC = 0x57414C5F53454D44ULL; // "WAL_SEMD"
    static constexpr uint32_t VERSION = 1;

    explicit WriteAheadLog(const std::string& filename);
    ~WriteAheadLog();

    // Disable copying
    WriteAheadLog(const WriteAheadLog&) = delete;
    WriteAheadLog& operator=(const WriteAheadLog&) = delete;

    // Move operations
    WriteAheadLog(WriteAheadLog&& other) noexcept;
    WriteAheadLog& operator=(WriteAheadLog&& other) noexcept;

    // Core operations
    bool log_put(const std::string& key, const std::string& value);
    bool log_delete(const std::string& key);

    // File operations
    std::vector<LogEntry> read_all_entries() const;
    void clear();
    size_t size() const;
    bool is_open() const;
    const std::string& get_filename() const;

    // Utility functions
    bool recover(std::vector<LogEntry>& entries) const;

private:
    std::string filename_;
    mutable std::fstream file_;  // mutable for const operations

    bool open_file();
    bool write_header(uint32_t entry_count);
    bool read_header(uint32_t& entry_count) const;
    bool write_entry(OpType type, const std::string& key, const std::string& value);

    // Internal helpers
    bool seek_to_entry_start() const;
    bool validate_file() const;
};


#endif //KVDB_WRITEAHEADLOG_H