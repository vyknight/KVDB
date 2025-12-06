//
// Created by K on 2025-12-06.
//

#ifndef KVDB_WRITEAHEADLOG_H
#define KVDB_WRITEAHEADLOG_H

#include <string>
#include <fstream>
#include <vector>
#include <cstdint>

/**
 * Ensures data is persisted to disk before acknowledging writes
 */
class WriteAheadLog
{
public:
    // operation types
    enum class OpType : uint8_t
    {
        PUT = 1,
        DELETE = 2
    };

    // Log entry structure
    struct LogEntry
    {
        OpType type;
        std::string key;
        std::string value;  // empty if OpType == DELETE

        LogEntry(OpType t, std::string k, std::string v = "")
            : type(t), key(std::move(k)), value(std::move(v)) {}
    };

    /** Constructor - opens or creates WAL file
     * @param filename WAL file path
     */
    explicit WriteAheadLog(const std::string& filename);

    /**
     * Destructor, makes sure that the file is closed
     */
    ~WriteAheadLog();

    // No copying
    WriteAheadLog(const WriteAheadLog&) = delete;
    WriteAheadLog& operator=(const WriteAheadLog&) = delete;

    /**
     * Log a put operation
     * @return true if successful false otherwise
     */
    bool log_put(const std::string & key, const std::string& value);

    /**
     * Log a delete operation
     * @return true if successful false otherwise
     */
    bool log_delete(const std::string& key);

    /**
     * Read all entries of WAL for recovery
     * @return Vector of log entries, in order
     */
    [[nodiscard]] std::vector<LogEntry> read_all_entries() const;

    /**
     * Clear the WAL after a successful memtable flush and create new WAL file
     */
    void clear();

    /**
     * Get current size of WAL in bytes
     */
    [[nodiscard]] size_t size() const;

    /**
     * Check if WAL is open and ready
     */
    [[nodiscard]] bool is_open() const;

    /**
     * Get filename
     */
    [[nodiscard]] const std::string& get_filename() const;

private:
    static constexpr uint64_t MAGIC = 0x4B5644425F57414CULL;  // "KVDB_WAL"
    static constexpr uint32_t VERSION = 1;

    std::string filename_;
    mutable std::fstream file_;  // for const read ops

    /**
     * Write a single log entry
     */
    bool write_entry(OpType type, const std::string& key, const std::string& value="");

    /**
     * Read header from file
     */
    bool read_header(uint32_t& entry_count) const;

    /**
     * write_header to file
     */
    bool write_header(uint32_t entry_count);

    /**
     * Open file with correct mode
     */
    bool open_file();
};

#endif //KVDB_WRITEAHEADLOG_H