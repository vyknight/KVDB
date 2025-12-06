//
// Created by K on 2025-12-06.
//

#ifndef KVDB_KVSTORE_H
#define KVDB_KVSTORE_H

#include "Memtable.h"
#include "SSTableReader.h"
#include "WriteAheadLog.h"
#include <string>
#include <vector>
#include <map>
#include <memory>
#include <optional>
#include <mutex>

class KVStore
{
public:
    /**
     * Open or create a database
     * @param db_name name of db (affects direct name)
     * @param memtable_size max memtable size in bytes
     * @return true if successful, false otherwise
     */
    static std::unique_ptr<KVStore> open(const std::string& db_name, size_t memtable_size = 4096);  //default 4kb

    /**
     * Close the database and flush memtable
     */
    void close();

    /**
     * Insert or update a KV pair
     * @return true if successful false otherwise
     */
    bool put(const std::string& key, const std::string& value);

    /**
     * Get value for a key
     * @return value if found empty optional otherwise
     */
    std::optional<std::string> get(const std::string& key);

    /**
     * Delete a key (adds tombstone)
     * @return true if successful false otherwise
     */
    bool remove(const std::string& key);

    /**
     * Scan range of keys [start_key, end_key]
     * @param start_key inclusive start
     * @param end_key inclusive end
     */
    std::vector<std::pair<std::string, std::string>> scan(const std::string& start_key, const std::string& end_key);

    /**
     * Get database statistics
     */
    struct KVDBStats
    {
        uint64_t puts = 0;
        uint64_t gets = 0;
        uint64_t deletes = 0;
        uint64_t scans = 0;
        uint64_t memtable_flushes = 0;
        uint64_t sst_files = 0;
        size_t total_data_size = 0;
    };

    /**
     * @return KVDBStats struct
     */
    KVDBStats get_stats() const;

    /**
     * Force flush current memtable
     */
    void flush_memtable();

    /**
     * Get DB directory path
     */
    std::string get_db_path() const;

    // Disable copying
    KVStore(const KVStore&) = delete;
    KVStore& operator=(const KVStore&) = delete;

    // enable moving
    KVStore(KVStore&&) = default;
    KVStore& operator=(KVStore&&) = default;

private:
    /**
     * Private constructor that uses the public static open() method
     */
    KVStore(const std::string& db_path, size_t memtable_size=4096);  // default size 4KB, redundant

    // 2 phase initialization
    bool initialize();

    /**
     * Check if memtable needs flushing
     */
    void check_and_flush_memtable();

    /**
     * Flush current memtable to sstable
     * @return true if successful false otherwise
     */
    bool flush_memtable_internal();

    /**
     *Load existing SSTables from DB directory
     */
    void load_existing_sstables();

    /**
     * Recover from WAL if exists
     */
    void recover_from_wal();

    /**
     * Generate unique SSTable filename
     */
    std::string generate_sst_filename() const;

    /**
     * Search all SSTables for a key (newest to oldest)
     */
    std::optional<std::string> search_sstables(const std::string& key) const;

    /**
     * Scan all SSTables for a key range
     */
    void scan_sstables(const std::string& start_key,
        const std::string& end_key,
        std::map<std::string, std::string>& results) const;

    // private member vars
    std::string db_path_;
    size_t memtable_size_;
    Memtable memtable_;
    std::unique_ptr<WriteAheadLog> wal_;
    std::vector<std::unique_ptr<SSTableReader>> sstables_;
    mutable std::mutex mutex_;  // for thread safety
    mutable KVDBStats stats_;
    uint64_t sst_counter_;  // for unique SSTable naming
};

#endif //KVDB_KVSTORE_H