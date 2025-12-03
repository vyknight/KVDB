//
// Created by K on 2025-12-03.
//

#ifndef KVDB_MEMTABLE_H
#define KVDB_MEMTABLE_H

// Allowed to use std::map since this project is completed solo.
#include <map>
#include <string>
#include <utility>
#include <vector>
#include <optional>
#include <cstddef>
#include <cstdint>

class Memtable
{
public:
    // Entry struct for ea. pair
    struct Entry
    {
        std::string value;
        bool is_deleted;

        explicit Entry(std::string val="", const bool deleted=false)
            : value(std::move(val)), is_deleted(deleted) {}
    };

private:
    std::map<std::string, Entry> table_;
    size_t current_size_;
    size_t max_size_;

    /**
     * Calculate memory footprint of KV pair
     * Includes size of key, value, and struct overhead
     */
    [[nodiscard]] size_t calculate_entry_size(const std::string& key, const std::string& value) const;

public:
    /**
     * Constructor
     * @param memtable_size Maximum size in bytes before table is flushed
     */
    explicit Memtable(size_t memtable_size = 4 * 1024);  // Default 4KB

    /**
     * Insert or update key value pair
     * @return true if successful, false if memtable full (triggers flush)
     */
    bool put(const std::string& key, const std::string& value);

    /**
     * Mark a key as deleted (with tombstone)
     * @return true if successful, false if table is full
     */
    bool remove (const std::string& key);

    /**
     * Get value for a key
     * @return std::optional containing value if key found, empty optional if key doesn't exist or is deleted
     */
    [[nodiscard]] std::optional<std::string> get(const std::string& key) const;

    /**
     * Check if key exists and isn't deleted
     */
    [[nodiscard]] bool contains(const std::string& key) const;

    /**
     * Check if key is marked as deleted
     * @return true if key exists AND is marked as deleted
     */
    [[nodiscard]] bool is_deleted(const std::string& key) const;

    /**
     * Get current size of memtable in bytes
     * @return size in bytes
     */
    [[nodiscard]] size_t size() const;

    /**
     * Get current number of entries in memtable
     * @return Number of KV pairs stored
     */
    [[nodiscard]] size_t entry_count() const;

    /**
     * Check if memtable should be flushed
     * @return true if current_size >= max_size
     */
    [[nodiscard]] bool should_flush() const;

    /**
     * Clear memtable (after data has been successfully flushed)
     */
    void clear();

    /**
     * Get all entries sorted by key for flushing to SSTable
     * @return Vector of all KV pairs sorted by key
     */
    [[nodiscard]] std::vector<std::pair<std::string, Entry>> get_all_entries() const;

    /**
     * Set iterator to beginning for range scans
     */
    [[nodiscard]] std::map<std::string, Entry>::const_iterator start() const;

    /**
     *Get iterator to end for range scans
     */
    [[nodiscard]] std::map<std::string, Entry>::const_iterator end() const;

    /**
     * Get approximate memory usage
     * @return Map with breakdown of memory usage
     */
    [[nodiscard]] std::map<std::string, size_t> get_memory_usage() const;

    /**
     * Set new max size
     * @param new_memtable_size new size in bytes
     */
    void set_new_memtable_size(size_t new_memtable_size);

    /**
     * Get maximum allowed size
     * @return Maximum size in bytes
     */
    [[nodiscard]] size_t get_memtable_size() const;

    // Statistics for debugging
    struct Stats
    {
        uint64_t puts = 0;
        uint64_t deletes = 0;
        uint64_t gets = 0;
        uint64_t flushes = 0;
        uint64_t operations = 0;
    };

    [[nodiscard]] Stats get_stats() const;

    void reset_stats();

private:
    Stats stats_;

};

#endif //KVDB_MEMTABLE_H