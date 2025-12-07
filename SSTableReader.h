//
// Created by K on 2025-12-04.
//

#ifndef KVDB_SSTABLEREADER_H
#define KVDB_SSTABLEREADER_H

#include <string>
#include <vector>
#include <optional>
#include <memory>
#include "BufferPool.h"

class SSTableReader
{
public:
    explicit SSTableReader(std::string  filename);
    ~SSTableReader();

    // no copying
    SSTableReader(const SSTableReader&) = delete;
    SSTableReader& operator=(const SSTableReader&) = delete;

    // Move support
    SSTableReader(SSTableReader&& other) noexcept;
    SSTableReader& operator=(SSTableReader&& other) noexcept;

    /**
     * Get val for a key using binary search
     * @return Value if found and not deleted, empty optional otherwise
     */
    [[nodiscard]] std::optional<std::string> get(const std::string& key) const;

    /**
     * Check if key exists (and not deleted)
     */
    [[nodiscard]] bool contains(const std::string& key) const;

    /**
     * Check if key exists and is marked deleted
     */
    [[nodiscard]] bool is_deleted(const std::string& key) const;

    /**
     * Get number of entries in SSTable
     */
    [[nodiscard]] size_t size() const;

    /**
     * Get filename
     */
    [[nodiscard]] const std::string& get_filename() const;

    /**
     * Validate SSTable file format
     */
    [[nodiscard]] bool is_valid() const;

    /**
     * Get approximate memory usage of loaded SSTable
     */
    [[nodiscard]] size_t memory_usage() const;

    /**
     * Get all keys for debugging or testing
     */
    [[nodiscard]] std::vector<std::string> get_all_keys() const;

    /**
     * Get minimum key
     */
    [[nodiscard]] std::string min_key() const;

    /**
     * Get maximum key
     */
    [[nodiscard]] std::string max_key() const;

    /**
     * Scans the SSTable for entries with keys in the range
     * @param start_key inclusive
     * @param end_key inclusive
     * @return vector with range of matching entries
     */
    [[nodiscard]] std::vector<std::pair<std::string, std::string>> scan_range(const std::string& start_key, const std::string& end_key) const;

    /**
     * Sets a static buffer pool that's shared and used by every reader
     * @param pool a BufferPool object
     */
    static void set_buffer_pool(std::unique_ptr<BufferPool> pool)
    {
        buffer_pool_ = std::move(pool);
    }

    static BufferPool* get_buffer_pool()
    {
        return buffer_pool_.get();
    }

private:
    struct KeyEntry
    {
        std::string key;
        uint64_t value_offset;
        uint32_t value_length;
        bool is_deleted;

        // for sorting and binary search
        bool operator<(const std::string& other) const { return key < other; }
        bool operator==(const std::string& other) const { return key == other; }
    };

    std::string filename_;
    std::vector<KeyEntry> key_entries_;  // sorted for binary search
    std::unique_ptr<char[]> value_data_;  // memory mapped or loaded values
    size_t value_data_size_;
    bool valid_;

    /**
     * Binary search for key in key entries
     */
    [[nodiscard]] int binary_search(const std::string& key) const;

    /**
     * Load SSTable file
     */
    bool load();

    [[nodiscard]] std::string read_value(const KeyEntry& entry) const;

    // buffer pool implementation
    static std::unique_ptr<BufferPool> buffer_pool_;
    std::string read_value_with_buffer_pool(const KeyEntry& entry) const;
};

#endif //KVDB_SSTABLEREADER_H