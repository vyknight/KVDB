//
// Created by K on 2025-12-04.
//

#ifndef KVDB_SSTABLEREADER_H
#define KVDB_SSTABLEREADER_H

#include <string>
#include <vector>
#include <optional>
#include <memory>

class SSTableReader
{
public:
    explicit SSTableReader(const std::string& filename);
    ~SSTableReader();

    // no copying
    SSTableReader(const SSTableReader&) = delete;
    SSTableReader& operator=(const SSTableReader&) = delete;

    /**
     * Get val for a key using binary search
     * @return Value if found and not deleted, empty optional otherwise
     */
    std::optional<std::string> get(const std::string& key) const;

    /**
     * Check if key exists (and not deleted)
     */
    bool contains(std::string& key) const;

    /**
     * Check if key exists and is marked deleted
     */
    bool is_deleted(const std::string& key) const;

    /**
     * Get number of entries in SSTable
     */
    size_t size() const;

    /**
     * Get filename
     */
    const std::string& get_filename() const;

    /**
     * Validate SSTable file format
     */
    bool is_valid() const;

private:
    struct KeyEntry
    {
        std::string key;
        uint64_t value_offset;
        uint32_t value_length;
        bool is_deleted;
    };

    std::string filename_;
    std::vector<KeyEntry> key_entries_;  // sorted for binary search
    std::unique_ptr<char[]> value_data_;  // memory mapped or loaded values
    size_t value_data_size_;
    bool valid_;

    /**
     * Binary search for key in key entries
     */
    int binary_search(const std::string& key) const;

    /**
     * Load SSTable file
     */
    bool load();
};

#endif //KVDB_SSTABLEREADER_H