#ifndef COMPACTOR_H
#define COMPACTOR_H

#include "SSTableReader.h"
#include "SSTableWriter.h"
#include "BufferPool.h"
#include <vector>
#include <memory>
#include <string>
#include <queue>
#include <functional>
#include <filesystem>

namespace fs = std::filesystem;

class Compactor {
public:
    // Configuration
    struct Config {
        size_t buffer_size;
        size_t max_merge_fan_in;
        bool remove_tombstones;

        Config(
            size_t buffer_size_ = 4096,
            size_t max_merge_fan_in_ = 10,
            bool remove_tombstones_ = true
        )
            : buffer_size(std::move(buffer_size_)),
              max_merge_fan_in(std::move(max_merge_fan_in_)),
              remove_tombstones(std::move(remove_tombstones_))
        {}
    };

    // Constructor
    Compactor(std::shared_ptr<BufferPool> buffer_pool, const Config& config = Config());

    // Compact multiple SSTables into one or more new SSTables
    std::vector<std::shared_ptr<SSTableReader>> compact(
        const std::vector<std::shared_ptr<SSTableReader>>& input_sstables,
        int target_level,
        bool is_largest_level);

    // Statistics
    struct Stats {
        size_t compactions_performed = 0;
        size_t entries_read = 0;
        size_t entries_written = 0;
        size_t tombstones_removed = 0;
        size_t duplicates_removed = 0;
        size_t bytes_read = 0;
        size_t bytes_written = 0;
    };

    Stats get_stats() const;

private:
    // Internal structures for merging
    struct MergeEntry {
        std::string key;
        std::string value;
        bool is_deleted;
        uint64_t timestamp;         // File modification time as sequence
        size_t source_index;        // Which SSTable it came from

        // For min-heap comparison: sort by key, then by timestamp (newer first)
        bool operator>(const MergeEntry& other) const {
            if (key != other.key) return key > other.key;
            return timestamp < other.timestamp;  // Older (smaller timestamp) comes first
        }
    };

    // Iterator for reading from SSTable
    class SSTableIterator {
    public:
        SSTableIterator(std::shared_ptr<SSTableReader> sstable, uint64_t timestamp);

        bool has_next() const;
        MergeEntry next();
        void reset();

    private:
        std::shared_ptr<SSTableReader> sstable_;
        uint64_t timestamp_;
        size_t current_index_;
        std::vector<std::string> all_keys_;
        bool eof_;
    };

    // Configuration
    std::shared_ptr<BufferPool> buffer_pool_;
    Config config_;

    // Statistics
    mutable Stats stats_;
    mutable std::mutex stats_mutex_;

    // Private methods
    std::vector<std::shared_ptr<SSTableReader>> merge_sstables(
        const std::vector<std::shared_ptr<SSTableReader>>& sstables,
        int target_level,
        bool is_largest_level);

    // Multi-way merge using min-heap and file timestamps
    void multiway_merge(
        const std::vector<std::shared_ptr<SSTableReader>>& sstables,
        const std::string& output_filename,
        bool is_largest_level);

    // Get file modification time as timestamp (nanoseconds since epoch)
    uint64_t get_file_timestamp(const std::string& filename) const;

    // Helper methods
    std::string generate_output_filename(int target_level, uint64_t timestamp);
    bool should_keep_entry(const MergeEntry& entry, bool is_largest_level);
    void update_stats_read(size_t bytes);
    void update_stats_written(size_t bytes);
};

#endif // COMPACTOR_H