//
// Created by Zekun Liu on 2025-12-07.
//

#ifndef KVDB_COMPACTOR_H
#define KVDB_COMPACTOR_H

#include "BufferPool.h"
#include "SSTableReader.h"

class Compactor {
private:
    struct MergeEntry {
        std::string key;
        std::string value;
        bool is_deleted;
        uint64_t sequence;  // For recency comparison
        size_t source_index;  // Which SSTable it came from

        bool operator<(const MergeEntry& other) const {
            // First by key, then by sequence (descending for same key)
            if (key != other.key) return key < other.key;
            return sequence > other.sequence;  // Higher sequence = newer
        }
    };

    BufferPool* buffer_pool_;
    size_t buffer_size_;  // Size of each buffer (e.g., 4KB)

public:
    Compactor(BufferPool* buffer_pool, size_t buffer_size = 4096);

    // Main compaction method
    std::vector<SSTableReader> compact(
        const std::vector<SSTableReader>& input_sstables,
        int target_level,
        bool is_largest_level);

private:
    // Merge two SSTables with three buffers
    SSTableReader merge_two_sstables(
        const SSTableReader& sst1,
        const SSTableReader& sst2,
        const std::string& output_filename,
        bool is_largest_level);

    // Multi-way merge for Dostoevsky bonus
    SSTableReader multiway_merge(
        const std::vector<SSTableReader>& sstables,
        const std::string& output_filename,
        bool is_largest_level);

    // Buffer management
    class MergeBuffer {
    private:
        std::unique_ptr<char[]> data_;
        size_t capacity_;
        size_t size_;
        size_t read_pos_;

    public:
        MergeBuffer(size_t capacity);

        bool append(const std::string& key, const std::string& value,
                   bool is_deleted, uint64_t sequence);
        bool is_full() const;
        void clear();
        void write_to_file(std::ofstream& file);
        void reset_read();
        std::optional<MergeEntry> read_next();
    };

    // Filter creation (placeholder for future)
    std::vector<uint8_t> create_filter(const std::vector<MergeEntry>& entries);
};

#endif //KVDB_COMPACTOR_H