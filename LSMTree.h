#ifndef LSM_TREE_H
#define LSM_TREE_H

#include "Memtable.h"
#include "SSTableReader.h"
#include "WriteAheadLog.h"
#include "BufferPool.h"
#include <vector>
#include <map>
#include <memory>
#include <string>
#include <atomic>
#include <mutex>
#include <optional>
#include <filesystem>

class LSMTree {
public:
    // Constructor with configuration
    LSMTree(const std::string& data_dir = "./data",
            size_t memtable_size = 1024 * 1024,      // 1MB
            size_t buffer_pool_size = 10 * 1024 * 1024, // 10MB
            size_t bits_per_entry = 8);              // For filters (future use)

    ~LSMTree();

    // Public API
    bool put(const std::string& key, const std::string& value);
    std::optional<std::string> get(const std::string& key);
    bool remove(const std::string& key);

    // Range scan (returns key-value pairs in range)
    std::vector<std::pair<std::string, std::string>>
        scan(const std::string& start_key, const std::string& end_key);

    // Statistics
    struct Stats {
        size_t total_puts = 0;
        size_t total_gets = 0;
        size_t total_deletes = 0;
        size_t memtable_flushes = 0;
        size_t compactions = 0;
        size_t sstables_created = 0;
        size_t sstables_deleted = 0;
        size_t memtable_size = 0;
        size_t memtable_entry_count = 0;
        std::vector<size_t> sstable_counts;
    };

    Stats get_stats() const;

    // For testing/debugging
    size_t get_memtable_size() const;
    size_t get_sstable_count() const;
    std::vector<int> get_level_sizes() const;

private:
    // Core components
    Memtable memtable_;
    std::unique_ptr<WriteAheadLog> wal_;
    std::unique_ptr<BufferPool> buffer_pool_;

    // LSM tree levels: level 0 (immutable memtables) and other levels
    struct LevelInfo {
        std::vector<SSTableReader> sstables;
        uint64_t next_sstable_id = 0;
    };

    std::vector<LevelInfo> levels_;

    // Configuration
    std::string data_directory_;
    size_t memtable_max_size_;
    size_t buffer_pool_size_;
    size_t bits_per_entry_;

    // State
    std::atomic<uint64_t> sequence_number_{0};
    std::atomic<bool> is_flushing_{false};
    std::atomic<bool> is_compacting_{false};

    // Mutexes for thread safety
    mutable std::mutex memtable_mutex_;
    mutable std::mutex level_mutex_;

    // Statistics
    Stats stats_;

    // Private methods
    void initialize_directories();
    void recover_from_wal();

    // Memtable operations
    bool flush_memtable();
    bool should_flush_memtable() const;

    // Compaction operations
    void trigger_compaction();
    void compact_level(int level);
    std::vector<SSTableReader> merge_sstables(
        const std::vector<SSTableReader>& sstables,
        int target_level);

    // Helper methods
    std::string generate_sstable_filename(int level, uint64_t id);
    void add_sstable_to_level(int level, SSTableReader&& sstable);
    void remove_sstable_from_level(int level, const std::string& filename);

    // Search methods
    std::optional<std::string> search_sstables(const std::string& key) const;
    std::vector<std::pair<std::string, std::string>>
        scan_sstables(const std::string& start_key, const std::string& end_key) const;

    // Tombstone handling
    bool is_tombstone(const std::string& value) const;
    std::string create_tombstone() const;

    // WAL helpers
    bool wal_file_exists() const;

    // Disallow copying
    LSMTree(const LSMTree&) = delete;
    LSMTree& operator=(const LSMTree&) = delete;
};

#endif // LSM_TREE_H