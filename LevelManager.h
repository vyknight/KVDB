#ifndef LEVEL_MANAGER_H
#define LEVEL_MANAGER_H

#include "SSTableReader.h"
#include "BufferPool.h"
#include "Compactor.h"  // Add this include
#include <vector>
#include <string>
#include <memory>
#include <map>
#include <mutex>
#include <atomic>

class LevelManager {
public:
    // Use shared_ptr for SSTableReader
    using SSTablePtr = std::shared_ptr<SSTableReader>;

    // Configuration struct
    struct Config {
        size_t max_levels;
        size_t level0_max_sstables;
        size_t size_ratio;
        size_t target_sstable_size;
        bool tiering;

        Config(
            size_t max_levels_ = 7,
            size_t level0_max_sstables_ = 2,
            size_t size_ratio_ = 2,
            size_t target_sstable_size_ = 2 * 1024 * 1024,
            bool tiering_ = false
        )
            : max_levels(std::move(max_levels_)),
              level0_max_sstables(std::move(level0_max_sstables_)),
              size_ratio(std::move(size_ratio_)),
              target_sstable_size(std::move(target_sstable_size_)),
              tiering(std::move(tiering_))
        {}
    };

    // Constructor
    LevelManager(const std::string& data_dir,
                 std::shared_ptr<BufferPool> buffer_pool,
                 const Config& config = Config());

    // Add an SSTable to level 0 (from memtable flush)
    bool add_sstable_level0(SSTablePtr sstable);

    // Get SSTables that need compaction
    struct CompactionTask {
        int source_level;
        std::vector<SSTablePtr> input_sstables;
        int target_level;
    };

    std::optional<CompactionTask> get_compaction_task();

    // Replace SSTables after compaction
    void replace_sstables(int source_level,
                         const std::vector<SSTablePtr>& old_sstables,
                         const std::vector<SSTablePtr>& new_sstables);

    // Perform compaction on a given task (new method)
    void perform_compaction(const CompactionTask& task);

    // Find SSTables that might contain a key (for get operations)
    std::vector<SSTablePtr> find_candidate_sstables(const std::string& key);

    // Find SSTables for range queries
    std::vector<SSTablePtr> find_sstables_for_range(const std::string& start_key,
                                                   const std::string& end_key);

    // Statistics
    struct Stats {
        std::vector<size_t> sstables_per_level;
        std::vector<size_t> bytes_per_level;
        size_t total_sstables = 0;
        size_t total_bytes = 0;
        size_t compactions_triggered = 0;
        size_t compactions_performed = 0;  // New stat
        size_t sstables_created = 0;
        size_t sstables_deleted = 0;
    };

    Stats get_stats() const;

    // Get level information
    size_t get_level_count() const { return levels_.size(); }
    size_t get_sstable_count(int level) const;
    size_t get_total_sstable_count() const;

    // For debugging
    void print_levels() const;

private:
    // Level structure
    struct Level {
        int level_id;
        size_t max_sstables;  // Based on size ratio
        std::vector<SSTablePtr> sstables;
        uint64_t next_sstable_id = 0;

        // For tiering (if enabled)
        std::vector<std::vector<SSTablePtr>> runs;
    };

    // Configuration
    std::string data_directory_;
    std::shared_ptr<BufferPool> buffer_pool_;
    Config config_;

    // Compactor and its configuration (new members)
    std::unique_ptr<Compactor> compactor_;
    Compactor::Config compactor_config_;

    // Levels
    std::vector<Level> levels_;

    // State
    mutable std::recursive_mutex levels_mutex_;
    std::atomic<uint64_t> global_sequence_{0};

    // Private methods
    void initialize_levels();
    void load_existing_sstables();

    // File management
    std::string generate_sstable_filename(int level, uint64_t sequence);
    uint64_t parse_sequence_from_filename(const std::string& filename);

    // Level management
    void add_sstable_to_level(int level, SSTablePtr sstable);
    void remove_sstable_from_level(int level, const std::string& filename);

    // Compaction triggers
    bool should_compact_level0() const;
    bool should_compact_level(int level) const;

    // Helper methods
    size_t calculate_level_capacity(int level) const;
    std::vector<SSTablePtr> get_overlapping_sstables(int level,
                                                   const SSTablePtr& new_sstable);

    // Tiering support (for bonus)
    void add_sstable_to_tier(int level, SSTablePtr sstable);
    std::optional<CompactionTask> get_tiering_compaction_task(int level);

    // Statistics
    mutable Stats stats_;
    mutable bool stats_dirty_ = true;
    void update_stats() const;
};

#endif // LEVEL_MANAGER_H