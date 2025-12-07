//
// Created by Zekun Liu on 2025-12-07.
//

#ifndef KVDB_LEVELMANAGER_H
#define KVDB_LEVELMANAGER_H

#include "SSTableReader.h"

class LevelManager {
private:
    struct Level {
        size_t level_id;
        size_t max_sstables;  // Based on size ratio
        std::vector<SSTableReader> sstables;
        uint64_t next_seq_number;
    };

    std::vector<Level> levels_;
    std::string data_directory_;

public:
    LevelManager(const std::string& data_dir);

    // Add SSTable to level
    bool add_sstable(int level, const std::string& filename);

    // Get SSTables that need compaction
    std::pair<int, std::vector<SSTableReader>> get_sstables_for_compaction();

    // Replace old SSTables with new ones after compaction
    void replace_sstables(int level,
                          const std::vector<SSTableReader>& old_sstables,
                          const std::vector<SSTableReader>& new_sstables);

    // Find SSTable containing key (for get operations)
    std::vector<SSTableReader*> find_candidate_sstables(const std::string& key);

    // Range query
    std::vector<SSTableReader*> find_sstables_for_range(const std::string& start_key,
                                                        const std::string& end_key);

    // Get all SSTables in all levels (for debugging)
    std::vector<SSTableReader*> get_all_sstables();

private:
    void initialize_levels();
    std::string generate_sstable_filename(int level, uint64_t seq);
};

#endif //KVDB_LEVELMANAGER_H