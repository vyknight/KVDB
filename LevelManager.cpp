#include "LevelManager.h"
#include "SSTableWriter.h"
#include <filesystem>
#include <fstream>
#include <algorithm>
#include <iostream>
#include <chrono>
#include <set>
#include "Compactor.h"

namespace fs = std::filesystem;

LevelManager::LevelManager(const std::string& data_dir,
                         std::shared_ptr<BufferPool> buffer_pool,
                         const Config& config)
    : data_directory_(data_dir),
      buffer_pool_(buffer_pool),
      config_(config) {

    // Initialize directories
    for (int i = 0; i < static_cast<int>(config_.max_levels); i++) {
        std::string level_dir = data_directory_ + "/level_" + std::to_string(i);
        fs::create_directories(level_dir);
    }

    // Initialize compactor configuration
    compactor_config_.buffer_size = 4096; // 4KB buffer as per assignment
    compactor_config_.max_merge_fan_in = 10;
    compactor_config_.remove_tombstones = true;

    // Initialize compactor
    compactor_ = std::make_unique<Compactor>(buffer_pool, compactor_config_);

    // Initialize levels
    initialize_levels();

    // Load existing SSTables
    load_existing_sstables();

    std::cout << "LevelManager initialized with " << levels_.size() << " levels" << std::endl;
}

void LevelManager::initialize_levels() {
    levels_.resize(config_.max_levels);

    for (int i = 0; i < static_cast<int>(levels_.size()); i++) {
        levels_[i].level_id = i;

        // Level 0 has special treatment (no size limit, just count limit)
        if (i == 0) {
            levels_[i].max_sstables = config_.level0_max_sstables;
        } else {
            // For level i, capacity = size_ratio^i * level0_max_sstables
            size_t capacity = config_.level0_max_sstables;
            for (int j = 0; j < i; j++) {
                capacity *= config_.size_ratio;
            }
            levels_[i].max_sstables = capacity;
        }

        // Initialize next SSTable ID by scanning directory
        std::string level_dir = data_directory_ + "/level_" + std::to_string(i);
        if (fs::exists(level_dir)) {
            uint64_t max_id = 0;
            for (const auto& entry : fs::directory_iterator(level_dir)) {
                if (entry.is_regular_file() && entry.path().extension() == ".sst") {
                    uint64_t seq = parse_sequence_from_filename(entry.path().string());
                    if (seq > max_id) {
                        max_id = seq;
                    }
                }
            }
            levels_[i].next_sstable_id = max_id + 1;
        }
    }
}

void LevelManager::load_existing_sstables() {
    std::lock_guard<std::mutex> lock(levels_mutex_);

    for (int level = 0; level < static_cast<int>(levels_.size()); level++) {
        std::string level_dir = data_directory_ + "/level_" + std::to_string(level);

        if (!fs::exists(level_dir)) {
            continue;
        }

        // Collect all SST files
        std::vector<std::string> sst_files;
        for (const auto& entry : fs::directory_iterator(level_dir)) {
            if (entry.is_regular_file() && entry.path().extension() == ".sst") {
                sst_files.push_back(entry.path().string());
            }
        }

        // Sort by sequence number (oldest first)
        std::sort(sst_files.begin(), sst_files.end(),
            [this](const std::string& a, const std::string& b) {
                return parse_sequence_from_filename(a) < parse_sequence_from_filename(b);
            });

        // Load each SSTable
        for (const auto& filename : sst_files) {
            auto sstable = std::make_shared<SSTableReader>(filename);
            if (sstable->is_valid()) {
                levels_[level].sstables.push_back(sstable);
                stats_.sstables_created++;
            }
        }
    }

    stats_dirty_ = true;
    std::cout << "Loaded " << get_total_sstable_count() << " existing SSTables" << std::endl;
}

bool LevelManager::add_sstable_level0(SSTablePtr sstable) {
    std::lock_guard<std::mutex> lock(levels_mutex_);

    // Generate new filename with sequence number
    uint64_t seq = levels_[0].next_sstable_id++;
    std::string new_filename = generate_sstable_filename(0, seq);

    // Rename the SSTable file to our naming convention
    try {
        fs::rename(sstable->get_filename(), new_filename);
    } catch (const fs::filesystem_error& e) {
        std::cerr << "Failed to rename SSTable: " << e.what() << std::endl;
        return false;
    }

    // Create new SSTableReader with new filename
    auto new_sstable = std::make_shared<SSTableReader>(new_filename);
    if (!new_sstable->is_valid()) {
        std::cerr << "Failed to reload SSTable with new name: " << new_filename << std::endl;
        return false;
    }

    // Add to level 0
    levels_[0].sstables.push_back(new_sstable);

    // Update stats
    stats_.sstables_created++;
    stats_dirty_ = true;

    std::cout << "Added SSTable to level 0: " << new_filename
              << " (total in level 0: " << levels_[0].sstables.size() << ")" << std::endl;

    return true;
}

std::optional<LevelManager::CompactionTask> LevelManager::get_compaction_task() {
    std::lock_guard<std::mutex> lock(levels_mutex_);

    // Check level 0 first (highest priority)
    if (should_compact_level0() && !levels_[0].sstables.empty()) {
        CompactionTask task;
        task.source_level = 0;
        task.target_level = 1;

        // Move all SSTables from level 0 to the task
        task.input_sstables = std::move(levels_[0].sstables);
        levels_[0].sstables.clear(); // Clear after moving

        if (!task.input_sstables.empty()) {
            stats_.compactions_triggered++;
            return task;
        }
    }

    // Check other levels
    for (int level = 1; level < static_cast<int>(levels_.size()) - 1; level++) {
        if (should_compact_level(level) && !levels_[level].sstables.empty()) {
            CompactionTask task;
            task.source_level = level;
            task.target_level = level + 1;

            // For leveling: when level L has more than capacity, compact all SSTables in level L
            if (levels_[level].sstables.size() > calculate_level_capacity(level)) {
                // Move SSTables to task
                task.input_sstables = std::move(levels_[level].sstables);
                levels_[level].sstables.clear(); // Clear after moving

                if (!task.input_sstables.empty()) {
                    stats_.compactions_triggered++;
                    return task;
                }
            }
        }
    }

    return std::nullopt;
}

void LevelManager::replace_sstables(int source_level,
                                  const std::vector<SSTablePtr>& old_sstables,
                                  const std::vector<SSTablePtr>& new_sstables) {
    std::lock_guard<std::mutex> lock(levels_mutex_);

    if (source_level < 0 || source_level >= static_cast<int>(levels_.size())) {
        std::cerr << "Invalid source level: " << source_level << std::endl;
        return;
    }

    // Add new SSTables to target level
    int target_level = source_level + 1;
    if (target_level < static_cast<int>(levels_.size())) {
        // Add all new SSTables
        for (const auto& new_sstable : new_sstables) {
            levels_[target_level].sstables.push_back(new_sstable);
            stats_.sstables_created++;
        }

        // Sort target level by min key for efficient searching
        std::sort(levels_[target_level].sstables.begin(),
                  levels_[target_level].sstables.end(),
                  [](const SSTablePtr& a, const SSTablePtr& b) {
                      return a->min_key() < b->min_key();
                  });
    }

    // Delete old SSTable files
    for (const auto& old_sstable : old_sstables) {
        try {
            if (fs::exists(old_sstable->get_filename())) {
                fs::remove(old_sstable->get_filename());
                stats_.sstables_deleted++;
            }
        } catch (const fs::filesystem_error& e) {
            std::cerr << "Failed to delete SSTable: " << old_sstable->get_filename()
                      << " - " << e.what() << std::endl;
        }
    }

    stats_dirty_ = true;

    std::cout << "Replaced " << old_sstables.size() << " SSTables from level " << source_level
              << " with " << new_sstables.size() << " SSTables in level " << target_level << std::endl;
}

std::vector<LevelManager::SSTablePtr> LevelManager::find_candidate_sstables(const std::string& key) {
    std::vector<SSTablePtr> candidates;
    std::lock_guard<std::mutex> lock(levels_mutex_);

    // Search from level 0 (newest) to highest level (oldest)
    for (int level = 0; level < static_cast<int>(levels_.size()); level++) {
        const auto& sstables = levels_[level].sstables;

        if (level == 0) {
            // Level 0: SSTables may have overlapping key ranges
            // Need to check all SSTables in reverse chronological order (newest first)
            for (auto it = sstables.rbegin(); it != sstables.rend(); ++it) {
                const auto& sstable = *it;
                // Check if key is in this SSTable's range
                if (key >= sstable->min_key() && key <= sstable->max_key()) {
                    candidates.push_back(sstable);
                }
            }
        } else {
            // Higher levels: SSTables have non-overlapping key ranges
            // Use binary search to find the right SSTable
            // Find first SSTable where max_key >= key
            auto it = std::lower_bound(sstables.begin(), sstables.end(), key,
                [](const SSTablePtr& sst, const std::string& k) {
                    return sst->max_key() < k;
                });

            if (it != sstables.end()) {
                const auto& sstable = *it;
                // Check if key is in this SSTable's range
                if (key >= sstable->min_key() && key <= sstable->max_key()) {
                    candidates.push_back(sstable);
                }
            }
        }

        // If we found candidates in this level, we can stop
        // (because newer levels have more recent data)
        if (!candidates.empty()) {
            break;
        }
    }

    return candidates;
}

std::vector<LevelManager::SSTablePtr> LevelManager::find_sstables_for_range(const std::string& start_key,
                                                                           const std::string& end_key) {
    std::vector<SSTablePtr> candidates;
    std::lock_guard<std::mutex> lock(levels_mutex_);

    // For range queries, we need to check all levels
    for (int level = 0; level < static_cast<int>(levels_.size()); level++) {
        const auto& sstables = levels_[level].sstables;

        if (level == 0) {
            // Level 0: Check all SSTables for overlap
            for (const auto& sstable : sstables) {
                // Check if ranges overlap: [sstable.min, sstable.max] intersects [start_key, end_key]
                if (!(sstable->max_key() < start_key || sstable->min_key() > end_key)) {
                    candidates.push_back(sstable);
                }
            }
        } else {
            // Higher levels: Find SSTables that overlap with the range
            // Use binary search to find first SSTable that might overlap
            auto it = std::lower_bound(sstables.begin(), sstables.end(), start_key,
                [](const SSTablePtr& sst, const std::string& k) {
                    return sst->max_key() < k;
                });

            // Check consecutive SSTables until we pass the end_key
            while (it != sstables.end() && (*it)->min_key() <= end_key) {
                candidates.push_back(*it);
                ++it;
            }
        }
    }

    return candidates;
}

LevelManager::Stats LevelManager::get_stats() const {
    std::lock_guard<std::mutex> lock(levels_mutex_);

    if (stats_dirty_) {
        update_stats();
    }

    return stats_;
}

void LevelManager::update_stats() const {
    stats_.sstables_per_level.clear();
    stats_.bytes_per_level.clear();
    stats_.total_sstables = 0;
    stats_.total_bytes = 0;

    for (const auto& level : levels_) {
        size_t level_sstables = level.sstables.size();
        size_t level_bytes = 0;

        for (const auto& sstable : level.sstables) {
            // Estimate file size (could use fs::file_size for accuracy)
            level_bytes += sstable->memory_usage(); // Rough estimate
        }

        stats_.sstables_per_level.push_back(level_sstables);
        stats_.bytes_per_level.push_back(level_bytes);
        stats_.total_sstables += level_sstables;
        stats_.total_bytes += level_bytes;
    }

    stats_dirty_ = false;
}

size_t LevelManager::get_sstable_count(int level) const {
    std::lock_guard<std::mutex> lock(levels_mutex_);

    if (level < 0 || level >= static_cast<int>(levels_.size())) {
        return 0;
    }

    return levels_[level].sstables.size();
}

size_t LevelManager::get_total_sstable_count() const {
    std::lock_guard<std::mutex> lock(levels_mutex_);

    size_t total = 0;
    for (const auto& level : levels_) {
        total += level.sstables.size();
    }

    return total;
}

void LevelManager::print_levels() const {
    std::lock_guard<std::mutex> lock(levels_mutex_);

    std::cout << "\n=== Level Manager Status ===" << std::endl;

    for (int level = 0; level < static_cast<int>(levels_.size()); level++) {
        const auto& lvl = levels_[level];

        std::cout << "Level " << level << ": "
                  << lvl.sstables.size() << " SSTables"
                  << " (capacity: " << lvl.max_sstables << ")" << std::endl;

        // Print SSTable info
        for (size_t i = 0; i < lvl.sstables.size(); i++) {
            const auto& sst = lvl.sstables[i];
            std::cout << "  " << i << ": " << fs::path(sst->get_filename()).filename().string()
                      << " [" << sst->min_key() << " - " << sst->max_key() << "]"
                      << " (" << sst->size() << " entries)" << std::endl;
        }
    }

    auto stats = get_stats();
    std::cout << "\nTotal SSTables: " << stats.total_sstables << std::endl;
    std::cout << "Total bytes: " << stats.total_bytes << " ("
              << stats.total_bytes / (1024 * 1024) << " MB)" << std::endl;
}

std::string LevelManager::generate_sstable_filename(int level, uint64_t sequence) {
    return data_directory_ + "/level_" + std::to_string(level) +
           "/sstable_" + std::to_string(sequence) + ".sst";
}

uint64_t LevelManager::parse_sequence_from_filename(const std::string& filename) {
    // Extract sequence from: .../sstable_<seq>.sst
    std::string basename = fs::path(filename).filename().string();

    // Find the sequence number
    size_t start = basename.find("sstable_");
    if (start == std::string::npos) {
        return 0;
    }
    start += 8; // Length of "sstable_"

    size_t end = basename.find(".sst");
    if (end == std::string::npos) {
        return 0;
    }

    try {
        std::string seq_str = basename.substr(start, end - start);
        return std::stoull(seq_str);
    } catch (...) {
        return 0;
    }
}

void LevelManager::add_sstable_to_level(int level, SSTablePtr sstable) {
    if (level < 0 || level >= static_cast<int>(levels_.size())) {
        return;
    }

    levels_[level].sstables.push_back(sstable);
    stats_.sstables_created++;
    stats_dirty_ = true;
}

void LevelManager::remove_sstable_from_level(int level, const std::string& filename) {
    if (level < 0 || level >= static_cast<int>(levels_.size())) {
        return;
    }

    auto& sstables = levels_[level].sstables;
    sstables.erase(
        std::remove_if(sstables.begin(), sstables.end(),
            [&filename](const SSTablePtr& sst) {
                return sst->get_filename() == filename;
            }),
        sstables.end()
    );

    stats_.sstables_deleted++;
    stats_dirty_ = true;
}

bool LevelManager::should_compact_level0() const {
    return levels_[0].sstables.size() >= config_.level0_max_sstables;
}

bool LevelManager::should_compact_level(int level) const {
    if (level <= 0 || level >= static_cast<int>(levels_.size())) {
        return false;
    }

    return levels_[level].sstables.size() > calculate_level_capacity(level);
}

size_t LevelManager::calculate_level_capacity(int level) const {
    if (level == 0) {
        return config_.level0_max_sstables;
    }

    // For level i, capacity = level0_max_sstables * (size_ratio)^(i)
    size_t capacity = config_.level0_max_sstables;
    for (int i = 0; i < level; i++) {
        capacity *= config_.size_ratio;
    }

    return capacity;
}

void LevelManager::perform_compaction(const CompactionTask& task) {
    if (task.input_sstables.empty()) {
        std::cerr << "Compaction task has no input SSTables" << std::endl;
        return;
    }

    std::cout << "Starting compaction: level " << task.source_level
              << " -> level " << task.target_level
              << " (" << task.input_sstables.size() << " SSTables)" << std::endl;

    // Check if this is the largest level
    bool is_largest_level = (task.target_level >= static_cast<int>(levels_.size()) - 1);

    // Perform compaction using the compactor
    auto new_sstables = compactor_->compact(task.input_sstables,
                                           task.target_level,
                                           is_largest_level);

    if (new_sstables.empty()) {
        std::cerr << "Compaction failed, no SSTables produced" << std::endl;
        return;
    }

    // Replace old SSTables with new ones
    replace_sstables(task.source_level, task.input_sstables, new_sstables);

    // Update statistics
    stats_.compactions_performed++;
    stats_dirty_ = true;

    // Get compaction stats
    auto compactor_stats = compactor_->get_stats();
    std::cout << "Compaction completed: " << compactor_stats.entries_written
              << " entries written, " << compactor_stats.tombstones_removed
              << " tombstones removed" << std::endl;
}