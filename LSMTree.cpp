#include "LSMTree.h"
#include "SSTableWriter.h"
#include <filesystem>
#include <fstream>
#include <algorithm>
#include <iostream>
#include <chrono>

namespace fs = std::filesystem;

LSMTree::LSMTree(const std::string& data_dir,
                 size_t memtable_size,
                 size_t buffer_pool_size,
                 size_t bits_per_entry)
    : data_directory_(data_dir),
      memtable_max_size_(memtable_size),
      buffer_pool_size_(buffer_pool_size),
      bits_per_entry_(bits_per_entry),
      memtable_(memtable_size) {

    // Create main directory
    fs::create_directories(data_directory_);

    // Initialize buffer pool
    buffer_pool_ = std::make_unique<BufferPool>(buffer_pool_size);

    // Initialize Write-Ahead Log
    std::string wal_path = data_directory_ + "/wal.log";
    wal_ = std::make_unique<WriteAheadLog>(wal_path);

    // Initialize LevelManager
    LevelManager::Config lm_config;
    lm_config.level0_max_sstables = 2;  // Compact when 2 SSTables in level 0
    lm_config.size_ratio = 2;           // Size ratio between levels
    lm_config.max_levels = 7;           // Levels 0-6

    level_manager_ = std::make_unique<LevelManager>(data_directory_, buffer_pool_, lm_config);

    // Recover from WAL if it exists
    recover_from_wal();
}

LSMTree::~LSMTree() {
    // Ensure memtable is flushed before destruction
    if (memtable_.size() > 0) {
        flush_memtable();
    }
}

bool LSMTree::wal_file_exists() const {
    std::string wal_path = data_directory_ + "/wal.log";
    return fs::exists(wal_path);
}

void LSMTree::recover_from_wal() {
    // Check if WAL file exists
    if (!wal_file_exists()) {
        std::cout << "No WAL file found, starting fresh." << std::endl;
        return;
    }

    std::cout << "Recovering from WAL..." << std::endl;

    try {
        // Use the recover method from WriteAheadLog
        std::vector<WriteAheadLog::LogEntry> entries;
        if (!wal_->recover(entries)) {
            std::cerr << "Failed to recover from WAL. Starting fresh." << std::endl;
            wal_->clear();
            return;
        }

        std::cout << "Recovered " << entries.size() << " entries from WAL." << std::endl;

        if (entries.empty()) {
            std::cout << "WAL is empty, nothing to recover." << std::endl;
            return;
        }

        // Replay entries into memtable
        for (const auto& entry : entries) {
            if (entry.type == WriteAheadLog::OpType::PUT) {
                memtable_.put(entry.key, entry.value);
            } else if (entry.type == WriteAheadLog::OpType::DELETE) {
                memtable_.remove(entry.key);
            }
        }

        std::cout << "Applied " << entries.size() << " entries to memtable." << std::endl;

        // If memtable is too big after recovery, flush it
        if (should_flush_memtable()) {
            std::cout << "Memtable is full after recovery, flushing..." << std::endl;
            flush_memtable();
        }

    } catch (const std::exception& e) {
        std::cerr << "Error during WAL recovery: " << e.what() << std::endl;
        std::cerr << "Clearing WAL and starting fresh." << std::endl;
        wal_->clear();
    }
}

std::string LSMTree::generate_sstable_filename(int level, uint64_t id) {
    return data_directory_ + "/level_" + std::to_string(level) +
           "/sstable_" + std::to_string(id) + ".sst";
}

bool LSMTree::put(const std::string& key, const std::string& value) {
    std::lock_guard<std::recursive_mutex> lock(memtable_mutex_);

    // 1. Write to Write-Ahead Log for durability
    if (!wal_->log_put(key, value)) {
        std::cerr << "Failed to write to WAL" << std::endl;
        return false;
    }

    // 2. Add to memtable
    bool should_flush = !memtable_.put(key, value);

    // 3. Update statistics
    stats_.total_puts++;

    // 4. Check if we need to flush memtable
    if (should_flush) {
        flush_memtable();
    }

    return true;
}

std::optional<std::string> LSMTree::get(const std::string& key) {
    // Update statistics
    stats_.total_gets++;

    // 1. First check memtable (most recent data)
    {
        std::lock_guard<std::recursive_mutex> lock(memtable_mutex_);
        auto memtable_value = memtable_.get(key);
        if (memtable_value) {
            // Check if it's a tombstone
            if (memtable_.is_deleted(key)) {
                return std::nullopt;  // Key is deleted
            }
            return memtable_value;
        }

        // Also check if key is deleted in memtable
        if (memtable_.is_deleted(key)) {
            return std::nullopt;
        }
    }

    // 2. If not found in memtable, search SSTables using LevelManager
    return search_sstables(key);
}

bool LSMTree::remove(const std::string& key) {
    std::lock_guard<std::recursive_mutex> lock(memtable_mutex_);

    // 1. Write delete to WAL
    if (!wal_->log_delete(key)) {
        std::cerr << "Failed to write delete to WAL" << std::endl;
        return false;
    }

    // 2. Add tombstone to memtable (using remove method which adds tombstone)
    bool should_flush = !memtable_.remove(key);

    // 3. Update statistics
    stats_.total_deletes++;

    // 4. Check if we need to flush memtable
    if (should_flush) {
        flush_memtable();
    }

    return true;
}

std::vector<std::pair<std::string, std::string>>
LSMTree::scan(const std::string& start_key, const std::string& end_key) {
    std::vector<std::pair<std::string, std::string>> results;

    // 1. Get from memtable first (most recent)
    {
        std::lock_guard<std::recursive_mutex> lock(memtable_mutex_);
        // We need to get all entries and filter by range
        auto entries = memtable_.get_all_entries();
        for (const auto& [key, entry] : entries) {
            if (key >= start_key && key <= end_key && !entry.is_deleted) {
                results.emplace_back(key, entry.value);
            }
        }
    }

    // 2. Get from SSTables (older data)
    auto sstable_results = scan_sstables(start_key, end_key);

    // Merge results, removing duplicates where memtable has newer values
    // Create a map for efficient lookup
    std::map<std::string, std::string> result_map;

    // First add SSTable results (older)
    for (const auto& [key, value] : sstable_results) {
        if (!is_tombstone(value)) {
            result_map[key] = value;
        }
    }

    // Then override with memtable results (newer)
    for (const auto& [key, value] : results) {
        result_map[key] = value;
    }

    // Convert back to vector
    results.clear();
    for (const auto& [key, value] : result_map) {
        results.emplace_back(key, value);
    }

    return results;
}

bool LSMTree::should_flush_memtable() const {
    return memtable_.should_flush();
}

bool LSMTree::flush_memtable() {
    if (is_flushing_.exchange(true)) {
        return false;  // Already flushing
    }

    try {
        std::lock_guard<std::recursive_mutex> mem_lock(memtable_mutex_);

        // 1. Get all entries from memtable
        auto entries = memtable_.get_all_entries();
        if (entries.empty()) {
            is_flushing_ = false;
            return true;
        }

        // std::cout << "[DEBUG] Flushing memtable with " << entries.size() << " entries" << std::endl;

        // 2. Generate temporary SSTable filename
        uint64_t timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        std::string temp_filename = data_directory_ + "/temp_" + std::to_string(timestamp) + ".sst";

        // 3. Write to SSTable
        if (!SSTableWriter::write(temp_filename, entries)) {
            std::cerr << "Failed to write SSTable: " << temp_filename << std::endl;
            is_flushing_ = false;
            return false;
        }

        // 4. Create SSTableReader as shared_ptr
        auto sstable = std::make_shared<SSTableReader>(temp_filename);
        if (!sstable->is_valid()) {
            std::cerr << "Failed to load SSTable: " << temp_filename << std::endl;
            fs::remove(temp_filename);
            is_flushing_ = false;
            return false;
        }

        // std::cout << "[DEBUG] Created SSTable with " << sstable->size() << " entries" << std::endl;

        // 5. Add to LevelManager
        if (!level_manager_->add_sstable_level0(sstable)) {
            std::cerr << "Failed to add SSTable to LevelManager" << std::endl;
            fs::remove(temp_filename);
            is_flushing_ = false;
            return false;
        }

        // std::cout << "[DEBUG] Added SSTable to LevelManager" << std::endl;

        // 6. Clear memtable and WAL
        memtable_.clear();
        wal_->clear();

        // 7. Update statistics
        stats_.memtable_flushes++;
        stats_.sstables_created = level_manager_->get_total_sstable_count();

        // 8. Check for compaction
        trigger_compaction();

        std::cout << "Memtable flushed with " << entries.size() << " entries" << std::endl;

        is_flushing_ = false;
        return true;

    } catch (const std::exception& e) {
        std::cerr << "Error flushing memtable: " << e.what() << std::endl;
        is_flushing_ = false;
        return false;
    }
}

std::optional<std::string> LSMTree::search_sstables(const std::string& key) const {
    // Use LevelManager to find candidate SSTables
    auto candidates = level_manager_->find_candidate_sstables(key);

    // Search from newest to oldest (candidates are already sorted by LevelManager)
    for (const auto& sstable : candidates) {
        if (auto value = sstable->get(key)) {
            if (is_tombstone(*value)) {
                return std::nullopt;  // Tombstone found
            }
            return value;
        }

        // Also check if key is deleted
        if (sstable->is_deleted(key)) {
            return std::nullopt;
        }
    }

    return std::nullopt;
}

std::vector<std::pair<std::string, std::string>>
LSMTree::scan_sstables(const std::string& start_key, const std::string& end_key) const {
    std::vector<std::pair<std::string, std::string>> results;
    std::map<std::string, std::string> merged_results;

    // Get all SSTables that might contain keys in the range using LevelManager
    auto candidates = level_manager_->find_sstables_for_range(start_key, end_key);

    // Search each SSTable (newest first)
    for (auto it = candidates.rbegin(); it != candidates.rend(); ++it) {
        const auto& sstable = *it;
        auto sstable_results = sstable->scan_range(start_key, end_key);

        // Merge results (newer SSTables override older ones)
        for (const auto& [key, value] : sstable_results) {
            if (!is_tombstone(value)) {
                merged_results[key] = value;
            } else {
                // Remove if tombstone
                merged_results.erase(key);
            }
        }
    }

    // Convert map to vector
    for (const auto& [key, value] : merged_results) {
        results.emplace_back(key, value);
    }

    return results;
}

void LSMTree::trigger_compaction() {
    if (is_compacting_.exchange(true)) {
        return;
    }

    try {
        // Check for compaction tasks and process them
        while (auto task = level_manager_->get_compaction_task()) {
            std::cout << "Compacting level " << task->source_level
                      << " -> level " << task->target_level
                      << " (" << task->input_sstables.size() << " SSTables)" << std::endl;

            // Perform compaction using LevelManager's compactor
            level_manager_->perform_compaction(*task);

            stats_.compactions++;

            // Print level status after compaction
            // level_manager_->print_levels();
        }

    } catch (const std::exception& e) {
        std::cerr << "Error during compaction: " << e.what() << std::endl;
    }

    is_compacting_ = false;
}

bool LSMTree::is_tombstone(const std::string& value) const {
    // In your Memtable implementation, remove() sets value to empty string
    // So empty string is a tombstone
    return value.empty();
}

std::string LSMTree::create_tombstone() const {
    // Return empty string as tombstone
    return "";
}

LSMTree::Stats LSMTree::get_stats() const {
    std::lock_guard<std::recursive_mutex> mem_lock(memtable_mutex_);

    Stats result = stats_;

    // Add current state information
    result.memtable_size = memtable_.size();
    result.memtable_entry_count = memtable_.entry_count();

    // Get LevelManager stats
    if (level_manager_) {
        auto lm_stats = level_manager_->get_stats();
        result.sstable_counts = lm_stats.sstables_per_level;
        result.sstables_created = lm_stats.total_sstables;
        result.sstables_deleted = lm_stats.sstables_deleted;
    }

    return result;
}

size_t LSMTree::get_memtable_size() const {
    std::lock_guard<std::recursive_mutex> lock(memtable_mutex_);
    return memtable_.size();
}

size_t LSMTree::get_sstable_count() const {
    if (!level_manager_) return 0;
    return level_manager_->get_total_sstable_count();
}

std::vector<int> LSMTree::get_level_sizes() const {
    if (!level_manager_) return {};

    std::vector<int> sizes;
    for (int i = 0; i < level_manager_->get_level_count(); i++) {
        sizes.push_back(static_cast<int>(level_manager_->get_sstable_count(i)));
    }
    return sizes;
}

void LSMTree::print_levels() const {
    if (level_manager_) {
        level_manager_->print_levels();
    }
}