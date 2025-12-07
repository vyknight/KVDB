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

    // Initialize directories
    initialize_directories();

    // Initialize buffer pool
    buffer_pool_ = std::make_unique<BufferPool>(buffer_pool_size);

    // Initialize Write-Ahead Log
    std::string wal_path = data_directory_ + "/wal.log";
    wal_ = std::make_unique<WriteAheadLog>(wal_path);

    // Initialize levels (we'll start with 5 levels)
    levels_.resize(5);

    // Recover from WAL if it exists
    recover_from_wal();

    // Load existing SSTables from disk (to be implemented later)
    // load_existing_sstables();
}

LSMTree::~LSMTree() {
    // Ensure memtable is flushed before destruction
    if (memtable_.size() > 0) {
        flush_memtable();
    }
}

void LSMTree::initialize_directories() {
    // Create main data directory
    fs::create_directories(data_directory_);

    // Create level directories
    for (int i = 0; i < 5; i++) {
        std::string level_dir = data_directory_ + "/level_" + std::to_string(i);
        fs::create_directories(level_dir);
    }
}

bool LSMTree::wal_file_exists() const {
    std::string wal_path = data_directory_ + "/wal.log";
    return fs::exists(wal_path);
}

void LSMTree::recover_from_wal() {
    // First check if WAL file exists before trying to open it
    std::string wal_path = data_directory_ + "/wal.log";

    if (!fs::exists(wal_path)) {
        std::cout << "No WAL file found, starting fresh." << std::endl;
        return;
    }

    std::cout << "Recovering from WAL..." << std::endl;

    try {
        // Check if WAL file is empty or too small
        uintmax_t file_size = fs::file_size(wal_path);
        if (file_size < 16) { // Less than header size
            std::cout << "WAL file too small or empty, skipping recovery." << std::endl;
            fs::remove(wal_path);
            return;
        }

        // Use the recover method from WriteAheadLog
        std::vector<WriteAheadLog::LogEntry> entries;

        // Note: wal_ is already constructed in the LSMTree constructor
        // We need to use the existing wal_ object
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

        // IMPORTANT: Don't clear the WAL here!
        // We only clear it after successfully flushing the memtable to SSTable
        // The WAL should persist until we successfully write to disk

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
            if (is_tombstone(*memtable_value)) {
                return std::nullopt;  // Key is deleted
            }
            return memtable_value;
        }

        // Also check if key is deleted in memtable
        if (memtable_.is_deleted(key)) {
            return std::nullopt;
        }
    }

    // 2. If not found in memtable, search SSTables
    std::lock_guard<std::recursive_mutex> lock(level_mutex_);
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
        std::lock_guard<std::recursive_mutex> level_lock(level_mutex_);

        // 1. Get all entries from memtable
        auto entries = memtable_.get_all_entries();
        if (entries.empty()) {
            is_flushing_ = false;
            return true;
        }

        // 2. Generate SSTable filename
        uint64_t sstable_id = levels_[0].next_sstable_id++;
        std::string filename = generate_sstable_filename(0, sstable_id);

        // 3. Write to SSTable
        if (!SSTableWriter::write(filename, entries)) {
            std::cerr << "Failed to write SSTable: " << filename << std::endl;
            is_flushing_ = false;
            return false;
        }

        // 4. Create SSTableReader and add to level 0
        SSTableReader sstable(filename);
        if (!sstable.is_valid()) {
            std::cerr << "Failed to load SSTable: " << filename << std::endl;
            is_flushing_ = false;
            return false;
        }

        levels_[0].sstables.push_back(std::move(sstable));

        // 5. Clear memtable AND WAL (only after successful SSTable write!)
        memtable_.clear();
        wal_->clear();  // Clear WAL here, after data is safely on disk

        // 6. Update statistics
        stats_.memtable_flushes++;
        stats_.sstables_created++;

        // 7. Check if we need to trigger compaction
        // For now, compact when we have 2 SSTables at level 0
        if (levels_[0].sstables.size() >= 2) {
            // trigger_compaction(); // We'll implement this later
            std::cout << "Compaction triggered (not yet implemented)" << std::endl;
        }

        std::cout << "Memtable flushed to " << filename
                  << " with " << entries.size() << " entries" << std::endl;

        is_flushing_ = false;
        return true;

    } catch (const std::exception& e) {
        std::cerr << "Error flushing memtable: " << e.what() << std::endl;
        is_flushing_ = false;
        return false;
    }
}

std::optional<std::string> LSMTree::search_sstables(const std::string& key) const {
    // Search from newest to oldest (level 0 to highest)
    // Within each level, search from newest to oldest SSTable

    for (int level = 0; level < static_cast<int>(levels_.size()); level++) {
        const auto& sstables = levels_[level].sstables;

        if (level == 0) {
            // Level 0: Search all SSTables (most recent first)
            for (auto it = sstables.rbegin(); it != sstables.rend(); ++it) {
                if (auto value = it->get(key)) {
                    if (is_tombstone(*value)) {
                        return std::nullopt;  // Tombstone found
                    }
                    return value;
                }

                // Also check if key is deleted
                if (it->is_deleted(key)) {
                    return std::nullopt;
                }
            }
        } else {
            // Higher levels: Search SSTables
            for (auto it = sstables.rbegin(); it != sstables.rend(); ++it) {
                // For now, search all SSTables in higher levels
                // Later we can implement key-range optimization
                if (auto value = it->get(key)) {
                    if (is_tombstone(*value)) {
                        return std::nullopt;
                    }
                    return value;
                }

                if (it->is_deleted(key)) {
                    return std::nullopt;
                }
            }
        }
    }

    return std::nullopt;
}

std::vector<std::pair<std::string, std::string>>
LSMTree::scan_sstables(const std::string& start_key, const std::string& end_key) const {
    std::vector<std::pair<std::string, std::string>> results;
    std::map<std::string, std::string> merged_results;  // To handle duplicates

    // For each level, from newest to oldest
    for (int level = 0; level < static_cast<int>(levels_.size()); level++) {
        const auto& sstables = levels_[level].sstables;

        // For each SSTable in this level, from newest to oldest
        for (auto it = sstables.rbegin(); it != sstables.rend(); ++it) {
            auto sstable_results = it->scan_range(start_key, end_key);

            // Merge with existing results (newer SSTables override older ones)
            for (const auto& [key, value] : sstable_results) {
                if (!is_tombstone(value)) {
                    merged_results[key] = value;
                } else {
                    // Remove if tombstone
                    merged_results.erase(key);
                }
            }
        }
    }

    // Convert map to vector
    for (const auto& [key, value] : merged_results) {
        results.emplace_back(key, value);
    }

    return results;
}

bool LSMTree::is_tombstone(const std::string& value) const {
    // For now, we'll check if the value is empty
    // In your Memtable::Entry, remove() sets value to ""
    return value.empty();
}

std::string LSMTree::create_tombstone() const {
    // Return empty string as tombstone
    return "";
}

LSMTree::Stats LSMTree::get_stats() const {
    std::lock_guard<std::recursive_mutex> mem_lock(memtable_mutex_);
    std::lock_guard<std::recursive_mutex> level_lock(level_mutex_);

    Stats result = stats_;

    // Add current state information
    result.memtable_size = memtable_.size();
    result.memtable_entry_count = memtable_.entry_count();

    // Count SSTables per level
    result.sstable_counts.clear();
    for (size_t i = 0; i < levels_.size(); i++) {
        result.sstable_counts.push_back(levels_[i].sstables.size());
    }

    return result;
}

size_t LSMTree::get_memtable_size() const {
    std::lock_guard<std::recursive_mutex> lock(memtable_mutex_);
    return memtable_.size();
}

size_t LSMTree::get_sstable_count() const {
    std::lock_guard<std::recursive_mutex> lock(level_mutex_);
    size_t total = 0;
    for (const auto& level : levels_) {
        total += level.sstables.size();
    }
    return total;
}

std::vector<int> LSMTree::get_level_sizes() const {
    std::lock_guard<std::recursive_mutex> lock(level_mutex_);
    std::vector<int> sizes;
    for (const auto& level : levels_) {
        sizes.push_back(static_cast<int>(level.sstables.size()));
    }
    return sizes;
}

// Placeholder compaction methods (to be implemented)
void LSMTree::trigger_compaction() {
    // TODO: Implement compaction logic
    std::cout << "Compaction triggered (not yet implemented)" << std::endl;
}

void LSMTree::compact_level(int level) {
    // TODO: Implement level compaction
    std::cout << "Compacting level " << level << " (not yet implemented)" << std::endl;
}

std::vector<SSTableReader> LSMTree::merge_sstables(
    const std::vector<SSTableReader>& sstables,
    int target_level) {
    // TODO: Implement SSTable merging
    std::cout << "Merging " << sstables.size() << " SSTables to level "
              << target_level << " (not yet implemented)" << std::endl;
    return {};
}

void LSMTree::add_sstable_to_level(int level, SSTableReader&& sstable) {
    std::lock_guard<std::recursive_mutex> lock(level_mutex_);
    if (level >= 0 && level < static_cast<int>(levels_.size())) {
        levels_[level].sstables.push_back(std::move(sstable));
    }
}

void LSMTree::remove_sstable_from_level(int level, const std::string& filename) {
    std::lock_guard<std::recursive_mutex> lock(level_mutex_);
    if (level >= 0 && level < static_cast<int>(levels_.size())) {
        auto& sstables = levels_[level].sstables;
        sstables.erase(
            std::remove_if(sstables.begin(), sstables.end(),
                [&filename](const SSTableReader& sst) {
                    return sst.get_filename() == filename;
                }),
            sstables.end()
        );
    }
}