#include "Compactor.h"
#include <algorithm>
#include <queue>
#include <fstream>
#include <iostream>
#include <chrono>
#include <cstring>
#include <sys/stat.h>  // For file timestamps

namespace {
    // Helper to check if a value is a tombstone
    bool is_tombstone(const std::string& value) {
        return value.empty(); // Empty string represents tombstone
    }
}

Compactor::Compactor(std::shared_ptr<BufferPool> buffer_pool, const Config& config)
    : buffer_pool_(buffer_pool), config_(config) {}

std::vector<std::shared_ptr<SSTableReader>> Compactor::compact(
    const std::vector<std::shared_ptr<SSTableReader>>& input_sstables,
    int target_level,
    bool is_largest_level) {

    std::lock_guard<std::mutex> lock(stats_mutex_);
    stats_.compactions_performed++;

    std::cout << "Compacting " << input_sstables.size()
              << " SSTables to level " << target_level
              << (is_largest_level ? " (largest level)" : "") << std::endl;

    // If only one SSTable and no filtering needed, just return it
    if (input_sstables.size() == 1 && !is_largest_level) {
        std::cout << "Single SSTable, no merging needed" << std::endl;
        return input_sstables;
    }

    // Perform the merge
    return merge_sstables(input_sstables, target_level, is_largest_level);
}

std::vector<std::shared_ptr<SSTableReader>> Compactor::merge_sstables(
    const std::vector<std::shared_ptr<SSTableReader>>& sstables,
    int target_level,
    bool is_largest_level) {

    if (sstables.empty()) {
        return {};
    }

    // Generate output filename with timestamp
    uint64_t timestamp = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    std::string output_filename = generate_output_filename(target_level, timestamp);

    // Perform multi-way merge
    multiway_merge(sstables, output_filename, is_largest_level);

    // Load and return the new SSTable
    auto new_sstable = std::make_shared<SSTableReader>(output_filename);
    if (!new_sstable->is_valid()) {
        std::cerr << "Failed to load compacted SSTable: " << output_filename << std::endl;
        return {};
    }

    std::cout << "Created compacted SSTable: " << output_filename
              << " with " << new_sstable->size() << " entries" << std::endl;

    return {new_sstable};
}

// Get file modification time in nanoseconds
uint64_t Compactor::get_file_timestamp(const std::string& filename) const {
    try {
        // Use C++17 filesystem API
        auto ftime = fs::last_write_time(filename);

        // Convert to nanoseconds since epoch
        auto s = std::chrono::time_point_cast<std::chrono::nanoseconds>(ftime);
        return s.time_since_epoch().count();

    } catch (const fs::filesystem_error& e) {
        std::cerr << "Warning: Could not get timestamp for " << filename
                  << ": " << e.what() << std::endl;

        // Fallback: use current time
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    }
}

void Compactor::multiway_merge(
    const std::vector<std::shared_ptr<SSTableReader>>& sstables,
    const std::string& output_filename,
    bool is_largest_level) {

    // Create iterators with timestamps
    std::vector<std::unique_ptr<SSTableIterator>> iterators;
    iterators.reserve(sstables.size());

    for (size_t i = 0; i < sstables.size(); ++i) {
        uint64_t timestamp = get_file_timestamp(sstables[i]->get_filename());
        iterators.emplace_back(
            std::make_unique<SSTableIterator>(sstables[i], timestamp));
    }

    // Min-heap for merge (using greater for min-heap)
    // Sorts by key, then by timestamp (newer files have higher priority)
    std::priority_queue<MergeEntry, std::vector<MergeEntry>, std::greater<MergeEntry>> heap;

    // Initialize heap with first entry from each iterator
    for (size_t i = 0; i < iterators.size(); ++i) {
        if (iterators[i]->has_next()) {
            MergeEntry entry = iterators[i]->next();
            entry.source_index = i;
            heap.push(entry);
        }
    }

    // Use a temporary memtable to collect results
    Memtable memtable(1024 * 1024 * 10); // 10MB temporary memtable
    std::string prev_key;
    uint64_t prev_timestamp = 0;
    bool has_prev = false;

    while (!heap.empty()) {
        MergeEntry current = heap.top();
        heap.pop();

        // Update read statistics
        update_stats_read(current.key.size() + current.value.size());
        stats_.entries_read++;

        // Check if we should skip this entry
        if (!should_keep_entry(current, is_largest_level)) {
            if (current.is_deleted) {
                stats_.tombstones_removed++;
            }

            // Get next entry from the same iterator
            if (iterators[current.source_index]->has_next()) {
                MergeEntry next_entry = iterators[current.source_index]->next();
                next_entry.source_index = current.source_index;
                heap.push(next_entry);
            }
            continue;
        }

        // Handle duplicates (same key)
        if (has_prev && current.key == prev_key) {
            // Same key as previous entry
            // Keep the one with higher timestamp (newer file)

            if (current.timestamp > prev_timestamp) {
                // Current is newer, replace previous
                // Remove previous from memtable
                if (!memtable.remove(prev_key)) {
                    // Key might not exist yet (if previous was just in buffer)
                }

                // Add current to memtable
                if (current.is_deleted) {
                    memtable.remove(current.key);
                } else {
                    memtable.put(current.key, current.value);
                }

                prev_timestamp = current.timestamp;
                stats_.duplicates_removed++;

            } else {
                // Previous is newer, skip current
                stats_.duplicates_removed++;

                // Get next entry from the same iterator
                if (iterators[current.source_index]->has_next()) {
                    MergeEntry next_entry = iterators[current.source_index]->next();
                    next_entry.source_index = current.source_index;
                    heap.push(next_entry);
                }
                continue;
            }

        } else {
            // New key
            if (has_prev) {
                // Add previous to memtable
                // (We buffer one entry to handle duplicates)
                // Actually, we add immediately for simplicity
            }

            // Add current to memtable
            if (current.is_deleted) {
                memtable.remove(current.key);
            } else {
                memtable.put(current.key, current.value);
            }

            prev_key = current.key;
            prev_timestamp = current.timestamp;
            has_prev = true;
            stats_.entries_written++;
        }

        // Get next entry from the same iterator
        if (iterators[current.source_index]->has_next()) {
            MergeEntry next_entry = iterators[current.source_index]->next();
            next_entry.source_index = current.source_index;
            heap.push(next_entry);
        }
    }

    // Write memtable to SSTable using your existing SSTableWriter
    auto entries = memtable.get_all_entries();
    if (!SSTableWriter::write(output_filename, entries)) {
        throw std::runtime_error("Failed to write SSTable: " + output_filename);
    }

    // Update written bytes statistics
    std::ifstream in_file(output_filename, std::ios::binary | std::ios::ate);
    if (in_file) {
        size_t file_size = in_file.tellg();
        update_stats_written(file_size);
    }

    std::cout << "Merge completed: " << stats_.entries_written
              << " entries written, " << stats_.tombstones_removed
              << " tombstones removed, " << stats_.duplicates_removed
              << " duplicates removed" << std::endl;
}

// SSTableIterator Implementation
Compactor::SSTableIterator::SSTableIterator(
    std::shared_ptr<SSTableReader> sstable,
    uint64_t timestamp)
    : sstable_(sstable),
      timestamp_(timestamp),
      current_index_(0),
      eof_(false) {

    all_keys_ = sstable_->get_all_keys();
}

bool Compactor::SSTableIterator::has_next() const {
    return !eof_ && current_index_ < all_keys_.size();
}

Compactor::MergeEntry Compactor::SSTableIterator::next() {
    if (!has_next()) {
        throw std::runtime_error("No more entries in SSTableIterator");
    }

    MergeEntry entry;
    const std::string& key = all_keys_[current_index_];
    entry.key = key;
    entry.timestamp = timestamp_;

    // Check if deleted
    entry.is_deleted = sstable_->is_deleted(key);

    if (!entry.is_deleted) {
        auto value = sstable_->get(key);
        if (value) {
            entry.value = *value;
        }
    }

    current_index_++;
    if (current_index_ >= all_keys_.size()) {
        eof_ = true;
    }

    return entry;
}

void Compactor::SSTableIterator::reset() {
    current_index_ = 0;
    eof_ = false;
}

Compactor::Stats Compactor::get_stats() const {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    return stats_;
}

std::string Compactor::generate_output_filename(int target_level, uint64_t timestamp) {
    // Create a temporary filename with timestamp
    return "./temp_compact_level" + std::to_string(target_level) +
           "_" + std::to_string(timestamp) + ".sst";
}

bool Compactor::should_keep_entry(const MergeEntry& entry, bool is_largest_level) {
    // Skip tombstones at largest level (they've served their purpose)
    if (is_largest_level && entry.is_deleted) {
        return false;
    }

    // Always keep non-deleted entries
    if (!entry.is_deleted) {
        return true;
    }

    // Keep tombstones if not at largest level
    return !is_largest_level;
}

void Compactor::update_stats_read(size_t bytes) {
    stats_.bytes_read += bytes;
}

void Compactor::update_stats_written(size_t bytes) {
    stats_.bytes_written += bytes;
}