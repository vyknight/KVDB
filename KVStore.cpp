//
// Created by K on 2025-12-06.
//

#include "KVStore.h"
#include "SSTableWriter.h"
#include <filesystem>
#include <fstream>
#include <algorithm>
#include <chrono>
#include <sstream>
#include <iomanip>
#include <iostream>

namespace fs = std::filesystem;

// Private constructor
KVStore::KVStore(const std::string& db_path, size_t memtable_size)
    : db_path_(db_path)
    , memtable_size_(memtable_size)
    , memtable_(memtable_size)
    , sst_counter_(0) {

    // Ensure database directory exists
    fs::create_directories(db_path_);

    // Load existing SSTables
    load_existing_sstables();

    // Setup WAL
    std::string wal_path = (fs::path(db_path_) / "wal.bin").string();
    wal_ = std::make_unique<WriteAheadLog>(wal_path);

    // Recover from WAL if it exists and has entries
    recover_from_wal();
}

bool KVStore::initialize() {
    try {
        // Ensure database directory exists
        fs::create_directories(db_path_);

        // Load existing SSTables
        load_existing_sstables();

        // Setup WAL
        std::string wal_path = (fs::path(db_path_) / "wal.bin").string();
        wal_ = std::make_unique<WriteAheadLog>(wal_path);
        if (!wal_->is_open()) {
            throw std::runtime_error("Failed to open Write-Ahead Log");
        }

        // Recover from WAL if it exists and has entries
        recover_from_wal();

        return true;
    } catch (const std::exception& e) {
        // Clean up partially initialized state
        sstables_.clear();
        wal_.reset();
        return false;
    }
}

std::unique_ptr<KVStore> KVStore::open(const std::string& db_name, size_t memtable_size) {
    try {
        // Create instance with simple constructor
        auto instance = std::unique_ptr<KVStore>(new KVStore(db_name, memtable_size));

        // Initialize (complex operations that can fail)
        if (instance->initialize()) {
            return instance;
        }

        // If initialization failed, instance will be destroyed automatically
        // and its destructor will clean up any partially initialized resources
    } catch (const std::bad_alloc& e) {
        std::cerr << "Memory allocation error: " << e.what() << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "Failed to create database: " << e.what() << std::endl;
    }

    return nullptr;
}

void KVStore::close() {
    std::lock_guard<std::mutex> lock(mutex_);

    // Flush memtable to SSTable
    if (memtable_.entry_count() > 0) {
        flush_memtable_internal();
    }

    // Clear WAL (data is now persisted in SSTables)
    wal_->clear();

    // Clear SSTable readers
    sstables_.clear();

    stats_.sst_files = 0;  // Reset since we've closed the readers
}

bool KVStore::put(const std::string& key, const std::string& value) {
    std::lock_guard<std::mutex> lock(mutex_);
    stats_.puts++;

    // Write to WAL first (for durability)
    if (!wal_->log_put(key, value)) {
        std::cerr << "Failed to write to WAL" << std::endl;
        return false;
    }

    // Insert into memtable
    if (!memtable_.put(key, value)) {
        // Memtable is full, need to flush
        if (!flush_memtable_internal()) {
            std::cerr << "Failed to flush memtable" << std::endl;
            return false;
        }

        // Retry insert into fresh memtable
        if (!memtable_.put(key, value)) {
            std::cerr << "Failed to insert after flush" << std::endl;
            return false;
        }
    }

    return true;
}

std::optional<std::string> KVStore::get(const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    stats_.gets++;

    // First check memtable
    auto memtable_result = memtable_.get(key);
    if (memtable_result.has_value()) {
        return memtable_result;
    }

    // Check if key is deleted in memtable
    if (memtable_.is_deleted(key)) {
        return std::nullopt;
    }

    // Search SSTables (newest to oldest)
    return search_sstables(key);
}

bool KVStore::remove(const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    stats_.deletes++;

    // Write delete to WAL
    if (!wal_->log_delete(key)) {
        std::cerr << "Failed to write delete to WAL" << std::endl;
        return false;
    }

    // Mark as deleted in memtable
    if (!memtable_.remove(key)) {
        // Memtable is full, need to flush
        if (!flush_memtable_internal()) {
            std::cerr << "Failed to flush memtable" << std::endl;
            return false;
        }

        // Retry delete in fresh memtable
        if (!memtable_.remove(key)) {
            std::cerr << "Failed to delete after flush" << std::endl;
            return false;
        }
    }

    return true;
}

std::vector<std::pair<std::string, std::string>>
KVStore::scan(const std::string& start_key, const std::string& end_key) {
    std::lock_guard<std::mutex> lock(mutex_);
    stats_.scans++;

    // Map to collect results (automatically sorted by key)
    // We use a map to deduplicate keys (newest wins)
    std::map<std::string, std::string> result_map;

    // Scan memtable
    auto memtable_start = memtable_.begin();
    auto memtable_end = memtable_.end();

    // Find first key >= start_key in memtable
    auto it = std::find_if(memtable_start, memtable_end,
        [&start_key](const auto& pair) {
            return pair.first >= start_key;
        });

    // Collect from memtable
    for (; it != memtable_end; ++it) {
        const auto& [key, entry] = *it;

        // Stop if key > end_key
        if (key > end_key) break;

        // Skip deleted entries
        if (entry.is_deleted) continue;

        // Add to results (memtable is newest, so overwrite any existing)
        result_map[key] = entry.value;
    }

    // Scan SSTables (newest to oldest)
    scan_sstables(start_key, end_key, result_map);

    // Convert map to vector
    std::vector<std::pair<std::string, std::string>> results;
    results.reserve(result_map.size());
    for (const auto& [key, value] : result_map) {
        results.emplace_back(key, value);
    }

    return results;
}

void KVStore::flush_memtable() {
    std::lock_guard<std::mutex> lock(mutex_);
    flush_memtable_internal();
}

bool KVStore::flush_memtable_internal() {
    if (memtable_.entry_count() == 0) {
        return true;  // Nothing to flush
    }

    // Get all entries from memtable
    auto entries = memtable_.get_all_entries();

    // Generate SSTable filename
    std::string sst_filename = generate_sst_filename();
    std::string sst_path = (fs::path(db_path_) / sst_filename).string();

    // Write to SSTable
    if (!SSTableWriter::write(sst_path, entries)) {
        std::cerr << "Failed to write SSTable: " << sst_path << std::endl;
        return false;
    }

    // Create reader for new SSTable
    auto reader = std::make_unique<SSTableReader>(sst_path);
    if (!reader->is_valid()) {
        std::cerr << "Failed to create SSTable reader for: " << sst_path << std::endl;
        return false;
    }

    // Add to front (newest first)
    sstables_.insert(sstables_.begin(), std::move(reader));

    // Clear memtable
    memtable_.clear();

    // Clear WAL (data is now persisted)
    wal_->clear();

    // Update stats
    stats_.memtable_flushes++;
    stats_.sst_files = sstables_.size();
    stats_.total_data_size += entries.size();

    return true;
}

void KVStore::load_existing_sstables() {
    // Clear existing
    sstables_.clear();

    // Collect all .sst files in directory
    std::vector<std::string> sst_files;
    for (const auto& entry : fs::directory_iterator(db_path_)) {
        if (entry.is_regular_file() && entry.path().extension() == ".sst") {
            sst_files.push_back(entry.path().string());
        }
    }

    // Sort by filename (which should include timestamp)
    std::sort(sst_files.begin(), sst_files.end(), std::greater<>());

    // Load each SSTable
    for (const auto& file : sst_files) {
        auto reader = std::make_unique<SSTableReader>(file);
        if (reader->is_valid()) {
            sstables_.push_back(std::move(reader));

            // Extract counter from filename for unique naming
            std::string filename = fs::path(file).filename().string();
            try {
                // Filename format: sst_<counter>_<timestamp>.sst
                size_t start = filename.find('_') + 1;
                size_t end = filename.find('_', start);
                if (start != std::string::npos && end != std::string::npos) {
                    uint64_t counter = std::stoull(filename.substr(start, end - start));
                    if (counter >= sst_counter_) {
                        sst_counter_ = counter + 1;
                    }
                }
            } catch (...) {
                // If parsing fails, just increment
                sst_counter_++;
            }
        }
    }

    stats_.sst_files = sstables_.size();
}

void KVStore::recover_from_wal() {
    // Read all entries from WAL
    auto entries = wal_->read_all_entries();

    if (entries.empty()) {
        return;  // No recovery needed
    }

    std::cout << "Recovering " << entries.size() << " entries from WAL..." << std::endl;

    // Replay entries
    for (const auto& entry : entries) {
        switch (entry.type) {
            case WriteAheadLog::OpType::PUT:
                memtable_.put(entry.key, entry.value);
                break;
            case WriteAheadLog::OpType::DELETE:
                memtable_.remove(entry.key);
                break;
        }

        // Check if memtable needs flushing during recovery
        if (memtable_.should_flush()) {
            flush_memtable_internal();
        }
    }

    // If there are remaining entries in memtable after recovery,
    // they'll be flushed on the next put or when close() is called
}

std::string KVStore::generate_sst_filename() const {
    auto now = std::chrono::system_clock::now();
    auto timestamp = std::chrono::duration_cast<std::chrono::microseconds>(
        now.time_since_epoch()).count();

    std::stringstream ss;
    ss << "sst_" << std::setw(6) << std::setfill('0') << sst_counter_
       << "_" << timestamp << ".sst";

    return ss.str();
}

std::optional<std::string> KVStore::search_sstables(const std::string& key) const {
    // Search SSTables from newest to oldest
    for (const auto& sst : sstables_) {
        auto result = sst->get(key);
        if (result.has_value()) {
            return result;  // Found (could be actual value or tombstone handled by SSTableReader)
        }

        // If key exists but is deleted, SSTableReader returns nullopt
        // But we need to stop searching if we found a tombstone
        if (sst->is_deleted(key)) {
            return std::nullopt;  // Key is deleted in this SSTable
        }
    }

    return std::nullopt;  // Not found in any SSTable
}

void KVStore::scan_sstables(const std::string& start_key,
                            const std::string& end_key,
                            std::map<std::string, std::string>& results) const {
    // We'll scan each SSTable and only add keys that aren't already in results
    // Since we process newest to oldest, older entries won't overwrite newer ones

    for (const auto& sst : sstables_) {
        // For simplicity, we'll get all keys from SSTable and filter
        // In a real implementation, you'd want range query in SSTableReader
        auto all_keys = sst->get_all_keys();

        for (const auto& key : all_keys) {
            // Check if key is in range
            if (key >= start_key && key <= end_key) {
                // Check if key already in results (newer SSTable already has it)
                if (results.find(key) == results.end()) {
                    // Key not in results yet, check if it's not deleted
                    auto value = sst->get(key);
                    if (value.has_value()) {
                        results[key] = value.value();
                    }
                    // If deleted, don't add it (and it won't appear in newer SSTables)
                }
            }
        }
    }
}

KVStore::KVDBStats KVStore::get_stats() const {
    std::lock_guard<std::mutex> lock(mutex_);
    KVDBStats s = stats_;
    s.sst_files = sstables_.size();
    return s;
}

std::string KVStore::get_db_path() const {
    return db_path_;
}