//
// Created by Zekun Liu on 2025-12-07.
//

#include "test_compaction.h"
#include <memory>
#include <random>
#include <iostream>
#include <filesystem>
#include "test_helper.h"
#include "../SSTableReader.h"
#include "../Memtable.h"
#include "../SSTableWriter.h"
#include "../Compactor.h"

namespace fs = std::filesystem;
using namespace std::chrono;

// Helper to create an SSTable from test data
std::shared_ptr<SSTableReader> create_test_sstable(const std::string& filename,
                                                  const std::vector<std::pair<std::string, std::string>>& data) {
    // Create memtable with data
    Memtable memtable(1024 * 1024); // 1MB memtable

    for (const auto& [key, value] : data) {
        if (value.empty()) {
            memtable.remove(key); // Tombstone
        } else {
            memtable.put(key, value);
        }
    }

    // Write to SSTable
    auto entries = memtable.get_all_entries();
    if (!SSTableWriter::write(filename, entries)) {
        std::cerr << "  Failed to write SSTable: " << filename << std::endl;
        return nullptr;
    }

    // Load and return
    auto sstable = std::make_shared<SSTableReader>(filename);
    if (!sstable->is_valid()) {
        std::cerr << "  Failed to load SSTable: " << filename << std::endl;
        return nullptr;
    }

    return sstable;
}

// Test 1: Basic merge of non-overlapping SSTables
bool test_compactor_basic_merge(const std::string& test_dir) {
    std::cout << "  Testing basic merge..." << std::endl;

    // Create buffer pool
    auto buffer_pool = std::make_shared<BufferPool>(10 * 1024 * 1024);

    // Create compactor
    Compactor::Config config;
    config.buffer_size = 4096;
    Compactor compactor(buffer_pool, config);

    // Create two SSTables with non-overlapping keys
    std::vector<std::pair<std::string, std::string>> data1 = {
        {"apple", "red"},
        {"banana", "yellow"},
        {"cherry", "red"}
    };

    std::vector<std::pair<std::string, std::string>> data2 = {
        {"date", "brown"},
        {"elderberry", "purple"},
        {"fig", "green"}
    };

    auto sstable1 = create_test_sstable(make_test_path(test_dir, "test1.sst"), data1);
    auto sstable2 = create_test_sstable(make_test_path(test_dir, "test2.sst"), data2);

    if (!sstable1 || !sstable2) {
        return false;
    }

    std::vector<std::shared_ptr<SSTableReader>> input = {sstable1, sstable2};

    // Perform compaction
    auto result = compactor.compact(input, 1, false); // Not largest level

    if (result.empty()) {
        std::cerr << "  Compaction failed to produce output" << std::endl;
        return false;
    }

    auto compacted = result[0];

    // Verify all keys are present
    for (const auto& [key, expected_value] : data1) {
        auto value = compacted->get(key);
        if (!value || *value != expected_value) {
            std::cerr << "  Key '" << key << "' missing or incorrect in merged SSTable" << std::endl;
            return false;
        }
    }

    for (const auto& [key, expected_value] : data2) {
        auto value = compacted->get(key);
        if (!value || *value != expected_value) {
            std::cerr << "  Key '" << key << "' missing or incorrect in merged SSTable" << std::endl;
            return false;
        }
    }

    // Verify correct number of entries
    if (compacted->size() != 6) {
        std::cerr << "  Expected 6 entries, got " << compacted->size() << std::endl;
        return false;
    }

    return true;
}

// Test 2: Duplicate removal (keep newest)
bool test_compactor_duplicate_removal(const std::string& test_dir) {
    std::cout << "  Testing duplicate removal..." << std::endl;

    auto buffer_pool = std::make_shared<BufferPool>(10 * 1024 * 1024);
    Compactor::Config config;
    config.buffer_size = 10 * 1024 * 1024;
    Compactor compactor(buffer_pool, config);

    // Create SSTables with overlapping keys (different values)
    // sstable1 has older values, sstable2 has newer values for some keys
    std::vector<std::pair<std::string, std::string>> data1 = {
        {"key1", "value1_old"},
        {"key2", "value2_old"},
        {"key3", "value3"}
    };

    std::vector<std::pair<std::string, std::string>> data2 = {
        {"key1", "value1_new"},  // Update
        {"key2", "value2_new"},  // Update
        {"key4", "value4"}       // New key
    };

    auto sstable1 = create_test_sstable(make_test_path(test_dir, "dup1.sst"), data1);
    auto sstable2 = create_test_sstable(make_test_path(test_dir, "dup2.sst"), data2);

    if (!sstable1 || !sstable2) {
        return false;
    }

    std::vector<std::shared_ptr<SSTableReader>> input = {sstable1, sstable2};

    // Perform compaction
    auto result = compactor.compact(input, 1, false);

    if (result.empty()) {
        std::cerr << "  Compaction failed" << std::endl;
        return false;
    }

    auto compacted = result[0];

    // Verify duplicates removed (newer values kept)
    std::vector<std::pair<std::string, std::string>> expected = {
        {"key1", "value1_new"},  // From sstable2 (newer)
        {"key2", "value2_new"},  // From sstable2 (newer)
        {"key3", "value3"},      // From sstable1 (only in sstable1)
        {"key4", "value4"}       // From sstable2 (only in sstable2)
    };

    // Check all expected keys exist with correct values
    for (const auto& [key, expected_value] : expected) {
        auto value = compacted->get(key);
        if (!value || *value != expected_value) {
            std::cerr << "  Key '" << key << "' has value '"
                      << (value ? *value : "NOT FOUND")
                      << "', expected '" << expected_value << "'" << std::endl;
            return false;
        }
    }

    // Verify no extra entries
    if (compacted->size() != 4) {
        std::cerr << "  Expected 4 entries after duplicate removal, got "
                  << compacted->size() << std::endl;
        return false;
    }

    // Check statistics
    auto stats = compactor.get_stats();
    if (stats.duplicates_removed < 2) { // key1 and key2 duplicates
        std::cerr << "  Expected at least 2 duplicates removed, got "
                  << stats.duplicates_removed << std::endl;
        return false;
    }

    return true;
}

// Test 3: Tombstone handling (not at largest level)
bool test_compactor_tombstone_handling(const std::string& test_dir) {
    std::cout << "  Testing tombstone handling..." << std::endl;

    auto buffer_pool = std::make_shared<BufferPool>(10 * 1024 * 1024);
    Compactor::Config config;
    config.buffer_size = 4096;
    Compactor compactor(buffer_pool, config);

    // Create SSTables with tombstones
    std::vector<std::pair<std::string, std::string>> data1 = {
        {"key1", "value1"},
        {"key2", "value2"},
        {"key3", "value3"}
    };

    std::vector<std::pair<std::string, std::string>> data2 = {
        {"key2", ""},  // Tombstone for key2
        {"key4", "value4"},
        {"key5", "value5"}
    };

    auto sstable1 = create_test_sstable(make_test_path(test_dir, "tomb1.sst"), data1);
    auto sstable2 = create_test_sstable(make_test_path(test_dir, "tomb2.sst"), data2);

    if (!sstable1 || !sstable2) {
        return false;
    }

    std::vector<std::shared_ptr<SSTableReader>> input = {sstable1, sstable2};

    // NOT at largest level - tombstones should be kept
    auto result = compactor.compact(input, 1, false);

    if (result.empty()) {
        std::cerr << "  Compaction failed" << std::endl;
        return false;
    }

    auto compacted = result[0];

    // Verify tombstone is preserved (not at largest level)
    if (!compacted->is_deleted("key2")) {
        std::cerr << "  Tombstone for key2 should be preserved (not at largest level)" << std::endl;
        return false;
    }

    // Verify other keys
    std::vector<std::pair<std::string, std::string>> expected = {
        {"key1", "value1"},
        {"key3", "value3"},
        {"key4", "value4"},
        {"key5", "value5"}
    };

    for (const auto& [key, expected_value] : expected) {
        auto value = compacted->get(key);
        if (!value || *value != expected_value) {
            std::cerr << "  Key '" << key << "' incorrect" << std::endl;
            return false;
        }
    }

    // Verify tombstone count in statistics
    auto stats = compactor.get_stats();
    if (stats.tombstones_removed > 0) {
        std::cerr << "  No tombstones should be removed (not at largest level)" << std::endl;
        return false;
    }

    return true;
}

// Test 4: Tombstone removal at largest level
bool test_compactor_largest_level_tombstones(const std::string& test_dir) {
    std::cout << "  Testing tombstone removal at largest level..." << std::endl;

    auto buffer_pool = std::make_shared<BufferPool>(10 * 1024 * 1024);
    Compactor::Config config;
    config.buffer_size = 4096;
    Compactor compactor(buffer_pool, config);

    // Create SSTable with tombstones
    std::vector<std::pair<std::string, std::string>> data = {
        {"key1", "value1"},
        {"key2", ""},  // Tombstone
        {"key3", "value3"},
        {"key4", ""},  // Tombstone
        {"key5", "value5"}
    };

    auto sstable = create_test_sstable(make_test_path(test_dir, "largest_tomb.sst"), data);
    if (!sstable) {
        return false;
    }

    std::vector<std::shared_ptr<SSTableReader>> input = {sstable};

    // AT largest level - tombstones should be removed
    auto result = compactor.compact(input, 6, true); // Largest level = true

    if (result.empty()) {
        std::cerr << "  Compaction failed" << std::endl;
        return false;
    }

    auto compacted = result[0];

    // Verify tombstones are removed
    if (compacted->get("key2") || compacted->is_deleted("key2")) {
        std::cerr << "  Tombstone for key2 should be removed at largest level" << std::endl;
        return false;
    }

    if (compacted->get("key4") || compacted->is_deleted("key4")) {
        std::cerr << "  Tombstone for key4 should be removed at largest level" << std::endl;
        return false;
    }

    // Verify non-tombstone keys are preserved
    std::vector<std::string> expected_keys = {"key1", "key3", "key5"};
    for (const auto& key : expected_keys) {
        if (!compacted->get(key)) {
            std::cerr << "  Key '" << key << "' should be preserved" << std::endl;
            return false;
        }
    }

    // Verify entry count
    if (compacted->size() != 3) {
        std::cerr << "  Expected 3 entries after tombstone removal, got "
                  << compacted->size() << std::endl;
        return false;
    }

    // Check statistics
    auto stats = compactor.get_stats();
    if (stats.tombstones_removed != 2) {
        std::cerr << "  Expected 2 tombstones removed, got "
                  << stats.tombstones_removed << std::endl;
        return false;
    }

    return true;
}

// Test 5: Empty values (not tombstones)
bool test_compactor_empty_values(const std::string& test_dir) {
    std::cout << "  Testing empty values (not tombstones)..." << std::endl;

    auto buffer_pool = std::make_shared<BufferPool>(10 * 1024 * 1024);
    Compactor::Config config;
    config.buffer_size = 4096;
    Compactor compactor(buffer_pool, config);

    // Create SSTable with empty values (not deletes)
    // Note: In our current design, empty string = tombstone
    // This test would need modification if we distinguish empty values from tombstones

    std::vector<std::pair<std::string, std::string>> data = {
        {"key1", "normal"},
        {"key2", ""},  // Currently treated as tombstone
        {"key3", "another"}
    };

    auto sstable = create_test_sstable(make_test_path(test_dir, "empty_vals.sst"), data);
    if (!sstable) {
        return false;
    }

    std::vector<std::shared_ptr<SSTableReader>> input = {sstable};

    // Test at non-largest level
    auto result = compactor.compact(input, 1, false);

    if (result.empty()) {
        std::cerr << "  Compaction failed" << std::endl;
        return false;
    }

    auto compacted = result[0];

    // Currently, empty strings are treated as tombstones
    // So key2 should be marked as deleted
    if (!compacted->is_deleted("key2")) {
        std::cerr << "  Empty string should be treated as tombstone" << std::endl;
        return false;
    }

    // Other keys should exist
    if (!compacted->get("key1") || !compacted->get("key3")) {
        std::cerr << "  Non-empty keys should be preserved" << std::endl;
        return false;
    }

    return true;
}

// Test 6: Merge multiple SSTables (3+)
bool test_compactor_multiple_sstables(const std::string& test_dir) {
    std::cout << "  Testing merge of multiple SSTables..." << std::endl;

    auto buffer_pool = std::make_shared<BufferPool>(10 * 1024 * 1024);
    Compactor::Config config;
    config.buffer_size = 4096;
    config.max_merge_fan_in = 5; // Allow merging up to 5 SSTables
    Compactor compactor(buffer_pool, config);

    // Create 4 SSTables with various overlaps
    std::vector<std::shared_ptr<SSTableReader>> sstables;

    for (int i = 0; i < 4; i++) {
        std::vector<std::pair<std::string, std::string>> data;

        // Each SSTable has some unique keys and some overlapping
        for (int j = 0; j < 5; j++) {
            std::string key = "key_" + std::to_string(i) + "_" + std::to_string(j);
            std::string value = "value_" + std::to_string(i) + "_" + std::to_string(j);
            data.emplace_back(key, value);
        }

        // Add one overlapping key with each previous SSTable
        for (int prev = 0; prev <= i; prev++) {
            std::string key = "overlap_" + std::to_string(prev);
            std::string value = "new_value_from_" + std::to_string(i);
            data.emplace_back(key, value);
        }

        auto sstable = create_test_sstable(
            make_test_path(test_dir, "multi_" + std::to_string(i) + ".sst"), data);

        if (!sstable) {
            return false;
        }
        sstables.push_back(sstable);
    }

    // Perform compaction
    auto result = compactor.compact(sstables, 1, false);

    if (result.empty()) {
        std::cerr << "  Compaction failed" << std::endl;
        return false;
    }

    auto compacted = result[0];

    // Count total unique keys
    // 4 SSTables * 5 unique keys each = 20 unique keys
    // Plus 0 + 1 + 2 + 3 = 6 overlapping keys (overlap_0, overlap_1, overlap_2, overlap_3)
    // But overlapping keys get overwritten, so we should have the newest versions

    // Verify all unique keys exist
    for (int i = 0; i < 4; i++) {
        for (int j = 0; j < 5; j++) {
            std::string key = "key_" + std::to_string(i) + "_" + std::to_string(j);
            if (!compacted->get(key)) {
                std::cerr << "  Unique key missing: " << key << std::endl;
                return false;
            }
        }
    }

    // Verify overlapping keys have newest values
    for (int i = 0; i < 4; i++) {
        std::string key = "overlap_" + std::to_string(i);
        auto value = compacted->get(key);
        std::string expected = "new_value_from_3"; // From last SSTable (i=3)

        // overlap_0 exists in SSTables 1, 2, 3 (indices 1, 2, 3)
        // overlap_1 exists in SSTables 2, 3 (indices 2, 3)
        // overlap_2 exists in SSTable 3 (index 3)
        // overlap_3 doesn't exist in any previous, only in SSTable 3

        if (i < 3) { // overlap_0, overlap_1, overlap_2 should have value from SSTable 3
            if (!value || *value != expected) {
                std::cerr << "  Overlap key '" << key << "' has wrong value" << std::endl;
                return false;
            }
        } else { // overlap_3 should have value from SSTable 3
            if (!value || *value != "new_value_from_3") {
                std::cerr << "  Overlap key '" << key << "' has wrong value" << std::endl;
                return false;
            }
        }
    }

    // Check statistics
    auto stats = compactor.get_stats();
    std::cout << "    Statistics: " << stats.entries_read << " read, "
              << stats.entries_written << " written, "
              << stats.duplicates_removed << " duplicates removed" << std::endl;

    return true;
}

// Test 7: Buffer management (small buffer to force multiple writes)
bool test_compactor_buffer_management(const std::string& test_dir) {
    std::cout << "  Testing buffer management..." << std::endl;

    auto buffer_pool = std::make_shared<BufferPool>(10 * 1024 * 1024);
    Compactor::Config config;
    config.buffer_size = 100; // Very small buffer to test flushing
    Compactor compactor(buffer_pool, config);

    // Create SSTable with data larger than buffer
    std::vector<std::pair<std::string, std::string>> data;
    for (int i = 0; i < 50; i++) {
        std::string key = "key" + std::to_string(i);
        std::string value(50, 'x'); // 50-byte values
        data.emplace_back(key, value);
    }

    auto sstable = create_test_sstable(make_test_path(test_dir, "buffer_test.sst"), data);
    if (!sstable) {
        return false;
    }

    std::vector<std::shared_ptr<SSTableReader>> input = {sstable};

    // Perform compaction with small buffer
    auto result = compactor.compact(input, 1, false);

    if (result.empty()) {
        std::cerr << "  Compaction failed with small buffer" << std::endl;
        return false;
    }

    auto compacted = result[0];

    // Verify all data preserved
    if (compacted->size() != 50) {
        std::cerr << "  Expected 50 entries, got " << compacted->size() << std::endl;
        return false;
    }

    for (int i = 0; i < 50; i++) {
        std::string key = "key" + std::to_string(i);
        auto value = compacted->get(key);
        if (!value || value->size() != 50) {
            std::cerr << "  Key '" << key << "' missing or incorrect" << std::endl;
            return false;
        }
    }

    auto stats = compactor.get_stats();
    std::cout << "    Small buffer test: " << stats.bytes_written << " bytes written" << std::endl;

    return true;
}

// Test 8: Edge cases
bool test_compactor_edge_cases(const std::string& test_dir) {
    std::cout << "  Testing edge cases..." << std::endl;

    auto buffer_pool = std::make_shared<BufferPool>(10 * 1024 * 1024);
    Compactor::Config config;
    config.buffer_size = 4096;
    Compactor compactor(buffer_pool, config);

    // Test 8a: Empty SSTable
    std::vector<std::pair<std::string, std::string>> empty_data;
    auto empty_sstable = create_test_sstable(make_test_path(test_dir, "empty.sst"), empty_data);

    if (empty_sstable) {
        std::vector<std::shared_ptr<SSTableReader>> empty_input = {empty_sstable};
        auto empty_result = compactor.compact(empty_input, 1, false);

        // Empty compaction should produce empty SSTable or no SSTable
        if (!empty_result.empty()) {
            if (empty_result[0]->size() != 0) {
                std::cerr << "  Empty SSTable compaction should produce empty result" << std::endl;
                return false;
            }
        }
    }

    // Test 8b: Single SSTable (no-op compaction)
    std::vector<std::pair<std::string, std::string>> single_data = {
        {"single", "value"}
    };

    auto single_sstable = create_test_sstable(make_test_path(test_dir, "single.sst"), single_data);
    if (!single_sstable) {
        return false;
    }

    std::vector<std::shared_ptr<SSTableReader>> single_input = {single_sstable};
    auto single_result = compactor.compact(single_input, 1, false);

    if (single_result.empty()) {
        std::cerr << "  Single SSTable compaction failed" << std::endl;
        return false;
    }

    // Test 8c: All tombstones (not at largest level)
    std::vector<std::pair<std::string, std::string>> all_tombs = {
        {"key1", ""},
        {"key2", ""},
        {"key3", ""}
    };

    auto tomb_sstable = create_test_sstable(make_test_path(test_dir, "all_tombs.sst"), all_tombs);
    if (!tomb_sstable) {
        return false;
    }

    std::vector<std::shared_ptr<SSTableReader>> tomb_input = {tomb_sstable};
    auto tomb_result = compactor.compact(tomb_input, 1, false); // Not largest level

    if (!tomb_result.empty()) {
        auto tomb_compacted = tomb_result[0];
        // All tombstones should be preserved (not at largest level)
        if (tomb_compacted->size() != 3) {
            std::cerr << "  All tombstones should be preserved when not at largest level" << std::endl;
            return false;
        }
    }

    // Test 8d: Very large key/value
    std::vector<std::pair<std::string, std::string>> large_data = {
        {std::string(1000, 'k'), std::string(10000, 'v')} // 1KB key, 10KB value
    };

    auto large_sstable = create_test_sstable(make_test_path(test_dir, "large.sst"), large_data);
    if (!large_sstable) {
        return false;
    }

    std::vector<std::shared_ptr<SSTableReader>> large_input = {large_sstable};
    auto large_result = compactor.compact(large_input, 1, false);

    if (large_result.empty()) {
        std::cerr << "  Large key/value compaction failed" << std::endl;
        return false;
    }

    auto large_compacted = large_result[0];
    if (large_compacted->size() != 1) {
        std::cerr << "  Large key/value not preserved" << std::endl;
        return false;
    }

    return true;
}

// Test 9: Performance with many entries
bool test_compactor_performance(const std::string& test_dir) {
    std::cout << "  Testing performance..." << std::endl;

    auto buffer_pool = std::make_shared<BufferPool>(100 * 1024 * 1024); // 100MB
    Compactor::Config config;
    config.buffer_size = 4096;
    Compactor compactor(buffer_pool, config);

    // Create 3 SSTables with many entries
    std::vector<std::shared_ptr<SSTableReader>> sstables;
    const int ENTRIES_PER_SST = 1000;

    for (int sst_idx = 0; sst_idx < 3; sst_idx++) {
        std::vector<std::pair<std::string, std::string>> data;

        for (int i = 0; i < ENTRIES_PER_SST; i++) {
            std::string key = "key_" + std::to_string(sst_idx) + "_" + std::to_string(i);
            std::string value = "value_" + std::to_string(sst_idx) + "_" + std::to_string(i);
            data.emplace_back(key, value);
        }

        auto sstable = create_test_sstable(
            make_test_path(test_dir, "perf_" + std::to_string(sst_idx) + ".sst"), data);

        if (!sstable) {
            return false;
        }
        sstables.push_back(sstable);
    }

    auto start = std::chrono::high_resolution_clock::now();

    // Perform compaction
    auto result = compactor.compact(sstables, 1, false);

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    if (result.empty()) {
        std::cerr << "  Performance test compaction failed" << std::endl;
        return false;
    }

    auto compacted = result[0];

    // Verify
    if (compacted->size() != ENTRIES_PER_SST * 3) {
        std::cerr << "  Performance test: expected " << (ENTRIES_PER_SST * 3)
                  << " entries, got " << compacted->size() << std::endl;
        return false;
    }

    auto stats = compactor.get_stats();
    std::cout << "    Performance: " << duration.count() << " ms" << std::endl;
    std::cout << "    " << stats.entries_read << " entries read" << std::endl;
    std::cout << "    " << stats.entries_written << " entries written" << std::endl;
    std::cout << "    " << stats.bytes_read / 1024 << " KB read" << std::endl;
    std::cout << "    " << stats.bytes_written / 1024 << " KB written" << std::endl;

    return true;
}

// Test 10: Statistics tracking
bool test_compactor_statistics(const std::string& test_dir) {
    std::cout << "  Testing statistics tracking..." << std::endl;

    auto buffer_pool = std::make_shared<BufferPool>(10 * 1024 * 1024);
    Compactor::Config config;
    config.buffer_size = 4096;
    Compactor compactor(buffer_pool, config);

    // Get initial stats
    auto initial_stats = compactor.get_stats();

    // Create and compact some data
    std::vector<std::pair<std::string, std::string>> data1 = {
        {"a", "1"},
        {"b", "2"},
        {"c", "3"}
    };

    std::vector<std::pair<std::string, std::string>> data2 = {
        {"b", "22"}, // Update
        {"d", "4"}
    };

    auto sstable1 = create_test_sstable(make_test_path(test_dir, "stats1.sst"), data1);
    auto sstable2 = create_test_sstable(make_test_path(test_dir, "stats2.sst"), data2);

    if (!sstable1 || !sstable2) {
        return false;
    }

    std::vector<std::shared_ptr<SSTableReader>> input = {sstable1, sstable2};

    // First compaction
    auto result1 = compactor.compact(input, 1, false);
    auto stats1 = compactor.get_stats();

    if (stats1.compactions_performed != initial_stats.compactions_performed + 1) {
        std::cerr << "  compactions_performed not incremented" << std::endl;
        return false;
    }

    if (stats1.entries_read < 5) { // a,b,c from first, b,d from second
        std::cerr << "  entries_read incorrect: " << stats1.entries_read << std::endl;
        return false;
    }

    if (stats1.entries_written != 4) { // a, b(new), c, d
        std::cerr << "  entries_written incorrect: " << stats1.entries_written << std::endl;
        return false;
    }

    if (stats1.duplicates_removed != 1) { // b duplicate
        std::cerr << "  duplicates_removed incorrect: " << stats1.duplicates_removed << std::endl;
        return false;
    }

    // Second compaction (tombstone test)
    std::vector<std::pair<std::string, std::string>> data3 = {
        {"a", ""}, // Tombstone
        {"e", "5"}
    };

    auto sstable3 = create_test_sstable(make_test_path(test_dir, "stats3.sst"), data3);
    if (!sstable3) {
        return false;
    }

    std::vector<std::shared_ptr<SSTableReader>> input2 = {result1[0], sstable3};
    auto result2 = compactor.compact(input2, 6, true); // Largest level

    auto stats2 = compactor.get_stats();

    if (stats2.compactions_performed != 2) {
        std::cerr << "  Second compaction not counted" << std::endl;
        return false;
    }

    if (stats2.tombstones_removed < 1) { // a tombstone removed at largest level
        std::cerr << "  tombstones_removed incorrect: " << stats2.tombstones_removed << std::endl;
        return false;
    }

    std::cout << "    Statistics verified: " << stats2.compactions_performed
              << " compactions performed" << std::endl;

    return true;
}

// Main test runner
int compaction_tests_main() {
    // Create unique test directory
    auto now = std::chrono::high_resolution_clock::now();
    auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()).count();
    std::string test_dir = "compaction_tests_" + std::to_string(timestamp);

    // Create the directory
    try {
        if (!fs::create_directory(test_dir)) {
            std::cerr << "Failed to create test directory: " << test_dir << std::endl;
            return 1;
        }
    } catch (const fs::filesystem_error& e) {
        std::cerr << "Filesystem error creating test directory: " << e.what() << std::endl;
        return 1;
    }

    std::cout << "\nRunning Compaction Tests" << std::endl;
    std::cout << "========================" << std::endl;
    std::cout << "Test directory: " << test_dir << std::endl;
    std::cout << std::endl;

    // Store all test functions
    std::vector<std::pair<std::string, std::function<bool(const std::string&)>>> tests = {
        {"1. Basic Merge", test_compactor_basic_merge},
        {"2. Duplicate Removal", test_compactor_duplicate_removal},
        {"3. Tombstone Handling", test_compactor_tombstone_handling},
        {"4. Largest Level Tombstones", test_compactor_largest_level_tombstones},
        {"5. Empty Values", test_compactor_empty_values},
        {"6. Multiple SSTables", test_compactor_multiple_sstables},
        {"7. Buffer Management", test_compactor_buffer_management},
        {"8. Edge Cases", test_compactor_edge_cases},
        {"9. Performance", test_compactor_performance},
        {"10. Statistics", test_compactor_statistics}
    };

    int passed = 0;
    int total = static_cast<int>(tests.size());

    for (const auto& [name, test_func] : tests) {
        try {
            bool result = test_func(test_dir);
            print_test_result(name, result);
            if (result) passed++;
        } catch (const std::exception& e) {
            std::cout << "X " << name << " (Exception: " << e.what() << ")" << std::endl;
        } catch (...) {
            std::cout << "X " << name << " (Unknown exception)" << std::endl;
        }
    }

    std::cout << "\n" << std::string(50, '=') << std::endl;
    std::cout << "Results: " << passed << "/" << total << " tests passed" << std::endl;

    // Optional: Clean up test directory
    try {
        fs::remove_all(test_dir);
        std::cout << "Cleaned up test directory: " << test_dir << std::endl;
    } catch (const fs::filesystem_error&) {
        std::cout << "\nNote: Could not clean up test directory: " << test_dir << std::endl;
        std::cout << "You may need to manually delete it." << std::endl;
    }

    if (passed == total) {
        std::cout << "\nAll compaction tests passed!" << std::endl;
        return 0;
    } else {
        std::cout << "\nSome compaction tests failed" << std::endl;
        return 1;
    }
}