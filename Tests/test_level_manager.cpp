//
// Created by K on 2025-12-06.
//

#include "test_level_manager.h"
#include "../LevelManager.h"
#include "../SSTableReader.h"
#include "../SSTableWriter.h"
#include "../Memtable.h"
#include "../BufferPool.h"
#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <filesystem>
#include <chrono>
#include <random>
#include <algorithm>
#include <cstring>
#include <functional>
#include <set>

#include "test_helper.h"

namespace fs = std::filesystem;
using namespace std::chrono;

// Helper function to create test config
LevelManager::Config create_test_config(size_t max_levels = 4,
                         size_t level0_max_sstables = 2,
                         size_t size_ratio = 2) {
    LevelManager::Config config;
    config.max_levels = max_levels;
    config.level0_max_sstables = level0_max_sstables;
    config.size_ratio = size_ratio;
    return config;
}

// Helper to create a real SSTable file from key-value pairs
std::string create_real_sstable(const std::string& filename,
                                const std::vector<std::pair<std::string, std::string>>& data) {
    Memtable mt(4096);  // 4KB memtable

    for (const auto& [key, value] : data) {
        mt.put(key, value);
    }

    if (!SSTableWriter::write_from_memtable(filename, mt)) {
        throw std::runtime_error("Failed to create SSTable");
    }

    return filename;
}

// Helper to verify SSTable contains expected data
bool verify_sstable_content(const std::string& filename,
                           const std::vector<std::pair<std::string, std::string>>& expected_data) {
    SSTableReader reader(filename);
    if (!reader.is_valid()) {
        return false;
    }

    for (const auto& [key, expected_value] : expected_data) {
        auto value = reader.get(key);
        if (!value.has_value() || value.value() != expected_value) {
            std::cerr << "  Key '" << key << "' mismatch" << std::endl;
            return false;
        }
    }

    return true;
}

// Test 1: Basic initialization
bool test_level_manager_init(const std::string& test_dir) {
    std::string data_dir = make_test_path(test_dir, "init_test");
    auto buffer_pool = std::make_shared<BufferPool>(100);  // Small capacity
    auto config = create_test_config();

    LevelManager manager(data_dir, buffer_pool, config);

    // Check that directories were created
    for (int i = 0; i < static_cast<int>(config.max_levels); i++) {
        std::string level_dir = data_dir + "/level_" + std::to_string(i);
        if (!fs::exists(level_dir)) {
            std::cerr << "  Level directory not created: " << level_dir << std::endl;
            return false;
        }
    }

    // Check level initialization
    auto stats = manager.get_stats();
    if (stats.total_sstables != 0) {
        std::cerr << "  Expected 0 SSTables initially, got " << stats.total_sstables << std::endl;
        return false;
    }

    return true;
}

// Test 2: Loading existing SSTables
bool test_load_existing_sstables(const std::string& test_dir) {
    std::string data_dir = make_test_path(test_dir, "load_test");
    auto buffer_pool = std::make_shared<BufferPool>(100);
    auto config = create_test_config();

    // Create directories and real SST files before initializing LevelManager
    fs::create_directories(data_dir + "/level_0");
    fs::create_directories(data_dir + "/level_1");

    // Create real SST files
    std::string sst1 = data_dir + "/level_0/sstable_1.sst";
    create_real_sstable(sst1, {{"key1", "value1"}, {"key2", "value2"}});

    std::string sst2 = data_dir + "/level_0/sstable_3.sst";
    create_real_sstable(sst2, {{"key3", "value3"}, {"key4", "value4"}});

    std::string sst3 = data_dir + "/level_1/sstable_2.sst";
    create_real_sstable(sst3, {{"key5", "value5"}, {"key6", "value6"}});

    // Now initialize LevelManager
    LevelManager manager(data_dir, buffer_pool, config);

    auto stats = manager.get_stats();
    if (stats.total_sstables != 3) {
        std::cerr << "  Expected 3 SSTables loaded, got " << stats.total_sstables << std::endl;
        return false;
    }

    // Check level distribution
    if (stats.sstables_per_level[0] != 2) {
        std::cerr << "  Expected 2 SSTables in level 0, got " << stats.sstables_per_level[0] << std::endl;
        return false;
    }

    if (stats.sstables_per_level[1] != 1) {
        std::cerr << "  Expected 1 SSTable in level 1, got " << stats.sstables_per_level[1] << std::endl;
        return false;
    }

    return true;
}

// Test 3: Adding SSTable to level 0
bool test_add_sstable_level0(const std::string& test_dir) {
    std::string data_dir = make_test_path(test_dir, "add_level0_test");
    auto buffer_pool = std::make_shared<BufferPool>(100);
    auto config = create_test_config();

    LevelManager manager(data_dir, buffer_pool, config);

    // Create a real SST file
    std::string temp_sst = make_test_path(test_dir, "temp.sst");
    std::vector<std::pair<std::string, std::string>> data = {
        {"apple", "fruit"},
        {"banana", "yellow fruit"},
        {"carrot", "vegetable"}
    };
    create_real_sstable(temp_sst, data);

    // Create SSTableReader for the file
    auto sstable = std::make_shared<SSTableReader>(temp_sst);
    if (!sstable->is_valid()) {
        std::cerr << "  Failed to create valid SSTableReader" << std::endl;
        return false;
    }

    bool result = manager.add_sstable_level0(sstable);

    if (!result) {
        std::cerr << "  Failed to add SSTable to level 0" << std::endl;
        return false;
    }

    auto stats = manager.get_stats();
    if (stats.total_sstables != 1) {
        std::cerr << "  Expected 1 SSTable after add, got " << stats.total_sstables << std::endl;
        return false;
    }

    if (stats.sstables_per_level[0] != 1) {
        std::cerr << "  SSTable not added to level 0" << std::endl;
        return false;
    }

    return true;
}

// Test 4: Compaction task generation (level 0 full)
bool test_compaction_task_level0(const std::string& test_dir) {
    std::string data_dir = make_test_path(test_dir, "compaction_level0_test");
    auto buffer_pool = std::make_shared<BufferPool>(100);
    auto config = create_test_config(4, 2, 2);  // level0_max_sstables = 2

    LevelManager manager(data_dir, buffer_pool, config);

    // Add first SSTable to level 0
    std::string temp_sst1 = make_test_path(test_dir, "temp1.sst");
    create_real_sstable(temp_sst1, {{"a", "1"}, {"b", "2"}, {"c", "3"}});
    auto sstable1 = std::make_shared<SSTableReader>(temp_sst1);
    manager.add_sstable_level0(sstable1);

    // Add second SSTable to level 0 (should trigger compaction)
    std::string temp_sst2 = make_test_path(test_dir, "temp2.sst");
    create_real_sstable(temp_sst2, {{"d", "4"}, {"e", "5"}, {"f", "6"}});
    auto sstable2 = std::make_shared<SSTableReader>(temp_sst2);
    manager.add_sstable_level0(sstable2);

    // Get compaction task
    auto task = manager.get_compaction_task();

    if (!task.has_value()) {
        std::cerr << "  Expected compaction task, got none" << std::endl;
        return false;
    }

    if (task->source_level != 0) {
        std::cerr << "  Expected source level 0, got " << task->source_level << std::endl;
        return false;
    }

    if (task->target_level != 1) {
        std::cerr << "  Expected target level 1, got " << task->target_level << std::endl;
        return false;
    }

    if (task->input_sstables.size() != 2) {
        std::cerr << "  Expected 2 input SSTables, got " << task->input_sstables.size() << std::endl;
        return false;
    }

    return true;
}

// Test 5: Find candidate SSTables for key
bool test_find_candidate_sstables(const std::string& test_dir) {
    std::string data_dir = make_test_path(test_dir, "find_candidates_test");
    auto buffer_pool = std::make_shared<BufferPool>(100);
    auto config = create_test_config();

    // Create a LevelManager and manually populate it
    LevelManager manager(data_dir, buffer_pool, config);

    // Create SSTables with known key ranges
    std::string sst1_path = data_dir + "/level_0/sstable_1.sst";
    create_real_sstable(sst1_path, {{"apple", "fruit"}, {"banana", "yellow"}});

    std::string sst2_path = data_dir + "/level_1/sstable_2.sst";
    create_real_sstable(sst2_path, {{"cherry", "red"}, {"date", "sweet"}});

    std::string sst3_path = data_dir + "/level_2/sstable_3.sst";
    create_real_sstable(sst3_path, {{"elderberry", "berry"}, {"fig", "dry"}});

    // Reinitialize to load the SSTables
    LevelManager manager2(data_dir, buffer_pool, config);

    // Test finding a key that exists
    auto candidates = manager2.find_candidate_sstables("cherry");

    // In LSM tree, we should find candidates from level 0 first
    // Since we have overlapping ranges in level 0, we need to check all
    // The implementation should return at least one candidate
    if (candidates.empty()) {
        std::cerr << "  Expected at least one candidate for 'cherry'" << std::endl;
        return false;
    }

    // Test key not found (outside all ranges)
    candidates = manager2.find_candidate_sstables("zucchini");
    // Should be empty since no SSTable contains this key
    if (!candidates.empty()) {
        std::cerr << "  Expected no candidates for 'zucchini'" << std::endl;
        return false;
    }

    return true;
}

// Test 6: Find SSTables for range query
bool test_find_sstables_for_range(const std::string& test_dir) {
    std::string data_dir = make_test_path(test_dir, "range_test");
    auto buffer_pool = std::make_shared<BufferPool>(100);
    auto config = create_test_config();

    // Create a LevelManager and populate with SSTables
    LevelManager manager(data_dir, buffer_pool, config);

    // Create SSTables with overlapping and non-overlapping ranges
    std::string sst1_path = data_dir + "/level_0/sstable_1.sst";
    create_real_sstable(sst1_path, {
        {"apple", "fruit"},
        {"banana", "yellow"},
        {"cherry", "red"}
    });

    std::string sst2_path = data_dir + "/level_1/sstable_2.sst";
    create_real_sstable(sst2_path, {
        {"date", "sweet"},
        {"elderberry", "berry"},
        {"fig", "dry"}
    });

    std::string sst3_path = data_dir + "/level_2/sstable_3.sst";
    create_real_sstable(sst3_path, {
        {"grape", "bunch"},
        {"honeydew", "melon"},
        {"kiwi", "fuzzy"}
    });

    // Reinitialize to load
    LevelManager manager2(data_dir, buffer_pool, config);

    // Test range query that spans multiple SSTables
    auto candidates = manager2.find_sstables_for_range("cherry", "grape");

    // Should find SSTables that overlap with [cherry, grape]
    // sst1: apple-cherry (includes cherry)
    // sst2: date-fig (within range)
    // sst3: grape-kiwi (includes grape)
    if (candidates.size() < 2) {
        std::cerr << "  Expected at least 2 SSTables for range [cherry, grape], got "
                  << candidates.size() << std::endl;
        return false;
    }

    // Test range with no matches
    candidates = manager2.find_sstables_for_range("zucchini", "zzz");
    if (!candidates.empty()) {
        std::cerr << "  Expected no SSTables for range after 'z'" << std::endl;
        return false;
    }

    return true;
}

// Test 7: Level capacity and overflow detection
bool test_level_capacity(const std::string& test_dir) {
    std::string data_dir = make_test_path(test_dir, "capacity_test");
    auto buffer_pool = std::make_shared<BufferPool>(100);

    LevelManager::Config config;
    config.max_levels = 3;
    config.level0_max_sstables = 3;  // Will trigger after 3 SSTables
    config.size_ratio = 2;

    LevelManager manager(data_dir, buffer_pool, config);

    // Add SSTables until level 0 is full
    for (int i = 0; i < config.level0_max_sstables; i++) {
        std::string temp_sst = make_test_path(test_dir, "temp_" + std::to_string(i) + ".sst");
        std::vector<std::pair<std::string, std::string>> data;
        for (int j = 0; j < 10; j++) {
            data.push_back({std::to_string(i * 100 + j),
                           "value_" + std::to_string(i * 100 + j)});
        }
        create_real_sstable(temp_sst, data);
        auto sstable = std::make_shared<SSTableReader>(temp_sst);

        bool result = manager.add_sstable_level0(sstable);
        if (!result) {
            std::cerr << "  Failed to add SSTable " << i << " to level 0" << std::endl;
            return false;
        }
    }

    auto stats = manager.get_stats();
    if (stats.sstables_per_level[0] != config.level0_max_sstables) {
        std::cerr << "  Level 0 should have " << config.level0_max_sstables
                  << " SSTables, got " << stats.sstables_per_level[0] << std::endl;
        return false;
    }

    // Should have a compaction task now
    auto task = manager.get_compaction_task();
    if (!task.has_value()) {
        std::cerr << "  Expected compaction task when level 0 is full" << std::endl;
        return false;
    }

    return true;
}

// Test 8: Statistics tracking with real SSTables
bool test_statistics_tracking(const std::string& test_dir) {
    std::string data_dir = make_test_path(test_dir, "stats_test");
    auto buffer_pool = std::make_shared<BufferPool>(100);
    auto config = create_test_config();

    LevelManager manager(data_dir, buffer_pool, config);

    // Add some real SSTables
    for (int i = 0; i < 3; i++) {
        std::string temp_sst = make_test_path(test_dir, "stats_temp_" + std::to_string(i) + ".sst");

        // Create SSTable with varying sizes
        std::vector<std::pair<std::string, std::string>> data;
        int num_entries = (i + 1) * 5;  // 5, 10, 15 entries
        for (int j = 0; j < num_entries; j++) {
            std::string key = "key_" + std::to_string(i * 100 + j);
            std::string value = std::string(50 + j * 10, 'x');  // Varying value sizes
            data.push_back({key, value});
        }

        create_real_sstable(temp_sst, data);
        auto sstable = std::make_shared<SSTableReader>(temp_sst);
        manager.add_sstable_level0(sstable);
    }

    auto stats = manager.get_stats();

    // Check basic stats
    if (stats.total_sstables != 3) {
        std::cerr << "  Expected 3 total SSTables, got " << stats.total_sstables << std::endl;
        return false;
    }

    if (stats.sstables_created < 3) {
        std::cerr << "  Expected at least 3 SSTables created, got " << stats.sstables_created << std::endl;
        return false;
    }

    if (stats.sstables_per_level.size() != config.max_levels) {
        std::cerr << "  Expected stats for " << config.max_levels
                  << " levels, got " << stats.sstables_per_level.size() << std::endl;
        return false;
    }

    // Check that we have bytes per level (should be > 0 for level 0)
    if (stats.bytes_per_level.size() != config.max_levels) {
        std::cerr << "  Expected bytes for " << config.max_levels
                  << " levels, got " << stats.bytes_per_level.size() << std::endl;
        return false;
    }

    if (stats.bytes_per_level[0] == 0) {
        std::cerr << "  Level 0 should have non-zero bytes" << std::endl;
        return false;
    }

    return true;
}

// Test 10: Multi-level LSM tree operations
bool test_multi_level_operations(const std::string& test_dir) {
    std::string data_dir = make_test_path(test_dir, "multi_level_test");
    auto buffer_pool = std::make_shared<BufferPool>(100);

    // Small config for testing
    LevelManager::Config config;
    config.max_levels = 3;
    config.level0_max_sstables = 2;  // Compact after 2 SSTables
    config.size_ratio = 2;

    LevelManager manager(data_dir, buffer_pool, config);

    // Create and add SSTables to trigger multi-level behavior
    for (int i = 0; i < 4; i++) {  // Add 4 SSTables
        std::string temp_sst = make_test_path(test_dir, "multi_" + std::to_string(i) + ".sst");

        std::vector<std::pair<std::string, std::string>> data;
        for (int j = 0; j < 3; j++) {
            std::string key = "key" + std::to_string(i * 10 + j);
            std::string value = "value" + std::to_string(i * 10 + j);
            data.push_back({key, value});
        }

        create_real_sstable(temp_sst, data);
        auto sstable = std::make_shared<SSTableReader>(temp_sst);

        bool result = manager.add_sstable_level0(sstable);
        if (!result) {
            std::cerr << "  Failed to add SSTable " << i << std::endl;
            return false;
        }

        // After adding 2 SSTables, we should have compaction tasks
        if (i == 1) {
            auto task = manager.get_compaction_task();
            if (!task.has_value()) {
                std::cerr << "  Expected compaction task after 2 SSTables" << std::endl;
                return false;
            }
        }
    }

    // Check that we have SSTables in different levels
    auto stats = manager.get_stats();
    std::cout << "  Multi-level stats: " << stats.total_sstables << " total SSTables" << std::endl;

    // Should have some SSTables (exact number depends on compaction)
    if (stats.total_sstables == 0) {
        std::cerr << "  Should have SSTables after adds" << std::endl;
        return false;
    }

    return true;
}

// Test 11: SSTable replacement (simulating compaction result)
bool test_sstable_replacement(const std::string& test_dir) {
    std::string data_dir = make_test_path(test_dir, "replacement_test");
    auto buffer_pool = std::make_shared<BufferPool>(100);
    auto config = create_test_config();

    LevelManager manager(data_dir, buffer_pool, config);

    // Create "old" SSTables that would be compacted
    std::vector<std::shared_ptr<SSTableReader>> old_sstables;
    for (int i = 0; i < 2; i++) {
        std::string old_sst = data_dir + "/level_0/old_" + std::to_string(i) + ".sst";
        create_real_sstable(old_sst, {
            {"key" + std::to_string(i*2), "old" + std::to_string(i*2)},
            {"key" + std::to_string(i*2+1), "old" + std::to_string(i*2+1)}
        });
        old_sstables.push_back(std::make_shared<SSTableReader>(old_sst));
    }

    // Create "new" SSTables that would result from compaction
    std::vector<std::shared_ptr<SSTableReader>> new_sstables;
    std::string new_sst = data_dir + "/level_1/new_merged.sst";
    create_real_sstable(new_sst, {
        {"key0", "new0"},  // Updated value
        {"key1", "new1"},
        {"key2", "new2"},
        {"key3", "new3"}
    });
    new_sstables.push_back(std::make_shared<SSTableReader>(new_sst));

    // Manually simulate replacement (normally done by compactor)
    // Note: This tests the replace_sstables method directly
    manager.replace_sstables(0, old_sstables, new_sstables);

    // Check that new SSTable is in level 1
    auto stats = manager.get_stats();

    // Level 0 should be empty (all moved)
    if (stats.sstables_per_level[0] != 0) {
        std::cerr << "  Level 0 should be empty after replacement" << std::endl;
        return false;
    }

    // Level 1 should have the new SSTable
    if (stats.sstables_per_level[1] < 1) {
        std::cerr << "  Level 1 should have at least 1 SSTable after replacement" << std::endl;
        return false;
    }

    return true;
}

// Test 12: Concurrent access simulation
bool test_concurrent_simulation(const std::string& test_dir) {
    std::string data_dir = make_test_path(test_dir, "concurrent_test");
    auto buffer_pool = std::make_shared<BufferPool>(100);
    auto config = create_test_config();

    LevelManager manager(data_dir, buffer_pool, config);

    // Simulate multiple operations that might happen concurrently
    const int NUM_OPERATIONS = 20;

    for (int i = 0; i < NUM_OPERATIONS; i++) {
        // Create and add SSTable
        std::string temp_sst = make_test_path(test_dir, "conc_" + std::to_string(i) + ".sst");

        std::vector<std::pair<std::string, std::string>> data;
        for (int j = 0; j < 5; j++) {
            std::string key = "op" + std::to_string(i) + "_key" + std::to_string(j);
            std::string value = "value" + std::to_string(i * 100 + j);
            data.push_back({key, value});
        }

        create_real_sstable(temp_sst, data);
        auto sstable = std::make_shared<SSTableReader>(temp_sst);

        bool result = manager.add_sstable_level0(sstable);
        if (!result && i < config.level0_max_sstables) {
            std::cerr << "  Failed to add SSTable " << i << std::endl;
            return false;
        }

        // Interleave different operations
        if (i % 3 == 0) {
            // Check compaction
            auto task = manager.get_compaction_task();
            // Just verify no crash
        }

        if (i % 4 == 0) {
            // Query for keys
            std::string search_key = "op" + std::to_string(i/2) + "_key0";
            auto candidates = manager.find_candidate_sstables(search_key);
            // Just verify no crash
        }

        if (i % 5 == 0) {
            // Get statistics
            auto stats = manager.get_stats();
            if (stats.total_sstables > NUM_OPERATIONS) {
                std::cerr << "  Too many SSTables: " << stats.total_sstables << std::endl;
                return false;
            }
        }
    }

    // Final check - should be in a consistent state
    auto stats = manager.get_stats();
    std::cout << "  Final state: " << stats.total_sstables << " SSTables" << std::endl;

    return true;
}

// Test 13: Error handling with real files
bool test_error_handling(const std::string& test_dir) {
    std::string data_dir = make_test_path(test_dir, "error_test");
    auto buffer_pool = std::make_shared<BufferPool>(100);
    auto config = create_test_config();

    LevelManager manager(data_dir, buffer_pool, config);

    // Test 1: Try to add non-existent SSTable
    auto non_existent_sstable = std::make_shared<SSTableReader>("/nonexistent/path/file.sst");
    bool result = manager.add_sstable_level0(non_existent_sstable);

    if (result) {
        std::cerr << "  Should have failed to add non-existent SSTable" << std::endl;
        return false;
    }

    // Test 2: Create corrupted SST file and try to load
    std::string corrupted_sst = data_dir + "/level_0/corrupted.sst";
    {
        std::ofstream file(corrupted_sst, std::ios::binary);
        file << "This is not a valid SST file format";
    }

    // Reinitialize manager to trigger loading
    LevelManager manager2(data_dir, buffer_pool, config);

    // The corrupted file should be skipped
    auto stats = manager2.get_stats();
    // Should have 0 SSTables (corrupted file should be ignored)
    if (stats.total_sstables != 0) {
        std::cerr << "  Corrupted file should not be loaded" << std::endl;
        return false;
    }

    // Test 3: Invalid operations should not crash
    manager.find_candidate_sstables("");
    manager.find_sstables_for_range("", "");

    // Try to print levels (should not crash)
    // manager.print_levels();

    return true;
}

// Test 14: SSTable metadata consistency
bool test_sstable_metadata(const std::string& test_dir) {
    std::string data_dir = make_test_path(test_dir, "metadata_test");
    auto buffer_pool = std::make_shared<BufferPool>(100);
    auto config = create_test_config();

    LevelManager manager(data_dir, buffer_pool, config);

    // Create SSTables with specific key ranges
    std::vector<std::pair<std::string, std::string>> wide_range_data;
    for (char c = 'a'; c <= 'z'; c++) {
        std::string key(1, c);
        wide_range_data.push_back({key, "value_" + key});
    }

    std::string wide_sst = make_test_path(test_dir, "wide_range.sst");
    create_real_sstable(wide_sst, wide_range_data);
    auto wide_sstable = std::make_shared<SSTableReader>(wide_sst);
    manager.add_sstable_level0(wide_sstable);

    // Create SSTable with narrow range
    std::string narrow_sst = make_test_path(test_dir, "narrow_range.sst");
    create_real_sstable(narrow_sst, {
        {"mango", "tropical"},
        {"melon", "juicy"},
        {"nectarine", "stone fruit"}
    });
    auto narrow_sstable = std::make_shared<SSTableReader>(narrow_sst);
    manager.add_sstable_level0(narrow_sstable);

    // Verify that find_candidate_sstables respects key ranges
    auto candidates = manager.find_candidate_sstables("apple");
    if (candidates.empty()) {
        std::cerr << "  Should find candidate for 'apple' in wide range SSTable" << std::endl;
        return false;
    }

    candidates = manager.find_candidate_sstables("nectarine");
    if (candidates.empty()) {
        std::cerr << "  Should find candidate for 'nectarine'" << std::endl;
        return false;
    }

    // Keys outside ranges should not be found
    candidates = manager.find_candidate_sstables("0");  // Before 'a'
    if (!candidates.empty()) {
        std::cerr << "  Should not find candidate for '0'" << std::endl;
        return false;
    }

    candidates = manager.find_candidate_sstables("~");  // After 'z'
    if (!candidates.empty()) {
        std::cerr << "  Should not find candidate for '~'" << std::endl;
        return false;
    }

    return true;
}

// Test 15: Integration test with actual compaction simulation
bool test_integration_compaction(const std::string& test_dir) {
    std::string data_dir = make_test_path(test_dir, "integration_test");
    auto buffer_pool = std::make_shared<BufferPool>(100);

    // Use small sizes for testing
    LevelManager::Config config;
    config.max_levels = 4;
    config.level0_max_sstables = 2;
    config.size_ratio = 2;

    LevelManager manager(data_dir, buffer_pool, config);

    // Phase 1: Fill level 0
    std::cout << "  Phase 1: Filling level 0..." << std::endl;
    for (int i = 0; i < config.level0_max_sstables; i++) {
        std::string temp_sst = make_test_path(test_dir, "phase1_" + std::to_string(i) + ".sst");

        std::vector<std::pair<std::string, std::string>> data;
        for (int j = 0; j < 10; j++) {
            std::string key = "data_" + std::to_string(i * 10 + j);
            std::string value = "content_" + std::to_string(i * 100 + j);
            data.push_back({key, value});
        }

        create_real_sstable(temp_sst, data);
        auto sstable = std::make_shared<SSTableReader>(temp_sst);

        if (!manager.add_sstable_level0(sstable)) {
            std::cerr << "  Failed to add SSTable in phase 1" << std::endl;
            return false;
        }
    }

    auto stats1 = manager.get_stats();
    std::cout << "    Level 0: " << stats1.sstables_per_level[0] << " SSTables" << std::endl;

    // Phase 2: Trigger compaction
    std::cout << "  Phase 2: Triggering compaction..." << std::endl;
    auto task = manager.get_compaction_task();
    if (!task.has_value()) {
        std::cerr << "  No compaction task generated" << std::endl;
        return false;
    }

    std::cout << "    Compaction: level " << task->source_level
              << " -> level " << task->target_level
              << " (" << task->input_sstables.size() << " SSTables)" << std::endl;

    // In a real test, you would call perform_compaction here
    // For unit test, we'll just verify the task is correct
    if (task->input_sstables.size() != config.level0_max_sstables) {
        std::cerr << "  Wrong number of SSTables in compaction task" << std::endl;
        return false;
    }

    // Phase 3: Simulate what happens after compaction
    std::cout << "  Phase 3: Simulating post-compaction..." << std::endl;

    // Create merged SSTable that would result from compaction
    std::string merged_sst = data_dir + "/level_1/merged_compacted.sst";

    // Combine keys from both SSTables (in real compaction they would be merged/sorted)
    std::vector<std::pair<std::string, std::string>> merged_data;
    for (int i = 0; i < 20; i++) {  // Combined 10 + 10 entries
        std::string key = "merged_" + std::to_string(i);
        std::string value = "compacted_" + std::to_string(i);
        merged_data.push_back({key, value});
    }

    create_real_sstable(merged_sst, merged_data);
    auto merged_sstable = std::make_shared<SSTableReader>(merged_sst);

    // Simulate replacement
    manager.replace_sstables(0, task->input_sstables, {merged_sstable});

    // Verify final state
    auto final_stats = manager.get_stats();
    std::cout << "    Final: " << final_stats.total_sstables << " total SSTables" << std::endl;

    // Level 0 should be empty after compaction
    if (final_stats.sstables_per_level[0] != 0) {
        std::cerr << "  Level 0 should be empty after compaction simulation" << std::endl;
        return false;
    }

    // Level 1 should have the merged SSTable
    if (final_stats.sstables_per_level[1] < 1) {
        std::cerr << "  Level 1 should have the merged SSTable" << std::endl;
        return false;
    }

    return true;
}

// Main test runner
int level_manager_tests_main() {
    // Create unique test directory
    auto now = system_clock::now();
    auto timestamp = duration_cast<milliseconds>(now.time_since_epoch()).count();
    std::string test_dir = "level_manager_tests_" + std::to_string(timestamp);

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

    std::cout << "\nRunning Level Manager Tests" << std::endl;
    std::cout << "===========================" << std::endl;
    std::cout << "Test directory: " << test_dir << std::endl;
    std::cout << std::endl;

    // Store all test functions
    std::vector<std::pair<std::string, std::function<bool(const std::string&)>>> tests = {
        {"1. Basic Initialization", test_level_manager_init},
        {"2. Load Existing SSTables", test_load_existing_sstables},
        {"3. Add SSTable to Level 0", test_add_sstable_level0},
        {"4. Compaction Task Generation", test_compaction_task_level0},
        {"5. Find Candidate SSTables", test_find_candidate_sstables},
        {"6. Find SSTables for Range", test_find_sstables_for_range},
        {"7. Level Capacity", test_level_capacity},
        {"8. Statistics Tracking", test_statistics_tracking},
        {"9. Multi-level Operations", test_multi_level_operations},
        {"10. SSTable Replacement", test_sstable_replacement},
        {"11. Concurrent Simulation", test_concurrent_simulation},
        {"12. Error Handling", test_error_handling},
        {"13. SSTable Metadata", test_sstable_metadata},
        {"14. Integration Compaction", test_integration_compaction}
    };

    int passed = 0;
    int total = static_cast<int>(tests.size());

    for (const auto& [name, test_func] : tests) {
        try {
            bool result = test_func(test_dir);
            print_test_result(name, result);
            if (result) passed++;
        } catch (const std::exception& e) {
            std::cout << name << " (Exception: " << e.what() << ")" << std::endl;
        } catch (...) {
            std::cout << name << " (Unknown exception)" << std::endl;
        }
    }

    std::cout << "\n" << std::string(50, '=') << std::endl;
    std::cout << "Results: " << passed << "/" << total << " tests passed" << std::endl;

    // Optional: Clean up test directory
    // Comment this out if you want to inspect the test files
    try {
        fs::remove_all(test_dir);
        std::cout << "Cleaned up test directory: " << test_dir << std::endl;
    } catch (const fs::filesystem_error&) {
        std::cout << "\nNote: Could not clean up test directory: " << test_dir << std::endl;
        std::cout << "You may need to manually delete it." << std::endl;
    }

    if (passed == total) {
        std::cout << "\nAll Level Manager tests passed!" << std::endl;
        return 0;
    } else {
        std::cout << "\nSome Level Manager tests failed" << std::endl;
        return 1;
    }
}