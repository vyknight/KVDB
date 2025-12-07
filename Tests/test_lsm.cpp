//
// Created by K on 2025-12-06.
//

#include "test_lsm.h"
#include "../LSMTree.h"
#include "../SSTableWriter.h"
#include "../SSTableReader.h"
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
#include <unordered_set>

#include "test_helper.h"

namespace fs = std::filesystem;
using namespace std::chrono;

// Helper function to generate random strings
std::string random_string(size_t length) {
    static const char alphanum[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";
    static std::mt19937 rng(std::random_device{}());
    static std::uniform_int_distribution<> dist(0, sizeof(alphanum) - 2);

    std::string result;
    result.reserve(length);
    for (size_t i = 0; i < length; ++i) {
        result += alphanum[dist(rng)];
    }
    return result;
}

// Helper to verify LSMTree contains expected data
bool verify_lsm_content(LSMTree& lsm,
                       const std::vector<std::pair<std::string, std::string>>& expected_data) {
    for (const auto& [key, expected_value] : expected_data) {
        auto value = lsm.get(key);
        if (!value.has_value()) {
            std::cerr << "  Key '" << key << "' not found" << std::endl;
            return false;
        }
        if (value.value() != expected_value) {
            std::cerr << "  Key '" << key << "' value mismatch. Expected: '"
                      << expected_value << "', Got: '" << value.value() << "'" << std::endl;
            return false;
        }
    }
    return true;
}

// Test 1: Basic put and get operations
bool test_basic_put_get(const std::string& test_dir) {
    std::string data_dir = make_test_path(test_dir, "basic_test");
    LSMTree lsm(data_dir, 4096, 100, 10);  // 4KB memtable, 100-page buffer pool

    // Test basic puts
    if (!lsm.put("key1", "value1")) {
        std::cerr << "  Failed to put key1" << std::endl;
        return false;
    }
    if (!lsm.put("key2", "value2")) {
        std::cerr << "  Failed to put key2" << std::endl;
        return false;
    }
    if (!lsm.put("key3", "value3")) {
        std::cerr << "  Failed to put key3" << std::endl;
        return false;
    }

    // Test gets
    auto val1 = lsm.get("key1");
    if (!val1.has_value() || val1.value() != "value1") {
        std::cerr << "  Get key1 failed" << std::endl;
        return false;
    }

    auto val2 = lsm.get("key2");
    if (!val2.has_value() || val2.value() != "value2") {
        std::cerr << "  Get key2 failed" << std::endl;
        return false;
    }

    auto val3 = lsm.get("key3");
    if (!val3.has_value() || val3.value() != "value3") {
        std::cerr << "  Get key3 failed" << std::endl;
        return false;
    }

    // Test non-existent key
    auto val4 = lsm.get("nonexistent");
    if (val4.has_value()) {
        std::cerr << "  Non-existent key should return nullopt" << std::endl;
        return false;
    }

    return true;
}

// Test 2: Update operations
bool test_update_operations(const std::string& test_dir) {
    std::string data_dir = make_test_path(test_dir, "update_test");
    LSMTree lsm(data_dir, 4096, 100, 10);

    // Put initial value
    if (!lsm.put("user1", "alice")) {
        return false;
    }

    // Update the value
    if (!lsm.put("user1", "bob")) {
        return false;
    }

    // Should get the updated value
    auto val = lsm.get("user1");
    if (!val.has_value() || val.value() != "bob") {
        std::cerr << "  Update failed, expected 'bob', got '"
                  << (val.has_value() ? val.value() : "null") << "'" << std::endl;
        return false;
    }

    // Multiple updates
    for (int i = 0; i < 5; i++) {
        std::string new_value = "value_" + std::to_string(i);
        if (!lsm.put("counter", new_value)) {
            std::cerr << "  Failed update " << i << std::endl;
            return false;
        }
    }

    val = lsm.get("counter");
    if (!val.has_value() || val.value() != "value_4") {
        std::cerr << "  Multiple updates failed" << std::endl;
        return false;
    }

    return true;
}

// Test 3: Delete operations
bool test_delete_operations(const std::string& test_dir) {
    std::string data_dir = make_test_path(test_dir, "delete_test");
    LSMTree lsm(data_dir, 4096, 100, 10);

    // Insert some data
    if (!lsm.put("key1", "value1") || !lsm.put("key2", "value2")) {
        return false;
    }

    // Verify data exists
    if (!lsm.get("key1").has_value() || !lsm.get("key2").has_value()) {
        std::cerr << "  Data not inserted properly" << std::endl;
        return false;
    }

    // Delete key1
    if (!lsm.remove("key1")) {
        std::cerr << "  Failed to delete key1" << std::endl;
        return false;
    }

    // key1 should not exist
    if (lsm.get("key1").has_value()) {
        std::cerr << "  key1 should be deleted" << std::endl;
        return false;
    }

    // key2 should still exist
    auto val2 = lsm.get("key2");
    if (!val2.has_value() || val2.value() != "value2") {
        std::cerr << "  key2 should still exist" << std::endl;
        return false;
    }

    // Delete non-existent key (should succeed - adds tombstone)
    if (!lsm.remove("nonexistent")) {
        std::cerr << "  Deleting non-existent key should succeed" << std::endl;
        return false;
    }

    // Delete and then re-insert
    if (!lsm.put("temp", "original")) return false;
    if (!lsm.remove("temp")) return false;
    if (!lsm.put("temp", "new")) return false;

    auto temp_val = lsm.get("temp");
    if (!temp_val.has_value() || temp_val.value() != "new") {
        std::cerr << "  Delete-then-insert failed" << std::endl;
        return false;
    }

    return true;
}

// Test 4: Scan operations
bool test_scan_operations(const std::string& test_dir) {
    std::string data_dir = make_test_path(test_dir, "scan_test");
    LSMTree lsm(data_dir, 4096, 100, 10);

    // Insert data in random order
    std::vector<std::pair<std::string, std::string>> test_data = {
        {"banana", "yellow"},
        {"apple", "red"},
        {"date", "brown"},
        {"cherry", "red"},
        {"elderberry", "purple"},
        {"fig", "purple"}
    };

    // Shuffle to test ordering
    std::mt19937 rng(std::random_device{}());
    std::shuffle(test_data.begin(), test_data.end(), rng);

    for (const auto& [key, value] : test_data) {
        if (!lsm.put(key, value)) {
            std::cerr << "  Failed to put " << key << std::endl;
            return false;
        }
    }

    // Test full scan
    auto results = lsm.scan("a", "z");

    // Results should be sorted by key
    if (results.size() != test_data.size()) {
        std::cerr << "  Scan returned wrong number of results: "
                  << results.size() << " vs " << test_data.size() << std::endl;
        return false;
    }

    // Check that results are sorted
    for (size_t i = 1; i < results.size(); i++) {
        if (results[i-1].first >= results[i].first) {
            std::cerr << "  Results not sorted: " << results[i-1].first
                      << " >= " << results[i].first << std::endl;
            return false;
        }
    }

    // Test partial scan
    results = lsm.scan("cherry", "elderberry");
    if (results.size() != 3) {  // cherry, date, elderberry
        std::cerr << "  Partial scan wrong size: " << results.size() << std::endl;
        return false;
    }

    // Test empty scan range
    results = lsm.scan("z", "zz");
    if (!results.empty()) {
        std::cerr << "  Empty range should return empty results" << std::endl;
        return false;
    }

    // Test single key scan
    results = lsm.scan("cherry", "cherry");
    if (results.size() != 1 || results[0].first != "cherry") {
        std::cerr << "  Single key scan failed" << std::endl;
        return false;
    }

    return true;
}

// Test 5: Memtable flush and persistence
bool test_memtable_flush(const std::string& test_dir) {
    std::string data_dir = make_test_path(test_dir, "flush_test");

    // Very small memtable to force flush
    LSMTree lsm(data_dir, 100, 100, 10);  // 100-byte memtable

    // Insert enough data to force flush
    std::vector<std::pair<std::string, std::string>> test_data;
    for (int i = 0; i < 10; i++) {
        std::string key = "key_" + std::to_string(i);
        std::string value(50, 'x');  // 50-byte values
        test_data.push_back({key, value});
        if (!lsm.put(key, value)) {
            std::cerr << "  Failed to put " << key << std::endl;
            return false;
        }
    }

    // Check that memtable was flushed (should have SSTables)
    size_t sstable_count = lsm.get_sstable_count();
    if (sstable_count == 0) {
        std::cerr << "  Memtable should have been flushed to SSTable" << std::endl;
        return false;
    }

    std::cout << "  Created " << sstable_count << " SSTables after flush" << std::endl;

    // Data should still be accessible after flush
    return verify_lsm_content(lsm, test_data);
}

// Test 6: Recovery from WAL
bool test_wal_recovery(const std::string& test_dir) {
    std::string data_dir = make_test_path(test_dir, "recovery_test");

    // Phase 1: Create DB and insert data
    {
        LSMTree lsm(data_dir, 4096, 100, 10);

        std::vector<std::pair<std::string, std::string>> test_data = {
            {"user1", "alice"},
            {"user2", "bob"},
            {"user3", "charlie"}
        };

        for (const auto& [key, value] : test_data) {
            if (!lsm.put(key, value)) {
                std::cerr << "  Phase 1: Failed to put " << key << std::endl;
                return false;
            }
        }

        // Delete one key
        if (!lsm.remove("user2")) {
            std::cerr << "  Phase 1: Failed to delete user2" << std::endl;
            return false;
        }

        // Verify before closing
        if (!verify_lsm_content(lsm, {{"user1", "alice"}, {"user3", "charlie"}})) {
            return false;
        }
    }  // LSMTree destroyed here - WAL should persist

    // Phase 2: Reopen DB (should recover from WAL)
    {
        LSMTree lsm(data_dir, 4096, 100, 10);

        // Verify recovered data
        if (!verify_lsm_content(lsm, {{"user1", "alice"}, {"user3", "charlie"}})) {
            std::cerr << "  Phase 2: Recovery verification failed" << std::endl;
            return false;
        }

        // user2 should be deleted
        if (lsm.get("user2").has_value()) {
            std::cerr << "  Phase 2: user2 should still be deleted" << std::endl;
            return false;
        }

        std::cout << "  Successfully recovered from WAL" << std::endl;
    }

    return true;
}

// Test 7: Compaction simulation
bool test_compaction_simulation(const std::string& test_dir) {
    std::string data_dir = make_test_path(test_dir, "compaction_test");

    // Small memtable and small level0 to trigger compaction quickly
    // Note: Actual compaction depends on LevelManager configuration
    LSMTree lsm(data_dir, 200, 100, 10);

    std::vector<std::pair<std::string, std::string>> all_data;

    // Insert multiple batches of data to trigger flushes
    for (int batch = 0; batch < 3; batch++) {
        std::vector<std::pair<std::string, std::string>> batch_data;

        for (int i = 0; i < 5; i++) {
            std::string key = "batch" + std::to_string(batch) + "_key" + std::to_string(i);
            std::string value = "value" + std::to_string(batch * 10 + i);
            batch_data.push_back({key, value});

            if (!lsm.put(key, value)) {
                std::cerr << "  Failed to put " << key << std::endl;
                return false;
            }
        }

        // Add to all_data for verification
        all_data.insert(all_data.end(), batch_data.begin(), batch_data.end());

        // Print progress
        std::cout << "  Batch " << batch << ": inserted " << batch_data.size()
                  << " entries, total SSTables: " << lsm.get_sstable_count() << std::endl;
    }

    // Force flush any remaining data in memtable
    if (lsm.get_memtable_size() > 0) {
        if (!lsm.flush_memtable()) {
            std::cerr << "  Failed to flush memtable" << std::endl;
            return false;
        }
    }

    // Verify all data is accessible
    if (!verify_lsm_content(lsm, all_data)) {
        std::cerr << "  Data verification failed after compaction simulation" << std::endl;
        return false;
    }

    // Print final state
    std::cout << "  Final: " << lsm.get_sstable_count() << " SSTables total" << std::endl;
    // lsm.print_levels();

    return true;
}

// Test 8: Large dataset handling
bool test_large_dataset(const std::string& test_dir) {
    std::string data_dir = make_test_path(test_dir, "large_dataset_test");

    // Larger memtable for this test
    LSMTree lsm(data_dir, 65536, 1000, 10);  // 64KB memtable, 1000-page buffer

    const int NUM_ENTRIES = 100;
    std::vector<std::pair<std::string, std::string>> test_data;

    auto start = high_resolution_clock::now();

    // Insert many entries
    for (int i = 0; i < NUM_ENTRIES; i++) {
        std::string key = "entry_" + std::to_string(i);
        std::string value = "This is a longer value for entry number " + std::to_string(i)
                          + " with some extra text to make it larger.";

        test_data.push_back({key, value});

        if (!lsm.put(key, value)) {
            std::cerr << "  Failed to put entry " << i << std::endl;
            return false;
        }

        // Occasionally verify some entries
        if (i % 20 == 0) {
            auto val = lsm.get(key);
            if (!val.has_value() || val.value() != value) {
                std::cerr << "  Verification failed for entry " << i << std::endl;
                return false;
            }
        }
    }

    auto insert_end = high_resolution_clock::now();
    auto insert_time = duration_cast<milliseconds>(insert_end - start);

    // Verify all entries
    bool all_correct = true;
    for (int i = 0; i < NUM_ENTRIES; i++) {
        const auto& [key, expected_value] = test_data[i];
        auto val = lsm.get(key);

        if (!val.has_value() || val.value() != expected_value) {
            std::cerr << "  Final verification failed for " << key << std::endl;
            all_correct = false;
            break;
        }
    }

    auto verify_end = high_resolution_clock::now();
    auto verify_time = duration_cast<milliseconds>(verify_end - insert_end);

    std::cout << "  Performance: " << NUM_ENTRIES << " entries inserted in "
              << insert_time.count() << "ms ("
              << (NUM_ENTRIES * 1000.0 / insert_time.count()) << " ops/sec)" << std::endl;
    std::cout << "  Verification: " << NUM_ENTRIES << " entries verified in "
              << verify_time.count() << "ms" << std::endl;

    return all_correct;
}

// Test 9: Concurrent operations simulation
bool test_concurrent_simulation_lsm(const std::string& test_dir) {
    std::string data_dir = make_test_path(test_dir, "concurrent_test");
    LSMTree lsm(data_dir, 4096, 100, 10);

    const int NUM_OPERATIONS = 50;

    // Simulate concurrent-like operations
    for (int i = 0; i < NUM_OPERATIONS; i++) {
        // Mix of operations
        if (i % 4 == 0) {
            // Put operation
            std::string key = "key_" + std::to_string(i / 4);
            std::string value = "value_" + std::to_string(i);
            if (!lsm.put(key, value)) {
                std::cerr << "  Concurrent put failed at operation " << i << std::endl;
                return false;
            }
        } else if (i % 4 == 1) {
            // Get operation
            std::string key = "key_" + std::to_string((i-1) / 4);
            lsm.get(key);  // Don't check result, just test no crash
        } else if (i % 4 == 2) {
            // Delete operation (every other key)
            if (i % 8 == 2) {
                std::string key = "key_" + std::to_string((i-2) / 8);
                lsm.remove(key);
            }
        } else {
            // Scan operation
            lsm.scan("key_0", "key_9");
        }

        // Occasionally flush
        if (i % 10 == 0 && lsm.get_memtable_size() > 0) {
            lsm.flush_memtable();
        }
    }

    // Should not crash and be in consistent state
    auto stats = lsm.get_stats();
    std::cout << "  Concurrent simulation completed: "
              << stats.total_puts << " puts, "
              << stats.total_gets << " gets, "
              << stats.total_deletes << " deletes" << std::endl;

    return true;
}

// Test 10: Statistics collection
bool test_statistics_collection(const std::string& test_dir) {
    std::string data_dir = make_test_path(test_dir, "stats_test");
    LSMTree lsm(data_dir, 4096, 100, 10);

    // Get initial stats
    auto initial_stats = lsm.get_stats();

    // Perform operations
    lsm.put("key1", "value1");
    lsm.put("key2", "value2");
    lsm.get("key1");
    lsm.get("key2");
    lsm.get("nonexistent");
    lsm.remove("key1");
    lsm.scan("a", "z");

    // Force flush to trigger memtable flush stat
    lsm.flush_memtable();

    // Get final stats
    auto final_stats = lsm.get_stats();

    // Verify stats were updated
    if (final_stats.total_puts < 2) {
        std::cerr << "  Stats: puts not counted properly" << std::endl;
        return false;
    }

    if (final_stats.total_gets < 3) {
        std::cerr << "  Stats: gets not counted properly" << std::endl;
        return false;
    }

    if (final_stats.total_deletes < 1) {
        std::cerr << "  Stats: deletes not counted properly" << std::endl;
        return false;
    }

    if (final_stats.memtable_flushes == 0) {
        std::cerr << "  Stats: flushes not counted" << std::endl;
        return false;
    }

    std::cout << "  Statistics: "
              << final_stats.total_puts << " puts, "
              << final_stats.total_gets << " gets, "
              << final_stats.total_deletes << " deletes, "
              << final_stats.memtable_flushes << " flushes" << std::endl;

    return true;
}

// Test 11: Edge cases and error handling
bool test_edge_cases(const std::string& test_dir) {
    std::string data_dir = make_test_path(test_dir, "edge_cases_test");
    LSMTree lsm(data_dir, 4096, 100, 10);

    // Test empty strings
    if (!lsm.put("", "")) {
        std::cerr << "  Failed to put empty key/value" << std::endl;
        return false;
    }

    auto empty_val = lsm.get("");
    if (!empty_val.has_value() || !empty_val.value().empty()) {
        std::cerr << "  Empty key/value retrieval failed" << std::endl;
        return false;
    }

    // Test special characters
    std::string special_key = "key\nwith\tnewlines\rand\ttabs";
    std::string special_value = "value\nwith\tspecial\rcharacters";
    if (!lsm.put(special_key, special_value)) {
        std::cerr << "  Failed to put special characters" << std::endl;
        return false;
    }

    auto special_val = lsm.get(special_key);
    if (!special_val.has_value() || special_val.value() != special_value) {
        std::cerr << "  Special characters retrieval failed" << std::endl;
        return false;
    }

    // Test very long keys/values
    std::string long_key(1000, 'x');
    std::string long_value(10000, 'y');
    if (!lsm.put(long_key, long_value)) {
        std::cerr << "  Failed to put long key/value" << std::endl;
        return false;
    }

    auto long_val = lsm.get(long_key);
    if (!long_val.has_value() || long_val.value() != long_value) {
        std::cerr << "  Long key/value retrieval failed" << std::endl;
        return false;
    }

    // Test duplicate puts
    for (int i = 0; i < 10; i++) {
        if (!lsm.put("duplicate", "value_" + std::to_string(i))) {
            std::cerr << "  Duplicate put " << i << " failed" << std::endl;
            return false;
        }
    }

    auto dup_val = lsm.get("duplicate");
    if (!dup_val.has_value() || dup_val.value() != "value_9") {
        std::cerr << "  Last duplicate value not preserved" << std::endl;
        return false;
    }

    // Test scan with various ranges
    lsm.scan("", "");  // Empty range
    lsm.scan("a", "a");  // Single character range
    lsm.scan("\x00", "\xFF");  // Full byte range

    return true;
}

// Test 12: Tombstone propagation and cleanup
bool test_tombstone_cleanup(const std::string& test_dir) {
    std::string data_dir = make_test_path(test_dir, "tombstone_test");

    // Very small memtable to force frequent flushes
    LSMTree lsm(data_dir, 100, 100, 10);

    // Insert and delete pattern
    std::vector<std::string> keys;
    for (int i = 0; i < 10; i++) {
        std::string key = "key" + std::to_string(i);
        keys.push_back(key);

        // Insert
        if (!lsm.put(key, "value_" + std::to_string(i))) {
            std::cerr << "  Failed to insert " << key << std::endl;
            return false;
        }

        // Delete every other key
        if (i % 2 == 0) {
            if (!lsm.remove(key)) {
                std::cerr << "  Failed to delete " << key << std::endl;
                return false;
            }
        }
    }

    // Force flush
    lsm.flush_memtable();

    // Verify: deleted keys should not exist
    for (size_t i = 0; i < keys.size(); i++) {
        auto val = lsm.get(keys[i]);
        if (i % 2 == 0) {
            // Deleted keys
            if (val.has_value()) {
                std::cerr << "  Tombstoned key " << keys[i] << " should not exist" << std::endl;
                return false;
            }
        } else {
            // Non-deleted keys
            if (!val.has_value()) {
                std::cerr << "  Non-deleted key " << keys[i] << " should exist" << std::endl;
                return false;
            }
        }
    }

    // Re-insert some deleted keys
    for (int i = 0; i < 10; i += 3) {
        std::string key = "key" + std::to_string(i);
        std::string new_value = "reinserted_" + std::to_string(i);

        if (!lsm.put(key, new_value)) {
            std::cerr << "  Failed to re-insert " << key << std::endl;
            return false;
        }

        // Verify new value
        auto val = lsm.get(key);
        if (!val.has_value() || val.value() != new_value) {
            std::cerr << "  Re-inserted key " << key << " has wrong value" << std::endl;
            return false;
        }
    }

    std::cout << "  Tombstone test completed successfully" << std::endl;
    return true;
}

// Test 13: Multiple LSMTree instances
bool test_multiple_instances(const std::string& test_dir) {
    // Test that multiple instances don't interfere

    std::string db1_dir = make_test_path(test_dir, "db1");
    std::string db2_dir = make_test_path(test_dir, "db2");

    // Create two independent databases
    LSMTree db1(db1_dir, 4096, 100, 10);
    LSMTree db2(db2_dir, 4096, 100, 10);

    // Insert different data into each
    if (!db1.put("db1_key", "db1_value")) return false;
    if (!db2.put("db2_key", "db2_value")) return false;

    // Each should only see its own data
    auto db1_val = db1.get("db1_key");
    if (!db1_val.has_value() || db1_val.value() != "db1_value") {
        std::cerr << "  DB1 can't see its own data" << std::endl;
        return false;
    }

    auto db2_val = db2.get("db2_key");
    if (!db2_val.has_value() || db2_val.value() != "db2_value") {
        std::cerr << "  DB2 can't see its own data" << std::endl;
        return false;
    }

    // Each should NOT see the other's data
    if (db1.get("db2_key").has_value()) {
        std::cerr << "  DB1 can see DB2's data" << std::endl;
        return false;
    }

    if (db2.get("db1_key").has_value()) {
        std::cerr << "  DB2 can see DB1's data" << std::endl;
        return false;
    }

    // Force flushes
    db1.flush_memtable();
    db2.flush_memtable();

    // Still shouldn't interfere
    if (db1.get("db2_key").has_value() || db2.get("db1_key").has_value()) {
        std::cerr << "  Databases interfere after flush" << std::endl;
        return false;
    }

    return true;
}

// Test 14: Performance under load
bool test_performance_load(const std::string& test_dir) {
    std::string data_dir = make_test_path(test_dir, "perf_load_test");

    // Medium-sized configuration
    LSMTree lsm(data_dir, 32768, 500, 10);  // 32KB memtable

    const int NUM_BATCHES = 5;
    const int BATCH_SIZE = 20;

    auto total_start = high_resolution_clock::now();

    for (int batch = 0; batch < NUM_BATCHES; batch++) {
        auto batch_start = high_resolution_clock::now();

        for (int i = 0; i < BATCH_SIZE; i++) {
            int id = batch * BATCH_SIZE + i;
            std::string key = "user_" + std::to_string(id) + "_profile";
            std::string value = "{\"id\":" + std::to_string(id) +
                               ",\"name\":\"User" + std::to_string(id) +
                               "\",\"data\":\"" + random_string(100) + "\"}";

            if (!lsm.put(key, value)) {
                std::cerr << "  Batch " << batch << ", entry " << i << " failed" << std::endl;
                return false;
            }
        }

        auto batch_end = high_resolution_clock::now();
        auto batch_time = duration_cast<milliseconds>(batch_end - batch_start);

        std::cout << "  Batch " << batch << ": " << BATCH_SIZE << " entries in "
                  << batch_time.count() << "ms ("
                  << (BATCH_SIZE * 1000.0 / batch_time.count()) << " ops/sec)" << std::endl;

        // Verify some random entries from this batch
        for (int i = 0; i < 3; i++) {
            int random_id = batch * BATCH_SIZE + (std::rand() % BATCH_SIZE);
            std::string key = "user_" + std::to_string(random_id) + "_profile";

            auto val = lsm.get(key);
            if (!val.has_value()) {
                std::cerr << "  Verification failed for " << key << std::endl;
                return false;
            }
        }
    }

    auto total_end = high_resolution_clock::now();
    auto total_time = duration_cast<milliseconds>(total_end - total_start);

    int total_entries = NUM_BATCHES * BATCH_SIZE;
    std::cout << "  Total: " << total_entries << " entries in "
              << total_time.count() << "ms ("
              << (total_entries * 1000.0 / total_time.count()) << " ops/sec)" << std::endl;

    // Final stats
    auto stats = lsm.get_stats();
    std::cout << "  Final stats: " << stats.sstables_created << " SSTables created, "
              << stats.memtable_flushes << " memtable flushes" << std::endl;

    return true;
}

// Test 15: Integration test - complete workflow
bool test_integration_workflow(const std::string& test_dir) {
    std::string data_dir = make_test_path(test_dir, "integration_test");

    std::cout << "\n  Starting integration test..." << std::endl;

    // Phase 1: Initial database creation
    std::cout << "  Phase 1: Creating database and inserting initial data..." << std::endl;
    {
        LSMTree lsm(data_dir, 2048, 100, 10);

        // Insert initial dataset
        std::vector<std::pair<std::string, std::string>> phase1_data;
        for (int i = 0; i < 10; i++) {
            std::string key = "customer_" + std::to_string(i);
            std::string value = "Initial data for customer " + std::to_string(i);
            phase1_data.push_back({key, value});

            if (!lsm.put(key, value)) {
                std::cerr << "  Phase 1: Failed to insert " << key << std::endl;
                return false;
            }
        }

        // Verify phase 1 data
        if (!verify_lsm_content(lsm, phase1_data)) {
            std::cerr << "  Phase 1: Verification failed" << std::endl;
            return false;
        }

        // Force flush
        lsm.flush_memtable();
        std::cout << "    Phase 1 complete: " << lsm.get_sstable_count() << " SSTables" << std::endl;
    }  // Database closes

    // Phase 2: Reopen and update
    std::cout << "  Phase 2: Reopening and updating data..." << std::endl;
    {
        LSMTree lsm(data_dir, 2048, 100, 10);

        // Update some customers
        for (int i = 0; i < 5; i++) {
            std::string key = "customer_" + std::to_string(i);
            std::string value = "Updated data for customer " + std::to_string(i);

            if (!lsm.put(key, value)) {
                std::cerr << "  Phase 2: Failed to update " << key << std::endl;
                return false;
            }
        }

        // Delete some customers
        for (int i = 5; i < 7; i++) {
            std::string key = "customer_" + std::to_string(i);
            if (!lsm.remove(key)) {
                std::cerr << "  Phase 2: Failed to delete " << key << std::endl;
                return false;
            }
        }

        // Add new customers
        for (int i = 10; i < 15; i++) {
            std::string key = "customer_" + std::to_string(i);
            std::string value = "New customer " + std::to_string(i);

            if (!lsm.put(key, value)) {
                std::cerr << "  Phase 2: Failed to add " << key << std::endl;
                return false;
            }
        }

        // Force flush
        lsm.flush_memtable();
        std::cout << "    Phase 2 complete: " << lsm.get_sstable_count() << " SSTables" << std::endl;

        // Print level structure
        // lsm.print_levels();
    }

    // Phase 3: Final verification
    std::cout << "  Phase 3: Final verification..." << std::endl;
    {
        LSMTree lsm(data_dir, 2048, 100, 10);

        // Verify final state
        // Customers 0-4: updated
        for (int i = 0; i < 5; i++) {
            std::string key = "customer_" + std::to_string(i);
            std::string expected = "Updated data for customer " + std::to_string(i);
            auto val = lsm.get(key);

            if (!val.has_value() || val.value() != expected) {
                std::cerr << "  Phase 3: Customer " << i << " verification failed" << std::endl;
                return false;
            }
        }

        // Customers 5-6: deleted
        for (int i = 5; i < 7; i++) {
            std::string key = "customer_" + std::to_string(i);
            if (lsm.get(key).has_value()) {
                std::cerr << "  Phase 3: Customer " << i << " should be deleted" << std::endl;
                return false;
            }
        }

        // Customers 7-9: original
        for (int i = 7; i < 10; i++) {
            std::string key = "customer_" + std::to_string(i);
            std::string expected = "Initial data for customer " + std::to_string(i);
            auto val = lsm.get(key);

            if (!val.has_value() || val.value() != expected) {
                std::cerr << "  Phase 3: Customer " << i << " verification failed" << std::endl;
                return false;
            }
        }

        // Customers 10-14: new
        for (int i = 10; i < 15; i++) {
            std::string key = "customer_" + std::to_string(i);
            std::string expected = "New customer " + std::to_string(i);
            auto val = lsm.get(key);

            if (!val.has_value() || val.value() != expected) {
                std::cerr << "  Phase 3: Customer " << i << " verification failed" << std::endl;
                return false;
            }
        }

        std::cout << "    Phase 3 complete: All verifications passed" << std::endl;

        // Final stats
        auto stats = lsm.get_stats();
        std::cout << "    Final stats:" << std::endl;
        std::cout << "      Puts: " << stats.total_puts << std::endl;
        std::cout << "      Gets: " << stats.total_gets << std::endl;
        std::cout << "      Deletes: " << stats.total_deletes << std::endl;
        std::cout << "      Flushes: " << stats.memtable_flushes << std::endl;
        std::cout << "      SSTables: " << stats.sstables_created << std::endl;
        std::cout << "      Compactions: " << stats.compactions << std::endl;
    }

    std::cout << "  Integration test completed successfully!" << std::endl;
    return true;
}

// Main test runner
int lsm_tests_main() {
    // Create unique test directory
    auto now = system_clock::now();
    auto timestamp = duration_cast<milliseconds>(now.time_since_epoch()).count();
    std::string test_dir = "lsm_tree_tests_" + std::to_string(timestamp);

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

    std::cout << "\nRunning LSM Tree Tests" << std::endl;
    std::cout << "======================" << std::endl;
    std::cout << "Test directory: " << test_dir << std::endl;
    std::cout << std::endl;

    // Store all test functions
    std::vector<std::pair<std::string, std::function<bool(const std::string&)>>> tests = {
        {"1. Basic Put/Get Operations", test_basic_put_get},
        {"2. Update Operations", test_update_operations},
        {"3. Delete Operations", test_delete_operations},
        {"4. Scan Operations", test_scan_operations},
        {"5. Memtable Flush", test_memtable_flush},
        {"6. WAL Recovery", test_wal_recovery},
        {"7. Compaction Simulation", test_compaction_simulation},
        {"8. Large Dataset", test_large_dataset},
        {"9. Concurrent Simulation", test_concurrent_simulation_lsm},
        {"10. Statistics Collection", test_statistics_collection},
        {"11. Edge Cases", test_edge_cases},
        {"12. Tombstone Cleanup", test_tombstone_cleanup},
        {"13. Multiple Instances", test_multiple_instances},
        {"14. Performance Under Load", test_performance_load},
        {"15. Integration Workflow", test_integration_workflow}
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
        std::cout << "\nAll LSM Tree tests passed!" << std::endl;
        return 0;
    } else {
        std::cout << "\nSome LSM Tree tests failed" << std::endl;
        return 1;
    }
}