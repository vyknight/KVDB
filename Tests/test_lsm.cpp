//
// Created by Zekun Liu on 2025-12-07.
//

#include "test_lsm.h"
#include <fstream>
#include <thread>
#include <filesystem>
#include <iostream>

#include "test_helper.h"
#include "../LSMTree.h"

namespace fs = std::filesystem;
using namespace std::chrono;

bool verify_key_value(LSMTree& db, const std::string& key, const std::string& expected_value) {
    auto value = db.get(key);
    if (!value.has_value()) {
        std::cerr << "  Key '" << key << "' not found (expected: '" << expected_value << "')" << std::endl;
        return false;
    }
    if (*value != expected_value) {
        std::cerr << "  Key '" << key << "' has value '" << *value
                  << "' (expected: '" << expected_value << "')" << std::endl;
        return false;
    }
    return true;
}

bool verify_key_missing(LSMTree& db, const std::string& key) {
    auto value = db.get(key);
    if (value.has_value()) {
        std::cerr << "  Key '" << key << "' should not exist but has value '" << *value << "'" << std::endl;
        return false;
    }
    return true;
}

// Test 1: Basic operations
bool test_lsm_basic_operations(const std::string& test_dir) {
    std::string db_path = make_test_path(test_dir, "basic_db");

    // Clean up any existing data
    fs::remove_all(db_path);

    // Create database with 1KB memtable for testing
    LSMTree db(db_path, 1024, 1024 * 1024);

    // Test put and get
    if (!db.put("key1", "value1")) {
        std::cerr << "  Failed to put key1" << std::endl;
        return false;
    }

    if (!verify_key_value(db, "key1", "value1")) {
        return false;
    }

    // Test multiple keys
    if (!db.put("key2", "value2") || !db.put("key3", "value3")) {
        std::cerr << "  Failed to put multiple keys" << std::endl;
        return false;
    }

    if (!verify_key_value(db, "key2", "value2") || !verify_key_value(db, "key3", "value3")) {
        return false;
    }

    // Test non-existent key
    if (!verify_key_missing(db, "nonexistent")) {
        return false;
    }

    // Test stats
    auto stats = db.get_stats();
    if (stats.total_puts != 3) {
        std::cerr << "  Expected 3 puts, got " << stats.total_puts << std::endl;
        return false;
    }

    if (stats.total_gets < 3) { // At least 3 gets from our verify calls
        std::cerr << "  Expected at least 3 gets" << std::endl;
        return false;
    }

    return true;
}

// Test 2: Update operations
bool test_lsm_updates(const std::string& test_dir) {
    std::string db_path = make_test_path(test_dir, "update_db");
    fs::remove_all(db_path);

    LSMTree db(db_path, 1024, 1024 * 1024);

    // Insert initial value
    if (!db.put("user1", "Alice")) {
        std::cerr << "  Failed to insert initial value" << std::endl;
        return false;
    }

    if (!verify_key_value(db, "user1", "Alice")) {
        return false;
    }

    // Update value
    if (!db.put("user1", "Bob")) {
        std::cerr << "  Failed to update value" << std::endl;
        return false;
    }

    if (!verify_key_value(db, "user1", "Bob")) {
        return false;
    }

    // Multiple updates
    if (!db.put("counter", "1") ||
        !db.put("counter", "2") ||
        !db.put("counter", "3") ||
        !db.put("counter", "4") ||
        !db.put("counter", "5")) {
        std::cerr << "  Failed multiple updates" << std::endl;
        return false;
    }

    if (!verify_key_value(db, "counter", "5")) {
        return false;
    }

    // Update after delete (simulated in test 3)
    if (!db.put("temp", "original") ||
        !db.remove("temp") ||
        !db.put("temp", "restored")) {
        std::cerr << "  Failed update after delete" << std::endl;
        return false;
    }

    if (!verify_key_value(db, "temp", "restored")) {
        return false;
    }

    return true;
}

// Test 3: Delete operations
bool test_lsm_deletes(const std::string& test_dir) {
    std::string db_path = make_test_path(test_dir, "delete_db");
    fs::remove_all(db_path);

    LSMTree db(db_path, 1024, 1024 * 1024);

    // Insert some data
    if (!db.put("key1", "value1") ||
        !db.put("key2", "value2") ||
        !db.put("key3", "value3")) {
        std::cerr << "  Failed to insert initial data" << std::endl;
        return false;
    }

    // Delete a key
    if (!db.remove("key2")) {
        std::cerr << "  Failed to delete key2" << std::endl;
        return false;
    }

    // Verify deletion
    if (!verify_key_missing(db, "key2")) {
        return false;
    }

    // Other keys should still exist
    if (!verify_key_value(db, "key1", "value1") ||
        !verify_key_value(db, "key3", "value3")) {
        return false;
    }

    // Delete non-existent key (should work, adds tombstone)
    if (!db.remove("nonexistent")) {
        std::cerr << "  Failed to delete non-existent key" << std::endl;
        return false;
    }

    // Delete, then re-insert
    if (!db.put("dynamic", "first") ||
        !db.remove("dynamic") ||
        !db.put("dynamic", "second")) {
        std::cerr << "  Failed delete-then-insert sequence" << std::endl;
        return false;
    }

    if (!verify_key_value(db, "dynamic", "second")) {
        return false;
    }

    // Multiple deletes
    if (!db.put("multi1", "val1") ||
        !db.put("multi2", "val2") ||
        !db.put("multi3", "val3")) {
        std::cerr << "  Failed to insert multiple keys" << std::endl;
        return false;
    }

    if (!db.remove("multi1") || !db.remove("multi2") || !db.remove("multi3")) {
        std::cerr << "  Failed multiple deletes" << std::endl;
        return false;
    }

    if (!verify_key_missing(db, "multi1") ||
        !verify_key_missing(db, "multi2") ||
        !verify_key_missing(db, "multi3")) {
        return false;
    }

    // Check delete stats
    auto stats = db.get_stats();
    if (stats.total_deletes < 6) { // key2, nonexistent, dynamic, multi1-3
        std::cerr << "  Expected at least 6 deletes, got " << stats.total_deletes << std::endl;
        return false;
    }

    return true;
}

// Test 4: Scan operations
bool test_lsm_scan_operations(const std::string& test_dir) {
    std::string db_path = make_test_path(test_dir, "scan_db");
    fs::remove_all(db_path);

    LSMTree db(db_path, 1024, 1024 * 1024);

    // Insert sequential keys
    for (int i = 0; i < 10; i++) {
        std::string key = "key" + std::to_string(i);
        std::string value = "value" + std::to_string(i);
        if (!db.put(key, value)) {
            std::cerr << "  Failed to insert " << key << std::endl;
            return false;
        }
    }

    // Test full range scan
    auto results = db.scan("key0", "key9");
    if (results.size() != 10) {
        std::cerr << "  Full scan expected 10 results, got " << results.size() << std::endl;
        return false;
    }

    // Verify order and values
    for (int i = 0; i < 10; i++) {
        if (results[i].first != "key" + std::to_string(i) ||
            results[i].second != "value" + std::to_string(i)) {
            std::cerr << "  Scan result mismatch at index " << i << std::endl;
            return false;
        }
    }

    // Test partial range
    results = db.scan("key3", "key7");
    if (results.size() != 5) { // key3, key4, key5, key6, key7
        std::cerr << "  Partial scan expected 5 results, got " << results.size() << std::endl;
        return false;
    }

    // Test range with deletions
    if (!db.remove("key5")) {
        std::cerr << "  Failed to delete key5" << std::endl;
        return false;
    }

    results = db.scan("key0", "key9");
    if (results.size() != 9) { // key5 is deleted
        std::cerr << "  Scan with deletions expected 9 results, got " << results.size() << std::endl;
        return false;
    }

    // Verify key5 is missing
    for (const auto& [key, value] : results) {
        if (key == "key5") {
            std::cerr << "  Deleted key5 found in scan results" << std::endl;
            return false;
        }
    }

    // Test empty range
    results = db.scan("x", "y");
    if (!results.empty()) {
        std::cerr << "  Empty range should return empty results" << std::endl;
        return false;
    }

    // Test single key range
    results = db.scan("key3", "key3");
    if (results.size() != 1 || results[0].first != "key3") {
        std::cerr << "  Single key range scan failed" << std::endl;
        return false;
    }

    // Test range with updates (newer values should override)
    if (!db.put("key3", "updated_value3") || !db.put("key7", "updated_value7")) {
        std::cerr << "  Failed to update keys" << std::endl;
        return false;
    }

    results = db.scan("key3", "key7");
    bool found_key3 = false, found_key7 = false;
    for (const auto& [key, value] : results) {
        if (key == "key3") {
            if (value != "updated_value3") {
                std::cerr << "  key3 not updated in scan" << std::endl;
                return false;
            }
            found_key3 = true;
        } else if (key == "key7") {
            if (value != "updated_value7") {
                std::cerr << "  key7 not updated in scan" << std::endl;
                return false;
            }
            found_key7 = true;
        }
    }

    if (!found_key3 || !found_key7) {
        std::cerr << "  Updated keys missing from scan" << std::endl;
        return false;
    }

    return true;
}

// Test 5: Memtable flush
bool test_lsm_memtable_flush(const std::string& test_dir) {
    std::string db_path = make_test_path(test_dir, "flush_db");
    fs::remove_all(db_path);

    // Create database with very small memtable (100 bytes)
    LSMTree db(db_path, 100, 1024 * 1024);

    // Insert data until memtable flushes
    std::vector<std::pair<std::string, std::string>> inserted;

    for (int i = 0; i < 20; i++) {
        std::string key = "key" + std::to_string(i);
        std::string value = "value" + std::to_string(i) + "_" + std::string(10, 'x'); // Make values larger
        if (!db.put(key, value)) {
            std::cerr << "  Failed to insert " << key << std::endl;
            return false;
        }
        inserted.emplace_back(key, value);
    }

    // Check that data is still accessible after flush
    for (const auto& [key, expected_value] : inserted) {
        if (!verify_key_value(db, key, expected_value)) {
            std::cerr << "  Data lost after memtable flush for key: " << key << std::endl;
            return false;
        }
    }

    // Check stats
    auto stats = db.get_stats();
    if (stats.memtable_flushes == 0) {
        std::cerr << "  Expected at least one memtable flush" << std::endl;
        return false;
    }

    if (stats.sstables_created == 0) {
        std::cerr << "  Expected at least one SSTable created" << std::endl;
        return false;
    }

    // Verify SSTable count
    size_t sstable_count = db.get_sstable_count();
    if (sstable_count == 0) {
        std::cerr << "  No SSTables after flush" << std::endl;
        return false;
    }

    std::cout << "  Memtable flushes: " << stats.memtable_flushes << std::endl;
    std::cout << "  SSTables created: " << stats.sstables_created << std::endl;
    std::cout << "  Current SSTables: " << sstable_count << std::endl;

    return true;
}

// Test 6: Recovery
bool test_lsm_recovery(const std::string& test_dir) {
    std::string db_path = make_test_path(test_dir, "recovery_db");
    fs::remove_all(db_path);

    // First session: insert data
    {
        LSMTree db(db_path, 1024, 1024 * 1024);

        if (!db.put("user1", "Alice") ||
            !db.put("user2", "Bob") ||
            !db.put("user3", "Charlie")) {
            std::cerr << "  Failed to insert data in first session" << std::endl;
            return false;
        }

        // Delete one user
        if (!db.remove("user2")) {
            std::cerr << "  Failed to delete user2" << std::endl;
            return false;
        }

        // Update another
        if (!db.put("user1", "Alice_updated")) {
            std::cerr << "  Failed to update user1" << std::endl;
            return false;
        }

        // Force flush
        db.put("flush_trigger", std::string(2000, 'x')); // Large value to trigger flush
    }

    // Second session: recover
    {
        LSMTree db(db_path, 1024, 1024 * 1024);

        // Verify recovered data
        if (!verify_key_value(db, "user1", "Alice_updated")) {
            std::cerr << "  user1 not recovered correctly" << std::endl;
            return false;
        }

        if (!verify_key_missing(db, "user2")) {
            std::cerr << "  user2 should be deleted" << std::endl;
            return false;
        }

        if (!verify_key_value(db, "user3", "Charlie")) {
            std::cerr << "  user3 not recovered correctly" << std::endl;
            return false;
        }

        // Add new data after recovery
        if (!db.put("user4", "David") || !db.put("user5", "Eve")) {
            std::cerr << "  Failed to add new data after recovery" << std::endl;
            return false;
        }

        if (!verify_key_value(db, "user4", "David") || !verify_key_value(db, "user5", "Eve")) {
            return false;
        }
    }

    // Third session: verify persistence
    {
        LSMTree db(db_path, 1024, 1024 * 1024);

        if (!verify_key_value(db, "user1", "Alice_updated") ||
            !verify_key_missing(db, "user2") ||
            !verify_key_value(db, "user3", "Charlie") ||
            !verify_key_value(db, "user4", "David") ||
            !verify_key_value(db, "user5", "Eve")) {
            std::cerr << "  Data not persistent across sessions" << std::endl;
            return false;
        }
    }

    return true;
}

// Test 7: Edge cases
bool test_lsm_edge_cases(const std::string& test_dir) {
    std::string db_path = make_test_path(test_dir, "edge_db");
    fs::remove_all(db_path);

    LSMTree db(db_path, 1024, 1024 * 1024);

    // Test empty strings
    if (!db.put("", "empty_key") || !db.put("empty_value", "")) {
        std::cerr << "  Failed to handle empty strings" << std::endl;
        return false;
    }

    if (!verify_key_value(db, "", "empty_key") || !verify_key_value(db, "empty_value", "")) {
        return false;
    }

    // Test special characters
    std::string special_key = "key\n\t\r\0\x01";
    std::string special_value = "value\n\t\r\0\xFF";

    if (!db.put(special_key, special_value)) {
        std::cerr << "  Failed to insert special characters" << std::endl;
        return false;
    }

    auto value = db.get(special_key);
    if (!value.has_value() || *value != special_value) {
        std::cerr << "  Failed to retrieve special characters" << std::endl;
        return false;
    }

    // Test very long key/value
    std::string long_key(1000, 'k');
    std::string long_value(10000, 'v');

    if (!db.put(long_key, long_value)) {
        std::cerr << "  Failed to insert long key/value" << std::endl;
        return false;
    }

    if (!verify_key_value(db, long_key, long_value)) {
        return false;
    }

    // Test duplicate puts
    for (int i = 0; i < 10; i++) {
        if (!db.put("duplicate", "value" + std::to_string(i))) {
            std::cerr << "  Failed duplicate put " << i << std::endl;
            return false;
        }
    }

    // Should have last value
    if (!verify_key_value(db, "duplicate", "value9")) {
        return false;
    }

    return true;
}

// Test 8: Concurrent simulation (not truly concurrent, but simulating multiple operations)
bool test_lsm_concurrent_simulation(const std::string& test_dir) {
    std::string db_path = make_test_path(test_dir, "concurrent_db");
    fs::remove_all(db_path);

    LSMTree db(db_path, 1024, 1024 * 1024);

    // Simulate multiple "threads" or operations interleaved
    std::vector<std::pair<std::string, std::string>> operations = {
        {"user:1", "Alice"},
        {"user:2", "Bob"},
        {"user:3", "Charlie"},
        {"user:2", ""}, // Delete Bob
        {"user:1", "Alicia"}, // Update Alice
        {"user:4", "David"},
        {"user:3", ""}, // Delete Charlie
        {"user:5", "Eve"},
        {"user:1", ""}, // Delete Alicia
        {"user:6", "Frank"}
    };

    for (const auto& [key, value] : operations) {
        if (value.empty()) {
            if (!db.remove(key)) {
                std::cerr << "  Failed to delete " << key << std::endl;
                return false;
            }
        } else {
            if (!db.put(key, value)) {
                std::cerr << "  Failed to put " << key << " = " << value << std::endl;
                return false;
            }
        }
    }

    // Verify final state
    if (!verify_key_missing(db, "user:1")) return false; // Deleted
    if (!verify_key_missing(db, "user:2")) return false; // Deleted
    if (!verify_key_missing(db, "user:3")) return false; // Deleted
    if (!verify_key_value(db, "user:4", "David")) return false;
    if (!verify_key_value(db, "user:5", "Eve")) return false;
    if (!verify_key_value(db, "user:6", "Frank")) return false;

    return true;
}

// Test 9: Large data handling
bool test_lsm_large_data(const std::string& test_dir) {
    std::string db_path = make_test_path(test_dir, "large_db");
    fs::remove_all(db_path);

    LSMTree db(db_path, 1024 * 1024, 10 * 1024 * 1024); // 1MB memtable

    const int NUM_ENTRIES = 1000;

    auto start = high_resolution_clock::now();

    // Insert large dataset
    for (int i = 0; i < NUM_ENTRIES; i++) {
        std::string key = "record_" + std::to_string(i);
        std::string value = "data_" + std::to_string(i) + "_" + std::string(100, 'x'); // ~100 bytes

        if (!db.put(key, value)) {
            std::cerr << "  Failed to insert entry " << i << std::endl;
            return false;
        }

        // Every 100 entries, verify some random ones
        if (i % 100 == 0 && i > 0) {
            for (int j = 0; j < 5; j++) {
                int random_idx = std::rand() % (i + 1);
                std::string test_key = "record_" + std::to_string(random_idx);
                std::string expected = "data_" + std::to_string(random_idx) + "_" + std::string(100, 'x');

                if (!verify_key_value(db, test_key, expected)) {
                    std::cerr << "  Verification failed at entry " << i << ", key " << test_key << std::endl;
                    return false;
                }
            }
        }
    }

    auto end = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(end - start);

    std::cout << "  Inserted " << NUM_ENTRIES << " entries in "
              << duration.count() << " ms ("
              << (NUM_ENTRIES * 1000.0 / duration.count()) << " ops/sec)" << std::endl;

    // Final verification
    int verify_count = 0;
    for (int i = 0; i < 50; i++) {
        int random_idx = std::rand() % NUM_ENTRIES;
        std::string test_key = "record_" + std::to_string(random_idx);
        std::string expected = "data_" + std::to_string(random_idx) + "_" + std::string(100, 'x');

        if (!verify_key_value(db, test_key, expected)) {
            std::cerr << "  Final verification failed for key " << test_key << std::endl;
            return false;
        }
        verify_count++;
    }

    // Test scan on large dataset
    auto scan_results = db.scan("record_100", "record_199");
    if (scan_results.size() != 100) {
        std::cerr << "  Large scan expected 100 results, got " << scan_results.size() << std::endl;
        return false;
    }

    // Check stats
    auto stats = db.get_stats();
    std::cout << "  Stats - Puts: " << stats.total_puts
              << ", Gets: " << stats.total_gets
              << ", Flushes: " << stats.memtable_flushes
              << ", SSTables: " << stats.sstables_created << std::endl;

    return true;
}

// Test 10: Performance
bool test_lsm_performance(const std::string& test_dir) {
    std::string db_path = make_test_path(test_dir, "perf_db");
    fs::remove_all(db_path);

    LSMTree db(db_path, 1024 * 1024, 10 * 1024 * 1024); // 1MB memtable

    const int NUM_OPS = 10000;
    const int KEY_SPACE = 1000; // Only 1000 unique keys

    std::srand(static_cast<unsigned>(std::time(nullptr)));

    auto start = high_resolution_clock::now();

    for (int i = 0; i < NUM_OPS; i++) {
        int key_idx = std::rand() % KEY_SPACE;
        std::string key = "key_" + std::to_string(key_idx);
        std::string value = "value_" + std::to_string(i);

        if (std::rand() % 10 == 0) { // 10% deletes
            db.remove(key);
        } else { // 90% puts
            db.put(key, value);
        }

        // Occasionally read
        if (i % 100 == 0) {
            db.get(key);
        }
    }

    auto end = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(end - start);

    std::cout << "  " << NUM_OPS << " operations in "
              << duration.count() << " ms ("
              << (NUM_OPS * 1000.0 / duration.count()) << " ops/sec)" << std::endl;

    auto stats = db.get_stats();
    std::cout << "  Final stats - Puts: " << stats.total_puts
              << ", Gets: " << stats.total_gets
              << ", Deletes: " << stats.total_deletes
              << ", Flushes: " << stats.memtable_flushes << std::endl;

    return true;
}

// Test 11: Statistics
bool test_lsm_statistics(const std::string& test_dir) {
    std::string db_path = make_test_path(test_dir, "stats_db");
    fs::remove_all(db_path);

    LSMTree db(db_path, 500, 1024 * 1024); // Small memtable to force flushes

    // Get initial stats
    auto stats1 = db.get_stats();
    if (stats1.total_puts != 0 || stats1.total_gets != 0 || stats1.total_deletes != 0) {
        std::cerr << "  Initial stats not zero" << std::endl;
        return false;
    }

    // Perform operations
    for (int i = 0; i < 10; i++) {
        db.put("key" + std::to_string(i), "value" + std::to_string(i));
    }

    for (int i = 0; i < 5; i++) {
        db.get("key" + std::to_string(i));
    }

    for (int i = 5; i < 8; i++) {
        db.remove("key" + std::to_string(i));
    }

    // Get updated stats
    auto stats2 = db.get_stats();

    if (stats2.total_puts != 10) {
        std::cerr << "  Expected 10 puts, got " << stats2.total_puts << std::endl;
        return false;
    }

    if (stats2.total_gets != 5) {
        std::cerr << "  Expected 5 gets, got " << stats2.total_gets << std::endl;
        return false;
    }

    if (stats2.total_deletes != 3) {
        std::cerr << "  Expected 3 deletes, got " << stats2.total_deletes << std::endl;
        return false;
    }

    // Check memtable size
    size_t memtable_size = db.get_memtable_size();
    if (memtable_size == 0) {
        std::cerr << "  Memtable should have some data" << std::endl;
        return false;
    }

    // Check SSTable count (might have flushed)
    size_t sstable_count = db.get_sstable_count();
    std::cout << "  Memtable size: " << memtable_size << " bytes" << std::endl;
    std::cout << "  SSTable count: " << sstable_count << std::endl;
    std::cout << "  Memtable flushes: " << stats2.memtable_flushes << std::endl;

    // Check level sizes
    auto level_sizes = db.get_level_sizes();
    std::cout << "  Level sizes: ";
    for (size_t size : level_sizes) {
        std::cout << size << " ";
    }
    std::cout << std::endl;

    return true;
}

// Test 12: Memory safety (create/destroy many databases)
bool test_lsm_memory_safety(const std::string& test_dir) {
    for (int i = 0; i < 10; i++) {
        std::string db_path = make_test_path(test_dir, "memtest_db_" + std::to_string(i));
        fs::remove_all(db_path);

        {
            LSMTree db(db_path, 1024, 1024 * 1024);

            // Insert some data
            if (!db.put("test_key", "test_value")) {
                std::cerr << "  Failed to insert data in database " << i << std::endl;
                return false;
            }

            // Verify
            if (!verify_key_value(db, "test_key", "test_value")) {
                return false;
            }
        } // db destroyed here

        // Reopen and verify data persists
        {
            LSMTree db(db_path, 1024, 1024 * 1024);

            if (!verify_key_value(db, "test_key", "test_value")) {
                std::cerr << "  Data not persistent in database " << i << std::endl;
                return false;
            }

            // Add more data
            if (!db.put("additional_key", "additional_value")) {
                std::cerr << "  Failed to add data in database " << i << std::endl;
                return false;
            }
        }

        // Reopen again and verify all data
        {
            LSMTree db(db_path, 1024, 1024 * 1024);

            if (!verify_key_value(db, "test_key", "test_value") ||
                !verify_key_value(db, "additional_key", "additional_value")) {
                std::cerr << "  All data not persistent in database " << i << std::endl;
                return false;
            }
        }
    }

    return true;
}

// Test 13: Multiple databases
bool test_lsm_multiple_databases(const std::string& test_dir) {
    // Create multiple independent databases
    LSMTree db1(make_test_path(test_dir, "multi1"), 1024, 1024 * 1024);
    LSMTree db2(make_test_path(test_dir, "multi2"), 1024, 1024 * 1024);
    LSMTree db3(make_test_path(test_dir, "multi3"), 1024, 1024 * 1024);

    // Insert different data in each
    if (!db1.put("db1_key", "db1_value") ||
        !db2.put("db2_key", "db2_value") ||
        !db3.put("db3_key", "db3_value")) {
        std::cerr << "  Failed to insert data in multiple databases" << std::endl;
        return false;
    }

    // Verify isolation
    if (!verify_key_value(db1, "db1_key", "db1_value") ||
        verify_key_missing(db1, "db2_key") ||
        verify_key_missing(db1, "db3_key")) {
        std::cerr << "  Database 1 not isolated" << std::endl;
        return false;
    }

    if (!verify_key_value(db2, "db2_key", "db2_value") ||
        verify_key_missing(db2, "db1_key") ||
        verify_key_missing(db2, "db3_key")) {
        std::cerr << "  Database 2 not isolated" << std::endl;
        return false;
    }

    if (!verify_key_value(db3, "db3_key", "db3_value") ||
        verify_key_missing(db3, "db1_key") ||
        verify_key_missing(db3, "db2_key")) {
        std::cerr << "  Database 3 not isolated" << std::endl;
        return false;
    }

    // Test same key in different databases
    if (!db1.put("common_key", "db1_common") ||
        !db2.put("common_key", "db2_common") ||
        !db3.put("common_key", "db3_common")) {
        std::cerr << "  Failed to insert common key" << std::endl;
        return false;
    }

    if (!verify_key_value(db1, "common_key", "db1_common") ||
        !verify_key_value(db2, "common_key", "db2_common") ||
        !verify_key_value(db3, "common_key", "db3_common")) {
        std::cerr << "  Common keys not isolated" << std::endl;
        return false;
    }

    return true;
}

// Test 14: Corner cases
bool test_lsm_corner_cases(const std::string& test_dir) {
    std::string db_path = make_test_path(test_dir, "corner_db");
    fs::remove_all(db_path);

    LSMTree db(db_path, 100, 1024 * 1024); // Very small memtable

    // Test many small inserts that trigger frequent flushes
    for (int i = 0; i < 100; i++) {
        std::string key = "tiny_" + std::to_string(i);
        if (!db.put(key, "x")) { // Single character value
            std::cerr << "  Failed tiny insert " << i << std::endl;
            return false;
        }
    }

    // Verify all tiny keys
    for (int i = 0; i < 100; i++) {
        std::string key = "tiny_" + std::to_string(i);
        if (!verify_key_value(db, key, "x")) {
            std::cerr << "  Failed to verify tiny key " << i << std::endl;
            return false;
        }
    }

    // Test immediate delete after insert
    if (!db.put("temp", "temp_value") || !db.remove("temp")) {
        std::cerr << "  Failed immediate delete" << std::endl;
        return false;
    }

    if (!verify_key_missing(db, "temp")) {
        std::cerr << "  Immediate delete not effective" << std::endl;
        return false;
    }

    // Test deleting non-existent key multiple times
    for (int i = 0; i < 5; i++) {
        if (!db.remove("never_existed")) {
            std::cerr << "  Failed to delete non-existent key" << std::endl;
            return false;
        }
    }

    // Test scan with inverted range
    auto results = db.scan("z", "a");
    if (!results.empty()) {
        std::cerr << "  Inverted range should return empty" << std::endl;
        return false;
    }

    // Test scan with same start and end but non-existent key
    results = db.scan("nonexistent", "nonexistent");
    if (!results.empty()) {
        std::cerr << "  Scan for non-existent single key should return empty" << std::endl;
        return false;
    }

    return true;
}

// Test 15: Mixed operations
bool test_lsm_mixed_operations(const std::string& test_dir) {
    std::string db_path = make_test_path(test_dir, "mixed_db");
    fs::remove_all(db_path);

    LSMTree db(db_path, 1024, 1024 * 1024);

    std::vector<std::string> keys = {"apple", "banana", "cherry", "date", "elderberry"};
    std::vector<std::string> values = {"red", "yellow", "red", "brown", "purple"};

    // Mixed sequence of operations
    std::vector<std::function<bool()>> operations = {
        [&]() { return db.put(keys[0], values[0]); },
        [&]() { return db.put(keys[1], values[1]); },
        [&]() { return db.put(keys[2], values[2]); },
        [&]() { return db.remove(keys[0]); },
        [&]() { return db.put(keys[3], values[3]); },
        [&]() { return verify_key_missing(db, keys[0]); },
        [&]() { return verify_key_value(db, keys[1], values[1]); },
        [&]() { return verify_key_value(db, keys[2], values[2]); },
        [&]() { return db.put(keys[0], "green"); }, // Reinsert deleted key
        [&]() { return db.put(keys[4], values[4]); },
        [&]() { return verify_key_value(db, keys[0], "green"); },
        [&]() { return db.remove(keys[2]); },
        [&]() { return verify_key_missing(db, keys[2]); },
        [&]() { return db.put(keys[2], "dark_red"); },
        [&]() { return verify_key_value(db, keys[2], "dark_red"); }
    };

    for (size_t i = 0; i < operations.size(); i++) {
        if (!operations[i]()) {
            std::cerr << "  Operation " << i << " failed" << std::endl;
            return false;
        }
    }

    // Final scan
    auto results = db.scan("a", "z");

    // Should have all keys except cherry was deleted and reinserted
    if (results.size() != 5) {
        std::cerr << "  Final scan expected 5 results, got " << results.size() << std::endl;
        return false;
    }

    // Verify all values
    std::map<std::string, std::string> expected = {
        {keys[0], "green"},
        {keys[1], values[1]},
        {keys[2], "dark_red"},
        {keys[3], values[3]},
        {keys[4], values[4]}
    };

    for (const auto& [key, value] : results) {
        auto it = expected.find(key);
        if (it == expected.end()) {
            std::cerr << "  Unexpected key in results: " << key << std::endl;
            return false;
        }
        if (it->second != value) {
            std::cerr << "  Key " << key << " has value " << value
                      << " but expected " << it->second << std::endl;
            return false;
        }
    }

    return true;
}

// Main test runner
int lsm_tests_main() {
    // Create unique test directory
    auto now = high_resolution_clock::now();
    auto timestamp = duration_cast<milliseconds>(now.time_since_epoch()).count();
    std::string test_dir = "lsm_tests_" + std::to_string(timestamp);

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
        {"1. Basic Operations", test_lsm_basic_operations},
        {"2. Update Operations", test_lsm_updates},
        {"3. Delete Operations", test_lsm_deletes},
        {"4. Scan Operations", test_lsm_scan_operations},
        {"5. Memtable Flush", test_lsm_memtable_flush},
        {"6. Recovery", test_lsm_recovery},
        {"7. Edge Cases", test_lsm_edge_cases},
        {"8. Concurrent Simulation", test_lsm_concurrent_simulation},
        {"9. Large Data", test_lsm_large_data},
        {"10. Performance", test_lsm_performance},
        {"11. Statistics", test_lsm_statistics},
        {"12. Memory Safety", test_lsm_memory_safety},
        {"13. Multiple Databases", test_lsm_multiple_databases},
        {"14. Corner Cases", test_lsm_corner_cases},
        {"15. Mixed Operations", test_lsm_mixed_operations}
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
    // Comment this out if you want to inspect the test files
    try {
        fs::remove_all(test_dir);
        std::cout << "Cleaned up test directory: " << test_dir << std::endl;
    } catch (const fs::filesystem_error&) {
        std::cout << "\nNote: Could not clean up test directory: " << test_dir << std::endl;
        std::cout << "You may need to manually delete it." << std::endl;
    }

    if (passed == total) {
        std::cout << "\nAll LSM tree tests passed!" << std::endl;
        return 0;
    } else {
        std::cout << "\nSome LSM tree tests failed" << std::endl;
        return 1;
    }
}