//
// Created by K on 2025-12-06.
//

#include "test_kvstore.h"
#include "../KVStore.h"
#include <iostream>
#include <vector>
#include <string>
#include <algorithm>
#include <filesystem>
#include <chrono>
#include <thread>
#include <functional>

#include "test_helper.h"

namespace fs = std::filesystem;
using namespace std::chrono;

std::string generate_test_db_name(const std::string& test_name) {
    // Use a timestamp to make it unique
    static std::atomic<size_t> counter = 0;
    size_t id = ++counter;
    return "test_db_" + test_name + "_" + std::to_string(id);
}

// RAII wrapper for test database cleanup
class TestDatabase {
public:
    TestDatabase(const std::string& name) : name_(name) {}

    ~TestDatabase() {
        cleanup();
    }

    const std::string& name() const { return name_; }

    void cleanup() const
    {
        // Try multiple times to clean up (Windows needs this)
        for (int i = 0; i < 3; ++i) {
            try {
                if (fs::exists(name_)) {
                    // Close any open files first
                    std::this_thread::sleep_for(std::chrono::milliseconds(10));
                    fs::remove_all(name_);
                }
                break;
            } catch (const fs::filesystem_error& e) {
                if (i == 2) {
                    std::cerr << "Warning: Could not clean up test database '"
                              << name_ << "': " << e.what() << std::endl;
                } else {
                    std::this_thread::sleep_for(std::chrono::milliseconds(50));
                }
            }
        }
    }

private:
    std::string name_;
};

// Test 1: Basic Put and Get
bool test_kvstore_basic_operations() {
    TestDatabase db(generate_test_db_name("basic"));
    auto kv_store = KVStore::open(db.name(), 1024 * 1024); // 1MB memtable

    if (!kv_store) {
        std::cerr << "  Failed to open database" << std::endl;
        return false;
    }

    // Test PUT
    if (!kv_store->put("key1", "value1")) {
        std::cerr << "  Failed to put key1" << std::endl;
        return false;
    }

    if (!kv_store->put("key2", "value2")) {
        std::cerr << "  Failed to put key2" << std::endl;
        return false;
    }

    if (!kv_store->put("key3", "value3")) {
        std::cerr << "  Failed to put key3" << std::endl;
        return false;
    }

    // Test GET
    auto value1 = kv_store->get("key1");
    if (!value1.has_value() || value1.value() != "value1") {
        std::cerr << "  Failed to get key1" << std::endl;
        return false;
    }

    auto value2 = kv_store->get("key2");
    if (!value2.has_value() || value2.value() != "value2") {
        std::cerr << "  Failed to get key2" << std::endl;
        return false;
    }

    // Test non-existent key
    auto non_existent = kv_store->get("key999");
    if (non_existent.has_value()) {
        std::cerr << "  Non-existent key should return empty optional" << std::endl;
        return false;
    }

    // Close and cleanup
    kv_store->close();

    // Try to reopen and verify data persisted
    auto kv_store2 = KVStore::open(db.name(), 1024 * 1024);
    if (!kv_store2) {
        std::cerr << "  Failed to reopen database" << std::endl;
        return false;
    }

    value1 = kv_store2->get("key1");
    if (!value1.has_value() || value1.value() != "value1") {
        std::cerr << "  Data not persisted after close/reopen" << std::endl;
        kv_store2->close();
        return false;
    }

    kv_store2->close();
    return true;
}

// Test 2: Updates and Overwrites
bool test_kvstore_updates() {
    TestDatabase db(generate_test_db_name("updates"));
    auto kv_store = KVStore::open(db.name(), 1024 * 1024);

    if (!kv_store) {
        std::cerr << "  Failed to open database" << std::endl;
        return false;
    }

    // Put initial value
    if (!kv_store->put("key", "value1")) {
        std::cerr << "  Failed initial put" << std::endl;
        return false;
    }

    // Update value
    if (!kv_store->put("key", "value2")) {
        std::cerr << "  Failed update put" << std::endl;
        return false;
    }

    // Should get latest value
    auto value = kv_store->get("key");
    if (!value.has_value() || value.value() != "value2") {
        std::cerr << "  Update failed, got: " << (value.has_value() ? value.value() : "<empty>")
                  << ", expected: value2" << std::endl;
        return false;
    }

    // Multiple updates
    for (int i = 0; i < 10; i++) {
        std::string expected = "value" + std::to_string(i);
        if (!kv_store->put("multikey", expected)) {
            std::cerr << "  Failed multiple update " << i << std::endl;
            return false;
        }

        auto val = kv_store->get("multikey");
        if (!val.has_value() || val.value() != expected) {
            std::cerr << "  Multiple update failed at iteration " << i << std::endl;
            return false;
        }
    }

    kv_store->close();
    return true;
}

// Test 3: Delete operations
bool test_kvstore_deletes() {
    TestDatabase db(generate_test_db_name("deletes"));
    auto kv_store = KVStore::open(db.name(), 1024 * 1024);

    if (!kv_store) {
        std::cerr << "  Failed to open database" << std::endl;
        return false;
    }

    // Insert test data
    std::vector<std::pair<std::string, std::string>> test_data = {
        {"apple", "fruit"},
        {"banana", "yellow fruit"},
        {"carrot", "vegetable"},
        {"date", "sweet fruit"},
        {"eggplant", "purple vegetable"},
        {"fig", "small fruit"}
    };

    for (const auto& [key, value] : test_data) {
        if (!kv_store->put(key, value)) {
            std::cerr << "  Failed to put " << key << std::endl;
            return false;
        }
    }

    // Delete one key
    if (!kv_store->remove("date")) {
        std::cerr << "  Failed to delete date" << std::endl;
        return false;
    }

    // Test range scan after delete (range c to ez to include eggplant)
    auto results = kv_store->scan("c", "ez");
    // Should get: carrot, eggplant (date is deleted)
    if (results.size() != 2) {
        std::cerr << "  Range with deleted key failed: expected 2, got " << results.size() << std::endl;
        for (const auto& [key, value] : results) {
            std::cerr << "    Found: " << key << std::endl;
        }
        return false;
    }

    // Verify we have the right keys
    bool found_carrot = false, found_eggplant = false;
    for (const auto& [key, value] : results) {
        if (key == "carrot") found_carrot = true;
        if (key == "eggplant") found_eggplant = true;
        if (key == "date") {
            std::cerr << "  Deleted key 'date' should not appear in scan" << std::endl;
            return false;
        }
    }

    if (!found_carrot || !found_eggplant) {
        std::cerr << "  Missing expected keys in scan" << std::endl;
        return false;
    }

    // Put then delete
    if (!kv_store->put("key1", "value1")) {
        std::cerr << "  Failed to put key1" << std::endl;
        return false;
    }

    if (!kv_store->remove("key1")) {
        std::cerr << "  Failed to delete key1" << std::endl;
        return false;
    }

    // Should not find deleted key
    auto value = kv_store->get("key1");
    if (value.has_value()) {
        std::cerr << "  Deleted key should return empty optional" << std::endl;
        return false;
    }

    // Delete non-existent key (should still succeed as tombstone)
    if (!kv_store->remove("nonexistent")) {
        std::cerr << "  Delete of non-existent key should succeed" << std::endl;
        return false;
    }

    // Re-insert after delete
    if (!kv_store->put("key1", "newvalue")) {
        std::cerr << "  Failed to re-put after delete" << std::endl;
        return false;
    }

    value = kv_store->get("key1");
    if (!value.has_value() || value.value() != "newvalue") {
        std::cerr << "  Re-insert after delete failed" << std::endl;
        return false;
    }

    kv_store->close();
    return true;
}

// Test 4: Range scans
bool test_kvstore_range_scans() {
    TestDatabase db(generate_test_db_name("scan"));
    auto kv_store = KVStore::open(db.name(), 1024 * 1024);

    if (!kv_store) {
        std::cerr << "  Failed to open database" << std::endl;
        return false;
    }

    // Insert keys in alphabetical order
    std::vector<std::pair<std::string, std::string>> test_data = {
        {"apple", "fruit"},
        {"banana", "yellow fruit"},
        {"carrot", "vegetable"},
        {"date", "sweet fruit"},
        {"eggplant", "purple vegetable"},
        {"fig", "small fruit"},
        {"grape", "bunch fruit"}
    };

    for (const auto& [key, value] : test_data) {
        if (!kv_store->put(key, value)) {
            std::cerr << "  Failed to put " << key << std::endl;
            return false;
        }
    }

    // Test full range
    auto results = kv_store->scan("a", "z");
    if (results.size() != test_data.size()) {
        std::cerr << "  Full range scan failed: expected " << test_data.size()
                  << ", got " << results.size() << std::endl;
        return false;
    }

    // Verify all data present
    for (const auto& [key, value] : test_data) {
        bool found = false;
        for (const auto& result : results) {
            if (result.first == key && result.second == value) {
                found = true;
                break;
            }
        }
        if (!found) {
            std::cerr << "  Missing key in scan: " << key << std::endl;
            return false;
        }
    }

    // Test subrange (c to ez to include eggplant)
    results = kv_store->scan("c", "ez");
    if (results.size() != 3) {
        std::cerr << "  Subrange scan failed: expected 3 (carrot, date, eggplant), got " << results.size() << std::endl;
        for (const auto& [key, value] : results) {
            std::cerr << "    Found: " << key << std::endl;
        }
        return false;
    }

    // Test empty range
    results = kv_store->scan("h", "i");
    if (!results.empty()) {
        std::cerr << "  Empty range should return empty results" << std::endl;
        return false;
    }

    // Test single key range
    results = kv_store->scan("banana", "banana");
    if (results.size() != 1 || results[0].first != "banana" || results[0].second != "yellow fruit") {
        std::cerr << "  Single key range scan failed" << std::endl;
        return false;
    }

    kv_store->close();
    return true;
}

// Test 5: Memtable flushing
bool test_kvstore_memtable_flushing() {
    TestDatabase db(generate_test_db_name("flush"));
    // Small memtable to trigger flushing
    auto kv_store = KVStore::open(db.name(), 1024); // 1KB memtable

    if (!kv_store) {
        std::cerr << "  Failed to open database" << std::endl;
        return false;
    }

    // Insert enough data to exceed memtable size
    // Each entry: key (5) + value (100) + overhead ≈ 110 bytes
    // 1KB / 110 ≈ 9 entries to fill
    const int NUM_ENTRIES = 20;

    for (int i = 0; i < NUM_ENTRIES; i++) {
        std::string key = "key" + std::to_string(i);
        std::string value = std::string(100, 'A' + (i % 26));

        if (!kv_store->put(key, value)) {
            std::cerr << "  Failed to put entry " << i << std::endl;
            return false;
        }

        // Verify immediate read
        auto retrieved = kv_store->get(key);
        if (!retrieved.has_value() || retrieved.value() != value) {
            std::cerr << "  Immediate read failed for entry " << i << std::endl;
            return false;
        }
    }

    // Check that we have SST files
    size_t sst_count = 0;
    try {
        for (const auto& entry : fs::directory_iterator(db.name())) {
            if (entry.path().extension() == ".sst") {
                sst_count++;
            }
        }
    } catch (const fs::filesystem_error&) {
        // Directory might not exist or be accessible
    }

    if (sst_count == 0) {
        std::cerr << "  Expected SST files after memtable flush, found none" << std::endl;
        return false;
    }

    std::cout << "  Created " << sst_count << " SST files" << std::endl;

    // Verify all data after flushing
    for (int i = 0; i < NUM_ENTRIES; i++) {
        std::string key = "key" + std::to_string(i);
        std::string expected_value = std::string(100, 'A' + (i % 26));

        auto value = kv_store->get(key);
        if (!value.has_value() || value.value() != expected_value) {
            std::cerr << "  Data lost after flush for key: " << key << std::endl;
            return false;
        }
    }

    // Manual flush
    kv_store->flush_memtable();

    kv_store->close();
    return true;
}

// Test 6: WAL recovery
bool test_kvstore_wal_recovery() {
    const TestDatabase db(generate_test_db_name("wal_recovery"));

    // First session: insert data without closing properly (simulate crash)
    {
        auto kv_store = KVStore::open(db.name(), 1024 * 1024);
        if (!kv_store) {
            std::cerr << "  Failed to open database (session 1)" << std::endl;
            return false;
        }

        if (!kv_store->put("key1", "value1") ||
            !kv_store->put("key2", "value2") ||
            !kv_store->remove("key1")) {
            std::cerr << "  Failed operations in session 1" << std::endl;
            return false;
        }

        // Note: NOT calling close() to simulate crash
        // Destructor will be called, but WAL should persist
    }

    // Small delay to ensure files are released
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // Second session: recover from WAL
    {
        auto kv_store = KVStore::open(db.name(), 1024 * 1024);
        if (!kv_store) {
            std::cerr << "  Failed to open database (session 2)" << std::endl;
            return false;
        }

        // key1 should be deleted
        auto value1 = kv_store->get("key1");
        if (value1.has_value()) {
            std::cerr << "  key1 should be deleted after recovery" << std::endl;
            kv_store->close();
            return false;
        }

        // key2 should exist
        auto value2 = kv_store->get("key2");
        if (!value2.has_value() || value2.value() != "value2") {
            std::cerr << "  key2 not recovered properly" << std::endl;
            kv_store->close();
            return false;
        }

        kv_store->close();
    }

    return true;
}

// Test 7: Concurrent operations simulation
bool test_kvstore_concurrent_simulation() {
    TestDatabase db(generate_test_db_name("concurrent"));

    // Simulate multiple processes/threads accessing same DB
    // Process 1
    {
        auto kv_store = KVStore::open(db.name(), 1024 * 1024);
        if (!kv_store) {
            std::cerr << "  Process 1 failed to open" << std::endl;
            return false;
        }

        kv_store->put("user:1", "Alice");
        kv_store->put("user:2", "Bob");
        kv_store->close();
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // Process 2
    {
        auto kv_store = KVStore::open(db.name(), 1024 * 1024);
        if (!kv_store) {
            std::cerr << "  Process 2 failed to open" << std::endl;
            return false;
        }

        kv_store->put("user:3", "Charlie");
        kv_store->remove("user:1");
        kv_store->close();
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // Process 3
    {
        auto kv_store = KVStore::open(db.name(), 1024 * 1024);
        if (!kv_store) {
            std::cerr << "  Process 3 failed to open" << std::endl;
            return false;
        }

        kv_store->put("user:1", "Alice v2");
        kv_store->put("user:4", "David");
        kv_store->close();
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // Final verification
    {
        auto kv_store = KVStore::open(db.name(), 1024 * 1024);
        if (!kv_store) {
            std::cerr << "  Final verification failed to open" << std::endl;
            return false;
        }

        // Check all users
        auto alice = kv_store->get("user:1");
        if (!alice.has_value() || alice.value() != "Alice v2") {
            std::cerr << "  user:1 should be 'Alice v2'" << std::endl;
            kv_store->close();
            return false;
        }

        auto bob = kv_store->get("user:2");
        if (!bob.has_value() || bob.value() != "Bob") {
            std::cerr << "  user:2 should be 'Bob'" << std::endl;
            kv_store->close();
            return false;
        }

        auto charlie = kv_store->get("user:3");
        if (!charlie.has_value() || charlie.value() != "Charlie") {
            std::cerr << "  user:3 should be 'Charlie'" << std::endl;
            kv_store->close();
            return false;
        }

        auto david = kv_store->get("user:4");
        if (!david.has_value() || david.value() != "David") {
            std::cerr << "  user:4 should be 'David'" << std::endl;
            kv_store->close();
            return false;
        }

        // Range scan
        auto users = kv_store->scan("user:", "user;");  // ';' comes after ':' in ASCII
        if (users.size() != 4) {
            std::cerr << "  Should have 4 users, got " << users.size() << std::endl;
            kv_store->close();
            return false;
        }

        kv_store->close();
    }

    return true;
}

// Test 8: Statistics
bool test_kvstore_statistics() {
    TestDatabase db(generate_test_db_name("stats"));
    auto kv_store = KVStore::open(db.name(), 1024);

    if (!kv_store) {
        std::cerr << "  Failed to open database" << std::endl;
        return false;
    }

    // Perform operations
    kv_store->put("key1", "value1");
    kv_store->put("key2", "value2");
    kv_store->get("key1");
    kv_store->get("key3");  // non-existent
    kv_store->remove("key1");
    auto scan_result = kv_store->scan("a", "z");

    const auto final_stats = kv_store->get_stats();

    // Verify counts increased
    if (final_stats.puts < 2) {
        std::cerr << "  PUT count should be at least 2" << std::endl;
        kv_store->close();
        return false;
    }

    if (final_stats.gets < 2) {
        std::cerr << "  GET count should be at least 2" << std::endl;
        kv_store->close();
        return false;
    }

    if (final_stats.deletes < 1) {
        std::cerr << "  DELETE count should be at least 1" << std::endl;
        kv_store->close();
        return false;
    }

    if (final_stats.scans < 1) {
        std::cerr << "  SCAN count should be at least 1" << std::endl;
        kv_store->close();
        return false;
    }

    std::cout << "  Stats: PUTs=" << final_stats.puts
              << ", GETs=" << final_stats.gets
              << ", DELETEs=" << final_stats.deletes
              << ", SCANs=" << final_stats.scans
              << ", Flushes=" << final_stats.memtable_flushes
              << ", SSTs=" << final_stats.sst_files << std::endl;

    kv_store->close();
    return true;
}

// Test 9: Large dataset performance
bool test_kvstore_large_dataset() {
    TestDatabase db(generate_test_db_name("large"));
    auto kv_store = KVStore::open(db.name(), 10 * 1024); // 10KB memtable

    if (!kv_store) {
        std::cerr << "  Failed to open database" << std::endl;
        return false;
    }

    const int NUM_ENTRIES = 1000;
    const int VALUE_SIZE = 200; // bytes

    auto start = high_resolution_clock::now();

    // Insert large dataset
    for (int i = 0; i < NUM_ENTRIES; i++) {
        std::string key = "user:" + std::to_string(i);
        std::string value = "data:" + std::string(VALUE_SIZE, 'X');

        if (!kv_store->put(key, value)) {
            std::cerr << "  Failed to put entry " << i << std::endl;
            return false;
        }
    }

    auto insert_end = high_resolution_clock::now();

    // Verify all data
    for (int i = 0; i < NUM_ENTRIES; i++) {
        std::string key = "user:" + std::to_string(i);
        std::string expected = "data:" + std::string(VALUE_SIZE, 'X');

        auto value = kv_store->get(key);
        if (!value.has_value() || value.value() != expected) {
            std::cerr << "  Data mismatch for key: " << key << std::endl;
            return false;
        }
    }

    auto verify_end = high_resolution_clock::now();

    // Range scan performance
    auto scan_start = high_resolution_clock::now();
    auto results = kv_store->scan("user:0", "user:999");
    auto scan_end = high_resolution_clock::now();

    if (results.size() != NUM_ENTRIES) {
        std::cerr << "  Range scan missing entries: expected " << NUM_ENTRIES
                  << ", got " << results.size() << std::endl;
        return false;
    }

    auto insert_time = duration_cast<milliseconds>(insert_end - start);
    auto verify_time = duration_cast<milliseconds>(verify_end - insert_end);
    auto scan_time = duration_cast<milliseconds>(scan_end - scan_start);

    std::cout << "  Insert " << NUM_ENTRIES << " entries: " << insert_time.count() << "ms" << std::endl;
    std::cout << "  Verify all entries: " << verify_time.count() << "ms" << std::endl;
    std::cout << "  Range scan all entries: " << scan_time.count() << "ms" << std::endl;
    std::cout << "  Throughput: " << (NUM_ENTRIES * 1000.0 / insert_time.count()) << " ops/sec" << std::endl;

    // Check SST files were created
    size_t sst_count = 0;
    try {
        for (const auto& entry : fs::directory_iterator(db.name())) {
            if (entry.path().extension() == ".sst") {
                sst_count++;
            }
        }
    } catch (const fs::filesystem_error&) {
        // Ignore if we can't access directory
    }

    std::cout << "  Created " << sst_count << " SST files" << std::endl;

    auto stats = kv_store->get_stats();
    std::cout << "  Memtable flushes: " << stats.memtable_flushes << std::endl;

    kv_store->close();
    return true;
}

// Test 10: Edge cases and error conditions
bool test_kvstore_edge_cases() {
    // Test 1: Empty keys and values
    {
        TestDatabase db(generate_test_db_name("edge1"));
        auto kv_store = KVStore::open(db.name(), 1024 * 1024);

        if (!kv_store) {
            std::cerr << "  Failed to open database for empty test" << std::endl;
            return false;
        }

        // Empty key
        if (!kv_store->put("", "empty key value")) {
            std::cerr << "  Should allow empty key" << std::endl;
            return false;
        }

        auto value = kv_store->get("");
        if (!value.has_value() || value.value() != "empty key value") {
            std::cerr << "  Failed to retrieve empty key" << std::endl;
            return false;
        }

        // Empty value
        if (!kv_store->put("emptyval", "")) {
            std::cerr << "  Should allow empty value" << std::endl;
            return false;
        }

        value = kv_store->get("emptyval");
        if (!value.has_value() || !value.value().empty()) {
            std::cerr << "  Empty value not stored correctly" << std::endl;
            return false;
        }

        kv_store->close();
    }

    // Test 2: Very large values
    {
        TestDatabase db(generate_test_db_name("edge2"));
        auto kv_store = KVStore::open(db.name(), 1024 * 1024);

        if (!kv_store) {
            std::cerr << "  Failed to open database for large value test" << std::endl;
            return false;
        }

        std::string large_value(10000, 'X'); // 10KB value

        if (!kv_store->put("large", large_value)) {
            std::cerr << "  Should handle large values" << std::endl;
            return false;
        }

        auto value = kv_store->get("large");
        if (!value.has_value() || value.value() != large_value) {
            std::cerr << "  Large value corrupted" << std::endl;
            return false;
        }

        kv_store->close();
    }

    // Test 3: Non-existent database directory (should create it)
    {
        std::string db_name = "non_existent_dir/test_db";

        // Remove if exists
        try {
            fs::remove_all("non_existent_dir");
        } catch (...) {
            // Ignore errors
        }

        auto kv_store = KVStore::open(db_name, 1024 * 1024);
        if (!kv_store) {
            std::cerr << "  Should create non-existent directory" << std::endl;
            // Clean up if we created it
            try { fs::remove_all("non_existent_dir"); } catch (...) {}
            return false;
        }

        // Should be able to write
        if (!kv_store->put("test", "value")) {
            std::cerr << "  Should write to newly created directory" << std::endl;
            kv_store->close();
            try { fs::remove_all("non_existent_dir"); } catch (...) {}
            return false;
        }

        kv_store->close();

        // Clean up
        try { fs::remove_all("non_existent_dir"); } catch (...) {}
    }

    return true;
}

// Main test runner
int kvstore_tests_main() {
    std::cout << "\n=== KVStore Unit Tests ===" << std::endl;
    std::cout << "==========================" << std::endl;

    std::vector<std::pair<std::string, std::function<bool()>>> tests = {
        {"1. Basic operations", test_kvstore_basic_operations},
        {"2. Updates", test_kvstore_updates},
        {"3. Deletes", test_kvstore_deletes},
        {"4. Range scans", test_kvstore_range_scans},
        {"5. Memtable flushing", test_kvstore_memtable_flushing},
        {"6. WAL recovery", test_kvstore_wal_recovery},
        {"7. Concurrent simulation", test_kvstore_concurrent_simulation},
        {"8. Statistics", test_kvstore_statistics},
        {"9. Large dataset", test_kvstore_large_dataset},
        {"10. Edge cases", test_kvstore_edge_cases}
    };

    int passed = 0;
    const int total = static_cast<int>(tests.size());

    for (const auto& [name, test_func] : tests) {
        try {
            const bool result = test_func();
            print_test_result(name, result);
            if (result) passed++;
        } catch (const std::exception& e) {
            std::cout << "Exception: " << e.what() << std::endl;
        } catch (...) {
            std::cout << "Unknown exception" << std::endl;
        }
    }

    std::cout << "\n" << std::string(50, '=') << std::endl;
    std::cout << "Results: " << passed << "/" << total << " tests passed" << std::endl;

    if (passed == total) {
        std::cout << "\nAll KVStore tests passed!" << std::endl;
        return 0;
    } else {
        std::cout << "\nSome KVStore tests failed" << std::endl;
        return 1;
    }
}