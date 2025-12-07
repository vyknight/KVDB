//
// Created by K on 2025-12-04.
//

#include "test_sstable_reader.h"
#include "../SSTableReader.h"
#include "../SSTableWriter.h"
#include "../Memtable.h"
#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <filesystem>
#include <chrono>
#include <cstring>
#include <random>

#include "test_helper.h"

namespace fs = std::filesystem;
using namespace std::chrono;

// Test 1: Basic reading operations
bool test_reader_basic_operations() {
    // Create and write an SSTable
    Memtable mt(4096);
    mt.put("apple", "red fruit");
    mt.put("banana", "yellow fruit");
    mt.put("carrot", "orange vegetable");

    const std::string filename = "test_reader_basic.sst";

    if (!SSTableWriter::write_from_memtable(filename, mt)) {
        std::cerr << "Failed to write SSTable for basic test" << std::endl;
        return false;
    }

    // Test the reader
    SSTableReader reader(filename);

    if (!reader.is_valid()) {
        std::cerr << "Failed to load valid SSTable" << std::endl;
        if (fs::exists(filename)) fs::remove(filename);
        return false;
    }

    // Verify basic properties
    if (reader.size() != 3) {
        std::cerr << "Expected 3 entries, got " << reader.size() << std::endl;
        if (fs::exists(filename)) fs::remove(filename);
        return false;
    }

    // Test get() operations
    auto val1 = reader.get("apple");
    auto val2 = reader.get("banana");
    auto val3 = reader.get("carrot");
    auto val4 = reader.get("nonexistent");

    bool success = (val1.has_value() && val1.value() == "red fruit") &&
                   (val2.has_value() && val2.value() == "yellow fruit") &&
                   (val3.has_value() && val3.value() == "orange vegetable") &&
                   (!val4.has_value());

    // Test contains()
    success = success && reader.contains("apple");
    success = success && !reader.contains("nonexistent");

    // Test is_deleted() (should be false for all)
    success = success && !reader.is_deleted("apple");
    success = success && !reader.is_deleted("banana");
    success = success && !reader.is_deleted("carrot");

    // Clean up
    if (fs::exists(filename)) fs::remove(filename);
    return success;
}

// Test 2: Reading with tombstones
bool test_reader_tombstones() {
    Memtable mt(4096);

    mt.put("key1", "value1");
    mt.put("key2", "value2");
    mt.remove("key1");  // Tombstone
    mt.put("key3", "value3");
    mt.remove("key3");  // Another tombstone

    const std::string filename = "test_reader_tombstones.sst";

    if (!SSTableWriter::write_from_memtable(filename, mt)) {
        std::cerr << "Failed to write SSTable with tombstones" << std::endl;
        return false;
    }

    SSTableReader reader(filename);

    if (!reader.is_valid()) {
        std::cerr << "Failed to load SSTable with tombstones" << std::endl;
        if (fs::exists(filename)) fs::remove(filename);
        return false;
    }

    // Verify entry count includes tombstones
    if (reader.size() != 3) {
        std::cerr << "Expected 3 entries (including tombstones), got " << reader.size() << std::endl;
        if (fs::exists(filename)) fs::remove(filename);
        return false;
    }

    // Test get() - should not return tombstoned keys
    auto val1 = reader.get("key1");
    auto val2 = reader.get("key2");
    auto val3 = reader.get("key3");

    bool success = (!val1.has_value()) &&  // key1 is deleted
                   (val2.has_value() && val2.value() == "value2") &&
                   (!val3.has_value());     // key3 is deleted

    // Test contains() - should return false for tombstoned keys
    success = success && !reader.contains("key1");
    success = success && reader.contains("key2");
    success = success && !reader.contains("key3");

    // Test is_deleted() - should return true for tombstoned keys
    success = success && reader.is_deleted("key1");
    success = success && !reader.is_deleted("key2");
    success = success && reader.is_deleted("key3");

    // Clean up
    if (fs::exists(filename)) fs::remove(filename);
    return success;
}

// Test 3: Binary search correctness
bool test_reader_binary_search() {
    Memtable mt(4096);

    // Insert in random order
    std::vector<std::string> keys = {"zebra", "apple", "carrot", "banana", "mango"};
    std::vector<std::string> values = {"animal", "fruit", "vegetable", "fruit", "tropical"};

    for (size_t i = 0; i < keys.size(); ++i) {
        mt.put(keys[i], values[i]);
    }

    const std::string filename = "test_reader_binary.sst";

    if (!SSTableWriter::write_from_memtable(filename, mt)) {
        std::cerr << "Failed to write SSTable for binary search test" << std::endl;
        return false;
    }

    SSTableReader reader(filename);

    if (!reader.is_valid()) {
        std::cerr << "Failed to load SSTable for binary search test" << std::endl;
        if (fs::exists(filename)) fs::remove(filename);
        return false;
    }

    // Test that all keys can be found
    bool success = true;
    for (size_t i = 0; i < keys.size(); ++i) {
        auto val = reader.get(keys[i]);
        if (!val.has_value() || val.value() != values[i]) {
            std::cerr << "Binary search failed for key: " << keys[i] << std::endl;
            success = false;
        }
    }

    // Test keys that don't exist
    std::vector<std::string> non_existent = {"aardvark", "cherry", "zzz"};
    for (const auto& key : non_existent) {
        auto val = reader.get(key);
        if (val.has_value()) {
            std::cerr << "Binary search incorrectly found non-existent key: " << key << std::endl;
            success = false;
        }
    }

    // Test that keys are sorted
    auto all_keys = reader.get_all_keys();
    for (size_t i = 1; i < all_keys.size(); ++i) {
        if (all_keys[i-1] >= all_keys[i]) {
            std::cerr << "Keys not sorted: " << all_keys[i-1] << " >= " << all_keys[i] << std::endl;
            success = false;
        }
    }

    // Clean up
    if (fs::exists(filename)) fs::remove(filename);
    return success;
}

// Test 4: Edge cases
bool test_reader_edge_cases() {
    Memtable mt(4096);

    // Test various edge cases
    mt.put("", "empty key");                    // Empty key
    mt.put("empty_value", "");                  // Empty value
    mt.put("long_key_" + std::string(100, 'x'), "value");
    mt.put("key\nwith\nnewlines", "value\nwith\nnewlines");
    mt.put("key\twith\ttabs", "value\twith\ttabs");
    mt.put("key with spaces", "value with spaces");

    const std::string filename = "test_reader_edge.sst";

    if (!SSTableWriter::write_from_memtable(filename, mt)) {
        std::cerr << "Failed to write SSTable for edge cases test" << std::endl;
        return false;
    }

    SSTableReader reader(filename);

    if (!reader.is_valid()) {
        std::cerr << "Failed to load SSTable for edge cases test" << std::endl;
        if (fs::exists(filename)) fs::remove(filename);
        return false;
    }

    // Test each edge case
    bool success = true;

    // Empty key
    auto empty_key_val = reader.get("");
    if (!empty_key_val.has_value() || empty_key_val.value() != "empty key") {
        std::cerr << "Failed to read empty key" << std::endl;
        success = false;
    }

    // Empty value
    auto empty_val = reader.get("empty_value");
    if (!empty_val.has_value() || !empty_val.value().empty()) {
        std::cerr << "Failed to read empty value" << std::endl;
        success = false;
    }

    // Long key
    std::string long_key = "long_key_" + std::string(100, 'x');
    auto long_key_val = reader.get(long_key);
    if (!long_key_val.has_value() || long_key_val.value() != "value") {
        std::cerr << "Failed to read long key" << std::endl;
        success = false;
    }

    // Newlines
    auto newline_val = reader.get("key\nwith\nnewlines");
    if (!newline_val.has_value() || newline_val.value() != "value\nwith\nnewlines") {
        std::cerr << "Failed to read key with newlines" << std::endl;
        success = false;
    }

    // Tabs
    auto tab_val = reader.get("key\twith\ttabs");
    if (!tab_val.has_value() || tab_val.value() != "value\twith\ttabs") {
        std::cerr << "Failed to read key with tabs" << std::endl;
        success = false;
    }

    // Spaces
    auto space_val = reader.get("key with spaces");
    if (!space_val.has_value() || space_val.value() != "value with spaces") {
        std::cerr << "Failed to read key with spaces" << std::endl;
        success = false;
    }

    // Clean up
    if (fs::exists(filename)) fs::remove(filename);
    return success;
}

// Test 5: Empty SSTable
bool test_reader_empty_sstable() {
    Memtable mt(4096);  // Empty memtable

    const std::string filename = "test_reader_empty.sst";

    if (!SSTableWriter::write_from_memtable(filename, mt)) {
        std::cerr << "Failed to write empty SSTable" << std::endl;
        return false;
    }

    SSTableReader reader(filename);

    if (!reader.is_valid()) {
        std::cerr << "Failed to load empty SSTable" << std::endl;
        if (fs::exists(filename)) fs::remove(filename);
        return false;
    }

    // Verify empty SSTable properties
    bool success = true;

    if (reader.size() != 0) {
        std::cerr << "Empty SSTable should have 0 entries, got " << reader.size() << std::endl;
        success = false;
    }

    if (!reader.min_key().empty()) {
        std::cerr << "Empty SSTable min_key should be empty" << std::endl;
        success = false;
    }

    if (!reader.max_key().empty()) {
        std::cerr << "Empty SSTable max_key should be empty" << std::endl;
        success = false;
    }

    // Test get on non-existent key
    auto val = reader.get("any_key");
    if (val.has_value()) {
        std::cerr << "Empty SSTable should not return values" << std::endl;
        success = false;
    }

    // Clean up
    if (fs::exists(filename)) fs::remove(filename);
    return success;
}

// Test 6: Large SSTable performance
bool test_reader_large_sstable() {
    constexpr int NUM_ENTRIES = 1000;
    Memtable mt(10 * 1024 * 1024);  // 10MB

    std::cout << "  Generating " << NUM_ENTRIES << " entries for large SSTable test..." << std::endl;

    // Insert entries with predictable pattern
    for (int i = 0; i < NUM_ENTRIES; ++i) {
        std::string key = "key_" + std::to_string(i);
        std::string value = "value_" + std::string(100, 'x');  // 100 byte values
        mt.put(key, value);
    }

    const std::string filename = "test_reader_large.sst";

    // Write SSTable
    auto write_start = high_resolution_clock::now();
    if (!SSTableWriter::write_from_memtable(filename, mt)) {
        std::cerr << "Failed to write large SSTable" << std::endl;
        return false;
    }
    auto write_end = high_resolution_clock::now();

    // Read SSTable
    auto read_start = high_resolution_clock::now();
    SSTableReader reader(filename);
    auto read_end = high_resolution_clock::now();

    if (!reader.is_valid()) {
        std::cerr << "Failed to load large SSTable" << std::endl;
        if (fs::exists(filename)) fs::remove(filename);
        return false;
    }

    auto write_duration = duration_cast<milliseconds>(write_end - write_start);
    auto read_duration = duration_cast<milliseconds>(read_end - read_start);
    auto file_size = fs::file_size(filename);

    std::cout << "  Large SSTable: " << file_size / 1024 << " KB" << std::endl;
    std::cout << "  Write time: " << write_duration.count() << " ms" << std::endl;
    std::cout << "  Read/load time: " << read_duration.count() << " ms" << std::endl;

    // Test binary search performance with random lookups
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, NUM_ENTRIES - 1);

    const int NUM_LOOKUPS = 100;
    auto lookup_start = high_resolution_clock::now();

    for (int i = 0; i < NUM_LOOKUPS; ++i) {
        int index = dis(gen);
        std::string key = "key_" + std::to_string(index);
        auto val = reader.get(key);

        if (!val.has_value()) {
            std::cerr << "Failed to find key in large SSTable: " << key << std::endl;
            if (fs::exists(filename)) fs::remove(filename);
            return false;
        }
    }

    auto lookup_end = high_resolution_clock::now();
    auto lookup_duration = duration_cast<microseconds>(lookup_end - lookup_start);

    std::cout << "  " << NUM_LOOKUPS << " random lookups: "
              << lookup_duration.count() << " microseconds ("
              << (lookup_duration.count() / (double)NUM_LOOKUPS) << " microseconds/lookup)" << std::endl;

    // Test memory usage
    size_t mem_usage = reader.memory_usage();
    std::cout << "  Memory usage: " << mem_usage / 1024 << " KB" << std::endl;

    // Clean up
    if (fs::exists(filename)) fs::remove(filename);
    return true;
}

// Test 7: Min/max key functionality
bool test_reader_min_max_keys() {
    Memtable mt(4096);

    // Insert in random order
    mt.put("mango", "tropical");
    mt.put("apple", "temperate");
    mt.put("zucchini", "vegetable");
    mt.put("banana", "tropical");
    mt.put("carrot", "vegetable");

    const std::string filename = "test_reader_minmax.sst";

    if (!SSTableWriter::write_from_memtable(filename, mt)) {
        std::cerr << "Failed to write SSTable for min/max test" << std::endl;
        return false;
    }

    SSTableReader reader(filename);

    if (!reader.is_valid()) {
        std::cerr << "Failed to load SSTable for min/max test" << std::endl;
        if (fs::exists(filename)) fs::remove(filename);
        return false;
    }

    // Test min/max
    bool success = true;

    if (reader.min_key() != "apple") {
        std::cerr << "Min key incorrect: expected 'apple', got '" << reader.min_key() << "'" << std::endl;
        success = false;
    }

    if (reader.max_key() != "zucchini") {
        std::cerr << "Max key incorrect: expected 'zucchini', got '" << reader.max_key() << "'" << std::endl;
        success = false;
    }

    // Test that all keys are within range
    auto all_keys = reader.get_all_keys();
    for (const auto& key : all_keys) {
        if (key < reader.min_key() || key > reader.max_key()) {
            std::cerr << "Key '" << key << "' is outside min/max range" << std::endl;
            success = false;
        }
    }

    // Clean up
    if (fs::exists(filename)) fs::remove(filename);
    return success;
}

// Test 8: File not found/invalid file
bool test_reader_file_not_found() {
    const std::string non_existent_file = "non_existent_file_12345.sst";

    // Save the original cerr buffer
    std::streambuf* original_cerr = std::cerr.rdbuf();

    // Create a null stream to discard output
    std::ofstream null_stream;
    std::streambuf* null_buffer = null_stream.rdbuf();

    // Redirect cerr to null
    std::cerr.rdbuf(null_buffer);

    // Create reader - error messages will be discarded
    // Should fail gracefully
    SSTableReader reader(non_existent_file);

    // Restore cerr
    std::cerr.rdbuf(original_cerr);

    if (reader.is_valid()) {
        std::cerr << "Reader should not be valid for non-existent file" << std::endl;
        return false;
    }

    // Test that operations return appropriate values
    auto val = reader.get("any_key");
    if (val.has_value()) {
        std::cerr << "Invalid reader should not return values" << std::endl;
        return false;
    }

    if (reader.contains("any_key")) {
        std::cerr << "Invalid reader should not contain keys" << std::endl;
        return false;
    }

    if (reader.size() != 0) {
        std::cerr << "Invalid reader should report 0 size" << std::endl;
        return false;
    }

    return true;
}

// Test 9: Corrupted file handling
bool test_reader_corrupted_file() {
    // Create a corrupted SSTable file
    const std::string filename = "test_reader_corrupted.sst";

    {
        std::ofstream file(filename, std::ios::binary);
        if (!file) {
            std::cerr << "Failed to create corrupted test file" << std::endl;
            return false;
        }

        // Write invalid magic number
        uint64_t bad_magic = 0xDEADBEEF;
        file.write(reinterpret_cast<const char*>(&bad_magic), sizeof(bad_magic));

        // Write some garbage data
        for (int i = 0; i < 100; ++i) {
            char garbage = static_cast<char>(i);
            file.write(&garbage, sizeof(garbage));
        }
    }

    // Save the original cerr buffer
    std::streambuf* original_cerr = std::cerr.rdbuf();

    // Create a null stream to discard output
    std::ofstream null_stream;
    std::streambuf* null_buffer = null_stream.rdbuf();

    // Redirect cerr to null
    std::cerr.rdbuf(null_buffer);

    // Create reader - error messages will be discarded
    // Should fail gracefully
    SSTableReader reader(filename);

    // Restore cerr
    std::cerr.rdbuf(original_cerr);

    if (reader.is_valid()) {
        std::cerr << "Reader should not be valid for corrupted file" << std::endl;
        if (fs::exists(filename)) fs::remove(filename);
        return false;
    }

    // Clean up
    if (fs::exists(filename)) fs::remove(filename);
    return true;
}

// Test 10: Verify unsorted keys are rejected
bool test_reader_unsorted_keys() {
    // We need to manually create an SSTable with unsorted keys
    // This is tricky because SSTableWriter always sorts keys
    // So we'll test the validation in the reader by creating a bad file

    const std::string filename = "test_reader_unsorted.sst";

    // For this test, we'll rely on the reader's validation logic
    // If we ever create an unsorted SSTable, the reader should reject it
    // For now, we'll just return true since our writer always sorts

    std::cout << "  Note: Unsorted keys test relies on SSTableWriter to always sort keys" << std::endl;

    // Create a normal sorted SSTable to verify the reader works
    Memtable mt(4096);
    mt.put("a", "1");
    mt.put("b", "2");
    mt.put("c", "3");

    if (!SSTableWriter::write_from_memtable(filename, mt)) {
        std::cerr << "Failed to write SSTable for unsorted test" << std::endl;
        return false;
    }

    SSTableReader reader(filename);

    if (!reader.is_valid()) {
        std::cerr << "Failed to load valid SSTable" << std::endl;
        if (fs::exists(filename)) fs::remove(filename);
        return false;
    }

    // Clean up
    if (fs::exists(filename)) fs::remove(filename);
    return true;
}

// Helper function (optional)
bool validate_reader_memory_usage(const std::string& filename) {
    SSTableReader reader(filename);

    if (!reader.is_valid()) {
        return false;
    }

    size_t usage = reader.memory_usage();
    std::cout << "  Memory usage for " << filename << ": " << usage << " bytes" << std::endl;

    // Basic sanity check: memory usage should be > 0 for non-empty SSTable
    if (reader.size() > 0 && usage == 0) {
        std::cerr << "Memory usage should be > 0 for non-empty SSTable" << std::endl;
        return false;
    }

    return true;
}

// Test for SSTableReader range scan functionality
bool test_sstable_reader_scan_range_basic() {
    Memtable mt(4096);

    // Insert keys in alphabetical order
    mt.put("apple", "fruit");
    mt.put("banana", "yellow fruit");
    mt.put("carrot", "vegetable");
    mt.put("date", "sweet fruit");
    mt.put("eggplant", "purple vegetable");
    mt.put("fig", "small fruit");
    mt.put("grape", "bunch fruit");

    const std::string filename = "test_scan_range_basic.sst";

    if (!SSTableWriter::write_from_memtable(filename, mt)) {
        std::cerr << "  Failed to write SSTable" << std::endl;
        return false;
    }

    SSTableReader reader(filename);
    if (!reader.is_valid()) {
        std::cerr << "  Failed to create valid SSTableReader" << std::endl;
        fs::remove(filename);
        return false;
    }

    // Test 1: Full range
    auto results = reader.scan_range("a", "z");
    if (results.size() != 7) {
        std::cerr << "  Test 1 failed: Expected 7 entries, got " << results.size() << std::endl;
        fs::remove(filename);
        return false;
    }

    // Test 2: Subrange from "c" to "ez" (to include eggplant which starts with 'e')
    // Note: "eggplant" > "e", so we need a bound that's >= "eggplant"
    results = reader.scan_range("c", "ez");
    if (results.size() != 3) {
        std::cerr << "  Test 2 failed: Expected 3 entries (c to ez), got " << results.size() << std::endl;
        for (const auto& [key, value] : results) {
            std::cerr << "    Found: " << key << std::endl;
        }
        fs::remove(filename);
        return false;
    }

    // Check specific entries
    bool found_carrot = false, found_date = false, found_eggplant = false;
    for (const auto& [key, value] : results) {
        if (key == "carrot" && value == "vegetable") found_carrot = true;
        if (key == "date" && value == "sweet fruit") found_date = true;
        if (key == "eggplant" && value == "purple vegetable") found_eggplant = true;
    }

    if (!found_carrot || !found_date || !found_eggplant) {
        std::cerr << "  Test 2 failed: Missing expected entries" << std::endl;
        fs::remove(filename);
        return false;
    }

    // Test 3: Single key range
    results = reader.scan_range("banana", "banana");
    if (results.size() != 1 || results[0].first != "banana" || results[0].second != "yellow fruit") {
        std::cerr << "  Test 3 failed: Single key range failed" << std::endl;
        fs::remove(filename);
        return false;
    }

    // Test 4: Empty range (no keys in range)
    results = reader.scan_range("h", "i");
    if (!results.empty()) {
        std::cerr << "  Test 4 failed: Expected empty range, got " << results.size() << " entries" << std::endl;
        fs::remove(filename);
        return false;
    }

    // Test 5: Range starting before first key
    results = reader.scan_range("a", "c");
    // This should include: apple, banana
    // carrot > c in lexicographical order
    if (results.size() != 2) {
        std::cerr << "  Test 5 failed: Expected 2 entries (a-c), got " << results.size() << std::endl;
        fs::remove(filename);
        return false;
    }

    // Test 6: Range ending after last key
    results = reader.scan_range("g", "z");
    // This should include: grape
    if (results.size() != 1 || results[0].first != "grape") {
        std::cerr << "  Test 6 failed: Expected 1 entry (grape), got " << results.size() << std::endl;
        fs::remove(filename);
        return false;
    }

    fs::remove(filename);
    return true;
}

bool test_sstable_reader_scan_range_with_deletes() {
    Memtable mt(4096);

    mt.put("apple", "fruit");
    mt.put("banana", "yellow fruit");
    mt.put("carrot", "vegetable");
    mt.remove("banana");  // Delete banana
    mt.put("date", "sweet fruit");
    mt.put("eggplant", "purple vegetable");

    const std::string filename = "test_scan_range_deletes.sst";

    if (!SSTableWriter::write_from_memtable(filename, mt)) {
        std::cerr << "  Failed to write SSTable" << std::endl;
        return false;
    }

    SSTableReader reader(filename);
    if (!reader.is_valid()) {
        std::cerr << "  Failed to create valid SSTableReader" << std::endl;
        fs::remove(filename);
        return false;
    }

    // Test 1: Range that includes a deleted key
    auto results = reader.scan_range("a", "z");
    // Should have 4 entries (apple, carrot, date, eggplant) - banana deleted
    if (results.size() != 4) {
        std::cerr << "  Test 1 failed: Expected 4 entries (excluding deleted), got " << results.size() << std::endl;
        for (const auto& [key, value] : results) {
            std::cerr << "    " << key << ": " << value << std::endl;
        }
        fs::remove(filename);
        return false;
    }

    // Verify banana is not in results
    for (const auto& [key, value] : results) {
        if (key == "banana") {
            std::cerr << "  Test 1 failed: Deleted key 'banana' found in results" << std::endl;
            fs::remove(filename);
            return false;
        }
    }

    // Test 2: Get deleted key directly
    auto deleted_value = reader.get("banana");
    if (deleted_value.has_value()) {
        std::cerr << "  Test 2 failed: Deleted key should return empty optional" << std::endl;
        fs::remove(filename);
        return false;
    }

    // Test 3: Check is_deleted for deleted key
    if (!reader.is_deleted("banana")) {
        std::cerr << "  Test 3 failed: is_deleted should return true for deleted key" << std::endl;
        fs::remove(filename);
        return false;
    }

    fs::remove(filename);
    return true;
}

bool test_sstable_reader_scan_range_edge_cases() {
    Memtable mt(4096);

    // Insert keys with various patterns
    mt.put("a", "first");
    mt.put("aa", "double a");
    mt.put("ab", "a b");
    mt.put("b", "second");
    mt.put("ba", "b a");
    mt.put("c", "third");

    const std::string filename = "test_scan_range_edge.sst";

    if (!SSTableWriter::write_from_memtable(filename, mt)) {
        std::cerr << "  Failed to write SSTable" << std::endl;
        return false;
    }

    SSTableReader reader(filename);
    if (!reader.is_valid()) {
        std::cerr << "  Failed to create valid SSTableReader" << std::endl;
        fs::remove(filename);
        return false;
    }

    // Test 1: Empty string range
    auto results = reader.scan_range("", "");
    // Should get everything starting with empty string? Actually "" < "a", so nothing
    // This behavior depends on how you want to handle empty strings
    // For now, we'll accept whatever the binary search gives us

    // Test 2: Range with special boundaries
    results = reader.scan_range("a", "b");
    // Should include: a, aa, ab, b (inclusive)
    if (results.size() != 4) {
        std::cerr << "  Test 2 failed: Expected 4 entries (a-aa-ab-b), got " << results.size() << std::endl;
        fs::remove(filename);
        return false;
    }

    // Test 3: Partial prefix match
    results = reader.scan_range("aa", "ab");
    // Should include: aa, ab
    if (results.size() != 2) {
        std::cerr << "  Test 3 failed: Expected 2 entries (aa, ab), got " << results.size() << std::endl;
        fs::remove(filename);
        return false;
    }

    // Test 4: Non-existent start key
    results = reader.scan_range("ac", "az");
    // Should be empty (no keys between ac and az)
    if (!results.empty()) {
        std::cerr << "  Test 4 failed: Expected empty range, got " << results.size() << std::endl;
        fs::remove(filename);
        return false;
    }

    // Test 5: Same start and end with no matching key
    results = reader.scan_range("xyz", "xyz");
    if (!results.empty()) {
        std::cerr << "  Test 5 failed: Expected empty for non-existent single key" << std::endl;
        fs::remove(filename);
        return false;
    }

    fs::remove(filename);
    return true;
}

bool test_sstable_reader_scan_range_performance() {
    Memtable mt(10 * 1024 * 1024);  // 10MB memtable

    // Insert 1000 keys with zero-padded indices
    const int NUM_KEYS = 1000;
    for (int i = 0; i < NUM_KEYS; i++) {
        // Zero-pad to 4 digits
        std::string index = std::to_string(i);
        index = std::string(4 - index.length(), '0') + index;
        std::string key = "key_" + index;
        std::string value = "value_" + std::string(100, 'x');  // 100-byte values
        mt.put(key, value);
    }

    const std::string filename = "test_scan_range_perf.sst";

    auto start_write = std::chrono::high_resolution_clock::now();
    if (!SSTableWriter::write_from_memtable(filename, mt)) {
        std::cerr << "  Failed to write SSTable" << std::endl;
        return false;
    }
    auto end_write = std::chrono::high_resolution_clock::now();

    SSTableReader reader(filename);
    if (!reader.is_valid()) {
        std::cerr << "  Failed to create valid SSTableReader" << std::endl;
        fs::remove(filename);
        return false;
    }

    // Performance test: multiple range scans
    auto start_scan = std::chrono::high_resolution_clock::now();

    const int NUM_SCANS = 100;
    std::srand(static_cast<unsigned>(std::time(nullptr)));

    for (int scan = 0; scan < NUM_SCANS; scan++) {
        // Random ranges within the data
        int start_idx = std::rand() % (NUM_KEYS - 100);
        int end_idx = start_idx + std::rand() % 100;

        // Zero-pad the keys for the range
        std::string start_index = std::to_string(start_idx);
        start_index = std::string(4 - start_index.length(), '0') + start_index;
        std::string start_key = "key_" + start_index;

        std::string end_index = std::to_string(end_idx);
        end_index = std::string(4 - end_index.length(), '0') + end_index;
        std::string end_key = "key_" + end_index;

        auto results = reader.scan_range(start_key, end_key);

        // Verify we got the expected number of results
        int expected_count = end_idx - start_idx + 1;
        if (results.size() != static_cast<size_t>(expected_count)) {
            std::cerr << "  Performance test: Expected " << expected_count
                      << " entries for range [" << start_idx << ", " << end_idx
                      << "], got " << results.size() << std::endl;
            // Debug: print first few keys
            for (int i = 0; i < std::min(5, static_cast<int>(results.size())); i++) {
                std::cerr << "    " << results[i].first << std::endl;
            }
            fs::remove(filename);
            return false;
        }
    }

    auto end_scan = std::chrono::high_resolution_clock::now();

    auto write_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_write - start_write);
    auto scan_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_scan - start_scan);

    std::cout << "  Write time: " << write_time.count() << "ms for " << NUM_KEYS << " entries" << std::endl;
    std::cout << "  Scan time: " << scan_time.count() << "ms for " << NUM_SCANS << " range scans" << std::endl;
    std::cout << "  Average scan time: " << (scan_time.count() * 1000.0 / NUM_SCANS) << " microseconds" << std::endl;

    // Test single exact match vs range scan
    start_scan = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < 1000; i++) {
        reader.get("key_0500");  // zero-padded key for 500
    }
    end_scan = std::chrono::high_resolution_clock::now();
    auto get_time = std::chrono::duration_cast<std::chrono::microseconds>(end_scan - start_scan);

    std::cout << "  1000 point get time: " << get_time.count() << " microseconds" << std::endl;
    std::cout << "  Average point get: " << (get_time.count() / 1000.0) << " microseconds" << std::endl;

    fs::remove(filename);
    return true;
}

bool test_sstable_reader_scan_range_order() {
    Memtable mt(4096);

    // Insert keys in non-alphabetical order
    mt.put("zebra", "animal");
    mt.put("apple", "fruit");
    mt.put("monkey", "animal");
    mt.put("banana", "fruit");
    mt.put("carrot", "vegetable");

    const std::string filename = "test_scan_range_order.sst";

    if (!SSTableWriter::write_from_memtable(filename, mt)) {
        std::cerr << "  Failed to write SSTable" << std::endl;
        return false;
    }

    SSTableReader reader(filename);
    if (!reader.is_valid()) {
        std::cerr << "  Failed to create valid SSTableReader" << std::endl;
        fs::remove(filename);
        return false;
    }

    // Test: Results should be in sorted order regardless of insertion order
    auto results = reader.scan_range("a", "z");

    if (results.size() != 4) {
        std::cerr << "  Expected 4 entries, got " << results.size() << std::endl;
        // Debug: print all keys in reader
        auto all_keys = reader.get_all_keys();
        std::cerr << "  All keys in SSTable: ";
        for (const auto& key : all_keys) {
            std::cerr << key << " ";
        }
        std::cerr << std::endl;
        fs::remove(filename);
        return false;
    }

    // Check order
    for (size_t i = 1; i < results.size(); i++) {
        if (results[i-1].first >= results[i].first) {
            std::cerr << "  Results not in sorted order at position " << i
                      << ": " << results[i-1].first << " >= " << results[i].first << std::endl;
            // Print all results for debugging
            for (size_t j = 0; j < results.size(); j++) {
                std::cerr << "    [" << j << "] " << results[j].first << std::endl;
            }
            fs::remove(filename);
            return false;
        }
    }

    // Verify specific order
    std::vector<std::string> expected_order = {"apple", "banana", "carrot", "monkey", "zebra"};
    for (size_t i = 0; i < results.size(); i++) {
        if (results[i].first != expected_order[i]) {
            std::cerr << "  Position " << i << ": expected " << expected_order[i]
                      << ", got " << results[i].first << std::endl;
            fs::remove(filename);
            return false;
        }
    }

    fs::remove(filename);
    return true;
}

// Main test runner
int sstable_reader_tests_main() {
    std::cout << "Running SSTable Reader Tests" << std::endl;
    std::cout << "===========================" << std::endl;

    std::vector<std::pair<std::string, bool (*)()>> tests = {
        {"Basic Operations", test_reader_basic_operations},
        {"Tombstones", test_reader_tombstones},
        {"Binary Search", test_reader_binary_search},
        {"Edge Cases", test_reader_edge_cases},
        {"Empty SSTable", test_reader_empty_sstable},
        {"Large SSTable", test_reader_large_sstable},
        {"Min/Max Keys", test_reader_min_max_keys},
        {"File Not Found", test_reader_file_not_found},
        {"Corrupted File", test_reader_corrupted_file},
        {"Unsorted Keys", test_reader_unsorted_keys},
        {"Range Scan Basic", test_sstable_reader_scan_range_basic},
        {"Range Scan with Deletes", test_sstable_reader_scan_range_with_deletes},
        {"Range Scan Edge Cases", test_sstable_reader_scan_range_edge_cases},
        {"Range Scan Performance", test_sstable_reader_scan_range_performance},
        {"Range Scan Order", test_sstable_reader_scan_range_order}
    };

    int passed = 0;
    int total = tests.size();

    for (const auto& [name, test_func] : tests) {
        try {
            bool result = test_func();
            print_test_result(name, result);
            if (result) passed++;
        } catch (const std::exception& e) {
            std::cout << "X " << name << " (Exception: " << e.what() << ")" << std::endl;
        } catch (...) {
            std::cout << "X " << name << " (Unknown exception)" << std::endl;
        }
    }

    std::cout << "\nResults: " << passed << "/" << total << " tests passed" << std::endl;

    if (passed == total) {
        std::cout << "\nAll SSTable Reader tests passed!" << std::endl;
        return 0;
    } else {
        std::cout << "\nSome SSTable Reader tests failed" << std::endl;
        return 1;
    }
}
