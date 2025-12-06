//
// Created by K on 2025-12-04.
//

#include "test_sstable_writer.h"
#include "../Memtable.h"
#include "../SSTableWriter.h"
#include "../SSTableReader.h"
#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <filesystem>
#include <chrono>

namespace fs = std::filesystem;
using namespace std::chrono;

void print_test_result_sstable_writer(const std::string& test_name, const bool passed)
{
    std::cout << (passed ? "O " : "X ") << test_name << std::endl;
}

// Test 1: Basic write
bool test_sstable_write_basic()
{
    Memtable mt(4096);

    mt.put("apple", "red fruit");
    mt.put("banana", "yellow fruit");
    mt.put("carrot", "orange vegetable");

    const std::string filename = "test_basic.sst";

    if (!SSTableWriter::write_from_memtable(filename, mt))
    {
        std::cerr << "SSTableWriter::write_from_memtable() failed" << std::endl;
        return false;
    }

    if (!fs::exists(filename))
    {
        std::cerr << "SST file not created" << std::endl;
        return false;
    }

    // verify file has approx correct size
    const auto size = fs::file_size(filename);
    if (size == 0)
    {
        std::cerr << "SST File size is zero" << std::endl;
        fs::remove(filename);
        return false;
    }

    fs::remove(filename);
    return true;
}

// Test 2: empty memtable case
bool test_sstable_write_empty_memtable()
{
    const Memtable mt(4096);

    const std::string filename = "test_empty.sst";

    if (!SSTableWriter::write_from_memtable(filename, mt))
    {
        std::cerr << "Failed to write empty table" << std::endl;
        return false;
    }

    if (!fs::exists(filename)) {
        std::cerr << "Empty SSTable file not created" << std::endl;
        return false;
    }

    // should still have header
    const auto size = fs::file_size(filename);
    if (size < 24)  // header size, see SSTableWriter.h/.cpp for format
    {
        std::cerr << "Empty SSTable too small: " << size << " bytes" << std::endl;
        fs::remove(filename);
        return false;
    }

    fs::remove(filename);
    return true;
}

// Test 3: write with tombstones
bool test_sstable_write_with_tombstones()
{
    Memtable mt(4096);

    mt.put("key1", "value1");
    mt.put("key2", "value2");
    mt.remove("key1");  // Tombstone
    mt.put("key3", "value3");
    mt.remove("key3");  // Another tombstone

    const std::string filename = "test_tombstone.sst";

    if (!SSTableWriter::write_from_memtable(filename, mt))
    {
        std::cerr << "Failed to write SSTable with tombstones" << std::endl;
        return false;
    }

    if (!fs::exists(filename))
    {
        std::cerr << "SSTable with tombstone file not created" << std::endl;
        return false;
    }

    const auto size = fs::file_size(filename);
    if (size == 0)
    {
        std::cerr << "SSTable with tombstone File size is zero" << std::endl;
        if (fs::exists(filename)) fs::remove(filename);
        return false;
    }

    // should have 3 entries
    uint32_t entry_count = 0;
    {
        std::ifstream file(filename, std::ios::binary);
        if (!file)
        {
            std::cerr << "Unable to open SSTable with tombstone file" << std::endl;
            if (fs::exists(filename)) fs::remove(filename);
            return false;
        }

        // read header
        file.seekg(8+4);  // skip magic and version
        file.read(reinterpret_cast<char*>(&entry_count), sizeof(entry_count));
    }  // file closes here

    if (fs::exists(filename)) fs::remove(filename);
    return entry_count == 3;
}

// Test 4: write large data
bool test_sstable_write_large_data()
{
    Memtable mt(10 * 1024 * 1024);  // 10 MB

    // fill table
    for (int i = 0; i < 500; i++)
    {
        std::string key = "key_" + std::to_string(i);
        std::string value(1000, 'x');  // 1 KB
        mt.put(key, value);
    }

    const std::string filename = "test_large.sst";
    const auto start = high_resolution_clock::now();

    bool success = SSTableWriter::write_from_memtable(filename, mt);

    const auto end = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(end - start);

    if (!success || !fs::exists(filename))
    {
        std::cerr << "Failed to write large SSTable file" << std::endl;
        return false;
    }

    const auto size = fs::file_size(filename);
    std::cout << "  Large SSTable written, size: " << size << " bytes (" << size / 1024 << " KB) in "
        << duration.count() << "ms" << std::endl;

    fs::remove(filename);
    return true;
}

// Test 5: verify entries are written in sorted order
bool test_sstable_write_sorted_order()
{
    Memtable mt(4096);

    // Insert out of order
    mt.put("zebra", "animal");
    mt.put("apple", "fruit");
    mt.put("carrot", "vegetable");
    mt.put("banana", "fruit");

    const std::string filename = "test_sorted_order.sst";

    if (!SSTableWriter::write_from_memtable(filename, mt))
    {
        return false;
    }

    // Use SSTableReader to verify the order
    SSTableReader reader(filename);

    if (!reader.is_valid()) {
        std::cerr << "Failed to load SSTable for sorted order verification" << std::endl;
        fs::remove(filename);
        return false;
    }

    // Get all keys
    auto keys = reader.get_all_keys();

    // Should be sorted
    bool sorted = true;
    for (size_t i = 1; i < keys.size(); ++i) {
        if (keys[i - 1] >= keys[i]) {
            std::cerr << "Keys not sorted: '" << keys[i-1] << "' >= '" << keys[i] << "'" << std::endl;
            sorted = false;
        }
    }

    // Verify specific order: apple, banana, carrot, zebra
    if (keys.size() == 4) {
        if (keys[0] != "apple" || keys[1] != "banana" ||
            keys[2] != "carrot" || keys[3] != "zebra") {
            std::cerr << "Keys not in expected order" << std::endl;
            sorted = false;
        }
    }

    // Also verify values are correct
    bool values_correct = true;
    auto apple_val = reader.get("apple");
    if (!apple_val.has_value() || apple_val.value() != "fruit") {
        std::cerr << "Apple value incorrect" << std::endl;
        values_correct = false;
    }

    auto zebra_val = reader.get("zebra");
    if (!zebra_val.has_value() || zebra_val.value() != "animal") {
        std::cerr << "Zebra value incorrect" << std::endl;
        values_correct = false;
    }

    fs::remove(filename);
    return sorted && values_correct;
}

// Test 6: verify file format
bool test_sstable_write_file_verification()
{
    Memtable mt(4096);

    mt.put("test1", "value1");
    mt.put("test2", "value2");

    const std::string filename = "test_verify.sst";

    if (!SSTableWriter::write_from_memtable(filename, mt))
    {
        return false;
    }

    // Use SSTableReader to verify the file
    SSTableReader reader(filename);

    if (!reader.is_valid()) {
        std::cerr << "Failed to load SSTable for verification" << std::endl;
        fs::remove(filename);
        return false;
    }

    // Verify basic properties
    if (reader.size() != 2) {
        std::cerr << "Expected 2 entries, got " << reader.size() << std::endl;
        fs::remove(filename);
        return false;
    }

    // Verify keys exist and have correct values
    auto val1 = reader.get("test1");
    auto val2 = reader.get("test2");

    bool success = true;

    if (!val1.has_value() || val1.value() != "value1") {
        std::cerr << "Key 'test1' has incorrect value" << std::endl;
        success = false;
    }

    if (!val2.has_value() || val2.value() != "value2") {
        std::cerr << "Key 'test2' has incorrect value" << std::endl;
        success = false;
    }

    // Verify min/max keys
    if (reader.min_key() != "test1") {
        std::cerr << "Min key should be 'test1', got '" << reader.min_key() << "'" << std::endl;
        success = false;
    }

    if (reader.max_key() != "test2") {
        std::cerr << "Max key should be 'test2', got '" << reader.max_key() << "'" << std::endl;
        success = false;
    }

    // Verify header with our helper
    if (!verify_sstable_header(filename)) {
        std::cerr << "Header verification failed" << std::endl;
        success = false;
    }

    fs::remove(filename);
    return success;
}

// Test 7: Edge cases
bool test_sstable_write_edge_cases()
{
    Memtable mt(4096);

    // empty key / value
    mt.put("", "");
    mt.put("empty_value", "");

    // special characters
    mt.put("key\nwith\nnewlines", "value\nwith\nnewlines");
    mt.put("key\twith\ttabs", "value\twith\ttabs");

    const std::string filename = "test_edge_cases.sst";

    if (!SSTableWriter::write_from_memtable(filename, mt))
    {
        std::cerr << "Failed to write SST file with edge cases" << std::endl;
        return false;
    }

    if (!fs::exists(filename))
    {
        std::cerr << "SST file with edge cases not created" << std::endl;
        return false;
    }

    // Verify with SSTableReader
    const SSTableReader reader(filename);

    if (!reader.is_valid()) {
        std::cerr << "Failed to load edge case SSTable" << std::endl;
        fs::remove(filename);
        return false;
    }

    // Verify all entries exist
    bool success = true;

    // Check empty key
    auto empty_key_val = reader.get("");
    if (!empty_key_val.has_value() || !empty_key_val.value().empty()) {
        std::cerr << "Empty key value incorrect" << std::endl;
        success = false;
    }

    // Check empty value
    auto empty_val = reader.get("empty_value");
    if (!empty_val.has_value() || !empty_val.value().empty()) {
        std::cerr << "Empty value incorrect" << std::endl;
        success = false;
    }

    // Check newlines
    auto newline_val = reader.get("key\nwith\nnewlines");
    if (!newline_val.has_value() || newline_val.value() != "value\nwith\nnewlines") {
        std::cerr << "Newline value incorrect" << std::endl;
        success = false;
    }

    // Check tabs
    auto tab_val = reader.get("key\twith\ttabs");
    if (!tab_val.has_value() || tab_val.value() != "value\twith\ttabs") {
        std::cerr << "Tab value incorrect" << std::endl;
        success = false;
    }

    fs::remove(filename);
    return success;
}

// Test 8: Performance test (optional)
bool test_sstable_write_performance() {
    constexpr int NUM_ENTRIES = 10000;
    Memtable mt(100 * 1024 * 1024);  // 100MB

    std::cout << "  Generating " << NUM_ENTRIES << " entries..." << std::endl;

    for (int i = 0; i < NUM_ENTRIES; i++) {
        std::string key = "user_" + std::to_string(i) + "_name";
        std::string value = "value_" + std::to_string(i) + "_" + std::string(50, 'x');
        mt.put(key, value);
    }

    const std::string filename = "test_perf.sst";

    const auto start = high_resolution_clock::now();
    const bool success = SSTableWriter::write_from_memtable(filename, mt);
    const auto end = high_resolution_clock::now();

    if (!success) {
        std::cerr << "Performance test failed to write" << std::endl;
        return false;
    }

    const auto duration = duration_cast<milliseconds>(end - start);
    const auto size = fs::file_size(filename);

    std::cout << "  Performance: " << NUM_ENTRIES << " entries, "
              << size / 1024 << " KB in "
              << duration.count() << " ms ("
              << (NUM_ENTRIES * 1000.0 / duration.count()) << " entries/sec)" << std::endl;

    // Optional: Verify the file can be read
    SSTableReader reader(filename);
    if (!reader.is_valid()) {
        std::cerr << "Performance test: written SSTable is invalid" << std::endl;
        fs::remove(filename);
        return false;
    }

    // Quick spot check of a few entries
    bool read_success = true;
    for (int i = 0; i < 10; i++) {
        std::string key = "user_" + std::to_string(i) + "_name";
        std::string expected = "value_" + std::to_string(i) + "_" + std::string(50, 'x');
        auto val = reader.get(key);
        if (!val.has_value() || val.value() != expected) {
            std::cerr << "Performance test: verification failed for key " << key << std::endl;
            read_success = false;
            break;
        }
    }

    // Clean up
    fs::remove(filename);
    return read_success;
}

// Helper function implementations
bool verify_sstable_header(const std::string& filename) {
    std::ifstream file(filename, std::ios::binary);
    if (!file) return false;

    uint64_t magic;
    uint32_t version;

    file.read(reinterpret_cast<char*>(&magic), sizeof(magic));
    file.read(reinterpret_cast<char*>(&version), sizeof(version));

    return (magic == SSTableWriter::MAGIC && version == SSTableWriter::VERSION);
}

uint64_t get_sstable_file_size(const std::string& filename) {
    if (!fs::exists(filename)) return 0;
    return fs::file_size(filename);
}

// Main test runner
int sstable_writer_tests_main() {
    std::cout << "Running SSTable Writer Tests" << std::endl;
    std::cout << "===========================" << std::endl;

    std::vector<std::pair<std::string, bool (*)()>> tests = {
        {"Basic Write", test_sstable_write_basic},
        {"Empty Memtable", test_sstable_write_empty_memtable},
        {"With Tombstones", test_sstable_write_with_tombstones},
        {"Large Data", test_sstable_write_large_data},
        {"Sorted Order", test_sstable_write_sorted_order},
        {"File Verification", test_sstable_write_file_verification},
        {"Edge Cases", test_sstable_write_edge_cases},
        {"Performance", test_sstable_write_performance}
    };

    int passed = 0;
    int total = tests.size();

    for (const auto& [name, test_func] : tests) {
        try {
            const bool result = test_func();
            print_test_result_sstable_writer(name, result);
            if (result) passed++;
        } catch (const std::exception& e) {
            std::cout << "X " << name << " (Exception: " << e.what() << ")" << std::endl;
        } catch (...) {
            std::cout << "X " << name << " (Unknown exception)" << std::endl;
        }
    }

    std::cout << "\nResults: " << passed << "/" << total << " tests passed" << std::endl;

    if (passed == total) {
        std::cout << "\nAll SSTable Writer tests passed!" << std::endl;
        return 0;
    } else {
        std::cout << "\nSome SSTable Writer tests failed" << std::endl;
        return 1;
    }
}