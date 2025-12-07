//
// Created by K on 2025-12-06.
//

#include "test_wal.h"
#include "../WriteAheadLog.h"

#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <filesystem>
#include <chrono>
#include <random>
#include <thread>
#include <algorithm>
#include <cstring>
#include <functional>

#include "test_helper.h"

namespace fs = std::filesystem;
using namespace std::chrono;

// Test 1: Basic operations
bool test_wal_basic_operations(const std::string& test_dir) {
    std::string filename = make_test_path(test_dir, "basic.bin");

    WriteAheadLog wal(filename);

    if (!wal.log_put("key1", "value1")) {
        std::cerr << "  Failed to log PUT for key1" << std::endl;
        return false;
    }

    if (!wal.log_put("key2", "value2")) {
        std::cerr << "  Failed to log PUT for key2" << std::endl;
        return false;
    }

    if (!wal.log_put("key3", "value3")) {
        std::cerr << "  Failed to log PUT for key3" << std::endl;
        return false;
    }

    auto entries = wal.read_all_entries();

    if (entries.size() != 3) {
        std::cerr << "  Expected 3 entries, got " << entries.size() << std::endl;
        return false;
    }

    // Verify entries
    if (entries[0].type != WriteAheadLog::OpType::PUT ||
        entries[0].key != "key1" || entries[0].value != "value1") {
        std::cerr << "  Entry 0 incorrect" << std::endl;
        return false;
    }

    if (entries[1].type != WriteAheadLog::OpType::PUT ||
        entries[1].key != "key2" || entries[1].value != "value2") {
        std::cerr << "  Entry 1 incorrect" << std::endl;
        return false;
    }

    if (entries[2].type != WriteAheadLog::OpType::PUT ||
        entries[2].key != "key3" || entries[2].value != "value3") {
        std::cerr << "  Entry 2 incorrect" << std::endl;
        return false;
    }

    return true;
}

// Test 2: DELETE operations
bool test_wal_delete_operations(const std::string& test_dir) {
    std::string filename = make_test_path(test_dir, "delete.bin");

    WriteAheadLog wal(filename);

    if (!wal.log_put("key1", "value1")) return false;
    if (!wal.log_delete("key1")) return false;
    if (!wal.log_put("key2", "value2")) return false;
    if (!wal.log_put("key3", "value3")) return false;
    if (!wal.log_delete("key2")) return false;

    auto entries = wal.read_all_entries();

    if (entries.size() != 5) {
        std::cerr << "  Expected 5 entries, got " << entries.size() << std::endl;
        return false;
    }

    // Verify each entry
    bool success = true;
    if (!(entries[0].type == WriteAheadLog::OpType::PUT && entries[0].key == "key1" && entries[0].value == "value1")) {
        std::cerr << "  Entry 0 mismatch" << std::endl;
        success = false;
    }
    if (!(entries[1].type == WriteAheadLog::OpType::DELETE && entries[1].key == "key1" && entries[1].value.empty())) {
        std::cerr << "  Entry 1 mismatch" << std::endl;
        success = false;
    }
    if (!(entries[2].type == WriteAheadLog::OpType::PUT && entries[2].key == "key2" && entries[2].value == "value2")) {
        std::cerr << "  Entry 2 mismatch" << std::endl;
        success = false;
    }
    if (!(entries[3].type == WriteAheadLog::OpType::PUT && entries[3].key == "key3" && entries[3].value == "value3")) {
        std::cerr << "  Entry 3 mismatch" << std::endl;
        success = false;
    }
    if (!(entries[4].type == WriteAheadLog::OpType::DELETE && entries[4].key == "key2" && entries[4].value.empty())) {
        std::cerr << "  Entry 4 mismatch" << std::endl;
        success = false;
    }

    return success;
}

// Test 3: Recovery scenario
bool test_wal_recovery_scenario(const std::string& test_dir) {
    std::string filename = make_test_path(test_dir, "recovery.bin");

    // Simulate first run
    {
        WriteAheadLog wal(filename);
        wal.log_put("user1", "Alice");
        wal.log_put("user2", "Bob");
        wal.log_delete("user1");
        wal.log_put("user3", "Charlie");
    }

    // Simulate restart and recovery
    {
        WriteAheadLog wal(filename);
        auto entries = wal.read_all_entries();

        if (entries.size() != 4) {
            std::cerr << "  Expected 4 recovered entries, got " << entries.size() << std::endl;
            return false;
        }

        // Verify order
        bool success = true;
        if (!(entries[0].type == WriteAheadLog::OpType::PUT && entries[0].key == "user1" && entries[0].value == "Alice")) {
            std::cerr << "  Recovered entry 0 mismatch" << std::endl;
            success = false;
        }
        if (!(entries[1].type == WriteAheadLog::OpType::PUT && entries[1].key == "user2" && entries[1].value == "Bob")) {
            std::cerr << "  Recovered entry 1 mismatch" << std::endl;
            success = false;
        }
        if (!(entries[2].type == WriteAheadLog::OpType::DELETE && entries[2].key == "user1" && entries[2].value.empty())) {
            std::cerr << "  Recovered entry 2 mismatch" << std::endl;
            success = false;
        }
        if (!(entries[3].type == WriteAheadLog::OpType::PUT && entries[3].key == "user3" && entries[3].value == "Charlie")) {
            std::cerr << "  Recovered entry 3 mismatch" << std::endl;
            success = false;
        }
        return success;
    }
}

// Test 4: Clear functionality
bool test_wal_clear_functionality(const std::string& test_dir) {
    std::string filename = make_test_path(test_dir, "clear.bin");

    WriteAheadLog wal(filename);

    wal.log_put("key1", "value1");
    wal.log_put("key2", "value2");

    size_t size_before = wal.size();

    wal.clear();

    // Verify file is now empty
    auto entries = wal.read_all_entries();
    if (!entries.empty()) {
        std::cerr << "  WAL not empty after clear" << std::endl;
        return false;
    }

    // Should be able to write after clear
    if (!wal.log_put("newkey", "newvalue")) {
        std::cerr << "  Cannot write to WAL after clear" << std::endl;
        return false;
    }

    entries = wal.read_all_entries();
    if (entries.size() != 1) {
        std::cerr << "  After clear: expected 1 entry, got " << entries.size() << std::endl;
        return false;
    }

    if (entries[0].type != WriteAheadLog::OpType::PUT ||
        entries[0].key != "newkey" ||
        entries[0].value != "newvalue") {
        std::cerr << "  Entry after clear is incorrect" << std::endl;
        return false;
    }

    return true;
}

// Test 5: Edge cases
bool test_wal_edge_cases(const std::string& test_dir) {
    std::string filename = make_test_path(test_dir, "edge.bin");

    WriteAheadLog wal(filename);

    // Test empty strings
    if (!wal.log_put("", "")) {
        std::cerr << "  Failed to log empty key/value" << std::endl;
        return false;
    }

    if (!wal.log_put("key", "")) {
        std::cerr << "  Failed to log empty value" << std::endl;
        return false;
    }

    if (!wal.log_put("", "value")) {
        std::cerr << "  Failed to log empty key" << std::endl;
        return false;
    }

    if (!wal.log_delete("")) {
        std::cerr << "  Failed to log delete empty key" << std::endl;
        return false;
    }

    auto entries = wal.read_all_entries();

    if (entries.size() != 4) {
        std::cerr << "  Expected 4 entries, got " << entries.size() << std::endl;
        return false;
    }

    return true;
}

// Test 6: Large data handling
bool test_wal_large_data(const std::string& test_dir) {
    std::string filename = make_test_path(test_dir, "large.bin");

    WriteAheadLog wal(filename);

    const int NUM_ENTRIES = 100;
    const size_t VALUE_SIZE = 100;

    for (int i = 0; i < NUM_ENTRIES; i++) {
        std::string key = "key_" + std::to_string(i);
        std::string value(VALUE_SIZE, static_cast<char>('A' + (i % 26)));

        if (!wal.log_put(key, value)) {
            std::cerr << "  Failed to log entry " << i << std::endl;
            return false;
        }
    }

    auto entries = wal.read_all_entries();

    if (entries.size() != NUM_ENTRIES) {
        std::cerr << "  Expected " << NUM_ENTRIES << " entries, got " << entries.size() << std::endl;
        return false;
    }

    // Quick spot check
    for (int i = 0; i < std::min(5, NUM_ENTRIES); i++) {
        if (entries[i].key != "key_" + std::to_string(i)) {
            std::cerr << "  Entry " << i << " key mismatch" << std::endl;
            return false;
        }
    }

    return true;
}

// Test 7: Concurrent simulation
bool test_wal_concurrent_simulation(const std::string& test_dir) {
    std::string filename = make_test_path(test_dir, "concurrent.bin");

    // Simulate multiple sessions writing to same WAL
    {
        WriteAheadLog wal1(filename);
        if (!wal1.log_put("session1_key1", "value1") ||
            !wal1.log_put("session1_key2", "value2")) {
            std::cerr << "  Session 1 failed" << std::endl;
            return false;
        }
    }

    {
        WriteAheadLog wal2(filename);
        if (!wal2.log_put("session2_key1", "valueA") ||
            !wal2.log_delete("session1_key1") ||
            !wal2.log_put("session2_key2", "valueB")) {
            std::cerr << "  Session 2 failed" << std::endl;
            return false;
        }
    }

    {
        WriteAheadLog wal3(filename);
        if (!wal3.log_delete("session2_key1") ||
            !wal3.log_put("session3_key1", "valueX")) {
            std::cerr << "  Session 3 failed" << std::endl;
            return false;
        }
    }

    // Read back all entries
    WriteAheadLog wal_read(filename);
    auto entries = wal_read.read_all_entries();

    // Should have 7 entries total (2 + 3 + 2)
    if (entries.size() != 7) {
        std::cerr << "  Expected 7 entries, got " << entries.size() << std::endl;
        return false;
    }

    return true;
}

// Test 8: Corrupted file handling
bool test_wal_corrupted_file(const std::string& test_dir) {
    std::string filename = make_test_path(test_dir, "corrupted.bin");

    // Create a corrupted WAL file
    {
        std::ofstream file(filename, std::ios::binary);
        if (!file) return false;

        // Write invalid magic number
        uint64_t bad_magic = 0xDEADBEEF;
        file.write(reinterpret_cast<const char*>(&bad_magic), sizeof(bad_magic));

        // Write some garbage data
        uint32_t garbage = 12345;
        file.write(reinterpret_cast<const char*>(&garbage), sizeof(garbage));
        file.close();
    }

    // Should handle gracefully and create new valid file
    try {
        WriteAheadLog wal(filename);

        // Should be able to write to new file
        if (!wal.log_put("test", "value")) {
            return false;
        }

        auto entries = wal.read_all_entries();
        return entries.size() == 1 && entries[0].key == "test";
    } catch (const std::exception& e) {
        std::cerr << "  Exception: " << e.what() << std::endl;
        return false;
    }
}

// Test 9: Mixed operations
bool test_wal_mixed_operations(const std::string& test_dir) {
    std::string filename = make_test_path(test_dir, "mixed.bin");

    WriteAheadLog wal(filename);

    const int NUM_OPERATIONS = 100;
    std::vector<std::string> keys;
    std::srand(static_cast<unsigned>(std::time(nullptr)));

    for (int i = 0; i < NUM_OPERATIONS; i++) {
        std::string key = "key" + std::to_string(std::rand() % 20);  // Only 20 possible keys
        std::string value = "value" + std::to_string(i);

        if (std::rand() % 2 == 0) {
            if (!wal.log_put(key, value)) {
                std::cerr << "  Failed to log PUT at operation " << i << std::endl;
                return false;
            }
            keys.push_back(key);
        } else if (!keys.empty()) {
            std::string key_to_delete = keys[std::rand() % keys.size()];
            if (!wal.log_delete(key_to_delete)) {
                std::cerr << "  Failed to log DELETE at operation " << i << std::endl;
                return false;
            }
        }
    }

    // Just verify we can read it back
    auto entries = wal.read_all_entries();
    return !entries.empty();
}

// Test 10: Performance
bool test_wal_performance(const std::string& test_dir) {
    std::string filename = make_test_path(test_dir, "perf.bin");

    WriteAheadLog wal(filename);

    const int NUM_OPS = 1000;

    auto start = high_resolution_clock::now();

    for (int i = 0; i < NUM_OPS; i++) {
        std::string key = "key_" + std::to_string(i);
        std::string value = "value_" + std::string(50, 'x');

        if (!wal.log_put(key, value)) {
            std::cerr << "  Failed at operation " << i << std::endl;
            return false;
        }
    }

    auto end = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(end - start);

    std::cout << "  Performance: " << NUM_OPS << " ops in "
              << duration.count() << " ms ("
              << (NUM_OPS * 1000.0 / duration.count()) << " ops/sec)" << std::endl;

    return true;
}

// Test 11: File operations
bool test_wal_file_operations(const std::string& test_dir) {
    // Test that we can create multiple WAL files
    for (int i = 0; i < 5; i++) {
        std::string filename = make_test_path(test_dir, "multi_" + std::to_string(i) + ".bin");
        WriteAheadLog wal(filename);

        if (!wal.log_put("key", "value")) {
            std::cerr << "  Failed to create WAL file " << i << std::endl;
            return false;
        }
    }

    return true;
}

// Test 12: Memory safety
bool test_wal_memory_safety(const std::string& test_dir) {
    // Create and destroy multiple WAL objects
    for (int i = 0; i < 10; i++) {
        std::string filename = make_test_path(test_dir, "memtest_" + std::to_string(i) + ".bin");
        {
            WriteAheadLog wal(filename);
            if (!wal.log_put("test", "value")) {
                std::cerr << "  Failed to write to WAL " << i << std::endl;
                return false;
            }
        } // wal destroyed here

        // Should be able to reopen
        WriteAheadLog wal2(filename);
        auto entries = wal2.read_all_entries();
        if (entries.size() != 1) {
            std::cerr << "  Expected 1 entry for WAL " << i << ", got " << entries.size() << std::endl;
            return false;
        }

        if (entries[0].key != "test" || entries[0].value != "value") {
            std::cerr << "  Data corrupted for WAL " << i << std::endl;
            return false;
        }
    }

    return true;
}

// Test 13: Header integrity
bool test_wal_header_integrity(const std::string& test_dir) {
    std::string filename = make_test_path(test_dir, "header.bin");

    // Create WAL and verify it has proper header
    {
        WriteAheadLog wal(filename);
        if (!wal.log_put("test", "value")) {
            return false;
        }
    }

    // Read raw file to check header
    std::ifstream file(filename, std::ios::binary);
    if (!file) return false;

    uint64_t magic;
    file.read(reinterpret_cast<char*>(&magic), sizeof(magic));

    // Should match WAL magic
    return magic == WriteAheadLog::MAGIC;
}

// Test 14: Sequential consistency
bool test_wal_sequential_consistency(const std::string& test_dir) {
    std::string filename = make_test_path(test_dir, "sequential.bin");

    WriteAheadLog wal(filename);

    // Specific sequence
    if (!wal.log_put("a", "1")) return false;
    if (!wal.log_put("b", "2")) return false;
    if (!wal.log_put("c", "3")) return false;
    if (!wal.log_delete("a")) return false;
    if (!wal.log_put("a", "4")) return false;
    if (!wal.log_put("d", "5")) return false;
    if (!wal.log_delete("c")) return false;
    if (!wal.log_put("e", "6")) return false;
    if (!wal.log_delete("b")) return false;
    if (!wal.log_put("b", "7")) return false;

    auto entries = wal.read_all_entries();

    if (entries.size() != 10) {
        std::cerr << "  Expected 10 entries, got " << entries.size() << std::endl;
        return false;
    }

    // Verify operation types in sequence
    std::vector<WriteAheadLog::OpType> expected_types = {
        WriteAheadLog::OpType::PUT,  // a:1
        WriteAheadLog::OpType::PUT,  // b:2
        WriteAheadLog::OpType::PUT,  // c:3
        WriteAheadLog::OpType::DELETE, // delete a
        WriteAheadLog::OpType::PUT,  // a:4
        WriteAheadLog::OpType::PUT,  // d:5
        WriteAheadLog::OpType::DELETE, // delete c
        WriteAheadLog::OpType::PUT,  // e:6
        WriteAheadLog::OpType::DELETE, // delete b
        WriteAheadLog::OpType::PUT   // b:7
    };

    for (size_t i = 0; i < entries.size(); i++) {
        if (entries[i].type != expected_types[i]) {
            std::cerr << "  Entry " << i << " type mismatch" << std::endl;
            return false;
        }
    }

    return true;
}

// Test 15: Move semantics
bool test_wal_move_semantics(const std::string& test_dir) {
    std::string filename1 = make_test_path(test_dir, "move1.bin");
    std::string filename2 = make_test_path(test_dir, "move2.bin");

    // Create first WAL and write data
    WriteAheadLog wal1(filename1);
    if (!wal1.log_put("key1", "value1")) return false;
    if (!wal1.log_put("key2", "value2")) return false;

    // Move construct
    WriteAheadLog wal2(std::move(wal1));

    // wal1 should be in moved-from state
    if (wal1.is_open()) {
        std::cerr << "  wal1 should not be open after move" << std::endl;
        return false;
    }

    // wal2 should have the data
    auto entries = wal2.read_all_entries();
    if (entries.size() != 2) {
        std::cerr << "  wal2 should have 2 entries after move" << std::endl;
        return false;
    }

    // Test move assignment
    WriteAheadLog wal3(filename2);
    if (!wal3.log_put("key3", "value3")) return false;

    wal3 = std::move(wal2);

    // wal3 should now have wal2's data
    entries = wal3.read_all_entries();
    if (entries.size() != 2) {
        std::cerr << "  wal3 should have 2 entries after move assignment" << std::endl;
        return false;
    }

    return true;
}

// Main test runner
int wal_tests_main() {
    // Create unique test directory
    auto now = system_clock::now();
    auto timestamp = duration_cast<milliseconds>(now.time_since_epoch()).count();
    std::string test_dir = "wal_tests_" + std::to_string(timestamp);

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

    std::cout << "\nRunning Write-Ahead Log Tests" << std::endl;
    std::cout << "=============================" << std::endl;
    std::cout << "Test directory: " << test_dir << std::endl;
    std::cout << std::endl;

    // Store all test functions
    std::vector<std::pair<std::string, std::function<bool(const std::string&)>>> tests = {
        {"1. Basic Operations", test_wal_basic_operations},
        {"2. Delete Operations", test_wal_delete_operations},
        {"3. Recovery Scenario", test_wal_recovery_scenario},
        {"4. Clear Functionality", test_wal_clear_functionality},
        {"5. Edge Cases", test_wal_edge_cases},
        {"6. Large Data", test_wal_large_data},
        {"7. Concurrent Simulation", test_wal_concurrent_simulation},
        {"8. Corrupted File", test_wal_corrupted_file},
        {"9. Mixed Operations", test_wal_mixed_operations},
        {"10. Performance", test_wal_performance},
        {"11. File Operations", test_wal_file_operations},
        {"12. Memory Safety", test_wal_memory_safety},
        {"13. Header Integrity", test_wal_header_integrity},
        {"14. Sequential Consistency", test_wal_sequential_consistency},
        {"15. Move Semantics", test_wal_move_semantics}
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
        std::cout << "\nAll WAL tests passed!" << std::endl;
        return 0;
    } else {
        std::cout << "\nSome WAL tests failed" << std::endl;
        return 1;
    }
}