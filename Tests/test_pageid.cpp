//
// Created by K on 2025-12-07.
//

#include "test_pageid.h"
#include "../PageId.h"
#include <iostream>
#include <string>
#include <unordered_set>
#include <filesystem>
#include <vector>

namespace fs = std::filesystem;

void print_test_result_pageid(const std::string& test_name, bool passed) {
    std::cout << (passed ? "O " : "X ") << test_name << std::endl;
}

// Test 1: Basic construction
bool test_pageid_basic_construction() {
    std::cout << "Testing PageId basic construction..." << std::endl;

    PageId id1("test.dat", 0);
    if (id1.get_filename() != "test.dat" || id1.get_offset() != 0) {
        std::cerr << "  Construction failed for (test.dat, 0)" << std::endl;
        return false;
    }

    PageId id2("another.sst", 4096);
    if (id2.get_filename() != "another.sst" || id2.get_offset() != 4096) {
        std::cerr << "  Construction failed for (another.sst, 4096)" << std::endl;
        return false;
    }

    // Test page alignment
    PageId id3("file.dat", 1234);  // Not 4096-aligned
    if (id3.get_offset() != 0) {  // Should be aligned to 0
        std::cerr << "  Page alignment failed: offset " << id3.get_offset()
                  << " not aligned" << std::endl;
        return false;
    }

    PageId id4("file.dat", 8192);  // 8192 is 2 * 4096
    if (id4.get_offset() != 8192) {
        std::cerr << "  Page alignment failed for already aligned offset" << std::endl;
        return false;
    }

    return true;
}

// Test 2: Comparison operators
bool test_pageid_comparisons() {
    std::cout << "Testing PageId comparison operators..." << std::endl;

    PageId id1("a.dat", 0);
    PageId id2("a.dat", 4096);
    PageId id3("b.dat", 0);
    PageId id4("a.dat", 0);  // Same as id1

    // Equality
    if (!(id1 == id4) || (id1 == id2) || (id1 == id3)) {
        std::cerr << "  Equality operator failed" << std::endl;
        return false;
    }

    // Inequality
    if (!(id1 != id2) || !(id1 != id3) || (id1 != id4)) {
        std::cerr << "  Inequality operator failed" << std::endl;
        return false;
    }

    // Less than
    if (!(id1 < id2) || !(id1 < id3) || (id2 < id1) || (id4 < id1)) {
        std::cerr << "  Less than operator failed" << std::endl;
        return false;
    }

    // Test ordering: filename first, then offset
    PageId id5("a.dat", 8192);
    PageId id6("b.dat", 0);

    if (!(id1 < id5) || !(id5 < id6)) {
        std::cerr << "  Ordering failed" << std::endl;
        return false;
    }

    return true;
}

// Test 3: Hash function
bool test_pageid_hash() {
    std::cout << "Testing PageId hash function..." << std::endl;

    PageId id1("test.dat", 0);
    PageId id2("test.dat", 4096);
    PageId id3("test.dat", 0);  // Same as id1
    PageId id4("other.dat", 0);

    PageIdHash hasher;

    size_t hash1 = hasher(id1);
    size_t hash2 = hasher(id2);
    size_t hash3 = hasher(id3);
    size_t hash4 = hasher(id4);

    // Same page should have same hash
    if (hash1 != hash3) {
        std::cerr << "  Same page IDs should have same hash" << std::endl;
        return false;
    }

    // Different pages should (usually) have different hashes
    // Note: There can be collisions, but they're unlikely
    if (hash1 == hash2 || hash1 == hash4) {
        std::cerr << "  Different page IDs should have different hashes (possible collision)" << std::endl;
        // This could be a legitimate hash collision, so we don't fail the test
        // Just warn
        std::cout << "  Warning: Hash collision detected" << std::endl;
    }

    // Test with unordered_set (uses hash)
    std::unordered_set<PageId, PageIdHash> page_set;
    page_set.insert(id1);
    page_set.insert(id2);
    page_set.insert(id3);  // Should not increase size (duplicate)
    page_set.insert(id4);

    if (page_set.size() != 3) {  // id1/id3 are same, id2 and id4 are different
        std::cerr << "  Unordered set size incorrect: expected 3, got " << page_set.size() << std::endl;
        return false;
    }

    return true;
}

// Test 4: String representation
bool test_pageid_to_string() {
    std::cout << "Testing PageId string representation..." << std::endl;

    PageId id1("test.dat", 0);
    PageId id2("path/to/file.sst", 8192);

    std::string str1 = id1.to_string();
    std::string str2 = id2.to_string();

    if (str1 != "test.dat:0") {
        std::cerr << "  String representation failed for id1: " << str1 << std::endl;
        return false;
    }

    if (str2 != "path/to/file.sst:8192") {
        std::cerr << "  String representation failed for id2: " << str2 << std::endl;
        return false;
    }

    // Test parsing isn't required, but string should be readable
    std::cout << "  id1: " << str1 << std::endl;
    std::cout << "  id2: " << str2 << std::endl;

    return true;
}

// Test 5: Copy and move semantics
bool test_pageid_copy_move() {
    std::cout << "Testing PageId copy and move semantics..." << std::endl;

    // Copy constructor
    PageId original("test.dat", 4096);
    PageId copy = original;

    if (!(original == copy)) {
        std::cerr << "  Copy constructor failed" << std::endl;
        return false;
    }

    // Move constructor
    PageId moved(std::move(copy));
    if (!(original == moved)) {
        std::cerr << "  Move constructor failed" << std::endl;
        return false;
    }

    // Copy assignment
    PageId assigned("other.dat", 0);
    assigned = original;
    if (!(original == assigned)) {
        std::cerr << "  Copy assignment failed" << std::endl;
        return false;
    }

    // Move assignment
    PageId move_assigned("another.dat", 8192);
    move_assigned = std::move(assigned);
    if (!(original == move_assigned)) {
        std::cerr << "  Move assignment failed" << std::endl;
        return false;
    }

    return true;
}

// Main test runner
int pageid_tests_main() {
    std::cout << "\n=== PageId Unit Tests ===" << std::endl;
    std::cout << "========================" << std::endl;

    std::vector<std::pair<std::string, bool (*)()>> tests = {
        {"Basic construction", test_pageid_basic_construction},
        {"Comparison operators", test_pageid_comparisons},
        {"Hash function", test_pageid_hash},
        {"String representation", test_pageid_to_string},
        {"Copy and move semantics", test_pageid_copy_move}
    };

    int passed = 0;
    int total = static_cast<int>(tests.size());

    for (const auto& [name, test_func] : tests) {
        std::cout << "\n" << name << "..." << std::endl;
        try {
            bool result = test_func();
            print_test_result_pageid("", result);
            if (result) passed++;
        } catch (const std::exception& e) {
            std::cout << "X Exception: " << e.what() << std::endl;
        } catch (...) {
            std::cout << "X Unknown exception" << std::endl;
        }
    }

    std::cout << "\n" << std::string(50, '=') << std::endl;
    std::cout << "Results: " << passed << "/" << total << " tests passed" << std::endl;

    if (passed == total) {
        std::cout << "\nO All PageId tests passed!" << std::endl;
        return 0;
    } else {
        std::cout << "\nX Some PageId tests failed" << std::endl;
        return 1;
    }
}