//
// Created by K on 2025-12-07.
//

#include "test_page.h"
#include "../Page.h"
#include <iostream>
#include <string>
#include <cstring>
#include <vector>
#include <filesystem>

namespace fs = std::filesystem;

void print_test_result_page(const std::string& test_name, bool passed) {
    std::cout << (passed ? "O " : "X ") << test_name << std::endl;
}

// Test 1: Basic construction and properties
bool test_page_basic_construction() {
    std::cout << "Testing Page basic construction..." << std::endl;

    Page page;

    // Check default properties
    if (page.get_data() == nullptr) {
        std::cerr << "  Page data should not be null" << std::endl;
        return false;
    }

    if (page.is_dirty()) {
        std::cerr << "  New page should not be dirty" << std::endl;
        return false;
    }

    if (page.is_pinned()) {
        std::cerr << "  New page should not be pinned" << std::endl;
        return false;
    }

    if (page.get_pin_count() != 0) {
        std::cerr << "  New page pin count should be 0" << std::endl;
        return false;
    }

    // Check page size
    if (Page::PAGE_SIZE != 4096) {
        std::cerr << "  Page size should be 4096 bytes" << std::endl;
        return false;
    }

    // Check that memory is zero-initialized
    const char* data = page.get_data();
    for (size_t i = 0; i < 100; ++i) {
        if (data[i] != 0) {
            std::cerr << "  Page memory not zero-initialized at byte " << i << std::endl;
            return false;
        }
    }

    return true;
}

// Test 2: Page ID management
bool test_page_id_management() {
    std::cout << "Testing Page ID management..." << std::endl;

    Page page;
    const PageId id("test.dat", 4096);

    // Set and get ID
    page.set_id(id);

    const PageId& retrieved_id = page.get_id();
    if (retrieved_id != id) {
        std::cerr << "  Page ID not set correctly" << std::endl;
        return false;
    }

    // Test default ID (should be empty)
    Page page2;
    const PageId& default_id = page2.get_id();
    if (!default_id.get_filename().empty() || default_id.get_offset() != 0) {
        std::cerr << "  Default page ID should be empty" << std::endl;
        return false;
    }

    return true;
}

// Test 3: Pin and unpin operations
bool test_page_pin_unpin() {
    std::cout << "Testing Page pin/unpin operations..." << std::endl;

    Page page;

    // Pin the page
    page.pin();
    if (!page.is_pinned()) {
        std::cerr << "  Page should be pinned after pin()" << std::endl;
        return false;
    }

    if (page.get_pin_count() != 1) {
        std::cerr << "  Pin count should be 1 after pin()" << std::endl;
        return false;
    }

    // Pin again (should increase count)
    page.pin();
    if (page.get_pin_count() != 2) {
        std::cerr << "  Pin count should be 2 after second pin()" << std::endl;
        return false;
    }

    // Unpin once
    page.unpin();
    if (page.get_pin_count() != 1) {
        std::cerr << "  Pin count should be 1 after unpin()" << std::endl;
        return false;
    }

    // Unpin to zero
    page.unpin();
    if (page.is_pinned()) {
        std::cerr << "  Page should not be pinned after unpin to zero" << std::endl;
        return false;
    }

    if (page.get_pin_count() != 0) {
        std::cerr << "  Pin count should be 0 after unpin to zero" << std::endl;
        return false;
    }

    // Test unpin when already at zero (should not go negative)
    page.unpin();
    if (page.get_pin_count() != 0) {
        std::cerr << "  Pin count should remain 0 after unpin at zero" << std::endl;
        return false;
    }

    return true;
}

// Test 4: Dirty flag management
bool test_page_dirty_flag() {
    std::cout << "Testing Page dirty flag management..." << std::endl;

    Page page;

    // Mark dirty
    page.mark_dirty();
    if (!page.is_dirty()) {
        std::cerr << "  Page should be dirty after mark_dirty()" << std::endl;
        return false;
    }

    // Clear dirty
    page.clear_dirty();
    if (page.is_dirty()) {
        std::cerr << "  Page should not be dirty after clear_dirty()" << std::endl;
        return false;
    }

    // Set dirty using setter
    page.set_dirty(true);
    if (!page.is_dirty()) {
        std::cerr << "  Page should be dirty after set_dirty(true)" << std::endl;
        return false;
    }

    page.set_dirty(false);
    if (page.is_dirty()) {
        std::cerr << "  Page should not be dirty after set_dirty(false)" << std::endl;
        return false;
    }

    return true;
}

// Test 5: Data copy operations
bool test_page_data_copy() {
    std::cout << "Testing Page data copy operations..." << std::endl;

    Page page;

    // Test copy_from
    const std::string test_data = "Hello, World!";
    page.copy_from(test_data.c_str(), test_data.length());

    // Verify data was copied
    char buffer[100];
    page.copy_to(buffer, test_data.length());

    if (std::memcmp(buffer, test_data.c_str(), test_data.length()) != 0) {
        std::cerr << "  Data copy failed" << std::endl;
        return false;
    }

    // Check that copy_from marks page dirty
    if (!page.is_dirty()) {
        std::cerr << "  copy_from should mark page dirty" << std::endl;
        return false;
    }

    // Test copy_to with offset
    page.clear_dirty();

    std::string data2 = "Test";
    page.copy_from(data2.c_str(), data2.length(), 100);  // Copy at offset 100

    char buffer2[10];
    page.copy_to(buffer2, data2.length(), 100);

    if (std::memcmp(buffer2, data2.c_str(), data2.length()) != 0) {
        std::cerr << "  Data copy with offset failed" << std::endl;
        return false;
    }

    // Test boundary conditions
    try {
        // This should throw because offset + size > PAGE_SIZE
        page.copy_from("test", 4, Page::PAGE_SIZE - 2);
        std::cerr << "  copy_from should throw on buffer overflow" << std::endl;
        return false;
    } catch (const std::out_of_range&) {
        // Expected
    }

    try {
        // This should throw
        page.copy_to(buffer, 4, Page::PAGE_SIZE - 2);
        std::cerr << "  copy_to should throw on buffer underflow" << std::endl;
        return false;
    } catch (const std::out_of_range&) {
        // Expected
    }

    return true;
}

// Test 6: Reset operation
bool test_page_reset() {
    std::cout << "Testing Page reset operation..." << std::endl;

    Page page;

    // Set some state
    page.set_id(PageId("test.dat", 4096));
    page.pin();
    page.pin();  // Pin count = 2
    page.mark_dirty();

    // Write some data
    std::string data = "Some data";
    page.copy_from(data.c_str(), data.length());

    // Reset the page
    page.reset();

    // Check that state is reset
    if (page.is_dirty()) {
        std::cerr << "  Page should not be dirty after reset" << std::endl;
        return false;
    }

    if (page.is_pinned()) {
        std::cerr << "  Page should not be pinned after reset" << std::endl;
        return false;
    }

    if (page.get_pin_count() != 0) {
        std::cerr << "  Pin count should be 0 after reset" << std::endl;
        return false;
    }

    // Check that data is zeroed
    const char* page_data = page.get_data();
    for (size_t i = 0; i < data.length(); ++i) {
        if (page_data[i] != 0) {
            std::cerr << "  Page data not zeroed after reset at byte " << i << std::endl;
            return false;
        }
    }

    // Check that ID is reset
    PageId default_id = page.get_id();
    if (!default_id.get_filename().empty() || default_id.get_offset() != 0) {
        std::cerr << "  Page ID should be reset to default" << std::endl;
        return false;
    }

    return true;
}

// Test 7: Move semantics
bool test_page_move_semantics() {
    std::cout << "Testing Page move semantics..." << std::endl;

    // Create a page with some state
    Page page1;
    page1.set_id(PageId("test.dat", 4096));
    page1.pin();
    page1.mark_dirty();

    std::string data = "Test data";
    page1.copy_from(data.c_str(), data.length());

    // Move construct
    Page page2(std::move(page1));

    // Check that page2 has the state
    if (!page2.is_dirty()) {
        std::cerr << "  Page2 should be dirty after move" << std::endl;
        return false;
    }

    if (!page2.is_pinned()) {
        std::cerr << "  Page2 should be pinned after move" << std::endl;
        return false;
    }

    // Check that data was moved
    char buffer[100];
    page2.copy_to(buffer, data.length());
    if (std::memcmp(buffer, data.c_str(), data.length()) != 0) {
        std::cerr << "  Data not moved correctly" << std::endl;
        return false;
    }

    // Check that page1 is reset after move
    if (page1.is_dirty()) {
        std::cerr << "  Page1 should not be dirty after move" << std::endl;
        return false;
    }

    // Move assignment
    Page page3;
    page3 = std::move(page2);

    // Check that page3 has the state
    if (!page3.is_dirty()) {
        std::cerr << "  Page3 should be dirty after move assignment" << std::endl;
        return false;
    }

    // Check that page2 is reset
    if (page2.is_dirty()) {
        std::cerr << "  Page2 should not be dirty after move assignment" << std::endl;
        return false;
    }

    return true;
}

// Test 8: Memory alignment
bool test_page_memory_alignment() {
    std::cout << "Testing Page memory alignment..." << std::endl;

    Page page;

    // Check that memory is properly aligned for direct I/O
    // This is important for performance on some architectures
    auto address = reinterpret_cast<uintptr_t>(page.get_data());

    // Should be aligned to 4096 bytes (page size)
    if (address % 4096 != 0) {
        std::cerr << "  Page memory not aligned to 4096 bytes: " << address << std::endl;
        return false;
    }

    // For direct I/O on Linux, alignment to 512 bytes might also be checked
    if (address % 512 != 0) {
        std::cerr << "  Page memory not aligned to 512 bytes (may affect direct I/O)" << std::endl;
        return false;
    }

    return true;
}

// Test 9: Large data operations
bool test_page_large_data() {
    std::cout << "Testing Page large data operations..." << std::endl;

    Page page;

    // Fill the entire page
    std::vector<char> large_data(Page::PAGE_SIZE);
    for (size_t i = 0; i < Page::PAGE_SIZE; ++i) {
        large_data[i] = static_cast<char>(i % 256);
    }

    // Copy to page
    page.copy_from(large_data.data(), Page::PAGE_SIZE);

    // Verify
    std::vector<char> read_data(Page::PAGE_SIZE);
    page.copy_to(read_data.data(), Page::PAGE_SIZE);

    if (std::memcmp(large_data.data(), read_data.data(), Page::PAGE_SIZE) != 0) {
        std::cerr << "  Large data copy failed" << std::endl;
        return false;
    }

    // Test partial fills
    page.reset();

    // Fill first half with 'A', second half with 'B'
    std::vector<char> half_a(Page::PAGE_SIZE / 2, 'A');
    std::vector<char> half_b(Page::PAGE_SIZE / 2, 'B');

    page.copy_from(half_a.data(), Page::PAGE_SIZE / 2);
    page.copy_from(half_b.data(), Page::PAGE_SIZE / 2, Page::PAGE_SIZE / 2);

    std::vector<char> verify(Page::PAGE_SIZE);
    page.copy_to(verify.data(), Page::PAGE_SIZE);

    bool success = true;
    for (size_t i = 0; i < Page::PAGE_SIZE / 2; ++i) {
        if (verify[i] != 'A') {
            std::cerr << "  First half verification failed at byte " << i << std::endl;
            success = false;
            break;
        }
    }

    for (size_t i = Page::PAGE_SIZE / 2; i < Page::PAGE_SIZE; ++i) {
        if (verify[i] != 'B') {
            std::cerr << "  Second half verification failed at byte " << i << std::endl;
            success = false;
            break;
        }
    }

    return success;
}

// Test 10: Performance test
bool test_page_performance() {
    std::cout << "Testing Page performance..." << std::endl;

    const int ITERATIONS = 10000;
    Page page;

    auto start = std::chrono::high_resolution_clock::now();

    // Perform many copy operations
    std::string test_data = "Performance test data";
    char buffer[100];

    for (int i = 0; i < ITERATIONS; ++i) {
        page.copy_from(test_data.c_str(), test_data.length());
        page.copy_to(buffer, test_data.length());
        page.clear_dirty();  // Reset dirty flag
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    std::cout << "  " << ITERATIONS << " copy operations in "
              << duration.count() << " ms" << std::endl;
    std::cout << "  " << (ITERATIONS * 1000.0 / duration.count())
              << " ops/sec" << std::endl;

    return true;
}

// Main test runner
int page_tests_main() {
    std::cout << "\n=== Page Unit Tests ===" << std::endl;
    std::cout << "======================" << std::endl;

    std::vector<std::pair<std::string, bool (*)()>> tests = {
        {"Basic construction", test_page_basic_construction},
        {"Page ID management", test_page_id_management},
        {"Pin/unpin operations", test_page_pin_unpin},
        {"Dirty flag management", test_page_dirty_flag},
        {"Data copy operations", test_page_data_copy},
        {"Reset operation", test_page_reset},
        {"Move semantics", test_page_move_semantics},
        {"Memory alignment", test_page_memory_alignment},
        {"Large data operations", test_page_large_data},
        {"Performance test", test_page_performance}
    };

    int passed = 0;
    int total = static_cast<int>(tests.size());

    for (const auto& [name, test_func] : tests) {
        std::cout << "\n" << name << "..." << std::endl;
        try {
            bool result = test_func();
            print_test_result_page("", result);
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
        std::cout << "\nO All Page tests passed!" << std::endl;
        return 0;
    } else {
        std::cout << "\nX Some Page tests failed" << std::endl;
        return 1;
    }
}