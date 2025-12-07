#include "test_page.h"
#include "../Page.h"
#include "../PageId.h"
#include <iostream>
#include <cstring>
#include <vector>

#include "test_helper.h"

// Test 1: Basic construction and properties
bool test_page_construction() {
    std::cout << "  Testing Page construction..." << std::endl;
    
    Page page;
    
    // Check initial state
    if (page.get_data() == nullptr) {
        std::cerr << "    Page data should not be null" << std::endl;
        return false;
    }
    
    if (page.is_dirty()) {
        std::cerr << "    New page should not be dirty" << std::endl;
        return false;
    }
    
    if (page.is_pinned()) {
        std::cerr << "    New page should not be pinned" << std::endl;
        return false;
    }
    
    if (page.get_pin_count() != 0) {
        std::cerr << "    New page pin count should be 0" << std::endl;
        return false;
    }
    
    // Check that page is zero-initialized
    const char* data = page.get_data();
    for (size_t i = 0; i < 10; ++i) {
        if (data[i] != 0) {
            std::cerr << "    New page should be zero-initialized" << std::endl;
            return false;
        }
    }
    
    return true;
}

// Test 2: Data operations
bool test_page_data_operations() {
    std::cout << "  Testing data operations..." << std::endl;
    
    Page page;
    
    // Test copy_from
    std::string test_data = "Hello, World!";
    page.copy_from(test_data.data(), test_data.size(), 0);
    
    // Verify copy
    char buffer[100];
    page.copy_to(buffer, test_data.size(), 0);
    buffer[test_data.size()] = '\0';
    
    if (std::string(buffer) != test_data) {
        std::cerr << "    copy_from/copy_to failed: expected '" << test_data 
                  << "', got '" << buffer << "'" << std::endl;
        return false;
    }
    
    // Test copy_from with offset
    std::string offset_data = "Offset";
    page.copy_from(offset_data.data(), offset_data.size(), 100);
    
    page.copy_to(buffer, offset_data.size(), 100);
    buffer[offset_data.size()] = '\0';
    
    if (std::string(buffer) != offset_data) {
        std::cerr << "    copy_from with offset failed" << std::endl;
        return false;
    }
    
    // Test that page is marked dirty after copy_from
    if (!page.is_dirty()) {
        std::cerr << "    Page should be dirty after copy_from" << std::endl;
        return false;
    }
    
    // Test clear_dirty
    page.clear_dirty();
    if (page.is_dirty()) {
        std::cerr << "    Page should not be dirty after clear_dirty" << std::endl;
        return false;
    }
    
    // Test mark_dirty
    page.mark_dirty();
    if (!page.is_dirty()) {
        std::cerr << "    Page should be dirty after mark_dirty" << std::endl;
        return false;
    }
    
    return true;
}

// Test 3: Pin/unpin operations
bool test_page_pin_unpin() {
    std::cout << "  Testing pin/unpin operations..." << std::endl;
    
    Page page;
    
    // Test pin
    page.pin();
    if (!page.is_pinned()) {
        std::cerr << "    Page should be pinned after pin()" << std::endl;
        return false;
    }
    
    if (page.get_pin_count() != 1) {
        std::cerr << "    Pin count should be 1 after pin()" << std::endl;
        return false;
    }
    
    // Pin multiple times
    page.pin();
    page.pin();
    if (page.get_pin_count() != 3) {
        std::cerr << "    Pin count should be 3 after 3 pins" << std::endl;
        return false;
    }
    
    // Test unpin
    page.unpin();
    if (page.get_pin_count() != 2) {
        std::cerr << "    Pin count should be 2 after unpin" << std::endl;
        return false;
    }
    
    // Unpin to zero
    page.unpin();
    page.unpin();
    if (page.is_pinned()) {
        std::cerr << "    Page should not be pinned after unpinning to 0" << std::endl;
        return false;
    }
    
    if (page.get_pin_count() != 0) {
        std::cerr << "    Pin count should be 0 after all unpins" << std::endl;
        return false;
    }
    
    // Test that unpin doesn't go below zero
    page.unpin();
    page.unpin();
    if (page.get_pin_count() != 0) {
        std::cerr << "    Pin count should not go below 0" << std::endl;
        return false;
    }
    
    return true;
}

// Test 4: Page ID operations
bool test_page_id_operations() {
    std::cout << "  Testing Page ID operations..." << std::endl;
    
    Page page;
    
    // Test set_id
    PageId id1("test.dat", 4096);
    page.set_id(id1);
    
    if (page.get_id() != id1) {
        std::cerr << "    Page ID not set correctly" << std::endl;
        return false;
    }
    
    // Test reset
    page.reset();
    
    PageId default_id;
    if (page.get_id() != default_id) {
        std::cerr << "    Page ID should be default after reset" << std::endl;
        return false;
    }
    
    if (page.is_dirty()) {
        std::cerr << "    Page should not be dirty after reset" << std::endl;
        return false;
    }
    
    if (page.is_pinned()) {
        std::cerr << "    Page should not be pinned after reset" << std::endl;
        return false;
    }
    
    // Check that data is cleared
    const char* data = page.get_data();
    for (size_t i = 0; i < 10; ++i) {
        if (data[i] != 0) {
            std::cerr << "    Page data should be cleared after reset" << std::endl;
            return false;
        }
    }
    
    return true;
}

// Test 5: Move semantics
bool test_page_move_semantics() {
    std::cout << "  Testing move semantics..." << std::endl;
    
    // Create and populate a page
    Page page1;
    page1.set_id(PageId("move.dat", 8192));
    page1.copy_from("Test data", 9);
    page1.pin();
    page1.mark_dirty();
    
    // Move constructor
    Page page2(std::move(page1));
    
    // Check that page2 has the data
    if (page2.get_id().get_filename() != "move.dat" || 
        page2.get_id().get_offset() != 8192) {
        std::cerr << "    Move constructor failed to transfer ID" << std::endl;
        return false;
    }
    
    char buffer[10];
    page2.copy_to(buffer, 9);
    buffer[9] = '\0';
    if (std::string(buffer) != "Test data") {
        std::cerr << "    Move constructor failed to transfer data" << std::endl;
        return false;
    }
    
    if (!page2.is_dirty()) {
        std::cerr << "    Move constructor failed to transfer dirty flag" << std::endl;
        return false;
    }
    
    if (page2.get_pin_count() != 1) {
        std::cerr << "    Move constructor failed to transfer pin count" << std::endl;
        return false;
    }
    
    // Move assignment
    Page page3;
    page3 = std::move(page2);
    
    if (page3.get_id().get_filename() != "move.dat") {
        std::cerr << "    Move assignment failed to transfer ID" << std::endl;
        return false;
    }
    
    return true;
}

// Test 6: Boundary conditions
bool test_page_boundary_conditions() {
    std::cout << "  Testing boundary conditions..." << std::endl;
    
    Page page;
    
    // Test copy_from at page boundary
    std::vector<char> large_data(Page::PAGE_SIZE, 'A');
    
    try {
        page.copy_from(large_data.data(), Page::PAGE_SIZE, 0);
        // Should succeed
    } catch (const std::exception& e) {
        std::cerr << "    copy_from at boundary should not throw: " << e.what() << std::endl;
        return false;
    }
    
    // Test copy_from exceeding boundary
    try {
        page.copy_from(large_data.data(), Page::PAGE_SIZE + 1, 0);
        std::cerr << "    copy_from exceeding boundary should throw" << std::endl;
        return false;
    } catch (const std::out_of_range&) {
        // Expected
    } catch (const std::exception& e) {
        std::cerr << "    Wrong exception type: " << e.what() << std::endl;
        return false;
    }
    
    // Test copy_from at offset exceeding boundary
    try {
        page.copy_from("test", 4, Page::PAGE_SIZE - 2);
        std::cerr << "    copy_from at offset exceeding boundary should throw" << std::endl;
        return false;
    } catch (const std::out_of_range&) {
        // Expected
    } catch (const std::exception& e) {
        std::cerr << "    Wrong exception type: " << e.what() << std::endl;
        return false;
    }
    
    return true;
}

// Main test runner
int page_tests_main() {
    std::cout << "\n=== Page Unit Tests ===" << std::endl;
    std::cout << "======================" << std::endl;
    
    std::vector<std::pair<std::string, bool (*)()>> tests = {
        {"Basic construction", test_page_construction},
        {"Data operations", test_page_data_operations},
        {"Pin/unpin operations", test_page_pin_unpin},
        {"Page ID operations", test_page_id_operations},
        {"Move semantics", test_page_move_semantics},
        {"Boundary conditions", test_page_boundary_conditions}
    };
    
    int passed = 0;
    int total = static_cast<int>(tests.size());
    
    for (const auto& [name, test_func] : tests) {
        std::cout << "\n" << name << "..." << std::endl;
        try {
            bool result = test_func();
            print_test_result("", result);
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