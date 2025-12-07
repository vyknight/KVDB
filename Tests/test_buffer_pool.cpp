#include "test_buffer_pool.h"
#include "../BufferPool.h"
#include "../Page.h"
#include "../PageId.h"
#include <iostream>
#include <vector>
#include <memory>
#include <thread>

void print_test_result_bufferpool(const std::string& test_name, bool passed) {
    std::cout << (passed ? "O " : "X ") << test_name << std::endl;
}

// Helper to create test pages
std::unique_ptr<Page> create_test_page(const std::string& filename, uint64_t offset, 
                                       const std::string& content = "") {
    auto page = std::make_unique<Page>();
    page->set_id(PageId(filename, offset));
    
    if (!content.empty()) {
        page->copy_from(content.data(), std::min(content.size(), Page::PAGE_SIZE));
    }
    
    return page;
}

// Test 1: Basic construction
bool test_bufferpool_construction() {
    std::cout << "  Testing BufferPool construction..." << std::endl;
    
    BufferPool pool(10);
    
    if (pool.capacity() != 10) {
        std::cerr << "    Capacity should be 10" << std::endl;
        return false;
    }
    
    if (pool.size() != 0) {
        std::cerr << "    Size should be 0 initially" << std::endl;
        return false;
    }
    
    // Test stats
    BufferPool::Stats stats = pool.get_stats();
    if (stats.capacity != 10 || stats.current_size != 0 || 
        stats.hits != 0 || stats.misses != 0 || stats.evictions != 0) {
        std::cerr << "    Initial stats incorrect" << std::endl;
        return false;
    }
    
    return true;
}

// Test 2: Add and get pages
bool test_bufferpool_add_get() {
    std::cout << "  Testing add_page and get_page..." << std::endl;
    
    BufferPool pool(5);
    
    // Add a page
    PageId id1("test1.dat", 0);
    auto page1 = create_test_page("test1.dat", 0, "Page 1");
    
    if (!pool.add_page(id1, std::move(*page1))) {
        std::cerr << "    Failed to add page" << std::endl;
        return false;
    }
    
    if (pool.size() != 1) {
        std::cerr << "    Size should be 1 after adding page" << std::endl;
        return false;
    }
    
    // Get the page
    Page* retrieved = pool.get_page(id1);
    if (!retrieved) {
        std::cerr << "    Failed to get page" << std::endl;
        return false;
    }
    
    // Check stats (should have 1 hit)
    BufferPool::Stats stats = pool.get_stats();
    if (stats.hits != 1) {
        std::cerr << "    Should have 1 hit, got " << stats.hits << std::endl;
        return false;
    }
    
    // Check page content
    char buffer[10];
    retrieved->copy_to(buffer, 6);
    buffer[6] = '\0';
    if (std::string(buffer) != "Page 1") {
        std::cerr << "    Page content incorrect" << std::endl;
        return false;
    }
    
    // Unpin the page
    pool.unpin_page(id1);
    
    // Try to get non-existent page
    PageId id2("test2.dat", 0);
    Page* not_found = pool.get_page(id2);
    if (not_found) {
        std::cerr << "    Should not find non-existent page" << std::endl;
        return false;
    }
    
    // Check stats (should have 1 miss)
    stats = pool.get_stats();
    if (stats.misses != 1) {
        std::cerr << "    Should have 1 miss, got " << stats.misses << std::endl;
        return false;
    }
    
    return true;
}

// Test 3: LRU eviction policy
bool test_bufferpool_lru_eviction() {
    std::cout << "  Testing LRU eviction policy..." << std::endl;
    
    BufferPool pool(3);  // Small pool for testing
    
    // Add three pages
    PageId ids[3] = {
        PageId("test.dat", 0),
        PageId("test.dat", 4096),
        PageId("test.dat", 8192)
    };
    
    for (int i = 0; i < 3; ++i) {
        auto page = create_test_page("test.dat", i * 4096, "Page " + std::to_string(i));
        if (!pool.add_page(ids[i], std::move(*page))) {
            std::cerr << "    Failed to add page " << i << std::endl;
            return false;
        }
        pool.unpin_page(ids[i]);  // Unpin so they can be evicted
    }
    
    // Pool should be full
    if (pool.size() != 3) {
        std::cerr << "    Pool should be full (size 3)" << std::endl;
        return false;
    }
    
    // Access page 0 to make it recently used
    pool.get_page(ids[0]);
    pool.unpin_page(ids[0]);
    
    // Add a fourth page - should evict least recently used (page 1 or 2)
    PageId id4("test.dat", 12288);
    auto page4 = create_test_page("test.dat", 12288, "Page 4");
    
    if (!pool.add_page(id4, std::move(*page4))) {
        std::cerr << "    Failed to add fourth page" << std::endl;
        return false;
    }
    pool.unpin_page(id4);
    
    // Check eviction count
    BufferPool::Stats stats = pool.get_stats();
    if (stats.evictions != 1) {
        std::cerr << "    Should have 1 eviction, got " << stats.evictions << std::endl;
        return false;
    }
    
    // Page 0 should still be in pool (recently accessed)
    if (!pool.contains(ids[0])) {
        std::cerr << "    Page 0 should still be in pool (recently accessed)" << std::endl;
        return false;
    }
    
    // Page 4 should be in pool (newly added)
    if (!pool.contains(id4)) {
        std::cerr << "    Page 4 should be in pool" << std::endl;
        return false;
    }
    
    // One of pages 1 or 2 should have been evicted
    bool page1_in = pool.contains(ids[1]);
    bool page2_in = pool.contains(ids[2]);
    
    if (page1_in && page2_in) {
        std::cerr << "    One of pages 1 or 2 should have been evicted" << std::endl;
        return false;
    }
    
    if (!page1_in && !page2_in) {
        std::cerr << "    Both pages 1 and 2 should not have been evicted" << std::endl;
        return false;
    }
    
    return true;
}

// Test 4: Pinning prevents eviction
bool test_bufferpool_pinning() {
    std::cout << "  Testing that pinned pages are not evicted..." << std::endl;
    
    BufferPool pool(2);
    
    // Add two pages, pin one
    PageId id1("test.dat", 0);
    PageId id2("test.dat", 4096);
    
    auto page1 = create_test_page("test.dat", 0, "Pinned");
    auto page2 = create_test_page("test.dat", 4096, "Unpinned");
    
    pool.add_page(id1, std::move(*page1));
    // Don't unpin page1 - it stays pinned
    pool.add_page(id2, std::move(*page2));
    pool.unpin_page(id2);  // Unpin page2
    
    // Add a third page - should evict page2 (unpinned), not page1 (pinned)
    PageId id3("test.dat", 8192);
    auto page3 = create_test_page("test.dat", 8192, "New page");
    
    if (!pool.add_page(id3, std::move(*page3))) {
        std::cerr << "    Failed to add third page" << std::endl;
        return false;
    }
    pool.unpin_page(id3);
    
    // Page1 should still be in pool (pinned)
    if (!pool.contains(id1)) {
        std::cerr << "    Pinned page should not be evicted" << std::endl;
        return false;
    }
    
    // Page2 should have been evicted
    if (pool.contains(id2)) {
        std::cerr << "    Unpinned page should have been evicted" << std::endl;
        return false;
    }
    
    // Page3 should be in pool
    if (!pool.contains(id3)) {
        std::cerr << "    New page should be in pool" << std::endl;
        return false;
    }
    
    // Now unpin page1 and add another page
    pool.unpin_page(id1);
    PageId id4("test.dat", 12288);
    auto page4 = create_test_page("test.dat", 12288, "Fourth page");
    
    if (!pool.add_page(id4, std::move(*page4))) {
        std::cerr << "    Failed to add fourth page" << std::endl;
        return false;
    }
    pool.unpin_page(id4);
    
    // Now page1 could be evicted (it's unpinned and least recent)
    // Either page1 or page3 could be evicted
    
    return true;
}

// Test 5: Dirty page marking
bool test_bufferpool_dirty_pages() {
    std::cout << "  Testing dirty page marking..." << std::endl;
    
    BufferPool pool(5);
    
    PageId id("test.dat", 0);
    auto page = create_test_page("test.dat", 0, "Original");
    
    pool.add_page(id, std::move(*page));
    
    // Get page and mark it dirty
    Page* retrieved = pool.get_page(id);
    if (!retrieved) {
        std::cerr << "    Failed to get page" << std::endl;
        return false;
    }
    
    pool.mark_dirty(id);
    
    if (!retrieved->is_dirty()) {
        std::cerr << "    Page should be marked dirty" << std::endl;
        return false;
    }
    
    // Modify page content
    retrieved->copy_from("Modified", 8);
    
    // Check that modification preserved dirty flag
    if (!retrieved->is_dirty()) {
        std::cerr << "    Page should still be dirty after modification" << std::endl;
        return false;
    }
    
    pool.unpin_page(id);
    
    return true;
}

// Test 6: Remove page
bool test_bufferpool_remove() {
    std::cout << "  Testing page removal..." << std::endl;
    
    BufferPool pool(5);
    
    PageId id("test.dat", 0);
    auto page = create_test_page("test.dat", 0, "To be removed");
    
    // Add page
    pool.add_page(id, std::move(*page));
    pool.unpin_page(id);
    
    if (!pool.contains(id)) {
        std::cerr << "    Page should be in pool after adding" << std::endl;
        return false;
    }
    
    // Remove page
    if (!pool.remove_page(id)) {
        std::cerr << "    Failed to remove page" << std::endl;
        return false;
    }
    
    if (pool.contains(id)) {
        std::cerr << "    Page should not be in pool after removal" << std::endl;
        return false;
    }
    
    if (pool.size() != 0) {
        std::cerr << "    Size should be 0 after removal" << std::endl;
        return false;
    }
    
    // Try to remove non-existent page
    if (pool.remove_page(PageId("nonexistent.dat", 0))) {
        std::cerr << "    Should not be able to remove non-existent page" << std::endl;
        return false;
    }
    
    return true;
}

// Test 7: Clear buffer pool
bool test_bufferpool_clear() {
    std::cout << "  Testing clear..." << std::endl;
    
    BufferPool pool(10);
    
    // Add some pages
    for (int i = 0; i < 5; ++i) {
        PageId id("test.dat", i * 4096);
        auto page = create_test_page("test.dat", i * 4096, "Page " + std::to_string(i));
        pool.add_page(id, std::move(*page));
        pool.unpin_page(id);
    }
    
    if (pool.size() != 5) {
        std::cerr << "    Should have 5 pages before clear" << std::endl;
        return false;
    }
    
    // Clear pool
    pool.clear();
    
    if (pool.size() != 0) {
        std::cerr << "    Size should be 0 after clear" << std::endl;
        return false;
    }
    
    // Check that stats are reset
    BufferPool::Stats stats = pool.get_stats();
    if (stats.hits != 0 || stats.misses != 0 || stats.evictions != 0) {
        std::cerr << "    Stats should be reset after clear" << std::endl;
        return false;
    }
    
    // Add pages again after clear
    PageId id("test.dat", 0);
    auto page = create_test_page("test.dat", 0, "After clear");
    
    if (!pool.add_page(id, std::move(*page))) {
        std::cerr << "    Failed to add page after clear" << std::endl;
        return false;
    }
    
    if (pool.size() != 1) {
        std::cerr << "    Should be able to add pages after clear" << std::endl;
        return false;
    }
    
    pool.unpin_page(id);
    
    return true;
}

// Test 8: Move semantics
bool test_bufferpool_move_semantics() {
    std::cout << "  Testing move semantics..." << std::endl;
    
    BufferPool pool1(10);
    
    // Add some pages to pool1
    PageId id1("test.dat", 0);
    auto page1 = create_test_page("test.dat", 0, "Page 1");
    pool1.add_page(id1, std::move(*page1));
    pool1.unpin_page(id1);
    
    // Move constructor
    BufferPool pool2(std::move(pool1));
    
    if (pool2.capacity() != 10) {
        std::cerr << "    Move constructor failed to preserve capacity" << std::endl;
        return false;
    }
    
    if (!pool2.contains(id1)) {
        std::cerr << "    Move constructor failed to transfer pages" << std::endl;
        return false;
    }
    
    // Move assignment
    BufferPool pool3(5);
    pool3 = std::move(pool2);
    
    if (pool3.capacity() != 10) {
        std::cerr << "    Move assignment failed to preserve capacity" << std::endl;
        return false;
    }
    
    if (!pool3.contains(id1)) {
        std::cerr << "    Move assignment failed to transfer pages" << std::endl;
        return false;
    }
    
    return true;
}

// Test 9: Concurrent access (basic thread safety test)
bool test_bufferpool_thread_safety() {
    std::cout << "  Testing basic thread safety..." << std::endl;
    
    BufferPool pool(100);
    const int num_threads = 4;
    const int pages_per_thread = 25;
    
    std::vector<std::thread> threads;
    std::atomic<int> errors{0};
    
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&pool, t, &errors]() {
            for (int i = 0; i < pages_per_thread; ++i) {
                uint64_t offset = (t * pages_per_thread + i) * Page::PAGE_SIZE;
                PageId id("concurrent.dat", offset);
                
                auto page = std::make_unique<Page>();
                page->set_id(id);
                page->copy_from("Thread data", 11);
                
                if (!pool.add_page(id, std::move(*page))) {
                    errors++;
                }
                
                // Sometimes get a page
                if (i % 5 == 0) {
                    Page* p = pool.get_page(id);
                    if (p) {
                        pool.unpin_page(id);
                    } else {
                        errors++;
                    }
                }
                
                // Sometimes remove a page
                if (i % 7 == 0 && i > 0) {
                    PageId prev_id("concurrent.dat", (offset - Page::PAGE_SIZE));
                    pool.remove_page(prev_id);
                }
            }
        });
    }
    
    // Wait for all threads
    for (auto& thread : threads) {
        thread.join();
    }
    
    if (errors > 0) {
        std::cerr << "    Found " << errors << " errors in concurrent access" << std::endl;
        return false;
    }
    
    // Pool should still be in a valid state
    if (pool.size() > pool.capacity()) {
        std::cerr << "    Pool size exceeds capacity after concurrent access" << std::endl;
        return false;
    }
    
    return true;
}

// Test 10: Statistics tracking
bool test_bufferpool_statistics() {
    std::cout << "  Testing statistics tracking..." << std::endl;
    
    BufferPool pool(5);
    
    // Initial stats
    BufferPool::Stats stats = pool.get_stats();
    if (stats.hits != 0 || stats.misses != 0 || stats.evictions != 0) {
        std::cerr << "    Initial stats should be zero" << std::endl;
        return false;
    }
    
    // Add and get a page (hit)
    PageId id1("stats.dat", 0);
    auto page1 = create_test_page("stats.dat", 0, "Stats test");
    pool.add_page(id1, std::move(*page1));
    pool.unpin_page(id1);
    
    Page* p = pool.get_page(id1);
    if (p) {
        pool.unpin_page(id1);
    }
    
    stats = pool.get_stats();
    if (stats.hits != 1) {
        std::cerr << "    Should have 1 hit, got " << stats.hits << std::endl;
        return false;
    }
    
    // Try to get non-existent page (miss)
    pool.get_page(PageId("nonexistent.dat", 0));
    
    stats = pool.get_stats();
    if (stats.misses != 1) {
        std::cerr << "    Should have 1 miss, got " << stats.misses << std::endl;
        return false;
    }
    
    // Fill pool to cause eviction
    for (int i = 1; i < 10; ++i) {
        PageId id("stats.dat", i * Page::PAGE_SIZE);
        auto page = create_test_page("stats.dat", i * Page::PAGE_SIZE, "Page " + std::to_string(i));
        pool.add_page(id, std::move(*page));
        pool.unpin_page(id);
    }
    
    stats = pool.get_stats();
    if (stats.evictions == 0) {
        std::cerr << "    Should have some evictions when pool is full" << std::endl;
        return false;
    }
    
    // Check current_size in stats matches actual size
    if (stats.current_size != pool.size()) {
        std::cerr << "    Stats current_size should match pool size" << std::endl;
        return false;
    }
    
    return true;
}

// Main test runner
int bufferpool_tests_main() {
    std::cout << "\n=== BufferPool Unit Tests ===" << std::endl;
    std::cout << "============================" << std::endl;
    
    std::vector<std::pair<std::string, bool (*)()>> tests = {
        {"Basic construction", test_bufferpool_construction},
        {"Add and get pages", test_bufferpool_add_get},
        {"LRU eviction policy", test_bufferpool_lru_eviction},
        {"Pinning prevents eviction", test_bufferpool_pinning},
        {"Dirty page marking", test_bufferpool_dirty_pages},
        {"Page removal", test_bufferpool_remove},
        {"Clear buffer pool", test_bufferpool_clear},
        {"Move semantics", test_bufferpool_move_semantics},
        {"Basic thread safety", test_bufferpool_thread_safety},
        {"Statistics tracking", test_bufferpool_statistics}
    };
    
    int passed = 0;
    int total = static_cast<int>(tests.size());
    
    for (const auto& [name, test_func] : tests) {
        std::cout << "\n" << name << "..." << std::endl;
        try {
            bool result = test_func();
            print_test_result_bufferpool("", result);
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
        std::cout << "\nO All BufferPool tests passed!" << std::endl;
        return 0;
    } else {
        std::cout << "\nX Some BufferPool tests failed" << std::endl;
        return 1;
    }
}