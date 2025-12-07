#include "test_bufferpool.h"
#include "../BufferPool.h"
#include "../PageId.h"
#include "../DirectIO.h"
#include <iostream>
#include <string>
#include <vector>
#include <filesystem>
#include <cstring>
#include <memory>

namespace fs = std::filesystem;

void print_test_result_bufferpool(const std::string& test_name, bool passed) {
    std::cout << (passed ? "O " : "X ") << test_name << std::endl;
}

// Helper function to create a test file with pages
std::string create_test_file(size_t num_pages) {
    std::string filename = "test_bufferpool_" + std::to_string(rand()) + ".dat";

    const auto file = DirectIO::open(filename, false); // write mode
    if (!file) {
        throw std::runtime_error("Failed to create test file");
    }

    std::vector<char> page_data(Page::PAGE_SIZE);
    for (size_t i = 0; i < num_pages; ++i) {
        // Fill page with pattern based on page number
        char pattern = 'A' + (i % 26);
        std::fill(page_data.begin(), page_data.end(), pattern);

        // Write page number at the beginning of each page for verification
        uint64_t page_num = i;
        std::memcpy(page_data.data(), &page_num, sizeof(page_num));

        uint64_t offset = i * Page::PAGE_SIZE;
        if (!file->write(offset, page_data.data(), Page::PAGE_SIZE)) {
            throw std::runtime_error("Failed to write to test file");
        }
    }

    return filename;
}

// Clean up test file
void cleanup_test_file(const std::string& filename) {
    if (fs::exists(filename)) {
        fs::remove(filename);
    }
}

// Test 1: Basic construction and initialization
bool test_bufferpool_construction() {
    std::cout << "  Testing LRU buffer pool construction..." << std::endl;
    BufferPool lru_pool(10, true);

    if (lru_pool.capacity() != 10) {
        std::cerr << "    LRU buffer pool capacity incorrect" << std::endl;
        return false;
    }

    if (lru_pool.size() != 0) {
        std::cerr << "    LRU buffer pool size should be 0 initially" << std::endl;
        return false;
    }

    std::cout << "  Testing Clock buffer pool construction..." << std::endl;
    BufferPool clock_pool(10, false);

    if (clock_pool.capacity() != 10) {
        std::cerr << "    Clock buffer pool capacity incorrect" << std::endl;
        return false;
    }

    if (clock_pool.size() != 0) {
        std::cerr << "    Clock buffer pool size should be 0 initially" << std::endl;
        return false;
    }

    return true;
}

// Test 2: Basic page loading and retrieval
bool test_bufferpool_get_page() {
    std::string filename = create_test_file(5);

    try {
        auto file = DirectIO::open(filename, true); // read-only

        BufferPool pool(10, true); // Use LRU for this test

        // Get first page
        PageId page_id1(filename, 0);
        Page* page1 = pool.get_page(page_id1, file.get());

        if (!page1) {
            std::cerr << "    Failed to get page 0" << std::endl;
            cleanup_test_file(filename);
            return false;
        }

        // Verify page content
        uint64_t page_num;
        std::memcpy(&page_num, page1->get_data(), sizeof(page_num));
        if (page_num != 0) {
            std::cerr << "    Page 0 content incorrect" << std::endl;
            pool.unpin_page(page_id1);
            cleanup_test_file(filename);
            return false;
        }

        pool.unpin_page(page_id1);

        // Get same page again (should be hit)
        Page* page1_again = pool.get_page(page_id1, file.get());
        if (!page1_again) {
            std::cerr << "    Failed to get page 0 again" << std::endl;
            cleanup_test_file(filename);
            return false;
        }

        pool.unpin_page(page_id1);

        // Get different page
        PageId page_id2(filename, Page::PAGE_SIZE);
        Page* page2 = pool.get_page(page_id2, file.get());

        if (!page2) {
            std::cerr << "    Failed to get page 1" << std::endl;
            pool.unpin_page(page_id1);
            cleanup_test_file(filename);
            return false;
        }

        std::memcpy(&page_num, page2->get_data(), sizeof(page_num));
        if (page_num != 1) {
            std::cerr << "    Page 1 content incorrect" << std::endl;
            pool.unpin_page(page_id1);
            pool.unpin_page(page_id2);
            cleanup_test_file(filename);
            return false;
        }

        pool.unpin_page(page_id2);

        cleanup_test_file(filename);
        return true;
    } catch (const std::exception& e) {
        std::cerr << "    Exception: " << e.what() << std::endl;
        cleanup_test_file(filename);
        return false;
    }
}

// Test 3: Hit and miss statistics
bool test_bufferpool_statistics() {
    std::string filename = create_test_file(5);

    try {
        auto file = DirectIO::open(filename, true);
        BufferPool pool(5, true);

        BufferPool::Stats stats = pool.get_stats();

        if (stats.hits != 0 || stats.misses != 0) {
            std::cerr << "    Initial stats should be zero" << std::endl;
            cleanup_test_file(filename);
            return false;
        }

        // First access - should be miss
        PageId page_id1(filename, 0);
        Page* page1 = pool.get_page(page_id1, file.get());
        pool.unpin_page(page_id1);

        stats = pool.get_stats();
        if (stats.misses != 1) {
            std::cerr << "    Expected 1 miss, got " << stats.misses << std::endl;
            cleanup_test_file(filename);
            return false;
        }

        // Second access to same page - should be hit
        page1 = pool.get_page(page_id1, file.get());
        pool.unpin_page(page_id1);

        stats = pool.get_stats();
        if (stats.hits != 1) {
            std::cerr << "    Expected 1 hit, got " << stats.hits << std::endl;
            cleanup_test_file(filename);
            return false;
        }

        // Check hit rate
        double hit_rate = pool.hit_rate();
        if (hit_rate != 0.5) { // 1 hit, 1 miss = 0.5
            std::cerr << "    Expected hit rate 0.5, got " << hit_rate << std::endl;
            cleanup_test_file(filename);
            return false;
        }

        // Clear stats
        pool.clear_stats();
        stats = pool.get_stats();
        if (stats.hits != 0 || stats.misses != 0) {
            std::cerr << "    Stats not cleared properly" << std::endl;
            cleanup_test_file(filename);
            return false;
        }

        cleanup_test_file(filename);
        return true;
    } catch (const std::exception& e) {
        std::cerr << "    Exception: " << e.what() << std::endl;
        cleanup_test_file(filename);
        return false;
    }
}

// Test 4: Pinning and unpinning
bool test_bufferpool_pin_unpin() {
    std::string filename = create_test_file(10);

    try {
        auto file = DirectIO::open(filename, true);
        BufferPool pool(3, true); // Small pool for testing

        PageId page_id1(filename, 0);
        PageId page_id2(filename, Page::PAGE_SIZE);
        PageId page_id3(filename, 2 * Page::PAGE_SIZE);
        PageId page_id4(filename, 3 * Page::PAGE_SIZE);

        // Pin three pages (fill the pool)
        Page* page1 = pool.get_page(page_id1, file.get());
        Page* page2 = pool.get_page(page_id2, file.get());
        Page* page3 = pool.get_page(page_id3, file.get());

        if (!page1 || !page2 || !page3) {
            std::cerr << "    Failed to get pages" << std::endl;
            cleanup_test_file(filename);
            return false;
        }

        // All pages should be pinned
        if (!page1->is_pinned() || !page2->is_pinned() || !page3->is_pinned()) {
            std::cerr << "    Pages should be pinned" << std::endl;
            pool.unpin_page(page_id1);
            pool.unpin_page(page_id2);
            pool.unpin_page(page_id3);
            cleanup_test_file(filename);
            return false;
        }

        // Try to get a fourth page - should succeed (eviction will happen)
        Page* page4 = pool.get_page(page_id4, file.get());
        if (!page4) {
            std::cerr << "    Failed to get fourth page" << std::endl;
            pool.unpin_page(page_id1);
            pool.unpin_page(page_id2);
            pool.unpin_page(page_id3);
            cleanup_test_file(filename);
            return false;
        }

        // Unpin pages
        pool.unpin_page(page_id1);
        pool.unpin_page(page_id2);
        pool.unpin_page(page_id3);
        pool.unpin_page(page_id4);

        // Check unpinned status
        if (page1->is_pinned()) {
            std::cerr << "    Page 1 should be unpinned" << std::endl;
            cleanup_test_file(filename);
            return false;
        }

        cleanup_test_file(filename);
        return true;
    } catch (const std::exception& e) {
        std::cerr << "    Exception: " << e.what() << std::endl;
        cleanup_test_file(filename);
        return false;
    }
}

// Test 5: Dirty page marking and flushing
bool test_bufferpool_dirty_pages() {
    std::string filename = create_test_file(2);
    std::string temp_filename = filename + ".temp";

    try {
        // Copy file to temp for writing test
        fs::copy(filename, temp_filename);

        auto file = DirectIO::open(temp_filename, false); // write mode
        BufferPool pool(5, true);

        PageId page_id(temp_filename, 0);
        Page* page = pool.get_page(page_id, file.get());

        if (!page) {
            std::cerr << "    Failed to get page" << std::endl;
            cleanup_test_file(filename);
            cleanup_test_file(temp_filename);
            return false;
        }

        // Modify page
        page->get_data()[0] = 'Z';

        // Mark as dirty
        pool.mark_dirty(page_id);

        if (!page->is_dirty()) {
            std::cerr << "    Page should be marked dirty" << std::endl;
            pool.unpin_page(page_id);
            cleanup_test_file(filename);
            cleanup_test_file(temp_filename);
            return false;
        }

        // Check stats
        BufferPool::Stats stats = pool.get_stats();
        if (stats.dirty_pages != 1) {
            std::cerr << "    Expected 1 dirty page, got " << stats.dirty_pages << std::endl;
            pool.unpin_page(page_id);
            cleanup_test_file(filename);
            cleanup_test_file(temp_filename);
            return false;
        }

        // Flush the page
        if (!pool.flush_page(page_id, file.get())) {
            std::cerr << "    Failed to flush page" << std::endl;
            pool.unpin_page(page_id);
            cleanup_test_file(filename);
            cleanup_test_file(temp_filename);
            return false;
        }

        // Page should no longer be dirty after flush
        if (page->is_dirty()) {
            std::cerr << "    Page should not be dirty after flush" << std::endl;
            pool.unpin_page(page_id);
            cleanup_test_file(filename);
            cleanup_test_file(temp_filename);
            return false;
        }

        pool.unpin_page(page_id);
        cleanup_test_file(filename);
        cleanup_test_file(temp_filename);
        return true;
    } catch (const std::exception& e) {
        std::cerr << "    Exception: " << e.what() << std::endl;
        cleanup_test_file(filename);
        cleanup_test_file(temp_filename);
        return false;
    }
}

// Test 6: LRU eviction policy
bool test_bufferpool_lru_eviction() {
    std::string filename = create_test_file(5);

    try {
        auto file = DirectIO::open(filename, true);
        BufferPool pool(3, true); // LRU with capacity 3

        // Load pages 0, 1, 2
        PageId page_ids[] = {
            PageId(filename, 0),
            PageId(filename, Page::PAGE_SIZE),
            PageId(filename, 2 * Page::PAGE_SIZE)
        };

        for (int i = 0; i < 3; ++i) {
            Page* page = pool.get_page(page_ids[i], file.get());
            pool.unpin_page(page_ids[i]);
            if (!page) {
                std::cerr << "    Failed to load page " << i << std::endl;
                cleanup_test_file(filename);
                return false;
            }
        }

        // Pool should be full
        if (pool.size() != 3) {
            std::cerr << "    Pool should be full (size 3)" << std::endl;
            cleanup_test_file(filename);
            return false;
        }

        // Access page 0 to make it recently used
        pool.get_page(page_ids[0], file.get());
        pool.unpin_page(page_ids[0]);

        // Load page 3 - should evict page 1 (least recently used)
        PageId page_id3(filename, 3 * Page::PAGE_SIZE);
        Page* page3 = pool.get_page(page_id3, file.get());
        pool.unpin_page(page_id3);

        if (!page3) {
            std::cerr << "    Failed to load page 3" << std::endl;
            cleanup_test_file(filename);
            return false;
        }

        // Check which pages are in pool
        // Page 0 should be in pool (recently accessed)
        // Page 1 should be evicted (least recently used)
        // Page 2 should be in pool
        // Page 3 should be in pool (newly loaded)

        if (!pool.contains(page_ids[0])) {
            std::cerr << "    Page 0 should be in pool (recently accessed)" << std::endl;
            cleanup_test_file(filename);
            return false;
        }

        if (pool.contains(page_ids[1])) {
            std::cerr << "    Page 1 should be evicted (least recently used)" << std::endl;
            cleanup_test_file(filename);
            return false;
        }

        if (!pool.contains(page_ids[2])) {
            std::cerr << "    Page 2 should be in pool" << std::endl;
            cleanup_test_file(filename);
            return false;
        }

        if (!pool.contains(page_id3)) {
            std::cerr << "    Page 3 should be in pool" << std::endl;
            cleanup_test_file(filename);
            return false;
        }

        cleanup_test_file(filename);
        return true;
    } catch (const std::exception& e) {
        std::cerr << "    Exception: " << e.what() << std::endl;
        cleanup_test_file(filename);
        return false;
    }
}

// Test 7: Clock eviction policy
bool test_bufferpool_clock_eviction() {
    std::string filename = create_test_file(5);

    try {
        auto file = DirectIO::open(filename, true);
        BufferPool pool(3, false); // Clock with capacity 3

        // Load pages 0, 1, 2
        PageId page_ids[] = {
            PageId(filename, 0),
            PageId(filename, Page::PAGE_SIZE),
            PageId(filename, 2 * Page::PAGE_SIZE)
        };

        for (int i = 0; i < 3; ++i) {
            Page* page = pool.get_page(page_ids[i], file.get());
            pool.unpin_page(page_ids[i]);
            if (!page) {
                std::cerr << "    Failed to load page " << i << std::endl;
                cleanup_test_file(filename);
                return false;
            }
        }

        // Access page 0 to give it a reference bit
        pool.get_page(page_ids[0], file.get());
        pool.unpin_page(page_ids[0]);

        // Load page 3 - should evict based on clock algorithm
        PageId page_id3(filename, 3 * Page::PAGE_SIZE);
        Page* page3 = pool.get_page(page_id3, file.get());
        pool.unpin_page(page_id3);

        if (!page3) {
            std::cerr << "    Failed to load page 3" << std::endl;
            cleanup_test_file(filename);
            return false;
        }

        // With clock algorithm:
        // - Page 0 was recently accessed (reference bit = 1)
        // - Page 1 hasn't been accessed since loading (reference bit = 0, should be evicted first)
        // - Page 2 hasn't been accessed since loading (reference bit = 0)

        // So page 1 should be evicted

        if (!pool.contains(page_ids[0])) {
            std::cerr << "    Page 0 should be in pool (recently accessed)" << std::endl;
            cleanup_test_file(filename);
            return false;
        }

        if (pool.contains(page_ids[1])) {
            std::cerr << "    Page 1 should be evicted by clock algorithm" << std::endl;
            cleanup_test_file(filename);
            return false;
        }

        if (!pool.contains(page_ids[2])) {
            std::cerr << "    Page 2 should be in pool" << std::endl;
            cleanup_test_file(filename);
            return false;
        }

        if (!pool.contains(page_id3)) {
            std::cerr << "    Page 3 should be in pool" << std::endl;
            cleanup_test_file(filename);
            return false;
        }

        cleanup_test_file(filename);
        return true;
    } catch (const std::exception& e) {
        std::cerr << "    Exception: " << e.what() << std::endl;
        cleanup_test_file(filename);
        return false;
    }
}

// Test 8: Resize buffer pool
bool test_bufferpool_resize() {
    std::string filename = create_test_file(10);

    try {
        auto file = DirectIO::open(filename, true);
        BufferPool pool(3, true);

        if (pool.capacity() != 3) {
            std::cerr << "    Initial capacity should be 3" << std::endl;
            cleanup_test_file(filename);
            return false;
        }

        // Load some pages
        for (int i = 0; i < 3; ++i) {
            PageId page_id(filename, i * Page::PAGE_SIZE);
            Page* page = pool.get_page(page_id, file.get());
            pool.unpin_page(page_id);
            if (!page) {
                std::cerr << "    Failed to load page " << i << std::endl;
                cleanup_test_file(filename);
                return false;
            }
        }

        // Resize to larger pool
        pool.resize(10);

        if (pool.capacity() != 10) {
            std::cerr << "    Capacity should be 10 after resize" << std::endl;
            cleanup_test_file(filename);
            return false;
        }

        // Should still have the same pages
        for (int i = 0; i < 3; ++i) {
            PageId page_id(filename, i * Page::PAGE_SIZE);
            if (!pool.contains(page_id)) {
                std::cerr << "    Page " << i << " should still be in pool after resize" << std::endl;
                cleanup_test_file(filename);
                return false;
            }
        }

        cleanup_test_file(filename);
        return true;
    } catch (const std::exception& e) {
        std::cerr << "    Exception: " << e.what() << std::endl;
        cleanup_test_file(filename);
        return false;
    }
}

// Test 9: Clear buffer pool
bool test_bufferpool_clear() {
    std::string filename = create_test_file(3);

    try {
        auto file = DirectIO::open(filename, true);
        BufferPool pool(5, true);

        // Load all pages
        for (int i = 0; i < 3; ++i) {
            PageId page_id(filename, i * Page::PAGE_SIZE);
            Page* page = pool.get_page(page_id, file.get());
            pool.unpin_page(page_id);
            if (!page) {
                std::cerr << "    Failed to load page " << i << std::endl;
                cleanup_test_file(filename);
                return false;
            }
        }

        // Mark one page as dirty
        PageId dirty_page_id(filename, Page::PAGE_SIZE);
        Page* dirty_page = pool.get_page(dirty_page_id, file.get());
        if (dirty_page) {
            dirty_page->get_data()[0] = 'X';
            pool.mark_dirty(dirty_page_id);
        }
        pool.unpin_page(dirty_page_id);

        // Check initial size
        if (pool.size() != 3) {
            std::cerr << "    Pool should have 3 pages before clear" << std::endl;
            cleanup_test_file(filename);
            return false;
        }

        // Clear pool
        size_t flushed = pool.clear(file.get());

        // Should have flushed 1 dirty page
        if (flushed != 1) {
            std::cerr << "    Should have flushed 1 dirty page, got " << flushed << std::endl;
            cleanup_test_file(filename);
            return false;
        }

        // Pool should be empty
        if (pool.size() != 0) {
            std::cerr << "    Pool should be empty after clear" << std::endl;
            cleanup_test_file(filename);
            return false;
        }

        // Pages should not be in pool
        for (int i = 0; i < 3; ++i) {
            PageId page_id(filename, i * Page::PAGE_SIZE);
            if (pool.contains(page_id)) {
                std::cerr << "    Page " << i << " should not be in pool after clear" << std::endl;
                cleanup_test_file(filename);
                return false;
            }
        }

        cleanup_test_file(filename);
        return true;
    } catch (const std::exception& e) {
        std::cerr << "    Exception: " << e.what() << std::endl;
        cleanup_test_file(filename);
        return false;
    }
}

// Test 10: Multiple file support
bool test_bufferpool_multiple_files() {
    std::string filename1 = create_test_file(2);
    std::string filename2 = create_test_file(2);

    try {
        auto file1 = DirectIO::open(filename1, true);
        auto file2 = DirectIO::open(filename2, true);

        BufferPool pool(4, true);

        // Load pages from both files
        PageId page1_0(filename1, 0);
        PageId page1_1(filename1, Page::PAGE_SIZE);
        PageId page2_0(filename2, 0);
        PageId page2_1(filename2, Page::PAGE_SIZE);

        Page* p1 = pool.get_page(page1_0, file1.get());
        Page* p2 = pool.get_page(page1_1, file1.get());
        Page* p3 = pool.get_page(page2_0, file2.get());
        Page* p4 = pool.get_page(page2_1, file2.get());

        if (!p1 || !p2 || !p3 || !p4) {
            std::cerr << "    Failed to load pages from multiple files" << std::endl;
            pool.unpin_page(page1_0);
            pool.unpin_page(page1_1);
            pool.unpin_page(page2_0);
            pool.unpin_page(page2_1);
            cleanup_test_file(filename1);
            cleanup_test_file(filename2);
            return false;
        }

        // Verify pages are from correct files
        uint64_t page_num;

        std::memcpy(&page_num, p1->get_data(), sizeof(page_num));
        if (page_num != 0 || p1->get_id().get_filename() != filename1) {
            std::cerr << "    Page 1 verification failed" << std::endl;
            pool.unpin_page(page1_0);
            pool.unpin_page(page1_1);
            pool.unpin_page(page2_0);
            pool.unpin_page(page2_1);
            cleanup_test_file(filename1);
            cleanup_test_file(filename2);
            return false;
        }

        std::memcpy(&page_num, p3->get_data(), sizeof(page_num));
        if (page_num != 0 || p3->get_id().get_filename() != filename2) {
            std::cerr << "    Page 3 verification failed" << std::endl;
            pool.unpin_page(page1_0);
            pool.unpin_page(page1_1);
            pool.unpin_page(page2_0);
            pool.unpin_page(page2_1);
            cleanup_test_file(filename1);
            cleanup_test_file(filename2);
            return false;
        }

        pool.unpin_page(page1_0);
        pool.unpin_page(page1_1);
        pool.unpin_page(page2_0);
        pool.unpin_page(page2_1);

        cleanup_test_file(filename1);
        cleanup_test_file(filename2);
        return true;
    } catch (const std::exception& e) {
        std::cerr << "    Exception: " << e.what() << std::endl;
        cleanup_test_file(filename1);
        cleanup_test_file(filename2);
        return false;
    }
}

// Main test runner
int bufferpool_tests_main() {
    std::cout << "\n=== BufferPool Unit Tests ===" << std::endl;
    std::cout << "=============================" << std::endl;

    std::vector<std::pair<std::string, bool (*)()>> tests = {
        {"Basic construction", test_bufferpool_construction},
        {"Get page functionality", test_bufferpool_get_page},
        {"Statistics tracking", test_bufferpool_statistics},
        {"Pinning and unpinning", test_bufferpool_pin_unpin},
        {"Dirty pages and flushing", test_bufferpool_dirty_pages},
        {"LRU eviction policy", test_bufferpool_lru_eviction},
        {"Clock eviction policy", test_bufferpool_clock_eviction},
        {"Resize buffer pool", test_bufferpool_resize},
        {"Clear buffer pool", test_bufferpool_clear},
        {"Multiple file support", test_bufferpool_multiple_files}
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