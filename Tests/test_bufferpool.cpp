//
// Created by K on 2025-12-07.
//

#include "test_bufferpool.h"
#include "../BufferPool.h"
#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <atomic>
#include <random>
#include <filesystem>
#include <fstream>
#include <cstring>

namespace fs = std::filesystem;
using namespace std::chrono;

void print_test_result_bufferpool(const std::string& test_name, bool passed) {
    std::cout << (passed ? "O " : "X ") << test_name << std::endl;
}

// Helper: Create test files
void create_test_file(const std::string& filename, size_t size_in_pages) {
    std::ofstream file(filename, std::ios::binary);
    for (size_t i = 0; i < size_in_pages; ++i) {
        std::vector<char> page(Page::PAGE_SIZE, static_cast<char>('A' + (i % 26)));
        file.write(page.data(), page.size());
    }
}

// Helper: Clean up test files
void cleanup_test_files(const std::vector<std::string>& filenames) {
    for (const auto& filename : filenames) {
        try {
            fs::remove(filename);
        } catch (...) {
            // Ignore
        }
    }
}

// Test 1: Basic buffer pool operations
bool test_bufferpool_basic_operations() {
    std::cout << "Testing BufferPool basic operations..." << std::endl;

    // Create test file
    std::string filename = "test_basic.dat";
    create_test_file(filename, 10);

    BufferPool::Config config;
    config.max_pages = 5;
    config.initial_global_depth = 2;
    config.bucket_capacity = 2;
    config.debug_logging = false;

    BufferPool pool(config);

    // Get some pages
    Page* page1 = pool.get_page(filename, 0);
    Page* page2 = pool.get_page(filename, 4096);
    Page* page3 = pool.get_page(filename, 8192);

    if (!page1 || !page2 || !page3) {
        std::cerr << "  Failed to get pages from buffer pool" << std::endl;
        cleanup_test_files({filename});
        return false;
    }

    // Check that pages have correct IDs
    if (page1->get_id().get_filename() != filename || page1->get_id().get_offset() != 0) {
        std::cerr << "  Page1 ID incorrect" << std::endl;
        pool.release_page(page1);
        pool.release_page(page2);
        pool.release_page(page3);
        cleanup_test_files({filename});
        return false;
    }

    // Check that pages are pinned
    if (!page1->is_pinned() || page1->get_pin_count() != 1) {
        std::cerr << "  Page1 should be pinned" << std::endl;
        pool.release_page(page1);
        pool.release_page(page2);
        pool.release_page(page3);
        cleanup_test_files({filename});
        return false;
    }

    // Mark pages dirty
    pool.mark_dirty(page1);
    pool.mark_dirty(page2);

    if (!page1->is_dirty() || !page2->is_dirty()) {
        std::cerr << "  Pages should be marked dirty" << std::endl;
        pool.release_page(page1);
        pool.release_page(page2);
        pool.release_page(page3);
        cleanup_test_files({filename});
        return false;
    }

    // Release pages
    pool.release_page(page1);
    pool.release_page(page2);
    pool.release_page(page3);

    // Check that pin count decreased
    if (page1->is_pinned()) {
        std::cerr << "  Page1 should not be pinned after release" << std::endl;
        cleanup_test_files({filename});
        return false;
    }

    // Get statistics
    auto stats = pool.get_stats();

    std::cout << "  Hits: " << stats.hits << std::endl;
    std::cout << "  Misses: " << stats.misses << std::endl;
    std::cout << "  Used pages: " << stats.used_pages << std::endl;
    std::cout << "  Directory size: " << stats.directory_size << std::endl;

    if (stats.misses != 3) {  // All three pages should be cache misses
        std::cerr << "  Expected 3 misses, got " << stats.misses << std::endl;
        cleanup_test_files({filename});
        return false;
    }

    // Get same page again (should be a hit)
    Page* page1_again = pool.get_page(filename, 0);
    if (!page1_again) {
        std::cerr << "  Failed to get page1 again" << std::endl;
        cleanup_test_files({filename});
        return false;
    }

    pool.release_page(page1_again);

    stats = pool.get_stats();
    if (stats.hits < 1) {
        std::cerr << "  Expected at least 1 hit, got " << stats.hits << std::endl;
        cleanup_test_files({filename});
        return false;
    }

    cleanup_test_files({filename});
    return true;
}

// Test 2: Extendible hashing - bucket splitting
bool test_bufferpool_extendible_hashing() {
    std::cout << "Testing BufferPool extendible hashing..." << std::endl;

    std::string filename = "test_hashing.dat";
    create_test_file(filename, 20);

    BufferPool::Config config;
    config.max_pages = 20;
    config.initial_global_depth = 1;  // Start with 2 directory entries
    config.bucket_capacity = 2;       // Small bucket to trigger splits
    config.debug_logging = false;

    BufferPool pool(config);

    // Get initial stats
    auto initial_stats = pool.get_stats();
    std::cout << "  Initial directory size: " << initial_stats.directory_size << std::endl;
    std::cout << "  Initial buckets: " << initial_stats.total_buckets << std::endl;

    // Insert pages to trigger splits
    std::vector<Page*> pages;
    for (int i = 0; i < 8; i++) {
        Page* page = pool.get_page(filename, i * 4096);
        if (!page) {
            std::cerr << "  Failed to get page " << i << std::endl;
            for (Page* p : pages) pool.release_page(p);
            cleanup_test_files({filename});
            return false;
        }
        pages.push_back(page);

        // Write some data to make pages different
        char data[50];
        snprintf(data, sizeof(data), "Data for page %d", i);
        page->copy_from(data, strlen(data));
        pool.mark_dirty(page);
    }

    // Release all pages
    for (Page* page : pages) {
        pool.release_page(page);
    }

    // Get final stats
    auto final_stats = pool.get_stats();
    std::cout << "  Final directory size: " << final_stats.directory_size << std::endl;
    std::cout << "  Final buckets: " << final_stats.total_buckets << std::endl;
    std::cout << "  Splits: " << final_stats.splits << std::endl;

    // Verify that splitting occurred
    if (final_stats.total_buckets <= initial_stats.total_buckets) {
        std::cerr << "  Bucket splitting should have increased bucket count" << std::endl;
        cleanup_test_files({filename});
        return false;
    }

    if (final_stats.directory_size <= initial_stats.directory_size) {
        std::cerr << "  Directory should have expanded" << std::endl;
        cleanup_test_files({filename});
        return false;
    }

    // Verify all pages can still be accessed
    for (int i = 0; i < 8; i++) {
        Page* page = pool.get_page(filename, i * 4096);
        if (!page) {
            std::cerr << "  Failed to get page " << i << " after splitting" << std::endl;
            cleanup_test_files({filename});
            return false;
        }
        pool.release_page(page);
    }

    // Print directory structure (debug)
    pool.print_directory();

    cleanup_test_files({filename});
    return true;
}

// Test 3: LRU eviction
bool test_bufferpool_lru_eviction() {
    std::cout << "Testing BufferPool LRU eviction..." << std::endl;

    std::string filename = "test_eviction.dat";
    create_test_file(filename, 10);

    BufferPool::Config config;
    config.max_pages = 3;  // Small pool to force evictions
    config.debug_logging = false;

    BufferPool pool(config);

    // Fill the buffer pool
    Page* page1 = pool.get_page(filename, 0);
    Page* page2 = pool.get_page(filename, 4096);
    Page* page3 = pool.get_page(filename, 8192);

    if (!page1 || !page2 || !page3) {
        std::cerr << "  Failed to get initial pages" << std::endl;
        cleanup_test_files({filename});
        return false;
    }

    auto stats = pool.get_stats();
    if (stats.used_pages != 3) {
        std::cerr << "  Expected 3 used pages, got " << stats.used_pages << std::endl;
        pool.release_page(page1);
        pool.release_page(page2);
        pool.release_page(page3);
        cleanup_test_files({filename});
        return false;
    }

    // Release pages in order: page1, page2, page3
    pool.release_page(page1);
    pool.release_page(page2);
    pool.release_page(page3);

    // Now get a new page - should evict page1 (LRU)
    Page* page4 = pool.get_page(filename, 12288);  // 3 * 4096
    if (!page4) {
        std::cerr << "  Failed to get page4" << std::endl;
        cleanup_test_files({filename});
        return false;
    }

    stats = pool.get_stats();
    if (stats.evictions < 1) {
        std::cerr << "  Expected at least 1 eviction, got " << stats.evictions << std::endl;
        pool.release_page(page4);
        cleanup_test_files({filename});
        return false;
    }

    // page1 should no longer be in cache
    // Try to access it - should be a miss
    Page* page1_again = pool.get_page(filename, 0);
    stats = pool.get_stats();

    if (stats.misses < 4) {  // Initial 3 + page1_again
        std::cerr << "  Expected at least 4 misses, got " << stats.misses << std::endl;
        pool.release_page(page4);
        pool.release_page(page1_again);
        cleanup_test_files({filename});
        return false;
    }

    pool.release_page(page4);
    pool.release_page(page1_again);

    std::cout << "  Evictions: " << stats.evictions << std::endl;
    std::cout << "  Misses: " << stats.misses << std::endl;
    std::cout << "  Hits: " << stats.hits << std::endl;

    cleanup_test_files({filename});
    return true;
}

// Test 4: Concurrent access
bool test_bufferpool_concurrent_access() {
    std::cout << "Testing BufferPool concurrent access..." << std::endl;

    std::string filename = "test_concurrent.dat";
    create_test_file(filename, 100);

    BufferPool::Config config;
    config.max_pages = 20;
    config.debug_logging = false;

    BufferPool pool(config);

    const int NUM_THREADS = 4;
    const int OPERATIONS_PER_THREAD = 100;

    std::vector<std::thread> threads;
    std::atomic<int> successful_operations{0};
    std::atomic<int> failed_operations{0};

    auto worker = [&](int thread_id) {
        std::mt19937 rng(thread_id);
        std::uniform_int_distribution<int> page_dist(0, 99);
        std::uniform_int_distribution<int> op_dist(0, 2);

        for (int i = 0; i < OPERATIONS_PER_THREAD; ++i) {
            int page_num = page_dist(rng);
            uint64_t offset = page_num * 4096;

            try {
                Page* page = pool.get_page(filename, offset);
                if (!page) {
                    failed_operations++;
                    continue;
                }

                // Random operation
                int op = op_dist(rng);
                switch (op) {
                    case 0:  // Read
                        {
                            char buffer[100];
                            page->copy_to(buffer, 100);
                        }
                        break;
                    case 1:  // Write
                        {
                            char data[100] = "Thread data";
                            page->copy_from(data, sizeof(data));
                            pool.mark_dirty(page);
                        }
                        break;
                    case 2:  // Just pin/unpin
                        break;
                }

                pool.release_page(page);
                successful_operations++;

            } catch (const std::exception& e) {
                failed_operations++;
            }
        }
    };

    auto start = high_resolution_clock::now();

    for (int i = 0; i < NUM_THREADS; ++i) {
        threads.emplace_back(worker, i);
    }

    for (auto& thread : threads) {
        thread.join();
    }

    auto end = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(end - start);

    int total_operations = NUM_THREADS * OPERATIONS_PER_THREAD;

    std::cout << "  " << total_operations << " operations in "
              << duration.count() << " ms" << std::endl;
    std::cout << "  Successful: " << successful_operations
              << ", Failed: " << failed_operations << std::endl;
    std::cout << "  " << (total_operations * 1000.0 / duration.count())
              << " ops/sec" << std::endl;

    // Get final statistics
    auto stats = pool.get_stats();
    std::cout << "  Hits: " << stats.hits << std::endl;
    std::cout << "  Misses: " << stats.misses << std::endl;
    std::cout << "  Disk reads: " << stats.disk_reads << std::endl;

    cleanup_test_files({filename});

    // Require high success rate
    if (successful_operations < total_operations * 0.95) {
        std::cerr << "  Success rate too low: "
                  << (successful_operations * 100.0 / total_operations) << "%" << std::endl;
        return false;
    }

    return true;
}

// Test 5: Flush operations
bool test_bufferpool_flush_operations() {
    std::cout << "Testing BufferPool flush operations..." << std::endl;

    std::string filename = "test_flush.dat";
    create_test_file(filename, 5);

    BufferPool::Config config;
    config.max_pages = 10;
    config.debug_logging = false;

    BufferPool pool(config);

    // Get pages and mark them dirty
    std::vector<Page*> pages;
    for (int i = 0; i < 5; i++) {
        Page* page = pool.get_page(filename, i * 4096);
        if (!page) {
            std::cerr << "  Failed to get page " << i << std::endl;
            for (Page* p : pages) pool.release_page(p);
            cleanup_test_files({filename});
            return false;
        }

        // Write unique data to each page
        char data[50];
        snprintf(data, sizeof(data), "Modified data for page %d", i);
        page->copy_from(data, strlen(data));
        pool.mark_dirty(page);

        pages.push_back(page);
    }

    // Flush individual page
    pool.flush_page(pages[0]);
    if (pages[0]->is_dirty()) {
        std::cerr << "  Page should not be dirty after flush" << std::endl;
        for (Page* page : pages) pool.release_page(page);
        cleanup_test_files({filename});
        return false;
    }

    // Flush all pages
    pool.flush_all();

    // Check that no pages are dirty
    for (Page* page : pages) {
        if (page->is_dirty()) {
            std::cerr << "  Page still dirty after flush_all" << std::endl;
            for (Page* p : pages) pool.release_page(p);
            cleanup_test_files({filename});
            return false;
        }
        pool.release_page(page);
    }

    // Verify data was actually written by reading it back
    for (int i = 0; i < 5; i++) {
        Page* page = pool.get_page(filename, i * 4096);
        if (!page) {
            std::cerr << "  Failed to get page " << i << " for verification" << std::endl;
            cleanup_test_files({filename});
            return false;
        }

        char buffer[50];
        page->copy_to(buffer, 50);

        char expected[50];
        snprintf(expected, sizeof(expected), "Modified data for page %d", i);

        if (std::strncmp(buffer, expected, strlen(expected)) != 0) {
            std::cerr << "  Data not properly flushed for page " << i << std::endl;
            std::cerr << "  Expected: " << expected << std::endl;
            std::cerr << "  Got: " << buffer << std::endl;
            pool.release_page(page);
            cleanup_test_files({filename});
            return false;
        }

        pool.release_page(page);
    }

    cleanup_test_files({filename});
    return true;
}

// Test 6: Clear and resize operations
bool test_bufferpool_clear_resize() {
    std::cout << "Testing BufferPool clear and resize operations..." << std::endl;

    std::string filename = "test_clear.dat";
    create_test_file(filename, 10);

    BufferPool::Config config;
    config.max_pages = 5;
    config.debug_logging = false;

    BufferPool pool(config);

    // Fill pool with dirty pages
    for (int i = 0; i < 5; i++) {
        Page* page = pool.get_page(filename, i * 4096);
        if (!page) {
            std::cerr << "  Failed to get page " << i << std::endl;
            cleanup_test_files({filename});
            return false;
        }

        char data[50];
        snprintf(data, sizeof(data), "Data before clear %d", i);
        page->copy_from(data, strlen(data));
        pool.mark_dirty(page);

        pool.release_page(page);
    }

    // Clear the pool
    pool.clear();

    auto stats = pool.get_stats();
    if (stats.used_pages != 0) {
        std::cerr << "  Pool should be empty after clear, got " << stats.used_pages << " pages" << std::endl;
        cleanup_test_files({filename});
        return false;
    }

    // Resize the pool
    pool.resize(10);

    // Fill with more pages than before
    for (int i = 0; i < 10; i++) {
        Page* page = pool.get_page(filename, i * 4096);
        if (!page) {
            std::cerr << "  Failed to get page " << i << " after resize" << std::endl;
            cleanup_test_files({filename});
            return false;
        }
        pool.release_page(page);
    }

    stats = pool.get_stats();
    if (stats.used_pages != 10) {
        std::cerr << "  Expected 10 pages after resize, got " << stats.used_pages << std::endl;
        cleanup_test_files({filename});
        return false;
    }

    // Try to resize smaller (should trigger evictions)
    pool.resize(5);

    // Get more pages to force evictions
    for (int i = 10; i < 15; i++) {
        Page* page = pool.get_page(filename, i * 4096);
        if (!page) {
            std::cerr << "  Failed to get page " << i << " after downsizing" << std::endl;
            cleanup_test_files({filename});
            return false;
        }
        pool.release_page(page);
    }

    stats = pool.get_stats();
    if (stats.evictions == 0) {
        std::cerr << "  Expected evictions after downsizing" << std::endl;
        cleanup_test_files({filename});
        return false;
    }

    std::cout << "  Evictions after resize: " << stats.evictions << std::endl;

    cleanup_test_files({filename});
    return true;
}

// Test 7: Statistics tracking
bool test_bufferpool_statistics() {
    std::cout << "Testing BufferPool statistics tracking..." << std::endl;

    std::string filename = "test_stats.dat";
    create_test_file(filename, 10);

    BufferPool::Config config;
    config.max_pages = 5;
    config.debug_logging = false;

    BufferPool pool(config);

    // Reset statistics
    pool.reset_stats();

    auto initial_stats = pool.get_stats();
    if (initial_stats.hits != 0 || initial_stats.misses != 0 || initial_stats.evictions != 0) {
        std::cerr << "  Statistics not properly reset" << std::endl;
        cleanup_test_files({filename});
        return false;
    }

    // Perform operations and track statistics
    Page* page1 = pool.get_page(filename, 0);      // Miss
    Page* page2 = pool.get_page(filename, 4096);   // Miss
    Page* page1_again = pool.get_page(filename, 0); // Hit

    auto stats = pool.get_stats();

    std::cout << "  After operations:" << std::endl;
    std::cout << "    Hits: " << stats.hits << std::endl;
    std::cout << "    Misses: " << stats.misses << std::endl;
    std::cout << "    Disk reads: " << stats.disk_reads << std::endl;
    std::cout << "    Used pages: " << stats.used_pages << std::endl;

    if (stats.misses != 2) {
        std::cerr << "  Expected 2 misses, got " << stats.misses << std::endl;
        pool.release_page(page1);
        pool.release_page(page2);
        pool.release_page(page1_again);
        cleanup_test_files({filename});
        return false;
    }

    if (stats.hits < 1) {
        std::cerr << "  Expected at least 1 hit, got " << stats.hits << std::endl;
        pool.release_page(page1);
        pool.release_page(page2);
        pool.release_page(page1_again);
        cleanup_test_files({filename});
        return false;
    }

    if (stats.disk_reads != 2) {
        std::cerr << "  Expected 2 disk reads, got " << stats.disk_reads << std::endl;
        pool.release_page(page1);
        pool.release_page(page2);
        pool.release_page(page1_again);
        cleanup_test_files({filename});
        return false;
    }

    // Mark page dirty and flush
    pool.mark_dirty(page1);
    pool.flush_page(page1);

    stats = pool.get_stats();
    if (stats.disk_writes < 1) {
        std::cerr << "  Expected at least 1 disk write, got " << stats.disk_writes << std::endl;
        pool.release_page(page1);
        pool.release_page(page2);
        pool.release_page(page1_again);
        cleanup_test_files({filename});
        return false;
    }

    // Fill pool to force eviction
    Page* page3 = pool.get_page(filename, 8192);
    Page* page4 = pool.get_page(filename, 12288);
    Page* page5 = pool.get_page(filename, 16384);

    // This should trigger eviction
    Page* page6 = pool.get_page(filename, 20480);

    stats = pool.get_stats();
    if (stats.evictions < 1) {
        std::cerr << "  Expected at least 1 eviction, got " << stats.evictions << std::endl;
    }

    // Release all pages
    pool.release_page(page1);
    pool.release_page(page2);
    pool.release_page(page1_again);
    pool.release_page(page3);
    pool.release_page(page4);
    pool.release_page(page5);
    pool.release_page(page6);

    std::cout << "  Final statistics:" << std::endl;
    std::cout << "    Hits: " << stats.hits << std::endl;
    std::cout << "    Misses: " << stats.misses << std::endl;
    std::cout << "    Evictions: " << stats.evictions << std::endl;
    std::cout << "    Disk reads: " << stats.disk_reads << std::endl;
    std::cout << "    Disk writes: " << stats.disk_writes << std::endl;

    cleanup_test_files({filename});
    return true;
}

// Test 8: Multiple files
bool test_bufferpool_multiple_files() {
    std::cout << "Testing BufferPool with multiple files..." << std::endl;

    // Create multiple test files
    std::vector<std::string> filenames;
    for (int i = 0; i < 3; i++) {
        std::string filename = "test_multi_" + std::to_string(i) + ".dat";
        create_test_file(filename, 5);
        filenames.push_back(filename);
    }

    BufferPool::Config config;
    config.max_pages = 10;
    config.debug_logging = false;

    BufferPool pool(config);

    // Access pages from different files
    std::vector<Page*> pages;

    for (int file_idx = 0; file_idx < 3; file_idx++) {
        for (int page_idx = 0; page_idx < 3; page_idx++) {
            uint64_t offset = page_idx * 4096;
            Page* page = pool.get_page(filenames[file_idx], offset);

            if (!page) {
                std::cerr << "  Failed to get page " << page_idx
                          << " from file " << file_idx << std::endl;
                for (Page* p : pages) pool.release_page(p);
                cleanup_test_files(filenames);
                return false;
            }

            // Write file-specific data
            char data[50];
            snprintf(data, sizeof(data), "File %d, Page %d", file_idx, page_idx);
            page->copy_from(data, strlen(data));
            pool.mark_dirty(page);

            pages.push_back(page);
        }
    }

    // Verify data
    for (int i = 0; i < pages.size(); i++) {
        int file_idx = i / 3;
        int page_idx = i % 3;

        char expected[50];
        snprintf(expected, sizeof(expected), "File %d, Page %d", file_idx, page_idx);

        char buffer[50];
        pages[i]->copy_to(buffer, 50);

        if (std::strncmp(buffer, expected, strlen(expected)) != 0) {
            std::cerr << "  Data mismatch for page " << i << std::endl;
            std::cerr << "  Expected: " << expected << std::endl;
            std::cerr << "  Got: " << buffer << std::endl;
            for (Page* p : pages) pool.release_page(p);
            cleanup_test_files(filenames);
            return false;
        }

        pool.release_page(pages[i]);
    }

    // Check that pool can handle files being closed/reopened
    auto stats = pool.get_stats();
    std::cout << "  Total pages accessed: " << stats.total_pages << std::endl;
    std::cout << "  Directory size: " << stats.directory_size << std::endl;
    std::cout << "  Buckets: " << stats.total_buckets << std::endl;

    cleanup_test_files(filenames);
    return true;
}

// Test 9: Performance test
bool test_bufferpool_performance() {
    std::cout << "Testing BufferPool performance..." << std::endl;

    std::string filename = "test_perf.dat";
    const int NUM_PAGES = 1000;
    create_test_file(filename, NUM_PAGES);

    BufferPool::Config config;
    config.max_pages = 100;  // Smaller than file size to force evictions
    config.debug_logging = false;

    BufferPool pool(config);

    const int OPERATIONS = 10000;
    std::mt19937 rng(42);
    std::uniform_int_distribution<int> page_dist(0, NUM_PAGES - 1);
    std::uniform_int_distribution<int> op_dist(0, 3);

    auto start = high_resolution_clock::now();

    for (int i = 0; i < OPERATIONS; i++) {
        int page_num = page_dist(rng);
        uint64_t offset = page_num * 4096;

        Page* page = pool.get_page(filename, offset);
        if (!page) {
            std::cerr << "  Failed to get page in performance test" << std::endl;
            cleanup_test_files({filename});
            return false;
        }

        int op = op_dist(rng);
        switch (op) {
            case 0:  // Simple read
                {
                    char buffer[100];
                    page->copy_to(buffer, 100);
                }
                break;
            case 1:  // Write
                {
                    char data[100];
                    snprintf(data, sizeof(data), "Op %d, Page %d", i, page_num);
                    page->copy_from(data, strlen(data));
                    pool.mark_dirty(page);
                }
                break;
            case 2:  // Read larger chunk
                {
                    char buffer[1024];
                    page->copy_to(buffer, 1024);
                }
                break;
            case 3:  // Multiple pins/unpins
                page->pin();
                page->unpin();
                break;
        }

        pool.release_page(page);
    }

    auto end = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(end - start);

    auto stats = pool.get_stats();

    std::cout << "  " << OPERATIONS << " operations in "
              << duration.count() << " ms" << std::endl;
    std::cout << "  " << (OPERATIONS * 1000.0 / duration.count())
              << " ops/sec" << std::endl;
    std::cout << "  Hit rate: "
              << (stats.hits * 100.0 / (stats.hits + stats.misses)) << "%" << std::endl;
    std::cout << "  Evictions: " << stats.evictions << std::endl;
    std::cout << "  Disk reads: " << stats.disk_reads << std::endl;
    std::cout << "  Disk writes: " << stats.disk_writes << std::endl;

    cleanup_test_files({filename});
    return true;
}

// Test 10: Integration with SSTableReader (simulated)
bool test_bufferpool_integration() {
    std::cout << "Testing BufferPool integration (simulated)..." << std::endl;

    // Create a simulated SSTable file
    std::string sst_filename = "test_integration.sst";

    // Write a simple SSTable format
    std::ofstream file(sst_filename, std::ios::binary);

    // Write some pages with data
    const int NUM_PAGES = 10;
    for (int i = 0; i < NUM_PAGES; i++) {
        std::vector<char> page(Page::PAGE_SIZE);
        std::string page_data = "Page " + std::to_string(i) + " data";
        std::copy(page_data.begin(), page_data.end(), page.begin());
        file.write(page.data(), page.size());
    }

    BufferPool::Config config;
    config.max_pages = 5;
    config.debug_logging = false;

    BufferPool pool(config);

    // Simulate SSTableReader using buffer pool
    std::cout << "  Simulating SSTableReader operations..." << std::endl;

    // Random access pattern (simulating binary search)
    std::mt19937 rng(123);
    std::uniform_int_distribution<int> dist(0, NUM_PAGES - 1);

    int hits = 0;
    int misses = 0;

    for (int i = 0; i < 100; i++) {
        int page_num = dist(rng);
        uint64_t offset = page_num * Page::PAGE_SIZE;

        Page* page = pool.get_page(sst_filename, offset);
        if (!page) {
            std::cerr << "  Failed to get page " << page_num << std::endl;
            cleanup_test_files({sst_filename});
            return false;
        }

        // Simulate reading key-value data from page
        char buffer[100];
        page->copy_to(buffer, 50, 0);  // Read first 50 bytes

        // Verify we got expected data (simplified)
        std::string expected_prefix = "Page " + std::to_string(page_num) + " ";
        if (std::strncmp(buffer, expected_prefix.c_str(), expected_prefix.length()) != 0) {
            std::cerr << "  Data verification failed for page " << page_num << std::endl;
            pool.release_page(page);
            cleanup_test_files({sst_filename});
            return false;
        }

        pool.release_page(page);
    }

    auto stats = pool.get_stats();
    std::cout << "  Simulation results:" << std::endl;
    std::cout << "    Cache hits: " << stats.hits << std::endl;
    std::cout << "    Cache misses: " << stats.misses << std::endl;
    std::cout << "    Hit rate: "
              << (stats.hits * 100.0 / (stats.hits + stats.misses)) << "%" << std::endl;

    // Print directory structure
    std::cout << "  Final buffer pool state:" << std::endl;
    pool.print_directory();

    cleanup_test_files({sst_filename});
    return true;
}

// Main test runner
int bufferpool_tests_main() {
    std::cout << "\n=== BufferPool Unit Tests ===" << std::endl;
    std::cout << "============================" << std::endl;

    std::vector<std::pair<std::string, bool (*)()>> tests = {
        {"Basic operations", test_bufferpool_basic_operations},
        {"Extendible hashing", test_bufferpool_extendible_hashing},
        {"LRU eviction", test_bufferpool_lru_eviction},
        {"Concurrent access", test_bufferpool_concurrent_access},
        {"Flush operations", test_bufferpool_flush_operations},
        {"Clear and resize", test_bufferpool_clear_resize},
        {"Statistics tracking", test_bufferpool_statistics},
        {"Multiple files", test_bufferpool_multiple_files},
        {"Performance test", test_bufferpool_performance},
        {"Integration (simulated)", test_bufferpool_integration}
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