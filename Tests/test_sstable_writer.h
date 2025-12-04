//
// Created by K on 2025-12-04.
//

#ifndef KVDB_TEST_SSTABLE_WRITER_H
#define KVDB_TEST_SSTABLE_WRITER_H

#include <string>
#include <cstdint>
#include <vector>

/**
 * SSTable Writer Test Suite
 * Tests for writing SSTable files from Memtables
 */

// Test utilities
void print_test_result_sstable_writer(const std::string& test_name, bool passed);

// Individual test functions
bool test_sstable_write_basic();
bool test_sstable_write_empty_memtable();
bool test_sstable_write_with_tombstones();
bool test_sstable_write_large_data();
bool test_sstable_write_sorted_order();
bool test_sstable_write_file_verification();
bool test_sstable_write_edge_cases();
bool test_sstable_write_performance();

// Helper functions for verification
bool verify_sstable_header(const std::string& filename);
bool verify_sstable_content(const std::string& filename,
                           const std::vector<std::pair<std::string, std::string>>& expected_data);
uint64_t get_sstable_file_size(const std::string& filename);

// Main test runner
int sstable_writer_tests_main();

#endif //KVDB_TEST_SSTABLE_WRITER_H