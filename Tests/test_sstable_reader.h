//
// Created by K on 2025-12-06.
//

#ifndef TEST_SSTABLE_READER_H
#define TEST_SSTABLE_READER_H

#include <string>

/**
 * SSTableReader Test Suite
 * Tests for reading SSTable files with binary search
 */

// Test utilities
void print_test_result_reader(const std::string& test_name, bool passed);

// Individual test functions
bool test_reader_basic_operations();
bool test_reader_tombstones();
bool test_reader_binary_search();
bool test_reader_edge_cases();
bool test_reader_empty_sstable();
bool test_reader_large_sstable();
bool test_reader_min_max_keys();
bool test_reader_file_not_found();
bool test_reader_corrupted_file();
bool test_reader_unsorted_keys();

// Helper functions
bool validate_reader_memory_usage(const std::string& filename);

// Main test runner
int sstable_reader_tests_main();

#endif // TEST_SSTABLE_READER_H