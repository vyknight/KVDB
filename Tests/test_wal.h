//
// Created by K on 2025-12-06.
//

#ifndef KVDB_TEST_WAL_H
#define KVDB_TEST_WAL_H

#include <string>

/**
 * Write-Ahead Log Test Suite
 * Portable tests that work on both Windows and Unix
 */

// Test utilities
void print_test_result_wal(const std::string& test_name, bool passed);

// Individual test functions (no cleanup in tests)
bool test_wal_basic_operations(const std::string& test_dir);
bool test_wal_delete_operations(const std::string& test_dir);
bool test_wal_recovery_scenario(const std::string& test_dir);
bool test_wal_clear_functionality(const std::string& test_dir);
bool test_wal_edge_cases(const std::string& test_dir);
bool test_wal_large_data(const std::string& test_dir);
bool test_wal_concurrent_simulation(const std::string& test_dir);
bool test_wal_corrupted_file(const std::string& test_dir);
bool test_wal_mixed_operations(const std::string& test_dir);
bool test_wal_performance(const std::string& test_dir);
bool test_wal_file_operations(const std::string& test_dir);
bool test_wal_memory_safety(const std::string& test_dir);
bool test_wal_header_integrity(const std::string& test_dir);
bool test_wal_sequential_consistency(const std::string& test_dir);

// Helper to create test file paths
std::string make_test_path(const std::string& test_dir, const std::string& filename);

// Main test runner
int wal_tests_main();

#endif //KVDB_TEST_WAL_H