#ifndef TEST_MEMTABLE_H
#define TEST_MEMTABLE_H

#include <string>

// Test utilities
void print_test_result(const std::string& test_name, bool passed);

// Test functions
bool test_basic_operations();
bool test_updates();
bool test_deletes();
bool test_query_methods();
bool test_size_tracking();
bool test_flush_trigger();
bool test_clear();
bool test_get_all_entries();
bool test_statistics();
bool test_memory_usage();
bool test_iterators();
bool test_edge_cases();
bool test_configurability();
bool test_stress();

// Main test runner
int memtable_tests_main();

#endif // TEST_MEMTABLE_H