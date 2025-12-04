//
// Created by K on 2025-12-04.
//

#include "../Memtable.h"
#include <iostream>
#include <cassert>
#include <vector>
#include <string>
#include <random>

void print_test_result(const std::string& test_name, const bool passed)
{
    std::cout << (passed ? "O " : "X ") << test_name << std::endl;
}

// Test 1: Basic put and get
bool test_basic_operations()
{
    Memtable mt(4096);  // 4KB max size

    // Test put and get
    assert(mt.put("key1", "value1"));
    auto val = mt.get("key1");
    if (!val.has_value() || val.value() != "value1") return false;

    val = mt.get("nonexistent");
    if (val.has_value()) return false;

    return true;
}

// Test 2: Update Operations
bool test_updates()
{
    Memtable mt(4096);

    mt.put("key1", "value1");
    mt.put("key1", "value2");

    auto val = mt.get("key1");
    if (!val.has_value() || val.value() != "value2") return false;

    // Size should reflect 1 entry
    if (mt.entry_count() != 1) return false;

    return true;
}

// Test 3: Delete Operations
bool test_deletes()
{
    Memtable mt(4096);

    mt.put("key1", "value1");
    mt.remove("key1");

    // shouldn't find anything
    auto val = mt.get("key1");
    if (val.has_value()) return false;

    if (!mt.is_deleted("key1")) return false;

    if (mt.entry_count() != 1) return false;

    return true;
}

// Test 4: Contains and IsDeleted
bool test_query_methods()
{
    Memtable mt(4096);

    mt.put("key1", "value1");
    mt.put("key2", "value2");
    mt.remove("key2");

    // Test contains
    if (!mt.contains("key1")) return false;
    if (mt.contains("key2")) return false;

    // Test is_deleted
    if (mt.is_deleted("key1")) return false;
    if (!mt.is_deleted("key2")) return false;

    // Test non-existent key
    if (mt.contains("nonexistent")) return false;
    if (mt.is_deleted("nonexistent")) return false;

    return true;
}

// Test 5: Size Tracking
bool test_size_tracking()
{
    Memtable mt(4096);

    if (mt.size() != 0) return false;

    size_t initial_size = mt.size();
    mt.put("key1", "value1");
    if (mt.size() <= initial_size) return false;

    initial_size = mt.size();
    mt.put("key1", "longer value that takes more space");
    if (mt.size() <= initial_size) return false;

    mt.remove("key1");
    if (mt.size() == 0) return false;  // Should still have tombstone

    return true;
}

// Test 6: Flush Trigger
bool test_flush_trigger()
{
    Memtable mt(100);  // very small size

    // Add small entries until flush is triggered
    const std::string SMALL_VALUE = "x";  // 1 byte value

    for (int i = 0; i < 100; i++)
    {
        bool can_continue = mt.put("key" + std::to_string(i), SMALL_VALUE);
        if (!can_continue)
        {
            if (!mt.should_flush()) return false;
            return true;
        }
    }

    return mt.should_flush();
}

// Test 7: Clear Operation
bool test_clear()
{
    Memtable mt(4096);

    // Add some data
    mt.put("key1", "value1");
    mt.put("key2", "value2");
    mt.remove("key1");

    mt.clear();

    if (mt.entry_count() != 0) return false;
    if (mt.size() != 0) return false;

    if (!mt.put("new-key", "new-value")) return false;
    if (mt.entry_count() != 1) return false;

    return true;
}

// Test 8: Get All Entries
bool test_get_all_entries()
{
    Memtable mt(4096);

    // Inserting out of order because get all entries should rearrange
    mt.put("a", "value_a");
    mt.put("c", "value_c");
    mt.put("b", "value_b");
    mt.remove("b");

    const auto entries = mt.get_all_entries();

    // Should have 3 entries (including tombstone)
    if (entries.size() != 3) return false;

    // Should be sorted by key
    if (entries[0].first != "a") return false;
    if (entries[1].first != "b") return false;
    if (entries[2].first != "c") return false;

    // Check tombstone
    if (!entries[1].second.is_deleted) return false;

    return true;
}

// Test 9: Statistics
bool test_statistics()
{
    Memtable mt(4096);

    // Perform operations
    mt.put("key1", "value1");
    mt.put("key2", "value2");
    // only collecting returns because it's marked [[nodiscard]]
    auto v1 = mt.get("key1");
    auto v2 = mt.get("key2");
    auto v3 = mt.get("nonexistent");
    mt.remove("key1");
    mt.clear();  // Should count as flush

    auto stats = mt.get_stats();

    // Verify counts
    if (stats.puts != 2) return false;
    if (stats.gets != 3) return false;
    if (stats.deletes != 1) return false;
    if (stats.flushes != 1) return false;
    if (stats.operations != 7) return false;

    // Test reset
    mt.reset_stats();
    stats = mt.get_stats();
    if (stats.puts != 0 || stats.operations != 0) return false;

    return true;
}

// Test 10: Memory Usage Report
bool test_memory_usage()
{
    Memtable mt(4096);

    mt.put("key1", "value1");
    mt.put("key2", "value2");
    mt.remove("key1");

    auto usage = mt.get_memory_usage();

    // Should have expected keys in report
    if (!usage.contains("estimated_total")) return false;
    if (!usage.contains("alive_entries")) return false;
    if (!usage.contains("tombstones")) return false;

    // Should report correct counts
    if (usage["alive_entries"] != 1) return false;
    if (usage["tombstones"] != 1) return false;

    return true;
}

// Test 11: Iterator functionality
bool test_iterators()
{
    Memtable mt(4096);

    mt.put("c", "value_c");
    mt.put("a", "value_a");
    mt.put("b", "value_b");

    // Test range-based for loop
    size_t count = 0;
    for (auto it = mt.begin(); it != mt.end(); ++it)
    {
        count++;
    }
    if (count != 3) return false;

    // Test manual iteration
    if (const auto it = mt.begin(); it->first != "a") return false;

    return true;
}

// Test 12: Edge cases
bool test_edge_cases()
{
    Memtable mt(4096);

    // Empty KV
    mt.put("", "");
    mt.put("key", "");

    auto val = mt.get("");
    if (!val.has_value()) return false;

    // Very long key/value
    const std::string long_str(1000, 'x');
    mt.put("long_key", long_str);

    val = mt.get("long_key");
    if (!val.has_value() || val.value().length() != 1000) return false;

    // Duplicate deletes
    mt.remove("key");
    mt.remove("key");

    if (!mt.is_deleted("key")) return false;

    return true;
}

// Test 13: configurability
bool test_configurability()
{
    Memtable mt(4096);

    if (mt.get_memtable_size() != 4096) return false;

    mt.set_new_memtable_size(2048);
    if (mt.get_memtable_size() != 2048) return false;

    return true;
}

// Test 14: Stress
bool test_stress()
{
    constexpr int NUM_ENTRIES = 1000;
    Memtable mt(10 * 1024 * 1024);  // 10 MB

    std::vector<std::string> keys;

    // Insert a lot of entries
    for (int i = 0; i < NUM_ENTRIES; i++)
    {
        std::string key = "key_" + std::to_string(i);
        std::string value = "value_" + std::to_string(i * 10);
        mt.put(key, value);
        keys.push_back(key);
    }

    // verify
    for (int i = 0; i < NUM_ENTRIES; i++)
    {
        auto val = mt.get(keys[i]);
        if (!val.has_value()) return false;
        if (val.value() != "value_" + std::to_string(i * 10)) return false;
    }

    // Delete half
    for (int i = 0; i < NUM_ENTRIES; i += 2)
    {
        mt.remove(keys[i]);
    }

    // Verify deletion
    for (int i = 0; i < NUM_ENTRIES; i++)
    {
        const bool should_exist = (i % 2 == 1);
        auto val = mt.get(keys[i]);

        if (should_exist && !val.has_value()) return false;
        if (!should_exist && val.has_value()) return false;
    }

    return true;
}

// Test runner
int memtable_tests_main()
{
    std::cout << "Running MemTable Tests" << std::endl;
    std::cout << "======================" << std::endl;

    std::vector<std::pair<std::string, bool (*)()>> tests = {
        {"Basic Operations", test_basic_operations},
        {"Updates", test_updates},
        {"Deletes", test_deletes},
        {"Query Methods", test_query_methods},
        {"Size Tracking", test_size_tracking},
        {"Flush Trigger", test_flush_trigger},
        {"Clear Operation", test_clear},
        {"Get All Entries", test_get_all_entries},
        {"Statistics", test_statistics},
        {"Memory Usage", test_memory_usage},
        {"Iterators", test_iterators},
        {"Edge Cases", test_edge_cases},
        {"Configurability", test_configurability},
        {"Stress Test", test_stress}
    };

    int passed = 0;

    for (const auto& [name, test_func] : tests)
    {
        try
        {
            const bool result = test_func();
            print_test_result(name, result);
            if (result) passed++;
        } catch (const std::exception& e)
        {
            std::cout << name << " (Exception: " << e.what() << ")" << std::endl;
        } catch (...)
        {
            std::cout << name << " (Unknown exception)" << std::endl;
        }
    }

    std::cout << "\nResults: " << passed << "/" << tests.size() << " tests passed" << std::endl;

    if (passed == tests.size()) {
        std::cout << "\nAll tests passed successfully!" << std::endl;
        return 0;
    } else {
        std::cout << "\nSome tests failed" << std::endl;
        return 1;
    }
}