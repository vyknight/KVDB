//
// Created by Zekun Liu on 2025-12-07.
//

#ifndef KVDB_TEST_HELPER_H
#define KVDB_TEST_HELPER_H

#include <filesystem>

namespace fs = std::filesystem;
using namespace std::chrono;

inline void print_test_result(const std::string& test_name, bool passed) {
    std::cout << (passed ? "O " : "X ") << test_name << std::endl;
}

// Helper to create test paths
inline std::string make_test_path(const std::string& test_dir, const std::string& filename) {
    return (fs::path(test_dir) / filename).string();
}

#endif //KVDB_TEST_HELPER_H