//
// Created by Zekun Liu on 2025-12-07.
//

#ifndef KVDB_PAGEID_H
#define KVDB_PAGEID_H

#include <string>
#include <cstdint>
#include <functional>

class PageId {
public:
    PageId() = default;
    PageId(std::string filename, uint64_t offset);

    // Getters
    const std::string& get_filename() const { return filename_; }
    uint64_t get_offset() const { return offset_; }

    // Comparison operators
    bool operator==(const PageId& other) const;
    bool operator!=(const PageId& other) const;
    bool operator<(const PageId& other) const;

    // String representation
    std::string to_string() const;

private:
    std::string filename_;
    uint64_t offset_ = 0;  // Should be 4096-byte aligned
};

// Hash function for PageId to use with std::unordered_map
struct PageIdHash {
    std::size_t operator()(const PageId& id) const;
};

#endif // KVDB_PAGEID_H