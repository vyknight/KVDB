//
// Created by K on 2025-12-06.
//

#ifndef KVDB_PAGEID_H
#define KVDB_PAGEID_H

#include <string>
#include <cstdint>

class PageId
{
public:
    PageId() = default;
    PageId(std::string filename, uint64_t offset);

    const std::string& get_filename() const { return filename_; }
    uint64_t get_offset() const { return offset_; }

    bool operator==(const PageId& other) const;
    bool operator!=(const PageId& other) const;
    bool operator<(const PageId& other) const;

    std::string to_string() const;

private:
    std::string filename_;
    uint64_t offset_ = 0;  // should be 4096 Bytes aligned to match page sizes
};


// hash function for page id
struct PageIdHash
{
    std::size_t operator()(const PageId& id) const;
};

#endif //KVDB_PAGEID_H