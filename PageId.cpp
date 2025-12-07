//
// Created by Zekun Liu on 2025-12-07.
//

#include "PageId.h"
#include <algorithm>
#include <sstream>

PageId::PageId(std::string filename, uint64_t offset)
    : filename_(std::move(filename)), offset_(offset) {
    // Ensure offset is page-aligned (multiple of 4096)
    if (offset_ % 4096 != 0) {
        offset_ = (offset_ / 4096) * 4096;
    }
}

bool PageId::operator==(const PageId& other) const {
    return filename_ == other.filename_ && offset_ == other.offset_;
}

bool PageId::operator!=(const PageId& other) const {
    return !(*this == other);
}

bool PageId::operator<(const PageId& other) const {
    if (filename_ != other.filename_) {
        return filename_ < other.filename_;
    }
    return offset_ < other.offset_;
}

std::string PageId::to_string() const {
    std::ostringstream oss;
    oss << filename_ << ":" << offset_;
    return oss.str();
}

std::size_t PageIdHash::operator()(const PageId& id) const {
    // Combine hash of filename and offset
    std::size_t h1 = std::hash<std::string>{}(id.get_filename());
    std::size_t h2 = std::hash<uint64_t>{}(id.get_offset());

    // From boost::hash_combine
    return h1 ^ (h2 + 0x9e3779b9 + (h1 << 6) + (h1 >> 2));
}