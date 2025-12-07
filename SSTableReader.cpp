//
// Created by K on 2025-12-04.
//

#include "SSTableReader.h"
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <algorithm>
#include <utility>

// Constants matching SSTableWriter format
namespace {
    constexpr uint64_t EXPECTED_MAGIC = 0x4B5644425F535354ULL;  // "KVDB_SST"
    constexpr uint32_t EXPECTED_VERSION = 1;
    constexpr size_t HEADER_SIZE = 24;  // magic(8) + version(4) + entry_count(4) + data_offset(8)
}

SSTableReader::SSTableReader(std::string  filename)
    : filename_(std::move(filename)), value_data_size_(0), valid_(false) {
    valid_ = load();
}

// Destructor
SSTableReader::~SSTableReader() = default;

// Move constructor
SSTableReader::SSTableReader(SSTableReader&& other) noexcept
    : filename_(std::move(other.filename_)),
      key_entries_(std::move(other.key_entries_)),
      value_data_(std::move(other.value_data_)),
      value_data_size_(other.value_data_size_),
      valid_(other.valid_) {
    other.valid_ = false;
    other.value_data_size_ = 0;
}

// Move assignment operator
SSTableReader& SSTableReader::operator=(SSTableReader&& other) noexcept {
    if (this != &other) {
        filename_ = std::move(other.filename_);
        key_entries_ = std::move(other.key_entries_);
        value_data_ = std::move(other.value_data_);
        value_data_size_ = other.value_data_size_;
        valid_ = other.valid_;

        other.valid_ = false;
        other.value_data_size_ = 0;
    }
    return *this;
}

// Load SSTable
bool SSTableReader::load()
{
    std::ifstream file(filename_, std::ios::binary | std::ios::ate);
    if (!file)
    {
        std::cerr << "Cannot open SSTable file: " << filename_ << std::endl;
        return false;
    }

    try
    {
        // Get file size
        std::streamsize file_size = file.tellg();
        file.seekg(0);

        if (file_size < HEADER_SIZE)
        {
            std::cerr << "File too small for SSTable header: " << filename_ << std::endl;
            return false;
        }

        // Read header
        uint64_t magic;
        uint32_t version;
        uint32_t entry_count;
        uint64_t data_offset;

        file.read(reinterpret_cast<char*>(&magic), sizeof(magic));
        file.read(reinterpret_cast<char*>(&version), sizeof(version));
        file.read(reinterpret_cast<char*>(&entry_count), sizeof(entry_count));
        file.read(reinterpret_cast<char*>(&data_offset), sizeof(data_offset));

        if (magic != EXPECTED_MAGIC)
        {
            std::cerr << "Invalid magic number in SSTable header: " << filename_ << std::endl;
            return false;
        }

        if (version != EXPECTED_VERSION)
        {
            std::cerr << "Unsupported SSTable version in " << filename_
                      << ": expected " << EXPECTED_VERSION << ", got " << version << std::endl;
            return false;
        }

        if (data_offset > static_cast<uint64_t>(file_size))
        {
            std::cerr << "Invalid data offset in " << filename_
                      << ": " << data_offset << " >= " << file_size << std::endl;
            return false;
        }

        // Reserve space for key entries
        key_entries_.reserve(entry_count);

        // calculate expected position after reading all keys
        uint64_t current_pos = HEADER_SIZE;

        // read key directory
        for (uint32_t i = 0; i < entry_count; ++i)
        {
            KeyEntry entry;

            // read key length
            uint32_t key_len;
            if (!file.read(reinterpret_cast<char*>(&key_len), sizeof(key_len)))
            {
                std::cerr << "Failed to read key length at entry " << i << std::endl;
                return false;
            }
            current_pos += sizeof(key_len);

            // Validate key length
            if (key_len > 1024 * 1024) {  // Sanity check: 1MB max key size
                std::cerr << "Key too large at entry " << i << ": " << key_len << " bytes" << std::endl;
                return false;
            }

            // Read key
            entry.key.resize(key_len);
            if (!file.read(entry.key.data(), key_len)) {
                std::cerr << "Failed to read key at entry " << i << std::endl;
                return false;
            }

            current_pos += key_len;

            // Read value offset
            if (!file.read(reinterpret_cast<char*>(&entry.value_offset), sizeof(entry.value_offset))) {
                std::cerr << "Failed to read value offset at entry " << i << std::endl;
                return false;
            }
            current_pos += sizeof(entry.value_offset);

            // Read value length
            if (!file.read(reinterpret_cast<char*>(&entry.value_length), sizeof(entry.value_length))) {
                std::cerr << "Failed to read value length at entry " << i << std::endl;
                return false;
            }
            current_pos += sizeof(entry.value_length);

            // Read tombstone flag
            uint8_t tombstone;
            if (!file.read(reinterpret_cast<char*>(&tombstone), sizeof(tombstone))) {
                std::cerr << "Failed to read tombstone flag at entry " << i << std::endl;
                return false;
            }
            entry.is_deleted = (tombstone != 0);
            current_pos += sizeof(tombstone);

            // Verify value offset is within file
            if (entry.value_offset + entry.value_length > static_cast<uint64_t>(file_size)) {
                std::cerr << "Value offset/length out of bounds at entry " << i
                          << ": offset=" << entry.value_offset
                          << ", length=" << entry.value_length
                          << ", file_size=" << file_size << std::endl;
                return false;
            }

            key_entries_.push_back(std::move(entry));
        }

        // verify we're at data section
        if (static_cast<uint64_t>(file.tellg()) != data_offset) {
            std::cerr << "Key directory size mismatch in " << filename_
                      << ": expected data offset " << data_offset
                      << ", but at position " << file.tellg() << std::endl;
            return false;
        }

        // load data section into memory
        // Load data section into memory
        value_data_size_ = file_size - data_offset;
        value_data_ = std::make_unique<char[]>(value_data_size_);

        if (!file.read(value_data_.get(), value_data_size_)) {
            std::cerr << "Failed to read data section in " << filename_ << std::endl;
            return false;
        }

        // Verify index is sorted (should be from SSTableWriter)
        for (size_t i = 1; i < key_entries_.size(); ++i) {
            if (key_entries_[i - 1].key >= key_entries_[i].key) {
                std::cerr << "Keys are not sorted in SSTable " << filename_
                          << ": '" << key_entries_[i - 1].key
                          << "' >= '" << key_entries_[i].key << "'" << std::endl;
                return false;
            }
        }

        return true;
    } catch (const std::exception& e)
    {
        std::cerr << "Exception while loading SSTable " << filename_ << ": " << e.what() << std::endl;
        return false;
    }
}

// Binary search implementation
int SSTableReader::binary_search(const std::string& key) const
{
    if (key_entries_.empty())
    {
        return -1;
    }

    int left = 0;
    int right = static_cast<int>(key_entries_.size()) - 1;

    while (left <= right) {
        int mid = left + (right - left) / 2;
        const auto& mid_key = key_entries_[mid].key;

        if (mid_key == key) {
            return mid;
        } else if (mid_key < key) {
            left = mid + 1;
        } else {
            right = mid - 1;
        }
    }

    return -1;  // Not found
}

// Get value for key
std::optional<std::string> SSTableReader::get(const std::string& key) const {
    if (!valid_) {
        return std::nullopt;
    }

    int idx = binary_search(key);
    if (idx == -1) {
        return std::nullopt;  // Key not found
    }

    const auto& entry = key_entries_[idx];
    if (entry.is_deleted) {
        return std::nullopt;  // Key is deleted (tombstone)
    }

    return read_value(entry);
}

// Check if key exists and is not deleted
bool SSTableReader::contains(const std::string& key) const {
    if (!valid_) return false;

    int idx = binary_search(key);
    return idx != -1 && !key_entries_[idx].is_deleted;
}

// Check if key is marked as deleted
bool SSTableReader::is_deleted(const std::string& key) const {
    if (!valid_) return false;

    int idx = binary_search(key);
    return idx != -1 && key_entries_[idx].is_deleted;
}

// Get number of entries
size_t SSTableReader::size() const {
    return key_entries_.size();
}

// Get filename
const std::string& SSTableReader::get_filename() const {
    return filename_;
}

// Check if SSTable is valid
bool SSTableReader::is_valid() const {
    return valid_;
}

// Get approximate memory usage
size_t SSTableReader::memory_usage() const {
    size_t total = 0;

    // Key entries memory
    for (const auto& entry : key_entries_) {
        total += sizeof(KeyEntry) + entry.key.capacity();
    }

    // Value data memory
    total += value_data_size_;

    return total;
}

// Get all keys (for debugging/testing)
std::vector<std::string> SSTableReader::get_all_keys() const {
    std::vector<std::string> keys;
    keys.reserve(key_entries_.size());

    for (const auto& entry : key_entries_) {
        keys.push_back(entry.key);
    }

    return keys;
}

// Get minimum key
std::string SSTableReader::min_key() const {
    if (key_entries_.empty()) {
        return "";
    }
    return key_entries_.front().key;
}

// Get maximum key
std::string SSTableReader::max_key() const {
    if (key_entries_.empty()) {
        return "";
    }
    return key_entries_.back().key;
}

// range scan of keys
std::vector<std::pair<std::string, std::string>>
SSTableReader::scan_range(const std::string& start_key, const std::string& end_key) const {
    std::vector<std::pair<std::string, std::string>> results;

    if (!valid_ || key_entries_.empty()) return results;

    // Find first key >= start_key
    int left = 0;
    int right = static_cast<int>(key_entries_.size()) - 1;
    int start_idx = -1;

    // Binary search for lower bound (first key >= start_key)
    while (left <= right) {
        int mid = left + (right - left) / 2;
        const std::string& mid_key = key_entries_[mid].key;

        if (mid_key >= start_key) {
            start_idx = mid;
            right = mid - 1;  // Look for earlier occurrence
        } else {
            left = mid + 1;
        }
    }

    // If no key >= start_key found, return empty
    if (start_idx == -1) {
        return results;
    }

    // Scan from start_idx until key > end_key (or end of array)
    for (int i = start_idx; i < static_cast<int>(key_entries_.size()); ++i) {
        const auto& entry = key_entries_[i];

        // Stop if we've passed the end of the range
        if (entry.key > end_key) {
            break;
        }

        // Skip deleted entries
        if (!entry.is_deleted) {
            results.emplace_back(entry.key, read_value(entry));
        }
    }

    return results;
}

std::string SSTableReader::read_value(const KeyEntry& entry) const
{
    if (buffer_pool_)
    {
        return read_value_with_buffer_pool(entry);
    }

    if (!value_data_ || key_entries_.empty()) {
        throw std::runtime_error("SSTable data not properly loaded");
    }

    // Find the minimum value offset (should be data_offset)
    // We need this to compute buffer-relative offset
    uint64_t min_offset = key_entries_[0].value_offset;
    for (const auto& e : key_entries_) {
        if (e.value_offset < min_offset) {
            min_offset = e.value_offset;
        }
    }

    // Compute buffer offset: absolute file offset - start of data section
    uint64_t buffer_offset = entry.value_offset - min_offset;

    // Validate bounds
    if (buffer_offset + entry.value_length > value_data_size_) {
        std::string msg = "Value offset out of bounds: offset=";
        msg += std::to_string(buffer_offset) + ", length=";
        msg += std::to_string(entry.value_length) + ", buffer_size=";
        msg += std::to_string(value_data_size_);
        throw std::runtime_error(msg);
    }

    // Extract value from buffer
    return std::string(value_data_.get() + buffer_offset, entry.value_length);
}

// Buffer Pool Implementation

std::unique_ptr<BufferPool> SSTableReader::buffer_pool_ = nullptr;

std::string SSTableReader::read_value_with_buffer_pool(const KeyEntry& entry) const
{
    if (!buffer_pool_)
    {
        // use non buffered reads
        return read_value(entry);
    }

    // calculate the page that would contain this value
    uint64_t page_offset = (entry.value_offset / Page::PAGE_SIZE) * Page::PAGE_SIZE;
    uint64_t offset_in_page = entry.value_offset - page_offset;

    // Get page from buffer pool
    Page* page = buffer_pool_->get_page(filename_, page_offset);
    if (!page) {
        throw std::runtime_error("Failed to get page from buffer pool");
    }

    // Extract value from page
    std::string value(entry.value_length, '\0');
    page->copy_to(&value[0], entry.value_length, offset_in_page);

    // Release page (decrement pin count)
    buffer_pool_->release_page(page);

    return value;
}