//
// Created by K on 2025-12-06.
//

#include "WriteAheadLog.h"
#include <iostream>
#include <stdexcept>
#include <algorithm>
#include <filesystem>

namespace fs = std::filesystem;

WriteAheadLog::WriteAheadLog(const std::string& filename)
    : filename_(filename) {
    if (!open_file()) {
        throw std::runtime_error("Failed to open WAL file: " + filename);
    }
}

WriteAheadLog::~WriteAheadLog() {
    if (file_.is_open()) {
        file_.close();
    }
}

// Move constructor
WriteAheadLog::WriteAheadLog(WriteAheadLog&& other) noexcept
    : filename_(std::move(other.filename_)) {
    file_ = std::move(other.file_);
}

// Move assignment
WriteAheadLog& WriteAheadLog::operator=(WriteAheadLog&& other) noexcept {
    if (this != &other) {
        if (file_.is_open()) {
            file_.close();
        }
        filename_ = std::move(other.filename_);
        file_ = std::move(other.file_);
    }
    return *this;
}

bool WriteAheadLog::open_file() {
    // Create directory if it doesn't exist
    fs::path filepath(filename_);
    if (filepath.has_parent_path()) {
        try {
            fs::create_directories(filepath.parent_path());
        } catch (const fs::filesystem_error&) {
            return false;
        }
    }

    // Try to open existing file for reading and appending
    file_.open(filename_, std::ios::binary | std::ios::in | std::ios::out | std::ios::ate);

    if (!file_ || !file_.is_open()) {
        // File doesn't exist or couldn't open in read/write mode
        // Create a new file
        file_.open(filename_, std::ios::binary | std::ios::out);
        if (!file_.is_open()) {
            return false;
        }

        // Write initial header
        if (!write_header(0)) {
            file_.close();
            return false;
        }

        file_.close();

        // Reopen for both reading and writing
        file_.open(filename_, std::ios::binary | std::ios::in | std::ios::out | std::ios::ate);
        return file_.is_open();
    }

    // Verify the file is a valid WAL
    if (!validate_file()) {
        file_.close();

        // Create a new valid file
        file_.open(filename_, std::ios::binary | std::ios::out);
        if (!file_.is_open()) {
            return false;
        }

        if (!write_header(0)) {
            file_.close();
            return false;
        }

        file_.close();
        file_.open(filename_, std::ios::binary | std::ios::in | std::ios::out | std::ios::ate);
    }

    return file_.is_open();
}

bool WriteAheadLog::validate_file() const {
    if (!file_.is_open()) return false;

    auto original_pos = file_.tellg();

    // Check file size
    file_.seekg(0, std::ios::end);
    auto file_size = file_.tellg();
    file_.seekg(original_pos);

    if (file_size < static_cast<std::streampos>(sizeof(MAGIC) + sizeof(VERSION) + sizeof(uint32_t))) {
        return false;
    }

    // Check magic number
    file_.seekg(0);
    uint64_t magic = 0;
    file_.read(reinterpret_cast<char*>(&magic), sizeof(magic));
    file_.seekg(original_pos);

    return magic == MAGIC && file_.good();
}

bool WriteAheadLog::write_header(uint32_t entry_count) {
    if (!file_.is_open()) return false;

    auto original_pos = file_.tellp();
    file_.seekp(0);

    file_.write(reinterpret_cast<const char*>(&MAGIC), sizeof(MAGIC));
    file_.write(reinterpret_cast<const char*>(&VERSION), sizeof(VERSION));
    file_.write(reinterpret_cast<const char*>(&entry_count), sizeof(entry_count));

    file_.flush();
    file_.seekp(original_pos);

    return file_.good();
}

bool WriteAheadLog::read_header(uint32_t& entry_count) const {
    if (!file_.is_open()) return false;

    auto original_pos = file_.tellg();
    file_.seekg(0);

    uint64_t magic = 0;
    uint32_t version = 0;

    file_.read(reinterpret_cast<char*>(&magic), sizeof(magic));
    file_.read(reinterpret_cast<char*>(&version), sizeof(version));
    file_.read(reinterpret_cast<char*>(&entry_count), sizeof(entry_count));

    file_.seekg(original_pos);

    if (!file_.good()) {
        return false;
    }

    return magic == MAGIC && version == VERSION;
}

bool WriteAheadLog::seek_to_entry_start() const {
    if (!file_.is_open()) return false;

    file_.seekg(sizeof(MAGIC) + sizeof(VERSION) + sizeof(uint32_t));
    return file_.good();
}

bool WriteAheadLog::write_entry(OpType type, const std::string& key, const std::string& value) {
    if (!file_.is_open()) return false;

    // Move to end of file
    file_.seekp(0, std::ios::end);
    auto entry_start_pos = file_.tellp();

    // Write operation type
    const uint8_t op_type = static_cast<uint8_t>(type);
    file_.write(reinterpret_cast<const char*>(&op_type), sizeof(op_type));

    // Write key length and key
    uint32_t key_len = static_cast<uint32_t>(key.size());
    file_.write(reinterpret_cast<const char*>(&key_len), sizeof(key_len));
    if (key_len > 0) {
        file_.write(key.data(), key_len);
    }

    // For PUT operations, write value
    if (type == OpType::PUT) {
        uint32_t value_len = static_cast<uint32_t>(value.size());
        file_.write(reinterpret_cast<const char*>(&value_len), sizeof(value_len));
        if (value_len > 0) {
            file_.write(value.data(), value_len);
        }
    }
    // For DELETE, no value is written

    // Update header with new entry count
    uint32_t current_count = 0;
    if (!read_header(current_count)) {
        // Rollback: truncate to where we started
        file_.seekp(entry_start_pos);
        // On Unix: we could use truncate() but for cross-platform, we'll leave it
        // The next open will validate and potentially fix the file
        return false;
    }

    current_count++;
    if (!write_header(current_count)) {
        // Couldn't update header, leave file as is
        return false;
    }

    // Ensure data is written to disk
    file_.flush();

    return file_.good();
}

bool WriteAheadLog::log_put(const std::string& key, const std::string& value) {
    return write_entry(OpType::PUT, key, value);
}

bool WriteAheadLog::log_delete(const std::string& key) {
    return write_entry(OpType::DELETE, key, "");
}

std::vector<WriteAheadLog::LogEntry> WriteAheadLog::read_all_entries() const {
    std::vector<LogEntry> entries;
    std::vector<LogEntry> temp;

    if (!recover(temp)) {
        return entries;  // Return empty on failure
    }

    return temp;
}

bool WriteAheadLog::recover(std::vector<LogEntry>& entries) const {
    entries.clear();

    if (!file_.is_open()) {
        return false;
    }

    // Get entry count from header
    uint32_t entry_count = 0;
    if (!read_header(entry_count)) {
        return false;
    }

    if (entry_count == 0) {
        return true;  // Empty WAL is valid
    }

    // Seek to first entry
    if (!seek_to_entry_start()) {
        return false;
    }

    entries.reserve(entry_count);

    for (uint32_t i = 0; i < entry_count; ++i) {
        // Read operation type
        uint8_t op_type = 0;
        file_.read(reinterpret_cast<char*>(&op_type), sizeof(op_type));
        if (!file_.good()) break;

        // Read key length and key
        uint32_t key_len = 0;
        file_.read(reinterpret_cast<char*>(&key_len), sizeof(key_len));
        if (!file_.good()) break;

        std::string key;
        if (key_len > 0) {
            key.resize(key_len);
            file_.read(key.data(), key_len);
            if (!file_.good()) break;
        }

        std::string value;
        if (static_cast<OpType>(op_type) == OpType::PUT) {
            // Read value length and value
            uint32_t value_len = 0;
            file_.read(reinterpret_cast<char*>(&value_len), sizeof(value_len));
            if (!file_.good()) break;

            if (value_len > 0) {
                value.resize(value_len);
                file_.read(value.data(), value_len);
                if (!file_.good()) break;
            }
        }

        entries.emplace_back(static_cast<OpType>(op_type), std::move(key), std::move(value));
    }

    // If we didn't read all entries, something went wrong
    if (entries.size() != entry_count) {
        entries.clear();
        return false;
    }

    return true;
}

void WriteAheadLog::clear() {
    if (file_.is_open()) {
        file_.close();
    }

    // Remove the file entirely
    try {
        fs::remove(filename_);
    } catch (const fs::filesystem_error&) {
        // Ignore if file doesn't exist
    }

    // Create new empty file
    open_file();  // This will create a new file with header
}

size_t WriteAheadLog::size() const {
    if (!file_.is_open()) return 0;

    auto current_pos = file_.tellg();
    file_.seekg(0, std::ios::end);
    auto file_size = file_.tellg();
    file_.seekg(current_pos);

    return static_cast<size_t>(file_size);
}

bool WriteAheadLog::is_open() const {
    return file_.is_open();
}

const std::string& WriteAheadLog::get_filename() const {
    return filename_;
}