//
// Created by K on 2025-12-06.
//

#include "WriteAheadLog.h"
#include <iostream>
#include <stdexcept>
#include <cstring>

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

bool WriteAheadLog::open_file() {
    // Try to open existing file for reading and appending
    file_.open(filename_, std::ios::binary | std::ios::in | std::ios::out | std::ios::ate);

    if (!file_) {
        // File doesn't exist, create it
        file_.open(filename_, std::ios::binary | std::ios::out);
        if (!file_) {
            return false;
        }

        // Write initial header (0 entries)
        if (!write_header(0)) {
            return false;
        }

        file_.close();
        // Reopen for both reading and appending
        file_.open(filename_, std::ios::binary | std::ios::in | std::ios::out | std::ios::ate);
    }

    return file_.is_open();
}

bool WriteAheadLog::write_header(const uint32_t entry_count) {
    if (!file_.is_open()) return false;

    // Save current position
    auto current_pos = file_.tellp();

    // Go to beginning
    file_.seekp(0);

    // Write magic
    file_.write(reinterpret_cast<const char*>(&MAGIC), sizeof(MAGIC));

    // Write version
    file_.write(reinterpret_cast<const char*>(&VERSION), sizeof(VERSION));

    // Write entry count
    file_.write(reinterpret_cast<const char*>(&entry_count), sizeof(entry_count));

    // Restore position
    file_.seekp(current_pos);

    return file_.good();
}

bool WriteAheadLog::read_header(uint32_t& entry_count) const {
    if (!file_.is_open()) return false;

    // Save current position
    auto current_pos = file_.tellg();

    // Go to beginning
    file_.seekg(0);

    // Read and verify magic
    uint64_t magic;
    file_.read(reinterpret_cast<char*>(&magic), sizeof(magic));
    if (magic != MAGIC) {
        file_.seekg(current_pos);
        return false;
    }

    // Read version
    uint32_t version;
    file_.read(reinterpret_cast<char*>(&version), sizeof(version));
    if (version != VERSION) {
        file_.seekg(current_pos);
        return false;
    }

    // Read entry count
    file_.read(reinterpret_cast<char*>(&entry_count), sizeof(entry_count));

    // Restore position
    file_.seekg(current_pos);

    return file_.good();
}

bool WriteAheadLog::write_entry(OpType type, const std::string& key, const std::string& value) {
    if (!file_.is_open()) return false;

    // Write operation type
    const auto op_type = static_cast<uint8_t>(type);
    file_.write(reinterpret_cast<const char*>(&op_type), sizeof(op_type));

    // Write key length and key
    const auto key_len = static_cast<uint32_t>(key.size());
    file_.write(reinterpret_cast<const char*>(&key_len), sizeof(key_len));
    file_.write(key.data(), key_len);

    // For PUT operations, write value length and value
    if (type == OpType::PUT) {
        const auto value_len = static_cast<uint32_t>(value.size());
        file_.write(reinterpret_cast<const char*>(&value_len), sizeof(value_len));
        if (value_len > 0) {
            file_.write(value.data(), value_len);
        }
    }

    // For DELETE operations, value length is 0 (not written)

    // Update entry count in header
    uint32_t current_count;
    if (!read_header(current_count)) {
        return false;
    }

    current_count++;
    if (!write_header(current_count)) {
        return false;
    }

    // Flush to ensure durability
    file_.flush();

    return file_.good();
}

bool WriteAheadLog::log_put(const std::string& key, const std::string& value) {
    return write_entry(OpType::PUT, key, value);
}

bool WriteAheadLog::log_delete(const std::string& key) {
    return write_entry(OpType::DELETE, key);
}

std::vector<WriteAheadLog::LogEntry> WriteAheadLog::read_all_entries() const {
    std::vector<LogEntry> entries;

    if (!file_.is_open()) {
        return entries;
    }

    // Read header to get entry count
    uint32_t entry_count;
    if (!read_header(entry_count)) {
        return entries;
    }

    // Seek to first entry (after header)
    file_.seekg(sizeof(MAGIC) + sizeof(VERSION) + sizeof(entry_count));

    for (uint32_t i = 0; i < entry_count && file_.good(); ++i) {
        // Read operation type
        uint8_t op_type;
        file_.read(reinterpret_cast<char*>(&op_type), sizeof(op_type));
        if (!file_) break;

        // Read key length and key
        uint32_t key_len;
        file_.read(reinterpret_cast<char*>(&key_len), sizeof(key_len));
        if (!file_) break;

        std::string key(key_len, '\0');
        if (key_len > 0) {
            file_.read(&key[0], key_len);
            if (!file_) break;
        }

        std::string value;
        if (static_cast<OpType>(op_type) == OpType::PUT) {
            // Read value length and value
            uint32_t value_len;
            file_.read(reinterpret_cast<char*>(&value_len), sizeof(value_len));
            if (!file_) break;

            if (value_len > 0) {
                value.resize(value_len);
                file_.read(&value[0], value_len);
                if (!file_) break;
            }
        }

        entries.emplace_back(static_cast<OpType>(op_type), std::move(key), std::move(value));
    }

    return entries;
}

void WriteAheadLog::clear() {
    if (file_.is_open()) {
        file_.close();
    }

    // Create new empty file
    file_.open(filename_, std::ios::binary | std::ios::out | std::ios::trunc);
    if (file_.is_open()) {
        write_header(0);
    }
}

size_t WriteAheadLog::size() const {
    if (!file_.is_open()) return 0;

    auto current_pos = file_.tellg();
    file_.seekg(0, std::ios::end);
    auto size = file_.tellg();
    file_.seekg(current_pos);

    return static_cast<size_t>(size);
}

bool WriteAheadLog::is_open() const {
    return file_.is_open();
}

const std::string& WriteAheadLog::get_filename() const {
    return filename_;
}