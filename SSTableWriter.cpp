//
// Created by K on 2025-12-04.
//

#include "SSTableWriter.h"
#include <fstream>
#include <iostream>
#include <vector>
#include <string>
#include <cstring>
#include <stdexcept>

/**
 * SSTable Format is the following:
 * [Header]
 * - Magic number (8 bytes): "KVDB_SST\0"
 * - Version (uint32_t): 1
 * - Entry count (uint32_t): Number of key-value pairs
 * - Data offset (uint64_t): Where key-value data starts
 *
 * [Key Directory] - For binary search
 * Array of:
 *  - Key length (uint32_t)
 *  - Key (variable)
 *  - Value offset (uint64_t): Position in data section
 *  - Value length (uint32_t)
 *  - Tombstone flag (uint8_t): 1 = deleted, 0 = alive
 *
 * [Value Data Section]
 * Sequential storage of all values
 */

// File format consts
namespace
{
    constexpr size_t HEADER_SIZE = 24;  // magic + version + entry count + data offset
    constexpr size_t KEY_ENTRY_HEADER_SIZE = 13;  // key len + value offset + value len + tombstone
}

bool SSTableWriter::write(const std::string& filename,
    const std::vector<std::pair<std::string, Memtable::Entry>>& entries)
{
    std::ofstream file(filename, std::ios::binary | std::ios::trunc);
    if (!file)
    {
        std::cerr << "Failed to open file for writing: " << filename << std::endl;
        return false;
    }

    try
    {
        // header
        const uint32_t entry_count = static_cast<uint32_t>(entries.size());

        // calculate data section offset
        uint64_t data_offset = HEADER_SIZE;
        for (const auto& [key, entry] : entries)
        {
            data_offset += KEY_ENTRY_HEADER_SIZE + key.size();
        }

        // write header
        file.write(reinterpret_cast<const char*>(&SSTableWriter::MAGIC), sizeof(SSTableWriter::MAGIC));
        file.write(reinterpret_cast<const char*>(&SSTableWriter::VERSION), sizeof(SSTableWriter::VERSION));
        file.write(reinterpret_cast<const char*>(&entry_count), sizeof(entry_count));
        file.write(reinterpret_cast<const char*>(&data_offset), sizeof(data_offset));

        // write key directory
        uint64_t current_value_offset = data_offset;
        std::vector<std::pair<uint64_t, std::string>> value_data;  // offset, value

        for (const auto& [key, entry] : entries)
        {
            // write key len
            uint32_t key_len = static_cast<uint32_t>(key.size());
            file.write(reinterpret_cast<const char*>(&key_len), sizeof(key_len));
            // write key
            file.write(key.data(), key_len);
            // write value offset
            file.write(reinterpret_cast<const char*>(&current_value_offset), sizeof(current_value_offset));
            // write value len
            uint32_t value_len = static_cast<uint32_t>(entry.value.size());
            file.write(reinterpret_cast<const char*>(&value_len), sizeof(value_len));
            // write tombstone
            uint8_t tombstone = entry.is_deleted ? 1 : 0;
            file.write(reinterpret_cast<const char*>(&tombstone), sizeof(tombstone));
            // store value for later writing
            value_data.emplace_back(current_value_offset, entry.value);
            // update offset
            current_value_offset += value_len;
        }

        // write values
        for (const auto& [offset, value] : value_data)
        {
            // writing sequentially so we don't have to worry about positions here
            file.write(value.data(), value.size());
        }

        // verify file is in good state
        if (!file)
        {
            throw std::runtime_error("Failed to write to file");
        }

        return true;
    } catch (const std::exception& e)
    {
        std::cerr << "SStable Write error: " << e.what() << std::endl;
        return false;
    }
}

bool SSTableWriter::write_from_memtable(const std::string& filename, const Memtable& memtable)
{
    const auto entries = memtable.get_all_entries();
    return write(filename, entries);
}

uint64_t SSTableWriter::calculate_total_size(const std::vector<std::pair<std::string, Memtable::Entry>>& entries)
{
    uint64_t total_size = HEADER_SIZE;
    for (const auto& [key, entry] : entries)
    {
        total_size += KEY_ENTRY_HEADER_SIZE + key.size() + entry.value.size();
    }
    return total_size;
}
