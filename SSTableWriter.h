//
// Created by K on 2025-12-04.
//

#ifndef KVDB_SSTABLEWRITER_H
#define KVDB_SSTABLEWRITER_H

#include <string>
#include <vector>
#include <cstdint>
#include "Memtable.h"

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

class SSTableWriter
{
public:
    /**
     * Write Memtable contents to an SStable file
     * @param filename Output filename
     * @param entries Sorted entries from Memtable
     * @return true if successful, false otherwise
     */
    static bool write(const std::string& filename,
        const std::vector<std::pair<std::string, Memtable::Entry>>& entries);

    /**
     * Write a single SSTable from a Memtable
     * @param filename Output filename
     * @param memtable Source Memtable
     * @return true if successful, false otherwise
     */
    static bool write_from_memtable(const std::string& filename, const Memtable& memtable);

private:
    // Constants
    static constexpr uint64_t MAGIC = 0x4B5644425F535354;  // "KVDB_SST" in hexcode
    static constexpr uint32_t VERSION = 1;

    /**
     * Calculate total size needed for SSTable
     */
    static uint64_t calculate_total_size(const std::vector<std::pair<std::string, Memtable::Entry>>& entries);
};

#endif //KVDB_SSTABLEWRITER_H