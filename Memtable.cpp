//
// Created by K on 2025-12-03.
//

#include "Memtable.h"
#include <iostream>
#include <bits/locale_facets_nonio.h>

// Constructor
Memtable::Memtable(const size_t memtable_size)
    : current_size_(0), max_size_(memtable_size), stats_({}) {}

// Calculate memory usage of a key value pair
size_t Memtable::calculate_entry_size(const std::string& key, const std::string& value)
{
    // Memory includes:
    // Key string + OH + Value string + OH + Entry struct bool + padding + std::map node OH

    // Estimation
    constexpr size_t STRING_OVERHEAD = 32;
    constexpr size_t MAP_NODE_OVERHEAD = 40;
    constexpr size_t ENTRY_STRUCT_SIZE = sizeof(bool) + 8;

    return key.capacity() + value.capacity() +
            STRING_OVERHEAD * 2 +
            MAP_NODE_OVERHEAD * 2 +
            ENTRY_STRUCT_SIZE;
}

// Insert or update KV pair
bool Memtable::put(const std::string& key, const std::string& value)
{
    // Check if key already exists
    const auto it = table_.find(key);
    const size_t new_entry_size = Memtable::calculate_entry_size(key, value);

    if (it != table_.end())
    {
        // Update existing entry: subtract old size, add new size
        const size_t old_size = calculate_entry_size(key, it->second.value);
        current_size_ -= old_size;
        current_size_ += new_entry_size;

        it->second.value = value;
        it->second.is_deleted = false;
    } else
    {
        current_size_ += new_entry_size;
        table_[key] = Entry(value, false);
    }

    // Update stats
    stats_.puts++;
    stats_.operations++;

    return !should_flush();
}

// Mark key as deleted
bool Memtable::remove(const std::string& key)
{
    const auto it = table_.find(key);
    const size_t tombstone_size = calculate_entry_size(key, "");

    if (it != table_.end())
    {
        // entry exists, mark as deleted
        const size_t old_size = calculate_entry_size(key, it->second.value);
        current_size_ -= old_size;
        current_size_ += tombstone_size;

        it->second.value = "";
        it->second.is_deleted = true;
    } else
    {
        // New tombstone entry
        // Need to make an entry even if KV not found in case it's in other pages
        current_size_ += tombstone_size;
        table_[key] = Entry("", true);
    }

    stats_.deletes++;
    stats_.operations++;

    return !should_flush();
}

// Get value for a key
std::optional<std::string> Memtable::get(const std::string& key) const
{
    const auto it = table_.find(key);

    stats_.gets++;
    stats_.operations++;

    if (it != table_.end() && !it->second.is_deleted)
    {
        return it->second.value;
    }

    return std::nullopt;
}

// Check if key exists and isn't deleted
bool Memtable::contains(const std::string& key) const
{
    const auto it = table_.find(key);
    return it != table_.end() && !it->second.is_deleted;
}

// Check if key is marked as deleted
bool Memtable::is_deleted(const std::string& key) const
{
    const auto it = table_.find(key);
    return it != table_.end() && !it->second.is_deleted;
}

// Get current size in bytes
size_t Memtable::size() const
{
    return current_size_;
}

// Return current number of entries in memtable
size_t Memtable::entry_count() const
{
    return table_.size();
}

// Check if memtable should be flushed
bool Memtable::should_flush() const
{
    return current_size_ >= max_size_;
}

// Clear the memtable
void Memtable::clear()
{
    current_size_ = 0;
    table_.clear();
    stats_.flushes++;
}

// Get all entries and in sorted order for flushing
std::vector<std::pair<std::string, Memtable::Entry>> Memtable::get_all_entries() const
{
    std::vector<std::pair<std::string, Memtable::Entry>> entries;
    entries.reserve(table_.size());

    for (const auto& [key, entry] : table_)
    {
        entries.emplace_back(key, entry);
    }

    // std::map maintains sorted order (thank you dispensation 1)
    return entries;
}

// Send iterator to beginning
std::map<std::string, Memtable::Entry>::const_iterator Memtable::begin() const
{
    return table_.begin();
}

// Send iterator to end
std::map<std::string, Memtable::Entry>::const_iterator Memtable::end() const
{
    return table_.end();
}

// Get approximate memory usage breakdown
std::map<std::string, size_t> Memtable::get_memory_usage() const
{
    std::map<std::string, size_t> usage;

    size_t keys_mem = 0;
    size_t values_mem = 0;
    size_t deleted_count = 0;
    size_t alive_count = 0;

    for (const auto& [key, entry] : table_)
    {
        keys_mem += key.capacity();
        values_mem += entry.value.capacity();

        if (entry.is_deleted)
        {
            deleted_count++;
        } else
        {
            alive_count++;
        }
    }

    // Estimate overhead
    constexpr size_t STRING_OVERHEAD = 32;
    constexpr size_t MAP_NODE_OVERHEAD = 40;

    const size_t string_overhead_total = table_.size() * STRING_OVERHEAD * 2;
    const size_t map_node_overhead_total = table_.size() * MAP_NODE_OVERHEAD;
    const size_t entry_struct_total = table_.size() * sizeof(Entry);

    usage["keys_memory"] = keys_mem;
    usage["values_memory"] = values_mem;
    usage["string_overhead"] = string_overhead_total;
    usage["map_node_overhead"] = map_node_overhead_total;
    usage["entry_struct_memory"] = entry_struct_total;
    usage["estimated_total"] = current_size_;
    usage["entries_count"] = table_.size();
    usage["alive_entries"] = alive_count;
    usage["tombstones"] = deleted_count;
    usage["memtable_size"] = max_size_;

    return usage;
}

// Set new maximum size
void Memtable::set_new_memtable_size(const size_t new_memtable_size)
{
    max_size_ = new_memtable_size;
}

// Get max allowed size
size_t Memtable::get_memtable_size() const
{
    return max_size_;
}

Memtable::Stats Memtable::get_stats() const
{
    return stats_;
}

// Reset stats
void Memtable::reset_stats() const
{
    stats_ = {};
}

