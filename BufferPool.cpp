//
// Created by K on 2025-12-06.
//

#include "BufferPool.h"
#include <iostream>
#include <algorithm>
#include <cmath>
#include <iomanip>
#include <unordered_set>

// Simple hash function (xxHash inspired)
size_t BufferPool::hash_page_id(const PageId& page_id) const {
    // Combine filename and offset
    std::string key = page_id.to_string();

    // Simple hash (replace with xxHash if available)
    std::hash<std::string> hasher;
    return hasher(key);
}

size_t BufferPool::get_directory_index(size_t hash, int depth) const {
    if (depth == 0) return 0;

    // Mask to get lowest 'depth' bits
    size_t mask = (1ULL << depth) - 1;
    return hash & mask;
}

size_t BufferPool::get_directory_index(const PageId& page_id) const {
    size_t h = hash_page_id(page_id);
    return get_directory_index(h, global_depth_);
}

BufferPool::BufferPool(const Config& config) : config_(config) {
    if (config.debug_logging) {
        std::cout << "[BufferPool] Initializing with max_pages=" << config.max_pages
                  << ", initial_global_depth=" << config.initial_global_depth
                  << ", bucket_capacity=" << config.bucket_capacity << std::endl;
    }

    // Initialize directory with initial global depth
    global_depth_ = static_cast<int>(config_.initial_global_depth);
    size_t directory_size = 1ULL << global_depth_;
    directory_.resize(directory_size);

    // Create initial buckets (all pointing to same bucket initially)
    auto initial_bucket = std::make_shared<Bucket>(next_bucket_id_++, global_depth_);
    for (size_t i = 0; i < directory_size; ++i) {
        directory_[i] = initial_bucket;
    }

    stats_.directory_size = directory_size;
    stats_.total_buckets = 1;

    log_debug("BufferPool initialized with directory size " + std::to_string(directory_size));
}

BufferPool::~BufferPool() {
    // Flush all dirty pages
    flush_all();

    // Clear all file handles
    {
        std::lock_guard<std::mutex> lock(files_mutex_);
        open_files_.clear();
    }

    if (config_.debug_logging) {
        std::cout << "[BufferPool] Destroyed" << std::endl;
    }
}

void BufferPool::log_debug(const std::string& message) const {
    if (config_.debug_logging) {
        std::cout << "[BufferPool] " << message << std::endl;
    }
}

Page* BufferPool::get_page(const std::string& filename, uint64_t offset) {
    PageId page_id(filename, offset);
    return get_page(page_id);
}

Page* BufferPool::get_page(const PageId& page_id) {
    std::unique_lock<std::shared_mutex> dir_lock(directory_mutex_);

    log_debug("Getting page: " + page_id.to_string());

    // First, try to find page in buffer pool
    Page* page = find_page_in_pool(page_id);
    if (page) {
        // Page found in buffer (cache hit)
        {
            std::lock_guard<std::mutex> stats_lock(stats_mutex_);
            stats_.hits++;
        }

        // Update LRU
        auto it = lru_map_.find(page);
        if (it != lru_map_.end()) {
            auto& lru_entry = *(it->second);
            update_lru(page, lru_entry.bucket, lru_entry.bucket_idx);
        }

        // Pin the page
        page->pin();
        return page;
    }

    // Cache miss
    {
        std::lock_guard<std::mutex> stats_lock(stats_mutex_);
        stats_.misses++;
    }

    // Check if we need to evict before loading new page
    if (stats_.used_pages >= config_.max_pages) {
        log_debug("Buffer pool full, evicting page");
        evict_page();
    }

    // Load page from disk
    page = load_page_from_disk(page_id);
    if (!page) {
        return nullptr;
    }

    // Pin the page
    page->pin();

    // Update statistics
    {
        std::lock_guard<std::mutex> stats_lock(stats_mutex_);
        stats_.used_pages++;
        stats_.total_pages++;
    }

    log_debug("Page loaded from disk: " + page_id.to_string());
    return page;
}

Page* BufferPool::find_page_in_pool(const PageId& page_id) {
    // Check page_to_bucket_map first (O(1) lookup)
    auto it = page_to_bucket_map_.find(page_id);
    if (it != page_to_bucket_map_.end()) {
        auto& [bucket, idx] = it->second;
        return bucket->find_page(page_id);
    }

    return nullptr;
}

Page* BufferPool::load_page_from_disk(const PageId& page_id) {
    // Get bucket for this page
    auto bucket = get_bucket_for_page(page_id);
    if (!bucket) {
        return nullptr;
    }

    // Check if bucket is full
    if (bucket->is_full(config_.bucket_capacity)) {
        log_debug("Bucket " + std::to_string(bucket->bucket_id) + " is full, attempting split");
        size_t bucket_idx = get_directory_index(page_id);
        maybe_split_bucket(bucket_idx);

        // Get bucket again (might have changed after split)
        bucket = get_bucket_for_page(page_id);
        if (!bucket) {
            return nullptr;
        }
    }

    // Create new page in bucket
    bucket->pages.emplace_back();
    Page& new_page = bucket->pages.back();
    new_page.set_id(page_id);

    // Read data from disk
    if (!read_page_from_disk(page_id, new_page)) {
        bucket->pages.pop_back();  // Remove failed page
        return nullptr;
    }

    // Update mappings
    size_t bucket_idx = get_directory_index(page_id);
    page_to_bucket_map_[page_id] = {bucket, bucket_idx};

    // Add to LRU
    update_lru(&new_page, bucket, bucket_idx);

    return &new_page;
}

std::shared_ptr<BufferPool::Bucket> BufferPool::get_bucket_for_page(const PageId& page_id) {
    size_t idx = get_directory_index(page_id);

    if (idx >= directory_.size()) {
        log_debug("Error: directory index out of bounds");
        return nullptr;
    }

    return directory_[idx];
}

void BufferPool::maybe_split_bucket(size_t bucket_idx) {
    if (bucket_idx >= directory_.size()) {
        return;
    }

    auto bucket = directory_[bucket_idx];

    // Only split if bucket is full and local depth < global depth (or we can expand)
    if (!bucket->is_full(config_.bucket_capacity)) {
        return;
    }

    // If local depth equals global depth, we need to expand directory first
    if (bucket->local_depth == global_depth_) {
        if (global_depth_ >= static_cast<int>(config_.max_global_depth)) {
            log_debug("Cannot split: reached max global depth");
            // Need to evict from this bucket instead
            evict_page();
            return;
        }
        expand_directory();
    }

    // Now split the bucket
    split_bucket(bucket_idx);
}

void BufferPool::expand_directory() {
    size_t old_size = directory_.size();
    size_t new_size = old_size * 2;

    log_debug("Expanding directory from " + std::to_string(old_size) +
              " to " + std::to_string(new_size));

    directory_.resize(new_size);

    // Copy old pointers to new positions
    for (size_t i = 0; i < old_size; ++i) {
        directory_[i + old_size] = directory_[i];
    }

    global_depth_++;

    {
        std::lock_guard<std::mutex> stats_lock(stats_mutex_);
        stats_.directory_size = new_size;
    }

    log_debug("Directory expanded, new global depth: " + std::to_string(global_depth_));
}

void BufferPool::split_bucket(size_t bucket_idx) {
    auto old_bucket = directory_[bucket_idx];

    log_debug("Splitting bucket " + std::to_string(old_bucket->bucket_id) +
              " at index " + std::to_string(bucket_idx));

    // Create new bucket
    auto new_bucket = std::make_shared<Bucket>(next_bucket_id_++, old_bucket->local_depth + 1);

    // Update old bucket's local depth
    old_bucket->local_depth++;

    // Calculate split image index
    size_t split_bit = 1ULL << (old_bucket->local_depth - 1);
    size_t split_image_idx = bucket_idx ^ split_bit;

    // Update directory pointers for split image
    update_directory_pointers(bucket_idx, split_image_idx, new_bucket);

    // Rehash pages from old bucket
    std::vector<Page> pages_to_rehash;
    pages_to_rehash.swap(old_bucket->pages);

    // Clear old mappings
    for (const auto& page : pages_to_rehash) {
        page_to_bucket_map_.erase(page.get_id());
        remove_from_lru(const_cast<Page*>(&page));
    }

    // Redistribute pages
    for (auto& page : pages_to_rehash) {
        PageId page_id = page.get_id();
        size_t new_idx = get_directory_index(page_id);

        if (new_idx == bucket_idx) {
            // Stays in old bucket
            old_bucket->pages.push_back(std::move(page));
            page_to_bucket_map_[page_id] = {old_bucket, bucket_idx};
        } else {
            // Goes to new bucket
            new_bucket->pages.push_back(std::move(page));
            page_to_bucket_map_[page_id] = {new_bucket, new_idx};
        }
    }

    // Add pages back to LRU
    for (auto& page : old_bucket->pages) {
        update_lru(&page, old_bucket, bucket_idx);
    }
    for (auto& page : new_bucket->pages) {
        size_t idx = get_directory_index(page.get_id());
        update_lru(&page, new_bucket, idx);
    }

    {
        std::lock_guard<std::mutex> stats_lock(stats_mutex_);
        stats_.splits++;
        stats_.total_buckets++;
    }

    log_debug("Bucket split completed. Old bucket size: " +
              std::to_string(old_bucket->size()) +
              ", New bucket size: " + std::to_string(new_bucket->size()));
}

void BufferPool::update_directory_pointers(size_t old_idx, size_t new_idx,
                                          std::shared_ptr<Bucket> new_bucket) {
    size_t local_depth = new_bucket->local_depth;
    size_t mask = (1ULL << local_depth) - 1;
    size_t prefix = old_idx & mask;

    for (size_t i = 0; i < directory_.size(); ++i) {
        if ((i & mask) == prefix) {
            // This directory entry belongs to the same bucket group
            size_t local_bit = (i >> (local_depth - 1)) & 1;
            if (local_bit == 1) {
                directory_[i] = new_bucket;
            }
        }
    }
}

void BufferPool::update_lru(Page* page, std::shared_ptr<Bucket> bucket, size_t bucket_idx) {
    // Remove from LRU if already exists
    remove_from_lru(page);

    // Add to front of LRU (most recently used)
    lru_list_.emplace_front(page, bucket, bucket_idx);
    lru_map_[page] = lru_list_.begin();
}

void BufferPool::remove_from_lru(Page* page) {
    auto it = lru_map_.find(page);
    if (it != lru_map_.end()) {
        lru_list_.erase(it->second);
        lru_map_.erase(it);
    }
}

Page* BufferPool::find_lru_victim() {
    // Start from back (least recently used)
    for (auto it = lru_list_.rbegin(); it != lru_list_.rend(); ++it) {
        Page* page = it->page;
        if (!page->is_pinned()) {
            return page;
        }
    }
    return nullptr;
}

void BufferPool::evict_page() {
    Page* victim = find_lru_victim();
    if (!victim) {
        log_debug("No unpinned page available for eviction");
        return;
    }

    log_debug("Evicting page: " + victim->get_id().to_string());

    // Flush if dirty
    if (victim->is_dirty()) {
        write_page_to_disk(*victim);
    }

    // Remove from bucket and mappings
    auto map_it = page_to_bucket_map_.find(victim->get_id());
    if (map_it != page_to_bucket_map_.end()) {
        auto& [bucket, idx] = map_it->second;

        // Remove from bucket
        auto& pages = bucket->pages;
        pages.erase(std::remove_if(pages.begin(), pages.end(),
            [victim](const Page& p) { return &p == victim; }),
            pages.end());

        page_to_bucket_map_.erase(map_it);
    }

    // Remove from LRU
    remove_from_lru(victim);

    // Update statistics
    {
        std::lock_guard<std::mutex> stats_lock(stats_mutex_);
        stats_.evictions++;
        stats_.used_pages--;
    }
}

void BufferPool::release_page(Page* page) {
    if (!page) return;

    page->unpin();
    log_debug("Released page: " + page->get_id().to_string() +
              ", pin count: " + std::to_string(page->get_pin_count()));
}

void BufferPool::mark_dirty(Page* page) {
    if (page) {
        page->mark_dirty();
        log_debug("Marked page dirty: " + page->get_id().to_string());
    }
}

void BufferPool::flush_page(Page* page) {
    if (!page) return;

    if (page->is_dirty()) {
        write_page_to_disk(*page);
        page->clear_dirty();
        log_debug("Flushed page to disk: " + page->get_id().to_string());
    }
}

bool BufferPool::read_page_from_disk(const PageId& page_id, Page& page) {
    auto dio = get_file_handle(page_id.get_filename(), true);
    if (!dio || !dio->is_open()) {
        log_debug("Failed to open file for reading: " + page_id.get_filename());
        return false;
    }

    // Read page data
    if (!dio->read(page_id.get_offset(), page.get_data(), Page::PAGE_SIZE)) {
        log_debug("Failed to read page from disk: " + page_id.to_string());
        return false;
    }

    {
        std::lock_guard<std::mutex> stats_lock(stats_mutex_);
        stats_.disk_reads++;
    }

    return true;
}

bool BufferPool::write_page_to_disk(Page& page) {
    const PageId& page_id = page.get_id();
    auto dio = get_file_handle(page_id.get_filename(), false);
    if (!dio || !dio->is_open()) {
        log_debug("Failed to open file for writing: " + page_id.get_filename());
        return false;
    }

    // Write page data
    if (!dio->write(page_id.get_offset(), page.get_data(), Page::PAGE_SIZE)) {
        log_debug("Failed to write page to disk: " + page_id.to_string());
        return false;
    }

    {
        std::lock_guard<std::mutex> stats_lock(stats_mutex_);
        stats_.disk_writes++;
    }

    return true;
}

std::unique_ptr<DirectIO> BufferPool::get_file_handle(const std::string& filename, bool read_only) {
    std::lock_guard<std::mutex> lock(files_mutex_);

    auto it = open_files_.find(filename);
    if (it != open_files_.end()) {
        // Check if existing handle has correct mode
        if (!read_only && it->second->is_using_direct_io()) {
            // We need write access but have read-only handle
            // Close and reopen with write access
            open_files_.erase(it);
        } else {
            return nullptr;  // Will use existing handle
        }
    }

    // Open new handle
    auto dio = DirectIO::open(filename, read_only);
    if (dio && dio->is_open()) {
        open_files_[filename] = std::move(dio);
        return nullptr;  // Stored in map, caller should get from map
    }

    return nullptr;
}

void BufferPool::flush_all() {
    std::shared_lock<std::shared_mutex> dir_lock(directory_mutex_);

    log_debug("Flushing all dirty pages");

    for (auto& bucket : directory_) {
        for (auto& page : bucket->pages) {
            if (page.is_dirty()) {
                write_page_to_disk(page);
                page.clear_dirty();
            }
        }
    }
}

void BufferPool::clear() {
    std::unique_lock<std::shared_mutex> dir_lock(directory_mutex_);

    log_debug("Clearing buffer pool");

    // Flush all dirty pages first
    flush_all();

    // Clear all data structures
    for (auto& bucket : directory_) {
        bucket->pages.clear();
    }

    lru_list_.clear();
    lru_map_.clear();
    page_to_bucket_map_.clear();

    // Reset directory to initial state
    global_depth_ = static_cast<int>(config_.initial_global_depth);
    size_t directory_size = 1ULL << global_depth_;
    directory_.resize(directory_size);

    auto initial_bucket = std::make_shared<Bucket>(0, global_depth_);
    for (size_t i = 0; i < directory_size; ++i) {
        directory_[i] = initial_bucket;
    }

    next_bucket_id_ = 1;

    // Reset statistics
    reset_stats();
    stats_.directory_size = directory_size;
    stats_.total_buckets = 1;
}

void BufferPool::resize(size_t new_max_pages) {
    std::unique_lock<std::shared_mutex> dir_lock(directory_mutex_);

    config_.max_pages = new_max_pages;
    log_debug("Resized buffer pool to " + std::to_string(new_max_pages) + " pages");

    // Evict pages if we're over the new limit
    while (stats_.used_pages > new_max_pages) {
        evict_page();
    }
}

BufferPool::BufferPoolStats BufferPool::get_stats() const {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    BufferPoolStats stats = stats_;
    stats.directory_size = directory_.size();

    // Count actual buckets (unique pointers)
    std::unordered_set<std::shared_ptr<Bucket>> unique_buckets;
    for (const auto& bucket : directory_) {
        unique_buckets.insert(bucket);
    }
    stats.total_buckets = unique_buckets.size();

    return stats;
}

void BufferPool::reset_stats() {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    stats_ = BufferPoolStats();
    stats_.directory_size = directory_.size();

    // Count unique buckets
    std::unordered_set<std::shared_ptr<Bucket>> unique_buckets;
    for (const auto& bucket : directory_) {
        unique_buckets.insert(bucket);
    }
    stats_.total_buckets = unique_buckets.size();
}

void BufferPool::print_directory() const {
    std::shared_lock<std::shared_mutex> lock(directory_mutex_);

    std::cout << "\n=== Buffer Pool Directory ===" << std::endl;
    std::cout << "Global Depth: " << global_depth_ << std::endl;
    std::cout << "Directory Size: " << directory_.size() << std::endl;

    std::unordered_set<std::shared_ptr<Bucket>> unique_buckets;

    for (size_t i = 0; i < directory_.size(); ++i) {
        auto bucket = directory_[i];
        unique_buckets.insert(bucket);

        std::cout << "  [" << std::setw(3) << i << "] -> Bucket "
                  << bucket->bucket_id
                  << " (depth=" << bucket->local_depth
                  << ", pages=" << bucket->size()
                  << ")" << std::endl;
    }

    std::cout << "Unique Buckets: " << unique_buckets.size() << std::endl;
}

void BufferPool::print_bucket_details() const {
    std::shared_lock<std::shared_mutex> lock(directory_mutex_);

    std::unordered_map<std::shared_ptr<Bucket>, std::vector<size_t>> bucket_to_indices;

    for (size_t i = 0; i < directory_.size(); ++i) {
        bucket_to_indices[directory_[i]].push_back(i);
    }

    std::cout << "\n=== Bucket Details ===" << std::endl;

    for (const auto& [bucket, indices] : bucket_to_indices) {
        std::cout << "Bucket " << bucket->bucket_id
                  << " (depth=" << bucket->local_depth
                  << ", pages=" << bucket->size() << ")" << std::endl;

        std::cout << "  Directory indices: ";
        for (size_t idx : indices) {
            std::cout << idx << " ";
        }
        std::cout << std::endl;

        if (!bucket->pages.empty()) {
            std::cout << "  Pages:" << std::endl;
            for (const auto& page : bucket->pages) {
                std::cout << "    " << page.get_id().to_string()
                          << " (pinned=" << page.is_pinned()
                          << ", dirty=" << page.is_dirty() << ")" << std::endl;
            }
        }
        std::cout << std::endl;
    }
}

bool BufferPool::Bucket::contains(const PageId& page_id) const {
    return find_page(page_id) != nullptr;
}

Page* BufferPool::Bucket::find_page(const PageId& page_id) {
    for (auto& page : pages) {
        if (page.get_id() == page_id) {
            return &page;
        }
    }
    return nullptr;
}

const Page* BufferPool::Bucket::find_page(const PageId& page_id) const {
    for (const auto& page : pages) {
        if (page.get_id() == page_id) {
            return &page;
        }
    }
    return nullptr;
}