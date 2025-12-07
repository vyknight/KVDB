#include "BufferPool.h"
#include "DirectIO.h"
#include <cstring>
#include <chrono>
#include <algorithm>

BufferPool::BufferPool(size_t max_pages, bool use_lru)
    : max_pages_(max_pages), use_lru_(use_lru) {

    // Initialize hash table with prime number of buckets
    size_t num_buckets = std::max(static_cast<size_t>(101), max_pages * 2);
    hash_table_.resize(num_buckets, nullptr);

    // Allocate all frames upfront
    frames_.resize(max_pages);
    free_frames_.reserve(max_pages);

    for (size_t i = 0; i < max_pages; ++i) {
        free_frames_.push_back(i);
    }

    stats_.max_size = max_pages;
}

BufferPool::~BufferPool() {
    // Clean up hash table
    for (auto& bucket : hash_table_) {
        HashEntry* entry = bucket;
        while (entry) {
            HashEntry* next = entry->next;
            delete entry;
            entry = next;
        }
    }
}

BufferPool::BufferPool(BufferPool&& other) noexcept
    : hash_table_(std::move(other.hash_table_)),
      frames_(std::move(other.frames_)),
      free_frames_(std::move(other.free_frames_)),
      lru_list_(std::move(other.lru_list_)),
      stats_(other.stats_),
      max_pages_(other.max_pages_),
      use_lru_(other.use_lru_) {

    // Reset the other buffer pool
    other.hash_table_.clear();
    other.frames_.clear();
    other.free_frames_.clear();
    other.lru_list_.clear();
    other.stats_ = Stats();
}

BufferPool& BufferPool::operator=(BufferPool&& other) noexcept {
    if (this != &other) {
        clear();  // Flush any dirty pages

        // Move resources
        hash_table_ = std::move(other.hash_table_);
        frames_ = std::move(other.frames_);
        free_frames_ = std::move(other.free_frames_);
        lru_list_ = std::move(other.lru_list_);
        stats_ = other.stats_;
        max_pages_ = other.max_pages_;
        use_lru_ = other.use_lru_;

        // Reset the other buffer pool
        other.hash_table_.clear();
        other.frames_.clear();
        other.free_frames_.clear();
        other.lru_list_.clear();
        other.stats_ = Stats();
    }
    return *this;
}

size_t BufferPool::hash(const PageId& page_id) const {
    // Combine filename and offset using std::hash
    std::size_t h1 = std::hash<std::string>{}(page_id.get_filename());
    std::size_t h2 = std::hash<uint64_t>{}(page_id.get_offset());

    // Simple hash combination (from boost::hash_combine)
    h1 ^= h2 + 0x9e3779b9 + (h1 << 6) + (h1 >> 2);
    return h1 % hash_table_.size();
}

BufferPool::Frame* BufferPool::find_frame(const PageId& page_id) {
    size_t bucket_idx = hash(page_id);
    HashEntry* entry = hash_table_[bucket_idx];

    while (entry) {
        if (entry->frame->id == page_id) {
            return entry->frame;
        }
        entry = entry->next;
    }

    return nullptr;
}

Page* BufferPool::get_page(const PageId& page_id, DirectIO* file, bool read_only) {
    std::lock_guard<std::mutex> lock(pool_mutex_);

    // Check if page is already in buffer
    Frame* frame = find_frame(page_id);

    if (frame) {
        // Hit: page found in buffer
        {
            std::lock_guard<std::mutex> lock(stats_mutex_);
            stats_.hits++;
        }

        // Update access time and pin
        if (use_lru_) {
            lru_access(frame);
        } else {
            frame->referenced = true;
        }

        frame->pinned = true;
        frame->page.pin();

        return &frame->page;
    }

    // Miss: page not in buffer
    {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        stats_.misses++;
    }

    // Allocate a new frame
    frame = allocate_frame(page_id);
    if (!frame) {
        return nullptr;
    }

    // Load page from disk if file provided
    if (file && !load_page(frame, file)) {
        // Failed to load page, free the frame
        size_t bucket_idx = hash(page_id);
        HashEntry*& bucket = hash_table_[bucket_idx];
        HashEntry* entry = bucket;
        HashEntry* prev = nullptr;

        while (entry && entry->frame != frame) {
            prev = entry;
            entry = entry->next;
        }

        if (entry) {
            if (prev) {
                prev->next = entry->next;
            } else {
                bucket = entry->next;
            }
            delete entry;
        }

        // Return frame to free list
        size_t frame_idx = frame - &frames_[0];
        free_frames_.push_back(frame_idx);
        return nullptr;
    }

    // Initialize frame
    frame->pinned = true;
    frame->page.set_id(page_id);
    frame->page.pin();

    if (use_lru_) {
        lru_access(frame);
    } else {
        frame->referenced = true;
    }

    return &frame->page;
}

void BufferPool::unpin_page(const PageId& page_id) {
    std::lock_guard<std::mutex> lock(pool_mutex_);

    Frame* frame = find_frame(page_id);
    if (frame && frame->pinned) {
        frame->pinned = false;
        frame->page.unpin();

        if (!use_lru_) {
            frame->referenced = true;  // Give it a reference bit
        }
    }
}

void BufferPool::mark_dirty(const PageId& page_id) {
    std::lock_guard<std::mutex> lock(pool_mutex_);

    Frame* frame = find_frame(page_id);
    if (frame) {
        frame->dirty = true;
        frame->page.mark_dirty();

        std::lock_guard<std::mutex> lock(stats_mutex_);
        stats_.dirty_pages++;
    }
}

BufferPool::Frame* BufferPool::allocate_frame(const PageId& page_id) {
    // Check if we have free frames
    if (!free_frames_.empty()) {
        size_t frame_idx = free_frames_.back();
        free_frames_.pop_back();
        Frame* frame = &frames_[frame_idx];

        // Initialize frame
        frame->id = page_id;
        frame->dirty = false;
        frame->pinned = false;
        frame->referenced = false;
        frame->page.reset();

        // Insert into hash table
        size_t bucket_idx = hash(page_id);
        HashEntry* new_entry = new HashEntry(frame);
        new_entry->next = hash_table_[bucket_idx];
        hash_table_[bucket_idx] = new_entry;

        {
            std::lock_guard<std::mutex> lock(stats_mutex_);
            stats_.current_size++;
        }

        return frame;
    }

    // No free frames, need to evict
    if (evict_one_frame()) {
        // Try again after eviction
        return allocate_frame(page_id);
    }

    return nullptr;
}

bool BufferPool::evict_one_frame() {
    if (use_lru_) {
        lru_evict();
    } else {
        clock_evict();
    }

    return !free_frames_.empty();
}

void BufferPool::lru_access(Frame* frame) {
    // Find frame index
    size_t frame_idx = frame - &frames_[0];

    // Remove from current position in LRU list
    auto it = std::find(lru_list_.begin(), lru_list_.end(), frame_idx);
    if (it != lru_list_.end()) {
        lru_list_.erase(it);
    }

    // Add to front of LRU list (most recently used)
    lru_list_.push_front(frame_idx);
}

void BufferPool::lru_evict() {
    // Go through LRU list from back to front (least recently used)
    for (auto it = lru_list_.rbegin(); it != lru_list_.rend(); ++it) {
        size_t frame_idx = *it;
        Frame* frame = &frames_[frame_idx];

        if (!frame->pinned) {
            // Found evictable frame
            // Remove from hash table first
            size_t bucket_idx = hash(frame->id);
            HashEntry*& bucket = hash_table_[bucket_idx];
            HashEntry* entry = bucket;
            HashEntry* prev = nullptr;

            while (entry && entry->frame != frame) {
                prev = entry;
                entry = entry->next;
            }

            if (entry) {
                if (prev) {
                    prev->next = entry->next;
                } else {
                    bucket = entry->next;
                }
                delete entry;
            }

            // Remove from LRU list
            lru_list_.erase(std::find(lru_list_.begin(), lru_list_.end(), frame_idx));

            // Add to free list
            free_frames_.push_back(frame_idx);

            {
                std::lock_guard<std::mutex> lock(stats_mutex_);
                stats_.current_size--;
                stats_.evictions++;
                if (frame->dirty) {
                    stats_.dirty_pages--;
                }
            }

            return;
        }
    }
}

void BufferPool::clock_evict() {
    // Simple clock algorithm (second chance)
    static size_t clock_hand = 0;
    const size_t max_attempts = max_pages_ * 2;

    for (size_t attempts = 0; attempts < max_attempts; attempts++) {
        // Skip frames that are in free list
        while (std::find(free_frames_.begin(), free_frames_.end(), clock_hand) != free_frames_.end()) {
            clock_hand = (clock_hand + 1) % max_pages_;
        }

        Frame* frame = &frames_[clock_hand];

        if (!frame->pinned) {
            if (!frame->referenced) {
                // Found frame to evict
                // Remove from hash table
                size_t bucket_idx = hash(frame->id);
                HashEntry*& bucket = hash_table_[bucket_idx];
                HashEntry* entry = bucket;
                HashEntry* prev = nullptr;

                while (entry && entry->frame != frame) {
                    prev = entry;
                    entry = entry->next;
                }

                if (entry) {
                    if (prev) {
                        prev->next = entry->next;
                    } else {
                        bucket = entry->next;
                    }
                    delete entry;
                }

                // Add to free list
                free_frames_.push_back(clock_hand);

                {
                    std::lock_guard<std::mutex> lock(stats_mutex_);
                    stats_.current_size--;
                    stats_.evictions++;
                    if (frame->dirty) {
                        stats_.dirty_pages--;
                    }
                }

                return;
            } else {
                // Give it a second chance
                frame->referenced = false;
            }
        }

        clock_hand = (clock_hand + 1) % max_pages_;
    }
}

bool BufferPool::load_page(Frame* frame, DirectIO* file) {
    if (!file || !file->is_open()) {
        return false;
    }

    uint64_t offset = frame->id.get_offset();
    if (offset % Page::PAGE_SIZE != 0) {
        return false;
    }

    char* page_data = frame->page.get_data();
    return file->read(offset, page_data, Page::PAGE_SIZE);
}

bool BufferPool::write_page(Frame* frame, DirectIO* file) {
    if (!frame->dirty || !file || !file->is_open()) {
        return true;
    }

    uint64_t offset = frame->id.get_offset();
    if (offset % Page::PAGE_SIZE != 0) {
        return false;
    }

    const char* page_data = frame->page.get_data();
    bool success = file->write(offset, page_data, Page::PAGE_SIZE);

    if (success) {
        frame->dirty = false;
        frame->page.clear_dirty();

        std::lock_guard<std::mutex> lock(stats_mutex_);
        if (stats_.dirty_pages > 0) {
            stats_.dirty_pages--;
        }
    }

    return success;
}

bool BufferPool::flush_page(const PageId& page_id, DirectIO* file) {
    std::lock_guard<std::mutex> lock(pool_mutex_);

    Frame* frame = find_frame(page_id);
    if (!frame) {
        return false;
    }

    return write_page(frame, file);
}

bool BufferPool::evict_page(const PageId& page_id) {
    std::lock_guard<std::mutex> lock(pool_mutex_);

    Frame* frame = find_frame(page_id);
    if (!frame || frame->pinned) {
        return false;
    }

    // Remove from hash table
    size_t bucket_idx = hash(page_id);
    HashEntry*& bucket = hash_table_[bucket_idx];
    HashEntry* entry = bucket;
    HashEntry* prev = nullptr;

    while (entry && entry->frame != frame) {
        prev = entry;
        entry = entry->next;
    }

    if (!entry) {
        return false;
    }

    if (prev) {
        prev->next = entry->next;
    } else {
        bucket = entry->next;
    }
    delete entry;

    // Remove from LRU list if using LRU
    if (use_lru_) {
        size_t frame_idx = frame - &frames_[0];
        auto it = std::find(lru_list_.begin(), lru_list_.end(), frame_idx);
        if (it != lru_list_.end()) {
            lru_list_.erase(it);
        }
    }

    // Add to free list
    size_t frame_idx = frame - &frames_[0];
    free_frames_.push_back(frame_idx);

    {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        stats_.current_size--;
        stats_.evictions++;
        if (frame->dirty) {
            stats_.dirty_pages--;
        }
    }

    return true;
}

BufferPool::Stats BufferPool::get_stats() const {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    Stats stats = stats_;

    // Count pinned pages
    stats.pinned_pages = 0;
    for (const auto& frame : frames_) {
        if (frame.pinned) {
            stats.pinned_pages++;
        }
    }

    return stats;
}

void BufferPool::clear_stats() {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    stats_ = Stats();
    stats_.max_size = max_pages_;
}

void BufferPool::resize(size_t new_max_pages) {
    std::lock_guard<std::mutex> lock(pool_mutex_);

    if (new_max_pages < size()) {
        // Need to evict pages first - for simplicity, we'll just fail
        return;
    }

    max_pages_ = new_max_pages;

    std::lock_guard<std::mutex> lock_stats(stats_mutex_);
    stats_.max_size = new_max_pages;
}

size_t BufferPool::clear(DirectIO* file) {
    std::lock_guard<std::mutex> lock(pool_mutex_);

    size_t flushed = 0;

    // Clear hash table
    for (size_t i = 0; i < hash_table_.size(); i++) {
        HashEntry* entry = hash_table_[i];
        while (entry) {
            if (file && entry->frame->dirty) {
                if (write_page(entry->frame, file)) {
                    flushed++;
                }
            }
            HashEntry* next = entry->next;
            delete entry;
            entry = next;
        }
        hash_table_[i] = nullptr;
    }

    // Reset all frames
    for (auto& frame : frames_) {
        frame.id = PageId();
        frame.dirty = false;
        frame.pinned = false;
        frame.referenced = false;
        frame.page.reset();
    }

    // Rebuild free list
    free_frames_.clear();
    free_frames_.reserve(max_pages_);
    for (size_t i = 0; i < max_pages_; ++i) {
        free_frames_.push_back(i);
    }

    // Clear LRU list
    lru_list_.clear();

    {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        stats_.current_size = 0;
        stats_.dirty_pages = 0;
        stats_.pinned_pages = 0;
    }

    return flushed;
}

bool BufferPool::contains(const PageId& page_id) const {
    std::lock_guard<std::mutex> lock(pool_mutex_);

    size_t bucket_idx = hash(page_id);
    const HashEntry* entry = hash_table_[bucket_idx];

    while (entry) {
        if (entry->frame->id == page_id) {
            return true;
        }
        entry = entry->next;
    }

    return false;
}

size_t BufferPool::size() const {
    std::lock_guard<std::mutex> lock(pool_mutex_);
    return frames_.size() - free_frames_.size();
}

double BufferPool::hit_rate() const {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    size_t total = stats_.hits + stats_.misses;
    return total > 0 ? static_cast<double>(stats_.hits) / total : 0.0;
}