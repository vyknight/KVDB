#include "BufferPool.h"
#include <algorithm>

BufferPool::BufferPool(size_t capacity) 
    : capacity_(capacity) {
    stats_.capacity = capacity;
}

BufferPool::BufferPool(BufferPool&& other) noexcept
    : page_map_(std::move(other.page_map_)),
      lru_list_(std::move(other.lru_list_)),
      capacity_(other.capacity_),
      stats_(other.stats_) {
}

BufferPool& BufferPool::operator=(BufferPool&& other) noexcept {
    if (this != &other) {
        page_map_ = std::move(other.page_map_);
        lru_list_ = std::move(other.lru_list_);
        capacity_ = other.capacity_;
        stats_ = other.stats_;
    }
    return *this;
}

Page* BufferPool::get_page(const PageId& page_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = page_map_.find(page_id);
    if (it != page_map_.end()) {
        // Hit: page found
        {
            std::lock_guard<std::mutex> stats_lock(stats_mutex_);
            stats_.hits++;
        }
        
        Frame* frame = it->second.get();
        frame->pinned = true;
        touch_frame(page_id);
        
        return &frame->page;
    }
    
    // Miss: page not in buffer
    {
        std::lock_guard<std::mutex> stats_lock(stats_mutex_);
        stats_.misses++;
    }
    
    return nullptr;
}

bool BufferPool::add_page(const PageId& page_id, Page&& page) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Check if already exists
    if (page_map_.find(page_id) != page_map_.end()) {
        return true;  // Already in pool
    }
    
    // If at capacity, evict one
    if (page_map_.size() >= capacity_) {
        if (!evict_one()) {
            return false;  // Could not evict
        }
    }
    
    // Add new frame
    auto frame = std::make_unique<Frame>(page_id, std::move(page));
    frame->pinned = true;
    
    page_map_[page_id] = std::move(frame);
    lru_list_.push_front(page_id);
    
    {
        std::lock_guard<std::mutex> stats_lock(stats_mutex_);
        stats_.current_size = page_map_.size();
    }
    
    return true;
}

bool BufferPool::remove_page(const PageId& page_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = page_map_.find(page_id);
    if (it == page_map_.end()) {
        return false;
    }
    
    // Remove from LRU list
    lru_list_.remove(page_id);
    
    // Remove from map
    page_map_.erase(it);
    
    {
        std::lock_guard<std::mutex> stats_lock(stats_mutex_);
        stats_.current_size = page_map_.size();
    }
    
    return true;
}

void BufferPool::unpin_page(const PageId& page_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = page_map_.find(page_id);
    if (it != page_map_.end()) {
        it->second->pinned = false;
    }
}

void BufferPool::mark_dirty(const PageId& page_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = page_map_.find(page_id);
    if (it != page_map_.end()) {
        it->second->dirty = true;
        it->second->page.mark_dirty();
    }
}

bool BufferPool::contains(const PageId& page_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return page_map_.find(page_id) != page_map_.end();
}

void BufferPool::clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    page_map_.clear();
    lru_list_.clear();
    
    {
        std::lock_guard<std::mutex> stats_lock(stats_mutex_);
        stats_.current_size = 0;
        stats_.hits = 0;
        stats_.misses = 0;
        stats_.evictions = 0;
    }
}

BufferPool::Stats BufferPool::get_stats() const {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    Stats stats = stats_;
    stats.current_size = page_map_.size();
    return stats;
}

bool BufferPool::evict_one() {
    // Find an unpinned page from LRU end
    for (auto it = lru_list_.rbegin(); it != lru_list_.rend(); ++it) {
        const PageId& page_id = *it;
        auto map_it = page_map_.find(page_id);
        
        if (map_it != page_map_.end() && !map_it->second->pinned) {
            // Found evictable page
            lru_list_.remove(page_id);
            page_map_.erase(map_it);
            
            {
                std::lock_guard<std::mutex> stats_lock(stats_mutex_);
                stats_.evictions++;
                stats_.current_size = page_map_.size();
            }
            
            return true;
        }
    }
    
    return false;  // All pages are pinned
}

void BufferPool::touch_frame(const PageId& page_id) {
    // Move to front of LRU list (most recently used)
    lru_list_.remove(page_id);
    lru_list_.push_front(page_id);
}