#ifndef KVDB_BUFFERPOOL_H
#define KVDB_BUFFERPOOL_H

#include "Page.h"
#include "PageId.h"
#include <unordered_map>
#include <list>
#include <memory>
#include <mutex>

class BufferPool {
public:
    /**
     * @param capacity Maximum number of pages in buffer pool
     */
    explicit BufferPool(size_t capacity = 1024);
    ~BufferPool() = default;

    // No copying
    BufferPool(const BufferPool&) = delete;
    BufferPool& operator=(const BufferPool&) = delete;

    // Move enabled
    BufferPool(BufferPool&& other) noexcept;
    BufferPool& operator=(BufferPool&& other) noexcept;

    /**
     * Get a page from buffer pool.
     * If page is not in pool, returns nullptr.
     * Caller must call unpin_page() when done.
     */
    Page* get_page(const PageId& page_id);

    /**
     * Add a page to buffer pool.
     * @param page_id Page identifier
     * @param page Page to add (will be moved)
     * @return true if added, false if eviction failed
     */
    bool add_page(const PageId& page_id, Page&& page);

    /**
     * Remove a page from buffer pool.
     */
    bool remove_page(const PageId& page_id);

    /**
     * Mark a page as unpinned.
     */
    void unpin_page(const PageId& page_id);

    /**
     * Mark a page as dirty.
     */
    void mark_dirty(const PageId& page_id);

    /**
     * Check if page is in buffer pool.
     */
    bool contains(const PageId& page_id) const;

    /**
     * Get current size.
     */
    size_t size() const { return page_map_.size(); }

    /**
     * Get capacity.
     */
    size_t capacity() const { return capacity_; }

    /**
     * Clear all pages.
     */
    void clear();

    /**
     * Statistics.
     */
    struct Stats {
        size_t hits = 0;
        size_t misses = 0;
        size_t evictions = 0;
        size_t current_size = 0;
        size_t capacity = 0;
    };

    Stats get_stats() const;

private:
    // Frame structure for LRU
    struct Frame {
        PageId id;
        Page page;
        bool pinned = false;
        bool dirty = false;

        Frame(PageId id, Page&& page)
            : id(std::move(id)), page(std::move(page)) {}
    };

    // Evict a page using LRU policy
    bool evict_one();

    // Update LRU order
    void touch_frame(const PageId& page_id);

    // Members
    std::unordered_map<PageId, std::unique_ptr<Frame>, PageIdHash> page_map_;
    std::list<PageId> lru_list_;  // Front = most recent, Back = least recent

    size_t capacity_;
    mutable std::mutex mutex_;

    // Statistics
    mutable std::mutex stats_mutex_;
    Stats stats_;
};

#endif // KVDB_BUFFERPOOL_H