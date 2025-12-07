#ifndef KVDB_BUFFERPOOL_H
#define KVDB_BUFFERPOOL_H

#include "Page.h"
#include <unordered_map>
#include <list>
#include <vector>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <functional>

// Forward declaration
class DirectIO;

class BufferPool {
public:
    /**
     * Constructor for BufferPool
     * @param max_pages Maximum number of pages in buffer pool
     * @param use_lru If true, use LRU eviction; if false, use Clock eviction
     */
    explicit BufferPool(size_t max_pages = 1024, bool use_lru = true);

    ~BufferPool();

    // No copy
    BufferPool(const BufferPool&) = delete;
    BufferPool& operator=(const BufferPool&) = delete;

    // Move enabled
    BufferPool(BufferPool&& other) noexcept;
    BufferPool& operator=(BufferPool&& other) noexcept;

    /**
     * Get a page from buffer pool or load from disk
     * @param page_id The page identifier
     * @param file DirectIO file handle for reading if needed
     * @param read_only Whether to mark page as read-only
     * @return Pointer to page if successful, nullptr otherwise
     */
    Page* get_page(const PageId& page_id, DirectIO* file = nullptr, bool read_only = false);

    /**
     * Mark a page as unpinned (decrease pin count)
     * @param page_id The page identifier
     */
    void unpin_page(const PageId& page_id);

    /**
     * Mark a page as dirty (modified)
     * @param page_id The page identifier
     */
    void mark_dirty(const PageId& page_id);

    /**
     * Write a specific page back to disk
     * @param page_id The page identifier
     * @param file DirectIO file handle
     * @return true if successful, false otherwise
     */
    bool flush_page(const PageId& page_id, DirectIO* file);

    /**
     * Remove a page from buffer pool
     * @param page_id The page identifier
     * @return true if page was removed, false if not found or pinned
     */
    bool evict_page(const PageId& page_id);

    /**
     * Get buffer pool statistics
     */
    struct Stats {
        size_t hits = 0;
        size_t misses = 0;
        size_t evictions = 0;
        size_t current_size = 0;
        size_t max_size = 0;
        size_t dirty_pages = 0;
        size_t pinned_pages = 0;
    };

    Stats get_stats() const;

    /**
     * Clear all statistics
     */
    void clear_stats();

    /**
     * Resize the buffer pool
     * @param new_max_pages New maximum size
     */
    void resize(size_t new_max_pages);

    /**
     * Clear the entire buffer pool (flush dirty pages first)
     * @param file DirectIO file handle for writing dirty pages
     * @return Number of pages flushed
     */
    size_t clear(DirectIO* file = nullptr);

    /**
     * Check if a page is in buffer pool
     * @param page_id The page identifier
     * @return true if page is in buffer pool
     */
    bool contains(const PageId& page_id) const;

    /**
     * Get current size (number of pages in buffer)
     */
    size_t size() const;

    /**
     * Get maximum capacity
     */
    size_t capacity() const { return max_pages_; }

    /**
     * Get hit rate
     */
    double hit_rate() const;

private:
    // Simple frame structure
    struct Frame {
        PageId id;
        Page page;
        bool dirty = false;
        bool pinned = false;
        bool referenced = false;  // For Clock algorithm

        Frame() = default;
        explicit Frame(const PageId& id) : id(id) {}
    };

    // Private methods
    Frame* find_frame(const PageId& page_id);
    Frame* allocate_frame(const PageId& page_id);
    bool evict_one_frame();

    // Eviction policy implementations
    void lru_access(Frame* frame);
    void lru_evict();
    void clock_evict();

    // Hash function using std::hash
    size_t hash(const PageId& page_id) const;

    // Load page from disk
    bool load_page(Frame* frame, DirectIO* file);

    // Write page to disk
    bool write_page(Frame* frame, DirectIO* file);

    // Simple chaining for hash table collisions
    struct HashEntry {
        Frame* frame;
        HashEntry* next = nullptr;

        HashEntry(Frame* f) : frame(f) {}
    };

    // Members
    std::vector<HashEntry*> hash_table_;
    std::vector<Frame> frames_;           // All frames allocated contiguously
    std::vector<size_t> free_frames_;     // Indices of free frames

    // LRU list - store indices into frames_ vector
    std::list<size_t> lru_list_;

    // Statistics
    mutable std::mutex stats_mutex_;
    Stats stats_;

    // Configuration
    size_t max_pages_;
    bool use_lru_;

    // For thread safety
    mutable std::mutex pool_mutex_;
};

#endif // KVDB_BUFFERPOOL_H