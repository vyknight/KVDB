//
// Created by K on 2025-12-06.
//

#ifndef KVDB_BUFFERPOOL_H
#define KVDB_BUFFERPOOL_H

#include "Page.h"
#include "PageId.h"
#include "DirectIO.h"
#include <vector>
#include <list>
#include <unordered_map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <functional>

class BufferPool
{
public:
    struct Config
    {
        size_t max_pages;
        size_t initial_global_depth;
        size_t max_global_depth;
        size_t bucket_capacity;
        bool use_direct_io;
        bool debug_logging;

        Config(size_t mp=100, size_t igd=2, size_t mgd=10, size_t bc=4, bool dio=true, bool dl=false) :
            max_pages(mp),
            initial_global_depth(igd),
            max_global_depth(mgd),
            bucket_capacity(bc),
            use_direct_io(dio),
            debug_logging(dl)
        {}
    };

    explicit BufferPool(const Config& config = Config());
    ~BufferPool();

    // no copy
    BufferPool(const BufferPool&) = delete;
    BufferPool& operator=(const BufferPool&) = delete;

    // Core ops
    Page* get_page(const std::string& filename, uint64_t offset);
    Page* get_page(const PageId& page_id);

    void release_page(Page* page);
    void mark_dirty(Page* page);
    void flush_page(Page* page);

    // pool management
    void flush_all();
    void clear();
    void resize(size_t new_max_pages);

    // stats
    struct BufferPoolStats
    {
        size_t total_pages = 0;
        size_t used_pages = 0;
        size_t hits = 0;
        size_t misses = 0;
        size_t evictions = 0;
        size_t disk_reads = 0;
        size_t disk_writes = 0;
        size_t directory_size = 0;
        size_t total_buckets = 0;
        size_t splits = 0;
        size_t merges = 0;
    };

    BufferPoolStats get_stats() const;
    void reset_stats();

    // debug
    void print_directory() const;
    void print_bucket_details() const;

private:
    // extensible hashing structures
    struct Bucket
    {
        std::vector<Page> pages;
        int local_depth = 0;
        size_t bucket_id;  // debug member

        explicit Bucket(size_t id, int depth = 0) : local_depth(depth), bucket_id(id) {}

        bool contains(const PageId& page_id) const;
        Page* find_page(const PageId& page_id);
        const Page* find_page(const PageId& page_id) const;
        bool is_full(size_t capacity) const { return pages.size() >= capacity; }
        size_t size() const { return pages.size(); }
    };

    struct LRUEntry
    {
        Page* page;
        std::shared_ptr<Bucket> bucket;
        size_t bucket_idx;

        LRUEntry(Page* p, std::shared_ptr<Bucket> b, const size_t idx)
            : page(p), bucket(std::move(b)), bucket_idx(idx) {}
    };

    size_t hash_page_id(const PageId& page_id) const;
    size_t get_directory_index(size_t hash, int depth) const;
    size_t get_directory_index(const PageId& page_id) const;

    // Page management
    Page* find_page_in_pool(const PageId& page_id);
    Page* load_page_from_disk(const PageId& page_id);
    void evict_page();

    // bucket management
    std::shared_ptr<Bucket> get_bucket_for_page(const PageId& page_id);
    void split_bucket(size_t bucket_idx);
    void maybe_split_bucket(size_t bucket_idx);
    void expand_directory();
    void update_directory_pointers(size_t old_idx, size_t new_idx, std::shared_ptr<Bucket> new_bucket);

    // lru management
    void update_lru(Page* page, std::shared_ptr<Bucket> bucket, size_t bucket_idx);
    void remove_from_lru(Page* page);
    Page* find_lru_victim();

    // io ops
    bool read_page_from_disk(const PageId& page_id, Page& page);
    bool write_page_to_disk(Page& page);
    std::unique_ptr<DirectIO> get_file_handle(const std::string& filename, bool read_only = true);

    // debug log
    void log_debug(const std::string& message) const;

    Config config_;

    // extensible hashing directory
    std::vector<std::shared_ptr<Bucket>> directory_;
    int global_depth_ = 0;
    size_t next_bucket_id_ = 0;

    // LRU list
    std::list<LRUEntry> lru_list_;
    std::unordered_map<Page*, std::list<LRUEntry>::iterator> lru_map_;

    mutable std::mutex files_mutex_;
    std::unordered_map<std::string, std::unique_ptr<DirectIO>> open_files_;

    // Statistics
    mutable std::mutex stats_mutex_;
    BufferPoolStats stats_;

    // sync
    mutable std::shared_mutex directory_mutex_;

    // find page in all buckets helper
    std::unordered_map<PageId, std::pair<std::shared_ptr<Bucket>, size_t>, PageIdHash> page_to_bucket_map_;
};

#endif //KVDB_BUFFERPOOL_H