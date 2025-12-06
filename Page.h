//
// Created by K on 2025-12-06.
//

#ifndef KVDB_PAGE_H
#define KVDB_PAGE_H

#include "PageId.h"
#include <cstdint>
#include <ctime>
#include <memory>

class Page
{
public:
    static constexpr size_t PAGE_SIZE = 4096;

    Page();
    ~Page();

    // no copy
    Page(const Page&) = delete;
    Page& operator=(const Page&) = delete;

    // move enabled
    Page(Page&& other) noexcept;
    Page& operator=(Page&& other) noexcept;

    // Getters
    const PageId& get_id() const { return id_; }
    char* get_data() { return data_.get(); }
    const char* get_data() const { return data_.get(); }
    bool is_dirty() const { return dirty_; }
    bool is_pinned() const { return pin_count_ > 0; }
    uint32_t get_pin_count() const { return pin_count_; }
    time_t get_last_access() const { return last_access_; }

    // Setters
    void set_id(const PageId& id) { id_ = id; }
    void set_dirty(bool dirty) { dirty_ = dirty; }
    void mark_dirty() { dirty_ = true; }
    void clear_dirty() { dirty_ = false; }

    // Pin/unpin
    void pin() {
        pin_count_++;
        update_access_time();
    }
    void unpin() {
        if (pin_count_ > 0) pin_count_--;
        update_access_time();
    }

    // Data ops
    void copy_from(const char* source, size_t size, size_t offset = 0);
    void copy_to(char* dest, size_t size, size_t offset = 0);

    // Reset page
    void reset();

private:
    void update_access_time();
    void allocate_buffer();

    PageId id_;
    std::unique_ptr<char[]> data_;
    bool dirty_ = false;
    uint32_t pin_count_ = 0;
    time_t last_access_ = 0;
    time_t load_time_ = 0;
};

#endif //KVDB_PAGE_H