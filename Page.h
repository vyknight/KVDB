#ifndef KVDB_PAGE_H
#define KVDB_PAGE_H

#include "PageId.h"
#include <cstdint>
#include <ctime>
#include <vector>
#include <cstring>

class Page {
public:
    static constexpr size_t PAGE_SIZE = 4096;

    Page();
    ~Page() = default;

    // No copy
    Page(const Page&) = delete;
    Page& operator=(const Page&) = delete;

    // Move enabled
    Page(Page&& other) noexcept = default;
    Page& operator=(Page&& other) noexcept = default;

    // Getters
    const PageId& get_id() const { return id_; }
    char* get_data() { return data_.data(); }
    const char* get_data() const { return data_.data(); }
    bool is_dirty() const { return dirty_; }
    bool is_pinned() const { return pin_count_ > 0; }
    uint32_t get_pin_count() const { return pin_count_; }
    time_t get_last_access() const { return last_access_; }
    time_t get_load_time() const { return load_time_; }

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
        if (pin_count_ > 0) {
            pin_count_--;
        }
        update_access_time();
    }

    // Data operations
    void copy_from(const char* source, size_t size, size_t offset = 0) {
        if (offset + size > PAGE_SIZE) {
            throw std::out_of_range("Copy exceeds page size");
        }
        std::memcpy(data_.data() + offset, source, size);
        dirty_ = true;
        update_access_time();
    }

    void copy_to(char* dest, size_t size, size_t offset = 0) {
        if (offset + size > PAGE_SIZE) {
            throw std::out_of_range("Copy exceeds page size");
        }
        std::memcpy(dest, data_.data() + offset, size);
        update_access_time();
    }

    // Clear/reset
    void clear() {
        std::memset(data_.data(), 0, PAGE_SIZE);
        dirty_ = false;
        update_access_time();
    }

    void reset() {
        clear();
        id_ = PageId();
        pin_count_ = 0;
        load_time_ = std::time(nullptr);
        last_access_ = load_time_;
    }

private:
    void update_access_time() {
        last_access_ = std::time(nullptr);
    }

    std::vector<char> data_;  // Automatically manages memory
    PageId id_;
    bool dirty_ = false;
    uint32_t pin_count_ = 0;
    time_t last_access_ = 0;
    time_t load_time_ = 0;
};

#endif // KVDB_PAGE_H