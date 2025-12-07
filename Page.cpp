//
// Created by K on 2025-12-06.
//

#include "Page.h"
#include <cstring>
#include <chrono>
#include <stdexcept>

Page::Page()
{
    allocate_buffer();
}

Page::~Page() = default;

Page::Page(Page&& other) noexcept
    : id_(std::move(other.id_)),
      data_(std::move(other.data_)),
      dirty_(other.dirty_),
      pin_count_(other.pin_count_),
      last_access_(other.last_access_),
      load_time_(other.load_time_) {
    other.reset();
}

Page& Page::operator=(Page&& other) noexcept {
    if (this != &other) {
        id_ = std::move(other.id_);
        data_ = std::move(other.data_);
        dirty_ = other.dirty_;
        pin_count_ = other.pin_count_;
        last_access_ = other.last_access_;
        load_time_ = other.load_time_;
        other.reset();
    }
    return *this;
}

// necessary because I'm developing this on a windows computer
void Page::allocate_buffer() {
#ifdef _WIN32
    data_.reset(static_cast<char*>(_aligned_malloc(PAGE_SIZE, 4096)));
    if (!data_) {
        throw std::bad_alloc();
    }
#else
    // Use aligned_alloc for C++17, or posix_memalign for older
    void* ptr = nullptr;

    // Try aligned_alloc first (C++17)
    ptr = aligned_alloc(4096, PAGE_SIZE);

    if (!ptr) {
        // Fall back to posix_memalign
        if (posix_memalign(&ptr, 4096, PAGE_SIZE) != 0) {
            throw std::bad_alloc();
        }
    }

    data_.reset(static_cast<char*>(ptr));
#endif

    std::memset(data_.get(), 0, PAGE_SIZE);
}

void Page::reset()
{
    id_ = PageId();
    dirty_ = false;
    pin_count_ = 0;
    last_access_ = 0;
    load_time_ = 0;
    if (data_) {
        std::memset(data_.get(), 0, PAGE_SIZE);
    }
}

void Page::update_access_time() {
    const auto now = std::chrono::system_clock::now();
    last_access_ = std::chrono::system_clock::to_time_t(now);
}

void Page::copy_from(const char* source, size_t size, size_t offset) {
    if (offset + size > PAGE_SIZE) {
        throw std::out_of_range("Page buffer overflow");
    }
    std::memcpy(data_.get() + offset, source, size);
    dirty_ = true;
    update_access_time();
}

void Page::copy_to(char* dest, size_t size, size_t offset) {
    if (offset + size > PAGE_SIZE) {
        throw std::out_of_range("Page buffer underflow");
    }
    std::memcpy(dest, data_.get() + offset, size);
    update_access_time();
}