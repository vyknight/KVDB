#include "Page.h"
#include <chrono>
#include <stdexcept>

Page::Page()
{
    // Zero-initialize the aligned storage
    std::memset(data_ptr(), 0, PAGE_SIZE);

    // Set initial timestamps
    const auto now = std::chrono::system_clock::now();
    load_time_ = std::chrono::system_clock::to_time_t(now);
    last_access_ = load_time_;
}

// Move constructor
Page::Page(Page&& other) noexcept
    : id_(std::move(other.id_)),
      dirty_(other.dirty_),
      pin_count_(other.pin_count_),
      last_access_(other.last_access_),
      load_time_(other.load_time_)
{
    // IMPORTANT: Copy the bytes from other's storage to this storage
    std::memcpy(data_ptr(), other.data_ptr(), PAGE_SIZE);

    // Reset the source object
    other.reset();
}

// Move assignment operator
Page& Page::operator=(Page&& other) noexcept {
    if (this != &other) {
        // Copy the bytes from other's storage to this storage
        std::memcpy(data_ptr(), other.data_ptr(), PAGE_SIZE);

        id_ = std::move(other.id_);
        dirty_ = other.dirty_;
        pin_count_ = other.pin_count_;
        last_access_ = other.last_access_;
        load_time_ = other.load_time_;

        other.reset();
    }
    return *this;
}

void Page::reset()
{
    // Zero out the storage
    std::memset(data_ptr(), 0, PAGE_SIZE);

    // Reset metadata
    id_ = PageId();
    dirty_ = false;
    pin_count_ = 0;

    // Set new timestamps
    const auto now = std::chrono::system_clock::now();
    load_time_ = std::chrono::system_clock::to_time_t(now);
    last_access_ = load_time_;
}

void Page::update_access_time() {
    const auto now = std::chrono::system_clock::now();
    last_access_ = std::chrono::system_clock::to_time_t(now);
}

void Page::copy_from(const char* source, size_t size, size_t offset) {
    if (offset + size > PAGE_SIZE) {
        throw std::out_of_range("Page buffer overflow");
    }
    std::memcpy(data_ptr() + offset, source, size);
    dirty_ = true;
    update_access_time();
}

void Page::copy_to(char* dest, size_t size, size_t offset) {
    if (offset + size > PAGE_SIZE) {
        throw std::out_of_range("Page buffer underflow");
    }
    std::memcpy(dest, data_ptr() + offset, size);
    update_access_time();
}