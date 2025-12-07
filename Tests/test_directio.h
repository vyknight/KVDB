//
// Created by K on 2025-12-07.
//

#ifndef KVDB_TEST_DIRECTIO_H
#define KVDB_TEST_DIRECTIO_H

#include <memory>
#include <cstring>

class AlignedBuffer {
public:
    AlignedBuffer(size_t size, size_t alignment = 4096) : size_(size) {
#ifdef _WIN32
        data_ = static_cast<char*>(_aligned_malloc(size, alignment));
        if (!data_) throw std::bad_alloc();
#else
        if (posix_memalign(reinterpret_cast<void**>(&data_), alignment, size) != 0) {
            throw std::bad_alloc();
        }
#endif
        std::memset(data_, 0, size);
    }

    ~AlignedBuffer() {
#ifdef _WIN32
        _aligned_free(data_);
#else
        free(data_);
#endif
    }

    char* data() { return data_; }
    const char* data() const { return data_; }

    // Add array access operators
    char& operator[](size_t index) {
        if (index >= size_) {
            throw std::out_of_range("AlignedBuffer index out of range");
        }
        return data_[index];
    }

    const char& operator[](size_t index) const {
        if (index >= size_) {
            throw std::out_of_range("AlignedBuffer index out of range");
        }
        return data_[index];
    }

    size_t size() const { return size_; }

    // Disable copy
    AlignedBuffer(const AlignedBuffer&) = delete;
    AlignedBuffer& operator=(const AlignedBuffer&) = delete;

    // Enable move
    AlignedBuffer(AlignedBuffer&& other) noexcept : data_(other.data_), size_(other.size_) {
        other.data_ = nullptr;
        other.size_ = 0;
    }

    AlignedBuffer& operator=(AlignedBuffer&& other) noexcept {
        if (this != &other) {
#ifdef _WIN32
            _aligned_free(data_);
#else
            free(data_);
#endif
            data_ = other.data_;
            size_ = other.size_;
            other.data_ = nullptr;
            other.size_ = 0;
        }
        return *this;
    }

private:
    char* data_ = nullptr;
    size_t size_ = 0;
};

inline uint64_t align_offset(uint64_t offset, size_t block_size) {
    return (offset / block_size) * block_size;
}

inline size_t align_size(size_t size, size_t block_size) {
    return ((size + block_size - 1) / block_size) * block_size;
}

int directio_tests_main();

#endif //KVDB_TEST_DIRECTIO_H