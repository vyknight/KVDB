//
// Created by K on 2025-12-06.
//

#ifndef KVDB_DIRECTIO_H
#define KVDB_DIRECTIO_H

#include <string>
#include <cstdint>
#include <memory>

class DirectIO
{
public:
    // Open file
    static std::unique_ptr<DirectIO> open(const std::string& filename, bool read_only=false);

    ~DirectIO();

    // No copy
    DirectIO(const DirectIO&) = delete;
    DirectIO& operator=(const DirectIO&) = delete;

    // enable move
    DirectIO(DirectIO&& other) noexcept;
    DirectIO& operator=(DirectIO&& other) noexcept;

    // Read / write aligned buffers
    bool read(uint64_t offset, char* buffer, size_t size);
    bool write(uint64_t offset, const char* buffer, size_t size);

    // File info
    uint64_t file_size() const;
    bool is_open() const;
    bool is_using_direct_io() const { return using_direct_io_; }
    size_t get_block_size() const { return block_size_; }

    DirectIO();

private:

#ifdef _WIN32
    void* file_handle_ = nullptr;
#else
    int fd_ = -1;
#endif
    std::string filename_;
    bool read_only_ = true;
    bool using_direct_io_ = false;
    size_t block_size_ = 4096;  // Default to 4KB

    bool open_file(const std::string& filename, bool read_only);
    void close_file();

};


#endif //KVDB_DIRECTIO_H