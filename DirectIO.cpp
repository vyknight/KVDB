//
// Created by K on 2025-12-06.
//

#include "DirectIO.h"
#include <iostream>
#include <memory>

#ifdef _WIN32
#include <windows.h>
#else
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#endif

DirectIO::DirectIO() = default;

DirectIO::~DirectIO() {
    close_file();
}

DirectIO::DirectIO(DirectIO&& other) noexcept
    :
#ifdef _WIN32
      file_handle_(other.file_handle_),
#else
      fd_(other.fd_),
#endif
      filename_(std::move(other.filename_)),
      read_only_(other.read_only_),
      using_direct_io_(other.using_direct_io_),
      block_size_(other.block_size_) {

#ifdef _WIN32
    other.file_handle_ = nullptr;
#else
    other.fd_ = -1;
#endif
    other.using_direct_io_ = false;
}

DirectIO& DirectIO::operator=(DirectIO&& other) noexcept {
    if (this != &other) {
        close_file();

#ifdef _WIN32
        file_handle_ = other.file_handle_;
        other.file_handle_ = nullptr;
#else
        fd_ = other.fd_;
        other.fd_ = -1;
#endif

        filename_ = std::move(other.filename_);
        read_only_ = other.read_only_;
        using_direct_io_ = other.using_direct_io_;
        block_size_ = other.block_size_;
        other.using_direct_io_ = false;
    }
    return *this;
}

std::unique_ptr<DirectIO> DirectIO::open(const std::string& filename, bool read_only) {
    auto instance = std::make_unique<DirectIO>();
    if (instance->open_file(filename, read_only)) {
        return instance;
    }
    return nullptr;
}

bool DirectIO::open_file(const std::string& filename, bool read_only) {
    filename_ = filename;
    read_only_ = read_only;

#ifdef _WIN32
    // Windows implementation
    DWORD desired_access = read_only ? GENERIC_READ : (GENERIC_READ | GENERIC_WRITE);
    DWORD share_mode = FILE_SHARE_READ | FILE_SHARE_WRITE;
    DWORD creation_disposition = OPEN_EXISTING;
    DWORD flags = FILE_FLAG_NO_BUFFERING | FILE_FLAG_WRITE_THROUGH;

    file_handle_ = CreateFileA(
        filename.c_str(),
        desired_access,
        share_mode,
        nullptr,
        creation_disposition,
        flags,
        nullptr
    );

    if (file_handle_ == INVALID_HANDLE_VALUE) {
        DWORD error = GetLastError();
        std::cerr << "Failed to open file " << filename
                  << " with direct I/O, error: " << error << std::endl;
        file_handle_ = nullptr;
        return false;
    }

    using_direct_io_ = true;
    block_size_ = 4096;  // Windows typically uses 4KB for FILE_FLAG_NO_BUFFERING
    return true;

#else
    // Unix/Linux/macOS implementation

    // First try with O_DIRECT
    int flags = read_only ? O_RDONLY : O_RDWR;
    flags |= O_DIRECT;

    fd_ = ::open(filename.c_str(), flags);

    if (fd_ >= 0) {
        using_direct_io_ = true;
        std::cout << "Successfully opened " << filename << " with O_DIRECT" << std::endl;
    } else {
        // O_DIRECT failed, try without it
        flags &= ~O_DIRECT;
        fd_ = ::open(filename.c_str(), flags);

        if (fd_ < 0) {
            std::cerr << "Failed to open file " << filename
                      << " even without O_DIRECT: " << strerror(errno) << std::endl;
            return false;
        }

        using_direct_io_ = false;
        std::cout << "Warning: Opened " << filename << " without O_DIRECT (using buffered I/O)" << std::endl;
    }

    // Get block size for alignment
    struct stat st;
    if (fstat(fd_, &st) == 0) {
        block_size_ = static_cast<size_t>(st.st_blksize);
        if (block_size_ == 0) {
            block_size_ = 4096;  // Default to 4KB
        }
    } else {
        block_size_ = 4096;  // Default to 4KB
    }

    std::cout << "Block size for " << filename << ": " << block_size_ << " bytes" << std::endl;

    return true;
#endif
}

void DirectIO::close_file() {
#ifdef _WIN32
    if (file_handle_) {
        CloseHandle(file_handle_);
        file_handle_ = nullptr;
    }
#else
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
#endif
    using_direct_io_ = false;
}

bool DirectIO::read(uint64_t offset, char* buffer, size_t size) {
    // Check alignment if using direct I/O
    if (using_direct_io_) {
        if (size % block_size_ != 0) {
            std::cerr << "Direct I/O read size " << size
                      << " not multiple of block size " << block_size_ << std::endl;
            return false;
        }

        if (offset % block_size_ != 0) {
            std::cerr << "Direct I/O read offset " << offset
                      << " not aligned to block size " << block_size_ << std::endl;
            return false;
        }

        uintptr_t buf_addr = reinterpret_cast<uintptr_t>(buffer);
        if (buf_addr % block_size_ != 0) {
            std::cerr << "Direct I/O buffer address not aligned to block size " << block_size_ << std::endl;
            return false;
        }
    }

#ifdef _WIN32
    if (!file_handle_) return false;

    OVERLAPPED overlapped = {};
    overlapped.Offset = static_cast<DWORD>(offset);
    overlapped.OffsetHigh = static_cast<DWORD>(offset >> 32);

    DWORD bytes_read = 0;
    if (!ReadFile(file_handle_, buffer, static_cast<DWORD>(size), &bytes_read, &overlapped)) {
        DWORD error = GetLastError();
        if (error != ERROR_IO_PENDING) {
            std::cerr << "ReadFile failed, error: " << error << std::endl;
            return false;
        }

        // Wait for the overlapped I/O to complete
        if (!GetOverlappedResult(file_handle_, &overlapped, &bytes_read, TRUE)) {
            std::cerr << "GetOverlappedResult failed, error: " << GetLastError() << std::endl;
            return false;
        }
    }

    return bytes_read == size;

#else
    if (fd_ < 0) return false;

    ssize_t bytes_read = 0;
    size_t total_read = 0;

    while (total_read < size) {
        bytes_read = ::pread(fd_, buffer + total_read, size - total_read,
                           static_cast<off_t>(offset + total_read));

        if (bytes_read < 0) {
            if (errno == EINTR) continue;  // Interrupted, try again

            std::cerr << "pread failed: " << strerror(errno) << std::endl;
            return false;
        }

        if (bytes_read == 0) {
            // EOF reached before reading requested amount
            std::cerr << "EOF reached, read " << total_read << " of " << size << " bytes" << std::endl;
            return false;
        }

        total_read += bytes_read;
    }

    return total_read == size;
#endif
}

bool DirectIO::write(uint64_t offset, const char* buffer, size_t size) {
    if (read_only_) {
        std::cerr << "Cannot write to read-only file" << std::endl;
        return false;
    }

    // Check alignment if using direct I/O
    if (using_direct_io_) {
        if (size % block_size_ != 0) {
            std::cerr << "Direct I/O write size " << size
                      << " not multiple of block size " << block_size_ << std::endl;
            return false;
        }

        if (offset % block_size_ != 0) {
            std::cerr << "Direct I/O write offset " << offset
                      << " not aligned to block size " << block_size_ << std::endl;
            return false;
        }

        uintptr_t buf_addr = reinterpret_cast<uintptr_t>(buffer);
        if (buf_addr % block_size_ != 0) {
            std::cerr << "Direct I/O buffer address not aligned to block size " << block_size_ << std::endl;
            return false;
        }
    }

#ifdef _WIN32
    if (!file_handle_) return false;

    OVERLAPPED overlapped = {};
    overlapped.Offset = static_cast<DWORD>(offset);
    overlapped.OffsetHigh = static_cast<DWORD>(offset >> 32);

    DWORD bytes_written = 0;
    if (!WriteFile(file_handle_, buffer, static_cast<DWORD>(size), &bytes_written, &overlapped)) {
        DWORD error = GetLastError();
        if (error != ERROR_IO_PENDING) {
            std::cerr << "WriteFile failed, error: " << error << std::endl;
            return false;
        }

        // Wait for the overlapped I/O to complete
        if (!GetOverlappedResult(file_handle_, &overlapped, &bytes_written, TRUE)) {
            std::cerr << "GetOverlappedResult failed, error: " << GetLastError() << std::endl;
            return false;
        }
    }

    return bytes_written == size;

#else
    if (fd_ < 0) return false;

    ssize_t bytes_written = 0;
    size_t total_written = 0;

    while (total_written < size) {
        bytes_written = ::pwrite(fd_, buffer + total_written, size - total_written,
                               static_cast<off_t>(offset + total_written));

        if (bytes_written < 0) {
            if (errno == EINTR) continue;  // Interrupted, try again

            std::cerr << "pwrite failed: " << strerror(errno) << std::endl;
            return false;
        }

        total_written += bytes_written;
    }

    return total_written == size;
#endif
}

uint64_t DirectIO::file_size() const {
#ifdef _WIN32
    if (!file_handle_) return 0;

    LARGE_INTEGER size;
    if (!GetFileSizeEx(file_handle_, &size)) {
        return 0;
    }
    return static_cast<uint64_t>(size.QuadPart);
#else
    if (fd_ < 0) return 0;

    struct stat st;
    if (fstat(fd_, &st) != 0) {
        return 0;
    }
    return static_cast<uint64_t>(st.st_size);
#endif
}

bool DirectIO::is_open() const {
#ifdef _WIN32
    return file_handle_ != nullptr;
#else
    return fd_ >= 0;
#endif
}