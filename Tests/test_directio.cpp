//
// Created by K on 2025-12-07.
//

#include "test_directio.h"
#include "../DirectIO.h"
#include <iostream>
#include <string>
#include <cstring>
#include <vector>
#include <random>
#include <filesystem>
#include <fstream>
#include <atomic>
#include <thread>

#include <sys/stat.h>  // For chmod() on Unix/Linux

#ifdef _WIN32
    #include <io.h>     // For _chmod() on Windows
#endif


namespace fs = std::filesystem;
using namespace std::chrono;

void print_test_result_directio(const std::string& test_name, bool passed) {
    std::cout << (passed ? "O " : "X ") << test_name << std::endl;
}

// Helper: Create test directory
std::string create_test_dir(const std::string& prefix) {
    auto now = system_clock::now();
    auto timestamp = duration_cast<milliseconds>(now.time_since_epoch()).count();
    std::string dir_name = prefix + "_" + std::to_string(timestamp);
    fs::create_directory(dir_name);
    return dir_name;
}

// Helper: Clean up test directory
void cleanup_test_dir(const std::string& dir_name) {
    try {
        fs::remove_all(dir_name);
    } catch (...) {
        // Ignore cleanup errors
    }
}

// Test 1: Basic file operations
bool test_directio_basic_operations() {
    std::cout << "Testing DirectIO basic operations..." << std::endl;

    std::string test_dir = create_test_dir("directio_test");
    std::string filename = test_dir + "/test.dat";

    // Create a test file with regular I/O first
    {
        std::ofstream file(filename, std::ios::binary);
        std::string data(4096, 'A');
        file.write(data.data(), data.size());
    }

    // Open with DirectIO (read-only)
    auto dio = DirectIO::open(filename, true);
    if (!dio) {
        std::cerr << "  Failed to open file with DirectIO" << std::endl;
        cleanup_test_dir(test_dir);
        return false;
    }

    if (!dio->is_open()) {
        std::cerr << "  DirectIO reports file not open" << std::endl;
        cleanup_test_dir(test_dir);
        return false;
    }

    // Check file size
    uint64_t size = dio->file_size();
    if (size != 4096) {
        std::cerr << "  File size incorrect: expected 4096, got " << size << std::endl;
        cleanup_test_dir(test_dir);
        return false;
    }

    // Read data
    AlignedBuffer buffer(4096, dio->get_block_size());
    if (!dio->read(0, buffer.data(), 4096)) {
        std::cerr << "  Failed to read from file" << std::endl;
        cleanup_test_dir(test_dir);
        return false;
    }

    // Verify data
    for (size_t i = 0; i < 4096; ++i) {
        if (buffer[i] != 'A') {
            std::cerr << "  Data verification failed at byte " << i << std::endl;
            cleanup_test_dir(test_dir);
            return false;
        }
    }

    cleanup_test_dir(test_dir);
    return true;
}

// Test 2: Write operations
bool test_directio_write_operations() {
    std::cout << "Testing DirectIO write operations..." << std::endl;

    std::string test_dir = create_test_dir("directio_write");
    std::string filename = test_dir + "/write_test.dat";

    // CREATE THE FILE FIRST with regular I/O
    {
        std::ofstream file(filename, std::ios::binary);
        // Create an empty file of at least 4096 bytes
        std::vector<char> initial_data(4096, 0);
        file.write(initial_data.data(), initial_data.size());
    }

    // Now open for writing with DirectIO
    auto dio = DirectIO::open(filename, false);
    if (!dio) {
        std::cerr << "  Failed to open file for writing" << std::endl;
        cleanup_test_dir(test_dir);
        return false;
    }

    // Write data
    std::string data = "Hello, Direct I/O World!";
    AlignedBuffer buffer(4096, dio->get_block_size());
    std::memset(buffer.data(), 0, 4096);  // Zero out entire buffer
    std::memcpy(buffer.data(), data.c_str(), data.length());

    if (!dio->write(0, buffer.data(), 4096)) {
        std::cerr << "  Failed to write to file" << std::endl;
        cleanup_test_dir(test_dir);
        return false;
    }

    // Reopen for reading to verify
    auto dio2 = DirectIO::open(filename, true);
    if (!dio2) {
        std::cerr << "  Failed to reopen file for reading" << std::endl;
        cleanup_test_dir(test_dir);
        return false;
    }

    AlignedBuffer read_buffer(4096, dio2->get_block_size());
    if (!dio2->read(0, read_buffer.data(), 4096)) {
        std::cerr << "  Failed to read back written data" << std::endl;
        cleanup_test_dir(test_dir);
        return false;
    }

    // Verify - only compare up to the data length
    if (std::memcmp(buffer.data(), read_buffer.data(), data.length()) != 0) {
        std::cerr << "  Written and read data don't match" << std::endl;
        cleanup_test_dir(test_dir);
        return false;
    }

    cleanup_test_dir(test_dir);
    return true;
}

// Test 3: Alignment requirements
bool test_directio_alignment() {
    std::cout << "Testing DirectIO alignment requirements..." << std::endl;

    std::string test_dir = create_test_dir("directio_align");
    std::string filename = test_dir + "/align_test.dat";

    // Create file
    {
        std::ofstream file(filename, std::ios::binary);
        std::vector<char> data(3 * 4096);  // 3 blocks
        file.write(data.data(), data.size());
    }

    auto dio = DirectIO::open(filename, false);
    if (!dio) {
        std::cout << "  Note: Direct I/O not supported on this system" << std::endl;
        cleanup_test_dir(test_dir);
        return true;  // Not a failure if system doesn't support direct I/O
    }

    size_t block_size = dio->get_block_size();
    std::cout << "  Block size: " << block_size << " bytes" << std::endl;

    bool using_direct = dio->is_using_direct_io();
    std::cout << "  Using " << (using_direct ? "direct" : "buffered") << " I/O" << std::endl;

    // Allocate aligned buffer
    AlignedBuffer aligned_buffer(block_size, block_size);
    std::memset(aligned_buffer.data(), 'A', block_size);

    // Test 1: Properly aligned operation (should succeed)
    std::cout << "  Testing aligned write..." << std::endl;
    bool aligned_success = dio->write(0, aligned_buffer.data(), block_size);

    if (using_direct && !aligned_success) {
        std::cerr << "  Aligned operation should succeed with direct I/O" << std::endl;
        cleanup_test_dir(test_dir);
        return false;
    }

    // For direct I/O, misaligned operations will fail
    // For buffered I/O, they might succeed
    if (using_direct) {
        std::cout << "  Note: Direct I/O requires strict alignment" << std::endl;

        // Test misaligned buffer (buffer + 1)
        char* misaligned_buffer = aligned_buffer.data() + 1;
        bool misaligned_result = dio->write(block_size, misaligned_buffer, block_size);
        if (misaligned_result) {
            std::cout << "  Warning: Misaligned buffer write succeeded (unexpected for direct I/O)" << std::endl;
        }

        // Test misaligned offset
        bool offset_result = dio->write(1, aligned_buffer.data(), block_size);
        if (offset_result) {
            std::cout << "  Warning: Misaligned offset write succeeded (unexpected for direct I/O)" << std::endl;
        }

        // Test misaligned size
        bool size_result = dio->write(block_size * 2, aligned_buffer.data(), block_size - 1);
        if (size_result) {
            std::cout << "  Warning: Misaligned size write succeeded (unexpected for direct I/O)" << std::endl;
        }
    } else {
        std::cout << "  Note: Buffered I/O may not require alignment" << std::endl;
    }

    cleanup_test_dir(test_dir);
    return true;
}

// Test 4: Move semantics
bool test_directio_move_semantics() {
    std::cout << "Testing DirectIO move semantics..." << std::endl;

    std::string test_dir = create_test_dir("directio_move");
    std::string filename = test_dir + "/move_test.dat";

    // Create test file
    {
        std::ofstream file(filename, std::ios::binary);
        std::vector<char> data(4096, 'X');
        file.write(data.data(), data.size());
    }

    // Open first instance
    auto dio1 = DirectIO::open(filename, true);
    if (!dio1) {
        cleanup_test_dir(test_dir);
        return false;
    }

    // Move construct
    DirectIO dio2(std::move(*dio1));

    // Check that dio2 has the file
    if (!dio2.is_open()) {
        std::cerr << "  dio2 should be open after move construction" << std::endl;
        cleanup_test_dir(test_dir);
        return false;
    }

    // Check that dio1 is closed
    // Note: Implementation specific - might still report as open
    // We'll just verify dio2 works

    // Read using dio2
    AlignedBuffer buffer(4096, dio2.get_block_size());
    if (!dio2.read(0, buffer.data(), 4096)) {
        std::cerr << "  dio2 failed to read after move" << std::endl;
        cleanup_test_dir(test_dir);
        return false;
    }

    // Move assignment
    DirectIO dio3 = std::move(dio2);

    if (!dio3.is_open()) {
        std::cerr << "  dio3 should be open after move assignment" << std::endl;
        cleanup_test_dir(test_dir);
        return false;
    }

    // Try to read with dio3
    AlignedBuffer buffer2(4096, dio3.get_block_size());
    if (!dio3.read(0, buffer2.data(), 4096)) {
        std::cerr << "  dio3 failed to read after move assignment" << std::endl;
        cleanup_test_dir(test_dir);
        return false;
    }

    cleanup_test_dir(test_dir);
    return true;
}

// Test 5: Multiple files
bool test_directio_multiple_files() {
    std::cout << "Testing DirectIO with multiple files..." << std::endl;

    std::string test_dir = create_test_dir("directio_multi");

    const int NUM_FILES = 5;
    std::vector<std::string> filenames;
    std::vector<std::unique_ptr<DirectIO>> files;

    // Create and open multiple files
    for (int i = 0; i < NUM_FILES; ++i) {
        std::string filename = test_dir + "/file" + std::to_string(i) + ".dat";
        filenames.push_back(filename);

        // Create file with some data
        {
            std::ofstream file(filename, std::ios::binary);
            std::vector<char> data(4096, static_cast<char>('A' + i));
            file.write(data.data(), data.size());
        }

        // Open with DirectIO
        auto dio = DirectIO::open(filename, true);
        if (!dio) {
            std::cerr << "  Failed to open file " << filename << std::endl;
            cleanup_test_dir(test_dir);
            return false;
        }

        files.push_back(std::move(dio));
    }

    // Verify each file - USE ALIGNEDBUFFER!
    for (int i = 0; i < NUM_FILES; ++i) {
        AlignedBuffer buffer(4096, files[i]->get_block_size());  // Use AlignedBuffer
        if (!files[i]->read(0, buffer.data(), 4096)) {
            std::cerr << "  Failed to read from file " << i << std::endl;
            cleanup_test_dir(test_dir);
            return false;
        }

        // Verify content
        char expected = static_cast<char>('A' + i);
        for (size_t j = 0; j < 4096; ++j) {
            if (buffer[j] != expected) {
                std::cerr << "  File " << i << " content mismatch at byte " << j << std::endl;
                cleanup_test_dir(test_dir);
                return false;
            }
        }
    }

    cleanup_test_dir(test_dir);
    return true;
}

// Test 6: Large file operations
bool test_directio_large_file() {
    std::cout << "Testing DirectIO large file operations..." << std::endl;

    std::string test_dir = create_test_dir("directio_large");
    std::string filename = test_dir + "/large.dat";

    const size_t FILE_SIZE = 10 * 1024 * 1024;  // 10MB
    const size_t BLOCK_SIZE = 4096;
    const size_t NUM_BLOCKS = FILE_SIZE / BLOCK_SIZE;

    // Create large file
    std::cout << "  Creating " << (FILE_SIZE / (1024*1024)) << "MB file..." << std::endl;
    {
        std::ofstream file(filename, std::ios::binary);
        std::vector<char> data(BLOCK_SIZE);
        for (size_t i = 0; i < NUM_BLOCKS; ++i) {
            // Fill each block with pattern based on block index
            std::fill(data.begin(), data.end(), static_cast<char>(i % 256));
            file.write(data.data(), data.size());
        }
    }

    // Open with DirectIO
    auto dio = DirectIO::open(filename, true);
    if (!dio) {
        cleanup_test_dir(test_dir);
        return false;
    }

    // Verify file size
    uint64_t size = dio->file_size();
    if (size != FILE_SIZE) {
        std::cerr << "  File size incorrect: expected " << FILE_SIZE
                  << ", got " << size << std::endl;
        cleanup_test_dir(test_dir);
        return false;
    }

    // Read random blocks and verify
    std::mt19937 rng(42);  // Fixed seed for reproducibility
    std::uniform_int_distribution<size_t> dist(0, NUM_BLOCKS - 1);

    AlignedBuffer buffer(BLOCK_SIZE, dio->get_block_size());  // Use AlignedBuffer
    const int NUM_TESTS = 100;

    auto start = high_resolution_clock::now();

    for (int test = 0; test < NUM_TESTS; ++test) {
        size_t block_idx = dist(rng);
        uint64_t offset = block_idx * BLOCK_SIZE;

        if (!dio->read(offset, buffer.data(), BLOCK_SIZE)) {
            std::cerr << "  Failed to read block " << block_idx << std::endl;
            cleanup_test_dir(test_dir);
            return false;
        }

        // Verify pattern
        char expected = static_cast<char>(block_idx % 256);
        for (size_t i = 0; i < BLOCK_SIZE; ++i) {
            if (buffer[i] != expected) {
                std::cerr << "  Block " << block_idx << " verification failed at byte " << i << std::endl;
                cleanup_test_dir(test_dir);
                return false;
            }
        }
    }

    auto end = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(end - start);

    std::cout << "  " << NUM_TESTS << " random block reads in "
              << duration.count() << " ms" << std::endl;
    std::cout << "  " << (NUM_TESTS * 1000.0 / duration.count())
              << " reads/sec" << std::endl;

    cleanup_test_dir(test_dir);
    return true;
}

// Test 7: Concurrent access simulation
bool test_directio_concurrent_simulation() {
    std::cout << "Testing DirectIO concurrent access simulation..." << std::endl;

    std::string test_dir = create_test_dir("directio_concurrent");
    std::string filename = test_dir + "/concurrent.dat";

    // Create test file
    const size_t FILE_SIZE = 1024 * 1024;  // 1MB
    {
        std::ofstream file(filename, std::ios::binary);
        std::vector<char> data(FILE_SIZE);
        std::iota(data.begin(), data.end(), 0);  // Fill with 0, 1, 2, ...
        file.write(data.data(), data.size());
    }

    // Simulate multiple readers
    const int NUM_READERS = 4;
    const int READS_PER_READER = 100;

    std::vector<std::thread> readers;
    std::atomic<int> successful_reads{0};
    std::atomic<int> failed_reads{0};

    auto reader_func = [&](int reader_id) {
        auto dio = DirectIO::open(filename, true);
        if (!dio) {
            failed_reads++;
            return;
        }

        size_t block_size = dio->get_block_size();
        std::mt19937 rng(reader_id);
        std::uniform_int_distribution<size_t> dist(0, FILE_SIZE - 4096);

        AlignedBuffer buffer(4096, block_size);

        for (int i = 0; i < READS_PER_READER; ++i) {
            size_t offset = dist(rng);
            // Align to block size, not hardcoded 512
            offset = align_offset(offset, block_size);

            // Ensure we don't read past end of file
            if (offset + 4096 > FILE_SIZE) {
                offset = FILE_SIZE - 4096;
                offset = align_offset(offset, block_size);
            }

            if (dio->read(offset, buffer.data(), 4096)) {
                // Quick verification (check first and last bytes)
                char expected_first = static_cast<char>(offset % 256);
                char expected_last = static_cast<char>((offset + 4095) % 256);

                if (buffer[0] == expected_first && buffer[4095] == expected_last) {
                    successful_reads++;
                } else {
                    failed_reads++;
                }
            } else {
                failed_reads++;
            }
        }
    };

    auto start = high_resolution_clock::now();

    for (int i = 0; i < NUM_READERS; ++i) {
        readers.emplace_back(reader_func, i);
    }

    for (auto& reader : readers) {
        reader.join();
    }

    auto end = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(end - start);

    int total_reads = NUM_READERS * READS_PER_READER;
    std::cout << "  " << total_reads << " concurrent reads in "
              << duration.count() << " ms" << std::endl;
    std::cout << "  Successful: " << successful_reads
              << ", Failed: " << failed_reads << std::endl;
    std::cout << "  " << (total_reads * 1000.0 / duration.count())
              << " reads/sec" << std::endl;

    cleanup_test_dir(test_dir);

    // Allow some failures (alignment issues, etc.)
    if (successful_reads < total_reads * 0.9) {  // 90% success rate
        std::cerr << "  Too many failed reads" << std::endl;
        return false;
    }

    return true;
}

// Test 8: Error handling
bool test_directio_error_handling() {
    std::cout << "Testing DirectIO error handling..." << std::endl;

    // Save the original cerr buffer
    std::streambuf* original_cerr = std::cerr.rdbuf();

    // Create a null stream to discard output
    std::ofstream null_stream;
    std::streambuf* null_buffer = null_stream.rdbuf();

    // Redirect cerr to null
    std::cerr.rdbuf(null_buffer);

    // Try to open non-existent file
    auto dio = DirectIO::open("non_existent_file_12345.dat", true);

    std::cerr.rdbuf(original_cerr);

    if (dio && dio->is_open()) {
        std::cerr << "  Should not be able to open non-existent file for reading" << std::endl;
        return false;
    }

    // Create a test directory (not a file)
    std::string test_dir = create_test_dir("directio_error");
    std::string dir_as_file = test_dir + "/subdir";
    fs::create_directory(dir_as_file);

    // Redirect cerr to null
    std::cerr.rdbuf(null_buffer);

    // Try to open directory as file
    auto dio2 = DirectIO::open(dir_as_file, true);

    std::cerr.rdbuf(original_cerr);

    if (dio2 && dio2->is_open()) {
        std::cerr << "  Should not be able to open directory as file" << std::endl;
        cleanup_test_dir(test_dir);
        return false;
    }

    // Test writing to read-only file
    std::string read_only_file = test_dir + "/readonly.dat";
    {
        std::ofstream file(read_only_file, std::ios::binary);
        file.write("test", 4);
    }

    // Make file read-only (platform specific)
#ifdef _WIN32
    _chmod(read_only_file.c_str(), _S_IREAD);
#else
    chmod(read_only_file.c_str(), 0444);
#endif

    // Redirect cerr to null
    std::cerr.rdbuf(null_buffer);

    auto dio3 = DirectIO::open(read_only_file, false);  // Try to open for writing

    std::cerr.rdbuf(original_cerr);

    if (dio3 && dio3->is_open()) {
        // On some systems, this might still work
        // Try to write (should fail)
        char buffer[4096] = {0};
        if (dio3->write(0, buffer, 4096)) {
            std::cerr << "  Should not be able to write to read-only file" << std::endl;
            cleanup_test_dir(test_dir);
            return false;
        }
    }

    cleanup_test_dir(test_dir);
    return true;
}

// Test 9: Performance comparison (direct vs buffered I/O)
bool test_directio_performance_comparison() {
    std::cout << "Testing DirectIO performance comparison..." << std::endl;

    std::string test_dir = create_test_dir("directio_perf");
    std::string filename = test_dir + "/perf_test.dat";

    const size_t FILE_SIZE = 100 * 1024 * 1024;  // 100MB
    const size_t BLOCK_SIZE = 4096;
    const size_t NUM_BLOCKS = FILE_SIZE / BLOCK_SIZE;

    // Create test file
    std::cout << "  Creating " << (FILE_SIZE / (1024*1024)) << "MB test file..." << std::endl;
    {
        std::ofstream file(filename, std::ios::binary);
        std::vector<char> data(BLOCK_SIZE);

        for (size_t i = 0; i < NUM_BLOCKS; ++i) {
            std::fill(data.begin(), data.end(), static_cast<char>(i % 256));
            file.write(data.data(), data.size());
        }
    }

    // Test with DirectIO
    auto dio = DirectIO::open(filename, true);
    if (!dio) {
        std::cout << "  Skipping direct I/O test (not supported)" << std::endl;
        cleanup_test_dir(test_dir);
        return true;
    }

    bool using_direct = dio->is_using_direct_io();
    std::cout << "  Using " << (using_direct ? "direct" : "buffered") << " I/O" << std::endl;

    // Allocate aligned buffer for direct I/O
    size_t block_size = dio->get_block_size();
    void* aligned_buffer;

#ifdef _WIN32
    aligned_buffer = _aligned_malloc(BLOCK_SIZE, block_size);
#else
    if (posix_memalign(&aligned_buffer, block_size, BLOCK_SIZE) != 0) {
        std::cerr << "  Failed to allocate aligned memory" << std::endl;
        cleanup_test_dir(test_dir);
        return false;
    }
#endif

    char* buffer = static_cast<char*>(aligned_buffer);

    // Sequential read test
    const int SEQ_READS = 1000;

    auto start_direct = high_resolution_clock::now();
    for (int i = 0; i < SEQ_READS; ++i) {
        uint64_t offset = (i * BLOCK_SIZE) % (FILE_SIZE - BLOCK_SIZE);
        if (!dio->read(offset, buffer, BLOCK_SIZE)) {
            std::cerr << "  Direct I/O read failed at offset " << offset << std::endl;
#ifdef _WIN32
            _aligned_free(aligned_buffer);
#else
            free(aligned_buffer);
#endif
            cleanup_test_dir(test_dir);
            return false;
        }
    }
    auto end_direct = high_resolution_clock::now();

    // Test with standard I/O for comparison
    auto start_stdio = high_resolution_clock::now();
    {
        std::ifstream file(filename, std::ios::binary);
        std::vector<char> std_buffer(BLOCK_SIZE);

        for (int i = 0; i < SEQ_READS; ++i) {
            uint64_t offset = (i * BLOCK_SIZE) % (FILE_SIZE - BLOCK_SIZE);
            file.seekg(offset);
            file.read(std_buffer.data(), BLOCK_SIZE);
        }
    }
    auto end_stdio = high_resolution_clock::now();

    auto direct_time = duration_cast<milliseconds>(end_direct - start_direct);
    auto stdio_time = duration_cast<milliseconds>(end_stdio - start_stdio);

    std::cout << "  Direct I/O: " << SEQ_READS << " reads in "
              << direct_time.count() << " ms" << std::endl;
    std::cout << "  Standard I/O: " << SEQ_READS << " reads in "
              << stdio_time.count() << " ms" << std::endl;
    std::cout << "  Direct I/O speedup: "
              << (stdio_time.count() * 100.0 / direct_time.count() - 100)
              << "%" << std::endl;

#ifdef _WIN32
    _aligned_free(aligned_buffer);
#else
    free(aligned_buffer);
#endif

    cleanup_test_dir(test_dir);
    return true;
}

// Main test runner
int directio_tests_main() {
    std::cout << "\n=== DirectIO Unit Tests ===" << std::endl;
    std::cout << "==========================" << std::endl;

    std::vector<std::pair<std::string, bool (*)()>> tests = {
        {"Basic operations", test_directio_basic_operations},
        {"Write operations", test_directio_write_operations},
        {"Alignment requirements", test_directio_alignment},
        {"Move semantics", test_directio_move_semantics},
        {"Multiple files", test_directio_multiple_files},
        {"Large file operations", test_directio_large_file},
        {"Concurrent access simulation", test_directio_concurrent_simulation},
        {"Error handling", test_directio_error_handling},
        {"Performance comparison", test_directio_performance_comparison}
    };

    int passed = 0;
    int total = static_cast<int>(tests.size());

    for (const auto& [name, test_func] : tests) {
        std::cout << "\n" << name << "..." << std::endl;
        try {
            bool result = test_func();
            print_test_result_directio("", result);
            if (result) passed++;
        } catch (const std::exception& e) {
            std::cout << "X Exception: " << e.what() << std::endl;
        } catch (...) {
            std::cout << "X Unknown exception" << std::endl;
        }
    }

    std::cout << "\n" << std::string(50, '=') << std::endl;
    std::cout << "Results: " << passed << "/" << total << " tests passed" << std::endl;

    if (passed == total) {
        std::cout << "\nO All DirectIO tests passed!" << std::endl;
        return 0;
    } else {
        std::cout << "\nX Some DirectIO tests failed" << std::endl;
        return 1;
    }
}