//
// Created by Zekun Liu on 2025-12-07.
//

#include "CLI.h"
#include "KVStore.h"
#include <iostream>
#include <string>
#include <vector>
#include <sstream>
#include <iomanip>
#include <filesystem>
#include <chrono>
#include <algorithm>
#include <memory>
#include <cstdlib>

namespace fs = std::filesystem;

void CLI::run() {
    std::cout << "=== KVStore Command Line Interface ===\n";
    std::cout << "Type 'help' for available commands\n";
    std::cout << "Type 'exit' to quit\n\n";

    while (running_) {
        try {
            display_prompt();
            std::string line;
            if (!std::getline(std::cin, line)) {
                break;  // EOF
            }

            if (line.empty()) continue;

            process_command(line);
        } catch (const std::exception& e) {
            std::cerr << "Error: " << e.what() << std::endl;
        }
    }

    if (db_) {
        std::cout << "Closing database...\n";
        db_->close();
    }
}

void CLI::display_prompt() const {
    if (current_db_path_.empty()) {
        std::cout << "kvstore> ";
    } else {
        std::cout << "kvstore[" << fs::path(current_db_path_).filename().string() << "]> ";
    }
}

void CLI::process_command(const std::string& line) {
    std::istringstream iss(line);
    std::string command;
    iss >> command;

    // Convert command to lowercase for case-insensitive matching
    std::transform(command.begin(), command.end(), command.begin(), ::tolower);

    if (command == "help" || command == "?") {
        show_help();
    } else if (command == "exit" || command == "quit") {
        running_ = false;
    } else if (command == "open") {
        open_database(iss);
    } else if (command == "close") {
        close_database();
    } else if (command == "put") {
        put_key_value(iss);
    } else if (command == "get") {
        get_value(iss);
    } else if (command == "delete" || command == "del" || command == "remove") {
        delete_key(iss);
    } else if (command == "scan") {
        scan_range(iss);
    } else if (command == "flush") {
        flush_memtable();
    } else if (command == "stats") {
        show_stats();
    } else if (command == "list") {
        list_databases(iss);
    } else if (command == "benchmark") {
        run_benchmark(iss);
    } else if (command == "clear") {
        clear_screen();
    } else if (command == "pwd") {
        print_working_directory();
    } else if (command == "ls") {
        list_directory();
    } else if (command == "cd") {
        change_directory(iss);
    } else if (command == "mkdir") {
        make_directory(iss);
    } else {
        std::cout << "Unknown command: " << command << "\n";
        std::cout << "Type 'help' for available commands\n";
    }
}

void CLI::show_help() {
    std::cout << "\n=== KVStore CLI Commands ===\n\n";

    std::cout << "Database Operations:\n";
    std::cout << "  open <db_name> [memtable_size]   - Open or create a database\n";
    std::cout << "  close                             - Close current database\n";
    std::cout << "  list [pattern]                   - List available databases\n\n";

    std::cout << "Data Operations:\n";
    std::cout << "  put <key> <value>                - Insert or update a key-value pair\n";
    std::cout << "  get <key>                        - Retrieve value for a key\n";
    std::cout << "  delete <key>                     - Delete a key\n";
    std::cout << "  scan <start_key> <end_key>       - Scan key range\n\n";

    std::cout << "System Operations:\n";
    std::cout << "  flush                            - Force flush memtable to disk\n";
    std::cout << "  stats                            - Show database statistics\n";
    std::cout << "  benchmark [ops] [key_size] [val_size] - Run performance benchmark\n\n";

    std::cout << "File System Operations:\n";
    std::cout << "  ls                               - List current directory\n";
    std::cout << "  cd <directory>                   - Change directory\n";
    std::cout << "  pwd                              - Print working directory\n";
    std::cout << "  mkdir <directory>                - Create directory\n\n";

    std::cout << "Utility:\n";
    std::cout << "  clear                            - Clear screen\n";
    std::cout << "  help, ?                          - Show this help message\n";
    std::cout << "  exit, quit                       - Exit the program\n\n";
}

void CLI::open_database(std::istringstream& iss) {
    std::string db_name;
    if (!(iss >> db_name)) {
        std::cout << "Usage: open <db_name> [memtable_size]\n";
        return;
    }

    size_t memtable_size = 4096;  // Default 4KB
    iss >> memtable_size;

    // Close current database if open
    if (db_) {
        std::cout << "Closing current database...\n";
        db_->close();
        db_.reset();
    }

    try {
        std::cout << "Opening database '" << db_name << "' with memtable size "
                  << memtable_size << " bytes...\n";

        auto start = std::chrono::high_resolution_clock::now();
        db_ = KVStore::open(db_name, memtable_size);
        auto end = std::chrono::high_resolution_clock::now();

        if (!db_) {
            std::cout << "Failed to open database\n";
            return;
        }

        current_db_path_ = db_->get_db_path();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

        std::cout << "Database opened successfully in " << duration.count() << "ms\n";

        // Show initial stats
        show_stats();

    } catch (const std::exception& e) {
        std::cerr << "Error opening database: " << e.what() << "\n";
        db_.reset();
        current_db_path_.clear();
    }
}

void CLI::close_database() {
    if (!db_) {
        std::cout << "No database is currently open\n";
        return;
    }

    std::cout << "Closing database...\n";
    db_->close();
    db_.reset();
    current_db_path_.clear();
    std::cout << "Database closed\n";
}

void CLI::put_key_value(std::istringstream& iss) {
    if (!db_) {
        std::cout << "No database is open. Use 'open <db_name>' first.\n";
        return;
    }

    std::string key;
    if (!(iss >> key)) {
        std::cout << "Usage: put <key> <value>\n";
        return;
    }

    // Get the rest of the line as the value
    std::string value;
    std::getline(iss, value);

    // Remove leading space if present
    if (!value.empty() && value[0] == ' ') {
        value = value.substr(1);
    }

    if (value.empty()) {
        std::cout << "Warning: Empty value will be stored\n";
    }

    try {
        auto start = std::chrono::high_resolution_clock::now();
        bool success = db_->put(key, value);
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

        if (success) {
            std::cout << "OK (" << duration.count() << "μs)\n";
        } else {
            std::cout << "Failed to put key-value pair\n";
        }
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
    }
}

void CLI::get_value(std::istringstream& iss) {
    if (!db_) {
        std::cout << "No database is open. Use 'open <db_name>' first.\n";
        return;
    }

    std::string key;
    if (!(iss >> key)) {
        std::cout << "Usage: get <key>\n";
        return;
    }

    try {
        auto start = std::chrono::high_resolution_clock::now();
        auto value = db_->get(key);
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

        if (value.has_value()) {
            std::cout << "Value: \"" << value.value() << "\" ("
                      << value.value().size() << " bytes, "
                      << duration.count() << "μs)\n";
        } else {
            std::cout << "Key not found (" << duration.count() << "μs)\n";
        }
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
    }
}

void CLI::delete_key(std::istringstream& iss) {
    if (!db_) {
        std::cout << "No database is open. Use 'open <db_name>' first.\n";
        return;
    }

    std::string key;
    if (!(iss >> key)) {
        std::cout << "Usage: delete <key>\n";
        return;
    }

    try {
        auto start = std::chrono::high_resolution_clock::now();
        bool success = db_->remove(key);
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

        if (success) {
            std::cout << "OK (" << duration.count() << "μs)\n";
        } else {
            std::cout << "Failed to delete key\n";
        }
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
    }
}

void CLI::scan_range(std::istringstream& iss) {
    if (!db_) {
        std::cout << "No database is open. Use 'open <db_name>' first.\n";
        return;
    }

    std::string start_key, end_key;
    if (!(iss >> start_key >> end_key)) {
        std::cout << "Usage: scan <start_key> <end_key>\n";
        return;
    }

    try {
        std::cout << "Scanning from \"" << start_key << "\" to \"" << end_key << "\"...\n";

        auto start = std::chrono::high_resolution_clock::now();
        auto results = db_->scan(start_key, end_key);
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

        if (results.empty()) {
            std::cout << "No keys found in range (" << duration.count() << "μs)\n";
            return;
        }

        std::cout << "\nFound " << results.size() << " key-value pairs:\n";
        std::cout << std::string(60, '-') << "\n";

        for (size_t i = 0; i < results.size(); i++) {
            const auto& [key, value] = results[i];
            std::cout << std::setw(4) << (i + 1) << ". Key: \"" << key
                      << "\" -> Value: \"" << value << "\" ("
                      << value.size() << " bytes)\n";
        }

        std::cout << std::string(60, '-') << "\n";
        std::cout << "Total: " << results.size() << " pairs ("
                  << duration.count() << "μs)\n";

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
    }
}

void CLI::flush_memtable() {
    if (!db_) {
        std::cout << "No database is open. Use 'open <db_name>' first.\n";
        return;
    }

    try {
        std::cout << "Flushing memtable to disk...\n";
        auto start = std::chrono::high_resolution_clock::now();
        db_->flush_memtable();
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

        std::cout << "Memtable flushed (" << duration.count() << "ms)\n";

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
    }
}

void CLI::show_stats() {
    if (!db_) {
        std::cout << "No database is open. Use 'open <db_name>' first.\n";
        return;
    }

    try {
        auto stats = db_->get_stats();

        std::cout << "\n=== Database Statistics ===\n\n";

        std::cout << "Operations:\n";
        std::cout << "  Puts:        " << stats.puts << "\n";
        std::cout << "  Gets:        " << stats.gets << "\n";
        std::cout << "  Deletes:     " << stats.deletes << "\n";
        std::cout << "  Scans:       " << stats.scans << "\n\n";

        std::cout << "Storage:\n";
        std::cout << "  SST Files:   " << stats.sst_files << "\n";
        std::cout << "  Total Data:  " << stats.total_data_size << " entries\n";
        std::cout << "  Memtable Flushes: " << stats.memtable_flushes << "\n\n";

        std::cout << "Database Path: " << current_db_path_ << "\n";

    } catch (const std::exception& e) {
        std::cerr << "Error getting stats: " << e.what() << "\n";
    }
}

void CLI::list_databases(std::istringstream& iss) {
    std::string pattern = "*";
    iss >> pattern;  // Optional pattern

    std::cout << "Available databases in current directory:\n";
    std::cout << std::string(50, '-') << "\n";

    try {
        int count = 0;
        for (const auto& entry : fs::directory_iterator(fs::current_path())) {
            if (!entry.is_directory()) continue;

            std::string dir_name = entry.path().filename().string();

            // Check if directory looks like a database (has .sst files or wal)
            bool has_sst = false;
            for (const auto& file : fs::directory_iterator(entry.path())) {
                if (file.is_regular_file()) {
                    std::string ext = file.path().extension().string();
                    if (ext == ".sst" || file.path().filename() == "wal.bin") {
                        has_sst = true;
                        break;
                    }
                }
            }

            if (has_sst) {
                count++;
                std::cout << "  " << dir_name;

                // Show size
                try {
                    size_t size = 0;
                    for (const auto& file : fs::recursive_directory_iterator(entry.path())) {
                        if (file.is_regular_file()) {
                            size += file.file_size();
                        }
                    }
                    std::cout << " (" << format_size(size) << ")";
                } catch (...) {
                    // Ignore size calculation errors
                }

                std::cout << "\n";
            }
        }

        if (count == 0) {
            std::cout << "  No databases found\n";
        }

    } catch (const fs::filesystem_error& e) {
        std::cerr << "Error listing databases: " << e.what() << "\n";
    }

    std::cout << std::string(50, '-') << "\n";
}

void CLI::run_benchmark(std::istringstream& iss) {
    if (!db_) {
        std::cout << "No database is open. Use 'open <db_name>' first.\n";
        return;
    }

    int num_ops = 1000;
    int key_size = 10;
    int value_size = 100;

    iss >> num_ops >> key_size >> value_size;

    if (num_ops <= 0) num_ops = 1000;
    if (key_size <= 0) key_size = 10;
    if (value_size <= 0) value_size = 100;

    std::cout << "\n=== Running Benchmark ===\n";
    std::cout << "Operations: " << num_ops << "\n";
    std::cout << "Key size: " << key_size << " bytes\n";
    std::cout << "Value size: " << value_size << " bytes\n\n";

    // Prepare test data
    std::vector<std::string> keys;
    std::vector<std::string> values;

    std::cout << "Generating test data...\n";
    for (int i = 0; i < num_ops; i++) {
        keys.push_back(generate_random_string(key_size));
        values.push_back(generate_random_string(value_size));
    }

    // Benchmark PUT operations
    std::cout << "\n1. PUT Operations:\n";
    auto put_start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < num_ops; i++) {
        db_->put(keys[i], values[i]);
    }
    auto put_end = std::chrono::high_resolution_clock::now();
    auto put_duration = std::chrono::duration_cast<std::chrono::milliseconds>(put_end - put_start);

    std::cout << "  Time: " << put_duration.count() << "ms\n";
    std::cout << "  Throughput: " << (num_ops * 1000.0 / put_duration.count()) << " ops/sec\n";

    // Benchmark GET operations
    std::cout << "\n2. GET Operations:\n";
    auto get_start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < num_ops; i++) {
        db_->get(keys[i]);
    }
    auto get_end = std::chrono::high_resolution_clock::now();
    auto get_duration = std::chrono::duration_cast<std::chrono::milliseconds>(get_end - get_start);

    std::cout << "  Time: " << get_duration.count() << "ms\n";
    std::cout << "  Throughput: " << (num_ops * 1000.0 / get_duration.count()) << " ops/sec\n";

    // Benchmark DELETE operations
    std::cout << "\n3. DELETE Operations:\n";
    auto del_start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < num_ops; i++) {
        db_->remove(keys[i]);
    }
    auto del_end = std::chrono::high_resolution_clock::now();
    auto del_duration = std::chrono::duration_cast<std::chrono::milliseconds>(del_end - del_start);

    std::cout << "  Time: " << del_duration.count() << "ms\n";
    std::cout << "  Throughput: " << (num_ops * 1000.0 / del_duration.count()) << " ops/sec\n";

    // Final stats
    std::cout << "\n=== Benchmark Complete ===\n";
    std::cout << "Total time: " << (put_duration + get_duration + del_duration).count() << "ms\n";

    // Force flush to see final state
    db_->flush_memtable();
    show_stats();
}

void CLI::clear_screen() {
#ifdef _WIN32
    system("cls");
#else
    system("clear");
#endif
}

void CLI::print_working_directory() {
    try {
        std::cout << fs::current_path().string() << "\n";
    } catch (const fs::filesystem_error& e) {
        std::cerr << "Error: " << e.what() << "\n";
    }
}

void CLI::list_directory() {
    try {
        std::cout << "Contents of " << fs::current_path().string() << ":\n";
        std::cout << std::string(60, '-') << "\n";

        int dir_count = 0, file_count = 0;

        for (const auto& entry : fs::directory_iterator(fs::current_path())) {
            if (entry.is_directory()) {
                std::cout << "[DIR]  " << entry.path().filename().string() << "/\n";
                dir_count++;
            } else {
                std::string filename = entry.path().filename().string();
                std::string ext = entry.path().extension().string();
                std::string size_str;

                try {
                    size_t size = entry.file_size();
                    size_str = " (" + format_size(size) + ")";
                } catch (...) {
                    size_str = " (unknown size)";
                }

                // Highlight database-related files
                if (ext == ".sst" || filename == "wal.bin") {
                    std::cout << "*SST*  ";
                } else {
                    std::cout << "       ";
                }

                std::cout << filename << size_str << "\n";
                file_count++;
            }
        }

        std::cout << std::string(60, '-') << "\n";
        std::cout << dir_count << " directories, " << file_count << " files\n";

    } catch (const fs::filesystem_error& e) {
        std::cerr << "Error: " << e.what() << "\n";
    }
}

void CLI::change_directory(std::istringstream& iss) {
    std::string path;
    if (!(iss >> path)) {
        std::cout << "Usage: cd <directory>\n";
        return;
    }

    try {
        fs::current_path(path);
        std::cout << "Changed directory to: " << fs::current_path().string() << "\n";
    } catch (const fs::filesystem_error& e) {
        std::cerr << "Error: " << e.what() << "\n";
        std::cout << "Current directory: " << fs::current_path().string() << "\n";
    }
}

void CLI::make_directory(std::istringstream& iss) {
    std::string dirname;
    if (!(iss >> dirname)) {
        std::cout << "Usage: mkdir <directory>\n";
        return;
    }

    try {
        fs::create_directory(dirname);
        std::cout << "Created directory: " << dirname << "\n";
    } catch (const fs::filesystem_error& e) {
        std::cerr << "Error: " << e.what() << "\n";
    }
}

// Note: Removed check_db_open() as it's not used in the header

std::string CLI::format_size(size_t bytes) const {
    const char* units[] = {"B", "KB", "MB", "GB"};
    size_t unit_index = 0;
    double size = static_cast<double>(bytes);

    while (size >= 1024.0 && unit_index < 3) {
        size /= 1024.0;
        unit_index++;
    }

    std::stringstream ss;
    ss << std::fixed << std::setprecision(2) << size << " " << units[unit_index];
    return ss.str();
}

std::string CLI::generate_random_string(size_t length) const {
    static const char alphanum[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";

    std::string result;
    result.reserve(length);

    for (size_t i = 0; i < length; ++i) {
        result += alphanum[rand() % (sizeof(alphanum) - 1)];
    }

    return result;
}