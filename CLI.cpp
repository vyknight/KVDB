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
#include <random>

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

    // Experiment parameters from requirements
    const size_t TOTAL_DATA_SIZE = 1 * 1024 * 1024 * 1024; // 1 GB
    const size_t BUFFER_POOL_SIZE = 10 * 1024 * 1024;      // 10 MB
    const int FILTER_BITS_PER_ENTRY = 8;                   // 8 bits
    const size_t MEMTABLE_SIZE = 1 * 1024 * 1024;         // 1 MB

    // Benchmark parameters
    int key_size = 16;     // Default 16 bytes for keys
    int value_size = 1024; // Default 1 KB values
    int interval_mb = 100; // Measure every 100 MB
    std::string output_csv = "benchmark_results.csv";

    // Parse additional parameters if provided
    iss >> key_size >> value_size >> interval_mb >> output_csv;

    if (key_size <= 0) key_size = 16;
    if (value_size <= 0) value_size = 1024;
    if (interval_mb <= 0) interval_mb = 100;

    // Calculate number of entries based on data size
    size_t entry_size = key_size + value_size;
    size_t total_entries = TOTAL_DATA_SIZE / entry_size;
    size_t interval_entries = (interval_mb * 1024 * 1024) / entry_size;
    size_t num_intervals = total_entries / interval_entries;

    std::cout << "\n=== Running Experiment ===\n";
    std::cout << "Total data size: 1 GB (" << total_entries << " entries)\n";
    std::cout << "Key size: " << key_size << " bytes\n";
    std::cout << "Value size: " << value_size << " bytes\n";
    std::cout << "Buffer pool: " << BUFFER_POOL_SIZE / (1024*1024) << " MB\n";
    std::cout << "Filter bits per entry: " << FILTER_BITS_PER_ENTRY << "\n";
    std::cout << "Memtable size: " << MEMTABLE_SIZE / (1024*1024) << " MB\n";
    std::cout << "Measurement interval: " << interval_mb << " MB ("
              << interval_entries << " entries)\n";
    std::cout << "Output CSV: " << output_csv << "\n\n";

    // Open CSV file for writing
    std::ofstream csv_file(output_csv);
    if (!csv_file.is_open()) {
        std::cout << "Error: Could not open CSV file for writing\n";
        return;
    }

    // Write CSV header
    csv_file << "interval,cumulative_data_mb,insert_throughput_ops_sec,"
             << "get_throughput_ops_sec,scan_throughput_ops_sec,"
             << "cumulative_entries,time_elapsed_ms\n";

    // Prepare test data for the entire experiment
    std::cout << "Generating test data...\n";
    std::vector<std::string> keys;
    std::vector<std::string> values;

    // Use sequential keys for better scan performance measurement
    for (size_t i = 0; i < total_entries; i++) {
        std::string key = std::to_string(i);
        // Pad key to required size
        key.resize(key_size, '0');
        keys.push_back(key);

        // Generate random value
        std::string value = generate_random_string(value_size);
        values.push_back(value);
    }

    // Shuffle keys/values to simulate random insertion pattern
    std::random_device rd;
    std::mt19937 g(rd());
    std::vector<size_t> indices(total_entries);
    std::iota(indices.begin(), indices.end(), 0);
    std::shuffle(indices.begin(), indices.end(), g);

    // Reorder keys and values according to shuffled indices
    std::vector<std::string> shuffled_keys(total_entries);
    std::vector<std::string> shuffled_values(total_entries);
    for (size_t i = 0; i < total_entries; i++) {
        shuffled_keys[i] = keys[indices[i]];
        shuffled_values[i] = values[indices[i]];
    }

    // Main experiment loop
    std::cout << "\nStarting experiment...\n";
    auto experiment_start = std::chrono::high_resolution_clock::now();

    for (size_t interval = 0; interval < num_intervals; interval++) {
        size_t start_idx = interval * interval_entries;
        size_t end_idx = std::min(start_idx + interval_entries, total_entries);

        std::cout << "\n=== Interval " << interval + 1 << "/" << num_intervals
                  << " (" << (end_idx - start_idx) << " entries) ===\n";

        // 1. INSERT operations for this interval
        auto insert_start = std::chrono::high_resolution_clock::now();
        for (size_t i = start_idx; i < end_idx; i++) {
            db_->put(shuffled_keys[i], shuffled_values[i]);
        }
        auto insert_end = std::chrono::high_resolution_clock::now();
        auto insert_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            insert_end - insert_start);

        double insert_throughput = (end_idx - start_idx) * 1000.0 / insert_duration.count();
        std::cout << "Insert: " << insert_duration.count() << "ms, "
                  << std::fixed << std::setprecision(2) << insert_throughput << " ops/sec\n";

        // 2. GET operations benchmark
        // Sample 1000 random keys from all inserted data so far
        const size_t sample_size = 1000;
        std::vector<std::string> get_keys;

        std::uniform_int_distribution<size_t> dist(0, end_idx - 1);
        for (size_t i = 0; i < sample_size; i++) {
            size_t random_idx = dist(g);
            get_keys.push_back(shuffled_keys[random_idx]);
        }

        auto get_start = std::chrono::high_resolution_clock::now();
        for (const auto& key : get_keys) {
            db_->get(key);
        }
        auto get_end = std::chrono::high_resolution_clock::now();
        auto get_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            get_end - get_start);

        double get_throughput = sample_size * 1000.0 / get_duration.count();
        std::cout << "Get: " << get_duration.count() << "ms, "
                  << std::fixed << std::setprecision(2) << get_throughput << " ops/sec\n";

        // 3. SCAN operations benchmark
        // Perform range scan (if supported by your database)
        const size_t scan_size = 1000;
        auto scan_start = std::chrono::high_resolution_clock::now();

        db_->scan("a", "\uffff");

        auto scan_end = std::chrono::high_resolution_clock::now();
        auto scan_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            scan_end - scan_start);

        double scan_throughput = scan_size * 1000.0 / scan_duration.count();
        std::cout << "Scan: " << scan_duration.count() << "ms, "
                  << std::fixed << std::setprecision(2) << scan_throughput << " ops/sec\n";

        // Calculate cumulative metrics
        size_t cumulative_data_mb = (end_idx * entry_size) / (1024 * 1024);
        auto time_elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::high_resolution_clock::now() - experiment_start);

        // Write to CSV
        csv_file << interval + 1 << ","
                 << cumulative_data_mb << ","
                 << insert_throughput << ","
                 << get_throughput << ","
                 << scan_throughput << ","
                 << end_idx << ","
                 << time_elapsed.count() << "\n";

        csv_file.flush(); // Ensure data is written after each interval

        // Optional: Show progress
        if ((interval + 1) % 5 == 0) {
            float progress = 100.0 * (interval + 1) / num_intervals;
            std::cout << "\nProgress: " << std::fixed << std::setprecision(1)
                      << progress << "% complete\n";
        }
    }

    // Final measurements after all data is inserted
    std::cout << "\n=== Final Measurements (After 1GB Insertion) ===\n";

    // Final GET benchmark
    const size_t final_sample = 10000;
    std::vector<std::string> final_get_keys;

    std::uniform_int_distribution<size_t> final_dist(0, total_entries - 1);
    for (size_t i = 0; i < final_sample; i++) {
        size_t random_idx = final_dist(g);
        final_get_keys.push_back(shuffled_keys[random_idx]);
    }

    auto final_get_start = std::chrono::high_resolution_clock::now();
    for (const auto& key : final_get_keys) {
        db_->get(key);
    }
    auto final_get_end = std::chrono::high_resolution_clock::now();
    auto final_get_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        final_get_end - final_get_start);

    double final_get_throughput = final_sample * 1000.0 / final_get_duration.count();
    std::cout << "Final Get Throughput: " << std::fixed << std::setprecision(2)
              << final_get_throughput << " ops/sec\n";

    // Final experiment statistics
    auto experiment_end = std::chrono::high_resolution_clock::now();
    auto total_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        experiment_end - experiment_start);

    double avg_insert_throughput = total_entries * 1000.0 / total_duration.count();

    std::cout << "\n=== Experiment Complete ===\n";
    std::cout << "Total time: " << total_duration.count() / 1000.0 << " seconds\n";
    std::cout << "Average insert throughput: " << std::fixed << std::setprecision(2)
              << avg_insert_throughput << " ops/sec\n";
    std::cout << "Results saved to: " << output_csv << "\n";

    // Force flush and show stats
    db_->flush_memtable();
    show_stats();

    csv_file.close();
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