//
// Created by Zekun Liu on 2025-12-07.
//

#ifndef KVDB_CLI_H
#define KVDB_CLI_H

#include "KVStore.h"
#include <iostream>

class CLI {
    std::unique_ptr<KVStore> db_;
    std::string current_db_path_;
    bool running_ = true;
    void display_prompt() const;
    void process_command(const std::string& line);
    void show_help();
    void open_database(std::istringstream& iss);
    void close_database();
    void put_key_value(std::istringstream& iss);
    void get_value(std::istringstream& iss);
    void delete_key(std::istringstream& iss);
    void scan_range(std::istringstream& iss);
    void flush_memtable();
    void show_stats();
    void list_databases(std::istringstream& iss);
    void run_benchmark(std::istringstream& iss);
    void clear_screen();
    void print_working_directory();
    void list_directory();
    void change_directory(std::istringstream& iss);
    void make_directory(std::istringstream& iss);
    void check_db_open();
    std::string format_size(size_t bytes) const;
    std::string generate_random_string(size_t length) const;
public:
    CLI() = default;
    void run();
};

static int main_cli_wrapper() {
    try {
        CLI cli;
        cli.run();
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << std::endl;
        return 1;
    }
}


#endif //KVDB_CLI_H