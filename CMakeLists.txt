cmake_minimum_required(VERSION 4.0)
project(KVDB)

set(CMAKE_CXX_STANDARD 20)

add_executable(KVDB main.cpp
        memtable.cpp
        Memtable.h
        Tests/test_memtable.cpp
        Tests/test_memtable.h
)
