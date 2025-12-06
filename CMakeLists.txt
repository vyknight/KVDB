cmake_minimum_required(VERSION 4.0)
project(KVDB)

set(CMAKE_CXX_STANDARD 20)

add_executable(KVDB main.cpp
        memtable.cpp
        Memtable.h
        Tests/test_memtable.cpp
        Tests/test_memtable.h
        SSTableWriter.cpp
        SSTableWriter.h
        SSTableReader.cpp
        SSTableReader.h
        Tests/test_sstable_writer.cpp
        Tests/test_sstable_writer.h
        Tests/test_sstable_reader.cpp
        Tests/test_sstable_reader.h
        Tests/test_runner.cpp
        Tests/test_runner.h
        WriteAheadLog.cpp
        WriteAheadLog.h
)
