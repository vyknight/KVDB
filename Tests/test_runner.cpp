//
// Created by K on 2025-12-06.
//

#include "test_runner.h"
#include "test_memtable.h"
#include "test_sstable_writer.h"
#include "test_sstable_reader.h"
#include "test_wal.h"
#include "test_kvstore.h"

void run_tests()
{
    memtable_tests_main();
    sstable_writer_tests_main();
    sstable_reader_tests_main();
    wal_tests_main();
    kvstore_tests_main();
}