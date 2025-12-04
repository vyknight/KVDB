#include <iostream>

#include "Tests/test_memtable.h"
#include "Tests/test_sstable_writer.h"

int main()
{
    std::cout << "Hello, World!" << std::endl;

    std::cout << "Running tests" << std::endl;
    memtable_tests_main();
    sstable_writer_tests_main();

    return 0;
}
