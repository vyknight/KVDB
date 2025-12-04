#include <iostream>

#include "Tests/test_memtable.h"

int main()
{
    std::cout << "Hello, World!" << std::endl;

    std::cout << "Running tests" << std::endl;
    memtable_tests_main();

    return 0;
}