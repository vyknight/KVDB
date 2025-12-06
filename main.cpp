#include <iostream>

#include "Tests/test_runner.h"

int main()
{
    std::cout << "Hello, World!" << std::endl;

    std::cout << "Running tests" << std::endl;
    run_tests();

    return 0;
}
