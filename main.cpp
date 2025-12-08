#include <iostream>

#include "Tests/test_runner.h"
#include "CLI.h"

int main(int argc, char* argv[])
{
    if (argc < 2) {
        return main_cli_wrapper();
    } else {
        std::cout << "Running tests" << std::endl;
        run_tests();
    }

    return 0;
}
