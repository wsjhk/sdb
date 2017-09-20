#include "shell.h"

#include <iostream>

namespace sdb::client {

// TODO
void Shell::run() {
    std::cout << "===============" << std::endl;
    printf("+-+-+-+\n");
    printf("|s|d|b|\n");
    printf("+-+-+-+\n");
    std::cout << "===============\n\n" << std::endl;
    std::string query = "";
    while (true) {
        std::cout << "\n[sdb] -:";
        std::string line;
        std::getline(std::cin, line);
        std::cin.clear();
        if (line == "exit") {
            return;
        }
        query += line + '\n';
        if (line.find(';') != std::string::npos) {
        }
    }
}

} // namespace sdb::client

