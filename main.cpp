#include <fstream>
#include <string>
#include <vector>
#include <iostream>
#include <map>
#include <cstring>
#include <ctime>
#include <functional>
#include <memory>

#include "src/db/table.h"
#include "src/db/util.h"
#include "src/db/bpTree.h"
#include "src/db/cache.h"
#include "src/db/io.h"
#include "src/db/db.h"
#include "src/parser/parser.h"
#include "src/parser/ast.h"
#include "executor.h"

using namespace SDB;

// shell
void run_shell();

int main(void) {
    run_shell();
    return 0;
}

void run_shell() {
    Executor exector;
    std::cout << "===============" << std::endl;
    printf("+-+-+-+\n");
    printf("|s|d|b|\n");
    printf("+-+-+-+\n");
    std::cout << "===============\n\n\n" << std::endl;
    std::string query = "";
    while (true) {
        std::cout << "[sdb] -:";
        std::string line;
        std::getline(std::cin, line);
        if (line == "exit") {
            return;
        }
        query += line + '\n';
        if (line.find(';') != std::string::npos) {
            Parser p;
            Ast ast = p.parsing(query);
            exector.evil(ast);
            query.clear();
        }
    }
}
