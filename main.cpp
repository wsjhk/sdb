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
#include "src/parser/parser.h"
#include "src/parser/ast.h"
#include "executor.h"

using namespace SDB;

// shell
void run_shell(const std::string &db_name);

int main(int argc, char *argv[]) {
    if (argc != 3) {
        std::cout << "错误命令" << std::endl;
        exit(1);
    }
    std::string command = argv[1];
    std::string obj = argv[2];
    if (command == "createdb") {
        DB::create_db(obj);
    } else if (command == "dropdb") {
        DB::drop_db(obj);
    } else if (command == "use") {
        run_shell(obj);
    } else if (command == "list" && obj == "db") {
        for (auto &&name: IO::get_db_name_list()) {
            std::cout << name << std::endl;
        }
    }
    return 0;
}

void run_shell(const std::string &db_name) {
    Executor exector(db_name);
    std::cout << "===============" << std::endl;
    printf("+-+-+-+\n");
    printf("|s|d|b|\n");
    printf("+-+-+-+\n");
    std::cout << "===============\n\n" << std::endl;
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
