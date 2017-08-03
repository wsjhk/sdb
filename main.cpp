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
#include "src/executor/executor.h"

using namespace SDB;

// SDB::Type::TableProperty get_table_property();
// void db_test();
//

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
        if (line.find(';') != -1) {
            Parser p;
            Ast ast = p.parsing(query);
            exector.evil(ast);
            // ast.output_graphviz("output.dot");
            query.clear();
        }
    }
}

// Type::TableProperty get_table_property(){
//     std::vector<std::string> col_name_lst{"col_1", "col_2"};
//     Type::ColProperty cp1("col_1", Enum::INT, 4);
//     Type::ColProperty cp2("col_2", Enum::VARCHAR, 4);
//     return Type::TableProperty("test", "test", "col_1", std::vector<Type::ColProperty>{cp1, cp2});
// }
// 
// void db_test(){
//     std::string db_name = "test";
//     DB::create_db(db_name);
//     DB db(db_name);
//     auto property_1 = get_table_property();
//     db.create_table(property_1);
//     db.drop_table(property_1.table_name);
//     db.drop_db();
// }
