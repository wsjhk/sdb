#include "executor.h"
#include "src/db/db.h"
#include "src/parser/type.h"

using ParserType::nodePtrType;

ResultList Executor::evil(const Ast &ast) {
    auto root = ast.get_root();
    ResultList res_list;
    for (auto &&node: root->children) {
        if (node->name == "create_database") {
            res_list.push_back(create_database(node->children[0]));
        }
    }
    return res_list;
}

bool Executor::create_database(const nodePtrType &node) {
    DB::create_db(node->name);
    std::cout << "create database ";
    std::cout << node->name;
    std::cout << ", ok" << std::endl;
    return true;
}
