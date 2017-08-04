#include "executor.h"
#include "src/db/db.h"
#include "src/parser/type.h"

using ParserType::nodePtrType;

ResultList Executor::evil(const Ast &ast) {
    auto root = ast.get_root();
    ResultList res_list;
    for (auto &&node: root->children) {
        if (node->name == "create_database") {
            res_list.push_back(create_database(node));
        } else if (node->name == "use_database") {
            res_list.push_back(use_database(node));
        }
    }
    return res_list;
}

bool Executor::create_database(const nodePtrType &node) {
    nodePtrType name_node = node->children[0];
    DB::create_db(name_node->name);
    std::cout << "create database ";
    std::cout << node->name;
    std::cout << ", ok" << std::endl;
    return true;
}

bool Executor::use_database(const nodePtrType &node) {
    nodePtrType name_node = node->children[0];
    DB::use_db(name_node->name);
    std::cout << "database change, ok" << std::endl;
    return true;
}
