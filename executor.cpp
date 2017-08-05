#include "executor.h"
#include "src/db/db.h"
#include "src/parser/type.h"

using ParserType::nodePtrType;

ResultList Executor::evil(const Ast &ast) {
    auto root = ast.get_root();
    ResultList res_list;
    for (auto &&node: root->children) {
    }
    return res_list;
}
