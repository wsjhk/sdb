#include "executor.h"
#include "src/db/db.h"
#include "src/parser/type.h"

using ParserType::nodePtrType;


Result<std::shared_ptr<Executor>, std::string> Executor::make(const std::string &db_name){
    auto res = DB::get_db(db_name);
    auto vp = [](DB *db)->Ok<std::shared_ptr<Executor>>{
        return std::make_shared<Executor>(db);
    };
    _VprOrEr(res, vp);
}

EvilResultList Executor::evil(const Ast &ast) {
    auto root = ast.get_root();
    EvilResultList res_list;
    for (auto &&node: root->children) {
        if (node->name == "create_table") {
            std::cout << "=========" << std::endl;
        }
    }
    return res_list;
}
