#ifndef SDB_EXECUTOR
#define SDB_EXECUTOR

#include <variant>

#include "src/parser/ast.h"
#include "src/db/db.h"
#include "src/parser/type.h"

using Result = std::variant<bool, SDB::Type::TupleLst>;
using ResultList = std::vector<Result>;

class Executor {
public: 
    Executor(const std::string &db_name){
        auto db_op = DB::get_db(db_name);
        if (db_op.has_value()) {
            this->db = db_op.value();
        } else {
            std::cout << "Error: db not found" << std::endl;
            exit(1);
        }
    }
    ResultList evil(const Ast &ast);

private:
    DB *db = nullptr;
};

#endif /* ifndef SDB_EXECUTOR */
