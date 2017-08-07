#ifndef SDB_EXECUTOR
#define SDB_EXECUTOR

#include <variant>

#include "src/parser/ast.h"
#include "src/db/db.h"
#include "src/parser/type.h"

using EvilResult = std::variant<std::string, SDB::Type::TupleLst>;
using EvilResultList = std::vector<EvilResult>;

class Executor {
public: 
    Executor(DB *db):db(db){}
    static Result<std::shared_ptr<Executor>, std::string> make(const std::string &db_name);
    EvilResultList evil(const Ast &ast);

    // evil function
    std::string create_table(const ParserType::nodePtrType &ptr);


private:
    Executor()=delete;
    DB *db;
};

#endif /* ifndef SDB_EXECUTOR */
