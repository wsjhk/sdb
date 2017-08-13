#ifndef SDB_EXECUTOR
#define SDB_EXECUTOR

#include <variant>

#include "ast.h"
#include "../db/db.h"
#include "type.h"
#include "../cpp_util/result.hpp"

using EvilResult = std::variant<std::string, SDB::Type::TupleLst>;
using EvilResultList = std::vector<EvilResult>;

class Executor {
public: 
    Executor(DB *db):db(db){}
    static cpp_util::Result<std::shared_ptr<Executor>, std::string> make(const std::string &db_name);
    EvilResultList execute(const Ast &ast);

    // =========== execute function =========== 
    // create table
    cpp_util::Result<void, std::string> create_table(const ParserType::nodePtrType &ptr);
    cpp_util::Result<SDB::Type::ColProperty, std::string>
        col_property(const ParserType::nodePtrType &ptr, std::string &primary_key);

private:
    Executor()=delete;
    DB *db;
};

#endif /* ifndef SDB_EXECUTOR */
