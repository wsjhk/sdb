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
    ResultList evil(const Ast &ast);

private:
    bool create_database(const ParserType::nodePtrType &node);
    bool use_database(const ParserType::nodePtrType &node);
};

#endif /* ifndef SDB_EXECUTOR */
