#include "executor.h"
#include "../db/db.h"
#include "../util/str.hpp"
#include "../util/log.hpp"
#include "type.h"

using ParserType::nodePtrType;
using namespace SDB;

Result<std::shared_ptr<Executor>, std::string> Executor::make(const std::string &db_name){
    auto res = DB::get_db(db_name);
    _VaOrEr(res, DB* db);
    return Ok<std::shared_ptr<Executor>>(std::make_shared<Executor>(db));
}

EvilResultList Executor::execute(const Ast &ast) {
    auto root = ast.get_root();
    EvilResultList res_list;
    for (auto &&stm: root->children) {
        if (stm->name == "create_table") {
            auto res = create_table(stm);
            _IfEEp(res, Log::log);
        }
    }
    return res_list;
}

Result<void, std::string> 
Executor::create_table(const nodePtrType &ptr) {
    auto children = ptr->children;
    std::string table_name = children[0]->name;
    Type::TableProperty::ColPropertyList col_ppt_lst;
    std::unordered_set<std::string> col_name_set;
    std::string primary_key;
    auto col_def_list = children[1]->children;
    for (auto &&col_def : col_def_list) {
        if (col_def->type == "col_def") {
            auto res = col_property(col_def, primary_key);
            _VaOrEr(res, Type::ColProperty col_ppt);
            if (col_name_set.find(col_ppt.col_name) != col_name_set.end()) {
                return Err<std::string>(Str::format("Error: col_name[%s] specified more than once", col_ppt.col_name));
            }
            col_ppt_lst.push_back(col_ppt);
        } else if (col_def->type == "primary_def") {
            // todo
        } else if (col_def->type == "foreign_def") {
            // todo
        }
    }
    if (primary_key.empty()) {
        return Err<std::string>("Error: primary_key is empty");
    }
    Type::TableProperty table_property(db->get_db_name(), table_name, primary_key, col_ppt_lst);
    db->create_table(table_property);
    Log::log(Str::format("table [%s] create ok!", table_name));
    return Ok<void>();
}

Result<SDB::Type::ColProperty, std::string> 
Executor::col_property(const ParserType::nodePtrType &ptr, std::string &primary_key) {
    auto children = ptr->children;
    std::string col_name = children[0]->name;
    std::string type_name = children[1]->name;
    Enum::ColType col_type = Enum::NONE;
    size_t type_size = 0;
    if (type_name == "int") {
        col_type = Enum::INT;
        type_size = 4;
    } else if (type_name == "smallint") {
        col_type = Enum::SMALLINT;
        type_size = 2;
    } else if (type_name == "varchar") {
        col_type = Enum::VARCHAR;
        type_size = std::stoi(children[1]->children[0]->name.c_str());
    }
    Type::ColProperty col_ppt(col_name, col_type, type_size);
    bool is_not_null = true;
    for (auto &&col_def_ctx : children[2]->children) {
        if (col_def_ctx->name == "not null") {
            is_not_null = true;
        } else if (col_def_ctx->name == "primary key") {
            if (!primary_key.empty()) {
                return Err<std::string>("multiple primary keys are not allowed");
            }
            primary_key = col_name;
        }
    }
    col_ppt.is_not_null = true;
    return Ok<Type::ColProperty>(col_ppt);
}
