//
// Created by sven on 17-3-23.
//

#include "db.h"
#include "io.h"
#include "util.h"
#include "table.h"
#include "cache.h"

using namespace SDB;

// ========== static =======
std::unordered_map<std::string, DB>DB::db_list = get_db_list();

// ========== Public =======
void DB::create_db(const std::string &db_name) {
    IO::create_dir(db_name);
    IO::create_file(get_meta_path(db_name));
    write_meta_data(db_name, TableNameSet());
}

std::optional<DB*> DB::get_db(const std::string &db_name) {
    if (db_list.find(db_name) == db_list.end()) {
        return {};
    }
    return &db_list.at(db_name);
}

bool DB::hasDatabase(const std::string &db_name){
    return get_db(db_name).has_value();
}

void DB::drop_db(const std::string &db_name){
    Cache &cache = Cache::make();
    cache.pop_file(get_meta_path(db_name));
    IO::remove_dir_force(db_name);
}

void DB::create_table(const SDB::Type::TableProperty &table_property) {
    Table::create_table(table_property);
    table_name_set.insert(table_property.table_name);
}

void DB::drop_table(const std::string &table_name) {
    Table table(db_name, table_name);
    if (table.is_referenced()) {
        throw std::runtime_error("Error:[drop table] table already is referenced");
    }
    for (auto &&item : table.get_referencing_map()) {
        Table ref_table(db_name, item.first);
        ref_table.remove_referenced(table_name);
    }
    table_name_set.erase(table_name);
    table.drop_table();
}

void DB::insert(const std::string &table_name, const Type::TupleData& tuple_data) {
    Table table(db_name, table_name);
    check_referencing(table, tuple_data);
    table.insert(tuple_data);
}

void DB::remove(const std::string &table_name,
                const std::string &col_name,
                const Value &value) {
    Table table(db_name, table_name);
    check_referenced(table, value);
    table.remove(col_name, value);
}

void DB::remove(const std::string &table_name,
                const std::string &col_name,
                SDB::Type::BVFunc predicate) {
    Table table(db_name, table_name);
    check_referenced(table, predicate);
    table.remove(col_name, predicate);
}

void DB::update(const std::string &table_name,
                const std::string &pred_col_name, SDB::Type::BVFunc predicate,
                const std::string &op_col_name, SDB::Type::VVFunc op) {
    Table table(db_name, table_name);
    TupleLst tuple_lst = table.find(pred_col_name, predicate);
    bool is_op_key = op_col_name == table.get_key();
    for (auto &&tuple_data : tuple_lst.data) {
        if (is_op_key) {
            Type::Pos col_pos = Type::TupleData::get_col_name_pos(table.get_col_name_lst(), op_col_name);
            Value check_value = tuple_data.get_value(col_pos);
            check_referenced(table, check_value);
        }
        check_referencing(table, tuple_data);
    }
    table.update(pred_col_name, predicate, op_col_name, op);
}

DB::TupleLst DB::find(const std::string &table_name,
                      const std::string &col_name,
                      const SDB::Type::Value &value) {
    Table table(db_name, table_name);
    return table.find(col_name, value);
}

DB::TupleLst DB::find(const std::string &table_name,
                      const std::string &col_name,
                      std::function<bool(DB::Value)> predicate) {
    Table table(db_name, table_name);
    return table.find(col_name, predicate);
}

// ========== Private =======
std::unordered_map<std::string, DB> DB::get_db_list() {
    std::unordered_map<std::string, DB> ret;
    for(auto &&name: IO::get_db_name_list()) {
        ret.insert_or_assign(name, DB(name));
    }
    return ret;
}

void DB::write_meta_data(const std::string &db_name, const TableNameSet &set) {
    Type::Bytes bytes;
    Function::bytes_append(bytes, set);
    Cache::make().write_file(get_meta_path(db_name), bytes);
}

void DB::read_meta_data() {
    Type::Bytes bytes = Cache::make().read_file(get_meta_path(db_name));
    size_t offset = 0;
    Function::de_bytes(table_name_set, bytes, offset);
}

std::string DB::get_meta_path(const std::string &db_name) {
    return db_name + "/meta.sdb";
}

// check integrity
template <typename T>
void DB::check_referenced(const Table &table, T t){
    auto referenced_map = table.get_referenced_map();
    for (auto &&item : referenced_map) {
        Table ref_table(db_name, item.first);
        Type::TupleLst tuple_lst = ref_table.find(item.second, t);
        if (!tuple_lst.data.empty()) {
            throw std::runtime_error(
                std::string("Error : referenced key")
            );
        }
    }
}

void DB::check_referencing(const Table &table, const Type::TupleData &tuple_data){
    auto referencing_map = table.get_referencing_map();
    for (auto &&[refing_name, refed_name] : referencing_map) {
        Table ref_table(db_name, refing_name);
        Type::Value check_value = tuple_data.get_value(tuple_data.get_col_name_pos(table.get_col_name_lst(), refed_name));
        Type::TupleLst tuple_lst = ref_table.find(ref_table.get_key(), check_value);
        if (tuple_lst.data.empty()) {
            throw std::runtime_error(
                std::string("Error [db.insert]: can't fonud referencing key:")+check_value.get_string()
            );
        }
    }
}
