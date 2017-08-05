#ifndef DB_H
#define DB_H

#include <string>
#include <unordered_set>
#include "util.h"
#include "table.h"

class DB {
public:
    // type
    using TableNameSet = std::unordered_set<std::string>;
    using Value = SDB::Type::Value;
    using Tuple = SDB::Type::Tuple;
    using TupleLst = SDB::Type::TupleLst;

public:
    ~DB()noexcept {
        write_meta_data(db_name, table_name_set);
    }

    // get_db
    static void create_db(const std::string &db_name);
    static std::optional<DB*> get_db(const std::string &db_name);
    static bool hasDatabase(const std::string &db_name);
    static void drop_db(const std::string &db_name);

    void create_table(const SDB::Type::TableProperty &table_property);
    void drop_table(const std::string &table_name);
    void insert(const std::string &table_name, const SDB::Type::TupleData &tuple_data);
    void remove(const std::string &table_name, const std::string &col_name, const Value &value);
    void remove(const std::string &table_name, const std::string &col_name, SDB::Type::BVFunc predicate);
    void update(const std::string &table_name, 
                const std::string &pred_col_name, SDB::Type::BVFunc predicate,
                const std::string &op_col_name, SDB::Type::VVFunc op);
    DB::TupleLst find(const std::string &table_name,
                      const std::string &col_name,
                      const SDB::Type::Value &value);
    DB::TupleLst find(const std::string &table_name,
                      const std::string &col_name,
                      std::function<bool(DB::Value)> predicate);

private:
    DB()=delete;
    DB(const std::string &db_name):db_name(db_name){
        read_meta_data();
    }
    // DB(const DB &db)=delete;
    // DB(DB &&db):db_name(std::move(db.db_name)), table_name_set(std::move(db.table_name_set)){}
    // DB &operator=(const DB &&db)=delete;

    static std::unordered_map<std::string, DB> get_db_list();

    static void write_meta_data(const std::string &db_name, const TableNameSet &set);
    void read_meta_data();

    static std::string get_meta_path(const std::string &db_name);

    // check integrity
    template <typename T>
    void check_referenced(const Table &table, T t);
    void check_referencing(const Table &table, const SDB::Type::TupleData &tuple_data);


private:
    static std::unordered_map<std::string, DB> db_list;
    std::string db_name;
    TableNameSet table_name_set;
};

#endif //DB_H
