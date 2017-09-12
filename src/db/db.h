#ifndef DB_H
#define DB_H

#include <string>
#include <unordered_set>
#include <utility>
#include "table.h"

namespace sdb {

class DB {
public:
    // type
    using DBPtr = std::shared_ptr<DB>;

public:
    DB(const std::string &db_name);

    // db op
    static void create_db(const std::string &db_name);
    static void drop_db(const std::string &db_name);

    // table
    void create_table(Tid t_id, const TableProperty &table_property);
    void drop_table(Tid t_id, const std::string &table_name);
    void insert(Tid t_id, const std::string &table_name, const Tuple &data);
    void remove(Tid t_id, const std::string &table_name, const Tuple &key);
    void remove(Tid t_id, const std::string &table_name, TuplePred pred);
    void update(Tid t_id, const std::string &table_name, TuplePred predicate, TupleOp op);
    Tuples find(Tid t_id, const std::string &table_name, const Tuple &key);
    Tuples find(Tid t_id, const std::string &table_name, TuplePred pred);

private:
    // check integrity
    // template <typename T>
    // void check_referenced(const Table &table, T t);
    // void check_referencing(const Table &table, const SDB::Type::TupleData &tuple_data);

    // add meta table to db_map
    void add_table_list();
    void add_col_list();
//    void add_index();
    void add_reference();
    std::vector<std::string> table_name_lst(Tid tid)const;

    TableProperty get_tp(Tid tid, const std::string &table_name);

private:
    std::string db_name;
    // TODO concurrent map
    // <name, tablePtr>
    using SmTp = std::pair<std::shared_mutex, Table::TablePtr>;
    static std::map<std::string, SmTp> db_map;
};

} // namespace sdb

#endif //DB_H
