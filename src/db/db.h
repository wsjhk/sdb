#ifndef DB_DB_H
#define DB_DB_H

#include <string>
#include <unordered_set>
#include <utility>
#include <boost/thread.hpp>

#include "table.h"
#include "tlog.h"
#include "../sql/ast.h"

namespace sdb {

class DB {
public:
    // type
    using TablePtr = std::shared_ptr<Table>;
    using TlogPtr = std::shared_ptr<Tlog>;

public:
    DB(const std::string &db_name);

    // db op
    static void create_db(const std::string &db_name);
    static void drop_db(const std::string &db_name);
    void execute(AstNodePtr ptr);
    // log
    void recover();

private:
    // check integrity
    // template <typename T>
    // void check_referenced(const Table &table, T t);
    // void check_referencing(const Table &table, const SDB::Type::TupleData &tuple_data);

    // add meta table to db_map
    void add_table_list();
    void add_col_list();
    // void add_index();
    // void add_reference();
    std::vector<std::string> table_name_lst(TransInfo ti);

    // tp
    TableProperty get_tp(TransInfo ti, const std::string &table_name);

    // table
    void create_table(TransInfo ti, const TableProperty &tp);
    void drop_table(TransInfo ti, const std::string &table_name);
    TablePtr get_table_ptr(Tid t_id, const std::string &db_name);

    // transaction check
    void trans_check(TransInfo t_info);

    // transaction begin/commit
    Tid get_new_tid();
    TransInfo begin(Tid t_id);
    void commit(Tid t_id);
    void rollback(Tid t_id);

    // log
    // redo op
    void log_redo_begin(Tid t_id);
    void log_redo_commit(Tid t_id);
    void log_redo_rollback(Tid t_id);
    void log_redo_update(Tid t_id, const Bytes &bytes);
    void log_redo_insert(Tid t_id, const Bytes &bytes);
    void log_redo_remove(Tid t_id, const Bytes &bytes);
    // undo
    void log_undo(const std::set<Tid> &undo_set);

private:
    std::string db_name;
    // TODO concurrent map
    // TODO deadlock maybe
    // <name, tablePtr>
    std::map<std::string, boost::upgrade_mutex> mutex_map;
    std::map<std::string, TablePtr> table_map;
    // int8_t => 
    // -3 : apply upgrade_lock
    // -2 : apply hared_lock
    // -1 : apply unque_lock
    // 0  : init
    // 1  : has shared_lock
    // 2  : has unique_lock
    //
    std::map<Tid, TransInfo> t_info_map;
    // <t_id, <table name, <lock info, table ptr>>>
    std::map<Tid, std::map<std::string, std::pair<int8_t, TablePtr>>> t_snapshot;

    // atomic transaction id
    std::atomic_int64_t atomic_t_id;

    // log
    TlogPtr t_log_ptr;
};

} // namespace sdb

#endif //DB_DB_H
