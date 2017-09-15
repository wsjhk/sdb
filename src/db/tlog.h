#ifndef DB_TLOG_H
#define DB_TLOG_H 

#include "util.h"
#include "db_type.h"
#include "property.h"
#include "tuple.h"

#include <mutex>

namespace sdb {

class Tlog {
public:
    using Lid = int64_t;
public:
    static Tlog &get() {
        static Tlog t_log;
        return t_log;
    }

    void begin(Tid t_id, const std::string &db_name);
    void commit(Tid t_id, const std::string &db_name);
    void rollback(Tid t_id, const std::string &db_name);
    void update(Tid t_id, const TableProperty &tp, const std::string &col_name, db_type::ObjCntPtr old_val, db_type::ObjCntPtr new_val);
    void insert(Tid t_id, const TableProperty &tp, const Tuple &data);
    void remove(Tid t_id, const TableProperty &tp, const Tuple &keys);
    void redo_only_update(Tid t_id, Lid l_id, const TableProperty &tp, const std::string &col_name, db_type::ObjCntPtr old_val, db_type::ObjCntPtr new_val);
    void redo_only_insert(Tid t_id, Lid l_id, const TableProperty &tp, const Tuple &data);
    void redo_only_remove(Tid t_id, Lid l_id, const TableProperty &tp, const Tuple &keys);

    template <typename F>
    void redo(F get_table_ptr);
    template <typename F>
    void undo(F get_table_ptr);

private:
    Tlog(){}

    void write(const std::string &db_name, const Bytes &bytes);

private:
    static std::map<std::string, std::pair<std::mutex, BlockNum>> db_mt;
};

} // namespace sdb

#endif /* ifndef DB_TLOG_H */
