#ifndef DB_TLOG_H
#define DB_TLOG_H 

#include "util.h"
#include "db_type.h"
#include "property.h"
#include "tuple.h"

#include <mutex>
#include <fstream>

namespace sdb {

class Tlog {
public:
    using Lid = int64_t;
    enum LogType : char { 
        BEGIN,
        COMMIT,
        ROLLBACK,
        UPDATE,
        INSERT,
        REMOVE,
    };

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

    template <typename F>
    void redo(const std::string &db_name, F get_table_ptr);
    template <typename F>
    void undo(F get_table_ptr);

private:
    Tlog(){}

    std::tuple<Tid, LogType, Bytes> get_log_info(std::ifstream &in);
    void write(const std::string &db_name, const Bytes &bytes);

private:
    // <db_name, <mutex, log id>>
    static std::map<std::string, std::pair<std::mutex, Lid>> db_mt;
};

template <typename F>
void Tlog::redo(const std::string &db_name, F get_table_ptr) {
    std::ifstream in(db_name + "/log.sdb");
    while (!in.eof()) {
        auto &&[t_id, l_type, data] = get_log_info(in);
        // TODO
        switch (l_type) {
            case BEGIN:
                break;
            case COMMIT:
                break;
            case ROLLBACK:
                break;
            case UPDATE:
                break;
            case INSERT:
                break;
            case REMOVE:
                break;
            default:
                break;
        }
    }
}

template <typename F>
void undo(F get_table_ptr);

} // namespace sdb

#endif /* ifndef DB_TLOG_H */
