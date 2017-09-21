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

    void begin(Tid t_id);
    void commit(Tid t_id);
    void rollback(Tid t_id);
    void update(Tid t_id, const std::string &table_name, const Tuple &new_tuple);
    void insert(Tid t_id, const std::string &table_name, const Tuple &tuple);
    void remove(Tid t_id, const std::string &table_name, const Tuple &keys);

    std::tuple<Tid, LogType, Bytes> get_log_info(std::ifstream &in);

private:
    Tlog(){}
    void write(const Bytes &bytes);

private:
    std::mutex mutex;
    Tid l_id;
};

} // namespace sdb

#endif /* ifndef DB_TLOG_H */
