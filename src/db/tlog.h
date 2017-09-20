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
    Tlog(const std::string &db_name):db_name(db_name){}
    void begin(Tid t_id, const std::string &db_name);
    void commit(Tid t_id, const std::string &db_name);
    void rollback(Tid t_id, const std::string &db_name);
    void update(Tid t_id, const std::string &table_name, const Tuple &new_tuple);
    void insert(Tid t_id, const std::string &table_name, const Tuple &tuple);
    void remove(Tid t_id, const std::string &table_name, const Tuple &keys);

    std::tuple<Tid, LogType, Bytes> get_log_info(std::ifstream &in);

private:
    void write(const std::string &db_name, const Bytes &bytes);

private:
    std::string db_name;
    std::mutex mutex;
    Tid l_id;
};

} // namespace sdb

#endif /* ifndef DB_TLOG_H */
