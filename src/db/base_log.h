#ifndef DB_BASE_LOG_H
#define DB_BASE_LOG_H

#include <string>
#include <mutex>
#include "util.h"
#include "io.h"

namespace sdb {

class BaseLog {
public:
    BaseLog(const std::string &path):file_path(IO::get().get_db_file_path(path)){}

    void log(const Bytes &bytes);
    void range(std::function<void(Bytes)> op);

protected: // function
    void write(const Bytes &bytes);

private:
    std::mutex mutex;
    std::string file_path;
};

} // namespace sdb

#endif /* ifndef DB_BASE_LOG_H */
