#ifndef DB_RECORD_H
#define DB_RECORD_H

#include <vector>
#include <string>
#include <functional>

#include "util.h"
#include "db_type.h"
#include "tuple.h"

namespace sdb {

class Record {
public:
    Record()= delete;
    explicit Record(const std::string &file_path, BlockNum bn);

    bool is_less()const;
    bool is_overflow()const;

    Record split();
    void merge(const Record &record);

    // create and drop
    static void create();
    static void drop();

private:
    const BlockNum block_num;

public:
    Tuples tuples;
};

} // namespace sdb

#endif //MAIN_RECORD_H
