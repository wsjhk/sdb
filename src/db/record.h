#ifndef DB_RECORD_H
#define DB_RECORD_H

#include <vector>
#include <string>
#include <functional>

#include "util.h"
#include "property.h"
#include "db_type.h"
#include "tuple.h"
#include "cache.h"

namespace sdb {

class Record {
public:
    Record()= delete;
    explicit Record(TableProperty, BlockNum);

    bool is_less()const;
    bool is_overflow()const;

    Record split();
    void merge(Record &&record);

    // tuple
    void push_tuple(const Tuples::Tuple &tuple);
    void push_tuple(Tuples::Tuple &&tuple);

    // sync
    void sync() const;

    // create and drop
    static Record create(const TableProperty &table_property);

private:
    TableProperty tp;
    const BlockNum block_num;
    BlockOffset offset;
    Tuples tuples;
};

} // namespace sdb

#endif //MAIN_RECORD_H
