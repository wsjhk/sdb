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
    bool is_full()const;

    Record split();
    void merge(Record &&record);
    
    // === sql ===
    // return new block num if split
    std::optional<BlockNum> insert(const Tuple &key, const Tuple &data);
    // return if remove min key
    std::optional<Tuple> remove(const Tuple &key, const Tuple &data);

    // tuple
    void push_tuple(const Tuple &tuple);
    void push_tuple(Tuple &&tuple);

    // sync
    void sync() const;

    // create and drop
    static Record create(const TableProperty &table_property, BlockNum next_record_num);

private:
    TableProperty tp;
    const BlockNum block_num;
    BlockNum next_record_num;
    BlockOffset offset;
    Tuples tuples;
};

} // namespace sdb

#endif //MAIN_RECORD_H
