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
#include "io.h"
#include "block_alloc.h"

namespace sdb {

class Record {
public:
    Record()= delete;
    Record(TransInfo info, TableProperty, BlockNum);

    bool is_less()const;
    bool is_full()const;

    Record split();
    void merge(Record &&record);
    
    // === sql ===
    // return new block num if split
    std::optional<BlockNum> insert(const Tuple &key, const Tuple &data);
    // remove
    void remove(const Tuple &key);
    // TODO
    void remove(TuplePred pred);
    // update
    // return right pos if split
    std::optional<BlockNum> update(const Tuple &key, const std::string &col_name, db_type::ObjCntPtr new_val);
    // TODO
    // return return rightmost pos if multi-split
    std::optional<BlockNum> update(TuplePred pred, TupleOp op);

    //  find
    Tuples find_key(const Tuple &tuple);
    Tuples find_less(const Tuple &key, bool is_close)const;
    Tuples find_greater(const Tuple &key, bool is_close)const;
    Tuples find_range(const Tuple &beg, const Tuple &end, bool is_beg_close, bool is_end_close)const;
    // TODO
    Tuples find(TuplePred pred);

    // get
    Tuples get_all_tuple()const;
    BlockNum get_block_num()const {return block_num;}
    BlockNum get_next_record_num()const {return block_num;}

    // sync
    void sync() const;

private: // function
    Size get_bytes_size()const;
    std::string block_path()const {
        return tp.db_name + "/block.sdb";
    }

private: // member
    TransInfo t_info;
    TableProperty tp;
    BlockNum block_num;
    BlockNum next_record_num = -1;

public:
    std::list<std::pair<Vid, Tuple>> record_lst;
};

} // namespace sdb

#endif //MAIN_RECORD_H
