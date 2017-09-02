#ifndef DB_TUPLE_H
#define DB_TUPLE_H

#include "util.h"
#include "db_type.h"

namespace sdb {

struct Tuples {
    // type
    using Tuple = std::vector<db_type::ObjPtr>;

    // constructor
    Tuples(int col_num):col_num(col_num){}
    Tuples(const Tuples &);
    Tuples(Tuples &&);
    Tuples &operator=(const Tuples &);
    Tuples &operator=(Tuples &&);

    // tuple 
    static Tuple tuple_clone(const Tuple &tuple);
    static Size tuple_type_size(const Tuple &tuple);

    // map
    Tuples map(std::function<db_type::ObjPtr(db_type::ObjCntPtr)> op, int col_offset)const;
    void invaded_map(std::function<void(db_type::ObjPtr)> op, int col_offset);

    // filter
    Tuples filter(std::function<bool(db_type::ObjCntPtr)> pred, int col_offset)const;
    void invaded_filter(std::function<bool(db_type::ObjCntPtr)> pred, int col_offset);

    // range
    void range(std::function<void(Tuple)> fn);
    void range(int beg, int offset, std::function<void(Tuple)> fn);

    // bytes
    Bytes en_bytes()const;
    void de_bytes(const std::vector<std::pair<db_type::TypeTag, int>> &type_tags,const Bytes &bytes, int &offset) ;

    // debug
    void print()const;

    const int col_num;
    std::vector<Tuple> data;
};

} // namespace sdb

#endif /* ifndef DB_TUPLE_H */
