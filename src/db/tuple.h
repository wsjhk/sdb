#ifndef DB_TUPLE_H
#define DB_TUPLE_H

#include "util.h"
#include "db_type.h"

namespace sdb {

class TuplesError : public std::runtime_error {
    TuplesError(const std::string &str):runtime_error(str){}
};

class Tuples {
public: 
    Tuples(int col_num):col_num(col_num){}
    Tuples(const Tuples &);
    Tuples(Tuples &&);
    Tuples &operator=(const Tuples &);
    Tuples &operator=(Tuples &&);

    // tuple 
    static std::vector<db_type::ObjPtr> tuple_clone(const std::vector<db_type::ObjPtr> &tuple);
    void push_back(const std::vector<db_type::ObjPtr> &tuple);

    // map
    Tuples map(std::function<db_type::ObjPtr(db_type::ObjCntPtr)> op, int col_offset)const;
    void invaded_map(std::function<void(db_type::ObjPtr)> op, int col_offset);

    // filter
    Tuples filter(std::function<bool(db_type::ObjCntPtr)> pred, int col_offset)const;
    void invaded_filter(std::function<bool(db_type::ObjCntPtr)> pred, int col_offset);

    // range
    void range(std::function<void(std::vector<db_type::ObjPtr>)> fn);
    void range(int beg, int offset, std::function<void(std::vector<db_type::ObjPtr>)> fn);

    // bytes
    Bytes en_bytes()const;
    void de_bytes(const std::vector<std::pair<db_type::TypeTag, int>> &type_tags,const Bytes &bytes, int &offset) ;

    // debug
    void print()const;

private: // menber
    const int col_num;
    std::vector<std::vector<db_type::ObjPtr>> data;
};

} // namespace sdb

#endif /* ifndef DB_TUPLE_H */
