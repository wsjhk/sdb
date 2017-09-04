#ifndef DB_TUPLE_H
#define DB_TUPLE_H

#include "util.h"
#include "db_type.h"

namespace sdb {

class Tuple {
public:
    Tuple(){}
    Tuple(const Tuple &tuple) {*this = tuple;}
    Tuple(Tuple &&tuple) {*this = std::move(tuple);}
    Tuple &operator=(const Tuple &);
    Tuple &operator=(Tuple &&);

    // index
    db_type::ObjPtr operator[](Size size) {
        assert(size >= 0 && size <= data.size());
        return data[size];
    }
    db_type::ObjCntPtr operator[](Size size) const {
        assert(size >= 0 && size <= data.size());
        return data[size];
    }

    Size type_size()const;

    // compare
    bool eq(const Tuple &tuple)const;

    // bytes
    Bytes en_bytes()const;
    void de_bytes(const std::vector<std::pair<db_type::TypeTag, int>> &tag_sizes, const Bytes &bytes, Size offset);

    // push back
    void push_back(db_type::ObjCntPtr ptr){
        data.push_back(ptr->clone());
    }
    void push_back(db_type::ObjPtr &&ptr){
        data.push_back(ptr);
    }

private:
    std::vector<db_type::ObjPtr> data;
};

struct Tuples {
    // member
    const int col_num;
    std::vector<Tuple> data;

    // constructor
    Tuples(int col_num):col_num(col_num){}
    Tuples(const Tuples &)=default;
    Tuples(Tuples &&)=default;
    Tuples &operator=(const Tuples &)=default;
    Tuples &operator=(Tuples &&)=default;

    // map
    Tuples map(std::function<db_type::ObjPtr(db_type::ObjCntPtr)> op, int col_offset)const;
    void inplace_map(std::function<void(db_type::ObjPtr)> op, int col_offset);

    // filter
    Tuples filter(std::function<bool(db_type::ObjCntPtr)> pred, int col_offset)const;
    void inplace_filter(std::function<bool(db_type::ObjCntPtr)> pred, int col_offset);

    // range
    void range(std::function<void(Tuple)> fn);
    void range(size_t beg, size_t offset, std::function<void(Tuple)> fn);

    // bytes
    Bytes en_bytes()const;
    void de_bytes(const std::vector<std::pair<db_type::TypeTag, int>> &type_tags,const Bytes &bytes, int &offset) ;

    // debug
    void print()const;
};

} // namespace sdb

#endif /* ifndef DB_TUPLE_H */
