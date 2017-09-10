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
    Size data_bytes_size()const;
    Size len()const { return data.size(); }

    // compare
    bool eq(const Tuple &tuple)const;
    bool pre_eq(const Tuple &tuple)const;
    bool less(const Tuple &tuple)const;

    // bytes
    Bytes en_bytes()const;
    void de_bytes(const std::vector<db_type::TypeInfo> &infos, const Bytes &bytes, Size offset);

    // push back
    void push_back(db_type::ObjCntPtr ptr){
        data.push_back(ptr->clone());
    }

    void range(db_type::ObjCntOp op) const {
        for (db_type::ObjCntPtr ptr : data) {
            op(ptr);
        }
    }

    Tuple select(std::vector<Size> pos_lst)const;


private:
    std::vector<db_type::ObjPtr> data;
};

class Tuples {
public:
    // constructor
    Tuples(Size col_num):col_num(col_num){}

    // map
    Tuples map(std::function<db_type::ObjPtr(db_type::ObjCntPtr)> op, int col_offset)const;
    void inplace_map(std::function<void(db_type::ObjPtr)> op, int col_offset);

    // filter
    Tuples filter(std::function<bool(db_type::ObjCntPtr)> pred, int col_offset)const;
    void inplace_filter(std::function<bool(db_type::ObjCntPtr)> pred, int col_offset);

    // range
    void range(std::function<void(Tuple)> fn);
    void range(size_t beg, size_t offset, std::function<void(Tuple)> fn);

    // append
    void append(const Tuples &tuples) {
        assert(tuple.len() == col_num);
        data.insert(data.end(), tuples.data.begin(), tuples.data.end());
    }

    // push
    void push_back(const Tuple &tuple) {
        assert(tuple.len() == col_num);
        data.push_back(tuple);
    }

    // bytes
    Bytes en_bytes()const;
    void de_bytes(const std::vector<db_type::TypeInfo> &infos, const Bytes &bytes, int &offset) ;

    // debug
    void print()const;

public:
    std::vector<Tuple> data;
private:
    Size col_num;
};

template <>
inline Bytes en_bytes(Tuple tuple) {
    return tuple.en_bytes();
}

// type alias
using TuplePred = std::function<bool(Tuple)>;
using TupleOp = std::function<Tuple(Tuple)>;

} // namespace sdb

#endif /* ifndef DB_TUPLE_H */
