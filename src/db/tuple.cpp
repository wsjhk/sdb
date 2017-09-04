#include <type_traits>

#include "tuple.h"

namespace sdb {

using db_type::ObjPtr;
using db_type::ObjCntPtr;

// ========== tuple =========
Tuple &Tuple::operator=(const Tuple &tuple) {
    // deepin copy
    for (auto &ptr : tuple.data) {
        data.push_back(ptr->clone());
    }
}

Tuple &Tuple::operator=(Tuple &&tuple) {
    // simple copy
    data = tuple.data;
    for (ObjPtr &ptr : tuple.data) {
        ptr = nullptr;
    }
}

Size Tuple::type_size()const {
    Size sum = 0;
    for (auto &&ptr : data) {
        sum += ptr->get_type_size();
    }
    return sum;
}

bool Tuple::eq(const Tuple &tuple)const{
    if (data.size() != tuple.data.size()) return false;

    // deepin compare
    for (size_t i = 0; i < data.size(); i++) {
        if (!data[i]->eq(tuple.data[i])) return false;
    }
    return true;
}

// bytes
Bytes Tuple::en_bytes()const {
    return sdb::en_bytes(data);
}

void Tuple::de_bytes(const std::vector<std::pair<db_type::TypeTag, int>> &tag_sizes, const Bytes &bytes, Size offset) {
    for (auto &&[tag, size] : tag_sizes) {
        ObjPtr ptr = db_type::get_default(tag, size);
        ptr->de_bytes(bytes, offset);
        data.push_back(ptr);
    }
}

// ========== tuples =========
// map
Tuples Tuples::map(std::function<db_type::ObjPtr(db_type::ObjCntPtr)> op, int col_offset)const {
    assert(col_offset >= 0 && col_offset < col_num);
    Tuples tuples(col_num);
    for (auto &&tuple : data) {
        Tuple tuple_copy = tuple;
        tuple_copy[col_offset] = op(tuple[col_offset]);
        tuples.data.push_back(tuple_copy);
    }
    return tuples;
}

void Tuples::inplace_map(std::function<void(db_type::ObjPtr)> op, int col_offset) {
    assert(col_offset >= 0 && col_offset < col_num);
    for (auto &&tuple : data) {
        op(tuple[col_offset]);
    }
}

// filter
Tuples Tuples::filter(std::function<bool(db_type::ObjCntPtr)> pred, int col_offset)const {
    assert(col_offset >= 0 && col_offset < col_num);
    Tuples tuples(col_num);
    for (auto &&tuple: data) {
        if (pred(tuple[col_offset])) {
            tuples.data.push_back(tuple);
        }
    }
    return tuples;
}

void Tuples::inplace_filter(std::function<bool(db_type::ObjCntPtr)> pred, int col_offset) {
    assert(col_offset >= 0 && col_offset < col_num);
    for (auto it = data.begin(); it != data.end(); it++) {
        if (pred((*it)[col_offset])) {
            it = std::prev(data.erase(it));
        }
    }
}

// range
void Tuples::range(std::function<void(Tuple)> fn) {
    for (auto && tuple : data) {
        fn(tuple);
    }
}

void Tuples::range(size_t beg, size_t len, std::function<void(Tuple)> fn) {
    for (size_t i = beg; i < data.size() && i < len; i++) {
        fn(data[i]);
    }
}

// bytes
Bytes Tuples::en_bytes()const {
    return sdb::en_bytes(data);
}

void Tuples::de_bytes(const std::vector<std::pair<db_type::TypeTag, int>> &tag_sizes, const Bytes &bytes, int &offset) {
    assert(tag_sizes.size() == col_num);
    data.clear();
    int len;
    sdb::de_bytes(len, bytes, offset);
    assert(len >= 0 || len < BLOCK_SIZE);
    for (int i = 0; i < len; i++) {
        Tuple tuple;
        tuple.de_bytes(tag_sizes, bytes, offset);
        data.push_back(std::move(tuple));
    }
}

} // namespace sdb

