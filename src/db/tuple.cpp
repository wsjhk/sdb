#include <type_traits>

#include "tuple.h"

namespace sdb {

using db_type::ObjPtr;
using db_type::ObjCntPtr;

Tuples::Tuples(const Tuples &tuples):col_num(tuples.col_num){
    *this = tuples;
}

Tuples::Tuples(Tuples &&tuples):col_num(tuples.col_num) {
    *this = std::move(tuples);
}

Tuples &Tuples::operator=(const Tuples &tuples) {
    for (auto &&tuple : tuples.data) {
        data.push_back(tuple_clone(tuple));
    }
}

Tuples &Tuples::operator=(Tuples &&tuples) {
    data = std::move(tuples.data);
}

// tuple
std::vector<ObjPtr> Tuples::tuple_clone(const std::vector<db_type::ObjPtr> &tuple) {
    std::vector<ObjPtr> ret;
    for (auto && ptr : tuple) {
        ret.push_back(ptr->clone());
    }
    return ret;
}

Size Tuples::tuple_type_size(const Tuple &tuple) {
    Size sum;
    for (auto &&ptr : tuple) {
        sum += ptr->get_type_size();
    }
    return sum;
}

// map
Tuples Tuples::map(std::function<db_type::ObjPtr(db_type::ObjCntPtr)> op, int col_offset)const {
    assert(col_offset >= 0);
    Tuples tuples(col_num);
    for (auto &&tuple : data) {
        std::vector<ObjPtr> tuple_copy = tuple_clone(tuple);
        tuple_copy[col_offset] = op(tuple[col_offset]);
        tuples.data.push_back(tuple_copy);
    }
    return tuples;
}

void Tuples::invaded_map(std::function<void(db_type::ObjPtr)> op, int col_offset) {
    assert(col_offset >= 0);
    for (auto &&lst : data) {
        op(lst[col_offset]);
    }
}

// filter
Tuples Tuples::filter(std::function<bool(db_type::ObjCntPtr)> pred, int col_offset)const {
    assert(col_offset >= 0);
    Tuples tuples(col_num);
    for (auto &&tuple: data) {
        if (pred(tuple[col_offset])) {
            tuples.data.push_back(tuple_clone(tuple));
        }
    }
    return tuples;
}

void Tuples::invaded_filter(std::function<bool(db_type::ObjCntPtr)> pred, int col_offset) {
    assert(col_offset >= 0);
    for (auto it = data.begin(); it != data.end(); it++) {
        if (pred(it->at(col_offset))) {
            it = std::prev(data.erase(it));
        }
    }
}

// range
void Tuples::range(std::function<void(std::vector<db_type::ObjPtr>)> fn) {
    for (auto && tuple : data) {
        fn(tuple);
    }
}

void Tuples::range(int beg, int len, std::function<void(std::vector<db_type::ObjPtr>)> fn) {
    if (beg < 0) beg = 0;
    for (int i = beg; i < data.size() || i < len; i++) {
        fn(data[i]);
    }
}

// bytes
Bytes Tuples::en_bytes()const {
    Bytes bytes;
    for (auto &&tuple : data) {
        for (auto && ptr : tuple) {
            Bytes obj_bytes = ptr->en_bytes();
            bytes.insert(bytes.end(), obj_bytes.begin(), obj_bytes.end());
        }
    }
}

void Tuples::de_bytes(const std::vector<std::pair<db_type::TypeTag, int>> &tag_sizes,const Bytes &bytes, int &offset) {
    int len;
    sdb::de_bytes(len, bytes, offset);
    assert(len >= 0 || len < BLOCK_SIZE);
    for (int i = 0; i < len; i++) {
        std::vector<ObjPtr> ptrs;
        for (auto &&[tag, size]: tag_sizes) {
            ObjPtr ptr = db_type::get_default(tag, size);
            ptr->de_bytes(bytes, offset);
        }
        data.push_back(ptrs);
    }
}

} // namespace sdb

