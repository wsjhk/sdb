#include <type_traits>

#include "type.h"

namespace SDB::Type {
}

namespace SDB::Function {

Type::BVFunc get_bvfunc(Enum::BVFunc func, Type::Value value) {
    using Type::Value;
    switch (func) {
        case Enum::EQ:
            return [value](Value v){ return v == value;};
        case Enum::LESS:
            return [value](Value v){ return v < value;};
        case Enum::GREATER:
            return [value](Value v){ return !(v <= value);};
    }
}

void tuple_lst_map(Type::TupleLst &tuple_lst,
                   const std::string &col_name,
                   Type::VVFunc) {
    for (auto &&tuple : tuple_lst.tuple_lst) {
    }
}

} // SDB::Function namespace
