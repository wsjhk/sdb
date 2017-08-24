#ifndef DB_TUPLE_HPP
#define DB_TUPLE_HPP

#include <boost/any.hpp>
#include <boost/format.hpp>
#include <type_traits>

#include "util.h"
#include "db_type.h"

namespace sdb {

struct Tuples {
    std::vector<std::string> col_name_lst;
    std::vector<std::vector<db_type::ObjPtr>> data;

    Tuples()=delete;
    Tuples(std::vector<std::string> col_name_lst):col_name_lst(std::move(col_name_lst)){}

    // debug
    void print()const;
};

} // namespace sdb

#endif /* ifndef DB_TUPLE_HPP */
