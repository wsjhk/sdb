#ifndef DB_PROPERTY_H
#define DB_PROPERTY_H

#include <boost/any.hpp>
#include <boost/format.hpp>
#include <type_traits>

#include "util.h"
#include "db_type.h"

namespace sdb {

struct ColProperty {
    std::string col_name;
    db_type::TypeTag col_type;
    size_t type_size;
    db_type::ObjCntPtr default_value;
    char is_not_null;

    ColProperty()=delete;
    ColProperty(const std::string &col_name, db_type::TypeTag col_type, size_t type_size)
        :col_name(col_name), col_type(col_type), type_size(type_size),
         default_value(db_type::get_default(col_type, type_size)), is_not_null(true){}

    ColProperty(const std::string col_name, 
                db_type::TypeTag col_type,
                size_t type_size,
                db_type::ObjCntPtr obj,
                char is_not_null)
        :col_name(col_name), col_type(col_type),
         type_size(type_size), default_value(obj),
         is_not_null(is_not_null){}

    Bytes en_bytes()const;
    static ColProperty de_bytes(const Bytes &bytes, Size &offset);
};

// table property
struct TableProperty {
    // type alias
    using ColPropertyList = std::vector<ColProperty>;

    // Type
    std::string db_name;
    std::string table_name;
    std::string key;
    ColPropertyList col_property_lst;
    // integrity
    // <table_name, col_name>
    std::unordered_map<std::string, std::string> referencing_map;
    // <table_name, col_name>
    std::unordered_map<std::string, std::string> referenced_map;

    TableProperty(){}
    TableProperty(const std::string &db_name,
                  const std::string &table_name,
                  const std::string &key,
                  const ColPropertyList &col_property_lst)
            :db_name(db_name), table_name(table_name), key(key), col_property_lst(col_property_lst){}

    // getter
    size_t get_col_property_pos(const std::string &col_name)const;
    std::vector<std::string> get_col_name_lst()const;
};

} // namespace sdb
#endif /* ifndef DB_PROPERTY_H */
