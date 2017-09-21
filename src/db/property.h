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
    db_type::TypeInfo type_info;
    int8_t order_num;
    bool is_key;
    bool is_not_null;

    ColProperty()=delete;
    ColProperty(const std::string &col_name, db_type::TypeInfo type_info, int8_t order_num, bool is_key = false, bool is_not_null = true):col_name(col_name), type_info(type_info), order_num(order_num), is_key(is_key), is_not_null(is_not_null){}

    Bytes en_bytes()const;
    static ColProperty de_bytes(const Bytes &bytes, Size &offset);
};

// table property
struct TableProperty {
    // type alias
    using ColPropertyList = std::vector<ColProperty>;

    // Type
    std::string table_name;
    BlockNum record_root;
    BlockNum keys_idx_root;
    ColPropertyList col_property_lst;
    // integrity
    // <table_name, col_name>
    std::unordered_map<std::string, std::string> referencing_map;
    // <table_name, col_name>
    std::unordered_map<std::string, std::string> referenced_map;

    // TableProperty(){}
    TableProperty(const std::string &table_name,
                  BlockNum record_root,
                  BlockNum keys_idx_root,
                  const ColPropertyList &col_property_lst)
            :table_name(table_name), record_root(record_root), keys_idx_root(keys_idx_root) , col_property_lst(col_property_lst){}

    // getter
    Size get_col_property_pos(const std::string &col_name)const;
    std::vector<std::string> get_col_name_lst()const;
    ColPropertyList get_keys_property()const;
    // TODO
    ColProperty get_col_property(const std::string &col_name)const;
    // TODO
    std::vector<db_type::TypeInfo> get_type_info_lst()const;
    // TODO
    std::vector<Size> get_keys_pos()const;
};

} // namespace sdb
#endif /* ifndef DB_PROPERTY_H */
