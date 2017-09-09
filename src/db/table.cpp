#include <vector>
#include <fstream>
#include <stdexcept>
#include <string>
#include <map>
#include <utility>
#include <functional>

#include "table.h"

#include "bpTree.h"
#include "util.h"
#include "io.h"
#include "cache.h"
#include "block_alloc.h"

namespace sdb {

using TablePtr = Table::TablePtr;
// static init
tbb::concurrent_unordered_map<std::pair<std::string, std::string>, TablePtr> Table::table_map;

// ========== public function ========
void Table::init(const std::string &db_name) {
    // .table_list
    auto tlt_ptr = table_list_table(db_name);
    table_map[{db_name, ".table_list"}] = tlt_ptr;
    // .col_list
    auto clt_ptr = table_list_table(db_name);
    table_map[{db_name, ".col_list"}] = clt_ptr;
    // .index
    auto idt_ptr = table_list_table(db_name);
    table_map[{db_name, ".index"}] = idt_ptr;
}

TablePtr Table::get_table(const std::string &db_name, const std::string &table_name) {
    auto it = table_map.find({db_name, table_name});
    if (it == table_map.end()) {
        throw TableNotFound(table_name);
    }
    return it->second;
}

void Table::create_table(const TableProperty &property) {
    // table list
    Tuple tl_tuple;
    auto table_name_ptr = std::make_shared<db_type::Varchar>(64, property.table_name);

    tl_tuple.push_back(table_name_ptr->clone());
    BlockNum pos = BlockAlloc::get().new_block(property.db_name);
    tl_tuple.push_back(std::make_shared<db_type::Int>(pos));
    table_map[{property.db_name, ".table_list"}]->insert(tl_tuple);

    // col list
    auto col_list_ptr = table_map[{property.db_name, ".col_list"}];
    int order_num = 0;
    std::list<std::string> key_list;
    for (auto &&cl : property.col_property_lst) {
        Tuple cl_tuple;
        // table name
        tl_tuple.push_back(table_name_ptr->clone());
        // col name
        tl_tuple.push_back(std::make_shared<db_type::Varchar>(64, cl.col_name));
        // type info
        tl_tuple.push_back(std::make_shared<db_type::Varchar>(64, cl.type_info));
        // order num
        tl_tuple.push_back(std::make_shared<db_type::Int>(order_num));
        order_num++;
        // is key
        if (cl.is_key) key_list.push_back(cl.col_name);
        tl_tuple.push_back(std::make_shared<db_type::Char>(cl.is_key));
        // is not null
        tl_tuple.push_back(std::make_shared<db_type::Char>(cl.is_not_null));
        col_list_ptr->insert(cl_tuple);
    }

    // index
    table_map[{property.db_name, ".col_list"}]->create_index("keys_idx", key_list);
}

void Table::drop_table(const std::string &db_name, const std::string &table_name) {
    // col list
    auto clt_ptr = table_map[{db_name, ".col_list"}];
    Tuple table_name_key;
    table_name_key.push_back(std::make_shared<db_type::Varchar>(64, table_name));
    Tuples ts = clt_ptr->keys_index->find_pre_key(table_name_key);
    auto keys_pos = clt_ptr->tp.get_keys_pos();
    for (auto &&tuple : ts.data) {
        auto remove_key = tuple.select(keys_pos);
        clt_ptr->keys_index->remove(remove_key);
    }

    // index
    clt_ptr->remove_index("keys_idx");

    // table list
    table_map[{db_name, ".table_list"}]->remove(table_name_key);
    // delete from
}

// ========== private function ========
Table::Table(const TableProperty &tp, bool is_init = false):tp(tp){
    if (is_init) {
        return;
    }
    auto ptr = table_map[{tp.db_name, ".index"}];
    Tuple tuple;
    tuple.push_back(std::make_shared<db_type::Varchar>(64, tp.table_name));
    tuple.push_back(std::make_shared<db_type::Varchar>(64, "keys_idx"));
    Tuples ts = ptr->keys_index->find_pre_key(tuple);
    assert(!ts.data.empty());
    BlockNum root_pos;
    std::memcpy(&root_pos, ts.data[0][3]->en_bytes().data(), 8);
    keys_index = std::make_shared<BpTree>(tp, root_pos);
}


TablePtr Table::table_list_table(const std::string &db_name) {
    // .table_list table attributes:
    //
    // 0. table_name  : Varchar(64)
    // 1. record_root : Int
    // 
    using namespace db_type;
    ColProperty table_name_col("table_name", sdb::en_bytes(static_cast<char>(VARCHAR), Size(64)), true);
    ColProperty record_root("record_root", sdb::en_bytes(static_cast<char>(INT)));
    TableProperty meta_tp(db_name, ".table_list", 0, {table_name_col, record_root});
    return std::make_shared<Table>(meta_tp);
}

TablePtr Table::col_list_table(const std::string &db_name) {
    // .col_list table attributes:
    //
    // 0. table_name     : Varchar(64)
    // 1. col_name       : Varchar(64)
    // 2. type_info      : Varchar(64)
    // 3. order_num      : Int
    // 4. is_key : Char
    // 5. is_not_null    : Char
    // 
    using namespace db_type;
    // setting col property
    TableProperty::ColPropertyList cp_lst;
    ColProperty table_name_cp("table_name", sdb::en_bytes(static_cast<char>(VARCHAR), Size(64)), true);
    cp_lst.push_back(table_name_cp);

    ColProperty col_name_cp("col_name", sdb::en_bytes(static_cast<char>(VARCHAR), Size(64)), true);
    cp_lst.push_back(col_name_cp);

    ColProperty is_key_cp("is_key", sdb::en_bytes(static_cast<char>(CHAR)));
    cp_lst.push_back(is_key_cp);

    ColProperty is_not_null_cp("is_not_null", sdb::en_bytes(static_cast<char>(CHAR)));
    cp_lst.push_back(is_not_null_cp);

    // get table property
    TableProperty col_list_tp(db_name, ".col_list", 1, cp_lst);
    return std::make_shared<Table>(col_list_tp);
}

TablePtr Table::index_table(const std::string &db_name) {
    // .index table attributes:
    //
    // 0. table_name : Varchar(64)
    // 1. index_name : Varchar(64)
    // 2. col_name   : Varchar(64)
    // 3. index_root : Int
    // 
    using namespace db_type;
    // setting col property
    TableProperty::ColPropertyList cp_lst;
    ColProperty table_name_cp("table_name", sdb::en_bytes(static_cast<char>(VARCHAR), Size(64)), true);
    cp_lst.push_back(table_name_cp);

    ColProperty index_name_cp("index_name", sdb::en_bytes(static_cast<char>(VARCHAR), Size(64)), true);
    cp_lst.push_back(index_name_cp);

    ColProperty col_name_cp("col_name", sdb::en_bytes(static_cast<char>(VARCHAR), Size(64)), true);
    cp_lst.push_back(col_name_cp);

    ColProperty index_root_cp("index_root", sdb::en_bytes(static_cast<char>(INT)));
    cp_lst.push_back(index_root_cp);

    // get table property
    TableProperty index_tp(db_name, ".index", 2, cp_lst);
    return std::make_shared<Table>();
}

} // namespace sdb
