#include <vector>
#include <fstream>
#include <stdexcept>
#include <string>
#include <map>
#include <boost/filesystem.hpp>
#include <utility>
#include <functional>

#include "bpTree.h"
#include "table.h"
#include "util.h"
#include "io.h"
#include "cache.h"


using std::ios;
using std::vector;
using SDB::Type::Pos;
using SDB::Type::PosList;
using SDB::Type::Bytes;
using SDB::Type::Value;
using SDB::Enum::ColType;

using namespace SDB;

// SQL
void Table::insert(const Type::TupleData &tuple_data) {
    auto bytes = tuple_data.en_bytes();
    BpTree bpTree(property);
    bpTree.insert(tuple_data.get_value(property.get_col_name_lst(), property.key), bytes);
}

void Table::update(const std::string &pred_col_name, SDB::Type::BVFunc predicate,
                   const std::string &op_col_name, SDB::Type::VVFunc op) {
    Record record(property);
    BpTree bpTree(property);
    bool is_var_type = SDB::Function::is_var_type(property.get_col_type(op_col_name));
    if (!is_has_index(pred_col_name) && !is_var_type) {
        record.update(pred_col_name, predicate, op_col_name, op);
        return;
    }
    // get tuple lst
    TupleLst tuple_lst(property.get_col_name_lst());
    if (is_has_index(pred_col_name)) {
        PosList pos_lst = bpTree.find(predicate);
        tuple_lst = record.read_record(pos_lst);
    } else {
        tuple_lst = record.find(pred_col_name, predicate);
    }
    // data update
    for (auto &&tuple_data : tuple_lst.data) {
        Value key = tuple_data.get_value(property.get_col_name_lst(), property.key);
        Type::Pos pred_pos = Type::TupleData::get_col_name_pos(tuple_lst.col_name_lst, pred_col_name);
        Type::Pos op_pos = Type::TupleData::get_col_name_pos(tuple_lst.col_name_lst, op_col_name);
        tuple_data.set_value(pred_pos, op_pos, predicate, op);
        Bytes data = tuple_data.en_bytes();
        bpTree.update(key, data);
    }
}

void Table::remove(const std::string &col_name, const Value &value) {
    BpTree bpTree(property);
    if (is_has_index(col_name)) {
        bpTree.remove(value);
        return;
    }
    TupleLst tuple_lst = find(col_name, value);
    for (auto &&tuple_data : tuple_lst.data) {
        Value key = tuple_data.get_value(property.get_col_name_lst(), property.key);
        bpTree.remove(key);
    }
}

void Table::remove(const std::string &col_name, std::function<bool(Value)> predicate) {
    TupleLst tuple_lst = find(col_name, predicate);
    BpTree bpTree(property);
    for (auto &&td : tuple_lst.data) {
        Value value = td.get_value(property.get_col_name_lst(), property.key);
        bpTree.remove(value);
    }
}

Table::TupleLst Table::find(const std::string &col_name, const SDB::Type::Value &value) {
    Record record(property);
    if (col_name == property.key) {
        BpTree bpTree(property);
        PosList pos_lst = bpTree.find(value);
        return record.read_record(pos_lst);
    }
    auto predicate = SDB::Function::get_bvfunc(SDB::Enum::EQ, value);
    return record.find(col_name, predicate);
}

Record::TupleLst Table::find(const std::string &col_name, std::function<bool(Value)> predicate) {
    Record record(property);
    if (col_name == property.key) {
        PosList pos_lst;
        BpTree bpTree(property);
        pos_lst = bpTree.find(predicate);
//        for (auto &&pos : pos_lst) {
//            std::cout << pos << " ";
//        }
//        std::cout << std::endl;
        return record.read_record(pos_lst);
    }
    return record.find(col_name, predicate);
}

void Table::create_table(const SDB::Type::TableProperty &property) {
    // table meta
    // create dir
    IO::create_dir(property.db_name+"/"+property.table_name);
    write_meta_data(property);

    //index
    Record::create(property);

    //record
    BpTree::create(property);
}

void Table::drop_table() {
    Cache &cache = Cache::make();
    cache.pop_file(get_table_meta_path(property));
    IO::delete_file(get_table_meta_path(property));
    // index
    BpTree::drop(property);
    //record
    Record::drop(property);
    // drop dir
    IO::remove_dir(property.db_name+"/"+property.table_name);
    is_table_drop = true;
}

void Table::add_referenced(const std::string &table_name, const std::string &col_name) {
    property.referenced_map[table_name] = col_name;
}

void Table::add_referencing(const std::string &table_name, const std::string &col_name) {
    property.referencing_map[table_name] = col_name;
}

void Table::remove_referenced(const std::string &table_name) {
    property.referenced_map.erase(table_name);
}

void Table::remove_referencing(const std::string &table_name) {
    property.referencing_map.erase(table_name);
}

bool Table::is_referenced() const {
    return !property.referenced_map.empty();
}

bool Table::is_referencing()const{
    return property.referencing_map.empty();
}

bool Table::is_referencing(const std::string &table_name)const{
    return (property.referencing_map.find(table_name) != property.referencing_map.end());
}

std::unordered_map<std::string, std::string> Table::get_referenced_map()const {
    return property.referenced_map;
}
std::unordered_map<std::string, std::string> Table::get_referencing_map()const {
    return property.referencing_map;
}

std::string Table::get_key()const {
    return property.key;
}

// ========= private ========
void Table::read_meta_data(const std::string &db_name, const std::string &table_name) {
    property.db_name = db_name;
    property.table_name = table_name;
    Bytes bytes = Cache::make().read_file(get_table_meta_path(property));
    size_t offset = 0;
    Function::de_bytes(property.key, bytes, offset);
    size_t col_count;
    Function::de_bytes(col_count, bytes, offset);
    for (size_t i = 0; i < col_count; ++i) {
        Type::ColProperty cp = Type::ColProperty::de_bytes(bytes, offset);
        property.col_property_lst.push_back(cp);
    }
    // referencing_map
    // bytes : <unordered_map> [[col_name][table_name]]*
    Function::de_bytes(property.referencing_map, bytes, offset);
    // referenced_map
    // bytes : <unordered_map> |[table_name][col_name]|*
    Function::de_bytes(property.referenced_map, bytes, offset);
}

void Table::write_meta_data(const SDB::Type::TableProperty &property) {
    // write table property
    Bytes bytes;
    // key
    Function::bytes_append(bytes, property.key);
    // tuple property list
    // bytes : [[col_name][col_type][type_size]]*
    Function::bytes_append(bytes, property.col_property_lst.size());
    for (auto &&item : property.col_property_lst) {
        // col name
        Function::bytes_append(bytes, item.col_name);
        // type
        Function::bytes_append(bytes, item.col_type);
        // type size
        Function::bytes_append(bytes, item.type_size);
        // default value
        Bytes dv_bytes = item.default_value.en_bytes();
        bytes.insert(bytes.end(), dv_bytes.begin(), dv_bytes.end());
        // is not null
        Function::bytes_append(bytes, item.is_not_null);
    }
    // referencing_map
    // bytes : <unordered_map> [[col_name][table_name]]*
    Function::bytes_append(bytes, property.referencing_map);
    // referenced_map
    // bytes : <unordered_map> |[table_name][col_name]|*
    Function::bytes_append(bytes, property.referenced_map);

    // write to meta file
    Cache::make().write_file(get_table_meta_path(property), bytes);
}

bool Table::is_has_index(const std::string &col_name) const {
    return col_name == property.key;
}

// ========== private function ========
std::string Table::get_table_meta_path(const TableProperty &property) {
    return property.db_name + "/" + property.table_name + "/meta.sdb";
}
