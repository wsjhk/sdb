#include <type_traits>

#include "type.h"

namespace SDB::Type {
// ========= Varchar ==========
void Varchar::check_size(size_t size)const{
    if (size> max_size) {
        throw std::runtime_error("Error: varchar data > max_size");
    }
}

Varchar Varchar::de_bytes(const Bytes &bytes, Pos &offset){
    size_t s;
    Function::de_bytes(s, bytes, offset);
    std::string str(bytes.data()+offset, bytes.data()+offset+s);
    offset += s;
    return Varchar(str, s);
}


// ========= Value ==========
// operator
// v < v
bool operator<(const Value &v1, const Value &v2) {
    std::string op_type_name = v1.get_generic_type_name()+ "<" + v2.get_generic_type_name();
    if (op_type_name == "Integer<Integer" || op_type_name == "Varchar<Varchar"){
        return Value::value_op(v1, v2, [](auto &&x, auto &&y){return x < y;});
    } else {
        std::string err_msg = (boost::format("Error: %s < %s") 
                % v1.get_generic_type_name() 
                % v2.get_generic_type_name()).str();
        throw std::runtime_error(err_msg);
    }
}
// v == v
bool operator==(const Value &v1, const Value &v2) {
    return !(v1 < v2 || v2 < v1);
}
bool operator<=(const Value &v1, const Value &v2) {
    return v1 < v2 || v1 == v2;
}
// v + v
Value operator+(const Value &v1, const Value &v2) {
    std::string op_type_name = v1.get_generic_type_name()+ "+" + v2.get_generic_type_name();
    if (op_type_name == "Integer+Integer"){
        return Value::value_op(v1, v2, [](auto &&x, auto &&y){return Value(x + y);});
    } else {
        std::string err_msg = (boost::format("Error: type operator: %s + %s") 
                % v1.get_generic_type_name() 
                % v2.get_generic_type_name()).str();
        throw std::runtime_error(err_msg);
    }
}
// v - v
Value operator-(const Value &v1, const Value &v2) {
    std::string op_type_name = v1.get_generic_type_name()+ "-" + v2.get_generic_type_name();
    if (op_type_name == "Integer-Integer"){
        return Value::value_op(v1, v2, [](auto &&x, auto &&y){return Value(x - y);});
    } else {
        std::string err_msg = (boost::format("Error: type operator: %s - %s") 
                % v1.get_generic_type_name() 
                % v2.get_generic_type_name()).str();
        throw std::runtime_error(err_msg);
    }
}
// v * v
Value operator*(const Value &v1, const Value &v2) {
    std::string op_type_name = v1.get_generic_type_name()+ "*" + v2.get_generic_type_name();
    if (op_type_name == "Integer*Integer"){
        return Value::value_op(v1, v2, [](auto &&x, auto &&y){return Value(x * y);});
    } else {
        std::string err_msg = (boost::format("Error: type operator: %s * %s") 
                % v1.get_generic_type_name() 
                % v2.get_generic_type_name()).str();
        throw std::runtime_error(err_msg);
    }
}
// v / v
Value operator/(const Value &v1, const Value &v2) {
    std::string op_type_name = v1.get_generic_type_name()+ "/" + v2.get_generic_type_name();
    if (op_type_name == "Integer/Integer"){
        return Value::value_op(v1, v2, [](auto &&x, auto &&y){return Value(x / y);});
    } else {
        std::string err_msg = (boost::format("Error: type operator: %s / %s") 
                % v1.get_generic_type_name() 
                % v2.get_generic_type_name()).str();
        throw std::runtime_error(err_msg);
    }
}

// === ColProperty === 
Bytes ColProperty::en_bytes()const{
    Bytes bytes;
    Function::bytes_append(bytes, col_name);
    Function::bytes_append(bytes, col_type);
    Function::bytes_append(bytes, type_size);
    Bytes dv_bytes = default_value.en_bytes();
    bytes.insert(bytes.end(), dv_bytes.begin(), dv_bytes.end());
    Function::bytes_append(bytes, is_not_null);
    return bytes;
}

// === TableProperty === 
size_t TableProperty::get_col_property_pos(const std::string &col_name)const{
    for (size_t i = 0; i < col_property_lst.size(); i++) {
        if (col_property_lst[i].col_name == col_name){
            return i;
        }
    }
    throw std::runtime_error("Error: get_col_name_pos");
}
size_t TableProperty::get_type_size(const std::string &col_name)const{
    return col_property_lst[get_col_property_pos(col_name)].type_size;
}
Enum::ColType TableProperty::get_col_type(const std::string &col_name)const{
    return col_property_lst[get_col_property_pos(col_name)].col_type;
}
std::vector<std::string> TableProperty::get_col_name_lst()const{
    std::vector<std::string> ret;
    for (auto && x : col_property_lst) {
        ret.push_back(x.col_name);
    }
    return ret;
}
std::vector<Enum::ColType> TableProperty::get_col_type_lst()const{
    std::vector<Enum::ColType> ret;
    for (auto &&x : col_property_lst) {
        ret.push_back(x.col_type);
    }
    return ret;
}

std::vector<TypeSizePair> TableProperty::get_tsp_lst()const{
    std::vector<TypeSizePair> tsp_lst;
    for (auto &&x : col_property_lst) {
        tsp_lst.push_back(std::make_pair(x.col_type, x.type_size));
    }
    return tsp_lst;
}

// === TupleData === 
size_t TupleData::get_col_name_pos(const std::vector<std::string> &col_name_lst, const std::string &col_name){
    for(size_t i = 0; i < col_name_lst.size(); i++){
        if (col_name_lst[i] == col_name){
            return i;
        }
    }
    throw std::runtime_error((boost::format("Error: have not col_name: %s") % col_name).str());
}

// value
Value TupleData::get_value(size_t pos)const{
    return value_lst[pos];
}
Value TupleData::get_value(const std::vector<std::string> &col_name_lst, const std::string &col_name)const{
    return get_value(get_col_name_pos(col_name_lst, col_name));
}
void TupleData::set_value(size_t pos, const Value &new_value){
    value_lst[pos] = new_value;
}
void TupleData::set_value(size_t pred_pos, size_t op_pos, BVFunc pred, VVFunc op){
    if (pred(value_lst[pred_pos])){
        Value &v = value_lst[op_pos];
        v = op(v);
    }
}
// bytes
Bytes TupleData::en_bytes()const{
    Bytes bytes = Function::en_bytes(value_lst.size());
    for (auto &&v : value_lst) {
        Bytes vb = v.en_bytes();
        bytes.insert(bytes.end(), vb.begin(), vb.end());
    }
    return bytes;
}
TupleData TupleData::de_bytes(const std::vector<TypeSizePair> &tsp, const Bytes &bytes, Pos &offset){
    size_t s;
    Function::de_bytes(s, bytes, offset);
    TupleData tuple_data;
    for (size_t i = 0; i < s; i++) {
        Value v = Value::make(tsp[i].first, tsp[i].second);
        Value::de_bytes(v, bytes, offset);
        tuple_data.value_lst.push_back(v);
    }
    return tuple_data;
}

// === Tuple ===
// value
Value Tuple::get_value(const std::string &col_name)const{
    return data.get_value(TupleData::get_col_name_pos(col_name_lst, col_name));
}

void Tuple::set_value(const std::string &col_name, const Value &new_value){
    data.set_value(TupleData::get_col_name_pos(col_name_lst, col_name), new_value);
}

// === TupleLst ===
Bytes TupleLst::en_bytes()const{
    Bytes bytes = Function::en_bytes(data.size());
    for (auto &&td : data) {
        Bytes tdb = td.en_bytes();
        bytes.insert(bytes.end(), tdb.begin(), tdb.end());
    }
    return bytes;
}

TupleLst de_bytes(
        const std::vector<std::string> &col_name_lst,
        const std::vector<TypeSizePair> &tsp_lst,
        const Bytes &bytes, 
        Pos &offset){
    size_t s;
    Function::de_bytes(s, bytes, offset);
    TupleLst tuple_lst(col_name_lst);
    for (size_t i = 0; i < s; i++) {
        TupleData d = TupleData::de_bytes(tsp_lst, bytes, offset);
        tuple_lst.data.push_back(d);
    }
    return tuple_lst;
}

} // SDB::Type

namespace SDB::Function {

bool is_var_type(Enum::ColType type){
    if (type == Enum::VARCHAR) {
        return true;
    } else {
        return false;
    }
}

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
    for (auto &&tuple : tuple_lst.data) {
    }
}

} // SDB::Function namespace about
