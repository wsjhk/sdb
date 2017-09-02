#include "property.h"

namespace sdb {

using namespace sdb::db_type;

// === ColProperty === 
Bytes ColProperty::en_bytes()const {
    return sdb::en_bytes(col_name, col_type, type_size, default_value->en_bytes(), is_not_null);
}

ColProperty ColProperty::de_bytes(const Bytes &bytes, Size &offset){
    // cal name
    std::string col_name;
    sdb::de_bytes(col_name, bytes, offset);
    // type
    TypeTag type;
    sdb::de_bytes(type, bytes, offset);
    // type_size
    size_t type_size;
    sdb::de_bytes(type_size, bytes, offset);
    // default value
    ObjPtr dv = get_default(type, type_size);
    dv->de_bytes(bytes, offset);
    // is not null
    char is_not_null = false;
    sdb::de_bytes(is_not_null, bytes, offset);
    return ColProperty(col_name, type, type_size, dv, is_not_null);
}

// === TableProperty === 
size_t TableProperty::get_col_property_pos(const std::string &col_name)const{
    auto f = [col_name](auto &&cp)->bool{return cp.col_name == col_name;};
    std::find_if(col_property_lst.begin(), col_property_lst.end(), f);
}

std::vector<std::string> TableProperty::get_col_name_lst()const{
    std::vector<std::string> ret;
    for (auto && x : col_property_lst) {
        ret.push_back(x.col_name);
    }
    return ret;
}

} // SDB::Function namespace about
