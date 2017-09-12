#include "db.h"

#include "io.h"
#include "util.h"
#include "table.h"
#include "cache.h"

namespace sdb {

// static member
std::map<std::string, DB::DBPtr> db_map = {};

DB::DB(const std::string &db_name):db_name(db_name) {
    add_table_list();
    add_col_list();
    // add_index();
    add_reference();
}

// ========== Public =======
void DB::create_db(const std::string &db_name) {
    IO &io = IO::get();
    io.create_dir(db_name);
    io.create_file(db_name + "/block.sdb");
    io.create_file(db_name + "/log.sdb");
}

void DB::drop_db(const std::string &db_name){
    IO &io = IO::get();
    io.remove_dir_force(db_name);
    io.delete_file(db_name + "/block.sdb");
    io.delete_file(db_name + "/log.sdb");
}

// ========== private =======
void DB::add_table_list() {
    // .table_list table attributes:
    //
    // 0. table_name  : Varchar(64)
    // 1. record_root : BigInt
    // 1. keys_index_root  : BigInt
    // 

    using namespace db_type;
    ColProperty table_name_col("table_name", sdb::en_bytes(static_cast<char>(VARCHAR), Size(64)), 0, true);
    ColProperty rr_cp("record_root", sdb::en_bytes(static_cast<char>(INT)), 1);
    ColProperty ki_cp("keys_idx_root", sdb::en_bytes(static_cast<char>(INT)), 2);
    TableProperty meta_tp(db_name, ".table_list", 0, 1, {table_name_col, rr_cp});

    db_map[".table_list"].second = std::make_shared<Table>(meta_tp);
}

void DB::add_col_list() {
    // .col_list table attributes:
    //
    // 0. table_name  : Varchar(64)
    // 1. col_name    : Varchar(64)
    // 2. type_info   : Varchar(64)
    // 3. order_num   : Char
    // 4. is_key      : Char
    // 5. is_not_null : Char
    // 
    using namespace db_type;
    // setting col property
    TableProperty::ColPropertyList cp_lst;
    ColProperty table_name_cp("table_name", sdb::en_bytes(static_cast<char>(VARCHAR), Size(64)), 0, true);
    cp_lst.push_back(table_name_cp);

    ColProperty col_name_cp("col_name", sdb::en_bytes(static_cast<char>(VARCHAR), Size(64)), 1, true);
    cp_lst.push_back(col_name_cp);

    ColProperty type_info("type_info", sdb::en_bytes(static_cast<char>(VARCHAR), Size(64)), 2);
    cp_lst.push_back(type_info);

    ColProperty order_num_cp("order_num", sdb::en_bytes(static_cast<char>(CHAR)), 3);
    cp_lst.push_back(order_num_cp);

    ColProperty is_key_cp("is_key", sdb::en_bytes(static_cast<char>(CHAR)), 4);
    cp_lst.push_back(is_key_cp);

    ColProperty is_not_null_cp("is_not_null", sdb::en_bytes(static_cast<char>(CHAR)), 5);
    cp_lst.push_back(is_not_null_cp);

    // get table property
    TableProperty col_list_tp(db_name, ".col_list", 2, 3, cp_lst);
    db_map[".col_list"].second = std::make_shared<Table>(col_list_tp);
}

// void DB::add_index() {
//     // .index table attributes:
//     //
//     // 0. table_name : Varchar(64)
//     // 1. index_name : Varchar(64)
//     // 2. col_name   : Varchar(64)
//     // 3. index_root : Int
//     // 
//     using namespace db_type;
//     // setting col property
//     TableProperty::ColPropertyList cp_lst;
//     ColProperty table_name_cp("table_name", sdb::en_bytes(static_cast<char>(VARCHAR), Size(64)), true);
//     cp_lst.push_back(table_name_cp);
// 
//     ColProperty index_name_cp("index_name", sdb::en_bytes(static_cast<char>(VARCHAR), Size(64)), true);
//     cp_lst.push_back(index_name_cp);
// 
//     ColProperty col_name_cp("col_name", sdb::en_bytes(static_cast<char>(VARCHAR), Size(64)), true);
//     cp_lst.push_back(col_name_cp);
// 
//     ColProperty index_root_cp("index_root", sdb::en_bytes(static_cast<char>(INT)));
//     cp_lst.push_back(index_root_cp);
// 
//     // get table property
//     TableProperty index_tp(db_name, ".index", 4, 5, cp_lst);
//     db_map[".index"].second = std::make_shared<Table>(index_tp);
// }

void DB::add_reference() {
    // .reference table attributes:
    //
    // 0. t1 : Varchar(64)
    // 1. t2 : Varchar(64)
    // 
    using namespace db_type;
    // setting col property
    TableProperty::ColPropertyList cp_lst;
    ColProperty t1("t1", sdb::en_bytes(static_cast<char>(VARCHAR), Size(64)), true);
    cp_lst.push_back(t1);

    ColProperty t2("t2", sdb::en_bytes(static_cast<char>(VARCHAR), Size(64)), true);
    cp_lst.push_back(t2);

    TableProperty ref_tp(db_name, ".reference", 7, 8, cp_lst);
    db_map[".reference"].second = std::make_shared<Table>(ref_tp);
}

TableProperty DB::get_tp(Tid tid, const std::string &table_name) {
    // get record root and idx root;
    db_map[".table_list"].first.lock_shared();
    auto tl_ptr = db_map[".table_list"].second;
    Tuple keys = {std::make_shared<db_type::Varchar>(64, table_name)};
    auto tl_ts = tl_ptr->find(tid, keys);
    BlockNum record_root, keys_idx_root;
    Size offset = 0;
    sdb::de_bytes(record_root, tl_ts.data[0][1]->en_bytes(), offset);
    offset = 0;
    sdb::de_bytes(keys_idx_root, tl_ts.data[0][2]->en_bytes(), offset);
    db_map[".table_list"].first.unlock_shared();

    // get col list
    db_map[".col_list"].first.lock_shared();
    auto cl_ptr = db_map[".col_list"].second;
    auto cl_ts  = cl_ptr->find(tid, keys);
    TableProperty::ColPropertyList col_lst;
    for (auto &&tuple : cl_ts.data) {
        std::string col_name;
        sdb::de_bytes(col_name, tuple[1]->en_bytes(), (offset = 0));
        Bytes type_info;
        sdb::de_bytes(type_info, tuple[2]->en_bytes(), (offset = 0));
        int8_t order_num;
        sdb::de_bytes(order_num, tuple[3]->en_bytes(), (offset = 0));
        bool is_key, is_not_null;
        sdb::de_bytes(is_key, tuple[4]->en_bytes(), (offset = 0));
        sdb::de_bytes(is_not_null, tuple[5]->en_bytes(), (offset = 0));
        ColProperty cp(col_name, type_info, is_key, is_not_null);
        col_lst.push_back(cp);
    }
    db_map[".col_list"].first.unlock_shared();
    return TableProperty(db_name, table_name, record_root, keys_idx_root, col_lst);
}

std::vector<std::string> DB::table_name_lst(Tid tid) const {
    db_map[".table_list"].first.lock_shared();
    auto tl_ptr = db_map[".table_list"].second;
    Tuple keys = {std::make_shared<db_type::Varchar>(3, ".z")};
    auto ts = tl_ptr->find_less(tid, keys, false);
    db_map[".table_list"].first.unlock_shared();
    std::vector<std::string> nl;
    for (auto && tuple : ts.data) {
        std::string name;
        Size offset = 0;
        sdb::de_bytes(name, tuple[0]->en_bytes(), offset);
        nl.push_back(name);
    }
    return nl;
}

} // namespace sdb
