#include "db.h"

#include "io.h"
#include "util.h"
#include "table.h"
#include "cache.h"
#include "block_alloc.h"

namespace sdb {

DB::DB(const std::string &db_name):db_name(db_name) {
    add_table_list();
    add_col_list();
    // add_index();
    // add_reference();
    for (auto &&tn : table_name_lst(TransInfo())) {
        TableProperty tp = get_tp(TransInfo(), tn);
        table_map[tn] = std::make_shared<Table>(tp);
    }
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

    table_map[".table_list"] = std::make_shared<Table>(meta_tp);
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
    table_map[".col_list"] = std::make_shared<Table>(col_list_tp);
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

// void DB::add_reference() {
//     // .reference table attributes:
//     //
//     // 0. t1 : Varchar(64)
//     // 1. t2 : Varchar(64)
//     // 
//     using namespace db_type;
//     // setting col property
//     TableProperty::ColPropertyList cp_lst;
//     ColProperty t1("t1", sdb::en_bytes(static_cast<char>(VARCHAR), Size(64)), true);
//     cp_lst.push_back(t1);
// 
//     ColProperty t2("t2", sdb::en_bytes(static_cast<char>(VARCHAR), Size(64)), true);
//     cp_lst.push_back(t2);
// 
//     TableProperty ref_tp(db_name, ".reference", 7, 8, cp_lst);
//     table_map[".reference"] = std::make_shared<Table>(ref_tp);
// }
// 
TableProperty DB::get_tp(TransInfo ti, const std::string &table_name) {
    // get record root and idx root;
    auto tl_ptr = table_map[".table_list"];
    Tuple keys = {std::make_shared<db_type::Varchar>(64, table_name)};
    auto tl_ts = tl_ptr->find(ti, keys);
    BlockNum record_root, keys_idx_root;
    Size offset = 0;
    sdb::de_bytes(record_root, tl_ts.data[0][1]->en_bytes(), offset);
    offset = 0;
    sdb::de_bytes(keys_idx_root, tl_ts.data[0][2]->en_bytes(), offset);

    // get col list
    
    auto cl_ptr = table_map[".col_list"];
    auto cl_ts  = cl_ptr->find(ti, keys);
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
    return TableProperty(db_name, table_name, record_root, keys_idx_root, col_lst);
}

std::vector<std::string> DB::table_name_lst(TransInfo t_info) {
    auto tl_ptr = table_map[".table_list"];
    Tuple keys = {std::make_shared<db_type::Varchar>(3, ".z")};
    auto ts = tl_ptr->find_less(t_info, keys, false);

    std::vector<std::string> nl;
    for (auto && tuple : ts.data) {
        std::string name;
        Size offset = 0;
        sdb::de_bytes(name, tuple[0]->en_bytes(), offset);
        nl.push_back(name);
    }
    return nl;
}


void DB::create_table(TransInfo t_info, const TableProperty &tp) {
    // insert table map
    auto &[lock_stat, ptr] = t_snapshot[t_info][tp.table_name];
    if (lock_stat == 0) {
        lock_stat = -2;
        mutex_map[tp.table_name].lock();
        lock_stat = 2;
        ptr = table_map[tp.table_name];
    } else {
        throw TableExisted(tp.table_name);
    }

    if (ptr != nullptr) {
        throw TableExisted(tp.table_name);
    }

    // table name
    auto table_name_ptr = std::make_shared<db_type::Varchar>(64, tp.table_name);

    // col list
    auto cl_ptr = table_map[".col_list"];
    int8_t order_num = 0;
    for (auto &&cl : tp.col_property_lst) {
        Tuple cl_tuple;
        cl_tuple.push_back(table_name_ptr->clone());
        // col name
        cl_tuple.push_back(std::make_shared<db_type::Varchar>(64, cl.col_name));
        // type info
        cl_tuple.push_back(std::make_shared<db_type::Varchar>(64, cl.type_info));
        // order num
        cl_tuple.push_back(std::make_shared<db_type::Char>(order_num));
        order_num++;
        // is key
        cl_tuple.push_back(std::make_shared<db_type::Char>(cl.is_key));
        // is not null
        cl_tuple.push_back(std::make_shared<db_type::Char>(cl.is_not_null));
        cl_ptr->insert(t_info, cl_tuple);
    }

    // table list
    Tuple tl_tuple;
    tl_tuple.push_back(table_name_ptr->clone());
    tl_tuple.push_back(std::make_shared<db_type::BigInt>(tp.record_root));
    tl_tuple.push_back(std::make_shared<db_type::BigInt>(tp.keys_idx_root));
    auto &tl_ptr = table_map[".table_list"];
    tl_ptr->insert(t_info, tl_tuple);
}

void DB::drop_table(TransInfo t_info, const std::string &table_name) {
    auto &[lock_stat, ptr] = t_snapshot[t_info][table_name];
    if (lock_stat == 0) {
        lock_stat = -2;
        mutex_map[table_name].lock();
        lock_stat = 2;
        ptr = table_map[table_name];
    } else if (lock_stat == 1) {
        lock_stat = -3;
        mutex_map[table_name].lock_upgrade();
        lock_stat = 2;
        ptr = table_map[table_name];
    }

    if (ptr == nullptr) {
        throw TableNotFound("table not found");
    }

    // col list
    auto clt_ptr = table_map[".col_list"];
    Tuple table_name_key = {std::make_shared<db_type::Varchar>(64, table_name)};
    clt_ptr->remove(t_info, table_name_key);

    // table list
    table_map[".table_list"]->remove(t_info, table_name_key);
}

} // namespace sdb
