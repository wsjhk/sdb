#include "tlog.h"
#include <fstream>

namespace sdb {

// log bytes: <log id, log type, log content>
// log type[char]:
//      0: begin 
//      1: commit
//      2: rollback
//      3: update
//      4: insert
//      5: remove
//      6: redo-only_update
//      7: redo-only_insert
//      8: redo-only_remove
//
// update content: <Tid, table_name, col_name, old_val, new_val>
// insert content: <Tid, table_name, tuple>
// remove content: <Tid, table_name, keys>
//
// redo_only update content: <Tid, prev_l_id, table_name, col_name, old_val>
// redo_only insert content: <Tid, prev_l_id, table_name, keys>
// redo_only remove content: <Tid, prev_l_id, table_name, tuple>

void Tlog::begin(Tid t_id, const std::string &db_name) {
    assert(db_mt.find(db_name) != db_mt.end());

    // log type
    Bytes bytes = sdb::en_bytes(char(0));
    // log content
    sdb::bytes_append(bytes, t_id);
    write(db_name, bytes);
}

void Tlog::commit(Tid t_id, const std::string &db_name) {
    assert(db_mt.find(db_name) != db_mt.end());

    // log type
    Bytes bytes = sdb::en_bytes(char(1));
    // log content
    sdb::bytes_append(bytes, t_id);
    write(db_name, bytes);
}

void Tlog::rollback(Tid t_id, const std::string &db_name) {
    assert(db_mt.find(db_name) != db_mt.end());

    // log type
    Bytes bytes = sdb::en_bytes(char(2));
    // log content
    sdb::bytes_append(bytes, t_id);
    write(db_name, bytes);
}

void Tlog::update(Tid t_id, const TableProperty &tp, const std::string &col_name, db_type::ObjCntPtr old_val, db_type::ObjCntPtr new_val) {
    assert(db_mt.find(db_name) != db_mt.end());

    // log type
    Bytes bytes = sdb::en_bytes(char(3));
    // log content
    sdb::bytes_append(bytes, t_id, tp.table_name, col_name, old_val, new_val);
    write(tp.db_name, bytes);
}

void Tlog::insert(Tid t_id, const TableProperty &tp, const Tuple &data) {
    assert(db_mt.find(db_name) != db_mt.end());
    // log type
    Bytes bytes = sdb::en_bytes(char(4));
    // log content
    sdb::bytes_append(bytes, t_id, tp.table_name, data);
    write(tp.db_name, bytes);
}

void Tlog::remove(Tid t_id, const TableProperty &tp, const Tuple &keys) {
    assert(db_mt.find(db_name) != db_mt.end());
    // log type
    Bytes bytes = sdb::en_bytes(char(5));
    // log content
    sdb::bytes_append(bytes, t_id, tp.table_name,keys);
    write(tp.db_name, bytes);
}

void Tlog::redo_only_update(Tid t_id, Lid l_id, const TableProperty &tp, const std::string &col_name, db_type::ObjCntPtr old_val, db_type::ObjCntPtr new_val) {
    assert(db_mt.find(db_name) != db_mt.end());

    // log type
    Bytes bytes = sdb::en_bytes(char(6));
    // log content
    sdb::bytes_append(bytes, l_id, t_id, tp.table_name, col_name, old_val, new_val);
    write(tp.db_name, bytes);
}

void Tlog::redo_only_insert(Tid t_id, Lid l_id, const TableProperty &tp, const Tuple &data) {
    assert(db_mt.find(db_name) != db_mt.end());
    // log type
    Bytes bytes = sdb::en_bytes(char(7));
    // log content
    sdb::bytes_append(bytes, l_id, t_id, tp.table_name, data);
    write(tp.db_name, bytes);
}

void Tlog::redo_only_remove(Tid t_id, Lid l_id, const TableProperty &tp, const Tuple &keys) {
    assert(db_mt.find(db_name) != db_mt.end());
    // log type
    Bytes bytes = sdb::en_bytes(char(8));
    // log content
    sdb::bytes_append(bytes, l_id, t_id, tp.table_name,keys);
    write(tp.db_name, bytes);
}

// ========== private =========
void Tlog::write(const std::string &db_name, const Bytes &bytes) {
    // lock
    auto &[mt, l_id] = db_mt[db_name];
    std::lock_guard<std::mutex> lg(mt);
    Bytes t_id_bytes = sdb::en_bytes(l_id);
    t_id_bytes.insert(t_id_bytes.end(), bytes.begin(), bytes.end());

    // write
    using std::ios;
    std::ofstream out(db_name + "/log.sdb", ios::binary | ios::app);
    out.write(bytes.data(), bytes.size());
    out.close();

    // update l_id
    l_id++;
}

} // namespace sdb
