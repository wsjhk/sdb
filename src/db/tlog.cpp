#include "tlog.h"
#include <fstream>

namespace sdb {

// log bytes: <log length, log id, log type, log content>
//
// begin content    : <>
// commit content   : <>
// rollback content : <>
// update content   : <table_name, col_name, old_val, new_val>
// insert content   : <table_name, tuple>
// remove content   : <table_name, keys>
//

void Tlog::begin(Tid t_id, const std::string &db_name) {
    assert(db_mt.find(db_name) != db_mt.end());

    // log type
    Bytes bytes = sdb::en_bytes(char(BEGIN));
    // log content
    sdb::bytes_append(bytes, t_id);
    write(db_name, bytes);
}

void Tlog::commit(Tid t_id, const std::string &db_name) {
    assert(db_mt.find(db_name) != db_mt.end());

    // log type
    Bytes bytes = sdb::en_bytes(char(COMMIT));
    // log content
    sdb::bytes_append(bytes, t_id);
    write(db_name, bytes);
}

void Tlog::rollback(Tid t_id, const std::string &db_name) {
    assert(db_mt.find(db_name) != db_mt.end());

    // log type
    Bytes bytes = sdb::en_bytes(char(ROLLBACK));
    // log content
    sdb::bytes_append(bytes, t_id);
    write(db_name, bytes);
}

void Tlog::update(Tid t_id, const TableProperty &tp, const std::string &col_name, db_type::ObjCntPtr old_val, db_type::ObjCntPtr new_val) {
    assert(db_mt.find(db_name) != db_mt.end());

    // log type
    Bytes bytes = sdb::en_bytes(char(UPDATE));
    // log content
    sdb::bytes_append(bytes, t_id, tp.table_name, col_name, old_val, new_val);
    write(tp.db_name, bytes);
}

void Tlog::insert(Tid t_id, const TableProperty &tp, const Tuple &data) {
    assert(db_mt.find(db_name) != db_mt.end());
    // log type
    Bytes bytes = sdb::en_bytes(char(INSERT));
    // log content
    sdb::bytes_append(bytes, t_id, tp.table_name, data);
    write(tp.db_name, bytes);
}

void Tlog::remove(Tid t_id, const TableProperty &tp, const Tuple &keys) {
    assert(db_mt.find(db_name) != db_mt.end());
    // log type
    Bytes bytes = sdb::en_bytes(char(REMOVE));
    // log content
    sdb::bytes_append(bytes, t_id, tp.table_name,keys);
    write(tp.db_name, bytes);
}

// ========== private =========
void Tlog::write(const std::string &db_name, const Bytes &bytes) {
    // lock
    auto &[mt, l_id] = db_mt[db_name];
    std::lock_guard<std::mutex> lg(mt);
    Size log_size = sizeof(Tid) + bytes.size();
    Bytes log_len_btyes = sdb::en_bytes(log_size);
    sdb::bytes_append(log_len_btyes, l_id);
    log_len_btyes.insert(log_len_btyes.end(), bytes.begin(), bytes.end());

    // write
    using std::ios;
    std::ofstream out(db_name + "/log.sdb", ios::binary | ios::app);
    out.write(log_len_btyes.data(), log_len_btyes.size());
    out.close();

    // update l_id
    l_id++;
}

std::tuple<Tid, Tlog::LogType, Bytes>
Tlog::get_log_info(std::ifstream &in) {
        // read log len
        Bytes len_bytes(sizeof(Size));
        in.read(len_bytes.data(), sizeof(Size));
        Size len;
        std::memcpy(&len, len_bytes.data(), len_bytes.size());
        // read data
        Bytes data(len);
        Size offset = 0;
        in.read(data.data(), len);
        // log id
        Tid t_id;
        sdb::de_bytes(t_id, data, offset);
        // log type
        LogType log_type;
        sdb::de_bytes(log_type, data, offset);
        return {t_id, log_type, data};
}

} // namespace sdb
