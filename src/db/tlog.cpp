#include "tlog.h"
#include "io.h"
#include <fstream>

namespace sdb {

// log bytes: <log length, log id, log type, log content>
//
// begin content    : <>
// commit content   : <>
// rollback content : <>
// update content   : <table_name, new_tuple>
// insert content   : <table_name, tuple>
// remove content   : <table_name, keys>
//

void Tlog::begin(Tid t_id) {
    assert(db_mt.find(db_name) != db_mt.end());

    // log type
    Bytes bytes = sdb::en_bytes(char(BEGIN));
    // log content
    sdb::bytes_append(bytes, t_id);
    write(bytes);
}

void Tlog::commit(Tid t_id) {
    // log type
    Bytes bytes = sdb::en_bytes(char(COMMIT));
    // log content
    sdb::bytes_append(bytes, t_id);
    write(bytes);
}

void Tlog::rollback(Tid t_id) {
    // log type
    Bytes bytes = sdb::en_bytes(char(ROLLBACK));
    // log content
    sdb::bytes_append(bytes, t_id);
    write(bytes);
}

void Tlog::update(Tid t_id, const std::string &table_name, const Tuple &new_tuple) {
    // log type
    Bytes bytes = sdb::en_bytes(char(UPDATE));
    // log content
    sdb::bytes_append(bytes, t_id, table_name, new_tuple);
    write(bytes);
}

void Tlog::insert(Tid t_id, const std::string &table_name, const Tuple &tuple) {
    // log type
    Bytes bytes = sdb::en_bytes(char(INSERT));
    // log content
    sdb::bytes_append(bytes, t_id, table_name, tuple);
    write(bytes);
}

void Tlog::remove(Tid t_id, const std::string &table_name, const Tuple &keys) {
    assert(db_mt.find(db_name) != db_mt.end());
    // log type
    Bytes bytes = sdb::en_bytes(char(REMOVE));
    // log content
    sdb::bytes_append(bytes, t_id, table_name, keys);
    write(bytes);
}

// ========== private =========
void Tlog::write(const Bytes &bytes) {
    // lock
    std::lock_guard<std::mutex> lg(mutex);
    Size log_size = sizeof(Tid) + bytes.size();
    Bytes log_len_btyes = sdb::en_bytes(log_size);
    sdb::bytes_append(log_len_btyes, l_id);
    log_len_btyes.insert(log_len_btyes.end(), bytes.begin(), bytes.end());

    // write
    using std::ios;
    std::ofstream out(IO::block_path(), ios::binary | ios::app);
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
