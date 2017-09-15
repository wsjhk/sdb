#include "record.h"
#include "util.h"
#include "cache.h"
#include "io.h"
#include "block_alloc.h"
#include "snapshot.h"

namespace sdb {

Record::Record(TransInfo t_info, TableProperty table_property, BlockNum bn):t_info(t_info), tp(table_property), block_num(bn) {
    Bytes bytes = t_info.s_ptr->read_block(bn, true);
    // read next record num
    Size offset = 0;
    sdb::de_bytes(next_record_num, bytes, offset);
    // read tuples
    std::vector<db_type::TypeInfo> info_lst;
    for (auto &&cp : tp.col_property_lst) {
        info_lst.push_back(cp.type_info);
    }
    Size len = 0;
    sdb::de_bytes(len, bytes, offset);
    for (Size i = 0; i < len; i++) {
        Vid v_id = 0;
        Tuple tuple;
        sdb::de_bytes(v_id, bytes, offset);
        tuple.de_bytes(info_lst, bytes, offset);
        record_lst.push_back({v_id, tuple});
    }
}

bool Record::is_less()const {
    return get_bytes_size() < BLOCK_SIZE / 2;
}

bool Record::is_full() const {
    return get_bytes_size() > BLOCK_SIZE;
}

Record Record::split() {
    // need log
    BlockNum new_bn = BlockAlloc::get().new_block(tp.db_name );
    Record record(t_info, tp, new_bn);
    // set next record num
    record.next_record_num = next_record_num;
    next_record_num = new_bn;

    // move tuples
    size_t len = record_lst.size() / 2;
    record.record_lst.splice(record.record_lst.begin(), record_lst, std::next(record_lst.begin(), len), record_lst.end());
    return record;
}

void Record::merge(Record &&record) {
    for (auto && pair : record.record_lst) {
        record_lst.push_back(std::move(pair));
    }
    next_record_num = record.next_record_num;
}

void Record::sync() const {
    std::string path = tp.db_name + "/block.sdb";
    // next record num
    Bytes bytes = sdb::en_bytes(next_record_num);
    // record list
    Bytes tuples_bytes = sdb::en_bytes(record_lst);
    bytes.insert(bytes.end(), tuples_bytes.begin(), tuples_bytes.end());
    assert(bytes.size() <= BLOCK_SIZE);
    bytes.resize(BLOCK_SIZE);
    t_info.s_ptr->write_block(block_num, bytes, true);
}

Size Record::get_bytes_size()const {
    Size size = sizeof(next_record_num);
    //  record list size
    size += sizeof(Size);
    for (auto &&[v_id, tuple] : record_lst) {
        size += sizeof(v_id);
        size += tuple.data_bytes_size();
    }
    return size;
}

} // namespace sdb
