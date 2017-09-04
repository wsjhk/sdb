#include <map>

#include "record.h"
#include "util.h"
#include "cache.h"
#include "io.h"
#include "block_alloc.h"

namespace sdb {

Record::Record(TableProperty table_property, BlockNum bn):tp(table_property), block_num(bn), offset(0), tuples(0) {
    IO &io = IO::get();
    Bytes bytes = io.read_block(tp.db_name+"/block.sdb", bn);
    // read next record num
    sdb::de_bytes(next_record_num, bytes, offset);
    // read tuples
    std::vector<std::pair<db_type::TypeTag, int>> tag_sizes;
    for (auto &&cpl : tp.col_property_lst) {
        tag_sizes.push_back({cpl.col_type , cpl.type_size});
    }
    tuples = Tuples(tag_sizes.size());
    tuples.de_bytes(tag_sizes, bytes, offset);
}

bool Record::is_less()const {
    return offset < BLOCK_SIZE / 2;
}

bool Record::is_full() const {
    return offset > BLOCK_SIZE;
}

Record Record::split() {
    // need log
    BlockNum new_bn = BlockAlloc::get().new_block(tp.db_name );
    Record record(tp, new_bn);
    // set next record num
    record.next_record_num = next_record_num;
    next_record_num = new_bn;

    // move tuples
    size_t len = tuples.data.size() / 2;
    for (size_t i = len; i < tuples.data.size(); i++) {
        offset -= Tuples::tuple_type_size(tuples.data[i]);
        record.push_tuple(std::move(tuples.data[i]));
    }
    tuples.data.erase(tuples.data.begin()+len, tuples.data.end());
    return record;
}

void Record::merge(Record &&record) {
    for (auto && tuple : tuples.data) {
        offset += Tuples::tuple_type_size(tuple);
        record.push_tuple(std::move(tuple));
    }
    next_record_num = record.next_record_num;
}

// tuple
void Record::push_tuple(const Tuples::Tuple &tuple) {
    offset -= Tuples::tuple_type_size(tuple);
    tuples.data.push_back(Tuples::tuple_clone(tuple));
}

void Record::push_tuple(Tuples::Tuple &&tuple) {
    offset -= Tuples::tuple_type_size(tuple);
    tuples.data.push_back(tuple);
}

void Record::sync() const {
    std::string path = tp.db_name + "/block.sdb";
    // next record num
    Bytes bytes = sdb::en_bytes(next_record_num);
    // tuples
    Bytes tuples_bytes = tuples.en_bytes();
    bytes.insert(bytes.end(), tuples_bytes.begin(), tuples_bytes.end());
    assert(bytes.size() <= BLOCK_SIZE);
    bytes.resize(BLOCK_SIZE);
    CacheMaster::get_misc_cache().put(path, block_num, bytes);
}

Record Record::create(const TableProperty &table_property, BlockNum next_record_num) {
    BlockNum new_bn = BlockAlloc::get().new_block(table_property.db_name);
    Record record(table_property, new_bn);
    record.next_record_num = next_record_num;
}

} // namespace sdb
