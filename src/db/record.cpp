#include <map>

#include "record.h"
#include "util.h"
#include "cache.h"
#include "io.h"
#include "block_alloc.h"

namespace sdb {

Record::Record(TableProperty table_property, BlockNum bn):tp(table_property), block_num(bn), tuples(0) {
    IO &io = IO::get();
    Bytes bytes = io.read_block(tp.db_name+"/block.sdb", bn);
    offset = 0;
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

bool Record::is_overflow() const {
    return offset > BLOCK_SIZE;
}

Record Record::split() {
    // need log
    BlockNum new_bn = BlockAlloc::get().new_block(tp.db_name );
    Record record(tp, new_bn);
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
    Bytes bytes = tuples.en_bytes();
    assert(bytes.size() <= BLOCK_SIZE);
    bytes.resize(BLOCK_SIZE);
    CacheMaster::get_misc_cache().put(path, block_num, bytes);
}

Record Record::create(const TableProperty &table_property) {
    BlockNum new_bn = BlockAlloc::get().new_block(table_property.db_name );
    return Record(table_property, new_bn);
}

} // namespace sdb
