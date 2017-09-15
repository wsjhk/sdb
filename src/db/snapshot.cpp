#include "snapshot.h"

namespace sdb {

// static init
Bytes Snapshot::read_block(BlockNum block_num, bool is_record) {
    auto it = block_map.find(block_num);
    if (it != block_map.end()) {
        return block_cache.get(block_path(), it->second.second);
    }
    Bytes bytes = block_cache.get(block_path(), block_num);
    if (t_info.level == TransInfo::READ) {
        return bytes;
    }
    BlockNum new_block_num = block_alloc.new_temp_block(block_path());
    block_cache.put(block_path(), new_block_num, bytes);
    block_map[block_num] = {is_record, new_block_num};
    return bytes;
}

void Snapshot::write_block(BlockNum block_num, const Bytes &bytes, bool is_record) {
    auto it = block_map.find(block_num);
    BlockNum num;
    if (it == block_map.end()) {
        num = block_alloc.new_temp_block(block_path());
        block_map[block_num] = {is_record, num};
    } else {
        num = it->second.first;
    }
    block_cache.put(block_path(), num, bytes);
}

void Snapshot::commit() {
    for (auto &&[old_num, pair] : block_map) {
        auto [is_record, new_num] = pair;
        if (is_record) {
            // Record record();
            // sync record
        } else {
            // sync index
        }
    }
}

} // namespace sdb
