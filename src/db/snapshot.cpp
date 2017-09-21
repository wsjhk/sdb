#include "snapshot.h"

namespace sdb {

// static init
Bytes Snapshot::read_block(BlockNum block_num) {
    auto it = block_map.find(block_num);
    if (it != block_map.end()) {
        return block_cache.get(it->second);
    }
    Bytes bytes = block_cache.get(block_num);
    if (level == TransInfo::READ) {
        return bytes;
    }
    BlockNum new_block_num = block_alloc.new_temp_block();
    block_cache.put(new_block_num, bytes);
    block_map[block_num] = new_block_num;
    return bytes;
}

void Snapshot::write_block(BlockNum block_num, const Bytes &bytes) {
    auto it = block_map.find(block_num);
    BlockNum num;
    if (it == block_map.end()) {
        num = block_alloc.new_temp_block();
        block_map[block_num] = num;
    } else {
        num = it->first;
    }
    block_cache.put(num, bytes);
}

void Snapshot::rollback(){
    for (auto &&[old_num, new_num] : block_map) {
        block_alloc.free_block(new_num);
    }
}

void Snapshot::commit() {
    for (auto &&[old_num, new_num] : block_map) {
        Bytes new_bytes = block_cache.get(new_num);
        block_cache.put(old_num, new_bytes);
    }
}

} // namespace sdb
