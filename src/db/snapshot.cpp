#include "snapshot.h"

namespace sdb {

// static init
static std::map<BlockNum, std::mutex> block_lock_map;

// ========== public function =========
Bytes Snapshot::read_block(BlockNum block_num) {
    auto it = block_map.find(block_num);
    if (it != block_map.end()) {
        return block_cache.get(it->second.first);
    }
    Bytes bytes = block_cache.get(block_num);
    if (level == TransInfo::READ) {
        return bytes;
    }
    BlockNum new_block_num = block_alloc.new_temp_block();
    block_cache.put(new_block_num, bytes);
    block_map[block_num].first = new_block_num;
    return bytes;
}

void Snapshot::write_block(BlockNum block_num, const Bytes &bytes) {
    if (level == TransInfo::READ) {
        block_lock_map[block_num].lock();
    }
    auto it = block_map.find(block_num);
    BlockNum num;
    if (it == block_map.end()) {
        num = block_alloc.new_temp_block();
        block_map[block_num].first = num;
    } else {
        num = it->first;
    }
    block_cache.put(num, bytes);
}

void Snapshot::rollback(){
    for (auto &&[old_num, pair] : block_map) {
        if (level == TransInfo::READ) {
            block_lock_map[old_num].unlock();
        }
        block_alloc.free_block(pair.first);
    }
}

void Snapshot::commit() {
    for (auto &&[old_num, pair] : block_map) {
        if (level == TransInfo::READ) {
            Bytes new_bytes = block_cache.get(pair.first);
            block_cache.put(old_num, new_bytes);
            block_lock_map[old_num].unlock();
        } else {
            // TODO compare and sync record
        }
    }
}

} // namespace sdb
