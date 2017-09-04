#include <memory>
#include <list>
#include <stdexcept>
#include <iostream>
#include <string>
#include <boost/variant.hpp>

#include "util.h"
#include "record.h"
#include "bpTree.h"
#include "cache.h"
#include "io.h"
#include "block_alloc.h"

namespace sdb {

using db_type::ObjPtr;
using db_type::ObjCntPtr;

// --------------- Function ---------------
// ========== BptNode Function =========
// node block bytes:
//     |is_leaf right_node_pos key_lst pos_lst|
BptNode BptNode::get(const TableProperty &tp, BlockNum pos) {
    std::string path = IO::get_block_path(tp.db_name);
    Bytes bytes = CacheMaster::get_block_cache().get(path, pos);
    BptNode node(tp);
    sdb::de_bytes(node.is_leaf, bytes, node.bytes_size);
    sdb::de_bytes(node.right_node_pos, bytes, node.bytes_size);
    node.file_pos = pos;
    Size size = 0;
    sdb::de_bytes(size, bytes, node.bytes_size);
    assert(size >= 0 && size <= BLOCK_SIZE);
    ColProperty cp = tp.get_key_property();
    for (int i = 0; i < size; i++) {
        ObjPtr key = db_type::get_default(cp.col_type, cp.type_size);
        key->de_bytes(bytes, node.bytes_size);
        node.key_lst.push_back(key);
    }
    sdb::de_bytes(node.pos_lst, bytes, node.bytes_size);
    return node;
}

bool BptNode::is_full()const {
    return bytes_size > BLOCK_SIZE;
}

void BptNode::sync() const{
    // node block bytes:
    //     |is_leaf right_node_pos key_lst pos_lst|
    assert(!is_full());
    assert(file_pos != -1);
    Bytes bytes = sdb::en_bytes(is_leaf, right_node_pos);
    bytes_append(bytes, key_lst);
    bytes_append(bytes, pos_lst);
    bytes.resize(BLOCK_SIZE);
    auto path = IO::get_block_path(tp.db_name);
    CacheMaster::get_block_cache().put(path, file_pos, bytes);
}

// split, return right node pos
BlockNum BptNode::split() {
    // alloc block
    BlockNum new_pos = BlockAlloc::get().new_block(tp.db_name);
    BptNode new_node(tp);
    new_node.is_leaf = is_leaf;

    // reset right node pos
    new_node.right_node_pos = right_node_pos;
    right_node_pos = new_pos;
    new_node.file_pos = new_pos;

    // split key list
    // e.g.: key_list: | 1 2 3 4 5 |
    //              => | 1 2 | and | 4 5 |
    //
    // e.g.: pos_list |p1, p2, p3, p4, p5, p6|
    //             => |p1, p2, p3| and |p4, p5, p6|
    //
    Size mid = key_lst.size() / 2;
    // assert(key_lst.size() > 3);
    new_node.key_lst = key_lst;
    // new_node.key_list => |1, 2, 3, 4, 5| => |4, 5|
    new_node.key_lst.erase(new_node.key_lst.begin(), std::next(new_node.key_lst.begin(), mid+1));
    // key_list => |1, 2, 3, 4, 5| => |1, 2|
    key_lst.erase(std::next(key_lst.begin(), mid), key_lst.end());

    // split pos list
    new_node.pos_lst = pos_lst;
    // new_node.pos_list: |p1, p2, p3, p4, p5, p6| => |p4, p5, p6|
    new_node.pos_lst.erase(new_node.pos_lst.begin(), std::next(new_node.pos_lst.begin(), mid+1));
    // pos_list: |p1, p2, p3, p4, p5, p6| => |p1, p2, p3|
    pos_lst.erase(std::next(pos_lst.begin(), mid+1), pos_lst.end());
    // sync
    new_node.reset_bytes_size();
    new_node.sync();
    reset_bytes_size();
    sync();
    return new_pos;
}

// ========== BpTree Function =========
std::string BpTree::index_path()const {
    return table_property.db_name + "/block.sdb";
}

std::vector<BlockNum> BpTree::search_path(ObjCntPtr key)const {
    std::vector<BlockNum> lst;
    BptNode node = BptNode::get(table_property, table_property.key_index_root);
    while (true) {
        lst.push_back(node.file_pos);
    }
    // TODO
}

} // namespace sdb
