#include <memory>
#include <list>
#include <stdexcept>
#include <iostream>
#include <string>

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
// ========== BpTree Function =========
std::string BpTree::index_path()const {
    return tp.db_name + "/block.sdb";
}

std::vector<BlockNum> BpTree::search_path(Tuple key)const {
    std::vector<BlockNum> lst;
    BptNode node = BptNode::get(tp, tp.keys_index_root);
    while (true) {
        lst.push_back(node.file_pos);
        auto [key_it, pos_it] = node.search_less_or_eq_key(key);
        if (pos_it == std::prev(node.pos_lst.end()) || node.right_node_pos != -1) {
            node = BptNode::get(tp, node.right_node_pos);
            continue;
        } else if (node.is_leaf) {
            lst.push_back(*pos_it);
            return lst;
        }
        node = BptNode::get(tp, *pos_it);
    }
}

void BpTree::insert(const Tuple &key, const Tuple &data) {
    auto lst = search_path(key);
    BlockNum record_pos = lst.back();
    Record record(tp, record_pos);
    auto res = record.insert(key, data);
    if (!res.has_value()) return;

    BlockNum insert_pos = res.value();
    lst.pop_back();
    BptNode node = BptNode::get(tp, lst.back());
    while (true) {
        auto &&[key_it, pos_it] = node.search_less_or_eq_key(key);
        node.key_lst.insert(std::next(key_it), key);
        node.pos_lst.insert(std::next(pos_it), insert_pos);
        if (node.is_full()) {
            auto [insert_pos, min_key] = node.split();
            if (node.file_pos == tp.keys_index_root) {
                BptNode new_node = BptNode::new_node(tp);
                new_node.is_leaf = false;
                new_node.key_lst.push_back(min_key);
                new_node.pos_lst.push_back(node.file_pos);
                new_node.pos_lst.push_back(insert_pos);
                new_node.sync();
                // TODO 
                // lock global B+tree and reset root pos;
            }
            lst.pop_back();
            node = BptNode::get(tp, lst.back());
        }
    }
}

// ========== BptNode Function =========
// node block bytes:
//     |is_leaf right_node_pos key_lst pos_lst|
BpTree::BptNode BpTree::BptNode::get(const TableProperty &tp, BlockNum pos) {
    std::string path = IO::get_block_path(tp.db_name);
    Bytes bytes = CacheMaster::get_block_cache().get(path, pos);
    BptNode node(tp);
    Size offset = 0;
    sdb::de_bytes(node.is_leaf, bytes, offset);
    sdb::de_bytes(node.right_node_pos, bytes, offset);
    node.file_pos = pos;
    Size size = 0;
    sdb::de_bytes(size, bytes, offset);
    assert(size >= 0 && size <= BLOCK_SIZE);
    auto cps = tp.get_keys_property();
    for (int i = 0; i < size; i++) {
        Tuple keys;
        if (node.is_single_key()) {
            // single-key
            ObjPtr key = db_type::get_default(cps[0].col_type, cps[0].type_size);
            key->de_bytes(bytes, offset);
            keys.push_back(key);
            node.key_lst.push_back(keys);
        } else {
            // multi-key
            std::vector<std::pair<db_type::TypeTag, Size>> tag_sizes;
            for (auto &&cp : cps) {
                tag_sizes.push_back({cp.col_type, cp.type_size});
            }
            keys.de_bytes(tag_sizes, bytes, offset);
        }
    }
    sdb::de_bytes(node.pos_lst, bytes, offset);
    return node;
}

BpTree::BptNode BpTree::BptNode::new_node(const TableProperty &tp) {
    BptNode node(tp);
    node.file_pos = BlockAlloc::get().new_block(tp.db_name);
    return node;
}

bool BpTree::BptNode::is_full()const {
    return get_bytes_size() > BLOCK_SIZE;
}

Size BpTree::BptNode::get_bytes_size()const {
    Size bytes_size = 0;
    bytes_size += sizeof(is_leaf);
    bytes_size += sizeof(right_node_pos);

    // key list
    for (auto &&key : key_lst) {
        key.range([&bytes_size](auto ptr){bytes_size+= ptr->get_size();});
    }

    // pos list
    bytes_size += sizeof(BlockNum) * pos_lst.size();
    return bytes_size;
}

using KeyListIt = std::list<Tuple>::const_iterator;
using PosListIt = std::list<BlockNum>::const_iterator;
std::pair<KeyListIt, PosListIt>
BpTree::BptNode::search_less_or_eq_key(Tuple key) {
    // e.g.: key_list: | 1 3 5 7 9 |
    //       pos_list |p1, p2, p3, p4, p5, p6|
    // key: 5 => return <5, p4>
    // key: 4 => return <3, p3>
    //
    auto key_it = key_lst.cbegin();
    auto pos_it = pos_lst.cbegin();
    for (; key_it != key_lst.cend(); key_it++) {
        if (key.less(*key_it)) {
            return {std::prev(key_it), pos_it};
        } else if (key.eq(*key_it)) {
            return {key_it, std::next(pos_it)};
        }
        pos_it++;
    }
    return {key_it, pos_it};
}

void BpTree::BptNode::sync() {
    assert(!is_full());
    assert(file_pos != -1);
    // reset_bytes_size();

    // node block bytes:
    //     |is_leaf right_node_pos key_lst pos_lst|
    Bytes bytes = sdb::en_bytes(is_leaf, right_node_pos);
    if (is_single_key()) {
        for (auto &&key : key_lst) {
            bytes_append(bytes, key[0]);
        }
    } else {
        bytes_append(bytes, key_lst);
    }
    bytes_append(bytes, pos_lst);
    bytes.resize(BLOCK_SIZE);
    auto path = IO::get_block_path(tp.db_name);
    CacheMaster::get_block_cache().put(path, file_pos, bytes);
}

// split, return right node pos and min key
std::pair<BlockNum, Tuple> BpTree::BptNode::split() {
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
    //              => | 1 2 3| and | 4 5 |
    //
    // e.g.: pos_list |p1, p2, p3, p4, p5, p6|
    //             => |p1, p2, p3, p4| and |p4, p5, p6|
    //
    Size mid = key_lst.size() / 2;
    assert(key_lst.size() >= 2);

    // key list
    new_node.key_lst.splice(new_node.key_lst.begin(), key_lst, std::next(key_lst.begin(), mid), key_lst.end());
    key_lst.pop_back();

    // pos list
    new_node.pos_lst.splice(new_node.pos_lst.begin(), pos_lst, std::next(pos_lst.begin(), mid), pos_lst.end());
    pos_lst.push_back(*new_node.pos_lst.begin());

    // sync new node
    new_node.sync();

    // sync current node
    sync();

    return {new_pos, new_node.key_lst.front()};
}

} // namespace sdb
