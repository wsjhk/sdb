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
void BpTree::insert(const Tuple &key, const Tuple &data) {
    auto lst = search_path(key);
    BlockNum record_pos = lst.back();
    Record record(tp, record_pos);
    auto res = record.insert(key, data);
    if (!res.has_value()) return;

    bubble_split(std::move(lst), key, res.value());
}

// remove record only, 
// we don't delete record block though block is empty,
// so B+Tree node don't need merge
void BpTree::remove(const Tuple &key) {
    auto lst = search_path(key);
    auto record_pos = lst.back();
    Record record(tp, record_pos);
    record.remove(key);
}

void BpTree::update(const Tuple &key, const Tuple &data) {
    auto lst = search_path(key);
    auto record_pos = lst.back();
    lst.pop_back();
    Record record(tp, record_pos);
    std::optional<BlockNum> res = record.update(key, data);
    if (!res.has_value()) return;

    bubble_split(std::move(lst), key, res.value());
}

Tuples BpTree::find_key(const Tuple &key)const {
    auto lst = search_path(key);
    Record record(tp, lst.back());
    return record.find_key(key);
}

// Tuples BpTree::find_pre_key(const Tuple &key)const {
// }

Tuples BpTree::find_less(const Tuple &key, bool is_close)const {
    Tuples ts(tp.col_property_lst.size());
    auto mid_pos = search_path(key).back();
    Record record(tp, tp.record_root);
    while (record.get_block_num() != mid_pos) {
        ts.append(record.get_all_tuple());
        record = Record(tp, record.get_block_num());
    }
    ts.append(record.find_less(key, is_close));
    return ts;
}

Tuples BpTree::find_greater(const Tuple &key, bool is_close)const {
    Tuples ts(tp.col_property_lst.size());
    auto pos = search_path(key).back();
    Record record(tp, pos);
    ts.append(record.find_greater(key, is_close));
    while (record.get_next_record_num() != -1) {
        assert(record.get_next_record_num() != -1);
        record = Record(tp, record.get_block_num());
        ts.append(record.get_all_tuple());
    }
    return ts;
}

Tuples BpTree::find_range(const Tuple &beg, const Tuple &end, bool is_beg_close, bool is_end_close)const {
    assert(beg <= end);

    Tuples ts(tp.col_property_lst.size());
    auto beg_pos = search_path(beg).back();
    auto end_pos = search_path(end).back();
    Record record(tp, beg_pos);
    if (beg_pos == end_pos) {
        return record.find_range(beg, end, is_beg_close, is_end_close);
    }

    // being block
    ts.append(record.find_greater(beg, is_beg_close));
    record = Record(tp, record.get_block_num());
    // inter block
    while (record.get_block_num() != end_pos) {
        assert(record.get_next_record_num() != -1);
        ts.append(record.get_all_tuple());
        record = Record(tp, record.get_block_num());
    }
    // end block
    ts.append(record.find_less(end, is_end_close));
}

//  === BpTree private function ===
std::string BpTree::index_path()const {
    return tp.db_name + "/block.sdb";
}

std::vector<BlockNum> BpTree::search_path(const Tuple &key)const {
    std::vector<BlockNum> lst;
    BptNode node = BptNode::get(tp, root_pos);
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

void BpTree::bubble_split(std::vector<BlockNum> &&lst, const Tuple &key, BlockNum record_pos) {
    BlockNum insert_pos = record_pos;
    mutex_map[insert_pos].lock();
    BptNode node = BptNode::get(tp, lst.back());
    while (true) {
        auto &&[key_it, pos_it] = node.search_less_or_eq_key(key);
        node.key_lst.insert(std::next(key_it), key);
        node.pos_lst.insert(std::next(pos_it), insert_pos);
        if (node.is_full()) {
            // split and sync 
            auto [right_node_pos, min_key] = node.split();
            if (node.file_pos == root_pos) {
                BptNode root_node = BptNode::new_node(tp);
                root_node.is_leaf = false;
                root_node.key_lst.push_back(min_key);
                root_node.pos_lst.push_back(node.file_pos);
                root_node.pos_lst.push_back(right_node_pos);
                root_node.sync();
                root_pos = root_node.file_pos;
                mutex_map[node.file_pos].unlock();
                return;
            }
            lst.pop_back();
            mutex_map[insert_pos].unlock();
            insert_pos = right_node_pos;
            mutex_map[lst.back()].lock();
            node = BptNode::get(tp, lst.back());
        } else {
            node.sync();
            mutex_map[insert_pos].unlock();
            return;
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
        for (auto &&cp : cps) {
            ObjPtr key = db_type::get_default(cp.type_info);
            key->de_bytes(bytes, offset);
            keys.push_back(key);
        }
        node.key_lst.push_back(keys);
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

    // node block bytes:
    //     |is_leaf right_node_pos key_lst pos_lst|
    Bytes bytes = sdb::en_bytes(is_leaf, right_node_pos);
    bytes_append(bytes, key_lst);
    for (auto &&key: key_lst) {
        key.range([&bytes](auto &&ptr){bytes_append(bytes, ptr);});
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
    // pass new pos
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
