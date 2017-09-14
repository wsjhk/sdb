// =======================
// B+Tree(B-link Tree)
// =======================

#ifndef DB_BPTREE_H
#define DB_BPTREE_H

#include <memory>
#include <list>
#include <stdexcept>
#include <iostream>
#include <string>
#include <functional>
#include <mutex>

#include "util.h"
#include "db_type.h"
#include "record.h"
#include "property.h"

namespace sdb {

// b-link tree
class BpTree {
public:
    // === type ===
    struct BptNode;
    using nodePtr = std::shared_ptr<BptNode>;

    BpTree()= delete;
    BpTree(const TableProperty &tp):tp(tp){}
    BpTree(const BpTree &bpt)= delete;
    BpTree(BpTree &&bpt)= delete;
    const BpTree &operator=(const BpTree &bpt)= delete;
    BpTree &operator=(BpTree &&bpt)= delete;
    // ~BpTree();

    static std::shared_ptr<BpTree> build(const TableProperty &property);
    void drop();

    // op
    void insert(TransInfo t_info, const Tuple &key, const Tuple &data);
    void remove(TransInfo t_info, const Tuple &key);
    void update(TransInfo t_info, const Tuple &key, const Tuple &data);
    Tuples find_key(TransInfo t_info, const Tuple &key)const;
    // TODO
    Tuples find_pre_key(TransInfo t_info, const Tuple &key)const;
    Tuples find_less(TransInfo t_info, const Tuple &key, bool is_close)const;
    Tuples find_greater(TransInfo t_info, const Tuple &key, bool is_close)const;
    Tuples find_range(TransInfo t_info, const Tuple &beg, const Tuple &end, bool is_beg_close, bool is_end_close)const;

    // debug log
    void print()const;

private:
    std::vector<BlockNum> search_path(const Tuple &key)const;
    // get
    std::string index_path()const;
    // bubble split
    void bubble_split(std::vector<BlockNum> &&lst, const Tuple &key, BlockNum record_pos);

    // === 异常处理 ===
    void throw_error(const std::string &str)const{
        throw std::runtime_error(str);
    }

private:
    const TableProperty tp;
    // TODO concurrent map
    std::unordered_map<BlockNum, std::mutex> mutex_map;
    // std::mutex global_mutex;
};

// Node
struct BpTree::BptNode {
    // copy constructor
    BptNode(const BptNode &node)=default;
    BptNode &operator=(const BptNode &node)=default;

    // read by cache
    static BptNode get(const TableProperty &tp, BlockNum pos);
    // new_node
    static BptNode new_node(const TableProperty &tp);
    
    bool is_full()const;
    Size get_bytes_size()const;
    // return => <key_list_iterator, pos_list_iterator>
    using KeyListIt = std::list<Tuple>::const_iterator;
    using PosListIt = std::list<BlockNum>::const_iterator;
    std::pair<KeyListIt, PosListIt> search_less_or_eq_key(Tuple key);

    // sync to cache
    void sync();

    // return right pos
    std::pair<BlockNum, Tuple> split();

    // ===== member =====
    bool is_leaf = true;
    BlockNum right_node_pos = -1;
    BlockNum file_pos = -1;

    std::list<Tuple> key_lst;
    std::list<BlockNum> pos_lst;

private:
    TableProperty tp;
    BptNode(const TableProperty &tp):tp(tp){}
};

} // namespace sdb

#endif /* DB_BPTREE_H */
