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
struct BptNode {
    // menber
    bool is_leaf = true;
    BlockNum right_node_pos = -1;
    BlockNum file_pos = -1;
    Size bytes_size = 0;

    std::vector<db_type::ObjCntPtr> key_lst;
    std::vector<BlockNum> pos_lst;

    // read by cache
    static BptNode get(const TableProperty &table_property, BlockNum pos);
    
    // TODO impl
    void reset_bytes_size();
    // get
    bool is_full()const;

    // sync to cache
    void sync()const;

    // return right pos
    BlockNum split();

private:
    const TableProperty tp;
    BptNode(const TableProperty &tp):tp(tp){}
};

class BpTree {
public:
    // === type ===
    using nodePtr = std::shared_ptr<BptNode>;
    using ObjCntPtr = db_type::ObjCntPtr;

    BpTree()= delete;
    BpTree(const TableProperty &table_property):table_property(table_property){}
    BpTree(const BpTree &bpt)= delete;
    BpTree(BpTree &&bpt)= delete;
    const BpTree &operator=(const BpTree &bpt)= delete;
    BpTree &operator=(BpTree &&bpt)= delete;
    ~BpTree();

    static void get(const TableProperty &property);
    // static void drop(const TableProperty &property);

    // sql
    void insert(ObjCntPtr key, const Tuple &tuple);
    void remove(ObjCntPtr key);
    void update(ObjCntPtr key, const Bytes &data);
    Tuples find(ObjCntPtr key)const;
    Tuples find(ObjCntPtr mid, bool is_less)const;
    Tuples find(ObjCntPtr beg, ObjCntPtr end)const;
    Tuples find(ObjPred pred) const;

    // debug log
    void print()const;

private:
    std::vector<BlockNum> search_path(ObjCntPtr key)const;
    BlockNum search_reocrd_pos(ObjCntPtr key)const;
    // get
    std::string index_path()const;

    // === 异常处理 ===
    void throw_error(const std::string &str)const{
        throw std::runtime_error(str);
    }

private:
    const TableProperty table_property;
    // TODO concurrent map
    std::unordered_map<BlockNum, std::mutex> map;
    // std::mutex global_mutex;
};

} // namespace sdb

#endif /* DB_BPTREE_H */
