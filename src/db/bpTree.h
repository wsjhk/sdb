#ifndef BPTREE_H
#define BPTREE_H

#include <memory>
#include <list>
#include <stdexcept>
#include <iostream>
#include <string>
#include <boost/variant.hpp>
#include <functional>

#include "util.h"
#include "db_type.h"
#include "record.h"
#include "property.h"

namespace sdb {

struct BptNode {
    // type
    std::list<std::pair<DBType::ObjPtr, Pos>> pos_lst;
    bool is_leaf = true;
    bool is_new_node = true;
    // Pos end_pos = 0;
    Pos right_node_pos = 0;
    Pos file_pos = 0;

    // === 节点操作 ===
    // 获取节点最后的key
};

class BpTree {
public:
    // === type ===
    using nodePtrType = std::shared_ptr<BptNode>;

    BpTree()= delete;
    BpTree(const TableProperty &table_property);
    // 禁止树的复制，防止文件读写不一致
    BpTree(const BpTree &bpt)= delete;
    BpTree(BpTree &&bpt)= delete;
    const BpTree &operator=(const BpTree &bpt)= delete;
    BpTree &operator=(BpTree &&bpt)= delete;
    ~BpTree();

    static void create(const TableProperty &property);
    static void drop(const TableProperty &property);

    void clear();
    void write_info_block();

    nodePtrType read(SDB::Type::Pos pos) const;
    void write(nodePtrType ptr);

    // sql
    void insert(const Value &key, const Bytes &data);
    void remove(const Value &key);
    void update(const Value &key, const Bytes &data);
    PosList find(const Value &key)const;
    PosList find(const Value &mid, bool is_less)const;
    PosList find(const Value &beg, const Value &end)const;
    PosList find(std::function<bool(Value)> predicate) const;

    // debug log
    void print()const;

private:
    void initialize();

    bool is_node_less(nodePtrType ptr)const;
    // 分裂节点
    nodePtrType node_split(nodePtrType &ptr);
    // 合并节点
    bool node_merge(nodePtrType &ptr_1, nodePtrType &ptr_2);

    // 递归插入
    nodePtrType insert_r(const Value &key, const Bytes &data, nodePtrType ptr);
    // 递归删除
    bool remove_r(const Value &key, nodePtrType &ptr);
    // find_near_key_node
    nodePtrType find_near_key_node(const Value &key)const;
    nodePosLstType::const_iterator get_pos_lst_iter(const Value &key, const nodePosLstType &pos_lst) const;
    nodePtrType get_leaf_begin_node()const;
    nodePtrType get_leaf_end_node()const;
    void pos_lst_insert(PosList &pos_lst,
                        nodePosLstType::const_iterator beg_iter,
                        nodePosLstType::const_iterator end_iter)const;
    void pos_lst_insert(PosList &pos_lst,
                        nodePosLstType::const_iterator beg_iter,
                        nodePosLstType::const_iterator end_iter,
                        std::function<bool(Value)> predicate)const;
    // get
    static std::string get_index_path(const TableProperty &property);
    static std::string get_index_meta_path(const TableProperty &property);

    // === 异常处理 ===
    void throw_error(const std::string &str)const{
        throw std::runtime_error(str);
    }

private:
//    nodePtrType root;
    Pos root_pos;
    SDB::Type::PosList free_pos_list;
    Pos free_end_pos;
    size_t node_key_count;
    SDB::Type::TableProperty table_property;
};

} // namespace sdb

#endif /* BPTREE_H */
