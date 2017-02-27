#ifndef BPTREE_H
#define BPTREE_H

#include <memory>
#include <list>
#include <stdexcept>
#include <iostream>
#include <string>
#include <boost/variant.hpp>

#include "util.h"
#include "io.h"
#include "record.h"

using DB::Const::BLOCK_SIZE;
using DB::Type::Bytes;
using DB::Type::Pos;
using DB::Type::PosList ;


template <typename KeyType, typename DataType>
struct BptNode {

    // type
//    using lstSecType = boost::variant<std::shared_ptr<DataType>, std::shared_ptr<BptNode>>;
//    using lstItemType = std::pair<KeyType, lstSecType>;
    using lstPosItemType = std::pair<KeyType, Pos>;
//    using lstType = std::list<lstItemType>;
    using PoslstType = std::list<lstPosItemType>;
    using nodePtrType = std::shared_ptr<BptNode>;

//    std::list<std::pair<KeyType, lstSecType>> lst;
    std::list<std::pair<KeyType, Pos>> pos_lst;
//    std::shared_ptr<BptNode> end_ptr;
    bool is_leaf = true;
    Pos end_pos;
    Pos file_pos;

    // === 节点操作 ===
    // 获取节点最后的key
    KeyType last_key()const;
    
    // lstSecType 
//    static std::shared_ptr<DataType> &get_data_ptr(lstSecType &sec){
//        return boost::get<std::shared_ptr<DataType>>(sec);
//    }
//    static std::shared_ptr<BptNode> &get_node_ptr(lstSecType &sec){
//        return boost::get<std::shared_ptr<BptNode>>(sec);
//    }
};

template <typename KeyType, typename DataType>
class BpTree{
public:
    // === type ===
    using nodeType = BptNode<KeyType, DataType>;
    using nodePtrType = std::shared_ptr<nodeType>;
    using nodePosLstType = typename BptNode<KeyType, DataType>::PoslstType;
//    using nodeLstItemType = typename BptNode<KeyType, DataType>::lstItemType;
//    using nodeLstType = typename BptNode<KeyType, DataType>::lstType;

    BpTree()= delete;
    BpTree(const DB::Type::TableProperty &table_property);
    // 禁止树的复制，防止文件读写不一致
    BpTree(const BpTree &bpt)= delete;
    BpTree(BpTree &&bpt)= delete;
    const BpTree &operator=(const BpTree &bpt)= delete;
    BpTree &operator=(BpTree &&bpt)= delete;
    ~BpTree();
    void clear();
    void write_info_block();

    void insert(const KeyType &key, const DataType &data);
    void remove(const KeyType &key);
    typename BpTree<KeyType, DataType>::nodePtrType read(DB::Type::Pos pos) const;
    void write(nodePtrType ptr);
    DataType find(const KeyType &key)const{return find_r(key, read(root_pos));}
    void print()const;

private:
//    nodePtrType root;
    Pos root_pos;
    PosList free_pos_list;
    Pos free_end_pos;
    size_t node_key_count;
    DB::Type::TableProperty table_property;

    void initialize();

    bool is_node_less(nodePtrType ptr)const;
    // 分裂节点
    nodePtrType node_split(nodePtrType &ptr);
    // 合并节点
    bool node_merge(nodePtrType &ptr_1, nodePtrType &ptr_2);

    // 递归插入
    nodePtrType insert_r(const KeyType &key, const DataType &data, nodePtrType ptr);
    // 递归删除
    bool remove_r(const KeyType &key, nodePtrType &ptr);
    DataType find_r(const KeyType &key, nodePtrType ptr) const;
    // === 异常处理 ===
    void throw_error(const std::string &str)const{
        throw std::runtime_error(str);
    }
};

// --------------- Function ---------------
// ========== BptNode Function =========
template <typename KeyType, typename DataType>
KeyType BptNode<KeyType, DataType>::last_key()const{
    return (std::prev(pos_lst.end()))->first;
}

// ========== BpTree Function =========
// ---------- BpTree Public Function ---------
template <typename KeyType, typename DataType>
BpTree<KeyType, DataType>::BpTree(const DB::Type::TableProperty &table_property):table_property(table_property){
    initialize();
}

template <typename KeyType, typename DataType>
BpTree<KeyType, DataType>::~BpTree(){
    write_info_block();
    clear();
}

template <typename KeyType, typename DataType>
void BpTree<KeyType, DataType>::clear() {
//    root = nullptr;
}

template <typename KeyType, typename DataType>
void BpTree<KeyType, DataType>::write_info_block() {
    IO io(table_property.get_file_abs_path(DB::Enum::INDEX_SUFFIX));
    Bytes data(BLOCK_SIZE);
    // root pos
    size_t Pos_len = sizeof(root_pos);
    std::memcpy(data.data(), &root_pos, Pos_len);
    // free pos list
    size_t size_len = sizeof(size_t);
    size_t free_pos_count = free_pos_list.size();
    std::memcpy(data.data()+Pos_len, &free_pos_count, size_len);
    auto beg = data.data() + Pos_len + size_len;
    for (size_t j = 0; j < free_pos_count; ++j) {
        std::memcpy(beg+(j*Pos_len), &free_pos_list[j], Pos_len);
    }
    std::memcpy(beg+(free_pos_count*Pos_len), &free_end_pos, Pos_len);
    io.write_block(data, 0);
}

template <typename KeyType, typename DataType>
void BpTree<KeyType, DataType>::initialize() {
    // set node_key_count
    size_t size_len = sizeof(size_t);
    KeyType key_size = table_property.col_property[table_property.key].second;
    size_t pos_size = sizeof(DB::Type::Pos);
    node_key_count = (DB::Const::BLOCK_SIZE-pos_size-1-size_len)/(key_size+pos_size);
    // read info block
    IO io(table_property.get_file_abs_path(DB::Enum::INDEX_SUFFIX));
    Bytes block_data = io.read_block(0);
    // set root_pos
    size_t Pos_len = sizeof(Pos);
    std::memcpy(&root_pos, block_data.data(), Pos_len);
    // get free pos
    size_t free_pos_count;
    std::memcpy(&free_pos_count, block_data.data()+Pos_len, sizeof(size_t));
    auto beg = block_data.data()+Pos_len+size_len;
    for (size_t j = 0; j < free_pos_count; ++j) {
        Pos pos;
        std::memcpy(&pos, beg+(j*Pos_len), Pos_len);
        free_pos_list.push_back(pos);
    }
    std::memcpy(&free_end_pos, beg+(free_pos_count*Pos_len), Pos_len);
}

template <typename KeyType, typename DataType>
void BpTree<KeyType, DataType>::insert(const KeyType &key, const DataType &data){
    auto root = read(root_pos);
    if (!root) {
        nodePtrType new_root;
        Record record_list(table_property);
        auto record_pos = record_list.insert_record(data);
        new_root = std::make_shared<nodeType>();
        new_root->pos_lst.push_back(std::make_pair(key, record_pos));
        new_root->is_leaf = true;
        write(new_root);
        root_pos = new_root->file_pos;
        return;
    }
    nodePtrType left_ptr = insert_r(key, data, root);
    if (left_ptr){
        auto new_root = std::make_shared<nodeType>();
        new_root->pos_lst.push_back(std::make_pair(left_ptr->last_key(), left_ptr->file_pos));
        new_root->pos_lst.push_back(std::make_pair(root->last_key(), root->file_pos));
        new_root->is_leaf = false;
        write(new_root);
        root_pos = new_root->file_pos;
    }
}

template <typename KeyType, typename DataType>
void BpTree<KeyType, DataType>::remove(const KeyType &key){
    nodePtrType root = read(root_pos);
    if (root == nullptr){
        throw_error("Error: B+ Tree is empty");
    }
    remove_r(key, root);
    if (root->pos_lst.size() == 1){
        free_pos_list.push_back(root->pos_lst.begin()->second);
        root = read(root->pos_lst.begin()->second);
    }
}

template <typename KeyType, typename DataType>
typename BpTree<KeyType, DataType>::nodePtrType
BpTree<KeyType, DataType>::read(DB::Type::Pos pos) const{
    if (pos == 0) {
        return nullptr;
    }
    using DB::Function::de_bytes;
    std::string path = table_property.get_file_abs_path(DB::Enum::INDEX_SUFFIX);
    IO io(path);
    Bytes block_data = io.read_block(pos / BLOCK_SIZE);

    // ptr
    nodePtrType ptr = std::make_shared<nodeType>();

    // leaf
    ptr->is_leaf = block_data[0];

    // get pos list
    size_t key_len = table_property.col_property.at(table_property.key).second;
    size_t item_len = key_len + sizeof(pos);
    size_t pos_lst_len;
    std::memcpy(&pos_lst_len, block_data.data()+1, sizeof(size_t));
    auto beg = block_data.data()+1+sizeof(size_t);
    for (size_t i = 0; i < pos_lst_len; ++i) {
        Bytes item_bytes(beg+(i*item_len), beg+((i+1)*item_len));
        auto item_beg = beg+(i*item_len);
        auto item_tem = item_beg+key_len;
        auto item_end = item_tem + sizeof(pos);
        KeyType key;
        Pos child_pos;
        de_bytes(Bytes(item_beg, item_tem), key);
        de_bytes(Bytes(item_tem, item_end), child_pos);
        std::pair<KeyType, Pos> item(key, child_pos);
        ptr->pos_lst.push_back(item);
    }
    if (ptr->is_leaf) {
        de_bytes(Bytes(beg, beg+(pos_lst_len*item_len)), ptr->end_pos);
    }
    ptr->file_pos = pos;
    return ptr;
}

template <typename KeyType, typename DataType>
void BpTree<KeyType, DataType>::write(nodePtrType ptr) {
    using DB::Function::en_bytes;
    Bytes block_data(DB::Const::BLOCK_SIZE);
    size_t key_len = table_property.col_property[table_property.key].second;
    size_t item_len = key_len + sizeof(Pos);
    size_t size_len = sizeof(size_t);
    size_t char_len = sizeof(char);
    char is_leaf = ptr->is_leaf;
    std::memcpy(block_data.data(), &is_leaf, char_len);
    size_t pos_lst_len = ptr->pos_lst.size();
    std::memcpy(block_data.data()+char_len, &pos_lst_len, size_len);
    auto beg = block_data.data()+sizeof(char)+size_len;
    size_t offset = 0;
    for (auto &&item : ptr->pos_lst) {
        Bytes key_bytes;
        en_bytes(key_bytes, item.first);
        std::memcpy(beg+offset, key_bytes.data(), key_len);
        std::memcpy(beg+offset+key_len, &item.second, sizeof(Pos));
        offset += item_len;
    }
    if (is_leaf) {
        std::memcpy(beg+offset, &ptr->end_pos, key_len);
    }
    IO io(table_property.get_file_abs_path(DB::Enum::INDEX_SUFFIX));
    Pos write_pos;
    if (ptr->file_pos) {
        write_pos = ptr->file_pos;
    } else if (free_pos_list.empty()) {
        write_pos = free_end_pos;
        free_end_pos += BLOCK_SIZE;
        ptr->file_pos = write_pos;
    } else {
        write_pos = free_pos_list.back();
        free_pos_list.pop_back();
        ptr->file_pos = write_pos;
    }
    size_t block_num = write_pos / BLOCK_SIZE;
    io.write_block(block_data, block_num);
}

template <typename KeyType, typename DataType>
void BpTree<KeyType, DataType>::print()const{
    nodePtrType root_node = read(root_pos);
    std::list<nodePtrType> deq;
    deq.push_back(root_node);
    size_t sub_count = 1;
    size_t level_f = 0;
    while (!deq.empty()) {
        nodePtrType ptr = deq.front();
        deq.pop_front();
        sub_count--;
        if (ptr == nullptr){
            std::cout << "nullptr";
            continue;
        }
        bool is_leaf = ptr->is_leaf;
        std::cout << "[ ";
        for (auto iter = ptr->pos_lst.begin(); iter != ptr->pos_lst.end(); iter++){
            std::cout << iter->second << ":";
            if (is_leaf){
                std::cout << iter->first << " ";
//                std::cout << iter->first << ":";
//                std::cout << *(ptr->get_data_ptr(iter->second)) << " ";
            } else {
                std::cout << iter->first << " ";
                auto node = read(iter->second);
                deq.push_back(node);
            }
        }
        std::cout << "]";
        if (sub_count == 0){
            sub_count = deq.size();
            std::cout << std::endl;
            level_f++;
        }
    }
}

// ---------- BpTree Private Function ---------
template <typename KeyType, typename DataType>
typename BpTree<KeyType, DataType>::nodePtrType
BpTree<KeyType, DataType>::node_split(nodePtrType &ptr) {
    auto left_lst_ptr = std::make_shared<nodePosLstType>();
    left_lst_ptr->splice(left_lst_ptr->begin(), ptr->pos_lst,
                         ptr->pos_lst.begin(), std::next(ptr->pos_lst.begin(), ptr->pos_lst.size()/2));
    auto left_node_ptr = std::make_shared<nodeType>();
    left_node_ptr->pos_lst= *left_lst_ptr;
    if (ptr->is_leaf){
        left_node_ptr->end_pos = ptr->file_pos;
    }
    left_node_ptr->is_leaf = ptr->is_leaf;
    write(ptr);
    write(left_node_ptr);
    return left_node_ptr;
}

template <typename KeyType, typename DataType>
bool BpTree<KeyType, DataType>::node_merge(nodePtrType &ptr_1, nodePtrType &ptr_2) {
    ptr_2->pos_lst.splice(ptr_2->pos_lst.begin(), ptr_1->pos_lst);
    if (ptr_2->pos_lst.size() > node_key_count){
        ptr_1 = node_split(ptr_2);
        write(ptr_1);
        write(ptr_2);
        return false;
    } else {
        ptr_1 = ptr_2;
        write(ptr_1);
        free_pos_list.push_back(ptr_2->file_pos);
        ptr_2 = nullptr;
        return true;
    }
}

template <typename KeyType, typename DataType>
bool BpTree<KeyType, DataType>::is_node_less(nodePtrType ptr)const{
    return (node_key_count/2) > ptr->pos_lst.size();
}

template <typename KeyType, typename DataType>
DataType BpTree<KeyType, DataType>::find_r(const KeyType &key, nodePtrType ptr) const{
    bool is_leaf = ptr->is_leaf;
    if (ptr == nullptr){
        throw_error("Error: can't fount key");
    }
    for (auto &x: ptr->pos_lst) {
        if (key == x.first && is_leaf){
            Record record(table_property);
            PosList pos_lst;
            pos_lst.push_back(ptr->file_pos);
            record.read_record(pos_lst)[0];
        } else if (!is_leaf && key <= x.first){
            return find_r(key, read(x.second));
        } else if (key < x.first && is_leaf) {
            break;
        }
    }
    throw_error("Error: can't fount key");
}

template <typename KeyType, typename DataType>
typename BpTree<KeyType, DataType>::nodePtrType
BpTree<KeyType, DataType>::insert_r(const KeyType &key, const DataType &data, nodePtrType ptr) {
    bool is_leaf = ptr->is_leaf;
    bool is_for_end = true;
    for (auto iter=ptr->pos_lst.begin(); iter!=ptr->pos_lst.end(); iter++){
        if (iter->first == key){
            throw_error("Error: key already existed!");
        } else if (key < iter->first) {
            if (is_leaf){
                Record record(table_property);
                Pos record_pos = record.insert_record(data);
                ptr->pos_lst.insert(iter, std::make_pair(key, record_pos));
            } else {
                auto node = read(iter->second);
                auto left_ptr = insert_r(key, data, node);
                if (left_ptr){
                    ptr->pos_lst.insert(iter, std::make_pair(left_ptr->last_key(), left_ptr->file_pos));
                }
            }
            is_for_end = false;
            break;
        }
    }
    if (is_for_end) {
        auto lst_end_ptr = std::prev(ptr->pos_lst.end());
        if (is_leaf){
            Record record(table_property);
            Pos record_pos = record.insert_record(data);
            ptr->pos_lst.insert(ptr->pos_lst.end(), std::make_pair(key, record_pos));
        } else {
            auto node = read(lst_end_ptr->second);
            auto left_ptr = insert_r(key, data, node);
            if (left_ptr){
                ptr->pos_lst.insert(lst_end_ptr, std::make_pair(left_ptr->last_key(), left_ptr->file_pos));
            }
            lst_end_ptr->first = key;
        }
    }
    write(ptr);
    return (ptr->pos_lst.size() > node_key_count) ? node_split(ptr) : nullptr;
}

template <typename KeyType, typename DataType>
bool BpTree<KeyType, DataType>::remove_r(const KeyType &key, nodePtrType &ptr) {
    bool is_leaf = ptr->is_leaf;
    bool is_for_end = true;
    for (auto iter = ptr->pos_lst.begin(); iter != ptr->pos_lst.end(); iter++){
        if (is_leaf) {
            if (key == iter->first) {
                free_pos_list.push_back(iter->second);
                ptr->pos_lst.erase(iter);
                is_for_end = false;
                break;
            } else if (key < iter->first) {
                throw_error("Error: can't find Key");
            }
        } else if (key <= iter->first) {
            nodePtrType iter_sec_ptr = read(iter->second);
            bool is_less = remove_r(key, iter_sec_ptr);
            iter->first = iter_sec_ptr->last_key();
            if (is_less){
                bool is_one;
                if (std::next(iter) == ptr->pos_lst.end()){
                    auto iter_prev = std::prev(iter);
                    nodePtrType prev_ptr = read(iter_prev->second);
                    is_one = node_merge(prev_ptr, iter_sec_ptr);
                    if (is_one) ptr->pos_lst.erase(iter);
                    iter_prev->first = read(iter_prev->second)->last_key();
                } else {
                    auto iter_next = std::next(iter);
                    nodePtrType next_ptr = read(iter->second);
                    is_one = node_merge(iter_sec_ptr, next_ptr);
                    if (is_one) ptr->pos_lst.erase(iter_next);
                    iter->first = iter_sec_ptr->last_key();
                }
            }
            is_for_end = false;
            break;
        }
    }
    if (is_for_end && is_leaf){
        throw_error("Error: can't find Key");
    }
    write(ptr);
    return is_node_less(ptr);
}

#endif /* BPTREE_H */