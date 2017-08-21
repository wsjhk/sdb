#ifndef MAIN_RECORD_H
#define MAIN_RECORD_H

#include <vector>
#include <string>
#include <functional>

#include "util.h"
#include "type.h"

class Record {
public:
    // type
    using Pos = SDB::Type::Pos ;
    using Byte = SDB::Type::Byte;
    using Bytes = SDB::Type::Bytes;
    using Value = SDB::Type::Value;
    using Tuple = SDB::Type::Tuple;
    using TupleLst = SDB::Type::TupleLst;
    using TableProperty = SDB::Type::TableProperty;

    Record()= delete;
    Record(const SDB::Type::TableProperty &property):property(property){
        read_meta_data();
    }
    ~Record(){
        write_meta_data();
    }

    // record
    TupleLst read_record(const SDB::Type::PosList &pos_lst);
    TupleLst read_record(size_t block_num);
    void write_record(size_t block_num, const TupleLst &tuple_lst);
    SDB::Type::Pos insert_record(const SDB::Type::Bytes &data);
    void remove_record(SDB::Type::Pos pos);
    // only non-variable column data
    void update(const std::string &pred_col_name, SDB::Type::BVFunc bvFunc, const std::string &op_col_name,
                SDB::Type::VVFunc vvFunc);
    Pos update(Pos pos, const Bytes &data);
    TupleLst find(const std::string &col_name, std::function<bool(Value)> predicate);

    // get
    Pos get_free_pos(size_t data_size);

    // create and drop
    static void create(const TableProperty &property);
    static void drop(const TableProperty &property);

private:
    void read_meta_data();
    void write_meta_data();
    static std::string get_record_path(const TableProperty &property);
    static std::string get_record_meta_path(const TableProperty &property);

private:
    SDB::Type::TableProperty property;
    std::map<SDB::Type::Pos, size_t> free_pos_lst;
    size_t end_block_num;
};
#endif //MAIN_RECORD_H
