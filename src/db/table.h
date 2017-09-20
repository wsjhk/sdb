#ifndef DB_TABLE_H
#define DB_TABLE_H

#include <string>
#include <map>
#include <shared_mutex>

#include "record.h"
#include "util.h"
#include "bpTree.h"

namespace sdb {

struct TableError : public std::runtime_error {
    TableError(const std::string &msg):runtime_error(msg){}
};

struct TableNotFound : public TableError {
    TableNotFound(const std::string &table_name)
        :TableError(cpp_util::format("table [%s] not found", table_name)) {}
};

struct TableExisted : public TableError {
    TableExisted(const std::string &table_name)
        :TableError(cpp_util::format("table [%s] existed", table_name)) {}
};

class Table {
public:
    Table()= delete;
    Table(const TableProperty &tp):tp(tp), keys_index(std::make_shared<BpTree>(tp)){}

    // index
    // void create_index(TransInfo ti, const std::string &index_name, const std::list<std::string> &col_name_list);
    // void remove_index(TransInfo ti, const std::string &index_name);

    // insert a tuple
    void insert(TransInfo t_info, const Tuple &tuple);

    using RecordPtr = std::shared_ptr<Record>;
    using RecordOp = std::function<void(RecordPtr)>;
    void record_range(TransInfo t_info, RecordOp op);

    // remove by key
    void remove(TransInfo ti, const Tuple &keys);
    // remove while predicate
    void remove(TransInfo ti, TuplePred pred);

    // can't update primary key, 
    // use insert/remove in primary index if need update key
    void update(TransInfo t_info, const Tuple &new_tuple);
    // void update(TransInfo t_info, const Tuple keys, const std::string &col_name, db_type::ObjCntPtr new_val);
    // update while predicate
    void update(TransInfo ti, TuplePred pred, TupleOp Op);

    // find use primary index
    Tuples find(TransInfo ti, const Tuple &keys);
    Tuples find_less(TransInfo ti, const Tuple &keys, bool is_close);
    Tuples find_greater(TransInfo ti, const Tuple &keys, bool is_close);
    Tuples find_range(TransInfo ti, const Tuple &beg, const Tuple &end, bool is_beg_close, bool is_end_close);
    // find use record
    Tuples find(TransInfo ti, TuplePred pred);

    // bool is_referenced()const;
    // bool is_referencing()const;
    // bool is_referencing(const std::string &table_name)const;

    std::unordered_map<std::string, std::string> get_referenced_map()const;
    std::unordered_map<std::string, std::string> get_referencing_map()const;
    std::string get_key()const;
    std::vector<std::string> get_col_name_lst()const{return tp.get_col_name_lst();}

private:
    bool is_has_index(const std::string &col_name)const;

public:
    TableProperty tp;
private:
    const std::shared_ptr<BpTree> keys_index;
};

} // namespace sdb

#endif // DB_TABLE_H
