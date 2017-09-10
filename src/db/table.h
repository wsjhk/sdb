#ifndef DB_TABLE_H
#define DB_TABLE_H

#include <string>
#include <map>
#include <tbb/concurrent_unordered_map.h>

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

class Table {
public:
    using TablePtr = std::shared_ptr<Table>;

    static void init(const std::string &db_name);
    static TablePtr get_table(const std::string &db_name, const std::string &table_name);

    static void create_table(const TableProperty &property);
    static void drop_table(const std::string &db_name, const std::string &table_name);

    // index
    // void create_index(const std::string &index_name, const std::list<std::string> &col_name_list);
    // void remove_index(const std::string &index_name);

    // insert a tuple
    void insert(const Tuple &tuple);

    using RecordPtr = std::shared_ptr<Record>;
    using RecordOp = std::function<void(RecordPtr)>;
    void record_range(RecordOp op);

    // remove by key
    void remove(const Tuple &keys);
    // remove while predicate
    void remove(TuplePred pred);

    // can't update primary key, 
    // use insert/remove in primary index if need update key
    void update(const Tuple &new_tuple);
    // update while predicate
    void update(TuplePred pred, TupleOp Op);

    // find use primary index
    Tuples find(const Tuple &keys);
    Tuples find_less(const Tuple &keys, bool is_close);
    Tuples find_greater(const Tuple &keys, bool is_close);
    Tuples find_range(const Tuple &beg, const Tuple &end, 
                      bool is_beg_close, bool is_end_close);
    // find use record
    Tuples find(TuplePred pred);

    void add_referencing(const std::string &table_name, const std::string &col_name);
    void add_referenced(const std::string &table_name, const std::string &col_name);
    void remove_referencing(const std::string &table_name);
    void remove_referenced(const std::string &table_name);

    bool is_referenced()const;
    bool is_referencing()const;
    bool is_referencing(const std::string &table_name)const;

    std::unordered_map<std::string, std::string> get_referenced_map()const;
    std::unordered_map<std::string, std::string> get_referencing_map()const;
    std::string get_key()const;
    std::vector<std::string> get_col_name_lst()const{return tp.get_col_name_lst();}

private:
    Table()= delete;
    Table(const TableProperty &tp, bool is_init);

    bool is_has_index(const std::string &col_name)const;

    // meta table
    static TablePtr table_list_table(const std::string &db_name);
    static TablePtr col_list_table(const std::string &db_name);
    static TablePtr index_table(const std::string &db_name);
    static TablePtr reference_table(const std::string &db_name);

    // TableProperty get_table_property(const std::string &table_name);

private:
    TableProperty tp;
    std::shared_ptr<BpTree> keys_index = nullptr;
    // std::vector<BpTree> bpt_index_lst;
    static std::map<std::pair<std::string, std::string>, TablePtr> table_map;
};

} // namespace sdb

#endif
