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

class Table {
public:
    using TablePtr = std::shared_ptr<Table>;

    static void create_table(Tid t_id, const TableProperty &property);
    void drop_table(Tid t_id, const std::string &db_name, const std::string &table_name);

    // meta table
    static TablePtr table_list_table(const std::string &db_name);
    static TablePtr col_list_table(const std::string &db_name);
    static TablePtr index_table(const std::string &db_name);
    static TablePtr reference_table(const std::string &db_name);

    // index
    // void create_index(Tid t_id, const std::string &index_name, const std::list<std::string> &col_name_list);
    // void remove_index(Tid t_id, const std::string &index_name);

    // insert a tuple
    void insert(Tid t_id, const Tuple &tuple);

    using RecordPtr = std::shared_ptr<Record>;
    using RecordOp = std::function<void(RecordPtr)>;
    void record_range(RecordOp op);

    // remove by key
    void remove(Tid t_id, const Tuple &keys);
    // remove while predicate
    void remove(Tid t_id, TuplePred pred);

    // can't update primary key, 
    // use insert/remove in primary index if need update key
    void update(Tid t_id, const Tuple &new_tuple);
    // update while predicate
    void update(Tid t_id, TuplePred pred, TupleOp Op);

    // find use primary index
    Tuples find(Tid t_id, const Tuple &keys);
    Tuples find_less(Tid t_id, const Tuple &keys, bool is_close);
    Tuples find_greater(Tid t_id, const Tuple &keys, bool is_close);
    Tuples find_range(Tid t_id, const Tuple &beg, const Tuple &end, 
                      bool is_beg_close, bool is_end_close);
    // find use record
    Tuples find(Tid t_id, TuplePred pred);

    void add_referencing(Tid t_id, const std::string &table_name, const std::string &col_name);
    void add_referenced(Tid t_id, const std::string &table_name, const std::string &col_name);
    void remove_referencing(Tid t_id, const std::string &table_name);
    void remove_referenced(Tid t_id, const std::string &table_name);

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

    // TableProperty get_table_property(const std::string &table_name);

private:
    TableProperty tp;
    std::shared_ptr<BpTree> keys_index = nullptr;
    // db_name, table_name
};

} // namespace sdb

#endif
