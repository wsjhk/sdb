#include <vector>
#include <fstream>
#include <stdexcept>
#include <string>
#include <map>
#include <utility>
#include <functional>

#include "table.h"

#include "bpTree.h"
#include "util.h"
#include "io.h"
#include "cache.h"
#include "block_alloc.h"

namespace sdb {

// ========== public function ========
void Table::record_range(TransInfo t_info, RecordOp op) {
    BlockNum pos = tp.record_root;
    while (pos != -1) {
        auto ptr = std::make_shared<Record>(t_info, tp, pos);
        op(ptr);
        pos = ptr->get_next_record_num();
    }
}

void Table::insert(TransInfo t_info, const Tuple &tuple) {
    Tuple keys = tuple.select(tp.get_keys_pos());
    keys_index->insert(t_info, keys, tuple);
}

void Table::remove(TransInfo t_info, const Tuple &keys) {
    keys_index->remove(t_info, keys);
}

void Table::remove(TransInfo t_info, TuplePred pred) {
    RecordOp f = [pred, t_info](RecordPtr ptr){
        ptr->remove(pred);
    };
    record_range(t_info, f);
}

void Table::update(TransInfo t_info, const Tuple &new_tuple) {
    Tuple keys = new_tuple.select(tp.get_keys_pos());
    keys_index->update(t_info, keys, new_tuple);
}

void Table::update(TransInfo t_info, TuplePred pred, TupleOp op) {
    RecordOp f = [pred, op](RecordPtr ptr){
        ptr->update(pred, op);
    };
    record_range(t_info, f);
}

Tuples Table::find(TransInfo t_info, const Tuple &keys) {
    return keys_index->find_key(t_info, keys);
}

Tuples Table::find_less(TransInfo t_info, const Tuple &keys, bool is_close) {
    return keys_index->find_less(t_info,  keys, is_close);
}

Tuples Table::find_greater(TransInfo t_info, const Tuple &keys, bool is_close) {
    return keys_index->find_greater(t_info, keys, is_close);
}

Tuples Table::find_range(TransInfo t_info, const Tuple &beg, const Tuple &end, 
                  bool is_beg_close, bool is_end_close) {
    return keys_index->find_range(t_info, beg, end, is_beg_close, is_end_close);
}

Tuples Table::find(TransInfo t_info, TuplePred pred) {
    Tuples ts(tp.col_property_lst.size());
    RecordOp f = [pred, &ts](RecordPtr ptr){
        ts.append(ptr->find(pred));
    };
    record_range(t_info, f);
}

// ========== private function ========
} // namespace sdb
