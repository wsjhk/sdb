#include <map>

#include "record.h"
#include "util.h"
#include "cache.h"
#include "io.h"

namespace sdb {

Record::Record(const std::string &file_path, BlockNum bn):block_num(bn), tuples(0) {
    IO &io = IO::get();
    Bytes bytes = io.read_block(file_path, bn);
    BlockOffset offset = 0;
    std::vector<std::pair<db_type::TypeTag, int>> tag_sizes;
    sdb::de_bytes(tag_sizes, bytes, offset);
    tuples = Tuples(tag_sizes.size());
}

} // namespace sdb
