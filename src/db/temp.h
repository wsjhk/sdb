#ifndef DB_TEMP_H
#define DB_TEMP_H 

#include <string>

#include "cache.h"

namespace sdb {

class Temp {
private:
    BlockCache block_cache;
    std::string db_name = "";
};

} // namespace sdb

#endif /* ifndef DB_TEMP_H */
