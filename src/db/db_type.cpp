#include "db_type.h"

namespace sdb::db_type {

void Varchar::assign(SP<const Object> obj) {
    if (auto p = dfc<const String>(obj)) {
        check_size(p->get_type_size());
        data = p->data;
    } else {
        throw DBTypeMismatchingError(get_type_name(), obj->get_type_name(), "/");
    }
}

} // namespace sdb
