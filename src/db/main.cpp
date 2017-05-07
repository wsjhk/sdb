#include <fstream>
#include <string>
#include <vector>
#include <iostream>
#include <map>
#include <cstring>
#include <ctime>
#include <functional>
#include <memory>

#include "table.h"
#include "util.h"
#include "bpTree.h"
#include "cache.h"
#include "io.h"
#include "db.h"

using namespace SDB;

SDB::Type::TableProperty get_table_property();
void db_test();

int main(void) {
    clock_t start = clock();
    db_test();
    std::cout << "time:" << (double)((clock()-start))/CLOCKS_PER_SEC << std::endl;
    return 0;
}

Type::TableProperty get_table_property(){
    std::vector<std::string> col_name_lst{"col_1", "col_2"};
    Type::ColProperty cp1("col_1", Enum::INT, 4);
    Type::ColProperty cp2("col_2", Enum::VARCHAR, 4);
    return Type::TableProperty("test", "test", "col_1", std::vector<Type::ColProperty>{cp1, cp2});
}

void db_test(){
    std::string db_name = "test";
    DB::create_db(db_name);
    DB db(db_name);
    auto property_1 = get_table_property();
    db.create_table(property_1);
    db.drop_table(property_1.table_name);
    db.drop_db();
}
