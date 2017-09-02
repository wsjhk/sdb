#include <gtest/gtest.h>

#include "../../src/db/io.h"
#include "../../src/db/tuple.h"

using namespace sdb;

TEST(db_tuple_test, tuples) {
    IO &io = IO::get();
    // create first
    if (io.has_file("_tuple_test")) {
        io.remove_dir_force("_tuple_test");
    }
    io.create_dir("_tuple_test");
    io.create_file("_tuple_test/block.sdb");
    Tuples tuples(2);
    db_type::ObjPtr x_ptr = std::make_shared<db_type::Int>(10);
    db_type::ObjPtr v_ptr = std::make_shared<db_type::Varchar>("asdf", 10);
    Tuples::Tuple tuple = {x_ptr, v_ptr};
    // tuple_clone
    Tuples::Tuple tuple_backup = Tuples::tuple_clone(tuple);
    tuples.data.push_back(Tuples::tuple_clone(tuple));
    for (size_t i = 0; i < 2; i++) {
        ASSERT_TRUE(tuple[i]->eq(tuple_backup[i]));
    }
    db_type::ObjPtr x2_ptr = std::make_shared<db_type::Int>(20);
    x_ptr->assign(x2_ptr);
    ASSERT_TRUE(!tuple[0]->eq(tuple_backup[0]));
}

