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
    ASSERT_TRUE(Tuples::tuple_eq(tuple, tuple_backup));
    db_type::ObjPtr x2_ptr = std::make_shared<db_type::Int>(20);
    x_ptr->assign(x2_ptr);
    ASSERT_TRUE(!Tuples::tuple_eq(tuple, tuple_backup));

    // check tuple size
    ASSERT_TRUE(Tuples::tuple_type_size(tuple) == Tuples::tuple_type_size(tuple_backup));

    // check bytes
    tuples.data.push_back(tuple);
    Tuples tuples_backup = tuples;
    Bytes bytes = tuples.en_bytes();
    int offset = 0;
    tuples.de_bytes({{x_ptr->get_type_tag(), x_ptr->get_type_tag()}, {v_ptr->get_type_tag(), v_ptr->get_type_tag()}}, bytes, offset);
    for (size_t i = 0; i < tuples.data.size(); i++) {
        ASSERT_TRUE(Tuples::tuple_eq(tuples.data[i], tuples_backup.data[i]));
    }
    // check copy constructor
    tuples.data[0][0]->assign(std::make_shared<db_type::Int>(30));
    ASSERT_TRUE(!Tuples::tuple_eq(tuples.data[0], tuples_backup.data[0]));

    // TODO map/filter/range check
}

