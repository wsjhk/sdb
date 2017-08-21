#include <gtest/gtest.h>

#include "../../src/db/db_type.h"

using namespace SDB::DBType;

TEST(db_db_type_test, integer_object) {
    UP<Object> obj = std::make_unique<Int>(10);
    // type
    ASSERT_TRUE(obj->get_type_name() == "int");
    ASSERT_TRUE(obj->get_type_tag() == INT);
    ASSERT_TRUE(obj->get_type_size() == 4);

    // show
    ASSERT_TRUE(obj->to_string() == "10");

    // bytes
    auto bytes = SDB::Function::en_bytes(10);
    size_t offset = 0;
    obj->de_bytes(bytes, offset);
    ASSERT_TRUE(obj->en_bytes() == bytes);

    UP<Object> int_obj = std::make_unique<Int>(10);
    UP<Object> uint_obj = std::make_unique<Int>(10);

    // operator
    ASSERT_TRUE(!obj->less(int_obj).get_ok_value());
    ASSERT_TRUE(obj->eq(int_obj).get_ok_value());
}

