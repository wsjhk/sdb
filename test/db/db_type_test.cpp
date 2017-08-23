#include <gtest/gtest.h>

#include "../../src/db/db_type.h"

using namespace SDB::DBType;

TEST(db_db_type_test, integer_object) {
    SP<Object> obj = std::make_unique<Int>(10);
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

    SP<Object> int_obj = std::make_unique<Int>(10);
    SP<Object> int_min_obj = std::make_unique<Int>(0);
    SP<Object> uint_obj = std::make_unique<UInt>(10);

    // operator
    // less
    ASSERT_TRUE(!obj->less(int_obj));
    ASSERT_TRUE(!obj->less(int_min_obj));
    ASSERT_TRUE(int_min_obj->less(obj));
    ASSERT_TRUE(!int_obj->less(obj));

    // eq
    ASSERT_TRUE(obj->eq(int_obj));
    try {
        obj->eq(uint_obj);
        ASSERT_TRUE(false);
    } catch (DBTypeMismatchingError err) {}

    // add sub mul div
}

