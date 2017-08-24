#include <gtest/gtest.h>

#include "../../src/db/db_type.h"

using namespace sdb::db_type;

TEST(db_db_type_test, integer_object) {
    SP<Object> obj = std::make_unique<Int>(10);
    // type
    ASSERT_TRUE(obj->get_type_name() == "int");
    ASSERT_TRUE(obj->get_type_tag() == INT);
    ASSERT_TRUE(obj->get_type_size() == 4);

    // show
    ASSERT_TRUE(obj->to_string() == "10");

    // bytes
    auto bytes = sdb::en_bytes(10);
    size_t offset = 0;
    obj->de_bytes(bytes, offset);
    std::cout << "==========" << std::endl;
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
}

TEST(db_db_type_test, integer) {
    SP<Int> int_ptr = std::make_unique<Int>(10);
    // type
    SP<Int> int_ptr2 = std::make_unique<Int>(10);
    SP<UInt> uint_ptr = std::make_unique<UInt>(10);

    // operator
    // add
    ASSERT_TRUE(int_ptr->add(int_ptr2)->to_string() == "20");
    ASSERT_TRUE(int_ptr->sub(int_ptr2)->to_string() == "0");
    ASSERT_TRUE(int_ptr->mul(int_ptr2)->to_string() == "100");
    ASSERT_TRUE(int_ptr->div(int_ptr2)->to_string() == "1");
    // type mismatching
    try{
        int_ptr->add(uint_ptr);
    } catch (DBTypeMismatchingError err) {}

    // overflow
    try{
        int_ptr2->data = INT_MAX;
        int_ptr->mul(int_ptr2);
        ASSERT_TRUE(false);
    } catch (DBTypeOverflowError err) {}

    // mismatching
    try {
        int_ptr->mul(uint_ptr);
        ASSERT_TRUE(false);
    } catch (DBTypeMismatchingError err) {}

    // div zero
    try {
        int_ptr2->data = 0;
        int_ptr->div(int_ptr2);
        ASSERT_TRUE(false);
    } catch (DBTypeDivzeroError err){}
}

TEST(db_db_type_test, varchar) {
    Varchar var("asdf", 100);
    ASSERT_TRUE(var.get_type_tag() == VARCHAR);
    ASSERT_TRUE(var.get_type_name() == "varchar");
    ASSERT_TRUE(var.get_type_size() == 4);

    auto var2_ptr = std::make_shared<Varchar>("asdf", 10);
    ASSERT_TRUE(var.eq(var2_ptr));
    ASSERT_TRUE(!var.less(var2_ptr));
}
