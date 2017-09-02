#include <gtest/gtest.h>

#include "../../src/db/util.h"

using namespace sdb;

TEST(db_util_test, bytes) {
    // === int check
    int i = 100;
    Bytes bytes = en_bytes(i);
    int offset = 0;
    de_bytes(i, bytes, offset);
    ASSERT_TRUE(i == 100);

    // === const and multi value check
    double d = 100.11;
    const float cf = 100.12;
    Bytes d_cf_bytes = en_bytes(d, cf);
    offset = 0;
    float f;
    de_bytes(d, d_cf_bytes, offset);
    de_bytes(f, d_cf_bytes, offset);
    ASSERT_TRUE(std::abs(d - 100.11) < 0.0001);
    ASSERT_TRUE(std::abs(f - 100.12) < 0.0001);

    // === string, set, map
    std::string str = "asdf";
    std::set<int> set = {1, 2, 3};
    std::unordered_map<int, int> map = {{1, 1}, {2, 2}};
    Bytes mul_bytes = en_bytes(str, set, map);
    std::string new_str;
    std::set<int> new_set;
    std::unordered_map<int, int> new_map;
    offset = 0;
    de_bytes(new_str, mul_bytes, offset);
    ASSERT_TRUE(str == new_str);
    de_bytes(new_set, mul_bytes, offset);
    ASSERT_TRUE(set == new_set);
    de_bytes(map, mul_bytes, offset);
    ASSERT_TRUE(new_map == new_map);
}
