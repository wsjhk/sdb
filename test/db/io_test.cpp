#include <gtest/gtest.h>

#include "../../src/db/io.h"
#include "../../src/db/util.h"

using SDB::Type::Bytes;
using SDB::Function::en_bytes;
using SDB::Function::de_bytes;

TEST(db_io_test, io) {
    IO &io = IO::get();
    // create first
    if (io.has_file("_test")) {
        io.remove_dir_force("_test");
    }

    // === dir create/remove
    io.create_dir("_test");
    ASSERT_TRUE(io.has_file("_test"));
    
    // === file create/delete
    io.create_file("_test/_test.txt");
    ASSERT_TRUE(io.has_file("_test/_test.txt"));

    // === file read/write
    std::string input_text = "gl";
    Bytes bytes = en_bytes(input_text);
    std::string file_path = "_test/_test.txt";
    io.full_write_file(file_path, bytes);
    Bytes read_bytes = io.read_file(file_path);
    ASSERT_TRUE(bytes == read_bytes);
    // append bytes
    Bytes append_bytes = en_bytes(12);
    io.append_write_file(file_path, append_bytes);
    read_bytes = io.read_file(file_path);
    bytes.insert(bytes.end(), append_bytes.begin(), append_bytes.end());
    ASSERT_TRUE(bytes == read_bytes);

    bytes.resize(SDB::Const::BLOCK_SIZE);
    io.write_block(file_path, 0, bytes);
    read_bytes = io.read_block(file_path, 0);
    ASSERT_TRUE(bytes == read_bytes);

    //  remove
    io.delete_file("_test/_test.txt");
    ASSERT_TRUE(!io.has_file("_test/_test.txt"));
    io.remove_dir_force("_test");
    ASSERT_TRUE(!io.has_file("_test"));
}

