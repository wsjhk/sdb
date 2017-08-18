#include <stdexcept>
#include <fstream>
#include <iostream>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <string.h>
#include <unistd.h>
#include <boost/format.hpp>
#include <experimental/filesystem>

#include "io.h"
#include "util.h"
#include "../cpp_util/log.hpp"

using std::ios;
using SDB::Const::BLOCK_SIZE;
using SDB::Type::Bytes;
namespace ef = std::experimental::filesystem;

using namespace cpp_util;

// ========= public =========
// dir
void IO::create_dir(const std::string &dir_path) {
    std::string abs_path = get_db_file_path(dir_path);
    bool sc = ef::create_directory(abs_path);
    assert_msg(sc, format("Error: file %s already existed", abs_path));
}

void IO::remove_dir_force(const std::string &dir_path) {
    try {
        ef::remove_all(get_db_file_dir_path()+"/"+dir_path);
    } catch (ef::filesystem_error err) {
        assert_msg(false, err.what());
    }
}

// file
void IO::create_file(const std::string &file_path) {
    std::string abs_path = get_db_file_path(file_path);
    // todo: multi-thread
    // unsafe
    assert_msg(!has_file(file_path), abs_path);
    std::ofstream out(abs_path, ios::binary | ios::app);
    out.close();
}

void IO::delete_file(const std::string &file_name) {
    // todo: multi-thread
    // unsafe 
    assert_msg(has_file(file_name), file_name);
    try {
        ef::remove(get_db_file_path(file_name));
    } catch (ef::filesystem_error err) {
        assert_msg(false, err.what());
    }
}

SDB::Type::Bytes IO::read_file(const std::string &file_path) {
    std::string abs_path = get_db_file_path(file_path);
    std::ifstream in(abs_path, std::ios::binary);
    assert_msg(in.is_open(), abs_path);
    size_t file_size = get_file_size(file_path);
    Bytes buffer_data(file_size);
    in.read(buffer_data.data(), file_size);
    return buffer_data;
}

void IO::full_write_file(const std::string &file_path, const SDB::Type::Bytes &data) {
    std::string abs_path = get_db_file_path(file_path);
    assert_msg(has_file(file_path), abs_path);
    std::ofstream out(abs_path, ios::binary);
    out.write(data.data(), data.size());
    out.close();
}

void IO::append_write_file(const std::string &file_path, const SDB::Type::Bytes &data) {
    std::string abs_path = get_db_file_path(file_path);
    assert_msg(has_file(file_path), abs_path);
    std::ofstream out(abs_path, ios::binary | ios::app);
    out.write(data.data(), data.size());
    out.close();
}

SDB::Type::Bytes IO::read_block(const std::string &file_path, size_t block_num) {
    // mmap read
    std::string abs_path = get_db_file_path(file_path);
    int fd = open(abs_path.data(), O_RDWR);
    assert_msg(fd >= 0, abs_path);
    if (get_file_size(file_path) < (block_num+1)*BLOCK_SIZE) {
        lseek(fd, BLOCK_SIZE*(block_num+1), SEEK_SET);
        write(fd, "", 1);
    }
    char *buff = (char*)mmap(nullptr, BLOCK_SIZE, PROT_READ, MAP_SHARED, fd, BLOCK_SIZE*block_num);
    Bytes bytes(buff, buff+BLOCK_SIZE);
    munmap(buff, BLOCK_SIZE);
    close(fd);

    return bytes;
}

void IO::write_block(const std::string &file_path, size_t block_num, const SDB::Type::Bytes &data){
    std::string abs_path = get_db_file_path(file_path);
    if (data.size() != BLOCK_SIZE) {
        throw std::runtime_error("Error: data size not equal BLOCK_SIZE");
    }
    int fd = open(abs_path.data(), O_RDWR);
    assert_msg(fd >= 0, abs_path);
    if (get_file_size(file_path) < (block_num+1)*BLOCK_SIZE) {
        lseek(fd, BLOCK_SIZE*(1+block_num), SEEK_SET);
        write(fd, "", 1);
    }
    char *buff = (char*)mmap(nullptr, BLOCK_SIZE, PROT_WRITE, MAP_SHARED, fd, BLOCK_SIZE*block_num);
    std::memcpy(buff, data.data(), BLOCK_SIZE);
    close(fd);
    munmap(buff, BLOCK_SIZE);
}

bool IO::has_file(const std::string &str) {
    return ef::exists(get_db_file_path(str));
}

size_t IO::get_file_size(const std::string &file_path) {
    std::string abs_path = get_db_file_path(file_path);
    struct stat file_info;
    stat(abs_path.data(), &file_info);
    return (size_t)(file_info.st_size);
}

std::string IO::get_db_file_dir_path() {
    auto dir_path = ef::path(__FILE__).parent_path();
    std::string file_path = dir_path.generic_string()+"/data";
    return file_path;
}

std::string IO::get_db_file_path(const std::string &file_name) {
    std::string file_path = get_db_file_dir_path();
    return file_path + '/' + file_name;
}
