//
// Created by sven on 17-2-19.
//

#ifndef DB_IO_H
#define DB_IO_H

#include <string>
#include "util.h"

namespace sdb {

class IO {
public:
    static IO &get() {
        static IO io;
        return io;
    }

    // dir
    void create_dir(const std::string &dir_path);
    void remove_dir_force(const std::string &dir_path);

    // file
    void create_file(const std::string &file_path);
    void delete_file(const std::string &file_path);

    Bytes read_file(const std::string &file_path);

    void full_write_file(const std::string &file_path, const Bytes &data);
    void append_write_file(const std::string &file_path, const Bytes &data);

    // block
    Bytes read_block(const std::string &file_path, size_t block_num);
    void write_block(const std::string &file_path, size_t block_num, const Bytes &data);

    // get
    bool has_file(const std::string &str);

private:
    // private function
    std::string get_db_file_dir_path();
    std::string get_db_file_path(const std::string &file_path);
    size_t get_file_size(const std::string &file_path);

private:
    // private member
    IO(){}
    IO(const IO &io)=delete;
    IO(IO &&io)=delete;
    IO &operator=(const IO &io)=delete;
    IO &operator=(IO &&io)=delete;

};

} // namespacce sdb

#endif
