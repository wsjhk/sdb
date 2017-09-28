#ifndef DB_IO_H
#define DB_IO_H

#include <string>
#include "util.h"

namespace sdb {

class IO {
public:
    static IO &get(const std::string &db_name = "") {
        static IO io(db_name);
        return io;
    }

    // dir
    static void create_dir(const std::string &dir_path);
    static void remove_dir_force(const std::string &dir_path);

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

    // path
    static std::string get_db_dir_path();
    std::string get_db_file_path(const std::string &path)const;
    static std::string block_path() {return "block.sdb";}
    std::string log_path() const {return "log.sdb";}
    std::string balloc_log_path()const{return "bloack_log.sdb";}
    std::string balloc_backup_path()const{return "bloack_backup.sdb";}

private:
    // private function
    std::string db_name;
    size_t get_file_size(const std::string &file_path);

private:
    // private member
    IO(const std::string &db_name):db_name(db_name){}
    IO(const IO &io)=delete;
    IO(IO &&io)=delete;
    IO &operator=(const IO &io)=delete;
    IO &operator=(IO &&io)=delete;

};

} // namespacce sdb

#endif
