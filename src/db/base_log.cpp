#include "base_log.h"
#include <fstream>

namespace sdb {

void BaseLog::log(const Bytes &bytes) {
    Bytes len_bytes = sdb::en_bytes(Size(bytes.size()));
    len_bytes.insert(len_bytes.begin(), bytes.begin(), bytes.end());
    write(len_bytes);
}

void BaseLog::write(const Bytes &bytes) {
    std::lock_guard<std::mutex> lg(mutex);
    using std::ios;
    std::ofstream out(file_path, ios::binary | ios::app);
    out.write(bytes.data(), bytes.size());
    out.close();
}

void BaseLog::range(std::function<void(Bytes)> op) {
    std::lock_guard<std::mutex> lg(mutex);
    using std::ios;
    std::ifstream reader(file_path, ios::binary);
    while(reader.good()) {
        Bytes len_bytes(sizeof(Size), '\0');
        reader.read(len_bytes.data(), sizeof(Size));
        Size size;
        std::memcpy(&size, len_bytes.data(), sizeof(Size));
        Bytes data(size, '\0');
        reader.read(data.data(), size);
        op(data);
    }
}

} // namespace sdb
