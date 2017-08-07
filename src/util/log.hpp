#ifndef UTIL_LOG_HPP
#define UTIL_LOG_HPP 

#include <iostream>

namespace Log {
    inline void log(const std::string &str) {
        std::cout << str << std::endl;
    }
}

#endif /* ifndef UTIL_LOG_HPP */
