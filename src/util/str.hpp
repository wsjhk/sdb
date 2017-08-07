#ifndef UTIL_STR_HPP
#define UTIL_STR_HPP 

#include <string>
#include <vector>
#include <iostream>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/format.hpp>

namespace Str {
    inline std::vector<std::string> split(const std::string &str, const std::string &sub_str) {
        std::vector<std::string> v;
        boost::split(v, str, boost::is_any_of(sub_str));
        return v;
    }

    template <typename ...Args>
    inline std::string format(std::string str, Args... args) {
        return (boost::format(str) % ... % args).str();
    }

    inline void log(const std::string &str) {
        std::cout << str << std::endl;
    }
}

#endif /* ifndef UTIL_STR_HPP */
