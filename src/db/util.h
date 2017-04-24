//
// Created by sven on 17-2-19.
//

#ifndef UTIL_H
#define UTIL_H

#include <stdexcept>
#include <vector>
#include <memory>
#include <cstring>
#include <string>
#include <iostream>
#include <map>
#include <list>
#include <unordered_map>
#include <unordered_set>
#include <boost/spirit/home/support/container.hpp>

namespace SDB {

namespace Enum {
    enum ColType: char {
        INT,
        FLOAT,
        VARCHAR
    };

    enum BVFunc: char {
        EQ,
        LESS,
        GREATER,
    };
} // Enum namespace

namespace Const {
    #include <unistd.h>
    const size_t BLOCK_SIZE = size_t(getpagesize());
    const size_t SIZE_SIZE = sizeof(size_t);
    const size_t INT_SIZE = sizeof(int32_t);
    const size_t FLOAT_SIZE = sizeof(float);
    const size_t POS_SIZE = sizeof(size_t);

    const size_t CACHE_COUNT = 100;
} // Const namespace

namespace Type {
    using Float = float;
    using Int = int32_t;
    using Pos = size_t;
    using PosList = std::vector<Pos>;
    using Byte = char;
    using Bytes = std::vector<Byte>;
    using BytesList = std::vector<Bytes>;
} // Type namespace

namespace Traits {
    template <typename T>
    struct PairTraits {
        static const bool value = false;
    };
    template <typename Fst, typename Sec>
    struct PairTraits<std::pair<Fst, Sec>> {
        static const bool value = true;
    };
    // vector
    template <typename T>
    struct VectorTraits {
        static const bool value = false;
    };
    template <typename SubType>
    struct VectorTraits<std::vector<SubType>> {
        static const bool value = true;
    };
    // list
    template <typename SubType>
    struct ListTraits {
        static const bool value = false;
    };
    template <typename SubType>
    struct ListTraits<std::list<SubType>> {
        static const bool value = true;
    };
    // string
    template <typename SubType>
    struct StringTraits {
        static const bool value = false;
    };
    template <>
    struct StringTraits<std::string> {
        static const bool value = true;
    };
    // set
    template <typename SubType>
    struct USetTraits {
        static const bool value = false;
    };
    template <typename SubType>
    struct USetTraits<std::unordered_set<SubType>> {
        static const bool value = true;
    };
    // map
    template <typename T>
    struct MapTraits {
        static const bool value = false;
    };
    template <typename Fst, typename Sec>
    struct MapTraits<std::map<Fst, Sec>> {
        static const bool value = true;
    };
    // unordered_map
    template <typename T>
    struct UMapTraits {
        static const bool value = false;
    };
    template <typename Fst, typename Sec>
    struct UMapTraits<std::unordered_map<Fst, Sec>> {
        static const bool value = true;
    };

    template<typename T>
    constexpr bool is_set = USetTraits<T>::value;
    template<typename T>
    constexpr bool is_map = UMapTraits<T>::value || MapTraits<T>::value;
} // SDB::Taits namespace

namespace Function {
    // en_bytes if basic type
    using boost::spirit::traits::is_container;
    template <typename T>
    inline typename std::enable_if<
            !is_container<T>::value && !Traits::PairTraits<T>::value,
            Type::Bytes>::type
    en_bytes(T t){
        SDB::Type::Bytes bytes = std::vector<char>(sizeof(t));
        std::memcpy(bytes.data(), &t, sizeof(t));
        return bytes;
    }
    //
    template <typename Fst, typename Sec>
    inline Type::Bytes en_bytes(std::pair<Fst, Sec> pair);
    // en_bytes if 'set' type
    template <typename Ctn>
    inline typename std::enable_if<
            is_container<Ctn>::value && !std::is_same<Ctn, Type::Bytes>::value,
            Type::Bytes>::type
    en_bytes(Ctn cnt){
        Type::Bytes bytes;
        Type::Bytes size_bytes = en_bytes(cnt.size());
        bytes.insert(bytes.end(), size_bytes.begin(), size_bytes.end());
        for (auto &&x : cnt) {
            Type::Bytes x_bytes = en_bytes(x);
            bytes.insert(bytes.end(), x_bytes.begin(), x_bytes.end());
        }
        return bytes;
    }
    // en_bytes if pair type
    template <typename Fst, typename Sec>
    inline Type::Bytes en_bytes(std::pair<Fst, Sec> pair){
        Type::Bytes bytes;
        Type::Bytes fst_bytes = en_bytes(pair.first);
        Type::Bytes sec_bytes = en_bytes(pair.second);
        bytes.insert(bytes.end(), fst_bytes.begin(), fst_bytes.end());
        bytes.insert(bytes.end(), sec_bytes.begin(), sec_bytes.end());
        return bytes;
    }

    template <typename T>
    inline typename std::enable_if<Traits::UMapTraits<T>::value || Traits::USetTraits<T>::value, void>::type
    container_append(T &t, typename T::value_type tail) {
        t.insert(tail);
    }
    template <typename T>
    inline typename std::enable_if<Traits::VectorTraits<T>::value 
                                   || Traits::ListTraits<T>::value
                                   || Traits::StringTraits<T>::value, void>::type
    container_append(T &t, typename T::value_type tail) {
        t.push_back(tail);
    }

    template <typename T>
    inline typename std::enable_if<
            !is_container<T>::value && !Traits::PairTraits<T>::value,
            void>::type
    de_bytes(T &t, const Type::Bytes &bytes, size_t &offset){
        std::memcpy(&t, bytes.data()+offset, sizeof(t));
        offset += sizeof(t);
    }

    template <typename Fst, typename Sec>
    inline void db_bytes(std::pair<Fst, Sec> &value, const Type::Bytes &bytes, size_t &offset);

    template <typename T>
    inline typename std::enable_if<is_container<T>::value && !Traits::is_map<T>, void>::type
    de_bytes(T &t, const Type::Bytes &bytes, size_t &offset) {
        size_t len;
        std::memcpy(&len, bytes.data()+offset, Const::SIZE_SIZE);
        offset += Const::SIZE_SIZE;
        for (size_t i = 0; i < len; i++) {
            typename T::value_type value;
            static_assert(!std::is_const<decltype(value)>::value, "ads");
            de_bytes(value, bytes, offset);
            container_append(t, value);
        }
    }
    template <typename T>
    inline typename std::enable_if<Traits::is_map<T>, void>::type
    de_bytes(T &t, const Type::Bytes &bytes, size_t &offset) {
        size_t len;
        std::memcpy(&len, bytes.data()+offset, Const::SIZE_SIZE);
        offset += Const::SIZE_SIZE;
        for (size_t i = 0; i < len; i++) {
            std::remove_const_t<typename T::value_type> value;
            std::remove_const_t<decltype(value.first)> fst_value;
            std::remove_const_t<decltype(value.second)> sec_value;
            de_bytes(fst_value, bytes, offset);
            de_bytes(sec_value, bytes, offset);
            t.insert(std::make_pair(fst_value, sec_value));
        }
    }

    template <typename Fst, typename Sec>
    inline void de_bytes(std::pair<Fst, Sec> &value, const Type::Bytes &bytes, size_t &offset) {
        de_bytes(value.first, bytes, offset);
        de_bytes(value.second, bytes, offset);
    };


    inline void bytes_print(const SDB::Type::Bytes &bytes) {
        for (auto &&item : bytes) {
            std::cout << item;
        }
        std::cout << std::endl;
    }

    template <typename T>
    inline void bytes_append(Type::Bytes &bytes, T &&tail) {
        Type::Bytes tail_bytes = en_bytes(tail);
        bytes.insert(bytes.end(), tail_bytes.begin(), tail_bytes.end());
    }

    size_t get_type_len(Enum::ColType col_type);
    bool is_var_type(Enum::ColType type);

} // SDB::Function namespace

} // SDB namespace

#endif //UTIL_H
