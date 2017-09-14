//
// Created by sven on 17-2-19.
//

#ifndef DB_UTIL_H
#define DB_UTIL_H

#include <vector>
#include <string>
#include <iostream>
#include <map>
#include <set>
#include <list>
#include <unordered_map>
#include <unordered_set>
#include <boost/spirit/home/support/container.hpp>

#include "../cpp_util/error.hpp"

namespace sdb {

// const
// static const Size BLOCK_SIZE = size_t(getpagesize());
// size
using Size = int32_t;

constexpr Size BLOCK_SIZE = 4096;

// - Pos -
using BlockNum = int64_t;
using BlockOffset = Size;

// bytes
using Byte = char;
using Bytes = std::vector<Byte>;

// transaction
using Tid = int64_t;
class Snapshot;
struct TransInfo {
    Tid id = -1;
    enum Level : char { READ, R_READ } level = READ;
    std::shared_ptr<Snapshot> s_ptr;
};

// traits
namespace Traits {
    // pair
    template <typename T>
    struct PairTraits {
        static const bool value = false;
    };
    template <typename Fst, typename Sec>
    struct PairTraits<std::pair<Fst, Sec>> {
        static const bool value = true;
    };

    // unordered_map
    template <typename T>
    struct UMapTraits {
        static const bool value = false;
    };
    template <typename Fst, typename Snd>
    struct UMapTraits<std::unordered_map<Fst, Snd>> {
        static const bool value = true;
    };

    // map
    template <typename T>
    struct MapTraits {
        static const bool value = false;
    };
    template <typename Fst, typename Snd>
    struct MapTraits<std::map<Fst, Snd>> {
        static const bool value = true;
    };

    // unordered_set
    template <typename T>
    struct USetTraits {
        static const bool value = false;
    };
    template <typename T>
    struct USetTraits<std::unordered_set<T>> {
        static const bool value = true;
    };

    // set
    template <typename T>
    struct SetTraits {
        static const bool value = false;
    };
    template <typename T>
    struct SetTraits<std::set<T>> {
        static const bool value = true;
    };

    // always_false
    template <typename T>
    struct always_false : std::false_type{};

    template<typename T>
    constexpr bool is_set_v = SetTraits<T>::value || USetTraits<T>::value;

    template<typename T>
    constexpr bool is_map_v = MapTraits<T>::value || UMapTraits<T>::value;
} // SDB::Taits namespace

using boost::spirit::traits::is_container;

template <typename T>
inline Bytes en_bytes(T t) {
    Bytes bytes;
    if constexpr (std::is_same_v<std::remove_reference_t<T>, std::string>) {
        Bytes size_bytes = en_bytes(static_cast<Size>(t.size()));
        bytes.insert(bytes.end(), size_bytes.begin(), size_bytes.end());
        bytes.insert(bytes.end(), t.begin(), t.end());
    } else if constexpr (is_container<T>::value) {
        Bytes size_bytes = en_bytes(static_cast<Size>(t.size()));
        bytes.insert(bytes.end(), size_bytes.begin(), size_bytes.end());
        for (auto &&x : t) {
            Bytes x_bytes = en_bytes(x);
            bytes.insert(bytes.end(), x_bytes.begin(), x_bytes.end());
        }
    } else if constexpr (Traits::PairTraits<T>::value) {
        Bytes fst_bytes = en_bytes(t.first);
        Bytes snd_bytes = en_bytes(t.second);
        bytes.insert(bytes.end(), fst_bytes.begin(), fst_bytes.end());
        bytes.insert(bytes.end(), snd_bytes.begin(), snd_bytes.end());
    } else if constexpr (std::is_fundamental_v<T>) {
        Bytes t_b(sizeof(t));
        std::memcpy(t_b.data(), &t, sizeof(t));
        bytes.insert(bytes.end(), t_b.begin(), t_b.end());
    } else {
        static_assert(Traits::always_false<T>::value);
    }
    return bytes;
}

template <typename T, typename ...Args>
inline Bytes en_bytes(T &&t, Args... args) {
    Bytes bytes = en_bytes(t);
    Bytes append_bytes = en_bytes(args...);
    bytes.insert(bytes.end(), append_bytes.begin(), append_bytes.end());
    return bytes;
}

// === de_bytes ===
inline void _bytes_length_check(Size size, const Bytes &bytes, Size offset){
    assert(size <= bytes.size() - offset);
}

template <typename T>
inline void container_append(T &t, typename T::value_type tail) {
    if constexpr(!is_container<T>::value) {
    } else if constexpr (Traits::is_set_v<T> || Traits::is_map_v<T>) {
        t.insert(tail);
    } else {
        t.push_back(tail);
    }
}

template <typename T>
inline void de_bytes(T &t, const Bytes &bytes, Size &offset) {
    if constexpr (std::is_fundamental_v<T>) {
        _bytes_length_check(sizeof(T), bytes, offset);
        std::memcpy(&t, bytes.data() + offset, sizeof(T));
        offset += sizeof(T);
    } else if constexpr (std::is_same_v<T, std::string>) {
        _bytes_length_check(sizeof(Size), bytes, offset);
        Size size;
        de_bytes(size, bytes, offset);
        assert(size <= bytes.size() - offset);
        t.resize(size, '\0');
        std::memcpy(t.data(), bytes.data()+offset, size);
        offset += size;
    } else if constexpr (Traits::PairTraits<T>::value) {
        // for std::pair<const F, S> in map and set
        std::remove_const_t<decltype(t.first)> fst;
        de_bytes(fst, bytes, offset);
        decltype(t.first) fst_copy = fst;
        decltype(t.second) snd;
        de_bytes(snd, bytes, offset);
        t = {fst_copy, snd};
    } else if constexpr (is_container<T>::value) {
        _bytes_length_check(sizeof(Size), bytes, offset);
        Size size;
        de_bytes(size, bytes, offset);
        assert(size >= 0);
        t.clear();
        for (Size i = 0; i < size; i++) {
            if constexpr (Traits::is_map_v<T>) {
                std::remove_const_t<typename T::value_type::first_type> fst;
                typename T::value_type::second_type snd;
                de_bytes(fst, bytes, offset);
                de_bytes(snd, bytes, offset);
                container_append(t, std::make_pair(fst, snd));
            } else {
                typename T::value_type val;
                de_bytes(val, bytes, offset);
                container_append(t, val);
            }
        }
    } else {
        static_assert(Traits::always_false<T>::value);
    }
}

template <typename ...Args>
inline void bytes_append(Bytes &bytes, Args... args) {
    Bytes append_bytes = en_bytes(args...);
    bytes.insert(bytes.end(), append_bytes.begin(), append_bytes.end());
}

} // SDB namespace

#endif // DB_UTIL_H
