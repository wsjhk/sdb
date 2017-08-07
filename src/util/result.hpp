#ifndef UTIL_RESULT_HPP
#define UTIL_RESULT_HPP

#include <utility>
#include <variant>
#include <string>
#include <assert.h>
#include <functional>
#include <iostream>

//=============== Ok =================
template<typename T>
struct Ok {
    Ok()=delete;
    Ok(const T &val):val(val){}
    Ok(T &&val):val(std::move(val)){}

    T val;
};

template<>
struct Ok<void> {};

//=============== Err =================
template<typename T>
struct Err {
    Err()=delete;
    Err(const T &val):val(val){}
    Err(T &&val):val(std::move(val)){}

    T val;
};

//=============== Result =================
template<typename T, typename E>
class Result {
public:
    Result(Ok<T> ok):res(ok){}
    Result(Err<E> err):res(err){}

public:
    bool has_value()const {
        return std::holds_alternative<Ok<T>>(res);
    }

    Ok<T> get_ok() {
        assert(has_value());
        return std::get<0>(res);
    }

    Err<E> get_err() {
        assert(!has_value());
        return std::get<1>(res);
    }

    T get_ok_value() {
        return get_ok().val;
    }

    E get_err_value() {
        return get_err().val;
    }

private:
    std::variant<Ok<T>, Err<E>> res;
};

// ============ macro ===============
// res => Result<T, E>
// ft  => std::function<void(T)>
// fe  => std::function<void(E)>
#define _VpOrEp(res, ft, fe)\
    if (res.has_value()) ft(res.get_ok_value());\
    else fe(res.get_err_value())

#define _VprOrEr(res, ft)\
    if (res.has_value()) return ft(res.get_ok_value());\
    else return res.get_err()

#define _VpOrEr(res, ft)\
    if (res.has_value()) ft(res.get_ok_value());\
    else return res.get_err()

#define _VaOrEr(ass, fe)\
    if (!res.has_value()) return fe(res.get_err_value());\
    ass = res.get_ok_value()

#endif /* ifndef UTIL_RESULT_HPP */
