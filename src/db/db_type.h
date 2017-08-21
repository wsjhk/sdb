#ifndef DB_TYPE
#define DB_TYPE

#include <boost/any.hpp>
#include <boost/format.hpp>
#include <type_traits>
#include <memory>

#include "util.h"
#include "../cpp_util/error.hpp"
#include "../cpp_util/str.hpp"


namespace SDB::DBType {
// util type
using Bytes = Type::Bytes;
template <typename T>
using SP = std::shared_ptr<T>;
template <typename T>
using UP = std::shared_ptr<T>;

using namespace cpp_util;

// type tag
enum TypeTag : char {
    NONE,
    INT,
    UINT,
    VARCHAR
};

struct TypeError {
    enum TypeErrorTag {
        OVER_FLOW,
        NOT_MATCHING,
        DIV_ZERO,
    };

    TypeError(TypeErrorTag tag, std::string desc):tag(tag), desc(desc){}

    TypeErrorTag tag;
    std::string desc;
};

// ========== Object ==========
class Object : public std::enable_shared_from_this<Object> {
public:
    // type
    virtual TypeTag get_type_tag()const =0;
    virtual std::string get_type_name()const =0;
    virtual size_t get_type_size()const =0;

    // show
    virtual std::string to_string()const =0;
    
    // bytes
    virtual Type::Bytes en_bytes()const =0;
    virtual void de_bytes(const Type::Bytes &bytes, size_t &offset)=0;

    // operator
    virtual Result<bool, TypeError> less(SP<const Object> obj)const =0;
    virtual Result<bool, TypeError> eq(SP<const Object> obj)const =0;

    Err<TypeError> ret_mismatching(SP<const Object> r)const{
        auto msg = format("TypeError: %s < %s isn't matching", get_type_name(), r->get_type_name());
        return Err<TypeError>(TypeError(TypeError::NOT_MATCHING, msg));
    }
};

// ===== Integer =====
template <typename T>
struct Integer : public Object {
public:
    Integer():data(0){}
    Integer(T data):data(data){
        static_assert(std::is_integral_v<T>);
    }

    // type
    TypeTag get_type_tag()const override;
    std::string get_type_name()const override;
    size_t get_type_size()const override {return sizeof(T);};
    
    // show
    std::string to_string()const override;

    // bytes
    Type::Bytes en_bytes()const override;
    void de_bytes(const Type::Bytes &bytes, size_t &offset) override {
        Function::de_bytes(data, bytes, offset);
    }

    // operator
    Result<bool, TypeError> less(SP<const Object> obj)const override;
    Result<bool, TypeError> eq(SP<const Object> obj)const override;

    Result<UP<Object>, TypeError> add(SP<const Object> obj)const;
    Result<UP<Object>, TypeError> sub(SP<const Object> obj)const;
    Result<UP<Object>, TypeError> mul(SP<const Object> obj)const;
    Result<UP<Object>, TypeError> div(SP<const Object> obj)const;

    T data;
};

// === Integer === 
using Int = Integer<int32_t>;
using UInt = Integer<uint32_t>;

// === Float ===
// approximate float

// ===== string =====
// string abstract class
class String : public Object {
public:
    String()=default;
    String(const std::string &data):data(data){}

    // show
    std::string to_string()const override{return data;}
    
    // bytes
    Bytes en_bytes()const override{
        return Function::en_bytes(data);
    }
    void de_bytes(const Type::Bytes &bytes, size_t &offset) override {
        Function::de_bytes(data, bytes, offset);
    }

    // operator
    Result<bool, TypeError> less(SP<const Object> obj)const override {
        if (auto p = std::dynamic_pointer_cast<const String>(obj)) {
            return Ok<bool>(data < p->data);
        } else {
            return ret_mismatching(obj);
        }
    }

    Result<bool, TypeError> eq(SP<const Object> obj)const override {
        if (auto p = std::dynamic_pointer_cast<const String>(obj)) {
            return Ok<bool>(data == p->data);
        } else {
            return ret_mismatching(obj);
        }
    }

public:
    std::string data;
};

// Varchar
class Varchar : public String {
public:
    Varchar()=delete;
    Varchar(size_t s):String(), max_size(s){ check_size(s); }
    Varchar(const std::string str, size_t s):String(str), max_size(s){check_size(s);}

    // type
    TypeTag get_type_tag()const override { return VARCHAR; }
    std::string get_type_name()const override { return "varchar"; }
    size_t get_type_size()const override { return data.size(); }

    Result<void, TypeError> check_size(size_t size)const {
        auto msg = format("TypeError: %s > max_size[%s]", size, max_size);
        return Err<TypeError>(TypeError(TypeError::OVER_FLOW, msg));
    }

private:
    size_t max_size;
};

// ========== templat function ==========
// ===== Integer =====
// get type name
template <typename T>
std::string Integer<T>::get_type_name()const{
    if constexpr (std::is_same<T, int32_t>::value){
        return "int";
    } else if constexpr (std::is_same<T, uint32_t>::value){
        return "uint";
    } 
}

template <typename T>
TypeTag Integer<T>::get_type_tag()const{
    if constexpr (std::is_same<T, int32_t>::value){
        return INT;
    } else if constexpr (std::is_same<T, uint32_t>::value){
        return UINT;
    } 
}

template <typename T>
std::string Integer<T>::to_string()const{
    return std::to_string(data);
}

template <typename T>
Type::Bytes Integer<T>::en_bytes()const{
    return Function::en_bytes(data);
}

// operator
template <typename T>
Result<bool, TypeError> Integer<T>::less(SP<const Object> obj)const{
    bool is_signed = std::is_signed_v<T>;
    if (auto p = std::dynamic_pointer_cast<const Integer<int32_t>>(obj)) {
        if (!is_signed && p->data < 0) {
            return Ok<bool>(false);
        } else {
            return Ok<bool>(data < p->data);
        }
    } else if (auto p = std::dynamic_pointer_cast<const Integer<uint32_t>>(obj)) {
        if (is_signed && data < 0) {
            return Ok<bool>(true);
        } else {
            return Ok<bool>(data < p->data);
        }
    } else {
        return ret_mismatching(obj);
    }
}

template <typename T>
Result<bool, TypeError> Integer<T>::eq(SP<const Object> obj)const{
    if (auto p =  std::dynamic_pointer_cast<const Integer<int32_t>>(obj)) {
        return Ok<bool>(data == p->data);
    } else {
        return ret_mismatching(obj);
    }
}

template <typename T>
Result<UP<Object>, TypeError> Integer<T>::add(SP<const Object> obj)const{
    if (auto p = std::dynamic_pointer_cast<const Integer<T>>(obj)) {
        T res = data + p->data;
        if ((data > 0 && p->data > 0 && res < data) || (data < 0 && p->data < 0 && res > data)){
            auto msg = format("TypeError: %s + %s overflow", data, p->data);
            return Err<TypeError>(TypeError(TypeError::OVER_FLOW, msg));
        }
        return Ok<UP<Object>>(std::make_unique<Integer<T>>(res));
    } else {
        return ret_mismatching(obj);
    }
}

template <typename T>
Result<UP<Object>, TypeError> Integer<T>::sub(SP<const Object> obj)const{
    if (auto p = std::dynamic_pointer_cast<const Integer<T>>(obj)) {
        T res = data + p->data;
        if ((data > 0 && p->data > 0 && res > data)
                || (data > 0 && p->data < 0 && res < 0) 
                || (data < 0 && p->data > 0 && res > 0)) {
            auto msg = format("TypeError: %s - %s overflow", data, p->data);
            return Err<TypeError>(TypeError(TypeError::OVER_FLOW, msg));
        }
        return Ok<UP<Object>>(std::make_unique<Integer<T>>(res));
    } else {
        return ret_mismatching(obj);
    }
}

template <typename T>
Result<UP<Object>, TypeError> Integer<T>::mul(SP<const Object> obj)const{
    if (auto p = std::dynamic_pointer_cast<const Integer<T>>(obj)) {
        if (INT_MAX / std::abs(p->data) > std::abs(data)) {
            auto msg = format("TypeError: %s * %s overflow", data, p->data);
            return Err<TypeError>(TypeError(TypeError::OVER_FLOW, msg));
        }
        return Ok<UP<Object>>(std::make_unique<Integer<T>>(data * p->data));
    } else {
        return ret_mismatching(obj);
    }
}

template <typename T>
Result<UP<Object>, TypeError> Integer<T>::div(SP<const Object> obj)const{
    if (auto p = std::dynamic_pointer_cast<const Integer<T>>(obj)) {
        if (p->data == 0) {
            auto msg = format("TypeError: div By 0");
            return Err<TypeError>(TypeError(TypeError::OVER_FLOW, msg));
        }
        return Ok<UP<Object>>(std::make_unique<Integer<T>>(data / p->data));
    } else {
        return ret_mismatching(obj);
    }
}

} // SDB::Type namespace

namespace SDB::Function {

} // SDB::Function namespace
#endif /* ifndef DB_TYPE */
