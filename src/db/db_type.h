#ifndef DB_TYPE
#define DB_TYPE

#include <boost/any.hpp>
#include <boost/format.hpp>
#include <type_traits>
#include <memory>

#include "util.h"
#include "../cpp_util/error.hpp"
#include "../cpp_util/str.hpp"


namespace sdb::db_type {
using cpp_util::format;

// alias type
template <typename T>
using SP = std::shared_ptr<T>;

template <typename T, typename U>
inline std::shared_ptr<T> dfc(U &&u) {
    return std::dynamic_pointer_cast<T>(std::forward<U>(u));
}

// type tag
enum TypeTag : char {
    NONE,
    INT,
    UINT,
    VARCHAR
};

struct DBTypeError : public std::runtime_error {
    DBTypeError(const std::string &str)
        :runtime_error(str){}
};

struct DBTypeOverflowError : public DBTypeError {
    DBTypeOverflowError(const std::string &str)
        :DBTypeError(str){}
};

struct DBTypeMismatchingError : public DBTypeError {
    DBTypeMismatchingError(const std::string &lv, const std::string &rv, const std::string &op)
        :DBTypeError(format("TypeError: %s %s %s isn't matching", lv, op, rv)){}
};

struct DBTypeDivzeroError : public DBTypeError {
    DBTypeDivzeroError()
        :DBTypeError(format("TypeError: div By 0")){}
};

struct DBTypeNullError : public DBTypeError {
    DBTypeNullError()
        :DBTypeError(format("TypeError: null")){}
};

// ========== Object ==========
class Object : public std::enable_shared_from_this<Object> {
public:
    // type
    virtual TypeTag get_type_tag()const =0;
    virtual std::string get_type_name()const =0;
    virtual int get_type_size()const =0;

    // show
    virtual std::string to_string()const =0;

    // clone
    virtual std::shared_ptr<Object> clone()const =0;
    
    // bytes
    virtual Bytes en_bytes()const =0;
    virtual void de_bytes(const Bytes &bytes, int &offset)=0;

    // operator
    virtual bool less(SP<const Object> obj)const =0;
    virtual bool eq(SP<const Object> obj)const =0;
};

// object alias
using ObjPtr = SP<Object>;
using ObjCntPtr = SP<const Object>;

// ===== Null =====
struct None : public Object {
    TypeTag get_type_tag()const override {return NONE;}
    std::string get_type_name()const override {return "null";}
    int get_type_size()const override {return 0;}

    // show
    std::string to_string()const override {return "null";}
    
    // clone
    std::shared_ptr<Object> clone()const override {
        return std::make_shared<None>();
    }

    // bytes
    Bytes en_bytes()const override {return Bytes();}
    void de_bytes(const Bytes &, int &)override{}

    // operator
    bool less(SP<const Object>)const override{
        throw DBTypeNullError();
    }

    bool eq(SP<const Object>)const override{
        throw DBTypeNullError();
    }
};

// ===== Integer =====
template <typename T>
struct Integer : public Object {
public:
    Integer():data(0){}
    explicit Integer(T data):data(data){
        static_assert(std::is_integral_v<T>);
    }

    // type
    TypeTag get_type_tag()const override;
    std::string get_type_name()const override;
    int get_type_size()const override {return sizeof(T);};
    
    // show
    std::string to_string()const override {
        return std::to_string(data);
    }

    // clone
    std::shared_ptr<Object> clone()const override {
        return std::make_shared<Integer<T>>(data);
    }

    // bytes
    Bytes en_bytes()const override {
        return sdb::en_bytes(data);
    }
    void de_bytes(const Bytes &bytes, int &offset) override {
        sdb::de_bytes(data, bytes, offset);
    }

    // operator
    bool less(SP<const Object> obj)const override;
    bool eq(SP<const Object> obj)const override;

    SP<Object> add(SP<const Object> obj)const;
    SP<Object> sub(SP<const Object> obj)const;
    SP<Object> mul(SP<const Object> obj)const;
    SP<Object> div(SP<const Object> obj)const;

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
        return sdb::en_bytes(data);
    }
    void de_bytes(const Bytes &bytes, int &offset) override {
        sdb::de_bytes(data, bytes, offset);
    }

    // operator
    bool less(SP<const Object> obj)const override {
        if (auto p = dfc<const String>(obj)) {
            return data < p->data;
        } else {
            throw DBTypeMismatchingError(get_type_name(), obj->get_type_name(), "<");
        }
    }

    bool eq(SP<const Object> obj)const override {
        if (auto p = dfc<const String>(obj)) {
            return data == p->data;
        } else {
            throw DBTypeMismatchingError(get_type_name(), obj->get_type_name(), "=");
        }
    }

public:
    std::string data;
};

// Varchar
class Varchar : public String {
public:
    Varchar()=delete;
    Varchar(int s):String(), max_size(s){}
    Varchar(const std::string &str, int s):String(str), max_size(s){check_size(str.size());}

    // type
    TypeTag get_type_tag()const override { return VARCHAR; }
    std::string get_type_name()const override { return "varchar"; }
    int get_type_size()const override { return data.size(); }

    // clone
    std::shared_ptr<Object> clone()const override {
        return std::make_shared<Varchar>(data, max_size);
    }
    

    void check_size(int size)const {
        if (size > max_size || size < 0) {
            auto msg = format("TypeError: %s > max_size[%s]", size, max_size);
            throw DBTypeOverflowError(msg);
        }
    }

private:
    int max_size;
};

// ========== type function ==========
static ObjPtr get_default(TypeTag tag, int size) {
    switch (tag) {
        case INT:
            return std::make_shared<Int>();
        case UINT: 
            return std::make_shared<UInt>();
        case VARCHAR:
            return std::make_shared<Varchar>(size);
        default:
            return std::make_shared<None>();
    }
}

// ========== templat function ==========
// ===== Integer =====
// get type name
template <typename T>
std::string Integer<T>::get_type_name()const{
    if constexpr (std::is_same<T, int32_t>::value){
        return "int";
    } else if constexpr (std::is_same<T, uint32_t>::value){
        return "uint";
    } else {
        static_assert(Traits::always_false<T>::value);
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

// operator
template <typename T>
bool Integer<T>::less(SP<const Object> obj)const{
    if (auto p = dfc<const Integer<T>>(obj)) {
        return data < p->data;
    } else {
        throw DBTypeMismatchingError(get_type_name(), obj->get_type_name(), "<");
    }
}

template <typename T>
bool Integer<T>::eq(SP<const Object> obj)const{
    if (auto p = dfc<const Integer<T>>(obj)) {
        return data == p->data;
    } else {
        throw DBTypeMismatchingError(get_type_name(), obj->get_type_name(), "=");
    }
}

template <typename T>
SP<Object> Integer<T>::add(SP<const Object> obj)const{
    if (auto p = dfc<const Integer<T>>(obj)) {
        T res = data + p->data;
        if ((data > 0 && p->data > 0 && res < data) || (data < 0 && p->data < 0 && res > data)){
            auto msg = format("TypeError: %s + %s overflow", data, p->data);
            throw DBTypeOverflowError(msg);
        }
        return std::make_unique<Integer<T>>(res);
    } else {
        throw DBTypeMismatchingError(get_type_name(), obj->get_type_name(), "+");
    }
}

template <typename T>
SP<Object> Integer<T>::sub(SP<const Object> obj)const{
    if (auto p = dfc<const Integer<T>>(obj)) {
        T res = data - p->data;
        if ((data > 0 && p->data > 0 && res > data)
            || (data > 0 && p->data < 0 && res < 0) 
            || (data < 0 && p->data > 0 && res > 0)) {
            auto msg = format("TypeError: %s - %s overflow", data, p->data);
            throw DBTypeOverflowError(msg);
        }
        return std::make_unique<Integer<T>>(res);
    } else {
        throw DBTypeMismatchingError(get_type_name(), obj->get_type_name(), "-");
    }
}

template <typename T>
SP<Object> Integer<T>::mul(SP<const Object> obj)const{
    if (auto p = dfc<const Integer<T>>(obj)) {
        if (std::numeric_limits<T>::max() / std::abs(p->data) < std::abs(data)) {
            auto msg = format("TypeError: %s * %s overflow", data, p->data);
            throw DBTypeOverflowError(msg);
        }
        return std::make_unique<Integer<T>>(data * p->data);
    } else {
        throw DBTypeMismatchingError(get_type_name(), obj->get_type_name(), "*");
    }
}

template <typename T>
SP<Object> Integer<T>::div(SP<const Object> obj)const{
    if (auto p = dfc<const Integer<T>>(obj)) {
        if (p->data == 0) {
            throw DBTypeDivzeroError();
        }
        return std::make_unique<Integer<T>>(data / p->data);
    } else {
        throw DBTypeMismatchingError(get_type_name(), obj->get_type_name(), "/");
    }
}

} // sdb::DBType namespace

#endif /* ifndef DB_TYPE */
