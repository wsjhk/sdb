#ifndef DB_DB_TYPE_H
#define DB_DB_TYPE_H

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
    CHAR,
    INT,
    UINT,
    BIGINT,
    VECTOR,
    VARCHAR,
};

using TypeInfo = Bytes;

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
    virtual int get_size()const =0;

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
    
    // assignment
    virtual void assign(SP<const Object> obj) =0;

    void throw_mismatching(std::shared_ptr<const Object> obj, const std::string &op) const {
        throw DBTypeMismatchingError(get_type_name(), obj->get_type_name(), op);
    }
};

// object alias
using ObjPtr = SP<Object>;
using ObjCntPtr = SP<const Object>;
// functional
using ObjPred = std::function<bool(std::shared_ptr<const Object>)>;
// using ObjMap = std::function<std::shared_ptr<Object>(std::shared_ptr<const Object>)>;
// using ObjInplaceMap = std::function<void(std::shared_ptr<Object>)>;
using ObjCntOp = std::function<void(std::shared_ptr<const Object>)>;


// ===== Null =====
struct None : public Object {
    TypeTag get_type_tag()const override {return NONE;}
    std::string get_type_name()const override {return "null";}
    int get_type_size()const override {return 0;}
    int get_size()const override {return 0;}

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

    void assign(SP<const Object>) override{
        throw DBTypeNullError();
    }
};

// ===== Char =====
class Char : public Object {
    explicit Char(char ch):data(ch){}

    TypeTag get_type_tag()const override {return CHAR;}
    std::string get_type_name()const override {return "char";}
    int get_type_size()const override {return 1;}
    int get_size()const override {return 1;}

    // show
    std::string to_string()const override {return std::string(1, data);}
    
    // clone
    std::shared_ptr<Object> clone()const override {
        return std::make_shared<Char>(data);
    }

    // bytes
    Bytes en_bytes()const override {return sdb::en_bytes(data);}
    void de_bytes(const Bytes &bytes, Size &offset)override{
        sdb::de_bytes(data, bytes, offset);
    }

    // operator
    bool less(SP<const Object> obj)const override;
    bool eq(SP<const Object>)const override;

    void assign(SP<const Object>) override;

private:
    char data;
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
    int get_size()const override {return sizeof(T);};
    
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

    void assign(SP<const Object> obj) override;

    T data;
};

// === Integer === 
using Int = Integer<int32_t>;
using UInt = Integer<uint32_t>;
using BigInt = Integer<uint64_t>;

// === Float ===
// approximate float
//

// ===== List ===== 
class Vector : public Object {
public:
    Vector()=delete;
    Vector(TypeTag vtt, Size max_size):vtt(vtt), max_size(max_size){}
    Vector(TypeTag vtt, Size max_size, const std::vector<ObjPtr> &data);
    Vector(const TypeInfo &info);

    // type
    TypeTag get_type_tag()const override {return VECTOR;}
    std::string get_type_name()const override {return "vector";}
    Size get_type_size()const override {
        return max_size * get_type_size(vtt);
    }
    Size get_size()const override {
        return data.size() * get_type_size(vtt);
    };

    // show
    std::string to_string()const override;

    // clone
    std::shared_ptr<Object> clone()const override;
    
    // bytes
    Bytes en_bytes()const override;
    void de_bytes(const Bytes &bytes, int &offset) override;

    // operator
    bool less(SP<const Object> obj)const override;
    bool eq(SP<const Object> obj)const override;

    // assignment
    void assign(SP<const Object> obj) override;
    void check_size(Size size)const {
        if (size > max_size || size < 0) {
            auto msg = format("TypeError: %s > max_size[%s]", size, max_size);
            throw DBTypeOverflowError(msg);
        }
    }

private:
    static Size get_type_size(TypeTag tt);

public:
    std::vector<ObjPtr> data;
private:
    TypeTag vtt;
    Size max_size;
};

// ===== string =====
// string abstract class
// Varchar
class Varchar : public Vector {
public:
    Varchar()=delete;
    Varchar(int max_size):Vector(CHAR, max_size) {}
    Varchar(int max_size, const std::string &str);
    Varchar(const TypeInfo &info):Vector(info){}

    // type
    TypeTag get_type_tag()const override { return VARCHAR; }
    std::string get_type_name()const override { return "varchar"; }
    Size get_type_size()const override { return Vector::get_type_size(); }
    Size get_size()const override { return Vector::get_size(); }

    // clone
    std::shared_ptr<Object> clone()const override {
        return std::make_shared<Varchar>(get_type_size(), data);
    }

    void assign(SP<const Object> obj) override;
};

// ========== type function ==========
static ObjPtr get_default(TypeInfo type_info) {
    TypeTag tag = static_cast<TypeTag>(type_info[0]);
    switch (tag) {
        case INT:
            return std::make_shared<Int>();
        case UINT: 
            return std::make_shared<UInt>();
        case BIGINT: 
            return std::make_shared<BigInt>();
        case VARCHAR:
            return std::make_shared<Varchar>(type_info);
        case VECTOR:
            return std::make_shared<Vector>(type_info);
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
    } else if constexpr (std::is_same<T, int64_t>::value){
        return "bigint";
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
    } else if constexpr (std::is_same<T, int64_t>::value){
        return BIGINT;
    }  else {
        static_assert(Traits::always_false<T>::value);
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

template <typename T>
void Integer<T>::assign(SP<const Object> obj) {
    if (auto p = dfc<const Integer<T>>(obj)) {
        data = p->data;
    } else {
        throw DBTypeMismatchingError(get_type_name(), obj->get_type_name(), "<");
    }
}

} // sdb::db_types namespace

namespace sdb {
    template <>
    inline Bytes en_bytes(db_type::ObjCntPtr ptr) {
        return ptr->en_bytes();
    }
}



#endif /* ifndef DB_DB_TYPE_H */
