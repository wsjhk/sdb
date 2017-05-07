#ifndef DB_TYPE
#define DB_TYPE

#include "util.h"
#include <boost/any.hpp>
#include <boost/format.hpp>
#include <type_traits>

namespace SDB::Enum {
    enum ColType : char {
        TINYINT,
        SMALLINT,
        INT,
        BIGINT,
        VARCHAR
    };
}

namespace SDB::Type {

using TypeSizePair = std::pair<Enum::ColType, size_t>;

// ========== db type list ==========
// int => Number<int32_t>
// float => Float<float>
// double => Float<double>
// ==================================

// ===== Integer =====
template <typename T>
class Integer {
public:
    Integer()=default;
    Integer(T data):data(data){}

    // getter
    std::string get_type_name()const;
    size_t get_type_size()const{return sizeof(data);};
    std::string get_generic_type_name()const{return "Integer";}
    std::string to_string()const;
    Bytes en_bytes()const;
    static Integer de_bytes(const Bytes &bytes, Pos &offset);
    T get_data()const{return data;}

    // operator
    template <typename NT>
    bool operator<(const Integer<NT> &num)const;
    template <typename NT>
    bool operator==(const Integer<NT> &num)const;
    template <typename NT>
    bool operator<=(const Integer<NT> &num)const;
    template <typename NT>
    auto operator+(const Integer<NT> &num)const;
    // unary operator-
    Integer operator-()const;
    template <typename NT>
    auto operator-(const Integer<NT> &num)const;
    template <typename NT>
    auto operator*(const Integer<NT> &num)const;
    template <typename NT>
    auto operator/(const Integer<NT> &num)const;

    // PT -> patamer type
    template <typename PT>
    static bool is_integral(const PT &x){
        return std::is_integral<decltype(x.get_data())>::value;
    }
    template <typename PT>
    static void check_is_integeral(const PT &x){
        if (!is_integral(x)){
            throw std::runtime_error("Error: is not is_integral type");
        }
    }


private:
    T data;
};

// === Integer === 
using TinyInt = Integer<int8_t>;
using SmallInt = Integer<int16_t>;
using Int = Integer<int32_t>;
using BigInt = Integer<int64_t>;
// === Float ===
// approximate float

// ===== string =====
// string abstract class
class String {
public:
    String()=default;
    String(const std::string &data):data(data){}

    std::string to_string()const{return data;}
    std::string get_data()const{return data;}
    Bytes en_bytes()const{
        Bytes bytes = Function::en_bytes(data.size());
        bytes.insert(bytes.end(), data.begin(), data.end());
        return bytes;
    }

protected:
    std::string data;
};

// Varchar
class Varchar : public String {
public:
    Varchar()=delete;
    Varchar(size_t s):String(), max_size(s){ check_size(s); }
    Varchar(const std::string str, size_t s):String(str), max_size(s){check_size(s);}
    bool operator<(const Varchar &vc)const{ return data < vc.get_data(); }
    bool operator==(const Varchar &vc)const{ return !(*this < vc && vc < *this); }
    bool operator<=(const Varchar &vc)const{ return *this < vc || *this == vc; }

    // getter
    std::string get_type_name()const{return "Varchar";}
    std::string get_generic_type_name()const{return "String";}
    void check_size(size_t size)const;

    // bytes
    static Varchar de_bytes(const Bytes &bytes, Pos &offset);

private:
    size_t max_size;
};

// === Type Operator ===
template<typename T>
bool operator<(const Varchar &, const Integer<T> &){
    throw std::runtime_error("Error: Varchar < Integer");
}
template<typename T>
bool operator<(const Integer<T>&, const Varchar&){
    throw std::runtime_error("Error: Integer < Varchar");
}

// abstract db value
struct Value {
    // data
    boost::any data;

    // function
    Value()= delete;
    template <typename T>
    Value(T data):data(data){}
    static Value make(Enum::ColType col_type, size_t size){
        switch (col_type) {
            case Enum::TINYINT:
                return Value(TinyInt());
            case Enum::SMALLINT:
                return Value(SmallInt());
            case Enum::INT:
                return Value(Int());
            case Enum::BIGINT:
                return Value(BigInt());
            case Enum::VARCHAR:
                return Value(Varchar(size));
            default:
                throw std::runtime_error("Error: Value::make: type");
        }
        throw std::runtime_error("Error: Value::make: type");
    }

    // value_op
    template <typename Func>
    auto value_op(Func op)const {
        const std::type_info &type = data.type();
        if (type == typeid(TinyInt)) {
            return op(boost::any_cast<TinyInt>(data));
        } else if (type == typeid(SmallInt)){
            return op(boost::any_cast<SmallInt>(data));
        } else if (type == typeid(Int)){
            return op(boost::any_cast<Int>(data));
        } else if (type == typeid(BigInt)){
            return op(boost::any_cast<BigInt>(data));
        } else if (type == typeid(Varchar)){
            return op(boost::any_cast<Varchar>(data));
        } else {
            throw std::runtime_error("Error: type switch error");
        }
    }

    template <typename Func>
    static auto value_op(const Value &v1, const Value &v2, Func op){
        auto f1 = [v2, op](auto &&x){
            auto f2 = [x, op](auto &&y){
                return op(x, y);
            };
            return v2.value_op(f2);
        };
        return v1.value_op(f1);
    }

    // operator
    friend bool operator<(const Value &v1, const Value &v2);
    friend bool operator==(const Value &v1, const Value &v2);
    friend bool operator<=(const Value &v1, const Value &v2);
    friend Value operator+(const Value &v1, const Value &v2);
    friend Value operator-(const Value &v1, const Value &v2);
    friend Value operator*(const Value &v1, const Value &v2);
    friend Value operator/(const Value &v1, const Value &v2);

    // getter
    std::string get_string()const{
        return value_op([this](auto x){ return str_ret(x);});
    }
    std::string get_type_name()const{
        return value_op([](auto &&x){ return x.get_type_name();});
    }
    std::string get_generic_type_name()const{
        return value_op([](auto &&x){ return x.get_generic_type_name();});
    }

    // bytes
    Bytes en_bytes()const{
        return value_op([](auto &&x){return x.en_bytes();});
    }
    static void de_bytes(Value &v, const Bytes &bytes, Pos &offset){
        auto f = [bytes, &offset](auto x){
            return Value(decltype(x)::de_bytes(bytes, offset));
        };
        v = v.value_op(f);
    }
    static Value de_bytes(Enum::ColType type, size_t size, const Bytes &bytes, Pos &offset){
        Value value = make(type, size);
        de_bytes(value, bytes, offset);
        return value;
    }

private:
    template <typename T> std::string str_ret(T t)const{
        return t.to_string();
    }
};

using BVFunc = std::function<bool(Value)>;
using VVFunc = std::function<Value(Value)>;

// ========== Property =========
struct ColProperty {
    std::string col_name;
    Enum::ColType col_type;
    size_t type_size;
    Value default_value;
    char is_not_null;

    ColProperty()=delete;
    ColProperty(const std::string &col_name, Enum::ColType col_type, size_t type_size)
        :col_name(col_name), col_type(col_type), type_size(type_size),
         default_value(Value::make(col_type, type_size)), is_not_null(1){}
    ColProperty(const std::string col_name, 
                Enum::ColType col_type,
                size_t type_size,
                const Value &dv,
                char is_not_null)
        :col_name(col_name), col_type(col_type),
         type_size(type_size), default_value(dv),
         is_not_null(is_not_null){}

    Bytes en_bytes()const;
    static ColProperty de_bytes(const Bytes &bytes, Pos &offset){
        // cal name
        std::string col_name;
        Function::de_bytes(col_name, bytes, offset);
        // type
        Enum::ColType type;
        Function::de_bytes(type, bytes, offset);
        // type_size
        size_t type_size;
        Function::de_bytes(type_size, bytes, offset);
        // default value
        Value value = Type::Value::de_bytes(type, type_size, bytes, offset);
        // is not null
        char is_not_null = false;
        Function::de_bytes(is_not_null, bytes, offset);
        return ColProperty(col_name, type, type_size, value, is_not_null);
    }
};

// table property
struct TableProperty {
    // Type
    std::string db_name;
    std::string table_name;
    std::string key;
    std::vector<ColProperty> col_property_lst;
    // integrity
    // <table_name, col_name>
    std::unordered_map<std::string, std::string> referencing_map;
    // <table_name, col_name>
    std::unordered_map<std::string, std::string> referenced_map;

    TableProperty(){}
    TableProperty(const std::string &db_name,
                  const std::string &table_name,
                  const std::string &key,
                  const std::vector<ColProperty> &col_property_lst)
            :db_name(db_name), table_name(table_name), key(key), col_property_lst(col_property_lst){}

    // getter
    size_t get_col_property_pos(const std::string &col_name)const;
    size_t get_type_size(const std::string &col_name)const;
    Enum::ColType get_col_type(const std::string &col_name)const;
    std::vector<std::string> get_col_name_lst()const;
    std::vector<Enum::ColType> get_col_type_lst()const;
    std::vector<TypeSizePair> get_tsp_lst()const;
};

struct TupleData {
    std::vector<Value> value_lst;

    // value
    static size_t get_col_name_pos(const std::vector<std::string> &col_name_lst, const std::string &col_name);
    Value get_value(size_t pos)const;
    Value get_value(const std::vector<std::string> &col_name_lst, const std::string &col_name)const;
    void set_value(size_t pos, const Value &new_value);
    void set_value(size_t pred_pos, size_t op_pos, BVFunc pred, VVFunc op);
    // bytes
    Bytes en_bytes()const;
    static TupleData de_bytes(const std::vector<TypeSizePair> &tsp, const Bytes &bytes, Pos &offset);
};

// ========== Tuple ==========
struct Tuple {
    std::vector<std::string> col_name_lst;
    TupleData data;

    // value
    size_t get_col_name_pos(const std::string &col_name);
    Value get_value(const std::string &col_name)const;
    void set_value(const std::string &col_name, const Value &new_value);
};

struct TupleLst {
    TupleLst()=delete;
    TupleLst(const std::vector<std::string> col_name_lst):col_name_lst(col_name_lst){}
    std::vector<TupleData> data;
    std::vector<std::string> col_name_lst;

    // bytes
    Bytes en_bytes()const;
    static TupleLst de_bytes(const std::vector<TypeSizePair> &tsp_lst, const Bytes &bytes, Pos &offset);

    void print()const;
};

// ========== templat function ==========
// ===== Integer =====
// get type name
template <typename T>
std::string Integer<T>::get_type_name()const{
    if constexpr (std::is_same<T, int8_t>::value){
        return "tinyint";
    } else if constexpr (std::is_same<T, int8_t>::value){
        return "smallint";
    } else if constexpr (std::is_same<T, int32_t>::value){
        return "int";
    } else if constexpr (std::is_same<T, int64_t>::value){
        return "bigint";
    } else {
        return "null";
    }
}

template <typename T>
std::string Integer<T>::to_string()const{
    return std::to_string(data);
}

template <typename T>
Bytes Integer<T>::en_bytes()const{
    return Function::en_bytes(data);
}
template <typename T>
Integer<T> Integer<T>::de_bytes(const Bytes &bytes, Pos &offset){
    T x;
    Function::de_bytes(x, bytes, offset);
    return Integer(x);
}

// operator
template <typename T>
template <typename NT>
bool Integer<T>::operator<(const Integer<NT> &num)const{
    return data < num.get_data();
}
template <typename T>
template <typename NT>
bool Integer<T>::operator==(const Integer<NT> &num)const{
    return !(*this < num && num < *this);
}
template <typename T>
template <typename NT>
bool Integer<T>::operator<=(const Integer<NT> &num)const{
    return *this < num && *this == num;
}
template <typename T>
template <typename NT>
auto Integer<T>::operator+(const Integer<NT> &num)const{
    auto res = data + num.get_data();
    if ((data > 0 && num.get_data() > 0 && res < 0) || (data < 0 && num.get_data() < 0 && res > 0)){
        throw std::runtime_error("Error: operator+ overflow");
    }
    return Integer<decltype(res)>(res);
}
template <typename T>
Integer<T> Integer<T>::operator-()const{
    return Integer<T>(-data);
}
template <typename T>
template <typename NT>
auto Integer<T>::operator-(const Integer<NT> &num)const{
    return *this + (-num);
}
template <typename T>
template <typename NT>
auto Integer<T>::operator*(const Integer<NT> &num)const{
    auto max = std::numeric_limits<NT>::max();
    if (max / std::abs(data) > std::abs(num.get_data())) {
        throw std::runtime_error("Error: operator* overflow");
    }
    return Integer(data * num.get_data());
}
template <typename T>
template <typename NT>
auto Integer<T>::operator/(const Integer<NT> &num)const{
    if (num.get_data() == 0) {
        throw std::runtime_error("Error: operator/ num can't equal 0!");
    }
    return data / num.get_data();
}

} // SDB::Type namespace

namespace SDB::Function {

bool is_var_type(Enum::ColType type);
Type::BVFunc get_bvfunc(Enum::BVFunc func, Type::Value value);
void tuple_lst_map(Type::TupleLst &tuple_lst, const std::string &col_name, Type::VVFunc);

} // SDB::Function namespace
#endif /* ifndef DB_TYPE */
