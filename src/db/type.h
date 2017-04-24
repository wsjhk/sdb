#ifndef DB_TYPE
#define DB_TYPE

#include "util.h"

namespace SDB::Type {

struct Value {
    // data
    SDB::Enum::ColType type;
    Bytes data;

    // function
    Value()= delete;
    Value(SDB::Enum::ColType type, Bytes data)
            :type(type), data(data){}

    // type switch
    template <typename Func>
    static auto type_switch(Enum::ColType col_type, Func op) {
        using namespace Enum;
        using namespace Const;
        switch (col_type){
            case INT:{
                Int i = 0;
                op(i);
                break;
            }
            case FLOAT:{
                Float f = 0.0;
                op(f);
                break;
            }
            case VARCHAR:{
                std::string str;
                op(str);
                break;
            }
        }
    }

    // value_op
    template <typename Func>
    static auto value_op(const Value &value1, const Value &value2, Func op) {
        using namespace Enum;
        using namespace Const;
        if (value1.type != value2.type) {
            throw std::runtime_error(
                    std::string("Error: type {0} {1} can't call op function")
            );
        }
        switch (value1.type) {
            case INT:
                Int i1;
                Int i2;
                std::memcpy(&i1, value1.data.data(), Const::INT_SIZE);
                std::memcpy(&i2, value2.data.data(), Const::INT_SIZE);
                return op(i1, i2);
            case FLOAT:
                float f1;
                float f2;
                std::memcpy(&f1, value1.data.data(), Const::FLOAT_SIZE);
                std::memcpy(&f2, value2.data.data(), Const::FLOAT_SIZE);
                return op(f1, f2);
            case VARCHAR:
                std::string v1(value1.data.begin(), value1.data.end());
                std::string v2(value2.data.begin(), value2.data.end());
                return op(v1, v2);
        }
    }
    template <typename Func>
    auto value_op(const Value &val, Func op) {
        using namespace Enum;
        using namespace Const;
        auto func = [this, op](auto x){op(*this, x);};
        val.value_op(func);
    }

    template <typename Func>
    static auto number_value_op(const Value &value1, const Value &value2, Func op){
        using namespace Enum;
        using namespace Const;
        if (value1.type != value2.type) {
            throw std::runtime_error(
                    std::string("Error: <value_op> type {0} {1} can't call op function")
            );
        }
        switch (value1.type) {
            case INT:
                Int i1;
                Int i2;
                std::memcpy(&i1, value1.data.data(), Const::INT_SIZE);
                std::memcpy(&i2, value2.data.data(), Const::INT_SIZE);
                return op(i1, i2);
            case FLOAT:
                float f1;
                float f2;
                std::memcpy(&f1, value1.data.data(), Const::FLOAT_SIZE);
                std::memcpy(&f2, value2.data.data(), Const::FLOAT_SIZE);
                return op(f1, f2);
            default:
                throw std::runtime_error("Error: type must be number type!");
        }
    }
    template <typename Func>
    auto value_op(Func op) const{
        using namespace Enum;
        using namespace Const;
        switch (type) {
            case INT:
                Int i;
                std::memcpy(&i, data.data(), Const::INT_SIZE);
                return op(i);
            case FLOAT:
                float f;
                std::memcpy(&f, data.data(), Const::FLOAT_SIZE);
                return op(f);
            case VARCHAR:
                std::string v(data.begin(), data.end());
                return op(v);
        }
    }

    // convert type
    template <typename T>
    void number_convert(Enum::ColType t){
        if (!is_convertiblity(type, t)){
            std::runtime_error("Error: type convert");
        }
        auto func = [this, t](auto x){
            auto f = [this, t, x](auto y){
                this->type = t;
                this->data = Function::en_bytes(static_cast<decltype(x)>(y));
            };
            this->value_op(f);
        };
        type_switch(t, func);
    }

    // make
    template <typename T>
    static Value make(Enum::ColType col_type, T data) {
        size_t type_size = sizeof(data);
        Bytes bytes(type_size);
        std::memcpy(bytes.data(), &data, type_size);
        return Value(col_type, bytes);
    }
    static Value make(Enum::ColType col_type, std::string data) {
        Bytes bytes(data.begin(), data.end());
        return Value(col_type, bytes);
    }
    static Value make(Enum::ColType col_type, Bytes data) {
        Bytes bytes(data.begin(), data.end());
        return Value(col_type, bytes);
    }
    static Value make(Enum::ColType col_type) {
        Bytes bytes;
        auto func = [&bytes](auto x){bytes = Function::en_bytes(x);};
        type_switch(col_type, func);
        return Value(col_type, bytes);
    }

    static Value str_to_value(Enum::ColType col_type, const std::string &str);

    // operator
    friend bool operator<(const Value &val, const Value &val2) {
        return value_op(val, val2, [](auto x, auto y){ return x<y;});
    }
    friend bool operator==(const Value &value1, const Value &value2) {
        return value_op(value1, value2, [](auto v1, auto v2){ return v1==v2;});
    }
    friend bool operator<=(const Value &value1, const Value &value2) {
        return value1 < value2 || value1 == value2;
    }
    friend Value operator+(const Value &value1, const Value &value2) {
        return number_value_op(value1, value2, [=](auto x, auto y){
            return make(value1.type, x+y);
        });
    }
    friend Value operator-(const Value &value1, const Value &value2) {
        return number_value_op(value1, value2, [=](auto x, auto y){
            return make(value1.type, x-y);
        });
    }
    friend Value operator*(const Value &value1, const Value &value2) {
        return number_value_op(value1, value2, [=](auto x, auto y){
            return make(value1.type, x*y);
        });
    }
    friend Value operator/(const Value &value1, const Value &value2) {
        return number_value_op(value1, value2, [=](auto x, auto y){
            return make(value1.type, x/y);
        });
    }

    // getter
    std::string get_string()const{
        return value_op([=](auto x){ return str_ret(x);});
    }

    // bool 
    static bool is_var_type(Enum::ColType col_type){
        return col_type == Enum::VARCHAR;
    }
    bool is_var_type()const{
        return is_var_type(type);
    }
    bool is_convertiblity(Enum::ColType t1, Enum::ColType T2){
        return true;
    }

private:
    template <typename T> std::string str_ret(T t)const{
        return std::to_string(t);
    }
    std::string str_ret(std::string str)const {
        return str;
    }
};

using BVFunc = std::function<bool(Value)>;
using VVFunc = std::function<Value(Value)>;

// ========== Property ==========
struct TupleProperty{
    // type
    struct ColProperty {
        ColProperty(const std::string &col_name, Enum::ColType col_type, size_t type_size)
                :col_name(col_name), col_type(col_type), type_size(type_size){}
        std::string col_name;
        Enum::ColType col_type;
        size_t type_size;
    };
    // data member
    std::vector<ColProperty> property_lst;

    // function
    Enum::ColType get_col_type(const std::string &col_name)const;
    size_t get_type_size(const std::string &col_name)const;
    ColProperty get_col_property(const std::string &col_name)const;
    void push_back(const std::string &col_name, Enum::ColType col_type, size_t type_size);
};

struct TableProperty {
    // Type
    std::string db_name;
    std::string table_name;
    std::string key;
    TupleProperty tuple_property;
    // integrity
    // <table_name, col_name>
    std::unordered_map<std::string, std::string> referencing_map;
    // <table_name, col_name>
    std::unordered_map<std::string, std::string> referenced_map;
    std::unordered_set<std::string> not_null_set;

    TableProperty(){}
    TableProperty(const std::string &db_name,
                  const std::string &table_name,
                  const std::string &key,
                  const TupleProperty &col_property)
            :db_name(db_name), table_name(table_name), key(key), tuple_property(col_property){}
};

struct Tuple {
    std::vector<Value> value_lst;

    Tuple()= default;
    Value get_col_value(const TupleProperty &tuple_property, const std::string &col_name)const;
    Value &get_col_value_ref(const TupleProperty &tuple_property, const std::string &col_name);
    void set_col_value(const TupleProperty &property, const std::string &col_name, const Value &value);
    void set_col_value(const TupleProperty &property, const std::string &col_name, VVFunc op);
    void set_col_value(const TupleProperty &property, const std::string &pred_col_name, BVFunc predicate,
                                   const std::string &op_col_name, VVFunc op);
};

struct TupleLst {
    std::vector<Tuple> tuple_lst;
    TupleProperty tuple_property;

    TupleLst()= delete;
    TupleLst(const TupleProperty &tuple_property):tuple_property(tuple_property){}

    void print()const;
};

} // namespace SDB::Type

namespace SDB::Function {

Type::BVFunc get_bvfunc(Enum::BVFunc func, Type::Value value);
void tuple_lst_map(Type::TupleLst &tuple_lst, const std::string &col_name, Type::VVFunc);

} // SDB::Function namespace about


#endif /* ifndef DB_TYPE */
