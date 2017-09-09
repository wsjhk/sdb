#include "db_type.h"

namespace sdb::db_type {

// ===== Char =====
bool Char::less(SP<const Object> obj)const {
    if (auto p = dfc<Char>(obj)) {
        return data < p->data;
    } else {
        throw_mismatching(obj, "<");
    }
}

bool Char::eq(SP<const Object> obj)const {
    if (auto p = dfc<Char>(obj)) {
        return data == p->data;
    } else {
        throw_mismatching(obj, "=");
    }
}

void Char::assign(SP<const Object> obj) {
    if (auto p = dfc<Char>(obj)) {
        data = p->data;
    } else {
        throw_mismatching(obj, "=");
    }
}

// ===== Vector =====
Vector::Vector(TypeTag vtt, Size max_size, const std::vector<ObjPtr> &data):vtt(vtt), max_size(max_size) {
    check_size(data.size());
    this->data.clear();
    for (auto ptr : data) {
        this->data.push_back(ptr->clone());
    }
}

Vector::Vector(const TypeInfo &info) {
    // bytes: |type_tag, dependent_type_tag, type_size|
    assert(info.size() >= 2 + sizeof(Size));
    max_size = static_cast<TypeTag>(info[1]);
    Size offset = 2;
    sdb::de_bytes(max_size, info, offset);
}

std::string Vector::to_string()const {
    std::string str;
    for (auto &&x : data) {
        str.append(x->to_string());
    }
    return str;
}

std::shared_ptr<Object> Vector::clone()const {
    auto vec_ptr = std::make_shared<Vector>(vtt, max_size);
    for (auto &&ptr : data) {
        vec_ptr->data.push_back(ptr->clone());
    }
    return vec_ptr;
}

Bytes Vector::en_bytes()const {
    Bytes bytes;
    for (auto &&ptr : data) {
        bytes_append(bytes, ptr->en_bytes());
    }
    return bytes;
}

void Vector::de_bytes(const Bytes &bytes, int &offset) {
    Size size = 0;
    sdb::de_bytes(size, bytes, offset);
    assert(size >= 0 && size <= max_size);
    data.clear();
    for (Size i = 0; i < size ;i++) {
        ObjPtr ptr = get_default(TypeInfo(1, static_cast<char>(vtt)));
        ptr->de_bytes(bytes, size);
        data.push_back(ptr);
    }
}

bool Vector::less(SP<const Object> obj)const {
    if (auto p = dfc<Vector>(obj)) {
        Size len_1 = data.size();
        Size len_2 = p->data.size();
        for (Size i = 0; i < len_1 && i < len_2; i++) {
            if (data[i]->eq(p->data[i])) {
                continue;
            }
            return data[i]->less(p->data[i]);
        }
        return len_1 < len_2 ;
    } else {
        throw_mismatching(obj, "<");
    }
}

bool Vector::eq(SP<const Object> obj)const {
    if (auto p = dfc<Vector>(obj)) {
        if (get_size() != obj->get_size()) return false;
        for (size_t i = 0; i < data.size(); i++) {
            if (!data[i]->eq(p->data[i])) {
                return false;
            }
        }
        return true;
    } else {
        throw DBTypeMismatchingError(get_type_name(), obj->get_type_name(), "==");
    }
}

void Vector::assign(SP<const Object> obj) {
    if (auto p = dfc<Vector>(obj)) {
        if (max_size < p->get_size()) {
            throw DBTypeOverflowError("vector");
        }
        for (size_t i = 0; i < p->data.size(); i++) {
            data[i] = p->data[i]->clone();
        }
    } else {
        throw DBTypeMismatchingError(get_type_name(), obj->get_type_name(), "==");
    }
}

// ===== Varchar =====
Varchar::Varchar(int max_size, const std::string &str):Vector(CHAR, max_size){
    check_size(str.size());
    for (char ch : str) {
        data.push_back(std::make_shared<Char>(ch));
    }
}

void Varchar::assign(SP<const Object> obj) {
    if (auto p = dfc<const Varchar>(obj)) {
        check_size(p->get_size());
        data = p->data;
    } else {
        throw_mismatching(obj, "assign");
    }
}

} // namespace sdb
