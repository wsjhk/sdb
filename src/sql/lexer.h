#ifndef PARSER_LEXER_HPP
#define PARSER_LEXER_HPP

#include <string>
#include <vector>
#include <utility>
#include <unordered_set>
#include <unordered_map>
#include <boost/lexical_cast.hpp>
#include <functional>


class Lexer{
public:
    Lexer():punctuation_char(get_punctuation_set()), reserved_set(get_reserved_set()){}
    Lexer(const Lexer &lexer){*this=lexer;}
    Lexer &operator=(const Lexer &lexer);

    // special set
    bool is_punctuation_char(char ch);
    bool is_reserved_word(const std::string &str);
    bool is_type_word(const std::string &str);
    
    // tokenozie
    std::vector<std::pair<std::string, std::string>> tokenize(const std::string &str);
    // 状态处理函数
    std::pair<std::string, std::string> identifier_process();
    std::pair<std::string, std::string> punctuation_procerss();

    // number
    std::pair<std::string, std::string> number_process();
    std::string number_float_process();
    // string
    std::pair<std::string, std::string> string_process();

    // 注释部分
    std::pair<std::string, std::string> div_and_comment_process();
    std::pair<std::string, std::string> minus_and_comment_process();

    // iter
    bool is_end()const{return iter == iter_end;}
    char get_char()const{return *iter;}
    char next_char(){return *iter++;}

    // get set
    const std::unordered_set<char> get_punctuation_set() {
        return std::unordered_set<char>{
             ',', '$', ':', ';', '*', '.', '+', '<', '>','=', '(', ')'
        };
    }

    const std::unordered_set<std::string> get_reserved_set() {
        return std::unordered_set<std::string>{
            "all", "alter", "and", "any", "array", "arrow", "as",
            "asc", "at", "begin", "between", "by", "case", "check",
            "clusters", "cluster", "colauth", "columns", "compress", "connect", "crash",
            "create", "current", "decimal", "declare", "default", "delete", "desc",
            "distinct", "drop", "else", "end", "exception", "exclusive", "exists",
            "fetch", "form", "for", "from", "goto", "grant", "group",
            "having", "identified", "if", "in", "indexes", "index", "insert",
            "intersect", "into", "is", "like", "lock", "minus", "mode",
            "nocompress", "not", "nowait", "null", "of", "on", "option",
            "or", "order,overlaps", "prior", "procedure", "public", "range", "record",
            "resource", "revoke", "select", "share", "size", "sql", "start",
            "subtype", "tabauth", "table", "then", "to", "type", "union",
            "unique", "update", "use", "values", "view", "views", "when",
            "where", "with"
        };
    }

    const std::unordered_set<std::string> get_type_set() {
        return std::unordered_set<std::string>{
            "int", "smallint", "float", "char", "varchar", "numberic"
        };
    }

private:
    // token集，lexer返回的值
    std::vector<std::pair<std::string, std::string>> tokens;

    // set
    const std::unordered_set<char> punctuation_char;
    const std::unordered_set<std::string> reserved_set;
    const std::unordered_set<std::string> type_set;

    // iter
    std::string::const_iterator iter;
    std::string::const_iterator iter_end;
};

#endif
