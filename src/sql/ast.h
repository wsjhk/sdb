#ifndef AST_H
#define AST_H

#include <memory>
#include <utility>
#include <vector>

struct AstNode{
    std::string name;
    std::string type;
    std::vector<std::shared_ptr<AstNode>> children;

    AstNode (){}
    AstNode (const std::string &n):name(n){}
    AstNode (const std::string &n, const std::string &type)
        :name(n), type(type){}
    AstNode (const std::string &n, const std::string &type,
            const std::vector<std::shared_ptr<AstNode>> &c)
        :name(n), type(type), children(c){}
};

using AstNodePtr = std::shared_ptr<AstNode>;

class Ast{
public:
    Ast():root(nullptr){}
    Ast(std::shared_ptr<AstNode> r):root(r){}
    
    void output_graphviz(const std::string &filename)const;
    std::string get_graphviz(std::shared_ptr<AstNode> ptr, int num, const std::string &p_name)const;

    std::shared_ptr<AstNode> get_root() const {
        return root;
    }

private:
    std::shared_ptr<AstNode> root;
};

#endif /* AST_H */
