代码现分为两个主要的layer，DB Layer实现数据库的储存引擎，SQL Layer包含SQL的解析|优化|执行．

### DB layer:

+ src/db/block_alloc: 磁盘块的分配管理。

+ src/db/bptree: B+Tree(B-link Tree)的实现，支持针对主键增删查改。

+ src/db/cache: 块缓冲器，实现了读写时间复杂度都为O(1)的LRU缓冲算法。

+ src/db/db: 整个系统的管理。

+ src/db/db_type: 数据库类型系统，支持Int/UInt/BigInt/Varchar。

+ src/db/io: 实现文件的io操作,包括增删读写文件，利用mmap实现的按块读写，配合索引提高随机读写效率。

+ src/db/property: 表结构属性。

+ src/db/record: 实现对记录的增删查改,支持可变长类型数据，但记录的长度不能超过Block的长度。

+ src/db/snapshot: 快照管理，为事务提供快照隔离机制（块级别）。

+ src/db/table: 表结构的实现，支持表的创建和删除，以及对记录的增删查改，支持谓词判断的查询。

+ src/db/temp: 临时空间创建，用于支持查询物化等需。

+ src/db/tlog: 日志机制的实现。

+ src/db/tuple: 数据元组，行数据。

+ src/db/util: 常用类型、函数集(如： de_bytes, en_bytes)

### SQL Layer

1.  SQL Parser(src/sql/parser)

    手写的SQL递归下降语法分析器,暂时只支持少量DDL/DML,完整的实现需要等到DB Layer成熟之后开始实现.

2.  SQL Optimize

    SQL的优化,尚未实现.

3.  SQL Executor(sql/sql/executor)

    todo ...
