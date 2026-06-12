# SQL 查询的生命周期

原作者: Andrei Matei (2021年6月更新)

## 引言

本文档旨在解释 CockroachDB 中一条 SQL 查询的执行过程，追踪代码路径穿过系统的各个层次：网络协议、SQL 会话管理、查询解析、规划与优化、执行、键值服务、事务管理、请求路由、请求处理、Raft 共识、磁盘存储引擎等。目的是提供各个组件结构的高层次统一视图；不会深入探讨任何组件的细节，但在有相关文档的地方会提供指向其他文档的链接。各处都将出现代码指针。

本文档通常不会讨论设计决策，而是专注于追踪实际（当前）代码。为简洁起见，只涵盖查询执行中最重要和最常见的部分，省略了大量细节和特殊情况。

目标读者是对现代数据库架构走读感兴趣的人，以不同于[设计文档](https://github.com/cockroachdb/cockroach/blob/master/docs/design.md)的方式呈现。希望它对开源贡献者和 Cockroach Labs 新工程师也有所帮助。

## PostgreSQL 客户端协议

SQL 查询通过 [PostgreSQL 有线协议](https://www.postgresql.org/docs/current/protocol.html)到达服务器。CockroachDB 使用此协议以兼容现有的客户端驱动和应用程序。

该协议由 [`pgwire` 包](https://github.com/cockroachdb/cockroach/tree/9c86c8f937e3c5d4728079d2bdb2a98943a7d872/pkg/sql/pgwire)实现。一旦客户端[连接](https://github.com/cockroachdb/cockroach/blob/4e4f31a0ed1a8ea985b9ab6f72e29266b259900e/pkg/server/server.go#L2250)，它由 [`pgwire.conn`](https://github.com/cockroachdb/cockroach/blob/89621764d4c2d438d1781238f10e9ef27ef2c392/pkg/sql/pgwire/conn.go#L55) 表示，该结构封装了一个 [`net.Conn`](https://golang.org/pkg/net/#Conn) 套接字接口。[`conn.serveImpl`](https://github.com/cockroachdb/cockroach/blob/89621764d4c2d438d1781238f10e9ef27ef2c392/pkg/sql/pgwire/conn.go#L211) 实现了主["读取、解析、执行"循环](https://github.com/cockroachdb/cockroach/blob/992812623e3719c25659f0a0d1c2b169fb196f76/pkg/sql/pgwire/conn.go#L327)：在连接的整个生命周期中，我们[读取](https://github.com/cockroachdb/cockroach/blob/9c86c8f937e3c5d4728079d2bdb2a98943a7d872/pkg/sql/pgwire/conn.go#L330)一条消息（通常包含一条或多条 SQL 语句），[解析](https://github.com/cockroachdb/cockroach/blob/9c86c8f937e3c5d4728079d2bdb2a98943a7d872/pkg/sql/pgwire/conn.go#L737)查询，将其[传递](https://github.com/cockroachdb/cockroach/blob/9c86c8f937e3c5d4728079d2bdb2a98943a7d872/pkg/sql/pgwire/conn.go#L779)给 `connExecutor` 执行，[序列化并缓冲](https://github.com/cockroachdb/cockroach/blob/9c86c8f937e3c5d4728079d2bdb2a98943a7d872/pkg/sql/pgwire/conn.go#L1229)结果，然后[发送](https://github.com/cockroachdb/cockroach/blob/89621764d4c2d438d1781238f10e9ef27ef2c392/pkg/sql/pgwire/conn.go#L1451)结果消息给客户端。

## SQL 处理

CockroachDB 的 SQL 引擎位于客户端连接和核心键值服务之间，将 SQL 查询转换为键值操作。它大致经过以下阶段：

解析 → 逻辑规划与优化 → 物理规划 → 执行

### 解析

解析将原始 SQL 语句（以 `string` 表示）转换为[抽象语法树](https://en.wikipedia.org/wiki/Abstract_syntax_tree)（AST），以便于处理。CockroachDB 使用 [LALR 解析器](https://en.wikipedia.org/wiki/LALR_parser)，该解析器由 [goyacc](https://pkg.go.dev/golang.org/x/tools/cmd/goyacc) 从类 Yacc 的[语法文件](https://github.com/cockroachdb/cockroach/blob/d96babaa3d484d4df156293010f922bd3fb2d970/pkg/sql/parser/sql.y)[生成](https://github.com/cockroachdb/cockroach/blob/60916fb31b1bb0562d1707a338aad536e4713744/Makefile#L1506-L1507)。该文件指定了 CockroachDB 的 SQL 方言，最初从 PostgreSQL 复制后进行了修改。

如前所述，`pgwire.conn` 使用 [`parser.Parser`](https://github.com/cockroachdb/cockroach/blob/31dcbfc964f22ede5b7f827f4e6c0091ccfb132f/pkg/sql/parser/parse.go#L81) 实例[解析](https://github.com/cockroachdb/cockroach/blob/9c86c8f937e3c5d4728079d2bdb2a98943a7d872/pkg/sql/pgwire/conn.go#L737)客户端语句。解析器首先将字符串[扫描](https://github.com/cockroachdb/cockroach/blob/31dcbfc964f22ede5b7f827f4e6c0091ccfb132f/pkg/sql/parser/parse.go#L167)为令牌——单独的语法单元，例如[字符串](https://github.com/cockroachdb/cockroach/blob/231953322e558dada2ae75c1b49771eee544aba2/pkg/sql/parser/scan.go#L133-L135)或[数字](https://github.com/cockroachdb/cockroach/blob/231953322e558dada2ae75c1b49771eee544aba2/pkg/sql/parser/scan.go#L389-L392)。它[逐个](https://github.com/cockroachdb/cockroach/blob/31dcbfc964f22ede5b7f827f4e6c0091ccfb132f/pkg/sql/parser/parse.go#L153)扫描，直到[遇到分号](https://github.com/cockroachdb/cockroach/blob/31dcbfc964f22ede5b7f827f4e6c0091ccfb132f/pkg/sql/parser/parse.go#L154)终止语句。接着，这些令牌经过[词法分析](https://github.com/cockroachdb/cockroach/blob/31dcbfc964f22ede5b7f827f4e6c0091ccfb132f/pkg/sql/parser/parse.go#L186)进行简单的后处理，词法分析器被[传递](https://github.com/cockroachdb/cockroach/blob/31dcbfc964f22ede5b7f827f4e6c0091ccfb132f/pkg/sql/parser/parse.go#L188)给生成的解析器，解析器遍历令牌并对其应用 SQL 语法。

此过程的结果是一个 [`parser.Statement`](https://github.com/cockroachdb/cockroach/blob/31dcbfc964f22ede5b7f827f4e6c0091ccfb132f/pkg/sql/parser/parse.go#L35)，它[封装](https://github.com/cockroachdb/cockroach/blob/31dcbfc964f22ede5b7f827f4e6c0091ccfb132f/pkg/sql/parser/parse.go#L37)了一个 AST [`tree.Statement`](https://github.com/cockroachdb/cockroach/blob/6349791f060743b515c7638b81ab3de2ee7bc0c4/pkg/sql/sem/tree/stmt.go#L86)，一个建模 SQL 语法的树形结构。例如，[`tree.SelectClause`](https://github.com/cockroachdb/cockroach/blob/8b14ea8b7109d61ef9ba3a099a24468ae8367be5/pkg/sql/sem/tree/select.go#L78-L79) 建模了熟悉的 `SELECT` 子句，具有可识别的字段如 `From` 和 `Where`。AST 的许多部分还包含 [`tree.Expr`](https://github.com/cockroachdb/cockroach/blob/31dcbfc964f22ede5b7f827f4e6c0091ccfb132f/pkg/sql/sem/tree/expr.go#L27)，嵌套的代数表达式如 `tax / total * 100`。

请注意，AST 仅表示查询的_语法_，并不说明如何执行，甚至是否能执行——例如，它不知道表是否存在或列的数据类型是什么。解决这些问题是规划器的工作。

### 会话管理

一旦 `pgwire.conn` 解析了语句，它被放入 `sql.ExecStmt` 命令并[推入](https://github.com/cockroachdb/cockroach/blob/9c86c8f937e3c5d4728079d2bdb2a98943a7d872/pkg/sql/pgwire/conn.go#L744)[语句缓冲区](https://github.com/cockroachdb/cockroach/blob/9c86c8f937e3c5d4728079d2bdb2a98943a7d872/pkg/sql/conn_io.go#L89)。该缓冲区的另一端是一个 [`sql.connExecutor`](https://github.com/cockroachdb/cockroach/blob/31db44da69bf21e67ebbef6fbb8c8bfb2e498efe/pkg/sql/conn_executor.go#L988)，它在[连接建立](https://github.com/cockroachdb/cockroach/blob/9c86c8f937e3c5d4728079d2bdb2a98943a7d872/pkg/sql/pgwire/conn.go#L655-L656)期间创建。它通过反复调用 [`execCmd()`](https://github.com/cockroachdb/cockroach/blob/31db44da69bf21e67ebbef6fbb8c8bfb2e498efe/pkg/sql/conn_executor.go#L1525) 来[处理](https://github.com/cockroachdb/cockroach/blob/31db44da69bf21e67ebbef6fbb8c8bfb2e498efe/pkg/sql/conn_executor.go#L1502)语句缓冲区中的命令，[执行](https://github.com/cockroachdb/cockroach/blob/31db44da69bf21e67ebbef6fbb8c8bfb2e498efe/pkg/sql/conn_executor.go#L1578)任何语句。

当 [`execStmt()`](https://github.com/cockroachdb/cockroach/blob/fc67a0c9202af348e919afc1e1f70acc9a83b300/pkg/sql/conn_executor_exec.go#L69) 执行语句时，它会记录各种[会话数据](https://github.com/cockroachdb/cockroach/blob/31db44da69bf21e67ebbef6fbb8c8bfb2e498efe/pkg/sql/conn_executor.go#L1167)，如当前数据库和用户定义的设置，并[实现](https://github.com/cockroachdb/cockroach/blob/fc67a0c9202af348e919afc1e1f70acc9a83b300/pkg/sql/conn_executor_exec.go#L101)了一个[有限状态机](https://en.wikipedia.org/wiki/Finite-state_machine)来跟踪[事务状态](https://github.com/cockroachdb/cockroach/blob/31db44da69bf21e67ebbef6fbb8c8bfb2e498efe/pkg/sql/conn_executor.go#L1027)：事务刚刚开始还是结束，或者我们遇到了错误？然后根据 SQL 事务的状态进行分支：[打开](https://github.com/cockroachdb/cockroach/blob/fc67a0c9202af348e919afc1e1f70acc9a83b300/pkg/sql/conn_executor_exec.go#L244)、[已中止](https://github.com/cockroachdb/cockroach/blob/fc67a0c9202af348e919afc1e1f70acc9a83b300/pkg/sql/conn_executor_exec.go#L1198)、[等待重试](https://github.com/cockroachdb/cockroach/blob/fc67a0c9202af348e919afc1e1f70acc9a83b300/pkg/sql/conn_executor_exec.go#L1252)或[无](https://github.com/cockroachdb/cockroach/blob/fc67a0c9202af348e919afc1e1f70acc9a83b300/pkg/sql/conn_executor_exec.go#L1148)。

简单语句主要在这里处理。例如，[`BEGIN` 语句](https://github.com/cockroachdb/cockroach/blob/fc67a0c9202af348e919afc1e1f70acc9a83b300/pkg/sql/conn_executor_exec.go#L1152)将[打开](https://github.com/cockroachdb/cockroach/blob/af9e6460c83a812ac320d3329f4c55a9b94367a1/pkg/sql/txn_state.go#L190)键值服务中的一个新 [`kv.Txn` 事务](https://github.com/cockroachdb/cockroach/blob/4e4f31a0ed1a8ea985b9ab6f72e29266b259900e/pkg/kv/txn.go#L46)（稍后会再讨论），而 [`COMMIT` 语句](https://github.com/cockroachdb/cockroach/blob/fc67a0c9202af348e919afc1e1f70acc9a83b300/pkg/sql/conn_executor_exec.go#L467-L470)将[提交](https://github.com/cockroachdb/cockroach/blob/fc67a0c9202af348e919afc1e1f70acc9a83b300/pkg/sql/conn_executor_exec.go#L776)该事务。如果查询在事务外运行，它将使用[隐式事务](https://github.com/cockroachdb/cockroach/blob/fc67a0c9202af348e919afc1e1f70acc9a83b300/pkg/sql/conn_executor_exec.go#L1183)。查询执行使用此事务来分发键值请求。

### 语句执行

现在我们知道了我们在哪个（KV）事务中运行，接下来关心的是逐条执行语句。

在 SQL `connExecutor` 代码（面向流，语句在 SQL 事务范围内逐条执行）和 CockroachDB 的键值（KV）接口（面向请求，事务显式附加到每个请求）之间存在阻抗不匹配。

提示一下复杂性：在 CockroachDB 中，[有时需要](https://www.cockroachlabs.com/docs/stable/transactions.html#transaction-retries)重试语句，通常是由于可串行化冲突或数据竞争。在 SQL 事务外执行的单条 SQL 语句（即隐式事务）可以自动重试。但是，跨越多个客户端语句的 SQL 事务重试是不安全的，因为它们可能取决于客户端逻辑（即不同的 `SELECT` 结果可能触发不同的后续语句）。在这种情况下，我们将[可重试错误](https://www.cockroachlabs.com/docs/stable/transactions.html#client-side-intervention)冒泡给客户端。

`connExecutor` 在 SQL 语句执行期间充当不同组件之间的协调器。这主要发生在 [`execStmtInOpenState()`](https://github.com/cockroachdb/cockroach/blob/fc67a0c9202af348e919afc1e1f70acc9a83b300/pkg/sql/conn_executor_exec.go#L244) 中，它通过 [`dispatchToExecutionEngine()`](https://github.com/cockroachdb/cockroach/blob/fc67a0c9202af348e919afc1e1f70acc9a83b300/pkg/sql/conn_executor_exec.go#L806) 分发到执行引擎。在此过程中，它[构建并优化](https://github.com/cockroachdb/cockroach/blob/fc67a0c9202af348e919afc1e1f70acc9a83b300/pkg/sql/conn_executor_exec.go#L831)逻辑计划，然后[构建并执行](https://github.com/cockroachdb/cockroach/blob/fc67a0c9202af348e919afc1e1f70acc9a83b300/pkg/sql/conn_executor_exec.go#L925-L927)物理计划。执行后，语句[结果](https://github.com/cockroachdb/cockroach/blob/31db44da69bf21e67ebbef6fbb8c8bfb2e498efe/pkg/sql/conn_executor.go#L1565)通过 [`ClientComm`](https://github.com/cockroachdb/cockroach/blob/9c86c8f937e3c5d4728079d2bdb2a98943a7d872/pkg/sql/conn_io.go#L576) 返回给客户端。

### 逻辑规划与优化

现在我们来讨论 SQL 引擎的核心。查询规划是将 SQL 查询 AST 转换为一棵[关系代数](https://en.wikipedia.org/wiki/Relational_algebra)操作符树的过程，该树将产生所需的结果。对于给定的查询，可以有大量功能等价的执行计划，但性能特征却大相径庭，优化器需要尝试找出最高效的计划。查询规划是一个复杂的主题，本身就可以填满数篇文章，因此我们在此不做深入探讨——请参阅 [`opt` 包文档](https://github.com/cockroachdb/cockroach/blob/c097a16427f65e9070991f062716d222ea5903fe/pkg/sql/opt/doc.go)了解更多细节。

可以通过 `EXPLAIN` 来查看 SQL 语句的查询计划。让我们看一个具体例子：

```sql
CREATE TABLE countries (id STRING PRIMARY KEY, name STRING);
INSERT INTO countries VALUES
    ('us', 'United States'), ('cn', 'China'), ('de', 'Germany');
CREATE TABLE companies (
    name STRING,
    country STRING REFERENCES countries(id),
    employees INT
);
INSERT INTO companies VALUES
    ('McDonalds', 'us', 1.9e6),
    ('Apple', 'us', 0.15e6),
    ('State Grid', 'cn', 1.5e6),
    ('Sinopec', 'cn', 0.58e6),
    ('Volkswagen', 'de', 0.67e6);

EXPLAIN
    SELECT companies.name AS company, countries.name AS country, employees
    FROM companies JOIN countries ON companies.country = countries.id
    WHERE employees > 1e6
    ORDER BY employees DESC;

  o sort
  | estimated row count: 3
  | order: -employees
  |
  +-- o lookup join
      | estimated row count: 3
      | table: countries@primary
      | equality: (country) = (id)
      | equality cols are key
      |
      +-- o filter
          | estimated row count: 3
          | filter: employees > 1000000
          |
          +-- o scan
                estimated row count: 5 (100% of the table; stats collected 12 seconds ago)
                table: companies@primary
                spans: FULL SCAN
```

计划中的每个节点都是一个关系操作符，结果从一个节点自下而上流向另一个节点。我们可以看到优化器决定对 `companies` 表进行全表 `scan`，通过 `filter` 对 `employees` 进行行过滤，与 `countries` 表执行 `lookup join`，最后按 `employees` 降序 `sort` 结果。优化器也可以选择使用 e.g. `hash join`，或者在 `scan` 之后立即 `sort`，但它认为此计划更高效。

`connExecutor` 使用 [`sql.planner`](https://github.com/cockroachdb/cockroach/blob/5ae7430a1bc5497229386d124a62e75e2805b8ae/pkg/sql/planner.go#L132) 并对其[调用 `makeOptimizerPlan()`](https://github.com/cockroachdb/cockroach/blob/fc67a0c9202af348e919afc1e1f70acc9a83b300/pkg/sql/conn_executor_exec.go#L950) 来构建计划。在内部，它首先将 AST [转换](https://github.com/cockroachdb/cockroach/blob/df67aa9707fbf0193ec8b3ca4062240c360fc808/pkg/sql/plan_opt.go#L194)为"memo"。

[`memo.Memo`](https://github.com/cockroachdb/cockroach/blob/a15efbfec8704526cd6475dd4a5ebdd0be149124/pkg/sql/opt/memo/memo.go#L107) 是一种数据结构，可以通过[记忆化](https://en.wikipedia.org/wiki/Memoization)高效地表示优化器探索的所有不同可能的查询计划树。简而言之，它将每个查询表达式（无论是关系表达式 `opt.RelExpr` 如 `scan`，还是标量表达式 `opt.ScalarExpr` 如 `=`）表示为一个"memo 组"，组与组之间有引用关系。然后优化器可以将每个组展开为其他等价表达式，使得每个组代表实现相同结果的多种不同方法，并尝试找出最优的那个。由于它使用组之间的引用，因此可以为每个组计算并存储一次等价表达式，而不是为所有可能的组合递归地重复计算。例如，考虑以下查询和对应的 memo 组：

```
SELECT * FROM companies, countries WHERE companies.country = countries.id

G6: [inner-join [G1 G2 G5]]
G5: [eq [G3 G4]]
G4: [variable countries.id]
G3: [variable companies.country]
G2: [scan countries]
G1: [scan companies]
```

由于有多种不同的连接方式，我们可以展开 G6 来表示多种可能的计划，但无需为每个连接重复和重新计算其他组：

```
G6: [inner-join [G1 G2 G5]] [inner-join [G2 G1 G5]] [lookup-join [G1 G2 G5]] ...
G5: [eq [G3 G4]]
G4: [variable countries.id]
G3: [variable companies.country]
G2: [scan countries]
G1: [scan companies]
```

[`buildExecMemo()`](https://github.com/cockroachdb/cockroach/blob/df67aa9707fbf0193ec8b3ca4062240c360fc808/pkg/sql/plan_opt.go#L463) 首先使用 [`optbuilder.Builder`](https://github.com/cockroachdb/cockroach/blob/fff814fca119dbc37f88ea368a554f1613096c9a/pkg/sql/opt/optbuilder/builder.go#L59) 从 AST [构建](https://github.com/cockroachdb/cockroach/blob/df67aa9707fbf0193ec8b3ca4062240c360fc808/pkg/sql/plan_opt.go#L521)一个初始的规范化 memo。如果我们继续追踪内部调用，最终会到达一个有趣的地方：[`buildSelectClause()`](https://github.com/cockroachdb/cockroach/blob/2e5cce372e402faa2155c6f6e85410a9c7bdd238/pkg/sql/opt/optbuilder/select.go#L924)，它将 `tree.SelectClause` AST 中的 `SELECT` 子句转换为一组 memo 组。让我们继续深入[构建 `FROM` 子句](https://github.com/cockroachdb/cockroach/blob/2e5cce372e402faa2155c6f6e85410a9c7bdd238/pkg/sql/opt/optbuilder/select.go#L931)，直到到达它[构建表名](https://github.com/cockroachdb/cockroach/blob/2e5cce372e402faa2155c6f6e85410a9c7bdd238/pkg/sql/opt/optbuilder/select.go#L73)的地方。它首先在系统目录中[解析](https://github.com/cockroachdb/cockroach/blob/2e5cce372e402faa2155c6f6e85410a9c7bdd238/pkg/sql/opt/optbuilder/select.go#L103)表名，例如确保表存在且用户具有访问权限。然后使用 memo 工厂[构造一个 `ScanExpr`](https://github.com/cockroachdb/cockroach/blob/2e5cce372e402faa2155c6f6e85410a9c7bdd238/pkg/sql/opt/optbuilder/select.go#L543) 用于扫描。类似地，[构建 `WHERE` 子句](https://github.com/cockroachdb/cockroach/blob/2e5cce372e402faa2155c6f6e85410a9c7bdd238/pkg/sql/opt/optbuilder/select.go#L934)将[解析并构建](https://github.com/cockroachdb/cockroach/blob/2e5cce372e402faa2155c6f6e85410a9c7bdd238/pkg/sql/opt/optbuilder/select.go#L1052)一个谓词的 `ScalarExpr` 并将其[放置](https://github.com/cockroachdb/cockroach/blob/2e5cce372e402faa2155c6f6e85410a9c7bdd238/pkg/sql/opt/optbuilder/select.go#L1063)在 `FiltersExpr` 中。

其他 AST 节点以类似的方式构建。特别值得注意的是，标量表达式（代数表达式如 `tax / total * 100`）最初由嵌套的 `tree.Expr` AST 节点表示，并实现[访问者模式](https://en.wikipedia.org/wiki/Visitor_pattern)进行分析和转换。这些表达式可以在查询的许多部分中找到（e.g. `SELECT`、`WHERE` 和 `ORDER BY`），需要通用的处理。这通常发生在 [`resolveAndBuildScalar()`](https://github.com/cockroachdb/cockroach/blob/2e5cce372e402faa2155c6f6e85410a9c7bdd238/pkg/sql/opt/optbuilder/util.go#L415) 中，它递归地[解析名称和类型](https://github.com/cockroachdb/cockroach/blob/2e5cce372e402faa2155c6f6e85410a9c7bdd238/pkg/sql/opt/optbuilder/util.go#L429)以生成 `tree.TypedExpr`（可以[递归求值](https://github.com/cockroachdb/cockroach/blob/31dcbfc964f22ede5b7f827f4e6c0091ccfb132f/pkg/sql/sem/tree/expr.go#L62)为标量值），然后为其[构建](https://github.com/cockroachdb/cockroach/blob/2e5cce372e402faa2155c6f6e85410a9c7bdd238/pkg/sql/opt/optbuilder/util.go#L430)一个 `ScalarExpr` memo 表达式。

当 `optbuilder.Builder.Build()` 返回一个 memo 时，它已经对 memo 组表达式进行了以下分析和准备：

* 名称解析
* 类型推断和检查（另请参阅[类型 RFC](https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20160203_typing.md)）
* 规范化

规范化值得进一步讨论。一个典型的例子是常量折叠，其中给定的常量标量表达式如 `1 + 2 * 3` 可以预先求值（规范化）为 `7`，以避免在查询执行期间重复计算。我们应用了大量此类规范化规则，在计划探索和优化期间，我们还需要应用类似的转换规则来为 memo 组生成等价表达式。这些规则以我们自定义的领域特定语言 Optgen 表达。我们在此不展开细节（请参阅 [`optgen` 包文档](https://github.com/cockroachdb/cockroach/blob/917fb6cef298c01684ed92311714526836b95804/pkg/sql/opt/optgen/lang/doc.go)），但让我们看一个简单的规范化规则，[`FoldInEmpty`](https://github.com/cockroachdb/cockroach/blob/83d5a776c10ece496f3039fee3ad031acaac67a9/pkg/sql/opt/norm/rules/fold_constants.opt#L50)：

```
[FoldInEmpty, Normalize]
(In * (Tuple []))
=>
(False)
```

这将查找右侧为空元组的 `IN` 操作符，并将任何匹配的表达式替换为 `FALSE`，因为空元组永远不可能包含任何内容——即 `SELECT 'foo' IN ()` 等价于 `SELECT FALSE`。

这些[规范化规则](https://github.com/cockroachdb/cockroach/blob/83d5a776c10ece496f3039fee3ad031acaac67a9/pkg/sql/opt/norm/rules/)被[编译](https://github.com/cockroachdb/cockroach/blob/60916fb31b1bb0562d1707a338aad536e4713744/Makefile#L1681)为 Go 代码，连同 [`norm.Factory`](https://github.com/cockroachdb/cockroach/blob/5ffe3c531385bbf52e5a414addbc489279f3d535/pkg/sql/opt/norm/factory.go#L64)，它构建规范化的 memo 表达式。我们之前见过这个工厂：`Builder` [使用它](https://github.com/cockroachdb/cockroach/blob/2e5cce372e402faa2155c6f6e85410a9c7bdd238/pkg/sql/opt/optbuilder/select.go#L543)为 `FROM` 子句的表名构造一个规范化的 `ScanExpr` 节点。请注意，由于 `ConstructScan()` 和其他工厂方法会立即应用规范化规则，它们最终可能返回与方法名称指示的不同类型的表达式——我们从不构造非规范化的 memo 表达式。

现在我们已经构建了一个规范化的 memo，`buildExecMemo()` 继续使用 [`xform.Optimizer`](https://github.com/cockroachdb/cockroach/blob/5ffe3c531385bbf52e5a414addbc489279f3d535/pkg/sql/opt/xform/optimizer.go#L53)[优化](https://github.com/cockroachdb/cockroach/blob/df67aa9707fbf0193ec8b3ca4062240c360fc808/pkg/sql/plan_opt.go#L525)计划。它通过应用 Optgen [转换规则](https://github.com/cockroachdb/cockroach/blob/bc95d8f5e79576e38208b89af65a2050ab52b982/pkg/sql/opt/xform/rules)来生成等价的 memo 组，从而探索可能的查询计划，并通过基于[表统计信息](https://www.cockroachlabs.com/docs/stable/create-statistics)的[成本优化](https://www.cockroachlabs.com/docs/stable/cost-based-optimizer.html)尝试选择最佳组合。

优化器从根 memo 组[开始](https://github.com/cockroachdb/cockroach/blob/5ffe3c531385bbf52e5a414addbc489279f3d535/pkg/sql/opt/xform/optimizer.go#L226)，递归地在组成员上调用 [`optimizeGroup()`](https://github.com/cockroachdb/cockroach/blob/5ffe3c531385bbf52e5a414addbc489279f3d535/pkg/sql/opt/xform/optimizer.go#L433)。在递归过程中，它[调用 `exploreGroup()`](https://github.com/cockroachdb/cockroach/blob/5ffe3c531385bbf52e5a414addbc489279f3d535/pkg/sql/opt/xform/optimizer.go#L469) 来生成等价的 memo 组表达式，重复此过程直到所有可能性都被搜索到。它还使用 [`Coster`](https://github.com/cockroachdb/cockroach/blob/9e1fb0dcabf6653924cfaa491ceb3e60b298fe7d/pkg/sql/opt/xform/coster.go#L54)[计算](https://github.com/cockroachdb/cockroach/blob/5ffe3c531385bbf52e5a414addbc489279f3d535/pkg/sql/opt/xform/optimizer.go#L524)每个 memo 组表达式的成本，该评估器考虑了多种因素，如 CPU 使用、顺序 IO 和随机 IO 的[相对成本](https://github.com/cockroachdb/cockroach/blob/9e1fb0dcabf6653924cfaa491ceb3e60b298fe7d/pkg/sql/opt/xform/coster.go#L100-L102)，以及[表统计信息](https://github.com/cockroachdb/cockroach/blob/9e1fb0dcabf6653924cfaa491ceb3e60b298fe7d/pkg/sql/opt/xform/coster.go#L544-L545)。例如，[扫描的成本](https://github.com/cockroachdb/cockroach/blob/9e1fb0dcabf6653924cfaa491ceb3e60b298fe7d/pkg/sql/opt/xform/coster.go#L624)大致是估计的行数乘以顺序 IO 成本和每行处理成本。最后，优化器[选择](https://github.com/cockroachdb/cockroach/blob/5ffe3c531385bbf52e5a414addbc489279f3d535/pkg/sql/opt/xform/optimizer.go#L231)成本最低的 memo 树并返回。

回到 `planner.makeOptimizerPlan()`，现在我们有了一个优化后的 memo，继续将其[转换为](https://github.com/cockroachdb/cockroach/blob/df67aa9707fbf0193ec8b3ca4062240c360fc808/pkg/sql/plan_opt.go#L262)[查询计划](https://github.com/cockroachdb/cockroach/blob/1d2fadff2299166292cd5274b54bf7fad5df53c3/pkg/sql/plan.go#L285)。此计划主要由 [`planNode`](https://github.com/cockroachdb/cockroach/blob/1d2fadff2299166292cd5274b54bf7fad5df53c3/pkg/sql/plan.go#L73) 组成（例如 [`scanNode`](https://github.com/cockroachdb/cockroach/blob/c9e116e586f24c5f3a831ac653f14fd03f588b93/pkg/sql/scan.go#L40) 和 [`filterNode`](https://github.com/cockroachdb/cockroach/blob/892b63e105413eca1f0b22efec55a056f2f18c02/pkg/sql/filter.go#L23)），对应于 memo 表达式。由于此过程主要是 memo 的直接转录，我们在此不做详细说明。

在过去，这些 `planNode` 会通过递归调用 [`Next()`](https://github.com/cockroachdb/cockroach/blob/1d2fadff2299166292cd5274b54bf7fad5df53c3/pkg/sql/plan.go#L84) 来检索下一行结果，自己执行查询。然而，它们现在大多是历史遗留物，暂时作为逻辑查询计划的中间表示。计划将被移交给 DistSQL（分布式 SQL）引擎，该引擎将生成分布式物理查询计划并执行它。


### 物理规划与执行

现在我们已经有了一个优化的逻辑查询计划，准备实际执行它。然而，CockroachDB 是一个分布式系统，通过网络获取大量数据可能会很慢。我们更希望将计算发送到数据所在位置，例如在包含数据的节点上执行 `WHERE` 过滤。

这由 [DistSQL 引擎](https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20160421_distributed_sql.md)管理，其灵感来自 [Sawzall](http://research.google.com/archive/sawzall.html) 和 [MapReduce](https://en.wikipedia.org)。运行 DistSQL 规划器的节点称为"网关节点"，它将为查询设置一个由_处理器_（processor）组成的_流_（flow），处理器之间通过_流_（stream）交换数据。为此，它找出哪些节点包含我们感兴趣的数据，并通过发送 RPC 请求来配置它们。流在本地处理器之间通过内存传输行数据，或通过网络通过 RPC 传输，最终汇聚到网关节点，在那里合并成最终结果。

让我们使用逻辑规划部分的示例，但将 `companies` 表移到节点 2，`countries` 表移到节点 3，使用节点 1 作为网关节点（使用适当的 `--locality node=X` 标志启动节点）：

```sql
ALTER TABLE companies CONFIGURE ZONE USING num_replicas=1, constraints='[+node=2]';
ALTER TABLE countries CONFIGURE ZONE USING num_replicas=1, constraints='[+node=3]';
```

然后我们可以使用 `EXPLAIN(DISTSQL)` 获取 DistSQL 计划，它将输出流程图形化图表的 URL。我们将使用[连接提示](https://www.cockroachlabs.com/docs/stable/cost-based-optimizer#join-hints) `INNER HASH JOIN` 来产生一个更具说明性的例子。

```sql
EXPLAIN(DISTSQL)
    SELECT companies.name AS company, countries.name AS country, employees
    FROM companies INNER HASH JOIN countries ON companies.country = countries.id
    WHERE employees > 1e6
    ORDER BY employees DESC;
```

[![DistSQL Plan](images/distsql-plan.png)](https://cockroachdb.github.io/distsqlplan/decode.html#...)

在这里，我们看到它在节点 2 上为 `companies` 设置了 `TableReader` 处理器，将行传递给本地 `Filterer` 处理器应用 `employees > 1e6`。类似地，节点 3 上为 `countries` 设置了 `TableReader`。结果使用确定性哈希进行分割，并发送到节点 2 或 3 上的 `HashJoiner` 处理器，这些处理器在通过本地 `Sorter` 排序之前连接部分结果——这跨多个节点并行化这些操作。最后，部分排序结果发送到网关节点（节点 1），网关节点使用 `No-op` 处理器合并它们并返回最终结果给客户端。

分布式 SQL 规划与执行由 [`DistSQLPlanner`](https://github.com/cockroachdb/cockroach/blob/bc95d8f5e79576e38208b89af65a2050ab52b982/pkg/sql/distsql_physical_planner.go#L73) 执行。一旦 `connExecutor` 获得了逻辑计划并[确定](https://github.com/cockroachdb/cockroach/blob/fc67a0c9202af348e919afc1e1f70acc9a83b300/pkg/sql/conn_executor_exec.go#L878-L879)了它应该在多大程度上被分布式（并不总是完全分布式执行有意义），它将[计划传递给](https://github.com/cockroachdb/cockroach/blob/fc67a0c9202af348e919afc1e1f70acc9a83b300/pkg/sql/conn_executor_exec.go#L1063-L1065) [`DistSQLPlanner.PlanAndRun()`](https://github.com/cockroachdb/cockroach/blob/a18b86331b33a76589f76cab0f86032f75c801a7/pkg/sql/distsql_running.go#L1245)，同时传递 SQL 会话的 KV 事务用于访问 KV 服务。

DistSQL 规划器首先[创建](https://github.com/cockroachdb/cockroach/blob/a18b86331b33a76589f76cab0f86032f75c801a7/pkg/sql/distsql_running.go#L1255)一个 [`PhysicalPlan`](https://github.com/cockroachdb/cockroach/blob/c4022daffac23749fa46c8043a286c89b9713f47/pkg/sql/physicalplan/physical_plan.go#L113)，指定查询流，包括[处理器](https://github.com/cockroachdb/cockroach/blob/c4022daffac23749fa46c8043a286c89b9713f47/pkg/sql/physicalplan/physical_plan.go#L76)和[流](https://github.com/cockroachdb/cockroach/blob/c4022daffac23749fa46c8043a286c89b9713f47/pkg/sql/physicalplan/physical_plan.go#L87)的规格。规划主要涉及递归进入逻辑计划的 `planNode` 并为其[构建](https://github.com/cockroachdb/cockroach/blob/bc95d8f5e79576e38208b89af65a2050ab52b982/pkg/sql/distsql_physical_planner.go#L2713) `PhysicalPlan` 节点。让我们考虑它如何构建上面的示例计划，从最深的节点开始：

* `scanNode(companies)`: [创建](https://github.com/cockroachdb/cockroach/blob/bc95d8f5e79576e38208b89af65a2050ab52b982/pkg/sql/distsql_physical_planner.go#L1256)一个初始的 [`TableReaderSpec`](https://github.com/cockroachdb/cockroach/blob/bc95d8f5e79576e38208b89af65a2050ab52b982/pkg/sql/execinfrapb/processors_sql.proto#L65)，然后通过[找出](https://github.com/cockroachdb/cockroach/blob/bc95d8f5e79576e38208b89af65a2050ab52b982/pkg/sql/distsql_physical_planner.go#L1316)哪个节点包含数据来[规划](https://github.com/cockroachdb/cockroach/blob/bc95d8f5e79576e38208b89af65a2050ab52b982/pkg/sql/distsql_physical_planner.go#L1262)其执行。这借助 [`SpanResolverIterator`](https://github.com/cockroachdb/cockroach/blob/0ac8ab9046d01e8f89d953c439e738bd3e7f75e1/pkg/sql/physicalplan/span_resolver.go#L69) 进行[查找](https://github.com/cockroachdb/cockroach/blob/bc95d8f5e79576e38208b89af65a2050ab52b982/pkg/sql/distsql_physical_planner.go#L955)，底层[使用 `kvcoord.RangeIterator`](https://github.com/cockroachdb/cockroach/blob/0ac8ab9046d01e8f89d953c439e738bd3e7f75e1/pkg/sql/physicalplan/span_resolver.go#L173)。如果表分散在许多节点上，我们会[设置多个表读取器](https://github.com/cockroachdb/cockroach/blob/bc95d8f5e79576e38208b89af65a2050ab52b982/pkg/sql/distsql_physical_planner.go#L1331)。

* `filterNode`: [附加](https://github.com/cockroachdb/cockroach/blob/c4022daffac23749fa46c8043a286c89b9713f47/pkg/sql/physicalplan/physical_plan.go#L707)一个 [`FiltererSpec`](https://github.com/cockroachdb/cockroach/blob/bc95d8f5e79576e38208b89af65a2050ab52b982/pkg/sql/execinfrapb/processors_sql.proto#L157) 作为 `TableReaderSpec` 的本地后处理器。

* `joinNode`: [合并](https://github.com/cockroachdb/cockroach/blob/bc95d8f5e79576e38208b89af65a2050ab52b982/pkg/sql/distsql_physical_planner.go#L2658)左右子计划的计划，[找出](https://github.com/cockroachdb/cockroach/blob/bc95d8f5e79576e38208b89af65a2050ab52b982/pkg/sql/distsql_physical_planner.go#L2668)在哪些节点上运行连接处理器，[创建](https://github.com/cockroachdb/cockroach/blob/a86faebfa7f6589738134653e5c6713e5447e669/pkg/sql/distsql_plan_join.go#L59)一个基本 [`HashJoinerSpec`](https://github.com/cockroachdb/cockroach/blob/a86faebfa7f6589738134653e5c6713e5447e669/pkg/sql/distsql_plan_join.go#L59)，并[添加连接阶段](https://github.com/cockroachdb/cockroach/blob/bc95d8f5e79576e38208b89af65a2050ab52b982/pkg/sql/distsql_physical_planner.go#L2683)。这在每个节点上[创建一个连接处理器](https://github.com/cockroachdb/cockroach/blob/c4022daffac23749fa46c8043a286c89b9713f47/pkg/sql/physicalplan/physical_plan.go#L925)，对父节点输出进行[哈希](https://github.com/cockroachdb/cockroach/blob/c4022daffac23749fa46c8043a286c89b9713f47/pkg/sql/physicalplan/physical_plan.go#L939)以分发到各连接器，并[合并](https://github.com/cockroachdb/cockroach/blob/c4022daffac23749fa46c8043a286c89b9713f47/pkg/sql/physicalplan/physical_plan.go#L965-L969)到正确的连接流程中。

* `sortNode`: [附加](https://github.com/cockroachdb/cockroach/blob/bc95d8f5e79576e38208b89af65a2050ab52b982/pkg/sql/distsql_physical_planner.go#L1445)一个本地 [`SorterSpec`](https://github.com/cockroachdb/cockroach/blob/bc95d8f5e79576e38208b89af65a2050ab52b982/pkg/sql/execinfrapb/processors_sql.proto#L412) 到父哈希连接处理器。

然后计划被[最终确定](https://github.com/cockroachdb/cockroach/blob/a18b86331b33a76589f76cab0f86032f75c801a7/pkg/sql/distsql_running.go#L1260)，通过[添加](https://github.com/cockroachdb/cockroach/blob/c4022daffac23749fa46c8043a286c89b9713f47/pkg/sql/physicalplan/physical_plan.go#L415)一个最终的 [`NoopCoreSpec`](https://github.com/cockroachdb/cockroach/blob/5041b220fe1b9ee60d882565b389e2f7d878a929/pkg/sql/execinfrapb/processors.proto#L133) 阶段在网关节点上，该阶段[合并](https://github.com/cockroachdb/cockroach/blob/c4022daffac23749fa46c8043a286c89b9713f47/pkg/sql/physicalplan/physical_plan.go#L398)来自父处理器的结果（保持顺序），并[添加投影](https://github.com/cockroachdb/cockroach/blob/bc95d8f5e79576e38208b89af65a2050ab52b982/pkg/sql/distsql_physical_planner.go#L3757)生成最终结果。


物理计划现在准备好了，`DistSQLPlanner` 开始[执行它](https://github.com/cockroachdb/cockroach/blob/a18b86331b33a76589f76cab0f86032f75c801a7/pkg/sql/distsql_running.go#L1262)。它首先基于物理计划[生成](https://github.com/cockroachdb/cockroach/blob/a18b86331b33a76589f76cab0f86032f75c801a7/pkg/sql/distsql_running.go#L433)每个节点的 [`FlowSpec`](https://github.com/cockroachdb/cockroach/blob/a18b86331b33a76589f76cab0f86032f75c801a7/pkg/sql/execinfrapb/api.proto#L58)，通过收集相关的 [`ProcessorSpec`](https://github.com/cockroachdb/cockroach/blob/5041b220fe1b9ee60d882565b389e2f7d878a929/pkg/sql/execinfrapb/processors.proto#L52)，通过[发送](https://github.com/cockroachdb/cockroach/blob/a18b86331b33a76589f76cab0f86032f75c801a7/pkg/sql/distsql_running.go#L337)并行的 [`SetupFlowRequest`](https://github.com/cockroachdb/cockroach/blob/a18b86331b33a76589f76cab0f86032f75c801a7/pkg/sql/execinfrapb/api.proto#L28) RPC 来[设置它们](https://github.com/cockroachdb/cockroach/blob/a18b86331b33a76589f76cab0f86032f75c801a7/pkg/sql/distsql_running.go#L491-L493)，并[等待](https://github.com/cockroachdb/cockroach/blob/a18b86331b33a76589f76cab0f86032f75c801a7/pkg/sql/distsql_running.go#L346)节点准备就绪。它还[设置并返回](https://github.com/cockroachdb/cockroach/blob/a18b86331b33a76589f76cab0f86032f75c801a7/pkg/sql/distsql_running.go#L367)网关节点上的本地流端点。让我们看看远程节点上发生了什么。

[`distsql.ServerImpl`](https://github.com/cockroachdb/cockroach/blob/a18b86331b33a76589f76cab0f86032f75c801a7/pkg/sql/distsql/server.go#L61) 实现了 [`DistSQL` gRPC 服务](https://github.com/cockroachdb/cockroach/blob/a18b86331b33a76589f76cab0f86032f75c801a7/pkg/sql/execinfrapb/api.proto#L140)。[`SetupFlow()`](https://github.com/cockroachdb/cockroach/blob/a18b86331b33a76589f76cab0f86032f75c801a7/pkg/sql/distsql/server.go#L515) 方法最终[分发](https://github.com/cockroachdb/cockroach/blob/a18b86331b33a76589f76cab0f86032f75c801a7/pkg/sql/distsql/server.go#L462)到 [`colflow.NewVectorizedFlow()`](https://github.com/cockroachdb/cockroach/blob/cbcdb5ff33da15e86614aa0c1cff6df21d228e75/pkg/sql/colflow/vectorized_flow.go#L166)，它使用[向量化执行引擎](https://www.cockroachlabs.com/docs/stable/vectorized-execution.html)设置流。此引擎将处理器和流基础设施表示为 [`colexecop.Operator`](https://github.com/cockroachdb/cockroach/blob/3d764753c54bd927b130656db49aa4f372abf8a7/pkg/sql/colexecop/operator.go#L26)，其中 [`Next()`](https://github.com/cockroachdb/cockroach/blob/3d764753c54bd927b130656db49aa4f372abf8a7/pkg/sql/colexecop/operator.go#L50) 方法将一组处理结果作为列向量的 [`Batch`](https://github.com/cockroachdb/cockroach/blob/33006d5f81fd74935edee2fb23deeef22aa811a8/pkg/col/coldata/batch.go#L28) 返回——这通常在子操作符上递归调用来获取输入数据。

向量化流设置[遍历](https://github.com/cockroachdb/cockroach/blob/cbcdb5ff33da15e86614aa0c1cff6df21d228e75/pkg/sql/colflow/vectorized_flow.go#L1113)流处理器规格，首先设置任何[处理器输入](https://github.com/cockroachdb/cockroach/blob/cbcdb5ff33da15e86614aa0c1cff6df21d228e75/pkg/sql/colflow/vectorized_flow.go#L1122)。对于本地输入，它只是[返回对该操作符的引用](https://github.com/cockroachdb/cockroach/blob/cbcdb5ff33da15e86614aa0c1cff6df21d228e75/pkg/sql/colflow/vectorized_flow.go#L865-L866)，但对于远程输入，它[创建](https://github.com/cockroachdb/cockroach/blob/cbcdb5ff33da15e86614aa0c1cff6df21d228e75/pkg/sql/colflow/vectorized_flow.go#L885)一个 [`colrpc.Inbox`](https://github.com/cockroachdb/cockroach/blob/9bcc860b904ac2cc6b7290fac0542f636f683aff/pkg/sql/colflow/colrpc/inbox.go#L54) 操作符，该操作符通过 gRPC [`ProducerMessage`](https://github.com/cockroachdb/cockroach/blob/c4022daffac23749fa46c8043a286c89b9713f47/pkg/sql/execinfrapb/data.proto#L235) 流接收数据，该流是通过 [`FlowStream()`](https://github.com/cockroachdb/cockroach/blob/a18b86331b33a76589f76cab0f86032f75c801a7/pkg/sql/execinfrapb/api.proto#L164) 调用建立的。然后处理器设置[创建](https://github.com/cockroachdb/cockroach/blob/cbcdb5ff33da15e86614aa0c1cff6df21d228e75/pkg/sql/colflow/vectorized_flow.go#L1149)一个[基于](https://github.com/cockroachdb/cockroach/blob/7869b781b66823c44f1b30047d0fcbcc2dbe8e0f/pkg/sql/colexec/colbuilder/execplan.go#L734)处理器规格类型的操作符。最后，它设置任何[处理器输出](https://github.com/cockroachdb/cockroach/blob/cbcdb5ff33da15e86614aa0c1cff6df21d228e75/pkg/sql/colflow/vectorized_flow.go#L1191-L1192)，通过将其[链接](https://github.com/cockroachdb/cockroach/blob/cbcdb5ff33da15e86614aa0c1cff6df21d228e75/pkg/sql/colflow/vectorized_flow.go#L1004)到本地操作符（如我们在输入中看到的）或[创建](https://github.com/cockroachdb/cockroach/blob/cbcdb5ff33da15e86614aa0c1cff6df21d228e75/pkg/sql/colflow/vectorized_flow.go#L1007-L1010)一个 [`colrpc.Outbox`](https://github.com/cockroachdb/cockroach/blob/03a2d500202af3e9c4a054821cb5a80721c97bab/pkg/sql/colflow/colrpc/outbox.go#L46)，将批次[序列化](https://github.com/cockroachdb/cockroach/blob/03a2d500202af3e9c4a054821cb5a80721c97bab/pkg/sql/colflow/colrpc/outbox.go#L93)为 [Apache Arrow](https://arrow.apache.org) 发送到远程 `Inbox` 操作符。在有多个输出的情况下（如我们的示例中的 `HashJoiner`），它[设置](https://github.com/cockroachdb/cockroach/blob/cbcdb5ff33da15e86614aa0c1cff6df21d228e75/pkg/sql/colflow/vectorized_flow.go#L779)一个 [`HashRouter`](https://github.com/cockroachdb/cockroach/blob/dd48ea52e6fbd304f48e4995c448364bc5b71321/pkg/sql/colflow/routers.go#L408)，对输入数据进行哈希并将其路由到适当的输出操作符。


网关节点现在已经设置了一个处理器和流的流网络，以向量化操作符在 gRPC 流上交换列批次的形式实现。当 DistSQL 处理器[运行流](https://github.com/cockroachdb/cockroach/blob/a18b86331b33a76589f76cab0f86032f75c801a7/pkg/sql/distsql_running.go#L525)时会发生什么？

在向量化流的根部是一个 [`BatchFlowCoordinator`](https://github.com/cockroachdb/cockroach/blob/9c86c8f937e3c5d4728079d2bdb2a98943a7d872/pkg/sql/colflow/flow_coordinator.go#L210)。当它被[运行](https://github.com/cockroachdb/cockroach/blob/9c86c8f937e3c5d4728079d2bdb2a98943a7d872/pkg/sql/colflow/flow_coordinator.go#L254)时，它会持续从其[输入操作符](https://github.com/cockroachdb/cockroach/blob/9c86c8f937e3c5d4728079d2bdb2a98943a7d872/pkg/sql/colflow/flow_coordinator.go#L189)[拉取](https://github.com/cockroachdb/cockroach/blob/9c86c8f937e3c5d4728079d2bdb2a98943a7d872/pkg/sql/colflow/flow_coordinator.go#L282)一批数据，通过[调用 `Next()`](https://github.com/cockroachdb/cockroach/blob/9c86c8f937e3c5d4728079d2bdb2a98943a7d872/pkg/sql/colflow/flow_coordinator.go#L127)，并将其[推送到](https://github.com/cockroachdb/cockroach/blob/9c86c8f937e3c5d4728079d2bdb2a98943a7d872/pkg/sql/colflow/flow_coordinator.go#L294)其[输出 `BatchReceiver`](https://github.com/cockroachdb/cockroach/blob/9c86c8f937e3c5d4728079d2bdb2a98943a7d872/pkg/sql/colflow/flow_coordinator.go#L190)——一个 [`DistSQLReceiver`](https://github.com/cockroachdb/cockroach/blob/a18b86331b33a76589f76cab0f86032f75c801a7/pkg/sql/distsql_running.go#L556)，它[早在 `connExecutor.execWithDistSQLEngine()` 中](https://github.com/cockroachdb/cockroach/blob/fc67a0c9202af348e919afc1e1f70acc9a83b300/pkg/sql/conn_executor_exec.go#L1005)就连接到了客户端会话。这触发了子操作符上递归的 `Next()` 调用级联，一直向下到流的叶子节点。使用前面的示例（见图表），这些操作符是：

* `Noop`: 由 [`noopOperator`](https://github.com/cockroachdb/cockroach/blob/3d764753c54bd927b130656db49aa4f372abf8a7/pkg/sql/colexecop/operator.go#L355) 实现，它[什么都不做](https://github.com/cockroachdb/cockroach/blob/3d764753c54bd927b130656db49aa4f372abf8a7/pkg/sql/colexecop/operator.go#L368)。
* `ordered`: 使用 [`OrderedSynchronizer`](https://github.com/cockroachdb/cockroach/blob/3d764753c54bd927b130656db49aa4f372abf8a7/pkg/sql/colexec/ordered_synchronizer.eg.go#L34) 操作符[组合](https://github.com/cockroachdb/cockroach/blob/3d764753c54bd927b130656db49aa4f372abf8a7/pkg/sql/colexec/ordered_synchronizer.eg.go#L121)来自两个远程输入流的有序输入。
* Remote stream: 实现为 `Inbox`/`Outbox` 操作符对，[接收](https://github.com/cockroachdb/cockroach/blob/9bcc860b904ac2cc6b7290fac0542f636f683aff/pkg/sql/colflow/colrpc/inbox.go#L293)/[发送](https://github.com/cockroachdb/cockroach/blob/03a2d500202af3e9c4a054821cb5a80721c97bab/pkg/sql/colflow/colrpc/outbox.go#L292)数据批次通过 gRPC 流。
* `Sorter`: 由 [`sortOp`](https://github.com/cockroachdb/cockroach/blob/3771c58e9f286bf26b264c9afa076924889fffeb/pkg/sql/colexec/sort.go#L186) 实现，它[缓冲](https://github.com/cockroachdb/cockroach/blob/3771c58e9f286bf26b264c9afa076924889fffeb/pkg/sql/colexec/sort.go#L266)输入数据然后[排序](https://github.com/cockroachdb/cockroach/blob/3771c58e9f286bf26b264c9afa076924889fffeb/pkg/sql/colexec/sort.go#L269)。
* `HashJoiner`: 由 [`hashJoiner`](https://github.com/cockroachdb/cockroach/blob/3771c58e9f286bf26b264c9afa076924889fffeb/pkg/sql/colexec/colexecjoin/hashjoiner.go#L168) 实现，它从左输入[构建](https://github.com/cockroachdb/cockroach/blob/3771c58e9f286bf26b264c9afa076924889fffeb/pkg/sql/colexec/colexecjoin/hashjoiner.go#L283)哈希表，然后用右输入数据[探测](https://github.com/cockroachdb/cockroach/blob/3771c58e9f286bf26b264c9afa076924889fffeb/pkg/sql/colexec/colexecjoin/hashjoiner.go#L293)，[发出](https://github.com/cockroachdb/cockroach/blob/3771c58e9f286bf26b264c9afa076924889fffeb/pkg/sql/colexec/colexecjoin/hashjoiner.go#L308)任何匹配的组合。
* `by hash`: 由 `HashRouter` 实现，它将输入数据[分发](https://github.com/cockroachdb/cockroach/blob/dd48ea52e6fbd304f48e4995c448364bc5b71321/pkg/sql/colflow/routers.go#L653)到其多个输出。
* `Filterer`: 由选择操作符如 [`BoolVecToSelOp`](https://github.com/cockroachdb/cockroach/blob/3771c58e9f286bf26b264c9afa076924889fffeb/pkg/sql/colexec/colexecutils/bool_vec_to_sel.go#L42) 实现，它[选择](https://github.com/cockroachdb/cockroach/blob/3771c58e9f286bf26b264c9afa076924889fffeb/pkg/sql/colexec/colexecutils/bool_vec_to_sel.go#L70)满足过滤谓词的批次成员。
* `TableReader`: 由 [`ColBatchScan`](https://github.com/cockroachdb/cockroach/blob/3d764753c54bd927b130656db49aa4f372abf8a7/pkg/sql/colfetcher/colbatch_scan.go#L47) 实现，它[使用 `cFetcher`](https://github.com/cockroachdb/cockroach/blob/3d764753c54bd927b130656db49aa4f372abf8a7/pkg/sql/colfetcher/colbatch_scan.go#L53) 从 KV 服务[获取](https://github.com/cockroachdb/cockroach/blob/3d764753c54bd927b130656db49aa4f372abf8a7/pkg/sql/colfetcher/colbatch_scan.go#L97)表数据。

一个有趣的旁注是，为了避免 Go 缺乏泛型通常所需的运行时类型断言和反射开销，向量化执行引擎改为使用"[execgen 模板](https://github.com/cockroachdb/cockroach/blob/d4610afe3db5d6ee3df14b0ecc5585155cdb0159/pkg/sql/colexec/execgen/)"为所有输入数据类型组合[生成](https://github.com/cockroachdb/cockroach/blob/60916fb31b1bb0562d1707a338aad536e4713744/Makefile#L1659-L1661)类型化操作符。

我们现在已经到达了 SQL 引擎与 KV 服务相遇的地方：[`cFetcher`](https://github.com/cockroachdb/cockroach/blob/ff28a1c72a8f2544e1b00c1cd55567fcd1fc0505/pkg/sql/colfetcher/cfetcher.go#L227)。我们不会在这里探讨 KV 数据到 SQL 数据的映射（请参阅[编码技术说明](https://github.com/cockroachdb/cockroach/blob/master/docs/tech-notes/encoding.md)了解详情），但就我们的目的而言，可以说广义上，表中的每一行都存储为一个序列化的二进制值，键由表标识符和行的主键值组成——类似于 `/companies/Apple`。因此，表的所有行都存储为一个连续的_键区间_（key span），按主键排序，共同拥有一个前缀。

回想一下，`TableReaderSpec` 已经[包含](https://github.com/cockroachdb/cockroach/blob/bc95d8f5e79576e38208b89af65a2050ab52b982/pkg/sql/execinfrapb/processors_sql.pb.go#L571)表的扫描键区间，从 `DistSQLPlanner` 找出哪些节点包含该表时就已经确定了。`ColBatchScan` 操作符通过 [`StartScan()`](https://github.com/cockroachdb/cockroach/blob/ff28a1c72a8f2544e1b00c1cd55567fcd1fc0505/pkg/sql/colfetcher/cfetcher.go#L657) 将此区间传递给 `cFetcher`。`cFetcher` 本身主要关注将 KV 数据[解码](https://github.com/cockroachdb/cockroach/blob/ff28a1c72a8f2544e1b00c1cd55567fcd1fc0505/pkg/sql/colfetcher/cfetcher.go#L859)为 SQL 数据结构，并[委托](https://github.com/cockroachdb/cockroach/blob/ff28a1c72a8f2544e1b00c1cd55567fcd1fc0505/pkg/sql/colfetcher/cfetcher.go#L687)给 [`KVFetcher`](https://github.com/cockroachdb/cockroach/blob/5c00812b5d495749133beb1e217639e03fd02246/pkg/sql/row/kv_fetcher.go#L29)。后者也主要关注[解码](https://github.com/cockroachdb/cockroach/blob/5c00812b5d495749133beb1e217639e03fd02246/pkg/sql/row/kv_fetcher.go#L115)——这次是 MVCC 元数据——并进一步委托给 [`txnKVFetcher`](https://github.com/cockroachdb/cockroach/blob/d6d394bf5c4974d79e21efe8c03f65ebf0bc10fa/pkg/sql/row/kv_batch_fetcher.go#L68)，实际获取在此发生。`txnKVFetcher`，顾名思义，使用最初为 SQL 会话在 `connExecutor` 中设置的 `kv.Txn` 事务来[发送](https://github.com/cockroachdb/cockroach/blob/d6d394bf5c4974d79e21efe8c03f65ebf0bc10fa/pkg/sql/row/kv_batch_fetcher.go#L225) KV gRPC 请求。具体来说，它为请求的键区间[提交](https://github.com/cockroachdb/cockroach/blob/d6d394bf5c4974d79e21efe8c03f65ebf0bc10fa/pkg/sql/row/kv_batch_fetcher.go#L388) [`roachpb.ScanRequest`](https://github.com/cockroachdb/cockroach/blob/4e4f31a0ed1a8ea985b9ab6f72e29266b259900e/pkg/roachpb/api.proto#L384)，并[返回](https://github.com/cockroachdb/cockroach/blob/d6d394bf5c4974d79e21efe8c03f65ebf0bc10fa/pkg/sql/row/kv_batch_fetcher.go#L551) [`roachpb.ScanResponse`](https://github.com/cockroachdb/cockroach/blob/4e4f31a0ed1a8ea985b9ab6f72e29266b259900e/pkg/roachpb/api.proto#L408) 结果。

我们现在将离开 SQL 引擎，深入键值服务。

## KV: 键/值存储服务

CockroachDB 的 KV 层是一个事务性的、分布式的键/值存储服务。其架构的完整描述超出了本文的范围，请参阅[架构概览](https://www.cockroachlabs.com/docs/stable/architecture/overview.html)了解详情。

简言之，KV 服务维护一个单一的有序键/值映射，分割成多个连续的范围（range），目标大小为 512 MB。每个范围对应一个单独的 [Raft](https://raft.github.io) 共识集群，其中每个 Raft 节点称为一个副本（replica）。单个 CockroachDB 节点包含属于许多范围的许多副本，使用一个共同的底层磁盘数据存储。跨范围事务通过由 KV 客户端协调的[分布式两阶段提交协议](https://www.cockroachlabs.com/docs/stable/architecture/transaction-layer.html)实现。

KV 服务是面向请求的，具有基于 [Protocol Buffers](https://developers.google.com/protocol-buffers) 的 [gRPC](https://grpc.io) API，定义在 [`api.proto`](https://github.com/cockroachdb/cockroach/blob/4e4f31a0ed1a8ea985b9ab6f72e29266b259900e/pkg/roachpb/api.proto) 中。在实践中，KV 客户端始终发送 [`roachpb.BatchRequest`](https://github.com/cockroachdb/cockroach/blob/4e4f31a0ed1a8ea985b9ab6f72e29266b259900e/pkg/roachpb/api.proto#L2023)，一个包含其他请求集合的通用请求。所有请求都有一个 [`Header`](https://github.com/cockroachdb/cockroach/blob/4e4f31a0ed1a8ea985b9ab6f72e29266b259900e/pkg/roachpb/api.proto#L1841)，包含路由信息（例如请求的目标副本）和[事务信息](https://github.com/cockroachdb/cockroach/blob/33c18ad1bcdb37ed6ed428b7527148977a8c566a/pkg/roachpb/api.proto#L818)（在哪个事务上下文中执行请求）。相应的响应，毫不意外，是 [`roachpb.BatchResponse`](https://github.com/cockroachdb/cockroach/blob/4e4f31a0ed1a8ea985b9ab6f72e29266b259900e/pkg/roachpb/api.proto#L2035)。

### KV 客户端接口

客户端通过内部 [`kv.DB`](https://github.com/cockroachdb/cockroach/blob/4e4f31a0ed1a8ea985b9ab6f72e29266b259900e/pkg/kv/db.go#L250-L251) 客户端接口发送 KV 请求，通常通过从 [`DB.NewTxn()`](https://github.com/cockroachdb/cockroach/blob/4e4f31a0ed1a8ea985b9ab6f72e29266b259900e/pkg/kv/db.go#L755) 获取 [`kv.Txn`](https://github.com/cockroachdb/cockroach/blob/4e4f31a0ed1a8ea985b9ab6f72e29266b259900e/pkg/kv/txn.go#L46) 事务句柄，并调用其方法如 [`Get()`](https://github.com/cockroachdb/cockroach/blob/4e4f31a0ed1a8ea985b9ab6f72e29266b259900e/pkg/kv/txn.go#L399) 和 [`Put()`](https://github.com/cockroachdb/cockroach/blob/4e4f31a0ed1a8ea985b9ab6f72e29266b259900e/pkg/kv/txn.go#L452)。回想一下 SQL 引擎使用此接口在事务上下文中执行语句。KV 事务是 [ACID](https://en.wikipedia.org/wiki/ACID) 兼容的，具有[可串行化](https://www.cockroachlabs.com/docs/stable/demo-serializable.html)隔离级别，因此其写入只有在调用 [`Commit()`](https://github.com/cockroachdb/cockroach/blob/4e4f31a0ed1a8ea985b9ab6f72e29266b259900e/pkg/kv/txn.go#L675) 后才生效，并可通过 [`Rollback()`](https://github.com/cockroachdb/cockroach/blob/4e4f31a0ed1a8ea985b9ab6f72e29266b259900e/pkg/kv/txn.go#L751) 丢弃。

由于我们经常希望在同一请求中发送多个操作（出于性能考虑），它们被分组在一个 [`kv.Batch`](https://github.com/cockroachdb/cockroach/blob/4e4f31a0ed1a8ea985b9ab6f72e29266b259900e/pkg/kv/batch.go#L33-L34) 中，可以通过 [`Txn.NewBatch()`](https://github.com/cockroachdb/cockroach/blob/4e4f31a0ed1a8ea985b9ab6f72e29266b259900e/pkg/kv/txn.go#L388) 获取。调用 e.g. [`Batch.Get()`](https://github.com/cockroachdb/cockroach/blob/4e4f31a0ed1a8ea985b9ab6f72e29266b259900e/pkg/kv/batch.go#L357) 和 [`Batch.Put()`](https://github.com/cockroachdb/cockroach/blob/4e4f31a0ed1a8ea985b9ab6f72e29266b259900e/pkg/kv/batch.go#L399) 将这些操作添加到批处理中，而 [`Txn.Run()`](https://github.com/cockroachdb/cockroach/blob/4e4f31a0ed1a8ea985b9ab6f72e29266b259900e/pkg/kv/txn.go#L631) 将批处理作为 `BatchRequest` 发送到服务器，将结果填充到 [`Batch.Results`](https://github.com/cockroachdb/cockroach/blob/4e4f31a0ed1a8ea985b9ab6f72e29266b259900e/pkg/kv/batch.go#L48)。便捷方法如 `Txn.Get()` 只需创建一个包含单个请求的内部批处理并运行它。

让我们看看 `Txn.Run()` 内部发生了什么。追踪代码，我们最终到达 [`txn.db.sendUsingSender(ctx, batchRequest, txn.mu.sender)`](https://github.com/cockroachdb/cockroach/blob/4e4f31a0ed1a8ea985b9ab6f72e29266b259900e/pkg/kv/txn.go#L982)，它在传递的 sender 上[调用 `sender.Send(ctx, batchRequest)`](https://github.com/cockroachdb/cockroach/blob/4e4f31a0ed1a8ea985b9ab6f72e29266b259900e/pkg/kv/db.go#L817)。此时，请求开始通过一系列 [`Sender`](https://github.com/cockroachdb/cockroach/blob/cfb4b95d94e717e985423d5313c58187fbde5da4/pkg/kv/sender.go#L54) 层级——这些对象在将请求路由到副本执行之前执行各种外围任务。

`Sender` 只有一个方法 `Send()`，它最终将请求传递给下一个 sender。让我们深入这个"发送"兔子洞：

`TxnCoordSender` -> `DistSender` -> `Node` -> `Stores` -> `Store` -> `Replica`

前两个运行在接收 SQL 查询并进行 SQL 处理的同一节点上（"网关节点"），其他运行在负责被访问数据的节点上（"范围节点"）。

请注意，在 DistSQL 流的情况下（参见前面的部分），在远程处理节点上[设置](https://github.com/cockroachdb/cockroach/blob/265a805622b0f2111082928568178780d1aa6d00/pkg/sql/distsql/server.go#L262)了额外的"叶子"事务（类型为 [`LeafTxn`](https://github.com/cockroachdb/cockroach/blob/cfb4b95d94e717e985423d5313c58187fbde5da4/pkg/kv/sender.go#L45)），使用它们自己本地的 `TxnCoordSender` 与 KV 交互。然而，写入总是通过网关节点上的"根"事务进行。我们在此不讨论叶子事务，只考虑在网关节点上执行的根事务。

### TxnCoordSender

最顶层的 sender 是 [`TxnCoordSender`](https://github.com/cockroachdb/cockroach/blob/cfb4b95d94e717e985423d5313c58187fbde5da4/pkg/kv/kvclient/kvcoord/txn_coord_sender.go#LL95)，负责管理事务状态（参见[设计文档](https://github.com/cockroachdb/cockroach/blob/master/docs/design.md)的"事务管理"部分）。事务的所有操作都通过一个 `TxnCoordSender` 实例进行，如果它崩溃，则事务被中止。

内部它被细分为一个[拦截器栈](https://github.com/cockroachdb/cockroach/blob/cfb4b95d94e717e985423d5313c58187fbde5da4/pkg/kv/kvclient/kvcoord/txn_coord_sender.go#L140-L150)，由 [`lockedSender`](https://github.com/cockroachdb/cockroach/blob/f176a39446e5235c9985a07f2550583dc106c26c/pkg/kv/kvclient/kvcoord/txn_lock_gatekeeper.go#L23-L24) 组成——基本上是共享 `TxnCoordSender.mu` 互斥锁并构成单个[临界区](https://en.wikipedia.org/wiki/Critical_section)的 sender。这些拦截器从上到下依次是：

* [`txnHeartbeater`](https://github.com/cockroachdb/cockroach/blob/cc83f59eed96e4ecc829b40d3fa53b85e55fcfb0/pkg/kv/kvclient/kvcoord/txn_interceptor_heartbeater.go#L55)：创建并定期[心跳](https://github.com/cockroachdb/cockroach/blob/615ddd04f27b5c3729a17be406b143b45dae65a5/pkg/kv/kvclient/kvcoord/txn_interceptor_heartbeater.go#L357)事务记录，[标记](https://github.com/cockroachdb/cockroach/blob/b7fb524c899c38e3e92c80d6b88f45afc9df7ca2/pkg/kv/kvserver/batcheval/cmd_heartbeat_txn.go#L80)事务存活，并[检测](https://github.com/cockroachdb/cockroach/blob/615ddd04f27b5c3729a17be406b143b45dae65a5/pkg/kv/kvclient/kvcoord/txn_interceptor_heartbeater.go#L439-L443)事务的外部中止（例如由冲突事务引起）。

* [`txnSeqNumAllocator`](https://github.com/cockroachdb/cockroach/blob/5e7f8bd7a511f58204a1419452fd4d439f9ccec8/pkg/kv/kvclient/kvcoord/txn_interceptor_seq_num_allocator.go#L60)：为事务内的操作[分配](https://github.com/cockroachdb/cockroach/blob/5e7f8bd7a511f58204a1419452fd4d439f9ccec8/pkg/kv/kvclient/kvcoord/txn_interceptor_seq_num_allocator.go#L98-L101)序列号，用于排序和幂等性。

* [`txnPipeliner`](https://github.com/cockroachdb/cockroach/blob/1bfd8cceb2b7f06970a26e800eacee372a203523/pkg/kv/kvclient/kvcoord/txn_interceptor_pipeliner.go#L179)：通过[异步](https://github.com/cockroachdb/cockroach/blob/1bfd8cceb2b7f06970a26e800eacee372a203523/pkg/kv/kvclient/kvcoord/txn_interceptor_pipeliner.go#L468)将写入提交给共识并与后续依赖其结果的操作[协调](https://github.com/cockroachdb/cockroach/blob/1bfd8cceb2b7f06970a26e800eacee372a203523/pkg/kv/kvclient/kvcoord/txn_interceptor_pipeliner.go#L252)，实现[流水线写入](https://www.cockroachlabs.com/blog/transaction-pipelining/)。

* [`txnSpanRefresher`](https://github.com/cockroachdb/cockroach/blob/ee23325c42c4464aa24e863d9cadf1a2e244a842/pkg/kv/kvclient/kvcoord/txn_interceptor_span_refresher.go#L104)：跟踪事务读取，在发生序列化错误时检查其过去的读取在更高的事务时间戳下是否仍然有效。如果有效，事务可以在更高的时间戳下继续，而不必将错误传播给客户端并重试整个事务。

* [`txnCommitter`](https://github.com/cockroachdb/cockroach/blob/ee23325c42c4464aa24e863d9cadf1a2e244a842/pkg/kv/kvclient/kvcoord/txn_interceptor_committer.go#L112)：管理提交和回滚，特别是实现[并行提交协议](https://www.cockroachlabs.com/blog/parallel-commits/)。

* [`txnMetricRecorder`](https://github.com/cockroachdb/cockroach/blob/5e7f8bd7a511f58204a1419452fd4d439f9ccec8/pkg/kv/kvclient/kvcoord/txn_interceptor_metric_recorder.go#L27)：记录各种事务指标。

* [`txnLockGatekeeper`](https://github.com/cockroachdb/cockroach/blob/f176a39446e5235c9985a07f2550583dc106c26c/pkg/kv/kvclient/kvcoord/txn_lock_gatekeeper.go#L41)：在请求飞行期间解锁 `TxnCoordSender.mu`，并对相关请求强制执行同步客户端协议。

遍历 `TxnCoordSender` 栈之后，请求被传递给 `DistSender`。

### DistSender

[`DistSender`](https://github.com/cockroachdb/cockroach/blob/904807640f6a28aa82c5e63f0b62de2dbf27b3a0/pkg/kv/kvclient/kvcoord/dist_sender.go#L235-L236) 是一个真正的主力：它处理网关节点和（可能很多）范围节点之间的通信，将"分布式"放入"分布式数据库"。它接收 `BatchRequest`，查看批处理内部的请求，找出每个命令需要去哪个范围，找到负责该范围的节点/副本，将请求路由到那里，然后收集并重新组装结果。

让我们浏览一下代码：

1. 请求被细分到各个范围：`DistSender.Send()` 调用 [`DistSender.divideAndSendBatchToRanges()`](https://github.com/cockroachdb/cockroach/blob/904807640f6a28aa82c5e63f0b62de2dbf27b3a0/pkg/kv/kvclient/kvcoord/dist_sender.go#L755)，它使用 [`RangeIterator`](https://github.com/cockroachdb/cockroach/blob/ac572af5be4e35dbdd58a95e32e5d1f6c24bc24c/pkg/kv/kvclient/kvcoord/range_iter.go#L28) [遍历](https://github.com/cockroachdb/cockroach/blob/904807640f6a28aa82c5e63f0b62de2dbf27b3a0/pkg/kv/kvclient/kvcoord/dist_sender.go#L1110)请求的组成范围。回想一下，单个请求（如 `ScanRequest`）可能引用一个可能跨越许多范围的键区间。

   这个看似简单的迭代背后隐藏着很多东西：需要访问集群的范围元数据以找到键到范围的映射（有关此元数据的信息可在设计文档的[范围元数据部分](https://github.com/cockroachdb/cockroach/blob/master/docs/design.md#range-metadata)中找到）。范围元数据作为常规数据存储在集群中，在一个两级索引中，将范围的结束键映射到关于相应范围副本的描述符（存储此索引的范围称为"元范围"）。`RangeIterator` 逻辑上按照范围键顺序遍历这些描述符。

   做好准备：当从一个范围移动到下一个时，迭代器[回调 `DistSender`](https://github.com/cockroachdb/cockroach/blob/ac572af5be4e35dbdd58a95e32e5d1f6c24bc24c/pkg/kv/kvclient/kvcoord/range_iter.go#L193)，后者知道如何找到负责某个特定键的范围描述符。`DistSender` [委托](https://github.com/cockroachdb/cockroach/blob/904807640f6a28aa82c5e63f0b62de2dbf27b3a0/pkg/kv/kvclient/kvcoord/dist_sender.go#L550)范围描述符查找给 [`RangeCache`](https://github.com/cockroachdb/cockroach/blob/d5f73c6a05c99f7c6b40b80dea80e8ed8ab15cac/pkg/kv/kvclient/rangecache/range_cache.go#L75)（一个按范围结束键索引的 LRU 树缓存）。当集群中的范围分裂或移动时，此缓存可能与现实不同步；当发现某个条目过时时，我们将在下面看到 `DistSender` 将其从缓存中移除。

   在理想情况下，缓存包含有关覆盖我们感兴趣的键的描述符信息，并[返回它](https://github.com/cockroachdb/cockroach/blob/d5f73c6a05c99f7c6b40b80dea80e8ed8ab15cac/pkg/kv/kvclient/rangecache/range_cache.go#L620)。在不理想的情况下，它需要在范围数据库 `RangeCache.db` 中[查找](https://github.com/cockroachdb/cockroach/blob/d5f73c6a05c99f7c6b40b80dea80e8ed8ab15cac/pkg/kv/kvclient/rangecache/range_cache.go#L649)范围描述符。这是一个[接口](https://github.com/cockroachdb/cockroach/blob/d5f73c6a05c99f7c6b40b80dea80e8ed8ab15cac/pkg/kv/kvclient/rangecache/range_cache.go#L57)，[隐藏着 `DistSender` 自身](https://github.com/cockroachdb/cockroach/blob/904807640f6a28aa82c5e63f0b62de2dbf27b3a0/pkg/kv/kvclient/kvcoord/dist_sender.go#L349)，后者将通过[调用 `DistSender.Send()`](https://github.com/cockroachdb/cockroach/blob/9b449c55c9af9ba4975fea51b1d798254955008f/pkg/kv/range_lookup.go#L312) 递归地[查找范围的元键](https://github.com/cockroachdb/cockroach/blob/9b449c55c9af9ba4975fea51b1d798254955008f/pkg/kv/range_lookup.go#L197)，该调用又进入范围缓存，依此类推。

   此递归不能永远继续。常规范围的描述符位于 meta2 范围（二级索引范围）中，而 meta2 范围的描述符存在于（唯一的）[meta1 范围](https://github.com/cockroachdb/cockroach/blob/d5f73c6a05c99f7c6b40b80dea80e8ed8ab15cac/pkg/kv/kvclient/rangecache/range_cache.go#L791)中，该范围由 [`Gossip.GetFirstRangeDescriptor()`](https://github.com/cockroachdb/cockroach/blob/8c5253bbccea8966fe20e3cb8cc6111712f6f509/pkg/gossip/gossip.go#L1596) [提供](https://github.com/cockroachdb/cockroach/blob/4e4f31a0ed1a8ea985b9ab6f72e29266b259900e/pkg/server/server.go#L404)。

2. 每个子请求（部分批处理）被发送到其范围。这通过调用 [`DistSender.sendPartialBatchAsync()`](https://github.com/cockroachdb/cockroach/blob/904807640f6a28aa82c5e63f0b62de2dbf27b3a0/pkg/kv/kvclient/kvcoord/dist_sender.go#L1259) 完成，它将批处理中的所有请求[截断](https://github.com/cockroachdb/cockroach/blob/904807640f6a28aa82c5e63f0b62de2dbf27b3a0/pkg/kv/kvclient/kvcoord/dist_sender.go#L1416)到当前范围，然后[发送](https://github.com/cockroachdb/cockroach/blob/904807640f6a28aa82c5e63f0b62de2dbf27b3a0/pkg/kv/kvclient/kvcoord/dist_sender.go#L1478)截断后的批处理。所有这些部分批处理被并发发送。

   `sendPartialBatch()` 是处理来自过时 `RangeCache` 信息的错误的地方：过时的范围描述符被[逐出](https://github.com/cockroachdb/cockroach/blob/904807640f6a28aa82c5e63f0b62de2dbf27b3a0/pkg/kv/kvclient/kvcoord/dist_sender.go#L1523)缓存，部分批处理被重试。

3. 将部分批处理发送到单个范围涉及选择该范围的适当副本并向其进行 RPC 调用。默认情况下，每个范围被复制三次，但三个副本中只有一个——"租约持有者"（lease holder）——是该范围的临时指定所有者，负责协调所有对其的读取和写入（参见设计文档中的[范围租约部分](https://github.com/cockroachdb/cockroach/blob/master/docs/design.md#range-leases)）。租约持有者也[缓存在 `RangeCache` 中](https://github.com/cockroachdb/cockroach/blob/d5f73c6a05c99f7c6b40b80dea80e8ed8ab15cac/pkg/kv/kvclient/rangecache/range_cache.go#L299)，这些信息可能会过时。

   处理此问题的 `DistSender` 方法是 [`sendToReplicas`](https://github.com/cockroachdb/cockroach/blob/33c18ad1bcdb37ed6ed428b7527148977a8c566a/pkg/kv/dist_sender.go#L451)。它将使用缓存将请求发送到租约持有者，但也准备好尝试其他副本，按["接近度"顺序](https://github.com/cockroachdb/cockroach/blob/904807640f6a28aa82c5e63f0b62de2dbf27b3a0/pkg/kv/kvclient/kvcoord/dist_sender.go#L1764)。缓存所说的租约持有者被简单地[移到](https://github.com/cockroachdb/cockroach/blob/904807640f6a28aa82c5e63f0b62de2dbf27b3a0/pkg/kv/kvclient/kvcoord/dist_sender.go#L1772)要尝试的副本列表的最前面，然后按顺序向所有副本[发送 RPC](https://github.com/cockroachdb/cockroach/blob/904807640f6a28aa82c5e63f0b62de2dbf27b3a0/pkg/kv/kvclient/kvcoord/dist_sender.go#L1860)，直到一个成功。如果租约持有者失败，我们假设它已过时并[将其逐出缓存](https://github.com/cockroachdb/cockroach/blob/904807640f6a28aa82c5e63f0b62de2dbf27b3a0/pkg/kv/kvclient/kvcoord/dist_sender.go#L1936)。

4. 实际发送 RPC 隐藏在 [`Transport` 接口](https://github.com/cockroachdb/cockroach/blob/4c76e19157ced57ba082ea0ab8cf5b317bdb5e1f/pkg/kv/kvclient/kvcoord/transport.go#L51)后面。具体来说，[`grpcTransport.SendNext()`](https://github.com/cockroachdb/cockroach/blob/4c76e19157ced57ba082ea0ab8cf5b317bdb5e1f/pkg/kv/kvclient/kvcoord/transport.go#L165) 通过 [`Internal` 服务](https://github.com/cockroachdb/cockroach/blob/4e4f31a0ed1a8ea985b9ab6f72e29266b259900e/pkg/roachpb/api.proto#L2224)向包含目标副本的节点进行 gRPC 调用。

5. 来自不同副本的（异步）响应被[组合](https://github.com/cockroachdb/cockroach/blob/904807640f6a28aa82c5e63f0b62de2dbf27b3a0/pkg/kv/kvclient/kvcoord/dist_sender.go#L1199)成单个 `BatchResponse`，最终从 `DistSender.Send()` 返回。

我们现已浏览了发生网关节点上的相关事情。接下来，我们将看看每个范围的"远程"端发生了什么。

### RPC 服务器: Node

我们已看到 `DistSender` 如何将 `BatchRequest` 拆分为部分批处理，每个部分批处理包含单个范围本地的命令，以及如何通过 RPC 将这些命令发送到其范围的租约持有者。我们现在转到这些 RPC 的服务器端。

[`Internal` RPC 服务](https://github.com/cockroachdb/cockroach/blob/4e4f31a0ed1a8ea985b9ab6f72e29266b259900e/pkg/roachpb/api.proto#L2224)由 [`Node`](https://github.com/cockroachdb/cockroach/blob/4e4f31a0ed1a8ea985b9ab6f72e29266b259900e/pkg/server/node.go#L156) 实现。它本身不做什么特别重要的事情，但将请求[委托](https://github.com/cockroachdb/cockroach/blob/4e4f31a0ed1a8ea985b9ab6f72e29266b259900e/pkg/server/node.go#L878)给 [`Stores`](https://github.com/cockroachdb/cockroach/blob/e8f925edd18d8800671a1a606f32d3ed9e299b3f/pkg/kv/kvserver/stores.go#L39)——一个磁盘数据存储的集合（参见设计文档的[架构部分](https://github.com/cockroachdb/cockroach/blob/master/docs/design.md#architecture)）。`Stores` 实现了 `Sender` 接口，与我们之前看到的层一样，并[识别](https://github.com/cockroachdb/cockroach/blob/e8f925edd18d8800671a1a606f32d3ed9e299b3f/pkg/kv/kvserver/stores.go#L186)哪个特定的存储包含目标副本（基于 `DistSender` 填写的[请求路由信息](https://github.com/cockroachdb/cockroach/blob/4e4f31a0ed1a8ea985b9ab6f72e29266b259900e/pkg/roachpb/api.proto#L1852)），然后将请求[路由](https://github.com/cockroachdb/cockroach/blob/e8f925edd18d8800671a1a606f32d3ed9e299b3f/pkg/kv/kvserver/stores.go#L191)到那里。

### Store

一个 [`Store`](https://github.com/cockroachdb/cockroach/blob/904807640f6a28aa82c5e63f0b62de2dbf27b3a0/pkg/kv/kvserver/store.go#L388) 通常代表一个物理磁盘设备。就我们的目的而言，它主要将请求[委托](https://github.com/cockroachdb/cockroach/blob/cdd48a011e849edd404a29b5a8bd52793f019c7d/pkg/kv/kvserver/store_send.go#L195)给 `Replica`，但它做的另一件有趣的事情是：在此事务的其他请求已在此节点上处理过的情况下，[更新](https://github.com/cockroachdb/cockroach/blob/cdd48a011e849edd404a29b5a8bd52793f019c7d/pkg/kv/kvserver/store_send.go#L156)当前请求的不确定性区间上限。

不确定性区间规定了哪些值由于节点之间的时钟偏移而不确定，即我们不知道这些值是在当前事务的序列化点之前还是之后写入的。此代码意识到，如果来自当前事务的请求之前已在此节点上处理过，则在处理其他请求时该节点的时间戳之后写入的任何值都不是不确定的。有关不确定性区间的更多详细信息，请参见设计文档的"选择时间戳"部分。

### Replica: 执行请求

回想一下，范围是由一个 [Raft 共识算法](https://raft.github.io/raft.pdf)实例管理的 KV 键空间的一个连续切片。一个 [`Replica`](https://github.com/cockroachdb/cockroach/blob/1041afb50f9bd2f7b3ccf704efb9cc2eac619b86/pkg/kv/kvserver/replica.go#L184) 是范围 Raft 集群中的一个节点，维护范围状态的一个副本。

`Replica` 是我们层次结构中的最终 `Sender`。所有其他 `Sender` 的作用主要是将请求路由到当前充当范围租约持有者的 `Replica`（一个承担一系列协调职责的_同侪之首_ `Replica`，我们将在下面探讨）。

副本以不同于写入请求的方式处理读取请求：读取直接求值，而写入将进入其生命周期的另一个大章节并通过 Raft 共识协议。这在 `Replica.Send()` 根据请求类型快速[分支](https://github.com/cockroachdb/cockroach/blob/31847acd14ed27a340dfc620a544c3e33cbd7c9a/pkg/kv/kvserver/replica_send.go#L100-L107)时显而易见。我们将分别讨论每条路径。

然而，在此之前，两条路径都包裹在 [`executeBatchWithConcurrencyRetries()`](https://github.com/cockroachdb/cockroach/blob/31847acd14ed27a340dfc620a544c3e33cbd7c9a/pkg/kv/kvserver/replica_send.go#L325) 中，它处理请求批次的并发控制：在一个大的[重试循环](https://github.com/cockroachdb/cockroach/blob/31847acd14ed27a340dfc620a544c3e33cbd7c9a/pkg/kv/kvserver/replica_send.go#L346)中，它将尝试通过 `concurrency.Manager.SequenceReq()` [获取闩锁](https://github.com/cockroachdb/cockroach/blob/31847acd14ed27a340dfc620a544c3e33cbd7c9a/pkg/kv/kvserver/replica_send.go#L357)（内部锁），然后[执行批次](https://github.com/cockroachdb/cockroach/blob/31847acd14ed27a340dfc620a544c3e33cbd7c9a/pkg/kv/kvserver/replica_send.go#L375)。任何与并发相关的错误将在可能的情况下在此处处理，然后重试批次，例如[写入意图解析](https://github.com/cockroachdb/cockroach/blob/31847acd14ed27a340dfc620a544c3e33cbd7c9a/pkg/kv/kvserver/replica_send.go#L400-L404)、[事务冲突](https://github.com/cockroachdb/cockroach/blob/31847acd14ed27a340dfc620a544c3e33cbd7c9a/pkg/kv/kvserver/replica_send.go#L405-L409)、[租约错误](https://github.com/cockroachdb/cockroach/blob/31847acd14ed27a340dfc620a544c3e33cbd7c9a/pkg/kv/kvserver/replica_send.go#L418-L427)等。

#### 并发控制

我们的请求很可能不会单独到达副本，而是伴随着来自许多不同节点、客户端和事务的各种其他请求。 [`concurrency.Manager`](https://github.com/cockroachdb/cockroach/blob/31847acd14ed27a340dfc620a544c3e33cbd7c9a/pkg/kv/kvserver/concurrency/concurrency_control.go#L148) 负责决定谁何时以及以什么顺序执行。总体而言，它允许读取者并发运行，而写入者单独运行，以 FIFO 顺序并考虑键区间、MVCC 时间戳和事务隔离等因素。它支持"闩锁"（latch），即在请求求值和复制期间获取的单请求内部锁，以及"锁"（lock），即跨请求的事务锁（例如对于未提交的写入），在事务完成之前持续存在——两者都有只读和读写访问模式。

首先，副本[找出](https://github.com/cockroachdb/cockroach/blob/31847acd14ed27a340dfc620a544c3e33cbd7c9a/pkg/kv/kvserver/replica_send.go#L330)需要为请求获取哪些闩锁和锁。为此，它为每个请求[查找](https://github.com/cockroachdb/cockroach/blob/31847acd14ed27a340dfc620a544c3e33cbd7c9a/pkg/kv/kvserver/replica_send.go#L330)命令，并[调用](https://github.com/cockroachdb/cockroach/blob/31847acd14ed27a340dfc620a544c3e33cbd7c9a/pkg/kv/kvserver/replica_send.go#L829)其 `DeclareKeys` 方法并传入请求头。此方法在[命令注册](https://github.com/cockroachdb/cockroach/blob/b7fb524c899c38e3e92c80d6b88f45afc9df7ca2/pkg/kv/kvserver/batcheval/command.go#L87)期间提供（参见例如 [`Put` 命令](https://github.com/cockroachdb/cockroach/blob/702d8bb2d2432c35aee90a543f7b2facc7c9c65e/pkg/kv/kvserver/batcheval/cmd_put.go#L23-L39)），通常会委托给 [`DefaultDeclareKey`](https://github.com/cockroachdb/cockroach/blob/a7472e3c109224d9dae3f739fe1d99fb12c22327/pkg/kv/kvserver/batcheval/declare.go#L24)（获取闩锁）或 [`DefaultDeclareIsolatedKeys`](https://github.com/cockroachdb/cockroach/blob/a7472e3c109224d9dae3f739fe1d99fb12c22327/pkg/kv/kvserver/batcheval/declare.go#L39)（获取锁和闩锁），针对头中[列出](https://github.com/cockroachdb/cockroach/blob/4e4f31a0ed1a8ea985b9ab6f72e29266b259900e/pkg/roachpb/api.proto#L50-L57)的键。

一旦知道了必要的闩锁和锁，请求将它们传递给并发管理器并[等待其轮次](https://github.com/cockroachdb/cockroach/blob/31847acd14ed27a340dfc620a544c3e33cbd7c9a/pkg/kv/kvserver/replica_send.go#L357)，通过调用 `Manager.SequenceReq()`。这将首先尝试在[闩锁管理器](https://github.com/cockroachdb/cockroach/blob/fd30130e281d712a52c7cf65988278a354e39250/pkg/kv/kvserver/spanlatch/manager.go#L58)中[获取闩锁](https://github.com/cockroachdb/cockroach/blob/31847acd14ed27a340dfc620a544c3e33cbd7c9a/pkg/kv/kvserver/concurrency/concurrency_manager.go#L206)，可能需要[等待](https://github.com/cockroachdb/cockroach/blob/fd30130e281d712a52c7cf65988278a354e39250/pkg/kv/kvserver/spanlatch/manager.go#L207)其他请求先释放它们。然后继续在[锁表](https://github.com/cockroachdb/cockroach/blob/31847acd14ed27a340dfc620a544c3e33cbd7c9a/pkg/kv/kvserver/concurrency/concurrency_control.go#L488)中[获取锁](https://github.com/cockroachdb/cockroach/blob/31847acd14ed27a340dfc620a544c3e33cbd7c9a/pkg/kv/kvserver/concurrency/concurrency_manager.go#L226)，[等待](https://github.com/cockroachdb/cockroach/blob/31847acd14ed27a340dfc620a544c3e33cbd7c9a/pkg/kv/kvserver/concurrency/concurrency_manager.go#L230)任何冲突的事务完成——这还将尝试[推送](https://github.com/cockroachdb/cockroach/blob/8132781d23003c0c6fa597d944fd94f665229093/pkg/kv/kvserver/concurrency/lock_table_waiter.go#L430)锁持有者的时间戳向前，这可能允许我们的事务在"其之前"运行，甚至根据事务优先级中止它。

一旦请求获取了其闩锁和锁，它就准备好[执行](https://github.com/cockroachdb/cockroach/blob/31847acd14ed27a340dfc620a544c3e33cbd7c9a/pkg/kv/kvserver/replica_send.go#L375)。此时，请求不再需要担心并发请求，因为它已经完全隔离。然而，有一个值得注意的例外：由并发事务写入的写入意图（新值），尚未完全迁移到并发管理器中，必须在遇到时进行处理。我们稍后会回来讨论这些。

#### 租约获取和重定向

`Replica.executeReadBatch()` 和 `Replica.executeWriteBatch()` 做的第一件事是调用 [`Replica.checkExecutionCanProceed()`](https://github.com/cockroachdb/cockroach/blob/1041afb50f9bd2f7b3ccf704efb9cc2eac619b86/pkg/kv/kvserver/replica.go#L1258) 执行一些预备检查。最重要的是，它检查请求是否到达了正确的位置，即副本是否是当前的租约持有者——记住很多路由都是基于缓存或直接猜测完成的。

租约持有者检查由 [`Replica.leaseGoodToGoRLocked()`](https://github.com/cockroachdb/cockroach/blob/1041afb50f9bd2f7b3ccf704efb9cc2eac619b86/pkg/kv/kvserver/replica.go#L1318) 执行，这本身就是一个兔子洞。简单来说，如果当前副本不是租约持有者，它返回 [`InvalidLeaseError`](https://github.com/cockroachdb/cockroach/blob/1f31803e9d158cae636b35856fb01aed9187ed93/pkg/kv/kvserver/replica_range_lease.go#L979)，这将[导致](https://github.com/cockroachdb/cockroach/blob/31847acd14ed27a340dfc620a544c3e33cbd7c9a/pkg/kv/kvserver/replica_send.go#L425)副本尝试[获取租约](https://github.com/cockroachdb/cockroach/blob/31847acd14ed27a340dfc620a544c3e33cbd7c9a/pkg/kv/kvserver/replica_send.go#L576)，或返回 [`NotLeaseholderError`](https://github.com/cockroachdb/cockroach/blob/1f31803e9d158cae636b35856fb01aed9187ed93/pkg/kv/kvserver/replica_range_lease.go#L1023-L1024)，该错误[返回给](https://github.com/cockroachdb/cockroach/blob/904807640f6a28aa82c5e63f0b62de2dbf27b3a0/pkg/kv/kvclient/kvcoord/dist_sender.go#L1977) `DistSender` 并[重定向](https://github.com/cockroachdb/cockroach/blob/904807640f6a28aa82c5e63f0b62de2dbf27b3a0/pkg/kv/kvclient/kvcoord/dist_sender.go#L1988)请求到真正的租约持有者。

请求租约是通过 [`pendingLeaseRequest`](https://github.com/cockroachdb/cockroach/blob/1f31803e9d158cae636b35856fb01aed9187ed93/pkg/kv/kvserver/replica_range_lease.go#L122) 辅助结构完成的，它合并对同一租约的多个请求，并最终构造一个 [`RequestLeaseRequest`](https://github.com/cockroachdb/cockroach/blob/1f31803e9d158cae636b35856fb01aed9187ed93/pkg/kv/kvserver/replica_range_lease.go#L269)，该请求被[发送执行](https://github.com/cockroachdb/cockroach/blob/1f31803e9d158cae636b35856fb01aed9187ed93/pkg/kv/kvserver/replica_range_lease.go#L423)直接到副本。在请求租约的情况下，`redirectOnOrAcquireLease` 将[等待](https://github.com/cockroachdb/cockroach/blob/1f31803e9d158cae636b35856fb01aed9187ed93/pkg/kv/kvserver/replica_range_lease.go#L1182)该请求完成并检查是否成功。如果成功，批次将被重试。如果不成功，请求将重定向到实际的租约持有者。

#### 读取请求路径

一旦租约问题解决，`Replica.executeReadOnlyBatch()` 就可以开始实际读取了。它[获取](https://github.com/cockroachdb/cockroach/blob/31847acd14ed27a340dfc620a544c3e33cbd7c9a/pkg/kv/kvserver/replica_read.go#L57)一个存储引擎句柄，并在其上[执行](https://github.com/cockroachdb/cockroach/blob/31847acd14ed27a340dfc620a544c3e33cbd7c9a/pkg/kv/kvserver/replica_read.go#L83)批次，最终将控制转移到 [`Replica.evaluateBatch()`](https://github.com/cockroachdb/cockroach/blob/759fdac56ec59c75aa872e8e00d0a614bcc593a3/pkg/kv/kvserver/replica_evaluate.go#L146)，它为批处理中的每个请求[调用 `Replica.evaluateCommand()`](https://github.com/cockroachdb/cockroach/blob/759fdac56ec59c75aa872e8e00d0a614bcc593a3/pkg/kv/kvserver/replica_evaluate.go#L271-L272)。这在[命令注册表](https://github.com/cockroachdb/cockroach/blob/b7fb524c899c38e3e92c80d6b88f45afc9df7ca2/pkg/kv/kvserver/batcheval/command.go#L69-L70)中[查找](https://github.com/cockroachdb/cockroach/blob/759fdac56ec59c75aa872e8e00d0a614bcc593a3/pkg/kv/kvserver/replica_evaluate.go#L497)请求方法的命令，并将执行[传递](https://github.com/cockroachdb/cockroach/blob/759fdac56ec59c75aa872e8e00d0a614bcc593a3/pkg/kv/kvserver/replica_evaluate.go#L506-L510)给它。

一个典型的读取请求是 [`ScanRequest`](https://github.com/cockroachdb/cockroach/blob/4e4f31a0ed1a8ea985b9ab6f72e29266b259900e/pkg/roachpb/api.proto#L384)，由 [`batcheval.Scan`](https://github.com/cockroachdb/cockroach/blob/261fc356765e61215478ba697d4fe252b9466c8f/pkg/kv/kvserver/batcheval/cmd_scan.go#L31) 求值。代码相当简洁，主要是[调用](https://github.com/cockroachdb/cockroach/blob/261fc356765e61215478ba697d4fe252b9466c8f/pkg/kv/kvserver/batcheval/cmd_scan.go#L54-L55)底层存储引擎的相应函数。

#### 引擎: 磁盘存储

我们正在接近 CockroachDB 栈的底部。[`Engine`](https://github.com/cockroachdb/cockroach/blob/83185789481f23bd6c4938a3588203f4cd4eef89/pkg/storage/engine.go#L640) 是一个抽象出不同磁盘数据存储的接口。从 21.1 版本开始，我们只支持 [Pebble](https://github.com/cockroachdb/pebble)，一个基于 [RocksDB](https://rocksdb.org)/[LevelDB](https://github.com/google/leveldb) 的内部开发的嵌入式键/值数据库。RocksDB 以前被使用，但为了[性能、稳定性和更紧密的集成](https://www.cockroachlabs.com/blog/pebble-rocksdb-kv-store/)而被替换。尽管存储引擎是服务请求的关键部分，为简洁起见，我们不会在此深入探讨 Pebble 的内部细节。

对于读取，我们通常使用 [`storage.Reader`](https://github.com/cockroachdb/cockroach/blob/83185789481f23bd6c4938a3588203f4cd4eef89/pkg/storage/engine.go#L349) 接口，它是 `Engine` 的子集。上一节中的 `Scan` 命令将此 `Reader` 传递给 [`storage.MVCCScanToBytes()`](https://github.com/cockroachdb/cockroach/blob/129b2dcaecfce350d4ed3a4b1f6b4b654b0bd11e/pkg/storage/mvcc.go#L2585)，这是一个辅助函数，从存储引擎中获取给定键区间中的键/值对列表。

CockroachDB 存储层使用[多版本并发控制 (MVCC)](https://en.wikipedia.org/wiki/Multiversion_concurrency_control)，为给定键存储多个版本的 Protobuf 编码的 [`Value`](https://github.com/cockroachdb/cockroach/blob/0254e86051c310d38e5668cf82ba128ff4c1ab28/pkg/roachpb/data.proto#L84)。每个版本由值的 [`Timestamp`](https://github.com/cockroachdb/cockroach/blob/0254e86051c310d38e5668cf82ba128ff4c1ab28/pkg/roachpb/data.proto#L90) 标识，给定事务可见的最新版本由其[读取时间戳](https://github.com/cockroachdb/cockroach/blob/0254e86051c310d38e5668cf82ba128ff4c1ab28/pkg/roachpb/data.proto#L388)给出。作为一个特殊情况，新值（称为写入意图）在写入时不带时间戳，一旦事务提交，这些必须通过用事务的最终[写入时间戳](https://github.com/cockroachdb/cockroach/blob/175e2daacc94cb47350be4c668e4236664515bbd/pkg/storage/enginepb/mvcc3.proto#L91)重写来清理——稍后会详细介绍。

MVCC 读取的主力是 [`storage.MVCCIterator`](https://github.com/cockroachdb/cockroach/blob/83185789481f23bd6c4938a3588203f4cd4eef89/pkg/storage/engine.go#L92)，它按顺序遍历 MVCC 键/值对。`MVCCScanToBytes` [使用它](https://github.com/cockroachdb/cockroach/blob/129b2dcaecfce350d4ed3a4b1f6b4b654b0bd11e/pkg/storage/mvcc.go#L2592-L2593)，借助 [`pebbleMVCCScanner`](https://github.com/cockroachdb/cockroach/blob/129b2dcaecfce350d4ed3a4b1f6b4b654b0bd11e/pkg/storage/mvcc.go#L2385) 在存储引擎中[扫描](https://github.com/cockroachdb/cockroach/blob/129b2dcaecfce350d4ed3a4b1f6b4b654b0bd11e/pkg/storage/pebble_mvcc_scanner.go#L215)键/值版本，[收集](https://github.com/cockroachdb/cockroach/blob/129b2dcaecfce350d4ed3a4b1f6b4b654b0bd11e/pkg/storage/pebble_mvcc_scanner.go#L329)每个键的适当版本，并[返回](https://github.com/cockroachdb/cockroach/blob/129b2dcaecfce350d4ed3a4b1f6b4b654b0bd11e/pkg/storage/mvcc.go#L2403)给调用者。

然而，如果扫描[遇到来自不同事务的冲突写入意图](https://github.com/cockroachdb/cockroach/blob/129b2dcaecfce350d4ed3a4b1f6b4b654b0bd11e/pkg/storage/mvcc.go#L2400-L2402)，这些必须首先处理。

#### 意图解析

每当请求遇到不是由其自身事务写入的写入意图（即未提交的值）时，它就面临与其他事务的可能冲突。为解决此冲突，MVCC 存储操作返回一个包含这些意图的 `WriteIntentError`。这在堆栈上层被 `Replica.executeBatchWithConcurrencyRetries` [捕获](https://github.com/cockroachdb/cockroach/blob/31847acd14ed27a340dfc620a544c3e33cbd7c9a/pkg/kv/kvserver/replica_send.go#L400-L404)，它[委托](https://github.com/cockroachdb/cockroach/blob/31847acd14ed27a340dfc620a544c3e33cbd7c9a/pkg/kv/kvserver/replica_send.go#L518)冲突解决给 [`concurrency.Manager.HandleWriterIntentError`](https://github.com/cockroachdb/cockroach/blob/31847acd14ed27a340dfc620a544c3e33cbd7c9a/pkg/kv/kvserver/concurrency/concurrency_manager.go#L309)。这将首先[将意图添加到锁表](https://github.com/cockroachdb/cockroach/blob/31847acd14ed27a340dfc620a544c3e33cbd7c9a/pkg/kv/kvserver/concurrency/concurrency_manager.go#L325)，因为它显然还不知道它们（否则请求不会失败），然后[等待](https://github.com/cockroachdb/cockroach/blob/31847acd14ed27a340dfc620a544c3e33cbd7c9a/pkg/kv/kvserver/concurrency/concurrency_manager.go#L346)冲突事务完成。作为此等待的一部分，它将尝试使用 [`IntentResolver`](https://github.com/cockroachdb/cockroach/blob/3575dea3e6b084a63a1432672705f949572bc211/pkg/kv/kvserver/intentresolver/intent_resolver.go#L132)[推送](https://github.com/cockroachdb/cockroach/blob/8132781d23003c0c6fa597d944fd94f665229093/pkg/kv/kvserver/concurrency/lock_table_waiter.go#L430)锁持有者以解决冲突的意图。

要解决意图，我们首先需要弄清楚拥有它的事务是否仍在挂起——它可能已经提交或中止。如果是，我们可以尝试推送它，使其在更高的时间戳重新启动或被中止。如果成功，则可以通过用提交值替换或丢弃意图来解决，具体取决于最终的事务状态。否则，当前事务必须等待它完成。

确定其他事务的状态并推送它发生在 [`IntentResolver.MaybePushTransaction`](https://github.com/cockroachdb/cockroach/blob/3575dea3e6b084a63a1432672705f949572bc211/pkg/kv/kvserver/intentresolver/intent_resolver.go#L379) 中，一系列 `PushTxnRequest` 被批处理并发送到集群（使用 `Sender` 的层级结构将它们路由到各个事务记录）。如果冲突事务仍在挂起，关于是否推送它的决定是在 [`PushTxnRequest` 处理的深层](https://github.com/cockroachdb/cockroach/blob/759fdac56ec59c75aa872e8e00d0a614bcc593a3/pkg/kv/kvserver/batcheval/cmd_push_txn.go#L254)基于事务的相对优先级做出的。解决意图本身是通过调用 [`IntentResolve.ResolveIntent`](https://github.com/cockroachdb/cockroach/blob/8132781d23003c0c6fa597d944fd94f665229093/pkg/kv/kvserver/concurrency/lock_table_waiter.go#L480) 完成的。

一旦冲突的意图被解决，无论是通过推送其他事务还是等待它完成，我们可以从之前中断的地方继续，回到 `Replica.executeBatchWithConcurrencyRetries` 并[重试](https://github.com/cockroachdb/cockroach/blob/31847acd14ed27a340dfc620a544c3e33cbd7c9a/pkg/kv/kvserver/replica_send.go#L346)请求。

#### 写入请求路径

写入请求在概念上比读取更有趣，因为它们不是简单地由一个节点/副本服务。相反，它们通过 Raft 共识协议，该协议最终将它们提交到一个由范围的所有副本应用的复制有序命令日志中（详情参见设计文档的 Raft 部分）。启动此过程的副本，就像在读取情况下一样，是租约持有者。因此，租约持有者上的执行分为两个阶段：在 Raft 之前（"上游"）和 Raft 之后（"下游"）。上游阶段最终将阻塞，直到提交的 Raft 命令被*本地*应用，此时未来的读取保证能看到其效果。

写入请求经过与读取请求相同的大部分准备工作，例如并发控制和租约获取/重定向。然而，它有一个与读取路径紧密关联的额外步骤：它[咨询](https://github.com/cockroachdb/cockroach/blob/08322ffa1b3060a2d4cb7e3ff3ee8a0b370cf1e6/pkg/kv/kvserver/replica_write.go#L119)[时间戳缓存](https://github.com/cockroachdb/cockroach/blob/e6e428f8029e4e90f391cb746e2339ec6fe7a581/pkg/kv/kvserver/tscache/cache.go#L53)，并在必要时[将事务的时间戳向前移动](https://github.com/cockroachdb/cockroach/blob/e6e428f8029e4e90f391cb746e2339ec6fe7a581/pkg/kv/kvserver/replica_tscache.go#L316)。此结构是一个有界的内存缓存，包含给定键区间被读取的最新时间戳，用于防止快照隔离的违反：在低于先前读取的时间戳处的键写入必须不能成功（参见 [Matt 博客文章](https://www.cockroachlabs.com/blog/serializable-lockless-distributed-isolation-cockroachdb/)中的"Read-Write Conflicts - Read Timestamp Cache"部分）。此缓存在[读取路径结语](https://github.com/cockroachdb/cockroach/blob/31847acd14ed27a340dfc620a544c3e33cbd7c9a/pkg/kv/kvserver/replica_read.go#L124)和[写入路径提议结语](https://github.com/cockroachdb/cockroach/blob/e4bb550d9abae97ab09c099592f32f88ad895177/pkg/kv/kvserver/replica_proposal.go#L144)中更新。

接下来，我们可以[对请求求值并提议生成的 Raft 命令](https://github.com/cockroachdb/cockroach/blob/08322ffa1b3060a2d4cb7e3ff3ee8a0b370cf1e6/pkg/kv/kvserver/replica_write.go#L149)。我们将在下面描述此过程，但在此之前，让我们看看当前方法在此之后做了什么。该调用返回一个通道，我们将[等待它](https://github.com/cockroachdb/cockroach/blob/08322ffa1b3060a2d4cb7e3ff3ee8a0b370cf1e6/pkg/kv/kvserver/replica_write.go#L207)。这是我们上面提到的解耦点——我们将控制权交给 Raft 机制的地方。提交提议的 `Replica` 接受其作为仅仅是众多副本之一的角色，并等待共识组以锁步方式取得进展。当（本地）副本应用了相应的命令时，通道将收到结果，这只能在命令被提交到共享的 Raft 日志（全局操作）之后发生。

#### 请求求值

如所承诺的，让我们看看 `Replica.evalAndPropose()` 内部发生了什么。我们首先对 KV 请求求值，将其转换为 Raft 命令。这是通过调用 [`requestToProposal()`](https://github.com/cockroachdb/cockroach/blob/6b9168de33628257f3f1fdb17af1f8205ee21d32/pkg/kv/kvserver/replica_raft.go#L89) 完成的，它快速调用 [`evaluateProposal()`](https://github.com/cockroachdb/cockroach/blob/e4bb550d9abae97ab09c099592f32f88ad895177/pkg/kv/kvserver/replica_proposal.go#L917)，后者又调用 [`evaluateWriteBatch()`](https://github.com/cockroachdb/cockroach/blob/e4bb550d9abae97ab09c099592f32f88ad895177/pkg/kv/kvserver/replica_proposal.go#L784)。这最后一个方法"模拟"了请求的执行，将所有将会发生的 `Engine` 更改记录到一个"批处理"中（这些批处理是 Pebble 建模事务的方式）。此批处理将被[序列化](https://github.com/cockroachdb/cockroach/blob/e4bb550d9abae97ab09c099592f32f88ad895177/pkg/kv/kvserver/replica_proposal.go#L834)为 Raft 命令。如果我们现在提交此批处理，更改将生效，但只在这个一个副本上——潜在的数据一致性问题。相反，我们[中止它](https://github.com/cockroachdb/cockroach/blob/e4bb550d9abae97ab09c099592f32f88ad895177/pkg/kv/kvserver/replica_proposal.go#L790)。当命令"从 Raft 出来"时，它将被复活，我们将在下面看到。

模拟部分发生在 [`evaluateWriteBatchWrapper()`](https://github.com/cockroachdb/cockroach/blob/08322ffa1b3060a2d4cb7e3ff3ee8a0b370cf1e6/pkg/kv/kvserver/replica_write.go#L693) 内部。它接受 `roachpb.BatchRequest`（我们一直在处理的 KV 请求），[分配一个 `engine.Batch`](https://github.com/cockroachdb/cockroach/blob/08322ffa1b3060a2d4cb7e3ff3ee8a0b370cf1e6/pkg/kv/kvserver/replica_write.go#L702)，并委托给 [`evaluateBatch()`](https://github.com/cockroachdb/cockroach/blob/759fdac56ec59c75aa872e8e00d0a614bcc593a3/pkg/kv/kvserver/replica_evaluate.go#L146)。这位"老兄"最终[遍历](https://github.com/cockroachdb/cockroach/blob/759fdac56ec59c75aa872e8e00d0a614bcc593a3/pkg/kv/kvserver/replica_evaluate.go#L235)批处理中的单个请求，对每个请求调用 [`evaluateCommand()`](https://github.com/cockroachdb/cockroach/blob/759fdac56ec59c75aa872e8e00d0a614bcc593a3/pkg/kv/kvserver/replica_evaluate.go#L4823)。我们之前在读取路径上见过 `evaluateCommand`：它查找每个请求方法的对应命令并[调用它](https://github.com/cockroachdb/cockroach/blob/759fdac56ec59c75aa872e8e00d0a614bcc593a3/pkg/kv/kvserver/replica_evaluate.go#L507)。一个这样的方法是 [`batcheval.Put`](https://github.com/cockroachdb/cockroach/blob/702d8bb2d2432c35aee90a543f7b2facc7c9c65e/pkg/kv/kvserver/batcheval/cmd_put.go#L42-L44)，它为一个键写入一个值。在其中我们看到一个[对引擎的调用](https://github.com/cockroachdb/cockroach/blob/702d8bb2d2432c35aee90a543f7b2facc7c9c65e/pkg/kv/kvserver/batcheval/cmd_put.go#L57)来执行写入——但请记住，这一切都在一个 Pebble 事务（`storage.Batch`）内部执行。

这一切都是为了记录需要向 Raft 提议的引擎更改。让我们展开栈回到 `evalAndPropose()`（我们开始本节的地方），看看 [`requestToProposal()`](https://github.com/cockroachdb/cockroach/blob/6b9168de33628257f3f1fdb17af1f8205ee21d32/pkg/kv/kvserver/replica_raft.go#L89) 的结果发生了什么。

首先，我们获取 [`proposalCh`](https://github.com/cockroachdb/cockroach/blob/6b9168de33628257f3f1fdb17af1f8205ee21d32/pkg/kv/kvserver/replica_raft.go#L107)，该通道将返回给调用者以等待提议的本地应用。它被[插入到](https://github.com/cockroachdb/cockroach/blob/33c18ad1bcdb37ed6ed428b7527148977a8c566a/pkg/storage/replica.go#L2292)待处理提议映射中——一个将在命令被*应用*和在通道上阻塞等待本地应用的 `executeWriteBatch()` 之间建立连接的结构。更重要的是，提议被传递给 [`Replica.propose`](https://github.com/cockroachdb/cockroach/blob/6b9168de33628257f3f1fdb17af1f8205ee21d32/pkg/kv/kvserver/replica_raft.go#L209)，它将提议插入[提议缓冲区](https://github.com/cockroachdb/cockroach/blob/6b9168de33628257f3f1fdb17af1f8205ee21d32/pkg/kv/kvserver/replica_raft.go#L365)，最终被[提议给 `raftGroup`](https://github.com/cockroachdb/cockroach/blob/8de3463817826d431bf0c6433ad3eb81bf06c9b3/pkg/kv/kvserver/replica_proposal_buf.go#L692)。

此 `raftGroup` 是 Raft 共识集群的句柄，由 [Etcd Raft 库](https://github.com/etcd-io/etcd/)实现。我们将其视为一个黑盒，向其提交命令提议，它处理通过 Raft 共识运行它们，将它们提交到复制的命令日志，并在每个副本上应用它们。

一个值得注意的例外是具有 [`AsyncConsensus` 标志](https://github.com/cockroachdb/cockroach/blob/4e4f31a0ed1a8ea985b9ab6f72e29266b259900e/pkg/roachpb/api.pb.go#L6270)设置的请求。这些请求用于[流水线写入](https://www.cockroachlabs.com/blog/transaction-pipelining/)和[并行提交协议](https://www.cockroachlabs.com/blog/parallel-commits/)，以避免等待写入通过 Raft，这显著提高了写入吞吐量。相反，正如我们之前简要提到的，`TxnCoordSender` 将在必要时检查这些写入是否已完成——特别是在提交事务时。对于这些请求，`evalAndPropose()` [立即通过 `proposalCh` 发送结果](https://github.com/cockroachdb/cockroach/blob/88d9b291f33abaf654f816400418fb33357e74ba/pkg/kv/kvserver/replica_raft.go#L153)，导致 `executeWriteBatch()` 返回而不是等待 Raft 应用。

#### Raft 命令应用

我们已经看到命令如何"进入" Raft，但它们如何"出来"呢？为了与 Etcd Raft 库集成，我们实现了一个状态机，它将日志命令应用到上面。对此的完整描述超出了本文的范围，但可以说这涉及 [`raftProcessor`](https://github.com/cockroachdb/cockroach/blob/8c5253bbccea8966fe20e3cb8cc6111712f6f509/pkg/kv/kvserver/scheduler.go#L135) 接口，由我们的老朋友 `Store` 实现。

重要的方法是 [`Store.processReady()`](https://github.com/cockroachdb/cockroach/blob/6b9168de33628257f3f1fdb17af1f8205ee21d32/pkg/kv/kvserver/store_raft.go#L506)，它最终通过 [`handleRaftReadyRaftMuLocked()`](https://github.com/cockroachdb/cockroach/blob/6b9168de33628257f3f1fdb17af1f8205ee21d32/pkg/kv/kvserver/replica_raft.go#L495) 回调到正在修改的 `Replica`。它接收序列化的 `storage.Batch` 并将其[应用到状态机](https://github.com/cockroachdb/cockroach/blob/6e4c3ae05d92f4104b7b9fbb33f9d1cecf055fd3/pkg/kv/kvserver/apply/task.go#L286)，通过[将批处理提交到 Pebble](https://github.com/cockroachdb/cockroach/blob/6b9168de33628257f3f1fdb17af1f8205ee21d32/pkg/kv/kvserver/replica_application_state_machine.go#L894)[应用](https://github.com/cockroachdb/cockroach/blob/6b9168de33628257f3f1fdb17af1f8205ee21d32/pkg/kv/kvserver/replica_raft.go#L839)已提交的条目。命令现在已在此特定副本上应用，但请记住此过程发生在_每个_副本上。

不要忘记等待的提议者，它仍然在 `executeWriteBatch()` 中的提议通道上阻塞。在命令应用后，在提议副本（即通常是租约持有者）上，我们需要通过通道返回结果。这发生在命令应用的[末尾](https://github.com/cockroachdb/cockroach/blob/6e4c3ae05d92f4104b7b9fbb33f9d1cecf055fd3/pkg/kv/kvserver/apply/task.go#L297)，其中在每个命令上调用 [`AckOutcomeAndFinish()`](https://github.com/cockroachdb/cockroach/blob/6e4c3ae05d92f4104b7b9fbb33f9d1cecf055fd3/pkg/kv/kvserver/replica_application_cmd.go#L166)：如果命令是本地的，它最终将通过[发送](https://github.com/cockroachdb/cockroach/blob/e4bb550d9abae97ab09c099592f32f88ad895177/pkg/kv/kvserver/replica_proposal.go#L161)结果来[通知](https://github.com/cockroachdb/cockroach/blob/e4bb550d9abae97ab09c099592f32f88ad895177/pkg/kv/kvserver/replica_proposal.go#L145)提议者。

我们现在已经走完了完整的一圈：提议者解除阻塞并收到其结果，展开堆栈让客户端知道请求已完成。响应通过 `Sender` 层级结构传回，从租约持有者节点到网关节点，通过 DistSQL 流和事务协调器，经由会话管理器和 `pgwire` 实现，最终到达 SQL 客户端。
