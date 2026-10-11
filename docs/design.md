# 数析通 设计思路

## 1. 项目定位

数析通是一个数据工程原型，做三件事：自然语言问数、Spark SQL 作业（ETL）、以及 Spark 作业诊断。三件事共用一条原则——把自然语言需求收敛成结构化计划，只允许调用注册在册的工具，每一步都留下可回放的记录。

三条业务线：

- **问数**：自然语言到 SQL，执行前做多层校验，只在只读边界内取数。
- **ETL**：以 Spark SQL 表达加工逻辑，支持本地模式执行，步骤之间有依赖。
- **作业诊断**：解析 Spark EventLog，把 Stage / Task / Shuffle / spill 指标聚合成结构化结论。

共同约束是审计：无论哪条线，产出最终都挂在同一个 `task_id` 下。

原型阶段明确不做的事：

- 不做通用 BI 平台，不追求图表和看板。
- 不做生产级多租户权限体系，只读边界用工程手段约束。
- 不做集群调度，以 Spark local 模式为主，Docker 只负责环境一致。

## 2. 总体架构

```
接入层    CLI / HTTP API
          ↑ 提交需求、查询状态、取审计产物

规划层    Planner —— 自然语言 → Plan（结构化步骤 DAG）
          ↑ Plan 是纯数据，可入库、可回放、可 diff

编排层    Orchestrator —— 依赖解析、状态机、重试
          ↑ 按拓扑序推进 Step，落 Job/Attempt 记录

能力层    Skills —— ask / etl / diagnose / dq
          ↑ 每个 Skill 是固定接口，参数经 schema 校验

治理层    语义层 / SQL 记忆 / SQL 校验器 / 审计归档
          ↑ 横切所有 Skill 的规则与数据

执行层    SparkSession(local) / SQLite / 文件系统
```

分层的核心目的是把**规划**和**执行**拆开。Planner 只产出数据，不碰执行；Orchestrator 只按计划推进，不重新理解需求。这样一次任务的全过程都能从库里重放：计划长什么样、每一步用了什么参数、第几次尝试失败、产物在哪。

## 3. 核心概念

| 概念 | 含义 |
|---|---|
| Request | 用户的原始需求，一段自然语言 |
| Plan | 结构化计划，由若干 Step 和依赖关系组成 |
| Step | 一步操作，绑定一个 Skill 和一组参数 |
| Job | 一次任务执行，用 `task_id` 标识 |
| Attempt | 某个 Step 的一次尝试，一个 Step 可以多次尝试 |
| Artifact | 产物文件，SQL、结果集、报告、图等 |

数据流：

```
自然语言 → Planner → Plan（入库）
         → Orchestrator 按拓扑序执行 Step
         → Skill 产出 Artifact，同时写 Attempt 记录
         → 诊断 / 审计模块汇总到同一 task_id
         → CLI 按 task_id 查询全过程
```

## 4. 模块一：问数安全链路

### 4.1 流程

```
自然语言
  → 语义层解析：把实体、指标、维度归一到受控词表
  → 记忆检索：召回相似的成功 SQL 作为参考
  → 生成 SQL
  → 静态校验（AST 层）
  → 执行约束（只读 + 行数 + 超时）
  → 返回结果
  → 成功则回写记忆
```

### 4.2 语义层

语义层是问数的地基。它把"物理表长什么样"升级成"业务上能问什么"，内容包括：

- 表与列：物理名、业务名、类型、是否可查询。
- 指标：指标名、口径、对应的 SQL 表达式。
- 维度：可用于分组、筛选的字段。
- 同义词：把用户口语映射到标准对象名。
- 枚举值：状态码之类的取值含义。
- 可见性：哪些对象对问数开放。

语义层要版本化。每次执行的 Plan 与 SQL 都记录当时用的语义层版本，否则历史 SQL 无法解释。

### 4.3 校验闸门

安全不靠单点，靠逐层收窄：

| 层 | 检查内容 |
|---|---|
| L1 语法 | 解析成 AST；拒绝多语句、拒绝注释绕过 |
| L2 语句类型 | 白名单只放 SELECT 与 WITH...SELECT；其余一律拒绝 |
| L3 对象 | 表名、列名必须命中语义层白名单；`SELECT *` 展开后再校验 |
| L4 危险构造 | 拒绝 `INTO OUTFILE`、临时函数、`SET`、动态执行 |
| L5 数据源 | Spark catalog 指向只读视图；文件挂载只读 |
| L6 资源 | 强制 LIMIT 或结果截断、查询超时、返回字节上限 |
| L7 审计 | 记录原始问题、生成 SQL、每层校验结论、耗时、行数 |

L1–L4 属于软件层拦截，能挡住绝大多数误操作，但绕过风险始终存在。真正兜底的是 L5：即使校验被绕过，数据源本身也不接受写操作。这两层都要有，缺一层都不算安全。

### 4.4 SQL 记忆

记忆库只收成功执行过的 SQL。存的内容包括原始问题、规范化后的问题、SQL、涉及的表与指标、语义层版本、执行统计。

召回分两步：先用结构化条件过滤（表、指标有交集），再用向量相似度排序取 top-k，作为生成 SQL 时的参考。校验失败或执行失败的 SQL 不入库，避免污染。

## 5. 模块二：任务状态治理

### 5.1 状态模型

三层结构：Job 装整体，Step 表达计划里的一步，Attempt 记录一次尝试。

```
Job:   PENDING → RUNNING → SUCCEEDED / FAILED / CANCELLED
Step:  PENDING → READY → RUNNING → SUCCEEDED / FAILED / SKIPPED
Attempt: RUNNING → SUCCEEDED / FAILED_RETRYABLE / FAILED_BUSINESS
```

Orchestrator 按依赖判定流转：依赖全部成功时把 Step 从 PENDING 推到 READY，依赖中任一步骤最终失败时把下游标记为 SKIPPED。

### 5.2 错误分类

这是整个治理模块最关键的判断：区分**可重试**和**业务失败**。

- **可重试**：瞬时问题。连接超时、资源不足、临时文件锁、Spark 偶发 task 失败。
- **业务失败**：确定性错误。表或字段不存在、语义校验不通过、数据质量不达标、除零。

分类靠显式规则：异常类型 + 错误码 + 消息模式，写成一个分类器。不能让模型来判断某次失败要不要重试，否则重试逻辑会变得不可预测。

重试策略：只对可重试错误触发，指数退避，设尝试上限。业务失败直接终止该 Step，不浪费时间。

### 5.3 SQLite 表结构（草案）

```sql
job(
  job_id TEXT PRIMARY KEY,
  task_id TEXT,
  kind TEXT,              -- ask / etl / diagnose
  request_text TEXT,
  plan_json TEXT,
  semantic_version TEXT,
  status TEXT,
  created_at TEXT, updated_at TEXT
)

step(
  step_id TEXT PRIMARY KEY,
  job_id TEXT REFERENCES job,
  name TEXT, skill TEXT,
  deps_json TEXT, params_json TEXT,
  status TEXT, ordinal INTEGER
)

attempt(
  attempt_id TEXT PRIMARY KEY,
  step_id TEXT REFERENCES step,
  seq INTEGER,
  status TEXT,
  error_class TEXT,       -- retryable / business
  error_code TEXT, error_msg TEXT,
  config_json TEXT,       -- 本次执行的配置快照
  spark_app_id TEXT,
  started_at TEXT, ended_at TEXT, duration_ms INTEGER
)

artifact(
  artifact_id TEXT PRIMARY KEY,
  job_id TEXT, step_id TEXT, attempt_id TEXT,
  kind TEXT, path TEXT, bytes INTEGER, sha256 TEXT,
  created_at TEXT
)

sql_memory(
  memory_id TEXT PRIMARY KEY,
  norm_question TEXT, question TEXT, sql TEXT,
  tables_json TEXT, metrics_json TEXT,
  semantic_version TEXT,
  success_count INTEGER, fail_count INTEGER,
  embedding BLOB, created_at TEXT
)
```

SQLite 开启 WAL 模式。要注意它同一时刻只有一个写者，所以状态写入要集中到一个写入路径串行化，不要多个执行线程直接并发写。

### 5.4 幂等与产物

产物目录按 `artifacts/{task_id}/{step}/{attempt}/` 组织。重跑同一任务时新起 attempt，不覆盖历史，这样失败现场可以保留下来复查。

## 6. 模块三：作业诊断与审计

### 6.1 EventLog 解析

Spark EventLog 是 JSON Lines 格式，每行一个监听器事件。诊断需要的核心事件：

- `SparkListenerApplicationStart/End`：应用边界。
- `SparkListenerSQLExecutionStart/End`：SQL 与 execution id 的对应。
- `SparkListenerStageSubmitted/StageCompleted`：Stage 结构、父 Stage。
- `SparkListenerJobStart/End`：作业与 Stage 的对应。
- `SparkListenerTaskEnd`：单个 task 的全部指标。

`TaskEnd` 里能拿到 task 时长、失败与推测执行标记、shuffle 读写字节数与记录数、内存与磁盘 spill、GC 时间、输入输出量。这些是诊断的证据来源。

### 6.2 指标聚合

按 Stage 和 SQL execution 两个维度聚合：

- Stage 级：task 数、失败数、重试数、shuffle 读写量、spill 量、task 时长分布。
- 倾斜检测：`max(task duration) / median(task duration)` 比值、shuffle read 分布。
- 时间构成：GC、序列化、执行时间占比。
- 数据量：输入字节、输出行数。

### 6.3 诊断规则

诊断结论由规则引擎产出，不由模型自由发挥。规则形式是「指标条件 → 结构化结论」：

| 规则 | 触发条件 | 建议 |
|---|---|---|
| 内存压力 | disk spill 超阈值且 shuffle write 大 | 提高 shuffle 并行度，降低单分区数据量 |
| 数据倾斜 | task 时长 max/median 超过阈值 | salting，或开启 AQE skew join |
| shuffle 放大 | shuffle read 远超输入量 | 调整 join 顺序，过滤下推 |
| 资源不稳 | task 失败重试比例高 | 检查内存与 GC 配置 |
| SQL 定位 | execution 关联到 Stage | 把结论落到具体 SQL 上 |

输出是结构化的 `diagnosis`：规则 id、严重级别、证据指标、建议。不是一段自由文本，这样前端和审计都能复用。

### 6.4 审计归档

同一个 `task_id` 下的产物：

```
artifacts/{task_id}/
  sql/        生成的 SQL 与校验记录
  result/     问数结果集
  dq/         数据质量报告
  graph/      依赖图与血缘图
  diag/       EventLog 诊断报告 + 原始日志副本
  manifest.json  产物清单，含 sha256
```

`manifest.json` 记录每个产物的路径、大小、哈希和时间，作为完整性校验的入口。

## 7. 目录结构（草案）

```
shuxitong/
  pyproject.toml
  README.md
  Dockerfile
  docker-compose.yml
  .env.example
  docs/
  semantic/
    orders.yml              按主题域分文件
    customers.yml
  data/
    synthetic/              合成数据产物，不进版本库
  src/shuxitong/
    cli/                    命令行入口
    planner/                自然语言 → Plan
    orchestrator/           DAG 调度与状态机
    datagen/                合成数据生成器
    model/                  ModelClient 抽象与实现
    skills/
      ask/                  Text-to-SQL
      etl/                  Spark SQL 作业
      diagnose/             EventLog 诊断
      dq/                   数据质量规则
    governance/
      semantic/             语义层加载、编译与漂移检测
      memory/               SQL 记忆与召回
      validator/            SQL AST 校验
      audit/                产物归档与 manifest
    store/                  SQLite 访问层与迁移
    spark/                  SparkSession 管理、EventLog 解析
    models/                 Pydantic 模型
  tests/
```

## 8. 技术选型

### Python

项目的主语言。选它的直接原因是 Spark 官方提供 PySpark API，编排、文本处理、规则校验这些周边工作也有成熟生态。

在本项目里，Python 负责规划、编排、校验、EventLog 解析和状态持久化；真正的计算交给 Spark 的 JVM 侧，所以 GIL 不构成瓶颈。依赖用 `uv` 管理。

### Spark SQL

执行引擎，同时承担 ETL 和问数的计算。核心概念值得先建立模型：

- **SparkSession**：应用入口，持有 catalog 和配置。
- **DataFrame / SQL**：两种等价的表达方式，SQL 更适合作为审计对象。
- **Catalyst 优化器**：把逻辑计划优化成物理计划，做谓词下推、列裁剪、join 重排。
- **Stage 与 Task**：作业被切成 Stage，Stage 内切成 Task 并行执行。这个模型是诊断模块的分析对象。

选它的理由是声明式、可本地可集群、优化器成熟。本项目用 `local[*]` 模式起步，后续可以平滑切到 standalone 集群。SQL 既是执行接口，也是审计对象——这一点决定了问数和 ETL 能共用同一套校验与归档。

### SQLite

元数据与状态的存储。单文件、零运维、支持事务，原型阶段不需要引入外部数据库。

它的限制要提前知道：同一时刻只有一个写者，不适合高并发写入。本项目的状态写入量不大，把写入收敛到单一路径即可。开启 WAL 后读写可以并行，读查询（比如审计查询）不会阻塞写入。

承载的内容：`job`、`step`、`attempt`、`artifact`、`sql_memory`，以及语义层定义。

### Docker

解决环境一致性。Spark 对 JDK 版本和 Python 版本都比较敏感，容器把这套组合固定下来，避免"我这儿能跑"的问题。

做法是在镜像里装好 JDK、Spark 和项目依赖，通过卷挂载持久化 SQLite 文件、产物目录和 EventLog。可选地，用 docker-compose 起一个 standalone 集群，用来演示 Spark 在非 local 模式下的 Stage / Task 行为，让诊断模块有真实数据可分析。

Windows 上注意卷挂载的 IO 性能，数据库和产物目录建议放在 WSL 文件系统或 Docker 卷里，而不是直接跨盘挂载。

### 隐性依赖

有几个技术不在这份清单里，但设计上绕不开：

- **SQL 解析器**：需要把 SQL 变成 AST 才能做 L1–L4 校验。跨方言方面，`sqlglot` 是比较省事的选择。
- **Pydantic**：Plan、Step、Attempt 这些结构需要用模型定义和校验，避免到处手写 dict。
- **OpenAI 兼容的模型客户端**：接入 DeepSeek 用官方 `openai` SDK 换 `base_url`，无需专用 SDK。
- **本地 embedding 模型**：混合检索中的向量一路，选型会影响镜像体积与启动时间。
- **本地向量索引**：`sqlite-vec` 与现有 SQLite 存储同处一文件，不引入新服务。

## 9. 已定设计决策

决策的立场是两条：把不确定性的作用范围压到最小；一切产物结构化、版本化、可回放。

### 9.1 Planner 用受约束规划

模型只做意图识别和槽位填充，DAG 拓扑由模板决定，不允许模型自由生成拓扑。

校验成本在这里是不对称的：参数填错容易校验（类型、范围、枚举），拓扑错了很难校验（依赖是否合理、步骤有没有漏）。凡是交给模型的部分，都必须能兜住。代价是要维护一套 skill 模板库，模板覆盖不到的需求靠人工扩。

### 9.2 SQL 以语义层编译为主，模型直出为逃生舱

分两路生成：

- 常规聚合与过滤：从语义层编译。输入是指标、维度、过滤条件，输出确定，不经模型。
- 开放问题：模型生成 SQL，过完整校验闸门。

让模型直出任意 SQL 是风险最高的路径，口径不一致、权限越界、性能不可预测都从这里来。dbt metrics、Cube、AtScale 走的都是前一形态。代价是语义层必须覆盖大多数问题，否则主路径形同虚设。

### 9.3 记忆召回用混合检索

BM25 + 向量 + RRF 融合。SQL 领域两种需求同时存在：精确标识符（`order_id`、错误码、表名）靠 BM25，自然语言改写靠向量，单用任一种都有明显盲区。这是 Elasticsearch、Weaviate、OpenSearch 的通用形态。

规模不是问题，SQL 记忆库几千到几十万条，本地 `sqlite-vec` 足够。

补一层反馈闭环：记录召回的 SQL 最终有没有被采用、执行成不成功，作为排序信号回灌。

### 9.4 Spark 按可切换设计

| 场景 | master |
|---|---|
| 日常开发 | `local[*]` |
| 诊断演练 | `local-cluster[2,1,1024]` |
| 演示 | compose standalone |
| 生产路径 | K8s 配置位 |

`local-cluster` 能在单个 JVM 里起多个 executor，EventLog 中就有多 executor 的 task 分布，数据倾斜和 task 重试都能演练出来，不必真起集群。诊断相关的开发用这个模式即可。

### 9.5 语义层手写为主、反射为辅

反射生成草稿（表、列、类型、主键、行数、样例值），人工补业务语义（口径、同义词、枚举、可见性、PII 标记），YAML 入库并版本化。

纯反射不可信：业务口径不在表结构里，legacy 系统里还有大量无语义列名。纯手写维护不动。

真实项目里语义层烂掉的主因是**漂移**——物理 schema 改了语义层没跟着改。所以要把漂移检测接进 CI，schema 变更时报警。指标口径需要 owner 字段，这是避免口径扯皮的最低成本。

### 9.6 核心做成库，CLI 与 HTTP 都是适配器

核心逻辑在库里，CLI 和 HTTP 只做薄适配，业务逻辑不进 CLI。

job 模型从一开始就是异步的：提交返回 `job_id`，状态查询独立。长任务不能同步阻塞 HTTP 请求，而 Spark 作业动辄几十秒到几分钟。CLI 天然符合这个模型，先交付它没有折损；HTTP API 等接前端或多用户时再加，核心不动。

### 9.7 不做界面，但产物必须结构化

原型阶段不做 UI，硬约束是所有产物输出稳定的 JSON schema，不输出给人看的文本日志。

真实平台里 UI 是薄展示层，消费结构化数据。现在图省事输出文本，将来做 UI 要返工后端；留下 schema，UI 就是一层不用碰后端的加法。

若项目需要作品集展示，任务列表加审计时间线的轻量界面成本不高、展示效果好，放在 M7 之后单独做。

### 9.8 图计算产物指血缘图与依赖图

原描述把它与 SQL、数据质量并列作为**归档产物**，血缘图和依赖图符合这个位置；GraphX 是作业类型，不是产物。

格式对齐 **OpenLineage** 的 run + facets 事件模型，不自己发明。这套格式已是行业事实标准，将来接 DataHub、Marquez 不用改数据结构，也省掉自己设计列级血缘和版本关联的力气。

若最终指向 GraphX，那是独立业务线——需要图数据模型、图算法和一套不同的诊断指标，应单独立项，不挂在诊断里程碑下。

### 9.9 合成数据作为一等测试夹具

项目没有真实可查的数据，用合成数据填充。这不是退而求其次：诊断模块的结论依赖**故意制造出来的病态现象**，而真实数据里的 spill、倾斜、task 重试是偶发的，无法按需复现，规则引擎就永远没有稳定的验证素材。

数据模型取电商星型：`dim_customer`、`dim_product`、`dim_date` 三张维表，`fact_order`、`fact_order_item` 两张事实表，规模可控。生成器要支持**参数化注入诊断场景**：

- 分区倾斜：少数客户占掉大部分订单，制造 task 时长 `max/median` 悬殊。
- join 放大：明细表行数为订单表的数倍。
- spill：把单分区数据量做大，或调小 `spark.sql.shuffle.partitions`。
- shuffle 放大：宽表 join 顺序不当。

生成脚本进版本库，Parquet 产物进 `data/synthetic/` 且不进版本库。备选方案是 TPC-H 的 8 表模型，适合做性能基准，但注入病态现象不如自建参数化数据灵活。

附带的好处是 schema 由自己定义，语义层是**设计**出来的而不是**逆向**出来的。

### 9.10 语义层按主题域分文件，版本用内容哈希

语义层拆成多个 YAML 文件，按主题域组织，例如 `semantic/orders.yml`、`semantic/customers.yml`。单文件在表数量增长后会迅速变得难以 review。

版本号用内容的哈希计算，不手写。内容变了哈希自然变，历史记录能精确对应到当时用的是哪一版，避免手写版本号漏更新的问题。

YAML 是唯一事实来源，加载后写入 SQLite 只作为运行时的查询缓存。

### 9.11 指标结构化表达

指标拆成独立字段，不用一整条 SQL 表达式。理由是结构化之后每一部分都能单独校验——聚合函数是否在白名单里、字段是否存在、过滤值类型是否匹配，这些都能在编译前查出来；一整条表达式只能靠解析。

```yaml
metrics:
  - name: gmv
    label: 成交总额
    entity: order
    agg: sum                  # sum / count / count_distinct / avg / max / min
    field: order_amount
    filters:
      - field: status
        op: eq
        value: paid
    owner: finance
    description: 已支付订单的成交金额合计
```

维度同样结构化，并显式声明层级：

```yaml
dimensions:
  - name: order_date
    label: 下单日期
    entity: order
    field: order_date
    type: date
    hierarchy: [year, quarter, month, day]
```

### 9.12 模型服务走 DeepSeek，经 ModelClient 抽象接入

模型调用走 DeepSeek 的 API，用 OpenAI 兼容接口接入，官方 `openai` SDK 换个 `base_url` 即可，不引入专用 SDK。

加一层 `ModelClient` 抽象，DeepSeek 只是其中一个实现。将来要切到别的兼容服务或本地模型时，上层不用改。

Planner 与逃生舱用 `deepseek-chat`，不用 reasoner：这两个环节需要的是 function calling / JSON mode 的可靠性，不是长链推理，而 reasoner 延迟显著更高。接入前要确认当前版本是否支持 function calling。

每次模型调用都要把模型名、参数和 token 用量记进 Attempt 记录。API 模型会静默更新，不记录版本就无法解释历史结果的差异。模型名要 pin 住，不用浮动别名。

发送给模型的只有语义层的受控元数据——表名、列名、指标口径，不含明细数据。这条既是安全边界，也与语义层的设计一致。

## 10. 里程碑

| 阶段 | 内容 |
|---|---|
| M0 | 仓库、依赖管理、Docker 环境、SparkSession 跑通 |
| M1 | Pydantic 模型 + SQLite schema + 状态机 |
| M2 | 问数闭环：语义层 + SQL 生成 + 校验 + 只读执行 + 审计 |
| M3 | SQL 记忆：成功入库 + 检索召回 |
| M4 | ETL：Spark SQL 作业 + 步骤依赖 |
| M5 | 诊断：EventLog 解析 + 规则引擎 + 报告 |
| M6 | 归档：manifest + 图产物 + CLI 全流程查询 |
