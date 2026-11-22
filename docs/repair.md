# 本地发布表修复（lineage-diag repair）

诊断器只读加工图、运行记录和发布包；当结算数据已经发布、随后发现一批订单与
账户归属需要更正时，值班人员不需要重刷整批数据。`repair` 子命令在**单个
SQLite 工作区**里先物化初始发布，再以一批源行更正为单位，只重算受牵连的
连接键 / 分组，成功后在同一事务里一次性切换到新版本。

## 1. 可执行子集

修复入口复用 `loader.load_graph` 的图加载与全限定列校验，但只执行**唯一
发布出口的依赖闭包**，且闭包内只接受下列节点；遇到闭包内不支持或有歧义的
定义一律拒绝（`RepairValidationError`），闭包外的节点不参与也不报错：

| 节点 | 约束 |
|---|---|
| `input` | 必须用结构化字段 `row_id` 声明表内唯一、不会改名的行 ID；行 ID 不参与任何业务表达式；每列带 `type`，只支持非空 `TEXT`/`INTEGER` |
| `project` / `output` | 单一上游；输出表达式只能是直接取列或重命名的 `kind="ref"`；`output` 不允许 `extra_inputs` |
| `join` | 两个上游、恰好一个单列等值 `=` 内连接（`join_type` 缺省或显式 `inner`）；两侧键类型一致；**连接键必须同时投影为输出列**，以便定位旧键分区；保留多对多配对 |
| `agg` | 结构化 `group_by`（至少一列）与 `measures`；度量仅支持整数 `SUM` 和 `COUNT(*)`；分组键经普通取列表达式带到输出；最后一条贡献离开时分组消失 |

聚合语义只来自 `group_by` / `measures` 结构化参数，**从不解析展示用的
`raw` 文本**。节点允许串接或共用上游，包括聚合结果再参与连接、聚合再喂
聚合（见 `tests/test_repair_shapes.py`）。

## 2. 工作区数据库布局

一个工作区就是一个 SQLite 文件（`PRAGMA foreign_keys=ON`）：

* `meta`：`version`（当前已提交版本，初始为 1）、`graph_version`、
  `graph_digest`（整图规范化摘要，换图即拒绝）、`schema_version`、
  `output_node`；
* `src_<input>`：源表，行 ID 列为主键，业务列 `NOT NULL`，业务键带索引；
* `rel_<node>`：每个物化中间/出口节点的多重集，一行一个副本；连接节点带
  隐藏 `_jkey`、聚合节点带隐藏 `_gkey`（均为规范 JSON 编码），连同每个
  业务列都有索引；
* `prov_<node>`：`(_sid, src_node, src_row_id, copies)` 多重集血缘，
  `copies` 记录份数，保证多对多扇出和投影后相同值的行各自保留；
* `trace_<node>`（可选，仅建库时 `--trace` 启用）：`(_sid, role,
  up_node, up_sid, up_row_id)` 实例级计算证据，见
  [第 10 节](#10-单行溯源repair-trace)；
* 视图 `release_rows`：`meta` 与出口表的连接，自带 `version` 列。

## 3. 读取发布行与版本号（同一事务/同一快照）

只读消费者不需要写连接。两种等价方式：

```python
from lineage_diag import read_release
version, rows = read_release("workspace.db", graph_payload)
```

或直接 SQL，在一条语句里同时拿到版本号和发布行：

```sql
SELECT version, region, total_amt, n, bucket
FROM release_rows;          -- 每行的 version 即当前提交版本
```

需要严格快照时，在自己的事务里先 `SELECT value FROM meta WHERE
key='version'` 再读出口表即可；视图已经把两者放在同一查询计划内。

## 4. 更正批次约定

```json
{
  "expected_version": 2,
  "corrections": [
    {"node_id": "orders", "row_id": "o1", "op": "replace",
     "values": {"acc_id": "B", "region": "R5", "amount": 10}},
    {"node_id": "orders", "row_id": "o2", "op": "delete"},
    {"node_id": "accounts", "row_id": "a900", "op": "insert",
     "values": {"acc_id": "B", "owner": "ben2"}}
  ]
}
```

* `expected_version` 必须等于工作区当前版本，否则报
  `RepairConflictError`（旧版本写入被拒绝）；
* 同一 `(node_id, row_id)` 在一批内最多出现一次；
* `insert` 已存在、`delete`/`replace` 不存在、换了加工图（图版本或摘要
  不符）都拒绝，且不留下任何部分改动；
* 值只能是非空字符串或整数，列集合必须与图声明完全一致；`delete` 不
  能带 `values`；
* 批内更正一起生效，与提交顺序无关（测试用逆序批次对照过）。

## 5. 为什么只重算这些范围

仅靠值列血缘会漏掉只出现在连接条件或分组键里的变化。引擎以被更正的源行
ID 为起点，按拓扑序逐节点定位：

* **投影/出口**：删除血缘与污点来源集合相交的行，并按**同一来源集合**
  从当前上游行重建（两侧判定一致，否则只被血缘交集波及的下游行会凭空
  消失）；
* **连接**：收集旧连接行的键与两侧更正后行的键的并集，按键删除旧分区，
  再用**更正后的两侧数据**重建该键的全部配对——键从 A 搬到 B 时既撤掉
  A 的旧配对，又建立 B 的新配对，绝不会新旧两边混算；
* **聚合**：收集旧分组键与新分组键的并集，重读受影响分组的完整成员后
  重新 `SUM`/`COUNT(*)`；成员为空则分组保持删除；
* 变化继续沿下游传播直到发布表。

所有重读都走索引并带精确键/行 ID 谓词；增量批次不允许全表扫描。
`AccessLedger` 记录每次访问的表与谓词，回归测试据此断言大批互不相交的键
从未被访问，而不是用耗时快慢判断。

## 6. 原子性与失败恢复

源行更正、必要中间状态、发布行和版本号在**一个 `BEGIN IMMEDIATE` 事务**
里提交。任何一步失败都 `ROLLBACK`：重新打开数据库只能读到批前版本与批前
数据。测试在连接已开始写入后、提交前注入异常，验证重开后源数据、发布结果
、版本均不变，再用同一 `expected_version` 重试成功，并确认没有残留的
孤儿血缘行。

## 7. CLI 与 Python 用法

```bash
bin/lineage-diag repair init  --graph graph.json --db ws.db --init initial.json
bin/lineage-diag repair apply --graph graph.json --db ws.db --batch batch1.json
bin/lineage-diag repair read  --graph graph.json --db ws.db
```

```python
from lineage_diag import (
    initialize_workspace, apply_repair_batch, read_release,
    RepairWorkspace, AccessLedger,
)

plan, version = initialize_workspace("ws.db", graph, initial_payload)
summary = apply_repair_batch("ws.db", graph, batch_payload)

# 需要访问台账或注入失败点时用长连接句柄
with RepairWorkspace("ws.db", plan, AccessLedger()) as ws:
    ws.apply(batch_payload, failpoint=lambda nid: maybe_raise(nid))
    version, rows = ws.read_release()
```

`initial_payload` 形如 `{"sources": {input_node: [rows]}}`，每行包含行
ID 与全部业务列。

## 8. 验证方法（可复跑）

```bash
python3 -I tests/build_repair_fixture.py   # 生成 data/repair 夹具
python3 -I tests/test_repair.py            # 朴素全算对照/范围/原子性/拒绝
python3 -I tests/test_repair_shapes.py     # 整数键、多列分组、聚合串接
python3 -I tests/verify_independent.py     # 原诊断用法回归
python3 -I tests/run_reports.py            # 原报告/分页用法回归
```

`tests/naive_reference.py` **不导入**增量引擎，而是按规格用朴素字典实现
从头全算；每个成功版本都把增量发布多重集（含重复份数）与朴素结果逐项对照。

## 9. 净变更订阅（repair subscribe）

下游对账库每晚消费发布表。它已持有上次同步的整表，落后若干版本时不需要
重新拉全量，只需要知道两个版本之间哪些**完整发布业务行**该补入或移除、
各几份。订阅为工作区提供可分页、可续读的净变更入口；消费者不接触源表、
更正批次，也看不到工作区内部的代理行号（`_sid`）。

### 9.1 启用与保留窗口

```bash
bin/lineage-diag repair subscribe enable --graph graph.json --db ws.db --retention 3
```

* 在**当前发布版本**启用，`--retention N`（整数，≥1）表示保留最近 N 次
  成功发布的变更；已有工作区原地启用，无需重建，也**不会**臆造启用前
  的变更——启用时的版本就是最早可用起点；
* 重复启用被拒绝（`already_enabled`）；未启用的工作区一切照旧，数据库里
  不出现任何订阅表；
* 启用后最早可用起点为 `max(启用版本, 当前版本 - N)`；每次成功发布在同一
  事务里写入该版净变更、推进版本号、并物理删除滑出窗口的旧变更数据
  （`DELETE`，不做 `VACUUM`，不回收文件空间）；
* 失败的修复（拒绝或回滚）不推进版本、不写变更、不清理旧数据——旧起点
  不会提前过期。

启用后工作区多一张 `sub_delta(version, row_key, delta)` 表：每行是某个
发布版本里某个业务行的净份数变化（非零），`row_key` 是整行业务列的规范
JSON（键排序，`"5"` 与 `5` 类型可区分）。发布时引擎直接从本批变更集
（出口节点被删/被插的行）计算净变更，**不扫整张发布表**；删除后原样重
发的行净额为 0，不落表。

### 9.2 全量基线与净变更如何衔接

消费者首次接入或起点过期后，先取全量基线：

```bash
bin/lineage-diag repair read --graph graph.json --db ws.db   # (version V0, 整表行)
```

把整表行按业务行保存为本地副本（含每行份数），并记录已同步版本 `V0`。
之后每晚用 `V0` 作为起点拉净变更，逐条按 `delta` 调整本地副本中该行的
份数（正数补入、负数移除），翻页到 `has_more=false` 时把已同步版本推进
到该页的 `to_version`。只要基线精确，份数不会出现负值；净变更区间
`(from, to]` 内先出现又撤掉的行净额为 0、不会交付，下游无需做无谓的
删除。

### 9.3 分页读取协议

```bash
# 首页：给出自己完整同步到的版本；目标固定为当时的最新已提交版本
bin/lineage-diag repair subscribe read --graph graph.json --db ws.db \
    --from-version 4 --page-size 100
# 后续页：凭上一页返回的 cursor 续读（可跨进程重启）
bin/lineage-diag repair subscribe read --graph graph.json --db ws.db \
    --cursor 'eyJ...'
```

每页返回：

```json
{
  "subscription": "…", "from_version": 4, "to_version": 6, "page_size": 2,
  "changes": [{"row": {"region": "R2", "total_amt": 60, "n": 2,
                        "bucket": "hot"}, "delta": -1}],
  "has_more": true, "cursor": "…"
}
```

* `from_version`/`to_version` 在每页固定：工作区的新提交**不会**让进行
  中的读取改用新目标；新目标只影响重新发起的读取；
* 变更按 `row_key`（规范行 JSON 的二进制序）这一稳定全序交付，页与页
  之间互不重叠，每条变更恰好交付一次；用同一有效游标重取同一页得到
  同一内容（窗口内的变更数据不可变）；
* 空区间也返回 `to_version`（`changes: []`、`has_more: false`），消费
  者据此知道可以推进到哪里；
* 游标是不透明字符串，绑定订阅身份、起点、目标与位置；消费者只需持久
  化它即可在进程重启后续读。

### 9.4 两类失败的处理

* **发布失败**（批次被拒绝或中途回滚）：版本号、已记录变更、保留边界
  全部不变，消费者无需任何处理，稍后重试读取即可；
* **读取失败**（`SubscriptionError`，带 `kind` 便于脚本分支）：
  * `not_enabled`：工作区未启用订阅；
  * `invalid_start`：起点早于启用版本、晚于已提交版本或格式非法——
    修正参数，或先取全量基线；
  * `cursor_invalid`：游标无法解码、属于其他工作区/订阅、或与请求
    的起点矛盾——用正确的起点重新发起；
  * `expired`：起点已滑出保留窗口（每页都检查，且在页查询后按最新
    提交版本复查，并发发布也无法让残缺页蒙混过关）。此时**必须**
    重新取全量基线（`repair read` 当前版本）并从该版本重新订阅；
    不会交付残缺结果，也不会把它当成读完。

### 9.5 访问范围保证

* 日常写入仍按原修复范围执行：发布时只把本批变更集涉及的出口行换算成
  净变更（删除前按行号精确重读被删行），增量批次不允许全表扫描；
* 读订阅只访问 `sub_delta` 中落在请求区间 `(from, to]` 的行及其必要
  历史，不读发布表/源表/血缘表，不重跑加工图；从未变化的发布行在订阅
  表里没有任何条目，永远不会被碰到；
* 分页用 `(row_key, version)` 索引按键位续读（keyset），服务端每页最多
  取 `page_size + 1` 条候选（多取的一条仅用于判断有无后页），不会先把
  整个区间结果装成内存列表再切页。

Python 用法：

```python
from lineage_diag import enable_subscription, read_subscription

enable_subscription("ws.db", graph, retention=3)
page = read_subscription("ws.db", graph, from_version=4, page_size=100)
while page["has_more"]:
    page = read_subscription("ws.db", graph, cursor=page["cursor"])
```

可复跑的端到端示例（落后数版的消费者、份数减少未归零、跨版本抵消、
业务行替换、取首页后又有新提交的两种情形、访问范围与失败注入）见
`tests/test_repair_subscription.py`。

## 10. 单行溯源（repair trace）

结算人员拿到一行发布数据，要核实它是由哪些原始单据、经过哪些实际配对
和汇总算出来的。列级诊断只能找到字段，订阅也只能说明行值份数变了多少；
`repair trace` 回答的是**这一行每一份实际产出的计算证据**。

### 10.1 启用：只在建库时，不迁移旧库

```bash
bin/lineage-diag repair init --graph graph.json --db ws.db \
    --init initial.json --trace
```

* `--trace`（Python：`initialize_workspace(..., enable_trace=True)`）让
  工作区在建库时为闭包内每个物化节点额外建一张 `trace_<node>` 证据表，
  并在 `meta` 里落下 `trace=1`；
* 溯源**只能**在建库时显式开启。已存在的工作区（包括本功能出现之前建
  的）不会被迁移：对它们执行 `repair trace` 一律以
  `kind="not_enabled"` 拒绝；
* 建库后日常修复**不重扫全图、不重建全部证据**：引擎在逐行发出/删除物
  化行时，于**同一事务**内逐行写入/删除对应证据，范围与该批已触及的
  行完全一致（台账可证）；回滚的批次连证据一起回滚，绝不留下孤儿证据。

证据表每行记录：某物化行消费了**哪一个直接上游实例**——

| 节点 | 证据条目 |
|---|---|
| `project` / `output` | 一条 `in`，指向它实际使用的那一行 |
| `join` | `left`、`right` 各一条，即使两侧最终追到同一张源表也可区分 |
| `agg` | 每个实际吃进去的成员一条 `member`，可据此核对分组键、`SUM`、`COUNT(*)` |

`prov_<node>` 回答不了这些问题：它把连接两侧和所有聚合成员合并成一个
源行多重集，既分不出左右，也还原不出成员级结构。

### 10.2 查询

```bash
bin/lineage-diag repair trace --graph graph.json --db ws.db \
    --row '{"region":"R2","total_amt":60,"n":2,"bucket":"hot"}' \
    --expected-version 3
# 或 --row-file row.json
```

```python
from lineage_diag import trace_release_row
result = trace_release_row("ws.db", graph, row, expected_version=3)
```

* `row` 必须是**完整发布业务行**：全部发布列、按声明类型给值（TEXT 的
  `"5"` 与 INTEGER 的 `5` 是不同的行）。少列（`missing_column`）、多列
  （`extra_column`）、值类型不符（`type_mismatch`，含 null 和布尔）、
  非对象（`bad_row`）都明确拒绝；
* `expected_version` 必须等于当前已提交版本，否则 `stale_version`；格
  式非法（非正整数）为 `invalid_version`。**不做历史版本查询**；
* **合法但不存在的行不是错误**：返回 `matched: 0`、空的 `roots` 与
  `instances`，并带回当前 `version`；
* 一次查询的版本、发布实例、成员关系与源行值全部在**同一个读事务**里
  取得，即同一份已提交状态；查询只定位匹配的发布实例并沿证据访问实际
  用到的实例，不扫描无关分区、不重跑加工图。

### 10.3 结果格式（可脚本读取）

```json
{
  "version": 3,
  "output_node": "o_release",
  "row": {"region": "R2", "total_amt": 60, "n": 2, "bucket": "hot"},
  "matched": 2,
  "roots": ["e1", "e10"],
  "instances": [
    {"id": "e1", "node": "o_release", "kind": "output",
     "values": {"region": "R2", "total_amt": 60, "n": 2, "bucket": "hot"},
     "uses": [{"role": "in", "ref": "e2"}]},
    {"id": "e2", "node": "j_target", "kind": "join",
     "values": {"region": "R2", "target_region": "R2", "total_amt": 60,
                "n": 2, "bucket": "hot"},
     "uses": [{"role": "left", "ref": "e3"},
              {"role": "right", "ref": "e4"}]},
    {"id": "e3", "node": "a_region", "kind": "agg",
     "values": {"region": "R2", "total_amt": 60, "n": 2},
     "uses": [{"role": "member", "ref": "e5"},
              {"role": "member", "ref": "e6"}]},
    {"id": "e4", "node": "targets", "kind": "source", "row_id": "t2",
     "values": {"region": "R2", "bucket": "hot"}}
  ]
}
```

* `matched` = 该行在当前版本的实际份数；`roots` 每份一个根证据标识，
  **投影后所有值都相同的两份配对也各自独立交付，绝不合并**；
* `instances` 是证据 DAG：`kind` 为 `output` / `project` / `join` /
  `agg` / `source`；关系实例带当版行值 `values` 与 `uses` 边（`role`
  ∈ `in` / `left` / `right` / `member`），源实例带源表节点 `node`、
  源行 ID `row_id` 与当版行值——可从源表、源行、当版行值一路核对到
  发布结果；
* **同一个中间行同时供给多处时只交付一次**：它作为单个实例出现，其
  在各处的使用关系和角色通过各消费方的 `uses` 边保留（DAG 而非按路
  径展开的树）；
* 核对聚合：`member` 边数 = `COUNT(*)`，逐成员取值求和 = `SUM`，分组
  键就在 agg 实例的 `values` 里；
* 证据标识（`e1`、`e2`…）**只在本次结果内可解引用**（`uses[].ref`），
  不对外暴露内部行号，也不保证跨版本、跨查询稳定。

### 10.4 与订阅的分工

订阅回答"两个版本之间哪些业务行的份数变了"；溯源回答"这一行这一份是
谁算出来的"。两者互补的典型情形：**换了一张原始单据但分组总额和笔数
不变**时，订阅的净变更为空、发布行逐字节相同，而下一版的溯源证据必然
指向新单据（证据随物化行一起按版重建）。`tests/test_repair_trace.py`
的 DEMO 2 演示了这一幕。

### 10.5 验证

```bash
python3 -I tests/test_repair_trace.py
```

覆盖：两条投影分支共用源表时同键行的实际左右配对（DEMO 1）；换单据不
换合计时发布前后的证据归属（DEMO 2）；多层汇合下实例只交付一次、不展
开成重复路径（DEMO 3，13 个实例 vs 24 次路径展开访问）；证据访问限于
命中实例与必要成员（台账断言，含 80 个互不相干分区零触碰）；一次中途
失败的更正不留任何证据；全部拒绝形态与"合法但不存在"的无匹配返回。
