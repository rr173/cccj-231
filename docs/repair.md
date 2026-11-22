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

* **投影/出口**：重写血缘命中的行，以及上游本批已重算的代理行；
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
