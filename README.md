# 发布包列级血缘诊断器（lineage_diag）

读取**声明式加工图**、**实际发布包 schema** 与**运行期正负列读取记录**，
输出列级正向血缘、反向影响和无法证明的缺口清单。

## 1. 设计原则：静态图给“可能”，运行记录给“实际”

* 加工图只能说明**可能依赖**：图上有边，不代表本次发布真的读过。
* 运行记录才说明**本次实际读过什么**：
  * `read_positive`：本节点执行且实际读取了该列；
  * `read_negative`：本节点执行但明确未读取该目标列；
  * `read_negative_refs`：边级负读（如 `coalesce` 的某个分支 ref 未读，
    但目标列本身被正读）；
  * 正、负名单都没有 = **无记录**。无记录永远不等于无依赖，
    只能归入 `UNPROVABLE`。

### 四类结果（互斥的语义，绝不混用）

| 类别常量 | 含义 |
|---|---|
| `NOT_EXECUTED` | 图上声明有依赖，但路径只存在于未执行的分支臂之后，或节点明确负读 |
| `NO_CONTRIBUTION` | 本次实际正读了某列，但没有任何静态路径把它带到发布输出 |
| `PARTIAL_PATH` | 输出列至少一条路径有证据，但不是全部静态路径都有证据 |
| `UNPROVABLE` | 证据缺失（节点无记录、列无正负记录、分支无 taken/skipped 条目） |

输出级状态另有 `PROVEN`（全部静态路径均有正读证据）。

## 2. 来源身份如何保留

所有列在加载时就被解析为全限定身份 `节点.列名`：

* **重命名**：普通有向边，`t1.uid -> p_filt.uid -> j_user.uid_copy`，
  新旧身份同时保留，绝不按名字匹配；
* **同名连接列**：`t2.uid -> j_user.uid` 与 `p_filt.uid -> j_user.uid_copy`
  是两条独立边，同名不合并；
* **聚合输入**：`j_user.amount -> a_sum.total_amt` 保留输入身份；
* **展开**：`t3.tags -> e_tags.tag_name`，passthrough 列独立建边；
* **遮蔽列**：`p_agg.country` 显式来源于 `a_sum.country`（`upper(...)`），
  与 `j_user.country` 不靠同名关联；
* **未执行分支**：分支节点对列血缘透明，只贡献门控 `(branch, arm)`，
  门控随路径累积；
* **连接条件 / 分支条件**：标记为 `join_on` / `condition` 角色，
  不进入值血缘，也不会产生无贡献误报。

## 3. 一致性闸门（任一不符即拒绝诊断）

`assert_consistent` 校验：

1. 图版本 = 运行记录图版本 = 发布包图版本；
2. 运行版本 = 发布包运行版本；
3. 运行记录的 `package_digest` = 发布包 schema 重算摘要；
4. 发布包每个输出列都有运行期样本摘要且逐列 digest 一致；
5. 输出节点存在、包内每列都由该节点产出、输出节点有运行记录且每列正读。

任一条失败抛 `ConsistencyError`，不产出任何诊断结论。

## 4. 接口

```python
from lineage_diag import (
    load_artifacts, diagnose,
    forward_query, backward_query,
    paged_report, export_report,
)

graph, runtime, package = load_artifacts(
    "data/graph.json", "data/runtime.json", "data/package.json")
result = diagnose(graph, runtime, package)

forward_query(result, "t1.uid")                 # 反向影响：输入 -> 输出
backward_query(result, "o_rel.country")         # 正向血缘：输出 -> 全部输入
paged_report(result, "gaps", page=2, page_size=20)
export_report(result, "tmp/report.jsonl", page_size=100)
```

CLI（隔离模式，防止数据目录中被植入的 `json.py` 等被加载）：

```bash
bin/lineage-diag diagnose  --graph ... --runtime ... --package ...
bin/lineage-diag forward   --input t1.uid ...
bin/lineage-diag backward  --output o_rel.country ...
bin/lineage-diag report    --section gaps --page 1 --page-size 20
bin/lineage-diag export    --out tmp/report.jsonl ...
```

## 5. 分页与内存约束

* 所有路径用惰性生成器枚举，从不一次性展开全图路径；
* 报告去重在临时 **SQLite** 表中完成（`fingerprint` 主键 upsert），
  相同路径只保留一条，`occurrence_count` 记重复次数，
  并保留一份代表性证据链；
* JSONL 导出逐行写盘，每页大小受 `page_size` 约束。

## 6. 夹具场景与手工核对表

夹具由 `tests/build_fixture.py` 生成，包含：两个条件分支
（`b_gate` 的 `b1` 未执行、`b_route` 的 `take_exec` 执行而 `skip_emit`
未执行）、同名连接列、聚合、展开、重命名、遮蔽列、完全无记录节点
（`p_note`）、边级负读（`t2.country_code`）和无贡献正读
（`p_dbg.dbg_audit`）。

### 6.1 手工列出的静态可能依赖（独立程序核对一致）

| 输出列 | 静态输入路径（全部，含未执行分支） |
|---|---|
| `o_rel.uid_copy` | `t1.uid`（经 `p_filt.uid`、`j_user.uid_copy`） |
| `o_rel.total_amt` | `t1.amount`（经 join、`a_sum` 聚合） |
| `o_rel.region` | `t1.region` |
| `o_rel.country` | `t2.country` 与 `t2.country_code`（coalesce 两条） |
| `o_rel.owner_id` | `t2.owner_id` 与 `t1.uid`（coalesce 两条） |
| `o_rel.tag_name` | `t3.tags`（经 explode） |
| `o_rel.note` | `t2.country`、`t2.country_code`（经 `p_note`） |
| `o_rel.overseas_flag` | `t1.overseas_flag`（经 `b_gate/b1` 门控） |

所有发布路径都带 `b_route/take_exec` 门控；海外标记路径额外带
`b_gate/b1` 门控。

### 6.2 运行实际证据与期望分类

| 输出列 | 证据情况 | 期望分类 |
|---|---|---|
| `uid_copy` | 唯一路径上 `p_filt.uid` 无任何记录 | `UNPROVABLE`（无记录≠无依赖） |
| `total_amt` / `region` / `tag_name` | 每条边正读 | `PROVEN` |
| `country` | `t2.country` 路径有证据；`t2.country_code` 边级负读 | `PARTIAL_PATH` |
| `owner_id` | `t2.owner_id` 有证据；`t1.uid` 路径在 `p_filt.uid` 缺记录 | `PARTIAL_PATH` |
| `note` | `p_note` 节点整体无运行记录 | `UNPROVABLE` |
| `overseas_flag` | 唯一路径被跳过的 `b_gate/b1` 阻断 | `NOT_EXECUTED` |
| `p_dbg.dbg_audit` | 正读但无任何输出路径 | `NO_CONTRIBUTION`（gap 清单） |

### 6.3 独立核对与拒绝测试

`tests/verify_independent.py` **不导入** `traversal.py` / `diagnoser.py`，
而是按规格用另一套字典驱动实现重新枚举静态路径、重新合并运行记录，
再与工具结果逐项比对，并执行 7 个篡改实验确认诊断被拒绝：

* 运行版本不一致、图版本不一致、包摘要不一致；
* 篡改 schema 类型（schema digest 不符）；
* 篡改单列样本摘要；
* 删除输出节点运行记录、删除单列样本摘要。

## 7. 运行方式

```bash
python3 -I tests/build_fixture.py      # 生成 data/ 下四份夹具
python3 -I tests/verify_independent.py # 独立遍历 + 分类 + 拒绝测试
python3 -I tests/run_reports.py        # 分页 / 流式导出 / 去重测试
```

全部仅使用 Python 标准库（3.10+），无第三方依赖。
