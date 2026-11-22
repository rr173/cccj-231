"""Validation of the *executable subset* accepted by the local repair entry.

The diagnostic tool accepts arbitrary declarative graphs.  The repair writer
is deliberately much narrower: only the sub-graph feeding the single release
output is executed, and every node inside that sub-graph must be one of:

* ``input``   - source table with an explicit, immutable row id column;
* ``project`` - one upstream, value expressions that are plain column
  references (direct column carry or renames);
* ``join``    - two upstreams, one single-column equi ``=`` inner join,
  output expressions are plain column references from either side;
* ``agg``     - at least one group-by column, integer ``SUM`` / ``COUNT(*)``
  measures, group columns carried through plain references;
* ``output``  - exactly one logical upstream, plain reference projections.

Aggregation semantics come from structured fields (``group_by`` /
``measures``) parsed here -- the display-only ``raw`` text of an expression
is never interpreted.  Anything unsupported or ambiguous inside the
dependency closure raises :class:`RepairValidationError`; nodes outside the
closure are irrelevant and are ignored rather than rejected.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Tuple

from .digest import digest_payload
from .loader import load_graph
from .model import ColRef, Graph, Node, RepairValidationError

_ID_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SUPPORTED = {"input", "project", "join", "agg", "output"}
_INTEGER = "INTEGER"
_TEXT = "TEXT"


@dataclass(frozen=True)
class JoinSpec:
    left: str
    right: str
    left_key: str                 # qualified left key column
    right_key: str                # qualified right key column


@dataclass(frozen=True)
class Measure:
    func: str                     # "sum" | "count"
    out_column: str
    ref: Optional[str]            # qualified input column for sum; None count
    out_type: str


@dataclass(frozen=True)
class AggSpec:
    input_node: str
    group_by: Tuple[str, ...]     # qualified group-by columns
    group_outputs: Tuple[str, ...]  # output columns carrying the keys
    measures: Tuple[Measure, ...]


@dataclass(frozen=True)
class Projection:
    """One output column produced as a plain reference."""

    out_column: str
    source: ColRef
    out_type: str


@dataclass
class RepairPlan:
    """A validated executable sub-graph for repair materialization."""

    graph: Graph
    graph_version: str
    graph_digest: str
    output_node_id: str
    closure: Tuple[str, ...]               # topologically ordered node ids
    inputs: Tuple[str, ...]
    # node id -> row id column name (input nodes only)
    row_ids: Dict[str, str]
    # node id -> business output column list in declared order
    columns: Dict[str, List[str]]
    # node id -> {column: storage type}
    types: Dict[str, Dict[str, str]]
    joins: Dict[str, JoinSpec]
    aggs: Dict[str, AggSpec]
    projections: Dict[str, List[Projection]]
    # node id -> ordered upstream node ids actually read
    upstreams: Dict[str, Tuple[str, ...]]

    def node(self, node_id: str) -> Node:
        return self.graph.node(node_id)


def build_repair_plan(payload: Mapping[str, Any]) -> RepairPlan:
    """Load via the shared graph loader, then validate the repair subset."""
    graph = load_graph(payload)
    graph_digest = digest_payload(payload)
    output_ids = [nid for nid, n in graph.nodes.items() if n.type == "output"]
    if len(output_ids) != 1:
        raise RepairValidationError(
            f"repair requires exactly one output node, found "
            f"{len(output_ids)}: {sorted(output_ids)}"
        )
    output_id = output_ids[0]
    closure = _topo_closure(graph, output_id)

    row_ids: Dict[str, str] = {}
    columns: Dict[str, List[str]] = {}
    types: Dict[str, Dict[str, str]] = {}
    joins: Dict[str, JoinSpec] = {}
    aggs: Dict[str, AggSpec] = {}
    projections: Dict[str, List[Projection]] = {}
    upstreams: Dict[str, Tuple[str, ...]] = {}

    for nid in closure:
        node = graph.nodes[nid]
        if node.type not in _SUPPORTED:
            raise RepairValidationError(
                f"node {nid!r} feeds the release output but its type "
                f"{node.type!r} is not supported by the repair writer "
                f"(supported: {sorted(_SUPPORTED)})"
            )
        if not _ID_RE.match(nid):
            raise RepairValidationError(
                f"node id {nid!r} must match {_ID_RE.pattern}"
            )
        if node.type == "input":
            _validate_input(node, row_ids, columns, types)
        elif node.type == "project":
            _validate_project(graph, node, columns, types, projections,
                              upstreams)
        elif node.type == "join":
            _validate_join(graph, node, closure, columns, types, joins,
                           projections, upstreams)
        elif node.type == "agg":
            _validate_agg(graph, node, closure, columns, types, aggs,
                          projections, upstreams)
        else:  # output
            _validate_output(graph, node, columns, types, projections,
                             upstreams)

    # Row ids are identity, never business data: no expression on any node in
    # the executable closure may reference an input's row id column.
    row_id_fqs = {f"{nid}.{rid}" for nid, rid in row_ids.items()}
    for nid in closure:
        for expr in graph.nodes[nid].exprs:
            bad = row_id_fqs.intersection(expr.refs)
            if bad:
                raise RepairValidationError(
                    f"node {nid!r}: input row id column(s) {sorted(bad)} "
                    "must not participate in business expressions"
                )

    return RepairPlan(
        graph=graph,
        graph_version=graph.graph_version,
        graph_digest=graph_digest,
        output_node_id=output_id,
        closure=tuple(closure),
        inputs=tuple(nid for nid in closure
                     if graph.nodes[nid].type == "input"),
        row_ids=row_ids,
        columns=columns,
        types=types,
        joins=joins,
        aggs=aggs,
        projections=projections,
        upstreams=upstreams,
    )


# ---------------------------------------------------------------------------
# Closure / topology
# ---------------------------------------------------------------------------
def _topo_closure(graph: Graph, output_id: str) -> List[str]:
    # reverse adjacency restricted to logical value inputs
    parents: Dict[str, List[str]] = {nid: [] for nid in graph.nodes}
    for nid, node in graph.nodes.items():
        if node.type == "project":
            parents[nid].append(node.raw["input"])
        elif node.type == "join":
            parents[nid].extend([node.left, node.right])
        elif node.type == "agg":
            parents[nid].append(node.agg_input)
        elif node.type == "output":
            parents[nid].append(node.output_of)
        # input / branch: no repair-visible parents (branches are rejected
        # inside the closure below)

    closure: List[str] = []
    seen = set()

    def visit(nid: str) -> None:
        if nid in seen:
            return
        seen.add(nid)
        for parent in parents[nid]:
            visit(parent)
        closure.append(nid)

    visit(output_id)
    # Reject branch nodes explicitly: repair cannot reason about conditional
    # arm execution, and branch nodes are not in the supported set anyway.
    for nid in closure:
        if graph.nodes[nid].type == "branch":
            raise RepairValidationError(
                f"branch node {nid!r} lies on a release path; conditional "
                "branching is not supported by the repair writer"
            )
    return closure


# ---------------------------------------------------------------------------
# Per-node validators
# ---------------------------------------------------------------------------
def _validate_input(node: Node, row_ids, columns, types) -> None:
    raw = node.raw
    row_id = raw.get("row_id")
    if not isinstance(row_id, str) or not row_id:
        raise RepairValidationError(
            f"input node {node.id!r} requires a structured 'row_id' naming "
            "its immutable in-table row identifier"
        )
    declared = [c["name"] for c in raw["columns"]]
    if row_id not in declared:
        raise RepairValidationError(
            f"input node {node.id!r} row_id {row_id!r} is not among its "
            f"declared columns {declared}"
        )
    business = [c for c in declared if c != row_id]
    if not business:
        raise RepairValidationError(
            f"input node {node.id!r} has no business columns besides row_id"
        )
    if len(set(declared)) != len(declared):
        raise RepairValidationError(
            f"input node {node.id!r} declares duplicate columns"
        )
    col_types: Dict[str, str] = {}
    for col in declared:
        ctype = _normalize_type(node.id, col, _raw_type(raw, col))
        col_types[col] = ctype
    row_ids[node.id] = row_id
    columns[node.id] = business
    types[node.id] = col_types


def _raw_type(raw_node: Mapping[str, Any], column: str) -> Any:
    for c in raw_node["columns"]:
        if c["name"] == column:
            if "type" not in c:
                raise RepairValidationError(
                    f"input node {raw_node.get('id')!r} column {column!r} "
                    "requires a structured 'type'"
                )
            return c["type"]
    raise RepairValidationError(
        f"input node {raw_node.get('id')!r}: column {column!r} missing"
    )


def _normalize_type(node_id: str, column: str, raw_type: Any) -> str:
    if not isinstance(raw_type, str):
        raise RepairValidationError(
            f"node {node_id!r} column {column!r}: type must be a string"
        )
    upper = raw_type.strip().upper()
    if upper in ("TEXT", "VARCHAR", "STRING"):
        return _TEXT
    if upper in ("INTEGER", "INT", "BIGINT", "SMALLINT", "TINYINT"):
        return _INTEGER
    raise RepairValidationError(
        f"node {node_id!r} column {column!r}: repair only supports non-null "
        f"TEXT/INTEGER values, got type {raw_type!r}"
    )


def _check_column_names(node_id: str, cols: List[str]) -> None:
    for col in cols:
        if not _ID_RE.match(col):
            raise RepairValidationError(
                f"node {node_id!r}: column {col!r} must match "
                f"{_ID_RE.pattern}"
            )
    if len(set(cols)) != len(cols):
        raise RepairValidationError(
            f"node {node_id!r}: duplicate output columns {cols}"
        )


def _ref_projection(graph: Graph, node: Node, expr, allowed_nodes: List[str],
                    out_type_map: Dict[str, Dict[str, str]]) -> Projection:
    if expr.kind != "ref" or len(expr.refs) != 1:
        raise RepairValidationError(
            f"node {node.id!r} output {expr.out_column!r}: repair only "
            "supports direct column references / renames, got "
            f"kind={expr.kind!r} op={expr.op!r}"
        )
    ref = ColRef.parse(expr.refs[0])
    if ref.node_id not in allowed_nodes:
        raise RepairValidationError(
            f"node {node.id!r} output {expr.out_column!r} references "
            f"{ref.fq} which is not one of its upstream nodes "
            f"{allowed_nodes}"
        )
    producer = graph.nodes[ref.node_id]
    if ref.column not in producer.output_columns:
        raise RepairValidationError(
            f"node {node.id!r} references {ref.fq}, a column not produced "
            f"by node {ref.node_id!r}"
        )
    return Projection(
        out_column=expr.out_column,
        source=ref,
        out_type=out_type_map[ref.node_id][ref.column],
    )


def _validate_project(graph, node, columns, types, projections,
                      upstreams) -> None:
    upstream_id = node.raw["input"]
    if upstream_id not in graph.nodes:
        raise RepairValidationError(
            f"project {node.id!r} has unknown input {upstream_id!r}"
        )
    value_exprs = [e for e in node.exprs if e.role == "value"]
    if not value_exprs:
        raise RepairValidationError(
            f"project {node.id!r} must produce at least one column"
        )
    cols = [e.out_column for e in value_exprs]
    _check_column_names(node.id, cols)
    projs = [
        _ref_projection(graph, node, e, [upstream_id], types)
        for e in value_exprs
    ]
    projections[node.id] = projs
    columns[node.id] = cols
    types[node.id] = {p.out_column: p.out_type for p in projs}
    upstreams[node.id] = (upstream_id,)


def _validate_output(graph, node, columns, types, projections,
                     upstreams) -> None:
    if node.extra_inputs:
        raise RepairValidationError(
            f"output {node.id!r}: repair outputs take exactly one logical "
            f"upstream; extra_inputs={node.extra_inputs!r} are ambiguous"
        )
    upstream_id = node.output_of
    value_exprs = [e for e in node.exprs if e.role == "value"]
    if not value_exprs:
        raise RepairValidationError(
            f"output {node.id!r} must declare at least one release column"
        )
    cols = [e.out_column for e in value_exprs]
    _check_column_names(node.id, cols)
    projs = [
        _ref_projection(graph, node, e, [upstream_id], types)
        for e in value_exprs
    ]
    projections[node.id] = projs
    columns[node.id] = cols
    types[node.id] = {p.out_column: p.out_type for p in projs}
    upstreams[node.id] = (upstream_id,)


def _validate_join(graph, node, closure, columns, types, joins,
                   projections, upstreams) -> None:
    left, right = node.left, node.right
    if left not in closure or right not in closure:
        raise RepairValidationError(
            f"join {node.id!r}: both sides must feed the same release "
            f"output (left={left!r}, right={right!r})"
        )
    on_exprs = [e for e in node.exprs if e.role == "join_on"]
    if len(on_exprs) != 1:
        raise RepairValidationError(
            f"join {node.id!r}: repair supports exactly one single-column "
            f"equi-join condition, found {len(on_exprs)}"
        )
    on = on_exprs[0]
    if on.kind != "op" or on.op != "=" or len(on.refs) != 2:
        raise RepairValidationError(
            f"join {node.id!r}: the join condition must be a single '=' "
            f"between two qualified columns, got op={on.op!r} "
            f"refs={on.refs!r}"
        )
    lref = ColRef.parse(on.refs[0])
    rref = ColRef.parse(on.refs[1])
    sides = {lref.node_id, rref.node_id}
    if sides != {left, right}:
        raise RepairValidationError(
            f"join {node.id!r}: join keys {on.refs!r} must come one each "
            f"from left {left!r} and right {right!r}"
        )
    if lref.node_id != left:
        lref, rref = rref, lref
    if lref.column not in graph.nodes[left].output_columns or \
            rref.column not in graph.nodes[right].output_columns:
        raise RepairValidationError(
            f"join {node.id!r}: join key column is not produced by its side "
            f"({lref.fq}, {rref.fq})"
        )
    if types[left][lref.column] != types[right][rref.column]:
        raise RepairValidationError(
            f"join {node.id!r}: key columns {lref.fq} and {rref.fq} have "
            "different storage types; equality would be ambiguous"
        )
    # No display-only join type declarations are interpreted; anything else
    # stated explicitly must claim an inner join.
    declared_join = node.raw.get("join_type")
    if declared_join is not None and \
            str(declared_join).lower() not in ("inner", "join"):
        raise RepairValidationError(
            f"join {node.id!r}: only inner joins are supported, got "
            f"{declared_join!r}"
        )

    value_exprs = [e for e in node.exprs if e.role == "value"]
    if not value_exprs:
        raise RepairValidationError(
            f"join {node.id!r} must produce at least one value column"
        )
    cols = [e.out_column for e in value_exprs]
    _check_column_names(node.id, cols)
    projs = [
        _ref_projection(graph, node, e, [left, right], types)
        for e in value_exprs
    ]
    # The repair writer must be able to locate the old partition of a moved
    # key, so both equi-key columns must be carried onto the join output.
    projected_sources = {p.source.fq for p in projs}
    for key_fq in (lref.fq, rref.fq):
        if key_fq not in projected_sources:
            raise RepairValidationError(
                f"join {node.id!r}: join key {key_fq} must also be projected "
                "as an output column so affected key partitions can be "
                "located during repair"
            )
    joins[node.id] = JoinSpec(left=left, right=right,
                              left_key=lref.fq, right_key=rref.fq)
    projections[node.id] = projs
    columns[node.id] = cols
    types[node.id] = {p.out_column: p.out_type for p in projs}
    upstreams[node.id] = (left, right)


def _validate_agg(graph, node, closure, columns, types, aggs,
                  projections, upstreams) -> None:
    input_id = node.agg_input
    if input_id not in closure:
        raise RepairValidationError(
            f"agg {node.id!r}: input {input_id!r} does not feed the output"
        )
    raw = node.raw
    group_by = raw.get("group_by")
    if not isinstance(group_by, list) or not group_by:
        raise RepairValidationError(
            f"agg {node.id!r}: requires a structured non-empty 'group_by' "
            "list of qualified columns"
        )
    group_refs: List[ColRef] = []
    for ref_fq in group_by:
        ref = ColRef.parse(ref_fq)
        if ref.node_id != input_id:
            raise RepairValidationError(
                f"agg {node.id!r}: group_by entry {ref_fq} must reference "
                f"its input node {input_id!r}"
            )
        if ref.column not in graph.nodes[input_id].output_columns:
            raise RepairValidationError(
                f"agg {node.id!r}: group_by column {ref_fq} is not produced "
                f"by {input_id!r}"
            )
        group_refs.append(ref)
    if len({r.fq for r in group_refs}) != len(group_refs):
        raise RepairValidationError(
            f"agg {node.id!r}: duplicate group_by columns"
        )

    measures_raw = raw.get("measures")
    if not isinstance(measures_raw, list) or not measures_raw:
        raise RepairValidationError(
            f"agg {node.id!r}: requires a structured non-empty 'measures' "
            "list; display 'raw' text is never parsed"
        )
    measures: List[Measure] = []
    seen_out: set = set()
    for m in measures_raw:
        func = m.get("func")
        out = m.get("out_column")
        if not isinstance(out, str) or not _ID_RE.match(out):
            raise RepairValidationError(
                f"agg {node.id!r}: measure needs a valid 'out_column', got "
                f"{out!r}"
            )
        if out in seen_out:
            raise RepairValidationError(
                f"agg {node.id!r}: duplicate measure output column {out!r}"
            )
        seen_out.add(out)
        if func == "sum":
            ref_fq = m.get("ref")
            if not isinstance(ref_fq, str):
                raise RepairValidationError(
                    f"agg {node.id!r} measure {out!r}: sum requires a "
                    "qualified 'ref'"
                )
            ref = ColRef.parse(ref_fq)
            if ref.node_id != input_id or \
                    ref.column not in graph.nodes[input_id].output_columns:
                raise RepairValidationError(
                    f"agg {node.id!r} measure {out!r}: ref {ref_fq} is not "
                    f"a column of input node {input_id!r}"
                )
            if types[input_id][ref.column] != _INTEGER:
                raise RepairValidationError(
                    f"agg {node.id!r} measure {out!r}: SUM requires an "
                    f"INTEGER input, {ref_fq} is "
                    f"{types[input_id][ref.column]}"
                )
            measures.append(Measure("sum", out, ref.fq, _INTEGER))
        elif func == "count":
            if m.get("ref") is not None:
                raise RepairValidationError(
                    f"agg {node.id!r} measure {out!r}: COUNT(*) takes no "
                    "input ref"
                )
            measures.append(Measure("count", out, None, _INTEGER))
        else:
            raise RepairValidationError(
                f"agg {node.id!r}: unsupported measure func {func!r}; only "
                "integer 'sum' and 'count' are supported"
            )

    # Group keys must be carried onto the output through reference exprs.
    value_exprs = [e for e in node.exprs if e.role == "value"]
    expr_out = {e.out_column: e for e in value_exprs}
    group_outputs: List[str] = []
    for gref in group_refs:
        carrier = next((out for out, e in expr_out.items()
                        if e.kind == "ref" and e.refs == (gref.fq,)), None)
        if carrier is None:
            raise RepairValidationError(
                f"agg {node.id!r}: group key {gref.fq} must be carried to "
                "an output column through a plain reference expression"
            )
        group_outputs.append(carrier)
    # Every reference expr must correspond to one group key; nothing else may
    # ride along implicitly.
    for e in value_exprs:
        if e.out_column in {m.out_column for m in measures}:
            continue
        if e.kind != "ref" or e.refs[0] not in {g.fq for g in group_refs}:
            raise RepairValidationError(
                f"agg {node.id!r} column {e.out_column!r}: non-measure "
                "outputs may only carry group-by keys via plain references"
            )
    measure_names = {m.out_column for m in measures}
    if not measure_names.isdisjoint(group_outputs):
        raise RepairValidationError(
            f"agg {node.id!r}: measure outputs and group outputs collide"
        )
    unknown_expr = set(expr_out) - measure_names - set(group_outputs)
    if unknown_expr:
        raise RepairValidationError(
            f"agg {node.id!r}: expr columns {sorted(unknown_expr)} match "
            "neither a structured measure nor a group-by carrier"
        )
    if set(measure_names) != {m.out_column for m in measures} or \
            not measure_names.issubset(set(expr_out)):
        raise RepairValidationError(
            f"agg {node.id!r}: every measure must have a matching value "
            "expression output column"
        )

    ordered_cols = [e.out_column for e in value_exprs]
    _check_column_names(node.id, ordered_cols)
    agg_types: Dict[str, str] = {}
    for gcol in group_outputs:
        src = next(e for e in value_exprs if e.out_column == gcol)
        ref = ColRef.parse(src.refs[0])
        agg_types[gcol] = types[input_id][ref.column]
    for m in measures:
        agg_types[m.out_column] = m.out_type
    aggs[node.id] = AggSpec(
        input_node=input_id,
        group_by=tuple(g.fq for g in group_refs),
        group_outputs=tuple(group_outputs),
        measures=tuple(measures),
    )
    projections[node.id] = [
        _ref_projection(graph, node, e, [input_id], types)
        for e in value_exprs if e.out_column in group_outputs
    ]
    columns[node.id] = ordered_cols
    types[node.id] = agg_types
    upstreams[node.id] = (input_id,)
