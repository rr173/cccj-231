"""Parsing and strict validation of graph, runtime and package inputs.

Design rule: everything is resolved to fully-qualified ``node.column``
identities while loading.  Same-named columns from different join inputs stay
distinct from this point onward, and renamed columns keep an explicit
reference edge instead of being matched by name later.
"""

from __future__ import annotations

from typing import Any, Dict, List, Mapping

from .digest import schema_digest
from .model import (
    ColRef,
    ConsistencyError,
    Expr,
    Graph,
    GraphValidationError,
    Node,
    NodeRecord,
    PackageColumn,
    ReleasePackage,
    RuntimeRecord,
)

_VALUE_NODE_TYPES = {"input", "project", "join", "agg", "explode", "output"}


# ---------------------------------------------------------------------------
# Graph loading
# ---------------------------------------------------------------------------
def _build_expr(raw: Mapping[str, Any], role: str) -> Expr:
    refs = tuple(raw.get("refs", ()))
    for r in refs:
        ColRef.parse(r)  # rejects unqualified references early
    return Expr(
        kind=raw["kind"],
        op=raw.get("op"),
        out_column=raw["out_column"],
        refs=tuple(refs),
        raw=raw.get("raw", raw["out_column"]),
        role=role,
    )


def load_graph(payload: Mapping[str, Any]) -> Graph:
    if "graph_version" not in payload or "nodes" not in payload:
        raise GraphValidationError("graph requires 'graph_version' and 'nodes'")
    graph_version = str(payload["graph_version"])
    nodes: Dict[str, Node] = {}

    for raw_node in payload["nodes"]:
        ntype = raw_node["type"]
        node = Node(
            id=raw_node["id"],
            type=ntype,
            table=raw_node.get("table"),
            raw=dict(raw_node),
        )
        if node.id in nodes:
            raise GraphValidationError(f"duplicate node id {node.id!r}")

        if ntype == "input":
            node.exprs = [
                Expr(kind="ref", op=None, out_column=c["name"], refs=(),
                     raw=c["name"])
                for c in raw_node["columns"]
            ]
        elif ntype in ("project",):
            node.exprs = [_build_expr(e, "value") for e in raw_node["exprs"]]
        elif ntype == "join":
            node.left, node.right = raw_node["left"], raw_node["right"]
            node.exprs = [_build_expr(e, "value") for e in raw_node["exprs"]]
            for on in raw_node.get("on", []):
                node.exprs.append(_build_expr(on, "join_on"))
        elif ntype == "agg":
            node.agg_input = raw_node["input"]
            node.exprs = [_build_expr(e, "value") for e in raw_node["exprs"]]
        elif ntype == "explode":
            node.explode_input = raw_node["input"]
            node.exprs.append(
                Expr(kind="ref", op=None,
                     out_column=raw_node["out_column"],
                     refs=(raw_node["ref"],), raw=raw_node["ref"])
            )
            for e in raw_node.get("passthrough", []):
                node.exprs.append(_build_expr(e, "value"))
        elif ntype == "branch":
            # A branch node is transparent for column lineage.  Its gate
            # registry is stored here; the structural edge from the branch's
            # upstream to each arm target is wired in _build_adjacency().
            for arm in raw_node["branches"]:
                node.branches.setdefault(arm["id"], []).append(
                    (arm["id"], arm["target"])
                )
            # branch condition references are recorded but never treated as
            # value-lineage input columns.
            node.exprs = [_build_expr(e, "condition")
                          for e in raw_node.get("condition", [])]
        elif ntype == "output":
            node.output_of = _resolve_logical_input(raw_node["input"], nodes)
            node.extra_inputs = [
                _resolve_logical_input(x, nodes)
                for x in raw_node.get("extra_inputs", [])
            ]
            node.table = raw_node.get("table")
            node.exprs = [_build_expr(e, "value") for e in raw_node["exprs"]]
        else:
            raise GraphValidationError(
                f"node {node.id!r} has unknown type {ntype!r}"
            )
        nodes[node.id] = node

    adjacency = _build_adjacency(nodes)
    _validate_dag(nodes, adjacency)
    _validate_references(nodes, adjacency)
    return Graph(graph_version=graph_version, nodes=nodes, adjacency=adjacency)


def _resolve_logical_input(input_id: str,
                           nodes: Dict[str, Node]) -> str:
    """Map a declared input through transparent branch nodes."""
    seen = set()
    cur = input_id
    while cur in nodes and nodes[cur].type == "branch" and cur not in seen:
        seen.add(cur)
        cur = nodes[cur].raw["input"]
    return cur


def _build_adjacency(nodes: Dict[str, Node]) -> Dict[str, List[str]]:
    adj: Dict[str, List[str]] = {nid: [] for nid in nodes}
    for nid, node in nodes.items():
        if node.type == "project":
            adj[_resolve_logical_input(node.raw["input"], nodes)].append(nid)
        elif node.type == "join":
            adj[node.left].append(nid)
            adj[node.right].append(nid)
        elif node.type == "agg":
            adj[node.agg_input].append(nid)
        elif node.type == "explode":
            adj[node.explode_input].append(nid)
        elif node.type == "output":
            adj[node.output_of].append(nid)
            for extra in node.extra_inputs:
                adj[extra].append(nid)
        elif node.type == "branch":
            upstream = node.raw["input"]
            for arms in node.branches.values():
                for _, target in arms:
                    adj[upstream].append(target)
    return adj


def _validate_dag(nodes: Dict[str, Node],
                  adjacency: Dict[str, List[str]]) -> None:
    WHITE, GRAY, BLACK = 0, 1, 2
    color = {nid: WHITE for nid in nodes}

    def visit(nid: str, stack: List[str]) -> None:
        color[nid] = GRAY
        for nxt in adjacency[nid]:
            if nxt not in nodes:
                raise GraphValidationError(
                    f"edge {nid} -> {nxt!r} points at an unknown node"
                )
            if color[nxt] == GRAY:
                raise GraphValidationError(
                    "cycle detected: " + " -> ".join(stack + [nxt]))
            if color[nxt] == WHITE:
                visit(nxt, stack + [nxt])
        color[nid] = BLACK

    for nid in nodes:
        if color[nid] == WHITE:
            visit(nid, [nid])


def _ancestors(nid: str, adjacency: Dict[str, List[str]]) -> set:
    seen, stack = set(), [nid]
    reverse: Dict[str, List[str]] = {}
    for up, downs in adjacency.items():
        for d in downs:
            reverse.setdefault(d, []).append(up)
    while stack:
        cur = stack.pop()
        for up in reverse.get(cur, ()):
            if up not in seen:
                seen.add(up)
                stack.append(up)
    return seen


def _validate_references(nodes: Dict[str, Node],
                         adjacency: Dict[str, List[str]]) -> None:
    for nid, node in nodes.items():
        if node.type in ("input", "branch"):
            continue
        ancestors = _ancestors(nid, adjacency)
        for expr in node.exprs:
            for ref in expr.refs:
                col = ColRef.parse(ref)
                if col.node_id not in nodes:
                    raise GraphValidationError(
                        f"node {nid!r} expression {expr.raw!r} references "
                        f"unknown node {col.node_id!r}"
                    )
                if col.node_id not in ancestors:
                    raise GraphValidationError(
                        f"node {nid!r} references {ref!r} which is not an "
                        "upstream node"
                    )
                producer = nodes[col.node_id]
                if col.column not in producer.output_columns:
                    raise GraphValidationError(
                        f"node {nid!r} references {ref!r} but column is not "
                        f"produced by node {col.node_id!r}"
                    )


# ---------------------------------------------------------------------------
# Runtime records loading
# ---------------------------------------------------------------------------
def load_runtime(payload: Mapping[str, Any], graph: Graph) -> RuntimeRecord:
    for key in ("graph_version", "run_version", "package_digest", "nodes"):
        if key not in payload:
            raise GraphValidationError(f"runtime record missing {key!r}")

    records: Dict[str, NodeRecord] = {}
    missing: List[str] = []
    for nid, node in graph.nodes.items():
        raw = payload["nodes"].get(nid)
        if raw is None:
            # No record is NOT evidence of absence: keep the node on the
            # missing list so the diagnostic layer reports "unprovable".
            missing.append(nid)
            continue
        if nid in records:
            raise GraphValidationError(f"duplicate runtime record for {nid!r}")
        pos, neg, neg_refs = [], [], []
        for col in raw.get("read_positive", []):
            pos.append(_qualify(col, nid, node))
        for col in raw.get("read_negative", []):
            neg.append(_qualify(col, nid, node))
        for col in raw.get("read_negative_refs", []):
            ref = ColRef.parse(col)
            if ref.node_id not in graph.nodes:
                raise GraphValidationError(
                    f"runtime record on {nid!r} negative ref {col!r} "
                    "references unknown node"
                )
            neg_refs.append(col)
        records[nid] = NodeRecord(
            node_id=nid,
            executed=bool(raw.get("executed", True)),
            branches_taken=tuple(raw.get("branches_taken", ())),
            branches_skipped=tuple(raw.get("branches_skipped", ())),
            pos=tuple(pos),
            neg=tuple(neg),
            neg_refs=tuple(neg_refs),
            has_record=True,
        )

    unknown = set(payload["nodes"]) - set(graph.nodes)
    if unknown:
        raise GraphValidationError(
            f"runtime records reference unknown nodes: {sorted(unknown)}"
        )

    samples = payload.get("output_samples", {})
    return RuntimeRecord(
        graph_version=str(payload["graph_version"]),
        run_version=str(payload["run_version"]),
        package_digest=str(payload["package_digest"]),
        nodes=records,
        output_samples=samples,
        missing_nodes=missing,
    )


def _qualify(col: str, node_id: str, node: Node) -> str:
    if "." in col:
        ColRef.parse(col)
        return col
    # Bare column names are accepted only when this node itself produces a
    # column of that name; otherwise the identity would be ambiguous.
    if node.type != "input" or col not in node.output_columns:
        raise GraphValidationError(
            f"runtime record on {node_id!r} uses ambiguous bare column "
            f"{col!r}; expected fully-qualified 'node.column'"
        )
    return f"{node_id}.{col}"


# ---------------------------------------------------------------------------
# Release package loading
# ---------------------------------------------------------------------------
def load_package(payload: Mapping[str, Any]) -> ReleasePackage:
    for key in ("package_name", "package_version", "graph_version",
                "run_version", "columns", "output_node"):
        if key not in payload:
            raise GraphValidationError(f"release package missing {key!r}")
    columns = [
        PackageColumn(
            name=c["name"],
            type=str(c["type"]),
            sample_digest=str(c["sample_digest"]),
        )
        for c in payload["columns"]
    ]
    if len({c.name for c in columns}) != len(columns):
        raise GraphValidationError("duplicate output column in package schema")

    declared = str(payload.get("schema_digest", ""))
    actual = schema_digest(
        [{"name": c.name, "type": c.type} for c in columns]
    )
    if declared and declared != actual:
        raise ConsistencyError(
            "package schema_digest does not match the digest recomputed "
            f"from its columns (declared {declared[:12]}..., "
            f"recomputed {actual[:12]}...)"
        )

    return ReleasePackage(
        package_name=str(payload["package_name"]),
        package_version=str(payload["package_version"]),
        graph_version=str(payload["graph_version"]),
        run_version=str(payload["run_version"]),
        schema_digest=actual,
        output_node=str(payload["output_node"]),
        columns=columns,
    )


# ---------------------------------------------------------------------------
# Cross-artifact consistency gate
# ---------------------------------------------------------------------------
def assert_consistent(graph: Graph, runtime: RuntimeRecord,
                      package: ReleasePackage) -> None:
    """Refuse diagnosis whenever versions or summaries disagree.

    The three artifacts are only jointly interpretable when graph version,
    run version, package digest and every output sample summary match.
    """
    problems: List[str] = []
    if graph.graph_version != runtime.graph_version:
        problems.append(
            f"graph version {graph.graph_version!r} != runtime graph "
            f"version {runtime.graph_version!r}"
        )
    if graph.graph_version != package.graph_version:
        problems.append(
            f"graph version {graph.graph_version!r} != package graph "
            f"version {package.graph_version!r}"
        )
    if runtime.run_version != package.run_version:
        problems.append(
            f"runtime run version {runtime.run_version!r} != package run "
            f"version {package.run_version!r}"
        )
    if runtime.package_digest != package.schema_digest:
        problems.append(
            "runtime package digest != package schema digest "
            f"({runtime.package_digest[:12]}... vs "
            f"{package.schema_digest[:12]}...)"
        )

    out_node = _find_output_node(graph, package)
    out_record = runtime.nodes.get(out_node.id)
    if out_record is None:
        problems.append(
            f"output node {out_node.id!r} has no runtime record at all; "
            "the package cannot be proven against this run"
        )
        out_record = None
    recorded_samples = runtime.output_samples.get(out_node.id, {})
    for col in package.columns:
        if col.name not in recorded_samples:
            problems.append(
                f"output column {col.name!r} has no runtime sample summary "
                "(record missing -> cannot prove package contents)"
            )
        elif recorded_samples[col.name] != col.sample_digest:
            problems.append(
                f"output column {col.name!r} sample digest disagrees with "
                "the release package summary"
            )
        if out_record is not None and (
            f"{out_node.id}.{col.name}" not in out_record.pos
        ):
            problems.append(
                f"output column {col.name!r} has no positive column read on "
                f"node {out_node.id!r} in the runtime record"
            )

    # Every output column declared by the package must exist on its node.
    produced = set(out_node.output_columns)
    for col in package.columns:
        if col.name not in produced:
            problems.append(
                f"package column {col.name!r} is not produced by output "
                f"node {out_node.id!r}"
            )

    if problems:
        raise ConsistencyError(
            "diagnosis refused due to inconsistent artifacts: "
            + " | ".join(problems)
        )


def _find_output_node(graph: Graph, package: ReleasePackage) -> Node:
    output_node_id = package.output_node
    if output_node_id not in graph.nodes:
        raise ConsistencyError(
            f"package output node {output_node_id!r} absent from graph"
        )
    node = graph.nodes[output_node_id]
    if node.type != "output":
        raise ConsistencyError(
            f"package output node {output_node_id!r} is not an output node"
        )
    return node
