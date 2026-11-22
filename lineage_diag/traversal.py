"""Static graph traversal: column-level possible lineage paths.

This module deliberately knows nothing about runtime records.  It enumerates
the *possible* dependencies of the declarative graph as column-to-column
edges, and resolves the optional branch gates that each edge sits behind.

Source identity rules enforced here:

* every edge is stored as fully-qualified ``node.column`` references;
* a rename is an ordinary edge, so ``uid -> uid_copy`` keeps both identities;
* same-named join keys produce two distinct edges
  (``t1.uid -> j.uid`` and ``t2.uid -> j.uid``);
* a branch node is transparent for column lineage but contributes a gate;
* shadowed columns are simply different qualified names and never merge.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from typing import Dict, FrozenSet, Iterator, List, Tuple

from .model import ColRef, Graph, GraphValidationError

# A gate records "this edge only exists at runtime when branch node X took
# arm Y".  Gates are structural facts; whether the arm actually fired is a
# runtime question answered by the diagnostic layer.
Gate = Tuple[str, str]  # (branch node id, arm id)


@dataclass(frozen=True)
class ColumnEdge:
    """One static column-level dependency edge."""

    source: ColRef
    target: ColRef
    expr_role: str          # "value" (condition/join_on edges are excluded)
    gates: FrozenSet[Gate]  # branch arms between the two *nodes*

    @property
    def fingerprint(self) -> str:
        gates = ";".join(f"{b}/{a}" for b, a in sorted(self.gates))
        return f"{self.source.fq}->{self.target.fq}[{gates}]"


@dataclass(frozen=True)
class LineagePath:
    """A complete input-column -> output-column static path."""

    input_ref: ColRef
    output_ref: ColRef
    edges: Tuple[ColumnEdge, ...]

    @property
    def fingerprint(self) -> str:
        return "|".join(e.fingerprint for e in self.edges)

    def gates(self) -> FrozenSet[Gate]:
        merged: set = set()
        for e in self.edges:
            merged.update(e.gates)
        return frozenset(merged)


class GraphExplorer:
    """Builds and indexes the static column-edge view of a graph."""

    def __init__(self, graph: Graph):
        self.graph = graph
        self._value_edges: List[ColumnEdge] = []
        # target fq -> edges producing it
        self._in_edges: Dict[str, List[ColumnEdge]] = {}
        # source fq -> edges consuming it
        self._out_edges: Dict[str, List[ColumnEdge]] = {}
        self._build_edges()

    # ------------------------------------------------------------------
    # Edge construction
    # ------------------------------------------------------------------
    def _build_edges(self) -> None:
        for nid, node in self.graph.nodes.items():
            if node.type in ("input", "branch"):
                continue
            for expr in node.exprs:
                if expr.role != "value":
                    # join conditions and branch predicates are not
                    # value-lineage edges; they are still visible in runtime
                    # records and handled by the no-contribution analysis.
                    continue
                target = ColRef(nid, expr.out_column)
                for ref_fq in expr.refs:
                    source = ColRef.parse(ref_fq)
                    gates = self.gates_for_target(nid)
                    edge = ColumnEdge(
                        source=source, target=target,
                        expr_role=expr.role, gates=gates,
                    )
                    self._value_edges.append(edge)
                    self._in_edges.setdefault(target.fq, []).append(edge)
                    self._out_edges.setdefault(source.fq, []).append(edge)

    @lru_cache(maxsize=None)
    def gates_for_target(self, node_id: str) -> FrozenSet[Gate]:
        """Gates that guard *entering* one value node.

        A branch arm targets exactly one value node.  Any column edge whose
        consumer is that target node is therefore executed only when the arm
        fires -- regardless of how many other (ungated) edges terminate at
        other nodes.  This keeps ``j_user -> o_rel`` (direct) ungated while
        ``p_over -> o_rel`` inherits the ``b_gate/b1`` gate because
        ``p_over`` itself is an arm target.
        """
        gates: set = set()
        for branch_id, upstream, arm_id, target in self._branch_gate_by_target():
            if target == node_id:
                gates.add((branch_id, arm_id))
        return frozenset(gates)

    @lru_cache(maxsize=None)
    def gates_between(self, src: str, dst: str) -> FrozenSet[Gate]:
        """Gates attached to the consumer node of a column edge.

        Kept as an explicit helper for callers thinking in node pairs.
        """
        del src  # gates are determined by the arm-target consumer
        return self.gates_for_target(dst)

    @lru_cache(maxsize=None)
    def _branch_gate_by_target(
        self
    ) -> Tuple[Tuple[str, str, str, str], ...]:
        """Flattened registry: (branch, upstream, arm, target node)."""
        entries: List[Tuple[str, str, str, str]] = []
        for nid, node in self.graph.nodes.items():
            if node.type != "branch":
                continue
            upstream = node.raw["input"]
            for arms in node.branches.values():
                for arm_id, target in arms:
                    entries.append((nid, upstream, arm_id, target))
        return tuple(entries)

    # ------------------------------------------------------------------
    # Path enumeration (lazy generators; nothing materialized wholesale)
    # ------------------------------------------------------------------
    def iter_backward_paths(self, output_fq: str) -> Iterator[LineagePath]:
        """All static paths feeding one output column, lazily."""
        seen: set = set()

        def walk(target_fq: str,
                 tail: Tuple[ColumnEdge, ...]) -> Iterator[LineagePath]:
            target = ColRef.parse(target_fq)
            producer = self.graph.nodes[target.node_id]
            incoming = self._in_edges.get(target_fq, ())
            if producer.type == "input":
                if tail:  # an input column terminates a real path
                    path = LineagePath(
                        input_ref=target,
                        output_ref=ColRef.parse(output_fq),
                        edges=tuple(reversed(tail)),
                    )
                    if path.fingerprint not in seen:
                        seen.add(path.fingerprint)
                        yield path
                return
            if not incoming:
                # defensive: non-input column without edges is a graph error
                raise GraphValidationError(
                    f"column {target_fq} has no producing edge and is not "
                    "an input column"
                )
            for edge in incoming:
                yield from walk(edge.source.fq, tail + (edge,))

        yield from walk(output_fq, ())

    def iter_forward_paths(self, input_fq: str) -> Iterator[LineagePath]:
        """All static paths starting from one input column, lazily."""
        seen: set = set()

        def walk(source_fq: str,
                 head: Tuple[ColumnEdge, ...]) -> Iterator[LineagePath]:
            outgoing = self._out_edges.get(source_fq, ())
            if not outgoing:
                return
            for edge in outgoing:
                new_head = head + (edge,)
                if self.graph.nodes[edge.target.node_id].type == "output":
                    path = LineagePath(
                        input_ref=ColRef.parse(input_fq),
                        output_ref=edge.target,
                        edges=new_head,
                    )
                    if path.fingerprint not in seen:
                        seen.add(path.fingerprint)
                        yield path
                # continue walking regardless: an intermediate node can
                # both be consumed and feed downstream transforms
                yield from walk(edge.target.fq, new_head)

        yield from walk(input_fq, ())

    def all_input_columns(self) -> List[ColRef]:
        return [
            ColRef(nid, col)
            for nid, node in self.graph.nodes.items()
            if node.type == "input"
            for col in node.output_columns
        ]

    def all_value_edges(self) -> List[ColumnEdge]:
        return list(self._value_edges)
