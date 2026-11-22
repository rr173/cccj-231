"""Merge static lineage paths with runtime evidence.

Classification vocabulary (the four mandated result kinds):

* ``NOT_EXECUTED``  - a dependency is declared by the graph but the only
  paths to it sit behind a branch arm that this run did not take, or the
  consuming node explicitly recorded a negative read;
* ``NO_CONTRIBUTION`` - the run actually read a column (positive record)
  but no static path connects it to any released output column;
* ``PARTIAL_PATH`` - an output column has at least one evidenced path but
  not every static path is evidenced;
* ``UNPROVABLE`` - evidence is missing (no record, or a gate with no
  executed/skipped entry).  Missing records are never treated as "no
  dependency".
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from .model import (
    ColRef,
    Graph,
    ReleasePackage,
    RuntimeRecord,
)
from .traversal import ColumnEdge, GraphExplorer, LineagePath

PROVEN = "PROVEN"
NOT_EXECUTED = "NOT_EXECUTED"
NO_CONTRIBUTION = "NO_CONTRIBUTION"
PARTIAL_PATH = "PARTIAL_PATH"
UNPROVABLE = "UNPROVABLE"

# Evidence states for a single static path.
_EV_EVIDENCED = "evidenced"
_EV_BLOCKED = "not_executed"
_EV_NEGATIVE = "negative_read"
_EV_MISSING = "no_record"


@dataclass
class Evidence:
    """Runtime proof attached to one edge (or gate) check."""

    kind: str                      # positive | negative | missing | gate
    node_id: str
    column: Optional[str]
    detail: str


@dataclass
class AssessedPath:
    path: LineagePath
    state: str                     # one of the _EV_* constants
    evidence_chain: List[Evidence] = field(default_factory=list)
    weak_point: Optional[Evidence] = None


@dataclass
class OutputDiagnosis:
    output_ref: ColRef
    status: str                    # PROVEN / PARTIAL_PATH / NOT_EXECUTED / UNPROVABLE
    static_path_count: int
    evidenced_path_count: int
    paths: List[AssessedPath]


@dataclass
class Finding:
    category: str
    output_ref: Optional[ColRef]
    input_ref: Optional[ColRef]
    message: str
    path_fingerprint: Optional[str] = None
    representative_evidence: Optional[List[Dict[str, Any]]] = None


@dataclass
class Diagnosis:
    graph: Graph
    runtime: RuntimeRecord
    package: ReleasePackage
    outputs: List[OutputDiagnosis]
    findings: List[Finding]
    # cached forward index: input fq -> assessed paths reaching outputs
    _forward_index: Dict[str, List[AssessedPath]]

    # -- queries --------------------------------------------------------
    def forward_query(self, input_fq: str, page: int = 1,
                      page_size: int = 50) -> Dict[str, Any]:
        """All output columns one input column can influence, with proof."""
        ColRef.parse(input_fq)
        items = self._forward_index.get(input_fq, [])
        return _page([_assessed_path_dict(p) for p in items], page, page_size)

    def backward_query(self, output_fq: str, page: int = 1,
                       page_size: int = 50) -> Dict[str, Any]:
        """All input columns feeding one output, plus unverified branches."""
        target = next((o for o in self.outputs
                       if o.output_ref.fq == output_fq), None)
        if target is None:
            raise KeyError(f"unknown output column {output_fq!r}")
        items = [_assessed_path_dict(p) for p in target.paths]
        page_obj = _page(items, page, page_size)
        page_obj["status"] = target.status
        page_obj["unverified_branches"] = sorted({
            f"{branch}/{arm}"
            for p in target.paths
            if p.state in (_EV_BLOCKED, _EV_MISSING)
            for branch, arm in p.path.gates()
        })
        page_obj["input_columns"] = sorted({
            p.path.input_ref.fq for p in target.paths
        })
        return page_obj

    def gap_list(self) -> List[Dict[str, Any]]:
        return [_finding_dict(f) for f in self.findings]


# ---------------------------------------------------------------------------
# Diagnostic engine
# ---------------------------------------------------------------------------
class Diagnoser:
    def __init__(self, graph: Graph, runtime: RuntimeRecord,
                 package: ReleasePackage):
        self.graph = graph
        self.runtime = runtime
        self.package = package
        self.explorer = GraphExplorer(graph)
        self._record_index = {
            fq: rec for rec in runtime.nodes.values() for fq in (*rec.pos,
                                                                  *rec.neg)
        }

    def run(self) -> Diagnosis:
        outputs: List[OutputDiagnosis] = []
        findings: List[Finding] = []
        forward_index: Dict[str, List[AssessedPath]] = {}

        output_node = self.graph.nodes[self.package.output_node]
        for col in output_node.output_columns:
            if col not in {c.name for c in self.package.columns}:
                continue  # node produces more than the package ships
            out_ref = ColRef(output_node.id, col)
            paths = [
                self._assess(p)
                for p in self.explorer.iter_backward_paths(out_ref.fq)
            ]
            status, output_findings = self._classify_output(out_ref, paths)
            outputs.append(OutputDiagnosis(
                output_ref=out_ref,
                status=status,
                static_path_count=len(paths),
                evidenced_path_count=sum(
                    1 for p in paths if p.state == _EV_EVIDENCED),
                paths=paths,
            ))
            findings.extend(output_findings)
            for assessed in paths:
                forward_index.setdefault(
                    assessed.path.input_ref.fq, []
                ).append(assessed)

        findings.extend(self._scan_no_contribution(forward_index))
        return Diagnosis(
            graph=self.graph, runtime=self.runtime, package=self.package,
            outputs=outputs, findings=findings, _forward_index=forward_index,
        )

    # ------------------------------------------------------------------
    # Per-path evidence assessment
    # ------------------------------------------------------------------
    def _assess(self, path: LineagePath) -> AssessedPath:
        chain: List[Evidence] = []
        weak: Optional[Evidence] = None
        state = _EV_EVIDENCED

        # 1. branch gates are evaluated first: a skipped arm blocks the path
        for branch_id, arm_id in sorted(path.gates()):
            gate_ev = self._gate_evidence(branch_id, arm_id)
            chain.append(gate_ev)
            if gate_ev.kind == "gate_blocked":
                weak = weak or gate_ev
                state = _EV_BLOCKED
            elif gate_ev.kind == "gate_missing":
                weak = weak or gate_ev
                state = _EV_MISSING

        # 2. every column edge needs a positive read at its consuming node
        for edge in path.edges:
            edge_ev = self._edge_evidence(edge)
            chain.append(edge_ev)
            if edge_ev.kind in ("negative", "negative_ref"):
                if state == _EV_EVIDENCED:
                    state = _EV_NEGATIVE
                weak = weak or edge_ev
            elif edge_ev.kind == "missing":
                if state == _EV_EVIDENCED:
                    state = _EV_MISSING
                weak = weak or edge_ev

        if state == _EV_NEGATIVE:
            state = _EV_BLOCKED  # explicit negative => declared but not used
        return AssessedPath(path=path, state=state,
                            evidence_chain=chain, weak_point=weak)

    def _gate_evidence(self, branch_id: str, arm_id: str) -> Evidence:
        rec = self.runtime.nodes.get(branch_id)
        if rec is None:
            return Evidence("gate_missing", branch_id, None,
                            f"no runtime record for branch {branch_id}; "
                            f"arm {arm_id} execution cannot be proven")
        if arm_id in rec.branches_taken:
            return Evidence("gate_taken", branch_id, None,
                            f"branch {branch_id} took arm {arm_id}")
        if arm_id in rec.branches_skipped:
            return Evidence("gate_blocked", branch_id, None,
                            f"branch {branch_id} skipped arm {arm_id}")
        return Evidence("gate_missing", branch_id, None,
                        f"branch {branch_id} has no taken/skipped entry "
                        f"for arm {arm_id}")

    def _edge_evidence(self, edge: ColumnEdge) -> Evidence:
        consumer = edge.target.node_id
        rec = self.runtime.nodes.get(consumer)
        if rec is None:
            return Evidence("missing", consumer, edge.target.column,
                            f"node {consumer} has no runtime record; "
                            f"read of {edge.target.fq} cannot be proven")
        # edge-level negative first: one arm of a function may be un-read
        # even though the produced column was positively read.
        if edge.source.fq in rec.neg_refs:
            return Evidence(
                "negative_ref", consumer, edge.target.column,
                f"{consumer} explicitly did not read source {edge.source.fq} "
                f"while producing {edge.target.fq}")
        if edge.target.fq in rec.pos:
            return Evidence("positive", consumer, edge.target.column,
                            f"{consumer} positively read {edge.target.fq}")
        if edge.target.fq in rec.neg:
            return Evidence("negative", consumer, edge.target.column,
                            f"{consumer} explicitly did NOT read "
                            f"{edge.target.fq}")
        return Evidence("missing", consumer, edge.target.column,
                        f"no read record for {edge.target.fq} (missing "
                        "evidence is not treated as absence of dependency)")

    # ------------------------------------------------------------------
    # Output-level classification
    # ------------------------------------------------------------------
    def _classify_output(
        self, out_ref: ColRef, paths: List[AssessedPath]
    ) -> Tuple[str, List[Finding]]:
        if not paths:
            return UNPROVABLE, [Finding(
                category=UNPROVABLE, output_ref=out_ref, input_ref=None,
                message=f"output {out_ref.fq} has no static input path at "
                        "all; lineage cannot be proven",
            )]

        evidenced = [p for p in paths if p.state == _EV_EVIDENCED]
        dead = [p for p in paths if p.state != _EV_EVIDENCED]
        findings: List[Finding] = []

        if not dead:
            return PROVEN, []

        blocked = [p for p in dead if p.state == _EV_BLOCKED]
        missing = [p for p in dead if p.state == _EV_MISSING]

        if evidenced:
            for p in dead:
                findings.append(self._path_finding(PARTIAL_PATH, out_ref, p))
            return PARTIAL_PATH, findings

        if missing and not blocked:
            for p in missing:
                findings.append(self._path_finding(UNPROVABLE, out_ref, p))
            return UNPROVABLE, findings

        if blocked and not missing:
            for p in blocked:
                findings.append(
                    self._path_finding(NOT_EXECUTED, out_ref, p))
            return NOT_EXECUTED, findings

        # Mixed dead paths with no evidence: the output headline is
        # UNPROVABLE; blocked paths are recorded inside that finding's
        # evidence chain rather than emitted as separate NOT_EXECUTED
        # findings, which would mislabel the whole output.
        for p in dead:
            findings.append(self._path_finding(UNPROVABLE, out_ref, p))
        return UNPROVABLE, findings

    def _path_finding(self, category: str, out_ref: ColRef,
                      assessed: AssessedPath) -> Finding:
        weak = assessed.weak_point
        reason = weak.detail if weak else "no runtime evidence"
        sub = {
            _EV_BLOCKED: "declared dependency not executed in this run",
            _EV_MISSING: "runtime record missing; cannot prove dependency",
        }.get(assessed.state, assessed.state)
        return Finding(
            category=category,
            output_ref=out_ref,
            input_ref=assessed.path.input_ref,
            message=f"{out_ref.fq} <- {assessed.path.input_ref.fq}: {sub} "
                    f"({reason})",
            path_fingerprint=assessed.path.fingerprint,
            representative_evidence=[_evidence_dict(e)
                                     for e in assessed.evidence_chain],
        )

    # ------------------------------------------------------------------
    # Positive reads that never reach an output
    # ------------------------------------------------------------------
    def _scan_no_contribution(
        self, forward_index: Dict[str, List[AssessedPath]]
    ) -> List[Finding]:
        """Positive reads that never influence a released output column.

        Only columns *produced* by a value node count as value reads; reading
        an input column while processing the node is consumption, not
        production, and is therefore excluded.  Output node reads are
        excluded too: by definition they feed the package.
        """
        findings: List[Finding] = []
        seen: set = set()
        for nid, node in self.graph.nodes.items():
            rec = self.runtime.nodes.get(nid)
            if rec is None or not rec.executed:
                continue
            # Input-table reads are source ingestion, never "no output
            # contribution" findings; output reads feed the package by
            # definition.  Only dead intermediate produced columns qualify.
            if node.type in ("input", "output", "branch"):
                continue
            produced_cols = {
                f"{nid}.{e.out_column}"
                for e in node.exprs if e.role == "value"
            }
            for fq in rec.pos:
                if fq not in produced_cols:
                    continue
                if self._reaches_output(fq):
                    continue
                key = ("cat2", fq)
                if key in seen:
                    continue
                seen.add(key)
                findings.append(Finding(
                    category=NO_CONTRIBUTION,
                    output_ref=None,
                    input_ref=ColRef.parse(fq),
                    message=f"{fq} was positively read during the run but no "
                            "static path carries it into any released output "
                            "column",
                ))
        return findings

    def _reaches_output(self, fq: str) -> bool:
        """Transitive value-flow check against any released output column.

        Walks the complete static column-edge map (not only path origins,
        which are input columns) so intermediate produced columns resolve
        correctly through diamond joins and multi-hop transforms.
        """
        output_cols = {
            f"{self.package.output_node}.{c.name}"
            for c in self.package.columns
        }
        stack = [fq]
        visited: set = set()
        while stack:
            cur = stack.pop()
            if cur in visited:
                continue
            visited.add(cur)
            if cur in output_cols:
                return True
            for edge in self.explorer._out_edges.get(cur, ()):
                nxt = edge.target.fq
                if nxt not in visited:
                    stack.append(nxt)
        return False


# ---------------------------------------------------------------------------
# Serialization helpers
# ---------------------------------------------------------------------------
def _evidence_dict(ev: Evidence) -> Dict[str, Any]:
    return {"kind": ev.kind, "node_id": ev.node_id,
            "column": ev.column, "detail": ev.detail}


def _assessed_path_dict(p: AssessedPath) -> Dict[str, Any]:
    return {
        "input_column": p.path.input_ref.fq,
        "output_column": p.path.output_ref.fq,
        "state": p.state,
        "gates": [f"{b}/{a}" for b, a in sorted(p.path.gates())],
        "path": [e.fingerprint for e in p.path.edges],
        "evidence": [_evidence_dict(e) for e in p.evidence_chain],
        "fingerprint": p.path.fingerprint,
    }


def _finding_dict(f: Finding) -> Dict[str, Any]:
    return {
        "category": f.category,
        "output_column": f.output_ref.fq if f.output_ref else None,
        "input_column": f.input_ref.fq if f.input_ref else None,
        "message": f.message,
        "path_fingerprint": f.path_fingerprint,
        "representative_evidence": f.representative_evidence,
    }


def _page(items: List[Any], page: int, page_size: int) -> Dict[str, Any]:
    if page < 1 or page_size < 1:
        raise ValueError("page and page_size must be positive integers")
    total = len(items)
    start = (page - 1) * page_size
    return {
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": (total + page_size - 1) // page_size,
        "items": items[start:start + page_size],
    }
