"""Core data models for the release-package lineage diagnostic tool.

All node types are normalized into a single :class:`Node` structure so the
traversal layer never has to branch on raw JSON shapes.  Column references
carry explicit (node, column) source identity: same-named join keys and
shadowed columns are therefore never conflated.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------
class LineageError(Exception):
    """Base class for diagnostic-tool errors."""


class GraphValidationError(LineageError):
    """The declarative graph is structurally invalid."""


class ConsistencyError(LineageError):
    """Runtime / package / graph versions or summaries do not agree."""


class RepairError(LineageError):
    """Base class for local release-repair errors."""


class RepairValidationError(RepairError):
    """A repair graph or correction payload is outside the executable subset.

    Raised before any write transaction is opened (or rolled back when the
    problem is only detectable against stored state), so a failed repair
    never leaves partial changes behind.
    """


class RepairConflictError(RepairError):
    """The batch conflicts with the stored workspace state.

    Covers stale expected versions, swapped graphs, inserting an existing
    row and deleting/replacing a missing row.
    """


class TraceError(RepairError):
    """A single-row release-trace query was rejected.

    ``kind`` is a stable machine-readable tag so scripts can branch on it:

    * ``not_enabled``     - the workspace was not built with tracing
                            (``repair init --trace``); tracing is only
                            available on workspaces that opted in at build
                            time, never retrofitted onto existing ones;
    * ``invalid_version`` - the expected version is not a positive integer;
    * ``stale_version``   - the expected version is not the currently
                            committed version (no historical queries);
    * ``bad_row``         - the release row is not a JSON object;
    * ``missing_column``  - one or more release columns are absent;
    * ``extra_column``    - columns beyond the release schema were given;
    * ``type_mismatch``   - a value's type does not match the declared
                            release column type (nulls and booleans
                            included).

    A well-formed row that simply does not exist at the current version is
    NOT an error: the query succeeds and reports zero matched copies.
    """

    def __init__(self, message: str, kind: str = "bad_row"):
        super().__init__(message)
        self.kind = kind


class SubscriptionError(RepairError):
    """A release-change subscription operation was rejected.

    ``kind`` is a stable machine-readable tag so scripts can branch on it:

    * ``not_enabled``          - the workspace has no subscription;
    * ``already_enabled``      - enable attempted twice;
    * ``invalid_start``        - the from-version is not consumable (before
                                 enablement, ahead of the committed version,
                                 or otherwise malformed);
    * ``expired``              - the from-version slid out of the retention
                                 window; the consumer must re-fetch a full
                                 baseline and resubscribe from it;
    * ``cursor_invalid``       - the continuation token is undecodable,
                                 belongs to another subscription, or
                                 contradicts the requested start;
    * ``invalid_page_size``    - page size outside the supported range.
    """

    def __init__(self, message: str, kind: str = "invalid_start"):
        super().__init__(message)
        self.kind = kind


# ---------------------------------------------------------------------------
# Expressions and nodes
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Expr:
    """A column expression.

    ``kind`` is ``"ref"`` for a plain column reference (including bare
    renames such as ``o.uid AS uid_copy``) or ``"op"`` for an applied
    function.  ``refs`` always uses *fully qualified* column names, even when
    the original graph wrote them unqualified, so a reference keeps the
    identity of its producing node.
    """

    kind: str                      # "ref" | "op"
    op: Optional[str]              # function name when kind == "op"
    out_column: str                # column produced on this node
    refs: Tuple[str, ...]          # fully-qualified input columns
    raw: str                       # original expression text
    role: str = "value"            # "value" | "condition" | "join_on"


@dataclass(frozen=True)
class ColRef:
    """A fully qualified column identity ``node.column``."""

    node_id: str
    column: str

    @property
    def fq(self) -> str:
        return f"{self.node_id}.{self.column}"

    @staticmethod
    def parse(fq_name: str) -> "ColRef":
        if "." not in fq_name:
            raise GraphValidationError(
                f"column reference {fq_name!r} is not fully qualified "
                "(expected 'node_id.column')"
            )
        node_id, col = fq_name.split(".", 1)
        return ColRef(node_id, col)


@dataclass
class Node:
    """Normalized processing-graph node."""

    id: str
    type: str                      # input/project/branch/join/agg/explode/output
    table: Optional[str] = None    # input nodes / output package name
    exprs: List[Expr] = field(default_factory=list)
    # join / agg / explode helpers
    left: Optional[str] = None
    right: Optional[str] = None
    agg_input: Optional[str] = None
    explode_input: Optional[str] = None
    # output wiring
    output_of: Optional[str] = None
    extra_inputs: List[str] = field(default_factory=list)
    # branch wiring: logical upstream -> [(branch id, target node)]
    branches: Dict[str, List[Tuple[str, str]]] = field(default_factory=dict)
    raw: Dict[str, Any] = field(default_factory=dict)

    @property
    def output_columns(self) -> List[str]:
        return [e.out_column for e in self.exprs]

    def expr_for(self, column: str) -> Optional[Expr]:
        for e in self.exprs:
            if e.out_column == column:
                return e
        return None


@dataclass
class Graph:
    """A validated declarative processing graph."""

    graph_version: str
    nodes: Dict[str, Node]
    # Structural edges: upstream node -> downstream nodes (branch targets
    # included; branch nodes themselves are transparent for column lineage).
    adjacency: Dict[str, List[str]]

    def node(self, node_id: str) -> Node:
        try:
            return self.nodes[node_id]
        except KeyError as exc:
            raise GraphValidationError(f"unknown node {node_id!r}") from exc


# ---------------------------------------------------------------------------
# Runtime records and release package
# ---------------------------------------------------------------------------
@dataclass
class NodeRecord:
    """What one run observed at one node.

    ``pos`` / ``neg`` hold fully-qualified columns.  Absence of a column from
    both lists means *no record* -- never a proof that the column was unused.

    ``neg_refs`` is an edge-level negative: the node executed and produced
    the target column, but a specific input reference (e.g. one arm of a
    ``coalesce``) was not read.  It is the runtime counterpart of an
    individual static edge rather than of a whole target column.
    """

    node_id: str
    executed: bool
    branches_taken: Tuple[str, ...]
    branches_skipped: Tuple[str, ...]
    pos: Tuple[str, ...]
    neg: Tuple[str, ...]
    neg_refs: Tuple[str, ...] = ()
    has_record: bool = True


@dataclass
class RuntimeRecord:
    graph_version: str
    run_version: str
    package_digest: str
    nodes: Dict[str, NodeRecord]
    # output node id -> {column: sample summary digest}
    output_samples: Dict[str, Dict[str, str]]
    missing_nodes: List[str] = field(default_factory=list)


@dataclass
class PackageColumn:
    name: str
    type: str
    sample_digest: str


@dataclass
class ReleasePackage:
    package_name: str
    package_version: str
    graph_version: str
    run_version: str
    schema_digest: str
    output_node: str
    columns: List[PackageColumn]
