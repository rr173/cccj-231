"""Single-row release tracing over a trace-enabled repair workspace.

A settlement clerk holds ONE complete published business row and wants to
know which original documents, pairings and aggregations actually produced
each physical copy of it.  Column-level diagnosis only names fields, and
the release-change subscription only reports copy-count differences; this
module answers the per-copy computation evidence instead.

Where the evidence lives
------------------------
Workspaces built with ``repair init --trace`` maintain, next to every
materialized node, a ``trace_<node>`` table recording for every
materialized row exactly which *immediate upstream instances* it was
computed from:

* project / output rows: one ``in`` entry naming the single upstream row;
* join rows: one ``left`` and one ``right`` entry, so the two sides stay
  distinguishable even when both trace back to the same source table;
* aggregate rows: one ``member`` entry per consumed input row, so the
  group key, ``SUM`` and ``COUNT(*)`` can be re-checked member by member.

The engine writes and deletes these entries row-by-row inside the same
transaction as the repair batch, so a committed version's evidence always
describes that version's actual computation, a rolled-back batch leaves no
evidence behind, and daily repairs never rebuild evidence graph-wide.

Query contract
--------------
* The row is matched against ALL release columns with their declared
  types (TEXT vs INTEGER is significant); a well-formed row that does not
  exist at the current version yields ``matched: 0`` -- it is not an
  error.
* ``expected_version`` must equal the currently committed version; there
  are no historical queries.  Missing/extra columns, mistyped values, a
  non-trace workspace or a stale version are rejected with
  :class:`TraceError` carrying a machine-readable ``kind``.
* The version, the matched release instances, the membership evidence and
  the source values are all read inside ONE read transaction, so a single
  answer always reflects a single committed state.
* The walk only visits the matched release instances and the instances
  actually reachable through their evidence: no unrelated partitions are
  scanned, the processing graph is not re-executed, and an instance shared
  by several consumers is delivered once (as a DAG node with several
  usage edges), never re-expanded per path.

Result shape (script-readable JSON)::

    {
      "version": 3,                 # committed version the evidence is from
      "output_node": "o_release",
      "row": {...},                 # the queried release row (echo)
      "matched": 2,                 # number of physical copies found
      "roots": ["e1", "e9"],        # one root evidence id per copy
      "instances": [                # DAG of every instance visited, once each
        {"id": "e1", "node": "o_release", "kind": "output",
         "values": {...},
         "uses": [{"role": "in", "ref": "e2"}]},
        {"id": "e2", "node": "j_target", "kind": "join",
         "values": {...},
         "uses": [{"role": "left", "ref": "e3"},
                  {"role": "right", "ref": "e4"}]},
        {"id": "e3", "node": "a_region", "kind": "agg",
         "values": {"region": "R2", "total_amt": 60, "n": 2},
         "uses": [{"role": "member", "ref": "e5"},
                  {"role": "member", "ref": "e6"}]},
        {"id": "e7", "node": "orders", "kind": "source",
         "row_id": "o3",
         "values": {"acc_id": "A", "region": "R2", "amount": 30}}
      ]
    }

Evidence ids (``e1``, ``e2``, ...) are local to one result: they exist so
a script can dereference ``uses[].ref`` within that answer.  They expose
no internal row numbers and are not stable across versions or queries.
"""

from __future__ import annotations

from collections import deque
from typing import Any, Dict, List, Mapping, Optional, Tuple

from .model import TraceError
from .repair_plan import RepairPlan
from .repair_store import RepairStore

# Instance keys inside the walker: ("rel", node_id, sid) for materialized
# rows, ("src", node_id, row_id) for source rows.
_InstanceKey = Tuple[str, str, Any]


def trace_row_in_store(store: RepairStore, plan: RepairPlan,
                       row: Any, expected_version: Any) -> Dict[str, Any]:
    """Trace every physical copy of one release row; see module docstring.

    ``store`` must be an open workspace whose graph identity has already
    been verified by the caller.
    """
    if not isinstance(expected_version, int) or \
            isinstance(expected_version, bool) or expected_version < 1:
        raise TraceError(
            f"expected_version must be a positive integer, got "
            f"{expected_version!r}",
            kind="invalid_version",
        )
    if not store.trace_enabled():
        raise TraceError(
            "this workspace was built without tracing; single-row tracing "
            "is only available on workspaces created with 'repair init "
            "--trace' (existing workspaces are not migrated)",
            kind="not_enabled",
        )
    out_node = plan.output_node_id
    out_cols = plan.columns[out_node]
    values = _validate_row(plan, row, out_cols)

    # One read transaction: the version check, the instance lookup, the
    # membership evidence and the source values all come from the same
    # committed snapshot, even if a repair publishes concurrently.
    store.conn.execute("BEGIN")
    try:
        version = int(store.get_meta("version"))
        if expected_version != version:
            raise TraceError(
                f"expected_version {expected_version} is not the current "
                f"committed version {version}; historical trace queries "
                "are not supported -- re-read the release at its current "
                "version and trace against that",
                kind="stale_version",
            )
        sids = store.find_release_instances(values)
        walker = _TraceWalker(store, plan)
        roots = [walker.walk(out_node, sid) for sid in sids]
        result: Dict[str, Any] = {
            "version": version,
            "output_node": out_node,
            "row": {c: values[c] for c in out_cols},
            "matched": len(roots),
            "roots": roots,
            "instances": walker.instances_json(),
        }
        store.conn.commit()
        return result
    except Exception:
        store.conn.rollback()
        raise


def _validate_row(plan: RepairPlan, row: Any,
                  out_cols: List[str]) -> Dict[str, Any]:
    """Shape- and type-check the queried release row (pure, no I/O)."""
    if not isinstance(row, Mapping):
        raise TraceError(
            f"the release row must be a JSON object mapping every release "
            f"column to its value, got {row!r}",
            kind="bad_row",
        )
    missing = [c for c in out_cols if c not in row]
    if missing:
        raise TraceError(
            f"the release row is missing column(s) {missing}; the full "
            f"published row {out_cols} is required for an exact match",
            kind="missing_column",
        )
    extra = [c for c in row if c not in set(out_cols)]
    if extra:
        raise TraceError(
            f"the release row carries unknown column(s) {extra}; the "
            f"release schema is exactly {out_cols}",
            kind="extra_column",
        )
    out_types = plan.types[plan.output_node_id]
    values: Dict[str, Any] = {}
    for col in out_cols:
        values[col] = _check_typed_value(col, out_types[col], row[col])
    return values


def _check_typed_value(col: str, ctype: str, val: Any) -> Any:
    if ctype == "INTEGER":
        if isinstance(val, bool) or not isinstance(val, int):
            raise TraceError(
                f"release column {col!r} is INTEGER but the queried value "
                f"{val!r} is not an integer",
                kind="type_mismatch",
            )
        return val
    if not isinstance(val, str):
        raise TraceError(
            f"release column {col!r} is TEXT but the queried value "
            f"{val!r} is not a string",
            kind="type_mismatch",
        )
    return val


class _TraceWalker:
    """Breadth-first DAG walk over the evidence of the matched copies.

    Every instance is expanded at most once: the first visit assigns its
    evidence id and enqueues it, later visits only add another usage edge
    pointing at the already-delivered id.  Shared upstream instances are
    therefore delivered exactly once no matter how many roots reach them.
    """

    def __init__(self, store: RepairStore, plan: RepairPlan):
        self.store = store
        self.plan = plan
        self._ids: Dict[_InstanceKey, str] = {}
        self._payloads: Dict[str, Dict[str, Any]] = {}
        self._order: List[str] = []
        self._queue: deque = deque()

    def walk(self, node_id: str, sid: int) -> str:
        """Walk one matched release instance; return its evidence id."""
        key = ("rel", node_id, sid)
        self._assign(key)
        self._drain()
        return self._ids[key]

    def instances_json(self) -> List[Dict[str, Any]]:
        return [self._payloads[eid] for eid in self._order]

    # ------------------------------------------------------------------
    def _assign(self, key: _InstanceKey) -> None:
        if key in self._ids:
            return
        eid = f"e{len(self._order) + 1}"
        self._ids[key] = eid
        self._order.append(eid)
        self._queue.append(key)

    def _drain(self) -> None:
        while self._queue:
            key = self._queue.popleft()
            if key[0] == "rel":
                self._expand_rel(key)
            else:
                self._expand_src(key)

    def _expand_rel(self, key: _InstanceKey) -> None:
        _, node_id, sid = key
        rows = self.store.get_rel_rows_by_sids(node_id, [sid])
        if not rows:
            raise TraceError(
                f"trace evidence references a {node_id!r} instance that no "
                "longer exists; the workspace state is inconsistent",
                kind="internal",
            )
        values = rows[0][1]
        uses = []
        for role, up_node, up_sid, up_row_id in self.store.get_trace(
                node_id, sid):
            if up_sid is not None:
                up_key: _InstanceKey = ("rel", up_node, up_sid)
            else:
                up_key = ("src", up_node, up_row_id)
            self._assign(up_key)
            uses.append({"role": role, "ref": self._ids[up_key]})
        if node_id in self.plan.joins:
            kind = "join"
        elif node_id in self.plan.aggs:
            kind = "agg"
        elif node_id == self.plan.output_node_id:
            kind = "output"
        else:
            kind = "project"
        self._payloads[self._ids[key]] = {
            "id": self._ids[key],
            "node": node_id,
            "kind": kind,
            "values": values,
            "uses": uses,
        }

    def _expand_src(self, key: _InstanceKey) -> None:
        _, node_id, row_id = key
        values = self.store.get_source_row(node_id, row_id)
        if values is None:
            raise TraceError(
                f"trace evidence references source row "
                f"{node_id}.{row_id} which no longer exists; the "
                "workspace state is inconsistent",
                kind="internal",
            )
        self._payloads[self._ids[key]] = {
            "id": self._ids[key],
            "node": node_id,
            "kind": "source",
            "row_id": row_id,
            "values": values,
        }
