"""Public entry points for local release-table repair.

A workspace is a single SQLite database.  Two phases are supported:

* :func:`initialize_workspace` - build a fresh workspace from the validated
  graph and the initial source-table rows, materialize the first release and
  store version 1;
* :func:`apply_repair_batch` - open an existing workspace, validate one
  correction batch against the expected current version, recompute only the
  implicated partitions and atomically switch the release to the new
  version.

Reads never need a writer connection: :func:`read_release` returns the
committed release rows together with their version from one consistent
snapshot, and the database also exposes a ``release_rows`` view carrying the
version column for ad-hoc SQL readers (see ``docs/repair.md``).
"""

from __future__ import annotations

import json
import os
import sqlite3
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from .model import RepairError, RepairValidationError
from .repair_engine import (
    Correction,
    Failpoint,
    RepairBatch,
    RepairEngine,
    parse_batch,
)
from .repair_plan import RepairPlan, build_repair_plan
from .repair_store import AccessLedger, RepairStore

__all__ = [
    "initialize_workspace",
    "apply_repair_batch",
    "read_release",
    "load_repair_plan",
    "RepairWorkspace",
    "RepairPlan",
    "RepairBatch",
    "Correction",
    "RepairError",
]


def load_repair_plan(graph_payload: Mapping[str, Any]) -> RepairPlan:
    """Validate the executable repair subset of a graph payload."""
    return build_repair_plan(graph_payload)


def initialize_workspace(db_path: str,
                         graph_payload: Mapping[str, Any],
                         initial_payload: Mapping[str, Any]
                         ) -> Tuple[RepairPlan, int]:
    """Create *db_path*, load initial source rows and materialize version 1.

    ``initial_payload`` is either ``{"sources": {node_id: [rows]}}`` or the
    bare ``{node_id: [rows]}`` mapping.  Fails if the database file already
    contains a workspace; nothing is created on validation failure.
    """
    plan = build_repair_plan(graph_payload)
    sources = _extract_sources(initial_payload, plan)
    conn = sqlite3.connect(db_path)
    ledger = AccessLedger()
    try:
        store = RepairStore(conn, plan, ledger)
        engine = RepairEngine(store, plan)
        version = engine.initialize(sources)
    except Exception:
        conn.close()
        raise
    conn.close()
    return plan, version


def _open_workspace(db_path: str, plan: RepairPlan,
                    ledger: Optional[AccessLedger] = None
                    ) -> RepairStore:
    if not os.path.exists(db_path):
        raise RepairValidationError(
            f"workspace database {db_path!r} does not exist; run "
            "'repair init' first"
        )
    conn = sqlite3.connect(db_path)
    store = RepairStore(conn, plan, ledger or AccessLedger())
    has_meta = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='meta'"
    ).fetchone()
    if has_meta is None or store.get_meta("graph_version") is None:
        conn.close()
        raise RepairValidationError(
            f"{db_path!r} is not an initialized repair workspace"
        )
    return store


def apply_repair_batch(db_path: str,
                       graph_payload: Mapping[str, Any],
                       batch_payload: Mapping[str, Any],
                       failpoint: Failpoint = None,
                       ledger: Optional[AccessLedger] = None
                       ) -> Dict[str, Any]:
    """Atomically apply one correction batch; return the new version summary.

    The graph payload is re-validated and its identity compared with the one
    stored when the workspace was built.  On any error the transaction is
    rolled back and reopening the database still shows the pre-batch version.
    """
    plan = build_repair_plan(graph_payload)
    batch = parse_batch(batch_payload, plan)
    store = _open_workspace(db_path, plan, ledger)
    try:
        engine = RepairEngine(store, plan)
        return engine.apply_batch(batch, failpoint=failpoint)
    finally:
        store.conn.close()


def read_release(db_path: str,
                 graph_payload: Mapping[str, Any]
                 ) -> Tuple[int, List[Dict[str, Any]]]:
    """Return ``(version, release rows)`` from one consistent snapshot."""
    plan = build_repair_plan(graph_payload)
    store = _open_workspace(db_path, plan)
    try:
        return store.read_release()
    finally:
        store.conn.close()


class RepairWorkspace:
    """Long-lived handle for callers (and tests) keeping one connection open."""

    def __init__(self, db_path: str, plan: RepairPlan,
                 ledger: Optional[AccessLedger] = None,
                 require_initialized: bool = False):
        self.db_path = db_path
        self.plan = plan
        self.ledger = ledger or AccessLedger()
        self.conn = sqlite3.connect(db_path)
        self.store = RepairStore(self.conn, plan, self.ledger)
        if require_initialized:
            has_meta = self.conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' "
                "AND name='meta'").fetchone()
            if has_meta is None or \
                    self.store.get_meta("graph_version") is None:
                self.conn.close()
                raise RepairValidationError(
                    f"{db_path!r} is not an initialized repair workspace")
        self.engine = RepairEngine(self.store, plan)

    def initialize(self, initial_payload: Mapping[str, Any]) -> int:
        sources = _extract_sources(initial_payload, self.plan)
        return self.engine.initialize(sources)

    def apply(self, batch_payload: Mapping[str, Any],
              failpoint: Failpoint = None) -> Dict[str, Any]:
        batch = parse_batch(batch_payload, self.plan)
        return self.engine.apply_batch(batch, failpoint=failpoint)

    def read_release(self) -> Tuple[int, List[Dict[str, Any]]]:
        return self.store.read_release()

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> "RepairWorkspace":
        return self

    def __exit__(self, *exc) -> None:
        self.close()


def _extract_sources(payload: Mapping[str, Any],
                     plan: RepairPlan) -> Dict[str, Sequence[Mapping[str, Any]]]:
    if "sources" in payload:
        candidate = payload["sources"]
    else:
        candidate = payload
    if not isinstance(candidate, Mapping):
        from .model import RepairValidationError
        raise RepairValidationError(
            "initial payload must map input node ids to row lists"
        )
    unknown = [nid for nid in candidate if nid not in plan.inputs]
    if unknown:
        from .model import RepairValidationError
        raise RepairValidationError(
            f"initial payload names unknown input nodes: {sorted(unknown)}"
        )
    missing = [nid for nid in plan.inputs if nid not in candidate]
    if missing:
        from .model import RepairValidationError
        raise RepairValidationError(
            f"initial payload is missing source tables: {sorted(missing)}"
        )
    normalized: Dict[str, Sequence[Mapping[str, Any]]] = {}
    for nid in plan.inputs:
        rows = candidate[nid]
        if not isinstance(rows, list):
            from .model import RepairValidationError
            raise RepairValidationError(
                f"initial payload for {nid!r} must be a list of rows"
            )
        normalized[nid] = rows
    return normalized


def load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)
