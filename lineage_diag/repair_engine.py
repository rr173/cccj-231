"""Local release-table repair engine.

The engine implements two entry points over a single SQLite workspace:

* :meth:`RepairEngine.initialize` - materialize the *initial* release by
  fully executing the validated sub-graph once;
* :meth:`RepairEngine.apply_batch` - apply one correction batch against an
  expected current version and recompute only the implicated partitions.

Why the recompute scope is keyed, not column-derived
----------------------------------------------------
A correction can change a value that only appears in a join condition or a
group-by key -- value-only column lineage would miss it.  The engine
therefore tracks every corrected source row id, then at each node
recomputes by *old and new* key/group membership:

* project/output: the exact rows carrying affected provenance are rebuilt;
* join: every equi-key present on either side before or after the batch is
  recomputed wholesale from the corrected both-side relations, so a key
  moving A -> B withdraws the old A pairings and builds B pairings without
  ever mixing old and new side data;
* agg: every old and new group partition is re-aggregated from its full
  corrected membership, and the group row is removed when the last
  contributing row leaves.

Reads are always scoped to affected keys/groups via indexed accessors
(:mod:`lineage_diag.repair_store` records every access for verification).
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

from .model import (
    ColRef,
    RepairConflictError,
    RepairValidationError,
)
from .repair_plan import (
    AggSpec,
    JoinSpec,
    Projection,
    RepairPlan,
)
from .repair_store import (
    INITIAL_VERSION,
    ReleaseDeltaTracker,
    RepairStore,
    canonical_gkey,
    rel_table,
    source_table,
)

_VALID_OPS = ("insert", "delete", "replace")
Provenance = Dict[Tuple[str, str], int]
# A row flowing inside the engine carries, besides its business values and
# its source-row provenance, a *token* naming the immediate upstream
# instance it was computed from:
#   ("src", row_id)  - a source row of an input node
#   ("rel", sid)     - a materialized row of a relation node
# Tokens cost nothing when tracing is disabled and let the emitters record
# instance-level evidence for the exact rows they write.
RowToken = Tuple[str, Any]


# ---------------------------------------------------------------------------
# Batch model
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Correction:
    node_id: str
    row_id: str
    op: str                       # insert | delete | replace
    values: Optional[Dict[str, Any]]


@dataclass
class RepairBatch:
    expected_version: int
    corrections: List[Correction]


@dataclass
class NodeChanges:
    """Old/new surrogate rows and keys observed at one node in this batch."""

    old_sids: List[int] = field(default_factory=list)
    new_sids: List[int] = field(default_factory=list)
    old_keys: List[Any] = field(default_factory=list)
    new_keys: List[Any] = field(default_factory=list)

    def all_sids(self) -> set:
        return set(self.old_sids) | set(self.new_sids)


# Optional test hook: invoked after each node's writes, before COMMIT.
Failpoint = Optional[Callable[[str], None]]


def parse_batch(payload: Mapping[str, Any],
                plan: RepairPlan) -> RepairBatch:
    if "expected_version" not in payload:
        raise RepairValidationError(
            "batch must bind an integer 'expected_version'"
        )
    expected = payload["expected_version"]
    if not isinstance(expected, int) or isinstance(expected, bool) \
            or expected < 1:
        raise RepairValidationError(
            f"expected_version must be a positive integer, got {expected!r}"
        )
    raw_corrections = payload.get("corrections")
    if not isinstance(raw_corrections, list) or not raw_corrections:
        raise RepairValidationError(
            "batch must contain a non-empty 'corrections' list"
        )

    corrections: List[Correction] = []
    seen: set = set()
    for idx, item in enumerate(raw_corrections):
        node_id = item.get("node_id")
        row_id = item.get("row_id")
        op = item.get("op")
        if node_id not in plan.inputs:
            raise RepairValidationError(
                f"correction #{idx}: node_id {node_id!r} is not an input "
                f"node of this graph {list(plan.inputs)}"
            )
        if not isinstance(row_id, str) or not row_id:
            raise RepairValidationError(
                f"correction #{idx}: row_id must be a non-empty string"
            )
        if op not in _VALID_OPS:
            raise RepairValidationError(
                f"correction #{idx}: op must be one of {_VALID_OPS}, "
                f"got {op!r}"
            )
        key = (node_id, row_id)
        if key in seen:
            raise RepairValidationError(
                f"correction #{idx}: row {node_id}.{row_id} appears more "
                "than once in the batch; each row may appear at most once"
            )
        seen.add(key)

        values = item.get("values")
        if op in ("insert", "replace"):
            values = _validate_values(plan, node_id, values, idx)
        elif values is not None:
            raise RepairValidationError(
                f"correction #{idx}: delete must not carry values"
            )
        corrections.append(Correction(node_id, row_id, op, values))
    return RepairBatch(expected_version=expected, corrections=corrections)


def _validate_values(plan: RepairPlan, node_id: str,
                     values: Any, idx: int) -> Dict[str, Any]:
    if not isinstance(values, Mapping):
        raise RepairValidationError(
            f"correction #{idx}: {node_id} values must be an object"
        )
    expected_cols = plan.columns[node_id]
    if set(values) != set(expected_cols):
        raise RepairValidationError(
            f"correction #{idx}: {node_id} values must provide exactly the "
            f"business columns {expected_cols}, got {sorted(values)}"
        )
    return {
        col: _check_scalar(node_id, col, plan.types[node_id][col],
                           values[col])
        for col in expected_cols
    }


def _check_scalar(node_id: str, col: str, ctype: str, val: Any) -> Any:
    if val is None:
        raise RepairValidationError(
            f"{node_id}.{col}: repair values must be non-null"
        )
    if ctype == "INTEGER":
        if isinstance(val, bool) or not isinstance(val, int):
            raise RepairValidationError(
                f"{node_id}.{col}: expected an integer, got {val!r}"
            )
        return val
    if not isinstance(val, str):
        raise RepairValidationError(
            f"{node_id}.{col}: expected a string, got {val!r}"
        )
    return val


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------
class RepairEngine:
    def __init__(self, store: RepairStore, plan: RepairPlan):
        self.store = store
        self.plan = plan
        self.conn = store.conn
        self._batch_changes: Optional[Dict[str, NodeChanges]] = None
        # Set for the duration of apply_batch when the workspace has a
        # release-change subscription; see _recompute_project_like.
        self._release_tracker: Optional[ReleaseDeltaTracker] = None

    # ------------------------------------------------------------------
    # Initial full materialization
    # ------------------------------------------------------------------
    def initialize(self,
                   source_rows: Mapping[str, Sequence[Mapping[str, Any]]],
                   enable_trace: bool = False
                   ) -> int:
        self._check_uninitialized()
        missing = [nid for nid in self.plan.inputs if nid not in source_rows]
        if missing:
            raise RepairValidationError(
                f"initial payload is missing source tables: {sorted(missing)}"
            )
        unknown = [nid for nid in source_rows if nid not in self.plan.inputs]
        if unknown:
            raise RepairValidationError(
                f"initial payload names unknown input nodes: {sorted(unknown)}"
            )

        try:
            self.conn.execute("BEGIN IMMEDIATE")
            self.store.create_schema(enable_trace=enable_trace)
            self.store.init_meta()
            for nid in self.plan.inputs:
                rid_col = self.plan.row_ids[nid]
                seen: set = set()
                for idx, row in enumerate(source_rows[nid]):
                    if not isinstance(row, Mapping) or rid_col not in row:
                        raise RepairValidationError(
                            f"initial {nid} row #{idx}: missing row id "
                            f"{rid_col!r}"
                        )
                    row_id = row[rid_col]
                    if not isinstance(row_id, str) or not row_id:
                        raise RepairValidationError(
                            f"initial {nid} row #{idx}: row id must be a "
                            "non-empty string"
                        )
                    if row_id in seen:
                        raise RepairValidationError(
                            f"initial {nid}: duplicate row id {row_id!r}; "
                            "row ids must be unique within the table"
                        )
                    seen.add(row_id)
                    values = _validate_values(
                        self.plan, nid,
                        {c: row.get(c) for c in self.plan.columns[nid]}, idx
                    )
                    self.store.insert_source(nid, row_id, values)
            for nid in self.plan.closure:
                if nid not in self.plan.inputs:
                    self._full_compute(nid)
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        return INITIAL_VERSION

    def _check_uninitialized(self) -> None:
        row = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='meta'"
        ).fetchone()
        if row is not None:
            raise RepairValidationError(
                "workspace already initialized; use a correction batch to "
                "change data"
            )

    # ------------------------------------------------------------------
    # Batch application
    # ------------------------------------------------------------------
    def apply_batch(self, batch: RepairBatch,
                    failpoint: Failpoint = None) -> Dict[str, Any]:
        self.verify_graph_identity()
        try:
            self.conn.execute("BEGIN IMMEDIATE")
            current = int(self.store.get_meta("version"))
            if current != batch.expected_version:
                raise RepairConflictError(
                    f"stale expected_version {batch.expected_version}; "
                    f"workspace is at version {current}"
                )
            # When a subscription is enabled, accumulate the release-row
            # copy difference from the change set this batch already
            # produces -- no extra release-table scan is needed.
            sub_cfg = self.store.subscription_config()
            if sub_cfg is not None:
                self._release_tracker = ReleaseDeltaTracker(
                    self.plan.columns[self.plan.output_node_id])
            changes: Dict[str, NodeChanges] = {
                nid: NodeChanges() for nid in self.plan.closure
            }
            self._batch_changes = changes
            taint = self._base_taint(batch)
            self._apply_source_corrections(batch, changes)
            for nid in self.plan.closure:
                if nid in self.plan.inputs:
                    continue
                # Topological order guarantees every upstream's recompute is
                # visible through ``changes`` when this node runs, so a join
                # can see both corrected sides even when its stale stored
                # rows still hold pre-batch values.
                self._recompute_node(nid, taint, changes)
                if failpoint is not None:
                    failpoint(nid)
            self._batch_changes = None
            new_version = current + 1
            tracker, self._release_tracker = self._release_tracker, None
            if tracker is not None:
                # New deltas, the version bump and the retention boundary
                # commit together; a rollback therefore never publishes
                # partial changes nor expires old start points early.
                self.store.write_subscription_deltas(
                    new_version, tracker.nonzero_items())
                self.store.gc_subscription(new_version - sub_cfg.retention)
            self.store.set_version(new_version)
            self.conn.commit()
        except Exception:
            self._release_tracker = None
            self.conn.rollback()
            raise
        return {
            "version": new_version,
            "corrections": len(batch.corrections),
            "recomputed_nodes": [
                nid for nid in self.plan.closure
                if nid not in self.plan.inputs and
                (changes[nid].old_sids or changes[nid].new_sids or
                 changes[nid].old_keys or changes[nid].new_keys)
            ],
        }

    def verify_graph_identity(self) -> None:
        stored_version = self.store.get_meta("graph_version")
        stored_digest = self.store.get_meta("graph_digest")
        if stored_version is None:
            raise RepairValidationError(
                "workspace database is not a repair workspace (no meta)"
            )
        if stored_version != self.plan.graph_version or \
                stored_digest != self.plan.graph_digest:
            raise RepairConflictError(
                "graph identity mismatch: this workspace was built from a "
                f"different processing graph (stored {stored_version!r}, "
                f"batch graph {self.plan.graph_version!r})"
            )

    @staticmethod
    def _base_taint(batch: RepairBatch) -> Dict[str, set]:
        taint: Dict[str, set] = {}
        for c in batch.corrections:
            taint.setdefault(c.node_id, set()).add(c.row_id)
        return taint

    def _taint_for(self, input_id: str, taint: Mapping[str, set]) -> set:
        return set(taint.get(input_id, ()))

    def _apply_source_corrections(self, batch: RepairBatch,
                                  changes: Dict[str, NodeChanges]) -> None:
        for c in batch.corrections:
            existing = self.store.get_source_row(c.node_id, c.row_id)
            if c.op == "insert":
                if existing is not None:
                    raise RepairConflictError(
                        f"insert rejected: {c.node_id}.{c.row_id} already "
                        "exists"
                    )
                self.store.insert_source(c.node_id, c.row_id, c.values)
            elif c.op == "delete":
                if existing is None:
                    raise RepairConflictError(
                        f"delete rejected: {c.node_id}.{c.row_id} does not "
                        "exist"
                    )
                self.store.delete_source(c.node_id, c.row_id)
            else:  # replace
                if existing is None:
                    raise RepairConflictError(
                        f"replace rejected: {c.node_id}.{c.row_id} does not "
                        "exist"
                    )
                self.store.delete_source(c.node_id, c.row_id)
                self.store.insert_source(c.node_id, c.row_id, c.values)

    # ------------------------------------------------------------------
    # Full (initial) computation
    # ------------------------------------------------------------------
    def _full_compute(self, nid: str) -> None:
        if nid in self.plan.joins:
            self._full_join(nid)
        elif nid in self.plan.aggs:
            self._full_agg(nid)
        else:
            self._full_project_like(nid)

    def _full_project_like(self, nid: str) -> None:
        upstream_id = self.plan.upstreams[nid][0]
        projs = self.plan.projections[nid]
        for token, values, prov in self._all_rows(upstream_id):
            out = self._project(projs, values)
            sid = self.store.insert_rel(nid, out)
            self.store.insert_prov(nid, sid, dict(prov))
            self._trace_insert(nid, sid, [("in", upstream_id, token)])

    def _full_join(self, nid: str) -> None:
        spec = self.plan.joins[nid]
        l_key = ColRef.parse(spec.left_key).column
        r_key = ColRef.parse(spec.right_key).column
        right_index: Dict[str, List[Tuple[RowToken, Dict[str, Any],
                                          Provenance]]] = {}
        for token, values, prov in self._all_rows(spec.right):
            pkey = canonical_gkey([values[r_key]])
            right_index.setdefault(pkey, []).append((token, values, prov))
        for l_token, l_values, l_prov in self._all_rows(spec.left):
            pkey = canonical_gkey([l_values[l_key]])
            for r_token, r_values, r_prov in right_index.get(pkey, ()):
                self._emit_join_row(nid, spec, l_values, r_values,
                                    l_prov, r_prov, pkey, l_token, r_token)

    def _full_agg(self, nid: str) -> None:
        spec = self.plan.aggs[nid]
        g_cols = [ColRef.parse(g).column for g in spec.group_by]
        groups: Dict[str, List[Tuple[RowToken, Dict[str, Any],
                                     Provenance]]] = {}
        for token, values, prov in self._all_rows(spec.input_node):
            gkey = canonical_gkey([values[c] for c in g_cols])
            groups.setdefault(gkey, []).append((token, values, prov))
        for gkey, members in groups.items():
            self._emit_agg_group(nid, spec, gkey, members)

    # ------------------------------------------------------------------
    # Incremental recomputation
    # ------------------------------------------------------------------
    def _recompute_node(self, nid: str, taint: Dict[str, set],
                        changes: Dict[str, NodeChanges]) -> None:
        if nid in self.plan.joins:
            self._recompute_join(nid, taint, changes)
        elif nid in self.plan.aggs:
            self._recompute_agg(nid, taint, changes)
        else:
            self._recompute_project_like(nid, taint, changes)

    def _recompute_project_like(self, nid: str, taint: Dict[str, set],
                                changes: Dict[str, NodeChanges]) -> None:
        upstream_id = self.plan.upstreams[nid][0]
        affected_sids = self._affected_sids(nid, taint, changes)
        # A directly-fed node is also dirty when its input source itself was
        # corrected, even when the correction is a pure insert that had no
        # pre-existing surrogate row.
        source_dirty = upstream_id in self.plan.inputs and bool(
            self._taint_for(upstream_id, taint))
        # And when an immediate upstream relation was rewritten in this batch
        # (new surrogate rows), even if no row of this node existed yet.
        upstream_dirty = upstream_id not in self.plan.inputs and bool(
            changes[upstream_id].all_sids())
        if not affected_sids and not source_dirty and not upstream_dirty:
            return
        track = self._release_tracker is not None and \
            nid == self.plan.output_node_id
        if track and affected_sids:
            # Capture the withdrawing release rows before deletion; the read
            # is scoped to exactly the sids this batch already touches.
            for _, old_values in self.store.get_rel_rows_by_sids(
                    nid, sorted(affected_sids)):
                self._release_tracker.record(old_values, -1)
        self.store.delete_rel_sids(nid, sorted(affected_sids))
        self.store.delete_prov_sids(nid, sorted(affected_sids))
        changes[nid].old_sids.extend(sorted(affected_sids))

        # Rebuild one row per *current* upstream row that the deleted rows
        # were derived from.  Upstream provenance is read from the upstream
        # node itself rather than reconstructed from this node's copies:
        # copies encode downstream fan-out (many-to-many joins, duplicate
        # rows) and must never inflate the row count of an upstream project.
        if upstream_id in self.plan.inputs:
            # Every corrected source row feeding this node must be rebuilt:
            # replaced rows no longer appear among the deleted sids' current
            # rows, and inserted rows had no sid at all.  Deleted rows simply
            # return nothing from get_source_row and are skipped.
            wanted = self._source_ids_from_sids(
                nid, sorted(affected_sids), upstream_id)
            wanted = sorted(set(wanted) |
                            self._taint_for(upstream_id, taint))
            for rid in wanted:
                values = self.store.get_source_row(upstream_id, rid)
                if values is None:
                    continue
                out = self._project(self.plan.projections[nid], values)
                if track:
                    self._release_tracker.record(out, +1)
                new_sid = self.store.insert_rel(nid, out)
                self.store.insert_prov(nid, new_sid,
                                       {(upstream_id, rid): 1})
                self._trace_insert(nid, new_sid,
                                   [("in", upstream_id, ("src", rid))])
                changes[nid].new_sids.append(new_sid)
        else:
            upstream_sids = self._upstream_sids_for_rebuild(
                upstream_id, taint, changes)
            if not upstream_sids:
                return
            members = self.store.get_rel_rows_by_sids(
                upstream_id, sorted(upstream_sids))
            up_prov = self.store.get_provenance(
                upstream_id, sorted(upstream_sids))
            for up_sid, values in members:
                out = self._project(self.plan.projections[nid], values)
                if track:
                    self._release_tracker.record(out, +1)
                new_sid = self.store.insert_rel(nid, out)
                self.store.insert_prov(nid, new_sid,
                                       dict(up_prov[up_sid]))
                self._trace_insert(nid, new_sid,
                                   [("in", upstream_id, ("rel", up_sid))])
                changes[nid].new_sids.append(new_sid)

    def _upstream_sids_for_rebuild(self, upstream_id: str,
                                   taint: Dict[str, set],
                                   changes: Dict[str, NodeChanges]) -> set:
        """Current upstream rows matching the deletion source set.

        A project-like node deletes its rows whose provenance shares any
        source row with a corrected input or with a changed upstream row
        (:meth:`_affected_sids`).  Because a project copies provenance
        verbatim, downstream row D is derived from upstream row U exactly
        when ``prov(D) == prov(U)``; the rebuild must therefore revisit
        every *current* upstream row matching that same source set.
        Otherwise a downstream row deleted only through provenance overlap
        (e.g. two release rows sharing one account source row) would
        vanish without being re-emitted.  Lookups go through the
        provenance index, so the scope stays keyed.
        """
        sids = set(self._tainted_relation_sids(upstream_id, taint))
        changed = changes[upstream_id].all_sids()
        if changed:
            prov_map = self.store.get_provenance(
                upstream_id, sorted(changed))
            shared_sources: set = set()
            for prov in prov_map.values():
                shared_sources.update(prov.keys())
            for (src, rid) in sorted(shared_sources):
                sids.update(self.store.find_sids_by_provenance(
                    upstream_id, src, [rid]))
        # New upstream sids are already reached through their provenance;
        # including the changed set is harmless (deleted sids simply
        # return no current row).
        sids |= changed
        return sids

    def _affected_sids(self, nid: str, taint: Dict[str, set],
                       changes: Dict[str, NodeChanges]) -> set:
        """Rows of nid implicated by source taint OR changed upstreams.

        Source-row provenance catches rows directly derived from corrected
        inputs; the changed-upstream pass catches rows whose immediate
        upstream surrogate was deleted/re-emitted (their stored provenance
        only names source rows, which alone cannot identify every stale
        downstream row).
        """
        sids = set(self._tainted_relation_sids(nid, taint))
        for upstream_id in self.plan.upstreams[nid]:
            if upstream_id in self.plan.inputs:
                continue
            changed = changes[upstream_id].all_sids()
            if not changed:
                continue
            prov_map = self.store.get_provenance(
                upstream_id, sorted(changed))
            # nid rows whose provenance shares any source row with a changed
            # upstream row.
            shared_sources: set = set()
            for prov in prov_map.values():
                shared_sources.update(prov.keys())
            for (src, rid) in shared_sources:
                sids.update(self.store.find_sids_by_provenance(
                    nid, src, [rid]))
        return sids

    def _source_ids_from_sids(self, nid: str, sids: Sequence[int],
                              input_id: str) -> List[str]:
        """Distinct row ids of input_id carried by nid's rows sids."""
        prov_map = self.store.get_provenance(nid, sids)
        wanted = {
            rid
            for prov in prov_map.values()
            for (src, rid) in prov if src == input_id
        }
        return sorted(wanted)

    def _recompute_join(self, nid: str, taint: Dict[str, set],
                        changes: Dict[str, NodeChanges]) -> None:
        spec = self.plan.joins[nid]
        l_key = ColRef.parse(spec.left_key).column
        r_key = ColRef.parse(spec.right_key).column
        # The join output may rename the key; old rows carry the output name.
        out_l_key = self._join_output_key_column(nid, spec.left)

        # Key universe derived from the corrected upstream rows on both
        # sides.  Old partitions are located by partition KEY rather than by
        # provenance sids: an aggregate may re-group into a fresh surrogate,
        # leaving a stale downstream join row whose provenance points at the
        # old surrogate, which provenance lookups can no longer see.
        old_pkeys: set = set()
        new_pkeys: set = set()
        tainted_sids = self._affected_sids(nid, taint, changes)
        if tainted_sids:
            for _, values in self.store.get_rel_rows_by_sids(
                    nid, sorted(tainted_sids)):
                old_pkeys.add(
                    canonical_gkey([values[out_l_key]]))
        for side, key_col in ((spec.left, l_key), (spec.right, r_key)):
            for values, _ in self._tainted_value_rows(side, taint):
                new_pkeys.add(
                    canonical_gkey([values[key_col]]))
        affected_pkeys = old_pkeys | new_pkeys
        if not affected_pkeys:
            return

        # Every currently stored partition carrying an affected key is
        # withdrawn, including rows whose upstream surrogate was replaced.
        stored_old = self._stored_partition_keys(nid, "_jkey",
                                                  affected_pkeys)
        changes[nid].old_keys.extend(sorted(stored_old))
        for pkey in stored_old:
            removed = self.store.delete_partition(nid, "_jkey", pkey)
            changes[nid].old_sids.extend(removed)
        for pkey in affected_pkeys:
            key_value = json.loads(pkey)[0]
            # Rebuild from BOTH corrected sides -- never old/new mix.
            left_rows = self._scoped_value_rows(spec.left, l_key, key_value)
            right_rows = self._scoped_value_rows(
                spec.right, r_key, key_value)
            for l_token, l_values, l_prov in left_rows:
                for r_token, r_values, r_prov in right_rows:
                    new_sid = self._emit_join_row(
                        nid, spec, l_values, r_values,
                        l_prov, r_prov, pkey, l_token, r_token)
                    changes[nid].new_sids.append(new_sid)
                    changes[nid].new_keys.append(pkey)

    def _join_output_key_column(self, nid: str, side: str) -> str:
        """Find the join output column carrying side's equi-key value.

        The plan validator guarantees every join key is also projected, so
        this lookup always succeeds for a validated graph.
        """
        spec = self.plan.joins[nid]
        key_fq = spec.left_key if side == spec.left else spec.right_key
        for p in self.plan.projections[nid]:
            if p.source.fq == key_fq:
                return p.out_column
        raise KeyError(key_fq)

    def _stored_partition_keys(self, nid: str, key_column: str,
                               candidate_keys: set) -> set:
        """Which of candidate_keys currently have any stored partition row.

        The query is a single indexed ``key IN (...)`` predicate; unrelated
        keys are never scanned.
        """
        keys = sorted(candidate_keys)
        if not keys:
            return set()
        table = rel_table(nid)
        qmarks = ", ".join("?" for _ in keys)
        rows = self.conn.execute(
            f'SELECT DISTINCT {key_column} FROM "{table}" '
            f'WHERE {key_column} IN ({qmarks})',
            keys,
        ).fetchall()
        for key in keys:
            self.store.ledger.record(
                "read_rel", table, f"probe:{key_column}={key}")
        return {row[0] for row in rows}

    def _recompute_agg(self, nid: str, taint: Dict[str, set],
                       changes: Dict[str, NodeChanges]) -> None:
        spec = self.plan.aggs[nid]
        g_cols = [ColRef.parse(g).column for g in spec.group_by]

        old_gkeys: set = set()
        new_gkeys: set = set()
        tainted_sids = self._affected_sids(nid, taint, changes)
        if tainted_sids:
            for _, values in self.store.get_rel_rows_by_sids(
                    nid, sorted(tainted_sids)):
                old_gkeys.add(
                    canonical_gkey([values[c] for c in g_cols]))
        for values, _ in self._tainted_value_rows(spec.input_node, taint):
            new_gkeys.add(
                canonical_gkey([values[c] for c in g_cols]))
        affected_gkeys = old_gkeys | new_gkeys
        if not affected_gkeys:
            return

        # Locate old group partitions by key so a group re-emitted under a
        # fresh surrogate still withdraws its stale row.
        stored_old = self._stored_partition_keys(nid, "_gkey",
                                                  affected_gkeys)
        changes[nid].old_keys.extend(sorted(stored_old))
        for gkey in stored_old:
            removed = self.store.delete_partition(nid, "_gkey", gkey)
            changes[nid].old_sids.extend(removed)
        for gkey in affected_gkeys:
            members = self._scoped_group_members(spec.input_node, g_cols,
                                                 gkey)
            if not members:
                # Last contributing row left: the group stays removed.
                continue
            new_sid = self._emit_agg_group(nid, spec, gkey, members)
            changes[nid].new_sids.append(new_sid)
            changes[nid].new_keys.append(gkey)

    # ------------------------------------------------------------------
    # Scoped row reads
    # ------------------------------------------------------------------
    def _scoped_value_rows(self, node_id: str, key_col: str,
                           key_value: Any
                           ) -> List[Tuple[RowToken, Dict[str, Any],
                                           Provenance]]:
        """All current rows of node_id with key_col == key_value, with prov."""
        if node_id in self.plan.inputs:
            return self._scoped_source_rows(node_id, key_col, key_value)
        if node_id in self.plan.joins:
            # Nested join: the physical partition stores the canonical key.
            pkey = canonical_gkey([key_value])
            members = self.store.get_rel_partition(
                node_id, "_jkey", pkey)
        else:
            members = self._scoped_rel_by_business_key(
                node_id, key_col, key_value)
        return self._attach_provenance(node_id, members)

    def _scoped_group_members(
        self, node_id: str, g_cols: Sequence[str], gkey: str
    ) -> List[Tuple[RowToken, Dict[str, Any], Provenance]]:
        gvals = json.loads(gkey)
        if node_id in self.plan.inputs:
            return self._scoped_source_group(node_id, g_cols, gvals, gkey)
        # Locate members by their business group columns, not by the physical
        # _gkey: an upstream aggregate may partition on a different number of
        # keys (e.g. two-key agg feeding a one-key agg), so the canonical
        # key encodings do not line up.  The business columns are indexed.
        members = self._scoped_rel_by_group(node_id, g_cols, gvals, gkey)
        return self._attach_provenance(node_id, members)

    def _scoped_source_rows(self, node_id: str, key_col: str,
                            key_value: Any
                            ) -> List[Tuple[RowToken, Dict[str, Any],
                                            Provenance]]:
        rid_col = self.plan.row_ids[node_id]
        table = source_table(node_id)
        rows = self.conn.execute(
            f'SELECT * FROM "{table}" WHERE "{key_col}" = ?',
            (key_value,),
        ).fetchall()
        self.store.ledger.record(
            "read_source", table, f"{key_col}={key_value}")
        out = []
        for row in rows:
            values = {k: row[k] for k in row.keys() if k != rid_col}
            out.append((("src", row[rid_col]), values,
                        {(node_id, row[rid_col]): 1}))
        return out

    def _scoped_source_group(self, node_id: str, g_cols: Sequence[str],
                             gvals: Sequence[Any], gkey: str
                             ) -> List[Tuple[RowToken, Dict[str, Any],
                                             Provenance]]:
        rid_col = self.plan.row_ids[node_id]
        table = source_table(node_id)
        where = " AND ".join(f'"{c}" = ?' for c in g_cols)
        rows = self.conn.execute(
            f'SELECT * FROM "{table}" WHERE {where}', list(gvals)
        ).fetchall()
        self.store.ledger.record("read_source", table, f"group={gkey}")
        return [
            (("src", row[rid_col]),
             {k: row[k] for k in row.keys() if k != rid_col},
             {(node_id, row[rid_col]): 1})
            for row in rows
        ]

    def _scoped_rel_by_business_key(
        self, node_id: str, key_col: str, key_value: Any
    ) -> List[Tuple[int, Dict[str, Any]]]:
        table = rel_table(node_id)
        rows = self.conn.execute(
            f'SELECT _sid, * FROM "{table}" WHERE "{key_col}" = ?',
            (key_value,),
        ).fetchall()
        self.store.ledger.record(
            "read_rel", table, f"{key_col}={key_value}")
        return [
            (int(row["_sid"]),
             {k: row[k] for k in row.keys() if not k.startswith("_")})
            for row in rows
        ]

    def _scoped_rel_by_group(
        self, node_id: str, g_cols: Sequence[str], gvals: Sequence[Any],
        gkey: str
    ) -> List[Tuple[int, Dict[str, Any]]]:
        table = rel_table(node_id)
        where = " AND ".join(f'"{c}" = ?' for c in g_cols)
        rows = self.conn.execute(
            f'SELECT _sid, * FROM "{table}" WHERE {where}', list(gvals)
        ).fetchall()
        self.store.ledger.record("read_rel", table, f"group={gkey}")
        return [
            (int(row["_sid"]),
             {k: row[k] for k in row.keys() if not k.startswith("_")})
            for row in rows
        ]

    def _attach_provenance(self, node_id: str, members):
        """Pair (sid, values) members with provenance; keep the sid token."""
        if not members:
            return []
        sids = [sid for sid, _ in members]
        prov_map = self.store.get_provenance(node_id, sids)
        return [(("rel", sid), values, prov_map[sid])
                for sid, values in members]

    # ------------------------------------------------------------------
    # Taint helpers
    # ------------------------------------------------------------------
    def _tainted_relation_sids(self, nid: str,
                               taint: Mapping[str, set]) -> List[int]:
        sids: set = set()
        for input_id in self.plan.inputs:
            ids = self._taint_for(input_id, taint)
            if ids:
                sids.update(self.store.find_sids_by_provenance(
                    nid, input_id, sorted(ids)))
        return sorted(sids)

    def _tainted_value_rows(self, node_id: str,
                            taint: Mapping[str, set]
                            ) -> List[Tuple[Dict[str, Any], Provenance]]:
        """Current corrected rows of node_id that this batch may have changed.

        Two sources are unioned:

        * rows carrying tainted source provenance (the original corrections);
        * rows whose surrogate was freshly emitted by an upstream recompute
          in this same batch (propagation through joins/aggregates whose
          stored rows were still stale at inspection time).
        """
        if node_id in self.plan.inputs:
            out = []
            for rid in sorted(self._taint_for(node_id, taint)):
                values = self.store.get_source_row(node_id, rid)
                if values is not None:
                    out.append((values, {(node_id, rid): 1}))
            return out
        sids = set(self._tainted_relation_sids(node_id, taint))
        sids |= self._changed_sids_for(node_id)
        members = self.store.get_rel_rows_by_sids(node_id, sorted(sids))
        # Only values + provenance are needed for key collection; the
        # instance token carried by _attach_provenance is dropped here.
        return [(values, prov)
                for _, values, prov in self._attach_provenance(
                    node_id, members)]

    def _changed_sids_for(self, node_id: str) -> set:
        """Surrogates of node_id already (re)written by this batch.

        During one batch walk a node is recomputed at most once, so its new
        sids are exactly the corrected rows a downstream consumer must see.
        """
        # The changes map is threaded through the recompute methods; look it
        # up from the per-batch registry installed by apply_batch().
        registry = getattr(self, "_batch_changes", None)
        if registry is None or node_id not in registry:
            return set()
        return registry[node_id].all_sids()

    # ------------------------------------------------------------------
    # Full-row helpers
    # ------------------------------------------------------------------
    def _all_rows(self, node_id: str):
        if node_id in self.plan.inputs:
            return [
                (("src", rid), values, {(node_id, rid): 1})
                for rid, values in self.store.iter_source_all(node_id)
            ]
        members = list(self.store.iter_rel_all(node_id))
        prov_map = self.store.get_provenance(
            node_id, [sid for sid, _ in members])
        return [
            (("rel", sid), values, prov_map[sid]) for sid, values in members
        ]

    # ------------------------------------------------------------------
    # Row builders
    # ------------------------------------------------------------------
    def _trace_insert(self, nid: str, sid: int,
                      entries: Sequence[Tuple[str, str, RowToken]]) -> None:
        """Record instance evidence for one freshly written row (if on).

        Evidence is written row-by-row as the engine emits rows, so a
        repair batch only ever touches the evidence of the rows it already
        deletes or inserts -- never a graph-wide rebuild.
        """
        if self.store.trace_enabled():
            self.store.insert_trace(nid, sid, entries)

    @staticmethod
    def _project(projs: Sequence[Projection],
                 values: Mapping[str, Any]) -> Dict[str, Any]:
        return {p.out_column: values[p.source.column] for p in projs}

    def _emit_join_row(self, nid: str, spec: JoinSpec,
                       l_values: Mapping[str, Any],
                       r_values: Mapping[str, Any],
                       l_prov: Provenance, r_prov: Provenance,
                       key_value: Any,
                       l_token: RowToken, r_token: RowToken) -> int:
        out: Dict[str, Any] = {"_jkey": key_value}
        for p in self.plan.projections[nid]:
            source_values = l_values if p.source.node_id == spec.left \
                else r_values
            out[p.out_column] = source_values[p.source.column]
        sid = self.store.insert_rel(nid, out)
        self.store.insert_prov(nid, sid,
                               _merge_provenance(l_prov, r_prov))
        self._trace_insert(nid, sid, [
            ("left", spec.left, l_token),
            ("right", spec.right, r_token),
        ])
        return sid

    def _emit_agg_group(self, nid: str, spec: AggSpec, gkey: str,
                        members: List[Tuple[RowToken, Dict[str, Any],
                                            Provenance]]
                        ) -> int:
        out: Dict[str, Any] = {"_gkey": gkey}
        first = members[0][1]
        for g_ref, g_out in zip(spec.group_by, spec.group_outputs):
            out[g_out] = first[ColRef.parse(g_ref).column]
        for measure in spec.measures:
            if measure.func == "count":
                # COUNT(*) counts relation rows.  Every materialized row is
                # one multiset entry, regardless of how many source rows its
                # provenance merged together (a join row always pairs at
                # least two sources, but it is still a single row).
                value = len(members)
            else:
                col = ColRef.parse(measure.ref).column
                value = sum(values[col] for _, values, _ in members)
            out[measure.out_column] = value
        sid = self.store.insert_rel(nid, out)
        merged: Provenance = {}
        for _, _, prov in members:
            merged = _merge_provenance(merged, prov)
        self.store.insert_prov(nid, sid, merged)
        self._trace_insert(nid, sid, [
            ("member", spec.input_node, token) for token, _, _ in members
        ])
        return sid


def _merge_provenance(a: Provenance, b: Provenance) -> Provenance:
    out: Provenance = dict(a)
    for key, copies in b.items():
        out[key] = out.get(key, 0) + copies
    return out
