"""Naive full-recompute reference for repair correctness.

This module deliberately does NOT import :mod:`lineage_diag.repair_engine`
or :mod:`lineage_diag.repair_store`.  It re-implements the executable
semantics from the spec in plain Python dicts:

* project/output: per-row reference projections;
* join: many-to-many single-column equi inner join, duplicate pairings and
  identical projected rows each keep their own multiset copy;
* agg: group by >=1 key, integer SUM and COUNT(*) over multisets; empty
  groups do not exist;
* inputs carry an immutable in-table row id excluded from business data.

Applying a full batch to the source tables and then running this from
scratch yields the ground-truth release multiset that the incremental
engine must reproduce after every successful version.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any, Dict, List, Mapping, Sequence, Tuple

from lineage_diag.repair_plan import RepairPlan


def apply_corrections(sources: Dict[str, List[Dict[str, Any]]],
                      plan: RepairPlan,
                      corrections: Sequence[Mapping[str, Any]]
                      ) -> Dict[str, List[Dict[str, Any]]]:
    """Return new source tables after one correction batch."""
    next_sources = {nid: [dict(r) for r in rows]
                    for nid, rows in sources.items()}
    row_id_cols = plan.row_ids
    for c in corrections:
        nid, rid, op = c["node_id"], c["row_id"], c["op"]
        rid_col = row_id_cols[nid]
        idx = next((i for i, r in enumerate(next_sources[nid])
                    if r[rid_col] == rid), None)
        if op == "insert":
            assert idx is None, "fixture must satisfy insert preconditions"
            row = {rid_col: rid, **c["values"]}
            next_sources[nid].append(row)
        elif op == "delete":
            assert idx is not None
            next_sources[nid].pop(idx)
        else:
            assert idx is not None
            next_sources[nid][idx] = {rid_col: rid, **c["values"]}
    return next_sources


def full_compute(sources: Mapping[str, List[Dict[str, Any]]],
                 plan: RepairPlan
                 ) -> Dict[str, Counter]:
    """Execute the whole sub-graph; return multisets keyed by node id."""
    multisets: Dict[str, Counter] = {}
    for nid in plan.closure:
        if nid in plan.inputs:
            rid_col = plan.row_ids[nid]
            multisets[nid] = Counter(
                tuple(sorted((c, r[c]) for c in plan.columns[nid]))
                for r in sources[nid]
            )
            continue
        if nid in plan.joins:
            multisets[nid] = _full_join(nid, plan, multisets)
        elif nid in plan.aggs:
            multisets[nid] = _full_agg(nid, plan, multisets)
        else:
            multisets[nid] = _full_project(nid, plan, multisets)
    return multisets


def _row_map(plan: RepairPlan, nid: str) -> List[str]:
    return plan.columns[nid]


def _full_project(nid: str, plan: RepairPlan,
                  multisets: Mapping[str, Counter]) -> Counter:
    upstream_id = plan.upstreams[nid][0]
    out: Counter = Counter()
    cols = plan.columns[upstream_id]
    for entry, multiplicity in multisets[upstream_id].items():
        row = dict(entry)
        projected = tuple(
            sorted((p.out_column, row[p.source.column])
                   for p in plan.projections[nid])
        )
        out[projected] += multiplicity
    return out


def _full_join(nid: str, plan: RepairPlan,
               multisets: Mapping[str, Counter]) -> Counter:
    from lineage_diag.model import ColRef
    spec = plan.joins[nid]
    l_key = ColRef.parse(spec.left_key).column
    r_key = ColRef.parse(spec.right_key).column
    right_index: Dict[Any, List[tuple]] = defaultdict(list)
    for entry, multiplicity in multisets[spec.right].items():
        row = dict(entry)
        right_index[row[r_key]].append((row, multiplicity))
    out: Counter = Counter()
    for l_entry, l_mult in multisets[spec.left].items():
        l_row = dict(l_entry)
        for r_row, r_mult in right_index.get(l_row[l_key], ()):
            joined = {}
            for p in plan.projections[nid]:
                source_row = l_row if p.source.node_id == spec.left \
                    else r_row
                joined[p.out_column] = source_row[p.source.column]
            key = tuple(sorted(joined.items()))
            out[key] += l_mult * r_mult
    return out


def _full_agg(nid: str, plan: RepairPlan,
              multisets: Mapping[str, Counter]) -> Counter:
    from lineage_diag.model import ColRef
    spec = plan.aggs[nid]
    g_cols = [ColRef.parse(g).column for g in spec.group_by]
    groups: Dict[Tuple, List[tuple]] = defaultdict(list)
    for entry, multiplicity in multisets[spec.input_node].items():
        row = dict(entry)
        gkey = tuple(row[c] for c in g_cols)
        groups[gkey].append((row, multiplicity))
    out: Counter = Counter()
    for gkey, members in groups.items():
        row_out: Dict[str, Any] = {}
        first = members[0][0]
        for g_ref, g_out in zip(spec.group_by, spec.group_outputs):
            row_out[g_out] = first[ColRef.parse(g_ref).column]
        for measure in spec.measures:
            if measure.func == "count":
                row_out[measure.out_column] = sum(m for _, m in members)
            else:
                col = ColRef.parse(measure.ref).column
                row_out[measure.out_column] = sum(
                    row[col] * m for row, m in members)
        out[tuple(sorted(row_out.items()))] = 1
    return out


def release_multiset(sources: Mapping[str, List[Dict[str, Any]]],
                     plan: RepairPlan) -> Counter:
    multisets = full_compute(sources, plan)
    return multisets[plan.output_node_id]


def normalize_release(rows: Sequence[Mapping[str, Any]],
                      plan: RepairPlan) -> Counter:
    cols = plan.columns[plan.output_node_id]
    return Counter(
        tuple(sorted((c, r[c]) for c in cols)) for r in rows
    )
