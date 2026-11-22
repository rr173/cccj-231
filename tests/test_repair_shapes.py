"""Additional repair coverage on a second, differently shaped graph.

This graph exercises shapes the settlement fixture does not:

* INTEGER equi-join keys and INTEGER group keys (not only TEXT);
* a multi-column GROUP BY (two group keys, including a key-only column that
  never appears in a value measure);
* an aggregate whose input is another aggregate (agg -> agg chain);
* a rename-only project and an output with a renamed release column;
* source rows that are fully identical except for the immutable row id
  (duplicate business rows must keep separate multiset copies).

The same naive from-scratch reference is used for equivalence.
"""

from __future__ import annotations

import copy
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from lineage_diag import (  # noqa: E402
    AccessLedger,
    RepairConflictError,
    RepairValidationError,
    RepairWorkspace,
    apply_repair_batch,
    initialize_workspace,
    load_repair_plan,
    read_release,
)
from lineage_diag.repair_store import rel_table, source_table  # noqa: E402
from tests.naive_reference import (  # noqa: E402
    apply_corrections,
    normalize_release,
    release_multiset,
)

TMP = os.path.join(ROOT, "tmp", "repair_tests")
FAIL = []


def check(cond, label, detail=""):
    print(f"[{'PASS' if cond else 'FAIL'}] {label}"
          + (f" -- {detail}" if detail and not cond else ""))
    if not cond:
        FAIL.append(label)


GRAPH = {
    "graph_version": "repair-graph-shapes-1",
    "nodes": [
        {"id": "events", "type": "input", "table": "db.events",
         "row_id": "event_id",
         "columns": [
             {"name": "event_id", "type": "TEXT"},
             {"name": "kind_id", "type": "INTEGER"},
             {"name": "dim", "type": "TEXT"},
             {"name": "qty", "type": "INTEGER"},
         ]},
        {"id": "kinds", "type": "input", "table": "db.kinds",
         "row_id": "kind_pk",
         "columns": [
             {"name": "kind_pk", "type": "TEXT"},
             {"name": "kind_id", "type": "INTEGER"},
             {"name": "label", "type": "TEXT"},
         ]},
        # rename-only projection
        {"id": "p_events", "type": "project", "input": "events",
         "exprs": [
             {"kind": "ref", "out_column": "kind_id",
              "refs": ["events.kind_id"], "raw": "events.kind_id"},
             {"kind": "ref", "out_column": "dim",
              "refs": ["events.dim"], "raw": "events.dim"},
             {"kind": "ref", "out_column": "qty",
              "refs": ["events.qty"], "raw": "events.qty"},
         ]},
        # INTEGER equi inner join, many-to-many
        {"id": "j_events", "type": "join",
         "left": "p_events", "right": "kinds", "join_type": "inner",
         "on": [
             {"kind": "op", "op": "=", "out_column": "_on_kind",
              "refs": ["p_events.kind_id", "kinds.kind_id"],
              "raw": "p_events.kind_id = kinds.kind_id",
              "role_override": "join_on"},
         ],
         "exprs": [
             {"kind": "ref", "out_column": "kind_id",
              "refs": ["p_events.kind_id"], "raw": "p_events.kind_id"},
             {"kind": "ref", "out_column": "matched_kind",
              "refs": ["kinds.kind_id"], "raw": "kinds.kind_id"},
             {"kind": "ref", "out_column": "dim",
              "refs": ["p_events.dim"], "raw": "p_events.dim"},
             {"kind": "ref", "out_column": "qty",
              "refs": ["p_events.qty"], "raw": "p_events.qty"},
             {"kind": "ref", "out_column": "label",
              "refs": ["kinds.label"], "raw": "kinds.label"},
         ]},
        # two-column GROUP BY; dim is a key-only column (no measure on it)
        {"id": "a_kd", "type": "agg", "input": "j_events",
         "group_by": ["j_events.kind_id", "j_events.dim"],
         "measures": [
             {"func": "sum", "out_column": "qty_sum",
              "ref": "j_events.qty"},
             {"func": "count", "out_column": "rows_n"},
         ],
         "exprs": [
             {"kind": "ref", "out_column": "kind_id",
              "refs": ["j_events.kind_id"], "raw": "kind key"},
             {"kind": "ref", "out_column": "dim",
              "refs": ["j_events.dim"], "raw": "dim key"},
             {"kind": "op", "op": "sum", "out_column": "qty_sum",
              "refs": ["j_events.qty"], "raw": "sum(qty)"},
             {"kind": "op", "op": "count", "out_column": "rows_n",
              "refs": [], "raw": "count(*)"},
         ]},
        # agg -> agg chain: roll the two-key agg up to one key
        {"id": "a_kind", "type": "agg", "input": "a_kd",
         "group_by": ["a_kd.kind_id"],
         "measures": [
             {"func": "sum", "out_column": "qty_total",
              "ref": "a_kd.qty_sum"},
             {"func": "count", "out_column": "group_n"},
         ],
         "exprs": [
             {"kind": "ref", "out_column": "kind_id",
              "refs": ["a_kd.kind_id"], "raw": "kind key"},
             {"kind": "op", "op": "sum", "out_column": "qty_total",
              "refs": ["a_kd.qty_sum"], "raw": "sum(qty_sum)"},
             {"kind": "op", "op": "count", "out_column": "group_n",
              "refs": [], "raw": "count(*)"},
         ]},
        {"id": "o_shapes", "type": "output", "table": "release_shapes",
         "input": "a_kind",
         "exprs": [
             {"kind": "ref", "out_column": "kind",
              "refs": ["a_kind.kind_id"], "raw": "renamed release column"},
             {"kind": "ref", "out_column": "qty_total",
              "refs": ["a_kind.qty_total"], "raw": "a_kind.qty_total"},
             {"kind": "ref", "out_column": "group_n",
              "refs": ["a_kind.group_n"], "raw": "a_kind.group_n"},
         ]},
    ],
}


def initial_sources():
    # e1/e2 are identical business rows with distinct row ids (duplicate
    # copies must survive).
    events = [
        {"event_id": "e1", "kind_id": 10, "dim": "x", "qty": 5},
        {"event_id": "e2", "kind_id": 10, "dim": "x", "qty": 5},
        {"event_id": "e3", "kind_id": 10, "dim": "y", "qty": 7},
        {"event_id": "e4", "kind_id": 20, "dim": "x", "qty": 9},
        # disjoint mass
        *[{"event_id": f"ed{i}", "kind_id": 900 + i, "dim": "d",
           "qty": 1} for i in range(40)],
    ]
    kinds = [
        {"kind_pk": "k1", "kind_id": 10, "label": "ten"},
        {"kind_pk": "k2", "kind_id": 10, "label": "ten-dup"},
        {"kind_pk": "k3", "kind_id": 20, "label": "twenty"},
        *[{"kind_pk": f"kd{i}", "kind_id": 900 + i, "label": f"c{i}"}
          for i in range(40)],
    ]
    return {"events": events, "kinds": kinds}


def main() -> int:
    os.makedirs(TMP, exist_ok=True)
    db = os.path.join(TMP, "shapes.db")
    if os.path.exists(db):
        os.remove(db)
    plan = load_repair_plan(GRAPH)
    init = {"sources": initial_sources()}
    initialize_workspace(db, GRAPH, init)
    truth = {nid: [dict(r) for r in rows]
             for nid, rows in init["sources"].items()}

    version, rows = read_release(db, GRAPH)
    check(normalize_release(rows, plan) == release_multiset(truth, plan),
          "shapes: initial release matches naive compute")

    # Batch: integer key migration 10 -> 20 on e3; duplicate e1 replaced;
    # a kind row inserted; both sides change at once.
    batch = {
        "expected_version": 1,
        "corrections": [
            {"node_id": "events", "row_id": "e3", "op": "replace",
             "values": {"kind_id": 20, "dim": "y", "qty": 8}},
            {"node_id": "events", "row_id": "e1", "op": "replace",
             "values": {"kind_id": 10, "dim": "x", "qty": 6}},
            {"node_id": "kinds", "row_id": "k4", "op": "insert",
             "values": {"kind_id": 20, "label": "twenty-dup"}},
        ],
    }
    ledger = AccessLedger()
    ws = RepairWorkspace(db, plan, ledger)
    # The workspace was initialized via initialize_workspace above; this
    # long-lived handle only applies subsequent batches.
    ws.apply(batch)
    truth = apply_corrections(truth, plan, batch["corrections"])
    version, rows = read_release(db, GRAPH)
    check(version == 2, "shapes: batch advances version")
    check(normalize_release(rows, plan) == release_multiset(truth, plan),
          "shapes: post-batch release matches naive compute")

    # Scope: the 40 disjoint integer keys must never be read.
    details = [d for _, _, d in ledger.entries]
    leaked = [i for i in range(40)
              if any(str(900 + i) in d for d in details)]
    check(not leaked, "shapes: disjoint integer keys never read",
          str(leaked[:5]))
    check(not [e for e in ledger.entries if e[0] == "full_scan"],
          "shapes: no full scans during repair")

    # A group that loses all members disappears in both agg levels.
    # Move the only kind-20/dim-x event away, then check group removal.
    batch2 = {
        "expected_version": 2,
        "corrections": [
            {"node_id": "events", "row_id": "e4", "op": "replace",
             "values": {"kind_id": 10, "dim": "x", "qty": 9}},
        ],
    }
    ws.apply(batch2)
    truth = apply_corrections(truth, plan, batch2["corrections"])
    version, rows = read_release(db, GRAPH)
    check(normalize_release(rows, plan) == release_multiset(truth, plan),
          "shapes: chained-agg recompute matches naive compute")
    ws.close()
    test_insert_only_and_removal()
    test_condition_and_group_key_only_changes()
    print()
    if FAIL:
        print(f"{len(FAIL)} SHAPES CHECK(S) FAILED: {FAIL}")
        return 1
    print("ALL SHAPES CHECKS PASSED")
    return 0


def test_insert_only_and_removal() -> None:
    print("== shapes: pure insert births a group; deleting both sides "
          "removes it ==")
    db = os.path.join(TMP, "shapes_insert.db")
    if os.path.exists(db):
        os.remove(db)
    plan = load_repair_plan(GRAPH)
    init = {"sources": initial_sources()}
    initialize_workspace(db, GRAPH, init)
    truth = {nid: [dict(r) for r in rows]
             for nid, rows in init["sources"].items()}

    born = {
        "expected_version": 1,
        "corrections": [
            {"node_id": "kinds", "row_id": "k99", "op": "insert",
             "values": {"kind_id": 777, "label": "new"}},
            {"node_id": "events", "row_id": "e99", "op": "insert",
             "values": {"kind_id": 777, "dim": "z", "qty": 3}},
        ],
    }
    apply_repair_batch(db, GRAPH, born)
    truth = apply_corrections(truth, plan, born["corrections"])
    _, rows = read_release(db, GRAPH)
    check(normalize_release(rows, plan) == release_multiset(truth, plan),
          "insert-only batch births a release group matching naive compute")
    check(any(r["kind"] == 777 for r in rows),
          "newborn key 777 reaches the release output")

    gone = {
        "expected_version": 2,
        "corrections": [
            {"node_id": "events", "row_id": "e99", "op": "delete"},
            {"node_id": "kinds", "row_id": "k99", "op": "delete"},
        ],
    }
    apply_repair_batch(db, GRAPH, gone)
    truth = apply_corrections(truth, plan, gone["corrections"])
    _, rows = read_release(db, GRAPH)
    check(normalize_release(rows, plan) == release_multiset(truth, plan),
          "deleting both sides removes the group, still naive-equivalent")
    check(not any(r["kind"] == 777 for r in rows),
          "key 777 fully removed from release after both-side delete")


# A change confined to a join condition or a group-by key must still be
# caught: value-only column lineage would miss it.  These tiny graphs are
# built inline so the intent stays local.
def test_condition_and_group_key_only_changes() -> None:
    print("== shapes: key-only changes (join condition / group key) ==")
    join_graph = {
        "graph_version": "gk-1", "nodes": [
            {"id": "l", "type": "input", "row_id": "lid", "table": "l",
             "columns": [
                 {"name": "lid", "type": "TEXT"},
                 {"name": "k", "type": "TEXT"},
                 {"name": "v", "type": "INTEGER"}]},
            {"id": "r", "type": "input", "row_id": "rid", "table": "r",
             "columns": [
                 {"name": "rid", "type": "TEXT"},
                 {"name": "k", "type": "TEXT"},
                 {"name": "tag", "type": "TEXT"}]},
            {"id": "j", "type": "join", "left": "l", "right": "r",
             "join_type": "inner", "on": [
                 {"kind": "op", "op": "=", "out_column": "_on",
                  "refs": ["l.k", "r.k"], "raw": "l.k=r.k",
                  "role_override": "join_on"}],
             "exprs": [
                 {"kind": "ref", "out_column": "lk", "refs": ["l.k"],
                  "raw": "l.k"},
                 {"kind": "ref", "out_column": "rk", "refs": ["r.k"],
                  "raw": "r.k"},
                 {"kind": "ref", "out_column": "v", "refs": ["l.v"],
                  "raw": "l.v"},
                 {"kind": "ref", "out_column": "tag", "refs": ["r.tag"],
                  "raw": "r.tag"}]},
            {"id": "o", "type": "output", "input": "j", "table": "rel",
             "exprs": [
                 {"kind": "ref", "out_column": "v", "refs": ["j.v"],
                  "raw": "j.v"},
                 {"kind": "ref", "out_column": "tag", "refs": ["j.tag"],
                  "raw": "j.tag"}]},
        ]}
    jplan = load_repair_plan(join_graph)
    jinit = {"sources": {
        "l": [{"lid": "L1", "k": "A", "v": 1},
              {"lid": "L2", "k": "A", "v": 2},
              {"lid": "L3", "k": "B", "v": 3}],
        "r": [{"rid": "R1", "k": "A", "tag": "aa"},
              {"rid": "R2", "k": "B", "tag": "bb"}]}}
    jdb = os.path.join(TMP, "keyonly_join.db")
    if os.path.exists(jdb):
        os.remove(jdb)
    initialize_workspace(jdb, join_graph, jinit)
    jtruth = {n: [dict(x) for x in rs] for n, rs in jinit["sources"].items()}
    jbatch = {"expected_version": 1, "corrections": [
        {"node_id": "l", "row_id": "L3", "op": "replace",
         "values": {"k": "A", "v": 3}}]}
    apply_repair_batch(jdb, join_graph, jbatch)
    jtruth = apply_corrections(jtruth, jplan, jbatch["corrections"])
    _, jrows = read_release(jdb, join_graph)
    check(normalize_release(jrows, jplan) ==
          release_multiset(jtruth, jplan),
          "join-key-only change (B->A) matches naive compute")
    check(sorted(r["v"] for r in jrows) == [1, 2, 3]
          and all(r["tag"] == "aa" for r in jrows),
          "old B pairing withdrawn, new A pairings built")

    agg_graph = {
        "graph_version": "gg-1", "nodes": [
            {"id": "s", "type": "input", "row_id": "sid", "table": "s",
             "columns": [
                 {"name": "sid", "type": "TEXT"},
                 {"name": "grp", "type": "TEXT"},
                 {"name": "amt", "type": "INTEGER"}]},
            {"id": "a", "type": "agg", "input": "s",
             "group_by": ["s.grp"], "measures": [
                 {"func": "sum", "out_column": "tot", "ref": "s.amt"},
                 {"func": "count", "out_column": "n"}],
             "exprs": [
                 {"kind": "ref", "out_column": "grp",
                  "refs": ["s.grp"], "raw": "key"},
                 {"kind": "op", "op": "sum", "out_column": "tot",
                  "refs": ["s.amt"], "raw": "sum"},
                 {"kind": "op", "op": "count", "out_column": "n",
                  "refs": [], "raw": "count(*)"}]},
            {"id": "o", "type": "output", "input": "a", "table": "rel",
             "exprs": [
                 {"kind": "ref", "out_column": "grp",
                  "refs": ["a.grp"], "raw": "a.grp"},
                 {"kind": "ref", "out_column": "tot",
                  "refs": ["a.tot"], "raw": "a.tot"},
                 {"kind": "ref", "out_column": "n", "refs": ["a.n"],
                  "raw": "a.n"}]},
        ]}
    aplan = load_repair_plan(agg_graph)
    ainit = {"sources": {"s": [
        {"sid": "S1", "grp": "X", "amt": 5},
        {"sid": "S2", "grp": "X", "amt": 7},
        {"sid": "S3", "grp": "Y", "amt": 9}]}}
    adb = os.path.join(TMP, "keyonly_agg.db")
    if os.path.exists(adb):
        os.remove(adb)
    initialize_workspace(adb, agg_graph, ainit)
    atruth = {n: [dict(x) for x in rs] for n, rs in ainit["sources"].items()}
    abatch = {"expected_version": 1, "corrections": [
        {"node_id": "s", "row_id": "S3", "op": "replace",
         "values": {"grp": "X", "amt": 9}}]}
    apply_repair_batch(adb, agg_graph, abatch)
    atruth = apply_corrections(atruth, aplan, abatch["corrections"])
    _, arows = read_release(adb, agg_graph)
    check(normalize_release(arows, aplan) ==
          release_multiset(atruth, aplan),
          "group-key-only change (Y->X) matches naive compute")
    check(len(arows) == 1 and arows[0]["grp"] == "X"
          and arows[0]["tot"] == 21 and arows[0]["n"] == 3,
          "old group Y removed when its last row migrates to X",
          str(arows))


if __name__ == "__main__":
    sys.exit(main())
