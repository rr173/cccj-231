"""Regression tests and runnable demo for single-row release tracing.

Run with:  python3 -I tests/test_repair_trace.py

A settlement clerk holds ONE complete published business row plus the
version it was read at, and asks: which original documents, through which
actual pairings and aggregations, produced each physical copy of this row?
Column diagnosis only names fields; the subscription only reports copy
count changes.  ``repair trace`` answers with per-copy instance evidence.

The demo is focused on the three questions from the spec:

  DEMO 1  two projection branches share ONE source table: which left/right
          pairings did same-key rows actually form (roles stay distinct
          even though both sides trace to the same table), and identical
          projected pairings are never merged into one copy;
  DEMO 2  a source document is swapped without changing the group totals:
          the release row and the subscription delta are identical, yet
          the next version's evidence must name the NEW document;
  DEMO 3  multi-layer convergence (one aggregate feeding two identical
          release copies): the few actual instances are delivered once as
          a DAG, not re-expanded into per-root duplicate paths.

Plus: evidence access stays scoped to the hit instances (proved from the
access ledger), a correction that fails mid-commit leaves no evidence
behind, and every malformed/unsupported query shape is explicitly
rejected while a well-formed but absent row simply reports no match.
"""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from lineage_diag import (  # noqa: E402
    AccessLedger,
    RepairWorkspace,
    TraceError,
    apply_repair_batch,
    enable_subscription,
    initialize_workspace,
    load_repair_plan,
    read_release,
    read_subscription,
    trace_release_row,
)
from lineage_diag.repair_store import (  # noqa: E402
    OP_READ_SOURCE,
    OP_READ_TRACE,
    OP_SCAN,
    rel_table,
    source_table,
    trace_table,
)

DATA = os.path.join(ROOT, "data", "repair")
TMP = os.path.join(ROOT, "tmp", "repair_tests")

FAIL = []


def check(cond, label, detail=""):
    print(f"[{'PASS' if cond else 'FAIL'}] {label}"
          + (f" -- {detail}" if detail and not cond else ""))
    if not cond:
        FAIL.append(label)


def fresh_db(name):
    os.makedirs(TMP, exist_ok=True)
    path = os.path.join(TMP, name)
    if os.path.exists(path):
        os.remove(path)
    return path


def load_json(name):
    with open(os.path.join(DATA, name), encoding="utf-8") as fh:
        return json.load(fh)


# ---------------------------------------------------------------------------
# Evidence navigation helpers
# ---------------------------------------------------------------------------
def by_id(result):
    return {i["id"]: i for i in result["instances"]}


def follow_one(insts, inst, role):
    refs = [u["ref"] for u in inst.get("uses", []) if u["role"] == role]
    assert len(refs) == 1, f"expected exactly one {role!r} use in {inst}"
    return insts[refs[0]]


def follow_all(insts, inst, role):
    return [insts[u["ref"]] for u in inst.get("uses", [])
            if u["role"] == role]


def join_side_source(insts, join_inst, role):
    """Source instance at the end of one join side's project chain."""
    side = follow_one(insts, join_inst, role)
    while side["kind"] != "source":
        side = follow_one(insts, side, "in")
    return side


def path_expansion_size(insts, eid):
    """Instance count if the DAG were expanded into per-root paths."""
    inst = insts[eid]
    return 1 + sum(path_expansion_size(insts, u["ref"])
                   for u in inst.get("uses", []))


def show_evidence(result, headline):
    """Compact human-readable rendering of one evidence DAG (demo output)."""
    insts = by_id(result)
    print(f"    {headline}: matched={result['matched']} "
          f"instances={len(result['instances'])}")
    seen = set()
    for eid in result["roots"]:
        _show_instance(insts, eid, indent="      ", seen=seen)


def _show_instance(insts, eid, indent, seen):
    inst = insts[eid]
    if inst["kind"] == "source":
        print(f"{indent}{eid} {inst['node']}.{inst['row_id']} "
            f"{inst['values']}")
        return
    again = " (shared, shown above)" if eid in seen else ""
    roles = "/".join(u["role"] for u in inst["uses"])
    print(f"{indent}{eid} {inst['kind']}:{inst['node']} "
          f"{inst['values']} [{roles}]{again}")
    if eid in seen:
        return
    seen.add(eid)
    for use in inst["uses"]:
        _show_instance(insts, use["ref"], indent + "  ", seen)


# ---------------------------------------------------------------------------
# DEMO 1: two projection branches over ONE source table.
#
#     docs ── p_left (k, x) ──┐
#       │                      ├─ j_pair (p_left.k = p_right.k) ── o_out
#       └── p_right (k, y) ───┘
#
# d1 and d2 share key K and project identical left rows (x=1), so the
# release holds TWO identical copies of (K,K,1,9) and (K,K,1,8).  The
# evidence must show the actual pairings (which doc fed left, which fed
# right) and must not merge the identical copies.
# ---------------------------------------------------------------------------
SHARED_SOURCE_GRAPH = {
    "graph_version": "trace-shared-src-1",
    "nodes": [
        {"id": "docs", "type": "input", "table": "db.docs",
         "row_id": "doc_id",
         "columns": [
             {"name": "doc_id", "type": "TEXT"},
             {"name": "k", "type": "TEXT"},
             {"name": "x", "type": "INTEGER"},
             {"name": "y", "type": "INTEGER"},
         ]},
        {"id": "p_left", "type": "project", "input": "docs",
         "exprs": [
             {"kind": "ref", "out_column": "k",
              "refs": ["docs.k"], "raw": "docs.k"},
             {"kind": "ref", "out_column": "x",
              "refs": ["docs.x"], "raw": "docs.x"},
         ]},
        {"id": "p_right", "type": "project", "input": "docs",
         "exprs": [
             {"kind": "ref", "out_column": "k",
              "refs": ["docs.k"], "raw": "docs.k"},
             {"kind": "ref", "out_column": "y",
              "refs": ["docs.y"], "raw": "docs.y"},
         ]},
        {"id": "j_pair", "type": "join",
         "left": "p_left", "right": "p_right", "join_type": "inner",
         "on": [
             {"kind": "op", "op": "=", "out_column": "_on_k",
              "refs": ["p_left.k", "p_right.k"],
              "raw": "p_left.k = p_right.k", "role_override": "join_on"},
         ],
         "exprs": [
             {"kind": "ref", "out_column": "lk",
              "refs": ["p_left.k"], "raw": "p_left.k AS lk"},
             {"kind": "ref", "out_column": "rk",
              "refs": ["p_right.k"], "raw": "p_right.k AS rk"},
             {"kind": "ref", "out_column": "x",
              "refs": ["p_left.x"], "raw": "p_left.x"},
             {"kind": "ref", "out_column": "y",
              "refs": ["p_right.y"], "raw": "p_right.y"},
         ]},
        {"id": "o_out", "type": "output", "table": "release_pairs",
         "input": "j_pair",
         "exprs": [
             {"kind": "ref", "out_column": "lk",
              "refs": ["j_pair.lk"], "raw": "j_pair.lk"},
             {"kind": "ref", "out_column": "rk",
              "refs": ["j_pair.rk"], "raw": "j_pair.rk"},
             {"kind": "ref", "out_column": "x",
              "refs": ["j_pair.x"], "raw": "j_pair.x"},
             {"kind": "ref", "out_column": "y",
              "refs": ["j_pair.y"], "raw": "j_pair.y"},
         ]},
    ],
}

SHARED_SOURCE_ROWS = {
    "docs": [
        {"doc_id": "d1", "k": "K", "x": 1, "y": 9},
        {"doc_id": "d2", "k": "K", "x": 1, "y": 8},  # left row == d1's
        {"doc_id": "d3", "k": "Q", "x": 5, "y": 5},
    ],
}


def demo1_shared_source_pairings():
    print("== DEMO 1. shared source table: actual left/right pairings ==")
    db = fresh_db("trace_shared.db")
    initialize_workspace(db, SHARED_SOURCE_GRAPH,
                         {"sources": SHARED_SOURCE_ROWS}, enable_trace=True)

    version, rows = read_release(db, SHARED_SOURCE_GRAPH)
    check(version == 1 and len(rows) == 5,
          "release holds every pairing copy (4x K + 1x Q)",
          f"v={version} rows={len(rows)}")

    # The release row (K,K,1,9) exists in TWO identical physical copies.
    result = trace_release_row(
        db, SHARED_SOURCE_GRAPH,
        {"lk": "K", "rk": "K", "x": 1, "y": 9}, expected_version=1)
    show_evidence(result, "release row (K,K,1,9)")
    check(result["matched"] == 2 and len(result["roots"]) == 2,
          "identical pairings stay two copies, never merged",
          f"matched={result['matched']}")
    check(len(set(result["roots"])) == 2,
          "each copy gets its own root evidence id")

    insts = by_id(result)
    # 2 output + 2 join + 2 p_left + 1 shared p_right + sources d1,d2 = 9.
    check(len(result["instances"]) == 9,
          "DAG delivers each instance once (2 out + 2 join + 2 p_left "
          "+ 1 shared p_right + 2 sources)", str(len(result["instances"])))

    # The actual pairings behind the two copies: (d1 x d1) and (d2 x d1).
    pairings = set()
    for root in result["roots"]:
        out_inst = insts[root]
        check(out_inst["kind"] == "output",
              "root instance is the release output row")
        join_inst = follow_one(insts, out_inst, "in")
        check(join_inst["kind"] == "join",
              "output traces to one join pairing instance")
        left_src = join_side_source(insts, join_inst, "left")
        right_src = join_side_source(insts, join_inst, "right")
        check(left_src["node"] == "docs" and right_src["node"] == "docs",
              "both sides trace to the same source table 'docs'")
        pairings.add((left_src["row_id"], right_src["row_id"]))
    check(pairings == {("d1", "d1"), ("d2", "d1")},
          "same-key rows formed exactly the pairings (d1,d1) and (d2,d1) "
          "-- roles distinguish the sides", str(sorted(pairings)))

    # The shared p_right(d1) instance and source d1 appear exactly once,
    # referenced from both join pairings.
    p_rights = [i for i in result["instances"] if i["node"] == "p_right"]
    check(len(p_rights) == 1, "shared p_right(d1) instance delivered once")
    d1s = [i for i in result["instances"]
           if i["kind"] == "source" and i["row_id"] == "d1"]
    check(len(d1s) == 1, "source row d1 delivered once")
    # d1 is used on the left of one pairing and on the right of both.
    d1_id = d1s[0]["id"]
    uses_of_d1 = [
        (i["node"], u["role"])
        for i in result["instances"]
        for u in i.get("uses", []) if u["ref"] == d1_id
    ]
    check(sorted(uses_of_d1) == [("p_left", "in"), ("p_right", "in")],
          "d1's usage relations (left branch + right branch) preserved",
          str(uses_of_d1))

    # Every join instance keeps exactly one left and one right use.
    join_uses_ok = all(
        sorted(u["role"] for u in i["uses"]) == ["left", "right"]
        for i in result["instances"] if i["kind"] == "join")
    check(join_uses_ok, "join evidence keeps left/right roles separate")

    # The second distinct pairing value (K,K,1,8) pairs d1/d2 with d2.
    result8 = trace_release_row(
        db, SHARED_SOURCE_GRAPH,
        {"lk": "K", "rk": "K", "x": 1, "y": 8}, expected_version=1)
    insts8 = by_id(result8)
    pairings8 = {
        (join_side_source(insts8, follow_one(insts8, insts8[r], "in"),
                          "left")["row_id"],
         join_side_source(insts8, follow_one(insts8, insts8[r], "in"),
                          "right")["row_id"])
        for r in result8["roots"]
    }
    check(pairings8 == {("d1", "d2"), ("d2", "d2")},
          "pairings for the sibling row are (d1,d2) and (d2,d2)",
          str(sorted(pairings8)))


# ---------------------------------------------------------------------------
# DEMO 2: swap the source document, keep the totals.
#
# o3 (R2, amount 30) is replaced by o9 with identical business values.
# The release row and the net-change subscription show NOTHING changed,
# yet the next version's evidence must name o9, never o3.
# ---------------------------------------------------------------------------
SETTLE_SOURCES = {
    "orders": [
        {"order_id": "o3", "acc_id": "A", "region": "R2", "amount": 30},
    ],
    "accounts": [
        {"acc_pk": "a1", "acc_id": "A", "owner": "ann"},
        {"acc_pk": "a2", "acc_id": "A", "owner": "ann2"},
    ],
    "targets": [
        {"target_id": "t2", "region": "R2", "bucket": "hot"},
    ],
}

R2_ROW = {"region": "R2", "total_amt": 60, "n": 2, "bucket": "hot"}


def _source_ids_in(result):
    return {i["row_id"] for i in result["instances"]
            if i["kind"] == "source"}


def demo2_document_swap_same_totals():
    print("== DEMO 2. document swap with unchanged totals ==")
    graph = load_json("graph.json")
    db = fresh_db("trace_swap.db")
    initialize_workspace(db, graph, {"sources": SETTLE_SOURCES},
                         enable_trace=True)
    enable_subscription(db, graph, retention=3)

    before = trace_release_row(db, graph, dict(R2_ROW), expected_version=1)
    check(before["matched"] == 1, "R2 row present once at v1")
    check("o3" in _source_ids_in(before),
          "v1 evidence names the original document o3")

    # Verify the aggregate evidence lets the clerk re-check the numbers:
    # two consumed members, 30 + 30 = 60, COUNT(*) = 2.
    insts = by_id(before)
    agg = next(i for i in before["instances"] if i["kind"] == "agg")
    members = follow_all(insts, agg, "member")
    check(agg["values"]["region"] == "R2" and
          agg["values"]["total_amt"] == 60 and agg["values"]["n"] == 2,
          "agg instance carries the group key and both measures")
    check(len(members) == agg["values"]["n"] == 2,
          "COUNT(*) equals the number of consumed member instances")
    check(sum(m["values"]["amount"] for m in members)
          == agg["values"]["total_amt"],
          "SUM re-checkable from the actual member rows (30+30=60)")

    # Swap the document: delete o3, insert o9 with identical values.
    batch = {"expected_version": 1, "corrections": [
        {"node_id": "orders", "row_id": "o3", "op": "delete"},
        {"node_id": "orders", "row_id": "o9", "op": "insert",
         "values": {"acc_id": "A", "region": "R2", "amount": 30}},
    ]}
    summary = apply_repair_batch(db, graph, batch)
    check(summary["version"] == 2, "swap batch publishes version 2")

    version, rows = read_release(db, graph)
    check(rows == [dict(R2_ROW)],
          "release row byte-identical after the swap")
    page = read_subscription(db, graph, from_version=1)
    check(page["changes"] == [] and page["to_version"] == 2,
          "subscription reports zero net change for the swap "
          "(copy counts did not move)")

    after = trace_release_row(db, graph, dict(R2_ROW), expected_version=2)
    check(after["matched"] == 1, "R2 row still present once at v2")
    src_ids = _source_ids_in(after)
    check("o9" in src_ids and "o3" not in src_ids,
          "v2 evidence names the NEW document o9 and never o3",
          str(sorted(src_ids)))
    check(json.dumps(after, sort_keys=True).find('"o3"') == -1,
          "no trace of the withdrawn document anywhere in the evidence")
    print(f"    documents behind the row: v1 -> {sorted(_source_ids_in(before))}"
          f"   v2 -> {sorted(src_ids)}   (release row and subscription "
          "delta identical)")

    # The pre-swap version can no longer be queried.
    try:
        trace_release_row(db, graph, dict(R2_ROW), expected_version=1)
        check(False, "stale expected_version is rejected")
    except TraceError as exc:
        check(exc.kind == "stale_version",
              "querying the pre-swap version is rejected as stale")


# ---------------------------------------------------------------------------
# DEMO 3: multi-layer convergence -- one aggregate feeds two identical
# release copies (two targets on R2).  The DAG must stay small.
# ---------------------------------------------------------------------------
def demo3_shared_instances_not_reexpanded():
    print("== DEMO 3. convergence: instances delivered once, not paths ==")
    graph = load_json("graph.json")
    sources = {
        "orders": [
            {"order_id": "o3", "acc_id": "A", "region": "R2",
             "amount": 30},
        ],
        "accounts": [
            {"acc_pk": "a1", "acc_id": "A", "owner": "ann"},
            {"acc_pk": "a2", "acc_id": "A", "owner": "ann2"},
        ],
        "targets": [
            {"target_id": "t2", "region": "R2", "bucket": "hot"},
            {"target_id": "t10", "region": "R2", "bucket": "hot"},
        ],
    }
    db = fresh_db("trace_dag.db")
    initialize_workspace(db, graph, {"sources": sources}, enable_trace=True)

    version, rows = read_release(db, graph)
    check(version == 1 and len(rows) == 2 and
          all(r == R2_ROW for r in rows),
          "two identical release copies from two targets")

    result = trace_release_row(db, graph, dict(R2_ROW), expected_version=1)
    insts = by_id(result)
    check(result["matched"] == 2, "both copies matched")
    show_evidence(result, "two copies, one shared aggregate")

    # Exact DAG: 2 output + 2 j_target + 1 a_region + 2 j_settle members
    # + 1 p_orders + sources {o3, a1, a2, t2, t10} = 13 instances.
    check(len(result["instances"]) == 13,
          "13 actual instances delivered for 2 copies",
          str(len(result["instances"])))
    expanded = sum(path_expansion_size(insts, r) for r in result["roots"])
    check(expanded == 24 and len(result["instances"]) < expanded,
          "per-path expansion would be 24 instance visits; the DAG "
          "delivers 13", f"expanded={expanded}")

    # The shared aggregate and projection appear exactly once, with their
    # usage relations to both consumers preserved.
    for node, expect_consumers in (("a_region", 2), ("p_orders", 2)):
        shared = [i for i in result["instances"] if i["node"] == node]
        check(len(shared) == 1, f"{node} delivered exactly once")
        consumers = [i["node"] for i in result["instances"]
                     for u in i.get("uses", [])
                     if u["ref"] == shared[0]["id"]]
        check(len(consumers) == expect_consumers,
              f"{node} referenced from {expect_consumers} consumers "
              f"({sorted(consumers)})", str(consumers))
    # The two j_target copies pair the shared aggregate with DIFFERENT
    # target documents.
    target_docs = {
        join_side_source(insts, j, "right")["row_id"]
        for j in (follow_one(insts, insts[r], "in")
                  for r in result["roots"])
    }
    check(target_docs == {"t2", "t10"},
          "each copy pairs the shared aggregate with its own target row",
          str(sorted(target_docs)))


# ---------------------------------------------------------------------------
# Access scope: evidence reads touch only the hit instances.
# ---------------------------------------------------------------------------
def test_trace_access_scope():
    print("== 4. trace query touches only the hit instances ==")
    graph = load_json("graph.json")
    plan = load_repair_plan(graph)
    initial = load_json("initial.json")
    db = fresh_db("trace_scope.db")
    ledger = AccessLedger()
    ws = RepairWorkspace(db, plan, ledger)
    ws.initialize(initial, enable_trace=True)
    ledger.reset()

    # v1 R2 row: sources are exactly orders.o3, accounts.a1/a2, targets.t2.
    result = ws.trace_row(dict(R2_ROW), expected_version=1)
    check(result["matched"] == 1, "R2 row matched in the big fixture")

    scans = [(t, d) for k, t, d in ledger.entries if k == OP_SCAN]
    check(not scans, "trace query performs no full table scans",
          str(scans))

    details = [d for _, _, d in ledger.entries]
    leaked = [d for d in details
              if any(z in d for z in ("Z0", "C0", "C1", "C2", "C3"))]
    check(not leaked,
          "no disjoint region/account key is ever touched",
          str(leaked)[:120])

    src_reads = {d for k, t, d in ledger.entries if k == OP_READ_SOURCE}
    check(src_reads == {"o3", "a1", "a2", "t2"},
          "source reads are exactly the four documents behind the row",
          str(sorted(src_reads)))

    # Every instance's evidence row is read at most once (memoized DAG
    # walk, no per-path re-expansion of shared upstreams).
    trace_reads = [(t, d) for k, t, d in ledger.entries
                   if k == OP_READ_TRACE]
    check(len(trace_reads) == len(set(trace_reads)),
          "each instance's evidence read exactly once",
          str(trace_reads))
    # 1 output + 1 j_target + 1 a_region + 2 j_settle + 1 p_orders = 6.
    check(len(trace_reads) == 6,
          "evidence reads limited to the 6 materialized instances on the "
          "walk", str(len(trace_reads)))
    ws.close()

    # A daily repair with tracing on stays scoped too: batch1 must not
    # touch evidence of the 80 disjoint regions.
    ledger2 = AccessLedger()
    ws2 = RepairWorkspace(db, plan, ledger2)
    ws2.apply(load_json("batch1.json"))
    bad = [d for _, _, d in ledger2.entries
           if any(z in d for z in ("Z0",))]
    check(not bad,
          "incremental repair never touches evidence of disjoint keys",
          str(bad)[:120])
    # Evidence of the migrated order now points at the post-batch rows.
    r5 = ws2.trace_row({"region": "R5", "total_amt": 30, "n": 3,
                        "bucket": "hot"}, expected_version=2)
    check(r5["matched"] == 1 and "o1" in _source_ids_in(r5),
          "post-batch evidence for the migrated group names o1")
    ws2.close()


# ---------------------------------------------------------------------------
# A correction that fails mid-commit leaves no evidence behind.
# ---------------------------------------------------------------------------
def test_failed_commit_leaves_no_evidence():
    print("== 5. rolled-back / rejected corrections leave no evidence ==")
    graph = load_json("graph.json")
    plan = load_repair_plan(graph)
    db = fresh_db("trace_atomic.db")
    ws = RepairWorkspace(db, plan, AccessLedger())
    ws.initialize({"sources": SETTLE_SOURCES}, enable_trace=True)

    swap_batch = {"expected_version": 1, "corrections": [
        {"node_id": "orders", "row_id": "o3", "op": "delete"},
        {"node_id": "orders", "row_id": "o9", "op": "insert",
         "values": {"acc_id": "A", "region": "R2", "amount": 30}},
    ]}

    class Boom(Exception):
        pass

    def fail_mid_write(node_id):
        if node_id == "j_settle":
            raise Boom("injected after intermediate writes began")

    try:
        ws.apply(swap_batch, failpoint=fail_mid_write)
        check(False, "failpoint actually fired")
    except Boom:
        check(True, "failpoint fired mid-transaction")
    ws.close()

    # Reopen: version, release and evidence are all the pre-batch state.
    version, rows = read_release(db, graph)
    check(version == 1 and rows == [dict(R2_ROW)],
          "rolled-back correction leaves version and release untouched")
    result = trace_release_row(db, graph, dict(R2_ROW), expected_version=1)
    check("o3" in _source_ids_in(result),
          "evidence still names the pre-batch document o3")
    check('"o9"' not in json.dumps(result),
          "no evidence of the rolled-back document o9 remains")

    # No orphan evidence rows in any trace table.
    conn = sqlite3.connect(db)
    for nid in plan.closure:
        if nid in plan.inputs:
            continue
        orphans = conn.execute(
            f"SELECT COUNT(*) FROM {trace_table(nid)} t "
            f"LEFT JOIN {rel_table(nid)} r ON r._sid = t._sid "
            "WHERE r._sid IS NULL").fetchone()[0]
        check(orphans == 0, f"no orphan evidence in {trace_table(nid)}")
    conn.close()

    # A rejected batch (stale version) also leaves evidence untouched.
    try:
        apply_repair_batch(db, graph,
                           {**swap_batch, "expected_version": 99})
        check(False, "stale batch rejected")
    except Exception:
        check(True, "stale batch rejected")
    result = trace_release_row(db, graph, dict(R2_ROW), expected_version=1)
    check("o3" in _source_ids_in(result),
          "rejected batch leaves the committed evidence intact")

    # Retry the same correction successfully: evidence flips to o9.
    summary = apply_repair_batch(db, graph, swap_batch)
    check(summary["version"] == 2, "retry publishes version 2")
    result = trace_release_row(db, graph, dict(R2_ROW), expected_version=2)
    src_ids = _source_ids_in(result)
    check("o9" in src_ids and "o3" not in src_ids,
          "evidence after the committed retry names o9 only")


# ---------------------------------------------------------------------------
# Rejections and the no-match case.
# ---------------------------------------------------------------------------
def test_trace_rejections():
    print("== 6. explicit rejections; well-formed absence is no-match ==")
    graph = load_json("graph.json")
    db = fresh_db("trace_reject.db")
    initialize_workspace(db, graph, {"sources": SETTLE_SOURCES},
                         enable_trace=True)
    db_plain = fresh_db("trace_plain.db")
    initialize_workspace(db_plain, graph, {"sources": SETTLE_SOURCES})

    def expect(kind, label, fn):
        try:
            fn()
        except TraceError as exc:
            check(exc.kind == kind, f"rejected ({kind}): {label}",
                  f"got kind={exc.kind}: {exc}")
            return
        except Exception as other:  # noqa: BLE001
            check(False, f"rejected ({kind}): {label}",
                  f"wrong exception {type(other).__name__}: {other}")
            return
        check(False, f"rejected ({kind}): {label}", "query was accepted")

    row = dict(R2_ROW)
    expect("not_enabled", "workspace built without --trace",
           lambda: trace_release_row(db_plain, graph, row, 1))
    expect("invalid_version", "expected_version as string",
           lambda: trace_release_row(db, graph, row, "1"))
    expect("invalid_version", "expected_version as bool",
           lambda: trace_release_row(db, graph, row, True))
    expect("invalid_version", "expected_version zero",
           lambda: trace_release_row(db, graph, row, 0))
    expect("stale_version", "expected_version ahead of committed",
           lambda: trace_release_row(db, graph, row, 2))
    expect("bad_row", "row is not an object",
           lambda: trace_release_row(db, graph, ["R2", 60, 2, "hot"], 1))

    missing = dict(row)
    missing.pop("n")
    expect("missing_column", "one release column missing",
           lambda: trace_release_row(db, graph, missing, 1))
    expect("extra_column", "unknown extra column",
           lambda: trace_release_row(db, graph, {**row, "x": 1}, 1))
    expect("type_mismatch", "INTEGER column given a string",
           lambda: trace_release_row(db, graph,
                                     {**row, "total_amt": "60"}, 1))
    expect("type_mismatch", "TEXT column given an integer",
           lambda: trace_release_row(db, graph, {**row, "region": 5}, 1))
    expect("type_mismatch", "INTEGER column given a bool",
           lambda: trace_release_row(db, graph, {**row, "n": True}, 1))
    expect("type_mismatch", "null value",
           lambda: trace_release_row(db, graph,
                                     {**row, "bucket": None}, 1))

    # Well-formed but absent: NOT an error, just zero matched copies.
    absent = trace_release_row(
        db, graph, {"region": "R9", "total_amt": 1, "n": 1,
                    "bucket": "hot"}, expected_version=1)
    check(absent["matched"] == 0 and absent["roots"] == [] and
          absent["instances"] == [] and absent["version"] == 1,
          "well-formed absent row reports no match at this version")

    # Type-strict matching is observable through validation: "60" and 60
    # are different rows; the string form is rejected above, and the
    # integer form matches exactly one copy.
    ok = trace_release_row(db, graph, row, 1)
    check(ok["matched"] == 1, "typed row matches exactly its copies")


# ---------------------------------------------------------------------------
# CLI end-to-end: init --trace, trace, and failure exit codes.
# ---------------------------------------------------------------------------
def test_trace_cli():
    print("== 7. CLI: repair init --trace / repair trace ==")
    graph_path = os.path.join(DATA, "graph.json")
    init_path = os.path.join(TMP, "trace_cli_initial.json")
    with open(init_path, "w", encoding="utf-8") as fh:
        json.dump({"sources": SETTLE_SOURCES}, fh)
    db = fresh_db("trace_cli.db")

    def cli(*args):
        return subprocess.run(
            [sys.executable, "-I", os.path.join(ROOT, "bin", "lineage-diag"),
             *args], cwd=ROOT, capture_output=True, text=True)

    res = cli("repair", "init", "--graph", graph_path, "--db", db,
              "--init", init_path, "--trace")
    check(res.returncode == 0 and json.loads(res.stdout)["trace"] is True,
          "CLI init --trace succeeds", res.stderr[:200])

    res = cli("repair", "trace", "--graph", graph_path, "--db", db,
              "--row", json.dumps(R2_ROW), "--expected-version", "1")
    check(res.returncode == 0, "CLI trace exits 0", res.stderr[:200])
    if res.returncode == 0:
        payload = json.loads(res.stdout)
        check(payload["matched"] == 1 and
              "o3" in {i.get("row_id") for i in payload["instances"]},
              "CLI trace payload names the source document")
        check(all(not str(i["id"]).startswith("_")
                  for i in payload["instances"]),
              "evidence ids are local, no internal row numbers exposed")

    res = cli("repair", "trace", "--graph", graph_path, "--db", db,
              "--row", json.dumps({**R2_ROW, "n": "2"}),
              "--expected-version", "1")
    check(res.returncode == 2 and "type_mismatch" in res.stderr,
          "CLI trace rejects a mistyped row with exit 2",
          res.stderr[:200])

    res = cli("repair", "trace", "--graph", graph_path, "--db", db,
              "--row", json.dumps(R2_ROW), "--expected-version", "9")
    check(res.returncode == 2 and "stale_version" in res.stderr,
          "CLI trace rejects a stale version with exit 2",
          res.stderr[:200])

    db2 = fresh_db("trace_cli_plain.db")
    res = cli("repair", "init", "--graph", graph_path, "--db", db2,
              "--init", init_path)
    check(res.returncode == 0, "CLI init without --trace succeeds")
    res = cli("repair", "trace", "--graph", graph_path, "--db", db2,
              "--row", json.dumps(R2_ROW), "--expected-version", "1")
    check(res.returncode == 2 and "not_enabled" in res.stderr,
          "CLI trace on a non-trace workspace is rejected",
          res.stderr[:200])


def main():
    demo1_shared_source_pairings()
    demo2_document_swap_same_totals()
    demo3_shared_instances_not_reexpanded()
    test_trace_access_scope()
    test_failed_commit_leaves_no_evidence()
    test_trace_rejections()
    test_trace_cli()
    print()
    if FAIL:
        print(f"{len(FAIL)} TRACE CHECK(S) FAILED:")
        for f in FAIL:
            print(" -", f)
        return 1
    print("ALL TRACE CHECKS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
