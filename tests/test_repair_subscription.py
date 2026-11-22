"""Regression tests and runnable demo for the release-change subscription.

Run with:  python3 -I tests/test_repair_subscription.py

Scenario: a downstream reconciliation library holds a full baseline of the
release table at some version and later pulls only the net per-row copy
changes to catch up.  The demo drives a consumer that lags several
versions and covers, with per-version copy counts checked by hand:

* the same business row's copies decrease but do not reach zero
  (R2 target row 2 -> 1 copy, delta -1);
* a row appears and is withdrawn *inside* the lag window (Z007/warm
  target inserted at v5, deleted at v6) and must not make the consumer do
  a pointless delete -- net 0, never delivered;
* a business row is replaced (R4 bucket warm -> hot at v6: old row -1,
  new row +1);
* multi-page delivery with a stable order, exactly-once semantics and
  re-fetch determinism;
* a publish landing after page 1: an in-window start keeps its pinned
  target, an expired start is rejected with a clear re-baseline error;
* a large mass of never-changing release rows vs. a few changes: the
  actual access scope of both publishing and reading stays narrow;
* rejected / rolled-back repairs never move the subscription-visible
  state.
"""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
from collections import Counter

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from lineage_diag import (  # noqa: E402
    AccessLedger,
    RepairConflictError,
    RepairValidationError,
    RepairWorkspace,
    SubscriptionError,
    apply_repair_batch,
    enable_subscription,
    initialize_workspace,
    load_repair_plan,
    read_release,
    read_subscription,
)
from lineage_diag.repair_store import SUB_DELTA_TABLE  # noqa: E402
from tests.naive_reference import (  # noqa: E402
    apply_corrections,
    normalize_release,
    release_multiset,
)

DATA = os.path.join(ROOT, "data", "repair")
TMP = os.path.join(ROOT, "tmp", "repair_tests")
RETENTION = 3
OUT_COLS = ["region", "total_amt", "n", "bucket"]

FAIL = []


def check(cond: bool, label: str, detail: str = "") -> None:
    mark = "PASS" if cond else "FAIL"
    print(f"[{mark}] {label}" + (f" -- {detail}" if detail and not cond
                                 else ""))
    if not cond:
        FAIL.append(label)


def load_json(name: str):
    with open(os.path.join(DATA, name), encoding="utf-8") as fh:
        return json.load(fh)


def fresh_db(name: str) -> str:
    os.makedirs(TMP, exist_ok=True)
    path = os.path.join(TMP, name)
    if os.path.exists(path):
        os.remove(path)
    return path


def row(region, total_amt, n, bucket):
    return {"region": region, "total_amt": total_amt, "n": n,
            "bucket": bucket}


def row_key_of(r) -> tuple:
    return tuple(sorted((c, r[c]) for c in OUT_COLS))


# ---------------------------------------------------------------------------
# Version script: init + enable at v1, then seven publishes.
# ---------------------------------------------------------------------------
def custom_batches():
    batch3 = {"expected_version": 3, "corrections": [
        # R2 release row gains a second identical copy (two hot targets).
        {"node_id": "targets", "row_id": "t10", "op": "insert",
         "values": {"region": "R2", "bucket": "hot"}},
        # A brand-new release row appears next to the cold Z006 row.
        {"node_id": "targets", "row_id": "t98", "op": "insert",
         "values": {"region": "Z006", "bucket": "warm"}},
    ]}
    batch4 = {"expected_version": 4, "corrections": [
        # R2 copies drop back 2 -> 1 (decrease, not to zero).
        {"node_id": "targets", "row_id": "t10", "op": "delete"},
        # Z007/warm appears (it will be withdrawn again at v6).
        {"node_id": "targets", "row_id": "t99", "op": "insert",
         "values": {"region": "Z007", "bucket": "warm"}},
    ]}
    batch5 = {"expected_version": 5, "corrections": [
        # Z007/warm withdrawn inside the window -> net 0 over (v4, v6].
        {"node_id": "targets", "row_id": "t99", "op": "delete"},
        # Z006/warm, present since the v4 baseline, is removed -> -1.
        {"node_id": "targets", "row_id": "t98", "op": "delete"},
        # R4 business row replaced: bucket warm -> hot.
        {"node_id": "targets", "row_id": "t4", "op": "replace",
         "values": {"region": "R4", "bucket": "hot"}},
    ]}
    batch6 = {"expected_version": 6, "corrections": [
        {"node_id": "orders", "row_id": "oz10", "op": "replace",
         "values": {"acc_id": "C10", "region": "Z010", "amount": 111}},
    ]}
    batch7 = {"expected_version": 7, "corrections": [
        {"node_id": "orders", "row_id": "oz11", "op": "replace",
         "values": {"acc_id": "C11", "region": "Z011", "amount": 112}},
    ]}
    return batch3, batch4, batch5, batch6, batch7


def replay_to_v6(db, graph, plan):
    """init + enable + publish v2..v6; return per-version release snapshots."""
    initial = load_json("initial.json")
    initialize_workspace(db, graph, initial)
    enable_subscription(db, graph, RETENTION)
    truth = {nid: [dict(r) for r in rows]
             for nid, rows in initial["sources"].items()}
    snapshots = {}
    version, rows = read_release(db, graph)
    snapshots[version] = normalize_release(rows, plan)
    batches = [load_json("batch1.json"), load_json("batch2.json"),
               *custom_batches()[:3]]
    for batch in batches:
        apply_repair_batch(db, graph, batch)
        truth = apply_corrections(truth, plan, batch["corrections"])
        version, rows = read_release(db, graph)
        snapshots[version] = normalize_release(rows, plan)
        # Independent cross-check: stored release == naive full recompute.
        check(snapshots[version] == release_multiset(truth, plan),
              f"v{version} release matches naive full compute")
    assert version == 6
    return snapshots


def fetch_all_pages(db, graph, from_version, page_size):
    """Consumer loop: first page by from_version, then follow cursors."""
    pages = []
    page = read_subscription(db, graph, from_version=from_version,
                             page_size=page_size)
    pages.append(page)
    while page["has_more"]:
        page = read_subscription(db, graph, cursor=page["cursor"],
                                 page_size=page_size)
        pages.append(page)
    return pages


def delivered_net(pages):
    net = Counter()
    for page in pages:
        for change in page["changes"]:
            net[row_key_of(change["row"])] += change["delta"]
    return net


def expected_net(snapshots, from_v, to_v):
    net = Counter()
    net.update(snapshots[to_v])
    net.subtract(snapshots[from_v])
    return Counter({k: d for k, d in net.items() if d != 0})


# ---------------------------------------------------------------------------
# 1. Lagging consumer: manual per-version copy verification, three showcase
#    phenomena, exactly-once paged delivery, baseline + deltas == target.
# ---------------------------------------------------------------------------
def test_lagging_consumer() -> None:
    print("== 1. consumer lags v4 -> v6: net copies checked by hand ==")
    graph = load_json("graph.json")
    plan = load_repair_plan(graph)
    db = fresh_db("sub_main.db")
    snapshots = replay_to_v6(db, graph, plan)

    r2 = row("R2", 60, 2, "hot")
    z006_warm = row("Z006", 106, 1, "warm")
    z007_warm = row("Z007", 107, 1, "warm")
    r4_warm = row("R4", 150, 3, "warm")
    r4_hot = row("R4", 150, 3, "hot")

    # --- hand-checked per-version copy counts ---------------------------
    check(snapshots[4][row_key_of(r2)] == 2 and
          snapshots[5][row_key_of(r2)] == 1 and
          snapshots[6][row_key_of(r2)] == 1,
          "copies decrease but not to zero: R2 row 2 -> 1 copies")
    check(snapshots[4][row_key_of(z007_warm)] == 0 and
          snapshots[5][row_key_of(z007_warm)] == 1 and
          snapshots[6][row_key_of(z007_warm)] == 0,
          "cross-version cancellation: Z007/warm appears at v5, gone at v6")
    check(snapshots[4][row_key_of(r4_warm)] == 1 and
          snapshots[6][row_key_of(r4_warm)] == 0 and
          snapshots[4][row_key_of(r4_hot)] == 0 and
          snapshots[6][row_key_of(r4_hot)] == 1,
          "row replacement: R4 bucket warm -> hot between v4 and v6")
    check(snapshots[4][row_key_of(z006_warm)] == 1 and
          snapshots[6][row_key_of(z006_warm)] == 0,
          "Z006/warm existed at baseline v4 and is gone by v6")

    # The per-version history keeps both sides of the cancellation...
    conn = sqlite3.connect(db)
    z007_rows = conn.execute(
        f"SELECT version, delta FROM {SUB_DELTA_TABLE} "
        "WHERE row_key LIKE '%Z007%' ORDER BY version").fetchall()
    conn.close()
    check(z007_rows == [(5, 1), (6, -1)],
          "per-version history stores +1 then -1 for the cancelled row",
          str(z007_rows))

    # --- paged consumption ----------------------------------------------
    pages = fetch_all_pages(db, graph, from_version=4, page_size=2)
    check(len(pages) == 2, "4 changes over page_size=2 yield 2 pages",
          str([len(p["changes"]) for p in pages]))
    check(all(p["from_version"] == 4 and p["to_version"] == 6
              for p in pages),
          "every page pins the same fixed start and target")
    check([len(p["changes"]) for p in pages] == [2, 2],
          "page sizes bounded by page_size")
    check(pages[-1]["has_more"] is False and
          pages[-1]["to_version"] == 6,
          "final page closes the interval at the pinned target")

    net = delivered_net(pages)
    check(net == expected_net(snapshots, 4, 6),
          "delivered net == release multiset(v6) - multiset(v4)",
          f"delivered={dict(net)} expected={dict(expected_net(snapshots, 4, 6))}")
    check(net[row_key_of(r2)] == -1,
          "decreased row delivered as a single -1 (not a delete+insert)")
    check(row_key_of(z007_warm) not in net,
          "cancelled row never delivered: no pointless delete downstream")
    check(net[row_key_of(r4_warm)] == -1 and net[row_key_of(r4_hot)] == 1,
          "replacement delivered as old row -1 and new row +1")
    keys = [row_key_of(c["row"]) for p in pages for c in p["changes"]]
    check(len(keys) == len(set(keys)),
          "exactly-once: no change row appears on two pages")

    # Order is the stable canonical-row total order, page after page.
    as_delivered = [json.dumps(c["row"], sort_keys=True,
                               ensure_ascii=False, separators=(",", ":"))
                    for p in pages for c in p["changes"]]
    check(as_delivered == sorted(as_delivered),
          "changes arrive in the stable canonical-row order")

    # Re-fetching the same valid pages yields identical content.
    again_1 = read_subscription(db, graph, from_version=4, page_size=2)
    check(again_1 == pages[0], "re-fetching page 1 gives identical content")
    again_2 = read_subscription(db, graph, cursor=pages[0]["cursor"],
                                page_size=2)
    check(again_2 == pages[1],
          "re-fetching page 2 via the same cursor gives identical content")

    # The consumer's stored baseline + the delivered deltas reproduce the
    # v6 release exactly -- the reconciliation contract end to end.
    consumer = Counter(snapshots[4])
    for change in (c for p in pages for c in p["changes"]):
        consumer[row_key_of(change["row"])] += change["delta"]
        assert consumer[row_key_of(change["row"])] >= 0
    consumer = Counter({k: c for k, c in consumer.items() if c != 0})
    check(consumer == Counter(snapshots[6]),
          "baseline(v4) + net changes == release multiset(v6)")


# ---------------------------------------------------------------------------
# 2. A publish lands after page 1: in-window start keeps its pinned target.
# ---------------------------------------------------------------------------
def test_publish_after_first_page_in_window() -> None:
    print("== 2. publish after page 1, start still inside the window ==")
    graph = load_json("graph.json")
    plan = load_repair_plan(graph)
    db = fresh_db("sub_continue.db")
    snapshots = replay_to_v6(db, graph, plan)
    batch6 = custom_batches()[3]

    page1 = read_subscription(db, graph, from_version=4, page_size=2)
    check(page1["to_version"] == 6, "page 1 pins target v6")
    apply_repair_batch(db, graph, batch6)   # workspace now at v7
    truth_note = read_release(db, graph)[0]
    check(truth_note == 7, "next version committed after page 1")

    # Retention window at v7: earliest = max(1, 7-3) = 4 -> start still valid.
    page2 = read_subscription(db, graph, cursor=page1["cursor"], page_size=2)
    check(page2["from_version"] == 4 and page2["to_version"] == 6,
          "in-progress read keeps its pinned target after a new commit")
    check(page2["has_more"] is False,
          "interval closes at the pinned target, not at the new commit")
    net = delivered_net([page1, page2])
    check(net == expected_net(snapshots, 4, 6),
          "collected pages still describe exactly v4 -> v6 (v7 excluded)")
    check(all("Z010" not in json.dumps(c["row"]) for c in
              [c for p in (page1, page2) for c in p["changes"]]),
          "the v7 change never leaks into the in-progress read")

    # A *new* read started now pins the newer target v7 and does see it.
    fresh = fetch_all_pages(db, graph, from_version=4, page_size=100)
    fresh_net = delivered_net(fresh)
    combined = Counter(expected_net(snapshots, 4, 6))
    combined.update({row_key_of(row("Z010", 111, 1, "cold")): 1,
                     row_key_of(row("Z010", 110, 1, "cold")): -1})
    combined = Counter({k: d for k, d in combined.items() if d != 0})
    check(fresh_net == combined,
          "a freshly started read pins the new target v7")
    check(fresh[0]["to_version"] == 7, "new read targets v7")


# ---------------------------------------------------------------------------
# 3. A publish lands after page 1: expired start must re-baseline.
# ---------------------------------------------------------------------------
def test_publish_after_first_page_expired() -> None:
    print("== 3. publish after page 1, start slid out of the window ==")
    graph = load_json("graph.json")
    plan = load_repair_plan(graph)
    db = fresh_db("sub_expired.db")
    replay_to_v6(db, graph, plan)
    batch6, batch7 = custom_batches()[3:5]

    page1 = read_subscription(db, graph, from_version=4, page_size=2)
    apply_repair_batch(db, graph, batch6)   # v7
    apply_repair_batch(db, graph, batch7)   # v8; earliest = max(1, 8-3) = 5

    try:
        read_subscription(db, graph, cursor=page1["cursor"], page_size=2)
        check(False, "expired start: continuation raises instead of "
              "delivering a truncated page")
    except SubscriptionError as exc:
        check(exc.kind == "expired" and "baseline" in str(exc),
              "expired start: continuation demands a full re-baseline",
              f"kind={exc.kind} msg={exc}")
    try:
        read_subscription(db, graph, from_version=4)
        check(False, "expired start: fresh read raises")
    except SubscriptionError as exc:
        check(exc.kind == "expired",
              "expired start: a fresh read from v4 is also rejected")
    try:
        read_subscription(db, graph, from_version=1)
        check(False, "expired enable-time start raises")
    except SubscriptionError as exc:
        check(exc.kind == "expired",
              "even the enable-time start expires once the window passes")

    # The retention window physically collected old deltas.
    conn = sqlite3.connect(db)
    lo, hi = conn.execute(
        f"SELECT MIN(version), MAX(version) FROM {SUB_DELTA_TABLE}"
    ).fetchone()
    stale = conn.execute(
        f"SELECT COUNT(*) FROM {SUB_DELTA_TABLE} WHERE version <= 5"
    ).fetchone()[0]
    conn.close()
    check((lo, hi) == (6, 8) and stale == 0,
          "deltas older than the retention window are deleted "
          f"(kept versions {lo}..{hi})")

    # Recovery path: full baseline at the current version, then subscribe.
    version, rows = read_release(db, graph)
    check(version == 8, "re-baseline reads the current version v8")
    page = read_subscription(db, graph, from_version=8)
    check(page["changes"] == [] and page["to_version"] == 8 and
          page["has_more"] is False,
          "after re-baselining at v8 the consumer is caught up "
          "(empty page at target v8)")


# ---------------------------------------------------------------------------
# 4. Empty pages: caught-up consumer and a publish with no release change.
# ---------------------------------------------------------------------------
def test_empty_pages() -> None:
    print("== 4. empty results still deliver the target version ==")
    graph = load_json("graph.json")
    plan = load_repair_plan(graph)
    db = fresh_db("sub_empty.db")
    replay_to_v6(db, graph, plan)

    page = read_subscription(db, graph, from_version=6)
    check(page["changes"] == [] and page["to_version"] == 6 and
          page["has_more"] is False and "cursor" not in page,
          "already-caught-up consumer gets an empty page at v6")

    # A correction that never reaches the release (no matching account)
    # still advances the version, with zero subscription rows recorded.
    ghost = {"expected_version": 6, "corrections": [
        {"node_id": "orders", "row_id": "o900", "op": "insert",
         "values": {"acc_id": "NOACC", "region": "R1", "amount": 5}},
    ]}
    apply_repair_batch(db, graph, ghost)
    conn = sqlite3.connect(db)
    n = conn.execute(
        f"SELECT COUNT(*) FROM {SUB_DELTA_TABLE} WHERE version = 7"
    ).fetchone()[0]
    conn.close()
    check(n == 0, "release-neutral publish records no delta rows")
    page = read_subscription(db, graph, from_version=6)
    check(page["changes"] == [] and page["to_version"] == 7,
          "empty delta version still lets the consumer advance to v7")


# ---------------------------------------------------------------------------
# 5. Rejected / rolled-back repairs never move subscription-visible state.
# ---------------------------------------------------------------------------
def test_rejected_repairs_keep_subscription_state() -> None:
    print("== 5. rejected corrections and rollbacks change nothing ==")
    graph = load_json("graph.json")
    plan = load_repair_plan(graph)
    db = fresh_db("sub_reject.db")
    replay_to_v6(db, graph, plan)

    def sub_state():
        conn = sqlite3.connect(db)
        rows = conn.execute(
            f"SELECT version, row_key, delta FROM {SUB_DELTA_TABLE} "
            "ORDER BY version, row_key").fetchall()
        version = conn.execute(
            "SELECT value FROM meta WHERE key='version'").fetchone()[0]
        conn.close()
        return int(version), rows

    before_version, before_rows = sub_state()
    before_page = read_subscription(db, graph, from_version=4, page_size=2)

    # (a) stale expected version
    try:
        apply_repair_batch(db, graph, {"expected_version": 99,
                                       "corrections": [
                                           {"node_id": "orders",
                                            "row_id": "o3", "op": "delete"}]})
        check(False, "stale batch rejected")
    except RepairConflictError:
        check(True, "stale batch rejected")
    # (b) insert of an existing row
    try:
        apply_repair_batch(db, graph, {"expected_version": 6, "corrections": [
            {"node_id": "orders", "row_id": "o3", "op": "insert",
             "values": {"acc_id": "A", "region": "R2", "amount": 1}}]})
        check(False, "conflicting insert rejected")
    except RepairConflictError:
        check(True, "conflicting insert rejected")
    # (c) validation failure (null value)
    try:
        apply_repair_batch(db, graph, {"expected_version": 6, "corrections": [
            {"node_id": "orders", "row_id": "o3", "op": "replace",
             "values": {"acc_id": None, "region": "R2", "amount": 1}}]})
        check(False, "invalid batch rejected")
    except RepairValidationError:
        check(True, "invalid batch rejected")
    # (d) failure injected after writes began, before COMMIT
    class Boom(Exception):
        pass

    def failpoint(node_id):
        if node_id == "j_settle":
            raise Boom("injected after writes began")

    ws = RepairWorkspace(db, plan, AccessLedger())
    try:
        ws.apply({"expected_version": 6, "corrections": [
            {"node_id": "orders", "row_id": "o3", "op": "replace",
             "values": {"acc_id": "A", "region": "R2", "amount": 35}}]},
            failpoint=failpoint)
        check(False, "failpoint fired")
    except Boom:
        check(True, "failpoint fired mid-transaction")
    ws.close()

    after_version, after_rows = sub_state()
    check(after_version == before_version == 6,
          "failed publishes never advance the version")
    check(after_rows == before_rows,
          "failed publishes never alter the recorded deltas or the "
          "retention boundary")
    after_page = read_subscription(db, graph, from_version=4, page_size=2)
    check(after_page == before_page,
          "subscription reads are byte-identical after rejected repairs")


# ---------------------------------------------------------------------------
# 6. Access scope: many untouched release rows, few changes.
# ---------------------------------------------------------------------------
def test_access_scope() -> None:
    print("== 6. access scope: only changed rows and their history ==")
    graph = load_json("graph.json")
    plan = load_repair_plan(graph)
    db = fresh_db("sub_scope.db")
    snapshots = replay_to_v6(db, graph, plan)

    release_rows = sum(snapshots[6].values())
    conn = sqlite3.connect(db)
    sub_rows = conn.execute(
        f"SELECT COUNT(*) FROM {SUB_DELTA_TABLE}").fetchone()[0]
    touched_regions = {r[0] for r in conn.execute(
        f"SELECT DISTINCT row_key FROM {SUB_DELTA_TABLE}")}
    conn.close()
    check(release_rows == 83 and sub_rows == 8,
          f"workspace holds {release_rows} release rows but only "
          f"{sub_rows} delta rows",
          f"release={release_rows} sub_delta={sub_rows}")
    leaked = [k for k in touched_regions
              if '"Z' in k and '"Z006"' not in k and '"Z007"' not in k]
    check(not leaked,
          "never-changing cold regions (78 of 80) never appear in the "
          "subscription store", str(leaked))

    # Read path: a fresh ledger proves the page read touches only sub_delta
    # and pulls at most page_size+1 candidate rows from SQLite.
    ledger = AccessLedger()
    page = read_subscription(db, graph, from_version=4, page_size=2,
                             ledger=ledger)
    targets = {t for _, t, _ in ledger.entries}
    check(targets == {SUB_DELTA_TABLE},
          "subscription read touches only the delta table "
          "(no release/source/provenance reads)", str(targets))
    fetched = [d for k, _, d in ledger.entries
               if k == "read_sub_delta" and d.startswith("fetched=")]
    check(fetched == ["fetched=3"],
          "server pulls at most page_size+1 rows (2+1), never the whole "
          "interval", str(fetched))
    check(len(page["changes"]) == 2 and page["has_more"],
          "page size bounds the delivered changes")

    # The paging query streams groups in index order (no temp b-tree, no
    # release-table access) -- verified from the query plan itself.
    conn = sqlite3.connect(db)
    plan_rows = conn.execute(
        f"EXPLAIN QUERY PLAN SELECT row_key, SUM(delta) AS net "
        f"FROM {SUB_DELTA_TABLE} INDEXED BY ix_sub_delta_key "
        "WHERE row_key > ? AND version > ? AND version <= ? "
        "GROUP BY row_key HAVING SUM(delta) <> 0 "
        "ORDER BY row_key LIMIT ?",
        ("", 4, 6, 3)).fetchall()
    conn.close()
    plan_text = " ".join(r[3] for r in plan_rows)
    check("ix_sub_delta_key" in plan_text and "TEMP B-TREE" not in plan_text,
          "page query streams via the (row_key, version) index",
          plan_text)

    # Write path: publishing one small batch appends exactly the changed
    # rows' deltas -- no release-table scan to find changes.
    ledger2 = AccessLedger()
    ws = RepairWorkspace(db, plan, ledger2)
    ws.apply(custom_batches()[3])   # v7: Z010 amount 110 -> 111
    ws.close()
    sub_writes = [d for k, _, d in ledger2.entries
                  if k == "write_sub_delta" and not d.startswith("gc")]
    check(len(sub_writes) == 2 and all("Z010" in d for d in sub_writes),
          "publish records exactly the two changed Z010 rows", sub_writes)
    check(not [e for e in ledger2.entries if e[0] == "full_scan"],
          "publish with subscription performs no full table scan")


# ---------------------------------------------------------------------------
# 7. Cursor and start validation.
# ---------------------------------------------------------------------------
def test_cursor_and_start_validation() -> None:
    print("== 7. cursor / start validation errors ==")
    graph = load_json("graph.json")
    plan = load_repair_plan(graph)
    db = fresh_db("sub_cursors.db")
    replay_to_v6(db, graph, plan)
    page1 = read_subscription(db, graph, from_version=4, page_size=2)

    def expect(kind, label, **kwargs):
        try:
            read_subscription(db, graph, **kwargs)
        except SubscriptionError as exc:
            check(exc.kind == kind, f"rejected: {label}",
                  f"kind={exc.kind} expected={kind}")
            return
        except Exception as other:  # noqa: BLE001
            check(False, f"rejected: {label}",
                  f"wrong exception {type(other).__name__}: {other}")
            return
        check(False, f"rejected: {label}", "read was accepted")

    expect("cursor_invalid", "garbage cursor", cursor="not-a-cursor")
    expect("cursor_invalid", "cursor with mismatched from_version",
           cursor=page1["cursor"], from_version=2)
    expect("invalid_start", "from_version 0", from_version=0)
    expect("invalid_start", "from_version ahead of current",
           from_version=99)
    expect("invalid_start", "from_version not an integer",
           from_version="4")
    expect("invalid_start", "neither from_version nor cursor")
    expect("invalid_page_size", "page_size 0", from_version=4, page_size=0)
    expect("invalid_page_size", "page_size too large", from_version=4,
           page_size=100000)

    # A cursor minted by another workspace is rejected clearly.
    other = fresh_db("sub_other.db")
    replay_to_v6(other, graph, plan)
    foreign = read_subscription(other, graph, from_version=4, page_size=2)
    expect("cursor_invalid", "cursor from another workspace",
           cursor=foreign["cursor"])

    # Re-enabling is refused, so cursors cannot silently jump subscriptions.
    try:
        enable_subscription(db, graph, 2)
        check(False, "rejected: enable twice")
    except SubscriptionError as exc:
        check(exc.kind == "already_enabled", "rejected: enable twice",
              exc.kind)


# ---------------------------------------------------------------------------
# 8. Enablement validation and non-enabled workspaces.
# ---------------------------------------------------------------------------
def test_enablement_and_disabled_workspace() -> None:
    print("== 8. enablement rules; disabled workspace unaffected ==")
    graph = load_json("graph.json")
    plan = load_repair_plan(graph)

    db = fresh_db("sub_disabled.db")
    initialize_workspace(db, graph, load_json("initial.json"))
    try:
        read_subscription(db, graph, from_version=1)
        check(False, "rejected: read on non-enabled workspace")
    except SubscriptionError as exc:
        check(exc.kind == "not_enabled",
              "rejected: read on non-enabled workspace", exc.kind)

    # Original commands work exactly as before on a non-enabled workspace.
    summary = apply_repair_batch(db, graph, load_json("batch1.json"))
    version, rows = read_release(db, graph)
    check(summary["version"] == 2 and version == 2 and
          normalize_release(rows, plan) ==
          release_multiset(
              apply_corrections(
                  {nid: [dict(r) for r in rows2]
                   for nid, rows2 in load_json("initial.json")
                   ["sources"].items()},
                  plan, load_json("batch1.json")["corrections"]), plan),
          "non-enabled workspace: apply/read behave as before")
    conn = sqlite3.connect(db)
    has_table = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (SUB_DELTA_TABLE,)).fetchone()
    conn.close()
    check(has_table is None,
          "non-enabled workspace has no subscription tables at all")

    # Enablement parameter validation.
    for bad in (0, -1, "3", True, 1.5):
        try:
            enable_subscription(db, graph, bad)
            check(False, f"rejected: retention {bad!r}")
        except RepairValidationError:
            check(True, f"rejected: retention {bad!r}")
    try:
        enable_subscription(os.path.join(TMP, "ghost.db"), graph, 3)
        check(False, "rejected: enable on missing workspace")
    except RepairValidationError:
        check(True, "rejected: enable on missing workspace")

    # Enabling an existing (already published) workspace needs no rebuild:
    # the current version becomes the earliest start, history is not
    # fabricated for earlier versions.
    info = enable_subscription(db, graph, 2)
    check(info["enabled_at_version"] == 2 and
          info["earliest_from_version"] == 2,
          "existing workspace enables in place at its current version")
    conn = sqlite3.connect(db)
    n = conn.execute(f"SELECT COUNT(*) FROM {SUB_DELTA_TABLE}").fetchone()[0]
    conn.close()
    check(n == 0, "no change history is fabricated for pre-enable versions")
    try:
        read_subscription(db, graph, from_version=1)
        check(False, "rejected: start before enablement")
    except SubscriptionError as exc:
        check(exc.kind == "invalid_start",
              "rejected: start before enablement", exc.kind)
    apply_repair_batch(db, graph, load_json("batch2.json"))
    page = read_subscription(db, graph, from_version=2)
    check(page["to_version"] == 3 and len(page["changes"]) > 0,
          "first post-enable publish becomes consumable")


# ---------------------------------------------------------------------------
# 9. CLI end-to-end (scriptable entry point, cursor survives restarts).
# ---------------------------------------------------------------------------
def test_cli() -> None:
    print("== 9. CLI: scriptable enable/read with cursor across processes ==")
    graph_path = os.path.join(DATA, "graph.json")
    db = fresh_db("sub_cli.db")
    replay_to_v6(db, load_json("graph.json"), load_repair_plan(
        load_json("graph.json")))

    def cli(*args):
        return subprocess.run(
            [sys.executable, "-I", os.path.join(ROOT, "bin", "lineage-diag"),
             *args],
            cwd=ROOT, capture_output=True, text=True)

    res = cli("repair", "subscribe", "read", "--graph", graph_path,
              "--db", db, "--from-version", "4", "--page-size", "2")
    check(res.returncode == 0, "CLI first page exits 0", res.stderr)
    page1 = json.loads(res.stdout)
    check(page1["from_version"] == 4 and page1["to_version"] == 6 and
          len(page1["changes"]) == 2 and page1["has_more"],
          "CLI first page payload")

    # A fresh process resumes purely from the cursor string.
    res = cli("repair", "subscribe", "read", "--graph", graph_path,
              "--db", db, "--cursor", page1["cursor"], "--page-size", "2")
    check(res.returncode == 0, "CLI continuation exits 0", res.stderr)
    page2 = json.loads(res.stdout)
    check(page2["to_version"] == 6 and len(page2["changes"]) == 2 and
          page2["has_more"] is False,
          "CLI continuation keeps the pinned target and closes the interval")

    res = cli("repair", "subscribe", "read", "--graph", graph_path,
              "--db", db, "--from-version", "6")
    page3 = json.loads(res.stdout)
    check(res.returncode == 0 and page3["changes"] == [] and
          page3["to_version"] == 6 and not page3["has_more"],
          "CLI empty page still delivers the target version")

    # Errors are clear and non-zero-exiting.
    disabled = fresh_db("sub_cli_disabled.db")
    initialize_workspace(disabled, load_json("graph.json"),
                         load_json("initial.json"))
    res = cli("repair", "subscribe", "read", "--graph", graph_path,
              "--db", disabled, "--from-version", "1")
    check(res.returncode == 2 and "subscription" in res.stderr,
          "CLI read on a non-enabled workspace fails clearly", res.stderr)
    res = cli("repair", "subscribe", "enable", "--graph", graph_path,
              "--db", disabled, "--retention", "0")
    check(res.returncode == 2 and "retention" in res.stderr,
          "CLI enable validates retention", res.stderr)
    res = cli("repair", "subscribe", "enable", "--graph", graph_path,
              "--db", disabled, "--retention", "3")
    check(res.returncode == 0 and
          json.loads(res.stdout)["enabled_at_version"] == 1,
          "CLI enable on an existing workspace", res.stderr)


def main() -> int:
    test_lagging_consumer()
    test_publish_after_first_page_in_window()
    test_publish_after_first_page_expired()
    test_empty_pages()
    test_rejected_repairs_keep_subscription_state()
    test_access_scope()
    test_cursor_and_start_validation()
    test_enablement_and_disabled_workspace()
    test_cli()
    print()
    if FAIL:
        print(f"{len(FAIL)} SUBSCRIPTION CHECK(S) FAILED:")
        for f in FAIL:
            print(" -", f)
        return 1
    print("ALL SUBSCRIPTION CHECKS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
