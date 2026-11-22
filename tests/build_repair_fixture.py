"""Build the repair fixture: graph + initial source rows + correction batches.

The fixture graph (all node types inside the executable subset) models a
settlement release with duplicated many-to-many matches and an aggregate
that itself participates in a downstream join::

    accounts(acc_id PK row id) ─┐
                                ├─ j_settle (acc_id = acc_id, inner)
    orders  (order_id row id) ─┘        │
                                         ├─ a_region (group by region)
                                         │      total_amt=SUM(amount),
                                         │      n=COUNT(*)
    targets (target_id row id) ── j_target (region = region) ── o_release

Correction batches deliberately:

* migrate an order's join key A -> B while the matched account row on both
  sides is corrected in the *same* batch (no old/new mixing allowed);
* make an old aggregation group disappear and a new one appear;
* let the downstream join/agg react to the changed aggregate row;
* keep ~80 disjoint region keys untouched so scope tests can prove those
  partitions are never read.
"""

from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(ROOT, "data", "repair")

GRAPH_VERSION = "repair-graph-3"

# Regions R1..R5 are the "hot" keys; R6.. are the large disjoint mass.
HOT_REGIONS = ["R1", "R2", "R3", "R4", "R5"]
DISJOINT_REGIONS = [f"Z{i:03d}" for i in range(80)]
ALL_REGIONS = HOT_REGIONS + DISJOINT_REGIONS


def build_graph() -> dict:
    return {
        "graph_version": GRAPH_VERSION,
        "nodes": [
            {
                "id": "orders", "type": "input", "table": "db.orders",
                "row_id": "order_id",
                "columns": [
                    {"name": "order_id", "type": "TEXT"},
                    {"name": "acc_id", "type": "TEXT"},
                    {"name": "region", "type": "TEXT"},
                    {"name": "amount", "type": "INTEGER"},
                ],
            },
            {
                "id": "accounts", "type": "input", "table": "db.accounts",
                "row_id": "acc_pk",
                "columns": [
                    {"name": "acc_pk", "type": "TEXT"},
                    {"name": "acc_id", "type": "TEXT"},
                    {"name": "owner", "type": "TEXT"},
                ],
            },
            {
                "id": "targets", "type": "input", "table": "db.targets",
                "row_id": "target_id",
                "columns": [
                    {"name": "target_id", "type": "TEXT"},
                    {"name": "region", "type": "TEXT"},
                    {"name": "bucket", "type": "TEXT"},
                ],
            },
            # rename / direct-carry projection over an input
            {
                "id": "p_orders", "type": "project", "input": "orders",
                "exprs": [
                    {"kind": "ref", "out_column": "order_acc",
                     "refs": ["orders.acc_id"], "raw": "orders.acc_id"},
                    {"kind": "ref", "out_column": "region",
                     "refs": ["orders.region"], "raw": "orders.region"},
                    {"kind": "ref", "out_column": "amount",
                     "refs": ["orders.amount"], "raw": "orders.amount"},
                ],
            },
            # many-to-many inner equi-join, duplicate matches preserved
            {
                "id": "j_settle", "type": "join",
                "left": "p_orders", "right": "accounts",
                "join_type": "inner",
                "on": [
                    {"kind": "op", "op": "=", "out_column": "_on_acc",
                     "refs": ["p_orders.order_acc", "accounts.acc_id"],
                     "raw": "p_orders.order_acc = accounts.acc_id",
                     "role_override": "join_on"},
                ],
                "exprs": [
                    {"kind": "ref", "out_column": "acc_id",
                     "refs": ["p_orders.order_acc"],
                     "raw": "p_orders.order_acc AS acc_id"},
                    {"kind": "ref", "out_column": "matched_acc",
                     "refs": ["accounts.acc_id"],
                     "raw": "accounts.acc_id AS matched_acc"},
                    {"kind": "ref", "out_column": "region",
                     "refs": ["p_orders.region"], "raw": "p_orders.region"},
                    {"kind": "ref", "out_column": "amount",
                     "refs": ["p_orders.amount"], "raw": "p_orders.amount"},
                    {"kind": "ref", "out_column": "owner",
                     "refs": ["accounts.owner"], "raw": "accounts.owner"},
                ],
            },
            # grouped integer SUM and COUNT(*)
            {
                "id": "a_region", "type": "agg", "input": "j_settle",
                "group_by": ["j_settle.region"],
                "measures": [
                    {"func": "sum", "out_column": "total_amt",
                     "ref": "j_settle.amount"},
                    {"func": "count", "out_column": "n"},
                ],
                "exprs": [
                    {"kind": "ref", "out_column": "region",
                     "refs": ["j_settle.region"],
                     "raw": "j_settle.region (group key)"},
                    {"kind": "op", "op": "sum", "out_column": "total_amt",
                     "refs": ["j_settle.amount"],
                     "raw": "sum(j_settle.amount)"},
                    {"kind": "op", "op": "count", "out_column": "n",
                     "refs": [], "raw": "count(*)"},
                ],
            },
            # aggregate result participates in another join
            {
                "id": "j_target", "type": "join",
                "left": "a_region", "right": "targets",
                "on": [
                    {"kind": "op", "op": "=", "out_column": "_on_region",
                     "refs": ["a_region.region", "targets.region"],
                     "raw": "a_region.region = targets.region",
                     "role_override": "join_on"},
                ],
                "exprs": [
                    {"kind": "ref", "out_column": "region",
                     "refs": ["a_region.region"], "raw": "a_region.region"},
                    {"kind": "ref", "out_column": "target_region",
                     "refs": ["targets.region"],
                     "raw": "targets.region AS target_region"},
                    {"kind": "ref", "out_column": "total_amt",
                     "refs": ["a_region.total_amt"],
                     "raw": "a_region.total_amt"},
                    {"kind": "ref", "out_column": "n",
                     "refs": ["a_region.n"], "raw": "a_region.n"},
                    {"kind": "ref", "out_column": "bucket",
                     "refs": ["targets.bucket"], "raw": "targets.bucket"},
                ],
            },
            {
                "id": "o_release", "type": "output",
                "table": "release_settlement", "input": "j_target",
                "exprs": [
                    {"kind": "ref", "out_column": "region",
                     "refs": ["j_target.region"], "raw": "j_target.region"},
                    {"kind": "ref", "out_column": "total_amt",
                     "refs": ["j_target.total_amt"],
                     "raw": "j_target.total_amt"},
                    {"kind": "ref", "out_column": "n",
                     "refs": ["j_target.n"], "raw": "j_target.n"},
                    {"kind": "ref", "out_column": "bucket",
                     "refs": ["j_target.bucket"], "raw": "j_target.bucket"},
                ],
            },
        ],
    }


def build_initial_sources() -> dict:
    orders = [
        # Hot regions with duplicated matches on purpose.
        {"order_id": "o1", "acc_id": "A", "region": "R1", "amount": 10},
        {"order_id": "o2", "acc_id": "A", "region": "R1", "amount": 20},
        {"order_id": "o3", "acc_id": "A", "region": "R2", "amount": 30},
        {"order_id": "o4", "acc_id": "B", "region": "R3", "amount": 40},
        {"order_id": "o5", "acc_id": "B", "region": "R4", "amount": 50},
    ]
    # Two account rows per hot id -> every matching order fans out 2 ways.
    accounts = []
    pk = 0
    for acc_id, owner in (("A", "ann"), ("B", "ben")):
        for _ in range(2):
            pk += 1
            accounts.append({"acc_pk": f"a{pk}", "acc_id": acc_id,
                             "owner": owner})
    targets = [
        {"target_id": "t1", "region": "R1", "bucket": "hot"},
        {"target_id": "t2", "region": "R2", "bucket": "hot"},
        {"target_id": "t3", "region": "R3", "bucket": "hot"},
        {"target_id": "t4", "region": "R4", "bucket": "hot"},
        # no target for R5 initially
    ]
    # Disjoint mass: one order per region, one account C* each, one target.
    for i, region in enumerate(DISJOINT_REGIONS):
        orders.append({"order_id": f"oz{i}", "acc_id": f"C{i}",
                       "region": region, "amount": 100 + i})
        pk += 1
        accounts.append({"acc_pk": f"a{pk}", "acc_id": f"C{i}",
                         "owner": f"owner{i}"})
        targets.append({"target_id": f"tz{i}", "region": region,
                        "bucket": "cold"})
    return {"orders": orders, "accounts": accounts, "targets": targets}


def build_batch_one() -> dict:
    """Cross-key migration with both sides corrected simultaneously.

    * o1 moves A -> B and R1 -> R5 (join key + group key change at once);
    * o2 is deleted so region R1 loses its last contribution (group dies);
    * accounts: an A row is replaced and a fresh B row is inserted, i.e. both
      sides of j_settle change inside the same batch;
    * a R5 target is inserted so the newborn group reaches the release.
    """
    return {
        "expected_version": 1,
        "corrections": [
            {"node_id": "orders", "row_id": "o1", "op": "replace",
             "values": {"acc_id": "B", "region": "R5", "amount": 10}},
            {"node_id": "orders", "row_id": "o2", "op": "delete"},
            {"node_id": "accounts", "row_id": "a1", "op": "replace",
             "values": {"acc_id": "A", "owner": "ann2"}},
            {"node_id": "accounts", "row_id": "a900", "op": "insert",
             "values": {"acc_id": "B", "owner": "ben2"}},
            {"node_id": "targets", "row_id": "t5", "op": "insert",
             "values": {"region": "R5", "bucket": "hot"}},
        ],
    }


def build_batch_two() -> dict:
    """Second batch: insert, delete, replace across inputs, bound at v2."""
    return {
        "expected_version": 2,
        "corrections": [
            {"node_id": "orders", "row_id": "o6", "op": "insert",
             "values": {"acc_id": "B", "region": "R3", "amount": 70}},
            {"node_id": "orders", "row_id": "o4", "op": "replace",
             "values": {"acc_id": "B", "region": "R3", "amount": 44}},
            {"node_id": "targets", "row_id": "t3", "op": "delete"},
            {"node_id": "targets", "row_id": "t4", "op": "replace",
             "values": {"region": "R4", "bucket": "warm"}},
        ],
    }


def main() -> None:
    os.makedirs(DATA, exist_ok=True)
    payloads = {
        "graph.json": build_graph(),
        "initial.json": {"sources": build_initial_sources()},
        "batch1.json": build_batch_one(),
        "batch2.json": build_batch_two(),
    }
    for name, payload in payloads.items():
        with open(os.path.join(DATA, name), "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2)
    print("wrote repair fixtures to", DATA)


if __name__ == "__main__":
    main()
