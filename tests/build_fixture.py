"""Build the fixture artifacts used by the independent verifier.

The fixture deliberately packs every mandated phenomenon into one graph:

* conditional branches (b_gate arm b1 never executed; b_route arm take_exec
  executed, arm skip_emit not executed);
* same-named join keys (t1.uid and t2.uid both feed j_user.uid, keeping
  distinct source identities);
* aggregation (a_sum over j_user) and explode (e_tags over t3.tags);
* a rename edge (t1.uid -> p_filt.uid -> j_user.uid_copy);
* a shadowed/coalesced column (p_agg.country hides j_user.country);
* a runtime node with no record at all (p_note -> note output, UNPROVABLE);
* partial evidence (owner_id, country: one proven path, one unproven);
* a positively read column that reaches no output (p_dbg.dbg_audit,
  NO_CONTRIBUTION);
* declared dependencies behind un-executed arms (overseas_flag,
  NOT_EXECUTED).

The script emits graph/runtime/package JSON plus a hand-written static
dependency inventory (expected_static_paths.json) derived independently of
the package's traversal code.
"""

from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lineage_diag.digest import digest_payload, schema_digest  # noqa: E402

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))), "data")

GRAPH_VERSION = "graph-7"
RUN_VERSION = "run-77"


def build_graph() -> dict:
    return {
        "graph_version": GRAPH_VERSION,
        "nodes": [
            {
                "id": "t1", "type": "input", "table": "db.orders",
                "columns": [
                    {"name": "uid"}, {"name": "amount"},
                    {"name": "region"}, {"name": "overseas_flag"},
                ],
            },
            {
                "id": "t2", "type": "input", "table": "db.users",
                "columns": [
                    {"name": "uid"}, {"name": "owner_id"},
                    {"name": "country"}, {"name": "country_code"},
                ],
            },
            {
                "id": "t3", "type": "input", "table": "db.user_tags",
                "columns": [{"name": "uid"}, {"name": "tags"}],
            },
            {
                "id": "p_filt", "type": "project", "input": "t1",
                "exprs": [
                    {"kind": "ref", "out_column": "uid",
                     "refs": ["t1.uid"], "raw": "t1.uid"},
                    {"kind": "op", "op": ">", "out_column": "amount",
                     "refs": ["t1.amount"], "raw": "t1.amount > 0"},
                    {"kind": "ref", "out_column": "region",
                     "refs": ["t1.region"], "raw": "t1.region"},
                    {"kind": "ref", "out_column": "overseas_flag",
                     "refs": ["t1.overseas_flag"],
                     "raw": "t1.overseas_flag"},
                ],
            },
            {
                "id": "j_user", "type": "join",
                "left": "p_filt", "right": "t2",
                "on": [
                    {"kind": "op", "op": "=", "out_column": "_on_uid",
                     "refs": ["p_filt.uid", "t2.uid"],
                     "raw": "p_filt.uid = t2.uid", "role_override": "join_on"},
                ],
                "exprs": [
                    {"kind": "ref", "out_column": "uid",
                     "refs": ["t2.uid"], "raw": "t2.uid AS uid"},
                    {"kind": "ref", "out_column": "uid_copy",
                     "refs": ["p_filt.uid"], "raw": "p_filt.uid AS uid_copy"},
                    {"kind": "op", "op": "coalesce",
                     "out_column": "owner_id",
                     "refs": ["t2.owner_id", "p_filt.uid"],
                     "raw": "coalesce(t2.owner_id, p_filt.uid)"},
                    {"kind": "ref", "out_column": "amount",
                     "refs": ["p_filt.amount"], "raw": "p_filt.amount"},
                    {"kind": "ref", "out_column": "region",
                     "refs": ["p_filt.region"], "raw": "p_filt.region"},
                    {"kind": "op", "op": "coalesce",
                     "out_column": "country",
                     "refs": ["t2.country", "t2.country_code"],
                     "raw": "coalesce(t2.country, t2.country_code)"},
                    {"kind": "ref", "out_column": "overseas_flag",
                     "refs": ["p_filt.overseas_flag"],
                     "raw": "p_filt.overseas_flag"},
                ],
            },
            {
                "id": "a_sum", "type": "agg", "input": "j_user",
                "exprs": [
                    {"kind": "op", "op": "sum", "out_column": "total_amt",
                     "refs": ["j_user.amount"], "raw": "sum(j_user.amount)"},
                    {"kind": "ref", "out_column": "region",
                     "refs": ["j_user.region"],
                     "raw": "j_user.region (group key)"},
                    {"kind": "ref", "out_column": "country",
                     "refs": ["j_user.country"],
                     "raw": "j_user.country (group key)"},
                ],
            },
            {
                "id": "b_gate", "type": "branch", "input": "j_user",
                "condition": [
                    {"kind": "op", "op": "=", "out_column": "_cond",
                     "refs": ["j_user.overseas_flag"],
                     "raw": "j_user.overseas_flag = 1"},
                ],
                "branches": [
                    {"id": "b1", "target": "p_over",
                     "label": "overseas detail"},
                ],
            },
            {
                "id": "p_over", "type": "project", "input": "j_user",
                "exprs": [
                    {"kind": "ref", "out_column": "overseas_flag",
                     "refs": ["j_user.overseas_flag"],
                     "raw": "j_user.overseas_flag"},
                ],
            },
            {
                "id": "e_tags", "type": "explode", "input": "t3",
                "ref": "t3.tags", "out_column": "tag_name",
                "passthrough": [
                    {"kind": "ref", "out_column": "uid",
                     "refs": ["t3.uid"], "raw": "t3.uid"},
                ],
            },
            {
                "id": "p_agg", "type": "project", "input": "a_sum",
                "exprs": [
                    {"kind": "ref", "out_column": "total_amt",
                     "refs": ["a_sum.total_amt"], "raw": "a_sum.total_amt"},
                    {"kind": "ref", "out_column": "region",
                     "refs": ["a_sum.region"], "raw": "a_sum.region"},
                    # shadowing: output name 'country' but source identity is
                    # explicitly a_sum.country, never matched by bare name
                    {"kind": "op", "op": "upper", "out_column": "country",
                     "refs": ["a_sum.country"],
                     "raw": "upper(a_sum.country)"},
                ],
            },
            {
                "id": "p_note", "type": "project", "input": "j_user",
                "exprs": [
                    {"kind": "op", "op": "lower", "out_column": "note",
                     "refs": ["j_user.country"],
                     "raw": "lower(j_user.country)"},
                ],
            },
            {
                "id": "p_dbg", "type": "project", "input": "j_user",
                "exprs": [
                    {"kind": "op", "op": "hash", "out_column": "dbg_audit",
                     "refs": ["j_user.uid"],
                     "raw": "hash(j_user.uid) -- audit only, never released"},
                ],
            },
            {
                "id": "b_route", "type": "branch", "input": "p_agg",
                "condition": [
                    {"kind": "op", "op": "is_not_null",
                     "out_column": "_cond",
                     "refs": ["p_agg.total_amt"],
                     "raw": "p_agg.total_amt is not null"},
                ],
                "branches": [
                    {"id": "take_exec", "target": "o_rel",
                     "label": "emit release rows"},
                    {"id": "skip_emit", "target": "o_dead",
                     "label": "suppressed alternative"},
                ],
            },
            {
                "id": "o_rel", "type": "output", "table": "release_user_metrics",
                "input": "p_agg",
                "extra_inputs": ["j_user", "e_tags", "p_note", "p_over"],
                "exprs": [
                    {"kind": "ref", "out_column": "uid_copy",
                     "refs": ["p_agg.region"],
                     "raw": "__placeholder__"},
                ],
            },
            {
                "id": "o_dead", "type": "output", "table": "release_suppressed",
                "input": "p_agg",
                "exprs": [
                    {"kind": "ref", "out_column": "would_be",
                     "refs": ["p_agg.total_amt"],
                     "raw": "p_agg.total_amt"},
                ],
            },
        ],
    }


# The real release output collects columns from several upstream nodes.
# It is rebuilt below with explicit, correctly qualified expressions.
def _finalize_graph(raw: dict) -> dict:
    for node in raw["nodes"]:
        if node["id"] == "o_rel":
            node["exprs"] = [
                {"kind": "ref", "out_column": "uid_copy",
                 "refs": ["j_user.uid_copy"], "raw": "j_user.uid_copy"},
                {"kind": "ref", "out_column": "total_amt",
                 "refs": ["p_agg.total_amt"], "raw": "p_agg.total_amt"},
                {"kind": "ref", "out_column": "region",
                 "refs": ["p_agg.region"], "raw": "p_agg.region"},
                {"kind": "ref", "out_column": "country",
                 "refs": ["p_agg.country"], "raw": "p_agg.country"},
                {"kind": "ref", "out_column": "owner_id",
                 "refs": ["j_user.owner_id"], "raw": "j_user.owner_id"},
                {"kind": "ref", "out_column": "tag_name",
                 "refs": ["e_tags.tag_name"], "raw": "e_tags.tag_name"},
                {"kind": "ref", "out_column": "note",
                 "refs": ["p_note.note"], "raw": "p_note.note"},
                {"kind": "ref", "out_column": "overseas_flag",
                 "refs": ["p_over.overseas_flag"],
                 "raw": "p_over.overseas_flag"},
            ]
    return raw


def build_runtime(schema_digest_value: str) -> dict:
    return {
        "graph_version": GRAPH_VERSION,
        "run_version": RUN_VERSION,
        "package_digest": schema_digest_value,
        "nodes": {
            "t1": {
                "executed": True,
                "read_positive": ["t1.uid", "t1.amount", "t1.region",
                                  "t1.overseas_flag"],
                "read_negative": [],
            },
            "t2": {
                "executed": True,
                "read_positive": ["t2.uid", "t2.owner_id", "t2.country",
                                  "t2.country_code"],
                "read_negative": [],
            },
            "t3": {
                "executed": True,
                "read_positive": ["t3.uid", "t3.tags"],
                "read_negative": [],
            },
            "p_filt": {
                "executed": True,
                # p_filt.uid is deliberately absent from both lists: missing
                # runtime evidence (NOT the same as a negative read).
                "read_positive": ["p_filt.amount",
                                  "p_filt.region", "p_filt.overseas_flag"],
                "read_negative": [],
            },
            "j_user": {
                "executed": True,
                # j_user.uid (the t2-sourced join key) is produced but is not
                # itself read as a value column this run; its absence from
                # read_positive is legitimate and must not create a finding.
                "read_positive": ["j_user.uid_copy",
                                  "j_user.owner_id",
                                  "j_user.amount", "j_user.region",
                                  "j_user.country", "j_user.overseas_flag"],
                # explicit edge-level negative read: the t2.country_code
                # branch of the coalesce was not used while j_user.country
                # itself was positively read.
                "read_negative": [],
                "read_negative_refs": ["t2.country_code"],
            },
            "a_sum": {
                "executed": True,
                "read_positive": ["a_sum.total_amt", "a_sum.region",
                                  "a_sum.country"],
                "read_negative": [],
            },
            "b_gate": {
                "executed": True,
                "branches_taken": [],
                "branches_skipped": ["b1"],
                "read_positive": [],
                "read_negative": [],
            },
            "p_over": {
                "executed": False,
                "branches_taken": [],
                "branches_skipped": [],
                "read_positive": [],
                "read_negative": ["p_over.overseas_flag"],
            },
            "e_tags": {
                "executed": True,
                # only the exploded value is released; the passthrough uid
                # is produced but not positively read this run.
                "read_positive": ["e_tags.tag_name"],
                "read_negative": [],
            },
            "p_agg": {
                "executed": True,
                "read_positive": ["p_agg.total_amt", "p_agg.region",
                                  "p_agg.country"],
                "read_negative": [],
            },
            # p_note intentionally has NO runtime record entry at all:
            # the note output must be UNPROVABLE rather than assumed unused.
            "p_dbg": {
                "executed": True,
                "read_positive": ["p_dbg.dbg_audit"],
                "read_negative": [],
            },
            "b_route": {
                "executed": True,
                "branches_taken": ["take_exec"],
                "branches_skipped": ["skip_emit"],
                "read_positive": [],
                "read_negative": [],
            },
            "o_rel": {
                "executed": True,
                "read_positive": [
                    "o_rel.uid_copy", "o_rel.total_amt", "o_rel.region",
                    "o_rel.country", "o_rel.owner_id", "o_rel.tag_name",
                    "o_rel.note", "o_rel.overseas_flag",
                ],
                "read_negative": [],
            },
            "o_dead": {
                "executed": False,
                "read_positive": [],
                "read_negative": ["o_dead.would_be"],
            },
        },
        "output_samples": {
            "o_rel": {
                "uid_copy": digest_payload({"col": "uid_copy",
                                            "min": 1, "max": 99}),
                "total_amt": digest_payload({"col": "total_amt",
                                             "min": 0.0, "max": 8200.5}),
                "region": digest_payload({"col": "region",
                                          "distinct": 7}),
                "country": digest_payload({"col": "country",
                                           "distinct": 12}),
                "owner_id": digest_payload({"col": "owner_id",
                                            "nulls": 0}),
                "tag_name": digest_payload({"col": "tag_name",
                                            "distinct": 203}),
                "note": digest_payload({"col": "note", "len_max": 64}),
                "overseas_flag": digest_payload({"col": "overseas_flag",
                                                 "nulls": 3}),
            }
        },
    }


def build_package() -> dict:
    columns = [
        ("uid_copy", "BIGINT", {"col": "uid_copy", "min": 1, "max": 99}),
        ("total_amt", "DECIMAL(18,2)",
         {"col": "total_amt", "min": 0.0, "max": 8200.5}),
        ("region", "VARCHAR(32)", {"col": "region", "distinct": 7}),
        ("country", "VARCHAR(64)", {"col": "country", "distinct": 12}),
        ("owner_id", "BIGINT", {"col": "owner_id", "nulls": 0}),
        ("tag_name", "VARCHAR(128)", {"col": "tag_name", "distinct": 203}),
        ("note", "VARCHAR(128)", {"col": "note", "len_max": 64}),
        ("overseas_flag", "TINYINT",
         {"col": "overseas_flag", "nulls": 3}),
    ]
    pkg_columns = [
        {"name": name, "type": typ, "sample_digest": digest_payload(summary)}
        for name, typ, summary in columns
    ]
    return {
        "package_name": "release_user_metrics",
        "package_version": "2026.11.0",
        "graph_version": GRAPH_VERSION,
        "run_version": RUN_VERSION,
        "output_node": "o_rel",
        "columns": pkg_columns,
        "schema_digest": schema_digest(
            [{"name": c["name"], "type": c["type"]} for c in pkg_columns]
        ),
    }


def build_expected_static_paths() -> dict:
    """Hand-written inventory, produced without importing traversal.py."""
    return {
        "description": "static possible column-level dependencies (all paths, "
                       "including ones behind un-executed branches)",
        "outputs": {
            "o_rel.uid_copy": [
                ["t1.uid", "p_filt.uid", "j_user.uid_copy",
                 "o_rel.uid_copy"],
            ],
            "o_rel.total_amt": [
                ["t1.amount", "p_filt.amount", "j_user.amount",
                 "a_sum.total_amt", "p_agg.total_amt", "o_rel.total_amt"],
            ],
            "o_rel.region": [
                ["t1.region", "p_filt.region", "j_user.region",
                 "a_sum.region", "p_agg.region", "o_rel.region"],
            ],
            "o_rel.country": [
                ["t2.country", "j_user.country", "a_sum.country",
                 "p_agg.country", "o_rel.country"],
                ["t2.country_code", "j_user.country", "a_sum.country",
                 "p_agg.country", "o_rel.country"],
            ],
            "o_rel.owner_id": [
                ["t2.owner_id", "j_user.owner_id", "o_rel.owner_id"],
                ["t1.uid", "p_filt.uid", "j_user.owner_id",
                 "o_rel.owner_id"],
            ],
            "o_rel.tag_name": [
                ["t3.tags", "e_tags.tag_name", "o_rel.tag_name"],
            ],
            "o_rel.note": [
                ["t2.country", "j_user.country", "p_note.note",
                 "o_rel.note"],
                ["t2.country_code", "j_user.country", "p_note.note",
                 "o_rel.note"],
            ],
            "o_rel.overseas_flag": [
                ["t1.overseas_flag", "p_filt.overseas_flag",
                 "j_user.overseas_flag", "p_over.overseas_flag",
                 "o_rel.overseas_flag"],
            ],
        },
        "gates": {
            # every released path passes through the executed routing arm;
            # the overseas path additionally sits behind the skipped b_gate.
            "o_rel.uid_copy": [["b_route", "take_exec"]],
            "o_rel.total_amt": [["b_route", "take_exec"]],
            "o_rel.region": [["b_route", "take_exec"]],
            "o_rel.country": [["b_route", "take_exec"]],
            "o_rel.owner_id": [["b_route", "take_exec"]],
            "o_rel.tag_name": [["b_route", "take_exec"]],
            "o_rel.note": [["b_route", "take_exec"]],
            "o_rel.overseas_flag": [
                ["b_gate", "b1"], ["b_route", "take_exec"],
            ],
        },
        "runtime_expected": {
            # single static path, one missing edge -> unprovable, not partial
            "o_rel.uid_copy": "UNPROVABLE",
            "o_rel.total_amt": "PROVEN",
            "o_rel.region": "PROVEN",
            # two static paths, one evidenced, one dead (explicit negative
            # read of a coalesce arm) -> partial
            "o_rel.country": "PARTIAL_PATH",
            # two static paths, one evidenced, one missing -> partial
            "o_rel.owner_id": "PARTIAL_PATH",
            "o_rel.tag_name": "PROVEN",
            "o_rel.note": "UNPROVABLE",
            "o_rel.overseas_flag": "NOT_EXECUTED",
        },
        "no_contribution_reads": ["p_dbg.dbg_audit"],
    }


def main() -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    graph = _finalize_graph(build_graph())
    package = build_package()
    runtime = build_runtime(package["schema_digest"])
    expected = build_expected_static_paths()

    for name, payload in (
        ("graph.json", graph),
        ("runtime.json", runtime),
        ("package.json", package),
        ("expected_static_paths.json", expected),
    ):
        with open(os.path.join(DATA_DIR, name), "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2)
    print("wrote fixtures to", DATA_DIR)


if __name__ == "__main__":
    main()
