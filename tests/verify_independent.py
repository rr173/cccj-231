"""Independent verifier.

This program deliberately re-implements graph traversal and runtime merging
instead of importing lineage_diag.traversal / lineage_diag.diagnoser.  Its
purpose is to cross-check the diagnostic tool against an independently
derived expectation: if both implementations make the same structural and
evidential mistakes, the cross-check would not catch them -- so the code
here is written from the spec only, in a different (dict-driven) style.

Checks performed:

1. independently enumerate every static input->output column path and
   compare against the hand-written expected_static_paths.json inventory;
2. independently classify each path with the runtime +/- records and
   compare the per-output status with the tool's diagnosis;
3. verify the four gap categories appear exactly where specified;
4. verify identity handling: rename edge, same-named join keys, aggregate
   inputs, shadowed column, un-executed branch;
5. mutation tests: alter runtime versions / package summary / remove a
   record and confirm diagnosis is REFUSED with ConsistencyError.
"""

from __future__ import annotations

import copy
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from lineage_diag import (  # noqa: E402
    ConsistencyError,
    NO_CONTRIBUTION,
    NOT_EXECUTED,
    PARTIAL_PATH,
    PROVEN,
    UNPROVABLE,
    diagnose,
    load_artifacts,
    load_artifacts_from_objects,
)
from lineage_diag.loader import load_graph, load_runtime  # noqa: E402

DATA = os.path.join(ROOT, "data")
FAILURES = []


def check(cond: bool, label: str, detail: str = "") -> None:
    mark = "PASS" if cond else "FAIL"
    print(f"[{mark}] {label}" + (f" -- {detail}" if detail and not cond
                                 else ""))
    if not cond:
        FAILURES.append(label)


def load_json(name):
    with open(os.path.join(DATA, name), encoding="utf-8") as fh:
        return json.load(fh)


# ---------------------------------------------------------------------------
# Independent graph model (dict-driven, no reuse of traversal.py)
# ---------------------------------------------------------------------------
def independent_edges(graph):
    """Return list of (src_fq, dst_fq, gates set).

    Independently re-derived rule set: branch nodes are transparent for
    columns; a branch's upstream connects directly to arm targets and the
    (upstream, target) pair carries the arm as a gate.
    """
    nodes = {n["id"]: n for n in graph["nodes"]}
    adj = {nid: [] for nid in nodes}
    target_gates = {}   # arm-target node -> set of (branch, arm)
    for nid, n in nodes.items():
        t = n["type"]
        if t == "project":
            adj[n["input"]].append(nid)
        elif t == "join":
            adj[n["left"]].append(nid)
            adj[n["right"]].append(nid)
        elif t == "agg":
            adj[n["input"]].append(nid)
        elif t == "explode":
            adj[n["input"]].append(nid)
        elif t == "output":
            adj[n["input"]].append(nid)
            for extra in n.get("extra_inputs", []):
                adj[extra].append(nid)
        elif t == "branch":
            for arm in n["branches"]:
                adj[n["input"]].append(arm["target"])
                target_gates.setdefault(arm["target"], set()).add(
                    (nid, arm["id"]))

    def reachable(src, dst):
        stack, seen = [src], set()
        while stack:
            cur = stack.pop()
            if cur in seen:
                continue
            seen.add(cur)
            if cur == dst:
                return True
            stack.extend(adj[cur])
        return False

    edges = []
    for nid, n in nodes.items():
        if n["type"] in ("input", "branch"):
            continue
        exprs = list(n.get("exprs", []))
        if n["type"] == "explode":
            exprs = [{"out_column": n["out_column"], "refs": [n["ref"]]}]
            exprs.extend(n.get("passthrough", []))
        for e in exprs:
            for ref in e.get("refs", []):
                src_node = ref.split(".", 1)[0]
                assert reachable(src_node, nid), \
                    f"no walk {src_node}->{nid}"
                edges.append((ref, f"{nid}.{e['out_column']}",
                              frozenset(target_gates.get(nid, set()))))
    return edges, nodes, adj


def independent_backward_paths(graph, output_fq):
    edges, nodes, adj = independent_edges(graph)
    incoming = {}
    for src, dst, gates in edges:
        incoming.setdefault(dst, []).append((src, gates))

    paths = []

    def walk(col_fq, chain, gate_acc):
        producer_node = col_fq.split(".", 1)[0]
        ins = incoming.get(col_fq, [])
        if nodes[producer_node]["type"] == "input" or not ins:
            if chain:
                paths.append((list(reversed(chain)), frozenset(gate_acc)))
            return
        for src, gates in ins:
            walk(src, chain + [(src, col_fq)], gate_acc | gates)

    walk(output_fq, [], frozenset())
    return paths


# ---------------------------------------------------------------------------
# Independent runtime classification
# ---------------------------------------------------------------------------
def independent_classify(graph, runtime, output_fq):
    paths = independent_backward_paths(graph, output_fq)
    recs = runtime["nodes"]
    states = []
    for chain, gates in paths:
        state = "evidenced"
        # gates first
        for branch_id, arm_id in sorted(gates):
            rec = recs.get(branch_id)
            if rec is None:
                state = "no_record"
            elif arm_id in rec.get("branches_skipped", []):
                state = "not_executed"
            elif arm_id not in rec.get("branches_taken", []):
                state = "no_record"
        # edges
        for src, dst in chain:
            dst_node = dst.split(".", 1)[0]
            rec = recs.get(dst_node)
            if rec is None:
                if state == "evidenced":
                    state = "no_record"
                continue
            if src in rec.get("read_negative_refs", []):
                if state == "evidenced":
                    state = "not_executed"
            elif dst in rec.get("read_positive", []):
                pass
            elif dst in rec.get("read_negative", []):
                if state == "evidenced":
                    state = "not_executed"
            else:
                if state == "evidenced":
                    state = "no_record"
        states.append(state)
    if states and all(s == "evidenced" for s in states):
        return PROVEN, states
    if any(s == "evidenced" for s in states):
        return PARTIAL_PATH, states
    if all(s == "not_executed" for s in states):
        return NOT_EXECUTED, states
    return UNPROVABLE, states


# ---------------------------------------------------------------------------
# Main verification
# ---------------------------------------------------------------------------
def main() -> int:
    graph = load_json("graph.json")
    runtime = load_json("runtime.json")
    package = load_json("package.json")
    expected = load_json("expected_static_paths.json")

    # ---- 1. static path inventory cross-check -----------------------
    print("== 1. independent static path enumeration ==")
    for out_fq, expected_path_chains in expected["outputs"].items():
        got = independent_backward_paths(graph, out_fq)
        got_chains = []
        for chain, gates in got:
            cols = [chain[0][0]] + [dst for _, dst in chain]
            got_chains.append(cols)
        check(
            sorted(map(_key_chain, got_chains))
            == sorted(map(_key_chain, expected_path_chains)),
            f"static paths for {out_fq}",
            f"got={got_chains} expected={expected_path_chains}",
        )

    # gate check
    for out_fq, gate_list in expected["gates"].items():
        got = independent_backward_paths(graph, out_fq)
        got_gates = sorted(tuple(sorted(g)) for _, g in got)
        want_gate_set = frozenset(tuple(x) for x in gate_list)
        check(all(frozenset(gg) == want_gate_set for gg in got_gates)
              and bool(got_gates),
              f"gates for {out_fq}",
              f"got={got_gates} want={sorted(want_gate_set)}")

    # ---- 2. run the actual tool --------------------------------------
    print("== 2. tool diagnosis vs independent classification ==")
    g, rt, pkg = load_artifacts(
        os.path.join(DATA, "graph.json"),
        os.path.join(DATA, "runtime.json"),
        os.path.join(DATA, "package.json"),
    )
    diagnosis = diagnose(g, rt, pkg)
    tool_status = {o.output_ref.fq: o.status for o in diagnosis.outputs}

    for out_fq, want_status in expected["runtime_expected"].items():
        indep_status, states = independent_classify(
            graph, runtime, out_fq)
        check(indep_status == want_status,
              f"independent classifier agrees with spec for {out_fq}",
              f"indep={indep_status} states={states} spec={want_status}")
        check(tool_status.get(out_fq) == want_status,
              f"tool status for {out_fq}",
              f"tool={tool_status.get(out_fq)} want={want_status}")

    # ---- 3. the four gap categories ----------------------------------
    print("== 3. four-category gap list ==")
    cats = {}
    for f in diagnosis.findings:
        cats.setdefault(f.category, []).append(f)
    cat_outputs = {
        cat: sorted({f.output_ref.fq for f in fs if f.output_ref})
        for cat, fs in cats.items()
    }
    check(cat_outputs.get(PARTIAL_PATH, []) ==
          sorted(["o_rel.country", "o_rel.owner_id"]),
          "PARTIAL_PATH outputs", str(cat_outputs.get(PARTIAL_PATH)))
    check(cat_outputs.get(NOT_EXECUTED, []) == ["o_rel.overseas_flag"],
          "NOT_EXECUTED outputs", str(cat_outputs.get(NOT_EXECUTED)))
    check(cat_outputs.get(UNPROVABLE, []) ==
          sorted(["o_rel.note", "o_rel.uid_copy"]),
          "UNPROVABLE outputs", str(cat_outputs.get(UNPROVABLE)))
    cat2 = sorted(f.input_ref.fq for f in cats.get(NO_CONTRIBUTION, []))
    check(cat2 == ["p_dbg.dbg_audit"], "NO_CONTRIBUTION reads", str(cat2))

    # missing-record node must surface explicitly, not be treated as
    # "no dependency"
    missing = diagnosis.runtime.missing_nodes
    check("p_note" in missing, "p_note flagged as missing runtime record")
    note_finding = next(f for f in diagnosis.findings
                        if f.output_ref and f.output_ref.fq == "o_rel.note")
    check(note_finding.category == UNPROVABLE
          and "no runtime record" in note_finding.message,
          "note gap says unprovable (missing record != no dependency)")

    # ---- 4. source identity checks -----------------------------------
    print("== 4. source identity preservation ==")
    edges, _, _ = independent_edges(graph)
    # rename: distinct qualified identities on one edge
    check(("t1.uid", "p_filt.uid", frozenset()) in edges
          and ("p_filt.uid", "j_user.uid_copy", frozenset()) in edges,
          "rename keeps source identity (t1.uid -> uid_copy)")
    # same-named join keys: two distinct source identities survive
    pairs = {(s, d) for s, d, _ in edges}
    check(("t2.uid", "j_user.uid") in pairs
          and ("p_filt.uid", "j_user.uid_copy") in pairs
          and ("t1.uid", "p_filt.uid") in pairs,
          "same-named join columns stay distinct sources",
          str(sorted(p for p in pairs if "uid" in p[0])))
    # aggregate inputs
    check(("j_user.amount", "a_sum.total_amt", frozenset()) in edges,
          "aggregate input edge retained")
    # explode edge
    check(("t3.tags", "e_tags.tag_name", frozenset()) in edges,
          "explode edge retained")
    # shadowing: p_agg.country explicitly sourced from a_sum.country
    shadow = [e for e in edges if e[1] == "p_agg.country"]
    check(shadow == [("a_sum.country", "p_agg.country", frozenset())],
          "shadowed column keeps explicit source identity", str(shadow))

    # backward query content
    bw = diagnosis.backward_query("o_rel.country")
    check(sorted(bw["input_columns"]) ==
          ["t2.country", "t2.country_code"],
          "backward query lists all coalesced input columns",
          str(bw["input_columns"]))
    check(bw["status"] == PARTIAL_PATH, "backward query reports status")

    # forward query content and evidence path presence
    fw = diagnosis.forward_query("t1.uid")
    influenced = {item["output_column"] for item in fw["items"]}
    check({"o_rel.uid_copy", "o_rel.owner_id"} <= influenced,
          "forward query finds both influenced outputs", str(influenced))
    check(all(item["evidence"] for item in fw["items"]),
          "forward items carry evidence chains")

    # pagination mechanics
    page1 = diagnosis.forward_query("t1.uid", page=1, page_size=1)
    check(page1["total"] >= 2 and len(page1["items"]) == 1
          and page1["total_pages"] >= 2,
          "pagination returns one item with correct totals")

    # ---- 5. mutation / refusal tests ---------------------------------
    print("== 5. consistency refusal mutations ==")
    _expect_refusal(graph, runtime, package,
                    mut_runtime=lambda r: r.update(run_version="run-OTHER"),
                    label="runtime run version mismatch")
    _expect_refusal(graph, runtime, package,
                    mut_graph=lambda g: g.update(graph_version="graph-9"),
                    label="graph version mismatch")
    _expect_refusal(graph, runtime, package,
                    mut_runtime=lambda r: r.update(
                        package_digest="0" * 64),
                    label="runtime package digest mismatch")
    tampered = copy.deepcopy(package)
    tampered["columns"][0]["type"] = "VARCHAR(999)"
    _expect_refusal(graph, runtime, tampered,
                    label="package schema summary tamper")
    tampered2 = copy.deepcopy(package)
    tampered2["columns"][1]["sample_digest"] = "deadbeef"
    _expect_refusal(graph, runtime, tampered2,
                    label="output sample summary tamper")
    missing_rec = copy.deepcopy(runtime)
    del missing_rec["nodes"]["o_rel"]
    _expect_refusal(graph, missing_rec, package,
                    label="missing output runtime record")
    missing_sample = copy.deepcopy(runtime)
    del missing_sample["output_samples"]["o_rel"]["region"]
    _expect_refusal(graph, missing_sample, package,
                    label="missing output sample summary")

    print()
    if FAILURES:
        print(f"{len(FAILURES)} CHECK(S) FAILED:")
        for f in FAILURES:
            print(" -", f)
        return 1
    print("ALL CHECKS PASSED")
    return 0


def _key_chain(chain):
    return " > ".join(chain)


def _expect_refusal(graph, runtime, package, label,
                    mut_graph=None, mut_runtime=None):
    g2 = copy.deepcopy(graph)
    r2 = copy.deepcopy(runtime)
    p2 = copy.deepcopy(package)
    if mut_graph:
        mut_graph(g2)
    if mut_runtime:
        mut_runtime(r2)
    try:
        g, rt, pkg = load_artifacts_from_objects(g2, r2, p2)
        diagnose(g, rt, pkg)
    except ConsistencyError as exc:
        check(True, f"refused: {label}")
        return
    except Exception as exc:  # noqa: BLE001 - any other failure is a bug
        check(False, f"refused: {label}", f"wrong exception {type(exc)}: {exc}")
        return
    check(False, f"refused: {label}", "diagnosis was NOT rejected")


if __name__ == "__main__":
    sys.exit(main())
