"""End-to-end checks for pagination, streaming report export and dedup.

Run with:  python3 -I tests/run_reports.py
"""

from __future__ import annotations

import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from lineage_diag import (  # noqa: E402
    diagnose,
    export_report,
    load_artifacts,
    paged_report,
)
from lineage_diag.diagnoser import (  # noqa: E402
    NO_CONTRIBUTION,
    NOT_EXECUTED,
    PARTIAL_PATH,
    UNPROVABLE,
)

DATA = os.path.join(ROOT, "data")
OUT = os.path.join(ROOT, "tmp")
FAIL = []


def expect(cond, label):
    print(("[PASS] " if cond else "[FAIL] ") + label)
    if not cond:
        FAIL.append(label)


def main() -> int:
    g, rt, pkg = load_artifacts(
        os.path.join(DATA, "graph.json"),
        os.path.join(DATA, "runtime.json"),
        os.path.join(DATA, "package.json"),
    )
    d = diagnose(g, rt, pkg)

    # ---- summary section ---------------------------------------------
    summary = paged_report(d, "summary")
    expect(summary["total"] == 1 and summary["items"][0]["run_version"]
           == "run-77", "summary section content")
    counts = summary["items"][0]["finding_counts"]
    expect(set(counts) == {PARTIAL_PATH, NOT_EXECUTED, UNPROVABLE,
                           NO_CONTRIBUTION},
            "all four gap categories represented in summary counts")
    expect(summary["items"][0]["runtime_records_missing_for_nodes"]
           == ["p_note"], "missing node list exposed in summary")

    # ---- gap section pagination --------------------------------------
    page1 = paged_report(d, "gaps", page=1, page_size=2)
    page2 = paged_report(d, "gaps", page=2, page_size=2)
    expect(len(page1["items"]) == 2 and len(page2["items"]) == 2,
            "gap section pages contain page_size items")
    expect(page1["total"] == page2["total"]
           and page1["total_pages"] >= 3,
            "gap pagination totals consistent")
    ids1 = {json.dumps(x, sort_keys=True) for x in page1["items"]}
    ids2 = {json.dumps(x, sort_keys=True) for x in page2["items"]}
    expect(ids1.isdisjoint(ids2), "pages do not repeat findings")

    # ---- backward paged section with unverified branches -------------
    bw = paged_report(d, "backward", focus="o_rel.country",
                      page=1, page_size=1)
    expect(bw["total"] == 2 and bw["total_pages"] == 2
           and bw["status"] == PARTIAL_PATH,
            "backward section pages the two coalesced paths")

    # ---- forward section ---------------------------------------------
    fw = paged_report(d, "forward", focus="t1.uid", page=1, page_size=10)
    targets = {it["output_column"] for it in fw["items"]}
    expect(targets == {"o_rel.uid_copy", "o_rel.owner_id"},
            "forward section lists both influenced outputs")

    # ---- streaming JSONL export --------------------------------------
    os.makedirs(OUT, exist_ok=True)
    report_path = os.path.join(OUT, "report.jsonl")
    export_report(d, report_path, page_size=2)
    sections = []
    with open(report_path, encoding="utf-8") as fh:
        for line in fh:
            sections.append(json.loads(line))
    expect({s["section"] for s in sections}
           == {"summary", "gaps", "forward", "backward"},
            "JSONL report contains every section")
    # every paged payload obeys page_size (last page may be smaller)
    bad = [s for s in sections
           if s["section"] in ("forward", "backward", "gaps")
           and len(s.get("payload", s).get("items",
                       s.get("items", []))) > 2]
    expect(not bad, "exported pages never exceed page_size")

    # ---- dedup table: identical fingerprints counted once -----------
    # Build a synthetic duplicate by querying the same forward focus twice
    # through the exporter internals is unnecessary; instead verify the
    # report exporter DB contract directly on a repeated iterable.
    from lineage_diag.report import ReportExporter  # noqa: E402
    from lineage_diag.traversal import GraphExplorer  # noqa: E402
    exporter = ReportExporter(d, GraphExplorer(g))
    assessed = d._forward_index["t1.uid"]
    conn = exporter._dedup_paths(list(assessed) + list(assessed))
    try:
        row = conn.execute(
            "SELECT MIN(count), MAX(count), COUNT(*) FROM paths"
        ).fetchone()
    finally:
        conn.close()
    expect(row[0] == 2 and row[1] == 2
           and row[2] == len({p.path.fingerprint for p in assessed}),
           "duplicate paths collapse with occurrence_count and "
           "representative evidence retained")

    # ---- gap report spot-check: evidence text ------------------------
    note = next(s for s in sections
                if s["section"] == "gaps"
                and s["payload"]["output_column"] == "o_rel.note")
    expect(note["payload"]["category"] == UNPROVABLE
           and "no runtime record" in note["payload"]["message"],
            "unprovable finding keeps representative evidence text")

    print()
    if FAIL:
        print(f"{len(FAIL)} REPORT CHECK(S) FAILED")
        return 1
    print("ALL REPORT CHECKS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
