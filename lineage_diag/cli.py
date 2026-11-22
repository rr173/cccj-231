"""Command-line entry point.

Examples:
    python -I -m lineage_diag.cli diagnose \\
        --graph g.json --runtime r.json --package p.json
    python -I -m lineage_diag.cli forward --input t1.uid ...
    python -I -m lineage_diag.cli backward --output o_rel.country ...
    python -I -m lineage_diag.cli export --out report.jsonl ...

Local release repair (single SQLite workspace):
    python -I -m lineage_diag.cli repair init \\
        --graph repair_graph.json --db workspace.db --init initial.json
    python -I -m lineage_diag.cli repair apply \\
        --graph repair_graph.json --db workspace.db --batch batch.json
    python -I -m lineage_diag.cli repair read \\
        --graph repair_graph.json --db workspace.db
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import List, Optional

from .api import (
    backward_query,
    diagnose,
    export_report,
    forward_query,
    load_artifacts,
    paged_report,
)
from .model import LineageError
from .repair import (
    apply_repair_batch,
    initialize_workspace,
    load_json,
    read_release,
)


def _common(p: argparse.ArgumentParser) -> None:
    p.add_argument("--graph", required=True)
    p.add_argument("--runtime", required=True)
    p.add_argument("--package", required=True)


def _repair_common(p: argparse.ArgumentParser) -> None:
    p.add_argument("--graph", required=True,
                   help="declarative processing graph (repair subset) JSON")
    p.add_argument("--db", required=True,
                   help="path to the single SQLite workspace database")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="lineage_diag",
        description="release-package column-level lineage diagnostic tool",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_diag = sub.add_parser("diagnose", help="run full diagnosis")
    _common(p_diag)

    p_fw = sub.add_parser("forward", help="forward query for one input")
    _common(p_fw)
    p_fw.add_argument("--input", required=True,
                      help="fully qualified input column, e.g. t1.uid")
    p_fw.add_argument("--page", type=int, default=1)
    p_fw.add_argument("--page-size", type=int, default=50)

    p_bw = sub.add_parser("backward", help="backward query for one output")
    _common(p_bw)
    p_bw.add_argument("--output", required=True,
                      help="fully qualified output column")
    p_bw.add_argument("--page", type=int, default=1)
    p_bw.add_argument("--page-size", type=int, default=50)

    p_rep = sub.add_parser("report", help="paged report section")
    _common(p_rep)
    p_rep.add_argument("--section", required=True,
                       choices=["summary", "gaps", "forward", "backward"])
    p_rep.add_argument("--focus")
    p_rep.add_argument("--page", type=int, default=1)
    p_rep.add_argument("--page-size", type=int, default=100)

    p_ex = sub.add_parser("export", help="streaming JSONL report export")
    _common(p_ex)
    p_ex.add_argument("--out", required=True)
    p_ex.add_argument("--page-size", type=int, default=100)

    p_repair = sub.add_parser(
        "repair", help="local release-table repair workspace")
    repair_sub = p_repair.add_subparsers(dest="repair_cmd", required=True)

    p_init = repair_sub.add_parser(
        "init", help="create a workspace and materialize the initial release")
    _repair_common(p_init)
    p_init.add_argument("--init", required=True,
                        help="JSON with initial source-table rows")

    p_apply = repair_sub.add_parser(
        "apply", help="atomically apply one correction batch")
    _repair_common(p_apply)
    p_apply.add_argument("--batch", required=True,
                         help="JSON correction batch with expected_version")

    p_read = repair_sub.add_parser(
        "read", help="read committed release rows and their version")
    _repair_common(p_read)
    return parser


def _run_repair(args: argparse.Namespace) -> dict:
    graph_payload = load_json(args.graph)
    if args.repair_cmd == "init":
        plan, version = initialize_workspace(
            args.db, graph_payload, load_json(args.init))
        return {
            "initialized": args.db,
            "version": version,
            "output_node": plan.output_node_id,
            "input_nodes": list(plan.inputs),
        }
    if args.repair_cmd == "apply":
        summary = apply_repair_batch(
            args.db, graph_payload, load_json(args.batch))
        return {"applied": args.db, **summary}
    version, rows = read_release(args.db, graph_payload)
    return {"db": args.db, "version": version, "row_count": len(rows),
            "rows": rows}


def main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    if args.cmd == "repair":
        try:
            payload = _run_repair(args)
        except LineageError as exc:
            print(f"repair rejected: {exc}", file=sys.stderr)
            return 2
        json.dump(payload, sys.stdout, ensure_ascii=False, indent=2)
        sys.stdout.write("\n")
        return 0

    graph, runtime, package = load_artifacts(
        args.graph, args.runtime, args.package)
    diagnosis = diagnose(graph, runtime, package)

    if args.cmd == "diagnose":
        payload = {
            "outputs": [
                {
                    "output_column": o.output_ref.fq,
                    "status": o.status,
                    "static_paths": o.static_path_count,
                    "evidenced_paths": o.evidenced_path_count,
                }
                for o in diagnosis.outputs
            ],
            "gaps": diagnosis.gap_list(),
        }
    elif args.cmd == "forward":
        payload = forward_query(diagnosis, args.input,
                                page=args.page, page_size=args.page_size)
    elif args.cmd == "backward":
        payload = backward_query(diagnosis, args.output,
                                 page=args.page, page_size=args.page_size)
    elif args.cmd == "report":
        payload = paged_report(diagnosis, args.section, page=args.page,
                               page_size=args.page_size, focus=args.focus)
    else:  # export
        export_report(diagnosis, args.out, page_size=args.page_size)
        payload = {"exported_to": args.out}

    json.dump(payload, sys.stdout, ensure_ascii=False, indent=2)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
