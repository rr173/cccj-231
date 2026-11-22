"""Command-line entry point.

Examples:
    python -I -m lineage_diag.cli diagnose \\
        --graph g.json --runtime r.json --package p.json
    python -I -m lineage_diag.cli forward --input t1.uid ...
    python -I -m lineage_diag.cli backward --output o_rel.country ...
    python -I -m lineage_diag.cli export --out report.jsonl ...

Local release repair (single SQLite workspace):
    python -I -m lineage_diag.cli repair init \\
        --graph repair_graph.json --db workspace.db --init initial.json \\
        [--trace]
    python -I -m lineage_diag.cli repair apply \\
        --graph repair_graph.json --db workspace.db --batch batch.json
    python -I -m lineage_diag.cli repair read \\
        --graph repair_graph.json --db workspace.db
    python -I -m lineage_diag.cli repair trace \\
        --graph repair_graph.json --db workspace.db \\
        --row '{"region":"R2","total_amt":60,"n":2,"bucket":"hot"}' \\
        --expected-version 3
    python -I -m lineage_diag.cli repair subscribe enable \\
        --graph repair_graph.json --db workspace.db --retention 3
    python -I -m lineage_diag.cli repair subscribe read \\
        --graph repair_graph.json --db workspace.db --from-version 2
    python -I -m lineage_diag.cli repair subscribe read \\
        --graph repair_graph.json --db workspace.db --cursor TOKEN
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
from .model import LineageError, TraceError
from .repair import (
    apply_repair_batch,
    enable_subscription,
    initialize_workspace,
    load_json,
    read_release,
    read_subscription,
    trace_release_row,
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
    p_init.add_argument("--trace", action="store_true",
                        help="also maintain instance-level computation "
                             "evidence so single release rows can be traced "
                             "with 'repair trace' (only settable at init)")

    p_apply = repair_sub.add_parser(
        "apply", help="atomically apply one correction batch")
    _repair_common(p_apply)
    p_apply.add_argument("--batch", required=True,
                         help="JSON correction batch with expected_version")

    p_read = repair_sub.add_parser(
        "read", help="read committed release rows and their version")
    _repair_common(p_read)

    p_trace = repair_sub.add_parser(
        "trace", help="trace one published row to its per-copy computation "
                      "evidence (trace-enabled workspaces only)")
    _repair_common(p_trace)
    p_trace.add_argument("--row", default=None,
                         help="JSON object mapping every release column to "
                              "its value, e.g. '{\"region\":\"R2\",...}'")
    p_trace.add_argument("--row-file", default=None,
                         help="JSON file containing the release row object")
    p_trace.add_argument("--expected-version", type=int, required=True,
                         help="the current committed release version the "
                              "row was read at")

    p_sub = repair_sub.add_parser(
        "subscribe", help="net-change subscription for downstream consumers")
    sub_sub = p_sub.add_subparsers(dest="subscribe_cmd", required=True)

    p_sub_en = sub_sub.add_parser(
        "enable", help="enable the subscription at the current version")
    _repair_common(p_sub_en)
    p_sub_en.add_argument("--retention", type=int, required=True,
                          help="keep change deltas for this many most "
                               "recent successful publishes (>= 1)")

    p_sub_rd = sub_sub.add_parser(
        "read", help="read one page of net release-row changes")
    _repair_common(p_sub_rd)
    p_sub_rd.add_argument("--from-version", type=int, default=None,
                          help="version the consumer has fully synced "
                               "(first page)")
    p_sub_rd.add_argument("--cursor", default=None,
                          help="continuation token from the previous page")
    p_sub_rd.add_argument("--page-size", type=int, default=100)
    return parser


def _run_repair(args: argparse.Namespace) -> dict:
    graph_payload = load_json(args.graph)
    if args.repair_cmd == "init":
        plan, version = initialize_workspace(
            args.db, graph_payload, load_json(args.init),
            enable_trace=args.trace)
        return {
            "initialized": args.db,
            "version": version,
            "output_node": plan.output_node_id,
            "input_nodes": list(plan.inputs),
            "trace": bool(args.trace),
        }
    if args.repair_cmd == "apply":
        summary = apply_repair_batch(
            args.db, graph_payload, load_json(args.batch))
        return {"applied": args.db, **summary}
    if args.repair_cmd == "trace":
        if (args.row is None) == (args.row_file is None):
            raise LineageError(
                "repair trace needs exactly one of --row / --row-file")
        if args.row_file:
            row = load_json(args.row_file)
        else:
            try:
                row = json.loads(args.row)
            except json.JSONDecodeError as exc:
                raise LineageError(
                    f"--row is not valid JSON: {exc}") from exc
        return trace_release_row(args.db, graph_payload, row,
                                 args.expected_version)
    if args.repair_cmd == "subscribe":
        if args.subscribe_cmd == "enable":
            return {"db": args.db,
                    **enable_subscription(args.db, graph_payload,
                                          args.retention)}
        return read_subscription(args.db, graph_payload,
                                 from_version=args.from_version,
                                 cursor=args.cursor,
                                 page_size=args.page_size)
    version, rows = read_release(args.db, graph_payload)
    return {"db": args.db, "version": version, "row_count": len(rows),
            "rows": rows}


def main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    if args.cmd == "repair":
        try:
            payload = _run_repair(args)
        except TraceError as exc:
            print(f"trace rejected ({exc.kind}): {exc}", file=sys.stderr)
            return 2
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
