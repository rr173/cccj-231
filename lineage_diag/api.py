"""Public API: import, diagnose, forward/backward query, report export.

Typical usage::

    from lineage_diag import load_artifacts, diagnose, export_report

    graph, runtime, package = load_artifacts("g.json", "r.json", "p.json")
    result = diagnose(graph, runtime, package)
    result.forward_query("t1.uid")
    export_report(result, "report.jsonl")
"""

from __future__ import annotations

import json
from typing import Any, Dict, Mapping, Optional

from .diagnoser import Diagnoser, Diagnosis
from .loader import (
    assert_consistent,
    load_graph,
    load_package,
    load_runtime,
)
from .model import Graph, ReleasePackage, RuntimeRecord
from .report import ReportExporter
from .traversal import GraphExplorer

__all__ = [
    "load_artifacts",
    "load_artifacts_from_objects",
    "diagnose",
    "forward_query",
    "backward_query",
    "export_report",
    "paged_report",
]


def _read_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def load_artifacts(graph_path: str, runtime_path: str,
                   package_path: str) -> tuple:
    """Import the three artifacts and run the full consistency gate."""
    return load_artifacts_from_objects(
        _read_json(graph_path),
        _read_json(runtime_path),
        _read_json(package_path),
    )


def load_artifacts_from_objects(graph_obj: Mapping[str, Any],
                                runtime_obj: Mapping[str, Any],
                                package_obj: Mapping[str, Any]) -> tuple:
    graph = load_graph(graph_obj)
    runtime = load_runtime(runtime_obj, graph)
    package = load_package(package_obj)
    assert_consistent(graph, runtime, package)
    return graph, runtime, package


def diagnose(graph: Graph, runtime: RuntimeRecord,
             package: ReleasePackage) -> Diagnosis:
    """Produce the full column-level diagnosis.

    Artifacts must already have passed :func:`load_artifacts`; the check is
    repeated so callers constructing objects manually cannot bypass it.
    """
    assert_consistent(graph, runtime, package)
    return Diagnoser(graph, runtime, package).run()


def forward_query(diagnosis: Diagnosis, input_fq: str,
                  page: int = 1, page_size: int = 50) -> Dict[str, Any]:
    """Reverse-impact view: which outputs an input column influences."""
    return diagnosis.forward_query(input_fq, page=page, page_size=page_size)


def backward_query(diagnosis: Diagnosis, output_fq: str,
                   page: int = 1, page_size: int = 50) -> Dict[str, Any]:
    """Forward-lineage view: every input feeding one released output."""
    return diagnosis.backward_query(output_fq, page=page, page_size=page_size)


def paged_report(diagnosis: Diagnosis, section: str, page: int = 1,
                 page_size: int = 100,
                 focus: Optional[str] = None) -> Dict[str, Any]:
    exporter = ReportExporter(diagnosis, GraphExplorer(diagnosis.graph))
    return exporter.page_section(section, page=page, page_size=page_size,
                                 focus=focus)


def export_report(diagnosis: Diagnosis, output_path: str,
                  page_size: int = 100) -> None:
    exporter = ReportExporter(diagnosis, GraphExplorer(diagnosis.graph))
    exporter.export_jsonl(output_path, page_size=page_size)
