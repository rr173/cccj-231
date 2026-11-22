"""Streaming, paginated report export.

A graph with many fan-out/fan-in shapes can produce an enormous number of
lineage paths.  The exporter therefore:

* enumerates paths lazily from the explorer/generator;
* dedupes identical path fingerprints inside a temporary SQLite table
  instead of holding an unbounded Python set;
* keeps a *count* plus one representative evidence chain per fingerprint;
* emits JSONL one record at a time and supports paging through sections.
"""

from __future__ import annotations

import json
import sqlite3
import tempfile
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional

from .diagnoser import (
    AssessedPath,
    Diagnosis,
    _assessed_path_dict,
    _finding_dict,
)
from .model import ColRef
from .traversal import GraphExplorer


@dataclass
class ReportSection:
    key: str
    title: str


SECTIONS = (
    ReportSection("summary", "versions, counts and output statuses"),
    ReportSection("gaps", "unprovable / partial / not-executed / no-contribution findings"),
    ReportSection("forward", "input-column -> output-column evidenced paths"),
    ReportSection("backward", "output-column -> input-column paths and unverified branches"),
)


class ReportExporter:
    def __init__(self, diagnosis: Diagnosis, explorer: GraphExplorer):
        self.diagnosis = diagnosis
        self.explorer = explorer

    # ------------------------------------------------------------------
    # Summary (always small)
    # ------------------------------------------------------------------
    def summary(self) -> Dict[str, Any]:
        d = self.diagnosis
        counts: Dict[str, int] = {}
        for f in d.findings:
            counts[f.category] = counts.get(f.category, 0) + 1
        return {
            "graph_version": d.graph.graph_version,
            "run_version": d.runtime.run_version,
            "package": d.package.package_name,
            "package_version": d.package.package_version,
            "schema_digest": d.package.schema_digest,
            "outputs": [
                {
                    "output_column": o.output_ref.fq,
                    "status": o.status,
                    "static_paths": o.static_path_count,
                    "evidenced_paths": o.evidenced_path_count,
                }
                for o in d.outputs
            ],
            "finding_counts": counts,
            "runtime_records_missing_for_nodes": d.runtime.missing_nodes,
        }

    # ------------------------------------------------------------------
    # SQLite-backed dedup of lazily enumerated paths
    # ------------------------------------------------------------------
    def _dedup_paths(
        self, assessed_iterable: Iterable[AssessedPath]
    ) -> sqlite3.Connection:
        conn = sqlite3.connect(tempfile.mkstemp(prefix="lineage_report_",
                                                suffix=".db")[1])
        try:
            conn.execute(
                "CREATE TABLE paths (fingerprint TEXT PRIMARY KEY, "
                "count INTEGER NOT NULL, payload TEXT NOT NULL)"
            )
            for assessed in assessed_iterable:
                payload = json.dumps(
                    _assessed_path_dict(assessed), ensure_ascii=False
                )
                conn.execute(
                    "INSERT INTO paths(fingerprint, count, payload) "
                    "VALUES (?, 1, ?) "
                    "ON CONFLICT(fingerprint) DO UPDATE SET "
                    "count = count + 1",
                    (assessed.path.fingerprint, payload),
                )
            conn.commit()
        except Exception:
            conn.close()
            raise
        return conn

    def _page_sql(self, conn: sqlite3.Connection, page: int,
                  page_size: int) -> Dict[str, Any]:
        total = conn.execute("SELECT COUNT(*) FROM paths").fetchone()[0]
        rows = conn.execute(
            "SELECT payload, count FROM paths ORDER BY fingerprint "
            "LIMIT ? OFFSET ?",
            (page_size, (page - 1) * page_size),
        ).fetchall()
        items: List[Dict[str, Any]] = []
        for payload, count in rows:
            obj = json.loads(payload)
            obj["occurrence_count"] = count
            items.append(obj)
        return {
            "page": page, "page_size": page_size, "total": total,
            "total_pages": (total + page_size - 1) // page_size,
            "items": items,
        }

    # ------------------------------------------------------------------
    # Section paging
    # ------------------------------------------------------------------
    def page_section(self, section: str, page: int = 1,
                     page_size: int = 100,
                     focus: Optional[str] = None) -> Dict[str, Any]:
        if section == "summary":
            return {"section": section, "page": 1, "page_size": 1,
                    "total": 1, "total_pages": 1,
                    "items": [self.summary()]}
        if section == "gaps":
            return self._page_findings(page, page_size)
        if section == "forward":
            if not focus:
                raise ValueError("forward section requires focus=input fq")
            ColRef.parse(focus)
            conn = self._dedup_paths(
                iter(self.diagnosis._forward_index.get(focus, []))
            )
            try:
                result = self._page_sql(conn, page, page_size)
            finally:
                conn.close()
            result["section"] = section
            result["focus"] = focus
            return result
        if section == "backward":
            if not focus:
                raise ValueError("backward section requires focus=output fq")
            target = next((o for o in self.diagnosis.outputs
                           if o.output_ref.fq == focus), None)
            if target is None:
                raise KeyError(f"unknown output column {focus!r}")
            conn = self._dedup_paths(iter(target.paths))
            try:
                result = self._page_sql(conn, page, page_size)
            finally:
                conn.close()
            result["section"] = section
            result["focus"] = focus
            result["status"] = target.status
            return result
        raise KeyError(f"unknown report section {section!r}")

    def _page_findings(self, page: int,
                       page_size: int) -> Dict[str, Any]:
        items = [_finding_dict(f) for f in self.diagnosis.findings]
        total = len(items)
        start = (page - 1) * page_size
        return {
            "section": "gaps",
            "page": page, "page_size": page_size, "total": total,
            "total_pages": (total + page_size - 1) // page_size,
            "items": items[start:start + page_size],
        }

    # ------------------------------------------------------------------
    # Full streaming export: JSONL, one JSON object per line
    # ------------------------------------------------------------------
    def export_jsonl(self, path: str, page_size: int = 100) -> None:
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(json.dumps({"section": "summary",
                                 "payload": self.summary()},
                                ensure_ascii=False) + "\n")
            for finding in self.diagnosis.findings:
                fh.write(json.dumps(
                    {"section": "gaps", "payload": _finding_dict(finding)},
                    ensure_ascii=False) + "\n")

            for input_fq in sorted(self.diagnosis._forward_index):
                conn = self._dedup_paths(
                    iter(self.diagnosis._forward_index[input_fq])
                )
                try:
                    pages = self._total_pages(conn, page_size)
                    for page in range(1, pages + 1):
                        fh.write(json.dumps(
                            {"section": "forward", "focus": input_fq,
                             "payload": self._page_sql(
                                 conn, page, page_size)},
                            ensure_ascii=False) + "\n")
                finally:
                    conn.close()

            for out in self.diagnosis.outputs:
                conn = self._dedup_paths(iter(out.paths))
                try:
                    pages = self._total_pages(conn, page_size)
                    for page in range(1, pages + 1):
                        fh.write(json.dumps(
                            {"section": "backward",
                             "focus": out.output_ref.fq,
                             "payload": self._page_sql(
                                 conn, page, page_size)},
                            ensure_ascii=False) + "\n")
                finally:
                    conn.close()

    @staticmethod
    def _total_pages(conn: sqlite3.Connection, page_size: int) -> int:
        total = conn.execute("SELECT COUNT(*) FROM paths").fetchone()[0]
        return max(1, (total + page_size - 1) // page_size)
