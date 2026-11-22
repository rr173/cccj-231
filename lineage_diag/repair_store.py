"""SQLite workspace layout for local release repair.

One workspace is one SQLite database.  It holds:

* ``meta``            - current committed version, graph identity and schema
                         version;
* one source table per ``input`` node - business columns plus the immutable
  in-table row id;
* ``rel_<node>``      - one row per multiset entry of every materialized
  intermediate / output node, with a surrogate ``_sid`` and, for join/agg
  nodes, the hidden partition key (``_jkey`` / ``_gkey``) used for indexed
  scoped re-reads;
* ``prov_<node>``     - (surrogate row, source node, source row id, copies)
  multiset provenance.  ``copies`` keeps multiplicity so duplicate rows and
  many-to-many join fan-out preserve every contribution.

All reads issued by the repair engine go through the accessor methods below,
which also append entries to an :class:`AccessLedger`.  Tests replay the
ledger to prove a small batch never touches unrelated keys/groups, instead
of relying on wall-clock timing.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from .model import RepairValidationError
from .repair_plan import RepairPlan

SCHEMA_VERSION = 1
INITIAL_VERSION = 1

# Ledger operation kinds.
OP_READ_SOURCE = "read_source"          # source table row read
OP_READ_REL = "read_rel"                # intermediate relation row read
OP_READ_PROV = "read_prov"             # provenance row read
OP_WRITE_SOURCE = "write_source"
OP_WRITE_REL = "write_rel"
OP_WRITE_PROV = "write_prov"
OP_DELETE_REL = "delete_rel"
OP_SCAN = "full_scan"                   # whole-table scan (init only)
OP_SQL = "sql"                          # raw executed statement (trace)


@dataclass
class AccessLedger:
    """Every storage access performed while the connection is open.

    Each entry is a ``(kind, target, detail)`` triple where ``target`` is the
    physical table and ``detail`` carries the key/row-id predicate (or
    ``"*"`` for a full scan).  The ledger is deliberately plain data so
    regression tests can assert exact scopes.
    """

    entries: List[Tuple[str, str, str]] = field(default_factory=list)
    sql_statements: List[str] = field(default_factory=list)

    def record(self, kind: str, target: str, detail: str) -> None:
        self.entries.append((kind, target, detail))

    def targets_for(self, kind: str) -> List[Tuple[str, str]]:
        return [(t, d) for k, t, d in self.entries if k == kind]

    def touched_source_rows(self, table: str) -> set:
        return {d for k, t, d in self.entries
                if k == OP_READ_SOURCE and t == table and d != "*"}

    def had_full_scan(self, table: str) -> bool:
        return any(k == OP_SCAN and t == table and d == "*"
                   for k, t, d in self.entries)

    def reset(self) -> None:
        self.entries.clear()
        self.sql_statements.clear()


def _ident(name: str) -> str:
    # Node/column names are restricted to [A-Za-z0-9_] by the plan validator,
    # so quoting is safe; the check is defense in depth.
    if not isinstance(name, str) or not name or \
            not all(ch.isalnum() or ch == "_" for ch in name) or \
            not (name[0].isalpha() or name[0] == "_"):
        raise RepairValidationError(f"unsafe SQL identifier {name!r}")
    return '"' + name + '"'


def source_table(node_id: str) -> str:
    return f"src_{node_id}"


def rel_table(node_id: str) -> str:
    return f"rel_{node_id}"


def prov_table(node_id: str) -> str:
    return f"prov_{node_id}"


class RepairStore:
    """Typed wrapper around the workspace SQLite connection."""

    def __init__(self, conn: sqlite3.Connection, plan: RepairPlan,
                 ledger: Optional[AccessLedger] = None):
        self.conn = conn
        self.plan = plan
        self.ledger = ledger or AccessLedger()
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.set_trace_callback(self._trace)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    @classmethod
    def open(cls, db_path: str, plan: RepairPlan,
             ledger: Optional[AccessLedger] = None) -> "RepairStore":
        conn = sqlite3.connect(db_path)
        return cls(conn, plan, ledger)

    def close(self) -> None:
        self.conn.set_trace_callback(None)
        self.conn.close()

    def _trace(self, statement: str) -> None:
        self.ledger.sql_statements.append(statement)

    # ------------------------------------------------------------------
    # DDL
    # ------------------------------------------------------------------
    def create_schema(self) -> None:
        c = self.conn
        c.execute(
            "CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )
        for nid in self.plan.inputs:
            self._create_source_table(nid)
        for nid in self.plan.closure:
            if nid in self.plan.inputs:
                continue
            self._create_rel_table(nid)
            self._create_prov_table(nid)
        c.execute(
            "CREATE VIEW release_rows AS SELECT m.value AS version, r.* "
            f"FROM meta m CROSS JOIN {_ident(rel_table(self.plan.output_node_id))} r "
            "WHERE m.key = 'version'"
        )

    def _create_source_table(self, nid: str) -> None:
        row_id = self.plan.row_ids[nid]
        cols = [f"{_ident(row_id)} TEXT PRIMARY KEY"]
        for col in self.plan.columns[nid]:
            ctype = self.plan.types[nid][col]
            cols.append(f"{_ident(col)} {ctype} NOT NULL")
        self.conn.execute(
            f"CREATE TABLE {_ident(source_table(nid))} ("
            + ", ".join(cols) + ")"
        )
        # Scoped repair re-reads source rows by join/group key columns.
        for col in self.plan.columns[nid]:
            self.conn.execute(
                f"CREATE INDEX {_ident('ix_src_' + nid + '_' + col)} ON "
                f"{_ident(source_table(nid))}({_ident(col)})"
            )

    def _create_rel_table(self, nid: str) -> None:
        cols = ['_sid INTEGER PRIMARY KEY AUTOINCREMENT']
        if nid in self.plan.joins:
            cols.append("_jkey TEXT NOT NULL")
        if nid in self.plan.aggs:
            cols.append("_gkey TEXT NOT NULL")
        for col in self.plan.columns[nid]:
            ctype = self.plan.types[nid][col]
            cols.append(f"{_ident(col)} {ctype} NOT NULL")
        self.conn.execute(
            f"CREATE TABLE {_ident(rel_table(nid))} ("
            + ", ".join(cols) + ")"
        )
        if nid in self.plan.joins:
            self.conn.execute(
                f"CREATE INDEX {_ident('ix_' + nid + '_jkey')} ON "
                f"{_ident(rel_table(nid))}(_jkey)"
            )
        if nid in self.plan.aggs:
            self.conn.execute(
                f"CREATE INDEX {_ident('ix_' + nid + '_gkey')} ON "
                f"{_ident(rel_table(nid))}(_gkey)"
            )
        # Downstream joins/aggs locate members through business key columns;
        # index every business column so scoped re-reads never table-scan.
        for col in self.plan.columns[nid]:
            self.conn.execute(
                f"CREATE INDEX {_ident('ix_' + nid + '_' + col)} ON "
                f"{_ident(rel_table(nid))}({_ident(col)})"
            )

    def _create_prov_table(self, nid: str) -> None:
        self.conn.execute(
            f"CREATE TABLE {_ident(prov_table(nid))} ("
            "_sid INTEGER NOT NULL, "
            "src_node TEXT NOT NULL, "
            "src_row_id TEXT NOT NULL, "
            "copies INTEGER NOT NULL CHECK (copies > 0), "
            "PRIMARY KEY (_sid, src_node, src_row_id))"
        )
        self.conn.execute(
            f"CREATE INDEX {_ident('ix_' + nid + '_prov_src')} ON "
            f"{_ident(prov_table(nid))}(src_node, src_row_id)"
        )

    # ------------------------------------------------------------------
    # Meta
    # ------------------------------------------------------------------
    def init_meta(self) -> None:
        meta = {
            "version": str(INITIAL_VERSION),
            "graph_version": self.plan.graph_version,
            "graph_digest": self.plan.graph_digest,
            "schema_version": str(SCHEMA_VERSION),
            "output_node": self.plan.output_node_id,
        }
        self.conn.executemany(
            "INSERT INTO meta(key, value) VALUES (?, ?)",
            list(meta.items()),
        )

    def get_meta(self, key: str) -> Optional[str]:
        row = self.conn.execute(
            "SELECT value FROM meta WHERE key = ?", (key,)
        ).fetchone()
        return row[0] if row else None

    def set_version(self, version: int) -> None:
        self.conn.execute(
            "UPDATE meta SET value = ? WHERE key = 'version'", (str(version),)
        )

    # ------------------------------------------------------------------
    # Source table access
    # ------------------------------------------------------------------
    def get_source_row(self, nid: str, row_id: str) -> Optional[Dict[str, Any]]:
        table = source_table(nid)
        rid_col = self.plan.row_ids[nid]
        row = self.conn.execute(
            f"SELECT * FROM {_ident(table)} WHERE {_ident(rid_col)} = ?",
            (row_id,),
        ).fetchone()
        self.ledger.record(OP_READ_SOURCE, table, str(row_id))
        if row is None:
            return None
        return {k: row[k] for k in row.keys() if k != rid_col}

    def iter_source_all(self, nid: str):
        """Full scan of one source table -- initialization materialization."""
        table = source_table(nid)
        rid_col = self.plan.row_ids[nid]
        self.ledger.record(OP_SCAN, table, "*")
        for row in self.conn.execute(f"SELECT * FROM {_ident(table)}"):
            self.ledger.record(OP_READ_SOURCE, table, str(row[rid_col]))
            yield str(row[rid_col]), {
                k: row[k] for k in row.keys() if k != rid_col
            }

    def insert_source(self, nid: str, row_id: str,
                      values: Mapping[str, Any]) -> None:
        table = source_table(nid)
        rid_col = self.plan.row_ids[nid]
        cols = [rid_col] + self.plan.columns[nid]
        params = [row_id] + [values[c] for c in self.plan.columns[nid]]
        self.conn.execute(
            f"INSERT INTO {_ident(table)} ("
            + ", ".join(_ident(c) for c in cols) + ") VALUES ("
            + ", ".join("?" for _ in cols) + ")",
            params,
        )
        self.ledger.record(OP_WRITE_SOURCE, table, str(row_id))

    def delete_source(self, nid: str, row_id: str) -> None:
        table = source_table(nid)
        rid_col = self.plan.row_ids[nid]
        self.conn.execute(
            f"DELETE FROM {_ident(table)} WHERE {_ident(rid_col)} = ?",
            (row_id,),
        )
        self.ledger.record(OP_WRITE_SOURCE, table, f"delete:{row_id}")

    # ------------------------------------------------------------------
    # Materialized relation access
    # ------------------------------------------------------------------
    def _business_select(self, nid: str) -> str:
        cols = self.plan.columns[nid]
        return ", ".join(_ident(c) for c in cols)

    def insert_rel(self, nid: str, values: Mapping[str, Any]) -> int:
        table = rel_table(nid)
        cols: List[str] = []
        params: List[Any] = []
        if nid in self.plan.joins:
            cols.append("_jkey")
            params.append(values["_jkey"])
        if nid in self.plan.aggs:
            cols.append("_gkey")
            params.append(values["_gkey"])
        for col in self.plan.columns[nid]:
            cols.append(col)
            params.append(values[col])
        cur = self.conn.execute(
            f"INSERT INTO {_ident(table)} ("
            + ", ".join(_ident(c) for c in cols) + ") VALUES ("
            + ", ".join("?" for _ in cols) + ")",
            params,
        )
        sid = int(cur.lastrowid)
        self.ledger.record(OP_WRITE_REL, table, str(sid))
        return sid

    def delete_rel_sids(self, nid: str, sids: Sequence[int]) -> None:
        if not sids:
            return
        table = rel_table(nid)
        qmarks = ", ".join("?" for _ in sids)
        self.conn.execute(
            f"DELETE FROM {_ident(table)} WHERE _sid IN ({qmarks})",
            list(sids),
        )
        for sid in sids:
            self.ledger.record(OP_DELETE_REL, table, str(sid))

    def delete_prov_sids(self, nid: str, sids: Sequence[int]) -> None:
        if not sids:
            return
        table = prov_table(nid)
        qmarks = ", ".join("?" for _ in sids)
        self.conn.execute(
            f"DELETE FROM {_ident(table)} WHERE _sid IN ({qmarks})",
            list(sids),
        )

    def insert_prov(self, nid: str, sid: int,
                    provenance: Mapping[Tuple[str, str], int]) -> None:
        table = prov_table(nid)
        rows = [(sid, src, rid, copies)
                for (src, rid), copies in provenance.items()]
        self.conn.executemany(
            f"INSERT INTO {_ident(table)}"
            "(_sid, src_node, src_row_id, copies) VALUES (?, ?, ?, ?)",
            rows,
        )
        for _sid, _src, _rid, _copies in rows:
            self.ledger.record(OP_WRITE_PROV, table, str(sid))

    def iter_rel_all(self, nid: str):
        """Full scan -- initialization only."""
        table = rel_table(nid)
        self.ledger.record(OP_SCAN, table, "*")
        select = self._business_select(nid)
        for row in self.conn.execute(
            f"SELECT _sid, {select} FROM {_ident(table)}"
        ):
            self.ledger.record(OP_READ_REL, table, str(row["_sid"]))
            yield int(row["_sid"]), {
                c: row[c] for c in self.plan.columns[nid]
            }

    def get_rel_rows_by_sids(self, nid: str, sids: Sequence[int]):
        if not sids:
            return []
        table = rel_table(nid)
        qmarks = ", ".join("?" for _ in sids)
        select = self._business_select(nid)
        rows = self.conn.execute(
            f"SELECT _sid, {select} FROM {_ident(table)} "
            f"WHERE _sid IN ({qmarks})",
            list(sids),
        ).fetchall()
        for row in rows:
            self.ledger.record(OP_READ_REL, table, str(row["_sid"]))
        return [
            (int(row["_sid"]), {c: row[c] for c in self.plan.columns[nid]})
            for row in rows
        ]

    def get_rel_partition(self, nid: str, key_column: str,
                          key_value: Any) -> List[Tuple[int, Dict[str, Any]]]:
        """Scoped re-read of one join-key / group partition via index."""
        table = rel_table(nid)
        select = self._business_select(nid)
        rows = self.conn.execute(
            f"SELECT _sid, {select} FROM {_ident(table)} "
            f"WHERE {_ident(key_column)} = ?",
            (key_value,),
        ).fetchall()
        for row in rows:
            self.ledger.record(OP_READ_REL, table,
                               f"{key_column}={key_value}")
        return [
            (int(row["_sid"]), {c: row[c] for c in self.plan.columns[nid]})
            for row in rows
        ]

    def delete_partition(self, nid: str, key_column: str,
                         key_value: Any) -> List[int]:
        """Remove one partition and its provenance; return removed sids."""
        existing = self.get_rel_partition(nid, key_column, key_value)
        sids = [sid for sid, _ in existing]
        self.delete_rel_sids(nid, sids)
        self.delete_prov_sids(nid, sids)
        return sids

    def find_sids_by_provenance(self, nid: str, src_node: str,
                                src_row_ids: Sequence[str]) -> List[int]:
        """All current rows of *nid* whose provenance contains any source id.

        Uses the provenance index, so only matching entries are touched.
        """
        if not src_row_ids:
            return []
        table = prov_table(nid)
        qmarks = ", ".join("?" for _ in src_row_ids)
        rows = self.conn.execute(
            f"SELECT DISTINCT _sid FROM {_ident(table)} "
            f"WHERE src_node = ? AND src_row_id IN ({qmarks})",
            [src_node, *src_row_ids],
        ).fetchall()
        for rid in src_row_ids:
            self.ledger.record(OP_READ_PROV, table,
                               f"{src_node}:{rid}")
        return [int(r[0]) for r in rows]

    def get_provenance(self, nid: str,
                       sids: Sequence[int]) -> Dict[int, Dict[Tuple[str, str],
                                                               int]]:
        if not sids:
            return {}
        table = prov_table(nid)
        qmarks = ", ".join("?" for _ in sids)
        rows = self.conn.execute(
            f"SELECT _sid, src_node, src_row_id, copies FROM "
            f"{_ident(table)} WHERE _sid IN ({qmarks})",
            list(sids),
        ).fetchall()
        out: Dict[int, Dict[Tuple[str, str], int]] = {}
        for row in rows:
            sid = int(row["_sid"])
            self.ledger.record(OP_READ_PROV, table, str(sid))
            out.setdefault(sid, {})[(row["src_node"],
                                     row["src_row_id"])] = int(row["copies"])
        return out

    # ------------------------------------------------------------------
    # Release read
    # ------------------------------------------------------------------
    def read_release(self) -> Tuple[int, List[Dict[str, Any]]]:
        """Return ``(version, release rows)`` from one consistent snapshot."""
        version = int(self.get_meta("version"))
        out_cols = self.plan.columns[self.plan.output_node_id]
        table = rel_table(self.plan.output_node_id)
        select = ", ".join(_ident(c) for c in out_cols)
        rows = self.conn.execute(
            f"SELECT {select} FROM {_ident(table)}"
        ).fetchall()
        return version, [{c: row[c] for c in out_cols} for row in rows]


def canonical_gkey(group_values: Sequence[Any]) -> str:
    """Deterministic TEXT encoding of a group-by tuple / join key."""
    return json.dumps(list(group_values), ensure_ascii=False,
                      separators=(",", ":"))
