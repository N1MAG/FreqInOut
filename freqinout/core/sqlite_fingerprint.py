from __future__ import annotations

import re
import sqlite3
from pathlib import Path
from typing import List, Sequence, Tuple

from freqinout.core.logger import log
from freqinout.core.sqlite_utils import table_exists


DEFAULT_NUMERIC_COLUMNS: tuple[str, ...] = (
    "updated_ts",
    "read_ts",
    "read_status",
    "flag_state",
    "is_deleted",
    "event_ts",
    "utc_ts",
    "ts",
)

DEFAULT_TEXT_COLUMNS: tuple[str, ...] = (
    "source_id",
    "source",
    "source_key",
    "ingest_source_key",
    "source_radio_id",
    "js8_instance_id",
    "source_path",
    "source_db_path",
    "source_table",
    "state",
    "status_label",
)


def sqlite_identifier(name: str) -> str:
    text = str(name or "").strip()
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", text):
        raise ValueError(f"invalid SQLite identifier: {name!r}")
    return f'"{text}"'


def sqlite_table_fingerprint(
    db_path: str | Path | None,
    tables: Sequence[str],
    *,
    numeric_columns: Sequence[str] = DEFAULT_NUMERIC_COLUMNS,
    text_columns: Sequence[str] = DEFAULT_TEXT_COLUMNS,
) -> Tuple[Tuple[str, ...], ...]:
    if not db_path or not Path(db_path).exists():
        return tuple()
    out: List[Tuple[str, ...]] = []
    try:
        conn = sqlite3.connect(db_path)
        try:
            for table_name in tables:
                table = str(table_name or "").strip()
                if not table:
                    continue
                if not table_exists(conn, table):
                    out.append((table, "missing"))
                    continue
                table_sql = sqlite_identifier(table)
                cur = conn.cursor()
                cur.execute(f"PRAGMA table_info({table_sql})")
                columns = {str(row[1]) for row in cur.fetchall()}
                selects = ["COUNT(*)"]
                if "id" in columns:
                    selects.append("COALESCE(MAX(id), 0)")
                for col in text_columns:
                    if col not in columns:
                        continue
                    selects.append(f"COALESCE(MIN(CAST({col} AS TEXT)), '')")
                    selects.append(f"COALESCE(MAX(CAST({col} AS TEXT)), '')")
                    selects.append(f"COALESCE(COUNT(DISTINCT CAST({col} AS TEXT)), 0)")
                    selects.append(f"COALESCE(SUM(LENGTH(COALESCE(CAST({col} AS TEXT), ''))), 0)")
                for col in numeric_columns:
                    if col not in columns:
                        continue
                    selects.append(f"COALESCE(MAX({col}), 0)")
                    selects.append(f"COALESCE(SUM(COALESCE({col}, 0)), 0)")
                cur.execute(f"SELECT {', '.join(selects)} FROM {table_sql}")
                row = cur.fetchone() or ()
                out.append(tuple([table, *[str(value) for value in row]]))
        finally:
            conn.close()
    except Exception as e:
        log.debug("SQLite table fingerprint failed for %s in %s: %s", tables, db_path, e)
        return tuple()
    return tuple(out)
