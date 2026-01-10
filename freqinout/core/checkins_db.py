from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Dict, Any, List

from freqinout.core.logger import log
from freqinout.core.config_paths import get_config_dir


def _db_path() -> Path:
    """
    Returns the path to freqinout_nets.db under the shared config directory.
    """
    try:
        config_dir = get_config_dir() / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        return config_dir / "freqinout_nets.db"
    except Exception as e:
        log.error("checkin_db: failed to determine DB path, falling back to home: %s", e)
        fallback = Path.home() / "freqinout_nets.db"
        return fallback


def _ensure_table(conn: sqlite3.Connection):
    """
    Ensures operator_checkins exists with the unified schema, migrating
    from older layouts if necessary.
    """
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS operator_checkins (
            callsign TEXT PRIMARY KEY,
            name TEXT,
            state TEXT,
            grid TEXT,
            group1 TEXT,
            group2 TEXT,
            group3 TEXT,
            group_role TEXT,
            first_seen_utc TEXT,
            last_seen_utc TEXT,
            last_net TEXT,
            last_role TEXT,
            checkin_count INTEGER DEFAULT 0,
            groups_json TEXT,
            trusted INTEGER DEFAULT 1
        )
        """
    )

    # Migrate legacy schemas by recreating when key columns are missing
    cur.execute("PRAGMA table_info(operator_checkins)")
    cols = {row[1] for row in cur.fetchall()}
    desired = {
        "callsign",
        "name",
        "state",
        "grid",
        "group1",
        "group2",
        "group3",
        "group_role",
        "first_seen_utc",
        "last_seen_utc",
        "last_net",
        "last_role",
        "checkin_count",
        "groups_json",
        "trusted",
    }
    if desired.issubset(cols):
        return

    try:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS operator_checkins_new (
                callsign TEXT PRIMARY KEY,
                name TEXT,
                state TEXT,
                grid TEXT,
                group1 TEXT,
                group2 TEXT,
                group3 TEXT,
                group_role TEXT,
                first_seen_utc TEXT,
                last_seen_utc TEXT,
                last_net TEXT,
                last_role TEXT,
                checkin_count INTEGER DEFAULT 0,
                groups_json TEXT,
                trusted INTEGER DEFAULT 1
            )
            """
        )
        # Copy what we can from the old table
        cur.execute("PRAGMA table_info(operator_checkins)")
        legacy_cols = [row[1] for row in cur.fetchall()]
        has_last_seen = "last_seen_utc" in legacy_cols
        has_groups_json = "groups_json" in legacy_cols
        insert_stmt = """
            INSERT OR REPLACE INTO operator_checkins_new
                (callsign, name, state, grid, group1, group2, group3, group_role,
                 first_seen_utc, last_seen_utc, last_net, last_role,
                 checkin_count, groups_json, trusted)
            SELECT
                callsign,
                COALESCE(name,''),
                COALESCE(state,''),
                COALESCE(grid,''),
                COALESCE(group1,''),
                COALESCE(group2,''),
                COALESCE(group3,''),
                COALESCE(group_role,''),
                COALESCE(last_seen_utc, date_added, ''),  -- best-effort first_seen
                COALESCE(last_seen_utc, ''),
                COALESCE(last_net,''),
                COALESCE(last_role,''),
                COALESCE(checkin_count,0),
                {groups_expr},
                COALESCE(trusted,1)
            FROM operator_checkins
        """.format(
            groups_expr="groups_json" if has_groups_json else "NULL"
        )
        try:
            cur.execute(insert_stmt)
        except Exception:
            pass
        cur.execute("DROP TABLE operator_checkins")
        cur.execute("ALTER TABLE operator_checkins_new RENAME TO operator_checkins")
    finally:
        conn.commit()


def upsert_checkins(entries: List[Dict[str, Any]]):
    """
    Inserts or updates operator check-ins.

    Each entry should have:
        callsign, name, state, last_seen_utc, last_net, last_role

    checkin_count is incremented if the callsign already exists.
    """
    if not entries:
        return

    db_path = _db_path()

    try:
        conn = sqlite3.connect(db_path)
        _ensure_table(conn)
        cur = conn.cursor()

        for e in entries:
            cs = (e.get("callsign") or "").upper().strip()
            if not cs:
                continue

            name = (e.get("name") or "").strip()
            state = (e.get("state") or "").upper().strip()
            grid = (e.get("grid") or "").strip().upper()
            group1 = (e.get("group1") or "").strip()
            group2 = (e.get("group2") or "").strip()
            group3 = (e.get("group3") or "").strip()
            group_role = (e.get("group_role") or "").strip()
            last_seen = (e.get("last_seen_utc") or "").strip()
            first_seen = (e.get("first_seen_utc") or "").strip()
            last_net = (e.get("last_net") or "").strip()
            last_role = (e.get("last_role") or "").upper().strip()
            groups_json = e.get("groups_json")
            trusted_raw = e.get("trusted")

            # Load existing to preserve first_seen/groups/trusted/checkin_count
            cur.execute(
                """
                SELECT first_seen_utc, last_seen_utc, checkin_count, groups_json, trusted,
                       grid, group1, group2, group3, group_role
                FROM operator_checkins WHERE callsign=?
                """,
                (cs,),
            )
            existing = cur.fetchone()
            if existing:
                (
                    existing_first,
                    existing_last,
                    existing_count,
                    existing_groups_json,
                    existing_trusted,
                    existing_grid,
                    existing_g1,
                    existing_g2,
                    existing_g3,
                    existing_role,
                ) = existing
            else:
                existing_first = ""
                existing_last = ""
                existing_count = 0
                existing_groups_json = None
                existing_trusted = 1
                existing_grid = ""
                existing_g1 = existing_g2 = existing_g3 = ""
                existing_role = ""

            first_out = first_seen or existing_first or last_seen
            last_out = last_seen or existing_last
            groups_json_out = groups_json if groups_json is not None else existing_groups_json
            trusted_out = (
                int(trusted_raw)
                if trusted_raw is not None
                else (int(existing_trusted) if existing_trusted is not None else 1)
            )
            grid_out = grid or existing_grid
            g1_out = group1 or existing_g1
            g2_out = group2 or existing_g2
            g3_out = group3 or existing_g3
            role_out = group_role or existing_role
            insert_count = int(existing_count or 0) + 1

            cur.execute(
                """
                INSERT INTO operator_checkins
                    (callsign, name, state, grid, group1, group2, group3, group_role,
                     first_seen_utc, last_seen_utc, last_net, last_role,
                     checkin_count, groups_json, trusted)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(callsign) DO UPDATE SET
                    name=excluded.name,
                    state=excluded.state,
                    grid=excluded.grid,
                    group1=excluded.group1,
                    group2=excluded.group2,
                    group3=excluded.group3,
                    group_role=excluded.group_role,
                    first_seen_utc=COALESCE(operator_checkins.first_seen_utc, excluded.first_seen_utc),
                    last_seen_utc=COALESCE(excluded.last_seen_utc, operator_checkins.last_seen_utc),
                    last_net=excluded.last_net,
                    last_role=excluded.last_role,
                    checkin_count=operator_checkins.checkin_count + 1,
                    groups_json=COALESCE(excluded.groups_json, operator_checkins.groups_json),
                    trusted=COALESCE(operator_checkins.trusted, excluded.trusted)
                """,
                (
                    cs,
                    name,
                    state,
                    grid_out,
                    g1_out,
                    g2_out,
                    g3_out,
                    role_out,
                    first_out,
                    last_out,
                    last_net,
                    last_role,
                    insert_count,
                    groups_json_out,
                    trusted_out,
                ),
            )

            # Mirror last_seen_utc into js8_links metadata if newer
            if last_out:
                _mirror_last_seen_to_js8_links(conn, cs, last_out)

        conn.commit()
        conn.close()
        log.info("checkin_db: saved %d check-in entries.", len(entries))

    except Exception as e:
        log.error("checkin_db: upsert failed: %s", e)


def get_all_operators() -> List[Dict[str, Any]]:
    """
    Returns all operators in the DB, sorted by callsign.
    """
    db_path = _db_path()
    if not db_path.exists():
        return []

    try:
        conn = sqlite3.connect(db_path)
        _ensure_table(conn)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT callsign, name, state, grid, group1, group2, group3, group_role,
                   first_seen_utc, last_seen_utc, last_net, last_role, checkin_count,
                   groups_json, trusted
            FROM operator_checkins
            ORDER BY callsign COLLATE NOCASE
            """
        )
        rows = []
        for (
            cs,
            name,
            st,
            grid,
            g1,
            g2,
            g3,
            role,
            first_seen,
            last_seen,
            last_net,
            last_role,
            count,
            groups_json,
            trusted,
        ) in cur.fetchall():
            rows.append(
                {
                    "callsign": cs or "",
                    "name": name or "",
                    "state": st or "",
                    "grid": grid or "",
                    "group1": g1 or "",
                    "group2": g2 or "",
                    "group3": g3 or "",
                    "group_role": role or "",
                    "first_seen_utc": first_seen or "",
                    "last_seen_utc": last_seen or "",
                    "last_net": last_net or "",
                    "last_role": last_role or "",
                    "checkin_count": count or 0,
                    "groups_json": groups_json,
                    "trusted": trusted if trusted is not None else 1,
                }
            )
        conn.close()
        return rows

    except Exception as e:
        log.error("checkin_db: get_all_operators failed: %s", e)
        return []


def _ensure_js8_links_seen(conn: sqlite3.Connection) -> None:
    """
    Ensure js8_links has a last_seen_utc column for mirroring, adding if needed.
    """
    try:
        cur = conn.execute("PRAGMA table_info(js8_links)")
        cols = {row[1] for row in cur.fetchall()}
        if "last_seen_utc" not in cols:
            conn.execute("ALTER TABLE js8_links ADD COLUMN last_seen_utc TEXT")
            conn.commit()
    except Exception:
        # js8_links may not exist; ignore here
        return


def _mirror_last_seen_to_js8_links(conn: sqlite3.Connection, callsign: str, last_seen: str) -> None:
    """
    Mirror a newer last_seen_utc into js8_links rows that mention the callsign.
    """
    try:
        _ensure_js8_links_seen(conn)
        conn.execute(
            """
            UPDATE js8_links
               SET last_seen_utc=?
             WHERE (origin=? OR destination=?)
               AND (last_seen_utc IS NULL OR last_seen_utc < ?)
            """,
            (last_seen, callsign, callsign, last_seen),
        )
        conn.commit()
    except Exception:
        # best-effort; station map will still function without this mirror
        return
