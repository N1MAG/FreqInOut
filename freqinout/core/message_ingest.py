from __future__ import annotations

import datetime
import json
import re
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from freqinout.core.config_paths import get_config_dir
from freqinout.core.js8_multi_source import (
    JS8InstanceSource,
    LEGACY_JS8_SOURCE_KEY,
    LEGACY_JS8_SOURCE_LABEL,
    ensure_js8_local_tables,
    load_js8_inbox_state_map,
    load_js8_inbox_watermarks,
    load_js8_offset_map,
    resolve_js8_instance_sources,
    sync_js8_source_metadata,
    upsert_js8_inbox_watermark,
    upsert_js8_offset_state,
)
from freqinout.core.logger import log
from freqinout.core.settings_manager import SettingsManager


JS8_MAX_AGE_SECONDS = 30 * 24 * 60 * 60  # 30 days
SPOTTER_STATUS_FORM_ID = "304"  # Kept for compatibility with older tests/callers.
SPOTTER_STATUS_FORMS = {"104", "301", "304"}
MCF304_EXPECTED_RESPONSES = 8


class JS8FormDecoder:
    def __init__(self, settings: SettingsManager):
        self.settings = settings
        self._form_cache: Dict[str, List[Dict]] = {}
        self._form_cache_max_entries = 256

    def _prune_form_cache(self) -> None:
        while len(self._form_cache) > self._form_cache_max_entries:
            try:
                self._form_cache.pop(next(iter(self._form_cache)))
            except Exception:
                break

    def decode_form(self, form_id: str, responses: str, comment: str, raw: str = "") -> str:
        form_id = (form_id or "").strip()
        if not form_id:
            return raw or responses
        form = self._load_form_definition(form_id)
        if not form:
            return raw or responses
        out_lines: List[str] = []
        for idx, q in enumerate(form):
            question = (q.get("q", "") or "").strip()
            answers = q.get("ans", {}) or {}
            out_lines.append(question)
            if idx < len(responses):
                code = responses[idx]
                ans = answers.get(code, f"(unknown: {code})")
                out_lines.append(ans)
            else:
                out_lines.append("(no response)")
            out_lines.append("")
        if comment:
            out_lines.append("Comment:")
            out_lines.append(comment.strip())
        return "\n".join(out_lines).strip() or (raw or responses)

    def _load_form_definition(self, form_id: str) -> List[Dict]:
        if form_id in self._form_cache:
            return self._form_cache[form_id]
        forms_dir = (self.settings.get("js8_forms_path", "") or "").strip()
        if not forms_dir:
            return []
        path = Path(forms_dir) / f"MCF{form_id}.txt"
        if not path.exists():
            return []
        questions: List[Dict] = []
        current_q = None
        try:
            for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
                line = line.strip()
                if not line or line.startswith("#") or line.startswith("."):
                    continue
                if line.startswith("?"):
                    if current_q:
                        questions.append(current_q)
                    current_q = {"q": line[1:].strip(), "ans": {}}
                elif line.startswith("@") and current_q:
                    try:
                        key, text = line[1], line[2:].strip()
                        current_q["ans"][key] = text
                    except Exception:
                        continue
            if current_q:
                questions.append(current_q)
        except Exception as e:
            log.debug("MessageIngest: failed to parse form %s: %s", form_id, e)
            questions = []
        self._form_cache[form_id] = questions
        self._prune_form_cache()
        return questions


class MessageIngestor:
    def __init__(self, settings: SettingsManager):
        self.settings = settings
        self._decoder = JS8FormDecoder(settings)

    def ingest_js8_messages(self) -> None:
        sources = resolve_js8_instance_sources(self.settings)
        if not sources:
            return
        db_path = self._local_js8_db()
        if not db_path:
            return
        local_conn = sqlite3.connect(db_path)
        try:
            ensure_js8_local_tables(local_conn, settings=self.settings)
            sync_js8_source_metadata(local_conn, sources)
            state_map = load_js8_inbox_state_map(local_conn)
            watermarks = load_js8_inbox_watermarks(local_conn)
            now_ts = time.time()
            cur = local_conn.cursor()
            for source in sources:
                inbox_path = source.inbox_path
                if not inbox_path or not inbox_path.exists():
                    continue
                last_remote_id = int(watermarks.get(source.source_key, 0) or 0)
                rows = self._read_source_inbox_rows(inbox_path, last_remote_id)
                if not rows:
                    upsert_js8_inbox_watermark(local_conn, source, last_remote_id)
                    continue
                payload: List[Tuple[Any, ...]] = []
                max_remote_id = last_remote_id
                for row in rows:
                    parsed = self._parse_js8_inbox_row(row, now_ts)
                    if not parsed:
                        continue
                    remote_id = int(parsed["remote_id"])
                    state_key = (source.source_key, remote_id)
                    saved_state = state_map.get(state_key)
                    effective_state = saved_state[0] if saved_state else parsed["state"]
                    read_ts = saved_state[1] if saved_state else 0.0
                    payload.append(
                        (
                            source.source_key,
                            source.source_scope,
                            source.source_label,
                            source.device_profile_id,
                            remote_id,
                            str(source.inbox_path or ""),
                            str(source.directed_path or ""),
                            str(source.all_path or ""),
                            parsed["from_call"],
                            parsed["to_call"],
                            parsed["msg_type"],
                            parsed["utc_str"],
                            parsed["utc_ts"],
                            parsed["raw_text"],
                            parsed["decoded_text"],
                            effective_state,
                            read_ts,
                        )
                    )
                    max_remote_id = max(max_remote_id, remote_id)
                    try:
                        self._enqueue_next_msg_id(str(parsed["from_call"]), str(parsed["raw_text"]))
                    except Exception:
                        pass
                if payload:
                    cur.executemany(
                        """
                        INSERT INTO js8_messages_v2 (
                            source_key, source_scope, source_label, device_profile_id, remote_id,
                            inbox_path, directed_path, all_path,
                            from_call, to_call, msg_type, utc_str, utc_ts, raw_text, decoded_text,
                            state, read_ts
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(source_key, remote_id) DO UPDATE SET
                            source_scope=excluded.source_scope,
                            source_label=excluded.source_label,
                            device_profile_id=excluded.device_profile_id,
                            inbox_path=excluded.inbox_path,
                            directed_path=excluded.directed_path,
                            all_path=excluded.all_path,
                            from_call=excluded.from_call,
                            to_call=excluded.to_call,
                            msg_type=excluded.msg_type,
                            utc_str=excluded.utc_str,
                            utc_ts=excluded.utc_ts,
                            raw_text=excluded.raw_text,
                            decoded_text=excluded.decoded_text
                        """,
                        payload,
                    )
                upsert_js8_inbox_watermark(local_conn, source, max_remote_id)
            local_conn.commit()
        finally:
            local_conn.close()

    def ingest_spotter_from_directed(self) -> None:
        sources = resolve_js8_instance_sources(self.settings)
        if not sources:
            return
        db_path = self._db_path()
        if not db_path:
            return
        conn = sqlite3.connect(db_path)
        try:
            self._ensure_spotter_table_conn(conn)
            offsets = load_js8_offset_map(conn, "js8_spotter_ingest_state_v2")
            cur = conn.cursor()
            for source in sources:
                directed_path = source.directed_path
                if not directed_path or not directed_path.exists():
                    continue
                try:
                    offset = int(offsets.get(source.source_key, 0) or 0)
                except Exception:
                    offset = 0
                try:
                    size_now = directed_path.stat().st_size
                    if offset < 0 or offset > size_now:
                        offset = 0
                    with directed_path.open("r", encoding="utf-8", errors="ignore") as fh:
                        if offset:
                            fh.seek(offset)
                        last_pos = fh.tell()
                        while True:
                            line = fh.readline()
                            if not line:
                                break
                            last_pos = fh.tell()
                            parsed = self._parse_directed_spotter_line(line)
                            if not parsed:
                                continue
                            form_id = str(parsed.get("form_id") or "").strip()
                            raw_form = str(parsed.get("raw_form") or "").strip()
                            if not form_id or not raw_form:
                                continue
                            from_call = str(parsed.get("from_call") or "").strip().upper()
                            token = str(parsed.get("spotter_token") or "").strip().upper()
                            if not from_call:
                                continue
                            if self._spotter_exists_cursor(cur, source.source_key, from_call, form_id, token, raw_form):
                                continue
                            form_part, resp, comment = self._parse_form_parts(raw_form)
                            decoded = self._decoder.decode_form(form_part, resp, comment, raw=raw_form)
                            ingested_ts = float(time.time())
                            cur.execute(
                                """
                                INSERT INTO spotter_traffic
                                    (utc_ts, utc_str, from_call, to_call, form_id, spotter_token,
                                     raw_text, decoded_text, state, read_ts, relay_via, ingested_ts,
                                     source_key, source_scope, source_label, device_profile_id, directed_path)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'UNREAD', 0, ?, ?, ?, ?, ?, ?, ?)
                                """,
                                (
                                    float(parsed.get("utc_ts") or 0.0),
                                    str(parsed.get("utc_str") or ""),
                                    from_call,
                                    str(parsed.get("to_call") or "").strip().upper(),
                                    form_id,
                                    token,
                                    raw_form,
                                    decoded or raw_form,
                                    str(parsed.get("relay_via") or "").strip().upper(),
                                    ingested_ts,
                                    source.source_key,
                                    source.source_scope,
                                    source.source_label,
                                    source.device_profile_id,
                                    str(source.directed_path or ""),
                                ),
                            )
                            self._upsert_spotter_station_status(
                                cur,
                                from_call=from_call,
                                form_id=form_id,
                                response_code=resp,
                                raw_form=raw_form,
                                utc_ts=float(parsed.get("utc_ts") or 0.0),
                                utc_str=str(parsed.get("utc_str") or ""),
                                ingested_ts=ingested_ts,
                            )
                        upsert_js8_offset_state(conn, "js8_spotter_ingest_state_v2", source, int(last_pos))
                except Exception as exc:
                    log.debug(
                        "MessageIngest: spotter ingest failed reading DIRECTED.TXT for %s: %s",
                        source.source_key,
                        exc,
                    )
            conn.commit()
        finally:
            conn.close()

    def _read_source_inbox_rows(self, inbox_path: Path, min_remote_id: int) -> List[tuple]:
        queries = [
            ("inbox_v1", "id", "id, json, type, value"),
            ("inbox_v1", "rowid", "rowid as id, json, type, value"),
            ("inbox_v1", "id", "id, message, type, value"),
            ("inbox_v1", "id", "id, blob"),
            ("inbox", "id", "id, json, type, value"),
            ("inbox", "rowid", "rowid as id, json, type, value"),
            ("inbox", "id", "id, message, type, value"),
        ]
        try:
            conn = sqlite3.connect(inbox_path)
            try:
                cur = conn.cursor()
                for table, id_expr, cols in queries:
                    try:
                        cur.execute(
                            f"SELECT {cols} FROM {table} WHERE {id_expr} > ? ORDER BY {id_expr} ASC",
                            (int(min_remote_id or 0),),
                        )
                        rows = cur.fetchall()
                        if rows:
                            return rows
                    except Exception:
                        continue
            finally:
                conn.close()
        except Exception as exc:
            log.debug("MessageIngest: JS8 ingest read failed for %s: %s", inbox_path, exc)
        return []

    def _parse_js8_inbox_row(self, row: tuple, now_ts: float) -> Optional[Dict[str, Any]]:
        remote_id = int(row[0] or 0) if len(row) > 0 else 0
        if remote_id <= 0:
            return None
        blob = row[1] if len(row) > 1 else ""
        row_state = row[2] if len(row) > 2 else ""
        try:
            parsed = json.loads(blob or "{}")
            if "params" not in parsed and len(row) >= 4:
                parsed = {
                    "params": parsed,
                    "type": row[2] if len(row) > 2 else "",
                    "value": row[3] if len(row) > 3 else "",
                }
            params = parsed.get("params", {}) or {}
            if not row_state:
                row_state = parsed.get("type", "") or parsed.get("TYPE", "")
        except Exception:
            params = {}
        text = (params.get("TEXT") or "").strip()
        from_call = (params.get("FROM") or "").strip().upper()
        to_call = (params.get("TO") or "").strip().upper()
        utc_str = (params.get("UTC") or "").strip()
        try:
            utc_ts = datetime.datetime.strptime(utc_str, "%Y-%m-%d %H:%M:%S").timestamp()
        except Exception:
            utc_ts = 0.0
        if utc_ts and (now_ts - utc_ts) > JS8_MAX_AGE_SECONDS:
            return None
        msg_type = "MSG"
        decoded = text
        if text.startswith("F!"):
            form_part, resp, comment = self._parse_form_parts(text)
            msg_type = f"F!{form_part}" if form_part else "MSG"
            decoded = self._decoder.decode_form(form_part, resp, comment, raw=text)
        return {
            "remote_id": remote_id,
            "from_call": from_call,
            "to_call": to_call,
            "msg_type": msg_type,
            "utc_str": utc_str,
            "utc_ts": utc_ts,
            "raw_text": text,
            "decoded_text": decoded,
            "state": (str(row_state or "").upper() or "UNREAD"),
        }

    def _db_path(self) -> Path | None:
        try:
            return get_config_dir() / "config" / "freqinout_nets.db"
        except Exception as e:
            log.debug("MessageIngest: failed to resolve DB path: %s", e)
            return None

    def _backlog_db_path(self) -> Path | None:
        return self._db_path()

    def _ensure_backlog_table(self) -> None:
        db_path = self._backlog_db_path()
        if not db_path:
            return
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS autoquery_backlog (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    callsign TEXT NOT NULL,
                    msg_id TEXT,
                    kind TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'PENDING',
                    attempts INTEGER DEFAULT 0,
                    last_attempt_ts REAL,
                    created_ts REAL
                )
                """
            )
            conn.commit()
            conn.close()
        except Exception as e:
            log.debug("MessageIngest: failed to ensure backlog table: %s", e)

    def _enqueue_next_msg_id(self, from_call: str, text: str) -> None:
        call = (from_call or "").strip().upper()
        if not call or not text:
            return
        m = re.search(r"NEXT\s+MSG\s+ID\s+(\d+)", text.upper())
        if not m:
            return
        next_id = m.group(1)
        if not next_id:
            return
        self._ensure_backlog_table()
        db_path = self._backlog_db_path()
        if not db_path:
            return
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                """
                SELECT 1 FROM autoquery_backlog
                WHERE callsign=? AND COALESCE(msg_id,'')=COALESCE(?, '') AND kind='MSG'
                LIMIT 1
                """,
                (call, next_id),
            )
            if cur.fetchone():
                conn.close()
                return
            now_ts = time.time()
            cur.execute(
                """
                INSERT INTO autoquery_backlog (callsign, msg_id, kind, status, attempts, last_attempt_ts, created_ts)
                VALUES (?, ?, 'MSG', 'PENDING', 0, ?, ?)
                """,
                (call, next_id, now_ts, now_ts),
            )
            conn.commit()
            conn.close()
        except Exception as e:
            log.debug("MessageIngest: failed to enqueue NEXT MSG ID: %s", e)

    def _spotter_offset_key(self) -> str:
        return "spotter_directed_offset"

    def _resolve_directed_path(self) -> Optional[Path]:
        directed = (self.settings.get("js8_directed_path", "") or "").strip()
        if not directed:
            return None
        return Path(directed)

    def _spotter_exists_cursor(
        self,
        cur: sqlite3.Cursor,
        source_key: str,
        from_call: str,
        form_id: str,
        token: str,
        raw_text: str,
    ) -> bool:
        source = (source_key or "").strip()
        if not source:
            source = LEGACY_JS8_SOURCE_KEY
        try:
            if token:
                cur.execute(
                    """
                    SELECT 1 FROM spotter_traffic
                    WHERE source_key=? AND from_call=? AND form_id=? AND spotter_token=?
                    LIMIT 1
                    """,
                    (source, from_call, form_id, token),
                )
            else:
                cur.execute(
                    """
                    SELECT 1 FROM spotter_traffic
                    WHERE source_key=? AND from_call=? AND form_id=? AND raw_text=?
                    LIMIT 1
                    """,
                    (source, from_call, form_id, raw_text),
                )
            return cur.fetchone() is not None
        except Exception:
            return False

    def _spotter_exists(self, from_call: str, form_id: str, token: str, raw_text: str) -> bool:
        db_path = self._db_path()
        if not db_path:
            return False
        try:
            conn = sqlite3.connect(db_path)
            exists = self._spotter_exists_cursor(
                conn.cursor(),
                LEGACY_JS8_SOURCE_KEY,
                from_call,
                form_id,
                token,
                raw_text,
            )
            conn.close()
            return exists
        except Exception:
            return False

    def _parse_directed_spotter_line(self, line: str) -> Optional[Dict[str, str | float]]:
        if not line:
            return None
        if not line.rstrip().endswith("\u2662"):
            return None
        parts = [p for p in line.strip().split("\t") if p]
        if len(parts) < 5:
            parts = re.split(r"\s+", line.strip(), maxsplit=4)
        if len(parts) < 5:
            return None
        dt_str, _freq_txt, _shift, _snr_txt, msg = parts[0], parts[1], parts[2], parts[3], parts[4]
        if ":" not in msg:
            return None
        msg_upper = msg.upper()
        if "?" in msg_upper or "E?" in msg_upper:
            return None
        if "..." in msg:
            return None
        if re.search(r"\bMSG\b", msg_upper):
            return None
        form_match = re.search(r"F!(\d{3})", msg_upper)
        if not form_match:
            return None
        try:
            ts = datetime.datetime.strptime(dt_str[:19], "%Y-%m-%d %H:%M:%S").replace(
                tzinfo=datetime.timezone.utc
            )
        except Exception:
            return None
        relay_via, rest = msg.split(":", 1)
        relay_via = relay_via.strip().upper()
        rest = rest.strip()
        dest_token = (rest.split() or [""])[0]
        dest = dest_token.split(">")[0].strip().strip(",").upper()
        if not dest:
            return None
        de_match = re.search(r"\*DE\*\s*([A-Z0-9/]+)", msg_upper)
        from_call = de_match.group(1) if de_match else relay_via
        form_start = msg_upper.find("F!")
        if form_start < 0:
            return None
        raw_form = msg[form_start:].strip()
        raw_form = re.split(r"\*DE\*", raw_form, maxsplit=1, flags=re.IGNORECASE)[0].strip()
        if raw_form.endswith("\u2662"):
            raw_form = raw_form[:-1].rstrip()
        token_match = re.search(r"(#[A-Z0-9]{3,})", raw_form.upper())
        token = token_match.group(1) if token_match else ""
        form_id = form_match.group(1)
        return {
            "utc_ts": ts.timestamp(),
            "utc_str": ts.strftime("%Y-%m-%d %H:%M:%S"),
            "from_call": from_call.strip().upper(),
            "to_call": dest.strip().upper(),
            "form_id": form_id,
            "spotter_token": token,
            "raw_form": raw_form,
            "relay_via": relay_via,
        }

    @staticmethod
    def _parse_form_parts(text: str) -> tuple[str, str, str]:
        parts = (text or "").split()
        if not parts or not parts[0].startswith("F!"):
            return "", "", ""
        form_part = parts[0][2:] if len(parts[0]) > 2 else ""
        resp = parts[1] if len(parts) > 1 else ""
        comment = " ".join(parts[2:]) if len(parts) > 2 else ""
        return form_part, resp, comment

    @staticmethod
    def _classify_mcf304_status(response_code: str) -> tuple[str, str]:
        digits = [ch for ch in (response_code or "") if ch in "12345"]
        if not digits:
            return "unknown", "Unknown"
        if "3" in digits:
            return "red", "Not Functioning"
        if "2" in digits:
            return "yellow", "Partially Functioning"
        # Only classify green if the full form was answered and every answer is "1".
        if len(digits) >= MCF304_EXPECTED_RESPONSES and all(ch == "1" for ch in digits[:MCF304_EXPECTED_RESPONSES]):
            return "green", "Functioning"
        return "unknown", "Unknown"

    @staticmethod
    def _status_label(status_key: str) -> str:
        key = (status_key or "").strip().lower()
        if key == "red":
            return "Not Functioning"
        if key == "yellow":
            return "Partially Functioning"
        if key == "green":
            return "Functioning"
        return "Unknown"

    @classmethod
    def _classify_spotter_status(
        cls, form_id: str, response_code: str
    ) -> tuple[str, str, str]:
        fid = (form_id or "").strip()
        if fid == "104":
            code = (response_code or "").strip().upper()[:1]
            if code == "1":
                return "green", cls._status_label("green"), "Q1"
            if code == "2":
                return "yellow", cls._status_label("yellow"), "Q1"
            if code == "3":
                return "red", cls._status_label("red"), "Q1"
            return "unknown", cls._status_label("unknown"), "Q1"

        if fid == "301":
            codes = list((response_code or "").strip().upper())
            # Field Situation Report: evaluate Q2-Q9 only.
            q2_map = {"1": "green", "2": "yellow", "3": "red", "4": "unknown"}
            q3_map = {"1": "green", "2": "yellow", "3": "yellow", "4": "red", "5": "unknown"}
            q4_q9_map = {"1": "green", "2": "yellow", "3": "red", "4": "unknown"}
            eval_statuses: List[str] = []
            for idx in range(1, 9):
                code = codes[idx] if idx < len(codes) else ""
                if idx == 1:
                    eval_statuses.append(q2_map.get(code, "unknown"))
                elif idx == 2:
                    eval_statuses.append(q3_map.get(code, "unknown"))
                else:
                    eval_statuses.append(q4_q9_map.get(code, "unknown"))
            if any(s == "red" for s in eval_statuses):
                return "red", cls._status_label("red"), "Q2-Q9 aggregate"
            if any(s == "yellow" for s in eval_statuses):
                return "yellow", cls._status_label("yellow"), "Q2-Q9 aggregate"
            if eval_statuses and all(s == "green" for s in eval_statuses):
                return "green", cls._status_label("green"), "Q2-Q9 aggregate"
            return "unknown", cls._status_label("unknown"), "Q2-Q9 aggregate"

        if fid == "304":
            status_key, status_label = cls._classify_mcf304_status(response_code)
            return status_key, status_label, "Aggregate"

        return "unknown", cls._status_label("unknown"), ""

    def _upsert_spotter_station_status(
        self,
        cur: sqlite3.Cursor,
        *,
        from_call: str,
        form_id: str,
        response_code: str,
        raw_form: str,
        utc_ts: float,
        utc_str: str,
        ingested_ts: float,
        status_source: str = "",
    ) -> None:
        fid = (form_id or "").strip()
        if fid not in SPOTTER_STATUS_FORMS:
            return
        call = (from_call or "").strip().upper()
        if not call:
            return
        status_key, status_label, source_detail = self._classify_spotter_status(fid, response_code)
        source = (status_source or "").strip().upper() or f"F!{fid}"
        cur.execute(
            """
            INSERT INTO spotter_station_status
                (from_call, form_id, status_key, status_label, response_code, updated_utc_ts, updated_utc_str,
                 raw_text, updated_ingested_ts, status_source, status_source_detail)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(from_call) DO UPDATE SET
                form_id=excluded.form_id,
                status_key=excluded.status_key,
                status_label=excluded.status_label,
                response_code=excluded.response_code,
                updated_utc_ts=excluded.updated_utc_ts,
                updated_utc_str=excluded.updated_utc_str,
                raw_text=excluded.raw_text,
                updated_ingested_ts=excluded.updated_ingested_ts,
                status_source=excluded.status_source,
                status_source_detail=excluded.status_source_detail
            WHERE (
                excluded.updated_utc_ts > COALESCE(spotter_station_status.updated_utc_ts, 0)
                OR (
                    excluded.updated_utc_ts = COALESCE(spotter_station_status.updated_utc_ts, 0)
                    AND excluded.updated_ingested_ts >= COALESCE(spotter_station_status.updated_ingested_ts, 0)
                )
            )
            """,
            (
                call,
                fid,
                status_key,
                status_label,
                (response_code or "").strip(),
                float(utc_ts or 0.0),
                (utc_str or "").strip(),
                (raw_form or "").strip(),
                float(ingested_ts or 0.0),
                source,
                source_detail,
            ),
        )

    def _backfill_spotter_station_status(self, cur: sqlite3.Cursor) -> None:
        try:
            cur.execute("SELECT COUNT(1) FROM spotter_station_status")
            row = cur.fetchone()
            has_rows = bool(row and int(row[0] or 0) > 0)
            if has_rows:
                cur.execute(
                    "SELECT COUNT(1) FROM spotter_station_status WHERE form_id IN ('104','301')"
                )
                upgraded = cur.fetchone()
                if upgraded and int(upgraded[0] or 0) > 0:
                    return
            forms = sorted(SPOTTER_STATUS_FORMS)
            placeholders = ",".join(["?"] * len(forms))
            cur.execute(
                f"""
                SELECT from_call, form_id, raw_text, utc_ts, utc_str, ingested_ts
                FROM spotter_traffic
                WHERE form_id IN ({placeholders})
                ORDER BY from_call ASC, COALESCE(utc_ts, 0) DESC, COALESCE(ingested_ts, 0) DESC, id DESC
                """,
                tuple(forms),
            )
            seen: set[str] = set()
            for from_call, form_id, raw_text, utc_ts, utc_str, ingested_ts in cur.fetchall():
                call = (from_call or "").strip().upper()
                if not call or call in seen:
                    continue
                parsed_form_id, response_code, _ = self._parse_form_parts(str(raw_text or ""))
                self._upsert_spotter_station_status(
                    cur,
                    from_call=call,
                    form_id=str(form_id or parsed_form_id or ""),
                    response_code=response_code,
                    raw_form=str(raw_text or ""),
                    utc_ts=float(utc_ts or 0.0),
                    utc_str=str(utc_str or ""),
                    ingested_ts=float(ingested_ts or 0.0),
                    status_source=f"F!{str(form_id or parsed_form_id or '').strip()}",
                )
                seen.add(call)
        except Exception as e:
            log.debug("MessageIngest: spotter status backfill failed: %s", e)

    def _ensure_spotter_table_conn(self, conn: sqlite3.Connection) -> None:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS spotter_traffic (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                utc_ts REAL,
                utc_str TEXT,
                from_call TEXT,
                to_call TEXT,
                form_id TEXT,
                spotter_token TEXT,
                raw_text TEXT,
                decoded_text TEXT,
                state TEXT,
                read_ts REAL,
                flag_state INTEGER DEFAULT 0,
                relay_via TEXT,
                ingested_ts REAL,
                source_key TEXT,
                source_scope TEXT,
                source_label TEXT,
                device_profile_id INTEGER,
                directed_path TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS spotter_station_status (
                from_call TEXT PRIMARY KEY,
                form_id TEXT NOT NULL,
                status_key TEXT NOT NULL,
                status_label TEXT NOT NULL,
                response_code TEXT,
                updated_utc_ts REAL NOT NULL DEFAULT 0,
                updated_utc_str TEXT,
                raw_text TEXT,
                updated_ingested_ts REAL,
                status_source TEXT,
                status_source_detail TEXT
            )
            """
        )
        for col_name, col_ddl in (
            ("flag_state", "INTEGER DEFAULT 0"),
            ("source_key", "TEXT"),
            ("source_scope", "TEXT"),
            ("source_label", "TEXT"),
            ("device_profile_id", "INTEGER"),
            ("directed_path", "TEXT"),
        ):
            try:
                cur.execute(f"ALTER TABLE spotter_traffic ADD COLUMN {col_name} {col_ddl}")
            except Exception:
                pass
        for col_name, col_ddl in (
            ("status_source", "TEXT"),
            ("status_source_detail", "TEXT"),
        ):
            try:
                cur.execute(f"ALTER TABLE spotter_station_status ADD COLUMN {col_name} {col_ddl}")
            except Exception:
                pass
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_spotter_traffic_form_call_ts ON spotter_traffic(form_id, from_call, utc_ts DESC, id DESC)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_spotter_traffic_from_ts ON spotter_traffic(from_call, utc_ts DESC, id DESC)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_spotter_traffic_source_ts ON spotter_traffic(source_key, utc_ts DESC, id DESC)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_spotter_status_key_ts ON spotter_station_status(status_key, updated_utc_ts DESC)"
        )
        legacy_directed = (self.settings.get("js8_directed_path", "") or "").strip()
        cur.execute(
            """
            UPDATE spotter_traffic
               SET source_key=COALESCE(NULLIF(source_key, ''), ?),
                   source_scope=COALESCE(NULLIF(source_scope, ''), 'legacy'),
                   source_label=COALESCE(NULLIF(source_label, ''), ?),
                   directed_path=COALESCE(NULLIF(directed_path, ''), ?)
             WHERE COALESCE(source_key, '')=''
            """,
            (LEGACY_JS8_SOURCE_KEY, LEGACY_JS8_SOURCE_LABEL, legacy_directed),
        )
        self._backfill_spotter_station_status(cur)

    def _ensure_spotter_table(self) -> None:
        db_path = self._db_path()
        if not db_path:
            return
        try:
            conn = sqlite3.connect(db_path)
            self._ensure_spotter_table_conn(conn)
            conn.commit()
            conn.close()
        except Exception as e:
            log.debug("MessageIngest: failed to ensure spotter table: %s", e)

    def _inbox_path(self) -> Path | None:
        directed = (self.settings.get("js8_directed_path", "") or "").strip()
        if not directed:
            return None
        p = Path(directed)
        candidates = [
            p.parent / "inbox_v1",
            p.parent / "inbox_v1.sqlite",
            p.parent / "inbox_v1.db",
            p.parent / "inbox.db3",
        ]
        for c in candidates:
            if c.exists():
                return c
        for c in p.parent.glob("inbox*"):
            if c.is_file():
                return c
        return candidates[0]

    def _local_js8_db(self) -> Path | None:
        try:
            return get_config_dir() / "config" / "freqinout_nets.db"
        except Exception as e:
            log.debug("MessageIngest: failed to resolve local JS8 DB path: %s", e)
            return None

    def _load_js8_state_map(self) -> Dict[int, Tuple[str, float]]:
        db_path = self._local_js8_db()
        if not db_path or not db_path.exists():
            return {}
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                "CREATE TABLE IF NOT EXISTS js8_inbox_state (id INTEGER PRIMARY KEY, state TEXT, last_seen REAL, read_ts REAL, last_ingested_id INTEGER)"
            )
            cur.execute("SELECT id, state, read_ts FROM js8_inbox_state")
            rows = cur.fetchall()
            conn.close()
            return {int(r[0]): ((r[1] or "").upper(), float(r[2] or 0.0)) for r in rows if r and r[0] is not None}
        except Exception as e:
            log.debug("MessageIngest: failed to load js8 state map: %s", e)
            return {}

    def _ensure_local_js8_tables(self) -> None:
        db_path = self._local_js8_db()
        if not db_path:
            return
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS js8_messages (
                id INTEGER PRIMARY KEY,
                from_call TEXT,
                to_call TEXT,
                msg_type TEXT,
                utc_str TEXT,
                utc_ts REAL,
                raw_text TEXT,
                decoded_text TEXT,
                state TEXT,
                read_ts REAL,
                flag_state INTEGER DEFAULT 0
            )
            """
        )
        cur.execute(
            "CREATE TABLE IF NOT EXISTS js8_inbox_state (id INTEGER PRIMARY KEY, state TEXT, last_seen REAL, read_ts REAL, last_ingested_id INTEGER)"
        )
        try:
            cur.execute("ALTER TABLE js8_messages ADD COLUMN read_ts REAL")
        except Exception:
            pass
        try:
            cur.execute("ALTER TABLE js8_messages ADD COLUMN flag_state INTEGER DEFAULT 0")
        except Exception:
            pass
        try:
            cur.execute("ALTER TABLE js8_inbox_state ADD COLUMN read_ts REAL")
        except Exception:
            pass
        try:
            cur.execute("ALTER TABLE js8_inbox_state ADD COLUMN last_ingested_id INTEGER")
        except Exception:
            pass
        conn.commit()
        conn.close()

    def _local_max_js8_id(self) -> int:
        db_path = self._local_js8_db()
        if not db_path or not db_path.exists():
            return 0
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute("SELECT MAX(id) FROM js8_messages")
            row = cur.fetchone()
            conn.close()
            return int(row[0]) if row and row[0] is not None else 0
        except Exception:
            return 0

    def _insert_js8_local(
        self,
        msg_id: int,
        from_call: str,
        to_call: str,
        msg_type: str,
        utc_str: str,
        utc_ts: float,
        raw_text: str,
        decoded_text: str,
        state: str,
        read_ts: float,
    ) -> None:
        db_path = self._local_js8_db()
        if not db_path:
            return
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO js8_messages (id, from_call, to_call, msg_type, utc_str, utc_ts, raw_text, decoded_text, state, read_ts)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO NOTHING
                """,
                (
                    int(msg_id),
                    from_call,
                    to_call,
                    msg_type,
                    utc_str,
                    float(utc_ts or 0.0),
                    raw_text,
                    decoded_text,
                    state,
                    float(read_ts or 0.0),
                ),
            )
            conn.commit()
            conn.close()
        except Exception as e:
            log.debug("MessageIngest: failed to insert local js8 message: %s", e)
