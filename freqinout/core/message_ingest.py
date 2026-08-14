from __future__ import annotations

import datetime
import hashlib
import json
import re
import sqlite3
import time
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

from freqinout.core.config_paths import get_config_dir
from freqinout.core.js8_expect_dispatcher import dispatch_expect_auto_reply, record_expect_dispatch_hold
from freqinout.core.js8_spotter_forms import (
    MAPPER_SETTINGS_KEY,
    form_id_enabled,
    form_codes_enabled_for,
    forms_enabled_for,
    normalize_form_code,
)
from freqinout.core.js8_spotter_decode import decode_spotter_form_text
from freqinout.core.js8_expect_store import ExpectEvaluationResult, evaluate_expect_request
from freqinout.core.logger import log
from freqinout.core.message_intelligence import analyze_spotter_text
from freqinout.core.observation_projection import observation_from_message_intelligence
from freqinout.core.observation_store import upsert_observation_conn
from freqinout.core.settings_manager import SettingsManager


JS8_MAX_AGE_SECONDS = 30 * 24 * 60 * 60  # 30 days
SPOTTER_STATUS_FORM_ID = "304"  # Kept for compatibility with older tests/callers.
SPOTTER_STATUS_FORMS = {"104", "301", "304"}
MCF304_EXPECTED_RESPONSES = 8
SPOTTER_PROMPT_RE = re.compile(r"([A-Z0-9]{2})\[(.*?)\]\s*", re.IGNORECASE)
SPOTTER_TOKEN_RE = re.compile(r"\s*#[A-Z0-9]{3,}\s*", re.IGNORECASE)


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
        prompt_values = {
            key.upper(): value.strip()
            for key, value in SPOTTER_PROMPT_RE.findall(str(comment or ""))
        }
        remaining_comment = SPOTTER_PROMPT_RE.sub("", str(comment or ""))
        remaining_comment = SPOTTER_TOKEN_RE.sub(" ", remaining_comment).strip()
        out_lines: List[str] = []
        resp_idx = 0
        for q in form:
            question = (q.get("q", "") or "").strip()
            prompt_key = str(q.get("prompt_key", "") or "").strip().upper()
            if prompt_key:
                out_lines.append(question)
                out_lines.append(prompt_values.get(prompt_key, "(no response)"))
                out_lines.append("")
                continue
            answers = q.get("ans", {}) or {}
            out_lines.append(question)
            if resp_idx < len(responses):
                code = responses[resp_idx]
                ans = answers.get(code, f"(unknown: {code})")
                out_lines.append(ans)
            else:
                out_lines.append("(no response)")
            resp_idx += 1
            out_lines.append("")
        if remaining_comment:
            out_lines.append("Comment:")
            out_lines.append(remaining_comment)
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
                elif line.startswith("[") and "]" in line:
                    if current_q:
                        questions.append(current_q)
                        current_q = None
                    prompt_key = line[1 : line.find("]")].strip().upper()
                    prompt_text = line[line.find("]") + 1 :].strip()
                    if prompt_key:
                        questions.append(
                            {
                                "q": prompt_text or prompt_key,
                                "prompt_key": prompt_key,
                                "ans": {},
                            }
                        )
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


ExpectDispatchClientFactory = Callable[[str, str], Any]


class MessageIngestor:
    def __init__(
        self,
        settings: SettingsManager,
        *,
        expect_dispatch_client_factory: Optional[ExpectDispatchClientFactory] = None,
        expect_auto_reply_enabled: Optional[bool] = None,
    ):
        self.settings = settings
        self._decoder = JS8FormDecoder(settings)
        self._expect_dispatch_client_factory = expect_dispatch_client_factory
        self._expect_auto_reply_enabled_override = expect_auto_reply_enabled

    def ingest_js8_messages(
        self,
        *,
        inbox_path: Optional[Path] = None,
        source_radio_id: object = "",
        js8_instance_id: object = "",
        source_key: str = "",
    ) -> None:
        inbox_path = inbox_path or self._inbox_path()
        if not inbox_path or not inbox_path.exists():
            return
        self._ensure_local_js8_tables()
        effective_source_key = self._js8_source_key(source_key=source_key, source_radio_id=source_radio_id, js8_instance_id=js8_instance_id)
        max_local_id = self._local_max_js8_id(source_key=effective_source_key)
        try:
            conn = sqlite3.connect(inbox_path)
            cur = conn.cursor()
            queries = [
                ("inbox_v1", "id, json, type, value"),
                ("inbox_v1", "rowid as id, json, type, value"),
                ("inbox_v1", "id, message, type, value"),
                ("inbox_v1", "id, blob"),
                ("inbox", "id, json, type, value"),
                ("inbox", "rowid as id, json, type, value"),
                ("inbox", "id, message, type, value"),
            ]
            rows = []
            for table, cols in queries:
                try:
                    cur.execute(f"SELECT {cols} FROM {table} WHERE id > ?", (max_local_id,))
                    rows = cur.fetchall()
                    break
                except Exception:
                    rows = []
            conn.close()
        except Exception as e:
            log.debug("MessageIngest: JS8 ingest read failed: %s", e)
            rows = []

        state_map = self._load_js8_state_map(source_key=effective_source_key)
        message_form_codes = self._form_codes_for_flag("messages")
        now_ts = time.time()
        for row in rows:
            rid = row[0] if len(row) > 0 else 0
            if rid <= max_local_id:
                continue
            blob = row[1] if len(row) > 1 else ""
            state = row[2] if len(row) > 2 else ""
            js = blob
            try:
                parsed = json.loads(js or "{}")
                if "params" not in parsed and len(row) >= 4:
                    parsed = {
                        "params": parsed,
                        "type": row[2] if len(row) > 2 else "",
                        "value": row[3] if len(row) > 3 else "",
                    }
                params = parsed.get("params", {}) or {}
                if not state:
                    state = parsed.get("type", "") or parsed.get("TYPE", "")
            except Exception:
                params = {}
            text = (params.get("TEXT") or "").strip()
            from_call = (params.get("FROM") or "").strip().upper()
            to_call = (params.get("TO") or "").strip()
            utc_str = (params.get("UTC") or "").strip()
            try:
                utc_ts = datetime.datetime.strptime(utc_str, "%Y-%m-%d %H:%M:%S").timestamp()
            except Exception:
                utc_ts = 0.0
            if utc_ts and (now_ts - utc_ts) > JS8_MAX_AGE_SECONDS:
                continue
            msg_type = "MSG"
            decoded = text
            if text.startswith("F!"):
                form_part, resp, comment = self._parse_form_parts(text)
                msg_type = f"F!{form_part}" if form_part else "MSG"
                if form_part and not form_id_enabled(form_part, message_form_codes):
                    continue
                decoded = self._decoder.decode_form(form_part, resp, comment, raw=text)
            saved_state = state_map.get(rid)
            if saved_state:
                eff_state = saved_state[0]
                read_ts = saved_state[1]
            else:
                eff_state = (state or "").upper() or "UNREAD"
                read_ts = 0.0
            self._insert_js8_local(
                rid,
                from_call,
                to_call,
                msg_type,
                utc_str,
                utc_ts,
                text,
                decoded,
                eff_state,
                read_ts,
                source_key=effective_source_key,
                source_id=rid,
                source_radio_id=source_radio_id,
                js8_instance_id=js8_instance_id,
                source_path=str(inbox_path),
            )
            try:
                self._enqueue_next_msg_id(
                    from_call,
                    text,
                    source_key=effective_source_key,
                    source_radio_id=source_radio_id,
                    js8_instance_id=js8_instance_id,
                    source_path=str(inbox_path),
                )
            except Exception:
                pass

    def ingest_spotter_from_directed(
        self,
        *,
        directed_path: Optional[Path] = None,
        source_radio_id: object = "",
        js8_instance_id: object = "",
        source_key: object = "",
        offset_key: str = "",
        evaluate_expect: bool = True,
    ) -> int:
        directed_path = directed_path or self._resolve_directed_path()
        if not directed_path or not directed_path.exists():
            return 0
        self._ensure_spotter_table()
        imported = 0
        try:
            offset = int(self.settings.get(offset_key or self._spotter_offset_key(directed_path, source_radio_id), 0) or 0)
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
                    if self._spotter_exists(
                        from_call,
                        form_id,
                        token,
                        raw_form,
                        source_radio_id=source_radio_id,
                        js8_instance_id=js8_instance_id,
                        source_key=source_key,
                    ):
                        continue
                    form_part, resp, comment = self._parse_form_parts(raw_form)
                    decoded = self._decoder.decode_form(form_part, resp, comment, raw=raw_form)
                    if not decoded or decoded == raw_form:
                        decoded = decode_spotter_form_text(raw_form)
                    db_path = self._db_path()
                    if not db_path:
                        continue
                    conn = sqlite3.connect(db_path)
                    cur = conn.cursor()
                    ingested_ts = float(time.time())
                    cur.execute(
                        """
                        INSERT INTO spotter_traffic
                            (utc_ts, utc_str, from_call, to_call, form_id, spotter_token,
                             raw_text, decoded_text, state, read_ts, relay_via,
                             source_radio_id, js8_instance_id, source_key, ingested_ts)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'UNREAD', 0, ?, ?, ?, ?, ?)
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
                            str(source_radio_id or ""),
                            str(js8_instance_id or ""),
                            str(source_key or ""),
                            ingested_ts,
                        ),
                    )
                    imported_id = int(cur.lastrowid or 0)
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
                    self._mirror_spotter_observation(
                        conn,
                        imported_id=imported_id,
                        raw_form=raw_form,
                        form_id=form_id,
                        from_call=from_call,
                        to_call=str(parsed.get("to_call") or "").strip().upper(),
                        utc_str=str(parsed.get("utc_str") or ""),
                        source_radio_id=source_radio_id,
                        js8_instance_id=js8_instance_id,
                        source_kind="directed",
                    )
                    conn.commit()
                    conn.close()
                    imported += 1
                    if evaluate_expect:
                        try:
                            event_id = f"directed:{str(source_radio_id or '')}:{str(js8_instance_id or '')}:{int(parsed.get('utc_ts') or 0)}:{from_call}:{form_id}:{token or raw_form[:24]}"
                            evaluation = evaluate_expect_request(
                                expect_key=f"F!{form_id}",
                                requesting_callsign=from_call,
                                target_group=str(parsed.get("to_call") or ""),
                                source_radio_id=source_radio_id,
                                js8_instance_id=js8_instance_id,
                                event_id=event_id,
                            )
                            self._maybe_dispatch_expect_auto_reply(
                                evaluation,
                                event_id=event_id,
                                source_radio_id=source_radio_id,
                                source_js8_instance_id=js8_instance_id,
                                requesting_callsign=from_call,
                                target_group=str(parsed.get("to_call") or ""),
                            )
                        except Exception as exc:
                            log.debug("MessageIngest: Expect evaluation failed for F!%s from %s: %s", form_id, from_call, exc)
                self.settings.set(offset_key or self._spotter_offset_key(directed_path, source_radio_id), int(last_pos))
                if hasattr(self.settings, "save"):
                    self.settings.save()
        except Exception as e:
            log.debug("MessageIngest: spotter ingest failed reading DIRECTED.TXT: %s", e)
        return imported

    def ingest_spotter_from_js8_events(
        self,
        messages: Iterable[Dict[str, Any]],
        *,
        source_radio_id: object = "",
        js8_instance_id: object = "",
        source_key: object = "",
        evaluate_expect: bool = True,
    ) -> int:
        self._ensure_spotter_table()
        imported = 0
        for event in list(messages or []):
            parsed = self._parse_js8_spotter_event(event)
            if not parsed:
                continue
            form_id = str(parsed.get("form_id") or "").strip()
            raw_form = str(parsed.get("raw_form") or "").strip()
            from_call = str(parsed.get("from_call") or "").strip().upper()
            token = str(parsed.get("spotter_token") or "").strip().upper()
            if not form_id or not raw_form or not from_call:
                continue
            if self._spotter_exists(
                from_call,
                form_id,
                token,
                raw_form,
                source_radio_id=source_radio_id,
                js8_instance_id=js8_instance_id,
                source_key=source_key,
            ):
                continue
            form_part, resp, comment = self._parse_form_parts(raw_form)
            decoded = self._decoder.decode_form(form_part, resp, comment, raw=raw_form)
            if not decoded or decoded == raw_form:
                decoded = decode_spotter_form_text(raw_form)
            db_path = self._db_path()
            if not db_path:
                continue
            ingested_ts = float(time.time())
            try:
                conn = sqlite3.connect(db_path)
                cur = conn.cursor()
                cur.execute(
                    """
                    INSERT INTO spotter_traffic
                        (utc_ts, utc_str, from_call, to_call, form_id, spotter_token,
                         raw_text, decoded_text, state, read_ts, relay_via,
                         source_radio_id, js8_instance_id, source_key, ingested_ts)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'UNREAD', 0, ?, ?, ?, ?, ?)
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
                        str(source_radio_id or ""),
                        str(js8_instance_id or ""),
                        str(source_key or ""),
                        ingested_ts,
                    ),
                )
                imported_id = int(cur.lastrowid or 0)
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
                self._mirror_spotter_observation(
                    conn,
                    imported_id=imported_id,
                    raw_form=raw_form,
                    form_id=form_id,
                    from_call=from_call,
                    to_call=str(parsed.get("to_call") or "").strip().upper(),
                    utc_str=str(parsed.get("utc_str") or ""),
                    source_radio_id=source_radio_id,
                    js8_instance_id=js8_instance_id,
                    source_kind="js8-api",
                )
                conn.commit()
                conn.close()
                imported += 1
            except Exception as exc:
                log.debug("MessageIngest: JS8 event Spotter insert failed: %s", exc)
                continue
            if evaluate_expect:
                try:
                    event_id = f"js8-api:{str(source_radio_id or '')}:{str(js8_instance_id or '')}:{int(parsed.get('utc_ts') or 0)}:{from_call}:{form_id}:{token or raw_form[:24]}"
                    evaluation = evaluate_expect_request(
                        expect_key=f"F!{form_id}",
                        requesting_callsign=from_call,
                        target_group=str(parsed.get("to_call") or ""),
                        source_radio_id=source_radio_id,
                        js8_instance_id=js8_instance_id,
                        event_id=event_id,
                    )
                    self._maybe_dispatch_expect_auto_reply(
                        evaluation,
                        event_id=event_id,
                        source_radio_id=source_radio_id,
                        source_js8_instance_id=js8_instance_id,
                        requesting_callsign=from_call,
                        target_group=str(parsed.get("to_call") or ""),
                    )
                except Exception as exc:
                    log.debug("MessageIngest: JS8 event Expect evaluation failed for F!%s from %s: %s", form_id, from_call, exc)
        return imported

    def _expect_auto_reply_runtime_enabled(self) -> bool:
        if self._expect_auto_reply_enabled_override is not None:
            return bool(self._expect_auto_reply_enabled_override)
        try:
            return bool(self.settings.get("js8_expect_unattended_auto_reply_enabled", False))
        except Exception:
            return False

    def _mirror_spotter_observation(
        self,
        conn: sqlite3.Connection,
        *,
        imported_id: int,
        raw_form: str,
        form_id: str,
        from_call: str,
        to_call: str,
        utc_str: str,
        source_radio_id: object,
        js8_instance_id: object,
        source_kind: str,
    ) -> None:
        try:
            info = analyze_spotter_text(
                raw_form,
                form_name=f"MCF{str(form_id or '').strip()}",
                from_call=from_call,
                to_call=to_call,
            )
            observation = observation_from_message_intelligence(
                info,
                source_ref=f"spotter_traffic:{int(imported_id or 0)}",
                source_family="spotter",
                source_radio_id=self._int_or_none(source_radio_id),
                source_app=str(js8_instance_id or "").strip(),
                received_utc=utc_str,
                event_utc=utc_str,
                status="UNREAD",
                extra_provenance={
                    "ingest_source": source_kind,
                    "js8_instance_id": str(js8_instance_id or "").strip(),
                },
            )
            upsert_observation_conn(conn, observation)
        except Exception as exc:
            log.debug("MessageIngest: observation projection mirror failed: %s", exc)

    @staticmethod
    def _int_or_none(value: object) -> int | None:
        try:
            return int(value) if str(value or "").strip() else None
        except Exception:
            return None

    def _maybe_dispatch_expect_auto_reply(
        self,
        evaluation: ExpectEvaluationResult,
        *,
        event_id: str,
        source_radio_id: object = "",
        source_js8_instance_id: object = "",
        requesting_callsign: object = "",
        target_group: object = "",
    ) -> None:
        if evaluation.decision != "reply-ready":
            return
        if not self._expect_auto_reply_runtime_enabled():
            return
        client_factory = self._expect_dispatch_client_factory
        if client_factory is None:
            record_expect_dispatch_hold(
                evaluation=evaluation,
                reason="No JS8 client factory is configured for Expect auto-reply.",
                event_id=event_id,
                source_radio_id=source_radio_id,
                source_js8_instance_id=source_js8_instance_id,
                requesting_callsign=requesting_callsign,
                target_group=target_group,
            )
            log.debug("MessageIngest: Expect auto-reply runtime enabled, but no JS8 client factory is configured.")
            return
        reply_radio_id = str(evaluation.reply_radio_id or source_radio_id or "")
        reply_js8_instance_id = str(evaluation.reply_js8_instance_id or source_js8_instance_id or "")
        try:
            client = client_factory(reply_radio_id, reply_js8_instance_id)
            if client is None:
                reason = "No JS8 client is available for this Expect auto-reply source."
                source_owner = getattr(client_factory, "__self__", None)
                status = getattr(source_owner, "last_status", None)
                status_reason = str(getattr(status, "reason", "") or "").strip()
                if status_reason:
                    reason = status_reason
                record_expect_dispatch_hold(
                    evaluation=evaluation,
                    reason=reason,
                    event_id=event_id,
                    source_radio_id=source_radio_id,
                    source_js8_instance_id=source_js8_instance_id,
                    requesting_callsign=requesting_callsign,
                    target_group=target_group,
                )
                log.debug("MessageIngest: Expect auto-reply client factory returned no client for radio=%s js8=%s.", reply_radio_id, reply_js8_instance_id)
                return
            dispatch_expect_auto_reply(
                evaluation=evaluation,
                client=client,
                runtime_unattended_enabled=True,
                event_id=event_id,
                source_radio_id=source_radio_id,
                source_js8_instance_id=source_js8_instance_id,
                requesting_callsign=requesting_callsign,
                target_group=target_group,
            )
        except Exception as exc:
            log.debug("MessageIngest: Expect auto-reply dispatch failed for %s: %s", event_id, exc)

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
            for column, col_type in (
                ("source_key", "TEXT"),
                ("source_radio_id", "TEXT"),
                ("js8_instance_id", "TEXT"),
                ("source_path", "TEXT"),
            ):
                try:
                    cur.execute(f"ALTER TABLE autoquery_backlog ADD COLUMN {column} {col_type}")
                except Exception:
                    pass
            conn.commit()
            conn.close()
        except Exception as e:
            log.debug("MessageIngest: failed to ensure backlog table: %s", e)

    def _enqueue_next_msg_id(
        self,
        from_call: str,
        text: str,
        *,
        source_key: object = "",
        source_radio_id: object = "",
        js8_instance_id: object = "",
        source_path: object = "",
    ) -> None:
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
                  AND COALESCE(source_key, '')=COALESCE(?, '')
                LIMIT 1
                """,
                (call, next_id, str(source_key or "").strip()),
            )
            if cur.fetchone():
                conn.close()
                return
            now_ts = time.time()
            cur.execute(
                """
                INSERT INTO autoquery_backlog
                    (callsign, msg_id, kind, status, attempts, last_attempt_ts, created_ts,
                     source_key, source_radio_id, js8_instance_id, source_path)
                VALUES (?, ?, 'MSG', 'PENDING', 0, ?, ?, ?, ?, ?, ?)
                """,
                (
                    call,
                    next_id,
                    now_ts,
                    now_ts,
                    str(source_key or "").strip(),
                    str(source_radio_id or "").strip(),
                    str(js8_instance_id or "").strip(),
                    str(source_path or "").strip(),
                ),
            )
            conn.commit()
            conn.close()
        except Exception as e:
            log.debug("MessageIngest: failed to enqueue NEXT MSG ID: %s", e)

    def _spotter_offset_key(self, directed_path: Optional[Path] = None, source_radio_id: object = "") -> str:
        source = str(source_radio_id or "").strip()
        if source:
            return f"spotter_directed_offset_radio_{source}"
        if directed_path is not None:
            try:
                key_src = str(directed_path.expanduser().resolve())
            except Exception:
                key_src = str(directed_path)
            digest = hashlib.sha1(key_src.encode("utf-8", errors="replace")).hexdigest()[:16]
            return f"spotter_directed_offset_path_{digest}"
        return "spotter_directed_offset"

    def _resolve_directed_path(self) -> Optional[Path]:
        directed = (self.settings.get("js8_directed_path", "") or "").strip()
        if not directed:
            return None
        return Path(directed)

    def _spotter_exists(
        self,
        from_call: str,
        form_id: str,
        token: str,
        raw_text: str,
        *,
        source_radio_id: object = "",
        js8_instance_id: object = "",
        source_key: object = "",
    ) -> bool:
        db_path = self._db_path()
        if not db_path:
            return False
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            source_radio = str(source_radio_id or "").strip()
            source_js8 = str(js8_instance_id or "").strip()
            source_identity = str(source_key or "").strip()
            source_clause = ""
            source_params: tuple[object, ...] = ()
            if source_identity:
                source_clause = " AND COALESCE(source_key, '')=?"
                source_params = (source_identity,)
            elif source_radio or source_js8:
                source_clause = " AND COALESCE(source_radio_id, '')=? AND LOWER(COALESCE(js8_instance_id, ''))=LOWER(?)"
                source_params = (source_radio, source_js8)
            if token:
                cur.execute(
                    f"""
                    SELECT 1 FROM spotter_traffic
                    WHERE from_call=? AND form_id=? AND spotter_token=?
                    {source_clause}
                    LIMIT 1
                    """,
                    (from_call, form_id, token) + source_params,
                )
            else:
                cur.execute(
                    f"""
                    SELECT 1 FROM spotter_traffic
                    WHERE from_call=? AND form_id=? AND raw_text=?
                    {source_clause}
                    LIMIT 1
                    """,
                    (from_call, form_id, raw_text) + source_params,
                )
            exists = cur.fetchone() is not None
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
        form_match = re.search(r"F!([0-9]{3}[A-Z]?)", msg_upper)
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
        raw_form = re.split(r"\*DE\*", raw_form, 1, flags=re.IGNORECASE)[0].strip()
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

    def _parse_js8_spotter_event(self, event: Dict[str, Any]) -> Optional[Dict[str, str | float]]:
        if not isinstance(event, dict):
            return None
        params = event.get("params") if isinstance(event.get("params"), dict) else {}
        text = str(params.get("TEXT") or event.get("value") or "").strip()
        if not text:
            return None
        text_upper = text.upper()
        if "?" in text_upper or "E?" in text_upper or "..." in text:
            return None
        if re.search(r"\bMSG\b", text_upper):
            return None
        form_match = re.search(r"F!([0-9]{3}[A-Z]?)", text_upper)
        if not form_match:
            return None
        form_start = text_upper.find("F!")
        raw_form = text[form_start:].strip()
        raw_form = re.split(r"\*DE\*", raw_form, 1, flags=re.IGNORECASE)[0].strip()
        if raw_form.endswith("\u2662"):
            raw_form = raw_form[:-1].rstrip()
        token_match = re.search(r"(#[A-Z0-9]{3,})", raw_form.upper())
        utc_str = str(params.get("UTC") or event.get("time") or "").strip()
        utc_ts = 0.0
        if utc_str:
            try:
                utc_ts = datetime.datetime.strptime(utc_str[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=datetime.timezone.utc).timestamp()
            except Exception:
                utc_ts = 0.0
        if utc_ts <= 0:
            utc_ts = time.time()
            utc_str = datetime.datetime.fromtimestamp(utc_ts, tz=datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        from_call = str(params.get("FROM") or "").strip().upper()
        to_call = str(params.get("TO") or "").strip().upper()
        de_match = re.search(r"\*DE\*\s*([A-Z0-9/]+)", text_upper)
        if de_match:
            from_call = de_match.group(1).strip().upper()
        if not from_call:
            return None
        if not to_call:
            leading = text[:form_start].strip()
            to_call = (leading.split()[-1] if leading.split() else "").strip().strip(",").upper()
        if not to_call:
            to_call = str(params.get("CMD") or "").strip().upper()
        return {
            "utc_ts": float(utc_ts),
            "utc_str": utc_str,
            "from_call": from_call,
            "to_call": to_call,
            "form_id": form_match.group(1),
            "spotter_token": token_match.group(1) if token_match else "",
            "raw_form": raw_form,
            "relay_via": str(params.get("FROM") or "").strip().upper(),
        }

    @staticmethod
    def _parse_form_parts(text: str) -> tuple[str, str, str]:
        parts = (text or "").split()
        if not parts or not parts[0].startswith("F!"):
            return "", "", ""
        form_code = normalize_form_code(parts[0])
        form_part = form_code[2:] if form_code.startswith("F!") else ""
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

    def _mapped_status_form_ids(self) -> set[str]:
        try:
            raw = self.settings.get(MAPPER_SETTINGS_KEY, [])
        except Exception:
            raw = []
        mapped = {
            code[2:]
            for code in forms_enabled_for(self.settings, flag="status")
            if code.startswith("F!") and code[2:] in SPOTTER_STATUS_FORMS
        }
        if isinstance(raw, list) and raw:
            return mapped
        return mapped or set(SPOTTER_STATUS_FORMS)

    def _form_codes_for_flag(self, flag: str) -> set[str] | None:
        return form_codes_enabled_for(self.settings, flag=flag)

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
        if fid not in self._mapped_status_form_ids():
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
            forms = sorted(self._mapped_status_form_ids())
            if not forms:
                return
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

    def _ensure_spotter_table(self) -> None:
        db_path = self._db_path()
        if not db_path:
            return
        try:
            conn = sqlite3.connect(db_path)
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
                    source_radio_id TEXT,
                    js8_instance_id TEXT,
                    source_key TEXT,
                    ingested_ts REAL
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
            try:
                cur.execute("ALTER TABLE spotter_traffic ADD COLUMN flag_state INTEGER DEFAULT 0")
            except Exception:
                pass
            for col_name, col_ddl in (
                ("relay_via", "TEXT"),
                ("source_radio_id", "TEXT"),
                ("js8_instance_id", "TEXT"),
                ("source_key", "TEXT"),
                ("ingested_ts", "REAL"),
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
                "CREATE INDEX IF NOT EXISTS idx_spotter_traffic_utc_ts ON spotter_traffic(utc_ts DESC, from_call, id DESC)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_spotter_status_key_ts ON spotter_station_status(status_key, updated_utc_ts DESC)"
            )
            self._backfill_spotter_station_status(cur)
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

    def _load_js8_state_map(self, *, source_key: str = "") -> Dict[int, Tuple[str, float]]:
        db_path = self._local_js8_db()
        if not db_path or not db_path.exists():
            return {}
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            cur.execute(
                "CREATE TABLE IF NOT EXISTS js8_inbox_state (id INTEGER PRIMARY KEY, state TEXT, last_seen REAL, read_ts REAL, last_ingested_id INTEGER)"
            )
            try:
                cur.execute("ALTER TABLE js8_inbox_state ADD COLUMN source_key TEXT")
            except Exception:
                pass
            try:
                cur.execute("ALTER TABLE js8_inbox_state ADD COLUMN source_id INTEGER")
            except Exception:
                pass
            try:
                cur.execute("UPDATE js8_inbox_state SET source_key='' WHERE source_key IS NULL")
            except Exception:
                pass
            if source_key:
                cur.execute("SELECT COALESCE(source_id, id), state, read_ts FROM js8_inbox_state WHERE COALESCE(source_key, '')=?", (source_key,))
            else:
                cur.execute("SELECT id, state, read_ts FROM js8_inbox_state WHERE COALESCE(source_key, '')=''")
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
        try:
            db_path.parent.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass
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
        for column, col_type in (
            ("source_key", "TEXT"),
            ("source_id", "INTEGER"),
            ("source_radio_id", "TEXT"),
            ("js8_instance_id", "TEXT"),
            ("source_path", "TEXT"),
        ):
            try:
                cur.execute(f"ALTER TABLE js8_messages ADD COLUMN {column} {col_type}")
            except Exception:
                pass
        try:
            cur.execute("UPDATE js8_messages SET source_key='' WHERE source_key IS NULL")
            cur.execute("UPDATE js8_messages SET source_id=id WHERE source_id IS NULL")
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
        try:
            cur.execute("ALTER TABLE js8_inbox_state ADD COLUMN source_key TEXT")
        except Exception:
            pass
        try:
            cur.execute("ALTER TABLE js8_inbox_state ADD COLUMN source_id INTEGER")
        except Exception:
            pass
        cur.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_js8_messages_source_native ON js8_messages(source_key, source_id)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_js8_messages_utc_ts ON js8_messages(utc_ts DESC, from_call)"
        )
        conn.commit()
        conn.close()

    def _local_max_js8_id(self, *, source_key: str = "") -> int:
        db_path = self._local_js8_db()
        if not db_path or not db_path.exists():
            return 0
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            if source_key:
                cur.execute("SELECT MAX(source_id) FROM js8_messages WHERE COALESCE(source_key, '')=?", (source_key,))
            else:
                cur.execute("SELECT MAX(id) FROM js8_messages WHERE COALESCE(source_key, '')=''")
            row = cur.fetchone()
            conn.close()
            return int(row[0]) if row and row[0] is not None else 0
        except Exception:
            return 0

    @staticmethod
    def _js8_source_key(*, source_key: str = "", source_radio_id: object = "", js8_instance_id: object = "") -> str:
        explicit = str(source_key or "").strip()
        if explicit:
            return explicit
        radio = str(source_radio_id or "").strip()
        js8 = str(js8_instance_id or "").strip()
        if radio or js8:
            digest_src = f"{radio}|{js8}"
            return f"js8:{hashlib.sha1(digest_src.encode('utf-8', errors='ignore')).hexdigest()[:16]}"
        return ""

    @staticmethod
    def _js8_local_row_id(native_id: object, source_key: str = "") -> int:
        try:
            native_int = int(native_id or 0)
        except Exception:
            native_int = 0
        if not source_key:
            return native_int
        digest_src = f"{source_key}|{native_int}"
        return int(hashlib.sha1(digest_src.encode("utf-8", errors="ignore")).hexdigest()[:15], 16)

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
        *,
        source_key: str = "",
        source_id: object = 0,
        source_radio_id: object = "",
        js8_instance_id: object = "",
        source_path: object = "",
    ) -> None:
        db_path = self._local_js8_db()
        if not db_path:
            return
        try:
            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            native_id = int(source_id or msg_id or 0)
            local_id = self._js8_local_row_id(native_id, source_key)
            cur.execute(
                """
                INSERT INTO js8_messages
                    (id, from_call, to_call, msg_type, utc_str, utc_ts, raw_text, decoded_text, state, read_ts,
                     source_key, source_id, source_radio_id, js8_instance_id, source_path)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_key, source_id) DO NOTHING
                """,
                (
                    int(local_id),
                    from_call,
                    to_call,
                    msg_type,
                    utc_str,
                    float(utc_ts or 0.0),
                    raw_text,
                    decoded_text,
                    state,
                    float(read_ts or 0.0),
                    source_key,
                    native_id,
                    str(source_radio_id or ""),
                    str(js8_instance_id or ""),
                    str(source_path or ""),
                ),
            )
            conn.commit()
            conn.close()
        except Exception as e:
            log.debug("MessageIngest: failed to insert local js8 message: %s", e)
