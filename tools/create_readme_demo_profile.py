#!/usr/bin/env python3
"""Create a callsign-masked FreqInOut profile for public screenshots and video.

The source databases are opened read-only and copied with SQLite's online
backup API. Personal names and other message content are intentionally retained.
This is an internal release-preparation tool and is intentionally not part of
the public runtime export.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import itertools
import json
import os
import re
import shutil
import sqlite3
import subprocess
from pathlib import Path
from typing import Any, Iterable


CALL_RE = re.compile(
    r"(?<![A-Z0-9])((?:(?:[A-Z][A-Z0-9]{0,2})|(?:[0-9][A-Z][A-Z0-9]?))"
    r"[0-9][A-Z]{1,4}(?:/[A-Z0-9]{1,5})?)(?![A-Z0-9])",
    re.IGNORECASE,
)
GRID_RE = re.compile(r"(?<![A-Z0-9])([A-R]{2}[0-9]{2}(?:[A-X]{2})?)(?![A-Z0-9])", re.IGNORECASE)
EMAIL_RE = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
NON_CALL_TOKENS = {
    "JS8CALL",
    "JS8NET",
    "JS8SPOTTER",
    "FIO",
    "FLRIG",
    "FLDIGI",
    "FLMSG",
    "FLAMP",
    "VARAC",
    "VARA",
    "COMMSTAT",
}

CALL_COLUMNS = {
    "callsign",
    "current_callsign",
    "old_callsign",
    "new_callsign",
    "from_call",
    "to_call",
    "via_callsign",
    "owner_callsign",
    "origin_callsign",
    "target_callsign",
    "requesting_callsign",
    "msg_auth_sign_callsign",
}
PERSON_TABLES = {
    "operator_checkins",
    "local_operator_checkins",
    "local_ncs_checkins",
    "local_operator_reports",
}
PERSON_COLUMNS = {"name", "first_name", "last_name", "from_name"}
PATH_WORDS = ("path", "dir", "folder", "root", "executable", "command", "launch")
US_LOCATIONS = (
    ("Seattle", "WA", "CN87AA", 47.6062, -122.3321),
    ("Portland", "OR", "CN85AA", 45.5152, -122.6784),
    ("Sacramento", "CA", "CM98AA", 38.5816, -121.4944),
    ("Phoenix", "AZ", "DM33AA", 33.4484, -112.0740),
    ("Denver", "CO", "DM79AA", 39.7392, -104.9903),
    ("Dallas", "TX", "EM12AA", 32.7767, -96.7970),
    ("Kansas City", "MO", "EM29AA", 39.0997, -94.5786),
    ("Minneapolis", "MN", "EN34AA", 44.9778, -93.2650),
    ("Chicago", "IL", "EN61AA", 41.8781, -87.6298),
    ("Nashville", "TN", "EM66AA", 36.1627, -86.7816),
    ("Atlanta", "GA", "EM73AA", 33.7490, -84.3880),
    ("Tampa", "FL", "EL87AA", 27.9506, -82.4572),
    ("Richmond", "VA", "FM17AA", 37.5407, -77.4360),
    ("Philadelphia", "PA", "FM29AA", 39.9526, -75.1652),
    ("Boston", "MA", "FN42AA", 42.3601, -71.0589),
)


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _sqlite_backup(source: Path, destination: Path) -> None:
    source_connection = sqlite3.connect(f"file:{source}?mode=ro", uri=True)
    try:
        destination_connection = sqlite3.connect(destination)
        try:
            source_connection.backup(destination_connection)
            destination_connection.commit()
        finally:
            destination_connection.close()
    finally:
        source_connection.close()


def _iter_text_values(connection: sqlite3.Connection) -> Iterable[tuple[str, str, str]]:
    tables = [
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
    ]
    for table in tables:
        columns = [row[1] for row in connection.execute(f"PRAGMA table_info({_quote(table)})")]
        if not columns:
            continue
        for row in connection.execute(f"SELECT * FROM {_quote(table)}"):
            for column, value in zip(columns, row):
                if isinstance(value, str) and value:
                    yield table, column, value


def _base_call(value: str) -> str:
    return value.upper().split("/", 1)[0]


def _is_call_candidate(value: str) -> bool:
    base = _base_call(value)
    return (
        3 <= len(base) <= 8
        and base not in NON_CALL_TOKENS
        and GRID_RE.fullmatch(base) is None
    )


class Sanitizer:
    def __init__(self, source_root: Path, output_root: Path) -> None:
        self.source_root = str(source_root)
        self.output_root = str(output_root)
        self.call_map: dict[str, str] = {}
        self.grid_map: dict[str, str] = {}
        self.name_map: dict[str, str] = {}
        self._email_counter = 0

    @staticmethod
    def _index_for(seed: str, length: int) -> int:
        digest = hashlib.sha256(seed.encode("utf-8", errors="ignore")).digest()
        return int.from_bytes(digest[:8], "big") % length

    def register_call(self, value: str) -> None:
        candidate = str(value or "").strip().upper()
        if not candidate:
            return
        base = _base_call(candidate)
        if not _is_call_candidate(candidate):
            return
        if not CALL_RE.fullmatch(candidate) and not CALL_RE.fullmatch(base):
            return
        if base not in self.call_map:
            self.call_map[base] = "FIO%03d" % (len(self.call_map) + 1)

    def register_grid(self, value: str) -> None:
        candidate = str(value or "").strip().upper()
        if not GRID_RE.fullmatch(candidate):
            return
        if candidate not in self.grid_map:
            location = US_LOCATIONS[self._index_for(candidate, len(US_LOCATIONS))]
            suffix_index = self._index_for("suffix:" + candidate, 24 * 24)
            suffix = chr(ord("A") + suffix_index // 24) + chr(ord("A") + suffix_index % 24)
            self.grid_map[candidate] = location[2][:4] + suffix

    def register_name(self, value: str) -> None:
        candidate = " ".join(str(value or "").split())
        if len(candidate) < 2 or candidate.isdigit():
            return
        if candidate not in self.name_map:
            self.name_map[candidate] = "Demo Operator %03d" % (len(self.name_map) + 1)

    def location(self, seed: str) -> tuple[str, str, str, float, float]:
        return US_LOCATIONS[self._index_for(seed, len(US_LOCATIONS))]

    def call(self, value: str) -> str:
        candidate = str(value or "")
        upper = candidate.upper()
        base, separator, suffix = upper.partition("/")
        self.register_call(upper)
        replacement = self.call_map.get(base)
        if replacement is None:
            return candidate
        return replacement + (separator + suffix if separator else "")

    def grid(self, value: str) -> str:
        candidate = str(value or "").upper()
        self.register_grid(candidate)
        return self.grid_map.get(candidate, candidate)

    def _replace_call_match(self, match: re.Match[str]) -> str:
        return self.call(match.group(1))

    def _replace_grid_match(self, match: re.Match[str]) -> str:
        return self.grid(match.group(1))

    def _replace_email(self, _match: re.Match[str]) -> str:
        self._email_counter += 1
        return f"operator{self._email_counter:03d}@example.invalid"

    def text(self, value: str) -> str:
        result = str(value)
        result = result.replace(self.source_root, self.output_root)
        result = result.replace("/Users/bill", "/Users/Shared/FIO-Demo-User")
        result = result.replace("\\Users\\bill", "\\Users\\FIO-Demo-User")
        result = result.replace("C:\\Users\\bill", "C:\\Users\\FIO-Demo-User")
        for original in sorted(self.name_map, key=len, reverse=True):
            if len(original) >= 3:
                result = re.sub(re.escape(original), self.name_map[original], result, flags=re.IGNORECASE)
        result = EMAIL_RE.sub(self._replace_email, result)
        result = CALL_RE.sub(self._replace_call_match, result)
        result = GRID_RE.sub(self._replace_grid_match, result)
        return result

    def structured(self, value: Any, context: str) -> Any:
        if isinstance(value, dict):
            result = {
                self.text(str(key)): self.structured(item, f"{context}.{key}")
                for key, item in value.items()
            }
            keys = {str(key).lower(): key for key in result}
            if "lat" in keys and "lon" in keys:
                city, state, grid, lat, lon = self.location(context)
                result[keys["lat"]] = lat
                result[keys["lon"]] = lon
                for possible, replacement in (("city", city), ("state", state), ("grid", grid)):
                    if possible in keys:
                        result[keys[possible]] = replacement
            return result
        if isinstance(value, list):
            return [self.structured(item, f"{context}[{index}]") for index, item in enumerate(value)]
        if isinstance(value, str):
            return self.text(value)
        return value

    def value(self, table: str, column: str, rowid: int, value: str) -> str:
        lower = column.lower()
        if lower in CALL_COLUMNS:
            exact = self.call(value)
            return exact if exact != value else self.text(value)
        if lower == "grid" or lower.endswith("_grid") or lower.endswith("_grid6"):
            return self.grid(value)
        if table in PERSON_TABLES and lower in PERSON_COLUMNS:
            original = " ".join(value.split())
            self.register_name(original)
            mapped = self.name_map.get(original, "Demo Operator")
            if lower == "first_name":
                return "Demo"
            if lower == "last_name":
                return mapped.removeprefix("Demo ")
            return mapped
        stripped = value.lstrip()
        if stripped.startswith("{") or stripped.startswith("["):
            try:
                parsed = json.loads(value)
            except (TypeError, ValueError):
                pass
            else:
                sanitized = self.structured(parsed, f"{table}:{rowid}:{column}")
                return json.dumps(sanitized, ensure_ascii=False, separators=(",", ":"))
        result = self.text(value)
        if column.lower() == "canonical_id" and table in {
            "ops_focus_entities",
            "ops_focus_message_entities",
        }:
            result = f"{result}:demo:{rowid}"
        return result


def _shift_ascii(value: str, *, letter_shift: int, digit_shift: int) -> str:
    shifted: list[str] = []
    for character in value.upper():
        if "A" <= character <= "Z":
            shifted.append(chr(ord("A") + ((ord(character) - ord("A") + letter_shift) % 26)))
        elif "0" <= character <= "9":
            shifted.append(str((int(character) + digit_shift) % 10))
        else:
            shifted.append(character)
    return "".join(shifted)


class CallsignShiftSanitizer(Sanitizer):
    """Mask only callsigns while preserving the rest of the production data."""

    def __init__(
        self,
        source_root: Path,
        output_root: Path,
        *,
        letter_shift: int = 3,
        digit_shift: int = 1,
        preserve_callsigns: Iterable[str] = ("N1MAG",),
    ) -> None:
        super().__init__(source_root, output_root)
        self.letter_shift = int(letter_shift) % 26
        self.digit_shift = int(digit_shift) % 10
        self.preserve_callsigns = {_base_call(value) for value in preserve_callsigns if value}

    def register_call(self, value: str) -> None:
        candidate = str(value or "").strip().upper()
        base = _base_call(candidate)
        if base in self.preserve_callsigns:
            return
        if not _is_call_candidate(candidate):
            return
        if not CALL_RE.fullmatch(candidate) and not CALL_RE.fullmatch(base):
            return
        shifted = _shift_ascii(
            base,
            letter_shift=self.letter_shift,
            digit_shift=self.digit_shift,
        )
        if shifted in self.preserve_callsigns:
            raise ValueError(
                f"Callsign transform collision: {base} would become preserved callsign {shifted}"
            )
        self.call_map.setdefault(base, shifted)

    def register_grid(self, value: str) -> None:
        return None

    def register_name(self, value: str) -> None:
        return None

    def text(self, value: str) -> str:
        return CALL_RE.sub(self._replace_call_match, str(value))

    def structured(self, value: Any, context: str) -> Any:
        if isinstance(value, dict):
            return {
                self.text(str(key)): self.structured(item, f"{context}.{key}")
                for key, item in value.items()
            }
        if isinstance(value, list):
            return [self.structured(item, f"{context}[{index}]") for index, item in enumerate(value)]
        if isinstance(value, str):
            return self.text(value)
        return value

    def value(self, table: str, column: str, rowid: int, value: str) -> str:
        lower = column.lower()
        if lower in CALL_COLUMNS:
            exact = self.call(value)
            return exact if exact != value else self.text(value)
        stripped = value.lstrip()
        if stripped.startswith("{") or stripped.startswith("["):
            try:
                parsed = json.loads(value)
            except (TypeError, ValueError):
                pass
            else:
                sanitized = self.structured(parsed, f"{table}:{rowid}:{column}")
                return json.dumps(sanitized, ensure_ascii=False, separators=(",", ":"))
        return self.text(value)


def _discover_sensitive_values(databases: Iterable[Path], sanitizer: Sanitizer) -> None:
    for database in databases:
        connection = sqlite3.connect(f"file:{database}?mode=ro", uri=True)
        try:
            for table, column, value in _iter_text_values(connection):
                lower = column.lower()
                if lower in CALL_COLUMNS:
                    sanitizer.register_call(value)
                if lower == "grid" or lower.endswith("_grid") or lower.endswith("_grid6"):
                    sanitizer.register_grid(value)
                if table in PERSON_TABLES and lower in PERSON_COLUMNS:
                    sanitizer.register_name(value)
                for match in CALL_RE.finditer(value):
                    sanitizer.register_call(match.group(1))
                for match in GRID_RE.finditer(value):
                    sanitizer.register_grid(match.group(1))
        finally:
            connection.close()


def _sanitize_database(database: Path, sanitizer: Sanitizer) -> int:
    connection = sqlite3.connect(database)
    changed = 0
    try:
        triggers = [
            (str(row[0]), str(row[1]))
            for row in connection.execute(
                "SELECT name, sql FROM sqlite_master "
                "WHERE type='trigger' AND sql IS NOT NULL ORDER BY name"
            )
        ]
        tables = [
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
            )
        ]
        connection.execute("BEGIN IMMEDIATE")
        for trigger_name, _trigger_sql in triggers:
            connection.execute(f"DROP TRIGGER IF EXISTS {_quote(trigger_name)}")
        for table in tables:
            columns = [row[1] for row in connection.execute(f"PRAGMA table_info({_quote(table)})")]
            if not columns:
                continue
            select_sql = f"SELECT rowid, * FROM {_quote(table)}"
            try:
                rows = list(connection.execute(select_sql))
            except sqlite3.OperationalError:
                continue
            row_updates: list[tuple[int, dict[str, str]]] = []
            for result in rows:
                rowid = int(result[0])
                values = result[1:]
                updates: dict[str, str] = {}
                for column, value in zip(columns, values):
                    if not isinstance(value, str) or not value:
                        continue
                    replacement = sanitizer.value(table, column, rowid, value)
                    if replacement != value:
                        updates[column] = replacement
                if updates:
                    row_updates.append((rowid, updates))
            unique_columns: set[str] = set()
            for index_row in connection.execute(f"PRAGMA index_list({_quote(table)})"):
                if not int(index_row[2] or 0):
                    continue
                index_name = str(index_row[1])
                unique_columns.update(
                    str(index_info[2])
                    for index_info in connection.execute(f"PRAGMA index_info({_quote(index_name)})")
                    if index_info[2] is not None
                )
            for rowid, updates in row_updates:
                temporary = {
                    column: f"__FIO_DEMO_PENDING__{table}__{column}__{rowid}"
                    for column in updates
                    if column in unique_columns
                }
                if temporary:
                    assignments = ", ".join(f"{_quote(column)} = ?" for column in temporary)
                    connection.execute(
                        f"UPDATE {_quote(table)} SET {assignments} WHERE rowid = ?",
                        [*temporary.values(), rowid],
                    )
            for rowid, updates in row_updates:
                assignments = ", ".join(f"{_quote(column)} = ?" for column in updates)
                connection.execute(
                    f"UPDATE {_quote(table)} SET {assignments} WHERE rowid = ?",
                    [*updates.values(), rowid],
                )
                changed += 1
        for _trigger_name, trigger_sql in triggers:
            connection.execute(trigger_sql)
        connection.commit()
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    finally:
        connection.close()
    return changed


def _verify_callsign_transform(
    source: Path,
    destination: Path,
    sanitizer: CallsignShiftSanitizer,
) -> None:
    source_connection = sqlite3.connect(f"file:{source}?mode=ro", uri=True)
    destination_connection = sqlite3.connect(f"file:{destination}?mode=ro", uri=True)
    try:
        tables = [
            row[0]
            for row in source_connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%' ORDER BY name"
            )
        ]
        for table in tables:
            columns = [
                row[1]
                for row in source_connection.execute(f"PRAGMA table_info({_quote(table)})")
            ]
            if not columns:
                continue
            query = f"SELECT rowid, * FROM {_quote(table)} ORDER BY rowid"
            try:
                source_rows = source_connection.execute(query)
                destination_rows = destination_connection.execute(query)
            except sqlite3.OperationalError:
                continue
            for source_row, destination_row in itertools.zip_longest(
                source_rows,
                destination_rows,
            ):
                if source_row is None or destination_row is None:
                    raise RuntimeError(f"Row-count mismatch while verifying table {table}")
                if source_row[0] != destination_row[0]:
                    raise RuntimeError(
                        f"Row identity changed while verifying {table}: "
                        f"{source_row[0]} != {destination_row[0]}"
                    )
                rowid = int(source_row[0])
                for column, source_value, destination_value in zip(
                    columns,
                    source_row[1:],
                    destination_row[1:],
                ):
                    expected = (
                        sanitizer.value(table, column, rowid, source_value)
                        if isinstance(source_value, str) and source_value
                        else source_value
                    )
                    if destination_value != expected:
                        raise RuntimeError(
                            f"Callsign transform verification failed at "
                            f"{destination.name}:{table}.{column}:rowid={rowid}"
                        )
    finally:
        destination_connection.close()
        source_connection.close()


def _kv_set(connection: sqlite3.Connection, key: str, value: Any) -> None:
    text = value if isinstance(value, str) else json.dumps(value, separators=(",", ":"))
    connection.execute(
        "INSERT INTO kv(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, text),
    )


def _configure_primary_js8_runtime(
    connection: sqlite3.Connection,
    *,
    host: str,
    port: int,
    offset_hz: int,
    save_dir: Path,
    data_root: Path,
) -> None:
    """Align legacy single-radio JS8 settings with the primary demo radio."""

    _kv_set(connection, "js8_host", host)
    _kv_set(connection, "js8_port", port)
    _kv_set(connection, "js8_offset_hz", offset_hz)
    _kv_set(connection, "js8_profile_path", str(save_dir))
    _kv_set(connection, "js8_directed_path", str(data_root / "DIRECTED.TXT"))
    _kv_set(connection, "js8_forms_path", str(data_root / "forms"))


def _demo_runtime_binding(
    index: int,
    output_root: Path,
    radio_tools: Path,
    gui_lab_root: Path | None,
) -> dict[str, Any]:
    profile = chr(ord("a") + index)
    binding: dict[str, Any] = {
        "profile": profile,
        "rigctld_port": 4532 + index,
        "flrig_port": 12345 + index,
        "fldigi_port": 7362 + index,
        "js8_port": 2442 + index,
        "flrig_path": radio_tools / "bin" / "flrig",
        "fldigi_path": radio_tools / "bin" / "fldigi",
        "js8_path": radio_tools / "bin" / "js8call",
        "fldigi_log_path": output_root / "demo-files" / profile / "fldigi" / "logs",
        "fldigi_checkin_dir": output_root / "demo-files" / profile / "fldigi" / "checkins",
        "js8_save_dir": output_root / "demo-files" / profile / "js8",
        "js8_data_root": output_root / "demo-files" / profile / "js8",
        "runtime_label": f"RadioTools suite {profile.upper()}",
    }
    if gui_lab_root is None:
        return binding

    label = f"fio-{profile}"
    js8_path = (
        Path("/Applications/RadioApps/JS8Call.app/Contents/MacOS/JS8Call")
        if index == 0
        else Path("/Applications/RadioApps/JS8Call 2.app/Contents/MacOS/JS8Call")
    )
    binding.update(
        {
            # start_multirig_gui_lab.sh deliberately assigns its TCP API to the
            # 2242 series rather than JS8Call's ordinary 2442 series.
            "js8_port": 2242 + index,
            "flrig_path": Path(
                "/Applications/RadioApps/flrig-2.0.10.app/Contents/MacOS/flrig"
            ),
            "fldigi_path": Path(
                "/Applications/RadioApps/fldigi-4.2.11.app/Contents/MacOS/fldigi"
            ),
            "js8_path": js8_path,
            "fldigi_log_path": gui_lab_root / "tool-profiles" / "fldigi" / label / "logs",
            "fldigi_checkin_dir": (
                gui_lab_root / "tool-homes" / "fldigi" / label / ".nbems" / "WRAP" / "auto"
            ),
            "js8_save_dir": gui_lab_root / "tool-homes" / "js8call" / label / "save",
            "js8_data_root": Path.home() / "Library" / "Application Support" / f"JS8Call - {label}",
            "runtime_label": f"GUI lab suite {label}",
        }
    )
    return binding


def _configure_demo_runtime(
    settings_db: Path,
    output_root: Path,
    radio_tools: Path,
    *,
    replace_operator_identity: bool = True,
    gui_lab_root: Path | None = None,
) -> None:
    connection = sqlite3.connect(settings_db)
    try:
        connection.execute("BEGIN IMMEDIATE")
        if replace_operator_identity:
            _kv_set(connection, "operator_callsign", "FIO000")
            _kv_set(connection, "operator_name", "Demo Operator")
            _kv_set(connection, "operator_grid6", "EM12AA")
            _kv_set(connection, "operator_state", "TX")
        for key in (
            "autostart_flrig",
            "autostart_fldigi",
            "autostart_flmsg",
            "autostart_flamp",
            "autostart_js8call",
            "js8_expect_unattended_auto_reply_enabled",
            "meshcore_send_enabled",
            "meshtastic_send_enabled",
            "meshcore_mqtt_enabled",
            "meshtastic_mqtt_enabled",
            "meshcore_bridge_to_reticulum_enabled",
            "meshtastic_bridge_to_reticulum_enabled",
            "varac_bbs_enabled",
            "varac_bbs_announce_enabled",
            "varac_bbs_vault_enabled",
        ):
            _kv_set(connection, key, "0")
        for key in (
            "meshcore_ble_device_id",
            "meshcore_ble_device_name",
            "meshcore_serial_port",
            "meshcore_tcp_host",
            "meshcore_http_base_url",
            "meshcore_mqtt_broker",
            "meshtastic_ble_device_id",
            "meshtastic_ble_device_name",
            "meshtastic_serial_port",
            "meshtastic_tcp_host",
            "meshtastic_http_base_url",
            "meshtastic_mqtt_broker",
            "js8spotter_import_db_path",
        ):
            _kv_set(connection, key, "")
        _kv_set(connection, "mesh_connection_library", [])
        _kv_set(connection, "message_paths", [])

        profiles = list(
            connection.execute(
                "SELECT id, fast_light_config_id, js8_instance_id, varac_node_id "
                "FROM device_profiles ORDER BY display_order, id"
            )
        )
        for index, (radio_id, fast_light_id, js8_id, varac_id) in enumerate(profiles[:4]):
            binding = _demo_runtime_binding(index, output_root, radio_tools, gui_lab_root)
            profile = str(binding["profile"])
            rigctld_port = int(binding["rigctld_port"])
            flrig_port = int(binding["flrig_port"])
            fldigi_port = int(binding["fldigi_port"])
            js8_port = int(binding["js8_port"])
            fldigi_log_path = Path(binding["fldigi_log_path"])
            fldigi_checkin_dir = Path(binding["fldigi_checkin_dir"])
            js8_save_dir = Path(binding["js8_save_dir"])
            js8_data_root = Path(binding["js8_data_root"])
            js8_directed_path = js8_data_root / "DIRECTED.TXT"
            js8_inbox_path = js8_data_root / "inbox.db3"
            js8_forms_path = js8_data_root / "forms"
            js8_all_path = js8_data_root / "ALL.TXT"
            js8_has_message_evidence = any(
                path.exists() for path in (js8_directed_path, js8_inbox_path, js8_all_path)
            )
            js8_storage_mode = "rig_scoped" if js8_has_message_evidence else "unverified"
            js8_storage_evidence = (
                "runtime_verified:message_files"
                if js8_has_message_evidence
                else "platform_candidate"
            )
            js8_storage_verified_utc = (
                dt.datetime.now(dt.timezone.utc).isoformat() if js8_has_message_evidence else None
            )
            if index == 0:
                _configure_primary_js8_runtime(
                    connection,
                    host="127.0.0.1",
                    port=js8_port,
                    offset_hz=2125,
                    save_dir=js8_save_dir,
                    data_root=js8_data_root,
                )
            demo_files = output_root / "demo-files" / profile
            for child in ("fldigi/logs", "fldigi/checkins", "js8", "flmsg", "flamp", "varac/incoming"):
                (demo_files / child).mkdir(parents=True, exist_ok=True)
            for path in (
                fldigi_log_path,
                fldigi_checkin_dir,
                js8_save_dir,
                js8_save_dir / "messages",
            ):
                path.mkdir(parents=True, exist_ok=True)
            if gui_lab_root is None:
                js8_forms_path.mkdir(parents=True, exist_ok=True)
                js8_directed_path.touch(exist_ok=True)
            connection.execute(
                "UPDATE device_profiles SET rig_host=?, rig_port=?, flrig_host=?, flrig_port=?, "
                "fldigi_host=?, fldigi_port=?, js8_host=?, js8_port=?, launch_enabled=1, "
                "fldigi_log_path=?, fldigi_checkin_dir=?, js8_profile_path=?, "
                "js8_directed_path=?, js8_forms_path=?, flmsg_path=?, "
                "flmsg_message_path=?, flamp_path=?, flamp_message_path=?, varac_install_path=?, "
                "varac_db_path='', varac_ini_path='', varac_outbox_dir='', varac_bbs_dir='', "
                "varac_bbs_archive_dir='', varac_bbs_enabled=0, varac_bbs_vault_enabled=0 "
                "WHERE id=?",
                (
                    "127.0.0.1",
                    rigctld_port,
                    "127.0.0.1",
                    flrig_port,
                    "127.0.0.1",
                    fldigi_port,
                    "127.0.0.1",
                    js8_port,
                    str(fldigi_log_path),
                    str(fldigi_checkin_dir),
                    str(js8_save_dir),
                    str(js8_directed_path),
                    str(js8_forms_path),
                    str(radio_tools / "bin" / "flmsg"),
                    str(demo_files / "flmsg"),
                    str(radio_tools / "bin" / "flamp"),
                    str(demo_files / "flamp"),
                    str(radio_tools / "bin" / "varac"),
                    radio_id,
                ),
            )
            if fast_light_id is not None:
                connection.execute(
                    "UPDATE fast_light_configs SET flrig_path=?, flrig_host=?, flrig_port=?, "
                    "fldigi_path=?, fldigi_host=?, fldigi_port=?, fldigi_log_path=?, fldigi_checkin_dir=? WHERE id=?",
                    (
                        str(binding["flrig_path"]),
                        "127.0.0.1",
                        flrig_port,
                        str(binding["fldigi_path"]),
                        "127.0.0.1",
                        fldigi_port,
                        str(fldigi_log_path),
                        str(fldigi_checkin_dir),
                        fast_light_id,
                    ),
                )
            if js8_id is not None:
                connection.execute(
                    "UPDATE js8_instances SET host=?, port=?, profile_path=?, directed_path=?, inbox_path=?, "
                    "forms_path=?, install_path=?, spotter_launch_path=?, commstat_launch_path=?, "
                    "rig_name=?, rig_name_source='fio_managed', application_data_root=?, all_path=?, "
                    "save_dir=?, storage_mode=?, storage_verified_utc=?, storage_evidence=? "
                    "WHERE id=?",
                    (
                        "127.0.0.1",
                        js8_port,
                        str(js8_save_dir),
                        str(js8_directed_path),
                        str(js8_inbox_path),
                        str(js8_forms_path),
                        str(binding["js8_path"]),
                        str(radio_tools / "bin" / "js8spotter"),
                        str(radio_tools / "bin" / "commstat"),
                        f"fio-{profile}",
                        str(js8_data_root),
                        str(js8_all_path),
                        str(js8_save_dir),
                        js8_storage_mode,
                        js8_storage_verified_utc,
                        js8_storage_evidence,
                        js8_id,
                    ),
                )
            if varac_id is not None:
                connection.execute(
                    "UPDATE varac_nodes SET install_path=?, db_path='', ini_path='', launch_cmd=?, incoming_path=?, "
                    "vara_runtime_path='', vara_ini_path='' WHERE id=?",
                    (
                        str(radio_tools / "bin" / "varac"),
                        f"{radio_tools / 'bin' / 'varac'} {profile}",
                        str(demo_files / "varac" / "incoming"),
                        varac_id,
                    ),
                )
            connection.execute(
                "UPDATE radio_launch_bundle_items SET launch_at_startup=0, command_override='', "
                "readiness_json='{}' WHERE radio_profile_id=?",
                (radio_id,),
            )
        connection.commit()
    finally:
        connection.close()


def _clear_live_sources(nets_db: Path, output_root: Path) -> None:
    connection = sqlite3.connect(nets_db)
    try:
        connection.execute("BEGIN IMMEDIATE")
        for table in ("message_viewer_paths", "message_scan_cache_dirs"):
            try:
                connection.execute(f"DELETE FROM {_quote(table)}")
            except sqlite3.OperationalError:
                pass
        try:
            connection.execute("UPDATE message_sources SET endpoint_or_path=''")
        except sqlite3.OperationalError:
            pass
        try:
            connection.execute("DELETE FROM sitrep_ingest_checkpoint")
        except sqlite3.OperationalError:
            pass
        connection.commit()
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    finally:
        connection.close()


def _verify_database(database: Path) -> None:
    connection = sqlite3.connect(f"file:{database}?mode=ro", uri=True)
    try:
        result = connection.execute("PRAGMA integrity_check").fetchone()
        if result is None or str(result[0]).lower() != "ok":
            raise RuntimeError(f"SQLite integrity check failed for {database}: {result}")
    finally:
        connection.close()


def _residual_count(
    databases: Iterable[Path],
    *,
    original_calls: Iterable[str],
    original_names: Iterable[str],
    forbidden_paths: Iterable[str],
) -> tuple[dict[str, int], dict[str, int]]:
    calls = {_base_call(value) for value in original_calls if value}
    names = [value for value in original_names if len(value) >= 3]
    name_pattern = None
    if names:
        name_pattern = re.compile(
            r"(?<![A-Z0-9])(?:" + "|".join(re.escape(value) for value in sorted(names, key=len, reverse=True)) + r")(?![A-Z0-9])",
            re.IGNORECASE,
        )
    paths = [value.lower() for value in forbidden_paths if value]
    residuals = {"path": 0, "callsign": 0, "name": 0}
    locations: dict[str, int] = {}
    for database in databases:
        connection = sqlite3.connect(f"file:{database}?mode=ro", uri=True)
        try:
            for _table, _column, value in _iter_text_values(connection):
                lowered = value.lower()
                path_count = sum(lowered.count(path) for path in paths)
                call_count = sum(
                    1 for match in CALL_RE.finditer(value) if _base_call(match.group(1)) in calls
                )
                name_count = 0
                if name_pattern is not None:
                    name_count = len(name_pattern.findall(value))
                residuals["path"] += path_count
                residuals["callsign"] += call_count
                residuals["name"] += name_count
                if path_count or call_count or name_count:
                    key = f"{database.name}:{_table}.{_column}"
                    locations[key] = locations.get(key, 0) + path_count + call_count + name_count
        finally:
            connection.close()
    return residuals, locations


def create_full_scrub_profile(source_root: Path, output_root: Path, radio_tools: Path) -> None:
    source_config = source_root / "config"
    source_databases = (source_config / "freqinout.db", source_config / "freqinout_nets.db")
    for database in source_databases:
        if not database.is_file():
            raise FileNotFoundError(database)
    if output_root.exists():
        raise FileExistsError(f"Output already exists; refusing to replace it: {output_root}")

    sanitizer = Sanitizer(source_root, output_root)
    _discover_sensitive_values(source_databases, sanitizer)
    output_config = output_root / "config"
    output_config.mkdir(parents=True)
    output_databases = (output_config / "freqinout.db", output_config / "freqinout_nets.db")
    try:
        for source, destination in zip(source_databases, output_databases):
            _sqlite_backup(source, destination)
        changed = sum(_sanitize_database(database, sanitizer) for database in output_databases)
        _configure_demo_runtime(output_databases[0], output_root, radio_tools)
        _clear_live_sources(output_databases[1], output_root)
        for database in output_databases:
            _verify_database(database)
        residuals, residual_locations = _residual_count(
            output_databases,
            original_calls=sanitizer.call_map,
            original_names=sanitizer.name_map,
            forbidden_paths=(str(source_root), "/Users/bill"),
        )
        if any(residuals.values()):
            leading_locations = sorted(residual_locations.items(), key=lambda item: (-item[1], item[0]))[:20]
            raise RuntimeError(
                f"Privacy verification found residual sensitive tokens by category: {residuals}; "
                f"locations={leading_locations}"
            )
        (output_root / "README-DEMO-PROFILE.txt").write_text(
            "FreqInOut README screenshot profile\n"
            "Synthetic identities only. Startup launches and live source paths are disabled.\n"
            f"Launch with: FREQINOUT_CONFIG_DIR={output_root} python -m freqinout.main\n",
            encoding="utf-8",
        )
    except Exception:
        shutil.rmtree(output_root, ignore_errors=True)
        raise

    print(f"Created sanitized profile: {output_root}")
    print(f"Synthetic callsigns: {len(sanitizer.call_map)}")
    print(f"Synthetic personal names: {len(sanitizer.name_map)}")
    print(f"Synthetic grids: {len(sanitizer.grid_map)}")
    print(f"Changed database rows: {changed}")
    print("SQLite integrity: ok")
    print("Residual identity/path scan: clean")


def _rotate_existing_output(output_root: Path, *, replace_output: bool) -> Path | None:
    if not output_root.exists():
        return None
    if not replace_output:
        raise FileExistsError(
            f"Output already exists; use --replace-output to rotate it safely: {output_root}"
        )
    open_owners = _profile_database_owners(output_root)
    if open_owners:
        raise RuntimeError(
            "Output appears to be open in FreqInOut; close that demo instance before "
            f"using --replace-output ({'; '.join(open_owners)})"
        )
    timestamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    previous = output_root.with_name(f"{output_root.name}.previous-{timestamp}-{os.getpid()}")
    output_root.rename(previous)
    return previous


def _profile_database_owners(output_root: Path) -> list[str]:
    targets = {
        path.resolve()
        for path in (output_root / "config").glob("*.db")
        if path.is_file()
    }
    if not targets:
        return []
    owners: set[str] = set()
    try:
        import psutil

        for process in psutil.process_iter(["pid", "name", "cmdline", "open_files"]):
            try:
                open_files = process.info.get("open_files") or ()
                if not any(Path(item.path).resolve() in targets for item in open_files):
                    continue
                command = " ".join(process.info.get("cmdline") or ()).strip()
                label = command or str(process.info.get("name") or "process")
                owners.add(f"pid {process.info.get('pid')}: {label}")
            except (OSError, psutil.Error):
                continue
    except ImportError:
        lsof = shutil.which("lsof")
        if lsof:
            result = subprocess.run(
                [lsof, *[str(path) for path in sorted(targets)]],
                check=False,
                capture_output=True,
                text=True,
            )
            for line in result.stdout.splitlines()[1:]:
                fields = line.split()
                if len(fields) >= 2:
                    owners.add(f"pid {fields[1]}: {fields[0]}")
    return sorted(owners)


def create_callsign_demo_profile(
    settings_source: Path,
    nets_source: Path,
    output_root: Path,
    radio_tools: Path,
    *,
    letter_shift: int = 3,
    digit_shift: int = 1,
    preserve_callsigns: Iterable[str] = ("N1MAG",),
    replace_output: bool = False,
    gui_lab_root: Path | None = None,
) -> None:
    source_databases = (settings_source, nets_source)
    for database in source_databases:
        if not database.is_file():
            raise FileNotFoundError(database)
    if settings_source.resolve() == nets_source.resolve():
        raise ValueError("Settings and operational database sources must be different files")

    previous = _rotate_existing_output(output_root, replace_output=replace_output)
    sanitizer = CallsignShiftSanitizer(
        settings_source.parent,
        output_root,
        letter_shift=letter_shift,
        digit_shift=digit_shift,
        preserve_callsigns=preserve_callsigns,
    )
    _discover_sensitive_values(source_databases, sanitizer)
    output_config = output_root / "config"
    output_databases = (output_config / "freqinout.db", output_config / "freqinout_nets.db")
    try:
        output_config.mkdir(parents=True)
        for source, destination in zip(source_databases, output_databases):
            _sqlite_backup(source, destination)
        changed = sum(_sanitize_database(database, sanitizer) for database in output_databases)
        for source, destination in zip(source_databases, output_databases):
            _verify_callsign_transform(source, destination, sanitizer)
        _configure_demo_runtime(
            output_databases[0],
            output_root,
            radio_tools,
            replace_operator_identity=False,
            gui_lab_root=gui_lab_root,
        )
        _clear_live_sources(output_databases[1], output_root)
        for database in output_databases:
            _verify_database(database)
        preserved = ", ".join(sorted({_base_call(value) for value in preserve_callsigns if value}))
        (output_root / "README-DEMO-PROFILE.txt").write_text(
            "FreqInOut public release-media profile\n"
            f"Source settings DB: {settings_source.name}\n"
            f"Source operational DB: {nets_source.name}\n"
            f"Callsign transform: letters +{letter_shift % 26}; digits +{digit_shift % 10}; suffixes preserved\n"
            f"Public callsigns preserved: {preserved or 'none'}\n"
            "Names, grids, schedules, timestamps, and message content otherwise remain unchanged.\n"
            "Startup launches and live source paths are disabled; "
            f"runtime endpoints: {'GUI lab suites' if gui_lab_root else 'RadioTools emulator suites'}.\n"
            f"Launch with: FREQINOUT_CONFIG_DIR={output_root} python -m freqinout.main\n",
            encoding="utf-8",
        )
    except BaseException:
        shutil.rmtree(output_root, ignore_errors=True)
        if previous is not None and previous.exists():
            previous.rename(output_root)
        raise

    print(f"Created callsign-masked profile: {output_root}")
    print(f"Masked callsign bases: {len(sanitizer.call_map)}")
    print(f"Preserved callsigns: {', '.join(sorted(sanitizer.preserve_callsigns)) or 'none'}")
    print(f"Changed database rows: {changed}")
    print("SQLite integrity: ok")
    print("Source-to-copy callsign transform verification: exact")
    if previous is not None:
        print(f"Previous output retained at: {previous}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path)
    parser.add_argument("--settings-db", type=Path)
    parser.add_argument("--nets-db", type=Path)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument(
        "--radio-tools",
        type=Path,
        default=Path("/Users/Shared/RadioTools-Demo"),
    )
    parser.add_argument(
        "--gui-lab-root",
        type=Path,
        help=(
            "Bind radios in order to the fio-a/fio-b/... suites prepared by "
            "tools/start_multirig_gui_lab.sh under this lab root."
        ),
    )
    parser.add_argument("--letter-shift", type=int, default=3)
    parser.add_argument("--digit-shift", type=int, default=1)
    parser.add_argument(
        "--preserve-callsign",
        action="append",
        default=["N1MAG"],
        help="Base callsign to retain unchanged; repeat for additional public identities.",
    )
    parser.add_argument(
        "--replace-output",
        action="store_true",
        help="Rotate an existing output to a timestamped previous profile before rebuilding.",
    )
    args = parser.parse_args()
    if args.source_root is not None:
        if args.settings_db is not None or args.nets_db is not None:
            parser.error("Use either --source-root or the explicit --settings-db/--nets-db pair")
        source_config = args.source_root.expanduser().resolve() / "config"
        settings_source = source_config / "freqinout.db"
        nets_source = source_config / "freqinout_nets.db"
    else:
        if args.settings_db is None or args.nets_db is None:
            parser.error("Provide --source-root or both --settings-db and --nets-db")
        settings_source = args.settings_db.expanduser().resolve()
        nets_source = args.nets_db.expanduser().resolve()
    create_callsign_demo_profile(
        settings_source,
        nets_source,
        args.output_root.expanduser().resolve(),
        args.radio_tools.expanduser(),
        letter_shift=args.letter_shift,
        digit_shift=args.digit_shift,
        preserve_callsigns=args.preserve_callsign,
        replace_output=bool(args.replace_output),
        gui_lab_root=(args.gui_lab_root.expanduser().resolve() if args.gui_lab_root else None),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
