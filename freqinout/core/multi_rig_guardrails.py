from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Mapping, Optional


@dataclass(frozen=True)
class MultiRigGuardrailWarning:
    warning_type: str
    resource_type: str
    resource_value: str
    affected_radio_ids: tuple[int, ...]
    affected_radio_names: tuple[str, ...]
    severity: str = "warning"
    message: str = ""

    def __post_init__(self) -> None:
        if not self.message:
            names = ", ".join(self.affected_radio_names)
            object.__setattr__(
                self,
                "message",
                f"Duplicate {self.resource_type} {self.resource_value} on active radios: {names}.",
            )


def _coerce_text(value: Any, default: str = "") -> str:
    try:
        return str(value if value is not None else default).strip()
    except Exception:
        return str(default or "").strip()


def _coerce_optional_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    try:
        if value in (None, ""):
            return default
        return int(value)
    except Exception:
        return default


def _coerce_bool_int(value: Any, default: bool = False) -> int:
    if value in (None, ""):
        return 1 if default else 0
    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, (int, float)):
        return 1 if int(value) != 0 else 0
    return 1 if str(value).strip().lower() in {"1", "true", "yes", "on"} else 0


def _fetchall_dicts(conn: sqlite3.Connection, query: str, params: Iterable[Any] = ()) -> list[dict[str, Any]]:
    original_row_factory = conn.row_factory
    try:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(query, tuple(params))
        return [dict(row) for row in cursor.fetchall()]
    finally:
        conn.row_factory = original_row_factory


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (str(table),),
    ).fetchone()
    return row is not None


def _normalized_endpoint_key(host: Any, port: Any) -> Optional[str]:
    host_text = _coerce_text(host, "").lower()
    port_int = _coerce_optional_int(port)
    if not host_text or port_int is None or port_int <= 0:
        return None
    return f"{host_text}:{int(port_int)}"


def _normalized_path_key(path: Any) -> Optional[str]:
    text = _coerce_text(path, "")
    if not text:
        return None
    return str(Path(text).expanduser()).rstrip("/\\").lower()


def _duplicate_value_warnings(
    rows: Iterable[Mapping[str, Any]],
    *,
    warning_type: str,
    resource_type: str,
    value_getter: Callable[[Mapping[str, Any]], Optional[str]],
) -> list[MultiRigGuardrailWarning]:
    by_value: Dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        value = value_getter(row)
        if not value:
            continue
        by_value.setdefault(str(value), []).append(row)
    warnings: list[MultiRigGuardrailWarning] = []
    for value, matches in sorted(by_value.items()):
        if len(matches) < 2:
            continue
        sorted_matches = sorted(matches, key=lambda item: (_coerce_text(item.get("name", ""), ""), int(item.get("id", 0) or 0)))
        ids = tuple(int(item.get("id", 0) or 0) for item in sorted_matches)
        names = tuple(_coerce_text(item.get("name", ""), "") or f"Radio {item.get('id')}" for item in sorted_matches)
        warnings.append(
            MultiRigGuardrailWarning(
                warning_type=warning_type,
                resource_type=resource_type,
                resource_value=value,
                affected_radio_ids=ids,
                affected_radio_names=names,
            )
        )
    return warnings


def collect_multi_rig_guardrail_warnings(conn: sqlite3.Connection) -> tuple[MultiRigGuardrailWarning, ...]:
    """Return structured non-fatal configuration warnings for active multi-rig profiles."""
    if not _table_exists(conn, "device_profiles"):
        return ()
    rows = _fetchall_dicts(
        conn,
        """
        SELECT *
          FROM device_profiles
         WHERE enabled=1
           AND runtime_active=1
      ORDER BY display_order ASC, id ASC
        """,
    )
    tx_rows = [
        row
        for row in rows
        if _coerce_text(row.get("device_class", "tx_rx"), "tx_rx").lower() != "observer"
    ]
    warnings: list[MultiRigGuardrailWarning] = []
    warnings.extend(
        _duplicate_value_warnings(
            rows,
            warning_type="duplicate_js8_endpoint",
            resource_type="JS8Call API endpoint",
            value_getter=lambda row: _normalized_endpoint_key(row.get("js8_host"), row.get("js8_port"))
            if _coerce_bool_int(row.get("use_js8call"), False)
            else None,
        )
    )
    warnings.extend(
        _duplicate_value_warnings(
            rows,
            warning_type="duplicate_fldigi_endpoint",
            resource_type="FLDigi XML-RPC endpoint",
            value_getter=lambda row: _normalized_endpoint_key(row.get("fldigi_host"), row.get("fldigi_port"))
            if _coerce_bool_int(row.get("use_fldigi"), False)
            else None,
        )
    )
    warnings.extend(
        _duplicate_value_warnings(
            tx_rows,
            warning_type="duplicate_flrig_endpoint",
            resource_type="FLRig control endpoint",
            value_getter=lambda row: _normalized_endpoint_key(row.get("flrig_host"), row.get("flrig_port"))
            if _coerce_text(row.get("control_backend"), "").lower() == "flrig"
            or _coerce_bool_int(row.get("use_flrig"), False)
            else None,
        )
    )
    warnings.extend(
        _duplicate_value_warnings(
            tx_rows,
            warning_type="duplicate_rigctld_endpoint",
            resource_type="rigctld control endpoint",
            value_getter=lambda row: _normalized_endpoint_key(row.get("rig_host"), row.get("rig_port"))
            if _coerce_text(row.get("control_backend"), "").lower() == "rigctld"
            else None,
        )
    )
    warnings.extend(
        _duplicate_value_warnings(
            rows,
            warning_type="duplicate_varac_bbs_dir",
            resource_type="VarAC live BBS directory",
            value_getter=lambda row: _normalized_path_key(row.get("varac_bbs_dir"))
            if _coerce_bool_int(row.get("use_varac"), False)
            else None,
        )
    )
    warnings.extend(
        _duplicate_value_warnings(
            rows,
            warning_type="duplicate_varac_db_path",
            resource_type="VarAC database path",
            value_getter=lambda row: _normalized_path_key(row.get("varac_db_path"))
            if _coerce_bool_int(row.get("use_varac"), False)
            else None,
        )
    )
    warnings.extend(
        _duplicate_value_warnings(
            rows,
            warning_type="duplicate_flamp_message_path",
            resource_type="FLAMP message path",
            value_getter=lambda row: _normalized_path_key(row.get("flamp_message_path"))
            if _coerce_bool_int(row.get("use_flamp"), False)
            else None,
        )
    )
    warnings.extend(
        _duplicate_value_warnings(
            rows,
            warning_type="duplicate_flmsg_message_path",
            resource_type="FLMSG message path",
            value_getter=lambda row: _normalized_path_key(row.get("flmsg_message_path"))
            if _coerce_bool_int(row.get("use_flmsg"), False)
            else None,
        )
    )
    return tuple(warnings)


def format_multi_rig_guardrail_warnings(
    warnings: Iterable[MultiRigGuardrailWarning],
) -> tuple[str, ...]:
    return tuple(dict.fromkeys(warning.message for warning in warnings))

