from __future__ import annotations

import datetime as dt
import os
from pathlib import Path

from freqinout.core.varac_guard import run_varac_guard


class _GuardSettings:
    def __init__(self, **values):
        self._data = dict(values)

    def get(self, key, default=None):
        return self._data.get(key, default)

    def set(self, key, value):
        self._data[key] = value


def _write_log(base: Path, text: str) -> Path:
    log_path = base / "VarAC_traffic.log"
    log_path.write_text(text, encoding="utf-8")
    return log_path


def _event_stamp() -> tuple[str, float]:
    when = dt.datetime.now(dt.timezone.utc)
    return when.strftime("%m/%d/%Y %H:%M:%S"), when.timestamp()


def test_varac_guard_allows_authorized_sender_without_touching_file(tmp_path: Path) -> None:
    base = tmp_path / "varac"
    incoming = tmp_path / "incoming"
    base.mkdir()
    incoming.mkdir()
    file_path = incoming / "Authorized.k2s"
    file_path.write_text("payload", encoding="utf-8")
    stamp_text, stamp_ts = _event_stamp()
    os.utime(file_path, (stamp_ts, stamp_ts))
    _write_log(
        base,
        f"{stamp_text} - FILE SUCCESSFULLY RECEIVED FROM AUTHORIZED - FILE: Authorized.k2s\n",
    )

    settings = _GuardSettings(
        varac_guard_enabled=True,
        varac_guard_mode="Delete unauthorized files",
        varac_guard_retry_seconds=120,
        varac_path=str(base),
        varac_incoming_path=str(incoming),
        message_paths={"varac": str(incoming)},
        varac_bbs_allowed_callsigns="AUTHORIZED",
    )

    result = run_varac_guard(settings)

    assert result.allowed_events == 1
    assert result.unauthorized_events == 0
    assert file_path.exists()
    assert settings.get("varac_guard_state_v1", {}).get("processed_event_keys")


def test_varac_guard_deletes_unauthorized_file_and_persists_state(tmp_path: Path) -> None:
    base = tmp_path / "varac"
    incoming = tmp_path / "incoming"
    base.mkdir()
    incoming.mkdir()
    file_path = incoming / "Blocked.k2s"
    file_path.write_text("payload", encoding="utf-8")
    stamp_text, stamp_ts = _event_stamp()
    os.utime(file_path, (stamp_ts, stamp_ts))
    _write_log(
        base,
        f"{stamp_text} - FILE SUCCESSFULLY RECEIVED FROM BADGUY - FILE: Blocked.k2s\n",
    )

    settings = _GuardSettings(
        varac_guard_enabled=True,
        varac_guard_mode="Delete unauthorized files",
        varac_guard_retry_seconds=120,
        varac_path=str(base),
        varac_incoming_path=str(incoming),
        message_paths={"varac": str(incoming)},
        varac_bbs_allowed_callsigns="",
    )

    result = run_varac_guard(settings)

    assert result.unauthorized_events == 1
    assert result.deleted_files == 1
    assert not file_path.exists()
    state = settings.get("varac_guard_state_v1", {})
    assert state.get("processed_event_keys")
    assert settings.get("varac_guard_last_summary", "").startswith("VGuard Delete unauthorized files")


def test_varac_guard_leaves_event_pending_when_file_has_not_arrived_yet(tmp_path: Path) -> None:
    base = tmp_path / "varac"
    incoming = tmp_path / "incoming"
    base.mkdir()
    incoming.mkdir()
    stamp_text, _stamp_ts = _event_stamp()
    _write_log(
        base,
        f"{stamp_text} - FILE SUCCESSFULLY RECEIVED FROM BADGUY - FILE: Delayed.k2s\n",
    )

    settings = _GuardSettings(
        varac_guard_enabled=True,
        varac_guard_mode="Delete unauthorized files",
        varac_guard_retry_seconds=600,
        varac_path=str(base),
        varac_incoming_path=str(incoming),
        message_paths={"varac": str(incoming)},
        varac_bbs_allowed_callsigns="",
    )

    result = run_varac_guard(settings)

    assert result.pending_events == 1
    assert result.deleted_files == 0
    assert settings.get("varac_guard_state_v1", {}).get("processed_event_keys") in (None, [])
