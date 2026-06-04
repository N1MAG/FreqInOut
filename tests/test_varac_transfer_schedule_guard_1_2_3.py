from __future__ import annotations

import datetime as dt
import sqlite3
from pathlib import Path

from freqinout.radio_interface.js8_status import VarACStatusClient


class _StatusSettings:
    def __init__(self, **values):
        self._data = dict(values)

    def get(self, key, default=None):
        return self._data.get(key, default)


def _ts(seconds_ago: float) -> str:
    when = dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=seconds_ago)
    return when.isoformat().replace("+00:00", "Z")


def _write_varac_db(path: Path, rows: list[tuple[int, str, str, str | None, str | None]]) -> None:
    conn = sqlite3.connect(str(path))
    try:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE qso (
                guid TEXT PRIMARY KEY,
                callsign TEXT,
                endtime TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE datastream (
                id INTEGER PRIMARY KEY,
                qso_guid TEXT,
                creation_time TEXT,
                entry TEXT,
                is_deleted INTEGER DEFAULT 0
            )
            """
        )
        seen_qso: set[str] = set()
        for rid, qso_guid, creation_time, entry, endtime in rows:
            if qso_guid not in seen_qso:
                cur.execute(
                    "INSERT INTO qso (guid, callsign, endtime) VALUES (?, ?, ?)",
                    (qso_guid, "W5TTA", endtime),
                )
                seen_qso.add(qso_guid)
            cur.execute(
                "INSERT INTO datastream (id, qso_guid, creation_time, entry, is_deleted) VALUES (?, ?, ?, ?, 0)",
                (rid, qso_guid, creation_time, entry),
            )
        conn.commit()
    finally:
        conn.close()


def test_varac_status_uses_db_transfer_activity_when_logs_are_missing(tmp_path: Path) -> None:
    varac_dir = tmp_path / "varac"
    varac_dir.mkdir()
    db_path = varac_dir / "VarAC.db"
    _write_varac_db(
        db_path,
        [
            (1, "qso-1", _ts(8), "<< RECEIVING FILE TRANSFER DATA >>", None),
        ],
    )
    settings = _StatusSettings(varac_path=str(varac_dir), varac_db_path=str(db_path))

    status = VarACStatusClient(settings=settings).get_status()

    assert status["busy"] is True
    assert status["reason"] == "transfer"
    assert status["db_transfer_active"] is True


def test_varac_status_holds_recent_successful_transfer_during_cooldown(tmp_path: Path) -> None:
    varac_dir = tmp_path / "varac"
    varac_dir.mkdir()
    db_path = varac_dir / "VarAC.db"
    _write_varac_db(
        db_path,
        [
            (1, "qso-1", _ts(40), "<< RECEIVING FILE TRANSFER DATA >>", None),
            (2, "qso-1", _ts(3), "FILE SUCCESSFULLY RECEIVED: file://C:/VarAC/Incoming/Test.k2s", _ts(2)),
        ],
    )
    settings = _StatusSettings(varac_path=str(varac_dir), varac_db_path=str(db_path))

    status = VarACStatusClient(settings=settings).get_status()

    assert status["busy"] is True
    assert status["reason"] == "transfer_cooldown"
    assert status["db_transfer_cooldown"] is True


def test_varac_status_clears_old_completed_transfer_without_logs(tmp_path: Path) -> None:
    varac_dir = tmp_path / "varac"
    varac_dir.mkdir()
    db_path = varac_dir / "VarAC.db"
    _write_varac_db(
        db_path,
        [
            (1, "qso-1", _ts(80), "<< RECEIVING FILE TRANSFER DATA >>", None),
            (2, "qso-1", _ts(45), "FILE SUCCESSFULLY SENT: file://C:/VarAC/BBS/Test.k2s", _ts(44)),
        ],
    )
    settings = _StatusSettings(varac_path=str(varac_dir), varac_db_path=str(db_path))

    status = VarACStatusClient(settings=settings).get_status()

    assert status["busy"] is False
    assert status["db_transfer_busy"] is False


def test_varac_status_clears_stale_incomplete_transfer_without_recent_activity(tmp_path: Path) -> None:
    varac_dir = tmp_path / "varac"
    varac_dir.mkdir()
    db_path = varac_dir / "VarAC.db"
    _write_varac_db(
        db_path,
        [
            (1, "qso-1", _ts(1200), "<< RECEIVING FILE TRANSFER DATA >>", None),
        ],
    )
    settings = _StatusSettings(varac_path=str(varac_dir), varac_db_path=str(db_path))

    status = VarACStatusClient(settings=settings).get_status()

    assert status["busy"] is False
    assert status["db_transfer_busy"] is False
    assert status["reason"] is None


def test_varac_status_clears_transfer_when_qso_has_ended(tmp_path: Path) -> None:
    varac_dir = tmp_path / "varac"
    varac_dir.mkdir()
    db_path = varac_dir / "VarAC.db"
    _write_varac_db(
        db_path,
        [
            (1, "qso-1", _ts(180), "<< RECEIVING FILE TRANSFER DATA >>", _ts(60)),
        ],
    )
    settings = _StatusSettings(varac_path=str(varac_dir), varac_db_path=str(db_path))

    status = VarACStatusClient(settings=settings).get_status()

    assert status["busy"] is False
    assert status["db_transfer_busy"] is False


def test_varac_status_clears_stale_waiting_for_frequency_without_follow_on_session() -> None:
    client = VarACStatusClient(settings=None)
    stale = (
        dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=1200)
    ).astimezone().strftime("%d/%m/%Y %H:%M:%S")
    text = f"{stale} - WAITING FOR FREQUENCY TO CLEAR"

    status = client._evaluate_status(text)

    assert status["busy"] is False
    assert status["waiting_for_frequency"] is False
