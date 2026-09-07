from __future__ import annotations

import os
from pathlib import Path
import sqlite3

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication

from freqinout.core.checkins_db import ensure_operator_checkins_schema
from freqinout.core.db_initializer import _ensure_js8_expect_tables
from freqinout.core.js8_expect_store import (
    evaluate_expect_request,
    list_expect_entries,
    list_expect_operator_access_catalog,
    save_expect_allow_policy,
    save_expect_entry,
    update_expect_entry_controls,
)
from freqinout.core import js8_expect_store
from freqinout.core.operator_identity import change_operator_callsign
from freqinout.gui import fio_spotter_tab as spotter_ui
from freqinout.gui.fio_spotter_tab import FioSpotterTab


class _Settings:
    def __init__(self) -> None:
        self.values: dict[str, object] = {}

    def get(self, key: str, default=None):
        return self.values.get(key, default)

    def set(self, key: str, value: object) -> None:
        self.values[key] = value

    def save(self) -> None:
        pass


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


def _seed_operator(
    db: Path,
    callsign: str,
    *,
    trusted: bool,
    groups: tuple[str, ...],
    name: str = "Operator",
) -> None:
    with sqlite3.connect(db) as conn:
        ensure_operator_checkins_schema(conn)
        padded = list(groups[:3]) + [""] * (3 - len(groups[:3]))
        conn.execute(
            """
            INSERT INTO operator_checkins(
                callsign, name, trusted, group1, group2, group3, groups_json,
                first_seen_utc, last_seen_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, '20260101', '20260101')
            """,
            (callsign, name, int(trusted), padded[0], padded[1], padded[2], str(list(groups)).replace("'", '"')),
        )
        ensure_operator_checkins_schema(conn)
        conn.commit()


def _save_ready_rule(db: Path, **overrides) -> int:
    values = {
        "expect_key": "INFO",
        "response_text": "READY",
        "source_scope": "all",
        "enabled": True,
        "auto_reply_enabled": True,
        "unattended_auto_reply_enabled": True,
    }
    values.update(overrides)
    return save_expect_entry(values, db_path=db).id


def _evaluate(db: Path, call: str):
    return evaluate_expect_request(
        expect_key="INFO",
        requesting_callsign=call,
        db_path=db,
        write_audit=False,
    )


def test_star_allows_every_caller_and_blocked_identity_still_wins(tmp_path: Path) -> None:
    db = tmp_path / "nets.db"
    _seed_operator(db, "K1OLD", trusted=False, groups=("MR08",))
    with sqlite3.connect(db) as conn:
        change_operator_callsign(conn, "K1OLD", "K1NEW", effective_at=2_000_000_000)
        conn.commit()
    _save_ready_rule(db, allowed_callsigns=["*"], blocked_callsigns=["K1NEW"])

    assert _evaluate(db, "W5TTA").decision == "reply-ready"
    blocked = _evaluate(db, "K1OLD")
    assert blocked.decision == "blocked"
    assert "blocked" in blocked.reason.lower()


def test_star_fast_path_does_not_load_operator_history(monkeypatch, tmp_path: Path) -> None:
    db = tmp_path / "nets.db"
    _save_ready_rule(db, allowed_callsigns=["*"])
    monkeypatch.setattr(
        js8_expect_store,
        "_operator_access_profile",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("operator history should stay lazy")),
    )
    assert _evaluate(db, "ANY1").decision == "reply-ready"


def test_explicit_callsign_access_follows_operator_identity_history(tmp_path: Path) -> None:
    db = tmp_path / "nets.db"
    _seed_operator(db, "K1OLD", trusted=False, groups=("MR08",))
    with sqlite3.connect(db) as conn:
        change_operator_callsign(conn, "K1OLD", "K1NEW", effective_at=2_000_000_000)
        conn.commit()
    _save_ready_rule(db, allowed_callsigns=["K1OLD"])

    result = _evaluate(db, "K1NEW")
    assert result.decision == "reply-ready"
    assert "identity" in result.reason.lower()


def test_trusted_operator_and_trusted_group_access_use_operator_history(tmp_path: Path) -> None:
    trusted_db = tmp_path / "trusted.db"
    _seed_operator(trusted_db, "K1ABC", trusted=True, groups=("MAGNET", "MR08"))
    _seed_operator(trusted_db, "K2ABC", trusted=False, groups=("MAGNET", "MR08"))
    _save_ready_rule(trusted_db, allow_trusted_operators=True)
    assert _evaluate(trusted_db, "K1ABC").decision == "reply-ready"
    assert _evaluate(trusted_db, "K2ABC").decision == "blocked"

    group_db = tmp_path / "group.db"
    _seed_operator(group_db, "K3ABC", trusted=True, groups=("MAGNET", "MR08"))
    _seed_operator(group_db, "K4ABC", trusted=True, groups=("AMRRON",))
    _save_ready_rule(group_db, trusted_operator_groups=["MR08"])
    assert _evaluate(group_db, "K3ABC").decision == "reply-ready"
    assert _evaluate(group_db, "K4ABC").decision == "blocked"


def test_allow_policy_supports_star_and_trusted_roster_groups(tmp_path: Path) -> None:
    db = tmp_path / "nets.db"
    _seed_operator(db, "K7ETC", trusted=True, groups=("MAGNET", "MR08"))
    policy = save_expect_allow_policy(
        {
            "name": "Regional traffic",
            "allowed_callsigns": ["*"],
            "allow_trusted_operators": True,
            "trusted_operator_groups": ["MR08"],
        },
        db_path=db,
    )
    _save_ready_rule(db, allow_policy_id=policy.id)
    assert _evaluate(db, "ANY1").decision == "reply-ready"


def test_access_schema_migration_is_additive_and_idempotent(tmp_path: Path) -> None:
    db = tmp_path / "nets.db"
    with sqlite3.connect(db) as conn:
        _ensure_js8_expect_tables(conn)
        conn.execute("INSERT INTO js8_expect_allow_policies(name) VALUES ('existing')")
        _ensure_js8_expect_tables(conn)
        policy_columns = {row[1] for row in conn.execute("PRAGMA table_info(js8_expect_allow_policies)")}
        entry_columns = {row[1] for row in conn.execute("PRAGMA table_info(js8_expect_entries)")}
        assert {"allow_trusted_operators", "trusted_operator_groups_json"} <= policy_columns
        assert {"allow_trusted_operators", "trusted_operator_groups_json"} <= entry_columns
        assert conn.execute("SELECT name FROM js8_expect_allow_policies").fetchall() == [("existing",)]


def test_legacy_control_update_preserves_trusted_access_fields(tmp_path: Path) -> None:
    db = tmp_path / "nets.db"
    entry_id = _save_ready_rule(
        db,
        allow_trusted_operators=True,
        trusted_operator_groups=["MR08"],
    )
    update_expect_entry_controls(
        entry_id,
        {
            "enabled": True,
            "auto_reply_enabled": True,
            "allowed_callsigns": [],
            "allowed_groups": [],
            "blocked_callsigns": [],
        },
        db_path=db,
    )
    row = list_expect_entries(db_path=db)[0]
    assert row["allow_trusted_operators"] == 1
    assert row["trusted_operator_groups"] == ["MR08"]


def test_operator_catalog_lists_current_and_former_callsigns_with_trust(tmp_path: Path) -> None:
    db = tmp_path / "nets.db"
    _seed_operator(db, "K1OLD", trusted=True, groups=("MAGNET", "MR08"), name="Test Op")
    with sqlite3.connect(db) as conn:
        change_operator_callsign(conn, "K1OLD", "K1NEW", effective_at=2_000_000_000)
        conn.commit()
    rows = list_expect_operator_access_catalog(db_path=db)
    by_call = {row["callsign"]: row for row in rows}
    assert {"K1OLD", "K1NEW"} <= set(by_call)
    assert by_call["K1OLD"]["historical"] is True
    assert by_call["K1OLD"]["current_callsign"] == "K1NEW"
    assert by_call["K1NEW"]["trusted"] is True
    assert set(by_call["K1NEW"]["groups"]) >= {"MAGNET", "MR08"}


def test_expect_access_ui_is_lazy_autocompleting_and_compact(monkeypatch, tmp_path: Path) -> None:
    app = _app()
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    calls: list[int] = []

    def catalog(*, limit: int):
        calls.append(limit)
        return [
            {"callsign": "K1OLD", "current_callsign": "K1NEW", "historical": True, "trusted": True, "groups": ["MAGNET", "MR08"]},
            {"callsign": "K1NEW", "current_callsign": "K1NEW", "historical": False, "trusted": True, "groups": ["MAGNET", "MR08"]},
        ]

    monkeypatch.setattr(spotter_ui, "list_expect_operator_access_catalog", catalog)
    tab = FioSpotterTab(settings=_Settings())
    try:
        assert calls == []
        tab.resize(900, 560)
        tab.tabs.setCurrentIndex(2)
        tab.show()
        app.processEvents()
        assert calls == [2000]
        assert "K1OLD" in tab.expect_calls._completion_values
        assert "@MR08" in tab.expect_groups._completion_values
        assert "MR08" in tab.expect_trusted_groups._completion_values
        assert tab.expect_editor_split.orientation() == Qt.Vertical
        tab.expect_calls.setText("K7ETC")
        tab.expect_allow_any.setChecked(True)
        assert tab.expect_calls.text() == "*, K7ETC"
        tab.expect_allow_any.setChecked(False)
        assert tab.expect_calls.text() == "K7ETC"
        tab.expect_key.setText("Q")
        tab.expect_calls.setText("*")
        tab.expect_trusted.setChecked(True)
        tab.expect_trusted_groups.setText("MR08")
        tab.expect_groups.setText("@MAGNET")
        tab.expect_enabled.setChecked(True)
        tab.expect_auto.setChecked(True)
        tab.expect_unattended.setChecked(True)
        tab._save_entry()
        row = spotter_ui.list_expect_entries()[0]
        assert row["allow_any"] == 1
        assert row["allow_trusted_operators"] == 1
        assert row["trusted_operator_groups"] == ["MR08"]
        assert row["allowed_groups"] == ["@MAGNET"]
        assert "Any caller" in tab._expect_access_summary(row)
        assert "addressed group" in tab._expect_access_summary(row)
    finally:
        tab.close()
        tab.deleteLater()
