from __future__ import annotations

import os
from pathlib import Path

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication

from freqinout.core.varac_callsign_tags import (
    resolve_varac_callsign_tags_path,
    sync_varac_callsign_tags_file,
)
from freqinout.gui.operator_history_tab import OperatorHistoryTab


class _DummySettings:
    def __init__(self, values: dict[str, object] | None = None):
        self._values = dict(values or {})

    def get(self, key: str, default=None):
        return self._values.get(key, default)


def test_varac_callsign_tag_sync_reconciles_duplicates_and_preserves_unmanaged_lines(tmp_path: Path) -> None:
    tags_path = tmp_path / "VarAC_callsign_tags.conf"
    tags_path.write_text(
        "# keep me\n"
        '"K1AAA / Old Name / TX"\n'
        'K1AAA,Duplicate / WY / OLD1 / OLD2 / OLD3 / NCS\n'
        "custom=true\n"
        'K9ZZZ,Remove Me / CO / G1 / G2 / G3 / OPS\n',
        encoding="utf-8",
    )

    result = sync_varac_callsign_tags_file(
        tags_path,
        [
            {"callsign": "k1aaa", "name": "New Name", "state": "co", "group1": "magnet", "group2": "sitrep", "group3": "", "group_role": "ops"},
            {"callsign": "k2bbb", "name": "Jane Doe", "state": "wy", "group1": "amrron", "group2": "", "group3": "ares", "group_role": "liaison"},
            {"callsign": "k3ccc", "name": "", "state": "nm", "group1": "foo", "group2": "", "group3": "", "group_role": "ops"},
        ],
    )

    assert result.changed is True
    assert result.managed_count == 2
    assert result.added == 1
    assert result.updated == 1
    assert result.removed == 1
    assert result.deduplicated == 1

    assert tags_path.read_text(encoding="utf-8").splitlines() == [
        "# keep me",
        "K1AAA,New Name / CO / MAGNET / SITREP /  / OPS",
        "custom=true",
        "K2BBB,Jane Doe / WY / AMRRON /  / ARES / LIAISON",
    ]


def test_resolve_varac_callsign_tags_path_uses_install_parent_for_executable() -> None:
    settings = _DummySettings({"varac_path": r"C:\VarAC\VarAC.exe"})
    path = resolve_varac_callsign_tags_path(settings)
    assert path is not None
    assert str(path).endswith(r"VarAC_callsign_tags.conf")
    assert str(path.parent).endswith(r"VarAC")


def test_operator_history_theme_styles_row_checkbox_indicator(monkeypatch, tmp_path: Path) -> None:
    profile = tmp_path / "profile"
    (profile / "config").mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(profile))
    app = QApplication.instance() or QApplication([])
    tab = OperatorHistoryTab()

    tab.apply_theme()

    style = tab.table.styleSheet()
    assert "QTableWidget::indicator" in style
    assert "border: 1px solid" in style
    assert "background-color" in style
    assert app is not None


def test_manage_menu_includes_sync_to_varac(monkeypatch, tmp_path: Path) -> None:
    profile = tmp_path / "profile"
    (profile / "config").mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(profile))
    QApplication.instance() or QApplication([])
    tab = OperatorHistoryTab()

    class _FakeMenu:
        actions: list[str] = []

        def __init__(self, *_args, **_kwargs):
            type(self).actions = []

        def addAction(self, label, _callback=None):
            self.actions.append(label)
            return None

        def addSeparator(self):
            self.actions.append("---")

        def exec(self, *_args, **_kwargs):
            return None

    monkeypatch.setattr("freqinout.gui.operator_history_tab.QMenu", _FakeMenu)

    tab._show_manage_menu()

    assert "Sync to VarAC" in _FakeMenu.actions


def test_add_operator_dialog_triggers_varac_sync_after_success() -> None:
    tab = OperatorHistoryTab.__new__(OperatorHistoryTab)
    seen: list[str] = []
    tab._collect_dialog_data = lambda: {"callsign": "K1AAA", "name": "Alpha", "state": "CO"}
    tab._upsert_record = lambda *_args, **_kwargs: True
    tab._load_data = lambda **_kwargs: seen.append("load")
    tab._schedule_history_update = lambda: seen.append("schedule")
    tab._sync_varac_callsign_tags = lambda: seen.append("sync")

    OperatorHistoryTab._add_operator_dialog(tab)

    assert seen == ["load", "schedule", "sync"]
