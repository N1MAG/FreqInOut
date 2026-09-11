from __future__ import annotations

import os
import json
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication, QMessageBox

from freqinout.core.message_file_scanner import FileRecord
from freqinout.core.sqlite_utils import connect_sqlite
from freqinout.core.varac_bbs_library_store import (
    list_bbs_artifact_location_ids,
    set_bbs_location_artifact,
    upsert_bbs_artifact_path,
    upsert_bbs_location,
)
from freqinout.gui.message_viewer_tab import MessageTableModel, MessageViewerTab, UnifiedMessage


@dataclass
class _Settings:
    db_path: str

    def get(self, _key: str, default=None):
        return default


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


def _tab(monkeypatch, tmp_path: Path) -> MessageViewerTab:
    for name in (
        "_setup_clock_timer",
        "_setup_timer",
        "_setup_js8_timer",
        "_setup_pending_timer",
        "_setup_bbs_auto_archive_timer",
        "_initial_refresh",
        "_refresh_compose_forms",
    ):
        monkeypatch.setattr(MessageViewerTab, name, lambda self: None)
    tab = MessageViewerTab()
    tab.settings = _Settings(str(tmp_path / "freqinout.db"))
    tab._save_settings = lambda: None
    tab._populate_messages_table = lambda **_kwargs: None
    tab._unfreeze_table = lambda: None
    tab._bbs_copy_targets_cache = []
    tab._bbs_copy_targets_cache_ts = 0.0
    tab._bbs_published_index_cache = {}
    tab._bbs_published_index_cache_ts = 0.0
    return tab


def _file_row(path: Path, *, origin: str = "flmsg") -> UnifiedMessage:
    stat = path.stat()
    rec = FileRecord(path=path, origin=origin, size=stat.st_size, mtime=stat.st_mtime)
    return UnifiedMessage(
        msg_type="FLMsg K2S",
        status="NEW",
        from_call="",
        to_call="",
        rcv_ts=stat.st_mtime,
        rcv_display="now",
        title=path.name,
        origin=origin,
        payload=rec,
    )


def _seed_locations(db_path: str) -> None:
    with connect_sqlite(db_path) as conn:
        with conn:
            upsert_bbs_location(conn, location_id="public", name="Public")
            upsert_bbs_location(conn, location_id="intel", name="Intel")


def test_reader_bbs_visibility_and_navigation_state_are_cache_only(monkeypatch, tmp_path) -> None:
    _app()
    tab = _tab(monkeypatch, tmp_path)
    source = tmp_path / "report.k2s"
    source.write_text("report", encoding="utf-8")
    eligible = _file_row(source)
    ineligible = UnifiedMessage("MSG", "NEW", "", "", 0.0, "", "text", "js8", object())
    try:
        tab._varac_bbs_copy_targets = lambda: (_ for _ in ()).throw(
            AssertionError("reader visibility must not query BBS targets")
        )

        tab._sync_reader_bbs_action(eligible)
        assert tab.reader_bbs_btn.isHidden() is False
        assert tab.reader_bbs_btn.text() == "+BBS"

        tab._sync_reader_bbs_action(ineligible)
        assert tab.reader_bbs_btn.isHidden() is True
    finally:
        tab.close()
        tab.deleteLater()


def test_reader_delete_is_only_offered_for_existing_flmsg_flamp_files(monkeypatch, tmp_path) -> None:
    _app()
    tab = _tab(monkeypatch, tmp_path)
    flmsg_path = tmp_path / "report.k2s"
    flmsg_path.write_text("report", encoding="utf-8")
    flmsg_row = _file_row(flmsg_path, origin="flmsg")
    bbs_path = tmp_path / "visitor.txt"
    bbs_path.write_text("visitor", encoding="utf-8")
    bbs_row = _file_row(bbs_path, origin="bbs")
    try:
        tab._sync_reader_delete_action(flmsg_row)
        assert tab.reader_delete_btn.isHidden() is False
        assert "Trash or Recycle Bin" in tab.reader_delete_btn.toolTip()

        tab._sync_reader_delete_action(bbs_row)
        assert tab.reader_delete_btn.isHidden() is True

        flmsg_path.unlink()
        tab._sync_reader_delete_action(flmsg_row)
        assert tab.reader_delete_btn.isHidden() is True
    finally:
        tab.close()
        tab.deleteLater()


def test_reader_delete_confirms_recoverable_effect_and_returns_to_inbox(monkeypatch, tmp_path) -> None:
    _app()
    tab = _tab(monkeypatch, tmp_path)
    source = tmp_path / "received.sig.b2s"
    source.write_text("signed report", encoding="utf-8")
    row = _file_row(source, origin="flamp")
    prompts: list[str] = []
    audits: list[tuple[str, str]] = []
    projected: list[UnifiedMessage] = []
    notices: list[str] = []

    def confirm(_row, prompt: str) -> bool:
        prompts.append(prompt)
        return True

    def trash(path: Path) -> bool:
        path.unlink()
        return True

    monkeypatch.setattr(
        QMessageBox,
        "information",
        lambda _parent, _title, text, *_args, **_kwargs: notices.append(str(text)) or QMessageBox.Ok,
    )
    tab._confirm_single_delete = confirm
    tab._send_to_recycle_bin = trash
    tab._record_message_delete_audit = (
        lambda _row, *, result, detail="", batch_id="": audits.append((result, batch_id))
    )
    tab._mark_projection_rows_deleted = lambda rows, **_kwargs: projected.extend(rows)
    try:
        tab._reader_snapshot = [row]
        tab._reader_index = 0
        tab._reader_open = True
        tab._sync_reader_delete_action(row)

        tab._delete_reader_file_message()

        assert not source.exists()
        assert tab._reader_open is False
        assert projected == [row]
        assert audits == [("deleted", "reader")]
        assert str(source) in prompts[0]
        assert "Trash / Recycle Bin" in prompts[0]
        assert "publication will stop" in prompts[0]
        assert notices == [f"Moved {source.name} to Trash / Recycle Bin."]
    finally:
        tab.close()
        tab.deleteLater()


def test_reader_delete_cancel_preserves_file_and_reader(monkeypatch, tmp_path) -> None:
    _app()
    tab = _tab(monkeypatch, tmp_path)
    source = tmp_path / "keep.k2s"
    source.write_text("keep", encoding="utf-8")
    row = _file_row(source, origin="flmsg")
    try:
        tab._reader_snapshot = [row]
        tab._reader_index = 0
        tab._reader_open = True
        tab._confirm_single_delete = lambda *_args, **_kwargs: False
        tab._send_to_recycle_bin = lambda _path: (_ for _ in ()).throw(
            AssertionError("Trash operation must not run after Cancel")
        )

        tab._delete_reader_file_message()

        assert source.read_text(encoding="utf-8") == "keep"
        assert tab._reader_open is True
        assert tab._reader_current_row() is row
    finally:
        tab.close()
        tab.deleteLater()


def test_file_delete_uses_native_macos_trash_without_shell(monkeypatch, tmp_path) -> None:
    source = tmp_path / 'report "quoted".k2s'
    source.write_text("report", encoding="utf-8")
    commands: list[list[str]] = []
    monkeypatch.setattr("freqinout.gui.message_viewer_tab.platform.system", lambda: "Darwin")
    monkeypatch.setattr("freqinout.gui.message_viewer_tab.shutil.which", lambda name: f"/usr/bin/{name}")

    def run(command, **_kwargs):
        commands.append(list(command))
        return SimpleNamespace(returncode=0, stderr="")

    monkeypatch.setattr("freqinout.gui.message_viewer_tab.subprocess.run", run)

    assert MessageViewerTab._send_to_recycle_bin(source) is True
    assert commands[0][0:2] == ["/usr/bin/osascript", "-e"]
    assert "Finder" in commands[0][2]
    assert json.dumps(str(source)) in commands[0][2]
    assert len(commands[0]) == 3


def test_reader_bbs_apply_updates_count_and_nonmodal_confirmation(monkeypatch, tmp_path) -> None:
    _app()
    tab = _tab(monkeypatch, tmp_path)
    source = tmp_path / "report.b2s"
    source.write_text("report", encoding="utf-8")
    row = _file_row(source, origin="flamp")
    try:
        tab._reader_snapshot = [row]
        tab._reader_index = 0
        tab._copy_row_to_varac_bbs = lambda *_args, **_kwargs: 2
        tab._sync_reader_bbs_action(row)

        tab._manage_reader_bbs_locations()

        assert tab.reader_bbs_btn.text() == "BBS · 2"
        assert tab.reader_bbs_status_label.text() == "BBS updated"
        assert tab.reader_bbs_status_label.isHidden() is False
    finally:
        tab.close()
        tab.deleteLater()


def test_reader_apply_replaces_memberships_without_touching_source(monkeypatch, tmp_path) -> None:
    _app()
    tab = _tab(monkeypatch, tmp_path)
    _seed_locations(tab.settings.db_path)
    source = tmp_path / "received.k2s"
    source.write_text("received data", encoding="utf-8")
    row = _file_row(source)
    target = {"id": "location:public", "location_id": "public", "label": "Public"}
    monkeypatch.setattr(QMessageBox, "information", lambda *_args, **_kwargs: QMessageBox.Ok)
    try:
        tab._is_row_bbs_copy_action_enabled = lambda _row: True
        tab._select_varac_bbs_publish_targets = lambda _row: [target]
        assert tab._copy_row_to_varac_bbs(row, show_confirmation=False) == 1

        with connect_sqlite(tab.settings.db_path) as conn:
            artifact_id = conn.execute(
                "SELECT artifact_id FROM bbs_artifacts WHERE source_path=?",
                (str(source.resolve()),),
            ).fetchone()[0]
            assert list_bbs_artifact_location_ids(conn, artifact_id) == ("public",)

        tab._select_varac_bbs_publish_targets = lambda _row: []
        assert tab._copy_row_to_varac_bbs(row, show_confirmation=False) == 0
        with connect_sqlite(tab.settings.db_path) as conn:
            assert list_bbs_artifact_location_ids(conn, artifact_id) == ()
        assert source.read_text(encoding="utf-8") == "received data"
    finally:
        tab.close()
        tab.deleteLater()


def test_bulk_publication_is_additive_bounded_and_skips_nonfiles(monkeypatch, tmp_path) -> None:
    _app()
    tab = _tab(monkeypatch, tmp_path)
    _seed_locations(tab.settings.db_path)
    source = tmp_path / "received.sig.b2s"
    source.write_text("signed report", encoding="utf-8")
    other_source = tmp_path / "already-in-bbs.txt"
    other_source.write_text("visitor file", encoding="utf-8")
    eligible = _file_row(source, origin="flamp")
    ineligible = _file_row(other_source, origin="bbs")
    with connect_sqlite(tab.settings.db_path) as conn:
        with conn:
            artifact_id = upsert_bbs_artifact_path(conn, source_path=source)
            set_bbs_location_artifact(conn, location_id="public", artifact_id=artifact_id)
    tab._messages_model = MessageTableModel([eligible, ineligible])
    tab.messages_table.setModel(tab._messages_model)
    tab._messages_model.set_selected_for_rows([eligible, ineligible], True)
    tab._select_varac_bbs_publish_targets = lambda _row: [
        {"id": "location:intel", "location_id": "intel", "label": "Intel"}
    ]
    notices: list[str] = []
    monkeypatch.setattr(
        QMessageBox,
        "information",
        lambda _parent, _title, text, *_args, **_kwargs: notices.append(str(text)) or QMessageBox.Ok,
    )
    try:
        tab._publish_selected_messages_to_bbs()

        with connect_sqlite(tab.settings.db_path) as conn:
            assert list_bbs_artifact_location_ids(conn, artifact_id) == ("intel", "public")
        assert source.read_text(encoding="utf-8") == "signed report"
        assert any("Skipped 1 selected item not eligible" in notice for notice in notices)
        assert len(tab._eligible_bbs_publication_rows([eligible] * 250)) == 1
        newer_snapshot = _file_row(source, origin="flamp")
        newer_snapshot.payload.mtime += 1.0
        assert len(tab._eligible_bbs_publication_rows([eligible, newer_snapshot])) == 1
    finally:
        tab.close()
        tab.deleteLater()
