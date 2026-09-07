from __future__ import annotations

import os
from pathlib import Path
from types import MethodType, SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from freqinout.core.sqlite_utils import connect_sqlite
from freqinout.core.varac_bbs_library_store import (
    ensure_bbs_library_schema,
    list_bbs_artifact_location_ids,
    upsert_bbs_artifact_path,
    upsert_bbs_location,
)
from freqinout.gui import message_viewer_tab as mvt
from freqinout.core.message_projection_payload import ProjectedMessagePayload
from freqinout.gui.message_viewer_tab import FileRecord, MessageViewerTab


class _MemorySettings:
    def __init__(self, bbs_dir: Path, db_path: Path | None = None) -> None:
        self._data = {"varac_bbs_enabled": True, "varac_bbs_dir": str(bbs_dir)}
        if db_path is not None:
            self.db_path = str(db_path)

    def get(self, key: str, default=None):
        return self._data.get(key, default)


def _row(path: Path, *, msg_type: str = "FLAMP") -> SimpleNamespace:
    st = path.stat()
    return SimpleNamespace(
        msg_type=msg_type,
        payload=FileRecord(path=path, origin="flamp", size=st.st_size, mtime=st.st_mtime),
    )


def _tab(bbs_dir: Path) -> SimpleNamespace:
    tab = SimpleNamespace(
        settings=_MemorySettings(bbs_dir),
        _bbs_copied_session_keys=set(),
        _bbs_copy_target_session_id="",
        _unfreeze_table=lambda: None,
        _populate_messages_table=lambda force=False: None,
    )
    for name in (
        "_file_record_for_message_row",
        "_projected_file_record",
        "_can_copy_row_to_varac_bbs",
        "_bbs_copy_session_key_for_record",
        "_bbs_copy_session_key_for_row",
        "_bbs_copy_session_marker",
        "_varac_bbs_copy_targets",
        "_managed_bbs_published_target_ids_for_record",
        "_select_varac_bbs_publish_targets",
        "_select_varac_bbs_copy_target",
        "_varac_bbs_destination_for_row",
        "_is_row_already_in_varac_bbs",
        "_is_row_bbs_copy_action_enabled",
        "_mark_row_copied_to_varac_bbs_session",
        "_varac_bbs_existing_copy_targets",
        "_remove_row_from_varac_bbs",
        "_copy_row_to_varac_bbs",
        "_invalidate_bbs_action_cache",
    ):
        setattr(tab, name, MethodType(getattr(MessageViewerTab, name), tab))
    tab._file_record_from_projected_refs = MessageViewerTab._file_record_from_projected_refs
    tab._file_record_from_projected_artifacts = MessageViewerTab._file_record_from_projected_artifacts
    return tab


def test_safe_varac_bbs_filename_normalizes_problem_names() -> None:
    cases = {
        "Report .k2s": "Report.k2s",
        "Report.k2s ": "Report.k2s",
        "A\\B.k2s": "A_B.k2s",
        "A/B.k2s": "A_B.k2s",
        "Report\tFinal.b2s": "Report Final.b2s",
        "Report?.k2s": "Report_.k2s",
        " .k2s": "message.k2s",
        "Report .k2s.sig": "Report.k2s.sig",
        " ": "message",
    }
    for raw, expected in cases.items():
        assert MessageViewerTab._safe_varac_bbs_filename(raw) == expected


def test_varac_bbs_destination_and_present_state_use_normalized_name(tmp_path: Path) -> None:
    src_dir = tmp_path / "src"
    bbs_dir = tmp_path / "bbs"
    src_dir.mkdir()
    bbs_dir.mkdir()
    src = src_dir / "Report .k2s"
    src.write_text("payload", encoding="utf-8")
    normalized = bbs_dir / "Report.k2s"
    normalized.write_text("payload", encoding="utf-8")
    os.utime(normalized, (src.stat().st_atime, src.stat().st_mtime))

    tab = _tab(bbs_dir)
    row = _row(src)

    assert tab._varac_bbs_destination_for_row(row) == normalized
    assert tab._is_row_already_in_varac_bbs(row) is True


def test_copy_to_varac_bbs_uses_unique_name_when_normalized_name_collides(
    tmp_path: Path,
) -> None:
    src_dir = tmp_path / "src"
    bbs_dir = tmp_path / "bbs"
    src_dir.mkdir()
    bbs_dir.mkdir()
    src = src_dir / "Report .k2s"
    src.write_text("new payload", encoding="utf-8")
    existing = bbs_dir / "Report.k2s"
    existing.write_text("different", encoding="utf-8")

    tab = _tab(bbs_dir)
    row = _row(src)

    assert tab._is_row_already_in_varac_bbs(row) is False
    assert tab._varac_bbs_destination_for_row(row) == existing
    assert tab._varac_bbs_destination_for_row(row, unique=True) == bbs_dir / "Report-2.k2s"
    assert MessageViewerTab._unique_varac_bbs_destination(existing) == bbs_dir / "Report-2.k2s"


def test_remove_from_varac_bbs_deletes_only_copied_artifact(tmp_path: Path) -> None:
    src_dir = tmp_path / "src"
    bbs_dir = tmp_path / "bbs"
    src_dir.mkdir()
    bbs_dir.mkdir()
    src = src_dir / "Report .k2s"
    src.write_text("payload", encoding="utf-8")
    copied = bbs_dir / "Report.k2s"
    copied.write_text("payload", encoding="utf-8")
    os.utime(copied, (src.stat().st_atime, src.stat().st_mtime))

    tab = _tab(bbs_dir)
    row = _row(src)
    tab._varac_bbs_existing_copy_targets = lambda _row: [{"id": "live", "copied_path": copied}]

    tab._remove_row_from_varac_bbs(row, confirm=False)

    assert src.exists()
    assert not copied.exists()
    assert tab._is_row_bbs_copy_action_enabled(row) is True


def test_safe_varac_bbs_filename_preserves_signature_pairing_shape() -> None:
    assert MessageViewerTab._safe_varac_bbs_filename("Report .k2s") == "Report.k2s"
    assert MessageViewerTab._safe_varac_bbs_filename("Report .k2s.sig") == "Report.k2s.sig"
    assert MessageViewerTab._safe_varac_bbs_filename("Report .b2s") == "Report.b2s"
    assert MessageViewerTab._safe_varac_bbs_filename("Report .b2s.sig") == "Report.b2s.sig"


def test_projected_file_refs_convert_to_file_records(tmp_path: Path) -> None:
    message_path = tmp_path / "incoming.k2s"
    message_path.write_text("payload", encoding="utf-8")

    rec = MessageViewerTab._file_record_from_projected_refs(
        (
            {
                "external_kind": "flamp_file",
                "external_path": str(message_path),
                "external_mtime": message_path.stat().st_mtime,
                "external_size": message_path.stat().st_size,
            },
        ),
        origin="flamp",
    )

    assert isinstance(rec, FileRecord)
    assert rec.path == message_path
    assert rec.origin == "flamp"
    assert rec.size == message_path.stat().st_size


def test_projected_artifacts_convert_to_file_records(tmp_path: Path) -> None:
    artifact_path = tmp_path / "transfer"
    artifact_path.write_text("blocks", encoding="utf-8")

    rec = MessageViewerTab._file_record_from_projected_artifacts(
        (SimpleNamespace(path=str(artifact_path)),),
        origin="flamp",
    )

    assert isinstance(rec, FileRecord)
    assert rec.path == artifact_path
    assert rec.origin == "flamp"
    assert rec.size == artifact_path.stat().st_size


def test_projected_flmsg_file_is_eligible_for_bbs_publish(tmp_path: Path) -> None:
    source_dir = tmp_path / "source"
    bbs_dir = tmp_path / "bbs"
    source_dir.mkdir()
    bbs_dir.mkdir()
    message_path = source_dir / "incoming.k2s"
    message_path.write_text("payload", encoding="utf-8")
    stat = message_path.stat()
    payload = ProjectedMessagePayload(
        message_id="message-1",
        canonical_key="file:message-1",
        source_family="flmsg",
        message_type="FLMSG",
        external_refs=(
            {
                "external_kind": "flmsg_file",
                "external_path": str(message_path),
                "external_mtime": stat.st_mtime,
                "external_size": stat.st_size,
            },
        ),
    )
    row = SimpleNamespace(msg_type="FLMSG", payload=payload)
    tab = _tab(bbs_dir)

    assert tab._can_copy_row_to_varac_bbs(row) is True
    assert tab._varac_bbs_destination_for_row(row) == bbs_dir / "incoming.k2s"
    assert mvt.MessageActionDelegate._supports_standard_management_actions(
        row,
        projected_file_row=True,
    ) is True


def test_managed_bbs_copy_target_uses_station_catalog_identity(tmp_path: Path) -> None:
    db_path = tmp_path / "freqinout.db"
    managed_dir = tmp_path / "managed" / "Intel"
    managed_dir.mkdir(parents=True)

    with connect_sqlite(db_path) as conn:
        with conn:
            ensure_bbs_library_schema(conn)
            upsert_bbs_location(conn, location_id="intel", name="Intel", source_dir=str(managed_dir))

    tab = SimpleNamespace(
        settings=_MemorySettings(tmp_path, db_path=db_path),
        _bbs_copy_targets_cache=[],
        _bbs_copy_targets_cache_ts=0.0,
    )

    targets = MessageViewerTab._varac_bbs_copy_targets(tab)

    assert len(targets) == 1
    assert targets[0]["id"] == "location:intel"
    assert targets[0]["kind"] == "location"
    assert targets[0]["location_id"] == "intel"
    assert targets[0]["location_name"] == "Intel"
    assert targets[0]["path"] == managed_dir
    assert targets[0]["valid"] is True
    assert managed_dir.is_dir()


def test_copy_row_to_varac_bbs_atomically_replaces_station_memberships(
    monkeypatch,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "freqinout.db"
    source_dir = tmp_path / "src"
    source_dir.mkdir()
    source = source_dir / "Report.k2s"
    source.write_text("payload", encoding="utf-8")

    with connect_sqlite(db_path) as conn:
        with conn:
            ensure_bbs_library_schema(conn)
            upsert_bbs_location(conn, location_id="intel", name="Intel", source_dir=str(tmp_path / "managed" / "Intel"))
            upsert_bbs_location(conn, location_id="weather", name="Weather", source_dir=str(tmp_path / "managed" / "Weather"))
            artifact_id = upsert_bbs_artifact_path(conn, source_path=source, display_name=source.name)

    tab = _tab(tmp_path / "bbs")
    tab.settings = _MemorySettings(tmp_path / "bbs", db_path=db_path)

    monkeypatch.setattr(mvt.QMessageBox, "warning", lambda *args, **kwargs: None)
    monkeypatch.setattr(mvt.QMessageBox, "information", lambda *args, **kwargs: None)

    selections = [
        [
            {"location_id": "intel", "label": "Intel"},
            {"location_id": "weather", "label": "Weather"},
        ],
        [],
    ]
    monkeypatch.setattr(tab, "_select_varac_bbs_publish_targets", lambda _row: selections.pop(0))

    row = _row(source)

    tab._copy_row_to_varac_bbs(row)
    with connect_sqlite(db_path) as conn:
        assert list_bbs_artifact_location_ids(conn, artifact_id) == ("intel", "weather")
        rows = conn.execute(
            "SELECT location_id, publish_enabled FROM bbs_location_artifacts WHERE artifact_id=? ORDER BY location_id",
            (artifact_id,),
        ).fetchall()
    assert rows == [("intel", 1), ("weather", 1)]
    assert source.exists()

    tab._copy_row_to_varac_bbs(row)
    with connect_sqlite(db_path) as conn:
        assert list_bbs_artifact_location_ids(conn, artifact_id) == ()
        rows = conn.execute(
            "SELECT location_id, publish_enabled FROM bbs_location_artifacts WHERE artifact_id=? ORDER BY location_id",
            (artifact_id,),
        ).fetchall()
    assert rows == [("intel", 0), ("weather", 0)]
    assert source.exists()
