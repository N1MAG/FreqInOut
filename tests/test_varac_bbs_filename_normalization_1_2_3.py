from __future__ import annotations

import os
from pathlib import Path
from types import MethodType, SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from freqinout.gui import message_viewer_tab as mvt
from freqinout.gui.message_viewer_tab import FileRecord, MessageViewerTab


class _MemorySettings:
    def __init__(self, bbs_dir: Path) -> None:
        self._data = {"varac_bbs_enabled": True, "varac_bbs_dir": str(bbs_dir)}

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
        "_can_copy_row_to_varac_bbs",
        "_bbs_copy_session_key_for_record",
        "_bbs_copy_session_key_for_row",
        "_bbs_copy_session_marker",
        "_varac_bbs_copy_targets",
        "_select_varac_bbs_copy_target",
        "_varac_bbs_destination_for_row",
        "_is_row_already_in_varac_bbs",
        "_is_row_bbs_copy_action_enabled",
        "_mark_row_copied_to_varac_bbs_session",
        "_varac_bbs_existing_copy_targets",
        "_remove_row_from_varac_bbs",
        "_copy_row_to_varac_bbs",
    ):
        setattr(tab, name, MethodType(getattr(MessageViewerTab, name), tab))
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
    assert tab._is_row_bbs_copy_action_enabled(row) is False


def test_copy_to_varac_bbs_uses_unique_name_when_normalized_name_collides(
    monkeypatch,
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

    messages: list[str] = []
    monkeypatch.setattr(mvt.QMessageBox, "warning", lambda *args, **kwargs: messages.append(str(args[2])))
    monkeypatch.setattr(mvt.QMessageBox, "information", lambda *args, **kwargs: messages.append(str(args[2])))

    tab = _tab(bbs_dir)
    row = _row(src)

    assert tab._is_row_already_in_varac_bbs(row) is False
    assert tab._is_row_bbs_copy_action_enabled(row) is True

    tab._copy_row_to_varac_bbs(row)

    copied = bbs_dir / "Report-2.k2s"
    assert copied.exists()
    assert copied.read_text(encoding="utf-8") == "new payload"
    assert tab._is_row_already_in_varac_bbs(row) is True
    assert tab._is_row_bbs_copy_action_enabled(row) is False
    assert any("Filename cleaned from" in msg and "Report .k2s" in msg for msg in messages)
    assert any("Existing BBS filename avoided" in msg and "Report-2.k2s" in msg for msg in messages)


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

    assert tab._is_row_already_in_varac_bbs(row) is True
    tab._remove_row_from_varac_bbs(row, confirm=False)

    assert src.exists()
    assert not copied.exists()
    assert tab._is_row_already_in_varac_bbs(row) is False
    assert tab._is_row_bbs_copy_action_enabled(row) is True


def test_safe_varac_bbs_filename_preserves_signature_pairing_shape() -> None:
    assert MessageViewerTab._safe_varac_bbs_filename("Report .k2s") == "Report.k2s"
    assert MessageViewerTab._safe_varac_bbs_filename("Report .k2s.sig") == "Report.k2s.sig"
    assert MessageViewerTab._safe_varac_bbs_filename("Report .b2s") == "Report.b2s"
    assert MessageViewerTab._safe_varac_bbs_filename("Report .b2s.sig") == "Report.b2s.sig"
