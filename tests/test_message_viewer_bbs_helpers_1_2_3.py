from pathlib import Path

from freqinout.gui.message_viewer_tab import FileRecord, _FileScanWorker


HELPER_NAMES = (
    "BBS MSG - Type INTEL to open Intel then refresh BBS.txt",
    "BBS_QUEUE_LIST.txt",
    "BBS_BLOCK_LIST_41D6.txt",
)


def _write_bbs_files(bbs_dir: Path) -> None:
    bbs_dir.mkdir()
    for name in HELPER_NAMES:
        (bbs_dir / name).write_text("helper", encoding="utf-8")
    (bbs_dir / "NATL-RR-260504-1430Z-AIB-sig.k2s").write_text("real", encoding="utf-8")


def test_bbs_helper_files_are_hidden_from_messages_full_scan(tmp_path: Path) -> None:
    bbs_dir = tmp_path / "BBS"
    _write_bbs_files(bbs_dir)

    worker = _FileScanWorker([{"origin": "bbs", "path": str(bbs_dir)}], force=True)
    records, _dir_mtimes = worker._run_full()

    assert [rec.path.name for rec in records["bbs"]] == ["NATL-RR-260504-1430Z-AIB-sig.k2s"]


def test_bbs_helper_files_are_hidden_from_messages_incremental_scan(tmp_path: Path) -> None:
    bbs_dir = tmp_path / "BBS"
    _write_bbs_files(bbs_dir)
    existing = bbs_dir / "NATL-RR-260504-1430Z-AIB-sig.k2s"

    worker = _FileScanWorker(
        [{"origin": "bbs", "path": str(bbs_dir)}],
        force=False,
        base_records={"bbs": [FileRecord(existing, "bbs", existing.stat().st_size, existing.stat().st_mtime)]},
        base_dir_mtimes={str(bbs_dir): 0.0},
    )
    records, _dir_mtimes = worker._run_incremental()

    assert [rec.path.name for rec in records["bbs"]] == ["NATL-RR-260504-1430Z-AIB-sig.k2s"]
