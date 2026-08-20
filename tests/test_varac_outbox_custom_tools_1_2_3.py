from __future__ import annotations

from pathlib import Path

from freqinout.core.software_path_detector import SoftwarePathDetector


ROOT = Path(__file__).resolve().parents[1]


class _DummySettings:
    def __init__(self, data: dict | None = None) -> None:
        self._data = dict(data or {})

    def get(self, key: str, default=None):
        return self._data.get(key, default)

    def set(self, key: str, value) -> None:
        self._data[key] = value

    def set_many(self, batch, save: bool = True) -> None:
        self._data.update(dict(batch or {}))


def _read(rel_path: str) -> str:
    return (ROOT / rel_path).read_text(encoding="utf-8-sig")


def test_software_path_detector_detects_varac_outbox_dir(tmp_path: Path) -> None:
    install_dir = tmp_path / "VarAC"
    outbox = install_dir / "Outgoing Files"
    outbox.mkdir(parents=True)

    detector = SoftwarePathDetector(_DummySettings())
    result = detector._detect_varac_outbox_dir(install_dir)

    assert result.key == "varac_outbox_dir"
    assert result.path == str(outbox)


def test_software_path_detector_detects_varac_outbox_from_ini(tmp_path: Path) -> None:
    install_dir = tmp_path / "VarAC"
    outbox = tmp_path / "VaraFiles" / "Outgoing"
    install_dir.mkdir(parents=True)
    outbox.mkdir(parents=True)
    (install_dir / "VarAC.ini").write_text(
        "[FILE_TRANSFER]\n"
        f"OutgoingFilesDir={outbox}\n",
        encoding="utf-8",
    )

    detector = SoftwarePathDetector(_DummySettings())
    result = detector._detect_varac_outbox_dir(install_dir)

    assert result.key == "varac_outbox_dir"
    assert result.path == str(outbox)


def test_software_path_detector_detects_varac_db_file(tmp_path: Path) -> None:
    install_dir = tmp_path / "VarAC"
    db_path = install_dir / "VarAC.db"
    db_path.parent.mkdir(parents=True)
    db_path.write_bytes(b"")

    detector = SoftwarePathDetector(_DummySettings())
    result = detector._detect_varac_db_file(install_dir)

    assert result.key == "varac_db_path"
    assert result.path == str(db_path)
    assert result.target_type == "file"


def test_software_path_detector_uses_radio_apps_base_folder_for_varac(tmp_path: Path) -> None:
    base = tmp_path / "RadioTools" / "Programs"
    install_dir = base / "VarAC_files"
    db_path = install_dir / "VarAC.db"
    db_path.parent.mkdir(parents=True)
    db_path.write_bytes(b"")

    detector = SoftwarePathDetector(_DummySettings({"radio_apps_base_folder": str(base)}))
    result = detector.detect_varac()["varac_path"]

    assert result.path == str(install_dir)
    assert result.confidence == "verified"


def test_software_path_detector_uses_radio_apps_base_folder_for_external_tools(tmp_path: Path) -> None:
    base = tmp_path / "RadioTools" / "Programs"
    spotter = base / "JS8Spotter" / "JS8Spotter"
    commstat = base / "CommStat" / "CommStat"
    spotter.parent.mkdir(parents=True)
    commstat.parent.mkdir(parents=True)
    spotter.write_text("#!/bin/sh\n", encoding="utf-8")
    commstat.write_text("#!/bin/sh\n", encoding="utf-8")

    detector = SoftwarePathDetector(_DummySettings({"radio_apps_base_folder": str(base)}))
    results = detector.detect_js8()

    assert results["path_js8spotter"].path == str(spotter)
    assert results["path_js8spotter"].reason == "Found from Radio Apps Base Folder"
    assert results["path_commstat"].path == str(commstat)


def test_software_path_detector_uses_radio_apps_base_folder_for_spotter_forms(tmp_path: Path) -> None:
    base = tmp_path / "RadioTools" / "Programs"
    forms_dir = base / "JS8Spotter" / "forms"
    forms_dir.mkdir(parents=True)
    (forms_dir / "MCF307.txt").write_text("Wildfire", encoding="utf-8")

    detector = SoftwarePathDetector(_DummySettings({"radio_apps_base_folder": str(base)}))
    result = detector.detect_js8()["js8_forms_path"]

    assert result.path == str(forms_dir)
    assert result.reason == "Found forms directory containing MCF*.txt files"


def test_software_path_detector_uses_radio_apps_base_folder_for_js8call_variants(tmp_path: Path) -> None:
    base = tmp_path / "RadioTools" / "Programs"
    subspace_bundle = base / "Subspace-Edition" / "build-trimode-baseline" / "JS8Call.app"
    executable = subspace_bundle / "Contents" / "MacOS" / "JS8Call"
    executable.parent.mkdir(parents=True)
    executable.write_text("#!/bin/sh\n", encoding="utf-8")

    detector = SoftwarePathDetector(_DummySettings({"radio_apps_base_folder": str(base)}))
    detector.system = "Darwin"
    result = detector.detect_js8()["path_js8call"]

    assert result.path == str(subspace_bundle)
    assert result.target_type == "app_bundle"
    assert result.reason == "Found from Radio Apps Base Folder"


def test_settings_and_compose_source_include_outbox_and_custom_tools() -> None:
    settings_text = _read("freqinout/gui/settings_tab.py")
    messages_text = _read("freqinout/gui/message_viewer_tab.py")
    launch_text = _read("freqinout/core/launch_orchestrator.py")

    assert 'QLabel("VarAC Outbox Directory")' in settings_text
    assert 'self.varac_outbox_dir_edit = QLineEdit()' in settings_text
    assert "self.varac_outbox_dir_edit.setText(fn)" in settings_text
    assert 'data["varac_outbox_dir"] = (' in settings_text
    assert 'custom_tools_group = QGroupBox("Custom Tools")' in settings_text
    assert 'self.custom_tools_table = QTableWidget(0, 2)' in settings_text
    assert 'data["custom_tool_items"] = [dict(item) for item in self._custom_tool_items_cache]' in settings_text
    assert "def normalize_custom_tools(raw_items: Any) -> List[Dict[str, str]]:" in launch_text
    assert 'return self.normalize_custom_tools(self.settings.get("custom_tool_items", []))' in launch_text
    assert 'return self._finalize_launch_command(name, cmd), "configured custom tool"' in launch_text
    assert 'configured = self._compose_profile_text(profile, "varac_outbox_dir")' in messages_text
    assert 'or str(self.settings.get("varac_outbox_dir", "") or "").strip()' in messages_text
