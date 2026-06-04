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


def test_settings_and_compose_source_include_outbox_and_custom_tools() -> None:
    settings_text = _read("freqinout/gui/settings_tab.py")
    messages_text = _read("freqinout/gui/message_viewer_tab.py")
    launch_text = _read("freqinout/core/launch_orchestrator.py")

    assert 'QLabel("VarAC Outbox Directory")' in settings_text
    assert 'self.varac_outbox_dir_edit = QLineEdit()' in settings_text
    assert 'self.settings.set("varac_outbox_dir", fn)' in settings_text
    assert 'custom_tools_group = QGroupBox("Custom Tools")' in settings_text
    assert 'self.custom_tools_table = QTableWidget(0, 2)' in settings_text
    assert 'data["custom_tool_items"] = [dict(item) for item in self._custom_tool_items_cache]' in settings_text
    assert "def normalize_custom_tools(raw_items: Any) -> List[Dict[str, str]]:" in launch_text
    assert 'return self.normalize_custom_tools(self.settings.get("custom_tool_items", []))' in launch_text
    assert 'return self._finalize_launch_command(name, cmd), "configured custom tool"' in launch_text
    assert 'configured = str(self.settings.get("varac_outbox_dir", "") or "").strip()' in messages_text
