from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import freqinout.gui.message_viewer_tab as message_viewer
from freqinout.gui.message_viewer_tab import MessageViewerTab


class _Settings:
    def __init__(self, values=None):
        self._values = dict(values or {})

    def get(self, key, default=None):
        return self._values.get(key, default)


def test_default_message_watch_dirs_are_current_operator_and_existing(tmp_path: Path) -> None:
    current_home = tmp_path / "operator"
    messages = current_home / "NBEMS.files" / "ICS" / "messages"
    flamp = current_home / "NBEMS.files" / "FLAMP"
    messages.mkdir(parents=True)
    flamp.mkdir(parents=True)

    discovered = message_viewer._default_message_watch_dirs(home=current_home, system="Windows")

    assert discovered == [
        {"path": str(messages), "origin": "flmsg"},
        {"path": str(flamp), "origin": "flamp"},
    ]
    assert all(str(current_home) in row["path"] for row in discovered)


def test_commstat_brevity_discovery_uses_saved_and_current_operator_roots(monkeypatch, tmp_path: Path) -> None:
    current_home = tmp_path / "operator"
    radio_apps = tmp_path / "Radio Apps"
    configured = tmp_path / "configured" / "CommStat.exe"
    configured.parent.mkdir(parents=True)
    configured.write_text("", encoding="utf-8")
    (radio_apps / "CommStat").mkdir(parents=True)
    monkeypatch.setattr(message_viewer.Path, "home", classmethod(lambda cls: current_home))
    tab = SimpleNamespace(
        settings=_Settings(
            {
                "commstat_app_path": str(configured),
                "radio_apps_base_folder": str(radio_apps),
            }
        )
    )

    paths = MessageViewerTab._compose_commstat_brevity_catalog_dirs(tab)

    assert paths[0] == configured.parent
    assert radio_apps / "CommStat" in paths
    assert current_home / "RadioTools" / "Programs" / "CommStat" in paths
    assert all("/Users/bill/" not in str(path) for path in paths)


def test_custom_form_fallback_uses_current_operator_nbems_root(monkeypatch, tmp_path: Path) -> None:
    nbems_root = tmp_path / "operator" / ".nbems"
    custom = nbems_root / "CUSTOM"
    custom.mkdir(parents=True)
    monkeypatch.setattr(message_viewer, "_operator_nbems_roots", lambda **_kwargs: (nbems_root,))
    tab = SimpleNamespace(
        settings=_Settings({"message_paths": {}}),
        _multi_radio_message_path_entries=lambda: [],
    )

    assert MessageViewerTab._resolve_custom_forms_path(tab) == custom


def test_message_viewer_source_contains_no_named_user_fallbacks() -> None:
    source = Path(message_viewer.__file__).read_text(encoding="utf-8")

    assert r"C:\Users\HP" not in source
    assert r"C:\Users\billd" not in source
    assert "/Users/bill/" not in source
