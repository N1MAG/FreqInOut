from __future__ import annotations

import datetime as dt
import os
import sys
from pathlib import Path

from freqinout.core.varac_bbs_vault import (
    DEFAULT_LOCATION_ID,
    VaultLocation,
    VaultRuntimeState,
    apply_unlock_request,
    build_publish_manifest,
    compute_default_managed_root,
    hash_access_code,
    import_live_bbs_to_default_location,
    initialize_managed_root,
    publish_location,
    run_varac_bbs_vault,
    verify_access_code,
)


class _Settings:
    def __init__(self, **values):
        self._data = dict(values)

    def get(self, key, default=None):
        return self._data.get(key, default)

    def set(self, key, value):
        self._data[key] = value


def _event_stamp() -> tuple[str, float]:
    when = dt.datetime.now(dt.timezone.utc)
    return when.strftime("%m/%d/%Y %H:%M:%S"), when.timestamp()


def test_hash_and_verify_access_code_round_trip() -> None:
    payload = hash_access_code("MAGNET-OPS")
    assert payload["access_code_hash"]
    assert verify_access_code(
        "MAGNET-OPS",
        access_code_hash=str(payload["access_code_hash"]),
        access_code_salt=str(payload["access_code_salt"]),
        access_code_iterations=int(payload["access_code_iterations"]),
    )
    assert not verify_access_code(
        "WRONG-CODE",
        access_code_hash=str(payload["access_code_hash"]),
        access_code_salt=str(payload["access_code_salt"]),
        access_code_iterations=int(payload["access_code_iterations"]),
    )


def test_initialize_and_import_live_bbs(tmp_path: Path) -> None:
    live_bbs = tmp_path / "BBS"
    live_bbs.mkdir()
    (live_bbs / "Report.k2s").write_text("payload", encoding="utf-8")
    managed_root = tmp_path / "FIO_BBS_Vault"

    created = initialize_managed_root(managed_root)
    assert Path(created["default"]).exists()

    copied = import_live_bbs_to_default_location(live_bbs, created["default"])
    assert copied == 1
    assert (Path(created["default"]) / "Report.k2s").exists()
    assert compute_default_managed_root(live_bbs).endswith("FIO_BBS_Vault")


def test_publish_location_removes_only_previously_managed_files(tmp_path: Path) -> None:
    live_bbs = tmp_path / "BBS"
    live_bbs.mkdir()
    managed_root = tmp_path / "FIO_BBS_Vault"
    created = initialize_managed_root(managed_root)
    location_a = Path(created["locations"]) / "A"
    location_b = Path(created["locations"]) / "B"
    location_a.mkdir(parents=True, exist_ok=True)
    location_b.mkdir(parents=True, exist_ok=True)
    (location_a / "Alpha.k2s").write_text("alpha", encoding="utf-8")
    (location_b / "Bravo.k2s").write_text("bravo", encoding="utf-8")
    unmanaged = live_bbs / "OperatorDrop.k2s"
    unmanaged.write_text("manual", encoding="utf-8")

    publish_location(
        VaultLocation(id="a", name="A", source_dir=str(location_a)),
        live_bbs_dir=live_bbs,
        managed_root=managed_root,
    )
    assert (live_bbs / "Alpha.k2s").exists()
    assert unmanaged.exists()

    publish_location(
        VaultLocation(id="b", name="B", source_dir=str(location_b)),
        live_bbs_dir=live_bbs,
        managed_root=managed_root,
    )
    assert not (live_bbs / "Alpha.k2s").exists()
    assert (live_bbs / "Bravo.k2s").exists()
    assert unmanaged.exists()


def test_build_publish_manifest_ignores_nested_directories(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "Visible.k2s").write_text("payload", encoding="utf-8")
    nested = source / "Nested"
    nested.mkdir()
    (nested / "Hidden.k2s").write_text("payload", encoding="utf-8")

    manifest, ignored_dirs = build_publish_manifest(source)

    assert len(manifest) == 1
    assert manifest[0].live_name == "Visible.k2s"
    assert ignored_dirs == 1


def test_apply_unlock_request_publishes_matching_location(tmp_path: Path) -> None:
    live_bbs = tmp_path / "BBS"
    live_bbs.mkdir()
    managed_root = tmp_path / "FIO_BBS_Vault"
    created = initialize_managed_root(managed_root)
    default_dir = Path(created["default"])
    restricted_dir = Path(created["locations"]) / "Restricted"
    restricted_dir.mkdir(parents=True, exist_ok=True)
    (default_dir / "Default.k2s").write_text("default", encoding="utf-8")
    (restricted_dir / "Restricted.k2s").write_text("restricted", encoding="utf-8")

    code_payload = hash_access_code("BLUEBELL")
    locations = [
        VaultLocation(id=DEFAULT_LOCATION_ID, name="Default", source_dir=str(default_dir)),
        VaultLocation(
            id="restricted",
            name="Restricted",
            source_dir=str(restricted_dir),
            access_code_hash=str(code_payload["access_code_hash"]),
            access_code_salt=str(code_payload["access_code_salt"]),
            access_code_iterations=int(code_payload["access_code_iterations"]),
        ),
    ]

    result = apply_unlock_request(
        "W8UFO",
        "BLUEBELL",
        locations=locations,
        live_bbs_dir=live_bbs,
        managed_root=managed_root,
        default_location_id=DEFAULT_LOCATION_ID,
        global_allowed_callsigns=["W8UFO"],
        limit_access_enabled=True,
        runtime_state=VaultRuntimeState(),
    )

    assert result.success
    assert result.runtime_state.current_location_id == "restricted"
    assert result.runtime_state.current_session_callsign == "W8UFO"
    assert (live_bbs / "Restricted.k2s").exists()


def test_run_varac_bbs_vault_processes_unlock_and_disconnect(tmp_path: Path) -> None:
    varac_root = tmp_path / "varac"
    varac_root.mkdir()
    live_bbs = tmp_path / "BBS"
    live_bbs.mkdir()
    managed_root = tmp_path / "FIO_BBS_Vault"
    created = initialize_managed_root(managed_root)
    default_dir = Path(created["default"])
    ops_dir = Path(created["locations"]) / "Ops"
    ops_dir.mkdir(parents=True, exist_ok=True)
    (default_dir / "Default.k2s").write_text("default", encoding="utf-8")
    (ops_dir / "Ops.k2s").write_text("ops", encoding="utf-8")
    publish_location(
        VaultLocation(id=DEFAULT_LOCATION_ID, name="Default", source_dir=str(default_dir)),
        live_bbs_dir=live_bbs,
        managed_root=managed_root,
    )

    code_payload = hash_access_code("REDROCK")
    locations = [
        {
            "id": DEFAULT_LOCATION_ID,
            "name": "Default",
            "source_dir": str(default_dir),
            "enabled": True,
            "inherit_global_allowed_callsigns": True,
            "allowed_callsigns": [],
            "access_code_hash": "",
            "access_code_salt": "",
            "access_code_iterations": 310000,
        },
        {
            "id": "ops",
            "name": "Ops",
            "source_dir": str(ops_dir),
            "enabled": True,
            "inherit_global_allowed_callsigns": True,
            "allowed_callsigns": [],
            "access_code_hash": str(code_payload["access_code_hash"]),
            "access_code_salt": str(code_payload["access_code_salt"]),
            "access_code_iterations": int(code_payload["access_code_iterations"]),
        },
    ]

    stamp_text, stamp_ts = _event_stamp()
    log_path = varac_root / "VarAC_traffic.log"
    log_path.write_text(
        (
            f"{stamp_text} - FROM: W8UFO MESSAGE: BBS OPEN REDROCK\n"
            f"{stamp_text} - DISCONNECTED FROM W8UFO\n"
        ),
        encoding="utf-8",
    )
    os.utime(log_path, (stamp_ts, stamp_ts))

    settings = _Settings(
        varac_bbs_vault_enabled=True,
        varac_bbs_dir=str(live_bbs),
        varac_path=str(varac_root),
        varac_bbs_vault_managed_root=str(managed_root),
        varac_bbs_vault_default_location_id=DEFAULT_LOCATION_ID,
        varac_bbs_vault_trigger_mode="Command prefix",
        varac_bbs_vault_return_mode="On disconnect",
        varac_bbs_vault_failed_attempt_limit=3,
        varac_bbs_vault_failed_attempt_window_seconds=900,
        varac_bbs_vault_cooldown_seconds=1800,
        varac_bbs_vault_idle_timeout_seconds=600,
        varac_bbs_vault_locations_v1=locations,
        varac_bbs_vault_runtime_state_v1={},
        varac_bbs_allowed_callsigns="W8UFO",
        varac_bbs_limit_access_enabled=True,
    )

    result = run_varac_bbs_vault(settings)

    assert result.enabled
    assert result.scanned_events >= 2
    state = settings.get("varac_bbs_vault_runtime_state_v1", {})
    assert state.get("current_location_id") == DEFAULT_LOCATION_ID
    assert state.get("current_session_callsign", "") == ""
    assert (live_bbs / "Default.k2s").exists()


def test_settings_tab_persists_managed_vault_configuration(monkeypatch, tmp_path: Path) -> None:
    if sys.platform == "darwin":
        import pytest

        pytest.skip("PySide6 QtWidgets import aborts in this macOS test environment")

    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    live_bbs = tmp_path / "BBS"
    live_bbs.mkdir()
    managed_root = tmp_path / "FIO_BBS_Vault"
    default_dir = managed_root / "locations" / "default"
    default_dir.mkdir(parents=True)

    from PySide6.QtWidgets import QApplication

    from freqinout.core.settings_manager import SettingsManager
    from freqinout.gui.settings_tab import SettingsTab

    app = QApplication.instance() or QApplication([])
    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)
    tab = SettingsTab()
    try:
        tab.varac_bbs_dir_edit.setText(str(live_bbs))
        tab.varac_bbs_vault_enabled_chk_main.setChecked(True)
        tab.varac_bbs_vault_root_edit.setText(str(managed_root))
        tab._set_varac_bbs_vault_locations(
            [
                {
                    "id": DEFAULT_LOCATION_ID,
                    "name": "Default",
                    "source_dir": str(default_dir),
                    "enabled": True,
                    "inherit_global_allowed_callsigns": True,
                    "allowed_callsigns": [],
                    "access_code_hash": "",
                    "access_code_salt": "",
                    "access_code_iterations": 310000,
                }
            ]
        )
        idx = tab.varac_bbs_vault_default_location_combo.findData(DEFAULT_LOCATION_ID)
        if idx >= 0:
            tab.varac_bbs_vault_default_location_combo.setCurrentIndex(idx)
        tab._save_settings(show_message=False)
    finally:
        tab.deleteLater()
        app.processEvents()

    settings = SettingsManager()
    assert settings.get("varac_bbs_vault_enabled") is True
    assert settings.get("varac_bbs_vault_managed_root") == str(managed_root)
    assert settings.get("varac_bbs_vault_default_location_id") == DEFAULT_LOCATION_ID
    stored_locations = settings.get("varac_bbs_vault_locations_v1", [])
    assert isinstance(stored_locations, list)
    assert stored_locations and stored_locations[0]["id"] == DEFAULT_LOCATION_ID
