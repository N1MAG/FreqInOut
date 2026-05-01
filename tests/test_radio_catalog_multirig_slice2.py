from __future__ import annotations

import os
import subprocess

from freqinout.core.multi_radio_store import MultiRadioStore, settings_db_path
from freqinout.core.radio_catalog import catalog_entry_control_methods, find_radio_catalog_entry, load_radio_catalog
from freqinout.core.settings_manager import SettingsManager


def test_load_radio_catalog_prefers_local_rigctl_output(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    sample_output = """
 Rig #  Mfg                    Model                   Version         Status      Macro
 1035   Yaesu                  FT-991                  20241118.18     Stable      RIG_MODEL_FT991
 3073   Icom                   IC-7300                 20241118.0      Stable      RIG_MODEL_IC7300
"""

    def _fake_run(*_args, **_kwargs):
        return subprocess.CompletedProcess(["rigctl", "-l"], 0, stdout=sample_output, stderr="")

    monkeypatch.setattr(subprocess, "run", _fake_run)

    catalog = load_radio_catalog(force_refresh=True)

    assert catalog["source"] == "hamlib-rigctl"
    assert any(entry["display_name"] == "Yaesu FT-991" for entry in catalog["entries"])
    assert any(entry["display_name"] == "Icom IC-7300" for entry in catalog["entries"])


def test_find_radio_catalog_entry_matches_saved_fields() -> None:
    catalog = [
        {
            "catalog_id": "RIG_MODEL_FT991",
            "manufacturer": "Yaesu",
            "model_name": "FT-991",
            "display_name": "Yaesu FT-991",
        }
    ]

    match = find_radio_catalog_entry(
        catalog,
        catalog_id="RIG_MODEL_FT991",
        manufacturer="Yaesu",
        model_name="FT-991",
    )

    assert match is not None
    assert match["display_name"] == "Yaesu FT-991"


def test_catalog_entry_control_methods_prefers_backend_support() -> None:
    entry = {
        "catalog_id": "RIG_MODEL_FT991",
        "display_name": "Yaesu FT-991",
        "backend_support": ["flrig", "rigctld", "js8call", "manual"],
    }

    methods = catalog_entry_control_methods(entry)

    assert methods == ["flrig", "rigctld", "js8call", "manual"]


def test_load_radio_catalog_uses_cached_payload_until_forced(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    first_output = """
 Rig #  Mfg                    Model                   Version         Status      Macro
 1035   Yaesu                  FT-991                  20241118.18     Stable      RIG_MODEL_FT991
"""
    second_output = """
 Rig #  Mfg                    Model                   Version         Status      Macro
 3073   Icom                   IC-7300                 20241118.0      Stable      RIG_MODEL_IC7300
"""
    calls = {"count": 0}

    def _fake_run(*_args, **_kwargs):
        calls["count"] += 1
        stdout = first_output if calls["count"] == 1 else second_output
        return subprocess.CompletedProcess(["rigctl", "-l"], 0, stdout=stdout, stderr="")

    monkeypatch.setattr(subprocess, "run", _fake_run)

    first_catalog = load_radio_catalog(force_refresh=True)
    cached_catalog = load_radio_catalog()
    refreshed_catalog = load_radio_catalog(force_refresh=True)

    assert calls["count"] == 2
    assert any(entry["display_name"] == "Yaesu FT-991" for entry in first_catalog["entries"])
    assert any(entry["display_name"] == "Yaesu FT-991" for entry in cached_catalog["entries"])
    assert any(entry["display_name"] == "Icom IC-7300" for entry in refreshed_catalog["entries"])


def test_device_profile_persists_radio_identity_fields(monkeypatch, tmp_path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    saved = store.save_device_profile(
        {
            "name": "Field JS8",
            "control_backend": "js8call",
            "radio_catalog_id": "RIG_MODEL_IC7300",
            "radio_manufacturer": "Icom",
            "radio_model": "IC-7300",
        }
    )

    assert saved["radio_catalog_id"] == "RIG_MODEL_IC7300"
    assert saved["radio_manufacturer"] == "Icom"
    assert saved["radio_model"] == "IC-7300"

    rows = store.list_device_profiles()
    matched = next(row for row in rows if int(row.get("id", 0) or 0) == int(saved["id"]))
    assert matched["radio_catalog_id"] == "RIG_MODEL_IC7300"
    assert matched["radio_manufacturer"] == "Icom"
    assert matched["radio_model"] == "IC-7300"
