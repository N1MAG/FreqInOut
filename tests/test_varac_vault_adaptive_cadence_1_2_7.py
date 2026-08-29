import time
from pathlib import Path

class _Settings:
    def __init__(self, values=None):
        self.values = dict(values or {})
        self.set_calls = []

    def get(self, key, default=None):
        return self.values.get(key, default)

    def set(self, key, value):
        self.values[key] = value
        self.set_calls.append((key, value))


def test_publish_location_skips_unchanged_manifest_write(tmp_path):
    from freqinout.core.varac_bbs_vault import VaultLocation, publish_location

    source = tmp_path / "source"
    live = tmp_path / "live"
    root = tmp_path / "vault"
    source.mkdir()
    live.mkdir()
    (source / "notice.txt").write_text("hello", encoding="utf-8")
    location = VaultLocation(id="default", name="Default", source_dir=str(source), enabled=True)

    first = publish_location(location, live_bbs_dir=live, managed_root=root)
    manifest = Path(first.manifest_path)
    first_mtime = manifest.stat().st_mtime_ns
    time.sleep(0.01)
    second = publish_location(location, live_bbs_dir=live, managed_root=root)

    assert first.changed
    assert not second.changed
    assert manifest.stat().st_mtime_ns == first_mtime


def test_runtime_state_persistence_skips_unchanged_values():
    from freqinout.core.varac_bbs_vault import VaultRuntimeState, _persist_runtime_state, vault_runtime_state_to_data

    state = VaultRuntimeState()
    summary = "Managed BBS Library Default"
    settings = _Settings(
        {
            "varac_bbs_vault_runtime_state_v1": vault_runtime_state_to_data(state),
            "varac_bbs_vault_last_summary": summary,
        }
    )

    _persist_runtime_state(settings, state, summary)

    assert settings.set_calls == []


def test_activity_signature_changes_without_consuming_state(tmp_path):
    from freqinout.core.varac_bbs_vault import build_varac_bbs_vault_activity_signature

    live = tmp_path / "BBS"
    source = tmp_path / "Default"
    live.mkdir()
    source.mkdir()
    settings = _Settings(
        {
            "varac_bbs_dir": str(live),
            "varac_bbs_vault_enabled": True,
            "varac_bbs_vault_locations_v1": [
                {"id": "default", "name": "Default", "source_dir": str(source), "enabled": True}
            ],
        }
    )
    first = build_varac_bbs_vault_activity_signature(settings)
    (source / "new.txt").write_text("new", encoding="utf-8")
    second = build_varac_bbs_vault_activity_signature(settings)

    assert first != second
    assert settings.set_calls == []


def test_controller_adaptive_cadence_and_active_session():
    from freqinout.core.background_ingest import BackgroundIngestController
    from freqinout.core.varac_bbs_vault import VaracBbsVaultRunResult

    controller = BackgroundIngestController(_Settings())
    idle = VaracBbsVaultRunResult(True, 0, 0, False, "default", "", "idle")
    controller._on_varac_vault_result([idle])
    assert controller._varac_vault_full_interval_ms == controller._VARAC_VAULT_WARM_IDLE_INTERVAL_MS
    controller._on_varac_vault_result([idle])
    controller._on_varac_vault_result([idle])
    assert controller._varac_vault_full_interval_ms == controller._VARAC_VAULT_IDLE_INTERVAL_MS
    active = VaracBbsVaultRunResult(True, 0, 0, False, "default", "K1ABC", "active", active_session=True)
    controller._on_varac_vault_result([idle, active])
    assert controller._varac_vault_full_interval_ms == controller._VARAC_VAULT_ACTIVE_INTERVAL_MS
    assert controller._varac_vault_no_change_runs == 0
