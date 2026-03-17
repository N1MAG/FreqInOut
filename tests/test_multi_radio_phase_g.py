from __future__ import annotations

import os
from collections import Counter

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import QObject, Signal
from PySide6.QtWidgets import QApplication, QLabel

from freqinout.core.multi_radio_store import MultiRadioStore, settings_db_path
from freqinout.core.settings_manager import SettingsManager
from freqinout.core.station_runtime_manager import DeviceRuntimeSnapshot
from freqinout.gui.station_overview_tab import StationOverviewTab


def _snapshot(
    device_profile_id: int,
    name: str,
    *,
    runtime_primary: bool = False,
    deployment_mode: str = "full",
    overall_state: str = "ok",
    shared_ptt_blocked: bool = False,
    shared_ptt_status_text: str = "",
    warning_text: str = "",
    swap_role: str = "",
    swap_summary: str = "",
) -> DeviceRuntimeSnapshot:
    return DeviceRuntimeSnapshot(
        device_profile_id=device_profile_id,
        name=name,
        device_class="tx_rx",
        control_backend="flrig",
        deployment_mode=deployment_mode,
        runtime_active=True,
        runtime_primary=runtime_primary,
        scheduler_owner=runtime_primary,
        endpoint_summary="127.0.0.1:12345",
        ptt_group="A",
        ptt_active=False,
        current_frequency_hz=14_078_000,
        current_frequency_label="14.078 MHz",
        current_band="20M",
        antenna_group="Dipole",
        frontend_group="Main",
        amplifier_group="AL-80",
        assigned_operating_profile_id=1,
        assigned_operating_profile_name="Default Operating Profile",
        assignment_state="active",
        scheduler_enabled=True,
        scheduler_mode="full",
        use_messages=True,
        use_map=True,
        use_background_ingest=True,
        use_launch_control=True,
        use_net_control_tabs=True,
        control_ready=True,
        overall_state=overall_state,
        status_summary="Runtime ready",
        warning_text=warning_text,
        shared_ptt_blocked=shared_ptt_blocked,
        shared_ptt_owner_device_id=1 if shared_ptt_blocked else None,
        shared_ptt_owner_name="Primary" if shared_ptt_blocked else "",
        shared_ptt_status_text=shared_ptt_status_text,
        observer_follow_source_device_id=None,
        observer_follow_source_name="",
        observer_follow_summary="",
        varac_cluster_name="",
        varac_cluster_id="",
        varac_instance_number=None,
        varac_gateway_handler=False,
        varac_gateway_handler_name="",
        varac_cluster_summary="",
        swap_role=swap_role,
        swap_summary=swap_summary,
        service_states={
            "FLRig": {
                "state": overall_state if overall_state in {"ok", "warn", "error"} else "idle",
                "tooltip": "FLRig status",
            }
        },
    )


class _StubRuntimeManager:
    def __init__(self, snapshots: list[DeviceRuntimeSnapshot]) -> None:
        self.snapshots = list(snapshots)
        self.calls = 0

    def get_runtime_snapshots(self, *, force: bool = False) -> list[DeviceRuntimeSnapshot]:
        self.calls += 1
        return list(self.snapshots)


class _StubStationBroker(QObject):
    station_snapshots_ready = Signal(object)
    station_snapshots_failed = Signal(str)

    def __init__(self) -> None:
        super().__init__()
        self.requests: list[dict[str, object]] = []

    def request_station_snapshots(self, *, store: object, settings: object, force: bool = False) -> None:
        self.requests.append({"store": store, "settings": settings, "force": bool(force)})


def test_station_overview_skips_inactive_refresh_and_avoids_rebuild_on_same_snapshot(monkeypatch):
    app = QApplication.instance() or QApplication([])
    manager = _StubRuntimeManager(
        [
            _snapshot(1, "Primary", runtime_primary=True, deployment_mode="minimal"),
            _snapshot(
                2,
                "Field Rig",
                overall_state="warn",
                shared_ptt_blocked=True,
                shared_ptt_status_text="Shared PTT A: blocked by Primary.",
                warning_text="Shared PTT A: blocked by Primary.",
            ),
        ]
    )

    tab = StationOverviewTab()
    rebuild_count = {"value": 0}
    original_rebuild = tab._rebuild_cards

    def _counted_rebuild(snapshots):
        rebuild_count["value"] += 1
        return original_rebuild(snapshots)

    monkeypatch.setattr(tab, "_rebuild_cards", _counted_rebuild)
    try:
        tab.set_runtime_manager(manager)
        assert manager.calls == 1
        assert rebuild_count["value"] == 1
        assert "Primary compatibility device: Primary." in tab.summary_label.text()
        assert "Minimal mode" in tab.alerts_label.text()
        assert "Shared PTT" in tab.alerts_label.text()

        tab.refresh_from_manager()
        assert manager.calls == 1
        assert rebuild_count["value"] == 1

        tab.set_tab_active(True)
        assert manager.calls == 2
        assert rebuild_count["value"] == 1

        tab.refresh_from_manager()
        assert manager.calls == 3
        assert rebuild_count["value"] == 1
    finally:
        tab.deleteLater()
        app.processEvents()


def test_station_overview_uses_async_broker_when_available():
    app = QApplication.instance() or QApplication([])
    manager = _StubRuntimeManager([_snapshot(1, "Primary", runtime_primary=True)])
    manager.store = object()
    manager.settings = SettingsManager()
    broker = _StubStationBroker()

    tab = StationOverviewTab()
    try:
        tab.set_status_broker(broker)
        tab.set_runtime_manager(manager)
        assert manager.calls == 0
        assert broker.requests == []

        tab.set_tab_active(True)
        assert manager.calls == 0
        assert len(broker.requests) == 1
        assert broker.requests[0]["force"] is True
    finally:
        tab.deleteLater()
        app.processEvents()


def test_station_overview_deduplicates_js8_service_labels():
    app = QApplication.instance() or QApplication([])
    tab = StationOverviewTab()
    try:
        snapshot = _snapshot(1, "JS8 Rig", runtime_primary=True)
        snapshot = DeviceRuntimeSnapshot(
            **{
                **snapshot.__dict__,
                "control_backend": "js8call",
                "endpoint_summary": "JS8Call 127.0.0.1:2442",
                "service_states": {
                    "JS8Call_API": {"state": "warn", "tooltip": "API unreachable"},
                    "JS8Call": {"state": "ok", "tooltip": "Process detected"},
                    "FLRig": {"state": "idle", "tooltip": "FLRig idle"},
                },
            }
        )

        card = tab._build_runtime_card(snapshot, {"success": "#0a0", "warning": "#aa0", "danger": "#a00", "surface_alt": "#333", "text_on_accent": "#fff", "text": "#ddd", "border": "#555", "surface": "#111", "info": "#09f", "accent": "#c90"})
        labels = [label.text() for label in card.findChildren(QLabel)]

        assert sum(1 for text in labels if text == "JS8Call") == 1
        meta = next(text for text in labels if text.startswith("Class:"))
        assert "Control: JS8Call 127.0.0.1:2442" in meta
        assert "JS8CALL | JS8Call" not in meta
    finally:
        tab.deleteLater()
        app.processEvents()


def test_settings_station_summary_and_batched_multi_radio_refresh(monkeypatch, tmp_path):
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    app = QApplication.instance() or QApplication([])

    SettingsManager()
    store = MultiRadioStore(settings_db_path())
    primary = store.get_runtime_primary_device_profile()
    assert primary is not None

    primary = store.save_device_profile(
        {
            "id": primary["id"],
            "name": primary["name"],
            "varac_install_path": "C:/VarAC/Main",
            "varac_db_path": "C:/VarAC/Main/VarAC.db",
            "varac_ini_path": "C:/VarAC/Main/VarAC.ini",
        }
    )
    secondary = store.save_device_profile(
        {
            "name": "Backup Rig",
            "control_backend": "flrig",
            "flrig_host": "127.0.0.2",
            "flrig_port": 22345,
            "varac_install_path": "C:/VarAC/Backup",
            "varac_db_path": "C:/VarAC/Backup/VarAC.db",
            "varac_ini_path": "C:/VarAC/Backup/VarAC.ini",
        }
    )
    store.set_device_profile_runtime_active(int(secondary["id"]), True)

    cluster = store.save_varac_cluster(
        {
            "name": "Ops Cluster",
            "cluster_id": "OPS-A",
            "shared_db_path": str(tmp_path / "shared" / "VarAC.db"),
        }
    )
    store.set_varac_cluster_member(int(cluster["id"]), int(primary["id"]), instance_number=1, enabled=True)
    store.set_varac_cluster_member(int(cluster["id"]), int(secondary["id"]), instance_number=2, enabled=True)

    from freqinout.gui.settings_tab import SettingsTab

    monkeypatch.setattr(SettingsTab, "_maybe_backfill_js8_geo", lambda self: None)
    monkeypatch.setattr(SettingsTab, "_refresh_running_status", lambda self: None)

    tab = SettingsTab()
    try:
        assert "2 active device profiles" in tab.multi_radio_summary_label.text()
        assert "enabled VarAC members" in tab.multi_radio_summary_label.text()
        assert "gateway-handler selection" in tab.multi_radio_attention_label.text()

        counts: Counter[str] = Counter()
        method_names = [
            "list_device_profiles",
            "list_operating_profiles",
            "list_effective_assignments",
            "list_varac_clusters",
            "list_varac_cluster_members",
        ]
        for method_name in method_names:
            original = getattr(tab.multi_radio_store, method_name)

            def _wrapped(*args, _method_name=method_name, _original=original, **kwargs):
                counts[_method_name] += 1
                return _original(*args, **kwargs)

            monkeypatch.setattr(tab.multi_radio_store, method_name, _wrapped)

        tab._refresh_multi_radio_tables()

        assert counts == Counter(
            {
                "list_device_profiles": 1,
                "list_operating_profiles": 1,
                "list_effective_assignments": 1,
                "list_varac_clusters": 1,
                "list_varac_cluster_members": 1,
            }
        )
    finally:
        tab.deleteLater()
        app.processEvents()
