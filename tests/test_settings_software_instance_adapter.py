"""Focused SettingsTab adapter tests for reviewed software-instance payloads.

The guided assistant itself is intentionally cache-only.  These tests exercise
the Settings-owned bridge that maps its reviewed payload to the existing
multi-radio store, starts bounded discovery, and keeps failed actions
non-mutating.
"""

from __future__ import annotations

from typing import Any

import pytest

from freqinout.gui import settings_tab as settings_module
from freqinout.gui.settings_tab import SettingsTab


class _Workspace:
    def __init__(self) -> None:
        self.discovery_results: list[list[dict[str, Any]]] = []
        self.completed: list[tuple[bool, str]] = []
        self._instance_assistant: object | None = None
        self.family_key = "js8call"

    def selected_family_key(self) -> str:
        return self.family_key

    def set_instance_discovery_results(self, rows: object) -> None:
        self.discovery_results.append([dict(row) for row in rows])

    def complete_instance_add(self, *, success: bool, message: str) -> None:
        self.completed.append((bool(success), str(message)))


class _Store:
    def __init__(self, *, fail: Exception | None = None) -> None:
        self.fail = fail
        self.adoptions: list[dict[str, Any]] = []
        self.disassociations: list[dict[str, Any]] = []
        self.js8_rows = [{"id": 11, "name": "Saved JS8", "profile_path": "/saved/js8.ini"}]
        self.fast_rows = [{"id": 12, "name": "Saved Fast", "flrig_port": 12345}]
        self.varac_rows = [{"id": 13, "name": "Saved VarAC", "ini_path": "/saved/VarAC.ini"}]

    def list_js8_instances(self):
        return list(self.js8_rows)

    def list_fast_light_configs(self):
        return list(self.fast_rows)

    def list_varac_nodes(self):
        return list(self.varac_rows)

    def list_varac_clusters(self):
        return []

    def adopt_software_instance(self, **kwargs):
        self.adoptions.append(kwargs)
        if self.fail is not None:
            raise self.fail
        return {"radio": {"id": int(kwargs["radio_profile_id"]), "name": "FIO-A"}}

    def adopt_observer_js8_instance(self, **kwargs):
        self.adoptions.append({"observer_js8": True, **kwargs})
        if self.fail is not None:
            raise self.fail
        return {"radio": {"id": int(kwargs["radio_profile_id"]), "name": "RTL-SDR"}}

    def disassociate_software_instance(self, **kwargs):
        self.disassociations.append(kwargs)
        if self.fail is not None:
            raise self.fail
        return {"radio": {"id": int(kwargs["radio_profile_id"]), "name": "FIO-A"}}


def _tab(monkeypatch: pytest.MonkeyPatch, *, store: _Store, profile: dict[str, Any] | None = None):
    """Build a no-UI Settings adapter harness with an explicit fake workspace."""

    monkeypatch.setattr(settings_module, "SoftwareAdministrationWorkspace", _Workspace)
    tab = SettingsTab.__new__(SettingsTab)
    tab.multi_radio_store = store
    tab.software_administration_workspace = _Workspace()
    current_profile = profile or {"id": 1, "name": "FIO-A"}
    tab._device_profile_by_id = lambda radio_id: current_profile if int(radio_id) == 1 else None
    tab._profile_display_name = lambda row: str(row.get("name") or "Radio")
    replaced: list[dict[str, Any]] = []
    refreshed: list[bool] = []
    tab._replace_cached_device_profile = lambda row: replaced.append(dict(row))
    tab._refresh_device_profiles_table = lambda: refreshed.append(True)
    return tab, tab.software_administration_workspace, replaced, refreshed, current_profile


@pytest.mark.parametrize(
    ("family", "payload", "expected"),
    [
        (
            "js8call",
            {
                "instance_name": "JS8 North", "host": "127.0.0.1", "port": 2447,
                "application_path": "/apps/JS8Call", "configuration_path": "/cfg/JS8Call - north.ini",
                "storage_path": "/data/JS8Call - north", "rig_name": "north",
            },
            {
                "host": "127.0.0.1", "port": 2447, "profile_path": "/cfg/JS8Call - north.ini",
                "install_path": "/apps/JS8Call", "rig_name": "north",
                "application_data_root": "/data/JS8Call - north",
                "directed_path": "/data/JS8Call - north/DIRECTED.TXT",
                "all_path": "/data/JS8Call - north/ALL.TXT",
                "inbox_path": "/data/JS8Call - north/inbox.db3", "storage_mode": "rig_scoped",
            },
        ),
        (
            "fast_light",
            {
                "instance_name": "Fast North", "host": "192.0.2.20", "port": 12445,
                "secondary_port": 7462, "application_path": "/apps/flrig",
                "secondary_application_path": "/apps/fldigi", "storage_path": "/logs/fldigi",
                "secondary_storage_path": "/checkins",
            },
            {
                "flrig_path": "/apps/flrig", "flrig_host": "192.0.2.20", "flrig_port": 12445,
                "fldigi_path": "/apps/fldigi", "fldigi_host": "192.0.2.20", "fldigi_port": 7462,
                "fldigi_log_path": "/logs/fldigi", "fldigi_checkin_dir": "/checkins",
            },
        ),
        (
            "varac",
            {
                "instance_name": "VarAC North", "application_path": "/apps/VarAC",
                "configuration_path": "/cfg/VarAC.ini", "storage_path": "/data/varac.db",
                "secondary_storage_path": "/incoming", "outbox_path": "/outbox", "launch_command": "/apps/VarAC",
            },
            {
                "install_path": "/apps/VarAC", "ini_path": "/cfg/VarAC.ini", "db_path": "/data/varac.db",
                "incoming_path": "/incoming", "outbox_path": "/outbox", "launch_cmd": "/apps/VarAC",
            },
        ),
    ],
)
def test_settings_adapter_maps_each_family_to_native_store_shape(
    monkeypatch: pytest.MonkeyPatch,
    family: str,
    payload: dict[str, Any],
    expected: dict[str, Any],
) -> None:
    tab, workspace, replaced, refreshed, _profile = _tab(monkeypatch, store=_Store())

    tab._on_software_instance_add_requested({"family_key": family, "radio_id": 1, "mode": "managed", **payload})

    assert len(tab.multi_radio_store.adoptions) == 1
    adoption = tab.multi_radio_store.adoptions[0]
    assert adoption["family_key"] == family
    assert adoption["radio_profile_id"] == 1
    assert adoption["replace_existing"] is False
    assert {key: adoption["application_values"][key] for key in expected} == expected
    assert adoption["manifest_values"]["application_system_key"] == adoption["application_values"]["system_key"]
    assert workspace.completed and workspace.completed[-1][0] is True
    assert replaced == [{"id": 1, "name": "FIO-A"}]
    assert refreshed == [True]


@pytest.mark.parametrize(
    ("family", "section", "inventory_key"),
    [
        ("js8call", "js8", "js8_rows"),
        ("fast_light", "fast_light", "fast_rows"),
        ("varac", "varac", "varac_rows"),
    ],
)
def test_settings_instance_discovery_immediately_shows_cached_inventory_then_queues_family_scan(
    monkeypatch: pytest.MonkeyPatch,
    family: str,
    section: str,
    inventory_key: str,
) -> None:
    store = _Store()
    tab, workspace, _replaced, _refreshed, _profile = _tab(monkeypatch, store=store)
    requested: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
    tab._request_software_autofill = lambda *args, **kwargs: requested.append((args, kwargs))

    assistant = object()
    tab._on_software_instance_discovery_requested({"family_key": family, "assistant": assistant})

    assert len(workspace.discovery_results) == 1
    classified_rows = workspace.discovery_results[0]
    assert len(classified_rows) == len(getattr(store, inventory_key))
    assert classified_rows[0]["id"] == getattr(store, inventory_key)[0]["id"]
    assert classified_rows[0]["name"] == getattr(store, inventory_key)[0]["name"]
    assert classified_rows[0]["family_key"] == family
    assert classified_rows[0]["candidate_classification"] == "diagnostic_only"
    assert classified_rows[0]["candidate_usable"] is False
    assert requested == [
        (
            (section, ()),
            {
                "target": "instance_assistant",
                "family_key": family,
                "assistant_token": id(assistant),
            },
        )
    ]


def test_settings_instance_add_error_reports_failure_without_replacing_cached_profile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _Store(fail=ValueError("JS8 endpoint already registered"))
    tab, workspace, replaced, refreshed, profile = _tab(monkeypatch, store=store)
    before = dict(profile)

    tab._on_software_instance_add_requested(
        {"family_key": "js8call", "radio_id": 1, "instance_name": "JS8 North", "port": 2447}
    )

    assert len(store.adoptions) == 1
    assert workspace.completed == [(False, "JS8 endpoint already registered")]
    assert replaced == []
    assert refreshed == []
    assert profile == before


def test_settings_observer_js8_add_uses_receive_only_adoption_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _Store()
    profile = {
        "id": 1,
        "name": "RTL-SDR",
        "device_class": "observer",
        "control_backend": "manual",
        "js8_instance_id": None,
    }
    tab, workspace, replaced, refreshed, _profile = _tab(
        monkeypatch,
        store=store,
        profile=profile,
    )

    tab._on_software_instance_add_requested(
        {
            "family_key": "js8call",
            "radio_id": 1,
            "instance_name": "RTL-SDR JS8Call",
            "host": "127.0.0.1",
            "port": 2448,
            "storage_path": "/data/rtl-sdr-js8",
            "launch_at_startup": True,
        }
    )

    assert len(store.adoptions) == 1
    adoption = store.adoptions[0]
    assert adoption["observer_js8"] is True
    assert "family_key" not in adoption
    assert adoption["replace_existing"] is False
    assert adoption["expected_current_instance_id"] is None
    assert adoption["launch_at_startup"] is True
    assert workspace.completed[-1][0] is True
    assert replaced == [{"id": 1, "name": "RTL-SDR"}]
    assert refreshed == [True]


def test_instance_discovery_completion_is_bound_to_the_originating_assistant(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tab, workspace, _replaced, _refreshed, _profile = _tab(monkeypatch, store=_Store())
    first = object()
    replacement = object()
    workspace._instance_assistant = first
    tab._software_autofill_generation = 9
    request = {
        "generation": 9,
        "target": "instance_assistant",
        "family_key": "js8call",
        "assistant_token": id(first),
    }

    assert tab._software_autofill_request_is_current(request) is True
    workspace._instance_assistant = replacement
    assert tab._software_autofill_request_is_current(request) is False


def test_settings_instance_replacement_requires_explicit_review_before_store_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _Store()
    profile = {"id": 1, "name": "FIO-A", "js8_instance_id": 17}
    tab, workspace, replaced, refreshed, _profile = _tab(monkeypatch, store=store, profile=profile)
    tab._on_software_instance_add_requested(
        {"family_key": "js8call", "radio_id": 1, "instance_name": "Replacement", "port": 2448}
    )

    assert store.adoptions == []
    assert workspace.completed == [(
        False,
        "This radio already has an instance in the selected software family. "
        "Use Replace instance and review the current and proposed assignments first.",
    )]
    assert replaced == []
    assert refreshed == []


def test_settings_instance_replacement_passes_stale_assignment_guard(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _Store()
    profile = {"id": 1, "name": "FIO-A", "js8_instance_id": 17}
    tab, workspace, replaced, refreshed, _profile = _tab(monkeypatch, store=store, profile=profile)

    tab._on_software_instance_add_requested(
        {
            "family_key": "js8call",
            "radio_id": 1,
            "instance_name": "Replacement",
            "port": 2448,
            "replace_existing": True,
            "replacement_instance_id": 17,
        }
    )

    assert len(store.adoptions) == 1
    assert store.adoptions[0]["replace_existing"] is True
    assert store.adoptions[0]["expected_current_instance_id"] == 17
    assert workspace.completed[-1][0] is True
    assert replaced == [{"id": 1, "name": "FIO-A"}]
    assert refreshed == [True]


def test_settings_disassociate_requires_confirmation_and_preserves_expected_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _Store()
    profile = {"id": 1, "name": "FIO-A", "js8_instance_id": 17}
    tab, _workspace, replaced, refreshed, _profile = _tab(monkeypatch, store=store, profile=profile)
    monkeypatch.setattr(
        settings_module.QMessageBox,
        "question",
        lambda *_args, **_kwargs: settings_module.QMessageBox.Yes,
    )

    tab._on_software_instance_disassociate_requested(
        {
            "family_key": "js8call",
            "family_title": "JS8Call",
            "radio_id": 1,
            "radio_name": "FIO-A",
            "instance_id": 17,
            "instance_name": "JS8 North",
        }
    )

    assert store.disassociations == [{
        "family_key": "js8call",
        "radio_profile_id": 1,
        "expected_current_instance_id": 17,
    }]
    assert replaced == [{"id": 1, "name": "FIO-A"}]
    assert refreshed == [True]


def test_settings_disassociate_cancel_is_non_mutating(monkeypatch: pytest.MonkeyPatch) -> None:
    store = _Store()
    profile = {"id": 1, "name": "FIO-A", "varac_node_id": 23}
    tab, _workspace, replaced, refreshed, _profile = _tab(monkeypatch, store=store, profile=profile)
    monkeypatch.setattr(
        settings_module.QMessageBox,
        "question",
        lambda *_args, **_kwargs: settings_module.QMessageBox.No,
    )

    tab._on_software_instance_disassociate_requested(
        {"family_key": "varac", "radio_id": 1, "instance_id": 23}
    )

    assert store.disassociations == []
    assert replaced == []
    assert refreshed == []
