"""SCA-S3 contract for family-scoped software drafts and saves.

These tests intentionally exercise the small, side-effect-free persistence
boundary.  SettingsTab remains responsible for radio identity checks,
shared-instance disclosure, dirty-state bookkeeping, and invoking the store;
the helper must only decide which values belong to a selected software family.
"""

from __future__ import annotations

import copy

import pytest

from freqinout.core.software_administration_persistence import (
    family_state_keys,
    merge_family_state,
)


class _Feedback:
    def __init__(self) -> None:
        self.text = ""

    def setText(self, value: str) -> None:
        self.text = value


def test_family_key_partitions_are_explicit_and_do_not_cross_software() -> None:
    js8 = set(family_state_keys("js8call"))
    fast = set(family_state_keys("fast_light"))
    varac = set(family_state_keys("varac"))

    assert {"js8_host", "js8_port", "js8_offset_hz", "path_js8call"} <= js8
    assert {"path_flrig", "path_fldigi", "flrig_port", "fldigi_port"} <= fast
    assert {"varac_path", "varac_ini_path", "varac_outbox_dir"} <= varac
    assert js8.isdisjoint(fast)
    assert js8.isdisjoint(varac)
    assert fast.isdisjoint(varac)


def test_merge_changes_only_selected_family_and_preserves_unrelated_state() -> None:
    persisted = {
        "_source_profile_id": 7,
        "js8_host": "127.0.0.1",
        "js8_port": "2442",
        "path_flrig": "/old/flrig",
        "varac_path": "/old/varac",
        "message_paths": {"flmsg": "/old/flmsg", "flamp": "/old/flamp", "varac": "/old/varac-in"},
    }
    staged = {
        "_source_profile_id": 7,
        "js8_host": "192.0.2.10",
        "js8_port": "2443",
        "path_flrig": "/new/flrig",
        "varac_path": "/new/varac",
        "message_paths": {"flmsg": "/new/flmsg", "flamp": "/new/flamp", "varac": "/new/varac-in"},
    }

    result = merge_family_state(persisted, staged, "js8call")

    assert result["js8_host"] == "192.0.2.10"
    assert result["js8_port"] == "2443"
    assert result["path_flrig"] == "/old/flrig"
    assert result["varac_path"] == "/old/varac"
    assert result["message_paths"] == persisted["message_paths"]
    assert result["_source_profile_id"] == 7


def test_nested_message_path_merge_updates_only_selected_origin() -> None:
    persisted = {"message_paths": {"flmsg": "/a", "flamp": "/b", "varac": "/c"}}
    staged = {"message_paths": {"flmsg": "/new-a", "flamp": "/new-b", "varac": "/new-c"}}

    result = merge_family_state(persisted, staged, "fast_light")

    assert result["message_paths"] == {"flmsg": "/new-a", "flamp": "/new-b", "varac": "/c"}


def test_merge_is_non_mutating_and_missing_staged_values_do_not_erase_state() -> None:
    persisted = {"js8_host": "127.0.0.1", "message_paths": {"flamp": "/amp"}}
    staged = {"js8_host": "", "message_paths": {"flamp": ""}}
    persisted_before = copy.deepcopy(persisted)
    staged_before = copy.deepcopy(staged)

    result = merge_family_state(persisted, staged, "js8call")

    assert persisted == persisted_before
    assert staged == staged_before
    assert result is not persisted
    assert result["message_paths"] is not persisted["message_paths"]
    assert result["js8_host"] == ""
    # JS8 does not own the FLAmp path, so it remains intact.
    assert result["message_paths"]["flamp"] == "/amp"


@pytest.mark.parametrize("family", ["", "unknown"])
def test_unknown_family_has_no_keys_instead_of_widening_save_scope(family: str) -> None:
    # The helper may normalize known keys, but an unknown family must never
    # fall back to all fields.  Settings can then surface the invalid context
    # without risking an unrelated save.
    assert family_state_keys(family) == ()
    assert merge_family_state({"js8_host": "old"}, {"js8_host": "new"}, family) == {"js8_host": "old"}


def _settings_save_harness(monkeypatch, *, failure: bool = False):
    from freqinout.gui.settings_tab import SettingsTab

    profile = {"id": 7, "name": "FIO-A", "js8_host": "127.0.0.1", "js8_port": 2442}
    base = {
        "_source_profile_id": 7,
        "js8_host": "127.0.0.1",
        "js8_port": "2442",
        "path_flrig": "/persisted/flrig",
        "path_commstat": "/persisted/commstat",
        "message_paths": {"flmsg": "/persisted/flmsg", "flamp": "/persisted/flamp"},
    }
    draft = copy.deepcopy(base)
    draft.update({"js8_port": "2443", "path_commstat": "/draft/commstat"})
    tab = SettingsTab.__new__(SettingsTab)
    tab.device_profiles = [profile]
    tab._software_radio_drafts = {7: draft}
    tab._software_dirty_families = {(7, "js8call"), (7, "commstat")}
    tab._software_task_editors = {}
    from freqinout.core.software_administration_model import SoftwareAdministrationSnapshot
    tab._software_administration_snapshot = SoftwareAdministrationSnapshot(families=())
    tab.settings_action_feedback_label = _Feedback()
    tab._device_profile_by_id = lambda ident: profile if ident == 7 else None
    tab._software_editor_state = lambda _ident: copy.deepcopy(draft)
    tab._radio_software_state_from_profile = lambda _profile: copy.deepcopy(base)
    tab._confirm_shared_software_save = lambda _family, _ident: True
    tab._runtime_primary_device_profile_id = lambda: None
    tab._refresh_software_administration_snapshot = lambda: None
    captured = []
    if failure:
        tab._save_radio_software_bundle = lambda _profile, _state: (_ for _ in ()).throw(RuntimeError("save failed"))
    else:
        def _save(_profile, state):
            captured.append(copy.deepcopy(state))
            return profile
        tab._save_radio_software_bundle = _save
    monkeypatch.setattr("freqinout.gui.settings_tab.QMessageBox.warning", lambda *args, **kwargs: None)
    return tab, captured


def test_selected_family_save_preserves_other_dirty_family(monkeypatch) -> None:
    tab, captured = _settings_save_harness(monkeypatch)

    tab._save_selected_software_family("js8call", 7)

    assert len(captured) == 1
    assert captured[0]["js8_port"] == "2443"
    assert captured[0]["path_commstat"] == "/persisted/commstat"
    assert tab._software_dirty_families == {(7, "commstat")}
    assert 7 in tab._software_radio_drafts


def test_failed_scoped_save_retains_draft_and_dirty_state(monkeypatch) -> None:
    tab, _captured = _settings_save_harness(monkeypatch, failure=True)
    before = copy.deepcopy(tab._software_radio_drafts)

    tab._save_selected_software_family("js8call", 7)

    assert tab._software_radio_drafts == before
    assert (7, "js8call") in tab._software_dirty_families


def test_deliberate_save_all_merges_dirty_families_once_per_radio(monkeypatch) -> None:
    tab, captured = _settings_save_harness(monkeypatch)

    tab._save_all_software_family_drafts()

    assert len(captured) == 1
    assert captured[0]["js8_port"] == "2443"
    assert captured[0]["path_commstat"] == "/draft/commstat"
    assert tab._software_dirty_families == set()
    assert tab._software_radio_drafts == {}


def test_shared_instance_confirmation_names_other_radios(monkeypatch) -> None:
    from freqinout.core.software_administration_model import build_software_administration_snapshot
    from freqinout.gui.settings_tab import SettingsTab

    snapshot = build_software_administration_snapshot(
        [
            {"id": 7, "name": "FIO-A", "enabled": 1, "use_js8call": 1, "js8_instance_id": 11},
            {"id": 8, "name": "FIO-B", "enabled": 1, "use_js8call": 1, "js8_instance_id": 11},
        ],
        js8_instances=[{"id": 11, "name": "Shared JS8"}],
    )
    tab = SettingsTab.__new__(SettingsTab)
    tab._software_administration_snapshot = snapshot
    seen = {}

    def _question(_parent, title, body, *_args):
        seen.update(title=title, body=body)
        return 16384  # QMessageBox.Yes

    monkeypatch.setattr("freqinout.gui.settings_tab.QMessageBox.question", _question)
    assert tab._confirm_shared_software_save("js8call", 7) is True
    assert "shared JS8Call" in seen["title"]
    assert "FIO-B" in seen["body"]


def test_fast_light_save_does_not_create_unconfigured_js8_or_varac_instances() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    calls = []

    class _Store:
        def save_js8_instance(self, payload):
            calls.append(("js8", payload))
            return {"id": 11}

        def save_fast_light_config(self, payload):
            calls.append(("fast_light", payload))
            return {"id": 12}

        def save_varac_node(self, payload):
            calls.append(("varac", payload))
            return {"id": 13}

        def save_device_profile(self, payload):
            calls.append(("profile", payload))
            return payload

    tab = SettingsTab.__new__(SettingsTab)
    tab.multi_radio_store = _Store()
    tab._profile_display_name = lambda profile: profile["name"]
    tab._radio_software_enabled = lambda _profile, _role: False
    profile = {"id": 7, "name": "FIO-A", "control_backend": "manual"}
    state = {
        "_source_profile_id": 7,
        "js8_host": "127.0.0.1",
        "js8_port": "2442",
        "path_flrig": "/apps/flrig",
        "flrig_port": "12345",
        "fldigi_host": "127.0.0.1",
        "fldigi_port": "7362",
        "message_paths": {},
    }

    tab._save_radio_software_bundle(profile, state)

    assert [name for name, _payload in calls] == ["fast_light", "profile"]


def test_global_settings_save_hook_does_not_commit_software_drafts() -> None:
    from freqinout.gui.settings_tab import SettingsTab

    tab = SettingsTab.__new__(SettingsTab)
    tab._software_dirty_families = {(7, "js8call")}
    tab._software_radio_drafts = {7: {"_source_profile_id": 7, "js8_port": "2443"}}

    assert tab._persist_staged_radio_software_bundles() is True
    assert tab._software_dirty_families == {(7, "js8call")}
    assert tab._software_radio_drafts[7]["js8_port"] == "2443"
