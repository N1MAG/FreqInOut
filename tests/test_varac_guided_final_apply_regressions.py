from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

from freqinout.gui.settings_tab import SettingsTab


def _host(*, current: bool):
    state = {"written": False, "reviewed": None, "retry": None}

    @contextmanager
    def transaction():
        yield SimpleNamespace(complete=lambda: None)

    host = SimpleNamespace()
    host._varac_native_session_from_guided_profile = SettingsTab._varac_native_session_from_guided_profile
    host._current_guided_instance_inventory = lambda **_kwargs: SimpleNamespace(
        fingerprint=(
            "durable-review-fingerprint" if current else "changed-durable-inventory"
        )
    )
    def review_is_current(payload):
        state["reviewed"] = payload
        return SettingsTab._guided_radio_review_is_current(host, payload)

    host._guided_radio_review_is_current = review_is_current
    host._rollback_guided_native_config = lambda _result: None
    host._rollback_varac_native_session = lambda _session: None
    host._present_guided_stale_review_recovery = lambda _detail: "review"
    host._add_device_profile = lambda **kwargs: state.update(retry=kwargs)
    host._refresh_multi_radio_tables = lambda: None
    host._emit_device_profiles_changed = lambda: None
    host._set_save_button_state = lambda *_args, **_kwargs: None
    host._settings_dirty = False
    host._complete_add_device_profile_in_transaction = lambda *_args, **_kwargs: (
        state.update(written=True) or True
    )
    host._complete_varac_native_session = lambda _session: None
    host.multi_radio_store = SimpleNamespace(guided_save_transaction=transaction)
    return host, state


def test_guided_transaction_suppresses_provisional_radio_and_identity_refreshes() -> None:
    host = SimpleNamespace(_guided_save_ui_deferred=True)

    # Both methods must return before touching Qt widgets or emitting a public
    # profile change while the canonical transaction is incomplete.
    SettingsTab._refresh_multi_radio_tables(host)
    SettingsTab._emit_device_profiles_changed(host)
    SettingsTab._refresh_runtime_projection_ui(host, refresh_multi_radio=True)


def test_guided_discovery_thread_release_cannot_discard_later_queued_result() -> None:
    events = []
    host = SimpleNamespace(
        _guided_radio_autofill_jobs={17: (object(), object())},
        _guided_radio_autofill_callbacks={
            17: (
                lambda payload: events.append(("finished", payload)),
                lambda detail: events.append(("failed", detail)),
            )
        },
    )

    # Qt does not guarantee that QThread.finished is delivered after the
    # separately queued SettingsTab result relay.
    SettingsTab._on_guided_radio_autofill_released(host, 17)
    assert 17 not in host._guided_radio_autofill_jobs
    assert 17 in host._guided_radio_autofill_callbacks

    SettingsTab._on_guided_radio_autofill_finished(host, 17, {"ready": True})

    assert events == [("finished", {"ready": True})]
    assert 17 not in host._guided_radio_autofill_callbacks


def test_guided_discovery_failure_consumes_callback_once() -> None:
    events = []
    host = SimpleNamespace(
        _guided_radio_autofill_callbacks={
            23: (
                lambda payload: events.append(("finished", payload)),
                lambda detail: events.append(("failed", detail)),
            )
        }
    )

    SettingsTab._on_guided_radio_autofill_failed(host, 23, "scan failed")

    assert events == [("failed", "scan failed")]
    assert 23 not in host._guided_radio_autofill_callbacks


def test_final_native_apply_uses_preapply_review_payload_after_readback_enrichment() -> None:
    reviewed_draft = {"family_key": "varac", "instance_name": "New Radio"}
    reviewed = {
        "guided_inventory_generation": 4,
        "guided_inventory_fingerprint": "durable-review-fingerprint",
        "guided_software_instance_drafts": {"varac": reviewed_draft},
    }
    enriched_draft = {
        **reviewed_draft,
        "observed_fingerprint": "readback-fingerprint",
        "native_management_state": "managed",
        "native_writer_key": "varac:13.2.7:linux-wine:create-member",
    }
    saved = {
        **reviewed,
        "guided_software_instance_drafts": {"varac": enriched_draft},
    }
    host, state = _host(current=True)

    SettingsTab._complete_add_device_profile(
        host,
        saved,
        reviewed_inventory_payload=reviewed,
    )

    assert state["reviewed"] is reviewed
    assert state["written"] is True
    assert saved["guided_software_instance_drafts"]["varac"]["observed_fingerprint"] == "readback-fingerprint"


def test_changed_durable_inventory_before_final_mutation_routes_back_to_review() -> None:
    reviewed = {
        "guided_inventory_generation": 7,
        "guided_inventory_fingerprint": "inventory-before-external-apply",
        "guided_software_instance_drafts": {"varac": {"instance_name": "New Radio"}},
    }
    enriched = {
        **reviewed,
        "guided_software_instance_drafts": {
            "varac": {
                "instance_name": "New Radio",
                "observed_fingerprint": "native-readback",
                "native_management_state": "managed",
            }
        },
    }
    host, state = _host(current=False)

    SettingsTab._complete_add_device_profile(
        host,
        enriched,
        reviewed_inventory_payload=reviewed,
    )

    assert state["reviewed"] is reviewed
    assert state["written"] is False
    assert state["retry"] == {"retry_draft": reviewed, "initial_step": "review"}


def test_add_radio_queues_plan_builder_only_after_guided_transaction_exits() -> None:
    events: list[object] = []

    @contextmanager
    def transaction():
        events.append("enter")
        try:
            yield SimpleNamespace(complete=lambda: events.append("complete"))
        finally:
            events.append("exit")

    host = SimpleNamespace()
    host._varac_native_session_from_guided_profile = SettingsTab._varac_native_session_from_guided_profile
    host._guided_radio_review_is_current = lambda _payload: True
    host._complete_add_device_profile_in_transaction = lambda *_args, **_kwargs: (
        events.append("persist") or True
    )
    host._refresh_multi_radio_tables = lambda: events.append("refresh")
    host._emit_device_profiles_changed = lambda: events.append("emit")
    host._set_save_button_state = lambda *_args, **_kwargs: events.append("save-state")
    host._settings_dirty = False
    host._rollback_guided_native_config = lambda _result: events.append("rollback-native")
    host._rollback_varac_native_session = lambda _session: events.append("rollback-varac")
    host._complete_varac_native_session = lambda _session: events.append("complete-varac")
    host._queue_plan_manager_after_guided_profile_save = (
        lambda profile, **kwargs: events.append(
            ("queue", dict(profile), kwargs.get("schedule_choice"))
        )
    )
    host._last_persisted_device_profile = {"id": 41, "name": "FT-710"}
    host.multi_radio_store = SimpleNamespace(guided_save_transaction=transaction)

    SettingsTab._complete_add_device_profile(
        host,
        {
            "guided_open_plan_manager_after_save": True,
            "guided_schedule_choice": "daily_plus_nets",
        },
    )

    assert events == [
        "enter",
        "persist",
        "complete",
        "exit",
        "refresh",
        "emit",
        "save-state",
        ("queue", {"id": 41, "name": "FT-710"}, "daily_plus_nets"),
    ]


def test_failed_add_radio_save_never_queues_plan_builder() -> None:
    events: list[str] = []

    @contextmanager
    def transaction():
        events.append("enter")
        try:
            yield SimpleNamespace(complete=lambda: events.append("complete"))
        finally:
            events.append("exit")

    host = SimpleNamespace()
    host._varac_native_session_from_guided_profile = SettingsTab._varac_native_session_from_guided_profile
    host._guided_radio_review_is_current = lambda _payload: True
    host._complete_add_device_profile_in_transaction = lambda *_args, **_kwargs: False
    host._refresh_multi_radio_tables = lambda: events.append("refresh")
    host._emit_device_profiles_changed = lambda: events.append("emit")
    host._set_save_button_state = lambda *_args, **_kwargs: events.append("save-state")
    host._settings_dirty = False
    host._last_persisted_device_profile = {"id": 999, "name": "provisional"}
    host._rollback_guided_native_config = lambda _result: None
    host._rollback_varac_native_session = lambda _session: None
    host._queue_plan_manager_after_guided_profile_save = lambda *_args, **_kwargs: events.append("queue")
    host.multi_radio_store = SimpleNamespace(guided_save_transaction=transaction)

    SettingsTab._complete_add_device_profile(
        host,
        {"guided_open_plan_manager_after_save": True},
    )

    assert events == ["enter", "exit", "refresh"]
    assert host._last_persisted_device_profile is None


def test_edit_radio_queues_plan_builder_only_after_guided_transaction_exits() -> None:
    events: list[object] = []

    @contextmanager
    def transaction():
        events.append("enter")
        try:
            yield SimpleNamespace(complete=lambda: events.append("complete"))
        finally:
            events.append("exit")

    host = SimpleNamespace()
    host._varac_native_session_from_guided_profile = SettingsTab._varac_native_session_from_guided_profile
    host._guided_radio_review_is_current = lambda _payload: True
    host._complete_edit_device_profile_in_transaction = lambda *_args, **_kwargs: (
        events.append("persist") or True
    )
    host._refresh_multi_radio_tables = lambda: events.append("refresh")
    host._emit_device_profiles_changed = lambda: events.append("emit")
    host._set_save_button_state = lambda *_args, **_kwargs: events.append("save-state")
    host._settings_dirty = False
    host._rollback_guided_native_config = lambda _result: events.append("rollback-native")
    host._rollback_varac_native_session = lambda _session: events.append("rollback-varac")
    host._complete_varac_native_session = lambda _session: events.append("complete-varac")
    host._queue_plan_manager_after_guided_profile_save = (
        lambda profile, **kwargs: events.append(
            ("queue", dict(profile), kwargs.get("schedule_choice"))
        )
    )
    host._last_persisted_device_profile = {"id": 42, "name": "FTDX-10"}
    host.multi_radio_store = SimpleNamespace(guided_save_transaction=transaction)

    SettingsTab._complete_edit_device_profile(
        host,
        {"id": 42},
        {
            "guided_open_plan_manager_after_save": True,
            "guided_schedule_choice": "daily_plus_nets",
        },
    )

    assert events == [
        "enter",
        "persist",
        "complete",
        "exit",
        "refresh",
        "emit",
        "save-state",
        ("queue", {"id": 42, "name": "FTDX-10"}, "daily_plus_nets"),
    ]
