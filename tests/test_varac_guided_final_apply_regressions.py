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
    host._complete_add_device_profile_in_transaction = lambda *_args, **_kwargs: (
        state.update(written=True) or True
    )
    host._complete_varac_native_session = lambda _session: None
    host.multi_radio_store = SimpleNamespace(guided_save_transaction=transaction)
    return host, state


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
