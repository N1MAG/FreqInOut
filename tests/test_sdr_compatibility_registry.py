from __future__ import annotations

from pathlib import Path

import pytest

from freqinout.core.receiver_control import ManualReceiverControl, ReceiverIdentity
from freqinout.core.sdr_compatibility import (
    FIO_IMPLEMENTATION_STATUSES,
    MAX_COMPATIBILITY_RECORDS,
    OPERATOR_STATES,
    SDR_COMPATIBILITY_REGISTRY,
    SDR_COMPATIBILITY_SCHEMA_VERSION,
    SdrCompatibilityEntry,
    SdrCompatibilityRegistry,
    classify_receiver_state,
)


def test_registry_is_versioned_bounded_and_deduplicated() -> None:
    entries = SDR_COMPATIBILITY_REGISTRY.entries()
    assert 0 < len(entries) <= MAX_COMPATIBILITY_RECORDS
    assert SDR_COMPATIBILITY_REGISTRY.schema_version == SDR_COMPATIBILITY_SCHEMA_VERSION
    assert len({entry.key for entry in entries}) == len(entries)
    assert all(entry.manual_available for entry in entries)
    assert all(entry.fio_status in FIO_IMPLEMENTATION_STATUSES for entry in entries)
    assert all(entry.as_dict()["schema_version"] == SDR_COMPATIBILITY_SCHEMA_VERSION for entry in entries)


def test_rtl_sdr_is_explicitly_cataloged_without_false_fio_verification() -> None:
    rtl = SDR_COMPATIBILITY_REGISTRY.for_hardware("rtl-sdr")
    assert {entry.application for entry in rtl} >= {"SDR++", "SDRangel", "Gqrx"}
    assert all(entry.fio_status != "Verified" for entry in rtl)
    assert all("unsupported" not in entry.notes.casefold() for entry in rtl)
    assert any("frequency" in entry.capabilities for entry in rtl)


def test_registry_queries_do_not_perform_discovery_and_manual_path_is_available() -> None:
    registry = SDR_COMPATIBILITY_REGISTRY
    assert registry.find("missing") is None
    assert registry.for_application("SDR++")[0].hardware_family == "RTL-SDR"
    assert registry.manual_entries()
    assert registry.verified_entries() == ()


def test_registry_rejects_duplicate_keys_and_unbounded_catalog() -> None:
    entry = SdrCompatibilityEntry(
        key="one",
        hardware_family="Test",
        application="Test app",
        api="None",
        upstream_support="manual",
        fio_status="Manual",
    )
    with pytest.raises(ValueError, match="keys must be unique"):
        SdrCompatibilityRegistry((entry, entry))
    with pytest.raises(ValueError, match="bounded size"):
        SdrCompatibilityRegistry(tuple(entry for _ in range(MAX_COMPATIBILITY_RECORDS + 1)))


@pytest.mark.parametrize(
    ("kwargs", "expected"),
    (
        ({"manual_only": True, "api_reachable": False}, "Manual tuning"),
        ({"configured": False, "api_reachable": False}, "Manual tuning"),
        ({"api_reachable": False}, "Receiver unavailable"),
        ({"api_reachable": True}, "Connected; verify tuning"),
        ({"api_reachable": True, "capability_verified": True, "hardware_verified": True}, "FIO tuning ready"),
    ),
)
def test_operator_state_language_is_truthful(kwargs: dict[str, bool], expected: str) -> None:
    assert classify_receiver_state(**kwargs) == expected
    assert expected in OPERATOR_STATES
    assert expected not in {"Supported", "Unsupported"}


def test_manual_receiver_remains_selectable_without_an_api() -> None:
    identity = ReceiverIdentity(
        adapter_id="manual",
        receiver_id="profile-7:rtl-sdr",
        display_name="RTL-SDR desk receiver",
        application_name="Other / manual",
        hardware_family="RTL-SDR",
    )
    receiver = ManualReceiverControl(identity, initial_frequency_hz=7_100_000, initial_mode="USB")
    assert receiver.list_targets(deadline=0.0, cancel=lambda: False) == (identity,)
    state = receiver.read_state(identity, deadline=0.0, cancel=lambda: False)
    assert state.manual is True
    assert state.available is True
    assert state.verified is False
    assert state.frequency_hz == 7_100_000


def test_sdr0_contract_documents_responsive_theme_keyboard_and_hint_guards() -> None:
    plan = Path("docs/internal/sdr_receiver_control_implementation_plan.md").read_text(encoding="utf-8")
    contract = Path("docs/internal/multirig_product_ui_contract.md").read_text(encoding="utf-8")
    assert "Responsive" in plan
    assert "theme" in plan.casefold()
    assert "keyboard" in plan.casefold()
    assert "no-real-identity hint" in plan
    assert "UI Hint Neutrality Contract" in contract


def test_registry_does_not_introduce_identity_examples_or_false_support_labels() -> None:
    source = Path("freqinout/core/sdr_compatibility.py").read_text(encoding="utf-8")
    assert "K7" not in source and "W5" not in source
    assert '"Supported"' not in source
    assert '"Unsupported"' not in source
