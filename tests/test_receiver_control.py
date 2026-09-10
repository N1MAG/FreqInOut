from __future__ import annotations

import inspect

import pytest

from freqinout.core.receiver_control import (
    ManualReceiverControl,
    ReceiverCapabilities,
    ReceiverCommand,
    ReceiverControlClient,
    ReceiverIdentity,
    ReceiverState,
)


def _identity() -> ReceiverIdentity:
    return ReceiverIdentity(
        adapter_id="manual",
        receiver_id="rx-1",
        display_name="Desk Receiver",
        application_name="Other / manual",
    )


def test_manual_receiver_is_runtime_contract_and_has_no_transmit_surface() -> None:
    receiver = ManualReceiverControl(_identity())

    assert isinstance(receiver, ReceiverControlClient)
    protocol_methods = {
        name
        for name, value in ReceiverControlClient.__dict__.items()
        if callable(value) and not name.startswith("_")
    }
    assert protocol_methods == {
        "probe",
        "list_targets",
        "read_state",
        "set_receive_frequency",
        "set_receive_mode",
        "verify_state",
        "close",
    }
    public_methods = {
        name
        for name, value in inspect.getmembers(ManualReceiverControl, predicate=callable)
        if not name.startswith("_")
    }
    assert not {"ptt", "set_ptt", "transmit", "send", "tx"} & public_methods


def test_manual_receiver_returns_manual_state_without_claiming_tune_or_readback() -> None:
    receiver = ManualReceiverControl(_identity(), initial_frequency_hz=7_100_000, initial_mode="USB")
    cancelled = lambda: False

    identity, capabilities = receiver.probe(deadline=1.0, cancel=cancelled)
    assert identity == receiver.identity
    assert capabilities == ReceiverCapabilities(detail="Tune this receiver manually, then confirm the displayed frequency.")
    assert capabilities.manual_only is True
    assert capabilities.can_set_receive_frequency is False
    assert receiver.list_targets(deadline=1.0, cancel=cancelled) == (receiver.identity,)

    state = receiver.set_receive_frequency(receiver.identity, 7_200_000, deadline=1.0, cancel=cancelled)
    assert state.manual is True
    assert state.verified is False
    assert state.frequency_hz == 7_100_000
    assert "did not tune" in state.detail

    verified = receiver.verify_state(
        receiver.identity,
        ReceiverCommand(target_id=receiver.identity.target_id, frequency_hz=7_200_000),
        tolerance_hz=25,
        deadline=1.0,
        cancel=cancelled,
    )
    assert verified.manual is True
    assert verified.verified is False
    assert "cannot be verified" in verified.detail


def test_manual_receiver_is_cancellable_and_close_is_memory_only() -> None:
    receiver = ManualReceiverControl(_identity())

    state = receiver.read_state(receiver.identity, deadline=0.0, cancel=lambda: True)
    assert state.cancelled is True
    assert state.manual is True
    receiver.close()
    assert receiver.list_targets(deadline=0.0, cancel=lambda: False) == ()
    closed = receiver.read_state(receiver.identity, deadline=0.0, cancel=lambda: False)
    assert closed.available is False
    assert closed.manual is True


def test_receiver_value_objects_reject_unsafe_or_ambiguous_values() -> None:
    with pytest.raises(ValueError):
        ReceiverIdentity(adapter_id="", receiver_id="rx", display_name="Receiver")
    with pytest.raises(ValueError):
        ReceiverCommand(target_id="rx", frequency_hz=None)
    with pytest.raises(ValueError):
        ReceiverState(identity=_identity(), manual=True, verified=True)
    with pytest.raises(ValueError):
        ReceiverCapabilities(readback_tolerance_hz=-1)
