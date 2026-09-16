"""Correlation-only coverage for MainWindow receiver qualification callbacks."""

from __future__ import annotations

from types import SimpleNamespace

from freqinout.core.receiver_qualification_service import QUALIFICATION_REQUEST_ID_FIELD
from freqinout.core.scheduler_coordination import EndpointKey, EndpointResult
from freqinout.core.scheduler_endpoint_lane import LaneSubmission
from freqinout.gui.main_window import MainWindow


class _CompletionSignal:
    def __init__(self) -> None:
        self.values: list[object] = []

    def emit(self, value: object) -> None:
        self.values.append(value)


class _Coordinator:
    def __init__(self) -> None:
        self.requests: list[dict[str, object]] = []

    def request(self, profile, completion, *, qualification_request_id):
        del completion
        self.requests.append(
            {
                "profile": dict(profile),
                "qualification_request_id": qualification_request_id,
            }
        )
        return LaneSubmission(_endpoint(), 1, "started", True)


def _endpoint() -> EndpointKey:
    return EndpointKey.network("sdrpp_rigctl", "127.0.0.1", 4532, target="selected-vfo")


def _window_stub() -> SimpleNamespace:
    stub = SimpleNamespace(
        _shutting_down=False,
        _receiver_qualification_profiles={},
        _receiver_qualification_finished=_CompletionSignal(),
        receiver_qualification=_Coordinator(),
        published=[],
    )

    def publish(profile, *, verification_state, detail, verification=None) -> None:
        stub.published.append(
            {
                "profile": dict(profile),
                "verification_state": verification_state,
                "detail": detail,
                "verification": verification,
            }
        )

    stub._publish_receiver_qualification_result = publish
    return stub


def test_main_window_matches_profile_zero_result_by_request_id_and_ignores_duplicates() -> None:
    window = _window_stub()
    draft = {
        "id": 0,
        "name": "RTL-SDR draft",
        QUALIFICATION_REQUEST_ID_FIELD: "draft-rtl-001",
    }

    MainWindow._on_receiver_control_test_requested(window, draft)
    assert len(window.receiver_qualification.requests) == 1
    assert window.receiver_qualification.requests[0]["qualification_request_id"] == "draft-rtl-001"

    result = EndpointResult.create(
        endpoint_key=_endpoint(),
        generation=1,
        status="applied_unverified",
        actual_state={
            "profile_id": 0,
            QUALIFICATION_REQUEST_ID_FIELD: "draft-rtl-001",
            "verification_state": "verified",
            "verification": {
                "tune_readback_verified": True,
                "restore_readback_verified": True,
            },
        },
    )
    MainWindow._on_receiver_qualification_finished(window, result)
    assert len(window.published) == 1
    assert window.published[0]["profile"]["id"] == 0
    assert window.published[0]["profile"][QUALIFICATION_REQUEST_ID_FIELD] == "draft-rtl-001"

    # Endpoint callbacks may be delivered more than once while shutdown or a
    # lane handoff is occurring.  The pending request is consumed exactly once.
    MainWindow._on_receiver_qualification_finished(window, result)
    assert len(window.published) == 1


def test_main_window_generates_request_id_for_saved_profile_and_drops_stale_result() -> None:
    window = _window_stub()
    saved = {"id": 91, "name": "RTL-SDR saved"}

    MainWindow._on_receiver_control_test_requested(window, saved)
    request = window.receiver_qualification.requests[0]
    request_id = str(request["qualification_request_id"])
    assert request_id
    assert request["profile"][QUALIFICATION_REQUEST_ID_FIELD] == request_id

    stale = EndpointResult.create(
        endpoint_key=_endpoint(),
        generation=1,
        status="failed",
        actual_state={
            "profile_id": 91,
            QUALIFICATION_REQUEST_ID_FIELD: "an-earlier-click",
        },
    )
    MainWindow._on_receiver_qualification_finished(window, stale)
    assert window.published == []
    assert request_id in window._receiver_qualification_profiles
