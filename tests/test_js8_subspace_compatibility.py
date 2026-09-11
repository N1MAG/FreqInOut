from __future__ import annotations

import pytest

from freqinout.core.js8_send_service import js8_speed_name, set_js8_selected_target
from freqinout.core.message_ingest import MessageIngestor, parse_js8_api_utc
from freqinout.core.settings_manager import SettingsManager


def test_parse_js8_api_utc_accepts_subspace_epoch_milliseconds() -> None:
    text, timestamp = parse_js8_api_utc(1769740328005)

    assert text == "2026-01-30 02:32:08"
    assert timestamp == pytest.approx(1769740328.005)


@pytest.mark.parametrize(
    ("value", "expected_text", "expected_timestamp"),
    [
        (1769740328.005, "2026-01-30 02:32:08", 1769740328.005),
        ("2026-01-30 02:32:08", "2026-01-30 02:32:08", 1769740328.0),
    ],
)
def test_parse_js8_api_utc_keeps_seconds_and_legacy_text(
    value: object, expected_text: str, expected_timestamp: float
) -> None:
    text, timestamp = parse_js8_api_utc(value)

    assert text == expected_text
    assert timestamp == pytest.approx(expected_timestamp)


def test_subspace_numeric_utc_is_used_by_all_api_event_parsers(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    settings = SettingsManager()
    ingestor = MessageIngestor(settings)
    event_utc = 1769740328005

    try:
        spotter = ingestor._parse_js8_spotter_event(
            {
                "type": "RX.DIRECTED",
                "value": "K1ABC: @MAGNET F!304 11111111 #HHJL *DE* K1ABC",
                "params": {
                    "FROM": "K1ABC",
                    "TO": "@MAGNET",
                    "TEXT": "K1ABC: @MAGNET F!304 11111111 #HHJL *DE* K1ABC",
                    "UTC": event_utc,
                },
            }
        )
        dynamic = ingestor._parse_dynamic_js8_event(
            {
                "type": "RX.DIRECTED",
                "value": "E? Q 970F",
                "params": {
                    "FROM": "K1ABC",
                    "TO": "N0CALL",
                    "TEXT": "E? Q 970F",
                    "UTC": str(event_utc),
                },
            }
        )
        message = ingestor._parse_directed_js8_message_event(
            {
                "type": "RX.DIRECTED",
                "value": "K1ABC: N0CALL HELLO",
                "params": {
                    "FROM": "K1ABC",
                    "TO": "N0CALL",
                    "TEXT": "K1ABC: N0CALL HELLO",
                    "UTC": event_utc,
                },
            },
            directed_callsigns={"N0CALL"},
            directed_groups=set(),
            source_radio_id="radio-a",
            js8_instance_id="subspace-a",
            source_key="api:radio-a",
        )

        assert spotter is not None
        assert spotter["utc_str"] == "2026-01-30 02:32:08"
        assert spotter["utc_ts"] == pytest.approx(event_utc / 1000)
        assert dynamic is not None
        assert dynamic["utc_ts"] == pytest.approx(event_utc / 1000)
        assert message is not None
        assert message["utc_str"] == "2026-01-30 02:32:08"
        assert message["utc_ts"] == pytest.approx(event_utc / 1000)
        assert message["source_radio_id"] == "radio-a"
        assert message["js8_instance_id"] == "subspace-a"
        assert "api:radio-a" in message["source_key"]
    finally:
        settings.close()


def test_js8_speed_name_covers_upstream_and_subspace_values() -> None:
    assert [js8_speed_name(value) for value in (0, 1, 2, 4, 8, 16)] == [
        "Normal",
        "Fast",
        "Turbo",
        "Slow",
        "Ultra",
        "Subspace",
    ]


def test_selected_call_prefers_upstream_rx_set_call_selected_and_keeps_aliases() -> None:
    class FakeClient:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str, dict[str, str]]] = []

        def send(self, command: str, *, value: str, params: dict[str, str]) -> None:
            self.calls.append((command, value, params))

    client = FakeClient()
    set_js8_selected_target(client, "k1abc", settle_s=0)

    assert [command for command, _value, _params in client.calls] == [
        "RX.SET_CALL_SELECTED",
        "RX.SET_SELECTED_CALL",
        "TX.SET_SELECTED_CALL",
        "STATION.SET_SELECTED_CALL",
    ]
    assert all(value == "K1ABC" for _command, value, _params in client.calls)
