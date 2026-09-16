"""Compose safety coverage for receive-only Observer / SDR profiles."""

from __future__ import annotations

from types import SimpleNamespace

from freqinout.core.js8_send_service import js8_profile_allows_transmit
from freqinout.gui.message_viewer_tab import MessageViewerTab


class _Combo:
    def __init__(self) -> None:
        self.items: list[tuple[str, object]] = []
        self.current = 0

    def currentData(self) -> object:
        return self.items[self.current][1] if self.items else ""

    def blockSignals(self, _blocked: bool) -> None:
        pass

    def clear(self) -> None:
        self.items.clear()

    def addItem(self, label: str, data: object) -> None:
        self.items.append((label, data))

    def findData(self, data: object) -> int:
        return next((index for index, (_label, value) in enumerate(self.items) if value == data), -1)

    def setCurrentIndex(self, index: int) -> None:
        self.current = index

    def count(self) -> int:
        return len(self.items)

    def itemText(self, index: int) -> str:
        return self.items[index][0]

    def currentText(self) -> str:
        return self.items[self.current][0] if self.items else ""

    def setSizeAdjustPolicy(self, _policy: object) -> None:
        pass

    def setMinimumContentsLength(self, _length: int) -> None:
        pass

    def setSizePolicy(self, *_args: object) -> None:
        pass

    class _Font:
        @staticmethod
        def horizontalAdvance(text: str) -> int:
            return len(text)

    def fontMetrics(self) -> _Font:
        return self._Font()

    class _View:
        def setMinimumWidth(self, _width: int) -> None:
            pass

    def view(self) -> _View:
        return self._View()

    def setItemData(self, *_args: object) -> None:
        pass


class _PolicyRadios:
    def __init__(self) -> None:
        self.values: list[str] = []

    def set_completion_values(self, values: list[str]) -> None:
        self.values = values


def test_observer_profile_never_allows_js8_transmit() -> None:
    assert js8_profile_allows_transmit({"device_class": "observer", "use_js8call": 1}) is False
    assert js8_profile_allows_transmit({"device_class": "tx_rx", "use_js8call": 1}) is True


def test_compose_targets_exclude_enabled_observer_with_js8_instance() -> None:
    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab._multi_radio_store = SimpleNamespace(
        list_device_profiles=lambda: [
            {
                "id": 11,
                "name": "Main Radio",
                "device_class": "tx_rx",
                "enabled": 1,
                "use_js8call": 1,
                "js8_instance_id": "main-js8",
            },
            {
                "id": 12,
                "name": "Receive SDR",
                "device_class": "observer",
                "enabled": 1,
                "use_js8call": 1,
                "js8_instance_id": "sdr-js8",
            },
        ]
    )

    targets = tab._load_compose_radio_targets()

    assert [target.radio_id for target in targets] == [11]
    assert targets[0].capabilities == ("JS8Call",)


def test_expect_reply_radio_choices_exclude_observer_js8_instance() -> None:
    from freqinout.gui.fio_spotter_tab import FioSpotterTab

    tab = FioSpotterTab.__new__(FioSpotterTab)
    tab._radio_store_override = SimpleNamespace(
        list_device_profiles=lambda: [
            {"id": 21, "name": "Main Radio", "device_class": "tx_rx", "enabled": 1, "use_js8call": 1},
            {"id": 22, "name": "Receive SDR", "device_class": "observer", "enabled": 1, "use_js8call": 1},
        ]
    )
    tab.expect_radio = _Combo()
    tab.policy_radios = _PolicyRadios()

    tab._refresh_expect_radio_choices()

    assert [data for _label, data in tab.expect_radio.items] == ["", "21"]
    assert tab.policy_radios.values == ["Main Radio"]


def test_js8_net_control_radio_choices_exclude_observer_js8_instance(monkeypatch) -> None:
    from freqinout.gui import js8call_net_control_tab

    class _Store:
        def list_runtime_active_device_profiles(self):
            return [
                {"id": 31, "name": "Main Radio", "device_class": "tx_rx", "use_js8call": 1},
                {"id": 32, "name": "Receive SDR", "device_class": "observer", "use_js8call": 1},
            ]

    monkeypatch.setattr(js8call_net_control_tab, "MultiRadioStore", _Store)
    tab = js8call_net_control_tab.JS8CallNetControlTab.__new__(js8call_net_control_tab.JS8CallNetControlTab)

    profiles = tab._ncs_radio_profiles()

    assert [profile["id"] for profile in profiles] == [31]


def test_inbox_retrieval_rejects_observer_source_provenance(monkeypatch) -> None:
    from freqinout.gui import message_viewer_tab

    class _Store:
        def list_profiles(self, *, enabled_only: bool = False):
            assert enabled_only is False
            return [
                {
                    "id": 41,
                    "js8_instance_id": 401,
                    "device_class": "observer",
                    "use_js8call": 1,
                },
                {
                    "id": 42,
                    "js8_instance_id": 402,
                    "device_class": "tx_rx",
                    "use_js8call": 1,
                },
            ]

    monkeypatch.setattr(message_viewer_tab, "MultiRadioStore", _Store)
    tab = MessageViewerTab.__new__(MessageViewerTab)

    assert tab._pending_js8_source_allows_transmit(
        {"source_radio_id": "41", "js8_instance_id": "401"}
    ) is False
    assert tab._pending_js8_source_allows_transmit(
        {"source_radio_id": "42", "js8_instance_id": "402"}
    ) is True
    assert tab._pending_js8_source_allows_transmit({}) is True
