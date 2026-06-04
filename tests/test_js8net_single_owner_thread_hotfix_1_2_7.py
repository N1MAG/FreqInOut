from __future__ import annotations


class _FakeJs8Net:
    def __init__(self) -> None:
        self.start_calls: list[tuple[str, int]] = []

    def start_net(self, host: str, port: int) -> None:
        self.start_calls.append((host, port))


def test_repeated_js8_control_clients_share_one_js8net_start(monkeypatch):
    import freqinout.radio_interface.js8_rx_hub as hub_module
    import freqinout.radio_interface.js8_status as status_module

    fake = _FakeJs8Net()
    monkeypatch.setattr(hub_module, "js8net", fake)
    monkeypatch.setattr(status_module, "js8net", fake)
    monkeypatch.setattr(hub_module, "_JS8NET_STARTED_ENDPOINT", None)
    monkeypatch.setattr(status_module.JS8ControlClient, "_js8call_running", lambda self: True)

    clients = [status_module.JS8ControlClient(host="127.0.0.1") for _ in range(20)]
    assert all(client._ensure_net() for client in clients)
    assert fake.start_calls == [("127.0.0.1", 2442)]


def test_shared_js8net_start_rejects_endpoint_change(monkeypatch):
    import freqinout.radio_interface.js8_rx_hub as hub_module

    fake = _FakeJs8Net()
    monkeypatch.setattr(hub_module, "js8net", fake)
    monkeypatch.setattr(hub_module, "_JS8NET_STARTED_ENDPOINT", None)

    assert hub_module.ensure_js8net_started("127.0.0.1", 2442) is True
    assert hub_module.ensure_js8net_started("127.0.0.1", 2443) is False
    assert fake.start_calls == [("127.0.0.1", 2442)]
