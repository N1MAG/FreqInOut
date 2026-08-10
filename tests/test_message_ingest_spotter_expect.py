from pathlib import Path
import sqlite3

from freqinout.core.js8_expect_dispatcher import list_expect_dispatch_audit
from freqinout.core.js8_expect_store import list_expect_runtime_audit, save_expect_entry
from freqinout.core.message_ingest import MessageIngestor
from freqinout.core.settings_manager import SettingsManager
from freqinout.radio_interface.js8_api_client import JS8ApiClient
from tests.test_js8_send_service import _safe_server


def test_spotter_directed_ingest_adds_source_and_expect_audit(monkeypatch, tmp_path: Path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    directed = tmp_path / "DIRECTED.TXT"
    directed.write_text(
        "2026-08-08 12:34:56\t7078000\t0\t-10\tN0CALL: @MAGNET F!304 11111111 #HHJL *DE* N0CALL \u2662\n",
        encoding="utf-8",
    )
    settings = SettingsManager()
    db_path = cfg_root / "config" / "freqinout_nets.db"
    save_expect_entry(
        {
            "source_radio_id": "7",
            "source_scope": "radio",
            "js8_instance_id": "fio-a",
            "expect_key": "F!304",
            "response_text": "@MAGNET F!304 OK",
            "allowed_groups": ["@MAGNET"],
            "enabled": True,
            "auto_reply_enabled": True,
        },
        db_path=db_path,
    )

    ingestor = MessageIngestor(settings)
    ingestor.ingest_spotter_from_directed(
        directed_path=directed,
        source_radio_id=7,
        js8_instance_id="fio-a",
        offset_key="spotter_directed_offset_radio_7",
    )
    ingestor.ingest_spotter_from_directed(
        directed_path=directed,
        source_radio_id=7,
        js8_instance_id="fio-a",
        offset_key="spotter_directed_offset_radio_7",
    )

    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT from_call, to_call, form_id, source_radio_id, js8_instance_id FROM spotter_traffic"
        ).fetchall()
    finally:
        conn.close()
    audit = list_expect_runtime_audit(db_path=db_path)

    assert rows == [("N0CALL", "@MAGNET", "304", "7", "fio-a")]
    assert len(audit) == 1
    assert audit[0]["decision"] == "reply-ready"
    assert audit[0]["source_radio_id"] == "7"
    assert audit[0]["source_js8_instance_id"] == "fio-a"
    assert settings.get("spotter_directed_offset_radio_7", 0) > 0


def test_spotter_js8_event_ingest_adds_source_and_expect_audit(monkeypatch, tmp_path: Path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    settings = SettingsManager()
    db_path = cfg_root / "config" / "freqinout_nets.db"
    save_expect_entry(
        {
            "source_radio_id": "8",
            "source_scope": "radio",
            "js8_instance_id": "fio-b",
            "expect_key": "F!304",
            "response_text": "@MAGNET F!304 OK",
            "allowed_groups": ["@MAGNET"],
            "enabled": True,
            "auto_reply_enabled": True,
        },
        db_path=db_path,
    )

    imported = MessageIngestor(settings).ingest_spotter_from_js8_events(
        [
            {
                "type": "RX.DIRECTED",
                "value": "@MAGNET F!304 11111111 #HHJL *DE* N0CALL",
                "params": {
                    "FROM": "N0CALL",
                    "TO": "@MAGNET",
                    "TEXT": "@MAGNET F!304 11111111 #HHJL *DE* N0CALL",
                    "UTC": "2026-08-08 12:34:56",
                },
            }
        ],
        source_radio_id=8,
        js8_instance_id="fio-b",
    )

    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT from_call, to_call, form_id, source_radio_id, js8_instance_id FROM spotter_traffic"
        ).fetchall()
    finally:
        conn.close()
    audit = list_expect_runtime_audit(db_path=db_path)

    assert imported == 1
    assert rows == [("N0CALL", "@MAGNET", "304", "8", "fio-b")]
    assert audit[0]["decision"] == "reply-ready"
    assert audit[0]["event_id"].startswith("js8-api:8:fio-b")
    assert list_expect_dispatch_audit(db_path=db_path) == []


def test_spotter_js8_event_expect_dispatch_sends_only_when_runtime_enabled(monkeypatch, tmp_path: Path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    settings = SettingsManager()
    db_path = cfg_root / "config" / "freqinout_nets.db"
    save_expect_entry(
        {
            "source_radio_id": "8",
            "source_scope": "radio",
            "js8_instance_id": "fio-b",
            "expect_key": "F!304",
            "response_text": "@MAGNET F!304 OK",
            "allowed_groups": ["@MAGNET"],
            "enabled": True,
            "auto_reply_enabled": True,
            "unattended_auto_reply_enabled": True,
        },
        db_path=db_path,
    )
    server = _safe_server()
    client = JS8ApiClient(server.endpoint, auto_reconnect=False, timeout_s=1.0)
    requested_sources: list[tuple[str, str]] = []

    def client_factory(radio_id: str, js8_instance_id: str) -> JS8ApiClient:
        requested_sources.append((radio_id, js8_instance_id))
        return client

    try:
        imported = MessageIngestor(
            settings,
            expect_dispatch_client_factory=client_factory,
            expect_auto_reply_enabled=True,
        ).ingest_spotter_from_js8_events(
            [
                {
                    "type": "RX.DIRECTED",
                    "value": "@MAGNET F!304 11111111 #HHJL *DE* N0CALL",
                    "params": {
                        "FROM": "N0CALL",
                        "TO": "@MAGNET",
                        "TEXT": "@MAGNET F!304 11111111 #HHJL *DE* N0CALL",
                        "UTC": "2026-08-08 12:34:56",
                    },
                }
            ],
            source_radio_id=8,
            js8_instance_id="fio-b",
        )

        assert imported == 1
        assert requested_sources == [("8", "fio-b")]
        assert server.received[-1]["type"] == "TX.SEND_MESSAGE"
        assert server.received[-1]["value"] == "@MAGNET F!304 OK"
        dispatch = list_expect_dispatch_audit(db_path=db_path)
        assert dispatch[0]["decision"] == "sent"
        assert dispatch[0]["source_radio_id"] == "8"
        assert dispatch[0]["source_js8_instance_id"] == "fio-b"
    finally:
        client.stop()
        server.stop()


def test_spotter_js8_event_expect_dispatch_audits_runtime_hold_without_client(monkeypatch, tmp_path: Path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    settings = SettingsManager()
    db_path = cfg_root / "config" / "freqinout_nets.db"
    save_expect_entry(
        {
            "source_radio_id": "8",
            "source_scope": "radio",
            "js8_instance_id": "fio-b",
            "expect_key": "F!304",
            "response_text": "@MAGNET F!304 OK",
            "allowed_groups": ["@MAGNET"],
            "enabled": True,
            "auto_reply_enabled": True,
            "unattended_auto_reply_enabled": True,
        },
        db_path=db_path,
    )

    imported = MessageIngestor(settings, expect_auto_reply_enabled=True).ingest_spotter_from_js8_events(
        [
            {
                "type": "RX.DIRECTED",
                "value": "@MAGNET F!304 11111111 #HHJL *DE* N0CALL",
                "params": {
                    "FROM": "N0CALL",
                    "TO": "@MAGNET",
                    "TEXT": "@MAGNET F!304 11111111 #HHJL *DE* N0CALL",
                    "UTC": "2026-08-08 12:34:56",
                },
            }
        ],
        source_radio_id=8,
        js8_instance_id="fio-b",
    )

    assert imported == 1
    dispatch = list_expect_dispatch_audit(db_path=db_path)
    assert dispatch[0]["decision"] == "held"
    assert "No JS8 client factory" in dispatch[0]["reason"]
    assert dispatch[0]["source_radio_id"] == "8"
    assert dispatch[0]["source_js8_instance_id"] == "fio-b"


def test_spotter_js8_event_duplicates_are_scoped_to_source(monkeypatch, tmp_path: Path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    settings = SettingsManager()
    event = {
        "type": "RX.DIRECTED",
        "value": "@MAGNET F!304 11111111 #HHJL *DE* N0CALL",
        "params": {
            "FROM": "N0CALL",
            "TO": "@MAGNET",
            "TEXT": "@MAGNET F!304 11111111 #HHJL *DE* N0CALL",
            "UTC": "2026-08-08 12:34:56",
        },
    }
    ingestor = MessageIngestor(settings)

    assert ingestor.ingest_spotter_from_js8_events([event], source_radio_id=8, js8_instance_id="fio-b") == 1
    assert ingestor.ingest_spotter_from_js8_events([event], source_radio_id=8, js8_instance_id="fio-b") == 0
    assert ingestor.ingest_spotter_from_js8_events([event], source_radio_id=9, js8_instance_id="fio-c") == 1

    conn = sqlite3.connect(cfg_root / "config" / "freqinout_nets.db")
    try:
        rows = conn.execute(
            "SELECT source_radio_id, js8_instance_id FROM spotter_traffic ORDER BY source_radio_id"
        ).fetchall()
    finally:
        conn.close()

    assert rows == [("8", "fio-b"), ("9", "fio-c")]
