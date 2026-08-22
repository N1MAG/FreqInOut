from pathlib import Path
import json
import sqlite3

from freqinout.core.condition_alerts import CONDITION_ALERT_RULES_SETTING_KEY
from freqinout.core.js8_expect_dispatcher import list_expect_dispatch_audit
from freqinout.core.js8_expect_store import list_expect_runtime_audit, save_expect_entry
from freqinout.core.message_ingest import MessageIngestor
from freqinout.core.observation_store import list_observations
from freqinout.core.settings_manager import SettingsManager
from freqinout.radio_interface.js8_api_client import JS8ApiClient
from tests.test_js8_send_service import _safe_server


def _write_js8_inbox(path: Path, *, row_id: int, from_call: str, text: str, utc: str = "2026-08-08 12:34:56") -> None:
    conn = sqlite3.connect(path)
    try:
        conn.execute("CREATE TABLE inbox_v1 (id INTEGER PRIMARY KEY, json TEXT, type TEXT, value TEXT)")
        conn.execute(
            "INSERT INTO inbox_v1 (id, json, type, value) VALUES (?, ?, 'RX.DIRECTED', '')",
            (
                int(row_id),
                json.dumps(
                    {
                        "type": "RX.DIRECTED",
                        "params": {
                            "FROM": from_call,
                            "TO": "@MAGNET",
                            "TEXT": text,
                            "UTC": utc,
                        },
                    }
                ),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def test_js8_inbox_read_state_is_scoped_by_source_key(monkeypatch, tmp_path: Path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    settings = SettingsManager()
    db_path = cfg_root / "config" / "freqinout_nets.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    inbox_a = tmp_path / "fio-a-inbox.sqlite"
    inbox_b = tmp_path / "fio-b-inbox.sqlite"
    _write_js8_inbox(inbox_a, row_id=1, from_call="K1AAA", text="MSG A")
    _write_js8_inbox(inbox_b, row_id=1, from_call="K1BBB", text="MSG B")
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "CREATE TABLE js8_inbox_state (id INTEGER PRIMARY KEY, state TEXT, last_seen REAL, read_ts REAL, last_ingested_id INTEGER, source_key TEXT, source_id INTEGER)"
        )
        conn.execute(
            "INSERT INTO js8_inbox_state (id, state, read_ts, source_key, source_id) VALUES (?, 'READ', 99.0, 'js8:fio-a', 1)",
            (MessageIngestor._js8_local_row_id(1, "js8:fio-a"),),
        )
        conn.execute(
            "INSERT INTO js8_inbox_state (id, state, read_ts, source_key, source_id) VALUES (?, 'UNREAD', 0.0, 'js8:fio-b', 1)",
            (MessageIngestor._js8_local_row_id(1, "js8:fio-b"),),
        )
        conn.commit()
    finally:
        conn.close()

    ingestor = MessageIngestor(settings)
    ingestor.ingest_js8_messages(
        inbox_path=inbox_a,
        source_radio_id="7",
        js8_instance_id="fio-a",
        source_key="js8:fio-a",
    )
    ingestor.ingest_js8_messages(
        inbox_path=inbox_b,
        source_radio_id="8",
        js8_instance_id="fio-b",
        source_key="js8:fio-b",
    )

    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT source_key, source_id, from_call, state, read_ts FROM js8_messages ORDER BY from_call"
        ).fetchall()
    finally:
        conn.close()

    assert rows == [
        ("js8:fio-a", 1, "K1AAA", "READ", 99.0),
        ("js8:fio-b", 1, "K1BBB", "UNREAD", 0.0),
    ]


def test_js8_inbox_visible_and_background_same_source_do_not_duplicate(monkeypatch, tmp_path: Path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    settings = SettingsManager()
    db_path = cfg_root / "config" / "freqinout_nets.db"
    inbox = tmp_path / "fio-a-inbox.sqlite"
    _write_js8_inbox(inbox, row_id=1, from_call="K1AAA", text="MSG A")

    background = MessageIngestor(settings)
    visible = MessageIngestor(settings)
    background.ingest_js8_messages(
        inbox_path=inbox,
        source_radio_id="7",
        js8_instance_id="fio-a",
        source_key="js8:fio-a",
    )
    visible.ingest_js8_messages(
        inbox_path=inbox,
        source_radio_id="7",
        js8_instance_id="fio-a",
        source_key="js8:fio-a",
    )

    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT source_key, source_id, from_call, raw_text FROM js8_messages"
        ).fetchall()
    finally:
        conn.close()

    assert rows == [("js8:fio-a", 1, "K1AAA", "MSG A")]


def test_js8_local_insert_conflict_is_source_scoped_for_parallel_refresh(monkeypatch, tmp_path: Path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    settings = SettingsManager()
    db_path = cfg_root / "config" / "freqinout_nets.db"
    ingestor = MessageIngestor(settings)
    ingestor._ensure_local_js8_tables()

    for _ in range(2):
        ingestor._insert_js8_local(
            1,
            "K1AAA",
            "@MAGNET",
            "MSG",
            "2026-08-08 12:34:56",
            111.0,
            "MSG A",
            "MSG A",
            "UNREAD",
            0.0,
            source_key="js8:fio-a",
            source_id=1,
            source_radio_id="7",
            js8_instance_id="fio-a",
            source_path=str(tmp_path / "fio-a-inbox.sqlite"),
        )
    ingestor._insert_js8_local(
        1,
        "K1BBB",
        "@MAGNET",
        "MSG",
        "2026-08-08 12:34:56",
        111.0,
        "MSG B",
        "MSG B",
        "UNREAD",
        0.0,
        source_key="js8:fio-b",
        source_id=1,
        source_radio_id="8",
        js8_instance_id="fio-b",
        source_path=str(tmp_path / "fio-b-inbox.sqlite"),
    )

    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT source_key, source_id, from_call, raw_text FROM js8_messages ORDER BY source_key"
        ).fetchall()
    finally:
        conn.close()

    assert rows == [
        ("js8:fio-a", 1, "K1AAA", "MSG A"),
        ("js8:fio-b", 1, "K1BBB", "MSG B"),
    ]


def test_js8_next_msg_backlog_preserves_source_context(monkeypatch, tmp_path: Path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    settings = SettingsManager()
    db_path = cfg_root / "config" / "freqinout_nets.db"
    inbox = tmp_path / "fio-b-inbox.sqlite"
    _write_js8_inbox(inbox, row_id=1, from_call="K1BBB", text="NEXT MSG ID 42")

    MessageIngestor(settings).ingest_js8_messages(
        inbox_path=inbox,
        source_radio_id="8",
        js8_instance_id="fio-b",
        source_key="js8:fio-b",
    )

    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            """
            SELECT callsign, msg_id, source_key, source_radio_id, js8_instance_id, source_path
            FROM autoquery_backlog
            """
        ).fetchone()
    finally:
        conn.close()

    assert row == ("K1BBB", "42", "js8:fio-b", "8", "fio-b", str(inbox))


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
    observations = list_observations(db_path, source_family="spotter")
    assert len(observations) == 1
    assert observations[0].source_ref == "spotter_traffic:1"
    assert observations[0].from_call == "N0CALL"
    assert observations[0].to_target == "@MAGNET"
    assert observations[0].source_radio_id == 7
    assert observations[0].source_app == "fio-a"
    assert observations[0].provenance["ingest_source"] == "directed"


def test_spotter_directed_ingest_mirrors_condition_alert_observation(monkeypatch, tmp_path: Path) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    directed = tmp_path / "DIRECTED.TXT"
    directed.write_text(
        "2026-08-08 12:34:56\t7078000\t0\t-10\tN1MAG: @MAGNET F!701 TO[@MAGNET] FR[N1MAG] NA[MAGCON+4] DA[260808-1234Z] #HHJL *DE* N1MAG \u2662\n",
        encoding="utf-8",
    )
    settings = SettingsManager()
    db_path = cfg_root / "config" / "freqinout_nets.db"
    settings.set(
        CONDITION_ALERT_RULES_SETTING_KEY,
        [
            {
                "id": "magcon-active",
                "enabled": True,
                "name": "MagNet MAGCON",
                "operating_group": "MAGNET",
                "source_families": ["JS8Spotter"],
                "target_groups": ["MAGNET"],
                "allowed_sender_mode": "explicit list",
                "allowed_senders": ["N1MAG"],
                "pattern": r"MAGCON\+?([1-5])",
            }
        ],
    )

    MessageIngestor(settings).ingest_spotter_from_directed(
        directed_path=directed,
        source_radio_id=7,
        js8_instance_id="fio-a",
        offset_key="spotter_directed_offset_radio_7",
    )

    spotter = list_observations(db_path, source_family="spotter")
    alerts = list_observations(db_path, source_family="condition_alert")
    assert len(spotter) == 1
    assert len(alerts) == 1
    assert alerts[0].source_ref == "spotter_traffic:1"
    assert alerts[0].source_radio_id == 7
    assert alerts[0].source_app == "fio-a"
    assert alerts[0].groups == ("MAGNET",)
    assert alerts[0].urgency == "LEVEL 4"


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
    observations = list_observations(db_path, source_family="spotter")
    assert len(observations) == 1
    assert observations[0].source_ref == "spotter_traffic:1"
    assert observations[0].source_radio_id == 8
    assert observations[0].source_app == "fio-b"
    assert observations[0].provenance["ingest_source"] == "js8-api"


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

    assert ingestor.ingest_spotter_from_js8_events([event], source_radio_id=8, js8_instance_id="fio-b", source_key="js8-api-b") == 1
    assert ingestor.ingest_spotter_from_js8_events([event], source_radio_id=8, js8_instance_id="FIO-B", source_key="js8-api-b") == 0
    assert ingestor.ingest_spotter_from_js8_events([event], source_radio_id=8, js8_instance_id="fio-b", source_key="js8-api-b-alt") == 1
    assert ingestor.ingest_spotter_from_js8_events([event], source_radio_id=9, js8_instance_id="fio-c", source_key="js8-api-c") == 1

    conn = sqlite3.connect(cfg_root / "config" / "freqinout_nets.db")
    try:
        rows = conn.execute(
            "SELECT source_radio_id, js8_instance_id, source_key FROM spotter_traffic ORDER BY source_key"
        ).fetchall()
    finally:
        conn.close()

    assert rows == [
        ("8", "fio-b", "js8-api-b"),
        ("8", "fio-b", "js8-api-b-alt"),
        ("9", "fio-c", "js8-api-c"),
    ]


def test_spotter_live_then_directed_same_source_does_not_duplicate_or_reevaluate_expect(
    monkeypatch,
    tmp_path: Path,
) -> None:
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
    ingestor = MessageIngestor(settings)

    imported = ingestor.ingest_spotter_from_js8_events(
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

    ingestor.ingest_spotter_from_directed(
        directed_path=directed,
        source_radio_id=8,
        js8_instance_id="fio-b",
        offset_key="spotter_directed_offset_visible_radio_8",
    )

    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT from_call, to_call, form_id, spotter_token, source_radio_id, js8_instance_id FROM spotter_traffic"
        ).fetchall()
    finally:
        conn.close()
    audit = list_expect_runtime_audit(db_path=db_path)

    assert rows == [("N0CALL", "@MAGNET", "304", "#HHJL", "8", "fio-b")]
    assert len(audit) == 1


def test_spotter_directed_visible_and_background_offsets_share_idempotence(
    monkeypatch,
    tmp_path: Path,
) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    directed = tmp_path / "DIRECTED.TXT"
    directed.write_text(
        "2026-08-08 12:34:56\t7078000\t0\t-10\tN0CALL: @MAGNET F!304 11111111 #HHJL *DE* N0CALL \u2662\n",
        encoding="utf-8",
    )
    settings = SettingsManager()
    ingestor = MessageIngestor(settings)

    ingestor.ingest_spotter_from_directed(
        directed_path=directed,
        source_radio_id=8,
        js8_instance_id="fio-b",
        offset_key="spotter_directed_offset_background_radio_8",
    )
    ingestor.ingest_spotter_from_directed(
        directed_path=directed,
        source_radio_id=8,
        js8_instance_id="fio-b",
        offset_key="spotter_directed_offset_visible_radio_8",
    )

    conn = sqlite3.connect(cfg_root / "config" / "freqinout_nets.db")
    try:
        count = conn.execute("SELECT COUNT(*) FROM spotter_traffic").fetchone()[0]
        rows = conn.execute(
            "SELECT from_call, to_call, form_id, spotter_token, source_radio_id, js8_instance_id FROM spotter_traffic"
        ).fetchall()
    finally:
        conn.close()

    assert count == 1
    assert rows == [("N0CALL", "@MAGNET", "304", "#HHJL", "8", "fio-b")]
    assert settings.get("spotter_directed_offset_background_radio_8", 0) > 0
    assert settings.get("spotter_directed_offset_visible_radio_8", 0) > 0


def test_spotter_directed_same_content_from_different_sources_keeps_provenance(
    monkeypatch,
    tmp_path: Path,
) -> None:
    cfg_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(cfg_root))
    directed_a = tmp_path / "fio-a" / "DIRECTED.TXT"
    directed_b = tmp_path / "fio-b" / "DIRECTED.TXT"
    for directed in (directed_a, directed_b):
        directed.parent.mkdir(parents=True, exist_ok=True)
        directed.write_text(
            "2026-08-08 12:34:56\t7078000\t0\t-10\tN0CALL: @MAGNET F!304 11111111 #HHJL *DE* N0CALL \u2662\n",
            encoding="utf-8",
        )
    settings = SettingsManager()
    ingestor = MessageIngestor(settings)

    assert (
        ingestor.ingest_spotter_from_directed(
            directed_path=directed_a,
            source_radio_id=8,
            js8_instance_id="fio-a",
            source_key="spotter:fio-a",
            offset_key="spotter_directed_offset_background_radio_8",
        )
        == 1
    )
    assert (
        ingestor.ingest_spotter_from_directed(
            directed_path=directed_b,
            source_radio_id=9,
            js8_instance_id="fio-b",
            source_key="spotter:fio-b",
            offset_key="spotter_directed_offset_background_radio_9",
        )
        == 1
    )

    conn = sqlite3.connect(cfg_root / "config" / "freqinout_nets.db")
    try:
        rows = conn.execute(
            """
            SELECT from_call, to_call, form_id, spotter_token, source_radio_id, js8_instance_id, source_key
            FROM spotter_traffic
            ORDER BY source_key
            """
        ).fetchall()
    finally:
        conn.close()

    assert rows == [
        ("N0CALL", "@MAGNET", "304", "#HHJL", "8", "fio-a", "spotter:fio-a"),
        ("N0CALL", "@MAGNET", "304", "#HHJL", "9", "fio-b", "spotter:fio-b"),
    ]
