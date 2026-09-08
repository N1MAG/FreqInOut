from __future__ import annotations

import sqlite3
from pathlib import Path
import time

from freqinout.core import js8_expect_store
from freqinout.core.db_initializer import _ensure_flamp_dynamic_tables
from freqinout.core.js8_expect_store import (
    claim_expect_request,
    complete_expect_request_claim,
    evaluate_dynamic_flamp_request,
    save_expect_allow_policy,
    save_expect_entry,
)
from freqinout.core.js8_expect_dispatcher import list_expect_dispatch_audit
from freqinout.core.message_ingest import MessageIngestor
from freqinout.core.settings_manager import SettingsManager
from freqinout.core.varac_bbs_vault import (
    FlampRelayStore,
    flamp_transfer_index_status,
    index_flamp_transfer_state,
    lookup_flamp_transfer_state,
    parse_dynamic_flamp_query,
)
from freqinout.radio_interface.js8_api_client import JS8ApiClient
from tests.test_js8_send_service import _safe_server


def _relay_file(path: Path, q_id: str, total: int, blocks: list[int]) -> None:
    lines = [f"<SIZE X>{{{q_id}}}10 {total} 1>"]
    lines.extend(f"{{{q_id}:{number}}} block-{number}" for number in blocks)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _initialize_flamp_projection(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        _ensure_flamp_dynamic_tables(conn)


def test_dynamic_query_parser_is_exact_and_case_insensitive() -> None:
    assert parse_dynamic_flamp_query("E? Q 970F").q_id == "970F"
    assert parse_dynamic_flamp_query("E? Q970F").q_id == "970F"
    assert parse_dynamic_flamp_query("e? q 970f").q_id == "970F"
    assert parse_dynamic_flamp_query("E? Q 970F trailing") is None
    assert parse_dynamic_flamp_query("E? Q 970") is None
    assert parse_dynamic_flamp_query("E? 970F") is None


def test_dynamic_directed_parser_accepts_production_js8_spacing(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    settings = SettingsManager()
    ingestor = MessageIngestor(settings)
    compact = ingestor._parse_dynamic_directed_line(
        "2026-09-08 13:20:42\t7.115000\t1925\t+12\tW5TTA: N1MAG  E? Q906F ♢ \n"
    )
    spaced = ingestor._parse_dynamic_directed_line(
        "2026-09-08 13:22:28\t7.115000\t1925\t+15\tW5TTA: N1MAG  E? Q 906F ♢ \n"
    )
    assert compact and compact["q_id"] == "906F" and compact["to_call"] == "N1MAG"
    assert spaced and spaced["q_id"] == "906F" and spaced["from_call"] == "W5TTA"
    settings.close()


def test_dynamic_directed_tail_is_incremental_and_preserves_partial_append(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    directed = tmp_path / "DIRECTED.TXT"
    old = "2026-09-08 13:19:00\t7.115000\t1925\t+10\tW5TTA: N1MAG TEST ♢ \n"
    directed.write_text(old, encoding="utf-8")
    settings = SettingsManager()
    settings.set("spotter_directed_offset_source-a", len(old.encode("utf-8")))
    settings.save()
    ingestor = MessageIngestor(settings)
    handled: list[dict] = []
    monkeypatch.setattr(
        ingestor,
        "_handle_dynamic_flamp_query",
        lambda parsed, **_kwargs: handled.append(dict(parsed)),
    )

    partial = "2026-09-08 13:20:42\t7.115000\t1925\t+12\tW5TTA: N1MAG  E? Q906F ♢ "
    with directed.open("a", encoding="utf-8") as fh:
        fh.write(partial)
    assert ingestor.ingest_dynamic_flamp_from_directed(
        directed_path=directed,
        source_radio_id=1,
        js8_instance_id="fio-a",
        source_key="source-a",
        offset_key="expect_directed_offset_source-a",
        fallback_offset_key="spotter_directed_offset_source-a",
    ) == 0
    assert handled == []

    with directed.open("a", encoding="utf-8") as fh:
        fh.write("\n")
    assert ingestor.ingest_dynamic_flamp_from_directed(
        directed_path=directed,
        source_radio_id=1,
        js8_instance_id="fio-a",
        source_key="source-a",
        offset_key="expect_directed_offset_source-a",
        fallback_offset_key="spotter_directed_offset_source-a",
    ) == 1
    assert [row["q_id"] for row in handled] == ["906F"]
    assert ingestor.ingest_dynamic_flamp_from_directed(
        directed_path=directed,
        source_radio_id=1,
        js8_instance_id="fio-a",
        source_key="source-a",
        offset_key="expect_directed_offset_source-a",
        fallback_offset_key="spotter_directed_offset_source-a",
    ) == 0
    settings.close()


def test_flamp_state_is_authoritative_source_scoped_and_digit_leading(tmp_path: Path) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    relay_a = tmp_path / "relay-a"
    relay_b = tmp_path / "relay-b"
    relay_a.mkdir()
    relay_b.mkdir()
    _initialize_flamp_projection(db_path)
    _relay_file(relay_a / "970F_payload.b2s", "970F", 4, [1, 2, 3, 4])
    _relay_file(relay_b / "970F_payload.b2s", "970F", 4, [1, 2])

    index_flamp_transfer_state(relay_a, db_path=db_path, source_radio_id="a", source_js8_instance_id="js8-a")
    index_flamp_transfer_state(relay_b, db_path=db_path, source_radio_id="b", source_js8_instance_id="js8-b")

    complete = lookup_flamp_transfer_state("970F", db_path=db_path, source_radio_id="a", source_js8_instance_id="js8-a")
    partial = lookup_flamp_transfer_state("970F", db_path=db_path, source_radio_id="b", source_js8_instance_id="js8-b")
    assert complete["state"] == "complete"
    assert complete["missing_blocks"] == []
    assert partial["state"] == "partial"
    assert partial["missing_blocks"] == [3, 4]
    assert FlampRelayStore(relay_a).authoritative_transfer("970F")["state"] == "complete"
    scan = flamp_transfer_index_status(
        db_path=db_path, source_radio_id="a", source_js8_instance_id="js8-a"
    )
    assert scan and scan["scan_success"] is True and scan["file_count"] == 1


def test_flamp_unknown_total_is_not_a_false_complete(tmp_path: Path) -> None:
    relay = tmp_path / "relay"
    relay.mkdir()
    (relay / "970F_payload.b2s").write_text("{970F:1} block-1\n", encoding="utf-8")
    db_path = tmp_path / "freqinout_nets.db"
    _initialize_flamp_projection(db_path)
    index_flamp_transfer_state(relay, db_path=db_path, source_radio_id="a", source_js8_instance_id="js8-a")
    state = lookup_flamp_transfer_state("970F", db_path=db_path, source_radio_id="a", source_js8_instance_id="js8-a")
    assert state["state"] == "unavailable"
    assert state["parser_confidence"] == 0.0


def test_unchanged_flamp_background_scan_reuses_the_persisted_projection(
    monkeypatch, tmp_path: Path
) -> None:
    relay = tmp_path / "relay"
    relay.mkdir()
    _relay_file(relay / "970F_payload.b2s", "970F", 4, [1, 2, 3, 4])
    db_path = tmp_path / "freqinout_nets.db"
    _initialize_flamp_projection(db_path)
    original = FlampRelayStore.parse_file
    calls = {"count": 0}

    def counted(self, file_path):
        calls["count"] += 1
        return original(self, file_path)

    monkeypatch.setattr(FlampRelayStore, "parse_file", counted)
    index_flamp_transfer_state(
        relay, db_path=db_path, source_radio_id="a", source_js8_instance_id="js8-a"
    )
    first_count = calls["count"]
    index_flamp_transfer_state(
        relay, db_path=db_path, source_radio_id="a", source_js8_instance_id="js8-a"
    )
    assert first_count == 1
    assert calls["count"] == first_count
    (relay / "970F_payload.b2s").unlink()
    index_flamp_transfer_state(
        relay, db_path=db_path, source_radio_id="a", source_js8_instance_id="js8-a"
    )
    removed = lookup_flamp_transfer_state(
        "970F", db_path=db_path, source_radio_id="a", source_js8_instance_id="js8-a"
    )
    assert removed["state"] == "unavailable"
    assert removed["source_mtime_ns"] == 0


def test_dynamic_request_claim_is_durable_duplicate_and_cooldown_safe(tmp_path: Path) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    save_expect_entry(
        {"expect_key": "Q", "source_scope": "all", "enabled": True},
        db_path=db_path,
    )
    first = claim_expect_request(
        event_key="event-1",
        expect_entry_id=7,
        q_id="970F",
        source_radio_id="a",
        source_js8_instance_id="js8-a",
        requesting_callsign="K1ABC",
        max_replies=2,
        cooldown_seconds=30,
        db_path=db_path,
        now=100.0,
    )
    assert first.acquired is True
    complete_expect_request_claim(event_key="event-1", status="sent", reply_text="Q 970F YES", db_path=db_path, now=100.0)
    duplicate = claim_expect_request(
        event_key="event-1",
        expect_entry_id=7,
        q_id="970F",
        source_radio_id="a",
        source_js8_instance_id="js8-a",
        requesting_callsign="K1ABC",
        max_replies=2,
        cooldown_seconds=30,
        db_path=db_path,
        now=101.0,
    )
    assert duplicate.acquired is False and duplicate.status == "duplicate"
    cooldown = claim_expect_request(
        event_key="event-2",
        expect_entry_id=7,
        q_id="970F",
        source_radio_id="a",
        source_js8_instance_id="js8-a",
        requesting_callsign="K1ABC",
        max_replies=2,
        cooldown_seconds=30,
        db_path=db_path,
        now=110.0,
    )
    assert cooldown.acquired is False and cooldown.status == "cooldown"


def test_dynamic_q_hot_path_reads_only_q_rules_and_referenced_policies(
    monkeypatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "freqinout_nets.db"
    policy = save_expect_allow_policy(
        {
            "name": "MAGNET Q access",
            "allowed_callsigns": ["K1ABC"],
            "allowed_groups": ["@MAGNET"],
            "enabled": True,
        },
        db_path=db_path,
    )
    save_expect_entry(
        {
            "expect_key": "Q",
            "allow_policy_id": policy.id,
            "source_scope": "all",
            "enabled": True,
            "auto_reply_enabled": True,
            "unattended_auto_reply_enabled": True,
        },
        db_path=db_path,
    )
    for index in range(40):
        save_expect_entry(
            {
                "expect_key": f"OTHER{index}",
                "source_scope": "all",
                "enabled": True,
                "auto_reply_enabled": True,
            },
            db_path=db_path,
        )

    calls: dict[str, list[object]] = {"entries": [], "policies": []}
    original_entries = js8_expect_store.list_expect_entries
    original_policies = js8_expect_store.list_expect_allow_policies

    def tracked_entries(**kwargs):
        calls["entries"].append(kwargs.get("expect_key"))
        return original_entries(**kwargs)

    def tracked_policies(**kwargs):
        calls["policies"].append(tuple(kwargs.get("policy_ids") or ()))
        return original_policies(**kwargs)

    monkeypatch.setattr(js8_expect_store, "list_expect_entries", tracked_entries)
    monkeypatch.setattr(js8_expect_store, "list_expect_allow_policies", tracked_policies)
    def reject_schema_setup(_conn) -> None:
        raise AssertionError("runtime path attempted schema setup")

    monkeypatch.setattr(js8_expect_store, "_ensure_js8_expect_tables", reject_schema_setup)

    result = evaluate_dynamic_flamp_request(
        q_id="970F",
        requesting_callsign="K1ABC",
        target_group="@MAGNET",
        source_radio_id="7",
        js8_instance_id="fio-a",
        db_path=db_path,
        write_audit=False,
    )

    assert result.decision == "reply-ready"
    assert calls == {"entries": ["Q"], "policies": [(policy.id,)]}
    with sqlite3.connect(db_path) as conn:
        plan = conn.execute(
            "EXPLAIN QUERY PLAN SELECT id FROM js8_expect_entries WHERE expect_key=?",
            ("Q",),
        ).fetchall()
    assert any("idx_js8_expect_entries_key" in str(row) for row in plan)

    claim = claim_expect_request(
        event_key="hot-path-claim",
        expect_entry_id=result.expect_entry_id,
        q_id="970F",
        source_radio_id="7",
        source_js8_instance_id="fio-a",
        requesting_callsign="K1ABC",
        max_replies=2,
        db_path=db_path,
    )
    assert claim.acquired is True
    complete_expect_request_claim(
        event_key="hot-path-claim",
        status="sent",
        db_path=db_path,
    )


def test_ingest_parser_accepts_api_payload_prefix_and_honors_pause(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    settings = SettingsManager()
    settings.set("js8_expect_unattended_auto_reply_enabled", True)
    settings.set("js8_expect_unattended_auto_reply_paused", True)
    ingestor = MessageIngestor(settings, expect_auto_reply_enabled=True)
    parsed = ingestor._parse_dynamic_js8_event(
        {
            "type": "RX.DIRECTED",
            "params": {"FROM": "K1ABC", "TO": "N0CALL", "TEXT": "N0CALL: E? Q 970F"},
        }
    )
    assert parsed and parsed["q_id"] == "970F"
    assert ingestor._expect_auto_reply_runtime_enabled() is False
    settings.close()


def test_dynamic_q_replies_are_database_only_and_use_receiving_js8_source(
    monkeypatch, tmp_path: Path
) -> None:
    config_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(config_root))
    settings = SettingsManager()
    settings.set("js8_expect_dynamic_flamp_enabled", True)
    settings.set("js8_expect_unattended_auto_reply_enabled", True)
    settings.set("js8_expect_unattended_auto_reply_paused", False)
    relay = tmp_path / "relay"
    relay.mkdir()
    settings.set("varac_bbs_vault_flamp_relay_dir", str(relay))
    settings.save()
    db_path = config_root / "config" / "freqinout_nets.db"
    _initialize_flamp_projection(db_path)
    _relay_file(relay / "970F_payload.b2s", "970F", 4, [1, 2, 3, 4])
    _relay_file(relay / "A10F_payload.b2s", "A10F", 4, [1, 2])
    index_flamp_transfer_state(
        relay,
        db_path=db_path,
        source_radio_id="7",
        source_js8_instance_id="fio-a",
    )
    save_expect_entry(
        {
            "expect_key": "Q",
            "source_scope": "all",
            "allowed_callsigns": ["K1ABC"],
            "max_replies": 2,
            "enabled": True,
            "auto_reply_enabled": True,
            "unattended_auto_reply_enabled": True,
        },
        db_path=db_path,
    )
    server = _safe_server()
    client = JS8ApiClient(server.endpoint, auto_reconnect=False, timeout_s=1.0)
    ingestor = MessageIngestor(
        settings,
        expect_dispatch_client_factory=lambda radio, instance: (
            client if (radio, instance) == ("7", "fio-a") else None
        ),
        expect_auto_reply_enabled=True,
    )
    original_exists = Path.exists

    def reject_relay_stat(path: Path) -> bool:
        if path.parent == relay:
            raise AssertionError("on-air dynamic Q path touched a relay file")
        return original_exists(path)

    monkeypatch.setattr(Path, "exists", reject_relay_stat)
    try:
        for q_id in ("970F", "A10F", "BEEF"):
            ingestor._handle_dynamic_flamp_query(
                {
                    "q_id": q_id,
                    "confidence": 1.0,
                    "from_call": "K1ABC",
                    "to_call": "N0CALL",
                    "relayed": False,
                    "event_id": f"event-{q_id}",
                },
                source_radio_id="7",
                js8_instance_id="fio-a",
                source_key="js8:fio-a",
                source_path=None,
            )
        deadline = time.time() + 1.0
        while len([row for row in server.received if row["type"] == "TX.SEND_MESSAGE"]) < 3 and time.time() < deadline:
            time.sleep(0.01)
        sent = [row["value"] for row in server.received if row["type"] == "TX.SEND_MESSAGE"]
        assert sent == [
            "K1ABC Q 970F YES",
            "K1ABC Q A10F 3,4",
            "K1ABC Q BEEF NO",
        ]
    finally:
        client.stop()
        server.stop()
        settings.close()


def test_replayed_old_directed_q_is_held_without_transmit(monkeypatch, tmp_path: Path) -> None:
    config_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(config_root))
    settings = SettingsManager()
    settings.set("js8_expect_dynamic_flamp_enabled", True)
    settings.set("js8_expect_unattended_auto_reply_enabled", True)
    settings.save()
    db_path = config_root / "config" / "freqinout_nets.db"
    save_expect_entry(
        {"expect_key": "Q", "source_scope": "all", "enabled": False},
        db_path=db_path,
    )
    ingestor = MessageIngestor(settings, expect_auto_reply_enabled=True)
    ingestor._handle_dynamic_flamp_query(
        {
            "q_id": "970F",
            "confidence": 1.0,
            "from_call": "K1ABC",
            "to_call": "N0CALL",
            "relayed": False,
            "event_id": "old-event",
            "utc_ts": time.time() - 3600,
        },
        source_radio_id="7",
        js8_instance_id="fio-a",
        source_key="js8:fio-a",
        source_path=None,
    )
    audit = list_expect_dispatch_audit(db_path=db_path)
    assert audit[0]["decision"] == "held"
    assert "live-request window" in str(audit[0]["reason"])
    settings.close()
