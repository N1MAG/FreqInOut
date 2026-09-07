from __future__ import annotations

import datetime
import sqlite3
from pathlib import Path

from freqinout.core.checkins_db import ensure_operator_checkins_schema
from freqinout.core.message_ingest import MessageIngestor
from freqinout.core.operator_identity import change_operator_callsign
from freqinout.core.settings_manager import SettingsManager


def _now_text() -> str:
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _line(sender: str, target: str, payload: str, *, when: str | None = None) -> str:
    return f"{when or _now_text()}\t7078000\t0\t-10\t{sender}: {target} {payload} ♢\n"


def _settings(monkeypatch, tmp_path: Path) -> SettingsManager:
    config_root = tmp_path / "profile"
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(config_root))
    settings = SettingsManager()
    settings.set("operator_callsign", "N1MAG")
    settings.set("operating_groups", [{"group": "MAGNET"}])
    settings.save()
    return settings


def test_directed_file_ingests_relevant_freeform_once_and_excludes_noise(
    monkeypatch, tmp_path: Path
) -> None:
    settings = _settings(monkeypatch, tmp_path)
    directed = tmp_path / "DIRECTED.TXT"
    when = _now_text()
    directed.write_text(
        "".join(
            (
                _line("K1AAA", "N1MAG", "WEATHER FIELD UPDATE", when=when),
                _line("K1BBB", "@MAGNET", "NEED WATER", when=when),
                _line("K1CCC", "@OTHER", "NOT FOR THIS STATION", when=when),
                _line("K1DDD", "N1MAG", "HEARTBEAT SNR -10", when=when),
                _line("K1EEE", "N1MAG", "SNR -8", when=when),
            )
        ),
        encoding="utf-8",
    )
    ingestor = MessageIngestor(settings)
    first = ingestor.ingest_spotter_from_directed(
        directed_path=directed,
        source_radio_id="7",
        js8_instance_id="fio-a",
        source_key="directed:fio-a",
        offset_key="slice3_directed_offset",
        evaluate_expect=False,
    )
    second = ingestor.ingest_spotter_from_directed(
        directed_path=directed,
        source_radio_id="7",
        js8_instance_id="fio-a",
        source_key="directed:fio-a",
        offset_key="slice3_directed_offset",
        evaluate_expect=False,
        force_rebuild=True,
    )
    db_path = settings.config_dir / "freqinout_nets.db"
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT from_call, to_call, raw_text, source_key, source_radio_id, js8_instance_id "
            "FROM js8_messages ORDER BY from_call"
        ).fetchall()
    assert first == 2
    assert second == 0
    assert rows == [
        ("K1AAA", "N1MAG", "WEATHER FIELD UPDATE", "directed:fio-a", "7", "fio-a"),
        ("K1BBB", "@MAGNET", "NEED WATER", "directed:fio-a", "7", "fio-a"),
    ]
    settings.close()


def test_api_directed_ingest_uses_current_and_historical_operator_callsigns(
    monkeypatch, tmp_path: Path
) -> None:
    settings = _settings(monkeypatch, tmp_path)
    db_path = settings.config_dir / "freqinout_nets.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        ensure_operator_checkins_schema(conn)
        conn.execute(
            "INSERT INTO operator_checkins(callsign, name, group1, group_role, trusted, first_seen_utc) "
            "VALUES ('N1MAG', 'Operator', 'MAGNET', 'HUB', 1, '20240101')"
        )
        ensure_operator_checkins_schema(conn)
        change_operator_callsign(conn, "N1MAG", "N1NEW", effective_at=1_725_494_400.0)
        conn.commit()
    settings.set("operator_callsign", "N1NEW")
    settings.save()
    when = _now_text()
    events = [
        {"id": "one", "params": {"FROM": "K1AAA", "TO": "N1MAG", "TEXT": "OLD CALL REACHED", "UTC": when}},
        {"id": "two", "params": {"FROM": "K1BBB", "TO": "N1NEW", "TEXT": "NEW CALL REACHED", "UTC": when}},
        {"id": "three", "params": {"FROM": "K1CCC", "TO": "@MAGNET", "TEXT": "GROUP REACHED", "UTC": when}},
        {"id": "four", "params": {"FROM": "K1DDD", "TO": "N1NEW", "TEXT": "HB", "UTC": when}},
        {"id": "five", "params": {"FROM": "K1EEE", "TO": "@OTHER", "TEXT": "IGNORE", "UTC": when}},
    ]
    ingestor = MessageIngestor(settings)
    assert ingestor.ingest_spotter_from_js8_events(
        events,
        source_radio_id="8",
        js8_instance_id="fio-b",
        source_key="api:fio-b",
        evaluate_expect=False,
    ) == 3
    assert ingestor.ingest_spotter_from_js8_events(
        events,
        source_radio_id="8",
        js8_instance_id="fio-b",
        source_key="api:fio-b",
        evaluate_expect=False,
    ) == 0
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT from_call, to_call, raw_text FROM js8_messages ORDER BY from_call"
        ).fetchall()
    assert rows == [
        ("K1AAA", "N1MAG", "OLD CALL REACHED"),
        ("K1BBB", "N1NEW", "NEW CALL REACHED"),
        ("K1CCC", "@MAGNET", "GROUP REACHED"),
    ]
    settings.close()


def test_inbox_and_directed_adapters_collapse_same_semantic_message(
    monkeypatch, tmp_path: Path
) -> None:
    settings = _settings(monkeypatch, tmp_path)
    ingestor = MessageIngestor(settings)
    ingestor._ensure_local_js8_tables()
    utc = _now_text()
    timestamp = datetime.datetime.strptime(utc, "%Y-%m-%d %H:%M:%S").replace(
        tzinfo=datetime.timezone.utc
    ).timestamp()
    assert ingestor._insert_js8_local(
        1, "K1AAA", "N1MAG", "MSG", utc, timestamp, "SAME MESSAGE", "SAME MESSAGE",
        "UNREAD", 0.0, source_key="inbox:fio-a", source_id=1,
        source_radio_id="7", js8_instance_id="fio-a",
    ) is True
    assert ingestor._insert_js8_local(
        2, "K1AAA", "N1MAG", "MSG", utc, timestamp, "SAME MESSAGE", "SAME MESSAGE",
        "UNREAD", 0.0, source_key="directed:fio-a", source_id=2,
        source_radio_id="7", js8_instance_id="fio-a",
    ) is False
    with sqlite3.connect(settings.config_dir / "freqinout_nets.db") as conn:
        assert conn.execute("SELECT COUNT(*) FROM js8_messages").fetchone()[0] == 1
    settings.close()
