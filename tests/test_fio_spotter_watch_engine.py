from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from freqinout.core.fio_spotter_store import (
    MAX_ENABLED_WATCHES,
    MAX_TOTAL_WATCHES,
    SpotterWatchStoreError,
    list_spotter_watches,
    load_spotter_watch_matcher,
    record_spotter_watch_candidate_matches,
    record_spotter_watch_matches,
    save_spotter_watch,
)
from freqinout.core.fio_spotter_watch_engine import compile_spotter_watch


def _watch(db: Path, index: int, *, enabled: bool = False):
    return save_spotter_watch(
        {"name": f"Watch {index}", "watch_kind": "keyword", "pattern": f"term-{index}", "enabled": enabled},
        db_path=db,
    )


def test_structured_watch_is_canonical_and_anded(tmp_path: Path) -> None:
    db = tmp_path / "nets.db"
    saved = save_spotter_watch(
        {
            "name": "Fire from operator",
            "criteria": {
                "topic": {"pattern": " Fire ", "match_mode": "whole-word"},
                "callsign": {"pattern": "w1abc", "match_mode": "exact"},
            },
            "source_families": ["Spotter"],
        },
        db_path=db,
    )
    row = list_spotter_watches(db_path=db)[0]
    assert row["watch_kind"] == "structured"
    assert [condition["kind"] for condition in row["criteria"]] == ["callsign", "topic"]
    matcher = load_spotter_watch_matcher(db_path=db)
    candidate = {"from_call": "W1ABC", "topics": ["Fire"], "source_family": "spotter"}
    assert matcher.match(candidate, now_ts=100) == (saved.id,)
    assert matcher.match({**candidate, "topics": ["Weather"]}, now_ts=100) == ()
    assert matcher.match({**candidate, "from_call": "K2XYZ"}, now_ts=100) == ()


def test_legacy_watch_compiles_and_source_scope_is_applied_when_known(tmp_path: Path) -> None:
    db = tmp_path / "nets.db"
    save_spotter_watch(
        {"name": "Watch 1", "watch_kind": "keyword", "pattern": "term-1", "enabled": True, "source_families": ["spotter"]},
        db_path=db,
    )
    matcher = load_spotter_watch_matcher(db_path=db)
    assert matcher.match({"body_text": "term-1", "source_family": "spotter"}) == (1,)
    assert matcher.match({"body_text": "term-1", "source_family": "commstat"}) == ()
    # Match mode and legacy shape remain accepted by the pure compiler.
    assert compile_spotter_watch({"watch_kind": "keyword", "pattern": "term", "match_mode": "contains"})


def test_duplicate_detection_normalizes_structured_order_and_legacy_text(tmp_path: Path) -> None:
    db = tmp_path / "nets.db"
    save_spotter_watch({"name": "First", "watch_kind": "keyword", "pattern": " Wildfire "}, db_path=db)
    with pytest.raises(SpotterWatchStoreError) as legacy:
        save_spotter_watch({"name": "Second", "watch_kind": "keyword", "pattern": "wildfire"}, db_path=db)
    assert legacy.value.code == "duplicate"
    save_spotter_watch(
        {"name": "Structured", "criteria": {"callsign": "W1ABC", "topic": "Fire"}}, db_path=db
    )
    with pytest.raises(SpotterWatchStoreError) as structured:
        save_spotter_watch(
            {"name": "Again", "criteria": [{"kind": "topic", "pattern": " fire "}, {"kind": "callsign", "pattern": "w1abc"}]},
            db_path=db,
        )
    assert structured.value.code == "duplicate"


def test_enabled_and_total_caps_are_enforced(tmp_path: Path) -> None:
    db = tmp_path / "nets.db"
    for index in range(MAX_ENABLED_WATCHES):
        _watch(db, index, enabled=True)
    with pytest.raises(SpotterWatchStoreError) as enabled:
        _watch(db, 100, enabled=True)
    assert enabled.value.code == "enabled_cap"
    # Disabled rows may fill the separate total cap, but not the enabled cap.
    for index in range(MAX_ENABLED_WATCHES, MAX_TOTAL_WATCHES):
        _watch(db, index, enabled=False)
    with pytest.raises(SpotterWatchStoreError) as total:
        _watch(db, MAX_TOTAL_WATCHES, enabled=False)
    assert total.value.code == "total_cap"
    assert len(list_spotter_watches(db_path=db, limit=MAX_TOTAL_WATCHES)) == MAX_TOTAL_WATCHES


def test_concurrent_saves_cannot_cross_enabled_cap(tmp_path: Path) -> None:
    db = tmp_path / "nets.db"
    for index in range(MAX_ENABLED_WATCHES - 5):
        _watch(db, index, enabled=True)

    def save(index: int):
        try:
            _watch(db, 1000 + index, enabled=True)
            return "saved"
        except SpotterWatchStoreError as exc:
            return exc.code

    with ThreadPoolExecutor(max_workers=10) as pool:
        outcomes = list(pool.map(save, range(10)))
    assert outcomes.count("saved") == 5
    assert sum(1 for row in list_spotter_watches(db_path=db) if row["enabled"]) == MAX_ENABLED_WATCHES


def test_batch_match_persistence_updates_counts_once_per_watch(tmp_path: Path) -> None:
    db = tmp_path / "nets.db"
    first = _watch(db, 1, enabled=True)
    second = _watch(db, 2, enabled=True)
    assert record_spotter_watch_matches({first.id: 3, second.id: 2}, matched_ts=1234.0, db_path=db) == 2
    rows = {row["id"]: row for row in list_spotter_watches(db_path=db)}
    assert rows[first.id]["match_count"] == 3
    assert rows[second.id]["match_count"] == 2
    assert rows[first.id]["last_match_ts"] == 1234.0


def test_candidate_match_persistence_is_idempotent_across_projection_retry(tmp_path: Path) -> None:
    db = tmp_path / "nets.db"
    first = _watch(db, 1, enabled=True)
    second = _watch(db, 2, enabled=True)
    batch = {"message-1": (first.id, second.id), "message-2": (first.id,)}

    assert record_spotter_watch_candidate_matches(batch, matched_ts=100.0, db_path=db) == 2
    assert record_spotter_watch_candidate_matches(batch, matched_ts=200.0, db_path=db) == 0
    rows = {row["id"]: row for row in list_spotter_watches(db_path=db)}
    assert rows[first.id]["match_count"] == 2
    assert rows[second.id]["match_count"] == 1
    assert rows[first.id]["last_match_ts"] == 100.0
