from __future__ import annotations

import sqlite3
from pathlib import Path

from freqinout.core.ingest_health import build_ingest_health_snapshot, source_health_key
from freqinout.core.ingest_source_model import AppInstanceDescriptor, IngestSourceDescriptor, IngestSourceInventory
from freqinout.core.ingest_runtime_status import (
    RuntimeIngestStatusRow,
    active_runtime_source_view_rows,
    enrich_ingest_status_rows_with_js8_endpoint_collisions,
    enrich_ingest_status_rows_with_js8_registry,
    enrich_ingest_status_rows_with_projection_counts,
    enrich_ingest_status_rows_with_varac_sync,
    ingest_status_rows,
    runtime_source_view_rows_from_skip_reasons,
    runtime_source_view_rows,
    summarize_runtime_source_view_rows,
)


def test_missing_file_source_is_degraded(tmp_path: Path) -> None:
    source = IngestSourceDescriptor(
        source_id="js8_a_directed",
        family="js8call",
        source_type="file",
        label="FIO-A DIRECTED",
        path=str(tmp_path / "missing" / "DIRECTED.TXT"),
    )

    snapshot = build_ingest_health_snapshot(IngestSourceInventory(ingest_sources=(source,)), now_ts=100.0)

    assert snapshot.missing_count == 1
    assert snapshot.degraded_count == 1
    assert snapshot.sources[0].exists is False
    assert snapshot.sources[0].last_error == "source path missing"


def test_api_source_uses_endpoint_as_configured_fingerprint() -> None:
    source = IngestSourceDescriptor(
        source_id="js8_api",
        family="js8call",
        source_type="api",
        label="FIO-A JS8 API",
        endpoint="127.0.0.1:2442",
    )

    snapshot = build_ingest_health_snapshot(IngestSourceInventory(ingest_sources=(source,)))

    assert snapshot.sources[0].exists is True
    assert snapshot.sources[0].fingerprint == ("api", "127.0.0.1:2442", "configured")


def test_js8_endpoint_collision_marks_api_status_degraded() -> None:
    inventory = IngestSourceInventory(
        app_instances=(
            AppInstanceDescriptor(
                source_id="app-a",
                family="js8call",
                label="FIO-A JS8Call",
                radio_id="A",
                api_host="127.0.0.1",
                api_port=2442,
            ),
            AppInstanceDescriptor(
                source_id="app-b",
                family="js8call",
                label="FIO-B JS8Call",
                radio_id="B",
                api_host="127.0.0.1",
                api_port=2442,
            ),
        )
    )
    row = RuntimeIngestStatusRow(
        source_id="api-a",
        family="js8call",
        source_type="api",
        label="FIO-A JS8Call API",
        location="127.0.0.1:2442",
        detail="API configured; shared client idle",
    )

    enriched = enrich_ingest_status_rows_with_js8_endpoint_collisions((row,), inventory)

    assert enriched[0].status == "degraded"
    assert enriched[0].status_label == "Needs attention"
    assert "Endpoint shared by FIO-A JS8Call, FIO-B JS8Call" in enriched[0].detail
    assert enriched[0].metadata["endpoint_collision_labels"] == ("FIO-A JS8Call", "FIO-B JS8Call")


def test_runtime_source_view_rows_summarize_shared_endpoint_for_ui() -> None:
    row = RuntimeIngestStatusRow(
        source_id="api-a",
        family="js8call",
        source_type="api",
        label="FIO-A JS8Call API",
        location="127.0.0.1:2442",
        status="degraded",
        status_label="Needs attention",
        detail="API configured; shared client idle; Endpoint shared by FIO-A JS8Call, FIO-B JS8Call",
        metadata={"endpoint_collision_labels": ("FIO-A JS8Call", "FIO-B JS8Call")},
    )

    view = runtime_source_view_rows((row,))[0]

    assert view.title == "FIO-A JS8Call API"
    assert view.source_kind == "JS8Call API"
    assert view.state == "shared_endpoint"
    assert view.state_label == "Shared Endpoint"
    assert view.severity == "warning"
    assert view.action_hint == "Give each JS8Call instance a unique TCP port"


def test_active_runtime_source_view_rows_wraps_status_pipeline(monkeypatch) -> None:
    import freqinout.core.ingest_runtime_status as status_module

    monkeypatch.setattr(
        status_module,
        "active_runtime_ingest_status_rows",
        lambda **_kwargs: (
            RuntimeIngestStatusRow(
                source_id="api-a",
                family="js8call",
                source_type="api",
                label="FIO-A JS8Call API",
                status="ok",
                detail="API configured; shared client idle",
            ),
        ),
    )

    rows = active_runtime_source_view_rows()

    assert len(rows) == 1
    assert rows[0].source_kind == "JS8Call API"
    assert rows[0].state == "idle"


def test_runtime_source_view_rows_summarize_missing_and_idle_for_ui() -> None:
    missing = RuntimeIngestStatusRow(
        source_id="directed-a",
        family="js8call",
        source_type="file",
        label="FIO-A JS8Call DIRECTED",
        status="missing",
        status_label="Missing",
        detail="source path missing",
        metadata={"role": "directed"},
    )
    idle = RuntimeIngestStatusRow(
        source_id="api-a",
        family="js8call",
        source_type="api",
        label="FIO-A JS8Call API",
        status="ok",
        status_label="Ready",
        detail="API configured; shared client idle",
    )

    view = runtime_source_view_rows((idle, missing))

    assert [row.state for row in view] == ["missing", "idle"]
    assert view[0].source_kind == "JS8Call DIRECTED.TXT"
    assert view[0].detail == "Path not found"
    assert view[1].detail == "Configured; connects when needed"
    assert view[1].action_hint == "Client will connect when needed"


def test_runtime_source_view_rows_mark_stale_file_source_as_quiet_not_alarm(tmp_path: Path) -> None:
    path = tmp_path / "DIRECTED.TXT"
    path.write_text("hello\n", encoding="utf-8")
    source = IngestSourceDescriptor(
        source_id="directed-a",
        family="js8call",
        source_type="file",
        label="FIO-A JS8Call DIRECTED",
        path=str(path),
    )
    health_key = source_health_key(source)
    snapshot = build_ingest_health_snapshot(
        IngestSourceInventory(ingest_sources=(source,)),
        health_registry_snapshot={
            health_key: {
                "last_success_ts": 100.0,
                "last_checked_ts": 100.0,
            }
        },
        now_ts=2100.0,
    )

    view = runtime_source_view_rows(ingest_status_rows(snapshot))[0]

    assert view.state == "stale"
    assert view.state_label == "Stale"
    assert view.severity == "info"
    assert view.action_hint == "Source is quiet or has not refreshed recently"
    assert view.metadata["freshness_label"] == "Stale"
    assert view.last_activity_label == "33 min ago"


def test_runtime_source_view_rows_keep_recent_file_source_ready(tmp_path: Path) -> None:
    path = tmp_path / "DIRECTED.TXT"
    path.write_text("hello\n", encoding="utf-8")
    source = IngestSourceDescriptor(
        source_id="directed-a",
        family="js8call",
        source_type="file",
        label="FIO-A JS8Call DIRECTED",
        path=str(path),
    )
    health_key = source_health_key(source)
    snapshot = build_ingest_health_snapshot(
        IngestSourceInventory(ingest_sources=(source,)),
        health_registry_snapshot={
            health_key: {
                "last_success_ts": 1900.0,
                "last_checked_ts": 1900.0,
            }
        },
        now_ts=2000.0,
    )

    view = runtime_source_view_rows(ingest_status_rows(snapshot))[0]

    assert view.state == "ready"
    assert view.state_label == "Ready"
    assert view.severity == "ok"
    assert view.metadata["freshness_label"] == "Fresh"


def test_runtime_source_view_rows_mark_quiet_file_source_as_informational(tmp_path: Path) -> None:
    path = tmp_path / "DIRECTED.TXT"
    path.write_text("hello\n", encoding="utf-8")
    source = IngestSourceDescriptor(
        source_id="directed-a",
        family="js8call",
        source_type="file",
        label="FIO-A JS8Call DIRECTED",
        path=str(path),
    )
    health_key = source_health_key(source)
    snapshot = build_ingest_health_snapshot(
        IngestSourceInventory(ingest_sources=(source,)),
        health_registry_snapshot={
            health_key: {
                "last_success_ts": 1000.0,
                "last_checked_ts": 1000.0,
            }
        },
        now_ts=1700.0,
    )

    view = runtime_source_view_rows(ingest_status_rows(snapshot))[0]

    assert view.state == "quiet"
    assert view.state_label == "Quiet"
    assert view.severity == "info"
    assert view.action_hint == "Source has been quiet but is still within the expected refresh window"
    assert view.metadata["freshness_label"] == "Quiet"
    assert view.last_activity_label == "11 min ago"


def test_runtime_source_summary_counts_stale_sources() -> None:
    rows = runtime_source_view_rows(
        (
            RuntimeIngestStatusRow(
                source_id="directed-a",
                family="js8call",
                source_type="file",
                label="FIO-A JS8Call DIRECTED",
                status="ok",
                detail="Source path available",
                metadata={"freshness_state": "stale", "freshness_label": "Stale"},
            ),
        )
    )

    summary = summarize_runtime_source_view_rows(rows)

    assert summary.stale == 1
    assert summary.headline == "1 source stale"


def test_runtime_source_summary_counts_quiet_sources_without_alerting() -> None:
    rows = runtime_source_view_rows(
        (
            RuntimeIngestStatusRow(
                source_id="directed-a",
                family="js8call",
                source_type="file",
                label="FIO-A JS8Call DIRECTED",
                status="ok",
                detail="Source path available",
                metadata={"freshness_state": "quiet", "freshness_label": "Quiet"},
            ),
        )
    )

    summary = summarize_runtime_source_view_rows(rows)

    assert summary.quiet == 1
    assert summary.info == 1
    assert summary.warning == 0
    assert summary.headline == "1 source quiet"


def test_runtime_source_view_rows_summarize_connected_api_ready() -> None:
    row = RuntimeIngestStatusRow(
        source_id="api-a",
        family="js8call",
        source_type="api",
        label="FIO-A JS8Call API",
        status="ok",
        status_label="Ready",
        detail="Shared JS8 API client connected",
        metadata={"api_status": {"connected": True, "running": True}},
    )

    view = runtime_source_view_rows((row,))[0]

    assert view.state == "ready"
    assert view.state_label == "Ready"
    assert view.severity == "ok"
    assert view.action_hint == ""


def test_runtime_source_view_rows_show_operator_readable_js8_link_summary() -> None:
    row = RuntimeIngestStatusRow(
        source_id="directed-a",
        family="js8call",
        source_type="file",
        label="FIO-A JS8Call DIRECTED",
        status="ok",
        detail="Source path available; 2 projected links",
        metadata={"role": "directed", "link_projection_summary": {"total": 2, "station_pairs": 2}},
    )

    view = runtime_source_view_rows((row,))[0]

    assert view.detail == "Path found; 2 heard paths across 2 station pairs"


def test_runtime_source_view_rows_show_operator_readable_commstat_summary() -> None:
    row = RuntimeIngestStatusRow(
        source_id="commstat-a",
        family="commstat",
        source_type="sqlite",
        label="FIO-A CommStat",
        status="ok",
        detail="Active groups: MAGNET; 3 projected artifacts",
        metadata={
            "projection_summary": {
                "total": 3,
                "transport_labels": {"JS8/RF": 1, "Internet": 1, "JS8/RF + Internet": 1},
                "reach_labels": {
                    "Limited Reach (RF only)": 1,
                    "Maximum Reach (RF + Internet)": 1,
                    "Maximum Reach relay": 1,
                },
            }
        },
    )

    view = runtime_source_view_rows((row,))[0]

    assert view.detail == (
        "Active groups: MAGNET; 3 CommStat artifacts; "
        "Transport: Internet 1, JS8/RF 1, JS8/RF + Internet 1; "
        "Reach: Limited Reach (RF only) 1, Maximum Reach (RF + Internet) 1, Maximum Reach relay 1"
    )


def test_runtime_source_view_rows_summarize_backoff_as_non_alarm() -> None:
    row = RuntimeIngestStatusRow(
        source_id="inbox-a",
        family="js8call",
        source_type="sqlite",
        label="FIO-A JS8Call Inbox",
        status="degraded",
        status_label="Needs attention",
        detail="cooldown 12s",
        metadata={"role": "inbox", "skip_reason": "backoff"},
    )

    view = runtime_source_view_rows((row,))[0]

    assert view.source_kind == "JS8Call Inbox"
    assert view.state == "backoff"
    assert view.state_label == "Backoff"
    assert view.severity == "info"
    assert view.action_hint == "Waiting before retry"


def test_runtime_source_view_rows_from_skip_reasons_use_same_ui_vocabulary() -> None:
    import time

    now = time.time()
    rows = runtime_source_view_rows_from_skip_reasons(
        {
            "source-a:inbox": {
                "source_id": "source-a",
                "label": "FIO-A JS8Call Inbox",
                "family": "js8call",
                "source_type": "js8-inbox",
                "reason": "missing",
                "path": "/tmp/missing-inbox.db",
                "skipped_at_ts": now - 90,
            },
            "source-b:spotter": {
                "source_id": "source-b",
                "label": "FIO-B Spotter",
                "family": "js8call",
                "source_type": "spotter-directed",
                "reason": "backoff",
                "cooldown_remaining_sec": 12,
                "path": "/tmp/DIRECTED.TXT",
                "skipped_at_ts": now - 5,
            },
        }
    )

    assert [(row.state, row.source_kind, row.detail) for row in rows] == [
        ("missing", "JS8Call Inbox", "JS8Call inbox database was not found"),
        ("backoff", "JS8Spotter DIRECTED.TXT", "Retry in 12s"),
    ]
    assert rows[0].severity == "warning"
    assert rows[1].severity == "info"
    assert rows[0].last_activity_label == "1 min ago"
    assert rows[1].last_activity_label == "Just now"


def test_summarize_runtime_source_view_rows_counts_operator_states() -> None:
    rows = runtime_source_view_rows(
        (
            RuntimeIngestStatusRow(
                source_id="api-a",
                family="js8call",
                source_type="api",
                label="FIO-A JS8Call API",
                status="ok",
                detail="Shared JS8 API client connected",
                metadata={"api_status": {"connected": True}},
            ),
            RuntimeIngestStatusRow(
                source_id="api-b",
                family="js8call",
                source_type="api",
                label="FIO-B JS8Call API",
                status="degraded",
                detail="Endpoint shared by FIO-A JS8Call, FIO-B JS8Call",
                metadata={"endpoint_collision_labels": ("FIO-A JS8Call", "FIO-B JS8Call")},
            ),
            RuntimeIngestStatusRow(
                source_id="api-c",
                family="js8call",
                source_type="api",
                label="FIO-C JS8Call API",
                status="ok",
                detail="API configured; shared client idle",
            ),
            RuntimeIngestStatusRow(
                source_id="directed-a",
                family="js8call",
                source_type="file",
                label="FIO-A JS8Call DIRECTED",
                status="ok",
                detail="Source path available",
                metadata={"freshness_state": "quiet", "freshness_label": "Quiet"},
            ),
        )
    )

    summary = summarize_runtime_source_view_rows(rows)

    assert summary.total == 4
    assert summary.ready == 1
    assert summary.shared_endpoint == 1
    assert summary.idle == 1
    assert summary.quiet == 1
    assert summary.warning == 1
    assert summary.headline == "1 source needs attention"


def test_source_health_reads_registry_snapshot(tmp_path: Path) -> None:
    path = tmp_path / "DIRECTED.TXT"
    path.write_text("hello\n", encoding="utf-8")
    source = IngestSourceDescriptor(
        source_id="js8_a_directed",
        family="js8call",
        source_type="file",
        label="FIO-A DIRECTED",
        path=str(path),
    )
    health_key = source_health_key(source)

    snapshot = build_ingest_health_snapshot(
        IngestSourceInventory(ingest_sources=(source,)),
        health_registry_snapshot={
            health_key: {
                "degraded": True,
                "last_success_ts": 80.0,
                "last_failure_ts": 90.0,
                "last_checked_ts": 90.0,
                "last_error": "timeout",
            }
        },
        now_ts=100.0,
    )

    health = snapshot.sources[0]
    assert health.exists is True
    assert health.degraded is True
    assert health.last_success_ts == 80.0
    assert health.last_error == "timeout"


def test_source_health_rolls_up_component_state(tmp_path: Path) -> None:
    path = tmp_path / "DIRECTED.TXT"
    path.write_text("hello\n", encoding="utf-8")
    source = IngestSourceDescriptor(
        source_id="js8_a_directed",
        family="js8call",
        source_type="file",
        label="FIO-A DIRECTED",
        path=str(path),
    )
    health_key = source_health_key(source)

    snapshot = build_ingest_health_snapshot(
        IngestSourceInventory(ingest_sources=(source,)),
        health_registry_snapshot={
            f"{health_key}:inbox": {
                "degraded": True,
                "last_success_ts": 70.0,
                "last_failure_ts": 95.0,
                "last_checked_ts": 95.0,
                "last_error": "inbox busy",
            }
        },
        now_ts=100.0,
    )

    health = snapshot.sources[0]
    assert health.exists is True
    assert health.degraded is True
    assert health.last_error == "inbox busy"
    assert health.components[0].name == "inbox"


def test_ingest_status_rows_are_operator_readable(tmp_path: Path) -> None:
    path = tmp_path / "DIRECTED.TXT"
    path.write_text("hello\n", encoding="utf-8")
    source = IngestSourceDescriptor(
        source_id="js8_a_directed",
        family="js8call",
        source_type="file",
        label="FIO-A DIRECTED",
        path=str(path),
        radio_id="1",
    )
    health_key = source_health_key(source)
    snapshot = build_ingest_health_snapshot(
        IngestSourceInventory(ingest_sources=(source,)),
        health_registry_snapshot={
            health_key: {
                "degraded": False,
                "last_success_ts": 40.0,
                "last_checked_ts": 40.0,
            }
        },
        now_ts=100.0,
    )

    rows = ingest_status_rows(snapshot)

    assert rows[0].status == "ok"
    assert rows[0].status_label == "Ready"
    assert rows[0].detail == "Source path available"
    assert rows[0].last_activity_label == "1 min ago"


def test_commstat_status_rows_show_active_group_context(tmp_path: Path) -> None:
    db_path = tmp_path / "traffic.db3"
    db_path.write_text("", encoding="utf-8")
    source = IngestSourceDescriptor(
        source_id="commstat_a",
        family="commstat",
        source_type="sqlite",
        label="FIO-A CommStat",
        path=str(db_path),
        metadata={"configured_groups": ("MAGNET", "MR08"), "active_groups": ("MAGNET",)},
    )
    snapshot = build_ingest_health_snapshot(IngestSourceInventory(ingest_sources=(source,)), now_ts=100.0)

    rows = ingest_status_rows(snapshot)

    assert rows[0].detail == "Active groups: MAGNET"


def test_varac_sync_history_enriches_runtime_status_rows() -> None:
    rows = (
        RuntimeIngestStatusRow(
            source_id="varac-a",
            label="FIO-A VarAC",
            family="varac",
            source_type="sqlite",
            status="unknown",
            status_label="Unknown",
            detail="Source path available",
        ),
    )

    enriched = enrich_ingest_status_rows_with_varac_sync(
        rows,
        {
            "varac-a": {
                "success": 1,
                "rows_scanned": 12,
                "rows_written": 5,
                "run_finished_ts": 40.0,
            }
        },
        generated_ts=100.0,
    )

    assert enriched[0].status == "ok"
    assert enriched[0].status_label == "Ready"
    assert enriched[0].detail == "Last sync scanned 12, wrote 5"
    assert enriched[0].last_activity_label == "1 min ago"


def test_js8_registry_enrichment_marks_connected_api_source_ready() -> None:
    rows = (
        RuntimeIngestStatusRow(
            source_id="js8-api-a",
            label="FIO-A JS8Call API",
            family="js8call",
            source_type="api",
            status="unknown",
            status_label="Unknown",
            detail="API endpoint configured",
            location="127.0.0.1:2442",
        ),
    )

    enriched = enrich_ingest_status_rows_with_js8_registry(
        rows,
        [
            {
                "key": "127.0.0.1:2442",
                "running": True,
                "connected": True,
                "last_connected_ts": 40.0,
                "last_message_ts": 60.0,
            }
        ],
        generated_ts=120.0,
    )

    assert enriched[0].status == "ok"
    assert enriched[0].status_label == "Ready"
    assert enriched[0].detail == "Shared JS8 API client connected"
    assert enriched[0].last_activity_label == "1 min ago"
    assert enriched[0].metadata["api_status"]["connected"] is True


def test_js8_registry_enrichment_keeps_idle_api_source_non_degraded() -> None:
    rows = (
        RuntimeIngestStatusRow(
            source_id="js8-api-a",
            label="FIO-A JS8Call API",
            family="js8call",
            source_type="api",
            status="ok",
            status_label="Ready",
            detail="API endpoint configured",
            location="127.0.0.1:2442",
        ),
    )

    enriched = enrich_ingest_status_rows_with_js8_registry(
        rows,
        [{"host": "127.0.0.1", "port": 2442, "running": False, "connected": False}],
        generated_ts=120.0,
    )

    assert enriched[0].status == "ok"
    assert enriched[0].status_label == "Ready"
    assert enriched[0].detail == "API configured; shared client idle"


def test_js8_registry_enrichment_marks_running_disconnected_api_source_degraded() -> None:
    rows = (
        RuntimeIngestStatusRow(
            source_id="js8-api-a",
            label="FIO-A JS8Call API",
            family="js8call",
            source_type="api",
            status="ok",
            status_label="Ready",
            detail="API endpoint configured",
            location="127.0.0.1:2442",
        ),
    )

    enriched = enrich_ingest_status_rows_with_js8_registry(
        rows,
        [
            {
                "key": "127.0.0.1:2442",
                "running": True,
                "connected": False,
                "last_error": "connect_failed:refused",
            }
        ],
        generated_ts=120.0,
    )

    assert enriched[0].status == "degraded"
    assert enriched[0].status_label == "Needs attention"
    assert enriched[0].detail == "connect_failed:refused"


def test_projection_count_enrichment_uses_file_metadata_source_id(tmp_path: Path) -> None:
    db_path = tmp_path / "fio.db"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE message_file_metadata (
                origin TEXT,
                path TEXT,
                mtime REAL,
                size INTEGER,
                source_id TEXT
            )
            """
        )
        conn.execute("INSERT INTO message_file_metadata (origin, path, mtime, size, source_id) VALUES ('flmsg', 'a.k2s', 1, 2, 'flmsg-a')")
        conn.execute("INSERT INTO message_file_metadata (origin, path, mtime, size, source_id) VALUES ('flmsg', 'b.k2s', 2, 3, 'flmsg-a')")
        conn.commit()
    finally:
        conn.close()
    rows = (
        RuntimeIngestStatusRow(
            source_id="flmsg-a",
            label="FIO-A FLMSG",
            family="flmsg",
            source_type="directory",
            detail="Source path available",
        ),
    )

    enriched = enrich_ingest_status_rows_with_projection_counts(rows, db_path=db_path)

    assert enriched[0].projection_count == 2
    assert enriched[0].detail == "Source path available; 2 cached messages"


def test_projection_count_enrichment_matches_js8_app_instance_key(tmp_path: Path) -> None:
    db_path = tmp_path / "fio.db"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("CREATE TABLE js8_messages (id INTEGER PRIMARY KEY, source_key TEXT)")
        conn.execute("INSERT INTO js8_messages (source_key) VALUES ('app-js8-a')")
        conn.execute("INSERT INTO js8_messages (source_key) VALUES ('app-js8-a')")
        conn.execute("INSERT INTO js8_messages (source_key) VALUES ('app-js8-b')")
        conn.commit()
    finally:
        conn.close()
    rows = (
        RuntimeIngestStatusRow(
            source_id="directed-child-a",
            app_instance_id="app-js8-a",
            label="FIO-A JS8Call DIRECTED",
            family="js8call",
            source_type="file",
            detail="Source path available",
        ),
    )

    enriched = enrich_ingest_status_rows_with_projection_counts(rows, db_path=db_path)

    assert enriched[0].projection_count == 2
    assert enriched[0].detail == "Source path available; 2 cached messages"


def test_projection_count_enrichment_adds_js8_link_summary_by_source(tmp_path: Path) -> None:
    db_path = tmp_path / "fio.db"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE js8_links (
                ts REAL,
                origin TEXT,
                destination TEXT,
                band TEXT,
                source_id TEXT,
                app_instance_id TEXT,
                source_radio_id TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO js8_links (ts, origin, destination, band, source_id, app_instance_id, source_radio_id)
            VALUES
                (10, 'K7AAA', 'N1MAG', '40M', 'directed-a', 'app-a', '1'),
                (20, 'K7BBB', 'N1MAG', '40M', 'directed-a', 'app-a', '1'),
                (30, 'K7CCC', 'N2MAG', '20M', 'directed-b', 'app-b', '2')
            """
        )
        conn.commit()
    finally:
        conn.close()
    rows = (
        RuntimeIngestStatusRow(
            source_id="directed-a",
            app_instance_id="app-a",
            label="FIO-A JS8Call DIRECTED",
            family="js8call",
            source_type="file",
            detail="Source path available",
        ),
    )

    enriched = enrich_ingest_status_rows_with_projection_counts(rows, db_path=db_path)

    assert enriched[0].projection_count == 0
    assert enriched[0].detail == "Source path available; 2 projected links"
    summary = enriched[0].metadata["link_projection_summary"]
    assert summary["total"] == 2
    assert summary["station_pairs"] == 2
    assert summary["newest_ts"] == 20


def test_projection_count_enrichment_adds_js8_link_summary_by_app_instance(tmp_path: Path) -> None:
    db_path = tmp_path / "fio.db"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE js8_links (
                ts REAL,
                origin TEXT,
                destination TEXT,
                band TEXT,
                source_id TEXT,
                app_instance_id TEXT,
                source_radio_id TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO js8_links (ts, origin, destination, band, source_id, app_instance_id, source_radio_id)
            VALUES
                (10, 'K7AAA', 'N1MAG', '40M', 'directed-a', 'app-a', '1'),
                (20, 'K7BBB', 'N1MAG', '40M', 'directed-a', 'app-a', '1')
            """
        )
        conn.commit()
    finally:
        conn.close()
    rows = (
        RuntimeIngestStatusRow(
            source_id="all-a",
            app_instance_id="app-a",
            label="FIO-A JS8Call ALL",
            family="js8call",
            source_type="file",
            detail="Source path available",
        ),
    )

    enriched = enrich_ingest_status_rows_with_projection_counts(rows, db_path=db_path)

    assert enriched[0].detail == "Source path available; 2 projected links"
    assert enriched[0].metadata["link_projection_summary"]["keys"]["app_instance_id"] == "app-a"


def test_projection_count_enrichment_uses_varac_ingest_source_key(tmp_path: Path) -> None:
    db_path = tmp_path / "fio.db"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("CREATE TABLE varac_messages (ingest_source_key TEXT, source TEXT, id INTEGER)")
        conn.execute("INSERT INTO varac_messages (ingest_source_key, source, id) VALUES ('varac-a', 'vmail', 1)")
        conn.execute("INSERT INTO varac_messages (ingest_source_key, source, id) VALUES ('varac-a', 'qso', 1)")
        conn.execute("INSERT INTO varac_messages (ingest_source_key, source, id) VALUES ('varac-b', 'vmail', 1)")
        conn.commit()
    finally:
        conn.close()
    rows = (
        RuntimeIngestStatusRow(
            source_id="varac-a",
            label="FIO-A VarAC",
            family="varac",
            source_type="sqlite",
            detail="Source path available",
        ),
    )

    enriched = enrich_ingest_status_rows_with_projection_counts(rows, db_path=db_path)

    assert enriched[0].projection_count == 2
    assert enriched[0].detail == "Source path available; 2 cached messages"


def test_commstat_projection_summary_enriches_runtime_status(tmp_path: Path) -> None:
    db_path = tmp_path / "fio.db"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE commstat_artifacts (
                id INTEGER PRIMARY KEY,
                artifact_kind TEXT,
                transport_mode TEXT,
                reach_mode TEXT,
                origin_path TEXT,
                source_last TEXT,
                report_group TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO commstat_artifacts (artifact_kind, transport_mode, reach_mode, origin_path, source_last, report_group)
            VALUES
                ('STATREP', 'js8+internet', 'maximum_reach', 'rf', 'COMMSTAT3', 'MAGNET'),
                ('MESSAGE', 'js8', 'rf_observed', 'rf', 'COMMSTAT3', 'MAGNET'),
                ('MESSAGE', 'internet', 'maximum_reach_relay', 'commstat_server', 'COMMSTAT3', 'MR08')
            """
        )
        conn.commit()
    finally:
        conn.close()
    rows = (
        RuntimeIngestStatusRow(
            source_id="commstat-a",
            label="FIO-A CommStat",
            family="commstat",
            source_type="sqlite",
            detail="Active groups: MAGNET",
        ),
    )

    enriched = enrich_ingest_status_rows_with_projection_counts(rows, db_path=db_path)

    assert enriched[0].projection_count == 3
    assert enriched[0].detail == "Active groups: MAGNET; 3 projected artifacts"
    summary = enriched[0].metadata["projection_summary"]
    assert summary["artifact_counts"] == {"message": 2, "statrep": 1}
    assert summary["transport_counts"] == {"internet": 1, "js8": 1, "js8+internet": 1}
    assert summary["transport_labels"] == {"Internet": 1, "JS8/RF": 1, "JS8/RF + Internet": 1}
    assert summary["reach_counts"] == {"maximum_reach": 1, "maximum_reach_relay": 1, "rf_observed": 1}
    assert summary["reach_labels"] == {
        "Limited Reach (RF only)": 1,
        "Maximum Reach (RF + Internet)": 1,
        "Maximum Reach relay": 1,
    }
    assert summary["origin_counts"] == {"commstat_server": 1, "rf": 2}
    assert summary["source_counts"] == {"commstat3": 3}
    assert summary["group_counts"] == {"magnet": 2, "mr08": 1}


def test_projection_fingerprint_changes_when_projection_table_changes(tmp_path: Path) -> None:
    db_path = tmp_path / "fio.db"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("CREATE TABLE js8_messages (id INTEGER PRIMARY KEY, state TEXT, ts REAL)")
        conn.execute("INSERT INTO js8_messages (state, ts) VALUES ('NEW', 100.0)")
        conn.commit()
    finally:
        conn.close()

    before = build_ingest_health_snapshot(
        IngestSourceInventory(),
        db_path=db_path,
        projection_table_sets=(("messages", ("js8_messages",)),),
    )
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("INSERT INTO js8_messages (state, ts) VALUES ('READ', 120.0)")
        conn.commit()
    finally:
        conn.close()
    after = build_ingest_health_snapshot(
        IngestSourceInventory(),
        db_path=db_path,
        projection_table_sets=(("messages", ("js8_messages",)),),
    )

    assert before.projections[0].fingerprint != after.projections[0].fingerprint
    assert before.station_fingerprint != after.station_fingerprint
