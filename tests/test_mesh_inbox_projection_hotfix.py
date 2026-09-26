from __future__ import annotations

import sqlite3
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import pytest

from freqinout.core.mesh import (
    MeshMessage,
    default_policy_for_channel,
    list_mesh_messages,
    store_mesh_message_with_channel_policy,
    upsert_mesh_channel_policy,
)
from freqinout.core.message_projection_coordinator import MessageProjectionCoordinator
from freqinout.core.message_projection_queue import queue_diagnostics
from freqinout.core.message_projection_store import (
    ensure_message_projection_schema,
    query_projected_message_page,
)
from freqinout.core.observation_store import list_observations


def _initialize_projection_schema(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    try:
        ensure_message_projection_schema(conn)
        conn.commit()
    finally:
        conn.close()


def _accepted_policy(
    db_path: Path,
    *,
    adapter_id: str,
    transport: str,
    inbox_enabled: bool = True,
    ops_enabled: bool = True,
) -> None:
    policy = default_policy_for_channel(
        adapter_id=adapter_id,
        transport=transport,
        channel_id="0",
        channel_name="Public",
        channel_role="public",
        review_state="accepted",
    )
    upsert_mesh_channel_policy(
        db_path,
        replace(policy, inbox_enabled=inbox_enabled, ops_enabled=ops_enabled),
    )


def _message(*, adapter_id: str, transport: str, message_id: str, text: str) -> MeshMessage:
    return MeshMessage(
        adapter_id=adapter_id,
        transport=transport,
        message_id=message_id,
        from_node="K7MESH",
        to_node="channel",
        channel="0",
        text=text,
        rx_time=datetime(2026, 9, 25, 12, 0, tzinfo=timezone.utc),
        topics=("Comms",),
    )


def _project_queued(db_path: Path) -> None:
    worker = MessageProjectionCoordinator(db_path)
    try:
        while queue_diagnostics(db_path)["depth"]:
            result = worker.run_once(reconcile=False)
            assert result.state not in {"failed", "cancelled"}
    finally:
        worker.close()


@pytest.mark.parametrize("transport", ("meshcore", "meshtastic"))
def test_policy_approved_mesh_traffic_appears_once_in_canonical_inbox(
    tmp_path: Path,
    transport: str,
) -> None:
    db_path = tmp_path / f"{transport}.db"
    _initialize_projection_schema(db_path)
    _accepted_policy(db_path, adapter_id=f"{transport}-field", transport=transport)
    message = _message(
        adapter_id=f"{transport}-field",
        transport=transport,
        message_id="message-1",
        text=f"{transport} operational update",
    )

    store_mesh_message_with_channel_policy(db_path, message)
    store_mesh_message_with_channel_policy(db_path, message)
    _project_queued(db_path)

    page = query_projected_message_page(
        db_path,
        source_families=(transport,),
        include_total=True,
    )
    assert page.total_count == 1
    assert len(page.rows) == 1
    assert page.rows[0]["source_family"] == transport
    assert page.rows[0]["body_preview"] == f"{transport} operational update"
    assert page.rows[0]["app_instance_id"] == f"{transport}-field"


def test_ops_only_mesh_policy_stays_out_of_inbox_without_harming_ops_projection(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "ops-only.db"
    _initialize_projection_schema(db_path)
    _accepted_policy(
        db_path,
        adapter_id="meshcore-ops",
        transport="meshcore",
        inbox_enabled=False,
        ops_enabled=True,
    )

    store_mesh_message_with_channel_policy(
        db_path,
        _message(
            adapter_id="meshcore-ops",
            transport="meshcore",
            message_id="ops-only-1",
            text="ops-only mesh traffic",
        ),
    )
    _project_queued(db_path)

    assert query_projected_message_page(db_path, include_total=True).total_count == 0
    observations = list_observations(db_path, source_family="meshcore")
    assert len(observations) == 1
    assert observations[0].summary == "ops-only mesh traffic"
    assert observations[0].provenance["surfaces"] == ["ops_center", "map", "topic_scan"]


def test_policy_change_removes_prior_mesh_inbox_projection_but_keeps_ops(tmp_path: Path) -> None:
    db_path = tmp_path / "policy-change.db"
    _initialize_projection_schema(db_path)
    _accepted_policy(db_path, adapter_id="meshcore-policy", transport="meshcore")
    message = _message(
        adapter_id="meshcore-policy",
        transport="meshcore",
        message_id="policy-change-1",
        text="policy-controlled traffic",
    )
    store_mesh_message_with_channel_policy(db_path, message)
    _project_queued(db_path)
    assert query_projected_message_page(db_path, include_total=True).total_count == 1

    _accepted_policy(
        db_path,
        adapter_id="meshcore-policy",
        transport="meshcore",
        inbox_enabled=False,
        ops_enabled=True,
    )
    store_mesh_message_with_channel_policy(db_path, message)
    _project_queued(db_path)

    assert query_projected_message_page(db_path, include_total=True).total_count == 0
    observations = list_observations(db_path, source_family="meshcore")
    assert len(observations) == 1
    assert observations[0].provenance["surfaces"] == ["ops_center", "map", "topic_scan"]


def test_same_transport_message_id_from_two_adapters_keeps_two_receipts(tmp_path: Path) -> None:
    db_path = tmp_path / "adapter-identity.db"
    _initialize_projection_schema(db_path)
    for adapter_id in ("meshcore-east", "meshcore-west"):
        _accepted_policy(db_path, adapter_id=adapter_id, transport="meshcore")
        store_mesh_message_with_channel_policy(
            db_path,
            _message(
                adapter_id=adapter_id,
                transport="meshcore",
                message_id="shared-device-id",
                text=f"traffic received by {adapter_id}",
            ),
        )
    _project_queued(db_path)

    page = query_projected_message_page(db_path, source_family="meshcore", include_total=True)
    assert page.total_count == 2
    assert {row["app_instance_id"] for row in page.rows} == {"meshcore-east", "meshcore-west"}


def test_mesh_reprojection_preserves_operator_read_state(tmp_path: Path) -> None:
    db_path = tmp_path / "read-state.db"
    _initialize_projection_schema(db_path)
    _accepted_policy(db_path, adapter_id="meshcore-read", transport="meshcore")
    message = _message(
        adapter_id="meshcore-read",
        transport="meshcore",
        message_id="read-state-1",
        text="read state survives",
    )
    store_mesh_message_with_channel_policy(db_path, message)
    _project_queued(db_path)

    conn = sqlite3.connect(db_path)
    try:
        conn.execute("UPDATE message_projection SET read_state='read', status='READ'")
        conn.commit()
    finally:
        conn.close()

    store_mesh_message_with_channel_policy(db_path, message)
    _project_queued(db_path)

    page = query_projected_message_page(db_path)
    assert page.rows[0]["read_state"] == "read"
    assert page.rows[0]["status"] == "READ"


def test_retained_mesh_history_is_caught_up_by_bounded_reconciliation(tmp_path: Path) -> None:
    db_path = tmp_path / "history.db"
    _accepted_policy(db_path, adapter_id="meshcore-history", transport="meshcore")
    store_mesh_message_with_channel_policy(
        db_path,
        _message(
            adapter_id="meshcore-history",
            transport="meshcore",
            message_id="history-1",
            text="retained history",
        ),
    )
    assert len(list_mesh_messages(db_path, transport="meshcore")) == 1
    _initialize_projection_schema(db_path)

    worker = MessageProjectionCoordinator(db_path)
    try:
        result = worker.run_once(reconcile=True)
        assert result.discovered == 1
        assert result.committed == 1
    finally:
        worker.close()

    page = query_projected_message_page(db_path, source_family="meshcore", include_total=True)
    assert page.total_count == 1
    assert page.rows[0]["summary"] == "retained history"
