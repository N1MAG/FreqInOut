"""Regression coverage for explicit legacy message-index convergence."""

from __future__ import annotations

import sqlite3

from freqinout.core.message_projection_repair import plan_legacy_projection_merges
from freqinout.core.message_projection_store import (
    ExternalMessageRef,
    MessageArtifactRecord,
    MessageProjectionRecord,
    ensure_message_projection_schema,
    upsert_external_ref,
    upsert_message_artifact,
    upsert_message_projection,
)
from freqinout.core.message_projection_writer import ProjectionBundleWriter


def _message(
    message_id: str,
    *,
    family: str,
    version: int,
    body: str = "same message",
    read: bool = False,
    pinned: bool = False,
) -> MessageProjectionRecord:
    return MessageProjectionRecord(
        message_id=message_id,
        canonical_key=f"legacy:{family}:{message_id}",
        content_hash=f"hash-{message_id}",
        primary_source_id=f"{family}:source",
        source_family=family,
        source_label=family.upper(),
        message_type="MSG",
        status="READ" if read else "UNREAD",
        read_state="read" if read else "unread",
        from_call="N0CALL",
        to_call="N1MAG",
        event_ts=1_800_000_000.25,
        received_ts=1_800_000_000.25,
        subject=body,
        summary=body,
        body_preview=body,
        search_text=body,
        pinned=pinned,
        projection_version=version,
    )


def _database(tmp_path):
    path = tmp_path / "projection-repair.sqlite"
    conn = sqlite3.connect(path)
    try:
        ensure_message_projection_schema(conn)
        conn.commit()
    finally:
        conn.close()
    return path


def test_repair_converges_js8_alias_and_preserves_receipts_and_state(tmp_path) -> None:
    path = _database(tmp_path)
    conn = sqlite3.connect(path)
    try:
        with conn:
            upsert_message_projection(
                conn, _message("legacy-js8", family="js8", version=3, read=True, pinned=True)
            )
            upsert_message_projection(conn, _message("canonical-js8", family="js8", version=4))
            upsert_external_ref(
                conn,
                ExternalMessageRef(
                    message_id="legacy-js8",
                    source_id="js8:profile-a",
                    external_kind="js8_message",
                    external_key="receipt-1",
                ),
            )
            upsert_external_ref(
                conn,
                ExternalMessageRef(
                    message_id="canonical-js8",
                    source_id="js8:directed_txt:profile-a",
                    external_kind="js8_message",
                    external_key="receipt-1",
                ),
            )
            upsert_message_artifact(
                conn,
                MessageArtifactRecord(
                    artifact_id="legacy-artifact",
                    message_id="legacy-js8",
                    artifact_type="attachment",
                    source_id="js8:profile-a",
                    external_key="receipt-1",
                ),
            )
            conn.execute(
                """
                INSERT INTO message_delete_queue(
                    delete_id,message_id,requested_effect,requested_by,
                    source_scope,state,requested_utc,result_json
                ) VALUES ('delete-1','legacy-js8','hide_fio','operator','selected',
                          'queued','2026-09-23T00:00:00Z','{}')
                """
            )
    finally:
        conn.close()

    writer = ProjectionBundleWriter(path)
    try:
        result = writer.repair_legacy_duplicates()
        assert result.state == "committed"
        assert result.planned == result.repaired == 1
        assert writer.repair_legacy_duplicates().repaired == 0
    finally:
        writer.close()

    conn = sqlite3.connect(path)
    try:
        assert conn.execute("SELECT COUNT(*) FROM message_projection").fetchone()[0] == 1
        assert conn.execute(
            "SELECT read_state,pinned FROM message_projection WHERE message_id='canonical-js8'"
        ).fetchone() == ("read", 1)
        assert conn.execute(
            "SELECT COUNT(*) FROM message_external_refs WHERE message_id='canonical-js8'"
        ).fetchone()[0] == 2
        assert conn.execute(
            "SELECT message_id FROM message_artifacts WHERE artifact_id='legacy-artifact'"
        ).fetchone()[0] == "canonical-js8"
        assert conn.execute(
            "SELECT message_id FROM message_delete_queue WHERE delete_id='delete-1'"
        ).fetchone()[0] == "canonical-js8"
    finally:
        conn.close()


def test_file_repair_requires_same_family_filename_and_sha256(tmp_path) -> None:
    path = _database(tmp_path)
    same_digest = "a" * 64
    changed_digest = "b" * 64
    conn = sqlite3.connect(path)
    try:
        with conn:
            for message_id, family, version in (
                ("flmsg-old", "flmsg", 4),
                ("flmsg-current", "flmsg", 5),
                ("flmsg-changed", "flmsg", 5),
                ("bbs-copy", "bbs", 5),
            ):
                upsert_message_projection(
                    conn, _message(message_id, family=family, version=version)
                )
            for message_id, source_id, family, path_text, digest in (
                ("flmsg-old", "flmsg:old", "flmsg", "/one/Report.213", same_digest),
                ("flmsg-current", "flmsg:new", "flmsg", "/two/report.213", same_digest),
                ("flmsg-changed", "flmsg:changed", "flmsg", "/three/report.213", changed_digest),
                ("bbs-copy", "bbs:new", "bbs", "/bbs/report.213", same_digest),
            ):
                upsert_external_ref(
                    conn,
                    ExternalMessageRef(
                        message_id=message_id,
                        source_id=source_id,
                        external_kind=f"{family}_file",
                        external_key=f"key-{message_id}",
                        external_path=path_text,
                        external_hash=digest,
                    ),
                )
    finally:
        conn.close()

    conn = sqlite3.connect(path)
    try:
        plan = plan_legacy_projection_merges(conn)
        assert {(row.target_message_id, row.duplicate_message_id) for row in plan} == {
            ("flmsg-current", "flmsg-old")
        }
    finally:
        conn.close()

    writer = ProjectionBundleWriter(path)
    try:
        assert writer.repair_legacy_duplicates().repaired == 1
    finally:
        writer.close()
    conn = sqlite3.connect(path)
    try:
        assert conn.execute(
            "SELECT COUNT(*) FROM message_projection WHERE source_family='flmsg'"
        ).fetchone()[0] == 2
        assert conn.execute(
            "SELECT COUNT(*) FROM message_external_refs WHERE message_id='flmsg-current'"
        ).fetchone()[0] == 2
        assert conn.execute(
            "SELECT COUNT(*) FROM message_projection WHERE message_id IN ('flmsg-changed','bbs-copy')"
        ).fetchone()[0] == 2
    finally:
        conn.close()


def test_varac_alias_repair_requires_newer_projection_and_same_durable_key(tmp_path) -> None:
    path = _database(tmp_path)
    conn = sqlite3.connect(path)
    try:
        with conn:
            upsert_message_projection(conn, _message("varac-old", family="varac", version=1))
            upsert_message_projection(conn, _message("varac-new", family="varac", version=4))
            upsert_message_projection(
                conn, _message("varac-ambiguous-old", family="varac", version=1)
            )
            upsert_message_projection(
                conn, _message("varac-ambiguous-new", family="varac", version=4)
            )
            upsert_external_ref(
                conn,
                ExternalMessageRef(
                    message_id="varac-old",
                    source_id="varac:station-a",
                    external_kind="varac_message",
                    external_key="guid-1",
                ),
            )
            upsert_external_ref(
                conn,
                ExternalMessageRef(
                    message_id="varac-new",
                    source_id="varac:station-a:vmail",
                    external_kind="varac_message",
                    external_key="guid-1",
                ),
            )
            upsert_external_ref(
                conn,
                ExternalMessageRef(
                    message_id="varac-ambiguous-old",
                    source_id="varac:station-b",
                    external_kind="varac_message",
                    external_key="guid-2",
                ),
            )
            upsert_external_ref(
                conn,
                ExternalMessageRef(
                    message_id="varac-ambiguous-new",
                    source_id="varac:station-b:vmail",
                    external_kind="varac_message",
                    external_key="guid-2",
                ),
            )
            # A second receipt on the old presentation prevents whole-message
            # convergence until that receipt also has a proven replacement.
            upsert_external_ref(
                conn,
                ExternalMessageRef(
                    message_id="varac-ambiguous-old",
                    source_id="varac:station-b",
                    external_kind="varac_message",
                    external_key="guid-3",
                ),
            )
    finally:
        conn.close()

    writer = ProjectionBundleWriter(path)
    try:
        assert writer.repair_legacy_duplicates().repaired == 1
    finally:
        writer.close()
    conn = sqlite3.connect(path)
    try:
        assert conn.execute("SELECT COUNT(*) FROM message_projection").fetchone()[0] == 3
        assert conn.execute(
            "SELECT message_id FROM message_external_refs WHERE external_key='guid-3'"
        ).fetchone()[0] == "varac-ambiguous-old"
    finally:
        conn.close()
