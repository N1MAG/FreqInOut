"""Isolated acceptance tests for the background Compose staging service."""

from __future__ import annotations

import json
from pathlib import Path

from freqinout.core import compose_stage_service as service
from freqinout.core.compose_stage_service import (
    ComposeStageRequest,
    _publish_to_managed_bbs,
    stage_compose_request,
)
from freqinout.core.nbems_compose import ComposeDestinationPlan
from freqinout.core.sqlite_utils import connect_sqlite
from freqinout.core.varac_bbs_library_store import (
    ensure_bbs_library_schema,
    list_bbs_artifact_location_ids,
    upsert_bbs_location,
)


def _plan(key: str, directory: Path, name: str = "message.k2s") -> ComposeDestinationPlan:
    return ComposeDestinationPlan(
        key=key,
        label=key.upper(),
        requested=True,
        ready=True,
        directory=str(directory),
        path=str(directory / name),
    )


def _request(
    plans: tuple[ComposeDestinationPlan, ...],
    *,
    payload: str = "PAYLOAD",
    **kwargs,
) -> ComposeStageRequest:
    return ComposeStageRequest(
        payload=payload,
        unsigned_name="message.k2s",
        plans=plans,
        radio_id="7",
        radio_label="FIO-A",
        **kwargs,
    )


def _bbs_db(path: Path, *location_ids: str) -> None:
    with connect_sqlite(path) as conn:
        ensure_bbs_library_schema(conn)
        for location_id in location_ids:
            upsert_bbs_location(conn, location_id=location_id, name=location_id)


def test_unsigned_flmsg_flamp_and_both_stage_payload_without_overwrite(tmp_path: Path) -> None:
    flmsg = tmp_path / "flmsg"
    flamp = tmp_path / "flamp"
    flmsg.mkdir()
    flamp.mkdir()

    result = stage_compose_request(
        _request((
            _plan("flmsg", flmsg),
            _plan("flamp", flamp),
        ), payload="exact unsigned payload")
    )

    assert result.problems == ()
    assert set(dict(result.output_by_key)) == {"flamp", "flmsg"}
    assert (flmsg / "message.k2s").read_text() == "exact unsigned payload"
    assert (flamp / "message.k2s").read_text() == "exact unsigned payload"


def test_collision_reserves_sibling_and_preserves_existing_artifact(tmp_path: Path) -> None:
    folder = tmp_path / "flmsg"
    folder.mkdir()
    original = folder / "message.k2s"
    original.write_text("operator-owned source")

    result = stage_compose_request(_request((_plan("flmsg", folder),), payload="new payload"))

    assert result.problems == ()
    assert original.read_text() == "operator-owned source"
    assert len(result.outputs) == 1
    staged = Path(result.outputs[0])
    assert staged.name == "message-2.k2s"
    assert staged.read_text() == "new payload"


def test_signed_success_verifies_and_publishes_only_signed_flamp(monkeypatch, tmp_path: Path) -> None:
    folder = tmp_path / "flamp"
    folder.mkdir()
    calls: list[tuple[str, str]] = []

    def fake_clearsign(source, *, output_path, **kwargs):
        calls.append((str(source), str(output_path)))
        Path(output_path).write_text("SIGNED\n" + Path(source).read_text())
        return True, "ok"

    monkeypatch.setattr(service, "clearsign_file", fake_clearsign)
    monkeypatch.setattr(
        service,
        "verify_file_with_discovery",
        lambda *_args, **_kwargs: type("Verification", (), {"status": "valid", "detail": "valid"})(),
    )

    result = stage_compose_request(
        _request(
            (_plan("flamp", folder, "message.sig.k2s"),),
            payload="signed payload",
            sign_flamp=True,
            signer_fingerprint="ABC123",
        )
    )

    assert len(calls) == 1
    assert result.problems == ()
    assert result.signature_notes == ("FLAmp signed file verified: message.sig.k2s",)
    assert Path(result.outputs[0]).read_text() == "SIGNED\nsigned payload"


def test_signed_failure_never_stages_unsigned_fallback(monkeypatch, tmp_path: Path) -> None:
    folder = tmp_path / "flamp"
    folder.mkdir()
    monkeypatch.setattr(service, "clearsign_file", lambda *_args, **_kwargs: (False, "bad key"))

    result = stage_compose_request(
        _request(
            (_plan("flamp", folder, "message.sig.k2s"),),
            payload="must not leak unsigned",
            sign_flamp=True,
            signer_fingerprint="ABC123",
        )
    )

    assert result.outputs == ()
    assert any("no unsigned FLAmp fallback" in issue for issue in result.problems)
    assert not list(folder.iterdir())


def test_signature_verification_failure_never_publishes_signed_artifact(monkeypatch, tmp_path: Path) -> None:
    folder = tmp_path / "flamp"
    folder.mkdir()

    def fake_clearsign(_source, *, output_path, **_kwargs):
        Path(output_path).write_text("SIGNED")
        return True, "ok"

    monkeypatch.setattr(service, "clearsign_file", fake_clearsign)
    monkeypatch.setattr(
        service,
        "verify_file_with_discovery",
        lambda *_args, **_kwargs: type("Verification", (), {"status": "invalid", "detail": "bad signature"})(),
    )

    result = stage_compose_request(
        _request(
            (_plan("flamp", folder, "message.sig.k2s"),),
            sign_flamp=True,
            signer_fingerprint="ABC123",
        )
    )

    assert result.outputs == ()
    assert any("signature verification failed" in issue for issue in result.problems)
    assert not list(folder.iterdir())


def test_bbs_publication_prefers_flamp_artifact_deterministically(tmp_path: Path) -> None:
    flmsg = tmp_path / "flmsg"
    flamp = tmp_path / "flamp"
    flmsg.mkdir()
    flamp.mkdir()
    bbs_db = tmp_path / "bbs.sqlite"
    _bbs_db(bbs_db, "ops")

    result = stage_compose_request(
        _request(
            (_plan("flmsg", flmsg), _plan("flamp", flamp)),
            publish_to_bbs=True,
            bbs_db_path=str(bbs_db),
            bbs_location_ids=("ops",),
            source_id="compose-1",
        )
    )

    assert result.problems == ()
    assert result.bbs_artifact_id
    assert result.bbs_publish_path == str(flamp / "message.k2s")
    with connect_sqlite(bbs_db) as conn:
        row = conn.execute(
            "SELECT source_path, metadata_json FROM bbs_artifacts WHERE artifact_id=?",
            (result.bbs_artifact_id,),
        ).fetchone()
    assert row[0] == str(flamp / "message.k2s")
    assert json.loads(row[1])["file_type"] == "flamp"


def test_bbs_membership_replacement_is_exact_and_logical(tmp_path: Path) -> None:
    bbs_db = tmp_path / "bbs.sqlite"
    _bbs_db(bbs_db, "a", "b", "c")
    output = tmp_path / "message.k2s"
    output.write_text("payload")
    plans = (_plan("flamp", tmp_path),)

    first = _publish_to_managed_bbs(
        _request(plans, bbs_db_path=str(bbs_db), bbs_location_ids=("a", "b")),
        {"flamp": output},
    )
    second = _publish_to_managed_bbs(
        _request(plans, bbs_db_path=str(bbs_db), bbs_location_ids=("b", "c")),
        {"flamp": output},
    )

    assert first[0] == second[0]
    with connect_sqlite(bbs_db) as conn:
        assert list_bbs_artifact_location_ids(conn, second[0]) == ("b", "c")
        rows = conn.execute(
            "SELECT location_id, publish_enabled FROM bbs_location_artifacts WHERE artifact_id=? ORDER BY location_id",
            (second[0],),
        ).fetchall()
    assert rows == [("a", 0), ("b", 1), ("c", 1)]


def test_bbs_db_failure_keeps_successfully_staged_file_and_rolls_back_metadata(monkeypatch, tmp_path: Path) -> None:
    folder = tmp_path / "flamp"
    folder.mkdir()
    bbs_db = tmp_path / "bbs.sqlite"
    _bbs_db(bbs_db, "ops")
    def fail_membership(*_args, **_kwargs):
        raise RuntimeError("membership failed")

    monkeypatch.setattr(service, "set_bbs_artifact_locations", fail_membership)

    result = stage_compose_request(
        _request(
            (_plan("flamp", folder),),
            publish_to_bbs=True,
            bbs_db_path=str(bbs_db),
            bbs_location_ids=("ops",),
        )
    )

    assert len(result.outputs) == 1
    assert any("Managed BBS: membership failed" in issue for issue in result.problems)
    with connect_sqlite(bbs_db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM bbs_artifacts").fetchone()[0] == 0


def test_bbs_connection_failure_keeps_partial_stage_result(tmp_path: Path) -> None:
    folder = tmp_path / "flamp"
    folder.mkdir()
    unavailable_db = tmp_path / "missing-parent" / "bbs.sqlite"

    result = stage_compose_request(
        _request(
            (_plan("flamp", folder),),
            publish_to_bbs=True,
            bbs_db_path=str(unavailable_db),
            bbs_location_ids=("ops",),
        )
    )

    assert len(result.outputs) == 1
    assert Path(result.outputs[0]).read_text() == "PAYLOAD"
    assert any(issue.startswith("Managed BBS:") for issue in result.problems)


def test_partial_destination_failure_keeps_other_output(monkeypatch, tmp_path: Path) -> None:
    flmsg = tmp_path / "flmsg"
    flamp = tmp_path / "flamp"
    flmsg.mkdir()
    flamp.mkdir()
    original = service._write_payload

    def fail_flamp(path: Path, payload: str) -> Path:
        if path.parent == flamp:
            raise OSError("FLAmp volume unavailable")
        return original(path, payload)

    monkeypatch.setattr(service, "_write_payload", fail_flamp)
    result = stage_compose_request(_request((_plan("flmsg", flmsg), _plan("flamp", flamp))))

    assert len(result.outputs) == 1
    assert result.output_by_key == (("flmsg", str(flmsg / "message.k2s")),)
    assert (flmsg / "message.k2s").read_text() == "PAYLOAD"
    assert any("FLAMP: FLAmp volume unavailable" in issue for issue in result.problems)


def test_missing_signer_is_rejected_before_any_source_or_destination_write(tmp_path: Path) -> None:
    folder = tmp_path / "flamp"
    folder.mkdir()
    source = tmp_path / "source.k2s"
    source.write_text("operator source")
    result = stage_compose_request(
        _request(
            (_plan("flamp", folder),),
            payload="must not stage",
            sign_flamp=True,
            signer_fingerprint="",
            source_id=str(source),
        )
    )

    assert result.outputs == ()
    assert source.read_text() == "operator source"
    assert not list(folder.iterdir())
