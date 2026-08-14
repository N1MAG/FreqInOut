from __future__ import annotations

from freqinout.core.ingest_refresh_planner import plan_ingest_refresh
from freqinout.core.ingest_source_model import IngestSourceDescriptor
from freqinout.core.ingest_refresh_planner import ingest_sources_fingerprint


def test_plan_ingest_refresh_runs_for_first_force_change_and_cadence():
    fp1 = ("sources", ("a", "file", "1"))
    fp2 = ("sources", ("a", "file", "2"))

    assert plan_ingest_refresh(fp1, previous_fingerprint=None, now_ts=100).reason == "first-run"
    assert plan_ingest_refresh(fp1, previous_fingerprint=fp1, force=True, now_ts=100).reason == "forced"
    assert plan_ingest_refresh(fp2, previous_fingerprint=fp1, now_ts=100).reason == "source-changed"
    assert (
        plan_ingest_refresh(
            fp1,
            previous_fingerprint=fp1,
            last_run_ts=0,
            now_ts=100,
            max_quiet_sec=300,
        ).reason
        == "cadence"
    )
    decision = plan_ingest_refresh(
        fp1,
        previous_fingerprint=fp1,
        last_run_ts=90,
        now_ts=100,
        max_quiet_sec=300,
    )
    assert decision.should_run is False
    assert decision.reason == "unchanged"
    assert decision.as_dict()["reason"] == "unchanged"
    assert decision.as_dict()["fingerprint_size"] == 1


def test_plan_ingest_refresh_keeps_realtime_sources_on_cadence():
    fp = ("sources", ("api", "127.0.0.1:2442"))

    decision = plan_ingest_refresh(
        fp,
        previous_fingerprint=fp,
        last_run_ts=99,
        now_ts=100,
        realtime_source_present=True,
    )

    assert decision.should_run is True
    assert decision.reason == "realtime-source"


def test_ingest_sources_fingerprint_is_stable_and_filters_disabled_sources(tmp_path):
    source_path = tmp_path / "Directed.txt"
    source_path.write_text("one\n")
    enabled = IngestSourceDescriptor(
        source_id="b",
        family="js8call",
        source_type="file",
        label="B",
        path=str(source_path),
    )
    disabled = IngestSourceDescriptor(
        source_id="a",
        family="js8call",
        source_type="file",
        label="A",
        path=str(source_path),
        enabled=False,
    )

    fp = ingest_sources_fingerprint((disabled, enabled), families=("js8call",), source_types=("file",))

    assert fp[0] == "ingest-sources-v1"
    assert len(fp) == 2
    assert fp[1][0] == "b"


def test_ingest_sources_fingerprint_tracks_radio_and_app_reassignment(tmp_path):
    source_path = tmp_path / "Directed.txt"
    source_path.write_text("one\n")
    base = IngestSourceDescriptor(
        source_id="directed",
        family="js8call",
        source_type="file",
        label="FIO-A DIRECTED",
        radio_id="radio-a",
        app_instance_id="js8-a",
        path=str(source_path),
        checkpoint_key="directed-a",
    )

    radio_changed = IngestSourceDescriptor(
        source_id="directed",
        family="js8call",
        source_type="file",
        label="FIO-A DIRECTED",
        radio_id="radio-b",
        app_instance_id="js8-a",
        path=str(source_path),
        checkpoint_key="directed-a",
    )
    app_changed = IngestSourceDescriptor(
        source_id="directed",
        family="js8call",
        source_type="file",
        label="FIO-B DIRECTED",
        radio_id="radio-a",
        app_instance_id="js8-b",
        path=str(source_path),
        checkpoint_key="directed-b",
    )

    fp = ingest_sources_fingerprint((base,))

    assert ingest_sources_fingerprint((radio_changed,)) != fp
    assert ingest_sources_fingerprint((app_changed,)) != fp
