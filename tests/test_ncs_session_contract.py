from __future__ import annotations

from freqinout.core.ncs_session_contract import (
    NCS_SESSION_SNAPSHOTS_KEY,
    NcsSessionSnapshot,
    active_ncs_session_flags,
    active_ncs_session_snapshot_list,
    active_ncs_session_summaries,
    active_ncs_session_summaries_by_kind,
    ncs_session_is_active,
    ncs_session_kind,
    ncs_session_key,
    read_ncs_session_snapshots,
    write_ncs_session_snapshot,
)


class _Settings:
    def __init__(self) -> None:
        self.data: dict[str, object] = {}

    def get(self, key: str, default: object = None) -> object:
        return self.data.get(key, default)

    def set(self, key: str, value: object) -> None:
        self.data[key] = value


def test_ncs_session_snapshot_key_and_label_are_stable() -> None:
    snapshot = NcsSessionSnapshot(
        protocol="JS8Call",
        source_id="42",
        source_name="FIO-B",
        role="ANCS",
        net_name="MAGNET",
        timing_state="active",
    )

    assert snapshot.session_key == "js8call:42"
    assert snapshot.label == "FIO-B | JS8Call | ANCS | MAGNET | active"
    assert ncs_session_key("FLDigi/SSB", "FIO A") == "fldigi/ssb:fio_a"
    assert ncs_session_kind("FLDigi/SSB") == "FLDIGI"
    assert ncs_session_kind("JS8Call") == "JS8"
    assert ncs_session_kind("Local") == "LOCAL"
    assert ncs_session_is_active(snapshot)


def test_ncs_session_snapshots_round_trip_through_settings() -> None:
    settings = _Settings()
    snapshot = NcsSessionSnapshot(
        protocol="Local",
        source_id="local",
        source_name="Local",
        role="NCS",
        net_name="County Voice Net",
        timing_state="idle",
        detail="VHF1, GMRS",
    )

    write_ncs_session_snapshot(settings, snapshot)
    raw = settings.data[NCS_SESSION_SNAPSHOTS_KEY]

    assert isinstance(raw, dict)
    assert raw["local:local"]["net_name"] == "County Voice Net"
    loaded = read_ncs_session_snapshots(settings)
    assert loaded["local:local"] == snapshot


def test_active_ncs_session_flags_project_current_active_sessions() -> None:
    settings = _Settings()
    write_ncs_session_snapshot(
        settings,
        NcsSessionSnapshot(
            protocol="JS8Call",
            source_id="1",
            source_name="FIO-A",
            timing_state="active",
        ),
    )
    write_ncs_session_snapshot(
        settings,
        NcsSessionSnapshot(
            protocol="FLDigi/SSB",
            source_id="2",
            source_name="FIO-B",
            timing_state="ended",
        ),
    )

    assert active_ncs_session_flags(settings) == {
        "FLDIGI": False,
        "JS8": True,
        "LOCAL": False,
    }


def test_active_ncs_session_summaries_are_sorted_for_shell_display() -> None:
    settings = _Settings()
    write_ncs_session_snapshot(
        settings,
        NcsSessionSnapshot(
            protocol="JS8Call",
            source_id="2",
            source_name="FIO-B",
            role="ANCS",
            net_name="MAGNET",
            timing_state="active",
        ),
    )
    write_ncs_session_snapshot(
        settings,
        NcsSessionSnapshot(
            protocol="FLDigi/SSB",
            source_id="1",
            source_name="FIO-A",
            role="NCS",
            net_name="County Voice",
            timing_state="started",
        ),
    )
    write_ncs_session_snapshot(
        settings,
        NcsSessionSnapshot(
            protocol="Local",
            source_id="local",
            source_name="Local",
            role="NCS",
            net_name="CERT",
            timing_state="ended",
        ),
    )

    active = active_ncs_session_snapshot_list(settings)

    assert [snapshot.source_name for snapshot in active] == ["FIO-A", "FIO-B"]
    assert active_ncs_session_summaries(settings) == [
        "FIO-A | FLDigi/SSB | NCS | County Voice | started",
        "FIO-B | JS8Call | ANCS | MAGNET | active",
    ]
    assert active_ncs_session_summaries_by_kind(settings) == {
        "FLDIGI": ["FIO-A | FLDigi/SSB | NCS | County Voice | started"],
        "JS8": ["FIO-B | JS8Call | ANCS | MAGNET | active"],
    }
