from __future__ import annotations

import datetime as dt

import pytest

from freqinout.core.commstat_sitrep import (
    build_commstat_brevity_rf_text,
    build_commstat_statrep_rf_text,
)


def test_commstat_rf_statrep_matches_superspotter_wire_shape() -> None:
    text = build_commstat_statrep_rf_text(
        callsign="n1mag",
        group="magnet",
        grid="em12jv",
        scope="1",
        report_id="A03",
        statuses={"overall": "Green", "power": "Yellow"},
        comment="Grid power intermittent; generators OK!",
        now=dt.datetime(2026, 8, 27, 0, 3, tzinfo=dt.timezone.utc),
    )

    assert text == "N1MAG: MAGNET ,EM12JV,1,A03,121111111111,Grid power intermittent generators OK!,{&%}"


def test_commstat_rf_statrep_compacts_all_green_statuses() -> None:
    text = build_commstat_statrep_rf_text(
        callsign="N1MAG",
        group="@MAGNET",
        grid="EM12JV",
        report_id="B10",
        statuses={},
    )

    assert text == "N1MAG: MAGNET ,EM12JV,1,B10,+,,{&%}"


def test_commstat_rf_brevity_is_short_and_validated() -> None:
    text = build_commstat_brevity_rf_text(
        destination="@MAGNET",
        brevity_code="#4BBGUB",
        comment="Bridge out north route",
    )

    assert text == "MAGNET #4BBGUB Bridge out north route"

    with pytest.raises(ValueError):
        build_commstat_brevity_rf_text(destination="@MAGNET", brevity_code="BAD")


def test_commstat_compose_treats_brevity_as_statrep_addition() -> None:
    source = open("freqinout/gui/message_viewer_tab.py", encoding="utf-8").read()

    assert 'self.compose_commstat_kind_combo.addItems(["StatRep"])' in source
    assert 'self.compose_commstat_brevity_chk = QCheckBox("Add Brevity")' in source
    assert "self.compose_commstat_brevity_edit.setVisible(commstat_mode)" in source
    assert "self.compose_commstat_brevity_edit.setEnabled(brevity_enabled)" in source
    assert "self.compose_commstat_brevity_builder_widget = QWidget()" in source
    assert "def _on_compose_commstat_brevity_enabled_changed" in source
    assert "self.compose_commstat_brevity_builder_widget.setVisible(commstat_mode and brevity_enabled)" in source
    assert "self.compose_commstat_brevity_builder_widget.setEnabled(brevity_enabled)" in source
    assert "def _compose_commstat_brevity_catalogs" in source
    assert "def _compose_commstat_brevity_builder_code" in source
    assert '"emergency_type"' in source
    assert '"status_codes"' in source
    assert '"shared_impacts"' in source
    assert '"public_reaction"' in source
    assert '"station_response"' in source
    assert 'comment = " ".join(part for part in (f"#{brevity_code}", comment.strip()) if part)' in source
    assert 'if kind == "Brevity":' not in source
