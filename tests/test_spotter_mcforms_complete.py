from __future__ import annotations

import json
from pathlib import Path

import pytest

from freqinout.core.js8_spotter_codec import (
    parse_spotter_form_payload,
    serialize_spotter_form_payload,
    unwrap_native_js8_form_payload,
)
from freqinout.core.js8_spotter_forms import (
    SPOTTER_COMMENTS_KEY,
    SPOTTER_COMMENTS_MAX_LENGTH,
    discover_spotter_forms,
    form_id_enabled,
    normalize_form_code,
    parse_spotter_form_fields,
    spotter_operator_autofill_kind,
)
from freqinout.core.js8_expect_store import update_mcform_response_datecode
from freqinout.core.message_ingest import MessageIngestor
from freqinout.core.message_intelligence import analyze_spotter_text
from freqinout.core.sitrep_fusion import _canonicalize_row
from freqinout.core.sitrep_ingest import SPOTTER_SITREP_FORMS
from freqinout.gui.message_viewer_tab import MessageViewerTab


REFERENCE_FORMS = Path("/Users/bill/RadioTools/Programs/JS8SuperSpotter2.6/forms")


SAMPLE = """MAGNET Basic Check-in|F!701C
!! Basic Check-in
[ST] State (2-letter code):
[GR] Maidenhead Grid Square:
? Current Operational Status (QTH)
@1 *Operations steady - Green
@2 Operations limited - Yellow
@3 Major issues - Red
? Situation Report
@0 *Nothing significant
@1 Details available
"""


def test_parser_keeps_prompts_explicit_defaults_and_source_order() -> None:
    fields = parse_spotter_form_fields(SAMPLE)

    assert [(field.key, field.kind) for field in fields] == [
        ("ST", "prompt"),
        ("GR", "prompt"),
        ("CURRENT_OPERATIONAL_STATUS_QTH", "choice"),
        ("SITUATION_REPORT", "choice"),
    ]
    assert fields[2].default_value == "1"
    assert fields[3].default_value == "0"
    assert fields[2].options[0] == ("1", "Operations steady - Green")


def test_codec_round_trips_choices_prompts_comments_and_datecode() -> None:
    fields = parse_spotter_form_fields(SAMPLE)
    values = {
        "ST": "CO",
        "GR": "DN70AA",
        "CURRENT_OPERATIONAL_STATUS_QTH": "2",
        "SITUATION_REPORT": "1",
        SPOTTER_COMMENTS_KEY: "Road access limited near mile 12",
    }

    encoded = serialize_spotter_form_payload("F!701C", fields, values, datecode="#AB12")
    decoded = parse_spotter_form_payload(encoded, fields, expected_form_code="F!701C")

    assert encoded == "F!701C 21 ST[CO] GR[DN70AA] Road access limited near mile 12 #AB12"
    assert decoded.complete is True
    assert decoded.datecode == "#AB12"
    assert decoded.values == values


def test_codec_unwraps_native_js8_msg_without_losing_form_text() -> None:
    wrapped = "K1ABC: N0CALL MSG F!701C 10 ST[CO] GR[DN70] clear #AB12"
    assert unwrap_native_js8_form_payload(wrapped) == "F!701C 10 ST[CO] GR[DN70] clear #AB12"


def test_codec_blocks_incomplete_choices_and_illegal_prompt_delimiter() -> None:
    fields = parse_spotter_form_fields(SAMPLE)
    with pytest.raises(ValueError, match="Current Operational Status"):
        serialize_spotter_form_payload("F!701C", fields, {"SITUATION_REPORT": "0"})
    with pytest.raises(ValueError, match="closing bracket"):
        serialize_spotter_form_payload(
            "F!701C",
            fields,
            {
                "ST": "C]O",
                "CURRENT_OPERATIONAL_STATUS_QTH": "1",
                "SITUATION_REPORT": "0",
            },
        )


def test_codec_enforces_bounded_optional_comment_without_truncating_received_text() -> None:
    fields = parse_spotter_form_fields(SAMPLE)
    values = {
        "ST": "CO",
        "GR": "DN70AA",
        "CURRENT_OPERATIONAL_STATUS_QTH": "1",
        "SITUATION_REPORT": "0",
    }
    with pytest.raises(ValueError, match="50 characters or fewer"):
        serialize_spotter_form_payload(
            "F!701C",
            fields,
            values,
            comments="X" * (SPOTTER_COMMENTS_MAX_LENGTH + 1),
        )

    received = parse_spotter_form_payload(
        "F!701C 10 ST[CO] GR[DN70AA] " + ("X" * (SPOTTER_COMMENTS_MAX_LENGTH + 5)),
        fields,
        expected_form_code="F!701C",
    )
    assert received.comments == "X" * (SPOTTER_COMMENTS_MAX_LENGTH + 5)


@pytest.mark.parametrize(
    ("form_code", "field_key", "kind"),
    [
        ("F!105", "CS", "callsign"),
        ("F!105", "ST", "state"),
        ("F!105", "GR", "grid"),
        ("F!701A", "FR", "callsign"),
        ("F!701A", "ST", ""),
        ("F!107", "ST", ""),
        ("F!305", "GR", ""),
        ("F!505", "ST", ""),
        ("F!720", "ST", ""),
        ("F!BDN", "GR", "grid"),
    ],
)
def test_operator_autofill_is_form_semantic(form_code: str, field_key: str, kind: str) -> None:
    assert spotter_operator_autofill_kind(form_code, field_key) == kind


def test_alphabetic_catalog_form_ids_are_supported(tmp_path: Path) -> None:
    (tmp_path / "MCFBDN.txt").write_text("BDN Survey|F!BDN\n[GR] Grid:\n", encoding="utf-8")
    forms = discover_spotter_forms(tmp_path)

    assert [(form.form_code, form.title) for form in forms] == [("F!BDN", "BDN Survey")]
    assert normalize_form_code("MCFBDN") == "F!BDN"
    assert normalize_form_code("BDN") == "F!BDN"
    assert normalize_form_code("FLMSG") == ""
    assert form_id_enabled("BDN", {"F!BDN"}) is True
    assert update_mcform_response_datecode(
        "F!BDN Y GR[DN70] #AB12", "F!BDN", datecode="#CD34"
    ) == "F!BDN Y GR[DN70] #CD34"


@pytest.mark.skipif(not REFERENCE_FORMS.is_dir(), reason="reference SuperSpotter catalog is not installed")
def test_reference_catalog_has_complete_stable_coverage() -> None:
    forms = discover_spotter_forms(REFERENCE_FORMS)
    fields = [
        field
        for form in forms
        for field in parse_spotter_form_fields(Path(form.path).read_text(encoding="utf-8", errors="replace"))
    ]

    assert len(forms) == 30
    assert sum(field.kind == "choice" for field in fields) == 214
    assert sum(len(field.options) for field in fields) == 1405
    assert sum(field.kind == "prompt" for field in fields) == 90
    assert len({field.key for field in fields if field.kind == "prompt"}) == 42
    assert sum(bool(field.default_value) for field in fields) == 62


@pytest.mark.skipif(not REFERENCE_FORMS.is_dir(), reason="reference SuperSpotter catalog is not installed")
def test_every_reference_form_round_trips_all_field_kinds_and_comments() -> None:
    for definition in discover_spotter_forms(REFERENCE_FORMS):
        fields = parse_spotter_form_fields(
            Path(definition.path).read_text(encoding="utf-8", errors="replace")
        )
        values = {}
        for field in fields:
            if field.kind == "choice":
                assert field.options, f"{definition.form_code} {field.label} has no answer options"
                values[field.key] = field.default_value or field.options[0][0]
            else:
                values[field.key] = f"VALUE {field.key}"
        values[SPOTTER_COMMENTS_KEY] = "Operator comment retained"

        encoded = serialize_spotter_form_payload(definition.form_code, fields, values, datecode="#AB12")
        decoded = parse_spotter_form_payload(
            encoded,
            fields,
            expected_form_code=definition.form_code,
        )

        assert decoded.complete, (definition.form_code, decoded.issues)
        assert decoded.datecode == "#AB12"
        assert decoded.values == values, definition.form_code


def test_native_js8_msg_spotter_event_is_not_discarded(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    from freqinout.core.settings_manager import SettingsManager

    ingestor = MessageIngestor(SettingsManager())
    parsed = ingestor._parse_js8_spotter_event(
        {
            "type": "RX.DIRECTED",
            "params": {
                "FROM": "K1ABC",
                "TO": "N0CALL",
                "TEXT": "N0CALL MSG F!BDN Y GR[DN70] local survey #AB12",
                "UTC": "2026-09-14 12:00:00",
            },
        }
    )

    assert parsed is not None
    assert parsed["form_id"] == "BDN"
    assert parsed["raw_form"] == "F!BDN Y GR[DN70] local survey #AB12"


def test_native_js8_msg_recovers_destination_without_api_to_field(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("FREQINOUT_CONFIG_DIR", str(tmp_path / "profile"))
    from freqinout.core.settings_manager import SettingsManager

    ingestor = MessageIngestor(SettingsManager())
    parsed = ingestor._parse_js8_spotter_event(
        {
            "type": "RX.DIRECTED",
            "params": {
                "FROM": "K1ABC",
                "TEXT": "N0CALL MSG F!701C 100 ST[CO] GR[DN70] checking... still operational #AB12",
                "UTC": "2026-09-14 12:00:00",
            },
        }
    )

    assert parsed is not None
    assert parsed["to_call"] == "N0CALL"
    assert "checking... still operational" in parsed["raw_form"]


def test_magnet_status_forms_use_conservative_shared_summary() -> None:
    assert MessageIngestor._classify_spotter_status("701C", "2")[:2] == (
        "yellow",
        "Partially Functioning",
    )
    # 701B Q5=2 is grid-power outage and therefore wins over otherwise green
    # health dimensions.
    assert MessageIngestor._classify_spotter_status("701B", "11112111")[:2] == (
        "red",
        "Not Functioning",
    )
    assert MessageIngestor._classify_spotter_status("701B", "111")[:2] == (
        "unknown",
        "Unknown",
    )
    info = analyze_spotter_text("F!701B 11112111 RM[GRID POWER LOST] #AB12")
    assert info.subject == "Not Functioning"
    assert info.metadata["operational_status"] == "red"
    assert info.metadata["operational_status_evidence"] == "Q3/Q5-Q8 aggregate"


def test_magnet_status_forms_enter_shared_sitrep_fusion() -> None:
    assert SPOTTER_SITREP_FORMS["F!701B"] == "SPOTTER_701B"
    assert SPOTTER_SITREP_FORMS["F!701C"] == "SPOTTER_701C"
    row = (
        1,
        "JS8SPOTTER",
        "spotter_traffic",
        "/tmp/fio.db",
        10,
        "SPOTTER_701B",
        "K1ABC",
        "@MAGNET",
        "@MAGNET",
        "DN70AA",
        "",
        "js8",
        "Grid power lost",
        "",
        "",
        "CO",
        "grid6",
        "grid6",
        json.dumps({"responses": "11112111"}),
        json.dumps({"form_id": "F!701B"}),
        1_789_387_200.0,
        "2026-09-14 12:00:00",
    )

    canonical = _canonicalize_row(row)

    assert canonical is not None
    assert canonical["fields"]["overall_status"] == "red"
    assert canonical["fields"]["power"] == "red"
    assert canonical["grid"] == "DN70AA"


class _Text:
    def __init__(self, value: str):
        self.value = value

    def text(self) -> str:
        return self.value

    def toPlainText(self) -> str:
        return self.value

    def currentText(self) -> str:
        return self.value

    def isChecked(self) -> bool:
        return self.value == "yes"


def test_js8_and_spotter_compose_add_native_msg_wrapper_only_when_selected() -> None:
    class Fake:
        compose_js8_target_edit = _Text("@GROUP")
        compose_js8_plain_text_edit = _Text("status update")
        compose_js8_plain_kind_combo = _Text("Directed Message")
        compose_js8_send_as_msg_chk = _Text("yes")
        _compose_rf_target_text = MessageViewerTab._compose_rf_target_text
        _compose_plain_js8_text = MessageViewerTab._compose_plain_js8_text
        _compose_plain_js8_kind = MessageViewerTab._compose_plain_js8_kind
        _compose_send_as_msg = MessageViewerTab._compose_send_as_msg
        _compose_spotter_message_text = lambda self, **_kwargs: "F!701C 100 #AB12"

    fake = Fake()
    assert MessageViewerTab._compose_plain_js8_command(fake) == "GROUP MSG status update"
    assert MessageViewerTab._compose_spotter_command(fake) == "GROUP MSG F!701C 100 #AB12"

    fake.compose_js8_plain_text_edit = _Text("GROUP status update")
    assert MessageViewerTab._compose_plain_js8_command(fake) == "GROUP MSG status update"
    fake.compose_js8_plain_text_edit = _Text("GROUP MSG status update")
    assert MessageViewerTab._compose_plain_js8_command(fake) == "GROUP MSG status update"
