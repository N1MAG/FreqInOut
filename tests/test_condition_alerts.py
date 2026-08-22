import json
import sqlite3
from pathlib import Path

from freqinout.core.condition_alerts import (
    ConditionAlertMessage,
    condition_alert_rule_to_settings,
    condition_alert_rules_from_settings,
    condition_alert_rules_setting_payload,
    default_condition_alert_rules,
    match_condition_alert_rules,
    normalize_condition_alert_rule,
)
from freqinout.core.condition_alert_ingest import condition_alert_observations_for_message_intelligence
from freqinout.core.message_intelligence import analyze_commstat_fields, analyze_spotter_text
from freqinout.core.observation_projection import (
    observation_from_condition_alert_match,
    observation_from_message_intelligence,
)
from freqinout.core.sitrep_metadata import source_family_label, source_short_label
from freqinout.core.varac_ingest import _mirror_varac_condition_alerts


def test_default_magnet_magcon_template_is_disabled_until_operator_enables_it() -> None:
    rule = default_condition_alert_rules()[0]

    assert rule.enabled is False
    assert rule.operating_group == "MAGNET"
    assert rule.action == "prompt-to-apply"
    assert "MR08" in rule.target_groups

    matches = match_condition_alert_rules(
        [rule],
        {
            "source_family": "JS8Call",
            "from_call": "N1MAG",
            "to_target": "@MAGNET",
            "text": "MAGCON+3 STANDBY",
        },
    )

    assert matches == ()


def test_enabled_magcon_rule_extracts_level_from_js8_text_for_target_group() -> None:
    rule = normalize_condition_alert_rule(
        {
            "id": "magnet-magcon",
            "enabled": True,
            "name": "MagNet MAGCON",
            "operating_group": "MAGNET",
            "source_families": ["JS8Call", "CommStat"],
            "target_groups": ["MAGNET", "MR08"],
            "allowed_sender_mode": "explicit list",
            "allowed_senders": ["N1MAG"],
            "required_auth_state": "none",
            "match_mode": "regex",
            "pattern": r"\bMAGCON\+?([1-5])\b",
            "level_capture_group": 1,
        }
    )

    matches = match_condition_alert_rules(
        [rule],
        ConditionAlertMessage(
            source_family="JS8Call",
            source_ref="directed:42",
            source_radio_id=2,
            source_app="FIO-A",
            received_utc="2026-08-21T18:30:00+00:00",
            from_call="N1MAG",
            to_target="@MR08",
            groups=("MR08",),
            text="N1MAG: @MR08 MAGCON+4 CHECK SOP",
        ),
    )

    assert len(matches) == 1
    assert matches[0].condition_level == 4
    assert matches[0].operating_group == "MAGNET"
    assert matches[0].action == "prompt-to-apply"
    assert matches[0].source_radio_id == 2


def test_condition_alert_rule_respects_source_sender_target_and_auth() -> None:
    rule = normalize_condition_alert_rule(
        {
            "id": "ops-alert",
            "enabled": True,
            "source_families": "JS8Call, VarAC",
            "target_groups": "OPS",
            "allowed_sender_mode": "trusted operator",
            "required_auth_state": "signed-and-trusted",
            "pattern": r"OPSCON\s*([1-5])",
        }
    )

    blocked = match_condition_alert_rules(
        [rule],
        {
            "source_family": "CommStat",
            "from_call": "K0BAD",
            "to_target": "@OPS",
            "text": "OPSCON 2",
            "auth_state": "valid",
            "trusted_state": "trusted",
        },
    )
    unsigned = match_condition_alert_rules(
        [rule],
        {
            "source_family": "JS8Call",
            "from_call": "K0OPS",
            "to_target": "@OPS",
            "text": "OPSCON 2",
        },
    )
    allowed = match_condition_alert_rules(
        [rule],
        {
            "source_family": "JS8Call",
            "from_call": "K0OPS",
            "to_target": "@OPS",
            "text": "OPSCON 2",
            "auth_state": "valid",
            "trusted_state": "trusted",
        },
    )

    assert blocked == ()
    assert unsigned == ()
    assert len(allowed) == 1
    assert allowed[0].condition_level == 2


def test_condition_alert_can_match_message_intelligence_and_project_observation() -> None:
    info = analyze_spotter_text(
        "F!701 TO[@MAGNET] FR[N1MAG] NA[MAGCON+5] DA[260821-1830Z]",
        form_name="MAGCON Update",
    )
    rule = normalize_condition_alert_rule(
        {
            "id": "spotter-magcon",
            "enabled": True,
            "name": "Spotter MAGCON",
            "operating_group": "MAGNET",
            "source_families": ["JS8Spotter"],
            "target_groups": ["MAGNET"],
            "allowed_sender_mode": "explicit list",
            "allowed_senders": ["N1MAG"],
            "pattern": r"MAGCON\+?([1-5])",
        }
    )

    matches = match_condition_alert_rules(
        [rule],
        {
            "source_family": "JS8Spotter",
            "source_ref": "spotter:99",
            "from_call": info.from_call,
            "to_target": info.to_call,
            "groups": info.groups,
            "text": info.summary + " " + info.body,
        },
    )
    obs = observation_from_condition_alert_match(matches[0])

    assert obs.source_family == "condition_alert"
    assert obs.urgency == "LEVEL 5"
    assert obs.subject == "Spotter MAGCON: Level 5"
    assert obs.groups == ("MAGNET",)
    assert obs.operator_attention is True
    assert json.loads(obs.provenance_json)["rule_id"] == "spotter-magcon"


def test_commstat_message_text_that_is_not_a_statrep_can_drive_condition_alert() -> None:
    info = analyze_commstat_fields(
        artifact_kind="MESSAGE",
        title="MAGCON+3 for MR08",
        body="Switch to yellow SOP.",
        from_call="N1MAG",
        target="@MR08",
        report_group="MR08",
        source_family="CommStat",
    )
    obs = observation_from_message_intelligence(info, source_ref="commstat:7", source_family="commstat")
    rule = normalize_condition_alert_rule(
        {
            "id": "commstat-magcon",
            "enabled": True,
            "operating_group": "MAGNET",
            "source_families": ["CommStat"],
            "target_groups": ["MR08"],
            "allowed_sender_mode": "explicit list",
            "allowed_senders": ["N1MAG"],
            "pattern": r"MAGCON\+?([1-5])",
        }
    )

    matches = match_condition_alert_rules(
        [rule],
        {
            "source_family": obs.source_family,
            "source_ref": obs.source_ref,
            "from_call": obs.from_call,
            "to_target": obs.to_target,
            "groups": obs.groups,
            "text": f"{obs.subject} {obs.summary}",
        },
    )

    assert len(matches) == 1
    assert matches[0].condition_level == 3


def test_condition_alert_settings_load_includes_builtin_template_once() -> None:
    rules = condition_alert_rules_from_settings(None)

    assert [rule.id for rule in rules].count("builtin-magnet-magcon") == 1
    assert rules[0].enabled is False


def test_condition_alert_settings_override_builtin_by_id_without_duplicate() -> None:
    rules = condition_alert_rules_from_settings(
        [
            {
                "id": "builtin-magnet-magcon",
                "enabled": True,
                "name": "MagNet MAGCON custom",
                "operating_group": "MAGNET",
                "source_families": ["JS8Call"],
                "target_groups": ["MAGNET"],
                "allowed_sender_mode": "explicit list",
                "allowed_senders": ["N1MAG"],
                "pattern": r"MAGCON\+?([1-5])",
            }
        ]
    )

    assert [rule.id for rule in rules].count("builtin-magnet-magcon") == 1
    rule = next(rule for rule in rules if rule.id == "builtin-magnet-magcon")
    assert rule.enabled is True
    assert rule.name == "MagNet MAGCON custom"
    assert rule.allowed_senders == ("N1MAG",)


def test_condition_alert_settings_roundtrip_custom_rule() -> None:
    original = normalize_condition_alert_rule(
        {
            "id": "custom-opcon",
            "enabled": True,
            "name": "OPCON",
            "source_families": "JS8Call, VarAC",
            "target_groups": "OPS",
            "pattern": r"OPCON ([1-5])",
            "action": "suggest",
        }
    )

    payload = condition_alert_rules_setting_payload([original])
    loaded = condition_alert_rules_from_settings(payload, include_builtin=False)

    assert loaded == (original,)
    assert condition_alert_rule_to_settings(loaded[0])["source_families"] == ["JS8CALL", "VARAC"]


def test_condition_alert_settings_ignores_bad_rows_and_assigns_missing_ids() -> None:
    rules = condition_alert_rules_from_settings(
        json.dumps(
            [
                "not a rule",
                {
                    "enabled": True,
                    "name": "Unnamed Condition Rule",
                    "source_families": ["CommStat"],
                    "pattern": r"LEVEL ([1-5])",
                },
            ]
        ),
        include_builtin=False,
    )

    assert len(rules) == 1
    assert rules[0].id == "custom-condition-alert-2"
    assert rules[0].source_families == ("COMMSTAT",)


def test_condition_alert_ingest_bridge_returns_pending_observations_from_settings_payload() -> None:
    info = analyze_commstat_fields(
        artifact_kind="MESSAGE",
        title="MAGCON+2",
        body="Condition change for the net.",
        from_call="N1MAG",
        target="@MAGNET",
        report_group="MAGNET",
        source_family="CommStat",
    )
    settings = condition_alert_rules_setting_payload(
        [
            {
                "id": "magcon-active",
                "enabled": True,
                "name": "MagNet MAGCON",
                "operating_group": "MAGNET",
                "source_families": ["CommStat"],
                "target_groups": ["MAGNET"],
                "allowed_sender_mode": "explicit list",
                "allowed_senders": ["N1MAG"],
                "pattern": r"MAGCON\+?([1-5])",
            }
        ]
    )

    observations = condition_alert_observations_for_message_intelligence(
        info,
        settings,
        source_ref="commstat_artifacts:42",
        source_family="CommStat",
        source_radio_id=8,
        source_app="CommStat",
        received_utc="2026-08-21T20:00:00+00:00",
    )

    assert len(observations) == 1
    assert observations[0].source_family == "condition_alert"
    assert observations[0].source_radio_id == 8
    assert observations[0].groups == ("MAGNET",)
    assert observations[0].urgency == "LEVEL 2"


def test_condition_alert_ingest_bridge_honors_disabled_builtin_template() -> None:
    info = analyze_commstat_fields(
        artifact_kind="MESSAGE",
        title="MAGCON+2",
        body="Condition change for the net.",
        from_call="N1MAG",
        target="@MAGNET",
        report_group="MAGNET",
        source_family="CommStat",
    )

    observations = condition_alert_observations_for_message_intelligence(
        info,
        None,
        source_ref="commstat_artifacts:42",
        source_family="CommStat",
    )

    assert observations == ()


def test_condition_alert_settings_ui_wires_rule_editor_to_settings_key() -> None:
    source = Path("freqinout/gui/settings_tab.py").read_text(encoding="utf-8")

    assert 'QGroupBox("Condition Alerts")' in source
    assert '"conditionAlertRulesTable"' in source
    assert "CONDITION_ALERT_RULES_SETTING_KEY" in source
    assert "condition_alert_rules_from_settings" in source
    assert "condition_alert_rules_to_settings" in source
    assert "Reset MagNet Template" in source
    assert "condition_alert_auto_sop_chk" in source
    assert "AUTO_SOP_INVOCATION_SETTING_KEY" in source
    assert "self._add_settings_section(self.condition_alert_section_group, scope=\"global\")" in source
    assert "def _save_condition_alert_rules" in source
    assert "Apply automatically" in source
    assert "Ask before applying" in source
    save_slice = source[
        source.index("def _save_condition_alert_rules") : source.index("def _summary_varac_settings")
    ]
    assert "AUTO_SOP_INVOCATION_SETTING_KEY" in save_slice
    assert "self._settings_dirty = False" not in source[
        source.index("def _save_condition_alert_rules") : source.index("def _summary_varac_settings")
    ]


def test_condition_alert_rules_are_wired_into_background_and_message_file_projection() -> None:
    background = Path("freqinout/core/background_ingest.py").read_text(encoding="utf-8")
    message_viewer = Path("freqinout/gui/message_viewer_tab.py").read_text(encoding="utf-8")
    varac_ingest = Path("freqinout/core/varac_ingest.py").read_text(encoding="utf-8")

    assert "CONDITION_ALERT_RULES_SETTING_KEY" in background
    assert "condition_alert_rules=condition_alert_rules" in background
    assert "worker_settings.get(CONDITION_ALERT_RULES_SETTING_KEY" in background
    assert "AUTO_SOP_INVOCATION_SETTING_KEY" in background
    assert "plan_condition_sop_invocations" in background
    assert "execute_condition_sop_invocation_plans" in background
    assert "def _run_condition_sop_invocation" in background
    assert "assigned_plan_rf_guard_impacts_for_sop_update" in background
    assert "schedule_layer_rows_for_condition_decision" in background
    assert "condition_sop_invocation_audited" in background
    assert "condition_sop_invocation_applied" in background
    assert "_queue_controller_thread_call" in background
    assert "deferred_ids" in background
    main = Path("freqinout/gui/main_window.py").read_text(encoding="utf-8")
    assert "condition_sop_invocation_audited.connect" in main
    assert "condition_sop_invocation_applied.connect" in main
    assert "station_health_tab.refresh_from_registry" in main
    assert "CONDITION_ALERT_RULES_SETTING_KEY" in message_viewer
    assert "condition_alert_rules=self.settings.get(CONDITION_ALERT_RULES_SETTING_KEY" in message_viewer
    assert "include_commstat_artifacts=True" in message_viewer
    assert "CONDITION_ALERT_RULES_SETTING_KEY" in varac_ingest
    assert "_mirror_varac_condition_alerts" in varac_ingest


def test_varac_live_ingest_mirrors_condition_alert_observations(tmp_path: Path) -> None:
    rules = condition_alert_rules_setting_payload(
        [
            {
                "id": "varac-magcon",
                "enabled": True,
                "name": "VarAC MAGCON",
                "operating_group": "MAGNET",
                "source_families": ["VarAC"],
                "target_groups": ["MAGNET"],
                "allowed_sender_mode": "explicit list",
                "allowed_senders": ["N1MAG"],
                "pattern": r"MAGCON\+?([1-5])",
            }
        ]
    )
    conn = sqlite3.connect(tmp_path / "fio.db")
    try:
        count = _mirror_varac_condition_alerts(
            conn,
            rules=rules,
            source_ref="varac_messages:radio-b:broadcast:7",
            msg_type="BROADCAST",
            subject="Broadcast",
            body="MAGCON+3 for tonight",
            from_call="N1MAG",
            to_call="@MAGNET",
            ts_utc="2026-08-21T20:00:00+00:00",
            source_key="radio-b",
            source_label="VarAC-B",
        )

        rows = conn.execute(
            """
            SELECT source_family, source_ref, source_app, from_call, to_target, urgency, subject
            FROM observation_projection
            """
        ).fetchall()
    finally:
        conn.close()

    assert count == 1
    assert rows == [
        (
            "condition_alert",
            "varac_messages:radio-b:broadcast:7",
            "VarAC-B",
            "N1MAG",
            "@MAGNET",
            "LEVEL 3",
            "VarAC MAGCON: Level 3",
        )
    ]


def test_condition_alert_source_family_has_operator_readable_labels() -> None:
    assert source_family_label("condition_alert") == "Condition Alert"
    assert source_short_label("condition_alert") == "ALRT"
    assert source_family_label("rf_pin") == "RF Pin"
    assert source_short_label("rf_pin") == "PIN"
