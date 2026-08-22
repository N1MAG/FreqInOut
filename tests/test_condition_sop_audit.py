from freqinout.core.condition_sop_audit import (
    append_condition_sop_invocation_audit,
    condition_sop_audit_display,
    condition_sop_audit_observability_item,
    condition_sop_audit_summary,
    ConditionSopAuditSummary,
    list_condition_sop_invocation_audit,
)


def test_condition_sop_audit_appends_and_lists_recent_records(tmp_path) -> None:
    db_path = tmp_path / "fio.db"

    row_id = append_condition_sop_invocation_audit(
        db_path,
        {
            "event": "condition_sop_invocation",
            "decision": "apply",
            "operating_group": "MAGNET",
            "condition_level": 4,
            "sop_profile_id": "7",
            "sop_profile_name": "MagNet Condition 4",
            "changed_rows": 2,
        },
        status="planned",
        created_utc="2026-08-21T20:00:00+00:00",
    )

    rows = list_condition_sop_invocation_audit(db_path)

    assert row_id == 1
    assert len(rows) == 1
    assert rows[0]["operating_group"] == "MAGNET"
    assert rows[0]["condition_level"] == 4
    assert rows[0]["status"] == "planned"
    assert rows[0]["payload"]["changed_rows"] == 2


def test_condition_sop_audit_summary_returns_latest_and_counts(tmp_path) -> None:
    db_path = tmp_path / "fio.db"
    append_condition_sop_invocation_audit(
        db_path,
        {
            "event": "condition_sop_invocation",
            "decision": "prompt",
            "operating_group": "MAGNET",
            "condition_level": 3,
            "execution_message": "operator confirmation required",
        },
        status="prompt",
        created_utc="2026-08-21T20:00:00+00:00",
    )
    append_condition_sop_invocation_audit(
        db_path,
        {
            "event": "condition_sop_invocation",
            "decision": "blocked",
            "operating_group": "AMRRON",
            "condition_level": 5,
            "execution_message": "RF Guard conflict",
        },
        status="blocked",
        created_utc="2026-08-21T20:01:00+00:00",
    )

    summary = condition_sop_audit_summary(db_path)

    assert summary.latest_status == "blocked"
    assert summary.latest_group == "AMRRON"
    assert summary.latest_condition_level == 5
    assert summary.latest_message == "RF Guard conflict"
    assert summary.blocked_count == 1
    assert summary.prompt_count == 1

    display = condition_sop_audit_display(summary)
    assert display.severity == "warning"
    assert display.text == "SOP automation needs review: AMRRON L5. RF Guard conflict"


def test_condition_sop_audit_display_formats_operator_states() -> None:
    applied = condition_sop_audit_display(
        ConditionSopAuditSummary(
            latest_status="applied",
            latest_message="Updated 2 SOP rows",
            latest_group="MAGNET",
            latest_condition_level=4,
        )
    )
    prompt = condition_sop_audit_display(
        ConditionSopAuditSummary(
            latest_status="prompt",
            latest_message="operator confirmation required",
            latest_group="MAGNET",
            latest_condition_level=3,
        )
    )

    assert applied.severity == "ok"
    assert applied.text == "SOP automation: applied MAGNET L4. Updated 2 SOP rows"
    assert prompt.severity == "review"
    assert prompt.text == "SOP automation ready for review: MAGNET L3. operator confirmation required"


def test_condition_sop_audit_observability_item_surfaces_blocked_automation(tmp_path) -> None:
    db_path = tmp_path / "fio.db"
    append_condition_sop_invocation_audit(
        db_path,
        {
            "event": "condition_sop_invocation",
            "decision": "blocked",
            "operating_group": "MAGNET",
            "condition_level": 5,
            "execution_message": "RF Guard conflict",
        },
        status="blocked",
        created_utc="2026-08-21T20:01:00+00:00",
    )

    item = condition_sop_audit_observability_item(db_path)

    assert item is not None
    assert item["dependency"] == "SOP Automation"
    assert item["scope"] == "MAGNET"
    assert item["state"] == "Needs Review"
    assert item["severity"] == "warning"
    assert item["is_issue"] is True
    assert item["last_issue"] == "SOP automation needs review: MAGNET L5. RF Guard conflict"
