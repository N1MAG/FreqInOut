from __future__ import annotations

from freqinout.core.condition_level_update import (
    condition_group_state_snapshot,
    revert_operating_group_condition_snapshot,
)
from freqinout.core.condition_sop_revert import revert_condition_sop_audit_row


def test_condition_group_snapshot_and_revert_restores_rows() -> None:
    settings = {
        "operating_groups": [
            {"group": "MAGNET", "band": "20M", "use_condition_levels": True, "condition_level": 2},
            {"group": "MAGNET", "band": "40M", "use_condition_levels": False},
            {"group": "AMRRON", "band": "20M", "use_condition_levels": True, "condition_level": 4},
        ]
    }
    snapshot = condition_group_state_snapshot(settings, operating_group="MAGNET")
    changed = {
        "operating_groups": [
            {"group": "MAGNET", "band": "20M", "use_condition_levels": True, "condition_level": 5},
            {"group": "MAGNET", "band": "40M", "use_condition_levels": True, "condition_level": 5},
            {"group": "AMRRON", "band": "20M", "use_condition_levels": True, "condition_level": 4},
        ]
    }

    result = revert_operating_group_condition_snapshot(
        changed,
        operating_group="MAGNET",
        snapshot=snapshot,
    )

    assert result.warnings == ()
    assert result.restored_rows == 2
    rows = result.settings_data["operating_groups"]
    assert rows[0]["condition_level"] == 2
    assert rows[0]["use_condition_levels"] is True
    assert "condition_level" not in rows[1]
    assert rows[1]["use_condition_levels"] is False
    assert rows[2]["condition_level"] == 4


def test_revert_condition_sop_audit_row_requires_applied_snapshot() -> None:
    current = {"operating_groups": [{"group": "MAGNET", "condition_level": 5, "use_condition_levels": True}]}

    blocked = revert_condition_sop_audit_row(
        current,
        {"status": "blocked", "operating_group": "MAGNET", "payload": {}},
    )
    missing_snapshot = revert_condition_sop_audit_row(
        current,
        {"status": "applied", "operating_group": "MAGNET", "payload": {}},
    )

    assert blocked.ok is False
    assert "only applied" in blocked.warnings[0]
    assert missing_snapshot.ok is False
    assert "snapshot" in missing_snapshot.warnings[0]


def test_revert_condition_sop_audit_row_restores_previous_condition_state() -> None:
    current = {"operating_groups": [{"group": "MAGNET", "condition_level": 5, "use_condition_levels": True}]}
    audit = {
        "status": "applied",
        "operating_group": "MAGNET",
        "payload": {
            "previous_condition_group_state": [
                {"index": 0, "group": "MAGNET", "use_condition_levels": True, "condition_level": 3}
            ]
        },
    }

    result = revert_condition_sop_audit_row(current, audit)

    assert result.ok is True
    assert result.operating_group == "MAGNET"
    assert result.restored_rows == 1
    assert result.settings_data["operating_groups"][0]["condition_level"] == 3
