from freqinout.core.condition_level_update import apply_operating_group_condition_level


def test_apply_condition_level_updates_all_rows_for_group() -> None:
    result = apply_operating_group_condition_level(
        {
            "operating_groups": [
                {"group": "MAGNET", "band": "20M", "use_condition_levels": False, "condition_level": 5},
                {"group": "MAGNET", "band": "40M", "use_condition_levels": False, "condition_level": 5},
                {"group": "AMRRON", "band": "20M", "use_condition_levels": True, "condition_level": 2},
            ]
        },
        operating_group="@MAGNET",
        condition_level=3,
    )

    assert result.operating_group == "MAGNET"
    assert result.condition_level == 3
    assert result.matched_rows == 2
    assert result.changed_rows == 2
    assert result.warnings == ()
    rows = result.settings_data["operating_groups"]
    magnet_rows = [row for row in rows if row["group"] == "MAGNET"]
    assert all(row["use_condition_levels"] is True for row in magnet_rows)
    assert all(row["condition_level"] == 3 for row in magnet_rows)
    assert rows[2]["condition_level"] == 2


def test_apply_condition_level_reports_missing_group_without_creating_by_default() -> None:
    result = apply_operating_group_condition_level(
        {"operating_groups": [{"group": "AMRRON", "condition_level": 2}]},
        operating_group="MAGNET",
        condition_level=9,
    )

    assert result.condition_level == 5
    assert result.matched_rows == 0
    assert result.changed_rows == 0
    assert result.created is False
    assert result.warnings == ("operating group MAGNET is not configured",)


def test_apply_condition_level_can_create_group_stub_when_requested() -> None:
    result = apply_operating_group_condition_level(
        {"operating_groups": []},
        operating_group="MR08",
        condition_level=1,
        create_if_missing=True,
    )

    assert result.created is True
    assert result.matched_rows == 1
    assert result.settings_data["operating_groups"][0]["group"] == "MR08"
    assert result.settings_data["operating_groups"][0]["use_condition_levels"] is True
    assert result.settings_data["operating_groups"][0]["condition_level"] == 1
