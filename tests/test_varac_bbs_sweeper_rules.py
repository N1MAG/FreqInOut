from freqinout.core.varac_bbs_sweeper import (
    BbsSweeperRule,
    BbsSweeperCopyPlan,
    apply_bbs_sweeper_copy_plan,
    bbs_sweeper_rule_matches,
    bbs_sweeper_rules_to_data,
    load_bbs_sweeper_rules,
    matching_bbs_sweeper_targets,
    plan_bbs_sweeper_copies,
)


def test_bbs_sweeper_rule_requires_enabled_target_and_match_terms() -> None:
    disabled = BbsSweeperRule(
        id="alerts",
        name="Alerts",
        enabled=False,
        subject_contains=("fire",),
        target_location_ids=("alerts",),
    )
    no_target = BbsSweeperRule(id="alerts", name="Alerts", enabled=True, subject_contains=("fire",))
    no_match_terms = BbsSweeperRule(id="alerts", name="Alerts", enabled=True, target_location_ids=("alerts",))

    assert bbs_sweeper_rule_matches(disabled, source_family="FLMsg", subject="Wildfire update") is False
    assert bbs_sweeper_rule_matches(no_target, source_family="FLMsg", subject="Wildfire update") is False
    assert bbs_sweeper_rule_matches(no_match_terms, source_family="FLMsg", subject="Wildfire update") is False


def test_bbs_sweeper_rule_matches_sender_and_subject_when_both_are_configured() -> None:
    rule = BbsSweeperRule(
        id="fire-watch",
        name="Fire Watch",
        enabled=True,
        source_families=("varac_bbs", "flmsg"),
        from_calls=("K7ETC",),
        subject_contains=("wildfire", "evacuation"),
        target_location_ids=("regional-intel",),
    )

    assert (
        bbs_sweeper_rule_matches(
            rule,
            source_family="FLMsg",
            from_call="K7ETC",
            subject="County wildfire update",
        )
        is True
    )
    assert (
        bbs_sweeper_rule_matches(
            rule,
            source_family="FLMsg",
            from_call="K7ETC",
            body="Evacuation center open",
        )
        is True
    )
    assert (
        bbs_sweeper_rule_matches(
            rule,
            source_family="FLMsg",
            from_call="N0PE",
            subject="County wildfire update",
        )
        is False
    )
    assert (
        bbs_sweeper_rule_matches(
            rule,
            source_family="FLAmp",
            from_call="K7ETC",
            subject="County wildfire update",
        )
        is False
    )


def test_bbs_sweeper_targets_can_copy_to_multiple_managed_locations() -> None:
    rules = load_bbs_sweeper_rules(
        [
            {
                "id": "weather",
                "name": "Weather",
                "enabled": True,
                "source_families": "BBS, FLMsg",
                "from_calls": "k7etc, @w5tta",
                "subject_contains": "storm; shelter",
                "target_location_ids": ["weather", "ops"],
                "copy_mode": "copy_once",
            },
            {
                "id": "disabled",
                "name": "Disabled",
                "enabled": False,
                "subject_contains": "storm",
                "target_location_ids": ["ignored"],
            },
        ]
    )

    assert rules[0].source_families == ("varac_bbs", "flmsg")
    assert rules[0].from_calls == ("K7ETC", "W5TTA")
    assert rules[0].subject_contains == ("storm", "shelter")
    assert rules[0].target_location_ids == ("weather", "ops")
    assert rules[0].copy_mode == "copy_once"
    assert matching_bbs_sweeper_targets(
        rules,
        source_family="VarAC BBS Inbox",
        from_call="W5TTA",
        subject="Storm shelter status",
    ) == ("weather", "ops")

    data = bbs_sweeper_rules_to_data(rules)
    assert data[0]["target_location_ids"] == ["weather", "ops"]
    assert data[0]["copy_mode"] == "copy_once"


def test_bbs_sweeper_copy_plan_filters_locations_and_dedupes_targets() -> None:
    rules = load_bbs_sweeper_rules(
        [
            {
                "id": "weather",
                "name": "Weather",
                "enabled": True,
                "sources": ["FLMsg"],
                "from_calls": ["K7ETC"],
                "subject_contains": ["storm"],
                "target_location_ids": ["weather", "ops", "weather"],
            },
            {
                "id": "ops",
                "name": "Ops",
                "enabled": True,
                "sources": ["FLAmp"],
                "from_calls": ["K7ETC"],
                "subject_contains": ["storm"],
                "target_location_ids": ["ops"],
            },
            {
                "id": "missing-target",
                "name": "Missing Target",
                "enabled": True,
                "sources": ["FLMsg"],
                "from_calls": ["K7ETC"],
                "subject_contains": ["storm"],
                "target_location_ids": ["not-managed"],
            },
        ]
    )

    plans = plan_bbs_sweeper_copies(
        rules,
        [
            {
                "path": "/tmp/inbox/storm.k2s",
                "source": "FL Msg",
                "sender": "k7etc",
                "subject": "Storm shelter update",
            },
            {
                "path": "/tmp/inbox/storm-repeat.k2s",
                "source": "FLAmp",
                "sender": "K7ETC",
                "subject": "Storm shelter update",
            },
        ],
        available_location_ids=["weather", "ops"],
    )

    assert len(plans) == 2
    assert plans[0].source_path == "/tmp/inbox/storm.k2s"
    assert plans[0].source_family == "flmsg"
    assert plans[0].target_location_ids == ("weather", "ops")
    assert plans[0].matched_rule_ids == ("weather", "missing-target")
    assert plans[1].source_family == "flamp"
    assert plans[1].target_location_ids == ("ops",)
    assert plans[1].matched_rule_ids == ("ops",)


def test_bbs_sweeper_copy_plan_skips_unusable_candidates() -> None:
    rules = [
        BbsSweeperRule(
            id="alerts",
            name="Alerts",
            enabled=True,
            source_families=("varac_bbs",),
            subject_contains=("alert",),
            target_location_ids=("alerts",),
        )
    ]

    assert (
        plan_bbs_sweeper_copies(
            rules,
            [
                {"source": "BBS", "subject": "Alert without path"},
                {"path": "/tmp/ignored.txt", "source": "unknown", "subject": "Alert"},
                {"path": "/tmp/no-match.txt", "source": "BBS", "subject": "Routine note"},
            ],
            available_location_ids=["alerts"],
        )
        == []
    )


def test_apply_bbs_sweeper_copy_plan_copies_to_multiple_locations_without_overwrite(tmp_path) -> None:
    src_dir = tmp_path / "src"
    weather_dir = tmp_path / "weather"
    ops_dir = tmp_path / "ops"
    src_dir.mkdir()
    weather_dir.mkdir()
    ops_dir.mkdir()
    src = src_dir / "Storm report .k2s"
    src.write_text("payload", encoding="utf-8")
    existing = weather_dir / "Storm report.k2s"
    existing.write_text("existing", encoding="utf-8")

    results = apply_bbs_sweeper_copy_plan(
        [
            BbsSweeperCopyPlan(
                source_path=str(src),
                source_family="flmsg",
                target_location_ids=("weather", "ops"),
                matched_rule_ids=("weather-rule",),
            )
        ],
        {"weather": weather_dir, "ops": ops_dir},
    )

    assert [result.copied for result in results] == [True, True]
    assert (weather_dir / "Storm report-2.k2s").read_text(encoding="utf-8") == "payload"
    assert existing.read_text(encoding="utf-8") == "existing"
    assert (ops_dir / "Storm report.k2s").read_text(encoding="utf-8") == "payload"
    assert all(result.matched_rule_ids == ("weather-rule",) for result in results)


def test_apply_bbs_sweeper_copy_plan_reports_missing_targets_and_dry_run(tmp_path) -> None:
    src = tmp_path / "Bulletin.k2s"
    target = tmp_path / "target"
    target.mkdir()
    src.write_text("payload", encoding="utf-8")

    results = apply_bbs_sweeper_copy_plan(
        [
            BbsSweeperCopyPlan(
                source_path=str(src),
                source_family="varac_bbs",
                target_location_ids=("target", "missing"),
                matched_rule_ids=("rule-1",),
            )
        ],
        {"target": target},
        dry_run=True,
    )

    assert results[0].copied is False
    assert results[0].skipped_reason == "dry_run"
    assert results[0].target_path.endswith("Bulletin.k2s")
    assert not (target / "Bulletin.k2s").exists()
    assert results[1].copied is False
    assert results[1].skipped_reason == "target_missing"
