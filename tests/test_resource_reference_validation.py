"""Focused characterization tests for LN-1 bundled reference guidance."""

from __future__ import annotations

import copy
from datetime import date
import json

import pytest

from freqinout.core.resource_reference_validation import (
    OUTSIDE_SELECTED_REFERENCE,
    REFERENCE_UNAVAILABLE_OR_OUT_OF_DATE,
    TRANSMIT_ELIGIBILITY_NOT_EVALUATED,
    WITHIN_SELECTED_REFERENCE,
    ReferenceManifestError,
    bundled_manifest_path,
    find_reference,
    load_bundled_reference_manifest,
    validate_selected_reference,
)


def test_bundled_manifest_is_versioned_cited_and_has_required_initial_coverage() -> None:
    manifest = load_bundled_reference_manifest()

    assert manifest["schema_version"] == 1
    assert manifest["content_version"].startswith("us-fcc-reference-v1")
    assert manifest["verified_date"] == "2026-09-09"
    assert {source["citation"] for source in manifest["sources"]} == {
        "47 CFR 97.301",
        "47 CFR 97.305",
        "47 CFR 95.1763",
    }
    assert {item["reference_key"] for item in manifest["references"]} >= {
        "us-amateur-6m",
        "us-amateur-2m",
        "us-amateur-1-25m-219",
        "us-amateur-1-25m-222",
        "us-amateur-70cm",
        "us-amateur-33cm",
        "us-amateur-23cm",
    }
    gmrs = [item for item in manifest["references"] if item["service"] == "GMRS"]
    assert len(gmrs) == 30
    assert all(item["kind"] == "channel" and isinstance(item["center_hz"], int) for item in gmrs)


def test_amateur_range_comparison_is_integer_hz_and_advisory_only() -> None:
    result = validate_selected_reference(
        load_bundled_reference_manifest(), "us-amateur-2m", 146_520_000, as_of="2026-09-09"
    )

    assert result.state == WITHIN_SELECTED_REFERENCE
    assert result.transmit_eligibility == TRANSMIT_ELIGIBILITY_NOT_EVALUATED
    assert "authorization" in result.detail
    assert validate_selected_reference(
        load_bundled_reference_manifest(), "us-amateur-2m", 148_000_001, as_of="2026-09-09"
    ).state == OUTSIDE_SELECTED_REFERENCE


def test_gmrs_centers_are_exact_and_preserve_channel_purpose() -> None:
    manifest = load_bundled_reference_manifest()
    reference = find_reference(manifest, "us-gmrs-467-main-01")

    assert reference is not None
    assert reference["center_hz"] == 467_550_000
    assert "repeater input" in reference["purpose"]
    assert validate_selected_reference(
        manifest, "us-gmrs-467-main-01", 467_550_000, as_of=date(2026, 9, 9)
    ).state == WITHIN_SELECTED_REFERENCE
    assert validate_selected_reference(
        manifest, "us-gmrs-467-main-01", 467_550_001, as_of=date(2026, 9, 9)
    ).state == OUTSIDE_SELECTED_REFERENCE


def test_missing_or_outdated_reference_never_falls_back_to_a_legal_conclusion() -> None:
    manifest = load_bundled_reference_manifest()

    missing = validate_selected_reference(manifest, "not-present", 146_520_000, as_of="2026-09-09")
    stale = validate_selected_reference(
        manifest, "us-amateur-2m", 146_520_000, as_of="2027-09-11", max_age_days=366
    )
    for result in (missing, stale):
        assert result.state == REFERENCE_UNAVAILABLE_OR_OUT_OF_DATE
        assert result.transmit_eligibility == TRANSMIT_ELIGIBILITY_NOT_EVALUATED


def test_loader_rejects_hash_tampering_without_writing_the_bundled_file(tmp_path) -> None:
    before = bundled_manifest_path().stat().st_mtime_ns
    manifest = load_bundled_reference_manifest()
    manifest["references"][0]["upper_hz"] = 53_999_999
    tampered = tmp_path / "tampered.json"
    tampered.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(ReferenceManifestError, match="content hash"):
        load_bundled_reference_manifest(tampered)
    assert bundled_manifest_path().stat().st_mtime_ns == before


def test_validator_reports_an_invalid_manifest_as_unavailable() -> None:
    manifest = copy.deepcopy(load_bundled_reference_manifest())
    manifest["references"][1]["reference_key"] = manifest["references"][0]["reference_key"]

    result = validate_selected_reference(manifest, "us-amateur-6m", 50_000_000, as_of="2026-09-09")
    assert result.state == REFERENCE_UNAVAILABLE_OR_OUT_OF_DATE
    assert result.transmit_eligibility == TRANSMIT_ELIGIBILITY_NOT_EVALUATED
