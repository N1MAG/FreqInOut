from __future__ import annotations

from pathlib import Path

from freqinout.core.js8_storage import variant_family_from_version
from freqinout.core.js8_storage_reconciliation import reconcile_js8_storage_profile


class _Store:
    def __init__(self, row: dict[str, object]) -> None:
        self.row = dict(row)
        self.saved: list[dict[str, object]] = []

    def get_js8_instance(self, instance_id: int) -> dict[str, object]:
        assert instance_id == int(self.row["id"])
        return dict(self.row)

    def save_js8_instance(self, values: dict[str, object]) -> dict[str, object]:
        self.saved.append(dict(values))
        self.row.update(values)
        return dict(self.row)


def test_reconciliation_verifies_only_the_configured_bounded_root(tmp_path: Path) -> None:
    root = tmp_path / "JS8Call - field"
    root.mkdir()
    (root / "DIRECTED.TXT").write_text("traffic", encoding="utf-8")
    save_dir = tmp_path / "operator-saves"
    store = _Store(
        {
            "id": 7,
            "system_key": "field-js8",
            "name": "Field JS8",
            "variant_family": "js8call_2_2",
            "variant_version": "2.2.0",
            "rig_name": "field",
            "rig_name_source": "managed",
            "application_data_root": str(root),
            "save_dir": str(save_dir),
            "storage_mode": "unverified",
            "storage_evidence": "launch_planned",
        }
    )

    result = reconcile_js8_storage_profile(store, {"js8_instance_id": 7})

    assert result.state == "verified"
    assert len(store.saved) == 1
    saved = store.saved[0]
    assert saved["application_data_root"] == str(root.resolve())
    assert saved["directed_path"] == str(root.resolve() / "DIRECTED.TXT")
    assert saved["all_path"] == str(root.resolve() / "ALL.TXT")
    assert saved["inbox_path"] == str(root.resolve() / "inbox.db3")
    assert saved["save_dir"] == str(save_dir)
    assert saved["storage_mode"] == "rig_scoped"
    assert saved["storage_evidence"] == "runtime_verified:message_files"


def test_reconciliation_is_write_free_when_verified_mapping_is_unchanged(tmp_path: Path) -> None:
    root = tmp_path / "JS8Call - field"
    root.mkdir()
    (root / "inbox.db3").write_bytes(b"sqlite")
    store = _Store(
        {
            "id": 8,
            "variant_family": "js8call_improved_3_0_3",
            "variant_version": "3.0.3",
            "rig_name": "field",
            "application_data_root": str(root.resolve()),
            "all_path": str(root.resolve() / "ALL.TXT"),
            "directed_path": str(root.resolve() / "DIRECTED.TXT"),
            "inbox_path": str(root.resolve() / "inbox.db3"),
            "storage_mode": "rig_scoped",
            "storage_evidence": "runtime_verified:message_files",
        }
    )

    result = reconcile_js8_storage_profile(store, {"js8_instance_id": 8})

    assert result.state == "verified_unchanged"
    assert store.saved == []


def test_reconciliation_does_not_claim_missing_message_files(tmp_path: Path) -> None:
    root = tmp_path / "empty-root"
    root.mkdir()
    store = _Store(
        {
            "id": 9,
            "variant_family": "js8call_2_2",
            "variant_version": "2.2.0",
            "rig_name": "field",
            "application_data_root": str(root),
            "storage_mode": "unverified",
            "storage_evidence": "launch_planned",
        }
    )

    result = reconcile_js8_storage_profile(store, {"js8_instance_id": 9})

    assert result.state == "needs_verification"
    assert store.saved == []


def test_reconciliation_preserves_legacy_explicit_path_without_adopting_ambient_root(tmp_path: Path) -> None:
    directed = tmp_path / "legacy" / "DIRECTED.TXT"
    directed.parent.mkdir()
    directed.write_text("legacy", encoding="utf-8")
    store = _Store(
        {
            "id": 10,
            "variant_family": "unknown",
            "directed_path": str(directed),
            "application_data_root": "",
            "storage_mode": "unverified",
            "storage_evidence": "legacy_explicit_path",
        }
    )

    result = reconcile_js8_storage_profile(
        store,
        {"js8_instance_id": 10, "js8_directed_path": str(directed)},
    )

    assert result.state == "legacy_preserved"
    assert store.saved == []


def test_reviewed_native_versions_map_to_storage_capability_families() -> None:
    assert variant_family_from_version("JS8Call v2.2.0") == "js8call_2_2"
    assert variant_family_from_version("3.0.3") == "js8call_improved_3_0_3"
    assert variant_family_from_version("Subspace 4.1.0.478") == "js8call_subspace_4_1"
    assert variant_family_from_version("9.9 downstream") == "unknown"


def test_reconciliation_keeps_mismatch_state_until_operator_review(tmp_path: Path) -> None:
    root = tmp_path / "retained-root"
    root.mkdir()
    (root / "ALL.TXT").write_text("traffic", encoding="utf-8")
    store = _Store(
        {
            "id": 11,
            "variant_family": "js8call_2_2",
            "variant_version": "2.2.0",
            "rig_name": "prior",
            "application_data_root": str(root),
            "storage_mode": "unverified",
            "storage_verified_utc": "2026-09-10T12:00:00Z",
            "storage_evidence": "mismatch:launch_planned",
        }
    )

    result = reconcile_js8_storage_profile(store, {"js8_instance_id": 11})

    assert result.state == "mismatch"
    assert result.application_data_root == str(root)
    assert store.saved == []
