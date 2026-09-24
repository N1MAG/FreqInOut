"""Read-only advisory validation for bundled frequency reference manifests.

This module deliberately does not decide whether an operator may transmit.  It
only compares an integer-Hz frequency with a selected, bundled reference and
reports the age/integrity of that reference.  It has no database, network, or
write-side effects.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping


WITHIN_SELECTED_REFERENCE = "Within selected reference"
OUTSIDE_SELECTED_REFERENCE = "Outside selected reference"
TRANSMIT_ELIGIBILITY_NOT_EVALUATED = "Transmit eligibility not evaluated"
REFERENCE_UNAVAILABLE_OR_OUT_OF_DATE = "Reference unavailable/out of date"

_MANIFEST_FILENAME = "us_fcc_reference_v1.json"
_REQUIRED_ROOT_FIELDS = {
    "schema_version",
    "catalog_key",
    "jurisdiction",
    "content_version",
    "effective_date",
    "verified_date",
    "sources",
    "references",
    "content_sha256",
}


class ReferenceManifestError(ValueError):
    """Raised when a bundled reference package is malformed or untrusted."""


@dataclass(frozen=True)
class ReferenceValidation:
    """An advisory result that intentionally excludes legal authorization."""

    state: str
    transmit_eligibility: str
    reference_key: str | None = None
    reference_label: str | None = None
    frequency_hz: int | None = None
    detail: str = ""


def bundled_manifest_path() -> Path:
    """Return the packaged manifest location without opening or changing it."""

    return Path(__file__).resolve().parents[2] / "config" / "resource_catalog" / _MANIFEST_FILENAME


def load_bundled_reference_manifest(path: str | Path | None = None) -> dict[str, Any]:
    """Load and validate a manifest entirely in memory.

    ``path`` exists for fixture/test use.  No fallback download, database
    update, cache, or other write is attempted if it cannot be read.
    """

    manifest_path = Path(path) if path is not None else bundled_manifest_path()
    try:
        document = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ReferenceManifestError(f"Unable to read reference manifest: {exc}") from exc
    _validate_manifest(document)
    return document


def find_reference(manifest: Mapping[str, Any], reference_key: str) -> Mapping[str, Any] | None:
    """Find one reference by stable bundled key after validating the manifest."""

    _validate_manifest(manifest)
    for reference in manifest["references"]:
        if reference["reference_key"] == reference_key:
            return reference
    return None


def validate_selected_reference(
    manifest: Mapping[str, Any] | None,
    reference_key: str | None,
    frequency_hz: int | None,
    *,
    as_of: date | str | None = None,
    max_age_days: int = 366,
) -> ReferenceValidation:
    """Compare a frequency to the selected reference, advisory-only.

    A missing, malformed, unknown, or old package is deliberately reported as
    unavailable rather than falling back to a broader band or inferring legal
    authorization.  Channel centers require an exact integer-Hz match; ranges
    include both stated endpoints.
    """

    if not isinstance(frequency_hz, int) or isinstance(frequency_hz, bool) or frequency_hz < 0:
        return _unavailable("A non-negative integer frequency in Hz is required.")
    if not reference_key or not isinstance(reference_key, str):
        return _unavailable("No selected reference was supplied.", frequency_hz)
    if max_age_days < 0:
        raise ValueError("max_age_days must be non-negative")
    try:
        if manifest is None:
            raise ReferenceManifestError("No reference manifest was supplied.")
        _validate_manifest(manifest)
        verified_date = _parse_date(manifest["verified_date"], "verified_date")
        today = _coerce_date(as_of)
    except (ReferenceManifestError, TypeError, ValueError) as exc:
        return _unavailable(str(exc), frequency_hz)

    if (today - verified_date).days > max_age_days:
        return _unavailable(
            f"Reference was verified on {verified_date.isoformat()} and exceeds the configured age.",
            frequency_hz,
        )

    reference = next(
        (item for item in manifest["references"] if item["reference_key"] == reference_key), None
    )
    if reference is None:
        return _unavailable("The selected reference is not present in this manifest.", frequency_hz)

    if reference["kind"] == "band_range":
        within = reference["lower_hz"] <= frequency_hz <= reference["upper_hz"]
    else:
        within = frequency_hz == reference["center_hz"]
    return ReferenceValidation(
        state=WITHIN_SELECTED_REFERENCE if within else OUTSIDE_SELECTED_REFERENCE,
        transmit_eligibility=TRANSMIT_ELIGIBILITY_NOT_EVALUATED,
        reference_key=reference["reference_key"],
        reference_label=reference["label"],
        frequency_hz=frequency_hz,
        detail="Frequency comparison only; license, location, emission, equipment, and authorization are not evaluated.",
    )


def _unavailable(detail: str, frequency_hz: int | None = None) -> ReferenceValidation:
    return ReferenceValidation(
        state=REFERENCE_UNAVAILABLE_OR_OUT_OF_DATE,
        transmit_eligibility=TRANSMIT_ELIGIBILITY_NOT_EVALUATED,
        frequency_hz=frequency_hz,
        detail=detail,
    )


def _validate_manifest(document: Mapping[str, Any]) -> None:
    if not isinstance(document, Mapping):
        raise ReferenceManifestError("Reference manifest must be an object.")
    missing = _REQUIRED_ROOT_FIELDS.difference(document)
    if missing:
        raise ReferenceManifestError(f"Reference manifest is missing fields: {', '.join(sorted(missing))}.")
    if document["schema_version"] != 1 or document["jurisdiction"] != "US":
        raise ReferenceManifestError("Unsupported reference manifest schema or jurisdiction.")
    if not all(isinstance(document[field], str) and document[field] for field in ("catalog_key", "content_version")):
        raise ReferenceManifestError("Reference manifest needs a catalog key and content version.")
    _parse_date(document["effective_date"], "effective_date")
    _parse_date(document["verified_date"], "verified_date")
    if not isinstance(document["sources"], list) or not document["sources"]:
        raise ReferenceManifestError("Reference manifest must contain cited sources.")
    source_keys: set[str] = set()
    for source in document["sources"]:
        if not isinstance(source, Mapping) or not all(
            isinstance(source.get(field), str) and source[field]
            for field in ("source_key", "citation", "url", "verified_date", "effective_date")
        ):
            raise ReferenceManifestError("Each source needs key, citation, URL, effective date, and verification date.")
        if source["source_key"] in source_keys:
            raise ReferenceManifestError("Source keys must be unique.")
        source_keys.add(source["source_key"])
        _parse_date(source["verified_date"], "source verified_date")
        _parse_date(source["effective_date"], "source effective_date")
    references = document["references"]
    if not isinstance(references, list) or not references:
        raise ReferenceManifestError("Reference manifest must contain references.")
    keys: set[str] = set()
    for item in references:
        _validate_reference(item, keys, source_keys)
    declared_hash = document["content_sha256"]
    if not isinstance(declared_hash, str) or len(declared_hash) != 64 or any(
        character not in "0123456789abcdef" for character in declared_hash
    ):
        raise ReferenceManifestError("Reference manifest content_sha256 is invalid.")
    actual_hash = _reference_content_hash(references)
    if declared_hash != actual_hash:
        raise ReferenceManifestError("Reference manifest content hash does not match its references.")


def _validate_reference(item: Any, keys: set[str], source_keys: set[str]) -> None:
    if not isinstance(item, Mapping):
        raise ReferenceManifestError("Each reference must be an object.")
    required = {"reference_key", "kind", "service", "label", "citation", "source_key"}
    if required.difference(item):
        raise ReferenceManifestError("A reference is missing a required identity or provenance field.")
    key = item["reference_key"]
    if not isinstance(key, str) or not key or key in keys:
        raise ReferenceManifestError("Reference keys must be nonempty and unique.")
    keys.add(key)
    if not all(isinstance(item[field], str) and item[field] for field in required):
        raise ReferenceManifestError(f"Reference {key} has an invalid identity or provenance field.")
    if item["source_key"] not in source_keys:
        raise ReferenceManifestError(f"Reference {key} names an unknown source.")
    if item["kind"] == "band_range":
        lower, upper = item.get("lower_hz"), item.get("upper_hz")
        if not _is_hz(lower) or not _is_hz(upper) or lower > upper:
            raise ReferenceManifestError(f"Range {key} needs ordered integer-Hz endpoints.")
    elif item["kind"] == "channel":
        if not _is_hz(item.get("center_hz")):
            raise ReferenceManifestError(f"Channel {key} needs an integer-Hz center.")
    else:
        raise ReferenceManifestError(f"Reference {key} has an unsupported kind.")


def _is_hz(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _reference_content_hash(references: Any) -> str:
    canonical = json.dumps(references, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _parse_date(value: Any, field: str) -> date:
    if not isinstance(value, str):
        raise ReferenceManifestError(f"{field} must be an ISO date.")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ReferenceManifestError(f"{field} must be an ISO date.") from exc


def _coerce_date(value: date | str | None) -> date:
    if value is None:
        return date.today()
    if isinstance(value, date):
        return value
    return _parse_date(value, "as_of")
