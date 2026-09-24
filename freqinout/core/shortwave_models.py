"""Qt-free immutable models for versioned shortwave schedule resources."""

from __future__ import annotations

from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Mapping


@dataclass(frozen=True, slots=True)
class ShortwaveParseDiagnostic:
    line_number: int | None
    severity: str
    code: str
    message: str
    raw_value: str = ""

    def __post_init__(self) -> None:
        if self.severity not in {"info", "warning", "error"}:
            raise ValueError("invalid shortwave diagnostic severity")


@dataclass(frozen=True, slots=True)
class ShortwaveEntryCandidate:
    provider_identity_hash: str
    source_line_number: int
    frequency_hz: int
    start_minute_utc: int | None
    end_minute_utc: int | None
    crosses_midnight: bool
    raw_days: str
    weekday_mask: int | None
    recurrence: Mapping[str, Any]
    parse_state: str
    special_flags: tuple[str, ...]
    station_name: str
    station_home_code: str
    language_raw: str
    language_labels: tuple[str, ...]
    signal_type: str | None
    target_raw: str
    target_labels: tuple[str, ...]
    transmitter_raw: str
    transmitter_labels: tuple[str, ...]
    persistence_raw: str
    inactive: bool
    utility: bool
    duplicate: bool
    classification: str
    start_date_raw: str
    stop_date_raw: str
    start_date_normalized: str | None
    stop_date_normalized: str | None
    last_heard_raw: str | None
    raw_source_row: str
    content_hash: str
    validation_state: str
    diagnostics: tuple[ShortwaveParseDiagnostic, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "recurrence", MappingProxyType(dict(self.recurrence)))
        object.__setattr__(self, "special_flags", tuple(self.special_flags))
        object.__setattr__(self, "language_labels", tuple(self.language_labels))
        object.__setattr__(self, "target_labels", tuple(self.target_labels))
        object.__setattr__(self, "transmitter_labels", tuple(self.transmitter_labels))
        object.__setattr__(self, "diagnostics", tuple(self.diagnostics))
        if self.frequency_hz <= 0:
            raise ValueError("frequency_hz must be positive")
        if self.parse_state not in {"complete", "special", "review"}:
            raise ValueError("invalid shortwave parse_state")


@dataclass(frozen=True, slots=True)
class ShortwaveDatasetCandidate:
    provider_key: str
    provider_label: str
    catalog_source_key: str
    season_code: str
    publisher_updated_utc: str | None
    season_effective_from_utc: str
    season_effective_to_utc: str
    source_uri: str
    source_filename: str
    csv_sha256: str
    readme_sha256: str
    parser_version: str
    encoding: str
    entries: tuple[ShortwaveEntryCandidate, ...]
    diagnostics: tuple[ShortwaveParseDiagnostic, ...]
    dictionaries: Mapping[str, Mapping[str, str]] = field(default_factory=dict)
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "entries", tuple(self.entries))
        object.__setattr__(self, "diagnostics", tuple(self.diagnostics))
        frozen_dictionaries = {
            str(kind): MappingProxyType({str(key): str(value) for key, value in values.items()})
            for kind, values in self.dictionaries.items()
        }
        object.__setattr__(self, "dictionaries", MappingProxyType(frozen_dictionaries))
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))
        if not self.provider_key or not self.catalog_source_key or not self.season_code:
            raise ValueError("provider, source, and season are required")
        if not self.season_effective_from_utc or not self.season_effective_to_utc:
            raise ValueError("authoritative season bounds are required")


@dataclass(frozen=True, slots=True)
class ShortwaveDataset:
    dataset_key: str
    catalog_source_key: str
    provider_key: str
    provider_label: str
    season_code: str
    publisher_updated_utc: str | None
    season_effective_from_utc: str
    season_effective_to_utc: str
    source_uri: str
    source_filename: str
    csv_sha256: str
    readme_sha256: str
    parser_version: str
    encoding: str
    record_count: int
    diagnostic_counts: Mapping[str, int]
    imported_utc: str
    state: str
    metadata: Mapping[str, Any]

    def __post_init__(self) -> None:
        object.__setattr__(self, "diagnostic_counts", MappingProxyType(dict(self.diagnostic_counts)))
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))


@dataclass(frozen=True, slots=True)
class ShortwaveEntry:
    entry_key: str
    dataset_key: str
    candidate: ShortwaveEntryCandidate


@dataclass(frozen=True, slots=True)
class ShortwaveImportPreview:
    candidate: ShortwaveDatasetCandidate
    expected_current_dataset_key: str | None
    new_count: int
    changed_count: int
    unchanged_count: int
    removed_count: int
    duplicate_count: int
    invalid_count: int
    special_count: int
    inactive_count: int
    fatal_errors: tuple[str, ...] = ()
    reminder_changed_count: int = 0
    reminder_missing_count: int = 0

    @property
    def actionable(self) -> bool:
        return not self.fatal_errors and bool(self.candidate.entries)


@dataclass(frozen=True, slots=True)
class ShortwaveImportResult:
    status: str
    dataset_key: str | None
    previous_dataset_key: str | None
    inserted_entries: int


class StaleShortwavePreviewError(RuntimeError):
    """The provider's current dataset changed after preview."""


class ShortwaveImportCancelled(RuntimeError):
    """A cooperative cancellation request aborted staging before promotion."""


__all__ = [
    "ShortwaveDataset",
    "ShortwaveDatasetCandidate",
    "ShortwaveEntry",
    "ShortwaveEntryCandidate",
    "ShortwaveImportCancelled",
    "ShortwaveImportPreview",
    "ShortwaveImportResult",
    "ShortwaveParseDiagnostic",
    "StaleShortwavePreviewError",
]
