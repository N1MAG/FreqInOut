"""Qt-free EiBi adapter for immutable Shortwave dataset candidates.

The adapter deliberately parses provider data into the neutral candidates owned
by :mod:`shortwave_models`.  It performs no database, network, GUI, or import
promotion work; the startup-owned Shortwave store owns those boundaries.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass, replace
import datetime as dt
from decimal import Decimal, InvalidOperation
import hashlib
from pathlib import Path
import re
import time
from typing import Callable, Mapping
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import HTTPRedirectHandler, Request, build_opener

from freqinout.core.shortwave_models import (
    ShortwaveDatasetCandidate,
    ShortwaveEntryCandidate,
    ShortwaveImportCancelled,
    ShortwaveParseDiagnostic,
)


EIBI_PROVIDER_KEY = "eibi"
EIBI_PROVIDER_LABEL = "EiBi / Eike Bierwirth"
EIBI_CATALOG_SOURCE_KEY = "source_eibi_shortwave"
EIBI_PROVIDER_LANDING_PAGE = "https://www.eibispace.de/"
EIBI_ENCODING = "iso-8859-1"
EIBI_PARSER_VERSION = "eibi-v1"
EIBI_MAX_SOURCE_BYTES = 2_000_000
EIBI_MAX_ROWS = 10_000
EIBI_ALLOWED_DOWNLOAD_HOSTS = frozenset({"eibispace.de", "www.eibispace.de"})
EIBI_DEFAULT_CSV_URL = "https://www.eibispace.de/dx/sked-a26.csv"
EIBI_DEFAULT_README_URL = "https://www.eibispace.de/dx/eibi_README.TXT"

_HEADER_PREFIXES = (
    "kHz:", "Time(UTC):", "Days:", "ITU:", "Station:", "Lng:",
    "Target:", "Remarks:", "P:", "Start:", "Stop:",
)
_TIME_RE = re.compile(r"^(\d{4})-(\d{4})$")
_DATE_RE = re.compile(r"^(\d{2})(\d{2})$")
_LAST_HEARD_RE = re.compile(r"^\[(.+)\]$")
_NAMED_DAY_RE = re.compile(r"Mo|Tu|We|Th|Fr|Sa|Su")
_DAY_INDEX = {"Mo": 0, "Tu": 1, "We": 2, "Th": 3, "Fr": 4, "Sa": 5, "Su": 6}
_MONTH_DAYS = {1: 31, 2: 29, 3: 31, 4: 30, 5: 31, 6: 30, 7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31}


@dataclass(frozen=True, slots=True)
class EiBiDictionaries:
    """Dataset-scoped provider dictionaries decoded from one EiBi README."""

    languages: Mapping[str, str]
    countries: Mapping[str, str]
    targets: Mapping[str, str]
    transmitter_sites: Mapping[str, str]

    def as_mapping(self) -> dict[str, Mapping[str, str]]:
        return {
            "languages": dict(self.languages),
            "countries": dict(self.countries),
            "targets": dict(self.targets),
            "transmitter_sites": dict(self.transmitter_sites),
        }


@dataclass(frozen=True, slots=True)
class EiBiDownloadConfig:
    """Reviewed official-download configuration; this is not UI input."""

    csv_url: str = EIBI_DEFAULT_CSV_URL
    readme_url: str = EIBI_DEFAULT_README_URL
    max_bytes: int = EIBI_MAX_SOURCE_BYTES
    max_redirects: int = 3
    timeout_seconds: float = 20.0

    def __post_init__(self) -> None:
        _validate_eibi_download_url(self.csv_url)
        _validate_eibi_download_url(self.readme_url)
        if int(self.max_bytes) < 1:
            raise ValueError("EiBi download byte limit must be positive")
        if not 0 <= int(self.max_redirects) <= 5:
            raise ValueError("EiBi download redirect limit must be between 0 and 5")
        if not 0 < float(self.timeout_seconds) <= 60:
            raise ValueError("EiBi download timeout must be between 0 and 60 seconds")


@dataclass(frozen=True, slots=True)
class EiBiDownloadedSource:
    """Validated provider bytes and response provenance for a later preview."""

    csv_bytes: bytes
    readme_bytes: bytes
    csv_url: str
    readme_url: str
    csv_content_type: str
    readme_content_type: str


class EiBiDownloadError(RuntimeError):
    """The reviewed official download could not be obtained or validated."""


class EiBiProviderAdapter:
    """Parse an EiBi CSV/README pair without taking ownership of persistence."""

    provider_key = EIBI_PROVIDER_KEY
    parser_version = EIBI_PARSER_VERSION

    def parse(
        self,
        csv_source: bytes | str | Path,
        readme_source: bytes | str | Path,
        *,
        season_code: str,
        season_effective_from_utc: str,
        season_effective_to_utc: str,
        publisher_updated_utc: str | None = None,
        source_uri: str = EIBI_PROVIDER_LANDING_PAGE,
        source_filename: str | None = None,
        cancellation_probe: Callable[[], bool] | None = None,
        max_source_bytes: int = EIBI_MAX_SOURCE_BYTES,
        max_rows: int = EIBI_MAX_ROWS,
    ) -> ShortwaveDatasetCandidate:
        return parse_eibi_dataset(
            csv_source,
            readme_source,
            season_code=season_code,
            season_effective_from_utc=season_effective_from_utc,
            season_effective_to_utc=season_effective_to_utc,
            publisher_updated_utc=publisher_updated_utc,
            source_uri=source_uri,
            source_filename=source_filename,
            cancellation_probe=cancellation_probe,
            max_source_bytes=max_source_bytes,
            max_rows=max_rows,
        )

    def download_official(
        self,
        config: EiBiDownloadConfig | None = None,
        *,
        opener: object | None = None,
        cancellation_probe: Callable[[], bool] | None = None,
    ) -> EiBiDownloadedSource:
        return download_official_eibi(
            config, opener=opener, cancellation_probe=cancellation_probe
        )


def parse_eibi_dataset(
    csv_source: bytes | str | Path,
    readme_source: bytes | str | Path,
    *,
    season_code: str,
    season_effective_from_utc: str,
    season_effective_to_utc: str,
    publisher_updated_utc: str | None = None,
    source_uri: str = EIBI_PROVIDER_LANDING_PAGE,
    source_filename: str | None = None,
    cancellation_probe: Callable[[], bool] | None = None,
    max_source_bytes: int = EIBI_MAX_SOURCE_BYTES,
    max_rows: int = EIBI_MAX_ROWS,
) -> ShortwaveDatasetCandidate:
    """Return a bounded, immutable candidate from a Latin-1 EiBi source pair.

    ``season_effective_*`` are deliberately required: date-bounded EiBi rows
    cannot be safely treated as current without provider-authoritative bounds.
    Cancellation raises ``ShortwaveImportCancelled`` before a caller can stage
    a partial candidate.
    """

    _check_cancelled(cancellation_probe)
    effective_from, effective_to = _validated_season_bounds(
        season_effective_from_utc, season_effective_to_utc
    )
    csv_bytes = _read_bounded_bytes(csv_source, max_source_bytes, "EiBi CSV")
    readme_bytes = _read_bounded_bytes(readme_source, max_source_bytes, "EiBi README")
    _check_cancelled(cancellation_probe)
    csv_text = csv_bytes.decode(EIBI_ENCODING)
    readme_text = readme_bytes.decode(EIBI_ENCODING)
    dictionaries = parse_eibi_readme_dictionaries(readme_text)
    diagnostics: list[ShortwaveParseDiagnostic] = []
    entries: list[ShortwaveEntryCandidate] = []
    seen_content_hashes: set[str] = set()
    raw_lines = csv_text.splitlines()
    if not raw_lines:
        raise ValueError("EiBi CSV is empty")
    header = _parse_csv_line(raw_lines[0], 1, diagnostics)
    if not _valid_header(header):
        diagnostics.append(_diagnostic(1, "error", "eibi.header", "EiBi CSV header does not match the expected 11 data columns.", raw_lines[0]))
    data_rows = 0
    for line_number, raw_line in enumerate(raw_lines[1:], start=2):
        if not raw_line.strip():
            continue
        data_rows += 1
        if data_rows > max_rows:
            raise ValueError(f"EiBi CSV exceeds {max_rows} row limit")
        if data_rows % 128 == 0:
            _check_cancelled(cancellation_probe)
        row = _parse_csv_line(raw_line, line_number, diagnostics)
        if row is None:
            continue
        if len(row) != 11:
            diagnostics.append(_diagnostic(line_number, "error", "eibi.column_count", "EiBi data row must contain exactly 11 fields.", raw_line))
            continue
        candidate = _parse_row(row, raw_line, line_number, dictionaries)
        if candidate is None:
            # Candidate construction stops only when exact frequency conversion
            # fails; retain the same diagnostic in the dataset-level audit.
            _frequency_hz(row[0], line_number, diagnostics)
            continue
        diagnostics.extend(candidate.diagnostics)
        if candidate.content_hash in seen_content_hashes:
            duplicate_diagnostic = _diagnostic(
                line_number,
                "warning",
                "eibi.exact_duplicate",
                "Exact duplicate provider row retained for audit and excluded from the normal projection.",
                raw_line,
            )
            candidate = replace(candidate, duplicate=True, diagnostics=(*candidate.diagnostics, duplicate_diagnostic))
            diagnostics.append(duplicate_diagnostic)
        else:
            seen_content_hashes.add(candidate.content_hash)
        entries.append(candidate)
    _check_cancelled(cancellation_probe)
    filename = source_filename or _source_filename(csv_source) or f"eibi_sked-{season_code.lower()}.csv"
    return ShortwaveDatasetCandidate(
        provider_key=EIBI_PROVIDER_KEY,
        provider_label=EIBI_PROVIDER_LABEL,
        catalog_source_key=EIBI_CATALOG_SOURCE_KEY,
        season_code=str(season_code).strip(),
        publisher_updated_utc=str(publisher_updated_utc).strip() or None,
        season_effective_from_utc=effective_from,
        season_effective_to_utc=effective_to,
        source_uri=str(source_uri).strip() or EIBI_PROVIDER_LANDING_PAGE,
        source_filename=filename,
        csv_sha256=hashlib.sha256(csv_bytes).hexdigest(),
        readme_sha256=hashlib.sha256(readme_bytes).hexdigest(),
        parser_version=EIBI_PARSER_VERSION,
        encoding=EIBI_ENCODING,
        entries=tuple(entries),
        diagnostics=tuple(diagnostics),
        dictionaries=dictionaries.as_mapping(),
        metadata={
            "provider_landing_page": EIBI_PROVIDER_LANDING_PAGE,
            "attribution": "EiBi schedule data by Eike Bierwirth.",
            "upstream_conditions": "EiBi README permits use, copying, and distribution; it does not identify a formal license.",
            "schedule_accuracy_caveat": "A listed schedule is not proof of reception or transmitter activity.",
            "decode_substitution_count": 0,
            "raw_data_rows": data_rows,
            "header_valid": _valid_header(header),
        },
    )


def parse_eibi_csv(*args: object, **kwargs: object) -> ShortwaveDatasetCandidate:
    """Compatibility spelling for callers that name the provider by its CSV."""

    return parse_eibi_dataset(*args, **kwargs)  # type: ignore[arg-type]


def download_official_eibi(
    config: EiBiDownloadConfig | None = None,
    *,
    opener: object | None = None,
    cancellation_probe: Callable[[], bool] | None = None,
) -> EiBiDownloadedSource:
    """Fetch the approved EiBi pair without credentials, cookies, or UI URLs.

    Redirect destinations are revalidated against the same HTTPS allow-list.
    This function only returns bounded bytes for the import preview; it never
    parses into a database or changes the current dataset.
    """

    approved = config or EiBiDownloadConfig()
    deadline = time.monotonic() + float(approved.timeout_seconds)
    _check_cancelled(cancellation_probe)
    csv_bytes, csv_url, csv_type = _download_eibi_asset(
        approved.csv_url, "csv", approved, opener, cancellation_probe, deadline
    )
    _check_cancelled(cancellation_probe)
    readme_bytes, readme_url, readme_type = _download_eibi_asset(
        approved.readme_url, "readme", approved, opener, cancellation_probe, deadline
    )
    return EiBiDownloadedSource(csv_bytes, readme_bytes, csv_url, readme_url, csv_type, readme_type)


def parse_eibi_readme_dictionaries(readme: bytes | str | Path) -> EiBiDictionaries:
    """Decode provider dictionaries from the README attached to this dataset."""

    text = _read_text(readme, "EiBi README")
    sections = _readme_sections(text)
    countries = _parse_code_dictionary(sections.get("countries", ()), separator=None)
    targets = _parse_code_dictionary(sections.get("targets", ()), separator="-")
    languages = _parse_code_dictionary(sections.get("languages", ()), separator=None)
    sites = _parse_transmitter_sites(sections.get("transmitter_sites", ()))
    return EiBiDictionaries(languages, countries, targets, sites)


def bundled_eibi_seed_paths() -> tuple[Path, Path]:
    """Return packaged EiBi seed files; callers decide when to import them."""

    root = Path(__file__).resolve().parents[2]
    return root / "config" / "shortwave" / "eibi" / "eibi_sked-a26.csv", root / "config" / "shortwave" / "eibi" / "eibi_README.TXT"


def _parse_row(
    row: list[str], raw_row: str, line_number: int, dictionaries: EiBiDictionaries
) -> ShortwaveEntryCandidate | None:
    row_diagnostics: list[ShortwaveParseDiagnostic] = []
    frequency_hz = _frequency_hz(row[0], line_number, row_diagnostics)
    if frequency_hz is None:
        return None
    start_minute, end_minute, crosses_midnight, time_state = _parse_time(row[1], line_number, row_diagnostics)
    weekday_mask, recurrence, day_state, special_flags = _parse_days(row[2], line_number, row_diagnostics)
    start_normalized, start_state = _parse_ddmm(row[9], line_number, "start", row_diagnostics)
    stop_normalized, stop_state, last_heard = _parse_stop(row[10], line_number, row_diagnostics)
    parse_state = "complete"
    if "review" in {time_state, start_state, stop_state}:
        parse_state = "review"
    elif "special" in {day_state, start_state, stop_state}:
        parse_state = "special"
    persistence_raw = row[8].strip()
    utility, inactive, classification = _classification(persistence_raw, row[5])
    language_labels, signal_type = _decode_languages(row[5], dictionaries.languages)
    target_labels = _decode_targets(row[6], dictionaries)
    transmitter_labels = _decode_transmitter(row[7], row[3], dictionaries)
    content_hash = hashlib.sha256(raw_row.encode(EIBI_ENCODING)).hexdigest()
    # EiBi may publish the same station/frequency/time/day more than once for
    # distinct languages, targets, transmitter sites, or persistence classes.
    # Those are separate operator-visible listings and must remain separately
    # saveable.  Normalized provider date bounds distinguish repeated seasonal
    # windows; the bracketed last-heard marker remains outside the identity so
    # that observation-only changes are still reviewable on the saved listing.
    identity_start = start_normalized or row[9].strip()
    identity_stop = stop_normalized or row[10].split("[", 1)[0].strip()
    provider_identity_hash = hashlib.sha256(
        (
            EIBI_PROVIDER_KEY + "\x1f"
            + "\x1f".join(row[index] for index in (3, 4, 0, 1, 2, 5, 6, 7, 8))
            + "\x1f" + identity_start + "\x1f" + identity_stop
        ).encode(EIBI_ENCODING)
    ).hexdigest()
    return ShortwaveEntryCandidate(
        provider_identity_hash=provider_identity_hash,
        source_line_number=line_number,
        frequency_hz=frequency_hz,
        start_minute_utc=start_minute,
        end_minute_utc=end_minute,
        crosses_midnight=crosses_midnight,
        raw_days=row[2],
        weekday_mask=weekday_mask,
        recurrence=recurrence,
        parse_state=parse_state,
        special_flags=tuple(special_flags),
        station_name=row[4].strip(),
        station_home_code=row[3].strip(),
        language_raw=row[5],
        language_labels=language_labels,
        signal_type=signal_type,
        target_raw=row[6],
        target_labels=target_labels,
        transmitter_raw=row[7],
        transmitter_labels=transmitter_labels,
        persistence_raw=persistence_raw,
        inactive=inactive,
        utility=utility,
        duplicate=False,
        classification=classification,
        start_date_raw=row[9],
        stop_date_raw=row[10],
        start_date_normalized=start_normalized,
        stop_date_normalized=stop_normalized,
        last_heard_raw=last_heard,
        raw_source_row=raw_row,
        content_hash=content_hash,
        validation_state="valid" if parse_state == "complete" else parse_state,
        diagnostics=tuple(row_diagnostics),
    )


def _frequency_hz(value: str, line: int, diagnostics: list[ShortwaveParseDiagnostic]) -> int | None:
    try:
        hertz = Decimal(value.strip()) * Decimal(1000)
        if hertz != hertz.to_integral_value() or hertz <= 0:
            raise InvalidOperation
        return int(hertz)
    except (InvalidOperation, ValueError):
        diagnostics.append(_diagnostic(line, "error", "eibi.frequency", "Frequency must be a positive exact decimal kHz value.", value))
        return None


def _parse_time(value: str, line: int, diagnostics: list[ShortwaveParseDiagnostic]) -> tuple[int | None, int | None, bool, str]:
    match = _TIME_RE.fullmatch(value.strip())
    if match is None:
        diagnostics.append(_diagnostic(line, "warning", "eibi.time", "Time is not an HHMM-HHMM UTC window.", value))
        return None, None, False, "review"
    start, start_valid = _minute(match.group(1), allow_2400=False)
    end, end_valid = _minute(match.group(2), allow_2400=True)
    if not start_valid or not end_valid or start is None or end is None or start == end:
        diagnostics.append(_diagnostic(line, "warning", "eibi.time", "Time window is malformed or has equal start and end values.", value))
        return None, None, False, "review"
    return start, end, end < start, "complete"


def _minute(value: str, *, allow_2400: bool) -> tuple[int | None, bool]:
    number = int(value)
    if number == 2400:
        return (1440, True) if allow_2400 else (None, False)
    hours, minutes = divmod(number, 100)
    if not 0 <= hours <= 23 or not 0 <= minutes <= 59:
        return None, False
    return hours * 60 + minutes, True


def _parse_days(value: str, line: int, diagnostics: list[ShortwaveParseDiagnostic]) -> tuple[int | None, dict[str, object], str, tuple[str, ...]]:
    raw = value.strip()
    if not raw:
        return 0b1111111, {"kind": "daily"}, "complete", ()
    if raw.isdigit() and set(raw) <= set("1234567"):
        mask = 0
        for digit in raw:
            mask |= 1 << (int(digit) - 1)
        return mask, {"kind": "weekday_mask", "days": raw}, "complete", ()
    mask = _named_day_mask(raw)
    if mask is not None:
        return mask, {"kind": "weekdays", "expression": raw}, "complete", ()
    flag = "unsupported_day_rule"
    diagnostics.append(_diagnostic(line, "warning", "eibi.days", "Day rule requires review and is not confidently evaluated.", raw))
    return None, {"kind": "special", "expression": raw}, "special", (flag,)


def _named_day_mask(value: str) -> int | None:
    mask = 0
    for part in value.split(","):
        token = part.strip()
        if not token:
            return None
        if "-" in token:
            pieces = token.split("-")
            if len(pieces) != 2 or pieces[0] not in _DAY_INDEX or pieces[1] not in _DAY_INDEX:
                return None
            start, end = _DAY_INDEX[pieces[0]], _DAY_INDEX[pieces[1]]
            index = start
            while True:
                mask |= 1 << index
                if index == end:
                    break
                index = (index + 1) % 7
            continue
        names = _NAMED_DAY_RE.findall(token)
        if not names or "".join(names) != token:
            return None
        for name in names:
            mask |= 1 << _DAY_INDEX[name]
    return mask or None


def _parse_ddmm(value: str, line: int, field: str, diagnostics: list[ShortwaveParseDiagnostic]) -> tuple[str | None, str]:
    raw = value.strip()
    if not raw:
        return None, "complete"
    match = _DATE_RE.fullmatch(raw)
    if match is None:
        diagnostics.append(_diagnostic(line, "warning", f"eibi.{field}_date", f"{field.title()} date is not DDMM.", value))
        return None, "review"
    day, month = int(match.group(1)), int(match.group(2))
    if month not in _MONTH_DAYS or not 1 <= day <= _MONTH_DAYS[month]:
        diagnostics.append(_diagnostic(line, "warning", f"eibi.{field}_date", f"{field.title()} date is not a valid DDMM value.", value))
        return None, "review"
    return f"--{month:02d}-{day:02d}", "complete"


def _parse_stop(value: str, line: int, diagnostics: list[ShortwaveParseDiagnostic]) -> tuple[str | None, str, str | None]:
    raw = value.strip()
    if _LAST_HEARD_RE.fullmatch(raw):
        return None, "complete", raw
    normalized, state = _parse_ddmm(value, line, "stop", diagnostics)
    return normalized, state, None


def _classification(persistence_raw: str, language_raw: str) -> tuple[bool, bool, str]:
    try:
        value = int(persistence_raw)
    except ValueError:
        value = -1
    signal_type = language_raw.strip().upper()
    # EiBi normally marks utilities with persistence 90+; the audited A26
    # snapshot also identifies utility signal rows through its ``-CW``/``-HF``
    # style language field. Preserve both provider signals rather than hiding
    # the latter as ordinary broadcasts.
    utility = value >= 90 or signal_type.startswith("-")
    base = value - 90 if value >= 90 else value
    inactive = base == 8
    if signal_type == "-TS":
        return utility, inactive, "time_standard"
    return utility, inactive, "utility" if utility else "broadcast"


def _decode_languages(value: str, dictionary: Mapping[str, str]) -> tuple[tuple[str, ...], str | None]:
    raw_codes = tuple(code.strip() for code in value.split(",") if code.strip())
    signal_types = tuple(code for code in raw_codes if code.startswith("-"))
    labels = tuple(dictionary.get(code, code) for code in raw_codes if not code.startswith("-"))
    return labels, ", ".join(signal_types) or None


def _decode_targets(value: str, dictionaries: EiBiDictionaries) -> tuple[str, ...]:
    code = value.strip()
    if not code:
        return ()
    return (_dictionary_label(dictionaries.targets, code) or dictionaries.countries.get(code, code),)


def _decode_transmitter(value: str, home_code: str, dictionaries: EiBiDictionaries) -> tuple[str, ...]:
    raw = value.strip()
    if not raw:
        return ()
    host, separator, site = raw[1:].partition("-") if raw.startswith("/") else (home_code.strip(), "", raw)
    key = f"{host}:{site}" if site else host
    resolved = dictionaries.transmitter_sites.get(key)
    if resolved:
        return (resolved,)
    country = dictionaries.countries.get(host, host)
    return (f"{country} — {site}" if site else country,)


def _readme_sections(text: str) -> dict[str, tuple[str, ...]]:
    headings = {
        "I) Language codes.": "languages",
        "II) Country codes.": "countries",
        "III) Target-area codes.": "targets",
        "IV) Transmitter site codes.": "transmitter_sites",
    }
    sections: dict[str, list[str]] = {value: [] for value in headings.values()}
    current: str | None = None
    for line in text.splitlines():
        heading = headings.get(line.strip())
        if heading is not None:
            current = heading
            continue
        if current is not None:
            sections[current].append(line)
    return {name: tuple(lines) for name, lines in sections.items()}


def _parse_code_dictionary(lines: tuple[str, ...], *, separator: str | None) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith(("Numbers are", "On the right", "For more", "Countries are", "Asterisks", "Alternatively,")):
            continue
        if separator is not None:
            match = re.match(r"^([A-Za-z0-9.]+)\s*-\s+(.+)$", stripped)
        else:
            match = re.match(r"^([A-Za-z0-9-]{1,5})\s{2,}(.+)$", stripped)
        if match is None:
            continue
        code, label = match.group(1), match.group(2)
        label = re.sub(r"\s+\[[^\]]+\]\s*$", "", label).strip()
        if code and label:
            values[code] = label
    return values


def _parse_transmitter_sites(lines: tuple[str, ...]) -> dict[str, str]:
    values: dict[str, str] = {}
    current_country = ""
    for line in lines:
        stripped = line.strip()
        country = re.match(r"^([A-Z0-9]{1,3}):\s*(.*)$", stripped)
        if country is not None:
            current_country = country.group(1)
            if country.group(2):
                values[current_country] = country.group(2)
            continue
        site = re.match(r"^([A-Za-z0-9]+)-(.+)$", stripped)
        if site is not None and current_country:
            values[f"{current_country}:{site.group(1)}"] = site.group(2).strip()
    return values


def _parse_csv_line(raw: str, line: int, diagnostics: list[ShortwaveParseDiagnostic]) -> list[str] | None:
    try:
        return next(csv.reader([raw], delimiter=";", strict=True))
    except csv.Error as exc:
        diagnostics.append(_diagnostic(line, "error", "eibi.csv", f"Malformed semicolon-delimited EiBi row: {exc}", raw))
        return None


def _valid_header(row: list[str] | None) -> bool:
    return bool(row and len(row) == 12 and row[-1] == "" and all(value.startswith(prefix) for value, prefix in zip(row[:-1], _HEADER_PREFIXES)))


class _EiBiRedirectHandler(HTTPRedirectHandler):
    def __init__(self, max_redirects: int) -> None:
        super().__init__()
        self._max_redirects = max_redirects
        self._redirects = 0

    def redirect_request(self, req: Request, fp: object, code: int, msg: str, headers: object, newurl: str):  # type: ignore[override]
        self._redirects += 1
        if self._redirects > self._max_redirects:
            raise EiBiDownloadError("EiBi download exceeded the redirect limit")
        _validate_eibi_download_url(newurl)
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def _download_eibi_asset(
    url: str,
    asset_kind: str,
    config: EiBiDownloadConfig,
    opener: object | None,
    cancellation_probe: Callable[[], bool] | None,
    deadline: float,
) -> tuple[bytes, str, str]:
    _validate_eibi_download_url(url)
    _check_cancelled(cancellation_probe)
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise EiBiDownloadError("EiBi download exceeded the overall timeout")
    request = Request(
        url,
        headers={"User-Agent": "FreqInOut EiBi source updater/1", "Accept": "text/csv, text/plain, application/octet-stream"},
        method="GET",
    )
    try:
        if opener is None:
            response = build_opener(_EiBiRedirectHandler(int(config.max_redirects))).open(
                request, timeout=remaining
            )
        elif hasattr(opener, "open"):
            response = opener.open(request, timeout=remaining)  # type: ignore[union-attr]
        elif callable(opener):
            response = opener(request, timeout=remaining)
        else:
            raise TypeError("EiBi opener must be callable or expose open()")
        with response:
            _check_cancelled(cancellation_probe)
            if time.monotonic() >= deadline:
                raise EiBiDownloadError("EiBi download exceeded the overall timeout")
            status = int(getattr(response, "status", None) or response.getcode())
            final_url = str(response.geturl())
            _validate_eibi_download_url(final_url)
            if status != 200:
                raise EiBiDownloadError(f"EiBi {asset_kind} download returned HTTP {status}")
            content_type = _response_content_type(response)
            if content_type not in {"text/csv", "text/plain", "application/octet-stream"}:
                raise EiBiDownloadError(f"EiBi {asset_kind} response has unsupported content type {content_type or 'unknown'}")
            content_length = _response_content_length(response)
            if content_length is not None and content_length > int(config.max_bytes):
                raise EiBiDownloadError(f"EiBi {asset_kind} response exceeds {config.max_bytes} byte limit")
            payload = _read_response_bounded(
                response,
                int(config.max_bytes),
                asset_kind,
                cancellation_probe=cancellation_probe,
                deadline=deadline,
            )
    except EiBiDownloadError:
        raise
    except (HTTPError, URLError, OSError, ValueError) as exc:
        raise EiBiDownloadError(f"EiBi {asset_kind} download failed: {exc}") from exc
    _validate_download_shape(payload, asset_kind)
    return payload, final_url, content_type


def _validate_eibi_download_url(value: str) -> None:
    parsed = urlparse(str(value or ""))
    if parsed.scheme.lower() != "https" or parsed.username or parsed.password or parsed.port:
        raise ValueError("EiBi download URL must use plain HTTPS")
    if (parsed.hostname or "").lower() not in EIBI_ALLOWED_DOWNLOAD_HOSTS:
        raise ValueError("EiBi download URL host is not approved")
    if not parsed.path or ".." in Path(parsed.path).parts:
        raise ValueError("EiBi download URL path is invalid")


def _response_content_type(response: object) -> str:
    headers = getattr(response, "headers", None)
    if headers is None:
        return ""
    raw_value = headers.get("Content-Type") if hasattr(headers, "get") else None
    if not raw_value:
        return ""
    getter = getattr(headers, "get_content_type", None)
    if callable(getter):
        return str(getter() or "").lower()
    return str(raw_value).split(";", 1)[0].strip().lower()


def _response_content_length(response: object) -> int | None:
    headers = getattr(response, "headers", None)
    value = headers.get("Content-Length") if headers is not None and hasattr(headers, "get") else None
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        raise EiBiDownloadError("EiBi response Content-Length is invalid")


def _read_response_bounded(
    response: object,
    max_bytes: int,
    asset_kind: str,
    *,
    cancellation_probe: Callable[[], bool] | None,
    deadline: float,
) -> bytes:
    chunks: list[bytes] = []
    total = 0
    while True:
        _check_cancelled(cancellation_probe)
        if time.monotonic() >= deadline:
            raise EiBiDownloadError("EiBi download exceeded the overall timeout")
        chunk = response.read(min(64 * 1024, max_bytes + 1))
        if not chunk:
            break
        if not isinstance(chunk, bytes):
            raise EiBiDownloadError(f"EiBi {asset_kind} response is not bytes")
        total += len(chunk)
        if total > max_bytes:
            raise EiBiDownloadError(f"EiBi {asset_kind} response exceeds {max_bytes} byte limit")
        chunks.append(chunk)
    return b"".join(chunks)


def _validate_download_shape(payload: bytes, asset_kind: str) -> None:
    try:
        text = payload.decode(EIBI_ENCODING)
    except UnicodeDecodeError as exc:  # Latin-1 accepts all bytes, kept for future encoding changes.
        raise EiBiDownloadError(f"EiBi {asset_kind} decode failed: {exc}") from exc
    if asset_kind == "csv":
        lines = text.splitlines()
        if not lines or not _valid_header(_parse_csv_line(lines[0], 1, [])):
            raise EiBiDownloadError("EiBi CSV response does not have the expected signature/header")
        if len(lines) - 1 > EIBI_MAX_ROWS:
            raise EiBiDownloadError("EiBi CSV response exceeds the row limit")
        for line_number, line in enumerate(lines[1:], start=2):
            if not line:
                continue
            row = _parse_csv_line(line, line_number, [])
            if row is None or len(row) != 11:
                raise EiBiDownloadError(f"EiBi CSV response has invalid shape at line {line_number}")
        return
    if "D) Codes used." not in text or "Format of the CSV database" not in text:
        raise EiBiDownloadError("EiBi README response does not have the expected signature")


def _read_bounded_bytes(source: bytes | str | Path, max_bytes: int, label: str) -> bytes:
    if isinstance(source, bytes):
        value = source
    elif isinstance(source, str) and ("\n" in source or "\r" in source):
        value = source.encode(EIBI_ENCODING)
    else:
        value = Path(source).read_bytes()
    if len(value) > max(1, int(max_bytes)):
        raise ValueError(f"{label} exceeds {max(1, int(max_bytes))} byte limit")
    return value


def _read_text(source: bytes | str | Path, label: str) -> str:
    if isinstance(source, bytes):
        return source.decode(EIBI_ENCODING)
    if isinstance(source, str) and ("\n" in source or "\r" in source):
        return source
    path = Path(source)
    try:
        exists = path.exists()
    except OSError:
        exists = False
    if exists:
        return path.read_bytes().decode(EIBI_ENCODING)
    return str(source)


def _source_filename(source: bytes | str | Path) -> str | None:
    if isinstance(source, bytes):
        return None
    if isinstance(source, str) and ("\n" in source or "\r" in source):
        return None
    value = Path(source)
    return value.name if value.name else None


def _dictionary_label(dictionary: Mapping[str, str], code: str) -> str | None:
    if exact := dictionary.get(code):
        return exact
    for pattern, label in dictionary.items():
        if len(pattern) != len(code) or "." not in pattern:
            continue
        if all(expected == "." or expected.casefold() == actual.casefold() for expected, actual in zip(pattern, code)):
            suffix = "".join(actual for expected, actual in zip(pattern, code) if expected == ".")
            return label.replace("..", suffix).replace(".", suffix)
    return None


def _required_bound(value: str | None, name: str) -> str:
    result = str(value or "").strip()
    if not result:
        raise ValueError(f"{name} is required")
    return result


def _validated_season_bounds(from_value: str | None, to_value: str | None) -> tuple[str, str]:
    """Validate authoritative UTC season bounds, including Bxx cross-years."""

    raw_from = _required_bound(from_value, "season_effective_from_utc")
    raw_to = _required_bound(to_value, "season_effective_to_utc")
    try:
        start = dt.datetime.fromisoformat(raw_from.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("season_effective_from_utc must be an ISO-8601 UTC timestamp") from exc
    try:
        end = dt.datetime.fromisoformat(raw_to.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("season_effective_to_utc must be an ISO-8601 UTC timestamp") from exc
    if start.tzinfo is None or start.utcoffset() != dt.timedelta(0):
        raise ValueError("season_effective_from_utc must be UTC")
    if end.tzinfo is None or end.utcoffset() != dt.timedelta(0):
        raise ValueError("season_effective_to_utc must be UTC")
    if end <= start:
        raise ValueError("season_effective_to_utc must be later than season_effective_from_utc")
    return raw_from, raw_to


def _check_cancelled(probe: Callable[[], bool] | None) -> None:
    if probe is not None and probe():
        raise ShortwaveImportCancelled("EiBi parsing cancelled")


def _diagnostic(line: int | None, severity: str, code: str, message: str, raw: str = "") -> ShortwaveParseDiagnostic:
    return ShortwaveParseDiagnostic(line, severity, code, message, raw)


__all__ = [
    "EIBI_CATALOG_SOURCE_KEY",
    "EIBI_ALLOWED_DOWNLOAD_HOSTS",
    "EIBI_DEFAULT_CSV_URL",
    "EIBI_DEFAULT_README_URL",
    "EIBI_ENCODING",
    "EIBI_MAX_ROWS",
    "EIBI_MAX_SOURCE_BYTES",
    "EIBI_PARSER_VERSION",
    "EIBI_PROVIDER_KEY",
    "EIBI_PROVIDER_LABEL",
    "EiBiDictionaries",
    "EiBiDownloadConfig",
    "EiBiDownloadedSource",
    "EiBiDownloadError",
    "EiBiProviderAdapter",
    "bundled_eibi_seed_paths",
    "download_official_eibi",
    "parse_eibi_csv",
    "parse_eibi_dataset",
    "parse_eibi_readme_dictionaries",
]
