from __future__ import annotations

import hashlib
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence


MESSAGE_FILENAMES = ("ALL.TXT", "DIRECTED.TXT", "inbox.db3")
RIG_SCOPED_VARIANTS = frozenset({
    "js8call_2_2",
    "js8call_improved_3_0_3",
    # Subspace 4.1 is intentionally handled like the reviewed 2.2.0 and
    # Improved 3.0.3 families: an independently launched rig uses its own
    # --rig-name namespace and therefore its own application-data root.
    "js8call_subspace_4_1",
})
SHARED_VARIANTS = frozenset()
STORAGE_MODES = frozenset({"rig_scoped", "shared", "unverified"})


@dataclass(frozen=True)
class JS8StorageResolution:
    variant_family: str
    variant_version: str
    rig_name: str
    rig_name_source: str
    application_name: str
    data_root: str
    all_path: str
    directed_path: str
    inbox_path: str
    save_dir: str
    storage_mode: str
    expected_mode: str
    verified: bool
    evidence: str

    @property
    def display_state(self) -> str:
        if self.storage_mode == "rig_scoped" and self.verified:
            return f"Isolated · {self.rig_name or 'default'}"
        if self.storage_mode == "shared":
            return "Shared"
        return "Needs verification"


@dataclass(frozen=True)
class JS8StorageCollision:
    canonical_root: str
    labels: tuple[str, ...]
    instance_ids: tuple[str, ...]


def normalize_variant_family(value: object, version: object = "") -> str:
    family = str(value or "").strip().casefold().replace("-", "_").replace(" ", "_")
    version_text = str(version or "").strip().casefold()
    if "subspace" in family:
        return "js8call_subspace_4_1" if "4.1" in version_text or not version_text else "unknown"
    if "improved" in family:
        return "js8call_improved_3_0_3" if "3.0.3" in version_text or not version_text else "unknown"
    if family in {"js8call_2_2", "stock", "js8call", "legacy"}:
        return "js8call_2_2" if "2.2" in version_text or not version_text else "unknown"
    return "unknown"


def variant_family_from_version(value: object) -> str:
    """Classify only the reviewed version families reported by the native API."""

    text = str(value or "").strip().casefold()
    if not text:
        return "unknown"
    if "subspace" in text or re.search(r"(?:^|\D)4\.1\.0\.478(?:\D|$)", text):
        return "js8call_subspace_4_1"
    if "improved" in text or re.search(r"(?:^|\D)3\.0\.3(?:\D|$)", text):
        return "js8call_improved_3_0_3"
    if re.search(r"(?:^|\D)2\.2(?:\.0)?(?:\D|$)", text):
        return "js8call_2_2"
    return "unknown"


def expected_storage_mode(variant_family: object, version: object = "") -> str:
    normalized = normalize_variant_family(variant_family, version)
    if normalized in RIG_SCOPED_VARIANTS:
        return "rig_scoped"
    if normalized in SHARED_VARIANTS:
        return "shared"
    return "unverified"


def normalize_rig_name(value: object) -> str:
    text = re.sub(r"\s+", " ", str(value or "").strip())
    if any(character in text for character in ("/", "\\", ",")):
        raise ValueError("JS8Call rig names cannot contain '/', '\\', or ','.")
    return text


def rig_name_collision_key(value: object) -> str:
    return normalize_rig_name(value).casefold()


def stable_managed_rig_name(*, system_key: object, name: object = "") -> str:
    identity = str(system_key or "").strip() or str(name or "").strip()
    if not identity:
        raise ValueError("A persisted JS8Call system key or name is required to generate a rig name.")
    slug = re.sub(r"[^A-Za-z0-9_.-]+", "-", identity).strip("-._").lower() or "radio"
    digest = hashlib.sha1(identity.encode("utf-8", errors="ignore")).hexdigest()[:8]
    return f"fio-{slug[:28]}-{digest}"


def native_managed_rig_name(name: object) -> str:
    """Return an operator-readable JS8Call ``--rig-name`` stem.

    Draft/system keys are deliberately not accepted here.  JS8Call makes the
    rig name part of its native application name, settings filename, writable
    data directory, lock name, and shared-memory identity.  Feeding an opaque
    Add Radio transaction key into this value therefore leaks temporary state
    into every durable application artifact.
    """

    source = re.sub(r"\s+", " ", str(name or "").strip())
    candidate = re.sub(r"[^A-Za-z0-9_.-]+", "-", source).strip("-._")[:48]
    return normalize_rig_name(candidate or "Radio")


def js8_application_name(rig_name: object = "") -> str:
    normalized = normalize_rig_name(rig_name)
    return f"JS8Call - {normalized}" if normalized else "JS8Call"


def rig_name_from_settings_path(value: object) -> str:
    name = Path(str(value or "")).name
    stem = name.rsplit(".", 1)[0]
    lowered = stem.casefold()
    if lowered.startswith("js8call - "):
        return normalize_rig_name(stem[len("JS8Call - ") :])
    # Subspace uses this settings-name convention.  It participates in the
    # same rig-name namespace as the other reviewed multi-instance families.
    if lowered.startswith("js8call-") and not lowered.startswith("js8call-improved"):
        return normalize_rig_name(stem[len("JS8Call-") :])
    return ""


def qt_data_root_candidates(
    *,
    application_name: str,
    platform: object | None = None,
    home: Path | None = None,
    env: Mapping[str, str] | None = None,
) -> tuple[Path, ...]:
    system = str(platform or os.sys.platform or "").strip().casefold()
    user_home = Path(home) if home is not None else Path.home()
    environment = dict(os.environ if env is None else env)
    candidates: list[Path] = []
    if system in {"darwin", "mac", "macos", "osx"}:
        candidates.append(user_home / "Library" / "Application Support" / application_name)
    elif system in {"windows", "win", "win32", "cygwin"}:
        local = environment.get("LOCALAPPDATA", "").strip()
        roaming = environment.get("APPDATA", "").strip()
        if local:
            candidates.append(Path(local) / application_name)
        if roaming:
            candidates.append(Path(roaming) / application_name)
        candidates.append(user_home / "AppData" / "Local" / application_name)
    else:
        xdg_data_home = environment.get("XDG_DATA_HOME", "").strip()
        candidates.append((Path(xdg_data_home) if xdg_data_home else user_home / ".local" / "share") / application_name)
        candidates.append(user_home / ".var" / "app" / "org.js8call.JS8Call" / "data" / application_name)
        candidates.append(user_home / "snap" / "js8call" / "common" / ".local" / "share" / application_name)
    return _unique_paths(candidates)


def qt_config_path_candidates(
    *,
    application_name: str,
    platform: object | None = None,
    home: Path | None = None,
    env: Mapping[str, str] | None = None,
) -> tuple[Path, ...]:
    """Return the native JS8Call MultiSettings paths created by Qt.

    JS8Call's ``MultiSettings::settings_path`` writes
    ``<ConfigLocation>/<applicationName>.ini``.  ``--rig-name`` changes the
    application name before that path is resolved, so settings and writable
    data must use the same reviewed rig identity.
    """

    system = str(platform or os.sys.platform or "").strip().casefold()
    user_home = Path(home) if home is not None else Path.home()
    environment = dict(os.environ if env is None else env)
    filename = f"{application_name}.ini"
    candidates: list[Path] = []
    if system in {"darwin", "mac", "macos", "osx"}:
        candidates.append(user_home / "Library" / "Preferences" / filename)
    elif system in {"windows", "win", "win32", "cygwin"}:
        # QStandardPaths::ConfigLocation is application-specific on Windows.
        # JS8Call then appends <applicationName>.ini inside that directory.
        local = environment.get("LOCALAPPDATA", "").strip()
        if local:
            candidates.append(Path(local) / application_name / filename)
        candidates.append(
            user_home / "AppData" / "Local" / application_name / filename
        )
        # Retain the older flat paths as discovery-only compatibility
        # candidates; new managed writers always use the first native path.
        if local:
            candidates.append(Path(local) / filename)
        candidates.append(user_home / "AppData" / "Local" / filename)
    else:
        xdg_config_home = environment.get("XDG_CONFIG_HOME", "").strip()
        candidates.append(
            (Path(xdg_config_home) if xdg_config_home else user_home / ".config")
            / filename
        )
        candidates.append(
            user_home
            / ".var"
            / "app"
            / "org.js8call.JS8Call"
            / "config"
            / filename
        )
        candidates.append(
            user_home / "snap" / "js8call" / "common" / ".config" / filename
        )
    return _unique_paths(candidates)


def resolve_js8_storage(
    values: Mapping[str, object],
    *,
    platform: object | None = None,
    home: Path | None = None,
    env: Mapping[str, str] | None = None,
    probe_existing: bool = True,
) -> JS8StorageResolution:
    variant_version = str(values.get("variant_version", values.get("js8_variant_version", "")) or "").strip()
    variant_family = normalize_variant_family(
        values.get("variant_family", values.get("js8_variant_family", "unknown")),
        variant_version,
    )
    rig_name = normalize_rig_name(values.get("rig_name", values.get("js8_rig_name", "")))
    rig_source = str(values.get("rig_name_source", values.get("js8_rig_name_source", "")) or "").strip()
    application_name = js8_application_name(rig_name)
    expected_mode = expected_storage_mode(variant_family, variant_version)
    explicit_root = str(
        values.get("application_data_root", values.get("js8_message_storage_root", "")) or ""
    ).strip()
    evidence = str(values.get("storage_evidence", values.get("js8_storage_evidence", "")) or "").strip()
    stored_mode = str(values.get("storage_mode", values.get("js8_storage_mode", "")) or "").strip().casefold()
    confirmed = evidence.startswith("operator_confirmed") or evidence.startswith("runtime_verified")
    root_path: Path | None = Path(os.path.expandvars(os.path.expanduser(explicit_root))) if explicit_root else None
    if root_path is None:
        candidates = qt_data_root_candidates(
            application_name=application_name,
            platform=platform,
            home=home,
            env=env,
        )
        root_path = next((candidate for candidate in candidates if _has_message_evidence(candidate)), None) if probe_existing else None
        if root_path is not None:
            evidence = "runtime_verified:message_files"
            confirmed = True
        elif candidates:
            root_path = candidates[0]
            evidence = evidence or "platform_candidate"

    root = canonicalize_storage_path(root_path) if root_path is not None else ""
    verified = bool(root and confirmed)
    if expected_mode == "shared":
        mode = "shared" if root else "unverified"
    elif expected_mode == "rig_scoped" and verified:
        mode = "rig_scoped"
    else:
        mode = stored_mode if stored_mode in STORAGE_MODES and verified else "unverified"
    save_dir = str(values.get("save_dir", values.get("js8_save_dir", "")) or "").strip()
    return JS8StorageResolution(
        variant_family=variant_family,
        variant_version=variant_version,
        rig_name=rig_name,
        rig_name_source=rig_source,
        application_name=application_name,
        data_root=root,
        all_path=str(Path(root) / "ALL.TXT") if root else "",
        directed_path=str(Path(root) / "DIRECTED.TXT") if root else "",
        inbox_path=str(Path(root) / "inbox.db3") if root else "",
        save_dir=save_dir,
        storage_mode=mode,
        expected_mode=expected_mode,
        verified=verified,
        evidence=evidence,
    )


def storage_collisions(instances: Sequence[Mapping[str, object]]) -> tuple[JS8StorageCollision, ...]:
    by_root: dict[str, list[tuple[str, str]]] = {}
    for index, instance in enumerate(instances):
        root = str(instance.get("application_data_root", instance.get("js8_message_storage_root", "")) or "").strip()
        if not root:
            continue
        key = canonicalize_storage_path(root).casefold()
        label = str(instance.get("radio_name", instance.get("name", "")) or f"JS8Call {index + 1}").strip()
        identity = str(instance.get("id", instance.get("system_key", "")) or label).strip()
        by_root.setdefault(key, []).append((label, identity))
    return tuple(
        JS8StorageCollision(
            canonical_root=root,
            labels=tuple(item[0] for item in rows),
            instance_ids=tuple(item[1] for item in rows),
        )
        for root, rows in sorted(by_root.items())
        if len(rows) > 1
    )


def canonicalize_storage_path(value: object) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    path = Path(os.path.expandvars(os.path.expanduser(raw)))
    try:
        return str(path.resolve(strict=False))
    except (OSError, RuntimeError):
        return str(path.absolute())


def has_js8_message_evidence(value: object) -> bool:
    """Return whether a bounded JS8 data root contains runtime message evidence."""

    root = str(value or "").strip()
    return bool(root) and _has_message_evidence(Path(root).expanduser())


def _has_message_evidence(path: Path) -> bool:
    try:
        return path.is_dir() and any((path / filename).is_file() for filename in MESSAGE_FILENAMES)
    except OSError:
        return False


def _unique_paths(paths: Sequence[Path]) -> tuple[Path, ...]:
    seen: set[str] = set()
    output: list[Path] = []
    for path in paths:
        key = str(path).casefold()
        if key in seen:
            continue
        seen.add(key)
        output.append(path)
    return tuple(output)
