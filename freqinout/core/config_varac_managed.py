"""Qualified, transactional writes for native VarAC cluster INI files.

This module deliberately has no store, UI, process-control, or live discovery
dependency.  A caller first captures the INI bytes and target state, then asks
the pure planner for an immutable plan.  Applying that plan is the only part
that touches the filesystem and is intentionally opt-in.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Callable, Mapping, Sequence, Tuple

from freqinout.core.config_backup import (
    ConfigBackupResult,
    ConfigRestoreResult,
    create_config_backup,
    restore_config_backup,
)
from freqinout.core.guided_app_config_plan import (
    GuidedAppConfigApplyItem,
    GuidedAppConfigApplyResult,
)


class VarACNativeConfigurationError(ValueError):
    """A source, plan, or apply precondition is not safe to continue with."""


_SECTION_RE = re.compile(r"^(?P<lead>\s*)\[(?P<section>[^]\r\n]+)\](?P<trail>\s*(?:[;#].*)?)$")
_KEY_RE = re.compile(
    r"^(?P<lead>[ \t]*)(?P<key>[^=;#\s][^=]*?)(?P<pre>[ \t]*)=(?P<post>[ \t]*)(?P<value>.*?)(?P<newline>\r\n|\n|\r)?\Z"
)
_VARAC_VARAHF_KEYS = (
    "VarahfMainPath",
    "VarahfMainPort",
    "VarahfMainHost",
    "VarahfEnableKissInterface",
    "VarahfMainKissPort",
    "VarahfMonitorPath",
    "VarahfMonitorPort",
    "VarahfLaunchOnModemConnect",
)
_VARA_INI_KEYS = {
    "Setup": ("TCP Command Port", "Enable KISS", "KISS Port"),
    "Monitor": ("Monitor Mode",),
}
_MAX_VARA_RUNTIME_FILES = 512
_MAX_VARA_RUNTIME_FILE_BYTES = 128 * 1024 * 1024
_MAX_VARA_RUNTIME_TOTAL_BYTES = 256 * 1024 * 1024


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _casefold(value: str) -> str:
    return str(value or "").strip().casefold()


def _frozen_mapping(value: Mapping[str, object]) -> Mapping[str, object]:
    """Freeze nested plan projections; frozen dataclasses alone are shallow."""

    return MappingProxyType(
        {
            str(key): _frozen_mapping(item) if isinstance(item, Mapping) else item
            for key, item in value.items()
        }
    )


@dataclass(frozen=True)
class VarACIniSource:
    """A source snapshot captured before planning (and therefore hashable)."""

    path: Path
    raw_bytes: bytes
    digest: str
    encoding: str
    newline: str
    layout_fingerprint: str
    values: Mapping[str, Mapping[str, str]]


@dataclass(frozen=True)
class VarACTargetState:
    path: Path
    exists: bool
    digest: str = ""


@dataclass(frozen=True)
class VarACWriterCapability:
    """One exact VarAC writer contract; there is deliberately no wildcard."""

    version: str
    platform: str
    operation: str
    layout_fingerprint: str
    required_sections: Tuple[str, ...] = ("OTHER", "VARAHF_CONFIG")
    required_varahf_keys: Tuple[str, ...] = _VARAC_VARAHF_KEYS
    vara_layout_fingerprint: str = ""
    required_vara_ini_sections: Tuple[str, ...] = ("Setup", "Monitor")
    creatable_sections: Tuple[str, ...] = ("VARAC_CLUSTER",)


def varac_layout_fingerprint(
    *,
    required_sections: Sequence[str] = ("OTHER", "VARAHF_CONFIG"),
    required_varahf_keys: Sequence[str] = _VARAC_VARAHF_KEYS,
) -> str:
    """Return the stable, qualified layout contract fingerprint.

    Unknown source keys are intentionally excluded: this fingerprint describes
    the contract FIO is permitted to edit, not the operator's full INI.
    """

    contract = "|".join(
        (_casefold(section) for section in required_sections)
    ) + "::" + "|".join(_casefold(key) for key in required_varahf_keys)
    return _sha256(contract.encode("utf-8"))


def vara_layout_fingerprint(
    *, required_sections: Sequence[str] = ("Setup", "Monitor")
) -> str:
    """Fingerprint the exact qualified VARA.ini key contract."""

    contract = "|".join(
        f"{_casefold(section)}:{'|'.join(_casefold(key) for key in _VARA_INI_KEYS[section])}"
        for section in required_sections
    )
    return _sha256(contract.encode("utf-8"))


# The native writer is intentionally exact-version only.  Tests and
# future release qualification inject additional capability tuples; do not turn
# this into a latest-version fallback.
SUPPORTED_VARAC_WRITERS: Tuple[VarACWriterCapability, ...] = (
    VarACWriterCapability(
        version="13.2.7",
        platform="windows",
        operation="create-member",
        layout_fingerprint=varac_layout_fingerprint(),
        vara_layout_fingerprint=vara_layout_fingerprint(),
    ),
    VarACWriterCapability(
        version="13.2.7",
        platform="windows",
        operation="convert-standalone",
        layout_fingerprint=varac_layout_fingerprint(),
        vara_layout_fingerprint=vara_layout_fingerprint(),
    ),
    VarACWriterCapability(
        version="13.2.7",
        platform="windows",
        operation="update-member",
        layout_fingerprint=varac_layout_fingerprint(),
        vara_layout_fingerprint=vara_layout_fingerprint(),
    ),
    VarACWriterCapability(
        version="13.2.7",
        platform="linux-wine",
        operation="create-member",
        layout_fingerprint=varac_layout_fingerprint(),
        vara_layout_fingerprint=vara_layout_fingerprint(),
    ),
    VarACWriterCapability(
        version="13.2.7",
        platform="linux-wine",
        operation="convert-standalone",
        layout_fingerprint=varac_layout_fingerprint(),
        vara_layout_fingerprint=vara_layout_fingerprint(),
    ),
    VarACWriterCapability(
        version="13.2.7",
        platform="linux-wine",
        operation="update-member",
        layout_fingerprint=varac_layout_fingerprint(),
        vara_layout_fingerprint=vara_layout_fingerprint(),
    ),
)
DEFAULT_SUPPORTED_VARAC_VERSIONS = frozenset(writer.version for writer in SUPPORTED_VARAC_WRITERS)


def supported_varac_versions(
    capabilities: Sequence[VarACWriterCapability] = SUPPORTED_VARAC_WRITERS,
) -> frozenset[str]:
    """Expose the exact (and test-injectable) supported writer version set."""

    return frozenset(item.version for item in capabilities)


@dataclass(frozen=True)
class VarACMemberInput:
    member_id: str
    source: VarACIniSource
    target: VarACTargetState
    member_number: int
    # Exact [VARAHF_CONFIG] allowlist, using VarAC 13.2.7's real key names.
    vara_settings: Mapping[str, str]
    executable_path: str
    vara_runtime: "VarARuntimeInput | None" = None
    working_directory: str = ""
    wine_prefix: str = ""
    launch_ini_path: str = ""


@dataclass(frozen=True)
class VarARuntimeInput:
    """A distinct VARA runtime's reviewed VARA.ini source and target.

    Runtime-directory discovery/copy is deliberately owned by a caller.  This
    bounded writer will only mutate the explicit VARA.ini target and will not
    call a member ready without this qualified profile evidence.
    """

    source_runtime_folder: Path
    target_runtime_folder: Path
    source: VarACIniSource
    target: VarACTargetState
    settings: Mapping[str, Mapping[str, str]]
    files: Tuple["VarARuntimeFileSource", ...]
    main_executable_relative_path: str = "VARA.exe"
    monitor_executable_relative_path: str = "VARA.exe"
    configured_main_executable_path: str = ""
    configured_monitor_executable_path: str = ""
    target_runtime_exists: bool = False


@dataclass(frozen=True)
class VarARuntimeFileSource:
    """One exact regular file in a caller-reviewed VARA runtime snapshot."""

    relative_path: str
    raw_bytes: bytes
    digest: str
    mode: int


@dataclass(frozen=True)
class VarACNativeClusterRequest:
    version: str
    platform: str
    operation: str
    members: Tuple[VarACMemberInput, ...]
    shared_db_path: str
    allowed_roots: Tuple[Path, ...]
    shared_bbs_path: str = ""
    shared_bbs_archive_path: str = ""
    managed_directories: Tuple[Path, ...] = ()
    native_shared_db_path: str = ""
    counter_refresh_seconds: int = 30
    ptt_lock_enabled: bool = True
    email_gateway_sender_member_id: str = ""
    wine_executable: str = "wine"
    # Discovery belongs to the worker/integration layer.  The pure planner
    # consumes its bounded evidence and refuses to prepare a running target.
    running_member_ids: Tuple[str, ...] = ()


@dataclass(frozen=True)
class VarACNativeMemberPlan:
    member_id: str
    source_path: Path
    target_path: Path
    source_digest: str
    expected_target_exists: bool
    expected_target_digest: str
    changes: Mapping[str, Mapping[str, str]]
    launch_command: Tuple[str, ...]
    working_directory: str
    wine_prefix: str
    vara_source_runtime_folder: Path
    vara_target_runtime_folder: Path
    vara_source_path: Path
    vara_target_path: Path
    vara_source_digest: str
    expected_vara_target_exists: bool
    expected_vara_target_digest: str
    vara_changes: Mapping[str, Mapping[str, str]]
    vara_main_executable_relative_path: str
    vara_monitor_executable_relative_path: str
    vara_runtime_files: Tuple[VarARuntimeFileSource, ...]


@dataclass(frozen=True)
class VarACNativeClusterPlan:
    version: str
    platform: str
    operation: str
    capability: VarACWriterCapability
    members: Tuple[VarACNativeMemberPlan, ...]
    shared_db_path: str
    shared_bbs_path: str
    shared_bbs_archive_path: str
    managed_directories: Tuple[Path, ...]
    managed_directory_resolved_paths: Tuple[Path, ...]
    native_shared_db_path: str
    allowed_roots: Tuple[Path, ...]
    plan_fingerprint: str
    counter_refresh_seconds: int
    ptt_lock_enabled: bool
    email_gateway_sender_member_id: str
    running_member_ids: Tuple[str, ...]


@dataclass(frozen=True)
class VarACNativeApplyResult:
    items: Tuple[GuidedAppConfigApplyItem, ...]
    backup: ConfigBackupResult | None = None
    restore: ConfigRestoreResult | None = None
    phase: str = ""
    error: str = ""
    created_directories: Tuple[Path, ...] = ()

    @property
    def ok(self) -> bool:
        return not self.error and not any(item.status in {"failed", "rolled_back"} for item in self.items)

    def as_guided_result(self) -> GuidedAppConfigApplyResult:
        """Adapt to the existing guided external-configuration rollback seam."""

        return GuidedAppConfigApplyResult(items=self.items, backup=self.backup, restore=self.restore)


def read_varac_ini_source(path: Path) -> VarACIniSource:
    """Capture an INI snapshot.  Keep this I/O outside pure plan construction."""

    actual = Path(path).expanduser()
    return parse_varac_ini_bytes(actual, actual.read_bytes())


def parse_varac_ini_bytes(path: Path, raw_bytes: bytes) -> VarACIniSource:
    return _parse_ini_bytes(path, raw_bytes, layout_fingerprint=varac_layout_fingerprint())


def parse_vara_ini_bytes(path: Path, raw_bytes: bytes) -> VarACIniSource:
    """Capture a qualified VARA.ini snapshot without runtime discovery."""

    return _parse_ini_bytes(path, raw_bytes, layout_fingerprint=vara_layout_fingerprint())


def snapshot_vara_runtime_files(runtime_folder: Path) -> Tuple[VarARuntimeFileSource, ...]:
    """Capture a bounded, regular-file-only VARA runtime source snapshot.

    This discovery helper is deliberately separate from pure planning.  It
    refuses symlinks at every level rather than copying an operator-controlled
    link into a managed runtime.
    """

    root = Path(runtime_folder).expanduser()
    if not root.is_dir() or root.is_symlink():
        raise VarACNativeConfigurationError(f"VARA runtime source is not a regular directory: {root}")
    entries: list[VarARuntimeFileSource] = []
    total_bytes = 0
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise VarACNativeConfigurationError(f"VARA runtime source contains a symlink: {path}")
        if path.is_dir():
            continue
        if not path.is_file():
            raise VarACNativeConfigurationError(f"VARA runtime source contains a non-regular file: {path}")
        if len(entries) >= _MAX_VARA_RUNTIME_FILES:
            raise VarACNativeConfigurationError("VARA runtime source contains too many files for a bounded managed clone.")
        size = int(path.stat().st_size)
        if size > _MAX_VARA_RUNTIME_FILE_BYTES:
            raise VarACNativeConfigurationError(f"VARA runtime source file is too large for a managed clone: {path}")
        total_bytes += size
        if total_bytes > _MAX_VARA_RUNTIME_TOTAL_BYTES:
            raise VarACNativeConfigurationError("VARA runtime source is too large for a bounded managed clone.")
        raw = path.read_bytes()
        if len(raw) != size:
            raise VarACNativeConfigurationError(f"VARA runtime source changed while it was being captured: {path}")
        entries.append(
            VarARuntimeFileSource(
                relative_path=str(path.relative_to(root)), raw_bytes=raw, digest=_sha256(raw), mode=path.stat().st_mode & 0o777,
            )
        )
    if not entries:
        raise VarACNativeConfigurationError("VARA runtime source contains no regular files.")
    return tuple(entries)


def _parse_ini_bytes(path: Path, raw_bytes: bytes, *, layout_fingerprint: str) -> VarACIniSource:
    encoding, text = _decode_ini(raw_bytes)
    newline = "\r\n" if "\r\n" in text else ("\n" if "\n" in text else "\r")
    values = _parse_values(text)
    return VarACIniSource(
        path=Path(path).expanduser(),
        raw_bytes=bytes(raw_bytes),
        digest=_sha256(raw_bytes),
        encoding=encoding,
        newline=newline,
        layout_fingerprint=layout_fingerprint,
        values=_frozen_mapping(values),
    )


def snapshot_target_state(path: Path) -> VarACTargetState:
    """Capture a target's expected state for a later stale-plan check."""

    actual = Path(path).expanduser()
    if not actual.exists():
        return VarACTargetState(path=actual, exists=False)
    if actual.is_symlink() or not actual.is_file():
        raise VarACNativeConfigurationError(f"Target is not a regular file: {actual}")
    return VarACTargetState(path=actual, exists=True, digest=_sha256(actual.read_bytes()))


def build_varac_native_cluster_plan(
    request: VarACNativeClusterRequest,
    *,
    capabilities: Sequence[VarACWriterCapability] = SUPPORTED_VARAC_WRITERS,
) -> VarACNativeClusterPlan:
    """Build a validated plan without reading, writing, or probing the system."""

    capability = _find_capability(request, capabilities)
    roots = tuple(Path(path).expanduser() for path in request.allowed_roots)
    if not roots:
        raise VarACNativeConfigurationError("At least one allowed VarAC configuration root is required.")
    members = tuple(request.members)
    if not members:
        raise VarACNativeConfigurationError("A native VarAC cluster requires at least one member.")
    _validate_request(request, capability, roots)
    managed_directory_resolved_paths = tuple(
        _resolved_managed_directory_target(Path(path))
        for path in request.managed_directories
    )
    changes_by_member = _member_changes(request)
    member_plans = tuple(
        VarACNativeMemberPlan(
            member_id=member.member_id,
            source_path=member.source.path,
            target_path=member.target.path,
            source_digest=member.source.digest,
            expected_target_exists=member.target.exists,
            expected_target_digest=member.target.digest,
            changes=changes_by_member[member.member_id],
            launch_command=plan_varac_launch_command(
                platform=request.platform,
                executable_path=member.executable_path,
                ini_path=member.launch_ini_path or str(member.target.path),
                wine_executable=request.wine_executable,
            ),
            working_directory=member.working_directory,
            wine_prefix=member.wine_prefix,
            vara_source_runtime_folder=member.vara_runtime.source_runtime_folder,  # validated above
            vara_target_runtime_folder=member.vara_runtime.target_runtime_folder,
            vara_source_path=member.vara_runtime.source.path,
            vara_target_path=member.vara_runtime.target.path,
            vara_source_digest=member.vara_runtime.source.digest,
            expected_vara_target_exists=member.vara_runtime.target.exists,
            expected_vara_target_digest=member.vara_runtime.target.digest,
            vara_changes=_frozen_mapping(member.vara_runtime.settings),
            vara_main_executable_relative_path=member.vara_runtime.main_executable_relative_path,
            vara_monitor_executable_relative_path=member.vara_runtime.monitor_executable_relative_path,
            vara_runtime_files=tuple(member.vara_runtime.files),
        )
        for member in members
    )
    fingerprint_data = json.dumps(
        {
            "version": request.version,
            "platform": request.platform,
            "operation": request.operation,
            "shared_db_path": request.shared_db_path,
            "shared_bbs_path": request.shared_bbs_path,
            "shared_bbs_archive_path": request.shared_bbs_archive_path,
            "managed_directories": [str(path) for path in request.managed_directories],
            "managed_directory_resolved_paths": [
                str(path) for path in managed_directory_resolved_paths
            ],
            "native_shared_db_path": request.native_shared_db_path,
            "counter_refresh_seconds": request.counter_refresh_seconds,
            "ptt_lock_enabled": request.ptt_lock_enabled,
            "email_gateway_sender_member_id": request.email_gateway_sender_member_id,
            "members": [
                {
                    "member_id": member.member_id,
                    "source_digest": member.source_digest,
                    "target_path": str(member.target_path),
                    "expected_target_exists": member.expected_target_exists,
                    "expected_target_digest": member.expected_target_digest,
                    "changes": {
                        str(section): dict(fields)
                        for section, fields in member.changes.items()
                    },
                    "launch_command": list(member.launch_command),
                    "working_directory": member.working_directory,
                    "wine_prefix": member.wine_prefix,
                    "vara_source_digest": member.vara_source_digest,
                    "vara_target_path": str(member.vara_target_path),
                    "vara_target_runtime_folder": str(member.vara_target_runtime_folder),
                    "expected_vara_target_exists": member.expected_vara_target_exists,
                    "expected_vara_target_digest": member.expected_vara_target_digest,
                    "vara_changes": {
                        str(section): dict(fields)
                        for section, fields in member.vara_changes.items()
                    },
                    "vara_main_executable_relative_path": member.vara_main_executable_relative_path,
                    "vara_monitor_executable_relative_path": member.vara_monitor_executable_relative_path,
                    "vara_runtime_files": [
                        {
                            "relative_path": item.relative_path,
                            "digest": item.digest,
                            "mode": item.mode,
                        }
                        for item in member.vara_runtime_files
                    ],
                }
                for member in member_plans
            ],
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return VarACNativeClusterPlan(
        version=request.version,
        platform=request.platform,
        operation=request.operation,
        capability=capability,
        members=member_plans,
        shared_db_path=request.shared_db_path,
        shared_bbs_path=request.shared_bbs_path,
        shared_bbs_archive_path=request.shared_bbs_archive_path,
        managed_directories=tuple(Path(path).expanduser() for path in request.managed_directories),
        managed_directory_resolved_paths=managed_directory_resolved_paths,
        native_shared_db_path=request.native_shared_db_path or request.shared_db_path,
        allowed_roots=roots,
        plan_fingerprint=_sha256(fingerprint_data),
        counter_refresh_seconds=request.counter_refresh_seconds,
        ptt_lock_enabled=request.ptt_lock_enabled,
        email_gateway_sender_member_id=request.email_gateway_sender_member_id,
        running_member_ids=tuple(request.running_member_ids),
    )


def plan_varac_launch_command(
    *,
    platform: str,
    executable_path: str,
    ini_path: str,
    wine_executable: str = "wine",
) -> Tuple[str, ...]:
    """Return argv, never a shell string, for the exact member INI identity."""

    normalized = _casefold(platform)
    if normalized == "windows":
        return (str(executable_path), str(ini_path))
    if normalized in {"linux-wine", "macos-wine"}:
        return (str(wine_executable), str(executable_path), str(ini_path))
    raise VarACNativeConfigurationError(f"Unsupported native VarAC platform: {platform}")


def render_varac_ini(source: VarACIniSource, changes: Mapping[str, Mapping[str, str]], *, capability: VarACWriterCapability) -> bytes:
    """Apply only reviewed keys while retaining all untouched source bytes/text."""

    encoding, text = _decode_ini(source.raw_bytes)
    if encoding != source.encoding:
        raise VarACNativeConfigurationError("Source INI encoding changed before rendering.")
    rendered = _rewrite_ini(text, changes, capability=capability, newline=source.newline)
    try:
        return rendered.encode(encoding)
    except UnicodeEncodeError as exc:
        raise VarACNativeConfigurationError(f"Reviewed value cannot be represented in {encoding}: {exc}") from exc


def render_vara_ini(source: VarACIniSource, changes: Mapping[str, Mapping[str, str]], *, capability: VarACWriterCapability) -> bytes:
    """Render only the qualified VARA.ini [Setup]/[Monitor] key projection."""

    return render_varac_ini(source, changes, capability=capability)


def apply_varac_native_cluster_plan(
    plan: VarACNativeClusterPlan,
    *,
    backup_root: Path,
    backup_reason: str = "varac-native-cluster",
    fail_at: str = "",
    failure_injector: Callable[[str], None] | None = None,
    running_checker: Callable[[VarACNativeMemberPlan], bool] | None = None,
    state_callback: Callable[[str, ConfigBackupResult | None], None] | None = None,
) -> VarACNativeApplyResult:
    """Back up, stage, validate, atomically promote, and read back one plan.

    ``fail_at`` is intentionally a test seam.  It accepts preflight, backup,
    stage, validate_staged, promote, or validate_promoted and exercises the
    same rollback path used for real exceptions.
    """

    items: list[GuidedAppConfigApplyItem] = []
    backup: ConfigBackupResult | None = None
    staged: dict[Path, Path] = {}
    staged_runtime_dirs: dict[Path, Path] = {}
    created_dirs: list[Path] = []
    phase = "preflight"
    try:
        _inject(phase, fail_at, failure_injector)
        if running_checker is not None:
            running = [member.member_id for member in plan.members if running_checker(member)]
            if running:
                raise VarACNativeConfigurationError(
                    "VarAC is running for native configuration target(s): " + ", ".join(running)
                )
        _revalidate_plan_paths_and_state(plan)
        phase = "backup"
        _inject(phase, fail_at, failure_injector)
        targets = tuple(
            target
            for member in plan.members
            for target in (member.target_path, member.vara_target_runtime_folder)
        )
        backup = create_config_backup(targets, reason=backup_reason, backup_root=backup_root)
        if any(item.status == "failed" for item in backup.items):
            raise OSError("Native VarAC configuration backup failed.")
        if state_callback is not None:
            state_callback("backup_ready", backup)
        phase = "stage"
        _inject(phase, fail_at, failure_injector)
        for member in plan.members:
            created_dirs.extend(_mkdir_untrusted_safe(member.target_path.parent, plan.allowed_roots))
            source = _read_source_if_fresh(member)
            payload = render_varac_ini(source, member.changes, capability=plan.capability)
            staged[member.target_path] = _stage_bytes(member.target_path, payload)
            created_dirs.extend(_mkdir_untrusted_safe(member.vara_target_runtime_folder.parent, plan.allowed_roots))
            staged_runtime_dirs[member.vara_target_runtime_folder] = _stage_runtime(member, plan.capability)
        for directory, resolved_directory in zip(
            plan.managed_directories,
            plan.managed_directory_resolved_paths,
        ):
            created_dirs.extend(
                _mkdir_reviewed_data_directory(
                    directory,
                    plan.allowed_roots,
                    expected_resolved=resolved_directory,
                )
            )
            items.append(
                GuidedAppConfigApplyItem(
                    action_id=f"varac:directory:{directory}",
                    app_id="varac",
                    action_type="create_directory",
                    target=str(directory),
                    status="applied",
                    detail="Reviewed VarAC managed directory is ready.",
                )
            )
        phase = "validate_staged"
        _inject(phase, fail_at, failure_injector)
        for member in plan.members:
            _semantic_verify(staged[member.target_path].read_bytes(), member.changes)
            _semantic_verify((staged_runtime_dirs[member.vara_target_runtime_folder] / "VARA.ini").read_bytes(), member.vara_changes)
        phase = "promote"
        if state_callback is not None:
            state_callback("promoting", backup)
        for member in plan.members:
            _inject(phase, fail_at, failure_injector)
            os.replace(staged[member.target_path], member.target_path)
            items.append(_item(member, "applied", "Native VarAC INI atomically promoted."))
            if member.vara_target_runtime_folder.exists():
                raise VarACNativeConfigurationError("Managed VARA runtime target appeared after preflight; refusing replacement.")
            os.replace(staged_runtime_dirs[member.vara_target_runtime_folder], member.vara_target_runtime_folder)
            items.append(_vara_item(member, "applied", "Distinct managed VARA runtime atomically promoted."))
        phase = "validate_promoted"
        _inject(phase, fail_at, failure_injector)
        for member in plan.members:
            _semantic_verify(member.target_path.read_bytes(), member.changes)
            _semantic_verify(member.vara_target_path.read_bytes(), member.vara_changes)
        return VarACNativeApplyResult(
            items=tuple(items),
            backup=backup,
            phase="complete",
            created_directories=tuple(created_dirs),
        )
    except Exception as exc:
        for member in plan.members:
            if not any(item.target == str(member.target_path) for item in items):
                items.append(_item(member, "failed", str(exc)))
            if not any(item.target == str(member.vara_target_path) for item in items):
                items.append(_vara_item(member, "failed", str(exc)))
        restore = restore_config_backup(backup) if backup is not None else None
        if restore is not None and restore.ok:
            items = [
                GuidedAppConfigApplyItem(
                    action_id=item.action_id,
                    app_id=item.app_id,
                    action_type=item.action_type,
                    target=item.target,
                    status="rolled_back" if item.status in {"applied", "failed"} else item.status,
                    detail=f"{item.detail} Native configuration backup was restored.",
                )
                for item in items
            ]
        _cleanup_staged(staged.values())
        _cleanup_staged_dirs(staged_runtime_dirs.values())
        _cleanup_empty_dirs(created_dirs)
        return VarACNativeApplyResult(
            items=tuple(items),
            backup=backup,
            restore=restore,
            phase=phase,
            error=str(exc),
        )
    finally:
        _cleanup_staged(staged.values())
        _cleanup_staged_dirs(staged_runtime_dirs.values())


def rollback_varac_native_cluster_apply(result: VarACNativeApplyResult) -> VarACNativeApplyResult:
    """Explicitly restore a completed native plan for a later FIO-store failure."""

    if result.backup is None:
        return result
    restore = restore_config_backup(result.backup)
    if restore.ok:
        _cleanup_empty_dirs(result.created_directories)
    items = tuple(
        GuidedAppConfigApplyItem(
            action_id=item.action_id,
            app_id=item.app_id,
            action_type=item.action_type,
            target=item.target,
            status="rolled_back" if item.status in {"applied", "failed"} and restore.ok else item.status,
            detail=f"{item.detail} Native configuration backup was restored." if restore.ok else item.detail,
        )
        for item in result.items
    )
    return VarACNativeApplyResult(
        items=items,
        backup=result.backup,
        restore=restore,
        phase="rolled_back",
        error="" if restore.ok else "Native rollback failed.",
    )


def _find_capability(request: VarACNativeClusterRequest, capabilities: Sequence[VarACWriterCapability]) -> VarACWriterCapability:
    matches = [
        item for item in capabilities
        if item.version == request.version and _casefold(item.platform) == _casefold(request.platform)
        and _casefold(item.operation) == _casefold(request.operation)
    ]
    if len(matches) != 1:
        raise VarACNativeConfigurationError(
            "No exact supported native writer matches this VarAC version, platform, and operation."
        )
    return matches[0]


def _validate_request(request: VarACNativeClusterRequest, capability: VarACWriterCapability, roots: Tuple[Path, ...]) -> None:
    if not 5 <= int(request.counter_refresh_seconds) <= 600:
        raise VarACNativeConfigurationError("CountersRefreshRateSec must be between 5 and 600.")
    ids = [_casefold(member.member_id) for member in request.members]
    if any(not item for item in ids) or len(set(ids)) != len(ids):
        raise VarACNativeConfigurationError("Each native VarAC member needs a unique non-empty identity.")
    numbers = [member.member_number for member in request.members]
    if any(not isinstance(number, int) or number <= 0 for number in numbers) or len(set(numbers)) != len(numbers):
        raise VarACNativeConfigurationError("Each native VarAC member needs a unique positive member number.")
    gateway = _casefold(request.email_gateway_sender_member_id)
    if gateway and gateway not in ids:
        raise VarACNativeConfigurationError("Email gateway sender must identify one enabled cluster member.")
    running = {_casefold(member_id) for member_id in request.running_member_ids}
    unknown_running = running - set(ids)
    if unknown_running:
        raise VarACNativeConfigurationError("Running-process evidence identifies an unknown native VarAC member.")
    if running:
        raise VarACNativeConfigurationError("Close VarAC before preparing a native configuration write.")
    for directory in request.managed_directories:
        _require_contained(Path(directory), roots, "managed VarAC directory")
        _resolved_managed_directory_target(Path(directory))
    if request.shared_bbs_path:
        _require_contained(Path(request.shared_bbs_path), roots, "shared VarAC BBS directory")
    if request.shared_bbs_archive_path:
        _require_contained(Path(request.shared_bbs_archive_path), roots, "shared VarAC BBS archive directory")
    target_keys: list[Path] = []
    runtime_folders: list[Path] = []
    claimed_ports: dict[int, str] = {}
    for member in request.members:
        _require_contained(member.source.path, roots, "source INI")
        _require_contained(member.target.path, roots, "target INI")
        if member.source.layout_fingerprint != capability.layout_fingerprint:
            raise VarACNativeConfigurationError("Source INI layout is not qualified for this exact native writer.")
        _require_layout(member.source.values, capability)
        if _sha256(member.source.raw_bytes) != member.source.digest:
            raise VarACNativeConfigurationError("Source INI snapshot digest does not match its bytes.")
        if member.target.exists and not member.target.digest:
            raise VarACNativeConfigurationError("Existing target requires an expected pre-write digest.")
        if not member.target.exists and member.target.digest:
            raise VarACNativeConfigurationError("Missing target cannot have a pre-write digest.")
        runtime = member.vara_runtime
        if runtime is None:
            raise VarACNativeConfigurationError(
                "A distinct qualified VARA runtime profile is required before native VarAC readiness can be planned."
            )
        _require_contained(runtime.source_runtime_folder, roots, "VARA source runtime folder")
        _require_contained(runtime.target_runtime_folder, roots, "managed VARA target runtime folder")
        _require_contained(runtime.source.path, roots, "VARA.ini source")
        _require_contained(runtime.target.path, roots, "VARA.ini target")
        if _paths_overlap(runtime.source_runtime_folder, runtime.target_runtime_folder):
            raise VarACNativeConfigurationError("Managed VARA target runtime folder may not overlap its source runtime folder.")
        if not _is_parent(_lexical_path(runtime.target_runtime_folder), _lexical_path(runtime.target.path)):
            raise VarACNativeConfigurationError("VARA.ini target must stay inside its distinct runtime folder.")
        if _lexical_path(runtime.target.path) != _lexical_path(runtime.target_runtime_folder / "VARA.ini"):
            raise VarACNativeConfigurationError("Managed VARA runtime must target its own VARA.ini.")
        if runtime.target_runtime_exists or runtime.target.exists:
            raise VarACNativeConfigurationError("Managed VARA target runtime must be absent; arbitrary existing folders are never replaced.")
        if runtime.source.layout_fingerprint != capability.vara_layout_fingerprint:
            raise VarACNativeConfigurationError("VARA.ini layout is not qualified for this exact native writer.")
        _require_vara_layout(runtime.source.values, capability)
        if _sha256(runtime.source.raw_bytes) != runtime.source.digest:
            raise VarACNativeConfigurationError("VARA.ini source snapshot digest does not match its bytes.")
        if runtime.target.exists and not runtime.target.digest:
            raise VarACNativeConfigurationError("Existing VARA.ini target requires an expected pre-write digest.")
        if not runtime.target.exists and runtime.target.digest:
            raise VarACNativeConfigurationError("Missing VARA.ini target cannot have a pre-write digest.")
        target_keys.extend((_lexical_path(member.target.path), _lexical_path(runtime.target.path)))
        runtime_folders.append(_lexical_path(runtime.target_runtime_folder))
        allowed_vara = {_casefold(key) for key in capability.required_varahf_keys}
        supplied = {_casefold(key): str(value) for key, value in member.vara_settings.items()}
        if set(supplied) != allowed_vara:
            raise VarACNativeConfigurationError("VARAHF_CONFIG settings must be the exact qualified allowlist.")
        if _casefold(supplied.get("varahflaunchonmodemconnect", "")) != "on":
            raise VarACNativeConfigurationError(
                "Managed VarAC cluster members must launch their node-local VARA modem."
            )
        _validate_runtime_files(runtime)
        _validate_vara_settings(member.member_id, supplied, runtime.settings, claimed_ports, runtime)
    for index, left in enumerate(target_keys):
        for right in target_keys[index + 1:]:
            if left == right or _is_parent(left, right) or _is_parent(right, left):
                raise VarACNativeConfigurationError("Native VarAC INI targets may not overlap.")
    for index, left in enumerate(runtime_folders):
        for right in runtime_folders[index + 1:]:
            if left == right or _is_parent(left, right) or _is_parent(right, left):
                raise VarACNativeConfigurationError("Each native VarAC member needs a distinct non-overlapping VARA runtime folder.")
    _require_contained(Path(request.shared_db_path), roots, "shared VarAC database")


def _require_layout(values: Mapping[str, Mapping[str, str]], capability: VarACWriterCapability) -> None:
    sections = {_casefold(name): data for name, data in values.items()}
    for section in capability.required_sections:
        if _casefold(section) not in sections:
            raise VarACNativeConfigurationError(f"Qualified source INI is missing [{section}].")
    other = sections[_casefold("OTHER")]
    if _casefold("DBCustomFilePath") not in {_casefold(key) for key in other}:
        raise VarACNativeConfigurationError("Qualified source INI is missing [OTHER] DBCustomFilePath.")
    vara = sections[_casefold("VARAHF_CONFIG")]
    available = {_casefold(key) for key in vara}
    missing = [key for key in capability.required_varahf_keys if _casefold(key) not in available]
    if missing:
        raise VarACNativeConfigurationError("Qualified source INI is missing VARAHF_CONFIG fields: " + ", ".join(missing))


def _require_vara_layout(values: Mapping[str, Mapping[str, str]], capability: VarACWriterCapability) -> None:
    sections = {_casefold(name): {_casefold(key) for key in fields} for name, fields in values.items()}
    for section in capability.required_vara_ini_sections:
        available = sections.get(_casefold(section))
        if available is None:
            raise VarACNativeConfigurationError(f"Qualified VARA.ini is missing [{section}].")
        missing = [key for key in _VARA_INI_KEYS[section] if _casefold(key) not in available]
        if missing:
            raise VarACNativeConfigurationError(
                f"Qualified VARA.ini is missing [{section}] fields: " + ", ".join(missing)
            )


def _validate_vara_settings(
    member_id: str,
    varac: Mapping[str, str],
    vara_ini: Mapping[str, Mapping[str, str]],
    claimed_ports: dict[int, str],
    runtime: VarARuntimeInput,
) -> None:
    expected_sections = {_casefold(section): {_casefold(key) for key in keys} for section, keys in _VARA_INI_KEYS.items()}
    supplied_sections = {_casefold(section): {_casefold(key): str(value) for key, value in keys.items()} for section, keys in vara_ini.items()}
    if set(supplied_sections) != set(expected_sections) or any(
        set(supplied_sections[section]) != keys for section, keys in expected_sections.items()
    ):
        raise VarACNativeConfigurationError("VARA.ini settings must be the exact qualified [Setup]/[Monitor] allowlist.")
    command = _port(varac[_casefold("VarahfMainPort")], "VarahfMainPort")
    data = command + 1
    if data > 65535:
        raise VarACNativeConfigurationError("VarahfMainPort leaves no valid implicit VARA data port.")
    kiss = _port(varac[_casefold("VarahfMainKissPort")], "VarahfMainKissPort")
    monitor = _port(varac[_casefold("VarahfMonitorPort")], "VarahfMonitorPort")
    setup = supplied_sections[_casefold("Setup")]
    if _port(setup[_casefold("TCP Command Port")], "VARA TCP Command Port") != command:
        raise VarACNativeConfigurationError("VARA.ini TCP Command Port must match VarahfMainPort.")
    if _port(setup[_casefold("KISS Port")], "VARA KISS Port") != kiss:
        raise VarACNativeConfigurationError("VARA.ini KISS Port must match VarahfMainKissPort.")
    if _enabled(setup[_casefold("Enable KISS")]) != _enabled(varac[_casefold("VarahfEnableKissInterface")]):
        raise VarACNativeConfigurationError("VARA.ini Enable KISS must match VarahfEnableKissInterface.")
    main_path = _configured_path_key(varac[_casefold("VarahfMainPath")])
    monitor_path = _configured_path_key(varac[_casefold("VarahfMonitorPath")])
    expected_main = _configured_path_key(
        runtime.configured_main_executable_path
        or str(runtime.target_runtime_folder / runtime.main_executable_relative_path)
    )
    expected_monitor = _configured_path_key(
        runtime.configured_monitor_executable_path
        or str(runtime.target_runtime_folder / runtime.monitor_executable_relative_path)
    )
    if main_path != expected_main or monitor_path != expected_monitor:
        raise VarACNativeConfigurationError(
            "VarahfMainPath and VarahfMonitorPath must point at the distinct managed VARA runtime executable."
        )
    for port, label in ((command, "command"), (data, "implicit data"), (kiss, "KISS"), (monitor, "monitor")):
        if port in claimed_ports:
            raise VarACNativeConfigurationError(
                f"Conflicting VARA {label} port {port} for {member_id} and {claimed_ports[port]}."
            )
        claimed_ports[port] = f"{member_id} {label}"


def _validate_runtime_files(runtime: VarARuntimeInput) -> None:
    seen: set[Path] = set()
    source_root = _lexical_path(runtime.source_runtime_folder)
    ini_relative = _lexical_path(runtime.source.path).relative_to(source_root) if _is_parent(source_root, _lexical_path(runtime.source.path)) else None
    if ini_relative is None or str(ini_relative).casefold() != "vara.ini":
        raise VarACNativeConfigurationError("Qualified VARA.ini source must be the source runtime's VARA.ini.")
    for entry in runtime.files:
        relative = Path(entry.relative_path)
        if not entry.relative_path or relative.is_absolute() or ".." in relative.parts:
            raise VarACNativeConfigurationError("VARA runtime snapshot file escapes its source folder.")
        if relative in seen:
            raise VarACNativeConfigurationError("VARA runtime snapshot contains duplicate relative paths.")
        seen.add(relative)
        if _sha256(entry.raw_bytes) != entry.digest:
            raise VarACNativeConfigurationError("VARA runtime snapshot file digest does not match its bytes.")
        if not 0 <= entry.mode <= 0o777:
            raise VarACNativeConfigurationError("VARA runtime snapshot file mode is invalid.")
    if Path("VARA.ini") not in seen:
        raise VarACNativeConfigurationError("VARA runtime snapshot must include VARA.ini.")
    if _sha256(next(item.raw_bytes for item in runtime.files if Path(item.relative_path) == Path("VARA.ini"))) != runtime.source.digest:
        raise VarACNativeConfigurationError("VARA.ini runtime snapshot does not match the reviewed INI source.")
    for required in (Path(runtime.main_executable_relative_path), Path(runtime.monitor_executable_relative_path)):
        if required.is_absolute() or ".." in required.parts or required not in seen:
            raise VarACNativeConfigurationError("Managed VARA runtime snapshot is missing its required executable.")


def _member_changes(request: VarACNativeClusterRequest) -> Mapping[str, Mapping[str, Mapping[str, str]]]:
    out: dict[str, object] = {}
    sender = _casefold(request.email_gateway_sender_member_id)
    for member in request.members:
        out[member.member_id] = _frozen_mapping({
            "VARAC_CLUSTER": {
                "ClusterEnabled": "ON",
                "InstanceNumber": str(member.member_number),
                "CountersRefreshRateSec": str(request.counter_refresh_seconds),
                "PTTLock": "ON" if request.ptt_lock_enabled else "OFF",
                "ClusterEmailGatewaySenderNode": "ON" if sender == _casefold(member.member_id) else "OFF",
            },
            "OTHER": {
                "DBCustomFilePath": str(
                    request.native_shared_db_path or request.shared_db_path
                )
            },
            "VARAHF_CONFIG": {str(key): str(value) for key, value in member.vara_settings.items()},
        })
    return _frozen_mapping(out)  # type: ignore[return-value]


def _decode_ini(raw: bytes) -> Tuple[str, str]:
    if raw.startswith(b"\xef\xbb\xbf"):
        return "utf-8-sig", raw.decode("utf-8-sig")
    try:
        return "utf-8", raw.decode("utf-8")
    except UnicodeDecodeError:
        return "cp1252", raw.decode("cp1252")


def _parse_values(text: str) -> Mapping[str, Mapping[str, str]]:
    values: dict[str, dict[str, str]] = {}
    current: str | None = None
    for line in text.splitlines():
        section = _SECTION_RE.match(line)
        if section:
            current = section.group("section").strip()
            values.setdefault(current, {})
            continue
        key = _KEY_RE.match(line)
        if key and current is not None:
            values.setdefault(current, {})[key.group("key").strip()] = key.group("value")
    return {section: dict(entries) for section, entries in values.items()}


def _rewrite_ini(text: str, changes: Mapping[str, Mapping[str, str]], *, capability: VarACWriterCapability, newline: str) -> str:
    lines = text.splitlines(keepends=True)
    wanted = {_casefold(section): {_casefold(key): str(value) for key, value in fields.items()} for section, fields in changes.items()}
    seen: dict[str, set[str]] = {section: set() for section in wanted}
    present: set[str] = set()
    current: str | None = None
    output: list[str] = []

    def append_missing(section: str | None) -> None:
        """Keep newly permitted keys inside their existing section."""

        if section not in wanted or section not in present:
            return
        missing = [key for key in wanted[section] if key not in seen[section]]
        if not missing:
            return
        if output and not output[-1].endswith(("\n", "\r")):
            output.append(newline)
        original_section = next(name for name in changes if _casefold(name) == section)
        for key in missing:
            original_key = next(name for name in changes[original_section] if _casefold(name) == key)
            output.append(f"{original_key}={wanted[section][key]}{newline}")
            seen[section].add(key)

    for line in lines:
        raw = line.rstrip("\r\n")
        ending = line[len(raw):]
        section_match = _SECTION_RE.match(raw)
        if section_match:
            append_missing(current)
            current = _casefold(section_match.group("section"))
            present.add(current)
            output.append(line)
            continue
        key_match = _KEY_RE.match(line)
        if key_match and current in wanted:
            key = _casefold(key_match.group("key"))
            if key in wanted[current]:
                # Preserve key spelling/whitespace/newline.  An inline comment
                # on a controlled value is part of that value for some INI
                # readers, so retaining it would make native semantic readback
                # ambiguous; reviewed keys are the documented exception to
                # byte preservation.
                value = wanted[current][key]
                output.append(
                    f"{key_match.group('lead')}{key_match.group('key')}{key_match.group('pre')}="
                    f"{key_match.group('post')}{value}{key_match.group('newline')}"
                )
                seen[current].add(key)
                continue
        output.append(line)
    append_missing(current)
    for section, fields in wanted.items():
        missing = [key for key in fields if key not in seen[section]]
        if not missing:
            continue
        if section not in present:
            if section not in {_casefold(item) for item in capability.creatable_sections}:
                raise VarACNativeConfigurationError(f"Writer may not create undocumented [{section}] section.")
            if output and not output[-1].endswith(("\n", "\r")):
                output.append(newline)
            if output and output[-1].strip():
                output.append(newline)
            output.append(f"[{next(name for name in changes if _casefold(name) == section)}]{newline}")
        for key in missing:
            original_key = next(name for name in changes[next(section_name for section_name in changes if _casefold(section_name) == section)] if _casefold(name) == key)
            output.append(f"{original_key}={fields[key]}{newline}")
    return "".join(output)


def _revalidate_plan_paths_and_state(plan: VarACNativeClusterPlan) -> None:
    if len(plan.managed_directories) != len(plan.managed_directory_resolved_paths):
        raise VarACNativeConfigurationError(
            "Managed VarAC directory evidence is incomplete; prepare the plan again."
        )
    for directory, expected_resolved in zip(
        plan.managed_directories,
        plan.managed_directory_resolved_paths,
    ):
        _require_contained(directory, plan.allowed_roots, "managed VarAC directory")
        if _resolved_managed_directory_target(directory) != _lexical_path(expected_resolved):
            raise VarACNativeConfigurationError(
                f"Reviewed VarAC directory alias changed before apply: {directory}"
            )
    target_paths = set()
    for member in plan.members:
        _require_contained(member.vara_source_runtime_folder, plan.allowed_roots, "VARA source runtime folder")
        _require_contained(member.vara_target_runtime_folder, plan.allowed_roots, "managed VARA target runtime folder")
        _reject_symlink_path(member.vara_source_runtime_folder)
        _reject_symlink_path(member.vara_target_runtime_folder)
        if member.vara_target_runtime_folder.exists():
            raise VarACNativeConfigurationError("Managed VARA target runtime already exists; refusing arbitrary replacement.")
        if not _is_parent(_lexical_path(member.vara_target_runtime_folder), _lexical_path(member.vara_target_path)):
            raise VarACNativeConfigurationError("VARA.ini target escaped its reviewed runtime folder.")
        for source, target, expected_exists, expected_digest, label in (
            (member.source_path, member.target_path, member.expected_target_exists, member.expected_target_digest, "VarAC INI"),
            (member.vara_source_path, member.vara_target_path, member.expected_vara_target_exists, member.expected_vara_target_digest, "VARA.ini"),
        ):
            _require_contained(source, plan.allowed_roots, f"{label} source")
            _require_contained(target, plan.allowed_roots, f"{label} target")
            _reject_symlink_path(source)
            _reject_symlink_path(target)
            key = _lexical_path(target)
            if key in target_paths:
                raise VarACNativeConfigurationError("Native VarAC/VARA INI targets may not overlap.")
            target_paths.add(key)
            exists = target.exists()
            if exists != expected_exists:
                raise VarACNativeConfigurationError(f"Stale target state: {target}")
            if exists:
                if not target.is_file():
                    raise VarACNativeConfigurationError(f"Target is not a regular file: {target}")
                if _sha256(target.read_bytes()) != expected_digest:
                    raise VarACNativeConfigurationError(f"Stale target digest: {target}")


def _read_source_if_fresh(member: VarACNativeMemberPlan) -> VarACIniSource:
    if not member.source_path.is_file():
        raise VarACNativeConfigurationError(f"Source INI is not a regular file: {member.source_path}")
    source = read_varac_ini_source(member.source_path)
    if source.digest != member.source_digest:
        raise VarACNativeConfigurationError(f"Stale source digest: {member.source_path}")
    return source


def _read_vara_source_if_fresh(member: VarACNativeMemberPlan) -> VarACIniSource:
    if not member.vara_source_path.is_file():
        raise VarACNativeConfigurationError(f"VARA.ini source is not a regular file: {member.vara_source_path}")
    source = parse_vara_ini_bytes(member.vara_source_path, member.vara_source_path.read_bytes())
    if source.digest != member.vara_source_digest:
        raise VarACNativeConfigurationError(f"Stale VARA.ini source digest: {member.vara_source_path}")
    return source


def _stage_runtime(member: VarACNativeMemberPlan, capability: VarACWriterCapability) -> Path:
    """Stage the reviewed regular-file runtime clone beside its managed target."""

    current = snapshot_vara_runtime_files(member.vara_source_runtime_folder)
    if tuple((item.relative_path, item.digest, item.mode) for item in current) != tuple(
        (item.relative_path, item.digest, item.mode) for item in member.vara_runtime_files
    ):
        raise VarACNativeConfigurationError("Stale VARA runtime source snapshot.")
    vara_source = _read_vara_source_if_fresh(member)
    rendered_vara = render_vara_ini(vara_source, member.vara_changes, capability=capability)
    stage = Path(tempfile.mkdtemp(prefix=f".{member.vara_target_runtime_folder.name}.fio-", suffix=".stage", dir=str(member.vara_target_runtime_folder.parent)))
    try:
        for item in member.vara_runtime_files:
            destination = stage / item.relative_path
            destination.parent.mkdir(parents=True, exist_ok=True)
            payload = rendered_vara if Path(item.relative_path) == Path("VARA.ini") else item.raw_bytes
            destination.write_bytes(payload)
            os.chmod(destination, item.mode)
        return stage
    except OSError:
        shutil.rmtree(stage, ignore_errors=True)
        raise


def _semantic_verify(raw: bytes, expected: Mapping[str, Mapping[str, str]]) -> None:
    values = parse_varac_ini_bytes(Path("readback.ini"), raw).values
    sections = {_casefold(section): {_casefold(key): value for key, value in fields.items()} for section, fields in values.items()}
    for section, fields in expected.items():
        actual = sections.get(_casefold(section), {})
        for key, value in fields.items():
            if actual.get(_casefold(key)) != str(value):
                raise VarACNativeConfigurationError(f"Native INI readback did not match [{section}] {key}.")


def _item(member: VarACNativeMemberPlan, status: str, detail: str) -> GuidedAppConfigApplyItem:
    return GuidedAppConfigApplyItem(
        action_id=f"varac:{member.member_id}:write-native-cluster-ini",
        app_id="varac",
        action_type="write_varac_native_cluster_ini",
        target=str(member.target_path),
        status=status,
        detail=detail,
    )


def _vara_item(member: VarACNativeMemberPlan, status: str, detail: str) -> GuidedAppConfigApplyItem:
    return GuidedAppConfigApplyItem(
        action_id=f"varac:{member.member_id}:write-vara-runtime-ini",
        app_id="vara",
        action_type="write_vara_runtime_ini",
        target=str(member.vara_target_path),
        status=status,
        detail=detail,
    )


def _stage_bytes(target: Path, payload: bytes) -> Path:
    """Create a same-filesystem staged file for one explicitly approved target."""

    fd, temp_name = tempfile.mkstemp(prefix=f".{target.name}.fio-", suffix=".tmp", dir=str(target.parent))
    with os.fdopen(fd, "wb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    return Path(temp_name)


def _inject(phase: str, fail_at: str, injector: Callable[[str], None] | None) -> None:
    if injector is not None:
        injector(phase)
    if fail_at and _casefold(fail_at) == _casefold(phase):
        raise OSError(f"Injected failure at {phase}.")


def _mkdir_untrusted_safe(target_parent: Path, roots: Sequence[Path]) -> list[Path]:
    """Create a native configuration/runtime parent with no symlink ancestry."""

    _require_contained(target_parent, roots, "target directory")
    if target_parent.exists() and not target_parent.is_dir():
        raise VarACNativeConfigurationError(
            f"Reviewed VarAC directory target is not a directory: {target_parent}"
        )
    created: list[Path] = []
    stack: list[Path] = []
    current = target_parent
    while not current.exists():
        stack.append(current)
        if current == current.parent:
            raise VarACNativeConfigurationError(f"Cannot create target directory: {target_parent}")
        current = current.parent
    _reject_symlink_path(current)
    for directory in reversed(stack):
        directory.mkdir()
        created.append(directory)
    _reject_symlink_path(target_parent)
    return created


def _mkdir_reviewed_data_directory(
    target_parent: Path,
    roots: Sequence[Path],
    *,
    expected_resolved: Path,
) -> list[Path]:
    """Create reviewed data directories through a stable filesystem alias.

    Wine commonly exposes the Linux Desktop as a symlink below
    ``drive_c/users/<user>``.  That alias is valid for VarAC BBS data, but it
    must resolve to the same directory at apply time that it resolved to when
    the immutable plan was built.  Native INI and executable/runtime targets
    continue to use the stricter no-symlink policy.
    """

    _require_contained(target_parent, roots, "target directory")
    expected = _lexical_path(expected_resolved)
    actual_before = _resolved_managed_directory_target(target_parent)
    if actual_before != expected:
        raise VarACNativeConfigurationError(
            f"Reviewed VarAC directory alias changed before apply: {target_parent}"
        )
    if target_parent.exists() and not target_parent.is_dir():
        raise VarACNativeConfigurationError(
            f"Reviewed VarAC directory target is not a directory: {target_parent}"
        )
    created: list[Path] = []
    stack: list[Path] = []
    current = target_parent
    while not current.exists():
        stack.append(current)
        if current == current.parent:
            raise VarACNativeConfigurationError(f"Cannot create target directory: {target_parent}")
        current = current.parent
    if not current.is_dir():
        raise VarACNativeConfigurationError(
            f"Reviewed VarAC directory ancestor is not a directory: {current}"
        )
    for directory in reversed(stack):
        directory.mkdir()
        if directory.is_symlink() or not directory.is_dir():
            raise VarACNativeConfigurationError(
                f"Reviewed VarAC directory changed while it was being created: {directory}"
            )
        created.append(directory)
    actual_after = _resolved_managed_directory_target(target_parent)
    if actual_after != expected:
        raise VarACNativeConfigurationError(
            f"Reviewed VarAC directory alias changed during apply: {target_parent}"
        )
    return created


def _resolved_managed_directory_target(path: Path) -> Path:
    """Resolve a reviewed data-directory alias without accepting broken links."""

    candidate = _lexical_path(path)
    existing_ancestor = candidate
    while not existing_ancestor.exists():
        if existing_ancestor.is_symlink():
            raise VarACNativeConfigurationError(
                f"Broken symlink is not allowed for a managed VarAC directory: {path}"
            )
        if existing_ancestor == existing_ancestor.parent:
            break
        existing_ancestor = existing_ancestor.parent
    if existing_ancestor.exists() and not existing_ancestor.is_dir():
        raise VarACNativeConfigurationError(
            f"Managed VarAC directory ancestor is not a directory: {existing_ancestor}"
        )
    if candidate.exists() and not candidate.is_dir():
        raise VarACNativeConfigurationError(
            f"Managed VarAC directory target is not a directory: {path}"
        )
    try:
        return _lexical_path(candidate.resolve(strict=False))
    except (OSError, RuntimeError) as exc:
        raise VarACNativeConfigurationError(
            f"Managed VarAC directory alias could not be resolved safely: {path}"
        ) from exc


def _cleanup_staged(paths: Sequence[Path]) -> None:
    for path in paths:
        try:
            if path.exists() or path.is_symlink():
                path.unlink()
        except OSError:
            pass


def _cleanup_staged_dirs(paths: Sequence[Path]) -> None:
    for path in paths:
        try:
            if path.is_dir() and not path.is_symlink():
                shutil.rmtree(path)
        except OSError:
            pass


def _cleanup_empty_dirs(paths: Sequence[Path]) -> None:
    for path in reversed(tuple(paths)):
        try:
            path.rmdir()
        except OSError:
            pass


def _reject_symlink_path(path: Path) -> None:
    current = Path(path).expanduser()
    while True:
        if current.is_symlink():
            raise VarACNativeConfigurationError(f"Symlink path is not allowed for native VarAC writes: {path}")
        if current == current.parent:
            return
        current = current.parent


def _require_contained(path: Path, roots: Sequence[Path], label: str) -> None:
    candidate = _lexical_path(path)
    if not any(_is_parent(_lexical_path(root), candidate) or _lexical_path(root) == candidate for root in roots):
        raise VarACNativeConfigurationError(f"{label.capitalize()} escapes the approved VarAC configuration roots: {path}")


def _lexical_path(path: Path) -> Path:
    return Path(os.path.abspath(os.path.expanduser(str(path))))


def _configured_path_key(value: object) -> str:
    """Compare Windows/Wine and host path spellings without filesystem I/O."""

    return re.sub(r"[\\/]+", "/", str(value or "").strip()).casefold()


def _is_parent(parent: Path, child: Path) -> bool:
    try:
        child.relative_to(parent)
        return True
    except ValueError:
        return False


def _paths_overlap(left: Path, right: Path) -> bool:
    left_path = _lexical_path(left)
    right_path = _lexical_path(right)
    return left_path == right_path or _is_parent(left_path, right_path) or _is_parent(right_path, left_path)


def _port(value: str, key: str) -> int:
    try:
        port = int(str(value).strip())
    except ValueError as exc:
        raise VarACNativeConfigurationError(f"{key} must be a TCP/UDP port number.") from exc
    if not 1 <= port <= 65535:
        raise VarACNativeConfigurationError(f"{key} must be between 1 and 65535.")
    return port


def _enabled(value: str) -> bool:
    normalized = _casefold(value)
    if normalized in {"1", "on", "true", "yes"}:
        return True
    if normalized in {"0", "off", "false", "no"}:
        return False
    raise VarACNativeConfigurationError(f"Expected ON/OFF-style value, got {value!r}.")
