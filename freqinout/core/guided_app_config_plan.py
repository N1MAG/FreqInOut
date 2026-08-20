from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping, Sequence, Tuple

from freqinout.core.config_autodiscovery import APP_DISPLAY_NAMES, RadioInstanceProposal, discover_varac_local_assets
from freqinout.core.config_backup import ConfigBackupResult, create_config_backup
from freqinout.core.config_js8_managed import (
    JS8CallManagedProfilePlan,
    apply_js8call_multisettings_plan,
    build_js8call_managed_profile_plans,
)
from freqinout.core.config_managed_profiles import build_flrig_fldigi_managed_profile_plans


@dataclass(frozen=True)
class GuidedAppConfigAction:
    action_id: str
    app_id: str
    instance_name: str
    action_type: str
    target: str
    summary: str
    requires_backup: bool
    writes_external_config: bool
    manual_review_required: bool = False
    details: Mapping[str, str] = field(default_factory=dict)
    notes: Tuple[str, ...] = field(default_factory=tuple)

    @property
    def display_name(self) -> str:
        return APP_DISPLAY_NAMES.get(self.app_id, self.app_id)


@dataclass(frozen=True)
class GuidedAppConfigPlan:
    actions: Tuple[GuidedAppConfigAction, ...]
    review_items: Tuple[str, ...]
    blocked: bool = False

    @property
    def backup_required(self) -> bool:
        return any(action.requires_backup for action in self.actions)

    @property
    def manual_review_required(self) -> bool:
        return any(action.manual_review_required for action in self.actions) or bool(self.review_items)


@dataclass(frozen=True)
class GuidedAppConfigApplyItem:
    action_id: str
    app_id: str
    action_type: str
    target: str
    status: str
    detail: str = ""


@dataclass(frozen=True)
class GuidedAppConfigApplyResult:
    items: Tuple[GuidedAppConfigApplyItem, ...]
    backup: ConfigBackupResult | None = None

    @property
    def ok(self) -> bool:
        return not any(item.status == "failed" for item in self.items)

    @property
    def external_writes_applied(self) -> bool:
        return any(item.status == "applied" and item.action_type != "create_directory" for item in self.items)


def build_guided_external_app_config_plan(
    proposals: Sequence[RadioInstanceProposal],
    *,
    config_root: Path,
    app_paths: Mapping[str, str] | None = None,
    callsign: str = "",
    grid: str = "",
    include_varac: bool = False,
    allow_external_writes: bool = True,
    js8_control_route: str = "flrig",
    radio_label: str = "",
) -> GuidedAppConfigPlan:
    """Return a reviewable external-app setup plan without writing files."""

    paths = dict(app_paths or {})
    actions: list[GuidedAppConfigAction] = []
    review_items: list[str] = []
    seen_dirs: set[str] = set()

    selected_varac = include_varac or any(proposal.varac_enabled for proposal in proposals)
    if selected_varac:
        _add_varac_integration_action(actions, proposals, paths)

    if not allow_external_writes:
        selected_apps = sorted({app for proposal in proposals for app in proposal.enabled_apps})
        app_text = ", ".join(APP_DISPLAY_NAMES.get(app, app) for app in selected_apps) if selected_apps else "selected apps"
        read_only_review = [f"Read-only setup will remember {app_text} references in FIO without changing external app configuration."]
        if selected_varac:
            read_only_review.extend(_varac_review_items())
        return GuidedAppConfigPlan(
            actions=tuple(actions),
            review_items=tuple(read_only_review),
        )

    for proposal in proposals:
        fast_plans = build_flrig_fldigi_managed_profile_plans(
            proposal,
            config_root=config_root,
            app_paths=paths,
        )
        for plan in fast_plans:
            for directory in (plan.config_dir, *plan.data_dirs):
                _add_directory_action(
                    actions,
                    seen_dirs,
                    app_id=plan.app_id,
                    instance_name=plan.instance_name,
                    directory=directory,
                )
            actions.append(
                GuidedAppConfigAction(
                    action_id=f"{plan.instance_name}:{plan.app_id}:write-managed-config",
                    app_id=plan.app_id,
                    instance_name=plan.instance_name,
                    action_type="write_managed_config",
                    target=str(plan.config_dir),
                    summary=(
                        f"Prepare {plan.display_name} profile {plan.instance_name} "
                        f"on {plan.expected_host}:{plan.expected_port}."
                    ),
                    requires_backup=True,
                    writes_external_config=True,
                    details={
                        "executable_path": plan.executable_path,
                        "config_dir": str(plan.config_dir),
                        "launch_args": " ".join(plan.launch_args),
                        "expected_host": plan.expected_host,
                        "expected_port": str(plan.expected_port),
                    },
                    notes=tuple(plan.notes),
                )
            )

    js8_plans = build_js8call_managed_profile_plans(
        proposals,
        config_root=config_root,
        js8call_path=paths.get("js8call", ""),
        callsign=callsign,
        grid=grid,
        control_route=js8_control_route,
        radio_label=radio_label,
    )
    for plan in js8_plans:
        for directory in (plan.config_dir, plan.save_dir, plan.forms_dir):
            _add_directory_action(
                actions,
                seen_dirs,
                app_id="js8call",
                instance_name=plan.instance_name,
                directory=directory,
            )
        if plan.control_route == "flrig":
            summary = (
                f"Prepare JS8Call profile {plan.profile_name} with FLRig "
                f"{plan.flrig_host}:{plan.flrig_port} and API port {plan.tcp_port}."
            )
            route_notes = ("Some JS8Call builds may still require operator confirmation in JS8Call after profile creation.",)
        else:
            summary = f"Prepare JS8Call profile {plan.profile_name} with API port {plan.tcp_port}; {plan.rig_summary}"
            route_notes = ("Confirm JS8Call's radio/CAT selection in JS8Call before relying on frequency control.",)
        actions.append(
            GuidedAppConfigAction(
                action_id=f"{plan.instance_name}:js8call:update-multisettings",
                app_id="js8call",
                instance_name=plan.instance_name,
                action_type="update_js8_multisettings",
                target=str(plan.config_dir),
                summary=summary,
                requires_backup=True,
                writes_external_config=True,
                details={
                    "executable_path": plan.executable_path,
                    "profile_name": plan.profile_name,
                    "config_dir": str(plan.config_dir),
                    "save_dir": str(plan.save_dir),
                    "directed_path": str(plan.directed_path),
                    "control_route": plan.control_route,
                    "rig_summary": plan.rig_summary,
                    "flrig_port": str(plan.flrig_port),
                    "tcp_port": str(plan.tcp_port),
                    "udp_port": str(plan.udp_port),
                    "js8call_ini_path": str(paths.get("js8call_ini_path", "") or ""),
                },
                notes=(
                    "JS8Call MultiSettings writes require backup of the existing JS8Call.ini before apply.",
                    *route_notes,
                ),
            )
        )

    if selected_varac:
        review_items.extend(_varac_review_items())

    return GuidedAppConfigPlan(actions=tuple(actions), review_items=tuple(review_items))


def apply_guided_external_app_config_plan(
    plan: GuidedAppConfigPlan,
    *,
    allow_external_writes: bool = False,
    backup_root: Path | None = None,
    backup_reason: str = "guided-app-config",
) -> GuidedAppConfigApplyResult:
    """Apply the safe portions of a guided app configuration plan.

    Directory preparation is safe and idempotent. External app writes remain
    opt-in and only run when the action has an explicit, supported target.
    """

    items: list[GuidedAppConfigApplyItem] = []
    write_targets = _guided_plan_external_write_targets(plan) if allow_external_writes else ()
    backup_result = (
        create_config_backup(write_targets, reason=backup_reason, backup_root=backup_root)
        if write_targets
        else None
    )
    backup_failed = bool(
        backup_result is not None and any(item.status == "failed" for item in backup_result.items)
    )
    js8_plans_by_action = _js8_multisettings_plans_by_action(plan)
    for action in plan.actions:
        action_type = str(action.action_type or "").strip()
        target = str(action.target or "").strip()
        if action_type == "create_directory":
            try:
                Path(target).expanduser().mkdir(parents=True, exist_ok=True)
                items.append(
                    GuidedAppConfigApplyItem(
                        action_id=action.action_id,
                        app_id=action.app_id,
                        action_type=action_type,
                        target=target,
                        status="applied",
                        detail="Directory ready.",
                    )
                )
            except OSError as exc:
                items.append(
                    GuidedAppConfigApplyItem(
                        action_id=action.action_id,
                        app_id=action.app_id,
                        action_type=action_type,
                        target=target,
                        status="failed",
                        detail=str(exc),
                    )
                )
            continue
        if not action.writes_external_config:
            items.append(
                GuidedAppConfigApplyItem(
                    action_id=action.action_id,
                    app_id=action.app_id,
                    action_type=action_type,
                    target=target,
                    status="remembered",
                    detail="FIO-side integration reference only.",
                )
            )
            continue
        if not allow_external_writes:
            items.append(
                GuidedAppConfigApplyItem(
                    action_id=action.action_id,
                    app_id=action.app_id,
                    action_type=action_type,
                    target=target,
                    status="skipped",
                    detail="External app writes were not enabled.",
                )
            )
            continue
        if backup_failed:
            items.append(
                GuidedAppConfigApplyItem(
                    action_id=action.action_id,
                    app_id=action.app_id,
                    action_type=action_type,
                    target=target,
                    status="failed",
                    detail="Backup failed; external app write was not attempted.",
                )
            )
            continue
        if action_type == "update_js8_multisettings":
            ini_path = str(action.details.get("js8call_ini_path", "") or "").strip()
            js8_plan = js8_plans_by_action.get(action.action_id)
            if ini_path and js8_plan is not None:
                try:
                    applied_path = apply_js8call_multisettings_plan(js8_plan, ini_path=Path(ini_path))
                    items.append(
                        GuidedAppConfigApplyItem(
                            action_id=action.action_id,
                            app_id=action.app_id,
                            action_type=action_type,
                            target=str(applied_path),
                            status="applied",
                            detail="JS8Call MultiSettings profile updated.",
                        )
                    )
                except OSError as exc:
                    items.append(
                        GuidedAppConfigApplyItem(
                            action_id=action.action_id,
                            app_id=action.app_id,
                            action_type=action_type,
                            target=ini_path,
                            status="failed",
                            detail=str(exc),
                        )
                    )
                continue
        items.append(
            GuidedAppConfigApplyItem(
                action_id=action.action_id,
                app_id=action.app_id,
                action_type=action_type,
                target=target,
                status="skipped",
                detail="No supported explicit external writer is available for this action yet.",
            )
        )
    return GuidedAppConfigApplyResult(items=tuple(items), backup=backup_result)


def _guided_plan_external_write_targets(plan: GuidedAppConfigPlan) -> Tuple[Path, ...]:
    targets = []
    for action in plan.actions:
        if not action.writes_external_config:
            continue
        if str(action.action_type or "").strip() == "update_js8_multisettings":
            ini_path = str(action.details.get("js8call_ini_path", "") or "").strip()
            if ini_path:
                targets.append(Path(ini_path).expanduser())
    return tuple(targets)


def _js8_multisettings_plans_by_action(plan: GuidedAppConfigPlan) -> Mapping[str, JS8CallManagedProfilePlan]:
    out: dict[str, JS8CallManagedProfilePlan] = {}
    for action in plan.actions:
        if str(action.action_type or "").strip() != "update_js8_multisettings":
            continue
        details = action.details
        profile_name = str(details.get("profile_name", "") or action.instance_name or "").strip()
        if not profile_name:
            continue
        settings = {
            "TCPEnabled": "true",
            "TCPServer": "127.0.0.1",
            "TCPServerPort": str(details.get("tcp_port", "") or ""),
            "TCPMaxConnections": "2",
            "UDPEnabled": "true",
            "UDPServerPort": str(details.get("udp_port", "") or ""),
            "SaveDir": str(details.get("save_dir", "") or ""),
        }
        control_route = str(details.get("control_route", "") or "flrig").strip().lower()
        if control_route == "flrig":
            settings["Rig"] = "FLRig FLRig"
            settings["CATNetworkPort"] = f"127.0.0.1:{details.get('flrig_port', '')}"
        out[action.action_id] = JS8CallManagedProfilePlan(
            profile_name=profile_name,
            instance_name=str(action.instance_name or profile_name),
            executable_path=str(details.get("executable_path", "") or ""),
            config_dir=Path(details.get("config_dir", "") or "."),
            save_dir=Path(details.get("save_dir", "") or "."),
            forms_dir=Path(details.get("forms_dir", "") or "."),
            directed_path=Path(details.get("directed_path", "") or "."),
            flrig_host="127.0.0.1",
            flrig_port=_int_text(details.get("flrig_port"), 0),
            tcp_host="127.0.0.1",
            tcp_port=_int_text(details.get("tcp_port"), 0),
            udp_port=_int_text(details.get("udp_port"), 0),
            control_route=control_route,
            rig_summary=str(details.get("rig_summary", "") or ""),
            settings={key: value for key, value in settings.items() if str(value or "").strip()},
        )
    return out


def _int_text(value: object, default: int) -> int:
    try:
        return int(str(value if value is not None else "").strip() or default)
    except (TypeError, ValueError):
        return default


def _varac_review_items() -> Tuple[str, str]:
    return (
        "VarAC guided setup is read/import only: FIO remembers paths and monitors VarAC data without rewriting VarAC.ini or VarAC DB.",
        "Use the dedicated VarAC BBS settings workflow for explicit [BBS] section sync; cluster membership remains read-only in this release.",
    )


def _add_varac_integration_action(
    actions: list[GuidedAppConfigAction],
    proposals: Sequence[RadioInstanceProposal],
    paths: Mapping[str, str],
) -> None:
    instance_names = ", ".join(proposal.instance_name for proposal in proposals if proposal.varac_enabled) or "selected radio"
    details = {
        "install_path": str(paths.get("varac_install_path") or paths.get("varac") or ""),
        "ini_path": str(paths.get("varac_ini_path") or ""),
        "db_path": str(paths.get("varac_db_path") or ""),
        "incoming_dir": str(paths.get("varac_incoming_dir") or paths.get("varac_inbox_dir") or ""),
        "outgoing_dir": str(paths.get("varac_outgoing_dir") or paths.get("varac_outbox_dir") or ""),
        "bbs_dir": str(paths.get("varac_bbs_dir") or ""),
        "bbs_archive_dir": str(paths.get("varac_bbs_archive_dir") or ""),
        "launch_cmd": str(paths.get("varac_launch_cmd") or paths.get("launch_cmd") or ""),
    }
    varac_assets = discover_varac_local_assets(app_paths=paths)
    for asset in varac_assets:
        if asset.asset_id in {
            "traffic_log",
            "app_log",
            "qso_log",
            "callsign_tags",
            "alert_tags",
            "templates",
            "bbs_archive",
        } and asset.path:
            details[f"{asset.asset_id}_path"] = asset.path
        if asset.asset_id == "ini" and asset.detail:
            details["ini_detail"] = asset.detail
        if asset.asset_id == "db" and asset.detail:
            details["db_detail"] = asset.detail
    found_labels = tuple(asset.label for asset in varac_assets if asset.exists)
    if found_labels:
        details["readable_assets"] = ", ".join(found_labels)
    target = details["install_path"] or details["ini_path"] or details["db_path"] or "FIO VarAC integration settings"
    actions.append(
        GuidedAppConfigAction(
            action_id="varac:remember-integration",
            app_id="varac",
            instance_name=instance_names,
            action_type="remember_integration",
            target=target,
            summary="Remember VarAC paths and enable read/import integration in FIO without changing VarAC configuration.",
            requires_backup=False,
            writes_external_config=False,
            manual_review_required=True,
            details=details,
            notes=(
                "FIO may read VarAC.ini, the VarAC database, logs, and message/BBS folders for discovery and ingest.",
                "Guided setup does not write VarAC.ini or the VarAC database.",
                "VarAC owns scheduler/frequency control for VarAC-only radios.",
            ),
        )
    )


def _add_directory_action(
    actions: list[GuidedAppConfigAction],
    seen_dirs: set[str],
    *,
    app_id: str,
    instance_name: str,
    directory: Path,
) -> None:
    key = str(Path(directory))
    if key in seen_dirs:
        return
    seen_dirs.add(key)
    actions.append(
        GuidedAppConfigAction(
            action_id=f"{instance_name}:{app_id}:create-dir:{len(seen_dirs)}",
            app_id=app_id,
            instance_name=instance_name,
            action_type="create_directory",
            target=key,
            summary=f"Create FIO-managed {APP_DISPLAY_NAMES.get(app_id, app_id)} folder for {instance_name}.",
            requires_backup=False,
            writes_external_config=False,
        )
    )
