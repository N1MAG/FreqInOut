from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping, Sequence, Tuple

from freqinout.core.config_autodiscovery import APP_DISPLAY_NAMES, RadioInstanceProposal
from freqinout.core.config_js8_managed import build_js8call_managed_profile_plans
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


def build_guided_external_app_config_plan(
    proposals: Sequence[RadioInstanceProposal],
    *,
    config_root: Path,
    app_paths: Mapping[str, str] | None = None,
    callsign: str = "",
    grid: str = "",
    include_varac: bool = False,
) -> GuidedAppConfigPlan:
    """Return a reviewable external-app setup plan without writing files."""

    paths = dict(app_paths or {})
    actions: list[GuidedAppConfigAction] = []
    review_items: list[str] = []
    seen_dirs: set[str] = set()

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
        actions.append(
            GuidedAppConfigAction(
                action_id=f"{plan.instance_name}:js8call:update-multisettings",
                app_id="js8call",
                instance_name=plan.instance_name,
                action_type="update_js8_multisettings",
                target=str(plan.config_dir),
                summary=(
                    f"Prepare JS8Call profile {plan.profile_name} with FLRig "
                    f"{plan.flrig_host}:{plan.flrig_port} and API port {plan.tcp_port}."
                ),
                requires_backup=True,
                writes_external_config=True,
                details={
                    "executable_path": plan.executable_path,
                    "profile_name": plan.profile_name,
                    "config_dir": str(plan.config_dir),
                    "save_dir": str(plan.save_dir),
                    "directed_path": str(plan.directed_path),
                    "flrig_port": str(plan.flrig_port),
                    "tcp_port": str(plan.tcp_port),
                    "udp_port": str(plan.udp_port),
                },
                notes=(
                    "JS8Call MultiSettings writes require backup of the existing JS8Call.ini before apply.",
                    "Some JS8Call builds may still require operator confirmation in JS8Call after profile creation.",
                ),
            )
        )

    selected_varac = include_varac or any(proposal.varac_enabled for proposal in proposals)
    if selected_varac:
        review_items.append(
            "VarAC app paths may be remembered, but VarAC database, BBS, and cluster membership require separate review."
        )

    return GuidedAppConfigPlan(actions=tuple(actions), review_items=tuple(review_items))


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
