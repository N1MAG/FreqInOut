from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence, Tuple

from freqinout.core.config_autodiscovery import (
    LOCALHOST,
    RadioInstanceProposal,
    build_lab_radio_proposals,
)
from freqinout.core.config_js8_managed import (
    build_js8call_managed_profile_plans,
    create_js8call_managed_directories,
)
from freqinout.core.config_managed_profiles import (
    build_flrig_fldigi_managed_profile_plans,
    create_managed_profile_directories,
)
from freqinout.core.multi_radio_store import (
    DEFAULT_DEVICE_SYSTEM_KEY,
    DEFAULT_FAST_LIGHT_SYSTEM_KEY,
    DEFAULT_JS8_INSTANCE_SYSTEM_KEY,
    MultiRadioStore,
    ensure_multi_rig_migration,
)


LAB_OPERATING_PLAN_NAME = "Lab All Features"
LAB_CREATED_BY = "config_lab_preset"
_SYSTEM_KEY_TABLES = frozenset({"device_profiles", "js8_instances", "fast_light_configs"})


@dataclass(frozen=True)
class LabRadioApplyResult:
    radio_profile_ids: Tuple[int, ...]
    radio_names: Tuple[str, ...]
    summary: str
    warnings: Tuple[str, ...] = ()
    managed_paths: Tuple[Path, ...] = ()


def build_lab_radio_profile_values(
    proposal: RadioInstanceProposal,
    *,
    app_paths: Mapping[str, str] | None = None,
    config_root: Path | None = None,
    existing_device_id: Optional[int] = None,
    js8_instance_id: Optional[int] = None,
    fast_light_config_id: Optional[int] = None,
) -> Mapping[str, Any]:
    paths = dict(app_paths or {})
    ports = _ports_by_service(proposal)
    instance_root = Path(config_root) / "managed-instances" / proposal.instance_name if config_root else None
    display_order = (proposal.index + 1) * 10
    values: dict[str, Any] = {
        "system_key": f"lab_radio_{_radio_suffix(proposal.index)}",
        "name": proposal.name,
        "radio_manufacturer": "RadioTools",
        "radio_model": f"Emulated Profile {_radio_suffix(proposal.index).upper()}",
        "enabled": 1,
        "runtime_active": 1,
        "runtime_primary": 1 if proposal.index == 0 else 0,
        "display_order": display_order,
        "device_class": "tx_rx",
        "deployment_mode": "full",
        "control_backend": "flrig",
        "use_flrig": 1 if "flrig" in proposal.enabled_apps else 0,
        "use_fldigi": 1 if "fldigi" in proposal.enabled_apps else 0,
        "use_js8call": 1 if "js8call" in proposal.enabled_apps else 0,
        "use_flmsg": 0,
        "use_flamp": 0,
        "use_varac": 0,
        "use_js8spotter": 0,
        "use_commstat": 0,
        "flrig_host": LOCALHOST,
        "flrig_port": ports.get("flrig", 12345),
        "fldigi_host": LOCALHOST,
        "fldigi_port": ports.get("fldigi", 7362),
        "js8_host": LOCALHOST,
        "js8_port": ports.get("js8call", 2442),
        "flrig_path": paths.get("flrig", ""),
        "fldigi_path": paths.get("fldigi", ""),
        "js8_install_path": paths.get("js8call", ""),
        "launch_enabled": 0,
        "launch_path": paths.get("flrig", ""),
        "ptt_group": f"LAB-{_radio_suffix(proposal.index).upper()}",
    }
    if existing_device_id is not None:
        values["id"] = int(existing_device_id)
    if js8_instance_id is not None:
        values["js8_instance_id"] = int(js8_instance_id)
    if fast_light_config_id is not None:
        values["fast_light_config_id"] = int(fast_light_config_id)
    if instance_root is not None:
        values.update(
            {
                "fldigi_log_path": str(instance_root / "fldigi" / "logs"),
                "fldigi_checkin_dir": str(instance_root / "fldigi" / "checkins"),
                "js8_directed_path": str(instance_root / "js8call" / "DIRECTED.TXT"),
                "js8_forms_path": str(instance_root / "js8call" / "forms"),
            }
        )
    return values


def apply_lab_radio_preset_to_store(
    store: MultiRadioStore,
    *,
    radio_count: int = 3,
    app_paths: Mapping[str, str] | None = None,
    config_root: Path | None = None,
    busy_checker: Callable[[str, int], bool] | None = None,
) -> LabRadioApplyResult:
    proposals = build_lab_radio_proposals(
        radio_count=radio_count,
        enabled_apps=("flrig", "fldigi", "js8call"),
        include_varac=False,
        busy_checker=busy_checker or (lambda _host, _port: False),
    )
    if not proposals:
        return LabRadioApplyResult(radio_profile_ids=(), radio_names=(), summary="No lab radios were requested.")

    managed_paths = _prepare_lab_managed_paths(
        proposals,
        config_root=config_root,
        app_paths=app_paths,
    )

    with store.connect() as conn:
        ensure_multi_rig_migration(
            conn,
            {},
            radio_name=proposals[0].name,
            radio_manufacturer="RadioTools",
            radio_model="Emulated Profile A",
            operating_plan_name=LAB_OPERATING_PLAN_NAME,
            enabled_software_roles=("fast_light", "js8call"),
        )

    saved_ids: list[int] = []
    saved_names: list[str] = []
    for proposal in proposals:
        ports = _ports_by_service(proposal)
        suffix = _radio_suffix(proposal.index)
        instance_key = _system_key_part(proposal.instance_name)
        existing_device_id = _existing_id_by_system_key(store, "device_profiles", f"lab_radio_{suffix}")
        if existing_device_id is None and proposal.index == 0:
            existing_device_id = _existing_id_by_system_key(store, "device_profiles", DEFAULT_DEVICE_SYSTEM_KEY)
        existing_js8_id = _existing_id_by_system_key(store, "js8_instances", f"lab_js8_{instance_key}")
        if existing_js8_id is None and proposal.index == 0:
            existing_js8_id = _existing_id_by_system_key(store, "js8_instances", DEFAULT_JS8_INSTANCE_SYSTEM_KEY)
        existing_fast_light_id = _existing_id_by_system_key(store, "fast_light_configs", f"lab_fast_light_{instance_key}")
        if existing_fast_light_id is None and proposal.index == 0:
            existing_fast_light_id = _existing_id_by_system_key(store, "fast_light_configs", DEFAULT_FAST_LIGHT_SYSTEM_KEY)
        js8 = store.save_js8_instance(
            {
                "id": existing_js8_id,
                "system_key": f"lab_js8_{instance_key}",
                "name": f"{proposal.name} JS8Call",
                "host": LOCALHOST,
                "port": ports.get("js8call", 2442),
                "profile_path": _managed_path(config_root, proposal.instance_name, "js8call"),
                "directed_path": _managed_path(config_root, proposal.instance_name, "js8call", "DIRECTED.TXT"),
                "inbox_path": _managed_path(config_root, proposal.instance_name, "js8call", "inbox"),
                "forms_path": _managed_path(config_root, proposal.instance_name, "js8call", "forms"),
                "install_path": (app_paths or {}).get("js8call", ""),
            }
        )
        fast_light = store.save_fast_light_config(
            {
                "id": existing_fast_light_id,
                "system_key": f"lab_fast_light_{instance_key}",
                "name": f"{proposal.name} Fast Light",
                "flrig_path": (app_paths or {}).get("flrig", ""),
                "flrig_host": LOCALHOST,
                "flrig_port": ports.get("flrig", 12345),
                "fldigi_path": (app_paths or {}).get("fldigi", ""),
                "fldigi_host": LOCALHOST,
                "fldigi_port": ports.get("fldigi", 7362),
                "fldigi_log_path": _managed_path(config_root, proposal.instance_name, "fldigi", "logs"),
                "fldigi_checkin_dir": _managed_path(config_root, proposal.instance_name, "fldigi", "checkins"),
            }
        )
        saved = store.save_device_profile(
            build_lab_radio_profile_values(
                proposal,
                app_paths=app_paths,
                config_root=config_root,
                existing_device_id=existing_device_id,
                js8_instance_id=int(js8["id"]),
                fast_light_config_id=int(fast_light["id"]),
            )
        )
        device_id = int(saved["id"])
        store.restore_default_operating_profile(
            device_id,
            reason="Assigned by the multi-rig lab preset.",
            created_by=LAB_CREATED_BY,
        )
        store.set_device_profile_runtime_active(device_id, True)
        saved_ids.append(device_id)
        saved_names.append(str(saved.get("name", proposal.name)))

    if saved_ids:
        store.set_runtime_primary_device_profile(saved_ids[0])
        for device_id in saved_ids[1:]:
            store.set_device_profile_runtime_active(device_id, True)

    return LabRadioApplyResult(
        radio_profile_ids=tuple(saved_ids),
        radio_names=tuple(saved_names),
        summary=f"Created or updated {len(saved_ids)} lab radio profile(s).",
        managed_paths=managed_paths,
    )


def _existing_id_by_system_key(store: MultiRadioStore, table_name: str, system_key: str) -> Optional[int]:
    if not system_key:
        return None
    if table_name not in _SYSTEM_KEY_TABLES:
        raise ValueError(f"Unsupported lab preset lookup table: {table_name}")
    with store.connect() as conn:
        row = conn.execute(f"SELECT id FROM {table_name} WHERE system_key=? LIMIT 1", (system_key,)).fetchone()
        return int(row[0]) if row is not None else None


def _prepare_lab_managed_paths(
    proposals: Sequence[RadioInstanceProposal],
    *,
    config_root: Path | None,
    app_paths: Mapping[str, str] | None,
) -> Tuple[Path, ...]:
    if config_root is None:
        return ()
    prepared = []
    seen = set()
    for proposal in proposals:
        for path in create_managed_profile_directories(
            build_flrig_fldigi_managed_profile_plans(
                proposal,
                config_root=config_root,
                app_paths=app_paths,
            )
        ):
            key = str(path)
            if key not in seen:
                seen.add(key)
                prepared.append(path)
    for path in create_js8call_managed_directories(
        build_js8call_managed_profile_plans(
            proposals,
            config_root=config_root,
            js8call_path=(app_paths or {}).get("js8call", ""),
        )
    ):
        key = str(path)
        if key not in seen:
            seen.add(key)
            prepared.append(path)
    return tuple(prepared)


def _ports_by_service(proposal: RadioInstanceProposal) -> Mapping[str, int]:
    return {assignment.service: int(assignment.assigned_port) for assignment in proposal.ports}


def _managed_path(config_root: Path | None, instance_name: str, *parts: str) -> str:
    if config_root is None:
        return ""
    return str(Path(config_root) / "managed-instances" / instance_name / Path(*parts))


def _radio_suffix(index: int) -> str:
    if 0 <= int(index) < 26:
        return chr(ord("a") + int(index))
    return str(int(index) + 1)


def _system_key_part(value: str) -> str:
    return "_".join(part for part in str(value or "").strip().lower().replace("-", "_").split("_") if part)
