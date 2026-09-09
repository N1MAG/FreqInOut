from __future__ import annotations

import hashlib
import json
import shlex
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from freqinout.core.launch_bundle_store import normalize_launch_items


DEFAULT_DEPENDENCIES = {
    "FLDigi": ("FLRig",),
    "FLAmp": ("FLDigi",),
    "FLMsg": ("FLDigi",),
    "JS8Spotter": ("JS8Call",),
    "CommStat": ("JS8Call",),
}


def _truthy(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


@dataclass(frozen=True)
class PlannedInstance:
    name: str
    instance_key: str
    instance_identity: str
    radio_ids: Tuple[int, ...]
    radio_names: Tuple[str, ...]
    launch_path_override: str = ""
    launch_command_override: str = ""
    dependencies: Tuple[str, ...] = ()
    readiness_policy: Tuple[Tuple[str, Any], ...] = ()

    def as_queue_item(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "instance_key": self.instance_key,
            "instance_identity": self.instance_identity,
            "radio_ids": list(self.radio_ids),
            "radio_names": list(self.radio_names),
            "launch_path_override": self.launch_path_override,
            "launch_command_override": self.launch_command_override,
            "dependencies": list(self.dependencies),
            "readiness_policy": dict(self.readiness_policy),
        }


@dataclass(frozen=True)
class LaunchPlan:
    trigger: str
    scope_radio_id: Optional[int]
    instances: Tuple[PlannedInstance, ...]

    def queue(self) -> List[Dict[str, Any]]:
        return [instance.as_queue_item() for instance in self.instances]


class StationLaunchPlanner:
    """Pure planner shared by preview, startup, and selected-radio manual launch."""

    def plan_startup(
        self,
        profiles: Sequence[Mapping[str, Any]],
        bundles: Mapping[int, Mapping[str, Any]],
        *,
        scope_radio_id: Optional[int] = None,
        trigger: str = "startup",
    ) -> LaunchPlan:
        candidates: List[Tuple[int, int, PlannedInstance]] = []
        ordered_profiles = sorted(
            (profile for profile in profiles if isinstance(profile, Mapping)),
            key=lambda row: (int(row.get("display_order", 0) or 0), int(row.get("id", 0) or 0)),
        )
        for profile in ordered_profiles:
            radio_id = int(profile.get("id", 0) or 0)
            if radio_id <= 0 or (scope_radio_id is not None and radio_id != int(scope_radio_id)):
                continue
            if int(profile.get("runtime_active", 0) or 0) != 1:
                continue
            bundle = bundles.get(radio_id, {})
            if not _truthy(bundle.get("launch_enabled", False)):
                continue
            for order, raw_item in enumerate(normalize_launch_items(bundle.get("items", []))):
                item = self._with_profile_overrides(profile, raw_item)
                if not item["enabled"] or not item["startup"]:
                    continue
                name = str(item["name"])
                dependencies = tuple(item["dependencies"] or DEFAULT_DEPENDENCIES.get(name, ()))
                identity = self._identity(profile, item)
                readiness = dict(item["readiness_policy"])
                if name == "JS8Call":
                    readiness.setdefault("host", str(profile.get("js8_host", "127.0.0.1") or "127.0.0.1"))
                    readiness.setdefault("port", int(profile.get("js8_port", 2442) or 2442))
                    readiness.setdefault("require_api", True)
                elif name == "FLRig":
                    readiness.setdefault("host", str(profile.get("flrig_host", "127.0.0.1") or "127.0.0.1"))
                    readiness.setdefault("port", int(profile.get("flrig_port", 12345) or 12345))
                    readiness.setdefault("require_service", True)
                elif name == "FLDigi":
                    readiness.setdefault("host", str(profile.get("fldigi_host", "127.0.0.1") or "127.0.0.1"))
                    readiness.setdefault("port", int(profile.get("fldigi_port", 7362) or 7362))
                    readiness.setdefault("require_service", True)
                instance = PlannedInstance(
                    name=name,
                    instance_key=str(item["instance_key"]),
                    instance_identity=identity,
                    radio_ids=(radio_id,),
                    radio_names=(str(profile.get("name", radio_id) or radio_id),),
                    launch_path_override=str(item["launch_path_override"]),
                    launch_command_override=str(item["launch_command_override"]),
                    dependencies=dependencies,
                    readiness_policy=tuple(sorted(readiness.items())),
                )
                candidates.append((int(profile.get("display_order", 0) or 0), order, instance))
        deduped: Dict[str, Tuple[int, int, PlannedInstance]] = {}
        for profile_order, item_order, instance in candidates:
            prior = deduped.get(instance.instance_identity)
            if prior is None:
                deduped[instance.instance_identity] = (profile_order, item_order, instance)
                continue
            existing = prior[2]
            merged = PlannedInstance(
                name=existing.name,
                instance_key=existing.instance_key,
                instance_identity=existing.instance_identity,
                radio_ids=tuple(dict.fromkeys((*existing.radio_ids, *instance.radio_ids))),
                radio_names=tuple(dict.fromkeys((*existing.radio_names, *instance.radio_names))),
                launch_path_override=existing.launch_path_override,
                launch_command_override=existing.launch_command_override,
                dependencies=existing.dependencies,
                readiness_policy=existing.readiness_policy,
            )
            deduped[instance.instance_identity] = (prior[0], prior[1], merged)
        ordered = self._dependency_order(list(deduped.values()))
        return LaunchPlan(trigger=trigger, scope_radio_id=scope_radio_id, instances=tuple(value[2] for value in ordered))

    @staticmethod
    def _with_profile_overrides(profile: Mapping[str, Any], item: Mapping[str, Any]) -> Dict[str, Any]:
        effective = dict(item)
        name = str(effective.get("name", "") or "")
        if not str(effective.get("launch_command_override", "") or "").strip() and name == "VarAC":
            effective["launch_command_override"] = str(profile.get("launch_cmd", "") or "").strip()
        if not str(effective.get("launch_path_override", "") or "").strip():
            path_key = {
                "FLRig": "flrig_path",
                "FLDigi": "fldigi_path",
                "FLAmp": "flamp_path",
                "FLMsg": "flmsg_path",
                "VarAC": "varac_install_path",
                "JS8Call": "js8_install_path",
                "JS8Spotter": "spotter_launch_path",
                "CommStat": "commstat_launch_path",
            }.get(name)
            if path_key:
                effective["launch_path_override"] = str(profile.get(path_key, "") or "").strip()
        return effective

    @staticmethod
    def _identity(profile: Mapping[str, Any], item: Mapping[str, Any]) -> str:
        command = str(item.get("launch_command_override", "") or "").strip()
        path = str(item.get("launch_path_override", "") or "").strip()
        try:
            normalized_command = shlex.join(shlex.split(command)) if command else ""
        except ValueError:
            normalized_command = command
        name = str(item.get("name", "") or "").strip()
        endpoint: Dict[str, Any] = {}
        if name == "JS8Call":
            endpoint = {
                "host": str(profile.get("js8_host", "127.0.0.1") or "127.0.0.1"),
                "port": int(profile.get("js8_port", 2442) or 2442),
                "profile": str(profile.get("js8_profile_path", "") or ""),
            }
        elif name == "FLRig":
            endpoint = {"host": profile.get("flrig_host"), "port": profile.get("flrig_port")}
        elif name == "FLDigi":
            endpoint = {"host": profile.get("fldigi_host"), "port": profile.get("fldigi_port")}
        elif name == "VarAC":
            endpoint = {"ini": profile.get("varac_ini_path"), "node": profile.get("varac_node_id")}
        payload = {"name": name.casefold(), "command": normalized_command, "path": path, "endpoint": endpoint}
        return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()[:24]

    @staticmethod
    def _dependency_order(
        values: List[Tuple[int, int, PlannedInstance]],
    ) -> List[Tuple[int, int, PlannedInstance]]:
        remaining = sorted(values, key=lambda value: (value[0], value[1], value[2].name.casefold()))
        output: List[Tuple[int, int, PlannedInstance]] = []
        emitted_names: set[str] = set()
        available_names = {value[2].name for value in remaining}
        while remaining:
            ready_index = next(
                (
                    index
                    for index, value in enumerate(remaining)
                    if all(dep in emitted_names or dep not in available_names for dep in value[2].dependencies)
                ),
                None,
            )
            if ready_index is None:
                cycle_names = ", ".join(value[2].name for value in remaining)
                raise ValueError(f"Launch dependency cycle: {cycle_names}")
            value = remaining.pop(ready_index)
            output.append(value)
            emitted_names.add(value[2].name)
        return output
