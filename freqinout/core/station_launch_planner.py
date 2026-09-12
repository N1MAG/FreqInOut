from __future__ import annotations

import hashlib
import json
from pathlib import PurePath
import shlex
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from freqinout.core.launch_bundle_store import normalize_launch_items
from freqinout.core.js8_storage import (
    expected_storage_mode,
    normalize_rig_name,
    resolve_js8_storage,
    rig_name_collision_key,
    stable_managed_rig_name,
)
from freqinout.core.multi_instance_review import (
    blocking_issue_message,
    validate_multi_instance_launch_records,
)


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
    launch_arguments: Tuple[str, ...] = ()
    rig_name: str = ""
    rig_name_source: str = ""
    application_data_root: str = ""
    storage_mode: str = "unverified"
    expected_storage_mode: str = "unverified"
    effective_command: Tuple[str, ...] = ()
    dependencies: Tuple[str, ...] = ()
    readiness_policy: Tuple[Tuple[str, Any], ...] = ()
    configuration_paths: Tuple[Tuple[str, str], ...] = ()

    def as_queue_item(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "instance_key": self.instance_key,
            "instance_identity": self.instance_identity,
            "radio_ids": list(self.radio_ids),
            "radio_names": list(self.radio_names),
            "launch_path_override": self.launch_path_override,
            "launch_command_override": self.launch_command_override,
            "launch_arguments": list(self.launch_arguments),
            "rig_name": self.rig_name,
            "rig_name_source": self.rig_name_source,
            "application_data_root": self.application_data_root,
            "storage_mode": self.storage_mode,
            "expected_storage_mode": self.expected_storage_mode,
            "effective_command": list(self.effective_command),
            "dependencies": list(self.dependencies),
            "readiness_policy": dict(self.readiness_policy),
            "configuration_paths": dict(self.configuration_paths),
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
                js8_values = self._js8_launch_values(profile, item) if name == "JS8Call" else {}
                configured_arguments = readiness.pop("launch_arguments", ())
                if not isinstance(configured_arguments, (list, tuple)):
                    configured_arguments = ()
                instance = PlannedInstance(
                    name=name,
                    instance_key=str(item["instance_key"]),
                    instance_identity=identity,
                    radio_ids=(radio_id,),
                    radio_names=(str(profile.get("name", radio_id) or radio_id),),
                    launch_path_override=str(item["launch_path_override"]),
                    launch_command_override=str(item["launch_command_override"]),
                    launch_arguments=tuple(
                        js8_values.get("launch_arguments", ())
                        if name == "JS8Call"
                        else (str(value) for value in configured_arguments)
                    ),
                    rig_name=str(js8_values.get("rig_name", "")),
                    rig_name_source=str(js8_values.get("rig_name_source", "")),
                    application_data_root=str(js8_values.get("application_data_root", "")),
                    storage_mode=str(js8_values.get("storage_mode", "unverified")),
                    expected_storage_mode=str(js8_values.get("expected_storage_mode", "unverified")),
                    dependencies=dependencies,
                    readiness_policy=tuple(sorted(readiness.items())),
                    configuration_paths=self._configuration_paths(name, profile),
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
                launch_arguments=existing.launch_arguments,
                rig_name=existing.rig_name,
                rig_name_source=existing.rig_name_source,
                application_data_root=existing.application_data_root,
                storage_mode=existing.storage_mode,
                expected_storage_mode=existing.expected_storage_mode,
                effective_command=existing.effective_command,
                dependencies=existing.dependencies,
                readiness_policy=existing.readiness_policy,
                configuration_paths=existing.configuration_paths,
            )
            deduped[instance.instance_identity] = (prior[0], prior[1], merged)
        ordered = self._dependency_order(list(deduped.values()))
        instances = tuple(value[2] for value in ordered)
        self._validate_js8_launch_collisions(instances)
        issues = validate_multi_instance_launch_records([instance.as_queue_item() for instance in instances])
        if blocking_issue_message(issues):
            raise ValueError(blocking_issue_message(issues))
        return LaunchPlan(trigger=trigger, scope_radio_id=scope_radio_id, instances=instances)

    @staticmethod
    def _configuration_paths(name: str, profile: Mapping[str, Any]) -> Tuple[Tuple[str, str], ...]:
        """Expose only launch-relevant native resources to pure preflight checks."""

        if name != "VarAC":
            return ()
        values = {
            "ini_path": profile.get("varac_ini_path", ""),
            "db_path": profile.get("varac_db_path", ""),
            "incoming_path": profile.get("varac_incoming_path", ""),
            "outbox_dir": profile.get("varac_outbox_dir", ""),
        }
        return tuple(
            (key, str(value or "").strip())
            for key, value in values.items()
            if str(value or "").strip()
        )

    @staticmethod
    def _js8_launch_values(profile: Mapping[str, Any], item: Mapping[str, Any]) -> Dict[str, Any]:
        command = str(item.get("launch_command_override", "") or "").strip()
        rig_name = ""
        rig_source = ""
        launch_arguments: Tuple[str, ...] = ()
        if command:
            rig_name = StationLaunchPlanner._rig_name_from_command(command)
            if rig_name:
                rig_source = "command_override"
        if not rig_name:
            system_key = str(profile.get("js8_instance_system_key", "") or "").strip()
            instance_name = str(profile.get("js8_instance_name", "") or "").strip()
            if not system_key and not instance_name:
                raise ValueError(
                    "JS8Call requires a persisted instance system key or name to generate a stable rig name."
                )
            rig_name = stable_managed_rig_name(system_key=system_key, name=instance_name)
            rig_source = "managed"
            launch_arguments = ("--rig-name", rig_name)
        storage_values = {
            **dict(profile),
            "rig_name": rig_name,
            "rig_name_source": rig_source,
        }
        stored_rig = str(profile.get("js8_rig_name", profile.get("rig_name", "")) or "").strip()
        if stored_rig and rig_name_collision_key(stored_rig) != rig_name_collision_key(rig_name):
            # A verified root belongs to the rig identity that produced it.
            # Changing --rig-name must derive a new Qt namespace instead of
            # carrying the prior rig's root forward.
            storage_values["application_data_root"] = ""
            storage_values["js8_message_storage_root"] = ""
            storage_values["storage_evidence"] = ""
            storage_values["js8_storage_evidence"] = ""
            storage_values["storage_mode"] = "unverified"
            storage_values["js8_storage_mode"] = "unverified"
        if (
            expected_storage_mode(
                storage_values.get("js8_variant_family", storage_values.get("variant_family", "unknown")),
                storage_values.get("js8_variant_version", storage_values.get("variant_version", "")),
            )
            == "unverified"
            and StationLaunchPlanner._launch_target_looks_subspace(item)
        ):
            # A Subspace-specific launch target identifies the reviewed family.
            # Like 2.2.0 and Improved 3.0.3 it receives a distinct --rig-name
            # namespace and rig-scoped storage candidate.
            storage_values["variant_family"] = "js8call_subspace_4_1"
            storage_values["variant_version"] = ""
        storage = resolve_js8_storage(storage_values, probe_existing=False)
        return {
            "launch_arguments": launch_arguments,
            "rig_name": rig_name,
            "rig_name_source": rig_source,
            "application_data_root": storage.data_root,
            "storage_mode": storage.storage_mode,
            "expected_storage_mode": storage.expected_mode,
        }

    @staticmethod
    def _rig_name_from_command(command: str) -> str:
        try:
            tokens = shlex.split(command)
        except ValueError as exc:
            raise ValueError(f"Invalid JS8Call launch command: {exc}") from exc
        values: List[str] = []
        index = 0
        while index < len(tokens):
            token = tokens[index]
            value = ""
            if token in {"-r", "--rig-name"}:
                if index + 1 >= len(tokens) or tokens[index + 1].startswith("-"):
                    raise ValueError("JS8Call --rig-name requires a non-empty value.")
                value = tokens[index + 1]
                index += 1
            elif token.startswith("--rig-name="):
                value = token.split("=", 1)[1]
            elif token.startswith("-r="):
                value = token.split("=", 1)[1]
            if token in {"--rig-name=", "-r="}:
                raise ValueError("JS8Call --rig-name requires a non-empty value.")
            if value:
                values.append(value)
            index += 1
        if len(values) > 1:
            raise ValueError("JS8Call launch command contains duplicate or conflicting rig-name options.")
        if not values:
            return ""
        try:
            return normalize_rig_name(values[0])
        except ValueError as exc:
            raise ValueError(f"Invalid JS8Call rig name: {exc}") from exc

    @staticmethod
    def _validate_js8_launch_collisions(instances: Sequence[PlannedInstance]) -> None:
        js8_instances = [
            instance
            for instance in instances
            if instance.name == "JS8Call" and StationLaunchPlanner._is_local_js8_instance(instance)
        ]
        rig_names: Dict[str, PlannedInstance] = {}
        roots: Dict[str, PlannedInstance] = {}
        for instance in js8_instances:
            rig_key = rig_name_collision_key(instance.rig_name)
            prior_rig = rig_names.get(rig_key)
            if prior_rig is not None:
                raise ValueError(
                    "JS8Call local profiles use the same effective rig name "
                    f"'{instance.rig_name}': {', '.join((*prior_rig.radio_names, *instance.radio_names))}."
                )
            rig_names[rig_key] = instance
            root = str(instance.application_data_root or "").strip()
            if not root or instance.expected_storage_mode != "rig_scoped":
                continue
            root_key = root.casefold()
            prior_root = roots.get(root_key)
            if prior_root is not None:
                raise ValueError(
                    "JS8Call local profiles resolve to the same message-storage root "
                    f"'{root}': {', '.join((*prior_root.radio_names, *instance.radio_names))}."
                )
            roots[root_key] = instance

    @staticmethod
    def _launch_target_looks_subspace(item: Mapping[str, Any]) -> bool:
        values = [str(item.get("launch_path_override", "") or "")]
        try:
            values.extend(shlex.split(str(item.get("launch_command_override", "") or "")))
        except ValueError:
            values.append(str(item.get("launch_command_override", "") or ""))
        for raw in values:
            normalized = str(raw or "").strip().replace("\\", "/")
            if not normalized:
                continue
            parts = {part.casefold() for part in PurePath(normalized).parts}
            basename = PurePath(normalized).name.casefold()
            if basename in {"js8call-subspace", "js8call-subspace.exe", "subspace", "subspace.exe"}:
                return True
            if parts.intersection({"subspace-edition", "js8call-subspace", "js8call subspace"}):
                return True
        return False

    @staticmethod
    def _is_local_js8_instance(instance: PlannedInstance) -> bool:
        readiness = dict(instance.readiness_policy)
        host = str(readiness.get("host", "127.0.0.1") or "127.0.0.1").strip().casefold()
        return host in {"127.0.0.1", "localhost", "::1"}

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
