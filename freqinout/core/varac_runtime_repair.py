"""Automatic bounded repair for obsolete FIO-managed Wine VARA runtimes."""

from __future__ import annotations

import sys
from dataclasses import replace
from pathlib import Path
from typing import Any, Mapping

from freqinout.core.multi_radio_store import MultiRadioStore
from freqinout.core.software_identity_bundle import SoftwareIdentityComponent
from freqinout.core.varac_native_preparation import (
    VarACManagedRuntimeRepair,
    prepare_managed_varac_runtime_repair,
)
from freqinout.core.varac_native_transaction import apply_varac_native_transaction


def _platform_key() -> str:
    return "linux-wine" if sys.platform.startswith("linux") else "windows" if sys.platform.startswith("win") else sys.platform


def _replace_resource(rows: Any, runtime: Path, ini: Path, *, identity: bool = False) -> list[dict[str, Any]]:
    updated: list[dict[str, Any]] = []
    for raw in rows or ():
        if not isinstance(raw, Mapping):
            continue
        row = dict(raw)
        kind = str(row.get("resource_type" if identity else "kind") or "").strip().casefold()
        if kind == "vara_runtime":
            row["value"] = str(runtime)
        elif kind == "vara_ini":
            row["value"] = str(ini)
        updated.append(row)
    return updated


def _vara_recipe_component(raw: Mapping[str, Any], runtime: Path, wine_prefix: str) -> dict[str, Any]:
    executable = runtime / "VARA.exe"
    component = dict(raw)
    component.update(
        {
            "component_key": "vara",
            "executable": "wine",
            "arguments": [str(executable)],
            "effective_command": ["wine", str(executable)],
            "working_directory": str(runtime),
            "managed_directories": [str(runtime)],
            "environment": {"WINEPREFIX": wine_prefix},
            "dependencies": [],
            "execution_scope": "standard",
            "operator_starts": False,
        }
    )
    component.setdefault("readiness", {"kind": "process"})
    return component


def _persist_runtime_projection(
    store: MultiRadioStore,
    repair: VarACManagedRuntimeRepair,
    *,
    observed: Mapping[str, Any],
) -> Mapping[str, Any]:
    runtime = Path(repair.target_runtime or "")
    vara_ini = Path(repair.target_ini or runtime / "VARA.ini")
    node = store.get_varac_node(repair.node_id)
    if not isinstance(node, Mapping):
        raise KeyError(f"Unknown VarAC node id: {repair.node_id}")
    ini_path = Path(str(node.get("ini_path") or "")).expanduser().absolute()
    parts = ini_path.parts
    drive_index = next(
        (index for index, part in enumerate(parts) if part.casefold().startswith("drive_") and len(part) == 7),
        -1,
    )
    wine_prefix = str(Path(*parts[:drive_index])) if drive_index >= 0 else ""
    writer_key = str(node.get("native_writer_key") or "")
    if repair.plan is not None:
        writer_key = f"varac:{repair.plan.version}:{repair.plan.platform}:{repair.plan.operation}"
    saved_node = store.save_varac_node(
        {
            **dict(node),
            "vara_runtime_path": str(runtime),
            "vara_ini_path": str(vara_ini),
            "native_writer_key": writer_key,
            "desired_fingerprint": (
                repair.plan.plan_fingerprint if repair.plan is not None
                else str(node.get("desired_fingerprint") or "")
            ),
            "observed_fingerprint": str(
                observed.get("observed_fingerprint")
                or node.get("observed_fingerprint")
                or ""
            ),
            "native_verification_summary": (
                "Managed VARA runtime verified inside the VarAC Wine drive; FIO projections synchronized."
            ),
        }
    )

    matching_manifests = [
        row for row in store.list_software_instance_manifests()
        if isinstance(row, Mapping)
        and str(row.get("family_key") or "") == "varac"
        and str(row.get("application_system_key") or "") == str(node.get("system_key") or "")
    ]
    for manifest in matching_manifests:
        evidence = dict(manifest.get("evidence") or {})
        recipe = dict(evidence.get("launch_recipe") or {})
        components = []
        found_vara = False
        for raw in recipe.get("components", ()) or ():
            if not isinstance(raw, Mapping):
                continue
            if str(raw.get("component_key") or "").strip().casefold() == "vara":
                components.append(_vara_recipe_component(raw, runtime, wine_prefix))
                found_vara = True
            else:
                components.append(dict(raw))
        if found_vara:
            recipe["components"] = components
            if repair.plan is not None:
                recipe["fingerprint"] = repair.plan.plan_fingerprint
            evidence["launch_recipe"] = recipe
        store.save_software_instance_manifest(
            {
                **dict(manifest),
                "resource_claims": _replace_resource(
                    manifest.get("resource_claims", ()), runtime, vara_ini
                ),
                "evidence": evidence,
            }
        )

    records = list(store.list_radio_software_identity_records(repair.radio_profile_id))
    if records:
        changed = False
        next_records = []
        for record in records:
            if record.family_key != "varac":
                next_records.append(record)
                continue
            components = []
            launch = dict(record.launch)
            argv_map = dict(launch.get("argv") or {})
            cwd_map = dict(launch.get("cwd") or {})
            env_map = dict(launch.get("env") or {})
            vara_argv = ("wine", str(runtime / "VARA.exe"))
            for component in record.components:
                if component.component_id.casefold() == "vara":
                    components.append(
                        SoftwareIdentityComponent(
                            component_id=component.component_id,
                            argv=vara_argv,
                            cwd=str(runtime),
                            env={"WINEPREFIX": wine_prefix},
                            dependencies=component.dependencies,
                            launch=component.launch,
                            readiness=component.readiness,
                        )
                    )
                    changed = True
                else:
                    components.append(component)
            argv_map["vara"] = list(vara_argv)
            cwd_map["vara"] = str(runtime)
            env_map["vara"] = {"WINEPREFIX": wine_prefix}
            launch.update({"argv": argv_map, "cwd": cwd_map, "env": env_map})
            next_records.append(
                replace(
                    record,
                    resources=tuple(
                        _replace_resource(record.resources, runtime, vara_ini, identity=True)
                    ),
                    components=tuple(components),
                    launch=launch,
                )
            )
        if changed:
            store.save_radio_software_identity_records(
                repair.radio_profile_id,
                next_records,
                expected_generation=store.radio_software_identity_generation(
                    repair.radio_profile_id
                ),
            )

    bundle = store.get_radio_launch_bundle(repair.radio_profile_id)
    items = []
    for raw in bundle.get("items", ()) or ():
        if not isinstance(raw, Mapping):
            continue
        row = dict(raw)
        items.append(
            {
                "name": str(row.get("name") or row.get("app_name") or ""),
                "instance_key": str(row.get("instance_key") or ""),
                "enabled": bool(row.get("enabled", True)),
                "startup": bool(
                    row.get("startup", row.get("launch_at_startup", False))
                ),
                "monitor_health": bool(row.get("monitor_health", True)),
                "launch_path_override": str(
                    row.get("launch_path_override")
                    or row.get("path_override")
                    or ""
                ),
                "launch_command_override": str(
                    row.get("launch_command_override")
                    or row.get("command_override")
                    or ""
                ),
                "dependencies": list(row.get("dependencies") or ()),
                "readiness_policy": dict(
                    row.get("readiness_policy") or row.get("readiness") or {}
                ),
            }
        )
    launch_changed = False
    for row in items:
        if str(row.get("name") or "").strip().casefold() != "vara":
            continue
        readiness = dict(row.get("readiness_policy") or {})
        readiness.update(
            {
                "structured_launch": True,
                "executable": "wine",
                "launch_arguments": [str(runtime / "VARA.exe")],
                "working_directory": str(runtime),
                "managed_directories": [str(runtime)],
                "environment": {"WINEPREFIX": wine_prefix},
            }
        )
        row.update(
            launch_path_override="wine",
            launch_command_override="",
            readiness_policy=readiness,
        )
        launch_changed = True
    if launch_changed:
        store.save_radio_launch_bundle(
            repair.radio_profile_id,
            launch_enabled=bool(bundle.get("launch_enabled", False)),
            items=items,
        )
    return {"node": saved_node, "radio_profile_id": repair.radio_profile_id}


def repair_managed_varac_wine_runtime_paths(
    store: MultiRadioStore,
    *,
    backup_root: Path,
    process_running: bool = False,
    platform_override: str = "",
) -> tuple[Mapping[str, Any], ...]:
    """Repair all exact, qualified legacy managed paths; never guess."""

    platform_key = str(platform_override or _platform_key()).strip().casefold()
    if platform_key != "linux-wine":
        return ()
    profiles = store.list_device_profiles()
    memberships = store.list_varac_cluster_members()
    clusters = {int(row.get("id") or 0): row for row in store.list_varac_clusters()}
    results: list[Mapping[str, Any]] = []
    for node in store.list_varac_nodes():
        linked = [row for row in profiles if int(row.get("varac_node_id") or 0) == int(node.get("id") or 0)]
        if len(linked) != 1:
            continue
        profile = linked[0]
        membership = next(
            (row for row in memberships if int(row.get("device_profile_id") or 0) == int(profile.get("id") or 0) and int(row.get("enabled", 1) or 0) == 1),
            None,
        )
        cluster = clusters.get(int((membership or {}).get("cluster_id") or 0))
        repair = prepare_managed_varac_runtime_repair(
            node, profile, membership=membership, cluster=cluster,
            platform_override=platform_key,
        )
        if repair.state in {"current", "not-applicable"}:
            continue
        if repair.state == "needs-attention":
            results.append({"node_id": repair.node_id, "state": repair.state, "detail": repair.why})
            continue
        if process_running:
            results.append({"node_id": repair.node_id, "state": "deferred", "detail": "VarAC or VARA is running."})
            continue
        if repair.state == "reconcile":
            with store.guided_save_transaction() as transaction:
                persisted = _persist_runtime_projection(store, repair, observed={})
                transaction.complete()
            results.append({"node_id": repair.node_id, "state": "reconciled", "persisted": persisted})
            continue
        if repair.plan is None:
            continue
        outcome = apply_varac_native_transaction(
            store=store,
            plan=repair.plan,
            backup_root=backup_root,
            persist=lambda active_store, observed, prepared=repair: _persist_runtime_projection(
                active_store, prepared, observed=observed
            ),
            running_checker=(lambda _member: process_running),
        )
        results.append(
            {
                "node_id": repair.node_id,
                "state": "repaired" if outcome.ok else "recovery-required" if outcome.needs_recovery else "failed",
                "detail": outcome.error,
            }
        )
    return tuple(results)


__all__ = ["repair_managed_varac_wine_runtime_paths"]
