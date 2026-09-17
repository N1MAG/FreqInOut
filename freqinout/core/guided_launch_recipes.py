"""Pure managed launch recipes for unsaved guided software drafts.

The resolver performs no filesystem, process, socket, or persistence work.  It
turns one already-reviewed distinct draft into an exact component plan that is
used by Add Radio, Software Administration, Review, and atomic persistence.
Unknown application families/versions fail to an explicit operator action
instead of inventing a launch command.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import json
import ntpath
from pathlib import Path
import posixpath
import re
import shlex
from typing import Any, Mapping, Sequence

from freqinout.core.js8_storage import (
    js8_application_name,
    normalize_variant_family,
    qt_data_root_candidates,
    stable_managed_rig_name,
)


_KNOWN_JS8_VERSIONS = {
    "js8call_2_2": "2.2.0",
    "js8call_improved_3_0_3": "3.0.3",
    "js8call_subspace_4_1": "4.1.0.478",
}
_KEY_RE = re.compile(r"[^a-z0-9_.-]+")


def _text(value: object) -> str:
    return str(value or "").strip()


def _key(value: object) -> str:
    return _KEY_RE.sub("-", _text(value).casefold()).strip("-._")


def _join(root: str, *parts: str) -> str:
    joiner = ntpath if "\\" in root and "/" not in root else posixpath
    return joiner.join(root, *parts)


def _command_text(values: Sequence[str]) -> str:
    return shlex.join(tuple(_text(value) for value in values if _text(value)))


def _scope(draft: Mapping[str, Any]) -> str:
    return "receive_only" if _text(draft.get("radio_role")).casefold() == "observer" else "standard"


def canonical_js8_version(variant: object, version: object) -> str:
    """Return the exact writer/recipe version represented by discovered evidence."""

    normalized = normalize_variant_family(variant, version)
    expected = _KNOWN_JS8_VERSIONS.get(normalized, "")
    observed = _text(version)
    if not expected or not observed:
        return ""
    pattern = rf"(?<![0-9]){re.escape(expected)}(?![0-9])"
    return expected if re.search(pattern, observed) else ""


@dataclass(frozen=True)
class GuidedLaunchComponent:
    component_key: str
    label: str
    executable: str
    arguments: tuple[str, ...] = ()
    working_directory: str = ""
    dependencies: tuple[str, ...] = ()
    profile_selector: str = ""
    configuration_roots: tuple[str, ...] = ()
    data_roots: tuple[str, ...] = ()
    endpoints: tuple[Mapping[str, Any], ...] = ()
    readiness: Mapping[str, Any] = field(default_factory=dict)
    execution_scope: str = "standard"
    launch_at_startup: bool = False
    operator_starts: bool = False

    @property
    def effective_command(self) -> tuple[str, ...]:
        return (self.executable, *self.arguments) if self.executable and not self.operator_starts else ()

    @property
    def effective_command_text(self) -> str:
        return _command_text(self.effective_command)

    def to_mapping(self) -> dict[str, Any]:
        return {
            "component_key": self.component_key,
            "label": self.label,
            "executable": self.executable,
            "arguments": list(self.arguments),
            "effective_command": list(self.effective_command),
            "effective_command_text": self.effective_command_text,
            "working_directory": self.working_directory,
            "dependencies": list(self.dependencies),
            "profile_selector": self.profile_selector,
            "configuration_roots": list(self.configuration_roots),
            "data_roots": list(self.data_roots),
            "endpoints": [dict(item) for item in self.endpoints],
            "readiness": dict(self.readiness),
            "execution_scope": self.execution_scope,
            "launch_at_startup": self.launch_at_startup,
            "operator_starts": self.operator_starts,
        }


@dataclass(frozen=True)
class GuidedLaunchRecipeResolution:
    family_key: str
    status: str
    components: tuple[GuidedLaunchComponent, ...] = ()
    raw_override_allowed: bool = False
    recovery_action: str = ""
    summary: str = ""
    fingerprint: str = field(init=False)

    def __post_init__(self) -> None:
        payload = {
            "family_key": self.family_key,
            "status": self.status,
            "components": [item.to_mapping() for item in self.components],
            "raw_override_allowed": self.raw_override_allowed,
            "recovery_action": self.recovery_action,
            "summary": self.summary,
        }
        object.__setattr__(
            self,
            "fingerprint",
            hashlib.sha256(
                json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
            ).hexdigest(),
        )

    @property
    def qualified(self) -> bool:
        return self.status == "qualified_managed"

    def to_mapping(self) -> dict[str, Any]:
        return {
            "family_key": self.family_key,
            "status": self.status,
            "components": [item.to_mapping() for item in self.components],
            "raw_override_allowed": self.raw_override_allowed,
            "recovery_action": self.recovery_action,
            "summary": self.summary,
            "fingerprint": self.fingerprint,
        }


def recipe_resolution_from_mapping(value: Mapping[str, Any]) -> GuidedLaunchRecipeResolution:
    components = tuple(
        GuidedLaunchComponent(
            component_key=_text(item.get("component_key")),
            label=_text(item.get("label")),
            executable=_text(item.get("executable")),
            arguments=tuple(_text(part) for part in item.get("arguments", ()) if _text(part)),
            working_directory=_text(item.get("working_directory")),
            dependencies=tuple(_text(part) for part in item.get("dependencies", ()) if _text(part)),
            profile_selector=_text(item.get("profile_selector")),
            configuration_roots=tuple(_text(part) for part in item.get("configuration_roots", ()) if _text(part)),
            data_roots=tuple(_text(part) for part in item.get("data_roots", ()) if _text(part)),
            endpoints=tuple(dict(endpoint) for endpoint in item.get("endpoints", ()) if isinstance(endpoint, Mapping)),
            readiness=dict(item.get("readiness") or {}),
            execution_scope=_text(item.get("execution_scope")) or "standard",
            launch_at_startup=bool(item.get("launch_at_startup", False)),
            operator_starts=bool(item.get("operator_starts", False)),
        )
        for item in value.get("components", ())
        if isinstance(item, Mapping)
    )
    return GuidedLaunchRecipeResolution(
        family_key=_text(value.get("family_key")),
        status=_text(value.get("status")) or "incomplete",
        components=components,
        raw_override_allowed=bool(value.get("raw_override_allowed", False)),
        recovery_action=_text(value.get("recovery_action")),
        summary=_text(value.get("summary")),
    )


def _unsupported(family: str, detail: str) -> GuidedLaunchRecipeResolution:
    return GuidedLaunchRecipeResolution(
        family_key=family,
        status="unsupported",
        raw_override_allowed=True,
        recovery_action=detail,
        summary="Operator setup required; the radio draft remains inactive.",
    )


def resolve_js8_managed_recipe(
    draft: Mapping[str, Any],
    *,
    managed_root: str,
    platform: object | None = None,
    storage_home: Path | None = None,
) -> GuidedLaunchRecipeResolution:
    executable = _text(draft.get("application_path"))
    variant = normalize_variant_family(draft.get("variant"), draft.get("version"))
    version = _text(draft.get("version"))
    qualified_version = canonical_js8_version(variant, version)
    if not executable:
        return _unsupported("js8call", "Choose the exact JS8Call executable, then resolve the managed recipe again.")
    if not qualified_version:
        return _unsupported(
            "js8call",
            "Verify a supported stock 2.2.0, Improved 3.0.3, or Subspace 4.1.x version, or use Advanced operator-managed launch.",
        )
    instance_key = _text(draft.get("draft_instance_key") or draft.get("instance_key"))
    if not instance_key:
        return _unsupported("js8call", "Return to Identity so FIO can allocate a stable instance key.")
    if not _text(managed_root):
        return _unsupported(
            "js8call",
            "FIO's managed-instance root is unavailable. Return to Settings and reopen this setup before continuing.",
        )
    radio_name = _text(draft.get("owner_label") or draft.get("instance_name") or "Radio")
    rig_name = stable_managed_rig_name(system_key=instance_key, name=radio_name)
    root = _join(_text(managed_root), instance_key, "js8call")
    profile_root = root
    save_root = _join(root, "save")
    forms_root = _join(root, "forms")
    data_root = str(
        qt_data_root_candidates(
            application_name=js8_application_name(rig_name),
            platform=platform,
            home=storage_home,
        )[0]
    )
    host = _text(draft.get("host")) or "127.0.0.1"
    tcp_port = int(draft.get("port") or 0)
    udp_port = int(draft.get("udp_port") or 0)
    if not tcp_port or not udp_port:
        return _unsupported("js8call", "Return to Connections so FIO can allocate both JS8Call TCP and UDP endpoints.")
    component = GuidedLaunchComponent(
        component_key="js8call",
        label="JS8Call",
        executable=executable,
        arguments=("--rig-name", rig_name),
        profile_selector=rig_name,
        configuration_roots=(profile_root,),
        data_roots=(data_root, save_root, forms_root),
        endpoints=(
            {"name": "JS8Call API", "protocol": "tcp", "host": host, "port": tcp_port},
            {"name": "JS8Call UDP", "protocol": "udp", "host": host, "port": udp_port},
        ),
        readiness={"kind": "js8_api", "host": host, "port": tcp_port, "require_api": True},
        execution_scope=_scope(draft),
        launch_at_startup=bool(draft.get("launch_at_startup", False)),
    )
    return GuidedLaunchRecipeResolution(
        family_key="js8call",
        status="qualified_managed",
        components=(component,),
        summary=f"Launch {variant} {qualified_version} as {rig_name} on {host}:{tcp_port}.",
    )


def resolve_fast_light_managed_recipe(
    draft: Mapping[str, Any],
    *,
    managed_root: str,
) -> GuidedLaunchRecipeResolution:
    observer = _scope(draft) == "receive_only"
    flrig = _text(draft.get("application_path"))
    fldigi = _text(draft.get("secondary_application_path"))
    if not fldigi or (not observer and not flrig):
        return _unsupported(
            "fast_light",
            "Choose the exact FLDigi executable and, for a transceiver, FLRig; then resolve the managed recipe again.",
        )
    instance_key = _text(draft.get("draft_instance_key") or draft.get("instance_key"))
    if not instance_key:
        return _unsupported("fast_light", "Return to Identity so FIO can allocate a stable Fast Light key.")
    if not _text(managed_root):
        return _unsupported(
            "fast_light",
            "FIO's managed-instance root is unavailable. Return to Settings and reopen this setup before continuing.",
        )
    root = _join(_text(managed_root), instance_key, "fast-light")
    flrig_profile = _join(root, "flrig")
    fldigi_profile = _join(root, "fldigi")
    logs = _join(fldigi_profile, "logs")
    checkins = _join(fldigi_profile, "checkins")
    host = _text(draft.get("host")) or "127.0.0.1"
    flrig_port = int(draft.get("port") or 0)
    fldigi_port = int(draft.get("secondary_port") or 0)
    if not fldigi_port or (not observer and not flrig_port):
        return _unsupported("fast_light", "Return to Connections so FIO can allocate the Fast Light endpoints.")
    startup = bool(draft.get("launch_at_startup", False))
    components: list[GuidedLaunchComponent] = []
    if not observer:
        components.append(
            GuidedLaunchComponent(
                component_key="flrig",
                label="FLRig",
                executable=flrig,
                arguments=("--config-dir", flrig_profile),
                profile_selector=flrig_profile,
                configuration_roots=(flrig_profile,),
                endpoints=({"name": "FLRig XML-RPC", "protocol": "tcp", "host": host, "port": flrig_port},),
                readiness={"kind": "xmlrpc", "host": host, "port": flrig_port, "require_service": True},
                launch_at_startup=startup,
            )
        )
    fldigi_arguments = (
        "--config-dir", fldigi_profile,
        "--xmlrpc-server-address", host,
        "--xmlrpc-server-port", str(fldigi_port),
    )
    components.append(
        GuidedLaunchComponent(
            component_key="fldigi",
            label="FLDigi",
            executable=fldigi,
            arguments=fldigi_arguments,
            dependencies=() if observer else ("flrig",),
            profile_selector=fldigi_profile,
            configuration_roots=(fldigi_profile,),
            data_roots=(logs, checkins),
            endpoints=({"name": "FLDigi XML-RPC", "protocol": "tcp", "host": host, "port": fldigi_port},),
            readiness={"kind": "xmlrpc", "host": host, "port": fldigi_port, "require_service": True},
            execution_scope="receive_only" if observer else "standard",
            launch_at_startup=startup,
        )
    )
    for key, label, path in (
        ("flmsg", "FLMsg", _text(draft.get("flmsg_application_path"))),
        ("flamp", "FLAmp", _text(draft.get("flamp_application_path"))),
    ):
        if path:
            components.append(
                GuidedLaunchComponent(
                    component_key=key,
                    label=label,
                    executable=path,
                    dependencies=("fldigi",),
                    execution_scope="station_shared_utility",
                    launch_at_startup=startup,
                )
            )
    return GuidedLaunchRecipeResolution(
        family_key="fast_light",
        status="qualified_managed",
        components=tuple(components),
        summary=(
            f"Launch FLDigi receive-only on {host}:{fldigi_port}."
            if observer
            else f"Launch FLRig on {host}:{flrig_port}, then FLDigi on {host}:{fldigi_port}."
        ),
    )


def resolve_guided_launch_recipe(
    draft: Mapping[str, Any],
    *,
    managed_root: str,
    platform: object | None = None,
    storage_home: Path | None = None,
) -> GuidedLaunchRecipeResolution:
    family = _key(draft.get("family_key"))
    if _text(draft.get("mode")).casefold() != "managed":
        return GuidedLaunchRecipeResolution(
            family_key=family,
            status="operator_start",
            raw_override_allowed=True,
            recovery_action="Review the imported or manual application's exact launch behavior in Advanced.",
            summary="Operator-managed launch; FIO does not rewrite this application's identity.",
        )
    if family == "js8call":
        return resolve_js8_managed_recipe(
            draft,
            managed_root=managed_root,
            platform=platform,
            storage_home=storage_home,
        )
    if family == "fast_light":
        return resolve_fast_light_managed_recipe(draft, managed_root=managed_root)
    return GuidedLaunchRecipeResolution(
        family_key=family,
        status="incomplete",
        raw_override_allowed=True,
        recovery_action="Use the family-specific Advanced setup.",
        summary="No managed launch recipe is available for this family.",
    )


def recipe_draft_updates(resolution: GuidedLaunchRecipeResolution) -> dict[str, Any]:
    """Project one qualified resolution into the existing draft/store schema."""

    value = resolution.to_mapping()
    updates: dict[str, Any] = {
        "launch_recipe": value,
        "launch_recipe_status": resolution.status,
        "launch_recipe_fingerprint": resolution.fingerprint,
    }
    if not resolution.qualified:
        return updates
    by_key = {component.component_key: component for component in resolution.components}
    if resolution.family_key == "js8call":
        component = by_key["js8call"]
        endpoints = {str(item.get("protocol")): item for item in component.endpoints}
        updates.update(
            rig_name=component.profile_selector,
            configuration_path=component.configuration_roots[0],
            storage_path=component.data_roots[0],
            port=int(endpoints["tcp"]["port"]),
            udp_port=int(endpoints["udp"]["port"]),
            launch_command="",
        )
    elif resolution.family_key == "fast_light":
        fldigi = by_key["fldigi"]
        updates.update(
            secondary_configuration_path=fldigi.configuration_roots[0],
            storage_path=fldigi.data_roots[0],
            secondary_storage_path=fldigi.data_roots[1],
            secondary_port=int(fldigi.endpoints[0]["port"]),
            launch_command="",
        )
        flrig = by_key.get("flrig")
        if flrig is not None:
            updates.update(
                configuration_path=flrig.configuration_roots[0],
                port=int(flrig.endpoints[0]["port"]),
            )
    return updates


__all__ = [
    "canonical_js8_version",
    "GuidedLaunchComponent",
    "GuidedLaunchRecipeResolution",
    "recipe_draft_updates",
    "recipe_resolution_from_mapping",
    "resolve_fast_light_managed_recipe",
    "resolve_guided_launch_recipe",
    "resolve_js8_managed_recipe",
]
