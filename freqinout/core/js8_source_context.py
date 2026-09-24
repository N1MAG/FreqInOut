from __future__ import annotations

from typing import Mapping

from freqinout.core.ingest_runtime_status import active_runtime_ingest_inventory
from freqinout.core.ingest_source_model import AppInstanceDescriptor, IngestSourceDescriptor, IngestSourceInventory


def resolve_js8_source_context(
    settings: object | None = None,
    *,
    host: object = "",
    port: object = "",
    inventory: IngestSourceInventory | None = None,
) -> dict[str, str]:
    host_txt = str(host or "").strip()
    port_num = _int_or_zero(port)
    if settings is not None:
        if not host_txt:
            try:
                host_txt = str(settings.get("js8_host", "") or "").strip()
            except Exception:
                host_txt = ""
        if not port_num:
            try:
                port_num = _int_or_zero(settings.get("js8_port", 2442))
            except Exception:
                port_num = 0
    host_txt = host_txt or "127.0.0.1"
    port_num = port_num or 2442
    endpoint = f"{host_txt}:{port_num}".strip().lower()
    try:
        inv = inventory if inventory is not None else active_runtime_ingest_inventory()
    except Exception:
        return {}
    for instance in inv.app_instances:
        if instance.family != "js8call":
            continue
        instance_endpoint = f"{str(instance.api_host or '').strip() or '127.0.0.1'}:{int(instance.api_port or 0)}".lower()
        if instance_endpoint != endpoint:
            continue
        api_source_id = ""
        for source in inv.ingest_sources:
            if source.family == "js8call" and source.source_type == "api" and source.app_instance_id == instance.source_id:
                api_source_id = source.source_id
                break
        metadata = instance.metadata if isinstance(instance.metadata, Mapping) else {}
        return {
            "source_id": api_source_id,
            "app_instance_id": str(instance.source_id or ""),
            "source_radio_id": str(instance.radio_id or ""),
            "js8_instance_id": str(metadata.get("js8_instance_id", "") or instance.source_id),
        }
    return {}


def resolve_js8_endpoint_context(
    settings: object | None = None,
    *,
    source_context: Mapping[str, object] | None = None,
    inventory: IngestSourceInventory | None = None,
) -> dict[str, str]:
    """
    Resolve a JS8Call send/query endpoint from source-scoped message context.

    Pending-message rows may carry either the JS8 app-instance source key or a
    file/API ingest source key. This helper makes both resolve to the owning
    JS8Call instance so UI actions do not fall back to the currently selected
    radio when they are acting on a different JS8Call instance.
    """
    context = source_context or {}
    source_key = str(context.get("source_key", "") or context.get("source_id", "") or "").strip()
    radio_id = str(context.get("source_radio_id", "") or context.get("radio_id", "") or "").strip()
    js8_id = str(context.get("js8_instance_id", "") or "").strip()
    try:
        inv = inventory if inventory is not None else active_runtime_ingest_inventory()
    except Exception:
        return {}

    matched_source: IngestSourceDescriptor | None = None
    if source_key:
        for source in inv.ingest_sources:
            if source.family != "js8call":
                continue
            if source.source_id == source_key or source.app_instance_id == source_key:
                matched_source = source
                break

    instance = _match_js8_instance(inv, source_key=source_key, radio_id=radio_id, js8_id=js8_id, matched_source=matched_source)
    if instance is None:
        return {}
    endpoint = _endpoint_for_instance(settings, instance, matched_source)
    if not endpoint:
        return {}
    metadata = instance.metadata if isinstance(instance.metadata, Mapping) else {}
    return {
        "host": endpoint[0],
        "port": str(endpoint[1]),
        "label": str(instance.label or source_key or js8_id or radio_id or f"{endpoint[0]}:{endpoint[1]}"),
        "app_instance_id": str(instance.source_id or ""),
        "source_radio_id": str(instance.radio_id or ""),
        "js8_instance_id": str(metadata.get("js8_instance_id", "") or instance.source_id),
    }


def _int_or_zero(value: object) -> int:
    try:
        return int(value or 0)
    except Exception:
        return 0


def _match_js8_instance(
    inventory: IngestSourceInventory,
    *,
    source_key: str,
    radio_id: str,
    js8_id: str,
    matched_source: IngestSourceDescriptor | None,
) -> AppInstanceDescriptor | None:
    js8_id_lc = js8_id.lower()
    radio_lc = radio_id.lower()
    source_lc = source_key.lower()
    source_app_lc = str(matched_source.app_instance_id or "").lower() if matched_source is not None else ""
    for instance in inventory.app_instances:
        if instance.family != "js8call":
            continue
        metadata = instance.metadata if isinstance(instance.metadata, Mapping) else {}
        instance_js8 = str(metadata.get("js8_instance_id", "") or instance.source_id or "").strip().lower()
        instance_radio = str(instance.radio_id or "").strip().lower()
        instance_source = str(instance.source_id or "").strip().lower()
        if source_lc:
            if matched_source is not None and source_app_lc:
                if instance_source != source_app_lc:
                    continue
            elif instance_source != source_lc:
                continue
        if radio_lc and instance_radio != radio_lc:
            continue
        if js8_id_lc and instance_js8 != js8_id_lc and instance_source != js8_id_lc:
            continue
        return instance
    if matched_source is not None and matched_source.app_instance_id:
        app_lc = str(matched_source.app_instance_id).lower()
        for instance in inventory.app_instances:
            if instance.family == "js8call" and str(instance.source_id or "").lower() == app_lc:
                return instance
    return None


def _endpoint_for_instance(
    settings: object | None,
    instance: AppInstanceDescriptor,
    source: IngestSourceDescriptor | None,
) -> tuple[str, int] | None:
    endpoint = str(getattr(source, "endpoint", "") or "").strip() if source is not None else ""
    if endpoint:
        host, port = _parse_endpoint(endpoint)
        if host and port:
            return host, port
    host = str(instance.api_host or "").strip()
    port = _int_or_zero(instance.api_port)
    if settings is not None:
        if not host:
            try:
                host = str(settings.get("js8_host", "") or "").strip()
            except Exception:
                host = ""
        if not port:
            try:
                port = _int_or_zero(settings.get("js8_port", 2442))
            except Exception:
                port = 0
    host = host or "127.0.0.1"
    port = port or 2442
    return host, port


def _parse_endpoint(endpoint: str) -> tuple[str, int]:
    text = str(endpoint or "").strip()
    if not text:
        return "", 0
    if ":" not in text:
        return text or "127.0.0.1", 0
    host, port_text = text.rsplit(":", 1)
    return (host.strip() or "127.0.0.1", _int_or_zero(port_text))
