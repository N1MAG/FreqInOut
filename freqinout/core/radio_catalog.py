from __future__ import annotations

import json
import re
import subprocess
import time
from pathlib import Path
from typing import Any, Dict, List

from freqinout.core.config_paths import get_config_dir
from freqinout.core.logger import log


_RIGCTL_TIMEOUT_SECONDS = 2.5
_CACHE_TTL_SECONDS = 7 * 24 * 60 * 60
_ROW_SPLIT_RE = re.compile(r"\s{2,}")
_DEFAULT_CONTROL_METHODS = ["flrig", "js8call", "rigctld", "manual"]

_STATIC_FALLBACK_ENTRIES: List[Dict[str, Any]] = [
    {"catalog_id": "ICOM_IC705", "manufacturer": "Icom", "model_name": "IC-705", "display_name": "Icom IC-705", "backend_support": _DEFAULT_CONTROL_METHODS},
    {"catalog_id": "ICOM_IC7100", "manufacturer": "Icom", "model_name": "IC-7100", "display_name": "Icom IC-7100", "backend_support": _DEFAULT_CONTROL_METHODS},
    {"catalog_id": "ICOM_IC7300", "manufacturer": "Icom", "model_name": "IC-7300", "display_name": "Icom IC-7300", "backend_support": _DEFAULT_CONTROL_METHODS},
    {"catalog_id": "ICOM_IC9700", "manufacturer": "Icom", "model_name": "IC-9700", "display_name": "Icom IC-9700", "backend_support": _DEFAULT_CONTROL_METHODS},
    {"catalog_id": "YAESU_FT710", "manufacturer": "Yaesu", "model_name": "FT-710", "display_name": "Yaesu FT-710", "backend_support": _DEFAULT_CONTROL_METHODS},
    {"catalog_id": "YAESU_FT818", "manufacturer": "Yaesu", "model_name": "FT-818", "display_name": "Yaesu FT-818", "backend_support": _DEFAULT_CONTROL_METHODS},
    {"catalog_id": "YAESU_FT891", "manufacturer": "Yaesu", "model_name": "FT-891", "display_name": "Yaesu FT-891", "backend_support": _DEFAULT_CONTROL_METHODS},
    {"catalog_id": "YAESU_FT991A", "manufacturer": "Yaesu", "model_name": "FT-991A", "display_name": "Yaesu FT-991A", "backend_support": _DEFAULT_CONTROL_METHODS},
    {"catalog_id": "YAESU_FTDX10", "manufacturer": "Yaesu", "model_name": "FTDX-10", "display_name": "Yaesu FTDX-10", "backend_support": _DEFAULT_CONTROL_METHODS},
    {"catalog_id": "KENWOOD_TS590SG", "manufacturer": "Kenwood", "model_name": "TS-590SG", "display_name": "Kenwood TS-590SG", "backend_support": _DEFAULT_CONTROL_METHODS},
    {"catalog_id": "ELECRAFT_KX3", "manufacturer": "Elecraft", "model_name": "KX3", "display_name": "Elecraft KX3", "backend_support": _DEFAULT_CONTROL_METHODS},
    {"catalog_id": "ELECRAFT_K4", "manufacturer": "Elecraft", "model_name": "K4", "display_name": "Elecraft K4", "backend_support": _DEFAULT_CONTROL_METHODS},
    {"catalog_id": "FLEX_6400", "manufacturer": "FlexRadio", "model_name": "6400", "display_name": "FlexRadio 6400", "backend_support": _DEFAULT_CONTROL_METHODS},
]


def _cache_path() -> Path:
    return get_config_dir() / "cache" / "radio_catalog.json"


def _normalize_entry(entry: Dict[str, Any]) -> Dict[str, Any]:
    manufacturer = str(entry.get("manufacturer", "") or "").strip()
    model_name = str(entry.get("model_name", "") or "").strip()
    display_name = str(entry.get("display_name", "") or "").strip()
    if not display_name:
        display_name = " ".join(part for part in [manufacturer, model_name] if part).strip()
    catalog_id = str(entry.get("catalog_id", "") or "").strip()
    if not catalog_id:
        token = re.sub(r"[^A-Z0-9]+", "_", display_name.upper()).strip("_")
        catalog_id = token or "RADIO"
    out = {
        "catalog_id": catalog_id,
        "manufacturer": manufacturer,
        "model_name": model_name,
        "display_name": display_name or catalog_id,
    }
    backend_support = entry.get("backend_support")
    if isinstance(backend_support, (list, tuple, set)):
        normalized_backends: List[str] = []
        for item in backend_support:
            token = str(item or "").strip().lower()
            if token and token not in normalized_backends:
                normalized_backends.append(token)
    else:
        normalized_backends = []
    source = str(entry.get("source", "") or "").strip()
    if source:
        out["source"] = source
    if not normalized_backends and source in {"hamlib-rigctl", "static-fallback"}:
        normalized_backends = list(_DEFAULT_CONTROL_METHODS)
    if normalized_backends:
        out["backend_support"] = normalized_backends
    return out


def _sort_key(entry: Dict[str, Any]) -> tuple[str, str]:
    return (
        str(entry.get("manufacturer", "") or "").casefold(),
        str(entry.get("model_name", "") or entry.get("display_name", "") or "").casefold(),
    )


def _dedupe_entries(entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen: set[str] = set()
    out: List[Dict[str, Any]] = []
    for raw in entries:
        entry = _normalize_entry(raw)
        key = str(entry.get("catalog_id", "") or "").strip().upper()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(entry)
    out.sort(key=_sort_key)
    return out


def _parse_rigctl_list_output(output: str) -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []
    for raw_line in output.splitlines():
        line = raw_line.rstrip()
        if not line:
            continue
        if line.lstrip().startswith("Rig #"):
            continue
        parts = _ROW_SPLIT_RE.split(line.strip())
        if len(parts) < 6:
            continue
        rig_number, manufacturer, model_name, _version, _status, macro = parts[:6]
        if not rig_number.isdigit():
            continue
        entries.append(
            {
                "catalog_id": str(macro or "").strip() or f"RIG_{rig_number}",
                "manufacturer": manufacturer,
                "model_name": model_name,
                "display_name": " ".join(part for part in [manufacturer, model_name] if part).strip(),
                "source": "hamlib-rigctl",
            }
        )
    return _dedupe_entries(entries)


def _load_from_rigctl() -> List[Dict[str, Any]]:
    try:
        result = subprocess.run(
            ["rigctl", "-l"],
            check=True,
            capture_output=True,
            text=True,
            timeout=_RIGCTL_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        log.debug("Radio catalog: rigctl catalog unavailable: %s", exc)
        return []
    entries = _parse_rigctl_list_output(result.stdout)
    if entries:
        log.debug("Radio catalog: loaded %s entries from rigctl.", len(entries))
    return entries


def _load_from_cache() -> Dict[str, Any] | None:
    path = _cache_path()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    generated_ts = float(payload.get("generated_ts", 0.0) or 0.0)
    entries = _dedupe_entries(list(payload.get("entries", []) or []))
    if not entries:
        return None
    if generated_ts and (time.time() - generated_ts) > _CACHE_TTL_SECONDS:
        return None
    return {
        "source": str(payload.get("source", "cache") or "cache"),
        "entries": entries,
    }


def _write_cache(source: str, entries: List[Dict[str, Any]]) -> None:
    path = _cache_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(
                {
                    "source": source,
                    "generated_ts": time.time(),
                    "entries": entries,
                },
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )
    except Exception as exc:
        log.debug("Radio catalog: unable to write cache: %s", exc)


def load_radio_catalog(*, force_refresh: bool = False) -> Dict[str, Any]:
    if not force_refresh:
        cached = _load_from_cache()
        if cached:
            return cached

    entries = _load_from_rigctl()
    if entries:
        _write_cache("hamlib-rigctl", entries)
        return {"source": "hamlib-rigctl", "entries": entries}

    fallback = _dedupe_entries([dict(row, source="static-fallback") for row in _STATIC_FALLBACK_ENTRIES])
    _write_cache("static-fallback", fallback)
    return {"source": "static-fallback", "entries": fallback}


def find_radio_catalog_entry(
    catalog: List[Dict[str, Any]],
    *,
    catalog_id: str = "",
    manufacturer: str = "",
    model_name: str = "",
    display_name: str = "",
) -> Dict[str, Any] | None:
    catalog_id_norm = str(catalog_id or "").strip().upper()
    manufacturer_norm = str(manufacturer or "").strip().casefold()
    model_name_norm = str(model_name or "").strip().casefold()
    display_name_norm = str(display_name or "").strip().casefold()
    for entry in catalog:
        if catalog_id_norm and str(entry.get("catalog_id", "") or "").strip().upper() == catalog_id_norm:
            return dict(entry)
    for entry in catalog:
        if manufacturer_norm and model_name_norm:
            if (
                str(entry.get("manufacturer", "") or "").strip().casefold() == manufacturer_norm
                and str(entry.get("model_name", "") or "").strip().casefold() == model_name_norm
            ):
                return dict(entry)
        if display_name_norm and str(entry.get("display_name", "") or "").strip().casefold() == display_name_norm:
                return dict(entry)
    return None


def catalog_entry_control_methods(entry: Dict[str, Any] | None) -> List[str]:
    if not isinstance(entry, dict):
        return ["manual"]
    backends = entry.get("backend_support")
    if isinstance(backends, (list, tuple, set)):
        out: List[str] = []
        for item in backends:
            token = str(item or "").strip().lower()
            if token and token not in out:
                out.append(token)
        if out:
            return out
    source = str(entry.get("source", "") or "").strip().lower()
    if source in {"hamlib-rigctl", "static-fallback"}:
        return list(_DEFAULT_CONTROL_METHODS)
    return ["manual"]
