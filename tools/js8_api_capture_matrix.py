#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import platform
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Mapping, Tuple

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

os.environ.setdefault("FREQINOUT_LOG_LEVEL", "DISABLED")

from freqinout.radio_interface.js8_api_client import (  # noqa: E402
    JS8ApiClient,
    JS8ApiEndpoint,
    JS8_TCP_API_DEFAULT_PORT,
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Capture JS8Call TCP API capabilities for one or more endpoints. "
            "Use this to compare JS8Call 2.2.0, 3.x, and Improved builds before "
            "enabling native-client runtime behavior."
        )
    )
    parser.add_argument(
        "--endpoint",
        action="append",
        default=[],
        metavar="LABEL=HOST:PORT",
        help=(
            "Endpoint to probe. Example: official-3.0.2=127.0.0.1:2442. "
            "May be repeated. If omitted, probes default=127.0.0.1:2442."
        ),
    )
    parser.add_argument("--timeout", type=float, default=0.4, help="Per-command timeout in seconds")
    parser.add_argument("--out", default="", help="Optional JSON output path")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output")
    parser.add_argument("--capture-label", default="", help="Optional label for this capture run")
    parser.add_argument(
        "--operator-note",
        action="append",
        default=[],
        help="Free-text note to include in the capture report. May be repeated.",
    )
    parser.add_argument(
        "--endpoint-build",
        action="append",
        default=[],
        metavar="LABEL=TEXT",
        help="Build/source note for an endpoint label, e.g. local-3.0.2='JS8Call 3.0.2 macOS app'.",
    )
    parser.add_argument(
        "--endpoint-platform",
        action="append",
        default=[],
        metavar="LABEL=TEXT",
        help="Target platform note for an endpoint label, e.g. official-2.2.0='Linux Mint 22'.",
    )
    parser.add_argument(
        "--endpoint-note",
        action="append",
        default=[],
        metavar="LABEL=TEXT",
        help="Free-text note for an endpoint label. May be repeated.",
    )
    parser.add_argument(
        "--endpoint-tcp-api",
        action="append",
        default=[],
        metavar="LABEL=enabled|disabled|unknown",
        help="Operator-observed TCP API setting for an endpoint label.",
    )
    parser.add_argument(
        "--endpoint-udp-port",
        action="append",
        default=[],
        metavar="LABEL=PORT",
        help="Operator-observed UDP/WSJT-X port for context. This is not the FIO TCP API target.",
    )
    return parser


def _parse_endpoint(raw: str, *, default_label: str = "endpoint") -> Tuple[str, JS8ApiEndpoint]:
    text = str(raw or "").strip()
    if not text:
        raise ValueError("empty endpoint")
    if "=" in text:
        label, address = text.split("=", 1)
        label = label.strip() or "endpoint"
    else:
        label = str(default_label or "endpoint").strip() or "endpoint"
        address = text
    host, _, port_text = address.strip().partition(":")
    host = host.strip() or "127.0.0.1"
    port = int(port_text.strip() or JS8_TCP_API_DEFAULT_PORT)
    return label, JS8ApiEndpoint(host, port)


def _parse_labeled_values(raw_items: List[str], *, allow_repeated: bool = False) -> Dict[str, Any]:
    values: Dict[str, Any] = {}
    for raw in raw_items or []:
        text = str(raw or "").strip()
        if not text:
            continue
        if "=" not in text:
            raise ValueError(f"Expected LABEL=VALUE, got {text!r}")
        label, value = text.split("=", 1)
        label = label.strip()
        if not label:
            raise ValueError(f"Expected LABEL=VALUE, got {text!r}")
        value = value.strip()
        if allow_repeated:
            values.setdefault(label, []).append(value)
        else:
            values[label] = value
    return values


def _endpoint_metadata(
    label: str,
    *,
    builds: Mapping[str, object],
    platforms: Mapping[str, object],
    notes: Mapping[str, object],
    tcp_api: Mapping[str, object],
    udp_ports: Mapping[str, object],
) -> Dict[str, Any]:
    metadata: Dict[str, Any] = {}
    if label in builds:
        metadata["build"] = str(builds.get(label) or "").strip()
    if label in platforms:
        metadata["target_platform"] = str(platforms.get(label) or "").strip()
    if label in tcp_api:
        metadata["tcp_api"] = str(tcp_api.get(label) or "").strip().lower()
    if label in udp_ports:
        try:
            metadata["udp_wsjtx_port"] = int(str(udp_ports.get(label) or "").strip())
        except Exception:
            metadata["udp_wsjtx_port"] = str(udp_ports.get(label) or "").strip()
    label_notes = notes.get(label)
    if isinstance(label_notes, list):
        metadata["notes"] = [str(item or "").strip() for item in label_notes if str(item or "").strip()]
    elif label_notes:
        metadata["notes"] = [str(label_notes or "").strip()]
    return {key: value for key, value in metadata.items() if value not in ("", [], None)}


def _unmatched_metadata_labels(endpoint_labels: set[str], metadata_maps: Mapping[str, Mapping[str, object]]) -> Dict[str, list[str]]:
    unmatched: Dict[str, list[str]] = {}
    for field, values in metadata_maps.items():
        labels = sorted(str(label) for label in values.keys() if str(label) not in endpoint_labels)
        if labels:
            unmatched[field] = labels
    return unmatched


def _probe(
    label: str,
    endpoint: JS8ApiEndpoint,
    *,
    timeout_s: float,
    metadata: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    client = JS8ApiClient(endpoint, timeout_s=max(0.1, timeout_s), auto_reconnect=False, name=label)
    started = client.start()
    result: Dict[str, Any] = {
        "label": label,
        "endpoint": {
            "host": endpoint.normalized().host,
            "port": endpoint.normalized().port,
        },
        "connected": bool(started),
        "mode": "offline",
        "version": "",
        "supported": {},
        "errors": {},
        "last_error": "",
        "metadata": dict(metadata or {}),
    }
    try:
        if started:
            snapshot = client.probe_capabilities(timeout_s=max(0.1, timeout_s))
            result.update(
                {
                    "connected": snapshot.connected,
                    "mode": snapshot.mode,
                    "version": snapshot.version,
                    "supported": snapshot.supported,
                    "errors": snapshot.errors,
                    "last_error": client.last_error,
                }
            )
        else:
            result["last_error"] = client.last_error or "JS8Call TCP API not reachable"
    finally:
        client.stop()
    return result


def main(argv: List[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    raw_endpoints = args.endpoint or [f"default=127.0.0.1:{JS8_TCP_API_DEFAULT_PORT}"]
    endpoints = [
        _parse_endpoint(raw, default_label=f"endpoint-{index}")
        for index, raw in enumerate(raw_endpoints, start=1)
    ]
    builds = _parse_labeled_values(args.endpoint_build)
    target_platforms = _parse_labeled_values(args.endpoint_platform)
    endpoint_notes = _parse_labeled_values(args.endpoint_note, allow_repeated=True)
    tcp_api = _parse_labeled_values(args.endpoint_tcp_api)
    udp_ports = _parse_labeled_values(args.endpoint_udp_port)
    endpoint_labels = {label for label, _endpoint in endpoints}
    metadata_warnings = _unmatched_metadata_labels(
        endpoint_labels,
        {
            "endpoint_build": builds,
            "endpoint_platform": target_platforms,
            "endpoint_note": endpoint_notes,
            "endpoint_tcp_api": tcp_api,
            "endpoint_udp_port": udp_ports,
        },
    )
    results = [
        _probe(
            label,
            endpoint,
            timeout_s=args.timeout,
            metadata=_endpoint_metadata(
                label,
                builds=builds,
                platforms=target_platforms,
                notes=endpoint_notes,
                tcp_api=tcp_api,
                udp_ports=udp_ports,
            ),
        )
        for label, endpoint in endpoints
    ]
    report: Dict[str, Any] = {
        "captured_ts": time.time(),
        "tool": "js8_api_capture_matrix",
        "capture_label": str(args.capture_label or "").strip(),
        "operator_notes": [str(item or "").strip() for item in (args.operator_note or []) if str(item or "").strip()],
        "metadata_warnings": metadata_warnings,
        "host": {
            "platform": platform.platform(),
            "python": platform.python_version(),
        },
        "results": results,
    }
    text = json.dumps(report, indent=2 if args.pretty else None, sort_keys=True)
    if args.out:
        Path(args.out).expanduser().resolve().write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0 if any(item.get("connected") for item in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
