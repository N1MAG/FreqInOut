#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import platform
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple

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
    return parser


def _parse_endpoint(raw: str) -> Tuple[str, JS8ApiEndpoint]:
    text = str(raw or "").strip()
    if not text:
        raise ValueError("empty endpoint")
    if "=" in text:
        label, address = text.split("=", 1)
        label = label.strip() or "endpoint"
    else:
        label = "endpoint"
        address = text
    host, _, port_text = address.strip().partition(":")
    host = host.strip() or "127.0.0.1"
    port = int(port_text.strip() or JS8_TCP_API_DEFAULT_PORT)
    return label, JS8ApiEndpoint(host, port)


def _probe(label: str, endpoint: JS8ApiEndpoint, *, timeout_s: float) -> Dict[str, Any]:
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
    endpoints = [_parse_endpoint(raw) for raw in raw_endpoints]
    results = [_probe(label, endpoint, timeout_s=args.timeout) for label, endpoint in endpoints]
    report: Dict[str, Any] = {
        "captured_ts": time.time(),
        "tool": "js8_api_capture_matrix",
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
