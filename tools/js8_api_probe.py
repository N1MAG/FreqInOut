#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

JS8_TCP_API_DEFAULT_PORT = 2442
JS8_UDP_WSJT_X_DEFAULT_PORT = 2242


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Probe a JS8Call TCP API endpoint and print a JSON capability report."
    )
    parser.add_argument("--host", default="127.0.0.1", help="JS8Call TCP API host")
    parser.add_argument(
        "--port",
        type=int,
        default=JS8_TCP_API_DEFAULT_PORT,
        help="JS8Call TCP API port. Default is 2442. Port 2242 is the UDP/WSJT-X interface, not this TCP API.",
    )
    parser.add_argument("--timeout", type=float, default=1.0, help="Per-command timeout in seconds")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)

    os.environ.setdefault("FREQINOUT_LOG_LEVEL", "DISABLED")

    from freqinout.radio_interface.js8_api_client import JS8ApiClient, JS8ApiEndpoint

    endpoint = JS8ApiEndpoint(args.host, args.port)
    client = JS8ApiClient(endpoint, timeout_s=max(0.1, args.timeout), auto_reconnect=False)
    started = client.start()
    report: Dict[str, Any] = {
        "checked_ts": time.time(),
        "endpoint": {
            "host": endpoint.normalized().host,
            "port": endpoint.normalized().port,
            "tcp_api_default_port": JS8_TCP_API_DEFAULT_PORT,
            "udp_wsjtx_default_port": JS8_UDP_WSJT_X_DEFAULT_PORT,
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
            snapshot = client.probe_capabilities(timeout_s=max(0.1, args.timeout))
            report.update(
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
            report["last_error"] = client.last_error or "JS8Call TCP API not reachable"
    finally:
        client.stop()

    print(json.dumps(report, indent=2 if args.pretty else None, sort_keys=True))
    return 0 if report.get("connected") else 1


if __name__ == "__main__":
    raise SystemExit(main())
