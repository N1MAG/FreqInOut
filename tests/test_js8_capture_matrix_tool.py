from __future__ import annotations

import json

from tools import js8_api_capture_matrix


def test_capture_matrix_labeled_values_support_repeated_notes() -> None:
    values = js8_api_capture_matrix._parse_labeled_values(
        ["official-2.2.0=first note", "official-2.2.0=second note"],
        allow_repeated=True,
    )

    assert values == {"official-2.2.0": ["first note", "second note"]}


def test_capture_matrix_embeds_endpoint_metadata(monkeypatch, capsys, tmp_path) -> None:
    def fake_probe(label, endpoint, *, timeout_s, metadata=None):
        return {
            "label": label,
            "endpoint": {"host": endpoint.normalized().host, "port": endpoint.normalized().port},
            "connected": True,
            "mode": "api_basic",
            "version": "",
            "supported": {"RIG.GET_FREQ": True},
            "errors": {},
            "last_error": "",
            "metadata": dict(metadata or {}),
        }

    monkeypatch.setattr(js8_api_capture_matrix, "_probe", fake_probe)
    out_path = tmp_path / "capture.json"

    rc = js8_api_capture_matrix.main(
        [
            "--endpoint",
            "local-3.0.2=127.0.0.1:2443",
            "--capture-label",
            "local real-build capture",
            "--operator-note",
            "No radio connected.",
            "--endpoint-build",
            "local-3.0.2=JS8Call 3.0.2 macOS app",
            "--endpoint-platform",
            "local-3.0.2=macOS 26.5",
            "--endpoint-note",
            "local-3.0.2=TCP API enabled on alternate port.",
            "--endpoint-tcp-api",
            "local-3.0.2=enabled",
            "--endpoint-udp-port",
            "local-3.0.2=2242",
            "--out",
            str(out_path),
            "--pretty",
        ]
    )

    captured = capsys.readouterr().out
    report = json.loads(captured)
    written = json.loads(out_path.read_text(encoding="utf-8"))
    result = report["results"][0]

    assert rc == 0
    assert written == report
    assert report["capture_label"] == "local real-build capture"
    assert report["operator_notes"] == ["No radio connected."]
    assert result["metadata"] == {
        "build": "JS8Call 3.0.2 macOS app",
        "target_platform": "macOS 26.5",
        "tcp_api": "enabled",
        "udp_wsjtx_port": 2242,
        "notes": ["TCP API enabled on alternate port."],
    }


def test_capture_matrix_no_metadata_path_and_unique_fallback_labels(monkeypatch, capsys) -> None:
    def fake_probe(label, endpoint, *, timeout_s, metadata=None):
        return {
            "label": label,
            "endpoint": {"host": endpoint.normalized().host, "port": endpoint.normalized().port},
            "connected": True,
            "mode": "api_basic",
            "version": "",
            "supported": {},
            "errors": {},
            "last_error": "",
            "metadata": dict(metadata or {}),
        }

    monkeypatch.setattr(js8_api_capture_matrix, "_probe", fake_probe)

    rc = js8_api_capture_matrix.main(
        [
            "--endpoint",
            "127.0.0.1:2442",
            "--endpoint",
            "127.0.0.1:2443",
        ]
    )

    report = json.loads(capsys.readouterr().out)

    assert rc == 0
    assert [item["label"] for item in report["results"]] == ["endpoint-1", "endpoint-2"]
    assert [item["metadata"] for item in report["results"]] == [{}, {}]
    assert report["metadata_warnings"] == {}


def test_capture_matrix_reports_unmatched_metadata_labels(monkeypatch, capsys) -> None:
    def fake_probe(label, endpoint, *, timeout_s, metadata=None):
        return {
            "label": label,
            "endpoint": {"host": endpoint.normalized().host, "port": endpoint.normalized().port},
            "connected": True,
            "mode": "api_basic",
            "version": "",
            "supported": {},
            "errors": {},
            "last_error": "",
            "metadata": dict(metadata or {}),
        }

    monkeypatch.setattr(js8_api_capture_matrix, "_probe", fake_probe)

    rc = js8_api_capture_matrix.main(
        [
            "--endpoint",
            "local-3.0.2=127.0.0.1:2443",
            "--endpoint-build",
            "typo-label=JS8Call 3.0.2",
            "--endpoint-note",
            "typo-label=This should be reported as unmatched.",
        ]
    )

    report = json.loads(capsys.readouterr().out)

    assert rc == 0
    assert report["results"][0]["metadata"] == {}
    assert report["metadata_warnings"] == {
        "endpoint_build": ["typo-label"],
        "endpoint_note": ["typo-label"],
    }
