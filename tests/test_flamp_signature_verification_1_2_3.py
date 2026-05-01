from __future__ import annotations

import subprocess
import os
from pathlib import Path

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from freqinout.core import gpg_tools
from freqinout.core.gpg_tools import (
    DEFAULT_INLINE_SIGNED_SUFFIXES,
    signature_payload_candidates,
    verify_file_with_discovery,
)
from freqinout.gui.message_viewer_tab import FileRecord, MessageViewerTab


def _valid_gpg_result() -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(
        args=["gpg"],
        returncode=0,
        stdout=(
            "[GNUPG:] GOODSIG 1234567890ABCDEF Test Operator <test@example.org>\n"
            "[GNUPG:] VALIDSIG 1234567890ABCDEF1234567890ABCDEF12345678 2026-04-22 0 4 0 1 10 01 "
            "1234567890ABCDEF1234567890ABCDEF12345678\n"
            "[GNUPG:] TRUST_FULLY 0 pgp\n"
        ),
        stderr="",
    )


def test_default_inline_suffixes_include_canonical_and_dot_style_names() -> None:
    assert "-sig.k2s" in DEFAULT_INLINE_SIGNED_SUFFIXES
    assert "-sig.b2s" in DEFAULT_INLINE_SIGNED_SUFFIXES
    assert ".sig.k2s" in DEFAULT_INLINE_SIGNED_SUFFIXES
    assert ".sig.b2s" in DEFAULT_INLINE_SIGNED_SUFFIXES


def test_signature_payload_candidates_pair_k2s_and_b2s_sidecars(tmp_path: Path) -> None:
    assert signature_payload_candidates(tmp_path / "Report.k2s.sig") == [tmp_path / "Report.k2s"]
    assert signature_payload_candidates(tmp_path / "Report.b2s.sig") == [tmp_path / "Report.b2s"]
    assert signature_payload_candidates(tmp_path / "Report.sig") == [
        tmp_path / "Report.k2s",
        tmp_path / "Report.b2s",
        tmp_path / "Report",
    ]
    assert signature_payload_candidates(tmp_path / "Report.v1.sig") == [
        tmp_path / "Report.v1.k2s",
        tmp_path / "Report.v1.b2s",
        tmp_path / "Report.v1",
    ]


def test_verify_file_with_discovery_uses_content_aware_inline_detection(monkeypatch, tmp_path: Path) -> None:
    signed = tmp_path / "Report.k2s"
    signed.write_text(
        "-----BEGIN PGP SIGNED MESSAGE-----\n"
        "Hash: SHA256\n\n"
        "payload\n"
        "-----BEGIN PGP SIGNATURE-----\n"
        "signature\n"
        "-----END PGP SIGNATURE-----\n",
        encoding="utf-8",
    )
    calls: list[list[str]] = []

    def fake_run_gpg(gpg_path: str, args: list[str], **kwargs):
        calls.append(args)
        return _valid_gpg_result()

    monkeypatch.setattr(gpg_tools, "resolve_gpg_executable", lambda configured_path="": "/usr/bin/gpg")
    monkeypatch.setattr(gpg_tools, "_run_gpg", fake_run_gpg)

    result = verify_file_with_discovery(signed, allow_inline_clearsigned=True)

    assert result.status == "valid"
    assert calls == [["--status-fd=1", "--verify", str(signed)]]


def test_verify_file_with_discovery_pairs_signature_record_to_payload(monkeypatch, tmp_path: Path) -> None:
    payload = tmp_path / "Report.k2s"
    signature = tmp_path / "Report.k2s.sig"
    payload.write_text("payload", encoding="utf-8")
    signature.write_text("signature", encoding="utf-8")
    calls: list[list[str]] = []

    def fake_run_gpg(gpg_path: str, args: list[str], **kwargs):
        calls.append(args)
        return _valid_gpg_result()

    monkeypatch.setattr(gpg_tools, "resolve_gpg_executable", lambda configured_path="": "/usr/bin/gpg")
    monkeypatch.setattr(gpg_tools, "_run_gpg", fake_run_gpg)

    result = verify_file_with_discovery(signature, allow_inline_clearsigned=True)

    assert result.status == "valid"
    assert result.signature_path == str(signature)
    assert calls == [["--status-fd=1", "--verify", str(signature), str(payload)]]


def test_verify_file_with_discovery_reports_missing_payload_for_signature_record(tmp_path: Path) -> None:
    signature = tmp_path / "Report.b2s.sig"
    signature.write_text("signature", encoding="utf-8")

    result = verify_file_with_discovery(signature, allow_inline_clearsigned=True)

    assert result.status == "unsigned"
    assert result.signature_path == str(signature)
    assert "payload not found" in result.detail.lower()


def test_flamp_auth_candidates_are_origin_and_suffix_bounded(tmp_path: Path) -> None:
    assert MessageViewerTab._is_flamp_auth_file(FileRecord(tmp_path / "Report.k2s", "flamp"))
    assert MessageViewerTab._is_flamp_auth_file(FileRecord(tmp_path / "Report.b2s", "flamp"))
    assert MessageViewerTab._is_flamp_auth_file(FileRecord(tmp_path / "Report-sig.k2s", "flamp"))
    assert MessageViewerTab._is_flamp_auth_file(FileRecord(tmp_path / "Report.sig.k2s", "flamp"))
    assert MessageViewerTab._is_flamp_auth_file(FileRecord(tmp_path / "Report.k2s.sig", "flamp"))
    assert MessageViewerTab._is_flamp_auth_file(FileRecord(tmp_path / "Report.b2s.sig", "flamp"))
    assert MessageViewerTab._is_flamp_auth_file(FileRecord(tmp_path / "Report.sig", "flamp"))

    assert not MessageViewerTab._is_flamp_auth_file(FileRecord(tmp_path / "Report.pdf", "flamp"))
    assert not MessageViewerTab._is_flamp_auth_file(FileRecord(tmp_path / "Report.k2s.old", "flamp"))
    assert not MessageViewerTab._is_flamp_auth_file(FileRecord(tmp_path / "Report.sig.txt", "flamp"))
    assert not MessageViewerTab._is_flamp_auth_file(FileRecord(tmp_path / "Report.k2s.sig", "flmsg"))
