from __future__ import annotations

import os
import subprocess
from pathlib import Path

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from freqinout.core import gpg_tools
from freqinout.core.gpg_tools import (
    DEFAULT_INLINE_SIGNED_SUFFIXES,
    clearsign_file,
    gpg_available,
    gpg_detail_indicates_passphrase_needed,
    list_secret_keys,
    resolve_gpg_executable,
    signature_payload_candidates,
    verify_file_with_discovery,
)
from freqinout.gui.message_viewer_tab import (
    ORIGIN_EXTS,
    SUPPORTED_EXT,
    FileRecord,
    FileSignatureState,
    MessageViewerTab,
)


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


def test_message_viewer_signature_detail_suppresses_raw_gpg_diagnostics() -> None:
    tab = MessageViewerTab.__new__(MessageViewerTab)
    tab._is_signature_verification_enabled = lambda: True
    tab._is_hash_verification_enabled = lambda: False
    state = FileSignatureState(
        status="invalid",
        detail=(
            "Signature verification failed (NODATA). gpg: no valid OpenPGP data found.\n"
            "gpg: block_filter: 1st length byte missing\n"
            "gpg: the signature could not be verified."
        ),
    )

    overall, detail, trusted = tab._derive_auth_ui(state)

    assert overall == "invalid"
    assert detail == "Signature: Invalid"
    assert trusted is False
    assert "gpg:" not in detail.lower()


def test_gpg_available_resolves_kleopatra_sibling_gpg(monkeypatch, tmp_path: Path) -> None:
    kleopatra = tmp_path / "kleopatra.exe"
    gpg = tmp_path / "gpg.exe"
    kleopatra.write_text("", encoding="utf-8")
    gpg.write_text("", encoding="utf-8")
    calls: list[tuple[str, list[str]]] = []

    def fake_run_gpg(gpg_path: str, args: list[str], **kwargs):
        calls.append((gpg_path, args))
        return subprocess.CompletedProcess(args=["gpg"], returncode=0, stdout="gpg (GnuPG) 2.4.0\n", stderr="")

    monkeypatch.setattr(gpg_tools, "_run_gpg", fake_run_gpg)

    assert resolve_gpg_executable(str(kleopatra)) == str(gpg)
    ok, msg, resolved = gpg_available(str(kleopatra))

    assert ok is True
    assert resolved == str(gpg)
    assert "Kleopatra is the Gpg4win key manager" in msg
    assert calls == [(str(gpg), ["--version"])]


def test_gpg_available_rejects_kleopatra_without_sibling_gpg(tmp_path: Path) -> None:
    kleopatra = tmp_path / "kleopatra.exe"
    kleopatra.write_text("", encoding="utf-8")

    ok, msg, resolved = gpg_available(str(kleopatra))

    assert ok is False
    assert resolved == ""
    assert "Select gpg.exe" in msg


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


def test_list_secret_keys_parses_primary_secret_fingerprints(monkeypatch) -> None:
    def fake_run_gpg(gpg_path: str, args: list[str], **kwargs):
        assert "--list-secret-keys" in args
        return subprocess.CompletedProcess(
            args=["gpg"],
            returncode=0,
            stdout=(
                "sec:u:4096:1:ABCDEF1234567890:1760000000:::u:::scESC:::+:::23::0:\n"
                "fpr:::::::::1234567890ABCDEF1234567890ABCDEF12345678:\n"
                "uid:u::::1760000000::hash::N1MAG <n1mag@example.org>::::::::::0:\n"
            ),
            stderr="",
        )

    monkeypatch.setattr(gpg_tools, "resolve_gpg_executable", lambda configured_path="": "/usr/bin/gpg")
    monkeypatch.setattr(gpg_tools, "_run_gpg", fake_run_gpg)

    keys, err = list_secret_keys()

    assert err == ""
    assert len(keys) == 1
    assert keys[0].fingerprint == "1234567890ABCDEF1234567890ABCDEF12345678"
    assert keys[0].user_ids == ["N1MAG <n1mag@example.org>"]


def test_clearsign_file_passes_selected_signer(monkeypatch, tmp_path: Path) -> None:
    src = tmp_path / "Report.k2s"
    dst = tmp_path / "Report-sig.k2s"
    src.write_text("payload", encoding="utf-8")
    calls: list[list[str]] = []

    def fake_run_gpg(gpg_path: str, args: list[str], **kwargs):
        calls.append(args)
        dst.write_text("signed", encoding="utf-8")
        return subprocess.CompletedProcess(args=["gpg"], returncode=0, stdout="", stderr="")

    monkeypatch.setattr(gpg_tools, "resolve_gpg_executable", lambda configured_path="": "/usr/bin/gpg")
    monkeypatch.setattr(gpg_tools, "_run_gpg", fake_run_gpg)

    ok, detail = clearsign_file(
        src,
        output_path=dst,
        signer_fingerprint="1234 5678 90ab cdef 1234 5678 90ab cdef 1234 5678",
    )

    assert ok, detail
    assert calls == [
        [
            "--pinentry-mode",
            "error",
            "--armor",
            "--clearsign",
            "--output",
            str(dst),
            "--local-user",
            "1234567890ABCDEF1234567890ABCDEF12345678",
            str(src),
        ]
    ]


def test_clearsign_file_reports_timeout_without_raw_command(monkeypatch, tmp_path: Path) -> None:
    src = tmp_path / "Report.k2s"
    dst = tmp_path / "Report-sig.k2s"
    src.write_text("payload", encoding="utf-8")

    def fake_run_gpg(gpg_path: str, args: list[str], **kwargs):
        raise subprocess.TimeoutExpired(cmd=["gpg", "--clearsign", str(src)], timeout=10.0)

    monkeypatch.setattr(gpg_tools, "resolve_gpg_executable", lambda configured_path="": "/usr/bin/gpg")
    monkeypatch.setattr(gpg_tools, "_run_gpg", fake_run_gpg)

    ok, detail = clearsign_file(src, output_path=dst)

    assert not ok
    assert "private key may require a passphrase" in detail
    assert "Command" not in detail
    assert str(src) not in detail


def test_clearsign_file_supports_passphrase_over_stdin(monkeypatch, tmp_path: Path) -> None:
    src = tmp_path / "Report.k2s"
    dst = tmp_path / "Report-sig.k2s"
    src.write_text("payload", encoding="utf-8")
    seen: dict[str, object] = {}

    def fake_run_gpg(gpg_path: str, args: list[str], **kwargs):
        seen["args"] = args
        seen["input_text"] = kwargs.get("input_text")
        dst.write_text("signed", encoding="utf-8")
        return subprocess.CompletedProcess(args=["gpg"], returncode=0, stdout="", stderr="")

    monkeypatch.setattr(gpg_tools, "resolve_gpg_executable", lambda configured_path="": "/usr/bin/gpg")
    monkeypatch.setattr(gpg_tools, "_run_gpg", fake_run_gpg)

    ok, detail = clearsign_file(src, output_path=dst, passphrase="correct horse battery staple")

    assert ok, detail
    assert seen["args"] == [
        "--pinentry-mode",
        "loopback",
        "--passphrase-fd",
        "0",
        "--armor",
        "--clearsign",
        "--output",
        str(dst),
        str(src),
    ]
    assert seen["input_text"] == "correct horse battery staple\n"
    assert "correct horse" not in " ".join(seen["args"])


def test_clearsign_file_reports_passphrase_needed(monkeypatch, tmp_path: Path) -> None:
    src = tmp_path / "Report.k2s"
    dst = tmp_path / "Report-sig.k2s"
    src.write_text("payload", encoding="utf-8")

    def fake_run_gpg(gpg_path: str, args: list[str], **kwargs):
        return subprocess.CompletedProcess(
            args=["gpg"],
            returncode=2,
            stdout="",
            stderr="gpg: signing failed: No pinentry",
        )

    monkeypatch.setattr(gpg_tools, "resolve_gpg_executable", lambda configured_path="": "/usr/bin/gpg")
    monkeypatch.setattr(gpg_tools, "_run_gpg", fake_run_gpg)

    ok, detail = clearsign_file(src, output_path=dst)

    assert not ok
    assert detail == "Clearsign requires the selected key passphrase."
    assert gpg_detail_indicates_passphrase_needed(detail)


def test_auth_candidates_are_origin_and_suffix_bounded(tmp_path: Path) -> None:
    for origin in ("flamp", "varac", "bbs"):
        assert MessageViewerTab._is_auth_verifiable_file(FileRecord(tmp_path / "Report.k2s", origin))
        assert MessageViewerTab._is_auth_verifiable_file(FileRecord(tmp_path / "Report.b2s", origin))
        assert MessageViewerTab._is_auth_verifiable_file(FileRecord(tmp_path / "Report-sig.k2s", origin))
        assert MessageViewerTab._is_auth_verifiable_file(FileRecord(tmp_path / "Report.sig.k2s", origin))
        assert MessageViewerTab._is_auth_verifiable_file(FileRecord(tmp_path / "Report.k2s.sig", origin))
        assert MessageViewerTab._is_auth_verifiable_file(FileRecord(tmp_path / "Report.b2s.sig", origin))
        assert MessageViewerTab._is_auth_verifiable_file(FileRecord(tmp_path / "Report.sig", origin))

    assert not MessageViewerTab._is_auth_verifiable_file(FileRecord(tmp_path / "Report.pdf", "varac"))
    assert not MessageViewerTab._is_auth_verifiable_file(FileRecord(tmp_path / "Report.k2s.old", "varac"))
    assert not MessageViewerTab._is_auth_verifiable_file(FileRecord(tmp_path / "Report.sig.txt", "varac"))
    assert not MessageViewerTab._is_auth_verifiable_file(FileRecord(tmp_path / "Report.k2s.sig", "flmsg"))


def test_varac_and_bbs_scanners_allow_signature_sidecars() -> None:
    for ext in (".sig", ".asc", ".gpg"):
        assert ext in SUPPORTED_EXT
        assert ext in ORIGIN_EXTS["varac"]
        assert ext in ORIGIN_EXTS["bbs"]
