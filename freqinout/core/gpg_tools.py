from __future__ import annotations

import os
import re
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Set, Tuple

from freqinout.core.logger import log


@dataclass
class GPGKeyInfo:
    fingerprint: str
    user_ids: List[str]
    key_id: str = ""
    trust: str = ""


@dataclass
class SignatureResult:
    status: str  # valid | invalid | unsigned | error
    detail: str
    signer_fingerprint: str = ""
    signer_uid: str = ""
    trusted: bool = False
    signature_path: str = ""
    gpg_status: str = ""


DEFAULT_INLINE_SIGNED_SUFFIXES: Tuple[str, ...] = ("-sig.k2s", "-sig.b2s", ".sig.k2s", ".sig.b2s")
DETACHED_SIGNATURE_SUFFIXES: Tuple[str, ...] = (".sig", ".asc", ".gpg")
FLAMP_PAYLOAD_SUFFIXES: Tuple[str, ...] = (".k2s", ".b2s")
_PGP_CLEARSIGNED_HEADER = b"-----BEGIN PGP SIGNED MESSAGE-----"


def normalize_fingerprint(value: str) -> str:
    return re.sub(r"[^A-F0-9]", "", str(value or "").upper())


def normalize_fingerprints(values: Iterable[str]) -> Set[str]:
    out: Set[str] = set()
    for value in values:
        norm = normalize_fingerprint(value)
        if norm:
            out.add(norm)
    return out


def normalize_signature_name_suffixes(values: Iterable[str]) -> List[str]:
    out: List[str] = []
    seen: Set[str] = set()
    for value in values:
        suffix = str(value or "").strip().lower()
        if not suffix:
            continue
        if suffix in seen:
            continue
        seen.add(suffix)
        out.append(suffix)
    return out


def _file_name_matches_suffixes(file_path: Path, suffixes: Iterable[str]) -> bool:
    name = str(Path(file_path).name or "").strip().lower()
    if not name:
        return False
    for suffix in suffixes:
        sfx = str(suffix or "").strip().lower()
        if sfx and name.endswith(sfx):
            return True
    return False


def _looks_like_clearsigned(file_path: Path, *, max_scan_bytes: int = 65536) -> bool:
    try:
        with Path(file_path).open("rb") as fh:
            head = fh.read(max(1024, int(max_scan_bytes)))
    except Exception:
        return False
    return _PGP_CLEARSIGNED_HEADER in head


def is_detached_signature_file(file_path: Path) -> bool:
    return str(Path(file_path).suffix or "").strip().lower() in DETACHED_SIGNATURE_SUFFIXES


def resolve_gpg_executable(configured_path: str = "") -> Optional[str]:
    cand = str(configured_path or "").strip()
    if cand:
        p = Path(cand)
        if p.exists():
            return str(p)
        resolved = shutil.which(cand)
        if resolved:
            return str(resolved)

    for name in ("gpg", "gpg2"):
        resolved = shutil.which(name)
        if resolved:
            return str(resolved)

    if os.name == "nt":
        common_dirs = []
        for env in ("ProgramFiles", "ProgramFiles(x86)"):
            base = os.environ.get(env, "")
            if base:
                common_dirs.append(Path(base) / "GnuPG" / "bin" / "gpg.exe")
        for p in common_dirs:
            if p.exists():
                return str(p)
    return None


def gpg_available(configured_path: str = "", gnupg_home: str = "") -> Tuple[bool, str, str]:
    gpg_path = resolve_gpg_executable(configured_path)
    if not gpg_path:
        return False, "GPG executable not found.", ""
    try:
        cp = _run_gpg(
            gpg_path,
            ["--version"],
            gnupg_home=gnupg_home,
            timeout_sec=8.0,
        )
    except Exception as e:
        return False, f"GPG check failed: {e}", gpg_path
    if cp.returncode != 0:
        detail = (cp.stderr or cp.stdout or "").strip() or f"exit code {cp.returncode}"
        return False, f"GPG check failed: {detail}", gpg_path
    first_line = ""
    for line in (cp.stdout or "").splitlines():
        if line.strip():
            first_line = line.strip()
            break
    if not first_line:
        first_line = "GPG available"
    return True, first_line, gpg_path


def list_public_keys(configured_path: str = "", gnupg_home: str = "") -> Tuple[List[GPGKeyInfo], str]:
    gpg_path = resolve_gpg_executable(configured_path)
    if not gpg_path:
        return [], "GPG executable not found."
    try:
        cp = _run_gpg(
            gpg_path,
            ["--with-colons", "--list-keys", "--fingerprint", "--fingerprint"],
            gnupg_home=gnupg_home,
            timeout_sec=15.0,
        )
    except Exception as e:
        return [], f"Failed to list keys: {e}"
    if cp.returncode != 0:
        detail = (cp.stderr or cp.stdout or "").strip() or f"exit code {cp.returncode}"
        return [], f"Failed to list keys: {detail}"

    keys: List[GPGKeyInfo] = []
    current: Optional[GPGKeyInfo] = None
    saw_primary_fpr = False
    for raw_line in (cp.stdout or "").splitlines():
        parts = raw_line.split(":")
        if not parts:
            continue
        rec_type = parts[0]
        if rec_type == "pub":
            if current and current.fingerprint:
                keys.append(current)
            trust = parts[1] if len(parts) > 1 else ""
            key_id = parts[4] if len(parts) > 4 else ""
            current = GPGKeyInfo(fingerprint="", user_ids=[], key_id=key_id, trust=trust)
            saw_primary_fpr = False
        elif rec_type == "fpr":
            if not current:
                continue
            fpr = parts[9] if len(parts) > 9 else ""
            fpr_norm = normalize_fingerprint(fpr)
            if fpr_norm and not saw_primary_fpr:
                current.fingerprint = fpr_norm
                saw_primary_fpr = True
        elif rec_type == "uid":
            if not current:
                continue
            uid = parts[9] if len(parts) > 9 else ""
            uid = str(uid or "").strip()
            if uid:
                current.user_ids.append(uid)

    if current and current.fingerprint:
        keys.append(current)

    keys.sort(key=lambda k: (k.user_ids[0] if k.user_ids else "", k.fingerprint))
    return keys, ""


def import_public_key_file(file_path: str, configured_path: str = "", gnupg_home: str = "") -> Tuple[bool, str]:
    gpg_path = resolve_gpg_executable(configured_path)
    if not gpg_path:
        return False, "GPG executable not found."
    src = Path(str(file_path or "").strip())
    if not src.exists() or not src.is_file():
        return False, "Key file not found."
    try:
        cp = _run_gpg(
            gpg_path,
            ["--import", str(src)],
            gnupg_home=gnupg_home,
            timeout_sec=20.0,
        )
    except Exception as e:
        return False, f"Key import failed: {e}"
    if cp.returncode != 0:
        detail = (cp.stderr or cp.stdout or "").strip() or f"exit code {cp.returncode}"
        return False, f"Key import failed: {detail}"
    detail = (cp.stderr or cp.stdout or "").strip() or "Key imported."
    return True, detail


def import_public_key_text(armored_key: str, configured_path: str = "", gnupg_home: str = "") -> Tuple[bool, str]:
    gpg_path = resolve_gpg_executable(configured_path)
    if not gpg_path:
        return False, "GPG executable not found."
    payload = str(armored_key or "").strip()
    if not payload:
        return False, "No key text provided."
    try:
        cp = _run_gpg(
            gpg_path,
            ["--import"],
            gnupg_home=gnupg_home,
            input_text=payload,
            timeout_sec=20.0,
        )
    except Exception as e:
        return False, f"Key import failed: {e}"
    if cp.returncode != 0:
        detail = (cp.stderr or cp.stdout or "").strip() or f"exit code {cp.returncode}"
        return False, f"Key import failed: {detail}"
    detail = (cp.stderr or cp.stdout or "").strip() or "Key imported."
    return True, detail


def local_sign_key(fingerprint: str, configured_path: str = "", gnupg_home: str = "") -> Tuple[bool, str]:
    gpg_path = resolve_gpg_executable(configured_path)
    if not gpg_path:
        return False, "GPG executable not found."
    fpr = normalize_fingerprint(fingerprint)
    if not fpr:
        return False, "Missing key fingerprint."
    try:
        cp = _run_gpg(
            gpg_path,
            ["--quick-lsign-key", fpr],
            gnupg_home=gnupg_home,
            timeout_sec=30.0,
        )
    except Exception as e:
        return False, f"Local-sign failed: {e}"
    if cp.returncode != 0:
        detail = (cp.stderr or cp.stdout or "").strip() or f"exit code {cp.returncode}"
        return False, f"Local-sign failed: {detail}"
    detail = (cp.stderr or cp.stdout or "").strip() or "Key locally signed."
    return True, detail


def clearsign_file(
    file_path: str | Path,
    *,
    output_path: str | Path,
    configured_path: str = "",
    gnupg_home: str = "",
) -> Tuple[bool, str]:
    gpg_path = resolve_gpg_executable(configured_path)
    if not gpg_path:
        return False, "GPG executable not found."
    src = Path(str(file_path or "").strip())
    dst = Path(str(output_path or "").strip())
    if not src.exists() or not src.is_file():
        return False, "Message file not found."
    if not str(dst):
        return False, "Missing output path."
    try:
        cp = _run_gpg(
            gpg_path,
            ["--armor", "--clearsign", "--output", str(dst), str(src)],
            gnupg_home=gnupg_home,
            timeout_sec=45.0,
        )
    except Exception as e:
        return False, f"Clearsign failed: {e}"
    if cp.returncode != 0:
        detail = (cp.stderr or cp.stdout or "").strip() or f"exit code {cp.returncode}"
        return False, f"Clearsign failed: {detail}"
    if not dst.exists():
        return False, "Clearsign did not create an output file."
    detail = (cp.stderr or cp.stdout or "").strip() or f"Signed file created: {dst}"
    return True, detail


def signature_candidates(file_path: Path) -> List[Path]:
    out: List[Path] = []
    name = file_path.name
    stem = file_path.stem
    for ext in DETACHED_SIGNATURE_SUFFIXES:
        out.append(file_path.with_name(name + ext))
    for ext in DETACHED_SIGNATURE_SUFFIXES:
        out.append(file_path.with_suffix(ext))
    for ext in DETACHED_SIGNATURE_SUFFIXES:
        out.append(file_path.with_name(stem + ext))
    dedup: List[Path] = []
    seen: Set[str] = set()
    for cand in out:
        key = str(cand)
        if key in seen:
            continue
        seen.add(key)
        dedup.append(cand)
    return dedup


def signature_payload_candidates(signature_path: Path) -> List[Path]:
    sig_ref = Path(signature_path)
    if not is_detached_signature_file(sig_ref):
        return []
    base = sig_ref.with_suffix("")
    out: List[Path] = []
    if str(base.suffix or "").strip().lower() in FLAMP_PAYLOAD_SUFFIXES:
        out.append(base)
    else:
        for ext in FLAMP_PAYLOAD_SUFFIXES:
            out.append(base.with_name(base.name + ext))
        out.append(base)
    dedup: List[Path] = []
    seen: Set[str] = set()
    for cand in out:
        key = str(cand)
        if key in seen:
            continue
        seen.add(key)
        dedup.append(cand)
    return dedup


def find_payload_for_signature(signature_path: Path) -> Optional[Path]:
    for cand in signature_payload_candidates(signature_path):
        try:
            if cand.exists() and cand.is_file():
                return cand
        except Exception:
            continue
    return None


def find_detached_signature(file_path: Path) -> Optional[Path]:
    for cand in signature_candidates(file_path):
        try:
            if cand.exists() and cand.is_file():
                return cand
        except Exception:
            continue
    return None


def verify_file_with_discovery(
    file_path: Path,
    *,
    configured_path: str = "",
    gnupg_home: str = "",
    trusted_fingerprints: Optional[Iterable[str]] = None,
    allow_inline_clearsigned: bool = False,
    inline_name_suffixes: Optional[Iterable[str]] = None,
) -> SignatureResult:
    file_ref = Path(file_path)
    if not file_ref.exists() or not file_ref.is_file():
        return SignatureResult(status="error", detail="Message file not found.")

    if allow_inline_clearsigned and _looks_like_clearsigned(file_ref):
        return verify_inline_clearsigned(
            file_ref,
            configured_path=configured_path,
            gnupg_home=gnupg_home,
            trusted_fingerprints=trusted_fingerprints,
        )

    if is_detached_signature_file(file_ref):
        payload = find_payload_for_signature(file_ref)
        if payload:
            result = verify_detached_signature(
                payload,
                file_ref,
                configured_path=configured_path,
                gnupg_home=gnupg_home,
                trusted_fingerprints=trusted_fingerprints,
            )
            result.signature_path = str(file_ref)
            return result
        return SignatureResult(
            status="unsigned",
            detail="Detached signature payload not found.",
            signature_path=str(file_ref),
        )

    sig = find_detached_signature(file_ref)
    if sig:
        result = verify_detached_signature(
            file_ref,
            sig,
            configured_path=configured_path,
            gnupg_home=gnupg_home,
            trusted_fingerprints=trusted_fingerprints,
        )
        result.signature_path = str(sig)
        return result

    if allow_inline_clearsigned:
        suffixes = normalize_signature_name_suffixes(
            inline_name_suffixes if inline_name_suffixes is not None else DEFAULT_INLINE_SIGNED_SUFFIXES
        )
        if suffixes and _file_name_matches_suffixes(file_ref, suffixes):
            return SignatureResult(status="unsigned", detail="No embedded PGP clearsigned content found.")

    return SignatureResult(status="unsigned", detail="No detached signature found.")


def verify_detached_signature(
    file_path: Path,
    signature_path: Path,
    *,
    configured_path: str = "",
    gnupg_home: str = "",
    trusted_fingerprints: Optional[Iterable[str]] = None,
) -> SignatureResult:
    gpg_path = resolve_gpg_executable(configured_path)
    if not gpg_path:
        return SignatureResult(status="error", detail="GPG executable not found.")
    file_ref = Path(file_path)
    sig_ref = Path(signature_path)
    if not file_ref.exists() or not file_ref.is_file():
        return SignatureResult(status="error", detail="Message file not found.")
    if not sig_ref.exists() or not sig_ref.is_file():
        return SignatureResult(status="unsigned", detail="Detached signature file not found.")

    try:
        cp = _run_gpg(
            gpg_path,
            ["--status-fd=1", "--verify", str(sig_ref), str(file_ref)],
            gnupg_home=gnupg_home,
            timeout_sec=25.0,
        )
    except Exception as e:
        return SignatureResult(status="error", detail=f"Verification failed: {e}", signature_path=str(sig_ref))
    return _parse_gpg_verify_result(
        cp,
        trusted_fingerprints=trusted_fingerprints,
        signature_path=str(sig_ref),
    )


def verify_inline_clearsigned(
    file_path: Path,
    *,
    configured_path: str = "",
    gnupg_home: str = "",
    trusted_fingerprints: Optional[Iterable[str]] = None,
) -> SignatureResult:
    gpg_path = resolve_gpg_executable(configured_path)
    if not gpg_path:
        return SignatureResult(status="error", detail="GPG executable not found.")
    file_ref = Path(file_path)
    if not file_ref.exists() or not file_ref.is_file():
        return SignatureResult(status="error", detail="Message file not found.")
    try:
        cp = _run_gpg(
            gpg_path,
            ["--status-fd=1", "--verify", str(file_ref)],
            gnupg_home=gnupg_home,
            timeout_sec=25.0,
        )
    except Exception as e:
        return SignatureResult(status="error", detail=f"Verification failed: {e}")
    return _parse_gpg_verify_result(
        cp,
        trusted_fingerprints=trusted_fingerprints,
        signature_path="",
    )


def _parse_gpg_verify_result(
    cp: subprocess.CompletedProcess[str],
    *,
    trusted_fingerprints: Optional[Iterable[str]] = None,
    signature_path: str = "",
) -> SignatureResult:
    signer_fpr = ""
    signer_uid = ""
    bad_tag = ""
    trust_tags: Set[str] = set()
    status_lines: List[str] = []
    for line in (cp.stdout or "").splitlines():
        if not line.startswith("[GNUPG:]"):
            continue
        status_lines.append(line)
        parts = line.split()
        if len(parts) < 2:
            continue
        tag = parts[1]
        if tag == "GOODSIG":
            if len(parts) > 3:
                signer_uid = " ".join(parts[3:]).strip()
        elif tag == "VALIDSIG":
            if len(parts) > 2:
                signer_fpr = normalize_fingerprint(parts[2])
        elif tag in {"BADSIG", "ERRSIG", "NO_PUBKEY", "NODATA", "EXPSIG", "EXPKEYSIG", "REVKEYSIG"}:
            bad_tag = tag
        elif tag.startswith("TRUST_"):
            trust_tags.add(tag)

    trust_norm = normalize_fingerprints(trusted_fingerprints or [])
    app_trusted = bool(signer_fpr and signer_fpr in trust_norm)
    gpg_trusted = bool(trust_tags.intersection({"TRUST_FULLY", "TRUST_ULTIMATE", "TRUST_MARGINAL"}))
    trusted = bool(app_trusted or gpg_trusted)

    if bad_tag:
        detail = f"Signature verification failed ({bad_tag})."
        if cp.stderr:
            detail = f"{detail} {(cp.stderr or '').strip()}"
        return SignatureResult(
            status="invalid",
            detail=detail.strip(),
            signer_fingerprint=signer_fpr,
            signer_uid=signer_uid,
            trusted=trusted,
            signature_path=str(signature_path or ""),
            gpg_status=" ".join(sorted(trust_tags)),
        )

    valid_by_gpg = cp.returncode == 0 and bool(signer_fpr or signer_uid)
    if valid_by_gpg:
        if trusted:
            detail = "Signature valid (trusted signer)."
        else:
            detail = "Signature valid (signer not in trusted list)."
        return SignatureResult(
            status="valid",
            detail=detail,
            signer_fingerprint=signer_fpr,
            signer_uid=signer_uid,
            trusted=trusted,
            signature_path=str(signature_path or ""),
            gpg_status=" ".join(sorted(trust_tags)),
        )

    stderr = (cp.stderr or "").strip()
    detail = stderr or "Verification failed."
    if not detail and status_lines:
        detail = status_lines[-1]
    return SignatureResult(
        status="error",
        detail=detail,
        signer_fingerprint=signer_fpr,
        signer_uid=signer_uid,
        trusted=trusted,
        signature_path=str(signature_path or ""),
        gpg_status=" ".join(sorted(trust_tags)),
    )


def _run_gpg(
    gpg_path: str,
    args: Sequence[str],
    *,
    gnupg_home: str = "",
    input_text: Optional[str] = None,
    timeout_sec: float = 20.0,
) -> subprocess.CompletedProcess[str]:
    cmd: List[str] = [str(gpg_path)]
    home = str(gnupg_home or "").strip()
    if home:
        cmd.extend(["--homedir", home])
    cmd.extend(["--batch", "--yes", "--no-tty"])
    cmd.extend([str(a) for a in args])
    log.debug("GPG: run %s", " ".join(cmd))
    run_kwargs = {}
    if os.name == "nt":
        try:
            creationflags = int(getattr(subprocess, "CREATE_NO_WINDOW", 0) or 0)
            if creationflags:
                run_kwargs["creationflags"] = creationflags
        except Exception:
            pass
        try:
            startupinfo = subprocess.STARTUPINFO()
            startupinfo.dwFlags |= getattr(subprocess, "STARTF_USESHOWWINDOW", 0)
            startupinfo.wShowWindow = 0
            run_kwargs["startupinfo"] = startupinfo
        except Exception:
            pass
    return subprocess.run(
        cmd,
        input=input_text,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
        timeout=max(1.0, float(timeout_sec)),
        **run_kwargs,
    )
