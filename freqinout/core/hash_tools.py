from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple


@dataclass
class HashVerifyResult:
    status: str  # valid | invalid | unsigned | error
    detail: str
    algorithm: str = ""
    expected_hash: str = ""
    actual_hash: str = ""
    checksum_path: str = ""
    source: str = ""  # sidecar | registry
    entry_label: str = ""


_SIDE_EXTS: Tuple[str, ...] = (
    ".sha256",
    ".sha512",
    ".sha1",
    ".md5",
    ".sha",
    ".checksum",
    ".hash",
)

_HASH_LEN_ALGO = {
    32: "md5",
    40: "sha1",
    64: "sha256",
    128: "sha512",
}

_ALGO_PRIORITY = {
    "sha256": 0,
    "sha512": 1,
    "sha1": 2,
    "md5": 3,
    "": 9,
}


def normalize_hash_hex(value: str) -> str:
    return re.sub(r"[^A-F0-9]", "", str(value or "").upper())


def normalize_hash_algorithm(value: str) -> str:
    txt = str(value or "").strip().lower().replace("-", "")
    if txt in {"sha256", "sha512", "sha1", "md5"}:
        return txt
    return ""


def normalize_trusted_hash_entries(entries: Iterable[dict]) -> List[dict]:
    out: List[dict] = []
    seen: set[tuple[str, str]] = set()
    for row in entries or []:
        if not isinstance(row, dict):
            continue
        enabled = bool(row.get("enabled", True))
        hash_norm = normalize_hash_hex(str(row.get("hash", "") or row.get("value", "") or ""))
        if not hash_norm:
            continue
        algo = normalize_hash_algorithm(str(row.get("algorithm", "") or ""))
        if not algo:
            algo = infer_algorithm_from_hash(hash_norm)
        if not algo:
            continue
        key = (algo, hash_norm)
        if key in seen:
            continue
        seen.add(key)
        out.append(
            {
                "enabled": enabled,
                "algorithm": algo,
                "hash": hash_norm,
                "label": str(row.get("label", "") or "").strip(),
            }
        )
    out.sort(key=lambda d: (_ALGO_PRIORITY.get(str(d.get("algorithm", "")), 9), str(d.get("hash", ""))))
    return out


def checksum_sidecar_candidates(file_path: Path) -> List[Path]:
    file_ref = Path(file_path)
    name = file_ref.name
    stem = file_ref.stem
    out: List[Path] = []
    seen: set[str] = set()
    for ext in _SIDE_EXTS:
        for cand in (
            file_ref.with_name(name + ext),
            file_ref.with_suffix(ext),
            file_ref.with_name(stem + ext),
        ):
            key = str(cand)
            if key in seen:
                continue
            seen.add(key)
            out.append(cand)
    return out


def existing_checksum_sidecars(file_path: Path) -> List[Path]:
    cands = []
    for cand in checksum_sidecar_candidates(file_path):
        try:
            if cand.exists() and cand.is_file():
                cands.append(cand)
        except Exception:
            continue

    def rank(path_obj: Path) -> tuple[int, str]:
        algo = infer_algorithm_from_path(path_obj)
        return (_ALGO_PRIORITY.get(algo, 8), str(path_obj))

    cands.sort(key=rank)
    return cands


def infer_algorithm_from_path(path_obj: Path) -> str:
    name = str(path_obj.name or "").lower()
    for token, algo in (
        ("sha256", "sha256"),
        ("sha512", "sha512"),
        ("sha-256", "sha256"),
        ("sha-512", "sha512"),
        ("sha1", "sha1"),
        ("sha-1", "sha1"),
        ("md5", "md5"),
    ):
        if token in name:
            return algo
    return ""


def infer_algorithm_from_hash(value: str) -> str:
    return _HASH_LEN_ALGO.get(len(normalize_hash_hex(value)), "")


def compute_file_hash(file_path: Path, algorithm: str) -> str:
    algo = normalize_hash_algorithm(algorithm)
    if not algo:
        raise ValueError(f"Unsupported hash algorithm: {algorithm}")
    hasher = hashlib.new(algo)
    with Path(file_path).open("rb") as fh:
        while True:
            chunk = fh.read(1024 * 1024)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest().upper()


def _target_name_matches(raw_name: str, target_file: Path) -> bool:
    txt = str(raw_name or "").strip().strip("\"'")
    if not txt:
        return True
    normalized = txt.replace("\\", "/")
    base = Path(normalized).name.lower()
    target = Path(target_file)
    target_name = target.name.lower()
    target_base = target.stem.lower()
    if base == target_name or base == target_base:
        return True
    if normalized.lower() in {
        str(target).replace("\\", "/").lower(),
        str(target.resolve()).replace("\\", "/").lower(),
    }:
        return True
    return False


def parse_checksum_sidecar(sidecar_path: Path, target_file: Path) -> Tuple[str, str, str]:
    """
    Returns (expected_hash, algorithm, detail).
    expected_hash and algorithm are empty on parse failure.
    """
    sidecar = Path(sidecar_path)
    target = Path(target_file)
    try:
        raw_text = sidecar.read_text(encoding="utf-8", errors="replace")
    except Exception as e:
        return "", "", f"Failed to read checksum sidecar: {e}"

    explicit_algo = infer_algorithm_from_path(sidecar)
    lines = [ln.strip() for ln in raw_text.splitlines() if ln.strip()]
    if not lines:
        return "", "", "Checksum sidecar is empty."

    for line in lines:
        if line.startswith("#") or line.startswith(";"):
            continue

        # BSD style: SHA256(filename)=hash
        m = re.match(
            r"^\s*(MD5|SHA1|SHA-1|SHA256|SHA-256|SHA512|SHA-512)\s*\((.+?)\)\s*=\s*([A-Fa-f0-9]{32,128})\s*$",
            line,
            flags=re.IGNORECASE,
        )
        if m:
            algo = normalize_hash_algorithm(m.group(1))
            fname = m.group(2)
            expected = normalize_hash_hex(m.group(3))
            if _target_name_matches(fname, target) and expected:
                algo = algo or explicit_algo or infer_algorithm_from_hash(expected)
                return expected, algo, ""
            continue

        # GNU style: hash [* ]filename
        m = re.match(r"^\s*([A-Fa-f0-9]{32,128})\s+[* ]?(.+?)\s*$", line, flags=re.IGNORECASE)
        if m:
            expected = normalize_hash_hex(m.group(1))
            fname = m.group(2)
            if _target_name_matches(fname, target) and expected:
                algo = explicit_algo or infer_algorithm_from_hash(expected)
                return expected, algo, ""
            continue

        # Key/value style: filename: hash
        m = re.match(r"^\s*(.+?)\s*[:=,]\s*([A-Fa-f0-9]{32,128})\s*$", line, flags=re.IGNORECASE)
        if m:
            fname = m.group(1)
            expected = normalize_hash_hex(m.group(2))
            if _target_name_matches(fname, target) and expected:
                algo = explicit_algo or infer_algorithm_from_hash(expected)
                return expected, algo, ""
            continue

        # Single hash line (sidecar dedicated to this file)
        m = re.match(r"^\s*([A-Fa-f0-9]{32,128})\s*$", line, flags=re.IGNORECASE)
        if m:
            expected = normalize_hash_hex(m.group(1))
            if expected:
                algo = explicit_algo or infer_algorithm_from_hash(expected)
                return expected, algo, ""

    return "", "", "No checksum entry matched this file."


def verify_file_hash_with_discovery(file_path: Path) -> HashVerifyResult:
    file_ref = Path(file_path)
    if not file_ref.exists() or not file_ref.is_file():
        return HashVerifyResult(status="error", detail="Message file not found.")

    sidecars = existing_checksum_sidecars(file_ref)
    if not sidecars:
        return HashVerifyResult(status="unsigned", detail="No checksum sidecar found.")

    parse_errors: List[str] = []
    for sidecar in sidecars:
        expected, algo, err = parse_checksum_sidecar(sidecar, file_ref)
        if err:
            parse_errors.append(f"{sidecar.name}: {err}")
            continue
        if not expected:
            parse_errors.append(f"{sidecar.name}: checksum missing")
            continue
        algo_norm = normalize_hash_algorithm(algo) or infer_algorithm_from_hash(expected)
        if not algo_norm:
            parse_errors.append(f"{sidecar.name}: unsupported checksum format")
            continue
        try:
            actual = compute_file_hash(file_ref, algo_norm)
        except Exception as e:
            return HashVerifyResult(
                status="error",
                detail=f"Failed to compute {algo_norm.upper()} hash: {e}",
                algorithm=algo_norm,
                checksum_path=str(sidecar),
                expected_hash=expected,
                source="sidecar",
            )
        if actual == expected:
            return HashVerifyResult(
                status="valid",
                detail=f"Checksum valid ({algo_norm.upper()}).",
                algorithm=algo_norm,
                expected_hash=expected,
                actual_hash=actual,
                checksum_path=str(sidecar),
                source="sidecar",
            )
        return HashVerifyResult(
            status="invalid",
            detail=f"Checksum mismatch ({algo_norm.upper()}).",
            algorithm=algo_norm,
            expected_hash=expected,
            actual_hash=actual,
            checksum_path=str(sidecar),
            source="sidecar",
        )

    if parse_errors:
        return HashVerifyResult(
            status="error",
            detail=parse_errors[0],
            checksum_path=str(sidecars[0]),
            source="sidecar",
        )
    return HashVerifyResult(status="unsigned", detail="No usable checksum sidecar found.", source="sidecar")


def verify_file_hash_against_registry(file_path: Path, entries: Iterable[dict]) -> HashVerifyResult:
    file_ref = Path(file_path)
    if not file_ref.exists() or not file_ref.is_file():
        return HashVerifyResult(status="error", detail="Message file not found.", source="registry")

    normalized = [r for r in normalize_trusted_hash_entries(entries) if bool(r.get("enabled", True))]
    if not normalized:
        return HashVerifyResult(status="unsigned", detail="No local trusted hashes configured.", source="registry")

    by_algo: Dict[str, List[dict]] = {}
    for row in normalized:
        algo = str(row.get("algorithm", "") or "")
        if not algo:
            continue
        by_algo.setdefault(algo, []).append(row)
    if not by_algo:
        return HashVerifyResult(status="unsigned", detail="No usable local trusted hashes.", source="registry")

    for algo in sorted(by_algo.keys(), key=lambda a: _ALGO_PRIORITY.get(a, 9)):
        try:
            actual = compute_file_hash(file_ref, algo)
        except Exception as e:
            return HashVerifyResult(
                status="error",
                detail=f"Failed to compute {algo.upper()} hash: {e}",
                algorithm=algo,
                source="registry",
            )
        for row in by_algo.get(algo, []):
            expected = normalize_hash_hex(str(row.get("hash", "") or ""))
            if expected and actual == expected:
                return HashVerifyResult(
                    status="valid",
                    detail=f"Matched local trusted hash ({algo.upper()}).",
                    algorithm=algo,
                    expected_hash=expected,
                    actual_hash=actual,
                    source="registry",
                    entry_label=str(row.get("label", "") or "").strip(),
                )
    return HashVerifyResult(
        status="unsigned",
        detail="No local trusted hash match.",
        source="registry",
    )
