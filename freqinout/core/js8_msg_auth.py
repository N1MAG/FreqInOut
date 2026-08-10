from __future__ import annotations

import datetime as dt
import hashlib
import re
import secrets
from dataclasses import dataclass
from typing import Iterable, Optional


BASE36_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
CRC_RE = re.compile(r"^[A-Z0-9]{3}$")
DATECODE_RE = re.compile(r"^#[A-Z0-9]{4}$")


@dataclass(frozen=True)
class MsgAuthKey:
    label: str
    key: str
    scope: str = ""
    scope_value: str = ""
    enabled: bool = True


@dataclass(frozen=True)
class MsgAuthSignature:
    canonical_message: str
    message_text: str
    crc: str
    datecode: str = ""
    signed_text: str = ""


@dataclass(frozen=True)
class MsgAuthVerification:
    state: str
    expected_crc: str = ""
    provided_crc: str = ""
    key_label: str = ""
    detail: str = ""
    datecode: str = ""
    decoded_datecode: str = ""

    @property
    def verified(self) -> bool:
        return self.state == "verified"


def normalize_callsign(value: object) -> str:
    return str(value or "").strip().upper()


def normalize_auth_text(value: object) -> str:
    text = str(value or "").upper()
    text = re.sub(r"[\x00-\x1f]", " ", text)
    text = re.sub(r"\s{2,}", " ", text.strip())
    return text


def canonicalize_js8_auth_message(from_call: object, target: object, message: object) -> str:
    source = normalize_callsign(from_call)
    dest = normalize_callsign(target)
    body = normalize_auth_text(message)
    return f"{source}: {dest} {body}"


def checksum(message: object, codekey: object) -> str:
    """
    Return a KF7MIX MsgAuth-compatible 3-character checksum.

    This intentionally follows the Message Authenticator v0.92 algorithm:
    SHA-256(message + key), SHA-256(hex digest), base36 alphabet
    A-Z0-9, last three characters.
    """
    key = str(codekey or "")
    out = hashlib.sha256((str(message or "") + key).encode("utf-8")).hexdigest()
    decimal_value = int(hashlib.sha256(out.encode("utf-8")).hexdigest(), 16)
    encoded = ""
    while decimal_value > 0:
        encoded = BASE36_CHARS[decimal_value % 36] + encoded
        decimal_value //= 36
    return encoded[-3:]


def generate_msg_auth_secret_key(*, length: int = 25) -> str:
    key_len = max(12, min(64, int(length or 25)))
    return "".join(secrets.choice(BASE36_CHARS) for _ in range(key_len))


def encode_short_datecode(moment: Optional[dt.datetime] = None) -> str:
    now = moment or dt.datetime.now()
    if now.tzinfo is not None:
        now = now.astimezone().replace(tzinfo=None)
    month = chr(int(now.month) + 64)
    day_num = int(now.day)
    day = chr(day_num + 64) if day_num < 27 else chr((day_num - 26) + 47)
    hour = chr(int(now.hour) + 65)
    minute_code = int(int(now.minute) / 2) + 1 + 64
    if minute_code > 90:
        minute_code -= 43
    return "#" + month + day + hour + chr(minute_code)


def decode_short_datecode(token: object) -> str:
    stamp = str(token or "").strip().upper()
    if not DATECODE_RE.match(stamp):
        return ""
    month = ""
    month_ord = ord(stamp[1])
    if 64 < month_ord < 77:
        month = str(month_ord - 64)
    day = ""
    day_ord = ord(stamp[2])
    if 47 < day_ord < 53:
        day = str((day_ord - 47) + 26)
    if 64 < day_ord < 91:
        day = str(day_ord - 64)
    hour = ""
    hour_ord = ord(stamp[3])
    if 64 < hour_ord < 88:
        hour = str(hour_ord - 65)
    minute = ""
    minute_ord = ord(stamp[4])
    if 47 < minute_ord < 52:
        minute = str(((minute_ord - 48) + 26) * 2).zfill(2)
    if 64 < minute_ord < 91:
        minute = str((minute_ord - 65) * 2).zfill(2)
    if not month or not day or not hour or not minute:
        return ""
    return f"{month}/{day} {hour}:{minute}"


def sign_js8_text(
    from_call: object,
    target: object,
    message: object,
    key: object,
    *,
    include_datecode: bool = False,
    moment: Optional[dt.datetime] = None,
    datecode: object = "",
) -> MsgAuthSignature:
    body = normalize_auth_text(message)
    fixed_datecode = str(datecode or "").strip().upper()
    if fixed_datecode and not DATECODE_RE.match(fixed_datecode):
        fixed_datecode = ""
    auth_datecode = fixed_datecode or (encode_short_datecode(moment) if include_datecode else "")
    message_for_auth = f"{body} {auth_datecode}".strip() if auth_datecode else body
    canonical = canonicalize_js8_auth_message(from_call, target, message_for_auth)
    crc = checksum(canonical, key)
    signed_text = f"{message_for_auth} {crc}".strip()
    return MsgAuthSignature(
        canonical_message=canonical,
        message_text=message_for_auth,
        crc=crc,
        datecode=auth_datecode,
        signed_text=signed_text,
    )


def parse_trailing_crc(message: object) -> tuple[str, str]:
    text = normalize_auth_text(message)
    parts = text.rsplit(" ", 1)
    if len(parts) != 2:
        return text, ""
    body, tail = parts[0].strip(), parts[1].strip().upper()
    if CRC_RE.match(tail):
        return body, tail
    return text, ""


def _key_rows(keys: Iterable[MsgAuthKey | dict | tuple | str]) -> list[MsgAuthKey]:
    out: list[MsgAuthKey] = []
    for item in keys or []:
        if isinstance(item, MsgAuthKey):
            out.append(item)
        elif isinstance(item, dict):
            out.append(
                MsgAuthKey(
                    label=str(item.get("label") or item.get("name") or "").strip(),
                    key=str(item.get("key") or item.get("key_value") or "").strip(),
                    scope=str(item.get("scope") or "").strip(),
                    scope_value=str(item.get("scope_value") or "").strip(),
                    enabled=bool(item.get("enabled", True)),
                )
            )
        elif isinstance(item, tuple):
            label = str(item[0] if len(item) > 0 else "").strip()
            key = str(item[1] if len(item) > 1 else "").strip()
            out.append(MsgAuthKey(label=label, key=key))
        else:
            key = str(item or "").strip()
            out.append(MsgAuthKey(label="", key=key))
    return [row for row in out if row.enabled and row.key]


def verify_js8_text(
    from_call: object,
    target: object,
    message: object,
    provided_crc: object = "",
    keys: Iterable[MsgAuthKey | dict | tuple | str] = (),
) -> MsgAuthVerification:
    body = normalize_auth_text(message)
    crc = str(provided_crc or "").strip().upper()
    if not crc:
        body, crc = parse_trailing_crc(body)
    if not crc:
        return MsgAuthVerification(state="unsigned", detail="No MsgAuth CRC was found.")
    if not CRC_RE.match(crc):
        return MsgAuthVerification(state="invalid", provided_crc=crc, detail="MsgAuth CRC must be three A-Z/0-9 characters.")
    datecode = ""
    decoded_datecode = ""
    parts = body.rsplit(" ", 1)
    if len(parts) == 2 and DATECODE_RE.match(parts[1].strip().upper()):
        datecode = parts[1].strip().upper()
        decoded_datecode = decode_short_datecode(datecode)
    key_rows = _key_rows(keys)
    if not key_rows:
        return MsgAuthVerification(state="no_key", provided_crc=crc, datecode=datecode, decoded_datecode=decoded_datecode, detail="No enabled MsgAuth key is available.")
    canonical = canonicalize_js8_auth_message(from_call, target, body)
    first_expected = ""
    for row in key_rows:
        expected = checksum(canonical, row.key)
        if not first_expected:
            first_expected = expected
        if expected == crc:
            return MsgAuthVerification(
                state="verified",
                expected_crc=expected,
                provided_crc=crc,
                key_label=row.label,
                detail="MsgAuth checksum verified.",
                datecode=datecode,
                decoded_datecode=decoded_datecode,
            )
    return MsgAuthVerification(
        state="failed",
        expected_crc=first_expected,
        provided_crc=crc,
        detail="MsgAuth checksum did not match any enabled key.",
        datecode=datecode,
        decoded_datecode=decoded_datecode,
    )
