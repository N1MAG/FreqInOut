from __future__ import annotations

import re
from pathlib import Path
from typing import Callable, Mapping


def form_label_key(label: object) -> str:
    text = str(label or "").strip().lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def form_date_summary(value: object) -> str:
    txt = str(value or "").strip()
    if not txt:
        return ""
    m = re.search(r"\b(\d{6})[-_\sT]?(\d{4,6})z?\b", txt, flags=re.IGNORECASE)
    if m:
        return f"{m.group(1)}-{m.group(2)[:4]}z"
    m = re.search(r"\b(\d{8})[-_\sT]?(\d{4,6})z?\b", txt, flags=re.IGNORECASE)
    if m:
        return f"{m.group(1)}-{m.group(2)[:4]}z"
    return txt[:24]


def match_field_text(text: str, pattern: str) -> str:
    m = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
    return m.group(1).strip().replace("\r\n", "\n") if m else ""


def extract_custom_form_name_text(text: str) -> str:
    if not text:
        return ""
    m = re.search(r"CUSTOM_FORM,([A-Za-z0-9_.-]+)", text, flags=re.IGNORECASE)
    return m.group(1).strip() if m else ""


def parse_custom_form_fields_text(text: str) -> dict[str, str]:
    fields: dict[str, str] = {}
    if not text:
        return fields
    for line in text.splitlines():
        line = line.strip()
        if not line or "," not in line:
            continue
        key, val = line.split(",", 1)
        key = key.strip().upper()
        if re.fullmatch(r"L\d{1,2}[A-Z]?", key):
            fields[key] = val.strip()
    return fields


def looks_like_group_or_call_text(value: object) -> bool:
    text = str(value or "").strip().upper()
    if not text:
        return False
    if re.fullmatch(r"\d{6,8}[-_]?\d{3,6}z?", text, flags=re.IGNORECASE) or re.fullmatch(
        r"\d{8}T\d{4,6}Z?", text, flags=re.IGNORECASE
    ):
        return False
    return bool(
        re.fullmatch(r"@?[A-Z0-9_-]{2,}", text)
        or re.fullmatch(r"[A-Z]{1,2}\d[A-Z0-9]{1,5}(?:/[A-Z0-9]{1,4})?", text)
    )


def looks_like_form_date_text(value: object) -> bool:
    text = str(value or "").strip()
    return bool(
        re.fullmatch(r"\d{6,8}[-_]?\d{3,6}z?", text, flags=re.IGNORECASE)
        or re.fullmatch(r"\d{8}T\d{4,6}Z?", text, flags=re.IGNORECASE)
    )


def extract_hdr_call_text(text: str, marker: str) -> str:
    if not text:
        return ""
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    for idx, line in enumerate(lines):
        if line.lower().startswith(str(marker or "").lower()):
            for nxt in lines[idx + 1 :]:
                match = re.search(r"\b@?[A-Z]{1,2}\d[A-Z0-9]{1,5}\b|\b@[A-Z0-9_-]{2,}\b", nxt.upper())
                if match:
                    return match.group(0).strip().upper()
            break
    return ""


def extract_sender_from_form_text(text: str, path: Path) -> str:
    sender = extract_hdr_call_text(text, ":hdr_fm:")
    if sender:
        return sender
    for tok in re.split(r"[-_\\s]+", path.stem):
        up = tok.strip().upper()
        if re.fullmatch(r"[A-Z]{1,2}\d[A-Z0-9]{1,4}", up):
            return up
    return ""


def apply_common_l_field_metadata_fallback(meta: dict[str, str], fields: Mapping[str, str]) -> None:
    if not fields:
        return
    values = {str(k or "").strip().upper(): str(v or "").strip() for k, v in fields.items() if str(v or "").strip()}
    if not values:
        return

    def set_if_empty(key: str, value: object) -> None:
        text = str(value or "").strip()
        if text and not meta.get(key):
            meta[key] = text

    l01 = values.get("L01", "")
    l02 = values.get("L02", "")
    if l01 and looks_like_group_or_call_text(l01):
        set_if_empty("to", l01.upper())
    if l02 and looks_like_group_or_call_text(l02):
        set_if_empty("from", l02.upper())

    l03 = values.get("L03", "")
    l04 = values.get("L04", "")
    l05 = values.get("L05", "")
    l06 = values.get("L06", "")
    l07 = values.get("L07", "")

    if l01 and looks_like_form_date_text(l01):
        set_if_empty("date_summary", form_date_summary(l01))
        if l02 and looks_like_group_or_call_text(l02):
            set_if_empty("to", l02.upper())
        if l03 and looks_like_group_or_call_text(l03):
            set_if_empty("from", l03.upper())
        if l06:
            set_if_empty("subject", l06)
        if l07:
            set_if_empty("body", l07)

    if l05:
        set_if_empty("subject", l05)
    elif l03 and not looks_like_form_date_text(l03) and l03.upper() not in {"R", "P", "I", "F"}:
        set_if_empty("subject", l03)

    if l04 and looks_like_form_date_text(l04):
        set_if_empty("date_summary", form_date_summary(l04))
    elif l03 and looks_like_form_date_text(l03):
        set_if_empty("date_summary", form_date_summary(l03))

    if l06:
        set_if_empty("body", l06)
    elif l04 and not looks_like_form_date_text(l04):
        set_if_empty("body", l04)

    for value in values.values():
        grid = re.search(r"\b[A-R]{2}[0-9]{2}(?:[A-X]{2})?\b", value.upper())
        if grid and not re.fullmatch(r"MR[0-9A-Z]{2,}", grid.group(0), flags=re.IGNORECASE):
            set_if_empty("grid", grid.group(0))
            break


def extract_form_metadata_from_text(
    text: str,
    path: Path,
    *,
    template_title_for_form: Callable[[str], str],
    template_labels_for_form: Callable[[str], list[tuple[str, str]]],
) -> dict[str, str]:
    if not text:
        return {}
    meta: dict[str, str] = {"_raw_head": text}
    from_call = extract_sender_from_form_text(text, path)
    if from_call:
        meta["from"] = from_call
    to_call = extract_hdr_call_text(text, ":hdr_to:")
    if to_call:
        meta["to"] = to_call
    subject = match_field_text(text, r":sub:\s*(.*?)\s*(?=:)")
    if subject:
        meta["subject"] = subject
    form_name = extract_custom_form_name_text(text)
    fields = parse_custom_form_fields_text(text)
    if form_name and fields:
        title = str(template_title_for_form(form_name) or "").strip()
        if title:
            meta["form_title"] = title
        for key, label in template_labels_for_form(form_name):
            value = fields.get(str(key or "").strip().upper(), "").strip()
            if not value:
                continue
            label_key = form_label_key(label)
            if label_key in {"from", "from call", "from callsign", "sender", "sender callsign"}:
                meta["from"] = value.strip().upper()
            elif label_key in {"to", "to call", "to callsign", "destination", "recipient", "group"}:
                meta["to"] = value.strip().upper()
            elif label_key in {"dtg", "date msg id", "date/time/msg id", "date time msg id"} or (
                "date" in label_key and ("msg" in label_key or "message" in label_key or "time" in label_key)
            ):
                meta["date_summary"] = form_date_summary(value)
            elif label_key in {"subject", "title", "incident", "report title"}:
                meta.setdefault("subject", value)
            elif label_key in {"state", "st"}:
                meta.setdefault("state", value)
            elif label_key in {"grid", "gr", "maidenhead", "grid square"}:
                meta.setdefault("grid", value)
            elif label_key in {"message", "body", "comments", "remarks", "narrative"}:
                meta.setdefault("body", value)
        apply_common_l_field_metadata_fallback(meta, fields)
    if "date_summary" not in meta:
        for pattern in (
            r"\b(\d{6}[-_\sT]?\d{4,6}z?)\b",
            r"\b(\d{8}[-_\sT]?\d{4,6}z?)\b",
        ):
            m = re.search(pattern, text, flags=re.IGNORECASE)
            if m:
                meta["date_summary"] = form_date_summary(m.group(1))
                break
    title_parts: list[str] = []
    if meta.get("form_title"):
        title_parts.append(meta["form_title"])
    elif meta.get("subject"):
        title_parts.append(meta["subject"])
    if meta.get("date_summary"):
        title_parts.append(meta["date_summary"])
    if title_parts:
        meta["title"] = " - ".join(title_parts)
    return meta
