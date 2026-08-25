from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Callable, Mapping

from freqinout.core.message_intelligence import analyze_form_text
from freqinout.core.message_file_metadata import cached_message_file_row_summary, form_report_timestamp_from_summary
from freqinout.core.message_file_scanner import FileRecord


@dataclass(frozen=True)
class FileMessageRowPresentation:
    msg_type: str
    from_call: str
    to_call: str
    title: str
    rcv_ts: float
    report_ts: float
    age_ts_source: str
    display_type: str
    topics: tuple[str, ...]
    actionable: bool
    search_detail: str


@dataclass(frozen=True)
class FileMessageRowCandidate:
    msg_type: str
    status: str
    from_call: str
    to_call: str
    title: str
    rcv_ts: float
    report_ts: float
    age_ts_source: str
    display_type: str
    topics: tuple[str, ...]
    actionable: bool
    search_detail: str
    used_cache: bool = False


def message_table_title(value: object, *, limit: int = 60) -> str:
    text = re.sub(r"\s+", " ", str(value or "").strip())
    if len(text) > limit:
        return text[: max(0, limit - 3)].rstrip() + "..."
    return text


def title_from_filename_path(path: Path) -> str:
    stem = path.stem
    tokens = [t for t in re.split(r"[-_]", stem) if t]
    if not tokens:
        return stem
    date_idx: int | None = None
    for i, tok in enumerate(tokens):
        t = tok.lower()
        if re.fullmatch(r"\d{6,8}", t) or re.fullmatch(r"\d{4,6}z", t) or re.fullmatch(r"\d{5,6}z", t):
            date_idx = i
            break
    title_tokens = tokens[date_idx + 1 :] if date_idx is not None else tokens[-1:]
    title = " ".join(title_tokens).strip()
    return title or stem


def clean_report_table_title(value: object) -> str:
    text = re.sub(r"\s+", " ", str(value or "").strip())
    if not text:
        return ""
    if text.lower() in {"<blankform>", "blankform"}:
        return ""
    text = re.sub(r"\bop\s+net(?=\d|\b)", "OpNet", text, flags=re.IGNORECASE)
    return text


def _looks_like_route_summary_part(value: object) -> bool:
    text = str(value or "").strip().upper()
    if not text:
        return False
    call = r"[A-Z]{1,2}\d[A-Z0-9]{1,5}(?:/[A-Z0-9]{1,4})?"
    target = rf"(?:{call}|@[A-Z0-9_-]{{2,}}|[A-Z0-9_-]{{2,}})"
    return bool(re.fullmatch(rf"{call}(?:\s*->\s*{target})?", text))


def _looks_like_date_summary_part(value: object) -> bool:
    text = str(value or "").strip()
    return bool(re.fullmatch(r"\d{6,8}[-_]\d{3,6}z?", text, flags=re.IGNORECASE))


def form_file_table_title(intelligence: object, fallback: object = "") -> str:
    """Prefer the operator-facing report name over the transport/form title."""
    if intelligence is None:
        return clean_report_table_title(fallback)
    metadata = getattr(intelligence, "metadata", {}) or {}
    for value in (
        getattr(intelligence, "subject", ""),
        metadata.get("subject", ""),
        metadata.get("title", ""),
        metadata.get("incident", ""),
        metadata.get("name", ""),
    ):
        title = clean_report_table_title(value)
        if title:
            return title

    summary = str(getattr(intelligence, "summary", "") or "").strip()
    for part in reversed([p.strip() for p in summary.split("|") if p.strip()]):
        title = clean_report_table_title(part)
        if not title:
            continue
        if _looks_like_route_summary_part(title) or _looks_like_date_summary_part(title):
            continue
        if "form" in title.lower() and len(title) <= 80:
            continue
        return title
    return clean_report_table_title(summary) or clean_report_table_title(fallback)


def form_message_type_label(form_name: object, fallback_type: object = "") -> str:
    text = re.sub(r"\s+", " ", str(form_name or "").strip())
    fallback = str(fallback_type or "").strip().upper()
    if not text:
        return fallback or "FORM"
    low = text.lower()
    m = re.search(r"\bICS\s*[- ]?\s*([0-9]{2,4}[A-Z]?)\b", text, flags=re.IGNORECASE)
    if m:
        return f"ICS {m.group(1).upper()}"
    if "blank" in low:
        return "Blank"
    if "statrep" in low or "status report" in low:
        return "StatRep"
    if "sitrep" in low or "situation report" in low:
        return "SitRep"
    if "general" in low:
        return "General"
    m = re.search(r"\b([A-Z]{1,4}[!-]?[0-9]{2,4}[A-Z]?)\b", text, flags=re.IGNORECASE)
    if m and m.group(1).upper() not in {"V1", "V1.0", "V1.1"}:
        return m.group(1).upper()
    return fallback or "FORM"


def file_message_type(origin: object) -> str:
    text = str(origin or "").strip().lower()
    if text == "flmsg":
        return "FLMSG"
    if text == "bbs":
        return "BBS"
    if text == "varac":
        return "VarAC"
    return text.upper()


def file_message_row_presentation(
    rec: FileRecord,
    origin: object,
    *,
    is_image: bool,
    intelligence: object | None,
    form_meta: Mapping[str, str],
    fallback_from: object = "",
) -> FileMessageRowPresentation:
    msg_type = file_message_type(origin)
    from_call = "" if is_image else (
        (getattr(intelligence, "from_call", "") if intelligence else "")
        or form_meta.get("from", "")
        or str(fallback_from or "")
    )
    to_call = "" if is_image else (
        (getattr(intelligence, "to_call", "") if intelligence else "") or form_meta.get("to", "")
    )
    title = "Image Received" if is_image else (
        form_file_table_title(
            intelligence,
            form_meta.get("title") or title_from_filename_path(rec.path),
        )
        or form_meta.get("title")
        or title_from_filename_path(rec.path)
    )
    title = message_table_title(title)
    report_ts = form_report_timestamp_from_summary(
        (getattr(intelligence, "date_summary", "") if intelligence else "") or form_meta.get("date_summary", "")
    )
    rcv_ts = float(report_ts or rec.mtime or 0.0)
    age_ts_source = "report" if report_ts else "received"
    topics = tuple(getattr(intelligence, "topics", ()) or ()) if intelligence else ()
    search_detail = " ".join(
        part
        for part in (
            title,
            getattr(intelligence, "summary", "") if intelligence else "",
            getattr(intelligence, "form_name", "") if intelligence else "",
            getattr(intelligence, "subject", "") if intelligence else "",
            getattr(intelligence, "date_summary", "") if intelligence else "",
            getattr(intelligence, "state", "") if intelligence else "",
            getattr(intelligence, "grid", "") if intelligence else "",
        )
        if part
    )
    return FileMessageRowPresentation(
        msg_type=msg_type,
        from_call=from_call,
        to_call=to_call,
        title=title,
        rcv_ts=rcv_ts,
        report_ts=float(report_ts or 0.0),
        age_ts_source=age_ts_source,
        display_type=form_message_type_label(
            getattr(intelligence, "form_name", "") if intelligence else form_meta.get("form_title", ""),
            msg_type,
        ),
        topics=topics,
        actionable=bool(getattr(intelligence, "actionable", False)) if intelligence else False,
        search_detail=search_detail,
    )


def file_message_search_text(
    msg_type: object,
    status: object,
    from_call: object,
    to_call: object,
    rcv_display: object,
    detail: object,
) -> str:
    return " ".join(
        [
            str(msg_type or ""),
            str(status or ""),
            str(from_call or ""),
            str(to_call or ""),
            str(rcv_display or ""),
            str(detail or ""),
        ]
    ).lower()


def cached_file_message_row_candidate(
    rec: FileRecord,
    cached_meta: Mapping[str, object] | None,
    *,
    origin: object,
    status: object,
    fallback_title: object = "",
) -> FileMessageRowCandidate | None:
    cached = cached_message_file_row_summary(
        rec,
        cached_meta,
        fallback_origin=str(origin or rec.origin or ""),
        fallback_title=str(fallback_title or rec.path.name),
    )
    if cached is None:
        return None
    detail = cached.search_text or " ".join(
        part
        for part in (
            cached.title,
            cached.display_type,
            rec.path.name,
        )
        if part
    )
    return FileMessageRowCandidate(
        msg_type=cached.msg_type,
        status=str(status or cached.status or "NEW").upper(),
        from_call=cached.from_call,
        to_call=cached.to_call,
        title=cached.title,
        rcv_ts=cached.rcv_ts,
        report_ts=cached.report_ts,
        age_ts_source=cached.age_ts_source,
        display_type=cached.display_type,
        topics=cached.topics,
        actionable=cached.actionable,
        search_detail=detail,
        used_cache=True,
    )


def parsed_file_message_row_candidate(
    rec: FileRecord,
    origin: object,
    *,
    status: object,
    is_image: bool,
    intelligence: object | None,
    form_meta: Mapping[str, str],
    fallback_from: object = "",
) -> FileMessageRowCandidate:
    presentation = file_message_row_presentation(
        rec,
        origin,
        is_image=is_image,
        intelligence=intelligence,
        form_meta=form_meta,
        fallback_from=fallback_from,
    )
    return FileMessageRowCandidate(
        msg_type=presentation.msg_type,
        status=str(status or "NEW").upper(),
        from_call=presentation.from_call,
        to_call=presentation.to_call,
        title=presentation.title,
        rcv_ts=presentation.rcv_ts,
        report_ts=presentation.report_ts,
        age_ts_source=presentation.age_ts_source,
        display_type=presentation.display_type,
        topics=presentation.topics,
        actionable=presentation.actionable,
        search_detail=presentation.search_detail,
        used_cache=False,
    )


def file_message_row_candidate(
    rec: FileRecord,
    origin: object,
    *,
    status: object,
    is_image: bool,
    is_transport_form: bool,
    cached_meta: Mapping[str, object] | None = None,
    form_meta: Mapping[str, str] | None = None,
    form_meta_loader: Callable[[], Mapping[str, str]] | None = None,
    fallback_from: object = "",
    fallback_from_loader: Callable[[], object] | None = None,
) -> FileMessageRowCandidate:
    if not is_image:
        cached = cached_file_message_row_candidate(
            rec,
            cached_meta,
            origin=origin,
            status=status,
            fallback_title=rec.path.name,
        )
        if cached is not None:
            return cached

    if form_meta is not None:
        meta: Mapping[str, str] = form_meta
    elif form_meta_loader is not None and not is_image:
        meta = form_meta_loader() or {}
    else:
        meta = {}
    if not fallback_from and fallback_from_loader is not None and not is_image:
        fallback_from = fallback_from_loader()
    intelligence = None
    if not is_image and is_transport_form:
        intelligence = analyze_form_text(
            meta.get("_raw_head", ""),
            form_name=meta.get("form_title", ""),
            source_type=str(origin or rec.origin or ""),
            path=rec.path,
            fields=meta,
        )
    return parsed_file_message_row_candidate(
        rec,
        origin,
        status=status,
        is_image=is_image,
        intelligence=intelligence,
        form_meta=meta,
        fallback_from=fallback_from,
    )
