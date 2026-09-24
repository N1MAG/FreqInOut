from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Mapping, Sequence

from freqinout.core.js8_spotter_forms import (
    FORM_TOKEN_RE,
    SPOTTER_COMMENTS_KEY,
    SPOTTER_COMMENTS_MAX_LENGTH,
    SpotterFormField,
    normalize_form_code,
)


_DATECODE_RE = re.compile(r"(?:^|\s)(#[A-Z0-9]{4})\s*$", re.IGNORECASE)
_BRACKET_FIELD_RE = re.compile(r"\b([A-Z0-9]{2})\[(.*?)\]", re.IGNORECASE)


@dataclass(frozen=True)
class SpotterFormPayload:
    form_code: str
    values: Mapping[str, str]
    comments: str = ""
    datecode: str = ""
    complete: bool = True
    issues: tuple[str, ...] = ()


def _clean_inline(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def unwrap_native_js8_form_payload(text: object) -> str:
    """Return the MCForm payload from directed/native-JS8 MSG text.

    The function only removes transport material before the first recognized
    form token.  It does not rewrite the form body or its Comments field.
    """
    raw = _clean_inline(text)
    if not raw:
        return ""
    match = FORM_TOKEN_RE.search(raw)
    return raw[match.start():].strip() if match else ""


def _decode_choice_prefix(
    text: str,
    fields: Sequence[SpotterFormField],
) -> tuple[dict[str, str], int]:
    choice_fields = [field for field in fields if field.kind == "choice"]
    memo: dict[tuple[int, int], tuple[dict[str, str], int] | None] = {}

    def visit(field_index: int, offset: int) -> tuple[dict[str, str], int] | None:
        key = (field_index, offset)
        if key in memo:
            return memo[key]
        if field_index >= len(choice_fields):
            result = ({}, offset)
            memo[key] = result
            return result
        field = choice_fields[field_index]
        options = sorted(
            (str(token or "") for token, _label in field.options if str(token or "")),
            key=len,
            reverse=True,
        )
        for token in options:
            if not text[offset:].upper().startswith(token.upper()):
                continue
            tail = visit(field_index + 1, offset + len(token))
            if tail is None:
                continue
            values, end = tail
            result_values = {field.key: text[offset:offset + len(token)], **values}
            result = (result_values, end)
            memo[key] = result
            return result
        memo[key] = None
        return None

    if not choice_fields:
        return {}, 0
    result = visit(0, 0)
    return result if result is not None else ({}, 0)


def parse_spotter_form_payload(
    text: object,
    fields: Sequence[SpotterFormField],
    *,
    expected_form_code: object = "",
) -> SpotterFormPayload:
    payload = unwrap_native_js8_form_payload(text)
    if not payload:
        return SpotterFormPayload("", {}, complete=False, issues=("No MCForm payload was found.",))
    parts = payload.split(maxsplit=1)
    form_code = normalize_form_code(parts[0])
    expected = normalize_form_code(expected_form_code)
    if not form_code:
        return SpotterFormPayload("", {}, complete=False, issues=("The MCForm ID is invalid.",))
    if expected and form_code != expected:
        return SpotterFormPayload(
            form_code,
            {},
            complete=False,
            issues=(f"Expected {expected}, but the payload contains {form_code}.",),
        )
    body = parts[1].strip() if len(parts) > 1 else ""
    datecode = ""
    date_match = _DATECODE_RE.search(body)
    if date_match:
        datecode = date_match.group(1).upper()
        body = body[:date_match.start()].rstrip()

    values, choice_end = _decode_choice_prefix(body, fields)
    remainder = body[choice_end:].lstrip()
    known_prompts = {field.key.upper(): field for field in fields if field.kind == "prompt"}
    spans: list[tuple[int, int]] = []
    for match in _BRACKET_FIELD_RE.finditer(remainder):
        key = match.group(1).upper()
        field = known_prompts.get(key)
        if field is None:
            continue
        values[field.key] = _clean_inline(match.group(2))
        spans.append(match.span())
    if spans:
        chars = list(remainder)
        for start, end in spans:
            chars[start:end] = " " * (end - start)
        comments = _clean_inline("".join(chars))
    else:
        comments = _clean_inline(remainder)
    if comments:
        values[SPOTTER_COMMENTS_KEY] = comments

    issues: list[str] = []
    for field in fields:
        if field.kind == "choice" and not str(values.get(field.key, "") or "").strip():
            issues.append(f"Choose an answer for {field.label}.")
    return SpotterFormPayload(
        form_code=form_code,
        values=values,
        comments=comments,
        datecode=datecode,
        complete=not issues,
        issues=tuple(issues),
    )


def serialize_spotter_form_payload(
    form_code: object,
    fields: Sequence[SpotterFormField],
    values: Mapping[str, object],
    *,
    comments: object = "",
    datecode: object = "",
) -> str:
    code = normalize_form_code(form_code)
    if not code:
        raise ValueError("Choose a valid MCForm.")
    choice_tokens: list[str] = []
    prompt_parts: list[str] = []
    for field in fields:
        value = _clean_inline(values.get(field.key, ""))
        if field.kind == "choice":
            if not value:
                raise ValueError(f"Choose an answer for {field.label}.")
            allowed = {str(token or "").upper() for token, _label in field.options}
            if value.upper() not in allowed:
                raise ValueError(f"Choose a valid answer for {field.label}.")
            choice_tokens.append(value)
        elif field.kind == "prompt" and value:
            if "]" in value:
                raise ValueError(f"Remove the closing bracket from {field.label}.")
            prompt_parts.append(f"{field.key.upper()}[{value}]")
    comment_text = _clean_inline(comments or values.get(SPOTTER_COMMENTS_KEY, ""))
    if len(comment_text) > SPOTTER_COMMENTS_MAX_LENGTH:
        raise ValueError(
            f"Comments must be {SPOTTER_COMMENTS_MAX_LENGTH} characters or fewer."
        )
    date_text = _clean_inline(datecode).upper()
    if date_text and not date_text.startswith("#"):
        date_text = f"#{date_text}"
    return " ".join(
        part for part in (code, "".join(choice_tokens), " ".join(prompt_parts), comment_text, date_text) if part
    ).strip()
