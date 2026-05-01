from __future__ import annotations

from typing import Iterable, Sequence


def build_support_summary(
    title: str,
    lines: Sequence[str] | None = None,
    *,
    sections: Sequence[tuple[str, Sequence[str]]] | None = None,
) -> str:
    out: list[str] = [str(title or "").strip()]
    for line in lines or ():
        text = str(line or "").strip()
        if text:
            out.append(text)
    for heading, section_lines in sections or ():
        section = [str(line or "").strip() for line in section_lines if str(line or "").strip()]
        if not section:
            continue
        out.append("")
        out.append(str(heading or "").strip())
        out.extend(section)
    return "\n".join(line for line in out if line is not None)


def bullet_lines(values: Iterable[str]) -> list[str]:
    out: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if text:
            out.append(f"- {text}")
    return out
