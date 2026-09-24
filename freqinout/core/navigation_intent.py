"""Immutable cross-workspace navigation context.

Labels remain presentation-only; every restorable selection uses a stable key.
The model is Qt-free so UI surfaces can exchange context without owning one
another or persisting widget state.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Mapping


def _text(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


@dataclass(frozen=True, slots=True)
class NavigationIntent:
    origin_surface: str
    destination_route: str
    return_route: str
    return_scroll_y: int = 0
    return_query: str | None = None
    return_filters: Mapping[str, object] = field(default_factory=dict)
    return_selection_key: str | None = None
    draft_id: str | None = None
    directory_entry_id: str | None = None
    directory_session_ids: tuple[str, ...] = ()
    frequency_resource_id: str | None = None
    group_id: str | None = None
    local_net_schedule_id: str | None = None
    sop_id: int | None = None
    readonly_reason: str | None = None
    draft_snapshot: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name in ("origin_surface", "destination_route", "return_route"):
            value = _text(getattr(self, name))
            if value is None:
                raise ValueError(f"{name} is required")
            object.__setattr__(self, name, value)
        for name in (
            "return_query", "return_selection_key", "draft_id", "directory_entry_id",
            "frequency_resource_id", "group_id", "local_net_schedule_id", "readonly_reason",
        ):
            object.__setattr__(self, name, _text(getattr(self, name)))
        object.__setattr__(self, "return_scroll_y", max(0, int(self.return_scroll_y or 0)))
        object.__setattr__(
            self,
            "directory_session_ids",
            tuple(dict.fromkeys(str(value).strip() for value in self.directory_session_ids if str(value).strip())),
        )
        object.__setattr__(self, "return_filters", MappingProxyType(dict(self.return_filters)))
        object.__setattr__(self, "draft_snapshot", MappingProxyType(dict(self.draft_snapshot)))
        object.__setattr__(self, "sop_id", None if self.sop_id in (None, "") else int(self.sop_id))


__all__ = ["NavigationIntent"]
