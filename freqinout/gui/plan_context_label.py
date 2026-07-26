from __future__ import annotations

from typing import Optional

from PySide6.QtWidgets import QLabel

from freqinout.core.logger import log
from freqinout.core.plan_context_service import PlanContext, PlanContextService


PLAN_CONTEXT_FALLBACK_TEXT = (
    "Read-only Frequency Plan coverage review for HF, Net, and SOP-effective schedule windows."
)


def plan_context_display_text(context: Optional[PlanContext]) -> str:
    if context is None:
        return PLAN_CONTEXT_FALLBACK_TEXT
    runtime = "primary runtime" if context.runtime_primary else "active runtime" if context.runtime_active else "not active"
    scheduler = "on" if context.scheduler_participating else "off"
    messages = "on" if context.messages_enabled else "off"
    map_state = "on" if context.map_enabled else "off"
    temporary = " Temporary override is active." if context.temporary_override else ""
    blocker = f" Attention: {context.top_blocker}" if context.top_blocker else ""
    return (
        f"Reviewing {context.plan_label} for {context.radio_label} "
        f"({runtime}). Scheduler: {scheduler}. Messages: {messages}. Map: {map_state}."
        f"{temporary}{blocker}"
    )


class PlanContextLabel(QLabel):
    def __init__(
        self,
        tab_id: str,
        *,
        service: Optional[PlanContextService] = None,
        fallback_text: str = PLAN_CONTEXT_FALLBACK_TEXT,
        create_service: bool = True,
        parent=None,
    ) -> None:
        super().__init__(str(fallback_text or PLAN_CONTEXT_FALLBACK_TEXT), parent)
        self.tab_id = str(tab_id or "").strip()
        self.plan_context_service = service or (PlanContextService() if create_service else None)
        self.fallback_text = str(fallback_text or PLAN_CONTEXT_FALLBACK_TEXT)
        self.setWordWrap(True)

    def refresh_context(self, *, refresh: bool = False) -> None:
        try:
            if self.plan_context_service is None:
                self.setText(self.fallback_text)
                return
            context = self.plan_context_service.context_for_tab(self.tab_id, refresh=refresh)
            if self.fallback_text != PLAN_CONTEXT_FALLBACK_TEXT and context is None:
                self.setText(self.fallback_text)
                return
            self.setText(plan_context_display_text(context))
        except Exception as e:
            log.debug("Plan context label refresh failed for %s: %s", self.tab_id or "unknown", e)
            self.setText(self.fallback_text)

    def invalidate_context(self) -> None:
        try:
            self.plan_context_service.invalidate()
        except Exception:
            pass
