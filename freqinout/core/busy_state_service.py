from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from freqinout.core.busy_evidence_service import BusyEvidenceService
from freqinout.core.multi_radio_store import MultiRadioStore
from freqinout.core.multi_rig_runtime_status import device_profile_id_from_radio_id, radio_shared_state_id
from freqinout.core.ptt_conflict_service import PttConflictService
from freqinout.core.shared_state import BusyEvidence, BusyState


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _radio_device_id(radio_profile_id: str | int) -> int:
    if isinstance(radio_profile_id, int):
        return int(radio_profile_id)
    text = str(radio_profile_id or "").strip()
    if text.startswith("radio_"):
        return device_profile_id_from_radio_id(text)
    return int(text)


def _summary_for(evidence: BusyEvidence) -> str:
    detail = str(evidence.description or "").strip()
    if detail:
        return detail
    reason = str(evidence.reason_code or "").replace("_", " ").strip()
    return reason[:1].upper() + reason[1:] if reason else "Busy"


class BusyStateService:
    """Read model for radio-scoped busy state.

    This is intentionally passive: it aggregates durable evidence but does not
    publish, clear, or alter scheduler behavior.
    """

    def __init__(self, store: Optional[MultiRadioStore] = None) -> None:
        self.store = store or MultiRadioStore()
        self.busy_evidence = BusyEvidenceService(self.store)
        self.ptt_conflicts = PttConflictService(self.store)

    def state_for_radio(self, radio_profile_id: str | int) -> BusyState:
        device_id = _radio_device_id(radio_profile_id)
        radio_id = radio_shared_state_id(device_id)
        active_evidence = self.busy_evidence.active_for_radio(radio_id)
        conflicts = self.ptt_conflicts.active_for_radio(radio_id)
        top = active_evidence[0] if active_evidence else None
        return BusyState(
            radio_profile_id=radio_id,
            busy=top is not None,
            severity=top.severity if top is not None else "none",
            source_family=top.source_family if top is not None else "",
            reason_code=top.reason_code if top is not None else "",
            summary=_summary_for(top) if top is not None else "",
            top_evidence_id=top.id if top is not None else None,
            evidence_ids=tuple(item.id for item in active_evidence),
            ptt_conflict_ids=tuple(item.id for item in conflicts),
            updated_at_utc=_utc_now_iso(),
        )

    def active_states(self) -> tuple[BusyState, ...]:
        states: list[BusyState] = []
        with self.store.connect() as conn:
            rows = conn.execute(
                """
                SELECT DISTINCT radio_profile_id
                  FROM busy_evidence
                UNION
                SELECT DISTINCT requested_radio_id AS radio_profile_id
                  FROM ptt_conflict_evidence
                UNION
                SELECT DISTINCT blocking_radio_id AS radio_profile_id
                  FROM ptt_conflict_evidence
                 WHERE blocking_radio_id IS NOT NULL
              ORDER BY radio_profile_id ASC
                """
            ).fetchall()
        for row in rows:
            try:
                state = self.state_for_radio(int(row[0]))
            except Exception:
                continue
            if state.busy or state.ptt_conflict_ids:
                states.append(state)
        return tuple(states)

