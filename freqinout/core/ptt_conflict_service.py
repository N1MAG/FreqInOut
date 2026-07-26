from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping, Optional
from uuid import uuid4

from freqinout.core.multi_radio_store import MultiRadioStore, normalize_ptt_group
from freqinout.core.multi_rig_runtime_status import device_profile_id_from_radio_id, radio_shared_state_id
from freqinout.core.shared_state import PttConflictEvidence


SEVERITIES = frozenset({"hard", "soft"})


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _clean_text(value: Any, default: str = "") -> str:
    try:
        text = str(value if value is not None else default).strip()
    except Exception:
        text = str(default or "").strip()
    return text or default


def _normalize_severity(value: object) -> str:
    severity = _clean_text(value, "hard").lower()
    return severity if severity in SEVERITIES else "hard"


def _radio_device_id(radio_profile_id: str | int | None) -> Optional[int]:
    if radio_profile_id in (None, ""):
        return None
    if isinstance(radio_profile_id, int):
        return int(radio_profile_id)
    text = _clean_text(radio_profile_id)
    if text.startswith("radio_"):
        return device_profile_id_from_radio_id(text)
    return int(text)


class PttConflictService:
    """Durable shared-PTT conflict evidence for scheduler safety and UI details."""

    def __init__(self, store: Optional[MultiRadioStore] = None) -> None:
        self.store = store or MultiRadioStore()

    def publish(self, evidence: PttConflictEvidence) -> PttConflictEvidence:
        requested_id = _radio_device_id(evidence.requested_radio_id)
        blocking_id = _radio_device_id(evidence.blocking_radio_id)
        if requested_id is None:
            raise ValueError("PTT conflict requires a requested radio.")
        ptt_group = normalize_ptt_group(evidence.ptt_group)
        if not ptt_group:
            raise ValueError("PTT conflict requires a PTT group.")
        now = _utc_now_iso()
        normalized = PttConflictEvidence(
            id=_clean_text(evidence.id) or f"ptt_{uuid4().hex}",
            ptt_group=ptt_group,
            requested_radio_id=radio_shared_state_id(requested_id),
            blocking_radio_id=radio_shared_state_id(blocking_id) if blocking_id is not None else None,
            severity=_normalize_severity(evidence.severity),
            source=_clean_text(evidence.source),
            created_at_utc=_clean_text(evidence.created_at_utc, now),
        )
        with self.store.connect() as conn:
            self._ensure_radio_exists(conn, requested_id)
            if blocking_id is not None:
                self._ensure_radio_exists(conn, blocking_id)
            conn.execute(
                """
                INSERT INTO ptt_conflict_evidence (
                    id,
                    ptt_group,
                    requested_radio_id,
                    blocking_radio_id,
                    severity,
                    source,
                    created_utc,
                    updated_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    ptt_group=excluded.ptt_group,
                    requested_radio_id=excluded.requested_radio_id,
                    blocking_radio_id=excluded.blocking_radio_id,
                    severity=excluded.severity,
                    source=excluded.source,
                    updated_utc=excluded.updated_utc
                """,
                (
                    normalized.id,
                    normalized.ptt_group,
                    requested_id,
                    blocking_id,
                    normalized.severity,
                    normalized.source,
                    normalized.created_at_utc,
                    now,
                ),
            )
            conn.commit()
        return self.get(normalized.id)

    def get(self, evidence_id: str) -> PttConflictEvidence:
        with self.store.connect() as conn:
            row = conn.execute(
                "SELECT * FROM ptt_conflict_evidence WHERE id=?",
                (_clean_text(evidence_id),),
            ).fetchone()
            if row is None:
                raise KeyError(f"Unknown PTT conflict id: {_clean_text(evidence_id)}")
            return self._evidence_from_row(dict(row))

    def clear(self, evidence_id: str) -> bool:
        with self.store.connect() as conn:
            cur = conn.execute("DELETE FROM ptt_conflict_evidence WHERE id=?", (_clean_text(evidence_id),))
            conn.commit()
            return int(cur.rowcount or 0) > 0

    def active_for_radio(self, radio_profile_id: str | int) -> tuple[PttConflictEvidence, ...]:
        device_id = _radio_device_id(radio_profile_id)
        if device_id is None:
            return ()
        with self.store.connect() as conn:
            self._ensure_radio_exists(conn, device_id)
            rows = conn.execute(
                """
                SELECT *
                  FROM ptt_conflict_evidence
                 WHERE requested_radio_id=? OR blocking_radio_id=?
              ORDER BY created_utc DESC
                """,
                (device_id, device_id),
            ).fetchall()
            return tuple(self._evidence_from_row(dict(row)) for row in rows)

    def active_for_group(self, ptt_group: str) -> tuple[PttConflictEvidence, ...]:
        group = normalize_ptt_group(ptt_group)
        if not group:
            return ()
        with self.store.connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                  FROM ptt_conflict_evidence
                 WHERE ptt_group=?
              ORDER BY created_utc DESC
                """,
                (group,),
            ).fetchall()
            return tuple(self._evidence_from_row(dict(row)) for row in rows)

    @staticmethod
    def _ensure_radio_exists(conn: Any, device_id: int) -> None:
        row = conn.execute("SELECT id FROM device_profiles WHERE id=?", (int(device_id),)).fetchone()
        if row is None:
            raise KeyError(f"Unknown radio id: {radio_shared_state_id(device_id)}")

    @staticmethod
    def _evidence_from_row(row: Mapping[str, Any]) -> PttConflictEvidence:
        blocking_id = row.get("blocking_radio_id")
        return PttConflictEvidence(
            id=_clean_text(row.get("id")),
            ptt_group=normalize_ptt_group(row.get("ptt_group")),
            requested_radio_id=radio_shared_state_id(row.get("requested_radio_id")),
            blocking_radio_id=radio_shared_state_id(blocking_id) if blocking_id not in (None, "") else None,
            severity=_normalize_severity(row.get("severity")),
            source=_clean_text(row.get("source")),
            created_at_utc=_clean_text(row.get("created_utc"), _utc_now_iso()),
        )

