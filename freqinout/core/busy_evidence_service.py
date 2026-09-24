from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping, Optional
from uuid import uuid4

from freqinout.core.multi_radio_store import MultiRadioStore
from freqinout.core.multi_rig_runtime_status import device_profile_id_from_radio_id, radio_shared_state_id
from freqinout.core.shared_state import BusyEvidence


SOURCE_FAMILIES = frozenset(
    {
        "ptt",
        "control_backend",
        "fl",
        "js8",
        "varac",
        "wsjtx",
        "sdr",
        "mesh",
        "scheduler",
        "unknown",
    }
)
SEVERITIES = frozenset({"hard", "soft"})
BUSY_PRIORITY = {
    "ptt_active": 10,
    "ptt_unknown_unsafe": 11,
    "shared_ptt_interlock": 12,
    "varac_transfer": 20,
    "protected_file_transfer": 21,
    "js8_tx": 30,
    "fldigi_tx": 31,
    "wsjtx_tx": 32,
    "varac_busy": 33,
    "varac_waiting_for_frequency": 34,
    "control_backend_busy": 40,
    "control_backend_unreachable": 41,
    "control_backend_unknown": 42,
    "receive_decode": 50,
}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_utc(value: object) -> Optional[datetime]:
    text = _clean_text(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _clean_text(value: Any, default: str = "") -> str:
    try:
        text = str(value if value is not None else default).strip()
    except Exception:
        text = str(default or "").strip()
    return text or default


def _normalize_source_family(value: object) -> str:
    family = _clean_text(value, "unknown").lower()
    return family if family in SOURCE_FAMILIES else "unknown"


def _normalize_severity(value: object) -> str:
    severity = _clean_text(value, "soft").lower()
    return severity if severity in SEVERITIES else "soft"


def _radio_device_id(radio_profile_id: str | int) -> int:
    if isinstance(radio_profile_id, int):
        return int(radio_profile_id)
    text = _clean_text(radio_profile_id)
    if text.startswith("radio_"):
        return device_profile_id_from_radio_id(text)
    return int(text)


def _is_expired(evidence: BusyEvidence, *, now_utc: Optional[datetime] = None) -> bool:
    expires = _parse_utc(evidence.expiration_timestamp_utc)
    if evidence.expiration_timestamp_utc and expires is None:
        return True
    if expires is None:
        return False
    now = now_utc or datetime.now(timezone.utc)
    return expires <= now.astimezone(timezone.utc)


def _timestamp_rank(value: object) -> float:
    parsed = _parse_utc(value)
    if parsed is None:
        return 0.0
    return parsed.timestamp()


def _priority(evidence: BusyEvidence) -> tuple[int, int, float]:
    severity_rank = 0 if evidence.severity == "hard" else 1
    reason_rank = BUSY_PRIORITY.get(evidence.reason_code, 90)
    return severity_rank, reason_rank, -_timestamp_rank(evidence.evidence_timestamp_utc)


class BusyEvidenceService:
    """Durable radio-scoped busy evidence for the station control center."""

    def __init__(self, store: Optional[MultiRadioStore] = None) -> None:
        self.store = store or MultiRadioStore()

    def publish(self, evidence: BusyEvidence) -> BusyEvidence:
        device_id = _radio_device_id(evidence.radio_profile_id)
        now = _utc_now_iso()
        normalized = BusyEvidence(
            id=_clean_text(evidence.id) or f"busy_{uuid4().hex}",
            radio_profile_id=radio_shared_state_id(device_id),
            source_family=_normalize_source_family(evidence.source_family),
            reason_code=_clean_text(evidence.reason_code, "unknown").lower(),
            severity=_normalize_severity(evidence.severity),
            evidence_timestamp_utc=_clean_text(evidence.evidence_timestamp_utc, now),
            expiration_timestamp_utc=_clean_text(evidence.expiration_timestamp_utc) or None,
            description=_clean_text(evidence.description),
            latest_event_id=_clean_text(evidence.latest_event_id) or None,
        )
        with self.store.connect() as conn:
            self._ensure_radio_exists(conn, device_id)
            conn.execute(
                """
                INSERT INTO busy_evidence (
                    id,
                    radio_profile_id,
                    source_family,
                    reason_code,
                    severity,
                    evidence_timestamp_utc,
                    expiration_timestamp_utc,
                    description,
                    latest_event_id,
                    created_utc,
                    updated_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    radio_profile_id=excluded.radio_profile_id,
                    source_family=excluded.source_family,
                    reason_code=excluded.reason_code,
                    severity=excluded.severity,
                    evidence_timestamp_utc=excluded.evidence_timestamp_utc,
                    expiration_timestamp_utc=excluded.expiration_timestamp_utc,
                    description=excluded.description,
                    latest_event_id=excluded.latest_event_id,
                    updated_utc=excluded.updated_utc
                """,
                (
                    normalized.id,
                    device_id,
                    normalized.source_family,
                    normalized.reason_code,
                    normalized.severity,
                    normalized.evidence_timestamp_utc,
                    normalized.expiration_timestamp_utc,
                    normalized.description,
                    normalized.latest_event_id,
                    now,
                    now,
                ),
            )
            conn.commit()
        return self.get(normalized.id)

    def get(self, evidence_id: str) -> BusyEvidence:
        with self.store.connect() as conn:
            row = conn.execute("SELECT * FROM busy_evidence WHERE id=?", (_clean_text(evidence_id),)).fetchone()
            if row is None:
                raise KeyError(f"Unknown busy evidence id: {_clean_text(evidence_id)}")
            return self._evidence_from_row(dict(row))

    def clear(self, evidence_id: str) -> bool:
        with self.store.connect() as conn:
            cur = conn.execute("DELETE FROM busy_evidence WHERE id=?", (_clean_text(evidence_id),))
            conn.commit()
            return int(cur.rowcount or 0) > 0

    def active_for_radio(
        self,
        radio_profile_id: str | int,
        *,
        now_utc: Optional[datetime] = None,
    ) -> tuple[BusyEvidence, ...]:
        device_id = _radio_device_id(radio_profile_id)
        with self.store.connect() as conn:
            self._ensure_radio_exists(conn, device_id)
            rows = conn.execute(
                """
                SELECT *
                  FROM busy_evidence
                 WHERE radio_profile_id=?
              ORDER BY severity ASC, evidence_timestamp_utc DESC
                """,
                (device_id,),
            ).fetchall()
        evidence = tuple(self._evidence_from_row(dict(row)) for row in rows)
        active = tuple(item for item in evidence if not _is_expired(item, now_utc=now_utc))
        return tuple(sorted(active, key=_priority))

    def top_busy_reason(
        self,
        radio_profile_id: str | int,
        *,
        now_utc: Optional[datetime] = None,
    ) -> Optional[BusyEvidence]:
        active = self.active_for_radio(radio_profile_id, now_utc=now_utc)
        return active[0] if active else None

    @staticmethod
    def _ensure_radio_exists(conn: Any, device_id: int) -> None:
        row = conn.execute("SELECT id FROM device_profiles WHERE id=?", (int(device_id),)).fetchone()
        if row is None:
            raise KeyError(f"Unknown radio id: {radio_shared_state_id(device_id)}")

    @staticmethod
    def _evidence_from_row(row: Mapping[str, Any]) -> BusyEvidence:
        return BusyEvidence(
            id=_clean_text(row.get("id")),
            radio_profile_id=radio_shared_state_id(row.get("radio_profile_id")),
            source_family=_normalize_source_family(row.get("source_family")),
            reason_code=_clean_text(row.get("reason_code"), "unknown").lower(),
            severity=_normalize_severity(row.get("severity")),
            evidence_timestamp_utc=_clean_text(row.get("evidence_timestamp_utc"), _utc_now_iso()),
            expiration_timestamp_utc=_clean_text(row.get("expiration_timestamp_utc")) or None,
            description=_clean_text(row.get("description")),
            latest_event_id=_clean_text(row.get("latest_event_id")) or None,
        )
