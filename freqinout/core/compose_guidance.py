from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence


@dataclass(frozen=True)
class ComposeRadioOption:
    radio_id: int
    label: str


@dataclass(frozen=True)
class ComposePeerSchedule:
    callsign: str = ""
    band: str = ""
    frequency_mhz: Optional[float] = None
    mode: str = ""
    minutes_to_end: int = 0


@dataclass(frozen=True)
class ComposeLastHeard:
    radio_id: int = 0
    radio_label: str = ""
    band: str = ""
    source: str = ""
    age_label: str = ""


@dataclass(frozen=True)
class ComposePathEvidence:
    kind: str = ""
    radio_id: int = 0
    band: str = ""
    source: str = ""
    relay: str = ""
    age_label: str = ""
    snr: Optional[float] = None


@dataclass(frozen=True)
class ComposeSendRecommendation:
    radio_id: int = 0
    radio_label: str = ""
    frequency_mhz: Optional[float] = None
    band: str = ""
    mode: str = ""
    reason: str = ""
    confidence: str = "low"
    tune_available: bool = False
    path_kind: str = ""
    relay: str = ""


def recommend_compose_send_path(
    radios: Sequence[ComposeRadioOption],
    *,
    peer_schedule: Optional[ComposePeerSchedule] = None,
    path_evidence: Optional[ComposePathEvidence] = None,
    last_heard: Optional[ComposeLastHeard] = None,
    selected_radio_id: int = 0,
) -> ComposeSendRecommendation:
    """Choose the clearest operator guidance for an outbound RF message."""
    radio_by_id = {int(r.radio_id): r for r in radios if int(r.radio_id or 0) > 0}
    selected = radio_by_id.get(int(selected_radio_id or 0))
    fallback = selected or (radios[0] if radios else None)
    schedule = peer_schedule if peer_schedule and peer_schedule.frequency_mhz else None
    if schedule:
        radio = selected or fallback
        return ComposeSendRecommendation(
            radio_id=int(radio.radio_id) if radio else 0,
            radio_label=radio.label if radio else "",
            frequency_mhz=schedule.frequency_mhz,
            band=str(schedule.band or "").strip().upper(),
            mode=str(schedule.mode or "").strip().upper(),
            reason="Peer schedule is active now; use that band/frequency before sending.",
            confidence="high",
            tune_available=True,
            path_kind="peer_schedule",
        )
    path = path_evidence if path_evidence and path_evidence.kind else None
    if path:
        evidence_radio = radio_by_id.get(int(path.radio_id or 0))
        radio = evidence_radio or selected or fallback
        bits = []
        if path.kind == "direct":
            bits.append("Direct JS8 path seen")
        elif path.kind == "relay":
            bits.append(f"Relay path seen via {path.relay}" if path.relay else "Relay path seen")
        else:
            bits.append("Path evidence found")
        if path.band:
            bits.append(path.band)
        if path.age_label:
            bits.append(path.age_label)
        return ComposeSendRecommendation(
            radio_id=int(radio.radio_id) if radio else 0,
            radio_label=radio.label if radio else "",
            band=str(path.band or "").strip().upper(),
            reason="; ".join(bits) + ".",
            confidence="medium" if path.kind == "relay" else "high",
            tune_available=False,
            path_kind=str(path.kind or ""),
            relay=str(path.relay or ""),
        )
    heard = last_heard if last_heard and int(last_heard.radio_id or 0) in radio_by_id else None
    if heard:
        radio = radio_by_id[int(heard.radio_id)]
        source = str(heard.source or "").strip()
        age = str(heard.age_label or "").strip()
        reason_bits = ["Last received on this radio"]
        if source:
            reason_bits.append(f"via {source}")
        if age:
            reason_bits.append(age)
        return ComposeSendRecommendation(
            radio_id=int(radio.radio_id),
            radio_label=radio.label,
            band=str(heard.band or "").strip().upper(),
            reason="; ".join(reason_bits) + ".",
            confidence="medium",
            tune_available=False,
        )
    if fallback:
        return ComposeSendRecommendation(
            radio_id=int(fallback.radio_id),
            radio_label=fallback.label,
            reason="No active peer schedule or last-heard radio hint was found; using the selected JS8Call radio.",
            confidence="low",
            tune_available=False,
        )
    return ComposeSendRecommendation(reason="No JS8Call-capable radio is configured.", confidence="low")
