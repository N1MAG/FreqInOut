from __future__ import annotations

from typing import Optional


VOICE_LSB_BANDS = {"160M", "80M", "60M", "40M", "30M"}
VOICE_USB_BANDS = {"20M", "17M", "15M", "12M", "10M", "6M", "2M"}


def normalize_band_label(band: object) -> str:
    val = str(band or "").strip().upper()
    if not val:
        return ""
    if val.endswith("M"):
        return val
    if val.isdigit():
        return f"{val}M"
    return val


def voice_sideband_for_band(band: object) -> str:
    b = normalize_band_label(band)
    if b in VOICE_LSB_BANDS:
        return "LSB"
    if b in VOICE_USB_BANDS:
        return "USB"
    # Default to USB when band is unknown/unspecified.
    return "USB"


def normalize_operating_group_mode(mode: object, band: object) -> str:
    raw = str(mode or "").strip().upper()
    if not raw:
        return ""
    if raw in {"DIGI", "DIGITAL", "DATA"}:
        return "Digi"
    if raw in {"USB", "LSB", "SSB", "VOICE"}:
        # Operating Group voice mode is presented/stored as SSB. Sideband is
        # derived at runtime from band (or explicit starting-mode override).
        return "SSB"
    return str(mode or "").strip()


def resolve_rig_voice_mode(mode: object, band: object) -> Optional[str]:
    raw = str(mode or "").strip().upper()
    if raw in {"USB", "LSB"}:
        return raw
    if raw in {"SSB", "VOICE"}:
        return voice_sideband_for_band(band)
    return None


def resolve_rig_mode(mode: object, band: object, voice_hint: object = None) -> Optional[str]:
    raw = str(mode or "").strip().upper()
    if not raw:
        return None
    if raw in {"USB", "LSB"}:
        return raw
    if raw in {"SSB", "VOICE"}:
        hint = str(voice_hint or "").strip().upper()
        if hint in {"USB", "LSB"}:
            return hint
        return voice_sideband_for_band(band)
    if raw in {"DIGI", "DIGITAL", "DATA", "JS8", "TRI"}:
        return "DIGI"
    return None
