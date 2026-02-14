from __future__ import annotations

import datetime as dt
import itertools
import json
import math
import random
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple


SUCCESS_OUTCOMES = {"HEARD", "DELIVERED", "ACKED", "QSO"}
FAIL_OUTCOMES = {"FAILED"}


@dataclass(frozen=True)
class CalibrationEvent:
    ts: dt.datetime
    origin_grid6: str
    target_type: str
    target_id: str
    band: str
    hour_bin: int
    success: int
    day_ordinal: int

    @property
    def key(self) -> Tuple[str, str, str, str]:
        return (self.origin_grid6, self.target_type, self.target_id, self.band)


@dataclass(frozen=True)
class _ValidationFeature:
    success: int
    modeled_prob: float
    ages_days_primary: Tuple[float, ...]
    historical_success_primary: Tuple[int, ...]
    historical_days_primary: Tuple[int, ...]
    ages_days_pooled: Tuple[float, ...]
    historical_success_pooled: Tuple[int, ...]
    historical_days_pooled: Tuple[int, ...]


def _parse_ts_utc(raw: object) -> Optional[dt.datetime]:
    txt = str(raw or "").strip()
    if not txt:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            parsed = dt.datetime.strptime(txt[:19], fmt)
            return parsed.replace(tzinfo=dt.timezone.utc)
        except Exception:
            continue
    return None


def _normalize_outcome(raw: object) -> str:
    return str(raw or "").strip().upper()


def load_contact_events(
    db_path: Path,
    *,
    target_type: str = "ALL",
    limit: int = 0,
) -> List[CalibrationEvent]:
    """
    Load calibration events from prop_contact_events.
    """
    if not db_path.exists():
        raise FileNotFoundError(f"DB does not exist: {db_path}")

    target_type = (target_type or "ALL").strip().upper()
    if target_type not in {"ALL", "OPERATOR", "STATE", "REGION"}:
        target_type = "ALL"

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='prop_contact_events'"
        )
        if not cur.fetchone():
            return []

        where = ["TRIM(IFNULL(band,'')) <> ''"]
        params: List[object] = []
        if target_type != "ALL":
            where.append("UPPER(TRIM(IFNULL(target_type,''))) = ?")
            params.append(target_type)
        where_sql = " AND ".join(where)
        limit_sql = ""
        if limit and limit > 0:
            limit_sql = " LIMIT ?"
            params.append(int(limit))

        cur.execute(
            f"""
            SELECT ts_utc, origin_grid6, target_type, target_id, band, outcome
            FROM prop_contact_events
            WHERE {where_sql}
            ORDER BY ts_utc ASC
            {limit_sql}
            """,
            tuple(params),
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    out: List[CalibrationEvent] = []
    for row in rows:
        ts = _parse_ts_utc(row["ts_utc"])
        if not ts:
            continue
        band = str(row["band"] or "").strip().upper()
        origin_grid6 = str(row["origin_grid6"] or "").strip().upper()
        tgt_type = str(row["target_type"] or "").strip().upper()
        tgt_id = str(row["target_id"] or "").strip().upper()
        if not (band and origin_grid6 and tgt_type and tgt_id):
            continue
        outcome = _normalize_outcome(row["outcome"])
        if outcome in FAIL_OUTCOMES:
            success = 0
        elif outcome in SUCCESS_OUTCOMES:
            success = 1
        else:
            continue
        out.append(
            CalibrationEvent(
                ts=ts,
                origin_grid6=origin_grid6,
                target_type=tgt_type,
                target_id=tgt_id,
                band=band,
                hour_bin=int(ts.hour // 3),
                success=success,
                day_ordinal=int(ts.date().toordinal()),
            )
        )
    return out


def _clamp_prob(value: float) -> float:
    return max(1e-6, min(1.0 - 1e-6, float(value)))


def _build_modeled_priors(training: Sequence[CalibrationEvent]) -> Mapping[str, Mapping]:
    global_total = len(training)
    global_success = sum(e.success for e in training)

    by_band: Dict[str, List[int]] = {}
    by_band_hour: Dict[Tuple[str, int], List[int]] = {}
    for ev in training:
        by_band.setdefault(ev.band, []).append(ev.success)
        by_band_hour.setdefault((ev.band, ev.hour_bin), []).append(ev.success)

    def posterior(successes: int, attempts: int, alpha: float = 2.0, beta: float = 2.0) -> float:
        return (float(successes) + alpha) / (float(attempts) + alpha + beta)

    global_prob = posterior(global_success, global_total)
    band_prob = {
        band: posterior(sum(vals), len(vals))
        for band, vals in by_band.items()
    }
    band_hour_prob = {
        key: posterior(sum(vals), len(vals))
        for key, vals in by_band_hour.items()
    }
    return {
        "global": global_prob,
        "band": band_prob,
        "band_hour": band_hour_prob,
    }


def _modeled_prob(
    priors: Mapping[str, Mapping],
    *,
    band: str,
    hour_bin: int,
) -> float:
    key = (band, int(hour_bin))
    bh = priors["band_hour"]  # type: ignore[index]
    b = priors["band"]  # type: ignore[index]
    if key in bh:
        return _clamp_prob(float(bh[key]))  # type: ignore[index]
    if band in b:
        return _clamp_prob(float(b[band]))  # type: ignore[index]
    return _clamp_prob(float(priors["global"]))  # type: ignore[arg-type]


def _calc_metric_components(prob: float, success: int) -> Tuple[float, float]:
    p = _clamp_prob(prob)
    y = 1.0 if int(success) else 0.0
    log_loss = -(y * math.log(p) + (1.0 - y) * math.log(1.0 - p))
    brier = (p - y) ** 2
    return float(log_loss), float(brier)


def calibrate_blend_parameters(
    events: Sequence[CalibrationEvent],
    *,
    validation_fraction: float = 0.2,
    max_validation: int = 4000,
    recent_window_days: int = 30,
    history_cap_days: int = 365,
    random_seed: int = 7,
    alpha_values: Optional[Sequence[float]] = None,
    beta_values: Optional[Sequence[float]] = None,
    half_life_values: Optional[Sequence[int]] = None,
    gate_attempt_values: Optional[Sequence[float]] = None,
    gate_unique_days_values: Optional[Sequence[int]] = None,
    max_blend_values: Optional[Sequence[float]] = None,
) -> Dict[str, Any]:
    """
    Calibrate empirical blend and gate constants from historical events.
    """
    if len(events) < 60:
        raise ValueError("Need at least 60 events for calibration.")

    ordered = sorted(events, key=lambda e: e.ts)
    split_idx = int(len(ordered) * (1.0 - validation_fraction))
    split_idx = max(30, min(len(ordered) - 30, split_idx))
    training = ordered[:split_idx]
    validation = ordered[split_idx:]
    if not validation:
        raise ValueError("No validation events after split.")

    if max_validation and len(validation) > max_validation:
        rnd = random.Random(int(random_seed))
        picks = sorted(rnd.sample(range(len(validation)), int(max_validation)))
        validation = [validation[i] for i in picks]

    alpha_values = list(alpha_values or [1.0, 2.0, 3.0, 4.0])
    beta_values = list(beta_values or [2.0, 3.0, 4.0, 5.0])
    half_life_values = list(half_life_values or [30, 45, 60, 75, 90, 120])
    gate_attempt_values = list(gate_attempt_values or [4.0, 8.0, 12.0])
    gate_unique_days_values = list(gate_unique_days_values or [2, 3, 4])
    max_blend_values = list(max_blend_values or [0.65, 0.75, 0.85])

    priors = _build_modeled_priors(training)
    train_by_key: Dict[Tuple[str, str, str, str], List[CalibrationEvent]] = {}
    train_by_pool: Dict[Tuple[str, str, str], List[CalibrationEvent]] = {}
    for ev in training:
        train_by_key.setdefault(ev.key, []).append(ev)
        train_by_pool.setdefault((ev.origin_grid6, ev.target_type, ev.band), []).append(ev)

    features: List[_ValidationFeature] = []
    for ev in validation:
        hist_primary = train_by_key.get(ev.key, [])
        hist_pool = train_by_pool.get((ev.origin_grid6, ev.target_type, ev.band), [])
        ages_primary: List[float] = []
        hs_primary: List[int] = []
        days_primary: List[int] = []
        for prev in hist_primary:
            age_days = (ev.ts - prev.ts).total_seconds() / 86400.0
            if age_days <= 0:
                continue
            if history_cap_days > 0 and age_days > float(history_cap_days):
                continue
            ages_primary.append(age_days)
            hs_primary.append(prev.success)
            days_primary.append(prev.day_ordinal)
        ages_pooled: List[float] = []
        hs_pooled: List[int] = []
        days_pooled: List[int] = []
        for prev in hist_pool:
            age_days = (ev.ts - prev.ts).total_seconds() / 86400.0
            if age_days <= 0:
                continue
            if history_cap_days > 0 and age_days > float(history_cap_days):
                continue
            ages_pooled.append(age_days)
            hs_pooled.append(prev.success)
            days_pooled.append(prev.day_ordinal)
        features.append(
            _ValidationFeature(
                success=ev.success,
                modeled_prob=_modeled_prob(priors, band=ev.band, hour_bin=ev.hour_bin),
                ages_days_primary=tuple(ages_primary),
                historical_success_primary=tuple(hs_primary),
                historical_days_primary=tuple(days_primary),
                ages_days_pooled=tuple(ages_pooled),
                historical_success_pooled=tuple(hs_pooled),
                historical_days_pooled=tuple(days_pooled),
            )
        )

    baseline_log = 0.0
    baseline_brier = 0.0
    for ft in features:
        ll, br = _calc_metric_components(ft.modeled_prob, ft.success)
        baseline_log += ll
        baseline_brier += br
    baseline_log /= max(1, len(features))
    baseline_brier /= max(1, len(features))

    pre_by_half: Dict[int, List[Tuple[float, float, float, int, float, float, float, float, int, float]]] = {}
    for half in half_life_values:
        cur: List[Tuple[float, float, float, int, float, float, float, float, int, float]] = []
        hf = float(max(1, int(half)))
        for ft in features:
            # Primary key history (origin + target + band).
            p_wa = 0.0
            p_ws = 0.0
            p_wa_recent = 0.0
            p_unique_recent_days: set[int] = set()
            p_min_age = float("inf")
            for age, succ, day_ord in zip(
                ft.ages_days_primary,
                ft.historical_success_primary,
                ft.historical_days_primary,
            ):
                p_min_age = min(p_min_age, age)
                w = math.pow(0.5, age / hf)
                p_wa += w
                p_ws += w * float(succ)
                if age <= float(recent_window_days):
                    p_wa_recent += w
                    p_unique_recent_days.add(int(day_ord))
            p_recency = 0.0 if not math.isfinite(p_min_age) else math.pow(0.5, p_min_age / hf)

            # Pooled history (origin + target_type + band).
            q_wa = 0.0
            q_ws = 0.0
            q_wa_recent = 0.0
            q_unique_recent_days: set[int] = set()
            q_min_age = float("inf")
            for age, succ, day_ord in zip(
                ft.ages_days_pooled,
                ft.historical_success_pooled,
                ft.historical_days_pooled,
            ):
                q_min_age = min(q_min_age, age)
                w = math.pow(0.5, age / hf)
                q_wa += w
                q_ws += w * float(succ)
                if age <= float(recent_window_days):
                    q_wa_recent += w
                    q_unique_recent_days.add(int(day_ord))
            q_recency = 0.0 if not math.isfinite(q_min_age) else math.pow(0.5, q_min_age / hf)

            cur.append(
                (
                    p_wa,
                    p_ws,
                    p_wa_recent,
                    len(p_unique_recent_days),
                    p_recency,
                    q_wa,
                    q_ws,
                    q_wa_recent,
                    len(q_unique_recent_days),
                    q_recency,
                )
            )
        pre_by_half[int(half)] = cur

    results: List[Dict[str, Any]] = []
    combos = itertools.product(
        alpha_values,
        beta_values,
        half_life_values,
        gate_attempt_values,
        gate_unique_days_values,
        max_blend_values,
    )
    for alpha, beta, half_life, gate_attempt, gate_days, max_blend in combos:
        half_stats = pre_by_half[int(half_life)]
        total_log = 0.0
        total_brier = 0.0
        gate_on = 0
        gate_on_pooled = 0
        for idx, ft in enumerate(features):
            (
                p_wa,
                p_ws,
                p_wa_recent,
                p_unique_recent,
                p_recency,
                q_wa,
                q_ws,
                q_wa_recent,
                q_unique_recent,
                q_recency,
            ) = half_stats[idx]
            wa = p_wa
            ws = p_ws
            wa_recent = p_wa_recent
            unique_recent = p_unique_recent
            recency_factor = p_recency
            blend_cap = float(max_blend)
            if not (wa_recent >= float(gate_attempt) and unique_recent >= int(gate_days)):
                # Fallback pool if specific target history is too sparse.
                wa = q_wa
                ws = q_ws
                wa_recent = q_wa_recent
                unique_recent = q_unique_recent
                recency_factor = q_recency
                blend_cap = float(max_blend) * 0.8
            empirical = (ws + float(alpha)) / (wa + float(alpha) + float(beta))
            gate = bool(wa_recent >= float(gate_attempt) and unique_recent >= int(gate_days))
            blend = 0.0
            if gate:
                gate_on += 1
                if blend_cap < float(max_blend):
                    gate_on_pooled += 1
                sample_factor = max(0.0, min(1.0, (wa_recent - float(gate_attempt)) / 24.0))
                blend = max(0.0, min(float(blend_cap), sample_factor * recency_factor))
            prob = (ft.modeled_prob * (1.0 - blend)) + (empirical * blend)
            ll, br = _calc_metric_components(prob, ft.success)
            total_log += ll
            total_brier += br
        n = max(1, len(features))
        results.append(
            {
                "alpha": float(alpha),
                "beta": float(beta),
                "half_life_days": int(half_life),
                "gate_attempt_min": float(gate_attempt),
                "gate_unique_days_min": int(gate_days),
                "max_blend_weight": float(max_blend),
                "log_loss": total_log / n,
                "brier": total_brier / n,
                "gate_activation_rate": float(gate_on) / float(n),
                "pooled_activation_rate": float(gate_on_pooled) / float(n),
            }
        )

    results.sort(key=lambda r: (r["log_loss"], r["brier"]))
    best = results[0]
    improvements = {
        "log_loss_delta_vs_modeled": float(best["log_loss"]) - float(baseline_log),
        "brier_delta_vs_modeled": float(best["brier"]) - float(baseline_brier),
    }
    better_than_modeled = bool(
        float(best["log_loss"]) < float(baseline_log)
        or float(best["brier"]) < float(baseline_brier)
    )
    suggested_settings = {
        "prop_blend_enabled": 1 if better_than_modeled else 0,
        "prop_empirical_alpha": round(float(best["alpha"]), 4),
        "prop_empirical_beta": round(float(best["beta"]), 4),
        "prop_decay_half_life_days": int(best["half_life_days"]),
        "prop_blend_gate_attempt_min": round(float(best["gate_attempt_min"]), 4),
        "prop_blend_gate_unique_days_min": int(best["gate_unique_days_min"]),
        "prop_blend_max_weight": round(float(best["max_blend_weight"]), 4),
    }
    summary = {
        "events_total": len(ordered),
        "training_events": len(training),
        "validation_events": len(validation),
        "recent_window_days": int(recent_window_days),
        "history_cap_days": int(history_cap_days),
        "baseline_modeled": {
            "log_loss": baseline_log,
            "brier": baseline_brier,
        },
        "best": best,
        "recommended_mode": "BLENDED" if better_than_modeled else "MODELED_ONLY",
        "improvements": improvements,
        "top_candidates": results[:5],
        "suggested_settings": suggested_settings,
    }
    return summary


def apply_suggested_settings(settings_db_path: Path, settings_values: Mapping[str, Any]) -> None:
    settings_db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(settings_db_path)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS kv (
                key TEXT PRIMARY KEY,
                value TEXT
            )
            """
        )
        payload = [(k, json.dumps(v)) for k, v in settings_values.items()]
        conn.executemany(
            "INSERT OR REPLACE INTO kv(key, value) VALUES (?, ?)",
            payload,
        )
        conn.commit()
    finally:
        conn.close()


def write_calibration_report(path: Path, report: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = dict(report)
    payload["generated_utc"] = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
