# Radio Operational Health / Cache Hotfix Specification

Status: `Approved—queued for next point release`

Date: 2026-09-29

Maintainer approval: approved on 2026-09-29 after private WIP testing. The
approval queues this hotfix for the next accumulated point release; it does not
authorize a standalone public release or public-branch push.

## Purpose

Make the Station Control Bar and Station Overview answer one operator question:
**can this radio perform its configured work now?** Imperfect setup or an
optional application must remain visible in Health Details without making an
otherwise operational radio yellow.

This hotfix is deliberately cache-only. It does not add a status poll, socket
call, process scan, settings read, or database read to a paint or refresh path.

## Supplied evidence and root cause

The supplied `freqinout-status.log` contains repeated successful FLRig
frequency commands for the endpoint on port 12345 while the UI could still
report FLRig unavailable. The same log shows the independent endpoint on port
12346 entering endpoint-local backoff after repeated failures. The intended
presentation is therefore green for the working radio and yellow for only the
stalled radio.

The false warning came from three interacting behaviors:

1. `DeviceRuntime.snapshot(cache_only=True)` intentionally skipped live probes
   but converted the resulting absence of control evidence to
   `control_ready=False` and, for the primary radio, a warning.
2. The command bar obtained the scheduler's exact per-endpoint operational
   summary but copied only its text. It did not use the summary's state to
   correct `control_ready` or the indicator color.
3. An empty radio-scoped service cache fell back to a station-global process
   cache. In a multi-endpoint setup this could apply one radio's process status
   to another radio.

## Authoritative cache model

- The scheduler endpoint registry remains the owner of endpoint readback,
  staleness, single-flight refresh, retry, and backoff evidence.
- `get_endpoint_operational_summaries()` remains the immutable, cache-only
  per-radio projection consumed by UI surfaces.
- Runtime snapshots carry a tri-state `control_ready`: `True` (verified or
  operational), `False` (known operational impairment), or `None` (no current
  evidence/checking).
- Station Control Bar and Station Overview use one pure classification helper.
  Neither surface maintains an independent mutable control cache.
- An explicit empty radio-scoped service cache is authoritative. It never
  falls back to another radio or the station-global dependency snapshot.
- Application/process status remains detail/advisory evidence. It does not
  override authoritative endpoint operation.

## Operator state matrix

| Cached endpoint state | Indicator | `control_ready` | Meaning |
| --- | --- | --- | --- |
| `on_schedule_verified` | green | `True` | Current readback matches current intent. |
| `js8_verification_unavailable` | green | `True` | RF operation is verified; JS8 offset verification is advisory. |
| `applying_schedule` | neutral | `None` | Work is in progress; no failure is asserted. |
| `waiting_shared_resource` | neutral | `None` | Normal coordinated hold. |
| `manual_tuning` | neutral | `None` | No automated-control success or failure is claimed. |
| `verification_unavailable` or no current evidence | neutral | `None` | Checking/unknown; stale evidence never remains green. |
| `control_stalled` | yellow | `False` | This endpoint's control lane is in retry/backoff. |
| `endpoint_unavailable` | yellow | `False` | Current endpoint evidence says the endpoint is unavailable. |
| `receiver_unavailable` | yellow | `False` | Configured receiver control cannot operate. |
| `readback_mismatch` | yellow | `True` | Endpoint responds, but the radio is not in the intended state. |

RF Guard and explicit off-schedule findings remain operational blockers and
retain warning/error treatment. Normal busy conditions, optional helper apps,
profile parity, launch state, and other setup imperfections remain visible as
advisories. “Needs attention” is not itself a reason to color a radio yellow.

## UI behavior

- Green summary label: **Operational**.
- Neutral summary label: **Checking** (or **No checks** when no radio health
  surface applies).
- Yellow summary labels remain **Review** or **Off Schedule**; RF Guard retains
  its existing wording and severity.
- Health Details explicitly distinguish **Affects operation** from
  **Advisory**.
- An operator's explicit radio selection is stable; another radio's warning
  cannot immediately steal focus.
- Each radio is classified only from its own endpoint summary.

## Persistence and migration

No schema, settings, profile, installer, or migration change is permitted.
All added fields are transient runtime snapshot fields with backward-compatible
defaults.

## Acceptance criteria

1. Cache-only runtime snapshots perform no endpoint, PTT, frequency, VarAC,
   process, database, or settings I/O.
2. Missing cache evidence produces neutral `control_ready=None`, never a false
   unavailable warning.
3. The command bar and Station Overview produce identical classification for
   every scheduler operational state.
4. A verified endpoint remains green when optional software has warnings or
   errors; those findings remain available as advisories.
5. A stalled/unavailable endpoint is yellow, and a healthy peer remains green.
6. Stale/unknown evidence is neutral, not green.
7. Explicit empty radio-scoped status never falls back to global status.
8. Off-schedule and RF Guard regression behavior remains intact.
9. The maintainer performs the final two-radio visual/operational pass before
   this hotfix is approved for release.

## Out of scope

The separately discussed per-radio, rig-control-gated startup sequence is not
part of this hotfix. It requires its own lifecycle specification and approval.
