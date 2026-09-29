# Launch Readiness Lifecycle Hotfix Specification

Status: `Awaiting maintainer pass approval`

Date: 2026-09-29

## Purpose

Make sequential application startup recognize the application FIO just
launched without weakening the immutable process evidence used to prevent
duplicate instances. Endpoint-backed applications must complete when their
exact configured endpoint is ready, process-only applications must complete
after the exact child FIO owns remains stable, and an exited launch command
must not consume the entire readiness timeout.

## Observable problem

The production run in `freqinout (89).log` completed the new per-radio control
gate correctly, then reported six ordinary application timeouts:

- FLDigi accepted mode and carrier-offset commands before its timeout;
- JS8Call's API was active about twelve seconds after launch but it still
  timed out, causing dependent CommStat to be skipped;
- FLAmp and VarAC visibly started but timed out;
- the `rigctl-dx10` custom command exited with return code 1 after about one
  second, but Launch Control waited the full timeout;
- each wait was about thirty seconds because an older saved
  `launch_readiness_timeout_sec=30` remained in the upgraded station.

The accepted process inventory is deliberately captured before the first
`Popen` and remains immutable for the complete launch transaction. Ordinary
readiness incorrectly asks that inventory to prove the existence of a process
FIO started afterward. This is the same lifecycle boundary already corrected
for the radio-control gate, but it remains in the general application path.

## Required behavior

1. The immutable pre-launch process inventory remains authoritative for
   duplicate prevention, exact-instance attribution, collision checks, and
   already-running decisions. It is never refreshed or reinterpreted to prove
   a process FIO launched later.
2. After `Popen` succeeds, FIO retains the exact child object for the current
   launch row separately from the station-wide preflight inventory.
3. FLDigi readiness after launch is the exact configured XML-RPC endpoint.
   JS8Call readiness after launch is the exact configured API endpoint. These
   checks do not require the pre-launch process inventory to contain the new
   process.
4. Endpoint readiness requests remain worker-owned and endpoint-scoped. A
   launch poll may request fresh endpoint evidence but may not force another
   station-wide process scan or run network work on the Qt GUI thread.
5. Process-only applications become ready after their exact child remains
   alive for a short three-second stabilization period. VarAC retains its
   existing twelve-second post-ready settling delay before the next launch.
6. A successful platform launcher (`open`/`xdg-open`) or explicit custom tool
   may exit with code 0 without being treated as an application crash. An
   endpoint-backed row must still prove its endpoint. A process-only row still
   observes the stabilization period.
7. Any nonzero child exit is reported immediately as `failed`, including the
   return code. A directly executed application that exits with code 0 before
   readiness is also failed; only qualified launcher/custom-tool commands may
   use successful early exit semantics.
8. JS8Call is recorded as `launched` as soon as its endpoint is ready. Existing
   dependency semantics and the existing four-second JS8 dependent settling
   delay then allow CommStat or JS8Spotter to proceed.
9. The current ninety-second readiness default remains the upper bound for an
   ordinary application. The exact legacy saved value `30` is normalized in
   memory to ninety seconds for each launch transaction. No database write or
   schema migration occurs, and other explicit timeout values remain intact.
10. Radio-control applications retain their separate thirty-second physical
    radio gate and fresh readback requirements.
11. Cancellation, endpoint preflight, self-launch blocking, structured VarAC
    commands, managed working directories, window-title handling, shared
    identities, and per-radio ordering remain unchanged.

## Performance and safety contract

- No new process walk, database read, socket call, XML-RPC call, or API request
  runs directly on the Qt GUI thread.
- The exact `Popen` child is polled non-blockingly; `wait()` is never used.
- Endpoint polling continues through the existing dependency-status worker
  with `force_process_snapshot=False`.
- Only one launch row owns the current child readiness record. The record is
  cleared on success, failure, timeout, cancellation, or sequence completion.
- No schema, durable launch recipe, radio profile, or endpoint configuration
  changes are permitted.

## Acceptance criteria

1. A newly launched FLDigi can become ready from its exact endpoint even while
   the immutable preflight inventory reports its process absent.
2. A newly launched JS8Call becomes `launched` when its exact API responds;
   CommStat is not blocked and observes the existing four-second settling
   delay.
3. FLAmp and VarAC can become ready from launch-owned child liveness after the
   stabilization period without a new process inventory.
4. A nonzero custom-tool exit is failed on the next readiness poll rather than
   timing out.
5. An unexpected successful exit from a directly executed application is not
   mistaken for readiness; qualified platform launchers and custom tools keep
   their explicit successful-exit behavior.
6. A legacy saved timeout of 30 seconds uses 90 seconds in memory. The
   radio-control gate remains capped at 30 seconds.
7. Existing duplicate prevention, endpoint collision, launch identity,
   per-radio gate, shared dependency, JS8 identity, VarAC, Windows subprocess,
   cancellation, and settings-preview tests remain green.
8. Native operator testing confirms FLDigi and JS8Call finish when their
   endpoints are usable, VarAC and FLAmp no longer consume the full timeout,
   CommStat follows a ready JS8Call, and a failing custom command reports its
   return code promptly.

## Work-package ownership

- primary `gpt-6-astra` (high reasoning): lifecycle design, orchestrator
  implementation, focused regression tests, diff review, specification and
  work-log reconciliation, and final exit-gate decision.
- no delegated package: the implementation is a tightly coupled process and
  endpoint lifecycle correction and the active execution environment does not
  authorize sub-agent delegation for this turn.

## Implementation and acceptance evidence

Implemented on private branch
`wip/private-testing-multi-rig-1.2.3-not-ready` for maintainer qualification.

- `.venv/bin/python -m pytest -q
  tests/test_launch_readiness_lifecycle_hotfix.py
  tests/test_launch_control_radio_gate_hotfix.py`: **22 passed**.
- The broader launch bundle, launch identity, endpoint, VarAC, guided setup,
  Windows subprocess, receiver, status, startup-surface, and Settings
  regression partition: **283 passed, 2 expected platform skips**.
- `.venv/bin/python -m py_compile` for the changed Python files: pass.
- `git diff --check`: pass.

Automated implementation gate: **passed**.

External qualification gate: **open**. Native production testing must confirm
FLDigi and JS8Call advance from their live endpoints, FLAmp and VarAC advance
from launch-owned child stability, CommStat follows ready JS8Call after the
existing delay, and a failing custom tool reports its return code promptly.
