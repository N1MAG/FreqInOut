# Multi-Endpoint Scheduler MES-1 Evidence — 2026-09-10

Status: exit gate passed; MES-2 may begin.

## Scope And Safety

MES-1 adds the immutable, Qt-free coordination boundary required before command
workers can be isolated. It changes no database schema, configuration value,
schedule precedence, endpoint command execution, or production data.

The implementation is `freqinout/core/scheduler_coordination.py`:

- `EndpointKey` normalizes adapter family, local host text, port, transport, and
  optional target without DNS or network access;
- `endpoint_binding_from_resolved_profile()` consumes the linked profile already
  resolved by FIO's configuration layer;
- observers and unknown/manual control remain non-automated and profile-scoped;
- `snapshot_from_resolved_lanes()` deep-freezes the current scheduler's selected
  source and entry without re-evaluating NET/SOP/HF precedence;
- `EndpointIntent` and `EndpointResult` are immutable, endpoint-scoped value
  objects with explicit generations and terminal result states; and
- `coordinate_schedule_snapshot()` deterministically coalesces unchanged state,
  collapses compatible same-route aliases, and blocks all writers involved in a
  capability, desired-state, or multi-route ownership conflict.

No cross-protocol hardware identity is guessed. A FLRig route, RigCtlD route,
JS8Call route, or future SDR application route identifies a control service. It
does not prove that two different service routes do or do not command the same
physical radio. A future additive operator-owned association is required before
FIO can safely merge that relationship.

## Delegation And Review

- High-reasoning primary model: architecture, concurrency and migration safety,
  production configuration interpretation, implementation of the coordinator,
  review of the delegated test diff, integrated acceptance, and gate decision.
- `gpt-5.6-terra` high: read-only audit of resolved multi-radio configuration,
  route identity, schedule precedence seams, runtime factories, manual authority,
  and compatibility test anchors. No files changed.
- `gpt-5.6-luna` high: 18 focused tests covering identity normalization, immutable
  snapshots, profile projection, alias/conflict behavior, generations, randomized
  ordering, result validation, and the no-I/O boundary. The primary review
  replaced Python 3.10-only test annotations to preserve FIO's Python 3.9 support.

## Acceptance Evidence

Focused MES-1 suite:

```text
.venv/bin/python -m pytest -q tests/test_scheduler_endpoint_identity.py
18 passed in 0.04s
```

Integrated scheduler/SOP/safety gate:

```text
QT_QPA_PLATFORM=offscreen .venv/bin/python -m pytest -q \
  tests/test_scheduler_*.py \
  tests/test_busy_evidence_service.py \
  tests/test_ptt_conflict_service.py \
  tests/test_condition_sop_*.py
150 passed, 1 skipped in 2.26s
```

The skip is the pre-existing platform guard in `test_scheduler_shutdown.py`.

Additional results:

```text
.venv/bin/python tools/scheduler_multi_endpoint_baseline.py
# exit 0; legacy three-radio global-worker defect still reproduced; cleanup stable

.venv/bin/python -m py_compile ...
# pass

git diff --check
# pass
```

## Exit Checklist

- deterministic identity/ordering/property coverage: pass;
- immutable resolved schedule input and output: pass;
- existing source precedence and single-radio output preserved: pass;
- duplicate same-route writers collapsed or blocked, never concurrently emitted:
  pass;
- multi-route ownership conflict blocks every affected route: pass;
- coordinator database/socket/file/adapter/Qt I/O: none; and
- schema or destructive migration: none.

MES-2 is responsible for replacing the legacy station-global command executor
with isolated serialized endpoint lanes behind the existing scheduler API.
