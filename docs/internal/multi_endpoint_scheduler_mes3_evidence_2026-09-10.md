# Multi-Endpoint Scheduler MES-3 Evidence — 2026-09-10

Status: exit gate passed; MES-4 may begin.

## Scope And Safety

MES-3 replaces target-radio dependence on station-global status with immutable,
endpoint-scoped snapshots while retaining central arbitration for resources that
are physically shared. It changes no database schema, stored setting, schedule
precedence, SOP authority, or production data.

The implementation adds `freqinout/core/scheduler_endpoint_status.py` and
integrates it through `scheduler_engine.py` and `station_runtime_manager.py`:

- each normalized endpoint route owns one serialized status worker and cache;
- cached reads are immediate and never perform adapter I/O;
- refresh is single-flight with endpoint-local TTL, timeout, exponential
  backoff, generation fencing, invalidation, metrics, and bounded shutdown;
- a slow or hung status source makes only its own snapshot stale;
- command-lane readback publishes an authoritative endpoint snapshot and fences
  an older polling result;
- explicit targets never inherit the primary radio's frequency or PTT state;
- unknown, stale, failed, or timed-out PTT state for a controllable target fails
  closed before an automated command;
- local/shared PTT evidence, RF conflict evaluation, health keys, and scheduler
  events carry the target radio identity; and
- shared PTT and RF/antenna/frontend/amplifier checks remain centrally
  serialized using cached endpoint evidence, without endpoint I/O on the
  scheduler or UI thread.

The legacy primary-radio status path remains available for single-radio callers
that do not identify a target. Its established behavior is unchanged.

## Delegation And Review

- High-reasoning primary model: endpoint-status and shared-safety architecture,
  scheduler integration, concurrency and compatibility review, every delegated
  diff review, combined acceptance run, and exit decision.
- `gpt-5.6-terra` high: status/safety seam audit and the endpoint status registry;
  separate target-safety work added target-aware runtime-manager evaluation and
  five focused shared-safety tests.
- `gpt-5.6-luna` high: six deterministic registry isolation tests and eight
  production-engine integration tests covering cached reads, isolation,
  shutdown fencing, target event/evidence identity, and fail-closed evaluation.

The primary review added the completion/request compatibility surface, corrected
deterministic clock and timeout expectations, preserved legacy monkeypatch
compatibility, and reran the complete gate after all delegated work was present.

## Acceptance Evidence

Focused MES-3 status and safety suite:

```text
QT_QPA_PLATFORM=offscreen .venv/bin/python -m pytest -q \
  tests/test_scheduler_endpoint_status_engine_integration.py \
  tests/test_scheduler_endpoint_status_isolation.py \
  tests/test_station_runtime_target_safety.py
19 passed in 1.28s
```

Integrated scheduler/SOP/station-safety gate:

```text
QT_QPA_PLATFORM=offscreen .venv/bin/python -m pytest -q \
  tests/test_scheduler_*.py \
  tests/test_busy_evidence_service.py \
  tests/test_ptt_conflict_service.py \
  tests/test_condition_sop_*.py \
  tests/test_station_runtime_*.py \
  tests/test_multi_rig_wave4_phase_e_slice1.py \
  tests/test_multi_rig_wave4_phase_e_slice2.py
213 passed, 3 skipped in 7.23s
```

The skips are existing platform/optional-environment guards; all new MES-3
tests ran on this host.

Additional results:

```text
.venv/bin/python tools/scheduler_multi_endpoint_baseline.py
# exit 0; MES-0 historical global-worker defect remains reproducible only in
# the standalone characterization harness; cleanup stable

.venv/bin/python -m py_compile ...
# pass

git diff --check
# pass
```

## Exit Checklist

- cached endpoint summaries perform no adapter I/O: pass;
- a blocked status read does not delay another endpoint: pass;
- duplicate refreshes are single-flight with bounded worker counts: pass;
- timeout, error, backoff, and recovery are endpoint-local: pass;
- invalidated, timed-out, and post-shutdown results cannot publish late state:
  pass;
- explicit targets do not inherit primary-radio status: pass;
- missing/stale/failed target PTT evidence cannot authorize control: pass;
- local/shared PTT evidence and health/event presentation are target-qualified:
  pass;
- shared-resource conflicts remain central, cached, fail-closed, and auditable:
  pass;
- legacy untargeted primary behavior remains compatible: pass; and
- schema, settings, or destructive migration: none.

MES-4 may now integrate receive-only SDR lanes. Physical five-endpoint and long
soak qualification remain MES-5 release gates rather than MES-3 unit-gate
claims.
