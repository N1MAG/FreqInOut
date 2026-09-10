# Multi-Endpoint Scheduler MES-2 Evidence — 2026-09-10

Status: exit gate passed; MES-3 may begin.

## Scope And Safety

MES-2 replaces the station-global command bottleneck with one serialized worker
lane per normalized automated endpoint while preserving the scheduler's external
Qt/API boundary. It changes no database schema, stored setting, schedule
precedence, SOP authority, or production data.

The implementation adds `freqinout/core/scheduler_endpoint_lane.py` and connects
it through `freqinout/core/scheduler_engine.py`:

- each endpoint key owns one long-lived `ThreadPoolExecutor(max_workers=1)`;
- work for the same route is serialized and queued work coalesces to the newest
  generation;
- apply and its immediate verification readback execute in the same endpoint
  worker;
- timeout, failure count, exponential backoff, circuit/half-open state, pending
  occurrence, and last result are isolated by endpoint;
- completion is generation-fenced and marshalled through the existing scheduler
  thread callback seam;
- stop/remove are bounded and suppress late callbacks without manufacturing
  replacement threads for an uninterruptible adapter call; and
- compatible profiles sharing one route use the lowest profile ID as the sole
  deterministic writer, while differing desired states block every affected
  profile and publish endpoint-specific evidence.

The former station-global executor fields remain dormant compatibility mirrors
for older tests and callers. They no longer gate normal endpoint dispatch.
Endpoint-scoped background status sampling and central shared-safety snapshot
work remain MES-3 responsibilities.

## Delegation And Review

- High-reasoning primary model: lane and scheduler integration architecture,
  concurrency and migration safety, implementation, every delegated diff review,
  coordinator writer/conflict integration tests, complete acceptance run, and
  final gate decision.
- `gpt-5.6-terra` high: read-only audit of legacy scheduler facade, command,
  verification, retry, callback, and shutdown seams; plus five production-engine
  integration tests for endpoint isolation and lifecycle behavior.
- `gpt-5.6-luna` high: read-only deterministic test-matrix audit and thirteen
  focused lane/fault tests for serialization, coalescing, timeout, retry,
  half-open recovery, stale completion, resource bounds, and shutdown.

The primary review retained the compatibility facade, verified Python 3.9 syntax,
added resolved-profile alias/conflict coverage at `_apply_active_schedule_lanes`,
and reran the combined suite after all delegated files were present.

## Acceptance Evidence

Focused lane, engine-integration, fault, and runtime-routing suite:

```text
QT_QPA_PLATFORM=offscreen .venv/bin/python -m pytest -q \
  tests/test_scheduler_endpoint_lane_integration.py \
  tests/test_scheduler_endpoint_lanes.py \
  tests/test_scheduler_multi_endpoint_faults.py \
  tests/test_scheduler_runtime_command_routing.py
41 passed in 0.39s
```

Integrated scheduler/SOP/safety gate:

```text
QT_QPA_PLATFORM=offscreen .venv/bin/python -m pytest -q \
  tests/test_scheduler_*.py \
  tests/test_busy_evidence_service.py \
  tests/test_ptt_conflict_service.py \
  tests/test_condition_sop_*.py
171 passed, 1 skipped in 2.49s
```

The skip is the existing platform guard in `test_scheduler_shutdown.py`.

Additional results:

```text
.venv/bin/python tools/scheduler_multi_endpoint_baseline.py
# exit 0; historical MES-0 global-worker defect reproduced by the standalone
# characterization harness; cleanup stable

.venv/bin/python -m py_compile ...
# pass

git diff --check
# pass
```

## Exit Checklist

- one hung radio does not delay two healthy endpoint lanes: pass;
- one blocked verification readback does not delay another endpoint: pass;
- same endpoint remains serialized and latest pending intent wins: pass;
- failure, backoff, circuit, half-open recovery, and retry are endpoint-local:
  pass;
- stale and post-shutdown completions cannot mutate scheduler state: pass;
- stop remains nonblocking for uninterruptible fake calls: pass;
- repeated work does not create unbounded lane workers: pass;
- compatible shared-route profiles have one writer: pass;
- competing shared-route schedules fail closed with profile-specific evidence:
  pass; and
- schema, settings, or destructive migration: none.

MES-3 is responsible for endpoint-scoped status snapshots and retaining shared
PTT/RF Guard/antenna/frontend/amplifier arbitration as central station safety.
