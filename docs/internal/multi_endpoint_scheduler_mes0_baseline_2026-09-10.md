# Multi-Endpoint Scheduler MES-0 Characterization Baseline

Status: MES-0 exit gate passed; no production behavior change

Date: 2026-09-10

Authority: `multi_endpoint_scheduler_concurrency_spec.md`

## Purpose

MES-0 freezes the current scheduler boundary before endpoint-lane implementation
begins. It establishes evidence for behavior that must remain compatible and for
the station-global blocking defect that MES-2 must remove.

No production module, configuration schema, database, migration, or user data was
changed by this package.

## Development Host

- macOS 26.5.2, Darwin 25.5.0, arm64
- Apple M5, 10 logical CPUs, 24 GiB memory
- Python 3.11.16
- PySide6 6.8.1.1
- psutil 7.2.2

This is development-host characterization. Linux production performance and the
physical three-transceiver/two-SDR station remain MES-5 release gates.

## Current Production Boundary Confirmed

The audit and production-code characterization confirm:

- active schedule projection is radio-scoped and visits each assigned radio;
- pending intent has a latest slot per radio and coalesces a newer target for that
  radio;
- retained intents are nevertheless drained through one station-global path;
- control uses one station-global executor/future/pending key;
- control failure count and backoff are station-global;
- routine scheduler status uses primary/station-global cache identity;
- a blocked control call prevents an independent radio command from starting;
- shared PTT and RF Guard stop command dispatch before endpoint control;
- manual QSY state retains precedence over routine schedule correction; and
- shutdown invalidates the control generation so a late worker completion cannot
  publish success after stop.

These are characterization facts, not approval of the global execution model.

## Deterministic Fault Harness

The test-only harness provides:

- a controllable monotonic/UTC clock;
- explicit connect/apply/readback barriers with no timing-only sleep dependency;
- fake endpoint adapters and ordered correlation capture;
- current RSS, worker-thread, file-descriptor, and child-process snapshots;
- a bounded single-worker model of the current control boundary; and
- machine-readable and concise baseline CLI output.

The one-radio scenario completed connect, apply, and readback, then returned to
zero harness worker threads with stable file-descriptor and child-process counts.

In the three-radio scenario, Radio A was held at its apply barrier. Radio B and
Radio C were independently healthy but both received
`dispatch_blocked_global_worker`, and neither began apply. After controlled
cancellation/release, the worker terminated and resource counts returned to the
pre-run state. This deterministically reproduces the defect that endpoint lanes
must remove.

Harness dispatch-decision measurements on this run were:

| Scenario | Samples | p50 | p95 | Maximum |
| --- | ---: | ---: | ---: | ---: |
| One-radio accepted dispatch | 1 | 0.088 ms | 0.088 ms | 0.088 ms |
| Three-radio accepted/blocked decisions | 3 | 0.002 ms | 0.044 ms | 0.049 ms |

These values measure only the small test harness decision boundary. They exclude
production `SchedulerEngine`, Qt delivery, adapter work, and endpoint I/O and are
not production performance claims. The durable performance result from MES-0 is
that the harness is bounded and leaves no worker/resource growth after cleanup.

## Acceptance Evidence

Pre-change focused regression baseline:

```text
QT_QPA_PLATFORM=offscreen .venv/bin/pytest -q \
  tests/test_scheduler_event_radio_scope.py \
  tests/test_scheduler_executor_thread_leak_1_2_7.py \
  tests/test_scheduler_external_busy_evidence_publishers.py \
  tests/test_scheduler_fldigi_busy_evidence_publishers.py \
  tests/test_scheduler_manual_control_service.py \
  tests/test_scheduler_runtime_command_routing.py \
  tests/test_scheduler_shared_ptt_evidence_publishers.py \
  tests/test_scheduler_shutdown.py \
  tests/test_busy_evidence_service.py \
  tests/test_ptt_conflict_service.py \
  tests/test_condition_sop_audit.py \
  tests/test_condition_sop_execution.py \
  tests/test_condition_sop_invocation.py \
  tests/test_condition_sop_policy.py \
  tests/test_condition_sop_revert.py

116 passed, 1 skipped in 2.13s
```

Integrated MES-0 gate:

```text
QT_QPA_PLATFORM=offscreen .venv/bin/python -m pytest -q \
  tests/test_scheduler_*.py \
  tests/test_busy_evidence_service.py \
  tests/test_ptt_conflict_service.py \
  tests/test_condition_sop_*.py

132 passed, 1 skipped in 2.17s
```

The skip is the existing macOS guard in `test_scheduler_shutdown.py`; equivalent
shutdown-generation coverage ran in the new MES-0 characterization suite.

Additional gates:

```text
.venv/bin/python tools/scheduler_multi_endpoint_baseline.py
# exit 0; one-radio success, three-radio defect reproduction, cleanup stable

.venv/bin/python -m py_compile \
  tests/support/scheduler_fault_harness.py \
  tests/test_scheduler_fault_harness.py \
  tests/test_scheduler_mes0_characterization.py \
  tools/scheduler_multi_endpoint_baseline.py
# pass

git diff --check
# pass
```

## MES-0 Exit Gate

- [x] Existing single-radio and scheduler regression behavior is frozen.
- [x] Three-radio projection is characterized.
- [x] The station-global blocking defect is deterministic and measurable.
- [x] Timeout/backoff and status-scope gaps are characterized without changing
  production behavior.
- [x] Shared PTT/RF Guard, manual precedence, and shutdown-generation behavior are
  covered.
- [x] The fault harness is bounded and cleans up its worker/resources.
- [x] All existing focused scheduler acceptance tests pass.
- [x] No production code or schema changed.

MES-0 passes. MES-1 has not begun.

## Work Packages And Models

- Primary high-reasoning model: architecture/code audit, concurrency and safety
  boundary, delegated-diff review, integration, acceptance, and documentation.
- `gpt-5.6-terra`, high reasoning: deterministic fault harness, resource capture,
  baseline CLI, and focused harness tests.
- `gpt-5.6-luna`, high reasoning: production-code characterization tests for
  schedule projection, global control/status state, safety, precedence, and
  shutdown.
