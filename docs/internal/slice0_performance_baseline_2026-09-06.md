# Slice 0 Performance Baseline — 2026-09-06

Status: automated exit gate passed on the macOS development host

This report is the retained evidence for Slice 0 of
`production_reliability_and_workflow_remediation_spec.md`. Personal logs and
runtime databases were not copied into the repository.

## Measurement Safety And Scope

- Legacy evidence came from the two operator-supplied logs named in the
  governing specification.
- The post-change production-size startup run used SQLite `.backup` snapshots of
  the active `freqinout.db` and `freqinout_nets.db` in a temporary configuration
  root.
- Scheduler tuning, Mesh startup, application launch, and background ingest were
  suppressed during automated startup/soak runs so validation could not operate
  the live station or mutate its production databases.
- The 30-minute soak used the real `MainWindow`, an isolated minimal profile,
  offscreen Qt, and operator-paced Ops/SOP/Settings/Help navigation, resize, and
  navigation-collapse interactions.
- Physical Linux production confirmation remains release/platform validation;
  the post-change measurements below were not taken on the Linux host.

## Legacy Supplied-Log Baseline

The read-only parser collected 3,324 samples. Values below are p95 unless noted.

| Metric | Budget | Legacy p95 | Result |
| --- | ---: | ---: | --- |
| Startup complete | 10,000 ms | 186,369.5 ms | Fail |
| Main-window construction | 10,000 ms | 160,008.9 ms | Fail |
| Database initialization | 10,000 ms | 18,953.9 ms | Fail |
| Native message sources | 500 ms | 73,904.6 ms | Fail |
| Message row projection | 500 ms | 29,459.2 ms | Fail |
| Native message files | 500 ms | 9,684.3 ms | Fail |
| Message file scan | 500 ms | 10,599.1 ms | Fail |
| Plan table rebuild | 500 ms | 11,943.4 ms | Fail |
| Ops heavy refresh | 500 ms | 4,946.6 ms | Fail |
| Slow UI refresh | 50 ms | 2,254.9 ms | Fail |
| Dependency process snapshot | 250 ms | 2,707.4 ms | Fail |
| Event-loop stalls | 0 | 32 observed | Fail |

The logs contained 389 slow UI refresh warnings and 841 slow dependency-process
snapshot records. The legacy logs predate the new first-usable-shell and
shutdown-complete spans.

Reproduce the report without starting FIO:

```text
.venv/bin/python tools/perf_report.py <log-path> [<log-path> ...]
```

Add `--json` for machine-readable output. The command returns nonzero when an
observed metric exceeds its budget; unobserved metrics are reported explicitly.

## Post-Change Production-Size Startup

| Metric | Budget | Observed | Result |
| --- | ---: | ---: | --- |
| First usable shell (warm) | 5,000 ms | 4,219.0 ms | Pass |
| Startup complete (cold ceiling) | 10,000 ms | 4,222.9 ms | Pass |
| Main-window construction | 10,000 ms | 2,565.0 ms | Pass |
| Database initialization | 10,000 ms | 380.3 ms | Pass |
| Graceful shutdown | 3,000 ms | 16.3 ms | Pass |
| Forced uncached process inventory | 250 ms | 19.4 ms maximum of 7 | Pass |

Heavy secondary workspaces are no longer part of first-usable construction.
Their domain-specific first-page/query budgets remain owned by later delivery
slices and are not claimed as passing merely because they were unobserved in the
startup run.

## Thirty-Minute Qt Soak

Command shape:

```text
QT_QPA_PLATFORM=offscreen .venv/bin/python tools/gui_slice0_soak.py \
  --config-dir <isolated-temp-root> --duration-sec 1800 --warmup-sec 6 \
  --json-out <isolated-temp-root>/report.json
```

Results:

| Measure | Observed |
| --- | ---: |
| Duration | 1,800 seconds |
| First usable shell | 855.7 ms |
| Construction | 743.3 ms |
| Event-loop samples | 17,932 |
| Operator-paced interactions | 871 |
| Screen switches | 436 |
| Resize cycles | 218 |
| Maximum event-loop lag | 33.7 ms |
| Shutdown | 156.4 ms |
| Hard Qt lifecycle warnings | 0 |

Result: **Pass**.

## Regression Evidence

The complete test-file set was executed in three process-stable partitions
because the monolithic repository suite has a pre-existing Qt/Mesh teardown
segmentation fault after its assertions. No test files were excluded:

- files sorting before `test_phase7_main_shell_ux.py`: 1,595 passed, 2 skipped;
- `test_phase7_main_shell_ux.py`: 127 passed;
- files sorting after it: 672 passed, 35 skipped.

Total: **2,394 passed, 37 skipped**.

Focused Slice 0 tests cover performance parsing, cached/coalesced dependency
refresh, cheap process inventory, cooperative cancellation, bounded shutdown
registration, deferred-screen replacement/wiring, UI-watchdog interruption,
and soak-controller behavior.
