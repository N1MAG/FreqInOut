# Multi-Endpoint Scheduler MES-5 Evidence — 2026-09-10

Status: implementation and automated macOS qualification complete. Linux
lifecycle and the physical three-transceiver / two-SDR matrix remain production
evidence gates and are not inferred from macOS or fake adapters.

## Lifecycle And Reconfiguration

MES-5 completes the scheduler-owned lifecycle boundary:

- active configuration is fingerprinted only from endpoint identity, control,
  and shared-RF safety fields;
- editing, disabling, or removing a profile retires only affected command and
  status lanes, purges their pending/last-applied state, increments the endpoint
  configuration epoch, and fences late callbacks;
- unchanged peer routes retain their worker and cached status;
- a forced configuration refresh now evaluates all active endpoint lanes rather
  than falling through the legacy singleton evaluator;
- first non-urgent endpoint reads are deterministically staggered over 750 ms,
  while current schedule commands remain urgent and independently dispatched;
- application resume, sleep/wake gaps, monotonic resets, and forward/backward
  wall-clock corrections retire and generation-fence pre-resume command/status
  lanes, invalidate assumed actual state, and offer only the currently valid
  schedule intent—missed transitions are not replayed;
- one endpoint can reset its backoff and retry without resetting peers; and
- shutdown rejects new work, stops Qt scheduling callbacks, fences generations,
  cancels queued work, joins cooperative endpoint workers within a bounded
  aggregate wait, and records survivor diagnostics. Scheduler-owned serial
  workers are daemon-isolated, so a contract-violating adapter that never
  returns cannot keep the Python process alive.

Receiver tune/readback uses a two-second local-operation target under the
existing eight-second absolute compatibility ceiling. The receive-only contract
still has no PTT/transmit surface.

## Cache-Only UI And Diagnostics

Station Overview and station command refreshes now request cache-only runtime
snapshots. Card selection, expansion, theme refresh, and periodic repaint do not
own process walks, VarAC calls, PTT reads, frequency reads, or receiver probes.
The scheduler supplies immutable operational wording such as `On schedule ·
verified`, `Applying schedule`, `Manual tuning`, `Waiting for shared RF
resource`, and `Control stalled · other radios unaffected`.

The bounded multi-endpoint diagnostic snapshot includes configuration/schedule
revision, safe endpoint labels plus hashes, lane generation/failure/circuit
state, status-cache metrics, lifecycle evidence, operational wording, and the
last shutdown result. The UI hang watchdog writes this snapshot before its
thread dump. It does not perform endpoint I/O or include credentials.

## Synthetic Qualification Harness

`tools/scheduler_multi_endpoint_soak.py` runs the real endpoint-lane registry
against eight normalized routes. Its default is a 30-minute real-time run. It
issues simultaneous transitions and deterministically includes a slow receiver,
periodic disconnect/reconnect, and a repeatedly failing endpoint. Expected
faults are separated from unexpected failures. Retained latency evidence uses a
deterministic bounded reservoir across the complete run rather than retaining
only early samples. The report includes a fail-closed 250 ms healthy-lane p95
budget, exact maximum latency, queue stability, endpoint thread counts, file
descriptors/handles, child processes, RSS growth, completion timeouts, and
dropped diagnostic counts.

The accelerated harness is a CI smoke only; it is not substituted for the
required real-time soak.

The first real-time attempt exposed an overflow after roughly 1,024 consecutive
failures on the deliberately recurring-failure lane: the capped backoff formula
constructed an unbounded integer before applying its maximum. The run was
discarded. Command and status lanes now share a finite, capped logarithmic
backoff calculation that never constructs the huge intermediate value. A
1,100-cycle accelerated regression completed 8,800 commands with 1,210 expected
faults, no unexpected failures or timeouts, stable queues/resources, and healthy
latency p50/p95/max of 0.071/0.235/0.293 ms. The required real-time run restarted
from zero after the backoff, full-run sampling, lifecycle-fencing, and
bounded-shutdown corrections were frozen.

The final real-time run passed after 1,800.03 seconds. It completed 1,742 cycles
and 13,936/13,936 accepted commands across eight endpoints, including 1,917
expected injected failures, 175 disconnects, 175 reconnections, and 1,742 slow
operations. There were zero unexpected failures, completion timeouts, queued
state observations, or recorded errors. Healthy-lane latency p50/p95/max was
0.507/1.032/6.653 ms against the 250 ms p95 budget. Endpoint threads returned
from eight to zero; file descriptors remained four; child processes remained
zero; measured RSS growth was zero. The bounded JSON evidence is
`/tmp/mes5-soak-20260910.json`.

## Migration And Rollback Rehearsal

The supplied production snapshot at
`/Users/bill/RadioTools/FIO_DB_prod/freqinout.db` was copied to an isolated
temporary configuration. Startup-owned initialization added/verified all six
receiver columns on the clone. The clone was then restored from the untouched
source files. The source and restored clone both had SHA-256:

```text
a9b927910e231f4d677b18f652a773e2ed0d2f25b82be28987e0f4f142361345
```

The supplied snapshot contained zero device-profile rows, so populated-row
preservation continues to be covered by the dedicated MES-4 populated-clone
test. No source database, schedule, or radio profile was modified.

## Delegation And Review

- High-reasoning primary model: lifecycle/concurrency architecture,
  reconfiguration fencing, resume/clock handling, cache-only UI boundary,
  active-intent/readback comparison, daemon-isolated bounded shutdown,
  diagnostics/hang-dump integration, migration rehearsal, delegated-diff review,
  integrated qualification, documentation, and gate decision.
- `gpt-5.6-terra` high: read-only lifecycle/UI/shutdown gap audit.
- `gpt-5.6-terra` high: bounded eight-endpoint soak harness, resource
  instrumentation, injected-fault refinement, and focused tests.
- `gpt-5.6-luna` high: deterministic lifecycle, reconfiguration,
  clock-correction, unavailability, permanent-hang process exit, cache-only UI,
  shutdown, and repeated-start/stop tests.

Primary review removed an environment-wide macOS skip from the delegated
lifecycle module, strengthened 25-cycle coverage to create and close a real lane
on every cycle, added production-engine reconfiguration/clock/jitter/cache-only
tests, corrected Qt application isolation in the UI test, and strengthened the
soak's macOS resource and full-run latency measurements.

## Automated Acceptance Evidence

Focused MES-5 and cross-slice scheduler gate before final soak hardening:

```text
83 passed in 1.56s
```

Current integrated scheduler, runtime, multi-rig, SOP, station-safety, and
watchdog gate:

```text
264 passed, 5 skipped in 8.05s
```

After final lifecycle fencing and bounded executor hardening, the focused
scheduler/receiver/runtime/watchdog source gate passes:

```text
210 passed, 3 skipped in 7.33s
```

The final-source monolithic repository run reached 62 percent without an
assertion failure, then reproduced the known native Qt/offscreen segmentation
fault while constructing the unrelated compact log-viewer test. That exact test
passes alone. Fresh-process A–H, I–K, L, M, N–R, S, and T–Z partitions all exit
cleanly and total `2950 passed, 37 skipped`. The combined L–M process also
completed all 897 assertions before reproducing the cumulative Qt teardown
signal; separate L and M processes pass 86 and 811 respectively. Python
compilation and `git diff --check` pass.

## Remaining External Evidence

The following are release evidence, not safe assumptions:

- Linux startup/resume/reconfiguration/shutdown execution on the production
  Mint system;
- the physical three independently controlled transceivers plus two verified
  receive-only SDR adapters; and
- exact SDR application, hardware, version, and OS acceptance records.

Until those pass, FIO must not label automated multi-endpoint SDR scheduling as
production-verified. Manual receiver operation remains available and is stated
truthfully in the UI.

### Linux and physical-station qualification record

Use this bounded record on the Mint production station; do not replace exact
values with a general success statement.

| Evidence | Required record | Current state |
| --- | --- | --- |
| Host | Mint version, kernel, CPU, memory, Python/PySide versions | Pending |
| Endpoints | Three transceiver and two receive-only SDR application/adapter/hardware/version tuples | Pending |
| Startup | Time to usable shell, first endpoint dispatch spread, idle CPU/RSS after settling | Pending |
| Simultaneous transition | Coordinator p95 and each endpoint dispatch/apply/readback latency | Pending |
| Hung peer | Hung endpoint/deadline plus proof that four healthy peers remained on time and controllable | Pending |
| Recovery | Per-endpoint disconnect, reconnect, Retry now, and unchanged-peer evidence | Pending |
| Lifecycle | Suspend/resume and forward/back clock correction with only current intent applied | Pending |
| Reconfiguration | Edit, disable, and remove while work is pending; no late stale state appears | Pending |
| Shutdown | UI close latency, outstanding-lane diagnostic, zero Qt timer/thread-destruction warnings | Pending |
| Resources | Endpoint threads, descriptors/sockets, child processes, RSS, queue/event bounds before/after | Pending |

Run the same focused and integrated commands recorded above on Linux, then run:

```text
.venv/bin/python tools/scheduler_multi_endpoint_soak.py \
  --json-out /tmp/fio-mes5-linux-soak.json
```

Retain the JSON report, the corresponding bounded `freqinout.log` interval, any
watchdog dump, and screenshots of target-scoped Station status. Do not include
credentials or Bluetooth pairing data.
