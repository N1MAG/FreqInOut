# MIP-5 Startup, Telemetry, And Qualification Evidence

Date: 2026-09-10

Status: implementation exit gate passed; Linux production confirmation remains
an external release-qualification observation

## Delivered

- Background ingest and native projection catch-up begin only after the first
  usable application shell has been shown and measured.
- One application-owned message maintenance lane coalesces source notifications
  and jittered 30–60 second reconciliation. Messages no longer owns a second
  native projection coordinator or writer.
- Catch-up and rebuild work remains capped at 100 complete bundles per writer
  transaction and yields between cycles. Actual transaction duration is exposed
  as `messages.write_batch` telemetry.
- Message Index rebuild is an explicit Message Maintenance action with an
  asynchronous read-only preview, confirmation, bounded progress, cancellation,
  durable restart recovery, and no native message or received-file mutation.
- Ordinary startup never inspects or resumes explicit rebuild state. Rebuild
  estimates, derived-state reset, maintenance-table reads, and explicit resume
  work run on the maintenance executor rather than the Qt thread.
- Performance metrics use a bounded non-blocking queue, batched writes, a
  long-lived file handle, 5 MiB rotation, and five backups. FIO explicitly
  flushes the sink before the optional Linux hard-exit path.
- UI watchdog diagnostics consume a bounded, credential-redacted snapshot of
  already-published scheduler and projection state. Hang capture performs no
  database, endpoint, filesystem-discovery, or service/provider callback work,
  including before the first snapshot is published.
- Shutdown cooperatively cancels message maintenance and retains durable dirty
  work/checkpoints for restart instead of draining history during UI teardown.
  Both catch-up and auxiliary maintenance futures remain lifecycle-tracked.

## Ownership

- High-reasoning primary model: startup ownership, concurrency and lifecycle
  integration, explicit rebuild safety/UI workflow, delegated-diff review,
  macOS shell verification, documentation, and final gate.
- `gpt-5.6-terra` high: bounded post-shell catch-up, durable rebuild state,
  source-state helpers, cancellation/resume behavior, and focused tests.
- `gpt-5.6-luna` high: buffered/rotated telemetry, cache-only watchdog
  diagnostics, credential redaction, and focused telemetry tests.
- `gpt-5.6-luna` focused test package: scheduler/Expect isolation, burst,
  restart, idle-zero-write qualification, and the reusable soak harness.

## Acceptance Evidence

- Core message projection, source, file, Ops, traffic, dynamic FLAMP Expect,
  startup-planner, maintenance, telemetry, and qualification partition:
  **225 passed in 22.11 seconds**.
- Clean Qt Messages, Station Control Bar, Settings/SOP, responsive-layout, and
  shell partition: **371 passed in 2.86 seconds**.
- A current-code real 12,000-row catch-up completed in **6.194 seconds**, produced exactly
  12,000 unique projection rows, drained queue depth to zero, completed all four
  concurrent synthetic scheduler commands, left no endpoint-lane threads, and
  recorded a **10.434 ms p95** preparation batch, **0.390 ms p95** unchanged
  source reconciliation, and **43.694 ms** maximum projection transaction.
- Disposable fresh-profile macOS shell check: database initialization
  **22.167 ms**, MainWindow construction **842.653 ms**, first usable shell
  **921.061 ms**, background ingest absent before the post-shell boundary and
  running afterward, and cooperative shutdown **15.293 ms**.
- The 30-minute integrated scheduler/message soak completed in **1,801.235
  seconds**. All **1,787/1,787** concurrent synthetic scheduler commands
  completed with zero failures; 1,787 idle checks discovered zero phantom work;
  the final projection contained exactly 500 unique burst rows; queue depth was
  zero after shutdown; maximum projection transaction time was **12.845 ms**;
  RSS growth was zero; file descriptors decreased from 6 to 5; and no endpoint
  worker thread remained after shutdown.

## External Qualification Boundary

The implementation gate is hardware-free and passed on macOS with temporary
profiles/databases only. Linux production confirmation remains an explicit
release-qualification observation because this development host cannot truthfully
measure the user's Linux window manager, filesystem, Bluetooth stack, radio/SDR
endpoints, or production workload. That observation does not weaken or replace
the separate physical multi-endpoint scheduler hardware gate.

No production database, native message source, received file, operator state,
BBS mapping, Spotter rule, or FLAMP Q evidence was modified.
