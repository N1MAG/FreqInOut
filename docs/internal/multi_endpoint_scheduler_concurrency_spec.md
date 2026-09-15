# Multi-Endpoint Scheduler Concurrency Specification

Status: active implementation authority; MES-0 through MES-5 implementation and
automated macOS qualification complete; the September 10 Qt-thread remediation
implementation gate passed; Linux lifecycle and the physical three-transceiver /
two-SDR release gates remain pending

Date: 2026-09-10

Implementation evidence:
`multi_endpoint_scheduler_mes0_baseline_2026-09-10.md`,
`multi_endpoint_scheduler_mes1_evidence_2026-09-10.md`,
`multi_endpoint_scheduler_mes2_evidence_2026-09-10.md`,
`multi_endpoint_scheduler_mes3_evidence_2026-09-10.md`,
`multi_endpoint_scheduler_mes4_evidence_2026-09-10.md`, and
`multi_endpoint_scheduler_mes5_evidence_2026-09-10.md`

## Purpose And Product Priority

Automated scheduling is a critical, core FIO feature. It must remain dependable
when a station grows from one transceiver to multiple independently controlled
transceivers and receive-only SDRs. A single slow, disconnected, or hung endpoint
must never make another endpoint late, unresponsive, incorrectly marked healthy,
or unsafe.

This document is the authoritative concurrency and lifecycle contract for all
schedule-driven and operator-requested radio/receiver control. It extracts and
expands requirements previously embedded in
`multirig_product_ui_contract.md` and `sdr_receiver_control_spec.md`.

The required production acceptance station is:

- three independently controlled transceiver endpoints; and
- two independently controlled receive-only SDR endpoints.

An eight-active-endpoint synthetic test provides engineering headroom. It is not
a promise that every eight-device hardware combination is supported.

## Governing Principles

The design is governed in this order:

1. RF and transmit safety;
2. correct schedule, SOP, and manual-control authority;
3. endpoint failure isolation;
4. deterministic and timely action;
5. clean startup, reconfiguration, and shutdown;
6. bounded CPU, memory, threads, queues, sockets, and database work; and
7. clear operator feedback and diagnostic evidence.

Throughput is not the primary goal. FIO issues relatively few control commands.
Predictable behavior under failure is more important than maximizing parallelism.

## Scope

This specification covers:

- HF and net schedule transitions;
- SOP-driven schedule/control intent;
- manual QSY, Tune, Hold, Resume, Suspend, and Park actions that share scheduler
  authority;
- transceiver control through FLRig, RigCtlD/Hamlib, JS8Call, or later adapters;
- receive-only SDR control through the adapters defined by
  `sdr_receiver_control_spec.md`;
- endpoint status/readback required to make or verify a scheduling decision;
- shared RF-resource arbitration;
- reconnect, retry, timeout, reconfiguration, and shutdown behavior; and
- health, event, and performance evidence for the above.

It does not change the product precedence between manual control, SOPs, nets, and
regular schedules. That precedence remains owned by the existing scheduler/SOP
specifications. This design changes how an already-authorized desired state is
dispatched and observed.

It does not authorize transmit control for an Observer / SDR profile. A
receive-only adapter has no PTT operation and cannot be promoted into a
transceiver lane by configuration inference.

## Terms

### Station coordinator

The single logical authority that evaluates time, schedules, manual overrides,
SOP precedence, RF Guard, and shared-resource conflicts. It computes desired
state and grants dispatch permits. It performs no endpoint network, serial,
USB, driver, process, or database I/O on its evaluation path.

### Physical endpoint

The actual controllable radio, receiver, or application-owned receiver target.
Configuration rows and aliases are not necessarily distinct endpoints.

### Endpoint key

A stable normalized identity for a physical endpoint. It includes the adapter
family and the minimum connection/target identity needed to prevent two FIO
profiles from issuing competing commands to the same device. Secrets are never
part of logs or user-visible keys.

Examples include a normalized FLRig/RigCtlD host and port plus radio identity, a
JS8Call instance endpoint, or an SDR application endpoint plus device-set/VFO.
The exact key grammar is adapter-specific and versioned.

A transceiver and the companion applications that can command that same physical
radio form one lane, even when FIO can reach them through more than one protocol.
The configured control backend selects the command route; it does not manufacture
another physical endpoint. Conversely, one SDR application controlling multiple
independent device sets must include the selected device set/VFO in identity so
unrelated receivers are not falsely serialized. If FIO cannot prove whether two
routes control the same hardware, automatic scheduling is blocked until the
operator resolves the identity instead of risking competing writers.

### Endpoint lane

The isolated serialized command actor for one endpoint key. A lane owns its
connection lifecycle, command state, latest desired intent, retry/backoff,
circuit breaker, readback cache, health, and shutdown state.

### Intent

An immutable desired-state request containing endpoint key, radio/profile ID,
source authority, target frequency/mode/VFO/offset or receive-only equivalent,
schedule occurrence identity, generation number, creation/deadline time, and any
coordinator-issued RF-resource permit.

### Command transaction

One lane-local apply plus verification attempt for an intent. It has one terminal
result: applied and verified, applied but unverified, rejected, superseded,
timed out, failed, or cancelled by shutdown/reconfiguration.

## Current-State Gap

The current `SchedulerEngine` correctly projects active schedule rows and retains
some pending intent by radio. It does not yet provide endpoint-level failure
isolation:

- all control commands share one `ThreadPoolExecutor(max_workers=1)`;
- one `_control_future`, `_pending_entry_key`, timeout, failure count, and
  backoff govern all radios;
- the next retained radio intent is selected through one station-global drain;
- post-apply verification shares a station-wide status executor;
- several actual/busy/readback caches still describe the primary or station-wide
  path rather than a specific endpoint; and
- an uninterruptible endpoint call cannot be safely abandoned by creating more
  replacement threads, so the existing worker is intentionally left stuck.

Consequently, a hung radio can delay commands and verification for healthy
radios. This is safe for serialization but insufficient for production multi-rig
scheduling.

## Non-Negotiable Reliability Invariants

1. **Endpoint isolation.** Failure, timeout, reconnect, backoff, or a hung call in
   one lane cannot delay dispatch, status, readback, manual control, or retry for
   another lane.
2. **One writer per endpoint.** Exactly one lane owns command writes for an
   endpoint key. Aliased profiles cannot create competing writers.
3. **One in-flight transaction per lane.** Commands for the same physical
   endpoint remain serialized even while different endpoints proceed in parallel.
4. **Latest relevant intent wins.** A newer desired state supersedes queued older
   work for the same lane. FIO does not replay every missed intermediate tune.
   Safety actions and existing manual/SOP precedence cannot be coalesced away.
5. **No cross-endpoint state.** Pending keys, last-applied state, actual state,
   failures, backoff, circuit state, deadlines, verification, and health are keyed
   by endpoint. A result from one lane cannot satisfy or overwrite another lane.
6. **Generation safety.** Completion from an old generation, replaced endpoint,
   previous connection, or shutting-down lane is ignored except for bounded audit
   logging.
7. **Bounded work.** Every queue, retry series, poll, command, readback, and
   shutdown wait is bounded. No failure path creates unbounded threads, timers,
   futures, sockets, logs, or database rows.
8. **No UI-thread I/O.** The Qt thread never opens or probes an endpoint, waits on
   a future, sleeps for retry, performs control/readback, or joins an unbounded
   worker.
9. **Fail safe.** Missing, stale, ambiguous, or conflicting safety evidence never
   authorizes a transmit-affecting command. Failure isolation cannot bypass
   shared PTT, RF Guard, antenna, frontend, or amplifier constraints.
10. **Clean lifecycle.** FIO can repeatedly start, stop, disconnect, reconnect,
    edit, disable, and remove endpoints without orphan workers or Qt callbacks
    after object destruction.
11. **Durable intent, not fabricated success.** If an endpoint is unavailable,
    the current relevant desired state remains known, but the UI and logs state
    that it was not applied. Reconnect applies only the still-current intent after
    authority and safety are revalidated.
12. **Manual receivers remain usable.** An SDR without verified API control stays
    in the schedule/reminder model as a manual endpoint and never blocks automated
    lanes.

## Required Architecture

```text
Schedules / SOPs / operator actions
                 |
                 v
       Station coordinator (no endpoint I/O)
       - desired state per endpoint
       - precedence and occurrence identity
       - shared RF-resource arbitration
       - immutable dispatch permits
                 |
       +---------+----------+----------+----------+
       v         v          v          v          v
    Radio A   Radio B    Radio C     SDR A      SDR B
      lane      lane       lane        lane       lane
       |         |          |           |          |
    adapter   adapter    adapter     adapter    manual/API
```

### Station coordinator responsibilities

The coordinator:

- uses one monotonic scheduling clock plus UTC wall time for presentation and
  persisted occurrence identity;
- evaluates all active endpoint assignments in one bounded pass;
- preserves existing schedule/SOP/manual precedence independently per target;
- resolves station-shared resource conflicts before dispatch;
- produces immutable intents with monotonically increasing lane generations;
- dispatches without waiting for endpoint completion;
- records why an intent was issued, held, rejected, or superseded; and
- consumes lane results through a thread-safe event boundary on its owning thread.

The coordinator must not call an adapter, probe software, read a log file, launch
a process, or perform an uncached database scan. Required configuration and
schedule projections are refreshed outside the timing-critical evaluation path
and swapped in as immutable snapshots.

### Endpoint lane responsibilities

Each automated endpoint lane owns:

- its endpoint key, adapter, connection, and lifecycle generation;
- at most one running command transaction;
- one bounded pending slot for the latest coalescible intent;
- a separate non-coalescible safety/control slot only where explicitly required;
- last desired, attempted, applied, and verified state;
- actual-state cache with value, source, timestamp, and staleness;
- lane-local deadline, failure count, retry/backoff, and circuit state;
- command/result counters and latency metrics; and
- orderly cancellation and close.

For ordinary schedule transitions, the pending structure is not an unbounded
FIFO. Replacing the pending intent records one `superseded` event and retains the
newest state. A manual hold/suspend or safety stop is handled before subsequent
tune work and cannot be discarded as a routine superseded frequency.

Lane state is explicit and inspectable:

| State | Meaning |
| --- | --- |
| `starting` | Worker/adapter ownership is being established; no success is implied. |
| `idle` | Lane can accept work; actual state may still be stale or unknown. |
| `applying` | One command transaction is in flight. |
| `backoff` | A bounded retry delay is active for this lane only. |
| `circuit_open` | Repeated failure suppressed automatic churn; one bounded recovery probe may later enter half-open state. |
| `stopping` | New work is rejected and owned resources are closing. |
| `stopped` | No worker, callback, socket, timer, or helper remains owned by the lane. |

Connection state, lane state, and schedule conformance are separate facts. For
example, a connected/idle lane may still be off schedule, and a disconnected lane
must never be displayed as on schedule because of its last known readback.

### Worker model

- Each active automated network/API endpoint has a dedicated long-lived,
  single-consumer worker or actor. This is logical ownership, not permission to
  start a new worker for each command.
- A manually controlled endpoint has no control worker. It receives reminder and
  acknowledgement state only.
- An endpoint lane may multiplex adapter push events and command work only when
  the adapter contract proves this cannot deadlock. Otherwise its bounded status
  reader remains endpoint-scoped and independent of other lanes.
- Multiple logical profiles that normalize to one endpoint key share one lane and
  one connection. Configuration conflicts are surfaced before scheduling.
- Native libraries or calls that cannot honor a deadline must run in a supervised
  helper process. The process may be terminated and recreated after bounded
  cleanup; FIO must not leak or endlessly replace stuck Python threads.
- Worker callbacks never mutate Qt objects or scheduler state directly. Results
  cross a queued signal/event boundary and carry endpoint key plus generation.

## Endpoint Identity And Ownership

Before activating a lane, FIO normalizes the endpoint key and validates that:

- no other lane owns the same key;
- the adapter type agrees with the profile's device class and capabilities;
- a receive-only profile exposes no transmit operation;
- connection parameters and target IDs are complete enough to avoid guessing;
- shared application endpoints identify the selected physical target/VFO; and
- credentials remain in the approved configuration store and are redacted from
  diagnostics.

If two profiles resolve to one endpoint:

- compatible aliases bind to one lane and share actual health/state;
- conflicting schedule assignments, capabilities, or control modes block
  automatic activation and provide a corrective action; and
- FIO never chooses one silently based on row order.

Changing an endpoint's connection or physical target creates a new generation.
The old lane stops accepting work, cancels what can be cancelled, closes within
its adapter contract, and cannot publish a result into the replacement lane.

## Desired State, Arbitration, And Dispatch

Every coordinator evaluation builds a desired-state map keyed by endpoint. It
compares the map to the last dispatched generation and queues only meaningful
changes or explicit forced reapplications.

Schedule timing must tolerate wall-clock correction, daylight-saving display
changes, system sleep/wake, and delayed event-loop delivery:

- UTC schedule selection is recomputed from the current authoritative time after
  wake or a detected clock discontinuity;
- monotonic time governs elapsed deadlines, retry delays, and latency metrics;
- FIO applies the currently valid desired state rather than replaying expired
  intermediate transitions; and
- a missed or late transition is recorded with expected/actual dispatch time and
  endpoint identity, without creating a catch-up command storm.

Before a transmit-capable intent is dispatched, the coordinator revalidates:

- target radio and assigned plan compatibility;
- scheduler enabled/hold/suspend/manual state;
- current SOP/net/schedule authority;
- endpoint-specific busy evidence and freshness;
- shared PTT ownership and RF Guard;
- shared antenna, frontend, and amplifier constraints; and
- the validity window of the dispatch permit.

A shared-resource decision is short, in-memory, and deterministic. The
coordinator does not hold a global lock while an endpoint performs I/O. If the
permit expires before the lane begins the command, the lane rejects it and asks
for reevaluation rather than using stale authorization.

Receive-only SDR tunes participate in shared frontend/antenna arbitration only
when their configuration declares such a physical dependency. They never acquire
PTT authority.

## Command Application And Verification

A lane processes an accepted intent as one transaction:

1. confirm lane generation and intent currency;
2. confirm that the coordinator permit is present and fresh when required;
3. connect or use the existing healthy connection within a deadline;
4. apply only the adapter capabilities explicitly required by the intent;
5. read back authoritative state within the adapter deadline;
6. classify the result without guessing; and
7. publish the result and fresh actual-state snapshot.

An adapter write returning success is not sufficient verification. If frequency
readback is supported, only matching readback within the documented tolerance is
`applied and verified`. A successful write without available readback is
`applied but unverified`, never falsely shown as on schedule.

Command operations should be idempotent. Reapplying the same current state may be
skipped only when the lane has fresh verified state. Stale cached success cannot
suppress a necessary transition or recovery attempt.

## Status And Polling Isolation

Status collection follows the same endpoint boundary as command execution.

- Actual frequency, mode, VFO, offset, PTT/busy, reachability, and readback age
  are endpoint-scoped.
- Push events are preferred when an adapter provides stable ordered state.
- Required background polling is centrally paced but dispatched independently;
  one endpoint poll never occupies the worker used by another endpoint.
- Status requests use single-flight coalescing per endpoint. Repeated UI reads
  consume cached snapshots and do not trigger new work.
- Hidden tabs do not create additional poll owners or increase cadence.
- UI consumers subscribe to immutable snapshots rather than interrogating
  adapters.
- Staleness is explicit. Unknown or stale is not converted to idle, ready, or on
  schedule.

Routine process inventory and other station-wide discovery remain shared cached
services. They must not be repeated once per endpoint.

## Timeouts, Retry, And Circuit Breakers

Every adapter operation declares a bounded timeout. The initial local network/API
target is 2 seconds per connect/apply/readback step; an adapter may justify a
different bound, but no in-process operation may be unbounded and the existing
8-second scheduler timeout is the absolute compatibility ceiling.

Failure handling is lane-local:

- transient failures use exponential backoff with jitter and a documented cap;
- a newer intent replaces the pending retry target;
- manual `Retry now` resets only the selected lane's backoff;
- successful verified communication resets only that lane's failure series;
- repeated timeout/open failures trip that lane's circuit breaker;
- an open circuit suppresses automatic connection churn but retains current
  desired state and low-rate recovery eligibility; and
- half-open recovery permits one probe, never a burst.

Retries revalidate schedule authority and shared-resource safety. FIO does not
transmit or tune to an obsolete occurrence merely because it was queued before
an outage.

Rate-limited health and audit events must distinguish `unreachable`, `timeout`,
`command rejected`, `applied unverified`, `readback mismatch`, `backoff`,
`circuit open`, and `superseded`.

## Fairness And Priority

Independent healthy lanes may run concurrently. Coordinator dispatch must not
iterate in a way that permanently favors the lowest radio ID.

- All intents produced by one evaluation are offered to lanes in that evaluation
  cycle without waiting for earlier lane completion.
- Lane-local coalescing prevents a noisy endpoint from building a queue.
- Safety actions and operator commands retain the priorities already defined by
  their owning specifications.
- One endpoint's retry storm cannot consume another endpoint's worker, connection,
  retry budget, or UI update capacity.
- Station-wide event emission and database persistence are batched or rate-limited
  so many endpoints cannot flood the UI thread.

## Startup, Reconfiguration, And Shutdown

### Startup

- Construct the coordinator and immutable configuration snapshot first.
- Paint the shell without waiting for endpoint probes.
- Start lanes only for active automated endpoints.
- Apply startup jitter to non-urgent probes so five endpoints do not create a
  connection storm.
- Evaluate the current schedule once, then dispatch current intents independently.
- Never infer that an endpoint is on schedule from saved last-known state.

### Reconfiguration

- Adding or editing an inactive endpoint does not disturb active lanes.
- Enabling a new endpoint creates only its lane.
- Disabling/removing an endpoint retires only its lane after invalidating its
  generation.
- Changing shared RF resources triggers a fresh central arbitration snapshot; it
  does not mutate workers concurrently.
- Configuration persistence is transactional and separate from runtime activation.

### Shutdown

Shutdown order is explicit:

1. stop schedule evaluation and reject new intents;
2. invalidate coordinator and lane generations;
3. cancel queued work and request cooperative cancellation of in-flight work;
4. close adapter sockets/subscriptions and terminate supervised helpers;
5. join each lane within a bounded aggregate shutdown budget;
6. record but do not block indefinitely on an adapter that violated its contract;
7. disconnect Qt signals/timers; and
8. destroy scheduler-owned QObjects only after callbacks can no longer arrive.

No lane creates a Qt timer from its worker. No callback uses a receiverless
cross-thread `QTimer.singleShot`. Repeated start/stop cycles must not increase live
thread, timer, file-descriptor, socket, or child-process counts.

## Operator Experience And Health

The Station Control Bar and Station Health consume lane snapshots. They must show
endpoint identity with state such as:

- `On schedule · verified`;
- `Applying schedule`;
- `Manual tuning`;
- `Waiting for shared RF resource`;
- `Receiver unavailable · retry in …`;
- `Applied · verification unavailable`;
- `Off schedule · readback mismatch`; or
- `Control stalled · other radios unaffected`.

An endpoint warning must route to the affected radio/SDR and explain the next
operator action. It must not make the whole station appear failed unless a shared
resource or coordinator failure truly affects the station.

Manual Tune/QSY receives immediate acknowledgement in the UI, normally within
150 ms, while endpoint completion remains asynchronous. Selecting, expanding, or
refreshing a radio card never performs live endpoint I/O.

## Persistence And Migration

Runtime lane state is ephemeral and must not be used as a second scheduler source
of truth. Persist only information needed across restarts, such as:

- normalized adapter configuration and target identity;
- operator-controlled enablement/assignment;
- last verification evidence for truthful capability presentation; and
- bounded scheduler event/health history under existing retention rules.

Do not persist futures, queue contents, circuit-open monotonic timestamps, or
assumed actual radio state.

Any schema change must be additive, startup-owned, idempotent, and rehearsed on a
production clone. Existing single-radio profiles must normalize to exactly one
lane without requiring operator reconfiguration. Rollback must leave prior
configuration readable and must not delete schedules or radio profiles.

## Performance Budgets

Budgets are measured on the documented Linux production baseline and reported as
p50, p95, and maximum where applicable.

### Required five-endpoint station

- Coordinator evaluation performs no endpoint I/O and completes within 50 ms p95.
- All healthy lanes receive dispatch acknowledgement within 250 ms of one
  coordinator decision, including when one peer is deliberately hung.
- No healthy endpoint waits for another endpoint's timeout or backoff.
- UI acknowledgement of a manual action is within 150 ms.
- Each endpoint command completes or reaches a truthful bounded terminal/degraded
  state within its adapter deadline.
- Queue depth is bounded by design and returns to zero/current-intent-only after a
  transition burst.
- Idle CPU remains stable after connections settle; adding inactive profiles or
  visiting a tab does not add polling work.

### Eight-endpoint synthetic stress gate

- Run at least 30 minutes with periodic simultaneous transitions, readbacks,
  disconnect/reconnect, one slow endpoint, and one repeatedly failing endpoint.
- Thread, timer, socket, file-descriptor, memory, event-row, and queue counts remain
  bounded and stable after warm-up.
- Healthy transition latency does not scale with the sum of peer timeouts.
- No cross-endpoint state, starvation, duplicate command storm, or stale callback
  is observed.
- UI event-loop responsiveness and idle/interaction CPU remain within the current
  Slice 0 production budgets; any intentional new allowance requires measured
  evidence and explicit approval.

## Observability Contract

Every coordinator decision and lane transaction uses a correlation envelope:

- endpoint key hash or safe stable label;
- device/radio profile ID;
- lane and configuration generation;
- schedule occurrence and intent ID;
- source authority (`manual`, `SOP`, `net`, `HF`, or other canonical value);
- queue/dispatch/start/finish timestamps and latency;
- result/reason code;
- retry count, backoff/circuit state; and
- verification value, source, age, and mismatch when relevant.

Logs must make the statement “Radio A timed out; Radios B, C, SDR A, and SDR B
continued” provable without enabling verbose debug output. Routine success logs
are sampled or aggregated to avoid load. Performance counters are exposed in the
existing diagnostic snapshot/hang-dump path.

Event persistence is asynchronous to coordinator evaluation and bounded. A slow
database writer may reduce or aggregate routine diagnostic detail, but it cannot
delay endpoint dispatch or discard an unsaved safety/terminal event silently.
Backpressure and dropped/aggregated diagnostic counts are themselves visible in
Station Health.

No log includes passwords, tokens, Bluetooth credentials, or unredacted secrets.

## Implementation Boundaries

The implementation should introduce focused Qt-free components rather than
copying the existing `SchedulerEngine` into five threads:

- `StationScheduleCoordinator` or an equivalently named desired-state/arbitration
  core;
- `EndpointKey` normalization and alias validation;
- `EndpointIntent` and `EndpointResult` immutable value objects;
- `EndpointLane` lifecycle/state machine;
- adapter contracts for transceiver and receive-only receiver control;
- an endpoint-scoped snapshot registry; and
- a thin `SchedulerEngine` Qt integration layer during migration.

Endpoint adapters do not query schedules or update UI. The coordinator does not
know protocol command syntax. UI presenters do not own polling. Database stores do
not invoke endpoint control.

## Delivery Packages And Exit Gates

Implementation is sequential. Do not begin the next package until the current
package passes its gate.

### MES-0 — Characterization And Fault Harness

- Freeze existing single-radio schedule, manual control, SOP/net precedence,
  shared PTT/RF Guard, retry, status, and shutdown behavior in tests.
- Add deterministic fake clock, fake adapters, controllable slow/hung calls,
  correlation capture, and thread/resource counters.
- Record the current one-radio and three-radio behavior/performance baseline.

Exit: no production behavior change; characterization tests reproduce the
station-global blocking defect and all existing scheduler tests pass.

### MES-1 — Endpoint Identity And Pure Coordinator

- Add endpoint-key normalization, alias/conflict validation, immutable intent and
  result types, and a Qt-free desired-state coordinator.
- Move schedule evaluation/arbitration inputs behind immutable snapshots.
- Preserve existing precedence and single-radio output exactly.

Exit: deterministic clock/precedence/property tests pass; duplicate endpoint
writers are impossible; coordinator performs no I/O.

Implementation evidence (2026-09-10): `scheduler_coordination.py` now provides
Qt-free normalized route identity, resolved-profile projection, deeply immutable
schedule snapshots, immutable generation-tagged intents/results, deterministic
alias/conflict validation, and pure desired-state coordination. It consumes the
existing scheduler's already-resolved `current_source` and `current_entry`; it
does not reimplement NET/SOP/HF precedence, query configuration, construct an
adapter, or perform I/O. Exact same-family routes can form one compatible alias
lane. Competing desired state, incompatible capabilities, or one profile mapped
to multiple route keys blocks every affected writer. Cross-protocol physical
ownership is not inferred from host/port and remains an explicit future
configuration concern. The MES-1 evidence record contains the commands and
results. MES-2 command-lane work did not begin until this gate passed.

### MES-2 — Isolated Lanes Behind A Compatibility Boundary

- Add one serialized lane per active automated endpoint.
- Move pending intent, generation, command future, failure/backoff, circuit state,
  actual/readback cache, and verification into the lane.
- Keep the current external scheduler/UI API through a compatibility facade.
- Route existing transceiver control adapters first.

Exit: three fake radios transition concurrently while one is hung; single-radio
regression, timeout, reconnect, stale callback, and no-thread-growth gates pass.

Implementation evidence (2026-09-10): `scheduler_endpoint_lane.py` now owns one
long-lived, single-worker lane per normalized endpoint key. Pending intent,
generation fencing, timeout reporting, exponential backoff, circuit/half-open
state, command result, and shutdown suppression are lane-local. The existing
`SchedulerEngine._queue_control_action()` remains the compatibility facade, but
dispatches the command and its immediate verification readback through the
target endpoint lane. Compatible aliases receive one deterministic writer;
competing schedules on the same route fail closed. No schema or configuration
migration was introduced. The MES-2 evidence record contains the complete gate.

### MES-3 — Endpoint-Scoped Status And Shared Safety

- Replace primary/station-global busy and readback fields with endpoint snapshots.
- Retain shared PTT/RF Guard/antenna/frontend/amplifier arbitration centrally.
- Make health/event presentation endpoint-specific.

Exit: stale or failed evidence cannot authorize unsafe control; one failed status
source does not delay another; shared resource conflicts remain serialized and
auditable.

Implementation evidence (2026-09-10): `scheduler_endpoint_status.py` now owns
immutable, endpoint-keyed status snapshots and one serialized status worker per
route. Cached status reads are nonblocking; refreshes are single-flight and
bounded by endpoint-local timeout/backoff state; inline command verification is
generation-fenced against late polling results. Targeted scheduling no longer
uses primary-radio PTT state or synchronous endpoint reads. Local/shared PTT,
RF-guard, health, and event evidence are qualified by the resolved target radio,
and missing, stale, or failed shared PTT evidence fails closed. Central shared
resource arbitration remains in `StationRuntimeManager`; no schema or
configuration migration was introduced. The MES-3 evidence record contains the
complete gate.

### MES-4 — Receive-Only SDR Lanes

- Integrate the receiver-control contract with no transmit surface.
- Support automated and manual receiver lanes in the same desired-state map.
- Ensure application/device aliases cannot create competing control owners.

Exit: three transceivers plus two SDRs pass simultaneous transition, manual
fallback, one-hung-endpoint, reconnect, and shutdown tests. Exact hardware gates
remain governed by each SDR adapter package.

Implementation evidence (2026-09-10): the Qt-free `receiver_control.py`
contract contains no PTT/transmit surface, and verified automated observer
profiles now resolve to target-qualified receiver endpoint keys. Manual,
disabled, incomplete, or unverified receivers remain zero-I/O manual endpoints.
The scheduler branches receive-only rows before the transceiver/QSY path and
uses the same endpoint-lane registry for tune plus readback. Receiver failures,
timeouts, reconnects, and shutdown are endpoint-local; compatible aliases have
one writer and competing intents fail closed. Six additive receiver fields are
startup-owned and preserve existing observer profiles with Manual defaults. No
real SDR application/hardware combination is claimed as verified by MES-4;
adapter-specific hardware gates remain governed by the SDR plan. The MES-4
evidence record contains the complete automated gate.

### MES-5 — Lifecycle, Soak, And Production Qualification

- Complete startup jitter, dynamic reconfiguration, bounded shutdown, diagnostics,
  and operational UI wording.
- Run the eight-endpoint synthetic soak and the required physical five-endpoint
  station matrix.
- Rehearse additive migrations and rollback on a production clone.

Exit: all performance budgets and automated suites pass; macOS and Linux lifecycle
tests pass; the physical hardware matrix is documented; no unresolved P0/P1
scheduler, concurrency, safety, or shutdown defect remains.

Implementation evidence (2026-09-10): active endpoint configuration now has a
separate epoch and targeted reconciliation path. Edit/disable/remove retires
only affected command/status lanes and fences late callbacks; unchanged peers
continue. Resume and clock-discontinuity handling retires and generation-fences
pre-discontinuity command/status lanes, invalidates assumptions, and offers only
current authority. Non-urgent first probes are staggered without
sleeping, and per-endpoint retry does not reset peers. Station cards use
cache-only runtime snapshots. Bounded scheduler diagnostics are included in UI
hang dumps. Scheduler-owned serial workers are daemon-isolated and production
shutdown uses bounded joins with survivor diagnostics, so one permanently hung
adapter cannot hold process exit. The repeatable eight-endpoint soak harness measures healthy latency,
queue stability, threads, file descriptors/handles, child processes, RSS,
disconnect/reconnect, one slow lane, and one recurring-failure lane. Automated
tests and the production-clone rollback rehearsal have passed. The real-time
30-minute eight-endpoint soak passed all latency/resource gates on the macOS
development host. Linux and physical five-endpoint results remain explicit
external gates, so the overall production release gate is not yet claimed.

Recommended focused test modules are
`tests/test_scheduler_endpoint_identity.py`,
`tests/test_scheduler_endpoint_lanes.py`,
`tests/test_scheduler_endpoint_status_isolation.py`,
`tests/test_scheduler_multi_endpoint_faults.py`, and
`tests/test_scheduler_multi_endpoint_lifecycle.py`, with the long soak exposed as
a bounded tool under `tools/` so production-like profiles can run it without
turning the normal unit suite into a hardware-dependent test.

## Required Automated Acceptance Matrix

At minimum, tests cover:

- one-radio backward compatibility;
- three simultaneous healthy transceiver transitions;
- three transceivers plus two SDRs;
- eight active fake endpoints for 30 minutes;
- one slow endpoint and one permanently hung endpoint;
- endpoint failure during connect, apply, and readback;
- repeated disconnect/reconnect and application restart;
- rapid schedule changes coalescing to the newest relevant state;
- system sleep/wake, forward/backward clock correction, and missed transitions;
- manual QSY followed by Hold/Resume during an in-flight transition;
- SOP/net/schedule conflict using existing precedence;
- shared PTT and shared antenna/frontend conflicts;
- two profiles resolving to the same endpoint;
- endpoint edit/disable/remove while work is pending;
- stale completion from an old generation;
- status staleness and readback mismatch;
- circuit open, half-open recovery, and manual retry;
- no API/manual SDR fallback;
- startup with unavailable endpoints;
- shutdown during connect, apply, readback, and backoff;
- 25 repeated start/stop cycles with stable resource counts; and
- Qt warnings, callbacks after destruction, leaked processes/threads, and
  unbounded log/event growth.

## September 10 Qt-Thread Requalification

Production hang dumps proved that the compatibility scheduler still allowed
three expensive operations to enter the Qt timer/render path despite endpoint
lane isolation: schedule/assignment SQLite reads (including schema assurance),
manual-control state reads, and process inventory during FLDigi availability
checks. The Station Control Bar independently reopened plan tables every 15
seconds and on forced refresh. Under message-writer contention these cache misses
became multi-second application freezes.

The following rules are therefore binding additions to MES-5:

1. The scheduler Qt timer polls completion registries, requests work, and applies
   immutable cached projections only. It cannot directly evaluate a DB-backed
   schedule or invoke endpoint application.
2. One dedicated serialized schedule-projection worker owns active profile,
   assignment, plan, SOP/policy, and manual-control reads. Results are generation
   fenced before publication on the scheduler thread.
3. Latency-sensitive schedule getters use read-only connections and never run
   schema assurance. Startup remains the sole migration/schema owner.
4. FLDigi/process availability and apply operations run through a worker. A
   cache miss in a render/evaluation path means unknown/unavailable; it never
   triggers a live process walk or socket call.
5. Scheduler construction performs no endpoint availability probe.
6. The Station Control Bar derives plan, frequency-reference, lane, and endpoint
   presentation from scheduler/runtime snapshots only. It performs no SQLite
   fallback when a snapshot is absent or stale.
7. A forced refresh invalidates/request-replaces worker state but continues to
   render the last immutable snapshot until its generation-valid successor is
   published.

Automated requalification passes 201 scheduler tests with one intentional skip.
Architecture guards inspect the Qt timer, schedule projection, constructor,
FLDigi cache boundary, and Station Control Bar plan cache so synchronous I/O
cannot silently return. Live Linux timer cadence and the physical endpoint gates
remain external.

## September 11 Projection Feedback-Loop Remediation

Local production evidence exposed a forced-refresh feedback loop that violated
the requalification boundary above. In a 21-second interval, FIO emitted 4,165
log lines, completed 303 schedule projections, attempted roughly 600 schedule
applications across two radios, and repeatedly wrote unchanged FLRig, FLDigi,
and JS8 state. The endpoint workers remained isolated, but projection completion
incorrectly requested another forced projection and treated a forced data read
as permission to force device writes. The resulting CPU, I/O, log, and Qt signal
load made the Station Control Bar appear unstable.

The following are binding scheduler invariants:

1. A projection completion publishes and consumes its immutable snapshot exactly
   once. Consuming that snapshot cannot request another projection.
2. A forced projection means "refresh authority now"; it does not mean "write
   unchanged state to every endpoint." A changed entry key still applies through
   normal endpoint-lane semantics.
3. "Applying entry" is logged only after deduplication and immediately before a
   command is actually queued. Skipped settled intent must remain quiet.
4. Projection request, forced-request, and completion counters are present in
   cache-only scheduler diagnostics so a future request/completion storm is
   visible without querying SQLite or an endpoint.
5. Scheduler active-entry signals are coalesced before status presentation. The
   Station Control Bar retains its bounded refresh cadence even during an event
   burst.
6. A process-level CPU watchdog samples only monotonic/process clocks. After
   sustained high CPU it writes one bounded, credential-redacted diagnostic and
   Python stack report under the FIO configuration directory's `cpu_hotspots`
   folder, then observes a cooldown. It performs no database, network, process
   inventory, Qt, or endpoint work while sampling and emits no normal-sample log.

Automated qualification for this follow-up requires a regression proving that a
forced projection completion neither requests a successor nor forces another
radio application, the full scheduler regression package, CPU-report redaction
and cooldown coverage, and scheduler-signal UI coalescing. Production Linux must
confirm settled idle CPU and the absence of repeated schedule-application logs.

## September 12 P1 Endpoint Verification And QSY Continuation

Production and local evidence showed healthy FLRig endpoints accepting commands
while FIO repeatedly reported `Applied · verification unavailable` and held QSY
with `Rig PTT state is unavailable`. Direct read-only XML-RPC checks returned PTT,
frequency, and VFO immediately. Scheduler events also contained successful
post-apply verification. The defect is therefore in FIO's asynchronous status
handoff, not basic FLRig connectivity.

The following rules are binding corrections to MES-3 and MES-5:

1. A target command that needs fresh PTT evidence may enter a bounded
   `checking target radio` state. Unknown or stale PTT must never authorize a
   frequency change, but completion of the exact endpoint's status request must
   resume the held intent promptly without waiting for a later timer tick.
2. The continuation is endpoint-, configuration-epoch-, and intent-scoped. A
   completion from an edited, disabled, removed, superseded, or different
   endpoint cannot apply a command. At most one continuation may be pending per
   endpoint, and repeated status callbacks cannot create an apply loop.
3. Resumption uses the fresh cached snapshot and the normal control lane. It
   must re-run shared PTT, RF Safety Guard, busy, ownership, and deduplication
   checks. It cannot bypass any existing safety control.
4. Automatic schedule intents and manual QSY intents retain their original
   source and safety options. A manual QSY request is accepted only as
   `pending verification`, `queued`, or `blocked`; UI code must not describe a
   request as sent merely because the scheduler method was called.
5. Shared-PTT and conflict preflight for a manual QSY must be qualified by
   `target_device_profile_id`. Another independent radio cannot block the
   selected target, while a genuinely shared resource continues to fail closed.
6. Active automatic endpoint status is maintained by a centrally paced,
   cache-only-for-consumers cadence. Polls remain endpoint-local, single-flight,
   bounded, backoff-aware, and free of UI-thread I/O. A hung endpoint cannot
   delay a healthy peer or cause thread growth.
7. A previously verified endpoint may be presented as `Verification aging`
   while a refresh is in flight. `Applied · verification unavailable` is reserved
   for a target with no usable intent/readback pair, an explicit read failure,
   or evidence beyond the operational stale limit. The UI must never imply that
   an unverified command is verified.
8. Apply, verify, hold, and failure events must carry the exact target radio and
   endpoint route so primary-radio fallback cannot misattribute evidence.
9. A liveness refresh must collect every field required by that endpoint's
   current expected-state contract. In particular, an FLRig-controlled radio
   with an expected JS8 offset must refresh both FLRig readback and that radio's
   JS8 offset. A partial refresh cannot erase a still-required field and turn a
   healthy combined endpoint into the generic verification fallback.
10. An active endpoint's liveness eligibility comes from its persisted control
    mode and instantiated endpoint client, not a global process-inventory
    snapshot. A newer queued or running generation does not erase the most
    recent successful readback; that evidence remains usable until a later
    generation succeeds, fails its verification, or naturally becomes stale.
11. Endpoint status coordination is purpose-scoped. A startup/liveness request
    and a post-command verification request for the same endpoint may share the
    bounded endpoint worker but must not share an in-flight snapshot key. The
    command completion owns a fresh post-apply readback; an older liveness
    completion cannot replace or downgrade it.
12. Scheduler resume recovery is authorized only after FIO has committed an
    inactive, hidden, or suspended application transition. The first native
    transition to `ApplicationActive` during window presentation is launch
    completion, not resume: it may settle timers and refresh visible UI but must
    not clear applied intents, expected state, command lanes, or readback.

The P1 automated exit gate requires:

- a cold-cache FLRig QSY remains safe, completes one target status poll, and
  queues exactly one command after fresh PTT-off evidence arrives;
- PTT-on, failed, timed-out, stale, and superseded status completions never
  authorize a command;
- a held automatic schedule intent resumes without another database projection
  or forced all-radio evaluation;
- one hung status endpoint does not delay the peer continuation or operational
  summary;
- the active-endpoint cadence maintains truthful cached readback without direct
  UI, database, process-inventory, or network work;
- an FLRig intent with a JS8 offset remains verified across repeated liveness
  refreshes, and a genuine JS8 read failure is identified as JS8 evidence rather
  than an FLRig disconnection;
- liveness polling still runs when global process detection is stale but the
  configured endpoint client exists, and rapid generation coalescing retains
  the newest successful readback until a later success replaces it;
- startup liveness and first-command verification remain independent, and the
  initial native application activation cannot invoke scheduler resume recovery;
- manual QSY feedback distinguishes pending, queued, and blocked outcomes;
- target-specific shared-PTT and event-attribution tests pass; and
- focused endpoint status, endpoint lane, lifecycle, runtime routing, manual
  control, QSY helper, and architecture guard suites pass.

Linux production confirmation remains an external hardware gate: with FLRig
reachable and PTT off, QSY must progress from checking to verified apply; an
unreachable or transmitting target must remain safely blocked; peer radios must
stay responsive throughout.

Implementation evidence (2026-09-12): the scheduler now retains at most one
newest PTT-waiting intent per normalized endpoint. Status completion is marshaled
back to the scheduler thread and resumes only a fresh, error-free, PTT-known
snapshot whose endpoint mapping and configuration epoch are still current. The
normal apply path is re-entered, so shared PTT, RF Safety Guard, busy checks,
ownership, and command-lane deduplication remain authoritative. Manual QSY now
returns a result-bearing disposition and its UI reports target checking, queued,
manual, or blocked rather than treating a method call as a sent command. Active
runtime endpoints receive paced background status requests through their
existing isolated single-flight lanes; UI consumers remain cache-only. Recent
matching readback remains stably `On schedule · verified` during the brief
in-flight refresh window. Liveness now derives eligibility from each configured
runtime client, refreshes every expected field including a mapped JS8 offset,
and preserves a successful readback while a newer generation is merely queued
or running. A later successful generation fences out an older completion.
FLRig and rigctld expose checked PTT reads so a transport error cannot be
silently converted into safe PTT-off evidence. No schema or configuration
migration was introduced. The final automated partition passes 289 tests with
5 intentional skips; Linux production confirmation remains external.

Thread-concurrency tests must use deterministic barriers/events rather than
timing-only sleeps wherever possible. Hardware tests supplement; they do not
replace fault-injection tests.

## Release Gate

FIO may claim reliable automated multi-endpoint scheduling only when:

- the required physical three-transceiver/two-SDR station passes;
- a deliberately hung endpoint demonstrably leaves every healthy endpoint on
  time and controllable;
- the eight-endpoint synthetic soak passes resource and latency budgets;
- single-radio behavior remains backward compatible;
- shared RF safety is preserved under concurrent dispatch;
- startup, reconfiguration, and shutdown gates pass on Linux and macOS;
- all UI states are based on endpoint-scoped evidence; and
- the work log records exact models, commands, measurements, hardware/software
  versions, failures, and any remaining unpassed human gate.

Until then, UI and documentation must describe multi-endpoint scheduling as
planned or experimental rather than production-verified.
