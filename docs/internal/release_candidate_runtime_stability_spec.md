# Release-candidate runtime stability specification

Status: implementation and automated acceptance complete; external hardware/soak qualification remains open

This specification is binding for the multi-rig release candidate. It addresses
the September 15 startup/control-bar report, the repeated dynamic Expect ingest
found in the attached performance evidence, and bounded Mesh connection retry.
The multi-endpoint scheduler, UI, background-ingest, and Mesh contracts remain
authoritative; this document narrows their behavior for these release defects.

## Evidence and root causes

- A normal desktop focus change was treated as an operating-system resume.
  Scheduler resume retired endpoint lanes and discarded expected station state,
  even though the rig's fresh frequency readback remained valid. The Station
  Control Bar therefore fell back to `Applied - verification unavailable`.
- PTT readback is a pre-retune safety interlock only. It is not frequency
  verification. A matching, fresh FLRig/rigctld frequency/VFO readback remains
  authoritative when the station is not being retuned.
- The per-radio dynamic FLAMP Expect cursor was written into a short-lived
  profile wrapper rather than durable worker settings. Historical records were
  consequently revisited on later background-ingest cycles.
- An unavailable enabled Mesh adapter retried forever. BLE connection attempts
  may each consume 20-30 seconds, so unbounded retry caused avoidable CPU,
  battery, and log churn.

## RCS-1: application lifecycle and station verification

1. Ordinary `ApplicationInactive` focus changes may pause noncritical UI work,
   but must not call scheduler resume, retire endpoint lanes, clear expected
   intent, or invalidate a fresh endpoint readback.
2. `ApplicationHidden`, `ApplicationSuspended`, and detected sleep/clock
   discontinuities may request scheduler lifecycle recomputation. The existing
   monotonic/wall-clock discontinuity observer remains the fallback when a
   platform does not emit a reliable suspended state.
3. First activation remains side-effect free.
4. Frequency verification comes from the configured rig-control path
   (FLRig/rigctld or JS8Call when it owns control). PTT evidence gates a required
   retune; missing PTT evidence alone must not turn an already matching fresh
   frequency readback into `verification unavailable`.
5. When a fresh readback already matches the current scheduled intent, the
   scheduler may adopt that intent into its read model without issuing a device
   command. This adoption is not an apply attempt and must not log `Applying`.

## RCS-2: scheduler command deduplication

1. Projection, focus, readback, and lifecycle feedback must not enqueue an
   identical frequency command while a matching fresh readback is available.
2. Only a real schedule transition, explicit operator action whose semantics
   require a write, or verified drift may enqueue a command.
3. A skipped matching intent must rebuild enough endpoint expectation state for
   the Station Control Bar to report verified/ready.
4. Endpoint isolation remains mandatory: adopting or retrying one endpoint must
   not mutate another endpoint's pending, expected, or last-applied state.

## RCS-3: dynamic Expect cursor durability

1. Both `spotter_directed_offset_*` and `expect_directed_offset_*` cursor keys
   are worker-owned durable settings, including source- and radio-scoped forms.
2. A newly constructed device-profile settings wrapper must read the last saved
   cursor. It must not replay already checkpointed records.
3. Held or rejected records that were successfully parsed and checkpointed must
   not become an unbounded CPU/log loop.
4. No database schema change is introduced for this correction.

## RCS-4: bounded Mesh retry

1. Each enabled adapter identity receives at most **three consecutive failed
   connection attempts**. Failures one and two use the existing exponential
   backoff. Failure three enters a paused, operator-action-required state with
   no next retry deadline.
2. Timer polls while paused must not mutate the failure count or call the native
   adapter. The visible state is `needs-attention`, with the release text:
   `Reconnect paused after 3 failed attempts - select Connect to try again.`
3. Pairing/security errors that already require operator action remain terminal
   after their first failure.
4. An explicit Connect/Reconnect action resets only the selected adapter's
   consecutive-failure budget and attempts immediately. A failed manual attempt
   is attempt one of a fresh three-attempt series. It must not unblock or restart
   unrelated adapters.
5. Success clears the budget. A connection-affecting configuration/identity
   change starts a fresh budget for that adapter. An unchanged in-process worker
   replacement preserves countdown or exhaustion so replacement cannot bypass
   the limit.
6. Retry state remains process-local; a full application restart starts one new
   bounded three-attempt series. This avoids a settings/schema migration while
   guaranteeing that a live process cannot retry forever.
7. Manual Disconnect remains stopped until Connect; it neither consumes nor
   resets a retry budget. Shutdown and in-flight native connection ownership
   contracts remain unchanged.
8. Health-only transitions must not clear retained Mesh messages, Inbox data,
   map pins, or trigger a full map redraw.

## RCS-5: performance and regression acceptance

- A focus-away/focus-back sequence issues zero rig writes when readback already
  matches and leaves the control bar verified.
- A true suspended/clock-discontinuity path fences stale completions and performs
  one bounded recomputation.
- Two background-ingest passes over unchanged DIRECTED data invoke each dynamic
  Expect record at most once.
- An always-failing Mesh adapter receives exactly three attempts despite later
  timer polls; one manual Connect produces one immediate fourth attempt and a
  fresh bounded series.
- Multi-adapter tests prove independent budgets and manual reset isolation.
- Existing scheduler endpoint-lane, PTT safety, background ingest, Mesh lifecycle,
  map-retention, and shutdown suites remain green.
- External Linux/macOS/Windows hardware and long-soak validation remain a
  release gate; automated tests do not claim native BLE or rig-hardware proof.

## Implementation and acceptance evidence

The implementation now distinguishes ordinary desktop focus loss from native
hidden/suspended recovery, defensively ignores direct scheduler resume calls
when no clock discontinuity occurred, and reconstructs endpoint expectation
state from a complete fresh matching readback without a device write. JS8
offset verification is required only when the active entry actually gives JS8
offset authority; PTT remains a retune-only safety gate.

Dynamic Spotter and Expect DIRECTED cursors share the durable worker-settings
path. Mesh retry state pauses on failure three, survives unchanged in-process
worker replacement, resets only through explicit per-adapter Connect/Reconnect
or success/config identity change, and projects visible `needs_attention`
guidance through the existing health store. Retained message/map data is not
mutated.

Work packages and models:

- high-reasoning primary GPT-5 model: specification, scheduler lifecycle and
  verification design, persistence and Mesh retry architecture, all production
  changes, delegated-result review/correction, integration, and exit gate;
- `gpt-5.6-terra`, high reasoning: read-only Mesh lifecycle/retry audit,
  operator-reset and multi-adapter isolation recommendations;
- `gpt-5.6-luna`, high reasoning: focused regression tests for Expect cursor
  durability, scheduler lifecycle preservation, and readback deduplication.

Primary review strengthened the delegate package by adding fresh-readback intent
adoption, explicit PTT-versus-frequency-verification coverage, JS8 authority
scoping, three-attempt worker and adapter-isolation tests, persisted
needs-attention guidance, live per-adapter manual reset, and connection-signature
coverage for ports/timeouts.

Acceptance results:

- 69 focused background-ingest, application-lifecycle, endpoint-lifecycle, and
  endpoint-verification tests passed.
- 291 adjacent scheduler/dynamic-Expect/background-ingest tests passed with
  2 skips and one existing Qt signal-disconnect warning.
- 144 focused Mesh/source-connection tests passed in isolated Qt groups; the
  broader adjacent Mesh collection passed 167 tests.
- Python compilation and `git diff --check` passed.
- A full repository run reached the early Compose Qt group, then aborted in the
  known shared-QApplication/thread test-harness failure. The exact reported
  Compose test passes alone (`1 passed`), and no changed production module is in
  that stack. Focused and adjacent release gates therefore pass; the repository
  harness limitation is recorded rather than silently waived.

External exit gate: validate one matching-frequency focus cycle and one actual
QSY on the configured FLRig/rigctld hardware, then confirm a missing Mesh device
pauses after three attempts and a Settings Connect starts one fresh bounded
series on macOS and Linux. Windows remains required before public release where
the configured adapters are supported.
