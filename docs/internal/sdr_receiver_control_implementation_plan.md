# SDR Receiver Control Implementation Plan

Status: active; MES-4 shared receiver-lane core and MES-5 automated lifecycle
infrastructure complete, SDR package gates remain sequential

Date: 2026-09-10

Authority: `sdr_receiver_control_spec.md`

Scheduler concurrency authority:
`multi_endpoint_scheduler_concurrency_spec.md`. Package SDR-1 depends on the
corresponding MES-0 through MES-3 gates; automated SDR adapter integration,
beginning with SDR-2, depends on MES-4.

## Delivery Priority And Rules

SDR receiver control is the prerequisite implementation priority before the
Shortwave feature packages begin. Documentation/data review may continue, but no
Shortwave production package should imply or implement tuning outside this plan.

- Preserve unrelated worktree changes.
- The high-reasoning primary model owns capability architecture, ownership,
  concurrency, migrations, safety boundaries, delegated-diff review, and final
  integration.
- Bounded adapter/UI work may be delegated to Terra.
- Protocol fixtures, focused tests, and performance harnesses may be delegated
  to Luna.
- Do not add a direct hardware driver dependency in these packages.
- Every package must pass on supported desktop platforms before the next begins.

## Package SDR-0 — Compatibility Catalog And Truthful Manual UX

### Work

- Add one versioned compatibility registry for hardware family, application/API,
  upstream support, FIO verification, capability, platform, and evidence.
- Add the four operator states from the governing specification.
- Correct observer setup/help so host/port and `SDR Follow` never imply control.
- Add the universal manual receiver card: frequency/mode/bandwidth, copy actions,
  optional safe application launch, and concise manual tuning guidance.
- Place manual receivers and future API receivers in one chooser without calling
  either hardware unsupported.
- Add Help tables for manual hardware and FIO-verified tuning.

### Exit gate

- No current configuration is mislabeled as FIO-controlled.
- Every configured observer can be selected for manual use.
- Registry data is bounded, versioned, and testable without device discovery.
- Responsive, theme, Large Text, keyboard, and no-real-identity hint tests pass.

## Package SDR-1 — Receive-Only Control Core

### Work

- Introduce the Qt-free receiver-control interface with no PTT surface.
- Implement or consume the central no-I/O coordinator, endpoint identity, lane,
  status, safety, and lifecycle contracts through MES-0 to MES-3 in
  `multi_endpoint_scheduler_concurrency_spec.md`; do not create a receiver-only
  competing scheduler architecture.
- Add capability probe, bounded target enumeration, state/readback, command
  serialization, cancellation, timeout, and lifecycle ownership.
- Adapt the useful frequency operations from the current RigCtlD client without
  routing observer devices through transceiver scheduling or QSY ownership.
- Add persisted adapter configuration and last verification evidence using an
  additive startup-owned migration.
- Add fake adapters and failure/latency/ownership test fixtures.

### Exit gate

- Static/runtime tests prove the receiver path cannot transmit or enter the HF
  scheduler.
- UI-thread blocking, leaked worker/timer, stale callback, and shutdown tests pass.
- Capability and readback failures fall back to Manual tuning.
- Production-clone migration rehearsal and rollback evidence pass.
- The MES-0 through MES-3 exit gates pass before an SDR adapter is attached.

Implementation note (2026-09-10): the Qt-free receiver contract, target-qualified
endpoint identity, shared isolated lane integration, zero-I/O manual fallback,
safe additive configuration fields, cache-only UI boundary, lifecycle fencing,
and bounded shutdown/diagnostics are complete under MES-4/MES-5. The additive
receiver columns and rollback were rehearsed on an isolated clone of the supplied
production database with matching pre/post source hashes. SDR-1 is not closed:
operator setup/capability UX and an attached application adapter remain gated
work. No hardware combination is yet labeled FIO-verified.

## Package SDR-2 — SDR++ RigCTL Adapter

This is the recommended first adapter because FIO already has a compatible
protocol seam and SDR++ exposes the broadest practical cross-platform hardware
module set through one receiver application.

Before SDR-2 is called complete, the adapter must also pass MES-4 receiver-lane
integration. The combined five-endpoint and synthetic soak remain MES-5 release
gates rather than being duplicated here.

### Work

- Add a named SDR++ RigCTL adapter, not a generic open-port success path.
- Provide setup for module enablement, selected VFO, tuning enablement, bind
  address, and port.
- Probe the basic commands SDR++ reports; tune frequency and optional mode only
  when advertised/verified.
- Read back the selected VFO frequency and retain the manual control card.
- Run hardware acceptance first with one RTL-SDR-family receiver and one
  HF-capable receiver available to the project; record exact combinations.

### Exit gate

- No PTT command is emitted.
- Tune/readback, manual-before/after, reconnect, restart, timeout, and shutdown
  pass on macOS and Linux for each published verified combination.
- Other upstream SDR++ hardware stays labeled application-compatible, not
  FIO-verified.

## Package SDR-3 — SDRconnect WebSocket Adapter

### Work

- Add WebSocket version/capability negotiation, bounded target enumeration, and
  push property-state handling.
- Require `can_control`; select VFO frequency rather than changing hardware
  center frequency for ordinary listening.
- Add demodulator and filter bandwidth only when writable and verified.
- Respect application/server hardware-control ownership and nRSP remote state.
- Test representative SDRplay hardware separately; never generalize one passing
  model to all RSPs.

### Exit gate

- Readback and property events agree after tune.
- A denied control lease stays manual and FIO does not retry aggressively.
- At least one exact SDRplay hardware combination passes each platform FIO claims.
- Linux Mint results are labeled FIO-tested, not vendor-supported.

## Package SDR-4 — SDRangel REST Adapter

### Work

- Discover bounded live RX device sets and expose friendly hardware/plugin names.
- Retrieve the exact device/channel settings schema before patching it.
- Select center-frequency and demodulator-channel behavior explicitly; never
  guess a plugin JSON subtree.
- Read back device and channel state; use reverse API events only as an optional
  visible-session optimization.
- Test representative hardware/plugin combinations and package caveats.

### Exit gate

- Multiple device sets cannot cause the wrong receiver to tune.
- Unknown/new plugin schemas degrade to Manual tuning.
- REST errors, application restart, stale indexes, cancellation, and shutdown pass.
- Only tested hardware/plugin/platform combinations enter the verified table.

## Package SDR-5 — Secondary Bridges

### Work

- Add a separate Gqrx RigCTL-compatible adapter profile and its center/VFO
  semantics only after focused acceptance.
- Add KiwiSDR `Open tuned receiver` URL handoff as a manual/operator-owned action;
  do not label it persistent verified API control without readback.
- Evaluate other receiver applications only when a stable documented control API
  and test hardware are available.

### Exit gate

- Gqrx uses its own capability/quirk tests rather than inheriting SDR++ claims.
- Kiwi URLs are encoded, bounded, contain no credentials, and require an operator
  action to open.
- Both paths retain the universal manual card.

## Deferred Direct-Device Engine

SoapySDR, UHD, librtlsdr/RTL-TCP, libhackrf, libairspy, and SDRplay's device API
are explicitly deferred. A future package requires a separate design for
exclusive ownership, out-of-process discovery/driver isolation, I/Q streaming,
demodulation/audio, device handoff, and crash recovery. It is not a shortcut for
the application adapters above.

## Shortwave Dependency

After SDR-2 passes, Shortwave may integrate the receiver chooser and manual/API
states. Shortwave browsing and data import remain logically independent, but the
user-visible tuning flow must use this receiver-control contract. SDRconnect and
SDRangel can follow as incremental verified adapters without changing Shortwave's
data model.

## Release Evidence

For each package, append the UI work log with:

- model/effort for each work package;
- reviewed diffs and acceptance commands;
- exact hardware/application/API/OS matrix;
- tune/readback and manual fallback evidence;
- CPU, latency, thread, disconnect/reconnect, and shutdown results; and
- any unpassed human hardware gate, without inferring success.
