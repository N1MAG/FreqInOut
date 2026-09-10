# Shortwave Resources Implementation Plan

Status: proposed; packages are sequential and stop at each exit gate

Date: 2026-09-10

Authority: `shortwave_resources_spec.md`

## Delivery Rules

- Preserve unrelated worktree changes.
- The high-reasoning primary model owns architecture, schema/migrations,
  concurrency, radio safety, delegated-diff review, and final integration gates.
- Bounded UI/mechanical work may be delegated to Terra.
- Parser fixtures, store/query tests, performance harnesses, and focused UI tests
  may be delegated to Luna.
- Every delegated diff is reviewed by the primary model before acceptance.
- No package begins until the prior package passes its exit gate.
- No destructive migration is authorized. All schema changes are additive,
  backup-first, transactional, idempotent, and rehearsed on an isolated database
  copy before production use.

## Package R-1 — Resources Navigation And Export Preview

Purpose: implement the clarified navigation and correct the existing export
workflow independently of Shortwave data.

### Work

- Convert Resources from a direct item to a master group in full and compact nav.
- Add Frequencies as its functional child. Reserve and test the typed Shortwave
  route, but do not expose a dead Shortwave child before SW-2 provides Explore
  and Data Sources.
- Route Frequencies to the existing Tools & Resources workspace while retaining
  internal Frequency Catalog, Net Directory, and Import / Export tabs and typed
  contextual deep links.
- Add multi-select to Frequency Catalog without changing detail-row selection.
- Create one Qt-free export-preview/dependency-closure service used by every
  Resources export entry point.
- Replace raw key entry as the primary export flow with contextual selection;
  retain advanced key paste only behind a clearly labeled technical action if it
  is still needed for compatibility.
- Show selected/direct/dependency records, friendly provenance, warnings, item
  count, and payload size before the save dialog.
- Make export/import enforce the same aggregate item and byte limits; add a
  round-trip invariant test.
- Update Help and screen-reader names.

### Suggested allocation

- Primary: navigation ownership, transfer invariant, concurrency/revalidation,
  integration review.
- Terra: responsive nav and preview dialog/sheet UI.
- Luna: transfer unit tests, geometry/theme/keyboard tests, round-trip fixtures.

### Exit gate

- Existing deep links still open the correct internal tab and return safely.
- Full and compact navigation expose Frequencies without an empty Shortwave
  destination; the master/route contract is ready for SW-2.
- Preview is mandatory and cancellation writes nothing.
- A catalog mutation invalidates an old preview.
- Selected net/session/frequency/group relationships round-trip. Station-owned
  HF/Local schedules appear as `Used by — not included in export`.
- 1920x1080, 1000x700, and 900x560 Light/Dark Normal/Large Text tests pass.
- Existing Resources/HF/Local Nets regression suites pass.

## Package SW-1 — EiBi Provider, Schema, And Transactional Import

Prerequisites: dataset distribution and update-policy decisions are confirmed.

### Work

- Add additive startup-owned dataset and schedule-entry schema plus indexes,
  integrated with the existing canonical resource-authority cutover and source
  registry. Legacy/pre-cutover profiles do not expose Shortwave.
- Add Qt-free provider interface and EiBi adapter.
- Decode Latin-1 and parse exact-decimal frequency, UTC/cross-midnight/2400,
  deterministic day rules, DDMM, last-heard markers, language/signal codes,
  target/site dictionaries, persistence, duplicates, and raw diagnostics.
- Require explicit provider season effective-from/to dates and cover winter
  cross-year date evaluation.
- Load README dictionaries per dataset version rather than hard-coding current
  values in UI code.
- Create preview DTO/service and staged import transaction with atomic current
  pointer switch, cancellation, rollback, and idempotency.
- Add provider/dataset bounded query APIs; no GUI yet.
- Add approved bundled data to source packaging and frozen-build manifests.
- Add a fixed-host HTTPS download adapter with bounded redirects/bytes, content
  validation, no credentials/cookies, and no arbitrary user URL execution.
- Add production-clone migration rehearsal and recovery documentation.

### Suggested allocation

- Primary: schema, migration, dataset identity/versioning, atomic promotion,
  cancellation/shutdown ownership, final parser semantics.
- Terra: provider adapter and mechanical packaging changes.
- Luna: authoritative parser fixtures, malformed-rule matrix, migration,
  idempotency, rollback, and 10,000-row performance tests.

### Exit gate

- All audited A26 rows are imported or explicitly diagnosed; no silent loss.
- Exact duplicate handling preserves source audit identity and deduplicates the
  user projection.
- Same bytes reimport without writes or duplicated current rows.
- Interrupted/rejected import leaves the current dataset unchanged.
- Query APIs remain bounded and meet warm p95 on the Linux baseline.
- Startup adds no network call and opening no screen causes import/migration work.
- Pre-cutover/failure profiles retain prior readers/data and expose recovery
  guidance rather than partial Shortwave ownership.
- Source packaging and clean-install frozen-build smoke pass.

## Package SW-2 — Shortwave Explore And Data Sources UI

Prerequisite: SW-1 passes and initial broadcast/utility content decision is
confirmed.

### Work

- Add the functional lazy Shortwave route/workspace under Resources, then expose
  the full/compact Shortwave child for the first time.
- Build Explore filters, search normalization, bounded result model, detail/Why,
  provenance, diagnostics, source age, Scheduled now/Starting soon evaluation,
  and UTC/local display.
- Build Data Sources status, import/update preview, explicit Apply, rollback, and
  diagnostics export.
- Keep raw keys/hashes in Technical details only.
- Coalesce/cancel stale searches and refresh only while visible.
- Add Help for listing-versus-reception, source fields, dates, special rules,
  stale data, and safe use.

### Suggested allocation

- Primary: time-rule service/UI boundary, provenance semantics, idle/concurrency
  review, integration gate.
- Terra: Explore/Data Sources responsive UI, chips/iconology, details and preview.
- Luna: scheduled-now fixtures, query-generation tests, theme/large-text/keyboard
  tests, activation and idle CPU profiling.

### Exit gate

- Users can find station, frequency, language, target, site, and now/soon rows.
- Unsupported time/day rules never appear as confidently Scheduled now.
- Home country, transmitter site, and target remain distinct.
- Hidden tab performs no polling and UI never scans/materializes all rows.
- Cold/warm activation, search, query, idle CPU, cancellation, and shutdown budgets
  pass on macOS and Linux.
- Responsive/theme/accessibility matrix and source update/rollback workflows pass.

## Package SW-3 — Receive-Only Listening Calendar

Prerequisite: the Listening behavior decision is confirmed.

### Work

- Add additive reminder/accepted-snapshot schema and store.
- Add `Add to Listening`, reminder editor, optional configured receiver identity,
  reminder lead, recurrence projection, dismissal, source-update diff, and
  `Apply listing update` / `Keep my reminder`.
- Add a separate Listening reminder surface to Ops Center only after its bounded
  projection and collapsed/no-query behavior pass.
- Do not add Operating Group, SOP, HF scheduler, QSY, launcher, or PTT behavior.

### Suggested allocation

- Primary: snapshot/version model, recurrence/concurrency, safety boundary,
  Ops integration review.
- Terra: Listening and Ops reminder UI.
- Luna: reminder recurrence, source-diff, scheduler-isolation, Ops collapse,
  responsive, and performance tests.

### Exit gate

- Reminder state survives source-season changes without silent mutation.
- Dismissing one occurrence preserves recurrence.
- Projection is bounded and fast; collapsed/hidden views perform no queries.
- Static/runtime tests prove no commandable scheduler, QSY, launch, PTT, or
  automatic SOP path is reachable.
- Mac/Linux reminder workflows and 30-minute soak pass.

## Package SW-4 — Optional Manual Transceiver Tune

This package is excluded until separately authorized and hardware-tested.

### Work

- Reuse the canonical manual QSY path; do not introduce a new CAT owner.
- Require an explicit configured transceiver, supported mode/bandwidth, operator
  confirmation, radio-not-busy/PTT checks, RF Guard, timeout, and readback.
- Reuse manual QSY's off-schedule/hold behavior. Confirmation shows the target
  radio, current-plan impact, hold/recovery behavior, and Resume path.
- Tune once. Never schedule automatic QSY from a listing or reminder. A separate
  transient receive-tune owner is not introduced by this package.

### Exit gate

- Supported-hardware matrix passes on macOS/Linux as applicable.
- Failure, cancellation, disconnect, busy/PTT, and shutdown leave radio and FIO in
  a known state.
- No unsupported radio displays an actionable Tune control.

## Package SW-5 — Optional Named SDR Adapter

This package is excluded until the user names the first supported SDR application
and control API.

### Work

- Define one adapter with capability discovery, tune/mode contract, lifecycle,
  timeout, cancellation, reconnection, and readback.
- Introduce an explicit receiver-control capability while keeping observer SDR
  identity separate from proven tuning capability. An endpoint alone cannot
  enable `control_via`, scheduler ownership, PTT, or auto-tune.
- Expose actions only when a live adapter reports the required capability.

### Exit gate

- Real hardware/application acceptance passes on its supported OS matrix.
- No UI claims tuning success without readback.
- Disconnect/restart/shutdown do not leak threads, timers, or endpoints.

## Release Evidence

For every completed package, append the work log with:

- status and exact gate result;
- model and effort used for each delegated work package;
- reviewed files/diffs;
- automated test counts and commands;
- performance/CPU/event-loop measurements;
- migration/rollback evidence when applicable;
- responsive/theme/text/hardware matrix results; and
- remaining human or Linux/hardware gates without inferring success.
