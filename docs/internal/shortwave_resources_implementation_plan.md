# Shortwave Resources Implementation Plan

Status: R-1 and SW-1 through SW-3 complete with automated exit gates passed.
SW-4 remains excluded pending separate authorization and hardware acceptance;
platform soak evidence remains part of release qualification.

Date: 2026-09-10

Authority: `shortwave_resources_spec.md`

SDR receiver work is governed and ordered by
`sdr_receiver_control_implementation_plan.md`. SDR control is the implementation
priority before Shortwave packages. The Shortwave data model remains independent
and cannot create a second receiver-control path.

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

Status: complete — exit gate passed 2026-09-10.

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

Status: complete — exit gate passed 2026-09-10.

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

Status: complete — automated exit gate passed 2026-09-10; macOS/Linux
interactive soak remains release evidence, not an implementation blocker.

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

Status: complete — automated exit gate passed 2026-09-10; macOS/Linux
30-minute interactive soak remains release evidence.

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

## SDR Receiver Dependency

The former generic SW-5 placeholder is replaced by the separately gated packages
in `sdr_receiver_control_implementation_plan.md`. Shortwave integrates only the
shared receiver chooser, universal manual card, and adapters that have already
passed their hardware/platform gates. Adding another SDR application does not
require a new Shortwave-specific adapter.

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

## September 11 Native Worker-Lifecycle Requalification

Linux production ended abruptly while the operator opened Data Sources. The
application log contained no Python exception or failed Shortwave parse record.
A local reproduction subsequently produced a native segmentation fault in the
short-lived Qt thread completion path. SW-2 and SW-3 therefore have this stronger
binding implementation rule:

- Explore, Listening, and Data Sources each own one serialized daemon task lane;
- rapid requests cancel/coalesce to the newest generation;
- completion is delivered to the owning GUI thread through a signal bridge;
- closing or replacing a page cancels pending work and returns immediately;
- no page owns or destroys a live `QThread`; and
- every Shortwave operation emits a bounded duration/cancellation metric and logs
  an actionable traceback on a Python failure.

The source review exit gate includes repeated open/review/switch/close cycles,
closing during a deliberately slow operation, and process-level proof that the
test exits normally rather than merely asserting widget state before teardown.
