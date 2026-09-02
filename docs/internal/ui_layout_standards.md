# FIO UI Layout Standards

## Minimized Window Usability

Every UI change must be reviewed at reduced window sizes, not only at the
developer's normal desktop size. A minimized or narrow view may become more
compact, but it must remain usable.

Required behavior:

- Primary navigation remains visible or scrollable.
- Action and filter panels use vertical scrolling when their content cannot fit.
- Buttons, combo boxes, text fields, and labels keep a readable minimum height.
- Tables may scroll internally, but surrounding controls must not collapse into
  clipped slivers.
- Horizontal scrolling is avoided for control panels unless the content is an
  intentionally wide data grid.
- Horizontal scrolling is permitted as an overflow safety net for high-use
  command surfaces whose controls must remain immediately reachable, such as the
  station command radio-card strip. It must not be the only adaptation: controls
  should first shorten, wrap, stack, or switch to compact labels before clipping.
- Dense inbox/table workspaces may preserve a wider designed body width and use
  horizontal scrolling when shrinking would clip filters, focus buttons, or
  operator-critical table columns.
- Responsive layouts should prefer stacked/vertical control groups over shrinking
  controls below their usable size.
- At minimized widths, a surface may promote a full-workbench/detail action, but
  the embedded surface must still look intentional and must preserve enough
  access for the operator to confirm draft/status state, reset or cancel work,
  and recover without assuming the UI is broken.
- Dense workspaces with many categories should use a stable left navigation rail
  with scrollable category content instead of long horizontal button rows.
- Settings-style views should keep the left rail bounded and scrollable, with the
  selected content pane owning the remaining width.
- Resizable split panels must advertise that they are resizable. Use the shared
  splitter-handle styling helper so the divider is wide enough to grab, visually
  distinct from surrounding borders, and hover-highlighted without relying on
  persistent tooltips that can cover operational content. Do not rely on an
  unmarked one-pixel divider for core workflows.
- If a label looks like a control, make it a real control. Time windows such as
  schedule horizons must be selectable when they appear in the view header;
  static `2h ?` labels are not acceptable on operational dashboards.
- Settings must always provide a clear path to create custom user-defined
  operating groups. `Settings > Main > HF Operating Groups` and the local group
  views may offer built-in or imported group presets, but those presets must not
  replace the user's ability to add a new custom group name and associated
  frequency/local configuration. Potential bug to verify: adding a new group
  under `Settings > Main` is currently not working.

Acceptance check for future UI slices:

- Test or manually verify the touched screen at a reduced window size around
  `1000x700` and at a tighter minimized-like height around `900x560`.
- If controls do not fit, add a scroll area or alternate compact layout rather
  than allowing Qt to compress fields.
- Add focused regression coverage for scroll areas or compact layout behavior
  when the changed screen contains dense controls.

## Text Size Accessibility

FIO must remain usable when `Settings > Main > Text Size` is set to `Large`.
This is a design rule, not a cosmetic preference. Operators with poor vision
must not be forced to choose between readable text and usable controls.

Required behavior:

- Large text must not clip inside buttons, chips, combo boxes, line edits,
  labels, tab selectors, radio cards, setup rows, or status banners.
- Widgets must grow from font metrics before they wrap, scroll, or elide. Do
  not hard-code a small height such as `24`, `30`, `32`, `36`, or `40` for a
  text-bearing control unless it is computed from the active font.
- Fixed heights are allowed only for non-text graphics or for controls whose
  height is derived from `QFontMetrics` plus padding.
- Fixed-width labels and buttons must be reviewed at Large text. Prefer
  content-aware widths, wrapping labels, elided summaries with tooltips, or a
  scrollable/stacked layout.
- Do not shrink the user's selected font to make text fit. Preserve readable
  text, then adapt layout with height, width, wrapping, elision, or scrolling.
- Dense operator workspaces may keep compact layouts, but every action remains
  reachable and readable. If a panel cannot fit at Large text, add local
  scrolling or promote the full workbench/detail surface.
- New UI code must use shared theme/layout helpers for text-bearing control
  sizing. Local one-off accessibility guards are temporary exceptions and
  should be moved into the shared helper layer when touched.
- Net-control/NCS views must present the workflow as compact sections, not as
  one long sparse row across the window. Setup controls belong in a bounded
  setup group, the check-in/roster table owns the remaining vertical space, and
  empty-state text stays attached to that table. At reduced height and Large
  text, controls may wrap or the page may scroll, but the operator must not see
  a large blank gap above the active net controls.
- Net-control/NCS views must expose an explicit `NCS Session` context before
  start, end, ACK, QSY, roster, or check-in actions. The session label uses the
  configured radio short name, protocol, role, and net name when known. When
  multiple active radios can run NCS or ANCS duty, show short-name radio chips
  so the operator can switch the scoped session without hunting through
  Settings. The selected session chip uses a success/go treatment; alternate
  eligible sessions use the information treatment and carry a tooltip. NCS
  actions must never be visually ambiguous about which radio/source they will
  affect.
- Parallel NCS and ANCS duty across more than one radio is represented as
  separate session snapshots: protocol, radio profile id, role, net name, and
  timing state form the session key. Only the visible session renders live
  controls; inactive sessions retain state and summarize as chips or cards.
  Long-running polling, file scans, and roster comparison work must follow the
  UI Responsiveness Contract and update the visible session through snapshots.
- The main shell, navigation, Ops Center, and future NCS-aware summaries must
  read active NCS/ANCS state from persisted session snapshots instead of
  inspecting the currently visible tab. Collapsed navigation groups and compact
  status indicators must expose active session labels in tooltips or detail
  views so a multi-radio operator can understand what is running after tab
  switches or restart.
- FLDigi / SSB NCS has its own live-workbench contract in
  `docs/internal/fldigi_ncs_workbench_spec.md`. UI changes there must preserve
  the `Action for: NCS <callsign> | ANCS <callsign>` wording, keep QSY actions
  in the Station Command Bar, and count only accepted aggregate check-ins for
  post-net summaries.

Implementation guardrails:

- Start with conservative global helpers that only increase undersized controls
  or remove unsafe maximum heights. Do not redesign every tab in the same slice.
- Provide an opt-out property for intentionally fixed non-text widgets.
- Keep full descriptions in tooltips/details when visible labels must remain
  short. This is especially important for radio profile names, file paths, and
  status explanations.
- Validate Large text on the high-use screens first: Station command bar,
  Messages Inbox, Compose, Settings, Map controls, SOP Builder, and net-control
  tabs.

See `docs/internal/ui_text_size_accessibility_assessment.md` for the current
assessment and rollout plan.

See `docs/internal/operational_view_framework_spec.md` for the preferred product
architecture for new data-driven screens. New sources should feed reusable
operational views through normalized projections; pages should opt into
supported views instead of creating bespoke layouts by default.

Any spec that introduces or changes a data source, projection, operational view,
or page-level data layout must explicitly answer the Operational View Framework
Mandatory Design Gates before implementation starts. The spec must cover source
meaning, volume and retention, provenance and trust, constrained customization,
map scaling, and action validity. If a gate does not apply, the spec must say why
so the omission is intentional and reviewable.

See `docs/internal/controlfreq_operational_awareness_center_spec.md` for the
Ops Center dashboard direction. It is the reference design for a high-use,
role-focused operational awareness surface that must remain glanceable at Large
text and reduced window sizes.

## Theme Contrast

FIO must remain readable in Light and Dark themes without requiring users to
find individual broken tabs. Theme safety is a shared UI contract, not a
per-screen polish task.

Required behavior:

- New tab bars, nested workbenches, tables, list views, status chips, and
  settings subpanels must use the shared theme palette or shared styling helpers.
- Do not hard-code light gray tab/table backgrounds with white or pale text.
  If a surface needs semantic colors, define both background and foreground for
  each theme and verify contrast.
- Item-level table backgrounds must be refreshed when the app theme changes.
  Tables that color rows by state must not keep stale light-theme brushes after
  switching to Dark theme.
- Selected and disabled table/list rows must keep readable foreground colors in
  both themes. Selection styling may be subtle, but text cannot wash out.
- `QTabWidget`/`QTabBar` controls should normally rely on the global app
  stylesheet. If a local override is necessary, it must explicitly cover normal,
  selected, hover, and disabled states for both themes.

Acceptance check for future UI slices:

- Review touched screens in Light and Dark themes, including selected rows,
  disabled actions, nested tab widgets, and settings tables.
- Add a focused regression test when a change introduces local stylesheet rules
  for tab bars, table items, semantic table rows, or dark-theme-specific colors.

## UI Responsiveness Contract

FIO must never make the operator wonder whether the app froze. Every new UI,
source integration, map layer, and data-view contract must preserve event-loop
responsiveness under normal use, reduced window sizes, Large text, and shutdown.
The UI watchdog is a last-resort diagnostic, not an acceptable steady-state
behavior.

Mandatory rules:

- GUI event handlers must not perform unbounded blocking work. Expensive device
  I/O, BLE/serial/TCP reads, filesystem scans, database rebuilds, geocoding,
  map projection, route derivation, subprocess work, and message parsing must
  run in a worker, async service, or coalesced timer-backed pipeline.
- Do not use `Qt.BlockingQueuedConnection` from GUI code. Cross-thread requests
  must be queued, signal-driven, cancellable where practical, and safe if the
  result arrives after the view changed.
- Do not use unbounded `QThread.wait()` during UI shutdown or tab transitions.
  If a bounded wait is unavoidable, keep each wait at or below 250 ms and prefer
  a non-blocking quit/request-interruption flow.
- Do not call `subprocess.run`, `subprocess.check_call`, or
  `subprocess.check_output` from GUI code without a small timeout and a clear
  reason it cannot be moved off-thread.
- Do not call `future.result()` on the GUI thread unless the future is already
  known complete inside a done callback. Prefer Qt signals or queued callbacks
  that carry the result without blocking.
- Do not use `QApplication.processEvents()` as a layout or performance fix.
  Existing uses are migration exceptions only; new UI code should use workers,
  timers, debouncing, or explicit progress states.
- Data-view contracts must be incremental and stale-result safe. A view may
  render a placeholder or last-known snapshot while a newer projection builds,
  but an old worker result must not overwrite a newer filter/source selection.
- Expensive projections must use worker snapshots. Capture widget state,
  filters, and settings into plain Python data on the UI thread; build schedule,
  map, route, message, and operational projections in a worker; then apply only
  the latest generation through a Qt signal on the UI thread.
- Follow-up actions that need the same projection, such as save, RF Guard
  review, or route-to-source actions, should reuse the latest matching worker
  snapshot before rebuilding. If the snapshot is stale, show progress and build
  a fresh worker result instead of blocking the event loop.
- Map views must coalesce redraws and avoid full WebEngine reloads for simple
  layer, filter, or selection changes. High-volume layers such as Mesh, APRS,
  and future MQTT sources must use stable layer updates and bounded projection
  work.
- Source health is not map data. Health-only changes, including MeshCore BLE
  reconnecting, away, or disabled states, may update chips and status labels but
  must not force map projection or WebEngine redraws.
- Local/device sources must publish a `SourceConnectionSnapshot` lifecycle
  (`connected`, `reconnecting`, `away`, `disabled`, or `config_error`) instead
  of making each view infer meaning from a boolean connection flag. Retained
  observations, nodes, and routes remain valid for Inbox, Ops Center, and Map
  even when the live device is disconnected.
- Shutdown and disconnect/reconnect flows must be non-blocking. Ordinary
  teardown must not trigger a UI watchdog hang report.
- QObjects that own timers must stop and delete those timers in their owning
  thread. GUI shutdown should queue the worker stop, let the worker emit its
  finished/stopped signal, and only use a short bounded wait as a cleanup
  grace period.

Implementation gates:

- Specs that add a UI surface, data source, source contract, or map layer must
  state how work is bounded, debounced, cancelled, or moved off the UI thread.
- Tests must include static guardrails for high-risk blocking patterns and
  focused behavior tests for any worker/coalescing path introduced by the slice.
- Known legacy exceptions must be recorded in the guardrail tests and reduced as
  touched. Adding a new exception requires a comment explaining why it is safe
  or temporary.
- User-observed regressions that span more than one implementation pass are
  tracked in `docs/internal/ui_regression_work_log.md`. Update that log whenever
  a new visual, routing, responsiveness, theme, or lifecycle issue is confirmed
  or closed.

Current remediation gates from the responsiveness audit:

1. Map rendering keeps a stable shell. Routine marker, path, city, mesh,
   traffic, and topic changes update the existing WebEngine page through a
   payload push. Full HTML/page reload is reserved for base-map or structural
   configuration changes. Asynchronous JavaScript payloads carry a generation
   guard so stale map updates cannot overwrite a newer view.
2. Message Inbox and Message Compose build file, database, and mesh projections
   from immutable snapshots. Results are applied only when their request or
   generation id is current; older results are discarded without clearing the
   current table.
3. Daily Schedule and Settings must not use `QApplication.processEvents()` to
   force repaint or commit editors. Use focus changes, model commits,
   `QTimer.singleShot(0, ...)`, or worker completion callbacks.
4. Mesh runtime shutdown and reconnect paths must avoid long waits. If a GUI
   thread waits for a worker, the wait is capped at 250 ms and failure to stop
   cleanly is logged rather than freezing the UI.
5. Plan Builder, daily schedule, net schedule, and SOP views must move broad
   table projection and RF Guard scans into worker snapshots before further
   high-volume source families are added.
6. NCS, operator history, and import/export workflows are lower-risk but still
   covered by this contract. New file scans, database scans, subprocess calls,
   and import/export operations must be bounded and must not run as open-ended
   work in GUI handlers.

Main navigation should follow operator decision flow, not legacy feature names:
`Ops Center`, `Map`, `Messages`, `NCS`, `Operators`, `Plan Builder`, `Station`,
`Settings`. Internal route keys may remain stable, but user-facing labels should
use the decision-flow names.

## Station Command Bar

The station command bar is the primary always-visible radio control surface. It
is organized as a compact source rail plus direct command cards for the sources
the operator is most likely to touch now.
Commandable radios get direct short-name chips. Source families that may have
many saved endpoints or noisy discovery state, such as Mesh now and APRS later,
render as one aggregate dropdown chip with saved-device actions.

When exactly two commandable radios are active and the viewport can fit two
usable cards, render both full radio command cards by default. This is the
normal two-radio operator posture and avoids hiding available actions behind an
extra click. At three or more commandable radios, or when the bar cannot fit two
usable cards, render one focused command card and keep every off-focus radio
reachable and status-visible in the chip rail.

Color semantics must be consistent and not color-only. A clear focused radio is
green because it is the active/go control surface. Clear available radios are
blue. Warnings and blockers override focus color with amber/red; inactive or
unavailable sources are muted. Text, tooltip, and state labels must carry the
same meaning for color-blind users.

The source rail model is used for one or more active radios. A single active
radio may use the available width, but it must still be the same chip + focus
interaction model used when more sources are activated. Activating or
deactivating a radio should not switch to a different legacy control-strip
layout.

Source rail chips must be operator-facing. Use radio short names and saved mesh
device names in menus. Raw BLE UUIDs, scan-only discoveries, adapter ids, and
debug identifiers belong in settings/details, not in the daily rail.

At minimized widths, station command cards must remain usable before they look
beautiful. The chip rail may horizontally scroll when necessary, but it should
show as many source chips as possible before scrolling. The focused card may
shorten action labels (`Timed QSY` to `Hold`, `Timed Suspend` to `Suspend`,
`Change Plan` to `Plan`) while keeping full action meaning in tooltips. QSY,
hold/suspend, resume, health, current target, and next/plan context must remain
reachable without overlapping controls.

The `Now` hero should prefer the operator-facing operating group and band, such
as `MAGNET 40M` or `S2/GHOSTNET 20M`, rather than the raw frequency. Exact
frequency, mode, and mismatch detail belong in the tooltip. If no operating
group can be inferred, fall back to frequency plus band.

Long operating group names must not expand the command bar. The hero label
elides when needed and keeps the full value in its tooltip. `S2 UNDERGROUND`
may be presented as `S2/GHOSTNET` in this control surface because that is the
more operator-recognizable label; the underlying stored group name remains
unchanged.

Each radio card uses that radio's saved frequency-plan assignment as the
authoritative source for the displayed target, next target, plan name, and QSY
option list. Runtime scheduler lanes, radio snapshots, and app-reported
frequencies are fallback or mismatch signals; they must not override another
radio's assigned plan. This prevents one FLRig, RigCtl, JS8Call, or SDR path from
leaking into another radio's card.

Every command emitted from a radio card must carry the selected
`device_profile_id` through the scheduler. The scheduler must resolve that
target to the matching runtime client or the matching configured radio endpoint
before transmitting. If no target-specific client can be resolved in a
multi-active-radio configuration, the command is skipped and surfaced as a
health/routing issue; it must not fall back to a singleton FLRig, RigCtl, or
JS8Call client. Singleton fallback is only acceptable for an unambiguous
single-active-radio compatibility path.

Targeted scheduler applies must also read actual state from the target radio's
own control context before deciding whether a frequency change is needed. A
cached singleton FLRig/JS8Call frequency is never proof that another radio is on
schedule. Scheduler de-duplication, pending-command, and latest-intent state is
radio-scoped so a queued correction for one radio cannot suppress, overwrite, or
satisfy another radio's correction.

Radio cards and radio-scoped scheduler lanes must not read or write legacy
global manual-control keys such as `schedule_suspend_until` when a
`device_profile_id` is known. Timed QSY, Timed Suspend, Indefinite Suspend, and
Resume use durable `SchedulerManualControlState` rows for the selected radio.
When the durable manual-control service exists, a missing row for a radio means
that radio is not in manual control; it must not inherit stale singleton
scheduler fields, card metadata, or legacy global hold state from another radio.
Legacy/global keys may remain only as a one-radio compatibility boundary for
older surfaces when the durable manual-control service is not available.

Scheduler startup semantics:

- Multi-rig runtime data review must use the active multi-rig runtime profile
  DB, normally `/Users/bill/RadioCode/runtime/multi-rig/config/freqinout.db`
  in Bill's test lab. Do not infer FIO-A/FIO-B state from the legacy/default
  DB at `/Users/bill/.freqinout/config/freqinout.db`; that DB may contain only
  a migrated `Default Radio` and can produce false findings during scheduler,
  endpoint, assignment, or manual-control review.
- When FIO starts and the scheduler is enabled for active radios, the scheduler
  performs one forced per-radio lane apply so every assigned plan takes control
  of its configured radio immediately.
- Manual QSY state is runtime-only. A manual QSY target from a previous FIO run
  must be cleared before startup lane apply, so launch always follows the saved
  radio-to-plan assignment rather than a stale operator override.
- That startup apply bypasses the frequency-control wait prompt because launch
  establishes the scheduler's baseline authority. It also bypasses JS8Call,
  VarAC, and FLDigi receive-busy deferrals; hard PTT/transmit protections,
  RF Guard block decisions, and radio-scoped routing still apply.
- The periodic scheduler timer starts only after this startup lane apply returns,
  so prompt evaluation cannot block launch before the assigned schedules are
  invoked.
- After startup, normal schedule ticks and user actions follow the configured
  frequency-control behavior, including Prompt, wait, activity, and manual
  control rules.
- Off-schedule prompts must be based on fresh verification against the affected
  radio's own control context. Cached or singleton status may start a suspicion,
  but FIO must re-check the targeted radio before showing a prompt; if that
  fresh radio-scoped check is on schedule, no prompt is shown.
- Intentional operator control is not an off-schedule error. Manual QSY, timed
  QSY, timed suspend, and indefinite suspend suppress off-schedule prompts only
  for the affected radio until that radio resumes schedule control.

Saving a radio profile must preserve endpoint fields for linked app instances
when those endpoint fields were not part of the user edit. A profile-name,
activation, schedule-assignment, or health-setting save must not default a linked
FLRig/FLDigi/JS8Call row back to standard ports.

Action semantics:

- `QSY`: immediately commands the selected manual QSY target and places only
  that radio into manual QSY state. Scheduled changes for that radio are
  suspended until the operator uses `Resume` or the scheduler explicitly
  transitions to a new active schedule entry.
- `Timed QSY`: commands the selected manual QSY target and applies a timed
  scheduler suspension for the selected duration. Include `Indefinite` as an
  option for operator-controlled manual duration.
- `Timed Suspend`: suspends scheduled frequency changes for the selected
  duration without changing the radio frequency. This supports manual control
  from FLRig, another app, or the radio itself. Include `Indefinite` as an
  option for manual control until the operator resumes.
- `Resume`: clears manual QSY or timed scheduler suspension and returns control
  to the active schedule for only that radio. Resume is operator-authoritative:
  it bypasses soft JS8Call, VarAC, and FLDigi receive-busy deferrals while still
  preserving hard PTT/transmit protections, RF Guard blocks, and radio-scoped
  endpoint routing.

Off-schedule prompts are part of radio health and must be radio-scoped. The
prompt title/text must identify the affected radio, and every action from the
prompt must pass that same `device_profile_id` back to the scheduler. Prompt
throttling is keyed by radio and schedule row so one off-schedule radio cannot
spam repeated prompts or suppress another radio's first actionable prompt.
`Skip Once` suppresses only the current radio and schedule mismatch until the
configured prompt interval expires; it must not suppress a different radio or a
different scheduled target.
`Resume` from this prompt is the same radio-scoped operator recovery action as
the station card and must not be blocked by FLDigi RX activity when the operator
explicitly chooses to resume.

When manual QSY is active, `Resume` must be enabled and visually highlighted.
The `QSY` button text remains stable so it does not resize or clip; button color
and the highlighted `Resume` action carry the state. If the radio is not fully
configured or has not yet reported the new frequency, the target field should
continue to show the commanded QSY target; the tooltip should disclose the
radio-reported frequency if it differs.

When timed QSY or timed suspend is active, only the button that initiated the
state should be highlighted. Its label should carry the countdown so the timed
state is visible where the operator acted. Use compact minute labels while there
is ample time left, such as `28m | Extend`. Under 10 minutes, switch to
`MM:SS | Extend`, such as `09:42 | Extend`. Restore the default button label
when the timed state expires or the user resumes the schedule. Button tooltips
should include the local resume time.

In compact layouts, retain the same card command model. `QSY`, `Timed QSY`,
`Timed Suspend`, `Resume`, and `Change Plan` may reflow or resize, but they
must not change into the older single-radio control-strip wording or share
state with another card.
