# Operational View Inventory

## Purpose

This document maps existing FIO screens into the Operational View Framework so
current and future work can be discussed as data sources, projections, and
selectable views instead of one-off pages.

The inventory is intentionally review-oriented. A view marked `Meets` is already
close to the framework and gates. A view marked `Partial` is useful but needs
explicit source/projection/gate cleanup before it becomes a reusable view. A view
marked `Legacy` should remain functional, but new work should avoid copying its
layout or data access pattern.

Code issues observed while implementing or reviewing these views are tracked in
`docs/internal/operational_view_code_issue_log.md`.

## Mandatory Gate Checklist

Every reusable view or future source/view spec must answer these gates:

- Source meaning: what native meaning must be preserved?
- Volume and retention: what volume is expected and what default limit protects
  the operator?
- Provenance and trust: how fresh, reliable, and source-specific is the item?
- Constrained customization: which page presets and user options are allowed?
- Map scaling: how does geography focus, cluster, filter, or decline to map?
- Action validity: which actions are valid, disabled, or source-specific?

Code gate:

- Reusable views must be represented in
  `freqinout/core/operational_view_registry.py`.
- Sources must be represented in `freqinout/core/source_view_contracts.py`.
- UI code may consume these declarations, but should not invent new source/view
  combinations locally.

## View Status Summary

| Area | Current Screen/View | Framework Template | Status | Priority |
| --- | --- | --- | --- | --- |
| ControlFreq | Operational Awareness | Attention Queue | Partial | P1 |
| ControlFreq | Schedule Outlook | Schedule Outlook | Partial | P1 |
| ControlFreq | Propagation Forecast | RF Readiness | Partial | P1 |
| ControlFreq | Unread Messages / BBS Files | Traffic Inbox Summary | Partial | P2 |
| Messages | Inbox | Traffic Inbox | Partial | P1 |
| Messages | Compose | Compose Workbench | Partial | P1 |
| Map | Stations / Reports Map | Map Context | Partial | P1 |
| SOP Builder | SOP and Traffic Suggestions | SOP Decision View | Partial | P2 |
| FreqPlanner | Plan Builder | Schedule/Plan Editor | Partial | P2 |
| HF Schedule | Daily Schedule | Schedule Outlook / Editor | Partial | P2 |
| Net Schedule | Net Schedule | Schedule Outlook / Editor | Partial | P2 |
| Peer Schedules | Peer Schedule Review | Schedule Outlook | Partial | P2 |
| Station Command Bar | Active Radio Cards | Station Command View | Partial | P1 |
| Station Overview | Control Center | Station Status Summary | Partial | P2 |
| Station Health | Health Details | Setup Checklist / Runtime Health | Meets | P2 |
| NCS-FLDigi/SSB | Net Control | Net Control Workspace | Specified | P2 |
| NCS-JS8 | Net Control | Net Control Workspace | Legacy | P2 |
| NCS-Local | Local NCS | Local Report / Net Control | Partial | P2 |
| Operators | HF Callsigns | Operator Directory | Partial | P3 |
| Operators | Local Callsigns | Local Operator Directory | Partial | P3 |
| Settings | Main / Radios / Protocols | Setup Checklist / Configuration | Partial | P2 |
| Help | Help | Reference | Out of Scope | P3 |

## Existing View Details

### Ops Center Operational Awareness

User-facing label: `Ops Center`. Internal route/module names may still use
`ControlFreq` for compatibility.

Template: `Attention Queue`

Current sources:

- CommStat RF reports
- FIOSpotter forms
- JS8 directed messages
- FLMsg/FLAmp files
- VarAC direct/BBS state
- SOP and schedule context
- topic extraction and pin state

Gate status:

- Source meaning: Partial. Source family is visible, but each row still uses
  compact text that can blur native differences between CommStat, Spotter, JS8,
  and file traffic.
- Volume and retention: Partial. Default list is capped, but high-volume source
  policy should be explicit before APRS, Mesh MQTT, or other streams are added.
- Provenance and trust: Partial. Freshness and RF/local/relayed confidence need a
  consistent display field.
- Constrained customization: Partial. Current view selector is useful, but
  allowable page presets need to be declared.
- Map scaling: Partial. Map handoff carries filters; zoom behavior needs
  confidence/fallback rules for grid, state, and inferred locations.
- Action validity: Partial. Read/Reply/Map/Pin actions exist, but each row should
  expose only valid actions as chips and explain disabled actions.

Near-term rule:

- Treat this as the first reusable `Attention Queue` implementation. New sources
  must feed a projection before appearing here.

Implementation note:

- ControlFreq selectable view cards are now registered in
  `freqinout/core/operational_view_registry.py`. The UI consumes registered
  labels and presets rather than keeping an independent local view list.
- The registry validates ControlFreq `activity`, `intersections`, `schedule`,
  and `propagation` views against the source contract gates.

### ControlFreq Schedule Outlook

Template: `Schedule Outlook`

Current sources:

- assigned frequency plans
- current/next scheduler state
- SOP overlays
- Peer Schedule Finder
- peer schedule hints and overlaps

Gate status:

- Source meaning: Partial. SOP actions, frequency plans, and intersections
  should remain distinguishable even when merged.
- Volume and retention: Meets for current data volumes.
- Provenance and trust: Partial. Scheduler source and SOP override/conflict state
  should be visible when relevant.
- Constrained customization: Partial. Window controls are present; presets should
  define what the view may show on ControlFreq versus plan editor pages.
- Map scaling: Mostly not applicable. Only map when an action has station,
  operator, grid, region, or route context.
- Action validity: Partial. `QSY`, `Hold`, `Compose`, and `Open SOP` should be
  shown only where supported.

Near-term rule:

- Peer Schedule Finder should feel like part of Schedule Outlook rather than a
  separate mental model. It renders compact peer rows with `Msg`, `Map`, and
  `Pin` actions instead of a wide technical intersection table.

Implementation note:

- ControlFreq schedule and peer finder tables now use row-fit sizing. Sparse
  schedule data no longer reserves a large empty panel, and peer availability
  stays visually attached to Schedule Outlook instead of behaving like a
  separate oversized block.
- ControlFreq presets are now registry-backed: `Operations`, `All`, `Traffic`,
  `Schedule`, and `Propagation`.

### ControlFreq Propagation Forecast

Template: `RF Readiness`

Current sources:

- propagation summary
- operator grid
- assigned schedule band/frequency
- peer schedule context

Gate status:

- Source meaning: Partial. The summary is useful, but generic propagation should
  not override schedule-known guidance.
- Volume and retention: Meets for current forecast shape.
- Provenance and trust: Gap. Forecast age, source, and confidence should be
  shown in details.
- Constrained customization: Partial. Details should remain opt-in.
- Map scaling: Future. Path/map drill-down should be added only when the forecast
  has meaningful geography.
- Action validity: Partial. `Forecast Details` is valid; future actions should be
  source-aware.

Near-term rule:

- Keep the default compact. Details should size around content and never force
  uncomfortable empty space.

Implementation note:

- The propagation forecast now remains compact by default. `Forecast Details`
  toggles the detailed table, and the card resizes after refresh/toggle so
  hidden details collapse back to the RF readiness summary.

### Messages Inbox

Template: `Traffic Inbox`

Current sources:

- FLMsg/FLAmp staged and received files
- JS8 directed traffic
- VarAC direct and BBS files
- FIOSpotter drafts/imported data
- CommStat RF messages
- local reports where integrated

Gate status:

- Source meaning: Partial. Native open/read behavior exists for several sources,
  but inbox projection should preserve native fields before rendering.
- Volume and retention: Partial. Filtering exists; indexing and visible row caps
  need to be explicit for large datasets.
- Provenance and trust: Partial. Source family is visible; freshness and trust
  labels should be standardized.
- Constrained customization: Partial. Source filters are useful, but user-chosen
  view modules are not yet formalized.
- Map scaling: Partial. Message-to-map filters exist for some paths and should
  use the shared filter contract everywhere.
- Action validity: Partial. Read/reply/map/open-source actions should be
  validated by projection.

Near-term rule:

- Convert message rows to a reusable `MessageSummary` projection before adding
  APRS, Mesh MQTT, MeshCore, or other high-volume sources.

Implementation note:

- `freqinout/core/message_summary.py` now defines the first reusable
  `MessageSummary` projection for Messages, ControlFreq, Map, and Compose
  handoffs. `MessageViewerTab` attaches summaries to built rows and exposes
  `message_summaries(...)` so follow-on views can consume source family,
  provenance, retention, severity, and action-validity fields without parsing
  rendered table text. The visible Messages table remains legacy-rendered until
  the next migration slice.
- Messages navigation now accepts normalized source families such as
  `fiospotter` and `commstat_rf`, plus `grid_filter`, so ControlFreq and Map
  `Read` handoffs can land on a correctly focused inbox instead of relying on
  broad search text.

### Messages Compose

Template: `Compose Workbench`

Current sources:

- compose intent from navigation and ControlFreq
- FLMsg/FLAmp forms and staging
- JS8 direct message target and signing
- FIOSpotter forms and Expect/BBS workflows
- CommStat RF forms and brevity
- BBS and message folder destinations

Gate status:

- Source meaning: Partial. Compose modes are source-specific, which is correct,
  but the shared compose intent should be the formal projection.
- Volume and retention: Mostly not applicable. Draft persistence and reset are
  the relevant lifecycle requirements.
- Provenance and trust: Partial. Signing and key availability should be visible
  only when meaningful.
- Constrained customization: Meets directionally. Common choices use chips;
  advanced destinations remain constrained.
- Map scaling: Not applicable except when replying to mapped traffic.
- Action validity: Partial. Stage/send/save actions should be enabled only when
  prerequisites are satisfied.

Near-term rule:

- Do not add new compose modes directly to layout code. Add a compose intent and
  a source-specific panel that follows the common workbench contract.

### Map

Template: `Map Context`

Current sources:

- stations/operators
- grids and states from traffic
- local reports
- regional intelligence
- schedule/plan context
- topic and source filters

Gate status:

- Source meaning: Partial. Map layers represent different concepts and should
  keep their native meaning in the inspector.
- Volume and retention: Partial. Future APRS/Mesh sources require clustering,
  culling, and retention before default rendering.
- Provenance and trust: Partial. Report source and freshness should be visible
  in map detail.
- Constrained customization: Partial. Layers and filters exist; presets need to
  define safe defaults.
- Map scaling: Partial. The map is the scaling surface; auto-fit and filter
  rules need to be source-specific.
- Action validity: Partial. Back-links to Messages, Compose, SOP, and
  ControlFreq should be consistent.

Near-term rule:

- A map handoff must carry the most specific available geometry and fall back
  visibly when only state, region, group, or topic is known.

### SOP Builder

Template: `SOP Decision View`

Current sources:

- SOP profiles and actions
- condition levels
- schedule overlays
- traffic suggestions
- groups/topics from messages

Gate status:

- Source meaning: Partial. SOP rules and traffic suggestions should remain
  separate but linked.
- Volume and retention: Meets for current policy data; traffic suggestions need
  caps if fed by high-volume sources.
- Provenance and trust: Partial. Suggestions need source/freshness/confidence.
- Constrained customization: Partial. SOP editing is constrained; view selection
  presets are not formalized.
- Map scaling: Partial. SOP actions may have geography and should use the map
  filter contract.
- Action validity: Partial. Suggested actions must clearly state what FIO can do
  automatically versus what the operator must confirm.

Near-term rule:

- SOP should consume projected traffic signals, not raw inbox rows.
- The existing SOP action-row table is a temporary advanced compatibility view.
  Remove it after the card-based `SopActionBuilder` reaches parity, with tests
  proving saved SOP payloads, RF Guard checks, and ControlFreq preview remain
  unchanged.
- SOP Builder should be redesigned around view contracts rather than a single
  wide action-row table:
  - `SopProfileSelector`
  - `SopContextSummary`
  - `SopSuggestionQueue`
  - `SopActionBuilder`
  - `SopScheduleImpact`
  - `SopConflictReview`
  - `SopPreview`
- SOP Builder must preview the operator-facing ControlFreq guidance that will be
  produced by the selected SOP.
- SOP profiles may be grouped by operating group/category and may be linked to
  different assigned plans/radios. The selected radio/group context should shape
  suggestions and conflict checks without hiding other affected assignments.

### Plan Builder And Schedule Editors

Templates: `Schedule Outlook`, `Plan Builder`, `Schedule Source Editor`

Current screens:

- Plan Builder
- HF Daily Schedule
- Net Schedule
- Peer Schedules

Gate status:

- Source meaning: Partial. Schedules, nets, peer availability, and operating
  models should remain distinct.
- Volume and retention: Meets for current row counts.
- Provenance and trust: Partial. Imported peer schedule age/source should be
  visible.
- Constrained customization: Partial. Editors are constrained; selectable views
  should be limited to schedule/plan-compatible templates.
- Map scaling: Mostly not applicable unless schedules include route/region/net
  geography.
- Action validity: Partial. QSY/assign/resolve actions should stay RF Guard
  gated.

Near-term rule:

- Reuse Schedule Outlook projections in ControlFreq instead of duplicating
  schedule summaries.
- Use `Plan Builder` as the user-facing name for FreqPlanner.
- HF Daily and HF Nets should share a source-first layout: source selector and
  usage context first, editable schedule table second, reusable row library
  third.
- Plan Builder should keep the projected table dominant. Ingredient/source cards
  should become compact when they push the schedule table out of the primary
  viewport. Use a horizontally scrollable ingredient strip with stable chip
  widths so minimized windows do not clip cards into the table. Do not render a
  duplicate Daily/Nets/SOP text summary below the ingredient strip.
- Plan Builder table-view controls must sit immediately above the projected
  table they control. Render those controls as a compact toolbar that can scroll
  horizontally when necessary; do not use a tall framed hint panel above the
  table. Selected-window edit details belong below the table unless the user is
  actively editing that window. View-specific controls should appear only in
  views that use them.
- SOP Builder must keep `Add Action Row` visible in the action-builder header.
  The advanced table is a temporary compatibility editor and must remain clearly
  labeled until card editing fully replaces it. The builder body must be
  scrollable on laptop/minimized windows.

### Station Command Bar

Template: `Station Command View`

Current sources:

- configured radios
- runtime health
- active schedule lane
- RF Guard status
- QSY candidates
- scheduler hold/suspend state

Gate status:

- Source meaning: Partial. Radio short names are primary; detailed descriptions
  belong in tooltips or setup.
- Volume and retention: Meets for current expected radio count, with scroll for
  overflow.
- Provenance and trust: Partial. Runtime source health and stale state need clear
  labels.
- Constrained customization: Meets directionally. This is not user-composable;
  it is a fixed high-use command surface.
- Map scaling: Not applicable.
- Action validity: Partial. QSY/hold/resume/health actions are guarded, but
  disabled reasons must remain visible at compact widths.

Near-term rule:

- Show as many radio cards as fully fit, then scroll. Never clip a configured
  radio card when horizontal room exists.

Implementation note:

- MainWindow station-command cards now compute width from the available summary
  viewport and rebuild when the shell width changes. Ordinary two-radio setups
  should fit before horizontal overflow appears; page controls remain hidden so
  they cannot escape as blank `Prev` windows.

### Station Overview

Template: `Station Control Center View`, `Station Status Summary`

Current sources:

- station runtime
- control status
- scheduler state
- setup and health summaries

Gate status:

- Source meaning: Meets directionally. `Overview` provides cross-source status;
  each active source gets its own short-name tab.
- Volume and retention: Meets for source count. Adding radios, SDRs, or future
  connections creates tabs instead of one long detail scroll.
- Provenance and trust: Partial.
- Constrained customization: Partial. Source tabs are generated from active
  sources; user-selectable subviews can be layered later through the registry.
- Map scaling: Partial. Current tabs expose capability state; future traffic/map
  handoffs must use source-aware map filters.
- Action validity: Partial.

Near-term rule:

- Keep `Overview` first, then one tab per active source. Use warning/error
  markers in tab labels so the operator can find trouble without scanning a
  long page.

### Station Health

Template: `Setup Checklist`, `Runtime Health`

Current sources:

- dependency/runtime checks
- radio software responsiveness
- scheduler log
- setup readiness

Gate status:

- Source meaning: Meets.
- Volume and retention: Meets for current logs when bounded.
- Provenance and trust: Meets directionally.
- Constrained customization: Meets.
- Map scaling: Not applicable.
- Action validity: Meets directionally; related settings links are valid.

Near-term rule:

- This is a good model for setup and runtime health views: short status,
  actionable detail, and direct remediation links.

### NCS Workspaces

Template: `Net Control Workspace`

Current screens:

- NCS-FLDigi/SSB
- NCS-JS8
- NCS-Local

Gate status:

- Source meaning: Partial for Local, Legacy for FLDigi/JS8. Native net-control
  semantics are strong, but projection boundaries are older. FLDigi/SSB now has
  a dedicated redesign contract in `docs/internal/fldigi_ncs_workbench_spec.md`.
- Volume and retention: Partial. Rosters and check-ins are manageable today.
- Provenance and trust: Partial. ANCS/relay/source provenance should be explicit.
- Constrained customization: Partial. These are role workspaces, not generic
  views.
- Map scaling: Partial for local/operator reports.
- Action validity: Partial. Macro/send/check-in actions should remain
  protocol-aware and disabled when unavailable.

Near-term rule:

- Do not force NCS into generic templates. Extract reusable projections for
  roster, traffic, relay, and local report signals where they feed ControlFreq,
  Messages, Map, or SOP.
- FLDigi/SSB must be treated as a role-scoped live net cockpit before it is
  surfaced in Ops Center. Its first reusable output is `NcsSessionSnapshot`;
  Ops Center may consume session state and accepted aggregate counts, but must
  not inspect FLDigi widgets or raw log-assisted candidates.

### Operator Directories

Template: `Operator Directory`

Current screens:

- HF Callsigns
- Local Callsigns

Gate status:

- Source meaning: Partial. HF operator history and local operators are distinct.
- Volume and retention: Partial. Search/filter is sufficient now; indexing may
  matter with imported directories.
- Provenance and trust: Partial. Imported/manual/source age should be visible.
- Constrained customization: Partial.
- Map scaling: Partial where operator grid/location is known.
- Action validity: Partial. View reports, edit, import/export, map, and compose
  actions should be projection-valid.

Near-term rule:

- Treat operator rows as identity/contact projections that can feed Messages,
  Map, NCS, and Compose.

### Settings

Template: `Setup Checklist`, `Configuration`

Current sources:

- application preferences
- radio profiles
- software paths and ports
- VarAC/BBS configuration
- JS8/FIOSpotter/CommStat setup
- GPG and MsgAuth keys
- operating groups and local nets
- condition alerts and custom tools

Gate status:

- Source meaning: Partial. Settings sections preserve meaning, but setup status
  should increasingly flow through checklist projections.
- Volume and retention: Partial. Configuration row counts are bounded; audit
  histories and imports need caps.
- Provenance and trust: Partial. Detected versus user-entered configuration
  should be clearly labeled.
- Constrained customization: Meets directionally. Settings is intentionally
  constrained by section.
- Map scaling: Not applicable except for configured grids/regions.
- Action validity: Partial. Prepare/apply/write/test actions must remain guarded.

Near-term rule:

- New protocol setup should add source contracts and setup checklist items, not
  only raw settings panels.

### Help

Template: Reference

Gate status:

- Out of scope for operational source/view gates unless Help begins rendering
  live operational data.

## Source Families Already Represented

| Source Family | Existing Views | Projection Maturity | Notes |
| --- | --- | --- | --- |
| FLMsg/FLAmp | Messages, Compose, ControlFreq, Map | Contracted | Source/view contract preserves form, staging, RF-file provenance, map hints, and compose/native actions. |
| JS8 | Messages, Compose, ControlFreq, NCS-JS8, Settings | Contracted | Source/view contract preserves SNR/offset/directed/auth semantics and source-aware read/reply/map actions. |
| FIOSpotter | Compose, ControlFreq, Settings, Messages-adjacent | Contracted | Source/view contract distinguishes FIO Spotter forms, Expect state, BBS destination, auth, and mapped report fields. |
| CommStat RF | Compose, ControlFreq, Messages-adjacent | Contracted | Source/view contract preserves StatRep/brevity/scope/report-id meaning and source-aware compose/map actions. |
| VarAC Direct/BBS | Messages, Compose, Settings, ControlFreq | Contracted | Source/view contract preserves store-and-forward/BBS context, RF provenance, and native open/read capability. |
| Local Reports | Local NCS, Local Operators, Map, Messages-adjacent | Contracted | Source/view contract preserves verified local/NCS/operator context and maps through grid/lat-lon/state. |
| Schedules/SOP | ControlFreq, Plan Builder, HF Schedule, Net Schedule, SOP | Partial | Strong domain model; reusable outlook projection should be the bridge. |
| Station Runtime | Station Command Bar, ControlFreq, Health, Settings | Contracted | `StationCommandRadio` defines short-name title, card context, and command actions. |
| Propagation | ControlFreq | Partial | Needs source/freshness/confidence detail before expansion. |
| Operators | Operators, NCS, Map, Compose-adjacent | Partial | Identity/contact projection would improve cross-links. |
| MeshCore | Future | Contracted | Priority future source; requires rollups, provenance, map clustering, and attention/inbox/map/compose views. |
| Mesh MQTT | Future | Contracted | Bridge/internet trust must be explicit; default views require rollups and clustering. |
| APRS | Future | Contracted | Map-first high-volume source; reply/compose disabled by contract, clustering and marker caps required. |
| Reticulum/LXMF | Future | Contracted | Store-and-forward identity context enters through inbox/operator/map/compose projections. |

## Current Source Priority

Future radio/data integrations should be evaluated in this order unless field
testing changes the priority:

1. `MeshCore`
2. `Mesh MQTT`
3. `APRS`
4. `Reticulum/LXMF`

Rationale:

- MeshCore is first because it most directly supports offline operator
  coordination, room-style traffic, and network-building behavior.
- Mesh MQTT is second because it can bridge mesh telemetry and trusted topics
  into FIO, but it needs stronger trust and topic-scope controls.
- APRS is third in product priority but should be used deliberately as a volume
  and map-scaling stress test because positions, objects, weather, and packets
  can become noisy quickly.
- Reticulum/LXMF is fourth because store-and-forward messaging is valuable, but
  it should enter through the same adapter/projection/capability boundary as the
  other sources.

Default retention rule:

- Default operational traffic views emphasize the last seven days.
- Older data is hidden from default views unless attached to an active event
  storyline, such as wildfire growth, weather escalation, civil unrest, logistics
  planning, or another incident that benefits from trend history.
- Drill-down and archive search may expose older data, but default dashboards
  should not make old traffic compete with recent operational signals.

Default attention rule:

- Severity drives how much provenance is visible.
- Low-severity items may show compact source/age badges.
- Urgent, conflicting, stale-but-important, relayed, or low-confidence items must
  show source, age, trust/confidence, and geography more explicitly.
- Direct messages are important because they build the offline operator network,
  not only because they contain formal incident content.

Default geography rule:

- Proximity to the operator, assigned group, route, station, affected area, or
  event geography should influence attention ranking and map focus.
- Map actions should prefer the most specific known geometry. If a row only has
  broad state/region/topic context, FIO should make that uncertainty visible.

## Recommended Next View Candidates

### Traffic Inbox View

Build a reusable `Traffic Inbox` view contract around `MessageSummary`.

Reasons:

- It directly supports Messages, ControlFreq handoffs, Map handoffs, and Compose
  replies.
- It is where high-volume future sources will first create stress.
- It gives APRS/Mesh/MQTT a safer path into FIO without becoming noise.

### Map Context View

Formalize map filter and auto-fit behavior as a view contract.

Reasons:

- ControlFreq `Map` actions already depend on it.
- Future APRS/Mesh sources will require clustering and culling.
- Operators need confidence that "Map" means "show the relevant place," not
  "open a broad map and make me search again."

Implementation note:

- `freqinout/core/view_contracts.py` now includes `MapContextFilter`, which
  normalizes group, topic, query, source family, state, grid, region, age, and
  concern flags before handoff. ControlFreq and Map report focus paths now route
  through this contract.

### Compose Intent View

Formalize compose navigation as `ComposeIntent`.

Reasons:

- Replies from Messages, Map, ControlFreq, and SOP should all land in the right
  compose mode.
- New source families should not add ad hoc compose setup code.
- Draft persistence and reset behavior become testable.

Implementation note:

- `freqinout/core/view_contracts.py` now includes `ComposeIntent`,
  `ScheduleWindow`, `RfReadiness`, and `SetupChecklistItem`.
  `MainWindow.open_messages_section`, `MessageViewerTab.prefill_compose_intent`,
  ControlFreq reply actions, and Map compose-from-station paths normalize
  compose payloads through this contract.

### Schedule, RF Readiness, And Setup Projections

Formalize the non-traffic operational views as small source-neutral contracts.

Reasons:

- Schedule Outlook, Schedule Intersections, Propagation Forecast, and setup
  review banners should follow the same source/projection/view rules as traffic.
- Sparse schedule and propagation data should size around current rows instead of
  reserving large blank panels.
- Setup items need explicit required/blocking semantics before they drive
  attention queues or station health.

Implementation note:

- `ScheduleWindow` provides a compact schedule headline and navigation context.
- `RfReadiness` provides a compact fallback recommendation when full forecast
  details are hidden or unavailable.
- `SetupChecklistItem` marks required incomplete items as operational blockers.

### Station Command View

Formalize the top control bar as a source-backed command view.

Reasons:

- It is always visible and high-risk.
- Multi-radio display and compact-width behavior need stable rules.
- RF Guard, schedule, health, and runtime state all converge there.

Implementation note:

- `freqinout/core/view_contracts.py` now includes `StationCommandRadio`.
  MainWindow station-command snapshot labels route through the contract so card
  titles use the radio short name and long generated descriptions do not become
  primary command labels.
- The top bar renders all active command-capable sources as compact short-name
  chips, then renders one focused command card for the source that needs action.
- Focus promotion order is critical health/blocker, active NCS/net, imminent
  QSY, active send/transfer, direct or high-severity traffic, manual user focus,
  normal primary, then all-clear fallback.
- Off-focus warnings must remain visible in the chip rail so a source with an
  issue is not hidden behind horizontal scrolling.

## Review Questions For Future Product Direction

- Which views should be user-selectable on ControlFreq versus fixed by role?
- Which view controls should be exposed first: show/hide, ordering, density,
  filters, or saved focus presets?
- What exact severity scale should drive provenance visibility?
- Which older event storylines should remain visible by default, and who decides
  when an event is still active?
- When a map handoff has only a state or region, should FIO zoom to that area or
  open a filtered list first?
- Which view presets should be available to a new user on first install?
