# Operational View Framework Spec

## Purpose

FIO should treat each data source, or useful combination of sources, as input to
selectable operational views. The view is responsible for rendering that data in
the most useful form for the operator.

See `docs/internal/operational_view_inventory.md` for the current inventory of
existing screens mapped into this framework and gate structure.

This keeps FIO from becoming a collection of hand-built, one-off tab layouts.
New development should add data projections and view templates that can be used
where they fit, rather than designing every page from scratch.

The product goal is not a generic dashboard builder. FIO remains an opinionated
radio operations tool. Users can choose useful views, but each view has a clear
operational purpose, tuned layout rules, and performance expectations.

All source, projection, and view work must satisfy the UI Responsiveness
Contract in `docs/internal/ui_layout_standards.md`. A projection may ingest or
summarize high-volume feeds such as MeshCore, APRS, Mesh MQTT, or Reticulum/LXMF,
but it must do so with bounded queries, coalesced updates, stale-result guards,
and non-blocking UI handoff. Rendering more data is not useful if it makes the
operator wait for the event loop to recover.

Current product defaults:

- Future source priority is `MeshCore`, `Mesh MQTT`, `APRS`, then
  `Reticulum/LXMF`. MeshCore leads because it best fits the offline operator
  network/social-fabric use case. APRS remains the likely stress test for map,
  retention, and high-volume projection rules.
- Attention and trust display should be driven by severity. Low-risk items can
  keep provenance in compact badges or hover/detail text; urgent or conflicting
  items should show source, age, confidence, and geography more visibly.
- Recency is the default traffic value. Most traffic older than seven days should
  fall out of default views unless it belongs to an active event storyline, such
  as a growing wildfire, weather warning, civil unrest pattern, or other
  planning-relevant incident.
- ControlFreq should remain reasonably opinionated around emergency-management
  best practices. Operators may control visibility, filters, density, pins, and
  focus, but should not need to rebuild the page to get a useful default view.
- Direct messages are operationally important even when they are informal. They
  help build the offline operator network, identify who is active, and provide
  social/coordination context.
- Geography frames importance. Proximity to the operator, assigned group, route,
  or affected area should influence ranking, map focus, logistics prompts, and
  SOP relevance.

## Core Model

FIO UI should be built from three separable concepts:

- `Source`: where data comes from.
- `Projection`: normalized data prepared for display and action.
- `View`: a reusable presentation and interaction template.

Sources must not own page layout. A source can feed many views. A view can blend
many sources when that helps the operator answer a real question.

Examples:

- JS8 direct messages, VarAC direct messages, and FLMsg files can all feed a
  `Traffic Inbox` view.
- Spotter, CommStat RF, Local Reports, RF pins, and condition alerts can feed an
  `Operational Awareness` view.
- SOP actions, frequency plans, peer schedules, and propagation can feed a
  `Schedule Outlook` or `RF Readiness` view.
- Message topics, grids, states, callsigns, and groups can feed a `Map Context`
  view.

## Source Contract

Every new source should declare:

- identity: source family, source reference, stable item id when available
- time: received time, event time, freshness
- actor: from callsign, to target, group/net, radio short name if applicable
- geography: grid, state/province, FEMA region, lat/lon when known
- content: subject, summary, raw body/reference, form type
- topics: normalized topic labels
- severity/attention: priority, status, concern flags
- actions: read, reply, map, compose, pin, tune, open source, acknowledge

The source should provide this through a projection-friendly structure before it
reaches a Qt widget. UI code should not parse raw message bodies, filenames, or
forms when a core projection can do it.

## Projection Rules

Projections are the bridge between raw data and display. They should:

- be Qt-free
- be cheap to test
- normalize source-specific names into common fields
- carry enough context for cross-linking
- be cached or query-indexed when data volume may grow
- preserve source-specific metadata for drill-down

Projection examples:

- `SituationSummary`
- `AttentionItem`
- `NeedSummary`
- `IncidentStoryline`
- `MessageSummary`
- `ScheduleWindow` for schedule rows, outlook rows, and schedule-driven handoffs
- `RfReadiness` for compact propagation/readiness guidance plus optional detail
- `MapReport`
- `SetupChecklistItem` for setup banners and readiness blockers
- `ComposeIntent`
- `StationCommandRadio` for always-visible radio command cards

Projection objects should answer these questions:

- What is this?
- Why does it matter?
- How fresh is it?
- Where is it?
- Who is involved?
- What should the operator do next?

For blended operational views, projections should also answer:

- Is this human/operator traffic, source telemetry, or infrastructure state?
- Is this part of a larger incident/storyline?
- Does it describe an open need, a handled need, or general awareness?
- What confidence should FIO show for the topic, location, and source?
- Which tab owns the authoritative drill-down?

## Standard View Templates

### Situation Summary

Purpose: summarize what FIO believes is happening now and what the operator
should do next.

Inputs:

- ranked traffic and direct messages
- NCS/event log entries
- needs for help, relay, welfare, logistics, communications, medical, power,
  water, shelter, or similar action
- incident/storyline clusters
- map/location projections
- SOP and schedule projections
- source health only when it affects current operation

Required behavior:

- produce one plain-language headline that is useful without opening another tab
- show 1 to 3 next actions with route-contract-backed destinations
- separate `open`, `handled`, `watching`, `stale`, and `false-positive` items
- preserve links to the underlying evidence in Messages, Map, NCS, SOP Builder,
  Plan Builder, or source-specific detail
- summarize by meaning, not source mechanics; for example, "two welfare messages
  unanswered" is better than "two rows in the JS8 table"
- never promote protocol telemetry, node advertisements, or router chatter as
  human traffic unless a source contract explicitly marks it as operator content
- accept operator corrections such as topic change, mute pattern, pin storyline,
  mark handled, or attach item to incident
- remain useful when only routine/social traffic exists by saying so clearly
  without creating false urgency

### Incident Storyline

Purpose: keep related reports understandable over time without requiring a full
incident-management workflow.

Inputs:

- topic/category
- geography
- recency and trend
- involved callsigns/nodes/sources
- direct messages, reports, NCS logs, map pins, and manual operator notes

Required behavior:

- cluster related observations by topic, place, actors, and time
- support state: new, active, watching, handled, stale, muted, false-positive
- surface trend signals such as "more reports", "new area", "no update", or
  "handled"
- link to map focus, message evidence, compose reply/update, and SOP review
- retain enough provenance that the user can tell whether the story came from
  RF, mesh, imported data, manual NCS notes, or mixed evidence

### Needs Tracker

Purpose: make requests for help, relays, welfare checks, resources, and
operator follow-up visible until they are handled.

Inputs:

- inbound messages and forms
- NCS/event log entries
- operator-created notes
- SOP suggestions
- map/context reports

Required behavior:

- show open needs prominently in Ops Center and relevant detail views
- show recently handled needs briefly so the operator sees progress
- support statuses: open, relayed, acknowledged, handled, stale, cancelled
- carry category, severity, requested-by, assigned-to, location, and last-update
  context when known
- route to authoritative detail and avoid duplicating a separate task system

### Attention Queue

Purpose: show what needs review now.

Inputs:

- direct messages
- high-value topics
- condition alerts
- CommStat status
- Spotter forms
- RF pins
- local reports

Required behavior:

- rank by directness, severity, freshness, assigned group, and pins
- show compact rows or cards with priority, source, focus, and actions
- use action chips/buttons for `Read`, `Reply`, `Map`, `Pin`
- action chips must call the shared view-context projection rather than parsing
  rendered row text
- every rendered cross-tab action must have an automated route-contract test
  proving that the visible label opens the expected destination with the
  expected context filters. Examples: `Inbox` opens Messages Inbox, `Reply`
  opens Messages Compose with a compose intent, `Map` opens the map with source
  filters, and Local Reports traffic opens Local Reports rather than the HF
  inbox.
- support topic/callsign/group filtering
- never require reading a long table to see the top issue

### Traffic Inbox

Purpose: scan received traffic across protocols.

Inputs:

- FLMsg/FLAmp
- JS8Call
- VarAC
- BBS
- FIOSpotter
- CommStat RF
- Local Reports

Required behavior:

- support source-family filters
- preserve native message access
- support topic, callsign, group, age, concern, and geography filters
- open Compose with a source-appropriate reply intent
- open Map when geography is known or inferable

### Schedule Outlook

Purpose: show where the operator should be and what is coming next.

Inputs:

- frequency plans
- SOP layers
- peer schedules
- scheduler state
- schedule intersections

Required behavior:

- combine schedule intersections with the schedule timeline
- size tables around visible rows
- sparse schedule/intersection data must not reserve large empty panels
- schedule/intersection horizon labels must be real controls when the operator
  can change the time window, and their labels must make their scope clear
- use compact action chips such as `QSY`, `Hold`, `Compose`
- keep the current/next state readable at Large text
- expose details without overwhelming the default view

### RF Readiness

Purpose: summarize band/path guidance.

Inputs:

- propagation model
- assigned schedules
- peer schedules
- operator grid
- radio capabilities and health

Required behavior:

- default to a compact recommendation
- show detailed tables only on demand
- details must fit current rows and return the default view to a compact height
  when hidden
- prefer schedule-known guidance over generic propagation
- include confidence/freshness where useful
- link to map/path views for drill-down

### Map Context

Purpose: show where relevant traffic, stations, or plans apply.

Inputs:

- station locations
- report grids/states/lat-lon
- regional intelligence
- paths and heard links
- selected traffic context

Required behavior:

- accept group, topic, callsign, query, state, grid, and region filters
- auto-fit focused result sets without fighting manual map navigation
- show selected detail with links back to Messages, Compose, SOP, and ControlFreq
- avoid showing broad national views when a row action has a specific location

### Compose Workbench

Purpose: create outbound traffic cleanly.

Inputs:

- compose intent
- selected source family
- radio short name
- destination/group
- form metadata
- BBS/staging target

Required behavior:

- use source-appropriate compose mode
- persist draft state while moving between main and full workbench
- offer reset/cancel
- use compact chips for radio and common choices
- remain scroll-safe at Large text and minimized widths

### Station Command View

Purpose: keep configured radios directly controllable from the shell.

Inputs:

- configured radio profiles
- runtime radio snapshots
- frequency plan assignments
- scheduler/hold state
- health summary

Required behavior:

- use the user-defined radio short name as the primary card title
- render commandable radios as compact status chips so 1-5 active radios remain
  visible at a glance
- render two full radio command cards by default when exactly two radios are
  active and the viewport can fit both at a usable width
- render high-volume source families, including Mesh and future APRS-style
  feeds, as aggregate source controls with saved endpoint actions instead of
  one chip per discovered or retained endpoint
- render one focused command card when three or more radios are active, or when
  the viewport cannot fit two usable radio cards
- use green for the active clear radio, blue for clear available radios, amber
  for warning, red for blocker/error, and muted styling for inactive/unavailable
  sources; never rely on color alone
- promote focus by operational need: critical health/blocker, active NCS/net,
  imminent QSY, active send/transfer, direct or high-severity traffic, manual
  user focus, normal primary, then all-clear fallback
- show off-focus issues in the chip rail so a problem on a source outside the
  focused card is still visible without scrolling
- keep QSY, hold/suspend, resume, health, current target, and next/plan context
  reachable from the focused card without overlapping controls
- use horizontal overflow only for the compact chip rail when the configured
  source count exceeds available width

### Station Control Center View

Purpose: provide the deeper operational workspace for every active radio, SDR,
and future connection without making the user scan one long scrolling page.

Inputs:

- runtime source snapshots
- source family/view contracts
- health and setup summaries
- assigned operating profiles and schedule state
- traffic and map action availability

Required behavior:

- render an `Overview` tab first for cross-source status, attention, and quick
  comparison
- render one tab per active source using the user-defined short name, such as
  `FIO-A`, `FIO-B`, `SDR-1`, `Mesh`, or `APRS`
- surface warning/error state in the source tab label so an off-screen source
  can ask for attention without requiring vertical scrolling
- keep each source tab bounded and scrollable independently; scrolling inside
  one source must not hide the Overview or other source tabs
- use the source/view contract to decide which cards are allowed in each source
  tab; radios may show tune/control/runtime details, observers may show
  receive/follow details, and future Mesh/APRS/Reticulum tabs may show ingest,
  traffic, map, and attention views
- keep the top station command bar as the compact action rail and use Station
  Control Center for deeper detail, not duplicate long command cards there

### Setup Checklist

Purpose: show configuration problems in context.

Inputs:

- dependency status
- radio configuration
- directories
- keys/auth state
- source availability

Required behavior:

- show required vs optional setup clearly
- link directly to the setting that fixes the issue
- do not occupy primary operational space after setup is healthy
- separate live dependency health from setup readiness and on-demand helpers:
  a control endpoint, ingest feed, or active gateway that is unreachable may be
  unhealthy; a built-in view or helper application that is simply not running
  must read as available/idle unless the user explicitly configured it as a
  required live dependency
- use plain action language in health guidance: `Fix` for blockers, `Review`
  for degraded optional setup, and `Healthy` when required runtime paths are
  ready
- each actionable health item must route to the FIO screen where the user can
  fix it, preferably with radio/source focus already applied

## Page-Level Composition

Each tab should declare which views it supports. The page owns navigation and
workflow context; the view owns display, filtering, and local actions.

The code-level declaration lives in
`freqinout/core/operational_view_registry.py`. New reusable views must be added
there before a tab exposes them. The registry records:

- stable view key and user-facing label
- standard template category
- default tab and whether the view can be user-selected
- allowed source families
- mandatory gates the source/view pair must pass
- supported action kinds
- default row/volume limits

This registry is intentionally small. It is not a dashboard-builder DSL; it is a
guardrail that keeps high-volume future sources such as MeshCore, Mesh MQTT,
APRS, and Reticulum/LXMF from entering the UI through one-off widget code.

Suggested page support:

- ControlFreq: Attention Queue, Schedule Outlook, RF Readiness, Setup Checklist
- Messages: Traffic Inbox, Compose Workbench, Map Context handoffs
- Map: Map Context, Attention Queue drill-down, RF Readiness overlay
- SOP Builder: Schedule Outlook, Setup Checklist, Traffic/SOP suggestions
- Settings: Setup Checklist, form/detail editors
- Station Health: Setup Checklist, radio status, RF Readiness diagnostics

Pages may offer presets such as:

- `Operations`
- `Traffic`
- `Schedule`
- `RF Planning`
- `Setup`
- `All`
- `Custom`

Custom selection should remain constrained to the view types a page explicitly
supports.

Current registered ControlFreq view keys:

- `activity`: Attention Queue
- `intersections`: Schedule Outlook subset
- `schedule`: Schedule Outlook
- `propagation`: RF Readiness

Current registered cross-tab reusable views:

- `traffic_inbox`
- `compose_workbench`
- `map_context`
- `station_command`
- `setup_checklist`
- `operator_directory`

## Filter Contract

Filters should be portable between views when possible:

- source family
- callsign
- group/net
- topic
- age/freshness
- concern/severity
- state/province
- FEMA region
- grid
- free-text query
- radio short name

When a user clicks from one view to another, carry the most specific available
filter context. For example:

- `Read` from ControlFreq opens Messages filtered to source/callsign/topic/group.
- `Reply` opens Compose with source family and target prefilled.
- `Map` opens Map filtered to topic/group and zoomed to grid/state/lat-lon when
  available.
- `QSY` carries radio short name, group, band, and frequency.

Views should visually show active filters and provide an obvious way to clear
them.

## Layout Rules

All view templates must follow the shared UI layout standards and these
additional rules:

- Large text must remain usable without clipped controls.
- Tables should size around useful rows, not consume empty vertical space.
- Scroll should appear on the container that overflows, not as random page-level
  scroll caused by inflated panels.
- Common binary or short-choice actions should be chips/buttons, not dropdowns.
- Long lists and details should be opt-in, collapsible, or drill-down.
- Radio labels in operational views should use user-defined short names unless a
  full description is explicitly needed for setup/troubleshooting.
- Empty states should be useful and short.

## Performance Rules

Views should be optimized independently of sources:

- use indexed queries for large message/report sources
- cache normalized projections with clear invalidation keys
- avoid rebuilding unchanged widgets where practical
- avoid parsing raw files/forms on every repaint
- do heavy data work outside paint/layout paths
- debounce search/filter refreshes
- cap default visible row counts
- make drill-down views pay the cost of expensive details, not default screens

Suggested targets:

- filter response: under 250 ms for normal local datasets
- default tab switch: under 500 ms
- heavy refresh: under 2 seconds with progress/status feedback
- no large blocking filesystem scans during initial view render

## Mandatory Design Gates

Every future source, projection, view, or page-level spec must explicitly address
these gates before implementation. It is acceptable to say a gate is not
applicable, but the spec must say why.

### Source Meaning

Do not flatten different radio technologies into a generic "message" shape when
that hides useful meaning. APRS positions, APRS objects, APRS weather, Mesh MQTT
telemetry, MeshCore rooms, Reticulum/LXMF messages, Winlink-like traffic, BBS
files, JS8, VarAC, Spotter, and CommStat can share a common envelope, but native
meaning must remain available for drill-down and source-appropriate actions.

Required spec answers:

- What source-specific fields must be preserved?
- Which fields fit the common envelope?
- What details are only shown in drill-down?

### Volume And Retention

High-volume sources must not be treated as always-visible traffic. APRS, Mesh
MQTT, telemetry, and relay/bulletin style systems can produce enough data to
overwhelm both the operator and the UI.

Required spec answers:

- What is the expected data volume?
- What indexes, rollups, and retention limits are required?
- What is hidden by default?
- What visible row/card limit protects the default view?

### Provenance And Trust

Operational awareness must show how reliable and fresh a data item is. Mixed
sources can look authoritative even when data is stale, relayed, repeated,
spoofable, internet-backed, manually entered, or low confidence.

Required spec answers:

- What source produced the item?
- Was it RF-only, local, relayed, imported, or internet-backed?
- How fresh is it?
- What confidence/trust label should the user see?
- What should happen with duplicate or conflicting reports?

### Constrained Customization

Selectable views should make FIO flexible without turning it into a generic
dashboard builder. A page may expose user-selectable views only from the view
types that support that page's workflow.

Required spec answers:

- Which page presets include this view?
- Which user options are safe?
- Which combinations are intentionally not allowed?
- What is the default for a new user?

### Map Scaling

Any source that can produce location, path, region, state, grid, or station data
must define how it behaves on the map. Large point sets must use filtering,
clustering, culling, rollups, or drill-down rather than drawing everything.

### Location Confidence Contract

Every view contract that projects an operator, message, report, route, node,
or pin onto the map must publish a normalized location confidence record. This
keeps FIO from silently treating every coordinate, grid, or inferred area as
equally reliable.

Required fields:

- `location_value`: the original lat/lon, grid, state, county, region, or route
  anchor used by the source.
- `location_kind`: `gps`, `grid6`, `grid4`, `state_grid4`, `state`,
  `route_derived`, `roster`, `manual`, `source_metadata`, or `unknown`.
- `confidence_rank`: ordered numeric or enum value used for comparison and
  replacement decisions.
- `source_family`: the producing source family, such as JS8Call, FIOSpotter,
  CommStat, FLMsg/FLAmp, MeshCore, APRS, or manual roster.
- `source_ref`: durable reference back to the message, packet, observation,
  roster entry, or settings row.
- `observed_utc`: when this evidence was observed or last confirmed.
- `stale_after`: source-specific freshness window used for UI aging and
  replacement decisions.
- `explanation`: short operator-facing wording, such as `reported grid`,
  `roster grid`, `device GPS`, or `route-derived from first repeater`.

Ranking defaults:

1. Manual operator/roster override or user-confirmed location.
2. Device GPS or explicit lat/lon from a trusted source payload.
3. Direct structured reported-for grid/location from CommStat, Spotter/MCF,
   APRS, MeshCore node metadata, or FLMsg/FLAmp form fields.
4. Six-character Maidenhead grid reported by the station or imported from a
   trusted source table.
5. Four-character grid constrained by state/province.
6. Four-character grid alone.
7. Route-derived location, such as first known mesh repeater/router heard.
8. Source fallback, inferred text, or unknown.

Updates must improve or clarify the stored location record. A lower-ranked
source must not overwrite a higher-ranked source unless the higher-ranked source
is stale and the operator has not locked it. Equal-rank updates may replace the
current value only when newer evidence is materially more precise, comes from a
more trusted source, or indicates the station has moved. All replacements must
preserve provenance so the operator can understand why the marker moved.

Location confidence must be visible in Map detail panels and available to Ops
Center, Messages, and SOP projections. It should be concise in the UI; details
belong in tooltips or source detail views.

Map rendering must follow a stable shell plus layer-payload contract:

- The map shell, base assets, JavaScript helpers, and theme scaffolding should
  load once and remain stable during ordinary source refreshes. Routine data,
  filter, source, and view-mode changes must be pushed as map payload updates
  instead of rebuilding the whole map document.
- A source map adapter must emit stable layer item ids, source family,
  confidence, freshness, action validity, and display priority. The map renderer
  should use those ids to update, add, remove, cluster, or fade items
  incrementally.
- High-volume adapters such as Mesh, APRS, Mesh MQTT, and Reticulum/LXMF must
  define payload caps, aggregation rules, stale-item rules, and update cadence
  before they are allowed on the map. The default is coalesced updates and
  bounded markers, not unbounded full redraws.
- `clear all layers and redraw` is allowed only for initial shell load, explicit
  reset, or incompatible renderer-version changes. It is not an acceptable
  routine refresh strategy for live source updates.
- Auto-fit is one-shot intent. It may occur when the user enters a map view,
  clicks a `Map`/`Center` action, or explicitly chooses fit results. Live
  refreshes after that must preserve the operator's viewport.
- View-specific reference layers, such as towns/cities for Mesh Nodes, may be
  enabled by the view contract without changing persistent global map
  preferences.
- Map refresh code must not pump nested UI events during rendering. Rendering
  must be scheduled/coalesced through timers or queued callbacks so WebEngine,
  splitter, focus, and window-state events cannot reenter the update pipeline.
- Hover and resize affordances must be scoped. Splitter handles may be visually
  discoverable, but map hover should never show generic panel-resize tooltips
  over map content.
- Each map adapter must provide a low-cost empty/loading state. A source with no
  mappable observations should not force a shell reload or create fake points.

Current implementation debt to retire before large APRS/Mesh volumes:

- The existing Leaflet bridge still has whole-layer `clearLayers()` refreshes in
  several renderer functions. New high-volume layers must not copy that pattern;
  they need stable id diffing or clustering adapters first.
- Some Python map configuration state is still coupled to full HTML reload
  decisions. View-specific display preferences should move into payload/runtime
  state so entering Mesh/APRS views does not cause a visible page sweep.
- Map source adapters should expose update telemetry during QA: incoming item
  count, rendered marker count, cluster count, dropped/stale count, render time,
  and whether a shell reload occurred.

Required spec answers:

- What map geometry is available: lat/lon, grid, state, region, route/path, or
  inferred location?
- When should the map auto-fit?
- What marker/cluster/rollup limits apply?
- What filters are passed from other views?

### Action Validity

Every projected item should declare which actions are valid. Do not show a
clickable action that cannot work for that source or item.

Required spec answers:

- Can the user read/open the native item?
- Can the user reply?
- Which compose mode should reply use?
- Can the item be mapped?
- Can it be pinned, acknowledged, tuned, copied, exported, or opened in an
  external tool?
- What disabled state or tooltip explains unavailable actions?

## Development Process

When adding a new data source:

1. Define its source family and normalized projection fields.
2. Complete the Mandatory Design Gates for source meaning, volume/retention,
   provenance/trust, constrained customization, map scaling, and action validity.
3. Add or reuse indexed storage/query helpers.
4. Decide which standard views it can feed.
5. Add source-specific drill-down only where native details matter.
6. Add cross-link context for Messages, Map, Compose, SOP, and ControlFreq.
7. Add focused tests at the projection layer and one view integration test.

When adding a new view:

1. Define the operator question it answers.
2. Complete the Mandatory Design Gates for the data the view accepts.
3. Define accepted projection types.
4. Define supported filters and actions.
5. Define layout behavior for Large text, compact width, empty state, and long
   content.
6. Define caching and refresh policy.
7. Add it to page-level presets only where it advances that page's workflow.

## Near-Term Application

The current ControlFreq work is the first implementation path:

- `Operational Awareness` becomes the first Attention Queue view.
- `Schedule Intersections` becomes part of Schedule Outlook.
- `Schedule Intersections` uses a real intersection-horizon selector rather
  than a static `2h ?` hint.
- `Propagation Forecast` becomes RF Readiness summary plus drill-down.
- ControlFreq row actions carry source, callsign, topic, group, state, grid, and
  query context to Messages and Map.

Next likely candidates:

- convert Messages Inbox filtering into a reusable Traffic Inbox view contract
- formalize Compose Intent as the common bridge into Compose Workbench
- make Map Context accept all portable filters consistently
- convert setup review banners into a reusable Setup Checklist view
- keep FreqPlanner, HF Daily, HF Nets, and SOP Builder aligned to the Plan
  Builder view contract: schedules are source ingredients, plans are linked
  projections, and assignments are visible usage/impact context

## Acceptance Criteria

- New source integrations do not require a new bespoke tab layout by default.
- A page can expose a selectable view without duplicating rendering logic.
- Cross-link actions preserve useful context.
- Large text and minimized widths remain usable.
- Performance work can be scoped to a view template or projection layer.
- The operator can understand what to do next without knowing which source
  produced the data.
- Every new source/view spec answers the Mandatory Design Gates before
  implementation begins.
