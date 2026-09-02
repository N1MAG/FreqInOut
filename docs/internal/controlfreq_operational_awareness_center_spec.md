# Ops Center Operational Awareness Spec

## Purpose

Ops Center is the user-facing name for the former ControlFreq tab. Internally,
existing code and route keys may still use `ControlFreq` until a dedicated
module rename is worth the migration risk.

Ops Center should become FIO's operational awareness center: the place an
operator glances to understand what matters now, what is coming next, and what
action FIO recommends based on assigned Frequency Plans, SOP layers, propagation,
and received traffic.

This is not a replacement for Messages, Map, SOP Builder, or Plan Builder. It is
the role-focused dashboard that fuses those systems into a clear answer:

- Where should I be now?
- What should I do next?
- What important traffic did I receive?
- Is there a location/topic I should inspect on the map?
- Does any received traffic suggest an SOP or condition-level review?

## Research Signals

Emergency management dashboard patterns point to a few durable principles:

- Situational awareness systems aggregate multiple sources into a common
  operating picture for current incidents, conditions, impacts, and decisions.
  For FIO, those sources are radio state, schedules, SOP, propagation, message
  intelligence, observations, BBS files, and map pins.
- Effective dashboards summarize incidents, resources, lifelines, live feeds,
  and field reports rather than exposing raw data first. FIO's equivalent
  lifelines are communications reachability, station readiness, upcoming nets,
  traffic topics, and critical message/report clusters.
- Role-focused views are better than one giant common picture. Ops Center
  should be tailored for the operator at the radio, while Map remains spatial,
  Messages remains the inbox/detail reader, and SOP Builder remains the policy
  and action-definition surface.
- The dashboard should provide decision support, not merely status. Every major
  module should answer either "what changed?", "what should I do?", or "where
  should I look?".

Reference ideas reviewed:

- DHS Common Operating Picture guidance:
  https://www.dhs.gov/publication/common-operating-picture-emergency-responders
- ArcGIS emergency management situational awareness architecture:
  https://architecture.arcgis.com/en/industry-and-tech-patterns/emergency-management-lens/systems/situational-awareness.html
- ArcGIS emergency information dashboard tutorial:
  https://learn.arcgis.com/en/projects/manage-and-communicate-emergency-information/
- Role-focused COP critique:
  https://bentearsolutions.com/blog/the-human-side-of-situational-awareness-one-size-doesnt-fit-all
- SEMA COP / Situational Awareness Portal summary:
  https://www.training.oa.mo.gov/erp/19-nomination/pinnacleaward-19.pdf
- Geovisual analytics for situational awareness:
  https://arxiv.org/abs/1910.05441

## Product Positioning

ControlFreq should feel like a ready-room dashboard for radio operations:
serious enough for emergency communications, but enjoyable enough that operators
want to use it every day. The visual language should suggest capability,
rhythm, and discovery rather than paperwork.

Design tone:

- quiet cockpit, not bureaucratic form
- graphical enough to be enjoyable, not decorative noise
- glanceable first, drill-down second
- operator language instead of database/source language
- action-oriented summaries, not wall-to-wall tables

The tab title, help title, and user-facing navigation must use `Ops Center`.
Internal route keys may keep `ControlFreq` for compatibility, but operators
should not have to learn the legacy name.

## North Star: Operational Picture

Ops Center should feel like a living operational picture, not a collection of
status widgets. During a real event, such as a hurricane, wildfire, flood,
communications outage, or local sheltering operation, the operator should be
able to glance at Ops Center and understand:

- what is happening
- where it is happening
- who is involved
- what needs action
- what has already been handled
- what is getting worse
- what FIO recommends next

The best version of Ops Center makes the operator feel competent and connected:
traffic has been heard, needs are visible, relays are not forgotten, schedules
and SOPs are guiding the next step, and geography is available without forcing
the user to mentally assemble several tabs.

This is the main product promise:

> FIO turns radio, mesh, schedules, SOPs, maps, and message traffic into a clear
> operational picture the user can act on.

The default view must therefore prioritize meaning over raw data. A table of
messages is not enough. A set of source counts is not enough. Ops Center should
compose a situation-level summary from normalized projections and offer fast
routes into the original evidence.

### Situation Summary Contract

Ops Center should render a `Situation Summary` above or alongside the existing
attention and schedule views. It is the first answer to "what is going on?"

Inputs:

- ranked attention traffic
- NCS/event log entries
- direct messages and welfare/relay requests
- mesh/local messages
- CommStat, Spotter, FLMsg/FLAmp, JS8Call, VarAC, BBS, APRS, Reticulum/LXMF,
  Mesh MQTT, and future source-family projections
- map reports and location clusters
- active SOP actions and upcoming nets
- source health only when it affects current operation

Required output:

- `headline`: one plain-language sentence about the current situation
- `active_incidents`: grouped event/storyline summaries
- `top_needs`: unhandled or unresolved needs for help, relay, welfare,
  logistics, comms, medical, water, power, shelter, or similar operator action
- `handled`: recently acknowledged/closed items so the user can see progress
- `where`: map-ready location hints with source confidence
- `who`: callsigns/nodes/groups most involved
- `next_actions`: 1 to 3 concrete actions such as `Read`, `Reply`, `Relay`,
  `Map`, `Tune`, `Open Net`, or `Review SOP`
- `confidence`: source count, freshness, duplicate/conflict state, and
  approximate location confidence when relevant

Example summaries:

- `Three weather reports near Asheville in 45 minutes; one welfare message is
  unanswered; MAGNET 80M net starts at 23:00.`
- `Mesh local traffic is active on COMAGNET; no urgent needs; one router area
  is newly mapped.`
- `CommStat reports regional yellow for Comms from two sources; map and review
  related messages.`

The summary must be generated from projections, not from rendered UI text. Qt
widgets consume the summary; they do not construct the operational meaning.

### Needs Attention vs Recent Traffic Contract

Ops Center must clearly separate ordinary awareness traffic from traffic that
needs operator attention. This is a data-view contract, not a visual-only label.
Every source family that contributes to Ops Center must project traffic into one
of these lanes after source/channel/user policy gates have already removed
muted or irrelevant feeds.

`Needs Attention` is the decision queue. It should stay small, prominent, and
actionable. Items belong here when they are direct to the local operator,
pinned, tied to an active or upcoming group/SOP, marked by the source as
operator attention, carry condition-alert state, include RED/YELLOW or
Level 3-5 severity, or match high-value operational topics such as weather,
comms, water, power, medical, wildfire, security, logistics, or general intel.

`Recent Traffic` is the awareness lane. It includes allowed operator/user
traffic that is useful to know about but does not currently require action.
Examples include social mesh chatter from enabled channels, ordinary public
channel updates, routine JS8/VarAC traffic, and source-family traffic that has
not crossed a configured attention threshold. Recent traffic should remain
visible and searchable without being described as urgent or actionable.

Hard gates:

- Protocol telemetry, Mesh node/router advertisements, health-only records,
  retained node observations, and channel feeds with Ops disabled must not enter
  either traffic lane.
- `actionable` wording is reserved for `Needs Attention`; ordinary activity
  should be described as `recent traffic`.
- Topic chips filter both lanes for review, but toggling a chip must not delete
  or permanently hide the other topic groups.
- `attention_items` may remain as a compatibility alias for visible
  `Needs Attention` rows while older code is migrated, but new renderers should
  use `needs_attention` and `recent_traffic`.

### Incident And Storyline Model

Ops Center should support lightweight incident/storyline grouping without
requiring formal incident management overhead.

An incident/storyline is a cluster of observations that share enough context to
be operationally meaningful:

- topic: wildfire, flood, power, comms, medical, logistics, security, weather,
  shelter, general intel, or operator-defined topics
- geography: lat/lon, grid, city/town, county, state, route-derived area, or
  regional hint
- time: fresh reports within a useful recency window, usually under seven days
  unless the storyline remains active
- actors: repeated callsigns, mesh nodes, NCS logs, or source families
- state: new, active, watching, handled, stale, false-positive, or muted

Incident grouping should remain assistive, not heavy. The operator should be
able to:

- pin a storyline
- mark an item handled
- change or clear a topic/category
- attach a traffic item to a storyline
- mute a false-positive pattern
- open the storyline on the map
- compose a reply/update from the storyline context
- review suggested SOP actions

False-positive handling is part of this model. For example, Mesh router/flood
advertisement chatter must not become a Weather/Flood incident simply because a
technical term includes "flood". Topic parsing must retain enough source
context to distinguish operator content from protocol telemetry, while also
allowing a user correction when FIO guesses wrong.

### Needs And Progress Model

Emergency communications value is not only knowing that traffic exists. The
operator needs to know whether needs are open, relayed, acknowledged, or closed.

FIO should introduce a lightweight `Need` projection derived from messages,
forms, NCS logs, manual pins, and future local-report sources.

Need fields:

- `need_id`
- `source_family`
- `source_ref`
- `summary`
- `category`
- `severity`
- `location_hint`
- `requested_by`
- `assigned_to`
- `status`: open, relayed, acknowledged, handled, stale, cancelled
- `last_update_utc`
- `actions`

Ops Center should surface open needs prominently, show recently handled needs
briefly for user confidence, and keep all details linked back to Messages, Map,
NCS, SOP Builder, or the native source detail.

### NCS And Event Log Integration

Ops Center should treat NCS logs and operator-entered event notes as first-class
operational evidence. A net control operator may log VHF requests, HF relays,
mesh discoveries, or local observations that did not arrive through a digital
message path.

Required behavior:

- NCS/event log entries feed the Situation Summary, Attention Queue, Map
  Context, and SOP Suggestions through the same projection contract as inbound
  message traffic.
- Event logs can create or update incidents/storylines.
- Event logs can create open needs and mark them handled.
- Event logs preserve net, group, band/frequency, operator, time, and source
  context.
- Event log detail remains available from the NCS area, while Ops Center shows
  only the operational summary and next action.

### Fun And Engagement Requirements

Ops Center should be meaningful during a crisis and enjoyable during routine
radio operations.

Useful delight:

- live "heard recently" pulses for active stations/nodes
- path/hop confidence markers for mesh and RF relays
- small source-lane badges for radio, mesh, APRS, and future local networks
- a compact timeline that shows the operation developing over time
- map-aware topic chips that feel connected to real places
- progress feedback when needs are relayed or handled

Avoid:

- decorative graphics that hide information
- gamification that trivializes emergency traffic
- excessive emergency-red styling
- large empty panels
- raw protocol noise promoted as human traffic

The operator should feel: "FIO is helping me make sense of this."

## Current Building Blocks

The codebase already has many pieces that should be reused:

- Station Command Bar: active radio cards, QSY, hold/suspend, health, current
  and next target.
- Frequency Control card: current QSY target, active source, next change, RF
  guard integration.
- Operational Activity snapshot:
  `operational_activity_snapshot`, topics, high-attention traffic, condition
  alerts, Messages/Map handoff buttons.
- Message Intelligence: topics, subject/title extraction, state/grid metadata,
  operator attention flags.
- Observation Projection: normalized observations from JS8/Spotter/CommStat,
  condition alerts, local reports, and RF pins.
- Schedule Outlook: HF/net/SOP rows and upcoming action generation.
- Propagation Service: target-oriented forecast data.
- Map handoffs: group/topic/source context can open filtered Map and Messages.
- SOP Builder Traffic Suggestions and condition-level review workflow.

The first implementation should reorganize and elevate these pieces rather than
inventing new data systems.

## Information Architecture

ControlFreq should be organized around five dashboard zones.

### 1. Source Lanes And Now / Next Strip

Purpose: answer "where should I be when?"

Content:

- every active operational source that can affect operator decisions
  - configured TX/RX radios by user-defined short name
  - observer/SDR sources when available
  - Mesh MQTT, APRS, Reticulum/LXMF, and other source-contract families as they are added
- each source lane shows:
  - short source name only, never a long hardware/backend description in the primary UI
  - current assigned group/band/frequency or `monitoring`
  - next scheduled group/band/time when known
  - scheduler/source state: primary, active, observer, hold, suspended, conflict, unknown
  - compact attention summary/count for traffic tied to that source
- one global recommended primary action:
  - `Stay`
  - `Tune`
  - `Resume`
  - `Review SOP`
  - `Read Traffic`
  - `Check Map`

Behavior:

- Use compact status chips and a single primary action button.
- Keep exact frequency, mode, and backend details in tooltip/details.
- If multiple radios or data sources are active, show one compact lane per
  source. ControlFreq must not collapse multi-source operations into a single
  "Now" string.
- Configured radio lanes render first. Received traffic from a source family
  that is not clearly attached to a radio still gets its own data-source lane
  so future APRS, Mesh MQTT, MeshCore, and Reticulum/LXMF traffic can appear
  without a tab-specific custom layout.
- Source lanes are the ControlFreq implementation of the view-contract source
  model: new data families should add a source lane projection before adding
  custom dashboard rendering.
- The headline may summarize source count and primary next action, but it must
  remain secondary to the source lane list.

### Traffic Source Filtering And Telemetry Boundary

Ops Center must provide a `Traffic Source` selector for high-volume traffic
families. The default is `All`; selectable families include current message
sources such as FLMsg/FLAmp, JS8Call, FIOSpotter, CommStat, Mesh, VarAC, and
BBS. Selecting a source filters the Operational Awareness attention queue,
topic chips, top-attention summary, and supporting activity text without
changing schedule control or map-layer visibility.

The attention queue is for user/operator traffic and actionable reports. Source
telemetry must not be shown as operator traffic. Examples:

- Mesh node advertisements and router/node-heard records stay available to Map,
  source lanes, health, and diagnostics, but they do not appear as Ops Center
  traffic rows.
- Real Mesh channel messages, direct messages, emergency reports, and
  topic-matched text messages can appear in Ops Center.
- Future APRS, Mesh MQTT, Reticulum/LXMF, SDR, or sensor integrations must make
  the same distinction between user/report traffic and source telemetry before
  contributing to the global attention queue.

### 2. Attention Queue

Purpose: answer "what needs my attention?"

Content:

- unread direct messages with sender and generated subject line
- high-attention topic clusters from recent traffic
- condition-alert matches
- urgent or stale BBS files
- failed/missing app or path dependencies only if they affect current operation

Presentation:

- Show 3 to 7 ranked cards, not a full table.
- Each card has:
  - source: JS8, VarAC, FIOSpotter, CommStat, BBS, Local
  - subject line or topic headline
  - age
  - source/callsign
  - group/topic chips
  - action buttons: `Read`, `Map`, `Reply`, `SOP`
- Direct operator messages must surface as message-like summaries, not only as
  source counts. Example: `N1ABC: Need relay to AMRRON 40M`.

Ranking:

1. direct messages to operator or active group
2. condition alerts and red/yellow status reports
3. traffic matching current/next SOP group
4. topic clusters with multiple sources
5. BBS files due soon/stale
6. routine unread traffic

The first dashboard uses one global Attention Queue, not one queue per radio.
Radio/source context remains visible on each card so the operator can understand
where the item came from without mentally merging several queues.

Routine unread traffic should not disappear when higher-priority attention
exists. It should move into a compact `More Traffic` strip with source counts,
latest sender, and a handoff to Messages.

### 3. Regional / Topic Awareness

Purpose: answer "what is developing around me?"

Content:

- topic chips: wildfire, power, water, medical, comms, weather, security,
  logistics, general intel
- clustered report counts by topic and recency
- state/grid/FEMA region hints when available
- source diversity: how many callsigns/sources support the topic

Presentation:

- Use topic chips with count badges and freshness.
- Use small severity/risk coloring: neutral, watch, important, urgent.
- Clicking a chip filters the Attention Queue and enables handoffs:
  - `Open Messages`
  - `Open Map`
  - `Compose Update`
  - `Review SOP`

Example:

- `Wildfire 3 | CO | 18m | 2 sources`
- Opens Map filtered to wildfire topic and matching geography/source.

The initial high-value topic set is wildfire, power, water, medical, comms,
weather, security, logistics, and general intel. This set is enough for the
first implementation; future additions should be based on operator use.

### 3a. Pinned Awareness

Purpose: answer "what do I want to keep watching?"

Pins let the operator keep a topic, callsign, or group visible even when the
global ranking would otherwise push it below the fold.

Pinned types:

- topic pin: keep a topic such as `Wildfire` or `Power` visible with new counts,
  freshness, Messages, Map, Compose, and SOP actions.
- callsign pin: keep one station's recent traffic, last-heard context, direct
  messages, and map/roster/SOP handoffs visible.
- group pin: keep one operating group visible across traffic, SOP timeline, and
  map context.

Pins are activation aids, not permanent configuration. They should be easy to
add/remove from topic chips, traffic cards, callsign details, and map selections.
Pinned items receive a ranking boost and appear in a small `Pinned` strip even
when there are no new matching messages, so the operator knows FIO is still
watching that focus.

### 4. SOP Timeline

Purpose: answer "what should I do soon?"

Content:

- next 2 to 6 hours of SOP actions and nets
- due-now and overdue actions
- assigned Frequency Plan context
- condition-level changes that would alter the timeline

Presentation:

- Use a vertical timeline with now marker.
- Emphasize upcoming nets, check-ins, scheduled listening windows, and actions
  from active SOP layers.
- Collapse routine schedule rows; expand on click.

Actions:

- `Open SOP`
- `Tune`
- `Compose`
- `Mark Done` only if the underlying SOP action supports completion state
- `Show on Map` when the action has a target/operator/geography

### 5. RF Readiness / Propagation Summary

Purpose: answer "can I reach who I need?"

Content:

- best band now for selected group/region/operator
- next best band window
- confidence/freshness
- known peer schedule override when available
- station/radio health relevant to transmit/receive

Presentation:

- Summarize propagation as a recommendation card, not a large forecast table.
- Use a simple band ladder or sparkline:
  - `20m strong now`
  - `40m improves after 1900Z`
  - `80m night fallback`
- Full propagation table/forecast remains a drill-down card or separate view.

## Dashboard Layout

Default `Operations` view:

1. Top: Source Lanes And Now / Next Strip
2. Situation Summary: current situation, open needs, handled progress, and next
   action
3. Left main column: Attention Queue and topic/storyline chips
4. Right main column: SOP Timeline, Schedule Outlook, and Peer Schedule Finder
   when selected
5. Bottom or right rail: RF Readiness / Propagation Summary

The current full-width propagation forecast should not dominate the default
view. Propagation is important, but it should be summarized unless the operator
selects `Propagation` or `RF Planning`.

### Custom Card Layout Contract

Ops Center can be configurable, but it must remain opinionated and reliable.
User customization chooses visible cards and density; FIO owns card placement
and reflow.

Card order:

1. Source Lanes And Now / Next
2. Situation Summary
3. Attention Queue
4. Topic/Storyline Rollup
5. Schedule Outlook
6. Peer Schedule Finder
7. SOP Timeline
8. RF Readiness / Propagation

Rules:

- Hidden cards reserve zero space.
- If Peer Schedule Finder is hidden, Schedule Outlook moves up immediately and
  uses the available top/right position.
- If only one main card is visible, it may use full width.
- At compact width, all visible cards stack vertically in card order with
  page-level scrolling.
- At wide width, Situation Summary spans the top of the main content when
  present; Attention Queue and Schedule/SOP cards split the remaining space.
- Sparse tables size around useful rows and do not reserve large blank panels.
- Manual splitter movement is allowed, but saved splitter sizes must not defeat
  card reflow when cards are hidden or the window is resized.
- Custom state persists per user but must have a `Reset Ops View` route back to
  the recommended `Operations` default.
- Future `Customize Ops View` UI may expose card visibility, card density
  (`Compact`, `Normal`, `Expanded`), and allowed presets only. It must not
  become a generic free-form dashboard builder.

View presets:

- `Operations`: default awareness center
- `Traffic`: attention queue + topic clusters + message/map handoffs
- `Schedule`: timeline + station command context
- `RF Planning`: propagation summary/table + map path handoff
- `Incident`: situation summary + open needs + map/storyline focus
- `Mesh Local`: mesh operator traffic + local map nodes + source lanes
- `NCS`: net/event log + open needs + relays + scheduled actions
- `All`: dense legacy-style mixed view for operators who want everything

## Cross-Linking Requirements

Traffic cards:

- `Read` opens Messages Inbox focused to the specific message when possible.
- If specific row identity is unavailable, open Messages with source, callsign,
  group, topic, and search context.
- `Reply` opens Compose with send-from radio, destination, source context, and
  topic carried into draft metadata where possible.
- `Reply` defaults to the source family of the received item. JS8 direct traffic
  opens JS8 compose, FIOSpotter traffic opens FIOSpotter compose, CommStat RF
  opens CommStat RF compose, and NBEMS/file traffic opens FLMsg/FLAmp compose
  when enough context is available.
- `Map` opens Map filtered to matching topic/group/source/geography.
- `SOP` opens SOP Builder Traffic Suggestions with group/topic/source context.

Topic chips:

- click: filter the Ops Center attention queue locally while leaving the full
  topic rollup visible
- clicking the selected chip again clears the local topic filter
- `Clear Filters` clears topic-chip focus along with search/source/window
  filters
- double-click or menu: open Map/Message filtered view
- Do not also show a separate `Topics: ...` text line when topic chips are
  present. The chips are the summary and the control.

SOP timeline:

- `Tune` uses the existing ControlFreq/RF Guard path.
- `Compose` opens Messages Compose with the current or upcoming operating
  context.
- `Open SOP` opens the corresponding SOP/profile/action detail.

Map:

- Map detail selections should be able to return to ControlFreq with the same
  group/topic/source context.

## Visual Design Requirements

- Use cards only for repeated actionable items and summaries, not nested
  decorative containers.
- Use compact chips for radio, band, group, topic, and source.
- Traffic-row actions must be separate small action chips/buttons, not text like
  `Read / Reply / Map`. The operator should be able to see that each action is
  clickable.
- Use simple glyph/icon language for:
  - message/direct traffic
  - map/location
  - SOP/action
  - RF/band
  - health/dependency
  - BBS/files
- Avoid one-note emergency-red styling. Reserve red/amber for real operational
  significance.
- Add small delightful touches that support radio enjoyment:
  - band ladder
  - signal/path confidence marks
  - "heard recently" operator pulse
  - timeline movement
  - station/radio readiness badges
- Do not make the dashboard feel like an incident report form.
- Peer Schedule Finder belongs with Schedule Outlook. It answers "who can I
  likely reach now or soon?" and should feel like an adjacent operational action
  surface rather than a competing schedule dashboard.
- Peer Schedule Finder must expose a real look-ahead control in the header
  (`30m`, `1h`, `2h`, `6h`). The selected horizon drives peer overlap
  calculation and persisted UI state. Label the control as `Overlap Window`
  until a future shared control truly drives both peer overlap calculation and
  the Schedule Outlook table.
- Peer Schedule Finder rows should stay narrow: visible columns are `Peer`,
  `When`, `Net/Band`, `Heard`, and compact action chips. Details such as exact
  overlap range, source schedule, confidence, and route can live in tooltips or
  an expandable detail view.
- Peer actions should route through the same view-contract destinations used by
  Operational Awareness: `Msg` opens Compose addressed to the peer, `Map` opens
  map context when location is known, and `Pin` keeps the peer visible in the
  attention queue.
- Propagation defaults to a compact RF Readiness summary. Detailed propagation
  tables are opt-in through `Forecast Details` and must not leave a large blank
  panel when collapsed.
- The station command bar should page radio cards only when they do not fit.
  Two configured radios must display together at normal/maximized laptop widths.
  Page controls must be child widgets inside the command bar, never independent
  windows.
- Resizable ControlFreq panels must use the shared visible splitter-handle style
  so the operator can discover and grab the divider without pixel hunting.

## Data Model Requirements

Introduce a ControlFreq dashboard projection in core, separate from Qt widgets.
Suggested shape:

- `AwarenessSnapshot`
  - `generated_at_utc`
  - `active_radios`
  - `recommended_actions`
  - `attention_items`
  - `more_traffic`
  - `topic_rollups`
  - `pins`
  - `sop_timeline_items`
  - `rf_readiness`
  - `filters`

- `AttentionItem`
  - `id`
  - `source_family`
  - `source_ref`
  - `callsign`
  - `to_target`
  - `subject`
  - `summary`
  - `topics`
  - `group`
  - `state`
  - `grid`
  - `age_seconds`
  - `priority`
  - `pinned`
  - `reply_compose_mode`
  - `actions`

- `PinnedAwareness`
  - `pin_type`: topic, callsign, or group
  - `value`
  - `label`
  - `matched_count`
  - `newest_utc`
  - `actions`

- `TopicRollup`
  - `topic`
  - `count`
  - `source_count`
  - `callsign_count`
  - `newest_utc`
  - `geography_hint`
  - `severity`

- `SopTimelineItem`
  - `due_utc`
  - `label`
  - `group`
  - `band`
  - `frequency`
  - `action_kind`
  - `status`
  - `source_profile_id`

- `RfReadinessItem`
  - `target`
  - `best_band`
  - `next_band`
  - `confidence`
  - `reason`
  - `peer_schedule_source`

Qt should consume this projection and render it. Database scans, message
intelligence, topic rollups, and propagation lookups should stay out of the UI
thread.

## Performance Requirements

- Build the awareness snapshot in a worker or cached core helper.
- Diff-apply card updates where practical; avoid rebuilding every dashboard
  widget on every timer tick.
- Cache topic taxonomy, SOP timeline, propagation summaries, and message
  subject extraction by source ID and timestamp.
- Bound default queries:
  - recent attention: 40 to 100 observations
  - dashboard cards: 7 visible
  - topic rollups: top 8 visible
  - SOP timeline: next 2 to 6 hours
- Use stale-but-visible data while refresh is running. Replace content only when
  a complete snapshot is ready.

## Large Text And Minimized Window Requirements

- The default dashboard must remain usable at Large text.
- At minimized width, keep Now/Next, top attention card, and workbench/detail
  actions reachable.
- Cards may stack vertically.
- Tables are secondary and may move behind `Details`.
- No important action may rely solely on hover text.

## Implementation Plan

Current implementation status:

- `freqinout.core.controlfreq_awareness` defines the first Qt-free awareness
  projection for attention ranking, topic rollups, pins, source-family reply
  defaults, map/SOP handoff actions, and `More Traffic`.
- `freqinout.core.observation_queries.operational_awareness_snapshot` exposes
  that projection from the existing observation store.
- ControlFreq's Activity card has begun migrating to `Operational Awareness`
  with an attention table, topic/pinned/more-traffic support text, and
  Messages, Map, Compose, and Pin Focus actions.
- The `Operational Awareness` card now includes Now/Next guidance, a top
  attention lead line, recommended actions, clickable topic/pin chips, Clear
  Pins, and a compact `More Traffic` strip.
- The `Operations` preset includes RF Readiness with a summary-first propagation
  card and collapsible forecast details.
- ControlFreq builds the legacy activity headline and new awareness projection
  from one scoped observation query to avoid duplicate message-database scans.
- Compose handoff from Operational Awareness now carries source-family intent
  into Message Compose, so replies can open the appropriate compose mode.
- Operational Awareness action rows include an operator topic-correction path.
  Corrections such as `Mark Social`, `Mark Comms`, `Mark Weather`, custom topic,
  and clear override must update the current row immediately. Source families
  with durable override stores, beginning with MeshCore/Meshtastic, must persist
  the correction by source reference so future reprojection does not re-promote
  a false-positive item.
- Mesh node and topology projections may inform Ops Center and Map, but they
  must not be promoted as message traffic. Ops Center source filters should let
  operators include mesh user traffic without flooding the attention queue with
  router/node advertisements.
- Existing saved `Schedule` view state migrates once to `Operations`, because
  the new dashboard should be visible for current users without requiring them
  to discover the Activity chip.
- The global station command bar uses compact radio cards that show as many
  configured radios as fit and enables horizontal scrolling only for true
  overflow. The summary row must recalculate actual content width after
  viewport changes so a stale wide card cannot clip the next radio.
- Next major Ops Center direction is an incident-aware operational picture:
  Situation Summary, Incident Storylines, open/handled Needs, NCS/event log
  evidence, source-aware filters, and custom card reflow. The existing
  Operational Awareness table remains transitional until card projections can
  replace it without losing route coverage.

### Current Implementation Slice: Situation Projection And Reflow

Status: in progress for the first operational-picture slice.

1. Core projection adds Qt-free `SituationSummary`, `IncidentStoryline`, and
   `NeedSummary` contracts to `AwarenessSnapshot`.
2. Existing observation-derived operator traffic feeds the first Situation
   Summary. Mesh node advertisements remain excluded from the attention queue
   and therefore do not become incidents or needs.
3. Open needs are detected conservatively from clear request/need/help/relay
   language plus operational categories such as Water, Medical, Shelter, Food,
   Fuel, Power, Comms, Rescue, Evacuation, and Welfare.
4. Handled needs are separated from open needs when traffic clearly indicates
   handled/resolved/complete/closed/delivered/filled/met/ack state.
5. Ops Center renders a compact Situation band above Needs Attention so the user
   can see the overall story before scanning rows.
6. Custom card reflow now treats a single visible right-column card as
   content-height, so hiding Peer Schedule Finder must move Schedule Outlook up
   instead of leaving it stranded at the bottom of the column.
7. Focused tests cover quiet monitored sources, open needs with storylines,
   handled needs, topic rollups, source reply routes, and mesh node telemetry
   exclusion.

### Current Implementation Slice: Situation Cards

Status: implemented as the first visible card projection over the Situation
Summary contract.

1. Ops Center now renders compact Situation cards above the legacy attention
   table. Cards are populated from `SituationSummary.top_needs`,
   `SituationSummary.active_incidents`, and `SituationSummary.handled`.
2. Open Need cards expose Inbox, Reply, and Map actions only when the underlying
   source contract/context supports those routes. Storyline cards expose Inbox,
   Map, and SOP routes without pretending a reply is possible when no concrete
   sender exists.
3. Topic-filtered custom views render a compact filtered-topic card so the user
   receives immediate feedback after selecting a topic chip.
4. The existing Operational Awareness table remains as a transitional evidence
   and detail surface. Future work should remove or demote the advanced table
   only after card actions, row drill-down, topic correction, pinning, and map
   routes have equivalent coverage.
5. The card projection is deliberately read-only with respect to ranking. It
   consumes the contract already produced by the core awareness projection so
   future APRS, Mesh MQTT, Meshtastic, Reticulum/LXMF, and event-log sources can
   populate the same surface without tab-specific rendering logic.

### Phase 1: Spec And Projection

1. Add this spec and cross-link it from existing Messages/Map/SOP integration
   specs.
2. Inventory existing ControlFreq data helpers and decide which can feed the
   new projection unchanged.
3. Add core dataclasses for `AwarenessSnapshot`, `AttentionItem`,
   `TopicRollup`, `PinnedAwareness`, `SopTimelineItem`, and
   `RfReadinessItem`.
4. Add unit tests for ranking, topic rollup, direct-message subject extraction,
   source-family reply defaults, pinned awareness, `More Traffic`, and SOP/RF
   summary formation.

### Phase 2: Situation Projection

1. Add Qt-free `SituationSummary`, `IncidentStoryline`, and `NeedSummary`
   projections.
2. Feed them from existing message intelligence, observation queries,
   CommStat/Spotter/JS8/FLMsg/VarAC/BBS rows, mesh operator traffic, and
   NCS/event logs.
3. Add source-family flags for `operator_traffic`, `infrastructure_telemetry`,
   `health`, and `event_log` so protocol noise cannot become a false incident.
4. Add durable operator correction state for topic/category changes, muted
   false-positive patterns, pins, and handled/acknowledged needs.
5. Add unit tests for incident grouping, false-positive suppression, open/handled
   need lifecycle, and source provenance.

### Phase 3: First Dashboard UI

1. Rework the default `Operations` preset around Source Lanes, Situation
   Summary, Attention Queue, Topic/Storyline Awareness, SOP Timeline, and RF
   Readiness Summary.
2. Replace the default propagation table with a summary card plus drill-down.
3. Replace raw unread/source counts with ranked attention cards and, where
   appropriate, situation/storyline cards.
4. Preserve existing tables under `All` or `Details` during transition.
5. Implement the Custom Card Layout Contract so card visibility never leaves
   reserved blank regions.

### Phase 4: Cross-Linking

1. Implement direct item handoff from attention cards to Messages.
2. Add Map handoff for topic/geography/source clusters.
3. Add Compose handoff for direct replies and topic updates.
4. Add SOP Builder handoff for condition-alert and traffic-suggestion contexts.
5. Add NCS/event-log handoff for storylines and needs that originate from
   manual or net-control entries.

### Phase 5: Delight And Field Polish

1. Add band ladder / RF readiness visual.
2. Add topic chip count/freshness styling.
3. Add timeline now marker and due-soon emphasis.
4. Add compact/minimized layouts and visual QA at normal and Large text.
5. Add "heard recently", mesh hop/path confidence, and handled-progress signals
   only where they clarify the operational picture.

## Acceptance Criteria

- Default ControlFreq view answers "what should I do now?" without reading a
  table.
- Direct messages show subject-like summaries and open the original message.
- Reply actions default to the received source family.
- One global Attention Queue ranks the most important items, with routine
  overflow represented in a `More Traffic` strip.
- Operators can pin a topic, callsign, or group and see that focus remain
  visible even if it is not the highest-ranked current item.
- Topic clusters such as fire/power/water can open Map filtered to matching
  geography and evidence.
- Upcoming nets and SOP actions are visible as a timeline.
- Propagation is summarized in the default view and available in detail on
  demand.
- Dashboard refresh does not freeze the UI when message databases or BBS
  folders are large.
- Large text and minimized window checks pass for the default Operations view.
- Existing ControlFreq command actions remain routed through RF Guard and
  scheduler-safe paths.
- Default Ops Center includes a Situation Summary that answers "what is going
  on?" without requiring the operator to read a table.
- Open needs remain visible until handled, relayed, acknowledged, stale, or
  cancelled.
- Recently handled needs are visible briefly so the operator sees progress.
- NCS/event log entries can feed Situation Summary, Map Context, Attention
  Queue, and SOP Suggestions.
- User topic/category corrections persist and prevent repeated false-positive
  attention, including protocol terms such as mesh flood advertisements.
- Custom card visibility reflows correctly: hiding Peer Schedule Finder moves
  Schedule Outlook up and leaves no awkward empty slot.
- Source telemetry and infrastructure/node chatter can inform source lanes,
  health, topology, and map layers without appearing as human/operator traffic.
- The Situation card remains visible even when traffic is quiet so operators can
  tell the operational-awareness contract is active and current.
- Situation Summary, Needs Attention, and Situation cards must use paired
  semantic background, foreground, and border colors for each theme. Light-theme
  alert fills must not be reused in dark theme unless the foreground and border
  are explicitly adjusted and tested for readability.
- Local mesh startup, reconnect, and shutdown must never block the Qt UI thread;
  BLE coroutine calls use bounded waits and shutdown uses non-blocking worker
  stop requests.

## Product Decisions

- The first high-value topic set is wildfire, power, water, medical, comms,
  weather, security, logistics, and general intel.
- `Reply` defaults to the received item's source family.
- The default dashboard uses one global Attention Queue.
- Pinned awareness is included for topics, callsigns, and groups.
- Routine unread traffic remains visible through a compact `More Traffic` strip.
- Ops Center should evolve toward an incident-aware operational picture rather
  than a raw source dashboard.
- Situation Summary, Incident Storylines, Needs, and NCS/event log projections
  are reusable operational view contracts, not Ops-Center-only widget logic.

## Open Product Questions

- Should topic/callsign/group pins persist only for the session, for the current
  activation, or across restarts?
- Should pinned awareness be available from day one in the UI, or should the
  first UI slice render read-only pinned focuses from settings/debug state?
