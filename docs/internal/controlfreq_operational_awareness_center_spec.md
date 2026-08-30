# ControlFreq Operational Awareness Center Spec

## Purpose

ControlFreq should become FIO's operational awareness center: the place an
operator glances to understand what matters now, what is coming next, and what
action FIO recommends based on assigned Frequency Plans, SOP layers, propagation,
and received traffic.

This is not a replacement for Messages, Map, SOP Builder, or FreqPlanner. It is
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
- Role-focused views are better than one giant common picture. ControlFreq
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

The tab title may remain `ControlFreq`, but the first screen should read as
`Operational Awareness` or `Awareness Center` in its header/subheader.

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

### 1. Now / Next Strip

Purpose: answer "where should I be when?"

Content:

- selected/active radio short name
- current assigned group/band
- next scheduled group/band/time
- scheduler state: on plan, manual QSY, hold, suspended, conflict, unknown
- one recommended primary action:
  - `Stay`
  - `Tune`
  - `Resume`
  - `Review SOP`
  - `Read Traffic`
  - `Check Map`

Behavior:

- Use compact status chips and a single primary action button.
- Keep exact frequency, mode, and backend details in tooltip/details.
- If multiple radios are active, show one mini-strip per active radio or use the
  existing station command cards as the source of truth.

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

1. Top: Now / Next Strip
2. Left main column: Attention Queue
3. Right main column: SOP Timeline
4. Bottom or right rail: RF Readiness / Propagation Summary
5. Topic chips sit between Now/Next and the main columns, acting as filters

The current full-width propagation forecast should not dominate the default
view. Propagation is important, but it should be summarized unless the operator
selects `Propagation` or `RF Planning`.

View presets:

- `Operations`: default awareness center
- `Traffic`: attention queue + topic clusters + message/map handoffs
- `Schedule`: timeline + station command context
- `RF Planning`: propagation summary/table + map path handoff
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

- click: filter ControlFreq dashboard
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
- Schedule Intersections belong with Schedule Outlook. They should feel like an
  explanatory/detail slice of the schedule timeline rather than a separate
  competing dashboard card in the traffic column.
- Schedule Intersections must expose a real schedule-horizon control in the
  header (`30m`, `1h`, `2h`, `6h`). The selected horizon drives both the
  calculation and the persisted UI state. Do not use a decorative `2h ?` label
  that appears clickable but does nothing.
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
- Existing saved `Schedule` view state migrates once to `Operations`, because
  the new dashboard should be visible for current users without requiring them
  to discover the Activity chip.
- The global station command bar pages radio cards and disables horizontal
  scrollbar display, so maximized laptop layouts do not ask the operator to
  scroll sideways for primary radio controls.

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

### Phase 2: First Dashboard UI

1. Rework the default `Operations` preset around Now/Next, Attention Queue,
   Topic Awareness, SOP Timeline, and RF Readiness Summary.
2. Replace the default propagation table with a summary card plus drill-down.
3. Replace raw unread/source counts with ranked attention cards.
4. Preserve existing tables under `All` or `Details` during transition.

### Phase 3: Cross-Linking

1. Implement direct item handoff from attention cards to Messages.
2. Add Map handoff for topic/geography/source clusters.
3. Add Compose handoff for direct replies and topic updates.
4. Add SOP Builder handoff for condition-alert and traffic-suggestion contexts.

### Phase 4: Delight And Field Polish

1. Add band ladder / RF readiness visual.
2. Add topic chip count/freshness styling.
3. Add timeline now marker and due-soon emphasis.
4. Add compact/minimized layouts and visual QA at normal and Large text.

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

## Product Decisions

- The first high-value topic set is wildfire, power, water, medical, comms,
  weather, security, logistics, and general intel.
- `Reply` defaults to the received item's source family.
- The default dashboard uses one global Attention Queue.
- Pinned awareness is included for topics, callsigns, and groups.
- Routine unread traffic remains visible through a compact `More Traffic` strip.

## Open Product Questions

- Should topic/callsign/group pins persist only for the session, for the current
  activation, or across restarts?
- Should pinned awareness be available from day one in the UI, or should the
  first UI slice render read-only pinned focuses from settings/debug state?
