# Map Operator Interaction Spec

Status: implemented for the multi-rig 2.0.0 operator-map workstream, with
source-expansion and configuration-guidance items tracked as follow-on work.

## Goal

The FIO map should behave like an operating picture and action surface. It
should help an HF operator move from awareness to decision to action without
learning a pile of layer combinations.

The map should answer:

- What needs attention?
- Where is it?
- Who reported it or can help reach it?
- What evidence supports it?
- What can I do next?

The operator should not have to discover meaning by toggling unrelated layers
until something interesting appears. The map should lead with a useful default,
explain what is currently being shown, and make drill-down actions obvious.

## Center Of Gravity

Primary map views are operator tasks, not raw layers. Raw layers remain
available as advanced tools, but the main workflow should be a small set of
smart combinations.

- Regional Intel: concern by state and FEMA region.
- Traffic: recent meaningful traffic with a source subtype selector.
- Stations: station inventory and latest known station status.
- Paths: observed radio topology and reachability.
- RF Planning: path-to-station planning, peer schedule, and propagation.
- Planning Pins: operator-created planning context.

The default age for map traffic views is 24 hours unless a specific view has a
strong reason to use another window. The selected age control and displayed map
content must always agree. Paths are operational planning data, not archive
review: the Paths view offers only 24 hours or less. Older link history is shown
as station context such as `Last seen`, not as route-planning evidence.

## Workspace And Data Rules

This codebase is high risk when the wrong workspace or database is inspected.
Before map, message, or launch-control changes, confirm the active repository,
branch, and runtime database paths.

Repository:

- `/Users/bill/RadioCode/FreqInOut-multi-rig`
- current multi-rig WIP branch: `wip/private-testing-multi-rig-1.2.3-not-ready`

Runtime databases:

- settings/radio profiles/launch: `/Users/bill/RadioCode/runtime/multi-rig/config/freqinout.db`
- traffic/messages/observations/map: `/Users/bill/RadioCode/runtime/multi-rig/config/freqinout_nets.db`

Rules:

- Do not infer behavior from the production single-rig database when working on
  multi-rig map intelligence.
- Inspect runtime data directly when diagnosing a map placement/filter problem.
- Treat screenshots as observations, not source of truth.
- Keep tests narrow to the touched behavior unless the change alters shared map
  or message contracts.

## Configuration Guidance

FIO should surface configuration contradictions in Settings and Station Health,
not leave them as log-only discoveries.

JS8Call multi-rig guidance:

- duplicate JS8Call TCP endpoints across active radios are warnings because two
  radios cannot safely claim the same control port.
- multiple distinct JS8Call endpoints are valid for multi-rig native control,
  but the legacy `js8net` fallback is process-global and can attach to only one
  endpoint at a time. FIO should show this as guidance when more than one active
  JS8Call endpoint exists.
- JS8 profile/save folder and `DIRECTED.TXT` should belong to the same radio
  bundle. If a radio's `DIRECTED.TXT` sits outside that radio's JS8 profile
  folder, FIO should tell the operator to review JS8Call Settings.
- Production-parity JS8 path testing requires the matched `DIRECTED.TXT`,
  `ALL.TXT`, and `inbox.db3` from the same JS8Call profile. `DIRECTED.TXT`
  supplies directed message evidence; `ALL.TXT` supplies local profile activity.
- Guidance should use direct actions such as `Review JS8Call Settings` rather
  than asking the operator to inspect logs or databases.

## Data Surfaces

Station detail can use:

- operator roster/index: callsign, name, group, role, state, grid, FEMA region.
- JS8Call activity: last heard, all/directed traffic, SNR, band/frequency.
- JS8Spotter/MCF activity: report forms, groups, topics, status, summaries.
- CommStat artifacts: structured status, event reports, internet-fed traffic,
  reported-for geography, reporter callsign.
- FLDigi/Fast Light: check-ins, traffic activity, NBEMS message availability.
- VarAC: activity and BBS/path context where available.
- peer schedule: current/next operating band and frequency.
- path/link tables: who heard whom and with what signal quality.
- propagation model: predicted best band when peer schedule is not known.

Regional Intel can use:

- normalized observation projections.
- direct CommStat artifacts for reported-for geography and scope.
- local reports.
- message metadata and topic/status extraction.
- JS8/VarAC link and activity patterns as confidence signals.
- future mesh reports and route/activity signals through the same evidence
  model.

## Evidence Model

Every mapped report or regional concern should distinguish the following fields
when known:

- `reported_for`: the impacted state/grid/region or event location.
- `reported_by`: the station, operator, or source that submitted the report.
- `reporter_location`: the reporter's station location when different from the
  impacted location.
- `source_family`: CommStat, JS8Spotter, JS8Call, FLMsg/FLAmp, Local Report,
  VarAC, RF Pin, or future mesh source.
- `topics`: normalized topic taxonomy used by both Map and Messages.
- `status`: FIO's operator-facing status/concern value, not `severity`.
- `age`: newest evidence timestamp used by the current view.
- `location_confidence`: direct structured location, message metadata, grid
  inference, source fallback, or unknown.
- `location_confidence_record`: normalized location evidence record from the
  operational view framework, including source, rank, staleness, and a short
  operator-facing explanation.

CommStat deserves special care because it can report on locations other than
the reporter's own station. Map placement for CommStat event/status artifacts
must prefer the structured reported-for location when present. The side panel
must show `Reported For` and `Reported By` so a report submitted by an operator
in one state about an issue in another state is not misread as a placement bug.

For CommStat 4.7 structured `statrep` rows, `statrep.grid` is the mapped report
location used by CommStat itself and `statrep.from_callsign` is the reporter.
FIO should project these as `commstat_artifacts.grid` and
`commstat_artifacts.from_call`, then present them as `Reported For` and
`Reported By`. When direct source fields are available, use them. Text parsing
is a fallback, not the preferred source for CommStat geography. Free text may
infer a state when the grid is missing or coarse, but it must not turn ordinary
words such as `in` or `or` into Indiana/Oregon or override structured
reported-for geography.

## Topic And Status Filtering

Topic filtering must be shared between Messages and Map so an operator does not
see a station or report on the map and then get an empty inbox for the same
filter.

Rules:

- A topic match belongs to the sender/report/evidence item, not merely the
  message target.
- Status-field labels such as Power, Food, Water, Fire, Medical, and Comms are
  not topic evidence when their value is only `Not Reported`.
- Real status values do count: `Power: Grid down`, `Food: available`,
  `Water: contaminated`, `Fire: active`, and similar values.
- Deleted messages must stop contributing to message-derived topic projections.
- Regional Intel Messages handoff should apply the active geography, topic,
  group, age, and non-green/status evidence filters.
- The Messages tab must visibly explain map-applied filters and offer a clear
  way back to the normal inbox.

## View Behavior

### Regional Intel

Regional Intel should show actionable concern. Green evidence may contribute to
scoring, but the default summary list should not show green areas because that
creates noise and doubt about omissions.

Default display:

- state/FEMA region heat colors.
- non-green state and FEMA-region summary rows.
- contributing report density pins for non-green reports.
- compact legend shown by default and hideable.
- optional non-green station/report pins to reveal density by location.

Click behavior:

- clicking a state/region always opens the matching right panel.
- right panel must match the clicked geography, not the last popup.
- Messages opens the matching non-green report evidence for the active age,
  topic, group, and geography filters.

Scoring:

- Blue: low-information/non-current or informational evidence.
- Yellow: caution, possible disruption, or low-volume non-green evidence.
- Orange: stronger concern, multiple reports, increasing trend, or higher
  impact status.
- Red: severe status, repeated non-green reports, or high-confidence disruptive
  event.
- Green: normal evidence. It may reduce concern internally but is hidden from
  the default summary list.

The score should combine report status, topic importance, source confidence,
recency, number of reports, number of unique stations, and trend. It should not
aggregate all history indefinitely; stale evidence should decay so resolved
events do not keep an area hot.

SitRep and station-status issues older than 7 days are stale for active map
status. If no update arrives within a week, red/yellow status should stop
painting a station or regional area as an active problem. The old report remains
available in history, but it no longer drives active concern.

### Traffic

Traffic is the default live review view. It should use a 24 hour age window by
default and only show records inside that window.

Traffic has a separate source subtype selector:

- All: source-neutral operating activity for the selected age.
- RF/App: radio-side and connected-application traffic, currently JS8/Spotter,
  FLMsg/FLAmp, VarAC, and condition-alert projections.
- Local: local operator and NCS reports only.
- CommStat: CommStat artifacts and internet-fed reports only.

Click behavior:

- clusters summarize newest reports, sources, groups, topics, and status.
- report popups and the right panel use the same payload.
- Messages opens the matching reports for the active filters.

Future mesh traffic should enter Traffic > All when source-neutral and a
dedicated subtype when source-specific review is useful.

### Stations And Status

Station status is pin meaning, not a separate primary destination. The Stations
view is the inventory/discovery view. It should show known/heard stations, and
each station pin should communicate latest known station condition when FIO has
one.

Default display:

- station pins colored by latest known status.
- blue/neutral only when the station is known/heard but has no current status
  report.
- current status legend shown by default.
- no large table as the primary experience.

Click behavior:

- show callsign, name, group, state/grid, latest status, status source, updated
  age, current/next schedule if known, and recent report summary.

If a future focused station-health list is needed, it belongs behind station
details or diagnostics, not in the primary map view selector.

### Icon Semantics

Map icons should answer what the operator is looking at before they reveal how
the data was collected.

- Station activity uses station pins/circles, colored by latest known status
  when available.
- Report evidence uses topic/status icons: fire, power, water, medical,
  security, shelter, fuel, food, transport, utility, warning, or general info.
- CommStat report evidence uses a distinct source shape so it does not look
  like a live RF station.
- Generic radio/waves icons are fallbacks only. A screen full of identical blue
  RF-looking icons is considered a UX failure.
- Clusters should use count badges and dominant topic/status rather than a pile
  of same-looking icons.

### Performance And Feedback

Changing age, view, subtype, path target, or topic must feel responsive.

- 24 hours is the normal default and should render quickly.
- Multi-day and all-history windows should aggregate first and draw details
  progressively.
- If a render may take more than a moment, the map status strip must say what
  FIO is building, for example “Building 7-day Traffic view; aggregating older
  traffic before drawing details.”
- The busy message must appear in the persistent status strip before the render
  timer or WebEngine load starts; it cannot depend on a map canvas placeholder
  that may be covered by the existing map.
- Expensive path/link work should only run when links are visible or the Paths
  view needs it.
- Network paths should summarize aggressively: dedupe repeated observations to
  best pair-level links, draw only a bounded strongest/recent subset when the
  network is dense, and tell the operator how many links were omitted.
- Station inventory/status should not load report/link datasets that are not
  needed for station pins.
- Traffic and Regional Intel must not pre-load station enrichment datasets
  such as JS8/VarAC/Fldigi presence, direct-contact summaries, or station status
  unless station pins need those fields for the selected view.
- Paths should not pre-load full station enrichment; link records already define
  the topology stations for that view.

### Paths

Paths is topology-first. Users want to see who can hear whom.

Default display:

- station pins.
- directional path links when useful.
- rounded SNR values.
- legend describing link color and direction.
- age choices capped at 24h because older heard paths are not reliable routing
  guidance.

Path To:

- can be launched from a selected station.
- if peer schedule is known, it supersedes propagation.
- if peer schedule is unknown, propagation suggests likely bands.
- can be used as an overlay from other primary views without changing the main
  view.

Path labels should be concise. SNR should be rounded for tooltips and labels,
for example `SNR -10.1`, not long floating point output.

### RF Planning

RF Planning should support operating choices without covering the map with raw
text labels.

Default display:

- topology and useful reachable stations.
- propagation colors or recommendations only when selected.
- peer schedule available as the strongest planning input.

RF Planning should not default to dense band text labels. The default value is
topology. Propagation bands become useful when the operator asks "how can I
reach this station?" Peer schedule supersedes propagation because a station's
actual scheduled operating band is better than a model.

### Planning Pins

Planning Pins are operator-created context. They should remain distinct from
observed traffic and regional evidence.

Rules:

- Pins should be visually different from reports and stations.
- Pins can be filtered by group/topic/age only when those fields are meaningful.
- Pin actions should include Center, SOP, and edit/manage where applicable.
- Pins should never create false Regional Intel concern unless explicitly marked
  as an operational report.

### Advanced Map Tools

Advanced Map Tools should not be the normal path for operating the map. They
exist for uncommon inspection and planning tasks.

Rules:

- The drawer is hidden by default.
- The main control strip owns the primary view, group, age, topic, sensitivity,
  path overlay, search, Clear Filters, and Clear Layers controls.
- Opening/closing the drawer must not change the active view or make overlays
  vanish.
- Advanced filters must be visibly active and fully cleared by Clear Filters.
- Layer toggles must not corrupt smart view defaults.
- Useful advanced features, such as cities by population, remain available but
  should be zoom-aware and not clutter default views.

## Right Panel Action Card

The right panel is the command surface for a selected object.

Station selection should show:

- callsign.
- name and group affiliation.
- state/grid/FEMA region.
- modes heard.
- latest known status.
- last heard / source mix.
- current or next peer schedule when known.
- recent topics and reports.
- reachability/path summary.

Station actions:

- Compose Message.
- Show Paths To.
- Messages.
- Center.
- Group.
- SOP.

Report selection should show:

- reported-for location.
- reported-by station.
- report scope.
- status.
- topics.
- source and evidence age.
- source-specific note when location is inferred.

Regional selection should show:

- concern level/status.
- topic drivers.
- trend.
- newest evidence.
- report count and unique stations.
- source mix.
- short evidence list.

Message tab in the right panel should show the actual handoff context before
the operator clicks:

- destination.
- age window.
- status/non-green filter.
- group.
- topic.
- source.
- search/geography/callsign query.

## Compose From Map

Compose from a station should open Messages > Compose and prefill the target.

Payload:

- target callsign.
- suggested group.
- source view.
- selected/default radio id when known.
- mode hint, usually JS8 when JS8 activity is available.
- optional schedule hint.

Rules:

- FIO may prepare a draft or JS8Spotter target.
- FIO must not transmit without explicit operator action.
- If a peer schedule is known, show current/next band/frequency context.
- If multiple radios are capable, choose the selected/default radio but keep the
  radio selector visible.

## Main Map Control Model

The main map controls should be simple and durable:

- View: `Stations`, `Traffic`, `Regional Intel`, `Paths`, `RF Planning`,
  `Planning Pins`, and `Peer Schedule`.
- Type: visible for Traffic, with `All`, `RF/App`, `Local`, and `CommStat`.
- Group: selected group or all groups.
- Age: default `24h`; applies consistently to map content, summaries, paths
  where age-scoped, and message handoff.
- Topic: all topics or selected normalized topic.
- Sensitivity: for Regional Intel only, defaults to actionable/active evidence.
- Paths: off, my station, selected station, or network overlay.
- Search: callsign, group, topic, state/grid, keyword.
- Clear Filters: resets group, age, topic, sensitivity, search, and advanced
  filters to the view default.
- Clear Layers: resets optional overlays without changing the primary view.

Controls should wrap cleanly on smaller widths without covering the map title or
map content.

## End User Description

The map is a smart operating picture. It opens to recent activity from the last
24 hours and lets the operator switch to Regional Intel to see where reports are
becoming concerning by state or FEMA region. Areas only appear in the Regional
Intel summary when there is actionable evidence; normal green reports are used
internally but do not clutter the list.

Clicking anything on the map opens a right-side action card. For a station, the
card shows who they are, where they are, how recently they were heard, what
groups and modes are known, and what actions are available. For a report or
regional concern, it shows what was reported, where it was reported for, who
reported it, what topics/status drove the map color, and how to open the
matching messages.

Paths can be added when useful instead of becoming a separate puzzle. From a
station card, `Show Paths To` means paths from my station to that selected
station inside the active age window. FIO should show a direct connection when
one exists, plus plausible shared-contact bridge paths through stations both
operators have had contact with in that same window. If a peer schedule is
known, FIO uses that schedule first; otherwise propagation recommendations help
suggest likely bands.

When no direct or one-hop shared-contact path exists, FIO may show a short
bounded relay chain inside the same age/band filters, such as
`me -> N7CWR -> KC7WOK -> KL5OP`. This must be presented as observed path
topology, not as proof that my station directly heard the target.

For JS8Call, path topology must avoid overclaiming "heard by my station" from
ambiguous directed-log activity alone. `DIRECTED.TXT` may prove a directed
message or station-to-station exchange, while `ALL.TXT` from the same profile is
the safer source for local profile activity. If FIO later models passive decode
evidence directly, the UI must label that provenance separately from a path edge.

The map should feel interactive and explanatory: legends appear for the current
view, filters are visible, and clicking `Messages` carries the exact map context
into the inbox so the operator lands on the evidence that caused the map state.

## Implementation Plan

### Phase 1: Stabilize Current Map Intelligence

1. Clean current UX mismatches: green Regional Intel list noise, report Status
   terminology, station status pin meaning, rounded SNR.
2. Stabilize selection actions so right panel, popups, Messages, and Compose use
   the same payload and active filters.
3. Ensure Regional Intel click handling always selects the clicked state/region
   and never falls back to the last marker popup.
4. Ensure report markers use reported-for location when source data supplies it.
5. Make map-to-Messages handoff visible in both the map side panel and Messages
   tab.

### Phase 2: Operator Action Cards

1. Upgrade station action cards with roster data, schedule context, source mix,
   recent reports, and a `Compose Message` action.
2. Add contextual `Show Paths To` behavior with peer schedule first and
   propagation fallback second.
3. Add status/report evidence snippets that are short enough to read in the side
   panel and defer full text to Messages.
4. Use source-specific location labels, especially `Reported For` and
   `Reported By` for CommStat.

### Phase 3: Smart Controls And Advanced Drawer

1. Make legends view-aware and shown by default, with a simple hide action.
2. Keep Advanced Map Tools available but secondary, with visible active-filter
   state and simple recovery from confusing combinations.
3. Move common overlays such as Paths into the main control strip.
4. Make advanced state/source/status/trust filters reliable and clearly scoped.
5. Preserve specialized features such as cities by population, weather,
   infrastructure/utilities, and planning pins without making them part of the
   default operating workflow.

### Phase 4: Source Expansion

1. Treat JS8Call all/directed traffic as confidence and activity signals even
   when it is not a formal message.
2. Add mesh traffic through the same evidence model once available.
3. Add CommStat internet-fed artifacts as first-class evidence when structured
   fields are available.
4. Preserve source-specific confidence so inferred signals do not override
   direct reports.

### Operational Pins

Operational pins are map-first observations for tactical information that is
not necessarily tied to a known station. They may be created manually, imported,
or received over an RF/app source such as JS8/Spotter, MeshCore, APRS,
Reticulum/LXMF, or future Mesh MQTT.

Required pin fields:

- `pin_id`: stable id derived from source, location, category, text, and sender
  when the source does not provide one.
- `pin_type`: `hazard`, `checkpoint`, `supply`, `medical`, `comms`, `shelter`,
  `road`, `weather`, `welfare`, `info`, or `custom`.
- `summary`: one short operator-facing sentence.
- `details`: optional longer note.
- `location_confidence_record`: normalized confidence evidence as defined by
  the operational view framework.
- `reported_by`: callsign, node id, operator, or source label.
- `group`: operating group/channel when available.
- `source_family` and `source_ref`.
- `created_utc`, `updated_utc`, and optional `expires_utc`.
- `trust_state`: `manual`, `trusted`, `signed`, `unsigned`, `imported`,
  `unverified`, or source-specific equivalent.
- `exercise_flag`.

Display rules:

- Pins render through the same stable map payload contract as reports and mesh
  nodes. A pin layer update must not rebuild the map shell.
- Pins must show source, age, confidence, reporter, and verification/trust state
  in the detail panel.
- Pin color/icon comes from `pin_type` and concern level, not from source
  family alone.
- Unknown or low-confidence pins should be visible only when the layer policy
  allows them, and should be visually humble.
- Pin actions are source-aware: `Center`, `Inbox`, `Compose/Message`, `Topic`,
  `SOP`, `Expire`, and `Copy` appear only when valid for the selected pin.

Lifecycle rules:

- RF/app pins are deduped by source id when present, otherwise by
  sender/category/location/text within a source-specific time window.
- Pins age out according to category defaults unless manually pinned by the
  operator.
- A newer duplicate may refresh age or improve confidence, but it must not
  overwrite a higher-confidence location with a weaker one.
- Manual edits create an audit trail and move the pin to manual/user-confirmed
  confidence.

## Acceptance Tests

- Regional Intel summary list excludes green rows by default.
- Green evidence can still lower concern or support trend internally.
- Regional Intel map geography paints no-action states green for situational
  reassurance, while the summary remains an exception list of non-green areas.
- Regional Intel summary caps visible rows and shows a “more” count when the
  current filters produce more actionable states or FEMA regions than fit.
- Regional Intel summary is collapsed by default and can be shown/hidden without
  changing the active map view, filters, selected detail, or map geography.
- Station status is represented on station pins; Station Status is not a
  primary map selector.
- Stations view may show blue/neutral unknown pins, while known status pins
  use green/yellow/red.
- Traffic is one primary view with a subtype selector for All, RF/App, Local,
  and CommStat.
- Generic blue RF-looking report icons are avoided when topic/source context can
  provide a more meaningful icon or source shape.
- Long-window map renders show an explicit working message in the map status
  strip.
- Report detail uses `Status`, not `Severity`.
- SNR tooltips and labels are rounded.
- Clicking a station opens details with roster/status/schedule data when known.
- Compose Message opens Compose with the selected callsign prefilled, except
  for the operator's own station.
- Compose Message visibility is based on the resolved selected station
  callsign, not the display title, because station detail titles may include
  roster text, routes, or multiline labels.
- Messages from Regional Intel are filtered by geography, topic/group, age, and
  non-green evidence.
- FEMA-region message routing uses structured region scope plus the current
  Regional Intel topic/group/age/sensitivity context, not a loose text search
  for the FEMA region label.
- Map side-panel Messages tab explains the handoff context before opening
  Messages.
- Messages tab displays a visible map filter banner after map handoff.
- Advanced state filters match reported-for state aliases, not only reporter
  station state.
- CommStat detail distinguishes reported-for and reported-by fields.
- Regional green/no-action areas do not steal click focus from actionable
  rollups.
- Paths overlay can be enabled from a station card in any map view, and that
  action converts the map into the Paths review context.
- `Show Paths To` is always scoped to the currently selected station and the
  currently selected age window. Age changes or station changes must refresh
  from helper-owned path state, not stale UI labels.
- `Show Paths To` is visible on station detail cards except for the operator's
  own station, where the action is disabled.
- The compact control bar does not repeat “Map View” text between filters and
  the map canvas.
- The map/detail split stays usable on narrow laptop windows by shrinking the
  selected-detail panel proportionally instead of consuming the map.
- Selected-station path refresh must be fast enough for normal operator use:
  link queries should be constrained to the operator station and target station
  before Python shared-contact topology is calculated.
- Map age quick choices apply immediately. Custom days is a blank, explicit
  custom input so operators do not think an Apply action is needed for every
  age choice.
- Station detail shows detected capabilities from traffic and artifacts:
  JS8Call, FLDigi, VarAC, plus application evidence for Spotter and CommStat
  when that station has current-window usage evidence.
- Advanced filters can be cleared and do not permanently corrupt view state.

## Known Open Work

- Continue field-testing Advanced Map Tools copy and layout with live operator
  workflows. Hidden advanced filters are surfaced through Clear Filters /
  Advanced Map Tools tooltips, and the drawer remains secondary by default.
- Configuration guidance hooks for JS8 endpoint/profile conflicts are future
  Settings work unless a map workflow directly depends on them.
- Add source-neutral mesh traffic once integration is available.
- Add deeper lazy evidence retrieval only if live payloads become too large;
  current Regional Intel payloads are capped and summarized for the browser.

## Completion Notes

This workstream now covers the requested operator-map baseline:

- smart primary views with a compact top control model.
- 24 hour default age and path planning capped to operationally useful windows.
- Regional Intel heat-map behavior with no-action green geography, non-green
  summary rows, collapsed overview, national/state/FEMA detail, and map-to-
  Messages handoff.
- station action cards with status, source mix, detected capabilities, Compose
  Message, Show Paths To, Messages, Center, Group, and SOP actions.
- path rendering scoped to selected station/age, including short observed relay
  chains when no direct/shared-contact path exists.
- reported-for/reported-by CommStat handling using structured data before text
  inference.
- view-aware legends, rounded SNR labels, focused auto-fit, and responsive map
  split behavior.

## Map Detail Panel V2

The selected-item panel is an operator action surface, not just a passive
tooltip. It must identify the selected object type clearly and expose useful
next actions without making the map render path heavier.

- Station selections show station identity, latest known status, area, groups,
  last seen time, last seen integration, detected traffic modes, and detected
  tools.
- Report selections keep the report identity primary, but expose the reporter
  as an action callsign when one is known. A report by `W5TTA` should allow
  Compose Message and Show Paths To `W5TTA`; the report title itself must not be
  parsed as a fake callsign.
- Compose Message is available for any non-self action callsign and is never
  available for the operator's own callsign.
- The Paths tab summarizes direct path, best relay chain, and shared contacts
  for the current age window, then the Show Paths To action renders those paths
  on the map.
- The Messages tab summarizes matching traffic counts, unread counts, source
  mix, newest item, and topics before sending the operator to the filtered
  Messages view.
- Side-panel enrichment is lazy, cached briefly by callsign and age, and
  bounded. Traffic, Regional Intel, and Paths rendering must not wait for these
  station detail queries.
