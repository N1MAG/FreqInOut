# Ops Center Dashboard And Focus Search Spec

## Status

Implemented on the multi-rig private-testing branch on 2026-09-04. The focus
resolver, two-horizon evidence model, bounded caches/background backfills,
command-search separation, icon family, and differentiated dashboard renderers
are now the maintained contract.
This spec refines, but does not replace,
`multirig_product_ui_contract.md`,
`controlfreq_operational_awareness_center_spec.md`, and
`actionable_traffic_summary_spec.md`.

## Product Outcome

Ops Center should feel like one operational dashboard rather than several tables
placed on one page. Its purpose is to help the operator answer:

- **What requires action or attention?**
- **Why does it matter?**
- **Who, what group, topic, or region is involved?**
- **Can I reach them, and what band is likely to work?**

The Station Control Bar continues to own persistent radio-specific **Where** and
**When** context. Ops Center uses the remaining workspace for **What**, **Why**,
and fast orientation around a station, group, topic, or place.

## Visual System: One Dashboard, Several Visual Grammars

The views should share typography, spacing, semantic colors, chips, and action
placement, but they should not all use the same table treatment. Each view gets
the smallest visualization that expresses its primary relationship:

| View | Primary question | Default presentation |
| --- | --- | --- |
| Actionable Traffic | What do I need to handle? | Ranked action chips and one concise What/Why line |
| Traffic by Group | Where is activity changing? | Horizontal current/prior comparison bars |
| Operational Awareness | What is developing? | Ranked situation/need cards with compact evidence strips |
| Source Lanes | What is each radio/source doing? | Stable lane cards: source, now, next, attention badge |
| Peer Schedule Finder | Who can I reach now or soon? | Rendezvous timeline with a visible Now marker and peer rows |
| Schedule Outlook | What is due next? | Vertical time rail with schedule/SOP events and direct actions |
| Propagation | What path/band is most promising? | Band ladder with modeled and observed confidence |

Tables remain appropriate for dense inspection and editing, but they are
secondary disclosures. The default dashboard uses cards, bars, lanes, and
timelines so adjacent sections remain visually distinguishable.

### Shared Card Rules

- Every card begins with one answer, not a generic container title.
- Metadata follows one consistent bullet-delimited grammar:
  `State or trend · count/freshness · source/provenance · latest age`.
- Primary actions occupy a predictable trailing edge. Secondary evidence opens
  through row/card activation or a Details disclosure.
- Empty states are explicit and compact, for example `No peer overlap in the
  next 2 hours`; they do not reserve a full empty table.
- Red and amber represent operational significance, not decoration. Shape,
  wording, and icons carry the same meaning without color.
- Updates should change values in place rather than resize or reorder the whole
  dashboard unnecessarily. Priority-driven reordering is allowed when the
  underlying operational state materially changes.

### Iconology Contract

Iconology should provide fast category recognition and help each view feel like
part of one application. It is a semantic layer, not decoration and not a
replacement for unfamiliar operational terms.

Use one application-owned outline icon family with consistent 24px view boxes,
stroke weight, padding, and theme behavior. Reuse the existing navigation icons
where their meaning is already correct, and add Ops-specific assets under
`assets/icons/ops/` rather than relying on platform theme icons or Unicode emoji.

Recommended mappings:

| Meaning | Symbol direction | Uses |
| --- | --- | --- |
| Callsign/operator | single operator/headset | autocomplete, callsign focus, peer row |
| Group | multiple operators/network nodes | autocomplete, group focus, traffic group |
| Topic/intelligence | tag with small signal mark | autocomplete, topic focus, storyline |
| Geography | existing map pin | place/region focus, Map action, location evidence |
| Message/reply | existing envelope with reply variant | traffic, Inbox, Reply |
| Relay/distribute | antenna with outward arrow | actionable Relay, need progress |
| Schedule/time | clock/calendar | schedule events, next rendezvous |
| SOP/action | clipboard/checklist | SOP timeline and Review SOP |
| RF readiness | signal bars or propagation arc | band ladder, path recommendation |
| Source/radio | existing antenna/radio mark | source lane and source provenance |
| Health/warning | existing health mark / triangle | blocked action or degraded source |
| Pin/watch | map-pin or bookmark variant | persistent awareness pin |

Rules:

- Use one leading icon per card, suggestion, or lane to identify its type.
- Use icons on compact repeated actions when the symbol is familiar; retain a
  tooltip, accessible name, visible keyboard focus, and text in Large Text or
  ambiguous contexts.
- Keep trend arrows (`↑`, `→`, `↓`) because they communicate direction more
  directly than a novel trend icon. Do not add a second decorative trend glyph.
- Source-mix icons may precede concise counts, but the accessible text must still
  name the source. Multiple source icons must not become an unexplained legend.
- A warning icon indicates operational significance only. Do not use it merely
  because a count is nonzero.
- Never distinguish two states by icon color alone; shape and adjacent wording
  must also differ.
- At narrow widths, preserve the leading type icon and primary action icon; hide
  secondary decorative/source icons before eliding the What/Why text.

## Traffic By Group Copy Contract

Explicitly associated operating and membership groups remain bold and sort
before unrelated groups. Do not add `My group` to every row; the repeated text
adds noise after the typographic and ordering treatment already establishes the
distinction.

The row detail is a single bullet-delimited sequence, for example:

`Rising ↑ · 0 new · CommStat 5 · SitRep 4 · latest 3h`

The current/prior exact values remain beside the bar. Full source mix and group
association remain in the accessible description/tooltip if the visible line is
elided. At narrow widths, preserve trend, new count, and latest age before
showing every source count.

## Dashboard View Directions

### Operational Awareness

Replace the transitional evidence table in the default Operations preset with
a small ranked card stack:

- lead line: plain-language situation or need
- evidence strip: severity/topic · callsign/group · source diversity · age
- progress state: open, relayed, acknowledged, handled, or watching
- contextual actions: Inbox, Reply, Map, SOP, or Pin only when supported

Keep the full evidence table behind `Details`. This preserves dense review
without making every default surface look like a spreadsheet.

### Source Lanes

Render each active radio or non-radio data source as a stable horizontal lane:

- concise source name and status icon
- current assignment or `monitoring`
- next meaningful transition
- attention count/status at the trailing edge

Configured commandable radios remain visually distinct from Mesh, APRS, and
other awareness-only sources. A lane activates source focus; it does not replace
the selected-radio control in the Station Control Bar.

### Peer Schedule Finder

Use a compact rendezvous timeline:

- fixed Now marker
- look-ahead horizon from the existing 30m/1h/2h/6h control
- exactly one row per operator identity/callsign, with every matching overlap
  window represented as a segment in that row
- combined band/frequency labels above the timeline; an operator scheduled on
  20m and 40m must not appear as two peer rows
- observed `heard` freshness/confidence marker when available
- a compact actions affordance for Message, Map, and Pin
- visible, peer-local filters for callsign, configured group membership,
  roster region, and duty role

The chart uses one native item-view viewport with a paint delegate rather than
rebuilding nested row widgets. It shows at most six rows in its card height and
scrolls internally when more operators match. This keeps the Ops page bounded
for 150-plus-operator rosters. The former peer detail table is not retained:
it duplicated the timeline, consumed space, and created competing scroll and
paint surfaces. Filter controls and the result count remain visible while the
chart scrolls.

Changing filters, the look-ahead window, card visibility, or outer-page scroll
must not leave stale row widgets or painted content. Ops card visibility changes
use deterministic final geometry inside the outer scroll area; height animation
is not used there.

### Schedule Outlook

Use a vertical time rail rather than a conventional grid:

- due/overdue and next events align to a clear Now marker
- event glyph distinguishes SOP action, HF schedule, local net, or condition
  transition
- group, band/frequency, and concise action appear on one event row
- QSY/Hold, Open SOP, or Compose remains directly actionable
- routine later rows collapse under `Later` by default

### Propagation / RF Readiness

The default card is a band ladder, not the full forecast table:

- best band now
- one or two alternatives
- next expected improvement/change
- modeled confidence and freshness
- observed signal evidence when available

When focused on a callsign or geographic target, the card should combine:

- the existing propagation model for that target
- recent historical signal strength/path observations
- peer schedule evidence
- station/radio availability

The result is a recommendation with Why, such as:

`20m best now · modeled good · heard -7 dB 38m ago · 40m improves after 2300Z`

Modeled and observed evidence must remain visibly distinct. Missing observations
must not be presented as a negative path result.

## Search FIO Becomes Focus And Orientation

### Intent

`Search FIO` is primarily a fast way to orient the entire Ops dashboard around
an operational entity. It is not initially a document search engine and does
not need to return an unstructured page of matches.

Typing shows categorized autocomplete suggestions. Choosing a suggestion
creates one typed `OpsFocus` and refreshes compatible dashboard projections.
Typing alone does not repeatedly run the full dashboard query.

The current behavior, where `textChanged` schedules several independent view
refreshes and Enter opens the navigation-oriented quick-result menu, is
transitional. During migration, `textChanged` must become autocomplete-only;
the dashboard refresh begins only when a typed focus is accepted. Existing raw
search matching may remain behind an explicit `Filter visible evidence for…`
fallback, bounded by the active Traffic Age window.

Initial focus kinds:

- callsign/operator
- operating or local group
- event/incident/storyline
- message-intelligence topic
- state, FEMA region, grid, or known place
- band/frequency
- source family

Examples:

- `K7ETC` → `Callsign · K7ETC · MR08 Hub · heard 38m ago`
- `MR08` → `Group · MR08 · configured membership group`
- `power` → `Topic · Power · 4 recent reports`
- `Pine Ridge Fire` → `Event · last activity 12d ago · 3 retained reports`
- `CO` → `State · Colorado · 6 active stations`
- `Region 8` → `FEMA Region · CO/MT/ND/SD/UT/WY`

Each suggestion begins with its entity-kind icon so callsigns, groups, topics,
and places remain distinguishable before the user reads the secondary text.
The applied focus banner repeats that icon once; individual cards do not repeat
it unless the card represents a different evidence type.

### Focused Dashboard Behavior

A visible focus banner prevents hidden-filter uncertainty:

`Focused on K7ETC · Callsign    [Clear]`

Existing Age, Group, and Source controls intersect with the focus and remain
visibly stated. The focus never silently rewrites user configuration.

For a **callsign focus**, show or filter:

- identity, trusted state, known roles, and explicit groups
- current callsign plus former callsigns from Operator History when applicable
- last heard by source and most recent status/SitRep
- direct and group-relevant traffic, open needs, and reply/relay state
- known personal/group schedule and next likely rendezvous
- map/grid/state context
- target-oriented RF readiness using modeled propagation and observed signal
  history
- actions: Message, Map, Pin, and applicable SOP/Net routes

For a **group focus**, show associated operators, traffic trend/source mix,
condition/SOP state, current and upcoming schedule, active needs, map context,
and group-oriented RF readiness.

For a **topic focus**, show traffic volume/trend, active incidents or needs,
involved callsigns/groups, geographic clusters, source diversity, relevant SOP
actions, and Messages/Map handoffs.

For a **region/place focus**, show active callsigns/groups, status distribution,
traffic topics, needs/incidents, schedule relevance, and propagation toward that
area.

For an **event/incident focus**, show its last known state, affected
groups/places, retained report and source summary, unresolved or last-reported
needs, most recent update, and applicable Messages, Map, SOP, and propagation
context. An inactive historical event remains orientable without being presented
as currently active.

If a field is unknown, say `No schedule known`, `No recent status`, or
`Location unknown`; do not leave an unexplained blank card.

### Two-Horizon Evidence Contract

The active Traffic Age window answers **what is current**. It must not determine
whether FIO appears to know an operator, group, or event at all. A focused
dashboard therefore has two explicitly labeled evidence horizons:

1. **Current scope** applies the selected Age, Group, and Source filters to
   traffic, awareness, needs, schedule relevance, and trend calculations.
2. **Last known** uses retained indexed summaries to show the newest available
   evidence for the focused entity even when that evidence is outside the
   selected time window.

Example:

```text
K7ETC · Callsign
No traffic received in the selected 24-hour window.
Last known · heard 34d ago · JS8Call · MR08
Last report · Green at receipt · received 35d ago · DM38ST
```

Rules:

- Never replace `No traffic in the selected 24-hour window` with a generic
  `No data` state when retained evidence exists.
- Never silently widen the active window or add old messages to current counts,
  charts, action queues, or incident status.
- Label historical evidence with its age and `Last known`, `Last heard`, or
  `Last report` language. A month-old Green report means `reported Green 35d
  ago`, not `currently Green`.
- Last-known identity/evidence may cross the active Age filter by design. If it
  also falls outside the selected Group or Source filter, state that scope
  mismatch rather than hiding the record or treating it as current.
- Read and archived retained evidence may supply Last Known. Deleted evidence
  does not reappear in operational focus; audit-only records remain in their
  existing audit surface.
- Receipt time determines when FIO learned the evidence. Preserve source/event
  time separately when available.
- If retention or missing projections prevent a historical answer, say
  `No retained traffic found` and show the oldest searchable boundary when FIO
  knows it. Do not imply the station has never been heard.
- A `History` action opens a descending, paginated entity-specific view. It does
  not load all retained rows into Ops Center.
- The compact Last Known summary is shown even when current results exist when
  it materially adds status, schedule, location, or path context; avoid
  duplicating the newest current item.

Callsign resolution follows
`docs/internal/operator_identity_history_spec.md`. `Change callsign` is owned
by Operator History management, not Ops Center. Searching a current or former
callsign opens one stable operator focus while historical evidence keeps the
callsign actually received.

### Autocomplete And Disambiguation

- Suggestions appear after one meaningful character for groups/topics/places
  and after two characters for callsigns unless an exact known callsign exists.
- Show no more than eight suggestions, grouped by entity kind.
- Rank exact match, prefix match, explicitly associated groups/operators,
  recency, and operational relevance ahead of generic substring matches.
- A callsign-shaped token is not assumed to be a known operator. Label unknown
  but syntactically valid input as `Callsign · not in operator roster` and
  allow a traffic-only focus.
- Ambiguous values such as `CO` show typed alternatives instead of guessing.
- Arrow keys move through suggestions; Enter applies the selected/exact focus;
  Escape closes suggestions; the Clear action returns to the unfiltered Ops
  dashboard.
- Suggestion rows and the focus banner expose accessible names/descriptions and
  never rely on icons alone.

### Navigation Search Separation

The existing quick-search records for tabs, settings, radios, setup issues, and
commands are useful but represent navigation, not operational focus. Preserve
them as a separate command palette, preferably available through `Ctrl+K` and a
small command/navigation affordance. Do not mix `Go To Settings` results into a
callsign/topic autocomplete list.

This separation makes the field predictable:

- Search FIO = focus the operational picture
- command palette = navigate or execute an application command

## Core Contracts

The Qt layer must consume immutable projections; it must not query several
tables independently to assemble meaning.

Suggested Qt-free contracts:

```text
OpsFocus
  kind: callsign | group | event | topic | geography | band | source
  canonical_id
  operator_id (when kind is callsign and identity is known)
  display_label
  query_text
  provenance

OpsFocusSuggestion
  focus
  primary_text
  secondary_text
  score

OpsFocusSnapshot
  focus
  generated_at
  current_scope_summary
  historical_summary
  identity
  traffic_summary
  status_summary
  schedule_summary
  awareness_summary
  map_context
  rf_readiness
  actions
  missing_data_reasons

OpsHistoricalSummary
  entity_kind
  entity_id
  latest_received_at
  latest_event_at
  latest_message_ref
  latest_observation_ref
  latest_status_at_receipt
  last_heard_by_source
  retained_source_summary
  scope_mismatch_notes
  retention_boundary
```

Build suggestions from compact entity dictionaries derived from existing
operator/group records, topic taxonomy, schedule targets, geography constants,
and source contracts. Build focus snapshots from the existing message,
observation, schedule, operator, situation, and propagation projections.

## Performance Contract

Performance is a product requirement for this feature.

- Never scan rendered Qt rows to resolve or apply focus.
- Do not execute a full dashboard refresh for each keystroke.
- Debounce autocomplete by 100–150 ms and query a compact in-memory index.
- Apply the focus only on explicit suggestion selection or Enter.
- Use a single generation/request ID so stale worker results cannot replace a
  newer query.
- Run database and propagation work off the Qt UI thread.
- Use bounded, indexed queries and batch reads; avoid one query per card or per
  callsign.
- Resolve Last Known through one-row/per-kind entity summaries or indexed
  `ORDER BY received DESC LIMIT n` probes. Never hydrate an entity's full
  message/observation history to build the Ops summary.
- Reuse `message_projection`, `observation_projection`, topic indexes, operator
  tables, schedule caches, situation projection, and `PropagationService`.
- Cache suggestions by entity-index generation. Cache focus snapshots by focus,
  age/group/source scope, projection checkpoints, and a short time bucket.
- Use stale-while-revalidate: keep the last complete focus visible with a small
  refresh indicator until the replacement snapshot is complete.
- Cancel or ignore superseded propagation/database work when focus changes.
- Optional free-text body search belongs behind a later indexed FTS projection;
  do not use `%substring%` scans across retained message bodies in the initial
  focus implementation.
- Maintain compact latest-evidence/aggregate rows incrementally when message and
  observation projections change. Store references and summary fields, not
  duplicate message bodies.
- Initial historical-summary backfill must be chunked and resumable in a
  background worker. Focus remains usable through bounded indexed probes while
  backfill is incomplete.
- The autocomplete dictionary includes unique retained entity keys and latest
  timestamps, not every historical record.
- The callsign autocomplete index maps current and former callsign keys to one
  stable operator display record. Alias resolution must not query retained
  traffic while the user types.
- History drill-down uses keyset pagination with a small page size; do not use
  large offsets or eager all-history loading.

Targets on the Linux 1920x1080 Normal Text baseline:

- autocomplete update: p95 under 50 ms warm, under 150 ms cold
- focus banner/selection feedback: under 50 ms
- cached focus snapshot: p95 under 150 ms
- uncached callsign/group/topic snapshot without propagation: p95 under 300 ms
- Last Known summary lookup: p95 under 100 ms warm and under 250 ms cold
- complete target-oriented snapshot including propagation: p95 under 750 ms,
  with non-propagation cards allowed to appear first
- no synchronous UI-thread task over 16 ms during typing or focus changes
- bounded memory: suggestion index and snapshot cache have explicit size limits

Instrumentation should record autocomplete latency, focus-build latency by
component, cache hit/miss, rows examined, stale-result drops, and UI apply time.
Do not log complete message bodies or other unnecessary sensitive content.

## State And Persistence

- The current focus is session state by default and is cleared at application
  restart. Persistent monitoring uses the existing Pin model instead.
- Age, Group, Source, and visible-card preferences retain their existing
  persistence behavior.
- Clearing focus does not clear Age/Group/Source unless the operator chooses
  `Clear All Filters`.
- Pins and focus are distinct: focus is the current lens; pins are things the
  operator wants FIO to keep watching.

## Efficient Implementation Slices

### Slice 1: Visual Language And Focus Contract

- Apply the Traffic-by-Group copy refinement.
- Define and add the small Ops icon asset family needed for autocomplete, focus,
  schedule, SOP, relay, and RF readiness; reuse existing navigation assets for
  operator, group, map, message, radio, and health meanings where appropriate.
- Add Qt-free focus/suggestion dataclasses and entity resolver.
- Consume stable operator/callsign-history resolution from Operator History;
  do not implement callsign mutation in Ops Center.
- Add the compact historical-summary contract, indexed latest-evidence queries,
  and incremental updater. Provide a bounded-query fallback while any chunked
  historical backfill is incomplete.
- Add performance instrumentation and deterministic resolver tests.
- Keep the existing quick-search command menu unchanged until the focus field is
  ready to replace it.

### Slice 2: Callsign Focus

- Add categorized autocomplete, explicit focus banner, and Clear Focus.
- Build a cached callsign snapshot from operator, traffic, status, schedule,
  map, and existing signal evidence.
- Render Current Scope and Last Known as distinct states; include retained
  read/archived evidence without adding it to current-window counts.
- Filter current cards through that snapshot and add Message/Map/Pin actions.
- Render propagation as a separately completing recommendation so it cannot
  delay the rest of the focus.

### Slice 3: Group, Topic, And Geography Focus

- Add group/event/topic/region resolvers and focused projections with the same
  Current Scope / Last Known distinction.
- Apply the same focus contract to traffic chart, awareness, schedule, peer,
  map handoff, SOP, and RF readiness.
- Move navigation/settings quick search to the command palette.

### Slice 4: Dashboard Differentiation

- Convert Schedule Outlook to the time rail.
- Convert Peer Schedule Finder to the rendezvous timeline.
- Convert Source Lanes to stable lane cards.
- Demote dense evidence tables behind Details after action and route parity is
  verified for each replacement. The peer table is removed entirely once the
  consolidated chart carries its filters, labels, count, and actions.

This sequence builds the reusable focus/data path before introducing several
new visual renderers, minimizing duplicate queries and rework.

## Acceptance Criteria

- Associated operating and membership groups remain first in Traffic by Group,
  are bold, and do not repeat `My group` text.
- Icons use one application-owned family, retain accessible names/tooltips, and
  never become the sole carrier of meaning.
- Traffic chart metadata follows the bullet-delimited sequence and remains
  interpretable when elided or read by assistive technology.
- Callsign focus produces identity/group, last-heard/status, traffic,
  schedule, map, and RF-readiness summaries without opening several tabs.
- Searching either side of a confirmed callsign change opens one operator
  focus; source evidence retains its transmitted callsign, and current actions
  use the current callsign.
- When current scope is empty but retained evidence exists, focus states both
  facts: no current-window match and an age-labeled Last Known summary.
- Historical evidence never inflates current action, unread, traffic, incident,
  or trend counts and never presents a stale report as current status.
- Last Known lookup uses indexed bounded queries or compact incremental
  summaries; Ops Center never loads all entity history.
- Event/incident focus remains discoverable after its active window and clearly
  identifies its last activity and inactive/historical state.
- Topic focus narrows all compatible Ops views to the same canonical topic and
  exposes related Messages and Map routes.
- Region/place focus can drive a persistent RF-readiness recommendation using
  modeled and observed evidence.
- Active focus and intersecting Age/Group/Source scope are always visible.
- Typing never causes a full dashboard/database/propagation refresh.
- Stale asynchronous results never replace a newer focus.
- Empty or missing data is explicit rather than silently omitted.
- Default Operations view contains visually distinct bars, cards, lanes, and
  timelines; detailed tables remain available without dominating the page.
- Peer Schedule Finder remains bounded with a 150-operator roster, exposes
  callsign/group/region/role filters, and renders one row per operator even when
  several bands or windows match.
- Scrolling and repeated card/filter clicks do not produce stale or overlapping
  peer rows, and left-column content stays top-aligned at its natural height.
- Light/Dark and Normal/Large Text layouts pass at 1920x1080, approximately
  1000x700, and approximately 900x560 without important horizontal scrolling.

## Decisions Applied

1. Unknown but valid callsigns are pinnable before they appear in the operator
   table and are labeled as not present in the operator roster.
2. Choosing a station or supported geographic focus retargets the visible
   Propagation card while leaving selected-radio and QSY state unchanged.
