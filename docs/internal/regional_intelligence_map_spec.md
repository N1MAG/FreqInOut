# Regional Intelligence Map Spec

## Center Of Gravity

The map should behave like an operator situation board, not a collection of
independent raw layers. Its primary job is to show where attention is needed,
why the area is warm, and what evidence supports the assessment.

The default experience should answer:

- What areas need attention?
- What topics are driving concern?
- How fresh and widespread is the evidence?
- Which stations or sources support the assessment?
- How can the operator drill into the exact evidence?

The operator should not need to discover the right combination of layers,
source filters, age filters, topic filters, and path controls just to see the
important picture.

## Runtime And Data Safety Rules

Regional intelligence is high-risk if it reads the wrong data. Development and
diagnostics must follow the repository `AGENTS.md` rules.

- Confirm the active FIO runtime profile before making data claims.
- In Bill's multi-rig lab, the usual runtime root is
  `/Users/bill/RadioCode/runtime/multi-rig/config`.
- `freqinout.db` owns settings, radio profiles, app paths, launch control, and
  configured app instances.
- `freqinout_nets.db` owns traffic, messages, observations, operator/map data,
  schedules, and projected evidence.
- Do not infer current multi-rig behavior from `~/.freqinout/config` unless the
  active runtime has been verified to use it.
- Map, Messages, and regional intelligence must agree after refresh, delete,
  and metadata rebuild operations.

## Product Model

Replace the mental model of "turn map layers on and off" with a small set of
operator modes:

- Recent Traffic: recent report/message activity.
- Regional Intelligence: state and FEMA-region concern heat map.
- Station Status: latest station/SitRep/CommStat status.
- Paths: reachability, JS8 links, propagation, and peer schedule.
- Planning: RF planning and operator-created planning pins.

Raw layer toggles remain available in an Advanced Layers drawer, but they are
not the primary operator workflow.

## Regional Intelligence View

Regional Intelligence aggregates normalized evidence into state and FEMA-region
concern levels.

Visual behavior:

- State fill color reflects aggregate concern.
- FEMA regions are outlined or summarized.
- Pins/clusters are optional supporting evidence, not the main discovery method.
- Clicking a state or FEMA region opens a summary with drivers and evidence.
- The map title must state the active interpretation, for example:
  `Regional Intelligence | Active | All Topics | All Regions`.

Concern levels:

- gray: insufficient data.
- green: normal or low concern with recent reassuring evidence.
- blue: activity observed, no clear concern.
- yellow: watch, weak signal, isolated issue, or low-confidence signal.
- orange: active concern, multiple reports, or serious topic.
- red: severe, spreading, high-confidence, or many independent reports.

Green must not mean "no data." No data is gray/neutral.

## Evidence Classes

Every source should be normalized before scoring.

- Impact: FLMSG/FLAMP, Spotter, CommStat/SitRep, local reports, future mesh
  incident reports. Can directly raise concern.
- Status: structured status such as power down, water contaminated, comms
  degraded, all clear, or green status. Can raise or reduce concern.
- Signal: JS8Call non-message activity, directed traffic, relay volume, silence,
  and future mesh traffic volume. Supports trend and confidence; should not
  alone turn an area red.
- Path: JS8 links, propagation outcomes, peer schedule, mesh reachability.
  Supports operational decisions and confidence.
- Planning/Admin: RF pins, schedules, SOP references. Does not raise concern
  unless explicitly marked as active impact evidence.

## Normalized Evidence Item

The implementation should normalize toward this shape:

```text
EvidenceItem
- id
- source_family
- source_ref
- evidence_type
- topic
- severity_hint
- confidence
- event_time
- received_time
- reporter_callsign
- target/group
- state
- grid
- fema_region
- summary
- detail_ref
- resolved_hint
```

## Time Model

Regional Intelligence must not use one hard age cutoff. A report from 8 hours
ago can still matter if a related report arrives 2 hours ago. Old issues should
fade unless new evidence reactivates them.

Sensitivity presets:

- Current: 0-6h strong, 6-24h weak.
- Active: 0-6h strong, 6-72h moderate, 3-7d weak. Default.
- Extended: 0-24h strong, 1-7d moderate, 7-14d weak.

Topic memory differs:

- Fire, hurricane, winter storm, grid failure, water contamination, and medical
  issues retain active memory longer.
- Routine check-ins and all-clear reports decay quickly.
- Not Reported has no concern contribution.
- SitRep red/yellow status older than 7 days is stale for active station or
  regional status. It remains available in history, but it should not keep an
  active map area hot without a newer confirming update.

## Scoring

Scoring is by geography and topic, then rolled up to states and FEMA regions.

Inputs:

- topic severity.
- report count.
- unique reporting stations.
- freshness/decay.
- worsening or improving trend.
- multi-grid or multi-state spread.
- confirming or contradicting status reports.
- source confidence.
- supporting signal/path evidence.

Stored topic tags alone must not increase concern. A topic must have visible
report evidence or validated structured status. This follows the current
message/map topic filter direction.

## JS8Call And Mesh

JS8Call non-message traffic should contribute as signal/path evidence:

- regional activity spikes.
- sudden silence from normally active stations.
- directed traffic increases.
- relay pattern changes.
- path availability into affected regions.

JS8 signal evidence should raise watch/confidence, not confirmed incident
severity by itself.

Future mesh traffic should feed the same normalized evidence model as one more
source family:

- structured mesh reports become impact/status evidence.
- mesh route/activity data becomes signal/path evidence.

## UI Requirements

Primary controls:

```text
Mode
Sensitivity
Topic
Scope
Search
Reset View
```

Advanced controls:

- raw layers.
- state/source/status/trust filters.
- path scope.
- planning pins.
- debug/diagnostic controls.

The right panel should show national, state, or FEMA-region intelligence:

- concern level.
- drivers by topic.
- current reports.
- active context reports.
- reporting station count.
- newest evidence.
- trend.
- actions: Open Messages, Show Stations, Show Paths, Focus Topic.

## Performance

Regional Intelligence must be fast enough to feel interactive.

- Pre-aggregate by state, FEMA region, topic, and sensitivity.
- Keep browser payloads small.
- Load detailed evidence lazily after click.
- Cache rollups with a short TTL or persisted summary table.
- Invalidate rollups after ingest, delete, metadata rebuild, or local report
  update.

## Deletion And Refresh Contract

When a source message/report is deleted:

- Message row disappears.
- Metadata cache is removed or rebuilt.
- Observation projection is removed.
- Regional evidence item disappears.
- State/FEMA scores are invalidated.
- Map and Messages agree after refresh.

## MVP Implementation Plan

1. Add a core regional intelligence service that aggregates existing
   `observation_projection` rows into state and FEMA-region rollups.
2. Implement decayed scoring for Current, Active, and Extended sensitivity.
3. Classify impact/status/signal evidence with conservative source defaults.
4. Produce explainable drivers and a limited evidence list per rollup.
5. Add tests for decay, multi-station aggregation, no-data behavior, green/all
   clear behavior, and JS8 signal limits.
6. Wire the map UI to a new Regional Intelligence mode in a small follow-up
   slice once the core model is stable.

## Implementation Status

Implemented:

- Core state and FEMA-region rollups from `observation_projection`.
- Current, Active, and Extended sensitivity windows.
- Conservative evidence classification for impact, status, signal, path, and
  planning/admin sources.
- Visible-evidence topic requirement so stored tags and Not Reported fields do
  not create false topic hits.
- Regional Intelligence map mode with state heat-map fill, legend entries, and
  clickable state summaries.
- Compact Regional Intel overview on the map with plain-language counts such as
  `8 reports from 3 stations`.
- Right-panel regional detail with topic drivers, trend, source mix, evidence,
  and actions to center or open messages.
- Compact top control bar using one View selector, with legacy view buttons kept
  in Map Tools.
- Ready-state map diagnostics card now hides repair/debug buttons until the map
  is warming, loading, or degraded.
- Regional Intel overview is collapsed by default and can be expanded from the
  map without changing filters or selection.
- Regional Intel message routing carries structured state/FEMA-region scope plus
  the active topic/group/age context.

Implemented in the operator-map completion pass:

- Raw layer/filter controls are secondary in Advanced Map Tools; the main map
  bar carries the normal operator controls.
- National Regional Intel can be selected from the summary and opens a right
  panel summary instead of using a literal `National` inbox search.
- FEMA-region message routing carries structured region scope plus current
  topic/group/age context.
- Regional overview is collapsed by default, hides green rows from the summary,
  caps visible rows, and shows overflow counts for busy filters.
- Browser payloads carry compact evidence lists. Deeper lazy evidence retrieval
  is deferred until live payload size shows a need.

Follow-on work:

- Continue field-testing direct FEMA-region message routing against live
  CommStat and Spotter data.
- Add mesh traffic as another normalized evidence source once that integration
  exists.
