# Map Operator Interaction Spec

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

## Center Of Gravity

Primary map views are operator tasks, not raw layers. Raw layers remain
available as advanced tools, but the main workflow should be a small set of
smart combinations.

- Regional Intel: concern by state and FEMA region.
- Recent Traffic: last 24 hours of meaningful traffic.
- Station Status: stations with known latest status.
- Paths: observed radio topology and reachability.
- RF Planning: path-to-station planning, peer schedule, and propagation.
- Planning Pins: operator-created planning context.

The default age for map traffic views is 24 hours unless a specific view has a
strong reason to use another window. The selected age control and displayed map
content must always agree.

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

Click behavior:

- clicking a state/region always opens the matching right panel.
- right panel must match the clicked geography, not the last popup.
- Messages opens the matching non-green report evidence for the active age,
  topic, group, and geography filters.

### Recent Traffic

Recent Traffic is the default live review view. It should use a 24 hour age
window by default and only show records inside that window.

Click behavior:

- clusters summarize newest reports, sources, groups, topics, and status.
- report popups and the right panel use the same payload.
- Messages opens the matching reports for the active filters.

### Station Status

Station Status is not station inventory. It should only show stations with a
known latest status. Unknown/no-report stations belong in All Stations.

Default display:

- known green/yellow/red status pins.
- current status legend shown by default.
- no large table as the primary experience.

Click behavior:

- show callsign, name, group, state/grid, latest status, status source, updated
  age, current/next schedule if known, and recent report summary.

### Paths

Paths is topology-first. Users want to see who can hear whom.

Default display:

- station pins.
- directional path links when useful.
- rounded SNR values.
- legend describing link color and direction.

Path To:

- can be launched from a selected station.
- if peer schedule is known, it supersedes propagation.
- if peer schedule is unknown, propagation suggests likely bands.

### RF Planning

RF Planning should support operating choices without covering the map with raw
text labels.

Default display:

- topology and useful reachable stations.
- propagation colors or recommendations only when selected.
- peer schedule available as the strongest planning input.

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

## Implementation Plan

1. Clean current UX mismatches: green Regional Intel list noise, report Status
   terminology, Station Status unknown pins, rounded SNR.
2. Stabilize selection actions so right panel, popups, Messages, and Compose use
   the same payload and active filters.
3. Upgrade station action cards with roster data, schedule context, source mix,
   recent reports, and a `Compose Message` action.
4. Add contextual `Show Paths` and `Path To` behavior with peer schedule first
   and propagation fallback second.
5. Make legends view-aware and shown by default, with a simple hide action.
6. Keep Advanced Map Tools available but secondary, with visible active-filter
   state and simple recovery from confusing combinations.

## Acceptance Tests

- Regional Intel summary list excludes green rows by default.
- Green evidence can still lower concern or support trend internally.
- Station Status mode excludes unknown/no-report stations.
- Report detail uses `Status`, not `Severity`.
- SNR tooltips and labels are rounded.
- Clicking a station opens details with roster/status/schedule data when known.
- Compose Message opens Compose with the selected callsign prefilled.
- Messages from Regional Intel are filtered by geography, topic/group, age, and
  non-green evidence.
- Advanced filters can be cleared and do not permanently corrupt view state.
