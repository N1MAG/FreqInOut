# SuperSpotter Offline Integration Spec

## Intent

FIO should absorb the strongest offline operational ideas from JS8SuperSpotter
2.3 without copying its monolithic Tk UI or adding online service dependency.
The goal is a unified operator workflow across Messages, Map, ControlFreq, and
SOP Builder:

- Messages remains the detailed triage and compose surface.
- Map shows spatial operational awareness from every ingest source.
- ControlFreq shows what matters now for the selected operating context.
- SOP Builder turns received traffic into actionable "what to do when I am
  there" guidance.

This work must reuse FIO's existing core projections:

- `MessageIntelligence` for message summary, topics, routing hints, and key
  fields.
- `Observation` for map, BBS, and operational activity projections.
- existing message metadata caches and ingestion stores so UI refreshes never
  perform heavy parsing.

## Reviewed SuperSpotter Concepts

JS8SuperSpotter 2.3 is largely one Tk application, but several offline features
are directly relevant to FIO:

- Expect System: stores request/reply rules with allowed callsigns/groups,
  limits, last-sent tracking, auto-TX flags, and MCForm response posting.
- MCForms: parses `F!xxx` JS8Spotter forms, extracts sender, target, compact
  response values, text, timestamps, and six-character Maidenhead locators.
- CommStat receive views: records StatRep/Brevity traffic and maps reports.
- Activity and search: keeps watch terms with contains/whole-word matching and
  updates "Matched Activity" rows when incoming traffic matches.
- Roster: imports region, callsign, name, role, tier, state, grid, and handle.
- Map pins: supports RF-broadcast pins with type, grid, description, sender,
  origin, and resend/delete workflows.
- Map overlays: shows JS8 activity, MCForm halos, CommStat rings, roster map,
  latest windows, area selection, and optional user pins.
- Condition alerting: hard-coded MAGCON detection watches JS8Call and VarAC
  broadcast text for `MAGCON+1` through `MAGCON+5` from allowed senders to
  allowed MagNet groups.
- Email gateway and APRS email exist in SuperSpotter but are out of current FIO
  scope.

## Non-Goals

- Do not integrate email, internet gateway, or online propagation services in
  this slice.
- Do not copy SuperSpotter's map tile download model. FIO's existing map remains
  primary.
- Do not create a second activity database parallel to FIO observations.
- Do not make auto-TX behavior easy to enable accidentally. Expect auto-reply
  remains explicit, reviewable, and policy-controlled.
- Do not hard-code MagNet-only condition logic into FIO.

Future-facing notes:

- Email-like workflows may later map to Reticulum, mesh MQTT, or other offline
  transports, but they should enter through the same message/observation model.
- VarAC remains read/import/monitor unless a future VarAC API supports safe
  direct automation.

## Target Data Model

### Operational Activity

Add a lightweight operational activity projection fed by `Observation` records.
It should be queryable by:

- source family: JS8Call, JS8Spotter, CommStat, FLMsg, FLAmp, VarAC, Local
- operating group family and explicit target
- topic
- callsign
- state/grid
- age window
- selected radio or all active radios

This is not a new raw ingest table. It is an operator-facing projection for
ControlFreq and Map, backed by indexed observations and cached message metadata.

### Map Pins

Represent pins as observations or observation-adjacent records:

- `pin_type`: hazard, checkpoint, supply, medical, comms, shelter, road,
  info, custom
- `grid`, optional `lat/lon`
- `description`
- `group`
- `added_by`
- `origin`: manual, RF, import
- `source_ref`
- `created_utc`
- optional expiry/age window

Pins received over RF should be stored once, deduped by sender/grid/type/text,
and displayed through the same map topic/layer controls used by reports.

### Condition Alert Rules

Add configurable condition-alert rules rather than hard-coded MAGCON handling.

Rule fields:

- `id`
- `enabled`
- `name`
- `operating_group`
- `source_families`: JS8Call, VarAC, CommStat, JS8Spotter, FLMsg, FLAmp
- `target_groups`
- `target_callsigns`
- `allowed_sender_mode`: explicit list, roster group, roster role, roster tier,
  trusted operator, any sender
- `allowed_senders`
- `required_auth_state`: none, signed, signed-and-trusted
- `match_mode`: contains, whole-word, regex, template
- `pattern`
- `level_extraction`: fixed level or regex capture mapping
- `action`: suggest, prompt-to-apply, auto-apply
- `scope`: station, operating group, subgroup
- `notes`

Initial defaults:

- Provide a disabled MagNet MAGCON template using `MAGCON+([1-5])`.
- Recommend `prompt-to-apply`, not auto-apply.
- Allow the operator to map senders from HF Operators roster role/tier or from
  an explicit callsign list.

When a rule matches:

1. Store a condition alert observation.
2. Surface a compact alert in ControlFreq for the affected group/radio context.
3. Offer "Apply Condition Level" in SOP Builder or the alert detail.
4. If applied, update the configured group's condition level and refresh SOP
   projections.
5. Preserve source provenance so the operator can review the original traffic.

### FastLight Filename Rules

Operating Groups need compose filename policy settings for FLMsg/FLAmp files.

Fields:

- `fastlight_filename_delimiter`: underscore, hyphen, custom
- `fastlight_custom_delimiter`
- `fastlight_signed_suffix`: `.sig.k2s`, `.sig.b2s`, group default
- `fastlight_unsigned_suffix`: `.k2s`, `.b2s`, group default

Defaults:

- MagNet: underscore delimiter and `.sig.k2s` or `.sig.b2s`.
- AMRRON: hyphen delimiter and `-sig.k2s` or `-sig.b2s`.
- Unknown group: use the current FIO default and show the generated filename
  preview before staging.

Compose behavior:

- The target operating group drives the delimiter.
- If the target is a callsign, use the selected sending context's operating
  group policy.
- If multiple group policies could apply, require the operator to choose one
  before staging.
- The preview must show the exact output filename and any signed companion file.

## UX Model

### Messages

Messages remains the deep workbench:

- source-focused views: All, New, FLMsg/FLAmp, Spotter, CommStat, JS8Call,
  VarAC
- topic search and filters
- human-readable row summaries
- "View on Map" for mappable reports
- Expect save/review for JS8Spotter drafts
- MsgAuth verification state only when a signature exists

SuperSpotter's long form lists should not become long dropdown filters in FIO.
Use human names such as `Wildfire | F!307`, `Net Check-In | F!103`, and
category/topic filters instead of exposing every form code as a primary control.

### Map

The FIO map should become the spatial counterpart to Messages:

- topic layer chips using the same taxonomy as Messages
- source chips for Spotter, CommStat, FLMsg/FLAmp, JS8Call, VarAC, Local
- visual distinction between report markers, station activity, map pins,
  MCForm halos, CommStat rings, and roster operators
- "latest" windows such as 30m, 2h, 24h, 72h
- "View HF Reports Map" from Messages opens Map pre-filtered to HF report
  observations
- local reports stay separate from HF reports but use the same topic vocabulary
- state-only observations roll up to state/area summaries rather than pretending
  to be precise points

SuperSpotter's map pin idea is worth adopting, but FIO should render pins as a
first-class map layer using FIO theme styling.

### ControlFreq

ControlFreq should not duplicate Messages. It should show a compact
"Operational Activity" surface scoped to the selected radio, assigned plan,
current group/band, and nearby operating groups:

- latest high-value reports
- active topic chips for traffic heard in the selected time window
- matched terms that affect the current plan/group
- condition-alert suggestions
- recent heard operators or relay links when operationally relevant
- one-click jump to Messages or Map with the same filters applied

This gives the operator an immediate answer to:

- What changed while I was on this frequency?
- Is anything actionable for this group?
- Should an SOP condition level change be considered?

### SOP Builder

SOP Builder consumes operational activity and condition alerts:

- "Traffic Suggestions" pane lists actionable observations by group/topic.
- Condition-alert matches can be reviewed and applied to a group's condition
  level.
- Applied condition levels immediately re-project SOP actions and assigned
  plans.
- SOP exports include rule metadata only when the operator explicitly exports
  condition-alert policy.

The center-of-gravity loop is:

1. Ingest traffic.
2. Message Intelligence assigns topics and key fields.
3. Observations expose mappable/actionable records.
4. ControlFreq and Map surface context.
5. SOP Builder turns context into "what to do when I am there."

## Performance Requirements

- Ingest and file scan workers parse and cache message intelligence outside the
  UI thread.
- ControlFreq and Map consume immutable snapshots or query projections with
  bounded limits.
- Topic matching is compiled/cached in core helpers.
- Map refresh is rate-limited and diff-applied where practical.
- Multi-radio source identity is preserved through every projection.
- Background scans must never block QSY, scheduler, or station command bar
  refresh.

## Implementation Slices

1. Add this spec and cross-link it from the SOP schedule plan spec.
2. Add group-level FastLight filename policy fields, defaults, and tests.
3. Wire compose filename generation to operating-group filename policy.
4. Add condition-alert rule data model and parser service with tests.
5. Seed disabled MagNet MAGCON template rule.
6. Project matched condition alerts into observations.
7. Add ControlFreq Operational Activity snapshot helper.
8. Add ControlFreq compact activity/alert panel.
9. Add Map topic/source layer refinements and RF pin records.
10. Add SOP Builder Traffic Suggestions and condition apply workflow.

## Product Input Needed

- Confirm initial condition-alert action should be `prompt-to-apply` only.
- Confirm whether MagNet default allowed MAGCON senders should come from explicit
  callsigns, roster role/tier, or both.
- Confirm which topics should be treated as ControlFreq "high-value" by default.
- Confirm whether RF pins should be send-capable in the first map-pin slice or
  receive/manual-only first.
- Confirm group filename defaults beyond MagNet and AMRRON.

