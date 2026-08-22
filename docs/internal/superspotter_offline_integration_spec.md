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

### Auto SOP Invocation Policy

Condition-alert rules may optionally invoke an SOP condition level without a
separate confirmation, but only when explicitly enabled by the operator.

Defaults:

- New and built-in rules default to `prompt-to-apply`.
- `auto-apply` is never enabled by a template without operator action.
- Auto-apply is configured per rule so one group or source can remain prompt-only
  while another trusted source is automatic.

Auto-apply requirements:

- The rule is enabled and action is `auto-apply`.
- Sender, target, source family, and authentication requirements pass exactly as
  configured on the rule.
- The target operating group has a matching SOP condition layer.
- RF Guard preflight for affected active radios has no blocking conflicts.
- FIO records an audit event with source message, rule id, old condition, new
  condition, affected plans/radios, and timestamp.

UI behavior:

- Prompt mode shows a compact alert with Apply, Ignore Once, and Rule Settings.
- Auto mode shows a compact applied status in ControlFreq/Station Health with
  Undo/Review available.
- Auto-apply failures become operator-visible warnings, not silent no-ops.
- Manual Apply Level uses the same assigned-plan RF Guard preflight before the
  condition level is changed.
- Condition-alert rules should make the action wording plain: `Suggest only`,
  `Ask before applying`, and `Apply automatically`.

Implementation status:

- A core condition-alert parser/model exists for configurable rules, source
  family filters, target groups/callsigns, allowed sender modes, auth
  requirements, regex/template/contains matching, and condition-level extraction.
- MagNet MAGCON is represented as a disabled built-in template, not hard-coded
  runtime behavior.
- Condition alert rules have a core settings serialization/merge contract:
  saved rules can override built-ins by `id`, built-ins are not duplicated, and
  malformed saved rows are ignored safely.
- Matched condition alerts can be projected into the common observation model for
  later ControlFreq, Map, and SOP Builder surfaces.
- A side-effect-free ingest bridge converts message intelligence plus configured
  rules into pending condition-alert observations. The bridge preserves source
  radio/app identity and does not write to the database or block ingestion.
- Live JS8Spotter/MCF ingestion now mirrors matching condition-alert
  observations through the existing observation store path when rules are
  configured.
- Spotter and CommStat observation backfill can optionally mirror matching
  condition-alert observations when configured rules are supplied. Plain
  CommStat messages that are not otherwise report-worthy can still create a
  condition-alert observation when they match a rule.
- FLMsg/FLAmp file observation projection can optionally mirror matching
  condition-alert observations using the same parsed form intelligence already
  used for message summaries.
- Condition alert rules are editable in Settings and saved through the global
  settings path.
- Live JS8Spotter/MCF ingest, CommStat UI refresh/backfill, VarAC VMail, and
  VarAC broadcast ingest can mirror matching condition-alert observations.
- A core operational activity snapshot helper can query projected observations
  by existing filters, scope them to an operating group, and return latest rows,
  high-attention rows, condition alerts, and topic chips for ControlFreq/Map UI
  use.
- ControlFreq and SOP Builder now surface recent matching condition alerts as
  SOP review/suggestion/blocked guidance.
- SOP Builder can explicitly apply the first matching traffic suggestion after
  operator confirmation and RF Guard preflight, updating the group-scoped
  condition level through the shared condition-level helper only when assigned
  Frequency Plans remain safe.
- A side-effect-free auto-invocation planner can prepare condition-level
  updates and audit payloads only when the rule, operator safety gate, matching
  SOP layer, and RF Guard state allow it.
- A condition-SOP invocation audit table records planned/applied/blocked
  decisions as append-only rows for later operator review and undo/revert UI.
- A core auto-invocation executor can persist audited outcomes and apply at most
  one ready auto-apply condition-level update per execution pass. Prompt,
  suggest, blocked, no-change, failed, and deferred outcomes are audited without
  mutating settings.
- The background ingest path now evaluates recent condition-alert observations
  against active SOP profiles and records audited SOP invocation decisions.
  Before any unattended auto-apply mutation, the matching SOP condition-layer
  rows are checked against assigned Frequency Plans through RF Guard. Any RF
  Guard warning, block, missing profile, or failed preflight blocks unattended
  application and is audited for review.
- Deferred auto-apply observations remain retryable on later background passes
  instead of being marked complete when the one-change-per-pass limit is hit.
- Background audited/applied notifications are queued back onto the controller
  thread before UI refresh work is signaled.
- Applied and audited outcomes refresh SOP/scheduler projections and Station
  Health through the condition-level update handler. SOP Builder exposes review
  and revert for the latest reversible applied automation row.

### FastLight Filename Rules

Operating Groups need compose filename policy settings for FLMsg/FLAmp files.

Fields:

- `fastlight_filename_delimiter`: group default, underscore, hyphen, custom
- `fastlight_custom_delimiter`
- `fastlight_signed_suffix`: group default, `.sig.k2s`, `.sig.b2s`,
  `-sig.k2s`, `-sig.b2s`
- `fastlight_unsigned_suffix`: group default, `.k2s`, `.b2s`

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

Implementation status:

- Group-level policy fields are stored with HF Operating Groups.
- Compose filename generation resolves the target group's policy before staging
  FLMsg/FLAmp output.
- Unit coverage confirms MagNet underscore plus `.sig` defaults, AMRRON hyphen
  plus `-sig` defaults, and saved group overrides.

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
2. Add group-level FastLight filename policy fields, defaults, and tests. Done.
3. Wire compose filename generation to operating-group filename policy. Done.
4. Add condition-alert rule data model and parser service with tests. Done.
5. Seed disabled MagNet MAGCON template rule. Done.
6. Project matched condition alerts into observations. Core projection done;
   live/backfill storage wiring is covered by the later completed ingest slices.
7. Add condition-alert settings serialization and built-in merge tests. Done.
8. Add condition-alert ingestion matcher that converts eligible message
   intelligence into pending observations without blocking ingestion. Done.
9. Wire live JS8Spotter/MCF ingest to condition-alert observations. Done.
10. Wire Spotter and CommStat observation backfill to condition-alert
    observations when rules are supplied. Done.
11. Wire FLMsg/FLAmp file observation projection to condition-alert observations
    when rules are supplied. Done.
12. Wire live CommStat and VarAC paths to condition-alert observations. Done.
13. Add ControlFreq Operational Activity snapshot helper. Done.
14. Add ControlFreq compact activity/alert panel. Done.
15. Add Map topic/source layer refinements and RF pin records. Topic/source
    map refinements done for observation-backed alerts, infrastructure,
    condition-alert pins, and a dedicated RF Pins map focus. Core RF pin
    projection, map styling, save/list/delete helpers, and Map-side manual pin
    creation/edit/delete management are done for receive/manual review records;
    send-capable workflows remain open.
16. Add SOP Builder Traffic Suggestions and condition apply workflow. Core
    condition-alert-to-SOP decision helpers done, including batch evaluation
    against SOP profile schedule layers; ControlFreq compact activity now shows
    read-only SOP review/suggestion/blocked status for matching condition
    alerts. SOP Builder now shows a Traffic Suggestions panel for recent
    matching condition alerts and supports an explicit Apply Level action after
    operator confirmation and assigned-plan RF Guard preflight.
17. Add optional auto-SOP invocation from trusted condition alerts with audit,
    undo/review, and RF Guard gating. Side-effect-free auto-apply policy
    evaluator done, including per-profile RF Guard block handling. Settings now
    includes the explicit operator safety gate for automatic SOP invocation.
    The side-effect-free invocation planner now prepares condition-level
    updates and audit payloads. Append-only audit storage is implemented. A
    core execution helper now applies one ready auto-invocation per pass and
    audits every non-applied outcome. Live background wiring now performs
    assigned-plan RF Guard preflight before unattended mutation; warnings block
    auto-apply. Condition-level status and Station Health now surface the
    latest automation audit outcome with blocked/failed outcomes treated as
    operator review items, and audited/applied outcomes wake the UI refresh
    path. Applied outcomes also queue the scheduler refresh path through the
    condition-level update handler. Deferred observations remain retryable
    after the apply-per-pass limit, and background signals are marshaled onto
    the controller thread before UI refresh. Applied automation captures the
    previous group condition-level state, and SOP Builder now exposes a Review
    Automation dialog that can revert the latest reversible applied automation
    row; older audit rows without before-state remain review-only.

## Product Input Needed

- Confirm whether any built-in template should ever ship as auto-apply. Current
  spec says no: templates ship prompt-only/disabled until the operator enables a
  rule.
- Confirm whether MagNet default allowed MAGCON senders should come from explicit
  callsigns, roster role/tier, or both.
- Confirm which topics should be treated as ControlFreq "high-value" by default.
- Confirm whether RF pins should become send-capable later; first slice is
  receive/manual review only.
- Confirm group filename defaults beyond MagNet and AMRRON.
