# FIO Spotter Operator Workflow Refinement Specification

Status: implementation complete — automated exit gate passed; production Linux visual/RF confirmation remains operator-assisted

Date: 2026-09-13

Governing contracts:

- `project_delivery_rules.md`
- `multirig_product_ui_contract.md`
- `task_oriented_workspace_design_guideline.md`
- `production_reliability_and_workflow_remediation_spec.md`
- `message_intelligence_projection_spec.md`
- `compose_messages_workbench_spec.md`
- `superspotter_offline_integration_spec.md`

## Purpose

FIO Spotter is a top-level local-RF observation and automatic-response service.
It should feel intelligent without presenting the operator with its internal
state machine. This refinement makes the common workflow small and predictable:

1. understand meaningful locally heard Activity;
2. save or watch an operator, group, topic, status, or useful combination;
3. maintain reusable E? responses and decide which may answer automatically;
4. define who may ask in one dedicated Access Policies workspace; and
5. select a saved response or form, review its current payload, and send it
   through the existing guarded JS8Call path.

Raw projection fields, compatibility flags, database keys, and duplicate
permission switches remain implementation detail. Message Intelligence remains
the shared interpretation layer for Activity, Watches, Messages, Map, and Ops
Center.

## Task-Oriented Redesign Brief

This implementation applies `task_oriented_workspace_design_guideline.md` to
the complete Spotter workflow. The service shares one visual vocabulary through
`freqinout/gui/theme.py`; no Spotter screen owns a separate palette or cloned
control treatment.

### Primary tasks and outcomes

- **Activity — Understand RF traffic:** start with the newest bounded local-RF
  projection; finish by understanding its meaning or routing a selected item to
  Inbox, Map, Operator, Reply, or a reviewed Watch draft.
- **Watches — Define what deserves attention:** start from an Activity
  suggestion or saved watch; finish with one understandable, bounded match rule
  whose enabled state and criteria are visible.
- **Expect — Manage reusable replies:** start from a saved response or new E?
  token; finish with a saved-only response or a safely authorized automatic
  reply, with manual Send available through guarded Compose.
- **Access Policies — Define who may ask:** start from a new or reusable policy;
  finish with an auditable caller/group/radio authorization that clearly shows
  its saved-response usage.
- **Forms — Decide how forms participate in FIO:** start from the configured
  bounded catalog; finish with reviewed routing mappings, Compose, or a staged
  Expect response.
- **Imports — Reconcile external JS8Spotter data:** start from an explicitly
  selected database; finish only after a bounded preview and confirmed import.
- **Spotter Compose — Prepare and send a response:** start from a saved response
  or catalog form; finish after radio, target, fields, exact payload, and guarded
  Send review.

### Archetypes and normal sequences

- Activity uses **dominant table with contextual inspector**:
  `meaning/filter → traffic → selected evidence → contextual action`.
- Watches uses **dominant table with contextual inspector** on wide displays and
  task-ordered stacking on compact displays:
  `saved watches → selected criteria → review state → save`.
- Expect and Access Policies use **editor above bounded list**:
  `service/selection context → selected editor → save/operational action → saved list`.
- Forms uses **dominant table with contextual preview**:
  `catalog source → mappings → selected preview → save/compose/Expect action`.
- Imports uses a **guided workflow**:
  `choose source → preview bounded impact → resolve warnings → confirm import`.
- Spotter Compose uses a **guided workflow**:
  `sending radio → target → source response/form → message fields → preview → send`.

### Shared hierarchy, disclosure, and styling

The selected radio, target, response, policy, watch, or form is stated once and
remains stable during background updates. Primary work receives the remaining
viewport; Refresh, Help, history, diagnostics, raw evidence, advanced limits,
and import details are secondary. Safety state, access, target, unsaved changes,
and disabled-action reasons are never hidden.

All primary, secondary, selected, destructive, warning, disabled, focus, table,
chip, splitter, typography, and control-size treatments use the application-wide
theme or shared helpers. Any missing semantic treatment is added centrally with
Light/Dark and Normal/Large Text support before Spotter consumes it. Local
styles may contain layout-only rules derived from the resolved theme; hard-coded
Spotter colors, fonts, text-bearing heights, and locally cloned shared controls
are prohibited.

### Responsive, lifecycle, and acceptance boundary

Wide, medium, and compact layouts preserve the task sequences above. The page
owns ordinary vertical overflow; only bounded tables, previews, evidence, or
chip rails own local scrolling. Selection, draft, expanded state, and scroll
position survive refresh publication unless their item was removed. Resize,
paint, theme, hover, selection, chip layout, filter typing, and field typing are
cache-only and perform no filesystem, database, process, endpoint, device, or
network I/O.

The slice must exercise Activity, Watches, Expect, Access Policies, Forms,
Imports, and Spotter Compose in populated/empty/selected/loading/error states as
applicable; Normal/Large Text; Light/Dark; 1920x1080, 1280x720, 1000x700, and
900x560; long values and bounded maximum selections. The exit gate rejects
clipping, overlap, page horizontal scrolling, competing same-axis scroll traps,
theme divergence, stale selection publication, geometry feedback, or any
regression in existing access, target, RF safety, persistence, or bounded-query
behavior.

## Operator Vocabulary And Navigation

The browser-tab order is:

1. `Activity`
2. `Watches`
3. `Expect`
4. `Access Policies`
5. `Forms`
6. `Imports`

The normal UI uses these terms consistently:

- **Expect service**: the station-wide operational service, shown as `On` or
  `Paused`. Pause is a persistent master safety hold; it is not a second rule
  permission.
- **Auto reply**: the single visible per-response automation choice.
- **Saved only**: a response retained for manual use that never answers an E?
  request automatically.
- **Access policy**: the reusable definition of who may request a response and,
  where applicable, which JS8 destination groups may receive it.
- **Send now…**: a guarded manual workflow that opens a concise destination,
  radio, payload, and confirmation surface. The ellipsis indicates that the
  action cannot transmit without reviewing a target.

The words `unattended auto reply`, `entry enabled`, legacy source IDs, JS8
instance IDs, and raw schedules do not appear in the normal response editor.
They may appear in diagnostics/help when necessary.

## Expect Response Model

An Expect row is a reusable saved response. Its operator-visible state is:

- `Auto reply on`: an eligible matching request may be answered according to
  the selected access policy and all runtime/RF safety gates;
- `Saved only`: available for manual sending but not for automatic reply; or
- `Needs access`: auto reply was requested but no safe caller/group access can
  be resolved.

Presence in Expect never by itself authorizes a transmission. The selected
access policy controls requester authorization. The persistent station-wide
Expect service pause, endpoint/source match, cooldown, maximum reply count,
durable claim/dedupe, selected-target clearing, JS8 busy/PTT state, RF Guard,
schedule ownership, and send preflight remain cumulative.

### Legacy compatibility

The existing `enabled`, `auto_reply_enabled`, and
`unattended_auto_reply_enabled` columns remain in place during this refinement.
No destructive migration or table rebuild is permitted.

The effective legacy state is conservative:

`auto_reply_on = enabled AND auto_reply_enabled AND unattended_auto_reply_enabled`

If any required legacy flag is false, the new UI initially presents `Saved
only`; opening the new UI must never activate an old row. A new targeted control
API changes all compatibility flags consistently when the operator deliberately
changes the one visible `Auto reply` control. Manual use of a saved response is
permitted regardless of these flags.

### Expect list and editor

The response list leads with human identity and only the information needed to
choose an action:

- selection checkbox;
- E? token and form name when known;
- concise response summary;
- access-policy name;
- `Auto reply on`, `Saved only`, or `Needs access`;
- last updated; and
- `Send now…`.

Protocol details, reply limits, cooldown, exact radio scope, signing, and audit
belong in the selected-row detail/editor. The list supports filters for `All`,
`Auto reply on`, `Saved only`, and `Needs attention`. Filtering operates over
one bounded loaded page and performs no query during each keystroke or paint.

The primary toolbar contains `New response`, `Auto reply on`, `Set saved only`,
`Update form dates…`, and `Delete`. Bulk actions operate on selected IDs; `all`
means all rows in the current bounded filtered result, never an unbounded hidden
catalog.

Turning auto reply on validates, before mutation:

- a nonempty, structurally usable static response or supported dynamic rule;
- a resolvable access policy or safe entry-specific access;
- an enabled referenced policy;
- valid source/radio scope; and
- existing dynamic-Q prerequisites when the token is `Q`.

Invalid rows are skipped and reported by name and reason. Bulk changes use one
short transaction, mutate only intended compatibility flags, and write one
management-audit record per changed response. Repeated clicks are generation-
fenced or disabled while the transaction is in flight. Exactly one bounded
refresh follows commit.

## Access Policies Workspace

`Access Policies` is the only full reusable-policy editor. Expect contains a
compact policy selector and resolved-access summary, plus an explicit `Manage
policies` route to this tab.

The policy workspace groups controls by operator meaning:

### Who may ask

- Anyone (`*`)
- Specific callsigns from bounded Operator History lookup or explicit entry
- Query groups, meaning JS8 group destinations authorized for a group request
- All trusted operators
- Trusted operators from selected Operating Groups

### Always blocked

Explicit blocked identities take precedence over every allow mechanism.

### Where it applies

- All JS8 radios, recommended
- Selected known FIO radios

### Used by

Each policy shows a bounded count and response chips/list. A policy referenced
by any response cannot be silently deleted or detached. The normal action is
blocked with a route to reassign those responses. Any later detach/replace
workflow requires an explicit impact preview and separate specification.

Policy reads are bounded. Selection, token chips, resizing, theme changes, and
painting perform no database, filesystem, process, endpoint, or network I/O.
Policy save/delete mutations are atomic and audited.

## Policy-First Expect And Compose View Workflow

The normal Expect editor treats a saved response as content plus one named
authorization policy. New automatic replies require a selected, enabled Access
Policy. Caller lists, query groups, trusted-operator groups, blocked callers,
and radio authorization are maintained in that policy rather than repeated in
every new response. A response without a policy may be saved and sent manually
as `Saved only`, but its `Auto reply` action remains unavailable with a direct
`Choose or create policy` route.

Existing entry-specific access remains valid and unchanged. The UI identifies
it as `Legacy inline access`, summarizes its effective authorization, and offers
an explicit `Create policy from this access` conversion workflow. Opening,
viewing, or saving unrelated response fields never converts, clears, broadens,
or disables that legacy authorization. No destructive schema migration or
automatic policy creation is permitted.

The selected response editor uses this task order:

1. E? token and response identity;
2. selected Access Policy chip or `Policy required` state;
3. `Saved only` / `Auto reply on` choice;
4. compact option chips for effective radio scope and rate limits; and
5. explicit Save, View, Send, New, and destructive actions.

Radio and rate-limit editors are progressively disclosed under `Options` or
`Advanced options`. Their collapsed summary remains visible, including values
such as `All JS8 radios`, `1 reply`, and `No cooldown`. Required safety state
and the reason Auto Reply is unavailable are never hidden.

The runtime header is a calm compact summary. It presents semantic chips for
`Expect service: On` or `Expect service: Paused` and `FLAMP Q: Ready`, `Waiting
for scan`, or `Needs attention`, including a bounded rule count where useful.
Color is supplementary; every chip includes text, accessibility metadata, and
a direct detail or corrective-action route. Pause is immediate. Resume clearly
states that eligible automatic responses will restart.

Every ordinary saved entry has an explicit `View` action. For a static MCF
response, View opens Message Compose with the exact stored Expect response and,
when safely decodable against the current catalog definition, populated MCF
fields. Compose identifies the source entry and provides `Back to Expect`,
`Edit working copy`, and guarded `Send now…` actions. Viewing does not update a
datecode, serialize a replacement, or write storage. Editing changes only a
working copy until `Save to Expect` is explicitly chosen. Returning restores
the same selected Expect entry.

If the current form definition cannot reconstruct a stored response without
loss, Compose displays the exact stored payload with a compatibility notice and
does not reinterpret or overwrite it. Dynamic FLAMP Q is request-derived and
therefore opens its rule editor in Expect rather than a fictional Compose form.

`New response…` begins in Expect and offers `From an MCF form`, `Text response`,
and `FLAMP Q rule`. The MCF path opens the shared Compose form editor and returns
the saved result to Expect. A new MCF response is `Saved only` until the operator
assigns a named policy and deliberately enables Auto Reply. The Forms workspace
action `Make available by E?…` enters this same flow; it does not create a
second form or persistence implementation.

All explicit View/Create/Save transitions carry an immutable entry/form intent.
Selection, resize, paint, theme, chip layout, filter typing, field typing, and
preview refresh remain I/O-free. An explicit View may perform one indexed,
SQL-bounded load during intentional navigation and must not repeat that load on
render or field changes. If production data cannot meet the interaction latency
budget, the same bounded load moves to a generation-fenced worker while Compose
retains a stable shell.

Dark-theme qualification covers line, combo, numeric, decimal, date, time, and
date/time editors, including suffixes and step controls in enabled, disabled,
focused, and selected states. These controls use the shared application theme;
Expect does not carry a local color workaround.

### Editor-first responsive layout

The Access Policy editor is above the saved-policy list so the operator first
sees the controls that explain and change the selected policy, then the full-
width comparison table. At wide widths the editor uses two balanced columns:
caller authorization on the left and block/scope/status information on the
right. At compact widths those columns stack and the page owns vertical
overflow; the application page never gains a horizontal scrollbar. The saved-
policy table receives all remaining height and remains the dominant review
surface.

Expect follows the same editor-first model. Its runtime summary remains first,
the selected saved-response editor is immediately below it, and the bounded
saved-response list follows at full width. The editor uses two columns above
1000 px and one stacked column at or below 1000 px. Token-chip growth updates
only local geometry and does not query a store or rebuild the page. On a wide
desktop the guidance/lookup summaries share one row and all six response
actions share one row. This density requirement leaves a useful portion of the
saved-response table visible at the logical equivalent of a 2392x1354 high-DPI
display; action controls must not push the table below the fold.

Access Policy lookup and usage evidence occupies a dedicated full-width
summary row below the two editor columns. It must not share competing wrapped
rows inside the scope form. Summary labels receive their natural wrapped
height, never overlap, and trigger only local geometry invalidation when their
cached values change.

Watches intentionally retain a table-and-editor comparison layout at wide
widths, but the table receives roughly three quarters of the available width.
The watch type, pattern, and match mode form one primary-condition row; Enabled
is folded into the editor action row; and the optional second condition remains
an explicit `AND (optional)` row. At compact widths the table and editor stack.
Large-font size hints must not force the service page back into a wide layout.
At 1200 px and below Watches stacks the table above the editor. At wider sizes
the action row and editor form are top-packed at their natural heights; no
expanding spacer may scatter fields vertically through the editor.

## Manual Use And Compose

FIO Spotter Compose starts with one choice:

`Start from: Saved response | Form catalog`

Selecting a saved response loads a working copy. It does not edit the stored
response, its access policy, auto-reply state, request history, cooldown, or
limits. Selecting a form starts a new response from the known form catalog.
Policy administration remains in FIO Spotter and is not duplicated in Compose.

The visible task sequence is:

1. Send from
2. Send to
3. Saved response or form
4. Message fields
5. Preview and Send

An Expect row's `Send now…` action deep-links into this same Compose workflow
with the response selected. It is never a direct database-to-radio shortcut.
The operator must review or provide a callsign/group destination and sending
radio. The existing selected-target, endpoint/source, schedule, busy/PTT,
RF Guard, signing, queue, and duplicate-click protections remain authoritative.
Dynamic `Q` responses do not expose generic manual Send because their content
depends on a received Q identifier and current reconciled FLAMP state.

### Send-time date and signing

For an eligible static MCForm response, manual send creates an outgoing copy and
refreshes its recognized form date/datecode at the final send-preparation
boundary. The date is computed once using current time before signing; any
signature is generated over the refreshed payload. Preview and queued payload
must be identical for that send generation.

The stored Expect response remains unchanged. Stored-response maintenance is
owned by the explicit bulk `Update form dates…` action. Signed, malformed,
ambiguous, mismatched, dynamic, or unsupported payloads are not rewritten. The
operator receives a clear hold/block reason rather than an unsafe best guess.

## Compose Responsive Layout

The screenshot-proven narrow fixed setup rail is removed. The Spotter mode uses
content-derived controls and progressive disclosure:

- Wide: a compact setup column and dominant form/message workspace.
- Medium: balanced setup and form regions; long names expand or elide with a
  full tooltip without squeezing primary actions.
- Compact: setup stacks above the form; preview becomes a collapsible drawer or
  lower panel; the primary Send action remains reachable while fields scroll.

Target-state guidance wraps below the target actions instead of competing with
buttons in one row. The `Configure in FIO Spotter` action is secondary and does
not occupy the primary targeting path. Mode-irrelevant controls are hidden, not
left as blank fixed-height regions.

All supported sizes pass Normal and Large Text in light and dark themes. There
is no page-level horizontal scrollbar, overlapping field, clipped primary
action, geometry feedback loop, or status-driven layout oscillation. Resize,
paint, theme, splitter, and keystroke paths perform no store discovery, file
scan, process inventory, API call, or network I/O.

Saved responses load once, bounded, when Spotter Compose is explicitly opened
or its revision is invalidated. Filtering is in-memory. Background refreshes
publish immutable results only if their generation still owns the visible
selection.

## Smart Activity To Watch Workflow

Activity adds `Add to Watch…` for a selected row. It opens a compact review
surface and makes no write until Save. Candidate chips may include:

- sender callsign;
- destination group;
- normalized Message Intelligence topic;
- meaningful CommStat/status; and
- `Callsign + topic` when both are reliable.

The suggested default follows this order:

1. callsign plus high-confidence actionable topic;
2. callsign plus meaningful warning/status;
3. callsign;
4. group or topic when no usable sender exists.

The operator sees a plain-language preview such as `Alert when this callsign is
heard about this topic`, may remove criteria, chooses priority/expiry, and then
saves. No plausible sample callsigns appear in hints.

### Structured matching and compatibility

The legacy one-kind/one-pattern watch remains readable. Additive structured
criteria store normalized conditions with explicit `AND` semantics; initial
conditions support callsign, group, topic, keyword, status, and location. A
structured watch may therefore represent callsign plus topic without encoding
an opaque string.

Duplicate identity is based on normalized criteria, source scope, and radio
scope. An equivalent duplicate is selected/reported rather than inserted.
Station limits are 100 enabled watches and 500 total watches. The limit is
enforced inside the write transaction so concurrent saves cannot exceed it.
Approaching 75 enabled watches produces nonblocking guidance; reaching the cap
blocks only the new enable/create request.

### Performance architecture

Watch matching must be real, not an administration-only promise. The runtime
loads one bounded immutable enabled-watch snapshot when its revision changes,
normalizes/compiles it outside the UI thread, and evaluates already-projected
candidate dictionaries with a pure matcher. It does not query SQLite once per
watch or once per candidate.

Matching is invoked from the message-projection/ingest completion path, never
from Activity rendering. Match-count/last-match persistence is deduplicated by
watch and message identity and flushed in a bounded batch. Reprojection or
restart cannot manufacture a new match. Notification/Message Intelligence
consumers receive a bounded immutable match event. One malformed rule is marked
unhealthy and skipped without blocking other watches or message ingest.

## Implementation Packages And Gates

### FSW-1 — Specification and compatibility foundation

- Record this operator model and legacy safety mapping.
- Add targeted core APIs for effective state, atomic selected-ID changes,
  validation, policy usage, and safe in-use deletion handling.
- Gate: all legacy flag combinations preserve or reduce automation; none gain
  permission merely by opening or saving through the new UI.

### FSW-2 — Expect and Access Policies UI

- Add the dedicated tab and remove the full policy editor from Expect.
- Present one per-row/per-editor Auto reply state and station service pause.
- Add filter-aware selected/all bulk actions and concise results.
- Gate: bounded reads/writes, exact selection preservation, responsive layouts,
  policy in-use protection, and full audit pass.

### FSW-3 — Saved response Compose and send-time dates

- Add `Saved response | Form catalog` start mode and Expect deep-link.
- Route all manual sends through the existing guarded send service.
- Refresh safe outgoing datecodes before signing without mutating storage.
- Correct wide/medium/compact Spotter layout.
- Gate: preview equals queued payload, stored row is unchanged, duplicate sends
  are blocked, other Compose modes regressions pass, and no render-path I/O is
  introduced.

### FSW-4 — Structured Watches and Add to Watch

- Add backward-compatible structured AND criteria, duplicate/cap enforcement,
  compiled snapshot matching, durable match dedupe, and batched persistence.
- Add Activity review/prefill and Watches editing/preview.
- Gate: legacy watches match identically, compound watches are deterministic,
  caps hold under concurrent writes, reprocessing is idempotent, and one bad
  watch or slow store write cannot block message ingest or UI rendering.

### FSW-5 — Integration and qualification

- Reconcile related specifications and work log.
- Run focused store/runtime/dispatcher/projection/watch/Compose/UI tests.
- Run adjacent Message Intelligence, JS8 target safety, FLAMP Q, scheduler/RF
  Guard, Settings navigation, and lazy-tab lifecycle regressions.
- Exercise 1920x1080, 1000x700, and application-minimum compact geometry with
  one radio, two radios plus Mesh, and three-radio configuration fixtures.

### FSW-6 — Policy-first Expect and Compose View

- Replace the verbose runtime block with stable semantic service/FLAMP-Q chips.
- Require a named enabled policy before a new response can enter `Auto reply
  on`; retain existing inline access without implicit conversion or behavior
  change.
- Summarize policy, response state, radio scope, and rate limits in the normal
  editor; disclose legacy access and advanced options intentionally.
- Add Expect-owned View and New-response routes into the shared Compose editor,
  exact-payload fallback, explicit Save to Expect, and selection-preserving
  return.
- Correct numeric/date/time input styling centrally and qualify enabled,
  disabled, focus, suffix, and step-control contrast in both themes.
- Gate: no view-time mutation, no silent policy/permission broadening, dynamic Q
  remains request-only, target/send safety remains cumulative, normal render
  paths remain I/O-free, focused and adjacent suites pass, and the work log
  records model ownership and production-visual status.

### FSW-7 — Expect response editing comfort and Compose alignment

This bounded refinement applies the task-oriented workspace guideline without
changing Expect authorization, persistence, send, or RF behavior.

- **Primary operator task:** review or edit one saved response quickly, then
  save it, view it in Compose, or send it through the existing guarded path.
- **Starting context:** the selected Expect row, service/FLAMP-Q status chips,
  and its resolved named policy are already loaded.
- **Completion outcome:** the full response is comfortably readable/editable,
  its identity, policy, summary, and mode are understood in one scan, and the
  selected action preserves all policy and routing metadata.
- **Task sequence:** `select/new response → identify token/policy/mode → edit
  reply → review options when needed → save/view/send`.
- **Primary action:** `Save response`; View, Send now, New, policy management,
  and Delete retain their existing semantic roles and safety gates.
- **Workspace archetype:** editor above bounded list. The editor uses natural
  height and gives the response body a full-width multiline row; the list keeps
  the remaining space.
- **Responsive behavior:** token, named policy, resolved policy summary, and
  mode share one metadata band on wide screens, wrap into task order at medium
  widths, and stack at compact/Large Text widths. Relayout is idempotent and
  I/O-free. Reply content wraps, remains keyboard accessible, and never shrinks
  to a single-line editing slot.
- **Shared theme:** use the application stylesheet, semantic button roles,
  font-derived control sizing, and existing Expect chips. No local palette or
  fixed text-size workaround is permitted.
- **Performance boundary:** typing, selection, resize, paint, and the metadata
  reflow use only the loaded editor model. No store, file, process, endpoint, or
  network access may be introduced.
- **Gate:** normal and long replies remain usable at wide, medium, compact, and
  Large Text sizes; policy-first compatibility and View/Create handoff tests
  pass; reply/token/policy/mode state survives responsive transitions; Compose
  layout regressions pass; and production Linux visual confirmation remains an
  explicit operator-assisted qualification.

No successor Spotter feature, including Store and Forward, begins until this
slice passes its automated exit gate. Live RF transmission remains a separate
operator-assisted qualification and is never performed by automated tests.

## Final Exit Gate

- Only one visible per-entry Auto reply control exists. Saved-only responses
  remain manually usable and cannot auto-transmit.
- The station-wide control is unmistakably a persistent service pause, not a
  competing permission.
- Bulk on/off actions are selected-ID, bounded, atomic, validated, audited, and
  cannot accidentally activate legacy rows.
- Access Policies is a dedicated responsive workspace; in-use policies cannot
  be silently detached or deleted.
- Expect `Send now…` and Compose saved-response selection use the existing
  guarded JS8 send path and require destination review.
- Eligible outgoing form dates reflect send preparation time, are signed only
  afterward, and do not mutate the stored Expect response.
- Activity can stage a meaningful single/compound watch; duplicate and absurd
  watch growth are prevented.
- Runtime watch matching is event-driven, cached, pure per candidate,
  idempotent, and isolated from UI rendering and database latency.
- All primary actions remain visible and usable at supported sizes/text/themes,
  with no horizontal page scrolling or dynamic geometry disappearance.
- Required automated suites, Python compilation, and `git diff --check` pass;
  model ownership and external qualification status are recorded.

## Implementation Record

Completed 2026-09-13. The implementation retains the legacy Expect columns but
maps them conservatively into one operator-visible response state. Access
Policies is a dedicated lazy tab; in-use policy deletion is blocked. Expect
filters and bulk selection operate on the loaded bounded page without query-on-
keystroke behavior. Static saved responses, including non-form responses, open
first as read-only Compose views with an explicit `Edit working copy`
transition and the existing guarded JS8 send path; dynamic FLAMP Q remains
request-derived and cannot be sent generically. Eligible
MCForm dates are refreshed on the outgoing copy before signing while the stored
response remains unchanged.

Activity stages an unsaved watch review, with callsign-plus-topic/status as
explicit AND criteria when reliable. Legacy watches remain readable. The
runtime uses a cached compiled snapshot from the background message-projection
lane, performs pure per-candidate matching, and writes one bounded batch keyed
by `(watch_id, message_id)` so projection retries are idempotent. A watch-store
failure is advisory and cannot retry, roll back, or delay authoritative message
projection.

Work-package ownership:

- high-reasoning primary GPT-5 model: specification, compatibility and safety
  architecture, tab/interaction integration, projection-lane concurrency,
  additive match-dedupe schema, review of every delegated diff, and final gate;
- `gpt-5.6-terra` (high): Expect single-state APIs, bounded bulk mutation,
  validation/audit, policy usage, safe deletion, and focused store tests;
- `gpt-5.6-luna` (high): saved-response Compose, guarded Send now, send-time
  date/signing behavior, responsive Compose tests; and
- `gpt-5.6-luna` (high): structured watch store/compiler, normalization,
  concurrent caps, batch persistence primitives, and focused tests.

Acceptance evidence: 392 primary Spotter/Expect/FLAMP-Q/Compose/Message-
Intelligence/projection/runtime/UI tests plus 11 screenshot-shaped responsive
regressions pass (**403 primary tests**), and 142 adjacent Compose, ingest, and
projection regressions pass (**545 tests total**). The 11 geometry regressions
also pass at a 1.5 high-DPI scale. Python compilation and
`git diff --check` pass. No destructive migration, RF transmission, device
write, external endpoint action, or application restart was performed. Live RF
and production Linux visual confirmation remain operator-assisted checks.

The final responsive correction was divided as follows: the high-reasoning
primary model owned layout architecture, wide/compact integration, review of
all delegated diffs, the two-column Access Policy refinement, specifications,
and the final gate; `gpt-5.6-terra` (high) implemented the editor-above-list
Expect/Access Policy foundation and no-I/O resize tests; `gpt-5.6-luna` (high)
implemented the compact Watch editor and focused behavior tests; and
`gpt-5.6-luna` (high) corrected the FIO Spotter Compose setup pane and bounded
splitter geometry tests.

The screenshot qualification follow-up further compacted Expect metadata and
actions, top-packed Watches, separated Access Policy summary evidence, and
made Spotter Compose restore its setup pane to the top on intentional open,
source, or form changes. Ordinary typing and preview refresh preserve the
operator's scroll position. Reset requests are coalesced into one queued UI
turn and perform no discovery or I/O.

### Task-oriented shared-theme qualification

The complete Spotter surface now implements the redesign brief in this
specification. Activity keeps its bounded traffic table dominant and exposes
selection-aware Inbox, Map, Operator, Reply, and Add to Watch actions. Watches
uses a table-first wide layout and task-ordered compact stack; one `Enabled`
checkbox is the only normal enable-state control, and its value is committed by
`Save`. Expect and Access Policies retain editor-above-list flow, use cached
selection detail, preserve selected records across refresh, and identify radios
by their known FIO names while retaining legacy IDs internally.

Forms is an explicit folder, route, and review/use workflow. Selecting a form
uses the loaded catalog only; reading its source requires `Preview selected`.
Forms can route the selected catalog item into Compose or stage its Expect
configuration, and the table reports `Expect status` using the same `Auto reply
on`, `Saved only`, and `Needs attention` vocabulary as Expect. Imports requires
an explicit bounded preview before its commit action becomes available. Spotter
Compose accepts the selected form intent, keeps its setup controls readable at
wide and compact sizes, and continues through the existing target and guarded
send path.

All Spotter actions, muted guidance, splitter handles, chip selection, control
sizing, and combo sizing use shared helpers from `freqinout/gui/theme.py`.
Application theme reapplication now includes the lazy Spotter workspace. No
selection, resize, paint, theme, or typing path introduced filesystem, database,
endpoint, process, device, or network work.

Task-oriented work-package ownership:

- high-reasoning primary GPT-5 model: redesign brief, workspace architecture,
  semantic vocabulary reconciliation, shared-theme integration, review and
  correction of every delegated diff, specifications/work log, and final gate;
- `gpt-5.6-terra` (high): Expect and Access Policy task flow, cache-only policy
  selection, known-radio presentation, shared action roles, and focused tests;
- `gpt-5.6-luna` (high): Activity and Watches table/inspector hierarchy,
  selection preservation, responsive behavior, semantic actions, and focused
  tests; and
- `gpt-5.6-terra` (high): Forms, Imports, and Spotter Compose guided workflows,
  explicit preview gates, cache-only form selection, Compose intent handoff,
  shared semantic styles, and focused tests.

Acceptance evidence: the focused task-oriented screen partition passes **72
tests**. The expanded Compose, Spotter, Expect, JS8 integration, message ingest,
Message Intelligence, message projection, and responsive UI partition passes
**555 tests**. Python compilation and `git diff --check` pass. No destructive
migration, RF transmission, device write, external endpoint action, application
restart, or production-data mutation was performed.

### Policy-first Expect and Compose View completion

Completed 2026-09-13. The normal Expect editor now presents the station service
and FLAMP-Q index as stable semantic chips, content identity, one named Access
Policy, one `Saved only` / `Auto reply on` choice, and a collapsed Options
summary for radio scope and rate limits. New automatic replies require an
enabled named policy. Existing active inline-access rules remain active and
unchanged; an inactive legacy rule cannot be newly activated without a policy.
Legacy conditions are disclosed only for an existing compatible row and can be
copied into an unsaved Access Policy draft for explicit review, saving, and
later assignment. No migration or automatic permission conversion occurs.

Expect owns response discovery. `New response…` routes MCF creation to the
shared Compose implementation, while `View` opens the selected static response
read-only using its exact stored payload. `Edit working copy` is an explicit
local transition; `Save changes to Expect` preserves access, policy, routing,
and automation metadata. `Back to Expect` restores the same entry identity.
Send Now refreshes an eligible outgoing MCF date only on the outgoing copy and
continues through the existing selected-target, radio, RF Guard, busy/PTT,
schedule, signing, and duplicate-click gates. Dynamic FLAMP Q stays in its
request-derived Expect editor.

The shared application stylesheet now covers line, combo, spin, decimal, date,
time, and date/time controls in Light and Dark themes, including enabled,
disabled, focused, selected-text, suffix, and step-control treatment. Expect
selection, filter typing, field typing, resize, paint, chip detail, and working-
copy transitions use loaded state only; explicit tab activation/Refresh retains
the bounded store-read boundary.

Work-package ownership:

- high-reasoning primary GPT-5 model: policy/legacy compatibility architecture,
  optional policy-first store enforcement, explicit read-only View boundary,
  shared-theme contract, review and correction of every delegated diff,
  specification/work-log reconciliation, and final integration gate;
- `gpt-5.6-terra` (high): compact Expect service/FLAMP-Q chips, policy-first
  response editor, progressive Options and legacy disclosure, central control
  styling, and focused UI tests;
- `gpt-5.6-luna` (high): Expect-to-Compose View/Create handoff, exact-payload
  fallback, selection-preserving return, explicit persistence behavior, and
  focused Compose tests; and
- `gpt-5.6-luna` (high): independent policy/theme/View/Create/performance
  acceptance coverage.

Acceptance evidence: **95 focused policy-first/Expect/Compose/store tests** and
**557 expanded Compose, Spotter, Expect, JS8 integration, ingest, Message
Intelligence, projection, and responsive UI tests** pass. All changed Python
modules compile and `git diff --check` passes. No destructive migration, RF
transmission, device write, external endpoint action, application restart, or
production-data mutation was performed. Production Linux visual confirmation
and live RF qualification remain operator-assisted.

### Expect response comfort and Compose alignment completion

Completed 2026-09-13. Expect now presents E? Token, named Access Policy, the
resolved policy-summary chip, and Saved-only/Auto-reply mode as true inline
label/control groups in one wide scan row, a 2×2 medium arrangement, and a
task-ordered compact stack. Reply is a bounded, wrapping three-line text editor
beneath that metadata, so the response can be read and edited comfortably
without surrendering the saved-response list. A reflow invalidates only cached
Qt geometry and releases stale wide scroll-area dimensions; it performs no
store or endpoint work.

The corresponding Compose refinement applies one readable guided-workflow
hierarchy to all four modes. Content-aware sidebar gates protect both the setup
rail and the dominant form/status surface, live JS8 target evidence no longer
competes horizontally with its actions, preview follows content vertically,
and the shared theme owns informational/muted presentation. Draft, selected
radio/form, target, and Expect handoff behavior are unchanged.

Work-package ownership:

- `GPT-5` high-reasoning primary (deployment identifier not exposed): FSW-7
  architecture, redesign brief, policy/persistence/performance review,
  delegated-diff correction, integrated acceptance, specifications/work log,
  and final gate;
- `gpt-5.6-terra` (high): responsive Expect metadata groups, multiline Reply,
  stale-width release, and focused Expect regressions;
- `gpt-5.6-luna` (high): all-mode Compose layout, content-aware setup rail,
  live-target evidence layout, shared semantic styles, and focused Compose
  regressions; and
- `gpt-5.6-luna` (medium): independent wide/medium/compact, draft-preservation,
  control-geometry, and cache-only acceptance tests.

Acceptance evidence: **133 focused Expect/Compose UI tests** and **564 expanded
Compose, Spotter, Expect, JS8 integration, ingest, Message Intelligence,
projection, and responsive UI tests** pass. Changed Python files compile and
`git diff --check` passes. No migration, RF transmission, device write,
external endpoint action, application restart, or production-data mutation was
performed. Production Linux visual confirmation and live RF qualification
remain operator-assisted.

## FSW-8 — Complete Offline MCForms And Native JS8 Message Interoperability

Status: implementation complete; external RF/platform qualification remains

This package closes the remaining compatibility gaps in the FIO-supported
SuperSpotter surface. It covers the configured MCForms catalog, Compose,
Expect round trips, received native-JS8 stored messages, and shared Message
Intelligence. It does not add the legacy application's email/HTTP/APRS
gateways. Protocol-neutral Store & Forward remains the separately gated
Message Relay Queue described in `superspotter_offline_integration_spec.md`.

| SuperSpotter behavior | FIO disposition |
|---|---|
| MCForm catalog, explicit defaults, bracket prompts, Comments | Supported offline through the shared codec |
| Create/view/edit/send saved E? responses | Supported through Compose + Expect |
| Ordinary directed and native JS8 stored-message delivery | Supported; `Send as MSG` is explicit and off by default |
| Receive, decode, summarize, map, and deduplicate MCForms | Supported through shared Message Intelligence |
| SuperSpotter proprietary Store & Forward commands | Not copied; protocol-neutral Message Relay Queue remains separately gated |
| Email, APRS-email, HTTP gateways, online propagation/tile services | Intentionally excluded by the offline product contract |

### Canonical form grammar and persistence contract

One core parser and codec owns MCForm identity and payload structure. A form ID
is `F!` followed by either the established three-digit code with an optional
letter suffix or a catalog-supported alphabetic code such as `F!BDN`.
Consumers must use that shared grammar instead of maintaining narrower local
regular expressions.

The catalog model retains, in source order:

- titles and `!`, `!!`, or `!!!` section headings;
- `.` operator instructions;
- `?` single-choice questions and their `@` answer tokens;
- the answer explicitly marked with `*`, if any;
- `[XX]` structured text prompts; and
- a universal optional `Comments` field supplied by FIO for every form.

Serialization is deterministic:

`F!code` + compact choice tokens + ordered `XX[value]` fields + Comments +
datecode.

Structured values may not contain an unmatched closing bracket. Empty
structured fields are omitted. Comments remain untagged free text and are
preserved separately from structured values. Every choice question must have
an answer before Save to Expect or Send can proceed. A choice is initialized
only when its source option carries `*`; an unmarked question remains visibly
unanswered. Opening and viewing an untouched saved response keeps the exact
stored payload. Editing uses the same codec to reconstruct choice answers,
structured prompts, Comments, and datecode together; bracket fields must never
cause compact answers or Comments to disappear.

Catalog discovery, parsing, and definition lookup are bounded and cached off
the resize, paint, theme, field-edit, and preview paths. A malformed form is
isolated with an operator-readable compatibility notice rather than partially
serialized.

### Guided Compose behavior

FIOSpotter Compose presents source headings and instructions once in a wrapped
orientation block, then the choice and structured fields in source order, then
a comfortable multi-line `Comments (optional)` control. This preserves the
form's operational context without repeating the same guidance under several
controls. Field labels and controls use the shared theme and font-derived
geometry contract. Long instructions wrap; they do not become fake editable
fields or force page-level horizontal scrolling.

Safe operator identity defaults are form-semantic, not label guesses. FIO may
fill the configured operator callsign, state, and grid only when the catalog
prompt clearly identifies the reporting station or operator's own location.
Incident, affected-area, assessment-area, destination, medivac, wildfire, and
`other area` prompts remain blank. A blank value never means the operator's
location when the form documents a special meaning such as `all areas`.

Both JS8Call Compose and FIOSpotter Compose expose `Send as MSG`. It is off by
default and is stored only in the local draft. When enabled, the exact command
passed to the existing guarded send service is:

`TARGET MSG PAYLOAD`

The preview displays that exact command. MSG does not relax target, radio,
selected-target, busy/PTT, RF Guard, schedule, signing, duplicate-click, or
endpoint/source checks. Traffic mode without a destination cannot use MSG.
Saving an Expect response stores the MCForm payload, not the destination or
the transient `MSG` wrapper.

### Receive, intelligence, and dedupe contract

FIO accepts an MCForm delivered as ordinary directed traffic or as a native
JS8Call stored message. The transport wrapper is removed before form parsing;
the normalized payload is classified as FIOSpotter and projected once. The
same RF event encountered through live API, directed log, and JS8 inbox paths
must deduplicate under the existing source/event identity rules. Protocol
control text remains excluded from the operator Inbox, but `MSG` is not a
reason to discard a valid form.

All discovered form IDs, including alphabetic IDs, participate consistently in
Forms, Expect date maintenance, Inbox classification, summaries, mappings, and
Message Intelligence. The MAGNET basic check-in (`F!701C`) maps its first
choice to Green/Yellow/Red status. MAGNET StatRep (`F!701B`) derives an overall
summary conservatively from its explicit status dimensions, with the worst
reported state winning and Unknown retained when no status is present. Raw
answers remain reviewable; the shared summary is supplementary.

### Acceptance gate

- Every active form in the reference catalog is discovered and parsed.
- Catalog totals and source order are stable: 30 forms, 214 choice questions,
  1,405 answer options, 90 structured prompts, and 42 distinct prompt codes.
- All 62 explicit source defaults are honored; no other choice is guessed.
- Callsign/state/grid autofill passes an allow/deny matrix, including all
  `other area` and affected/incident-area exclusions.
- Every form serializes and round-trips choices, prompt values, Comments, and
  datecode without field loss.
- Incomplete choices and illegal prompt brackets block Save and Send with the
  first actionable field identified.
- JS8Call and FIOSpotter previews and guarded-worker commands match exactly in
  normal and `Send as MSG` modes; mode and draft state survive tab changes.
- Directed, live-API, and inbox-native MSG fixtures ingest one FIOSpotter
  message each and do not regress protocol-frame filtering.
- F!701B/F!701C status summaries agree across Activity, Inbox, Map, and other
  Message Intelligence consumers.
- Focused and expanded tests, Python compilation, and `git diff --check` pass.
  Live RF transmission and packaged macOS/Linux/Windows visual qualification
  remain operator-assisted release gates.

### Implementation and acceptance evidence

The completed package introduces one shared MCForm grammar, catalog parser,
payload codec, and operational-status classifier. Compose, Expect, native JS8
inbox ingestion, live/directed ingest, Message Intelligence, SitRep fusion,
Inbox presentation, and Map form routing now consume those shared contracts.
The reference SuperSpotter 2.6 catalog was reviewed directly: all 30 definitions
are discovered, including `F!BDN`; all 214 questions, 1,405 options, 90
structured prompts, 42 prompt codes, and 62 explicit source defaults are
covered by executable tests. Every catalog form is serialized and parsed back
with its choice answers, bracket prompts, optional Comments, and datecode.

JS8Call and FIOSpotter Compose now expose an explicit, off-by-default `Send as
MSG` control. It is draft-local per compose mode and reflows below the target at
bounded setup-rail widths. The exact previewed command is the exact command
given to the established guarded JS8 worker. Save to Expect persists only the
target-neutral MCForm response and no longer depends on a configured radio.
Operator callsign/state/grid defaults are limited to the reviewed allowlist;
affected, incident, assessment, destination, wildfire, medivac, and prompts
that explicitly allow another area remain empty.

Acceptance: the final focused parser/codec, catalog, Compose/Expect, JS8
ingest, Message Intelligence, projection, SitRep, UI reflow, and draft-state
matrix passes **398 tests**. A process-isolated full repository sweep produced
**3,805 passes and 42 skips** before the one feature-related setup-width failure
was corrected; its focused 51-test recheck passes. Four unrelated baseline
failures remain outside this package: one Local Nets timing threshold, two
Settings source-shape assertions against already-refactored theme code, and one
native-Map test harness missing an attribute already accessed by `HEAD`.
Changed Python modules compile and `git diff --check` passes. No migration,
runtime data write, network call, RF/device command, application restart,
commit, or push occurred. Live JS8Call stored-message round-trip and packaged
macOS/Linux/Windows visual qualification remain operator-assisted release
gates.

## Compose workspace UI-contract correction

The FLMsg/FLAmp, JS8Call, FIOSpotter, and CommStat RF modes share one Compose
workspace in both Messages and the pop-out workbench. Their mode selector is a
compact, font-derived control: every label remains complete, the selector wraps
only when its real content width requires it, and its titled container owns only
that natural content height. It must never retain a list viewport's default
size hint as blank vertical space.

At a wide desktop viewport, every mode uses the same task sequence: a readable
setup rail on the left and the dominant editor/preview work surface on the
right. This includes JS8Call; its small setup form must not expand into a tall
empty band above the editor. At medium and compact widths the setup promotes
above the work surface and owns vertical overflow without manufacturing a
horizontal scrollbar. The embedded surface and workbench use this same
decision function, control metrics, theme, and retained draft widgets rather
than parallel layouts.

Acceptance requires all four modes at 1920x1080, 1000x700, and 900x560 in
Normal and Large Text, Light and Dark themes. The mode container remains at its
natural font-derived height, mode names do not elide, wide layouts keep the
work surface larger than the setup rail, compact layouts remain vertically
scrollable, and changing mode, theme, font, or parent window never ratchets a
container minimum upward. Opening and closing the workbench preserves the
selected mode and draft and never introduces page-level horizontal scrolling.

The JS8Call and FIOSpotter destination block is explicitly a two-row unit:
destination label/editor first, then the optional `Send as MSG` delivery mode
aligned with that editor. A wide rail must preserve the block's multi-row
natural height; a generic one-line row cap may not compress or overlap it. The
setup rail itself ends at its natural content height when the viewport is
taller. Blank space may remain as quiet rail background, but it must not be
painted as a large empty setup card that implies missing controls or unused
work surface. When the viewport is shorter, the setup scroll area—not clipped
children or an expanded container—owns overflow.
