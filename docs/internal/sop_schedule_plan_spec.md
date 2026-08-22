# SOP Schedule Plan Spec

## Intent

SOP scheduling becomes a first-class operational plan: where to be, when to be there,
what to do, and which radio can safely support it. It consumes HF Daily, HF Nets,
Net Resources, and one or more active SOPs, but it may diverge from those sources
and be saved as its own assignable object.

## Objects

### Unified Operational Projection

The operational projection is a side-by-side day view. It does not collapse every
source into a single winner. Instead it preserves simultaneous lanes so operators
can see multi-group and multi-radio obligations.

Sources:

- `HF`: daily baseline rows.
- `NET`: scheduled HF Net rows.
- `NET_RESOURCE`: known net resources not necessarily folded into HF Nets.
- `SOP`: SOP schedule layer rows.

HF Daily owns named Daily schedule snapshots. HF Nets owns named Net schedule
snapshots. FreqPlanner Overview composes from the current live HF Daily / HF Net
tables or from those named source snapshots. This lets an operator intentionally
select combinations such as an exercise daily schedule plus county net schedule
before review, RF Guard preflight, and save.

Saved Frequency Plans keep concrete schedule windows for fast scheduler startup,
but plans built from named HF Daily or HF Net schedules also keep dependency refs
to those source schedules. Editing a named Daily or Net schedule must immediately
reproject every dependent Frequency Plan, refresh planner/scheduler caches, and
reload the visible source table if that source is selected. Startup and forced
scheduler refresh may repair stale source-backed plans, but this repair must be
idempotent and should not rewrite an already-current plan. The operator-facing
model is simple: editing a named source schedule updates every assigned plan that
uses that source before any radio follows it again.

Lane keys:

- `radio:<id>` when a row is explicitly assigned to a radio.
- `sop:<profile_id>` for SOP rows without radio assignment.
- `group:<group_name>` for group-scoped HF/Net/resource rows.
- `station` when no stronger identity exists.

Each lane has hourly cells for the projected day. Cells retain every matching
entry. More than one entry in a cell is a contention signal, not a hidden overwrite.

SOP Lanes are scoped to Operating Groups configured in Settings. Rows whose group
is not in the configured Operating Groups list are excluded from the SOP Lanes
view and SOP Schedule Plan projection, so untrusted or incidental group strings
do not appear as operational lanes.
Plan-local edits must keep the entry on a configured Operating Group; unconfigured
group names are rejected instead of saved and then hidden.

### Known Operating Groups

Settings > HF Operating Groups can seed active Operating Groups from known HF Net
Resources. The known catalog is derived from the `net_resources` database table
when present and falls back to bundled SitRepNet resource JSON. The catalog groups
resource rows by `group_name`, then exposes one selectable group with its associated
mode/band/frequency configurations.

The bundled catalog also includes built-in digital-mode standards so operators can
enable common baseline working frequencies without building a net resource first:
JS8Call Standard, FT8 Standard, WSPR Standard, and FLDigi WEFAX. The FLDigi
WEFAX preset uses NOAA/NWS HF marine radiofax stations and is selected from the
operator's Grid 6 when available, then state, then timezone. It stores USB radio
dial frequencies 1.9 kHz below the published assigned frequencies and starts
FLDigi in WEFAX576. The auto station choice can be overridden from the Settings
known-group controls when propagation or mission context favors a different
coastal broadcast site. AHRN and RATPACK are removed from bundled SitRepNet
resources because they are no longer active; the catalog loader also filters
those group names from older local resource databases.

Enabling a known group creates or updates active Operating Group configurations
without a modal confirmation. The inline preview names the group and the first
frequencies that will be enabled, with a View Frequencies action for the full list.
Once enabled, the group is edited through the normal active Operating Groups inline
editor or removed with Disable Group. Existing active groups are preserved; matching
group/mode/band configurations are refreshed from the resource selection.

This keeps SOP lanes and schedule assignment anchored to explicitly enabled
Operating Groups while avoiding manual re-entry of common SitRepNet groups.

### SOP Schedule Plan

An SOP Schedule Plan is saved through the existing `frequency_plans` table with
category `sop_schedule`. It carries structured `schedule_refs` and provenance in
the same format RF Guard and Schedule Assignment already understand.

It is saved as one coherent object even when it contains multiple `radio:<id>`
lanes. RF Guard preflight validates each radio lane against that lane's entries
so one radio is not warned or blocked for another radio's work.

Expected payload fields:

- `category`: `sop_schedule`
- `source_refs`: includes `hf_daily`, `hf_nets`, `net_resources`, and/or `sop`
- `schedule_refs`: structured operational entries
- `frequency_refs`: unique band/frequency refs from entries
- `group_refs`: operating groups represented by the plan
- `notes`: JSON summary for lane counts, source counts, and generated timestamp

Saved Frequency Plan and SOP Schedule Plan review prompts include the selected
HF Daily and HF Net source names. The plan name prompt uses a dedicated editable
dialog so naming is explicit and reliable. The saved Frequency Plan or SOP
Schedule Plan is its own blended object and does not overwrite either named
source schedule.

## RF Guard

Saving an SOP Schedule Plan may happen before radio assignment, but assignment must
run RF Guard when enabled. When a radio context is available, preflight should run
before save. Multi-radio plans must surface:

- unsupported antenna band
- same-band overlap groups
- close-frequency guard groups
- simultaneous TX risks where known

RF Guard also applies during initial guided radio configuration and every selected
radio configuration path. Supported antenna bands are captured before schedule
assignment so FIO can warn before a user assigns a plan that contains an unsupported
band. The guard reads the effective plan layers directly: HF Daily rows, HF Net
rows, SOP rows, structured `schedule_refs`, and summarized `frequency_refs`. A
schedule row carrying `band: 80M` must warn or block against a radio whose antenna
supports only 40M/20M/15M even if no separate frequency summary was saved.
Guided Add Radio must surface this check twice: first in the RF Guard step as a
plain-language Plan Check tied to the selected antenna bands, then again beside
the Schedule Assignment controls if a specific Frequency Plan is selected. The
RF Guard step should therefore never appear "done" in a way that hides a known
antenna/plan mismatch.
In Guided Add Radio, the RF Guard step is a direct configuration surface, not a
collapsed advanced panel. Supported antenna bands are shown first, followed by
the unsupported-band action, the selected-plan check, and then optional shared
hardware groups. This ordering lets the operator see equipment-protection
decisions before choosing or saving a schedule.
The Schedule Assignment card must use the same validation result and summarize
the whole mismatch, not only the first event. Example: "this radio supports 15M
and 10M, but the selected plan uses 80M, 40M, and 20M." This summary appears
before save and after save whenever warning-level assignment is allowed, so the
operator does not have to infer equipment risk from a buried RF Guard event row.
If the operator saves or activates a radio with a warning-level antenna/schedule
mismatch, the selected radio's Health surface must continue to show RF Guard
needs review until the antenna bands or assigned plan are corrected. This is a
core equipment-protection signal, not just a setup note.

Launch Control is a selected-radio management feature, not an initial Add Radio
decision. Add Radio records the app paths and control endpoints needed by the
radio profile, then selected-radio Settings exposes Launch Control for startup
opt-in, launch order, and manual launch actions. Manual launch actions use the
selected radio's configured app paths and commands rather than station-default
compatibility paths. Startup launch policy can still be gated by the radio opt-in
and assigned operating model, but review and "launch now" are always scoped to the
radio the operator is editing.

Guided Add Radio may also prepare managed app configuration, but this is not the
same action as saving the radio profile. The Review step must clearly separate
"save this FIO radio profile" from "prepare selected external app profiles".
External writes are explicit, backup-gated, and limited to supported writers
such as managed JS8Call MultiSettings updates; VarAC remains read/import-only.
The UI should therefore summarize what FIO will remember, show any app profiles
it can prepare, warn about port/path conflicts with existing radio profiles, and
require a deliberate managed setup action before external app files are changed.
Detected app review rows are confirmation controls, not primary browse controls.
After Configure Automatically, any app path FIO filled must be shown as the
selected value in the detected-app review row. If multiple candidates exist, the
row is highlighted until the operator chooses the correct app/profile. If nothing
was found, the row remains highlighted and Browse is the fallback. JS8Call app
and JS8Call profile selection stay adjacent because the profile, API port, and
DIRECTED.TXT path are one operational decision.
The guided setup progress area is a compact Setup Status strip. Each step shows
only the step title plus Ready, Review, Needed, or Backup; full detail belongs in
tooltips, the current step body, or the final Review step. This prevents the
wizard from becoming a text-heavy checklist while still making the next required
operator action visible through highlighted controls.
The Review step uses short cards for Radio Profile, Software and Control,
Endpoints, RF Guard and Schedule, and Files. Long filesystem paths are not shown
as a paragraph by default; the Files card summarizes how many paths are configured
or need review, with full path detail available in tooltip/details surfaces. This
keeps Save Radio understandable while still preserving enough detail for audit.

Known Net Resources are included when they are not already represented by HF Net
rows. If an HF Net row carries the same resource identity or the same
day/time/frequency/net signature, the resource is treated as already folded into
the schedule.

Recurring resources and SOP layers must honor `Weekly`, `Daily`, `Periodic`, and
`Bi-Weekly` rules for the projected week. Bi-weekly rows use
`biweekly_offset_weeks` against the ISO week number.

## Station Command Bar And Manual QSY

Schedule assignment and RF Guard review should treat station-card manual QSY as
an operator override of the configured schedule for exactly one radio. `QSY` is
not a transient tune while schedule automation continues. It commands the
selected manual target and places that radio into manual QSY state. While this
state is active, the station is intentionally off the configured schedule until
`Resume` is used or the scheduler explicitly transitions to a new active
schedule entry.

The command bar presents one card per active radio. Each card uses that radio's
assigned plan as the authoritative source for target, next target, and QSY
choices. Scheduler lanes and radio/app snapshots are runtime status signals and
must not override another radio's assigned plan. The card presents the
operator-facing group and band as the primary target, with exact frequency/mode
in the tooltip. This is intentional: users assign and operate by group/band in
the center-of-gravity workflow, while RF Guard and scheduler internals continue
to use exact frequency metadata. If the radio cannot yet report the newly
commanded frequency, the UI should preserve the commanded target and disclose
any mismatch in the tooltip rather than snapping back to the previous scheduled
target.

`Resume` is the visible recovery action from manual QSY or timed suspension. It
must be enabled and highlighted when the station is off schedule because of
manual QSY.

## UI Direction

The target UI mirrors FreqPlanner’s cell grid but adds lanes. Operators should be
able to scan the full day and answer:

- Which group/SOP/radio is active?
- What frequency or net should I monitor?
- What action should I take?
- Is RF Guard clean, warning, or blocked?

Clicking a cell opens an inspector. If the entry is source-backed, the inspector
offers source navigation. If the entry is plan-local, it edits the SOP Schedule
Plan entry directly. Resource-backed edits must distinguish “update this plan
only” from “update the master resource.”

For the first editable slice, saved `sop_schedule` plans render in SOP Lanes view
when selected in FreqPlanner. Editing a cell updates the selected plan only and
does not mutate HF Daily, HF Nets, SOP Builder, or master Net Resource rows. The
save path still runs RF Guard preflight, including per-radio lane validation and
same-plan sibling lane checks.

For resource-backed entries with a `resource_id`, the operator is prompted to
save the plan-local edit only or also update the master Net Resource. Master
resource updates are explicit, happen only after the SOP Schedule Plan save passes
RF Guard, and mark the resource as manually updated from an SOP Schedule Plan.

## Traffic-Driven SOP Inputs

SOP Builder also consumes operational observations from Messages, ControlFreq,
and Map. JS8Spotter, CommStat, FLMsg/FLAmp, VarAC, JS8Call, and local reports can
produce topic-tagged observations that suggest SOP actions or condition-level
changes. This is specified in
`docs/internal/superspotter_offline_integration_spec.md`.

Condition-level changes must be rule-driven and configurable by operating group.
MagNet `MAGCON` traffic is a default template, not a hard-coded behavior. Other
groups can define their own match patterns, allowed senders, source families,
authentication requirements, and apply behavior. The default behavior for
received condition alerts is review-and-apply, so inbound traffic does not
silently mutate an active SOP.

## Implementation Slices

1. Core operational projection and SOP Schedule Plan payload builder.
2. Draft SOP Schedule Plan creation from HF Daily + HF Nets + Net Resources + SOP.
3. RF Guard preflight for selected radio context and per-radio lanes.
4. Read-only lane grid in SOP Scheduler/FreqPlanner style.
5. Cell inspector and plan-local edits.
6. Optional master-resource update workflow.
7. Named HF Daily and HF Net source schedule management.
8. Known Operating Groups seeded from SitRepNet/Net Resources.
