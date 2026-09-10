# Local Nets And Tools & Resources Specification

Status: implementation authority; LN-0 through LN-4 passed; Ops Center and SOP integration is the next gated package

Date: 2026-09-09

## Purpose And Authority

This specification defines two connected capabilities:

1. **Local Nets**, a station awareness calendar for scheduled Amateur VHF/UHF
   and GMRS nets or activities; and
2. **Tools & Resources**, the canonical home for reusable operational reference
   data such as frequency plans and a net directory.

It extends, but does not replace:

- `multirig_product_ui_contract.md`;
- `sop_schedule_plan_spec.md`;
- `controlfreq_operational_awareness_center_spec.md`;
- `adaptive_shell_controls_navigation_spec.md`;
- `ui_layout_standards.md`; and
- `settings_configuration_assistant_spec.md`.

If this document conflicts with the product-level Where/When/What/Why or UI
neutrality contracts, those contracts remain authoritative. This feature must
reuse the existing Operating Group, SOP, Schedule Outlook, radio, and resource
models rather than create UI-owned copies.

## Confirmed Product Decisions

- User-facing feature name: **Local Nets**.
- Navigation: **Plans > Local Nets**.
- Operating Group association: encouraged, but optional.
- Frequency data: selected from a shared catalog with station-defined entries.
- Runtime behavior: awareness and reminders only; never automatic QSY.
- SOP behavior: a Local Net may link to and suggest an SOP, while activation
  remains independently controlled.
- Initial services: Amateur VHF/UHF and GMRS.
- Reusable catalog navigation: compact navigation label **Resources**; workspace
  title **Tools & Resources**.

## Product Mental Model

The operator-facing distinction is:

| Object | Operator question | Ownership |
| --- | --- | --- |
| Operating Group | Who is this activity associated with? | Existing configured group identity and policy |
| Frequency Resource | Where does it occur? | Tools & Resources frequency catalog |
| Net Directory Entry | What organized net is this? | Tools & Resources net directory |
| Published Session | When does that net normally meet? | Net directory child record |
| HF Net Schedule | Which HF session will this station actively follow? | Existing commandable HF schedule source |
| Local Net Schedule | Which local session should this station remember? | New non-commandable reminder calendar |
| SOP | What should the operator do? | Existing SOP profile/action model |

Tools & Resources is the information FIO knows once and reuses. Plans are the
station's chosen operating commitments. A directory entry is not active merely
because it exists, and adding a Local Net does not grant transmit authority or
place any radio under scheduler control.

## Scope

### In scope

- station-owned frequency catalog;
- versioned bundled US/FCC reference records;
- station-defined Amateur VHF/UHF, GMRS, and repeater records;
- normalized net directory entries with one or more published sessions;
- adding a published session to HF Nets or Local Nets;
- creating a new net and schedule in one guided workflow;
- Local Net recurrence, reminders, occurrence state, and history summary;
- Operating Group association;
- Ops Center Schedule Outlook projection;
- optional SOP association and SOP entry/reference support;
- resource usage, update, retirement, import, and export workflows;
- responsive, accessible administration and selection surfaces;
- compatibility migration from the current `net_resources` row library; and
- compatibility treatment for the existing Settings `local_net_profiles` SOP
  metadata without inventing schedules from those rows.

### Not in the initial scope

- automatic tuning, PTT, software launch, or schedule-engine ownership for Local
  Nets;
- determining whether an individual operator is legally authorized to transmit;
- automatic internet synchronization of regulatory or repeater directories;
- replacing the existing HF Daily or HF Net scheduler;
- automatic SOP activation merely because a Local Net occurrence begins;
- repeater coordination, frequency coordination, or equipment certification;
- a general-purpose collection of unrelated maintenance utilities;
- FRS, MURS, Part 90, public-safety transmit, or non-US regulatory catalogs.

The schemas and service enums must remain extensible, but unimplemented services
must not appear as selectable promises in the initial UI.

## Information Architecture And Navigation

### Plans master group

The Plans flyout becomes:

- Plan Builder
- SOP Builder
- HF Daily
- HF Nets
- Local Nets
- HF Peer Schedules

Local Nets opens a task workspace. It is not placed in Settings because normal
use is scheduling and reviewing upcoming activity, not station administration.

### Resources master group

The main navigation gains a grouped **Resources** item. Its expanded workspace
title is **Tools & Resources**. The icon should communicate a library or catalog,
not repair/maintenance. It must use the shared navigation canvas, stroke, color,
accessible name, and compact-rail geometry.

Initial implemented children:

- Frequency Catalog
- Net Directory
- Resource Import / Export

`Forms & Templates` is a planned resource family, but it must not be exposed as
an empty or nonfunctional tab. Existing form administration remains where it is
until a separate migration contract is approved.

Resources must not become a miscellaneous drawer. Launch Control, software
settings, logs, database repair, and health diagnostics remain owned by their
respective Software, Station, or Settings workflows.

## Shared Resource-Linking Contract

Every resource-consuming screen follows the same rules:

1. Common selection happens in context; a user does not have to leave Local Nets
   or HF Nets to pick an existing resource.
2. The selected resource is shown as a concise chip or card with its service,
   band/channel, source, and health.
3. `Manage Frequency Catalog` or `Open in Net Directory` deep-links to the
   canonical editor.
4. Returning restores the originating screen, selected draft, scroll position,
   and unsaved work.
5. A context action may create a custom resource and return it as the selected
   value without requiring a second lookup.
6. Consumers persist stable resource/session identifiers plus an accepted
   snapshot. Display labels alone are never relational keys.
7. Editing, retiring, or replacing a resource shows its computed `Used by`
   impact before confirmation.
8. A referenced resource is retired, not hard-deleted. Hard deletion is allowed
   only when the usage query proves there are no references and the user confirms.
9. A source update never silently rewrites an active HF or Local schedule.
   Consumers show a reviewable field diff with `Apply Update` and `Keep My
   Schedule`.
10. Historical occurrences retain the accepted resource snapshot that was in
    effect at the time.

## Tools & Resources Workspace

### Shared workspace shell

The shell contains:

- workspace title and Help;
- a global resource search;
- implemented browser-style tabs;
- source/status chips;
- a primary result surface;
- a selected-resource detail/editor panel; and
- a compact `Used by` summary.

Wide mode may use results on the left and detail on the right. Compact mode
stacks results above detail. The primary result surface receives the remaining
height. Long file paths, provenance, and regulatory text belong in details or
tooltips rather than permanent header prose.

### Frequency Catalog

Supported resource classes:

- regulatory/reference band range;
- regulatory/reference channel;
- bundled reference;
- station-defined simplex frequency;
- station-defined repeater/channel pair;
- imported group/community resource; and
- retired resource.

Primary filters use chips where the choice set is bounded:

- Service: Amateur, GMRS;
- Band/channel family;
- Source: Reference, Station, Imported;
- Status: Active, Update available, Retired; and
- Operating Group when associated.

Free-text search covers resource label, repeater/net name, channel, frequency,
group, locality, grid, coverage, and source. Results remain bounded and paged or
virtualized.

Frequency details include:

- friendly label;
- resource kind (`band_range`, `channel`, `simplex`, or `repeater`);
- service and jurisdiction;
- band or channel identifier;
- lower/upper Hz for a reference range, or center/receive Hz for an operational
  resource;
- receive frequency;
- optional transmit frequency or offset;
- mode and optional bandwidth reference;
- optional CTCSS/DCS tone data;
- location, coverage, grid, and notes;
- associated groups;
- provenance, version, and last verified time;
- active/retired state; and
- usage summary.

Frequencies and range boundaries are stored as integer Hz. Display formatting
must not be used as a comparison key. A reference range is useful for advisory
validation but is not directly schedulable: the user must select or create an
operational channel, simplex frequency, or repeater resource within it.

### Regulatory reference behavior

Part 97 Amateur allocations and Part 95 GMRS channels are not the same data
shape. Amateur resources may describe ranges and applicable constraints. GMRS
resources describe channel centers and, where applicable, distinct station or
repeater use. The initial bundled catalog cites the current eCFR source pages,
captured with an explicit verification date rather than copied into unversioned
UI code:

- [`47 CFR 97.301`](https://www.ecfr.gov/current/title-47/chapter-I/subchapter-D/part-97/subpart-D/section-97.301)
  for Amateur authorized frequency bands;
- [`47 CFR 97.305`](https://www.ecfr.gov/current/title-47/chapter-I/subchapter-D/part-97/subpart-D/section-97.305)
  for authorized emission types; and
- [`47 CFR 95.1763`](https://www.ecfr.gov/current/title-47/chapter-I/subchapter-D/part-95/subpart-E/section-95.1763)
  for GMRS channels.

The reference package records jurisdiction, rule citation, effective/version
date, source URL, content version, and verification date. Bundled records are
read-only. An operator may create a station resource based on a bundled record,
but the source record is not overwritten.

FIO presents reference validation as guidance:

- `Within selected reference`;
- `Outside selected reference`;
- `Transmit eligibility not evaluated`; or
- `Reference unavailable/out of date`.

FIO must never display `Legal`, `FCC approved for you`, or another statement
that implies it evaluated license class, location, emission, equipment
certification, or current authorization. Receive-only reminders remain valid
when transmit eligibility is unknown.

### Net Directory

A Net Directory Entry owns stable net identity:

- net name;
- associated Operating Group, when configured;
- purpose/description;
- scope/coverage;
- contact or public information that is not a secret;
- source and last verified time;
- active/retired state; and
- zero or more Published Sessions.

A Published Session owns reusable timing and frequency defaults:

- service;
- frequency resource;
- recurrence;
- local start time, duration, and IANA timezone;
- effective start/end dates;
- exception dates;
- early-check-in/reminder suggestion;
- operational mode details; and
- published-session version/hash.

Directory cards show name, group, service, concise session bullets, source, and
station usage. Actions are `View`, `Add Session`, `Add to HF Nets`, `Add to Local
Nets`, and context-appropriate edit/retire actions. A session already followed
by the station shows `Scheduled` and `Open Schedule`, not another ambiguous Add.

## HF Nets And Net Directory Relationship

HF Nets remains the station's commandable HF schedule source. Net Directory is
the reusable catalog. The normal flow is:

1. Choose `Add HF Net`.
2. Search/filter Net Directory or choose `Create New Net`.
3. Select one or more published HF sessions.
4. Choose the named HF Net source schedule that will receive them.
5. Review target/radio, recurrence, time, frequency, early check-in, and SOP
   conflict policy.
6. Save the HF Net schedule through its existing validation, plan reprojection,
   RF Guard, and scheduler refresh path.

Starting from Net Directory, `Add to HF Nets` opens the same guided review with
the resource/session already selected. Starting from HF Nets does not require a
prior Resources visit.

An HF schedule row stores its directory session reference, accepted resource
version, and local schedule snapshot. A directory change produces an update
badge and field-level comparison; it never silently changes an active plan.
Station-specific schedule overrides remain explicit.

`Create New Net` creates the directory identity/session and schedule subscription
in one reviewed operation. The normal default is `Save in Net Directory`.
`One-time/private schedule` may create a station-private directory record so the
schedule still has stable identity without publishing it as a general resource.

## Local Nets Workspace

### Primary task

The primary task is to answer:

- What local activity is next?
- When and where does it occur?
- Which group and SOP apply?
- What should I prepare?

The page must not resemble a radio scheduler or imply that FIO will tune the
radio.

### Default presentation

The top summary shows:

- `Now`, if an occurrence is active;
- `Next`, with countdown and local/UTC time access;
- today/upcoming counts; and
- attention only for invalid/missing resources or an applicable SOP/readiness
  issue.

The main list/calendar is ordered by next occurrence. Each card or adaptive row
shows:

- activity/net name;
- group or `Community / Unassigned`;
- service and band/channel/frequency;
- next local day/time and timezone;
- reminder state;
- SOP availability; and
- enabled, paused, update-available, or retired-resource state.

Primary actions are `Add Local Net`, `Edit`, `Enable/Pause`, `Open SOP`, and
`Dismiss This Occurrence` when a reminder is active. Resource management remains
contextual.

### Add/edit workflow

The guided flow is:

1. **Net** — find a Net Directory entry or create a new one.
2. **Session** — select a published local session or define a custom session.
3. **Group** — select a configured Operating Group or Community / Unassigned.
4. **Where** — select/create a frequency resource and review receive/transmit,
   tone, mode, and reference guidance.
5. **When** — recurrence, local start, duration, timezone, effective dates, and
   exceptions.
6. **Reminder & SOP** — reminder offsets, optional SOP, and participation notes.
7. **Review** — exact next occurrences, source/update behavior, and the explicit
   statement `Reminder only — FIO will not tune a radio`.

Wide layouts may combine compatible steps, but the conceptual order and review
must remain clear. Compact layouts stack fields and preserve primary actions.

## Recurrence And Time Contract

Initial recurrence support matches existing FIO semantics where possible:

- Daily;
- Weekly with one or more days;
- Periodic weeks of month;
- Bi-weekly with stable ISO-week offset; and
- one-time occurrences.

Each schedule stores an IANA timezone and wall-clock start. UTC occurrence times
are projections, not the editable source of truth. Daylight-saving transitions,
overnight duration, leap dates, effective date bounds, and exception dates must
be deterministic and tested.

The service computes only a bounded horizon and the next occurrence. It does not
materialize an unlimited recurrence series. Editing recurrence invalidates and
rebuilds the bounded projection.

Occurrence state is keyed by schedule plus occurrence start and may record:

- pending;
- active;
- dismissed;
- completed/missed; and
- optional operator note.

Dismissing one occurrence does not disable the schedule.

## Operating Group Contract

Local Nets references the same configured Operating Group identity used by SOP,
Messages, Ops Center, and planning. It must not create `local groups` that drift
from HF groups.

The long-term group model is transport-neutral: one group may own HF, local
Amateur, GMRS, message, and SOP resources. Existing Settings copy that says `HF
Operating Groups` may remain during the first implementation, but new storage and
APIs must not encode HF-only ownership.

Selecting a group may filter/suggest its resources and SOPs. It does not import
membership, create a group, or prove transmit authorization. `Create Operating
Group` is a contextual handoff to the canonical group editor and returns to the
Local Net draft afterward.

## Ops Center Integration

Local Nets projects immutable, non-commandable Schedule Outlook items. Required
fields include:

- stable occurrence and schedule IDs;
- source type `LOCAL_NET`;
- `commandable = false`;
- net/session/resource/group IDs;
- name and concise where text;
- start/end UTC plus display timezone;
- reminder/attention state;
- SOP ID and availability;
- source freshness and accepted version; and
- Why text.

Ops Center renders a collapsible `Local Nets` section within Schedule Outlook:

- active occurrence, if any;
- the next occurrence;
- a bounded `Later` count/list;
- group, service, channel/frequency, and countdown;
- `Details`, `Dismiss`, and `Open SOP` actions.

No `QSY` action is shown. A future `Open Radio Controls` handoff may be evaluated
separately, but it cannot become an implicit tune.

Reminder prominence may increase at 30 and 15 minutes using text/iconography as
well as color, consistent with the Station Control Bar urgency language. The
section must remain visually distinct from actionable message traffic. A missed
occurrence rolls forward after a bounded review period and does not create a
permanent warning.

## SOP Integration

A Local Net schedule may reference an SOP profile. An SOP action may reference a
Local Net or directory session as context/trigger evidence. When the reminder
window begins, Ops Center may recommend `Open SOP` and display the applicable
What/Why summary.

The initial implementation does not automatically activate the SOP. Any future
automatic activation requires a separately configured, auditable policy with a
clear preview and manual override. Local Net overlap is informational and does
not participate in HF Net/SOP scheduler priority or RF Guard unless the operator
later chooses an explicit radio action outside this feature.

## Persistence Model

The following names describe the intended ownership; final SQL names may follow
repository conventions but must preserve these boundaries.

### Resource catalog source

`resource_catalog_sources`:

- stable source key;
- label and source kind (`bundled`, `station`, `imported`);
- jurisdiction;
- version/effective date;
- source URI;
- last verified UTC;
- content hash;
- read-only and enabled flags.

### Frequency resource

`frequency_resources`:

- stable resource key and source reference;
- resource kind (`band_range`, `channel`, `simplex`, `repeater`);
- service, jurisdiction, band, and channel;
- label;
- lower/upper Hz for range references;
- receive/transmit/center Hz and optional offset for operational resources;
- mode/bandwidth and tone fields;
- locality/grid/coverage;
- provenance/version;
- active/retired state and replacement reference;
- created/updated UTC.

Group associations use a normalized relation rather than a comma-delimited
field.

### Net directory and sessions

`net_directory_entries` owns net identity and descriptive fields.

`net_directory_sessions` owns recurrence, timezone, frequency resource,
effective dates, reminder suggestion, source version, and active/retired state.

### Local Net schedules and occurrences

`local_net_schedules` owns the station subscription, accepted session/resource
versions, local override fields, group/SOP references, reminder policy, enabled
state, next occurrence UTC, and accepted snapshot JSON.

`local_net_occurrence_state` owns dismiss/completion state and an optional note
for a specific projected occurrence. Old occurrence rows are retained or
summarized under a documented bounded retention policy.

### Usage and updates

Usage is computed from indexed foreign-key/reference columns; it is not a stale
UI-maintained counter. Resource update state compares the consumer's accepted
version/hash with the current resource/session version.

Foreign keys are enabled for new write connections where repository migration
policy permits. Otherwise, service-layer validation and orphan-health queries
are mandatory.

## Compatibility And Migration Contract

The current `net_resources` table is a row library: one record combines net,
session, frequency, and recurrence data. It remains a compatibility source during
migration and must not be renamed, dropped, or destructively reinterpreted.

Settings `local_net_profiles` is a second legacy source, but it represents local
group/resource hints used by SOP—not recurrence or calendar entries. Parseable
targets may seed reviewed station Frequency Resources and group associations;
unparseable rows remain intact and review-required. No Local Net schedule,
occurrence, or automatic SOP behavior may be inferred from this metadata.

Migration requirements:

1. Create a timestamped database/config backup before ownership cutover.
2. Produce a dry-run report with source rows, inferred net identities,
   frequencies, sessions, duplicates, warnings, and conflicts.
3. Create the new schema in one transaction.
4. Classify existing rows before import. General standards and rows without a
   credible net identity become Frequency Catalog resources only. Rows with net
   identity/session semantics also become Net Directory entries/sessions.
   Ambiguous rows remain preserved and review-required rather than being forced
   into a false net identity.
5. Import classified rows idempotently using stable source keys and a legacy-ID
   mapping table.
6. Preserve source set, source type/ref, read-only state, group, recurrence,
   frequency, timing, mode, coverage, comments, and update time.
7. Infer initial service conservatively; ambiguous rows are retained with a
   review state rather than dropped.
8. Add nullable canonical session references to HF schedule rows without
   removing the existing `resource_id` compatibility reference.
9. Move all current FIO resource writes behind one canonical repository before
   enabling Tools & Resources editing.
10. If legacy projections remain necessary, the canonical repository is their
   single writer in the same transaction. Two independent writers may not own
   the same resource state.
11. Leave the legacy table available for rollback/read compatibility until a
    later removal specification proves it unused.

Backup failure or migration conflict blocks cutover and leaves current readers
and data unchanged. Repeated startup is idempotent. No migration may modify a
production database merely because a read-only Resources screen was opened.

## Import And Export

Imports are preview-first and non-mutating until confirmed. The preview reports:

- new;
- updated;
- unchanged;
- duplicate;
- invalid;
- ambiguous service/frequency;
- missing group/resource reference; and
- conflict.

Warnings retain source line/item, field, supplied value, reason, and suggested
resolution. Copy/export diagnostics is available before commit. Imported content
cannot overwrite bundled read-only records; it creates a new version, station
override, or explicit conflict.

Export supports selected resources, a net plus its sessions/frequencies, and the
station's Local Net subscriptions. Stable keys and version metadata are included.
Secrets and unrelated station configuration are excluded.

## Performance And Concurrency

- Resources and Local Nets are lazy main-window workspaces.
- Opening either screen performs bounded indexed reads only and starts no network
  synchronization.
- Result lists return no more than 200 rows per page/query.
- Ops Center requests active, next, and at most 50 later Local Net occurrences.
- Recurrence expansion is bounded to 90 days or the next 20 occurrences per
  schedule, whichever is smaller.
- `next_occurrence_utc` is updated on schedule write, relevant timezone/settings
  change, and occurrence rollover; it is not recalculated for every paint.
- A production corpus of 10,000 frequency resources, 2,000 nets, 5,000 sessions,
  and 1,000 Local Net subscriptions must keep warm filtered catalog queries below
  100 ms p95 and Ops occurrence queries below 50 ms p95 on the Linux baseline.
- Initial tab acknowledgement should occur within 100 ms; heavy import,
  recurrence rebuilding, or migration work runs outside the GUI thread with
  cancellation and shutdown ownership.
- Refresh coalesces duplicate requests. No unconditional sub-minute full-catalog
  polling is allowed.
- UI models update incrementally or by bounded replacement; they do not construct
  thousands of per-row widgets.

## Responsive And Accessibility Contract

Validate at 1920x1080, approximately 1000x700, and approximately 900x560 in
Light/Dark and Normal/Large Text.

- No primary workflow depends on page-level horizontal scrolling.
- Text-bearing controls use font metrics rather than one platform-specific fixed
  height.
- Tables may scroll internally, but names and next-occurrence information receive
  elastic width.
- Wide split views stack predictably at compact widths.
- Chips wrap or use bounded horizontal scrolling without clipping focus borders.
- Icon-only controls have accessible names, tooltips, keyboard focus, and visible
  focus state.
- Selected resource, unsaved state, validation, and blocked actions are conveyed
  by text/iconography as well as color.
- Live status changes do not move primary actions or cause disruptive layout
  growth.

## Help And Operator Language

Help must explain:

- directory versus schedule;
- HF commandable schedule versus Local reminder schedule;
- resource source/version/update state;
- receive versus transmit fields;
- why reference guidance is not a legal authorization decision;
- how Operating Groups and SOPs relate;
- how to apply or reject a resource update; and
- how to recover from a missing/retired resource.

Placeholders and examples follow the UI neutrality contract and contain no real
or plausible callsigns. Live configured data may appear in completion results.

## Acceptance Criteria

The feature is acceptable only when:

1. Resources has a functional Frequency Catalog and Net Directory with no empty
   promised tabs.
2. A user can add a directory HF session to a named HF Net schedule from either
   screen and reach the existing validation/reprojection path.
3. A user can create a Local Net from a known or custom net without first visiting
   Resources.
4. Local Nets never enters the scheduler engine, emits QSY, launches software,
   or changes a radio.
5. Ops Center shows active/next Local Nets with accurate What/Why, countdown,
   group, frequency/channel, and SOP handoff.
6. Dismissing one occurrence preserves future recurrence.
7. Timezone/DST, overnight, bi-weekly, periodic, exception, and effective-date
   fixtures project deterministically.
8. Operating Group association uses the existing canonical group data and
   Community / Unassigned remains available.
9. An SOP can reference a Local Net and be opened from its reminder without
   automatic activation.
10. Resource edits produce a reviewable update and never silently rewrite active
    HF or Local schedules.
11. Retiring a referenced resource preserves schedules/history and clearly marks
    the dependency.
12. Migration is backup-first, dry-runnable, transactional, idempotent, and
    preserves every existing valid `net_resources` and `local_net_profiles` row
    or reports it for review.
13. Import is preview-first and never overwrites bundled read-only resources.
14. Resource use/impact is visible before retirement or destructive removal.
15. Performance, responsive/theme/text, accessibility, shutdown, and Linux
    production gates pass.

## Definition Of Complete

Completion means the operator can discover reusable net/frequency information,
subscribe the station to an HF or Local session through the correct ownership
path, see Local reminders in Ops Center, open applicable SOP guidance, understand
why each state exists, and trust that the feature cannot silently tune a radio or
destroy/mutate legacy resource data.
