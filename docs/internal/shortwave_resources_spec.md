# Shortwave Resources Specification

Status: proposed implementation authority; product decisions in **Decision gates**
must be confirmed before the corresponding gated package begins

Date: 2026-09-10

## Purpose And Authority

This specification adds a first-class **Shortwave** reference experience under
the **Resources** master navigation group. It also corrects Resources navigation
and makes catalog export preview-first.

It extends:

- `multirig_product_ui_contract.md`;
- `adaptive_shell_controls_navigation_spec.md`;
- `local_nets_tools_resources_spec.md`;
- `ui_layout_standards.md`; and
- `controlfreq_operational_awareness_center_spec.md`.

The file `/Users/bill/RadioCode/TOOL_IDEAS/short_wave.txt` is design input only.
It is not an instruction source or implementation authority. Its useful product
ideas have been independently checked against the local EiBi files, existing FIO
architecture, and the safety contracts above.

## Confirmed Product Direction

- **Resources** is a master navigation group, not a direct page.
- Its first-level destinations are **Frequencies** and **Shortwave**.
- **Frequencies** opens the existing Tools & Resources workspace. Frequency
  Catalog, Net Directory, and Import / Export remain browser-style tabs inside
  that workspace; they are not repeated in main navigation.
- **Shortwave** is a dedicated task workspace for answering: “What is listed on
  shortwave now or soon, and what am I hearing?”
- Frequency export is preview-first. A file is not written until the operator has
  reviewed and confirmed the selected records and included dependencies.
- Shortwave source, season, update age, and interpretation state are visible in
  operator language. This state describes only whether FIO completely and
  deterministically parsed the provider's source and recurrence rule. It is not
  a probability of reception, transmitter activity, propagation, legal
  authorization, or station identity. Opaque keys and hashes are available only
  in diagnostics.
- A source listing is schedule intelligence, not proof of reception. UI language
  uses **Scheduled now**, **Starting soon**, and **Listed**, never an unsupported
  claim that a station is on air.
- Adding a shortwave item to FIO initially creates a receive-only listening
  reminder. It does not silently enter the HF scheduler, QSY a radio, launch
  software, transmit, or activate an SOP.

## Operator Mental Model

| Object | Operator question | Ownership |
| --- | --- | --- |
| Frequency Catalog | What reusable frequencies and channels does FIO know? | Existing Resources catalog |
| Net Directory | What organized nets are published? | Existing Resources catalog |
| Shortwave Listing | What does a named source list on this frequency and time? | Versioned provider dataset |
| Listening Reminder | What broadcast or utility listing do I want to remember? | Station-owned receive-only calendar |
| Radio/SDR assignment | Which configured receiver might I use? | Existing device identity; no implied control |
| Observation | What did this station actually receive? | Future operator/log data, distinct from a listing |

Shortwave is not another editable frequency list. Many stations can share a
frequency, and time, day, language, target, transmitter site, and source version
are essential to the meaning of each listing. The shortwave schedule therefore
has a dedicated normalized model while reusing canonical frequency identities
where doing so is safe.

## Information Architecture And Navigation

### Full navigation

The full navigation shows a `Resources` master row with these children:

1. `Frequencies`
2. `Shortwave`

`Frequencies` opens Tools & Resources on the last internal tab, or Frequency
Catalog on first use. Contextual links may open Frequency Catalog, Net Directory,
or Import / Export directly without adding those tabs to the main tree.

### Compact navigation

The compact Resources item opens the same two-item flyout. It does not silently
choose a child. Its active state represents the Resources master group while the
workspace heading identifies Frequencies or Shortwave.

### Shortwave workspace

The workspace uses a browser-like task flow without nested navigation clutter:

- `Explore` — bounded search, now/soon browsing, filters, and listing details;
- `Listening` — accepted receive-only reminders and their source-update health;
- `Data Sources` — installed datasets, provenance, update/import preview, and
  rollback status.

Only implemented areas are visible. Actual radio or SDR control must not appear
until its gated adapter package passes its safety and hardware acceptance gate.
The Shortwave child itself is not exposed until Explore and Data Sources pass the
SW-2 gate; Resources may temporarily contain only Frequencies while R-1/SW-1 are
in progress.

Resources navigation remains governed by the existing canonical catalog
authority gate. Shortwave depends on the canonical `resource_catalog_sources`
registry and is unavailable in a legacy/pre-cutover profile. Startup may perform
the existing backup-first authority cutover or the additive SW-1 migration before
the UI is enabled; visiting a Resources or Shortwave tab never triggers a
migration. If startup cutover fails, current data/readers remain unchanged and
the unavailable state is explained through health/recovery rather than exposing
a partially owned editor.

## Shortwave Explore Experience

### Default answer

The default view should quickly answer:

- what is **Scheduled now** and **Starting soon**;
- station or service name;
- frequency, shown primarily in decimal MHz with kHz available as a secondary
  conventional shortwave value;
- UTC window and local-time equivalent;
- language and target area;
- broadcast/utility classification; and
- which source and season support the claim.

The initial result set defaults to broadcast listings, with a visible Utility
chip so EiBi utility/DX information is discoverable rather than silently omitted.
Time signals and other recognized service types may be separate chips when the
provider data supports a confident classification. Unknown classifications remain
visible as `Other / review`, not discarded.

### Filters and search

Primary filter chips:

- `Scheduled now`, `Starting soon`, `All listings`;
- `Broadcast`, `Utility`, `Time / standard`, `Other`;
- common shortwave band;
- language;
- target region; and
- current or historical source season.

Search covers station/service, frequency, language, target, station home country,
transmitter site, and notes. Search is debounced and bounded. Entering `9955`,
`9.955`, or `9955 kHz` resolves consistently without locale grouping ambiguity.

### Results and detail

Results use a virtualized/bounded table or cards rather than per-row widgets.
Columns prioritize:

- Station / service
- Frequency
- UTC and local window
- Days
- Language
- Target
- Listing state

The detail panel clearly separates:

- station home country;
- transmitter country/site;
- target audience/region;
- language or signal type;
- exact UTC time/day/date rule;
- mode when explicit or confidently derived;
- provider, dataset season, provider update date, FIO import date, and source
  caveat;
- raw provider codes and row diagnostics behind `Technical details`; and
- `Why this result` describing time, filters, target relevance, or search match.

`Interpretation: Complete` means the current provider row, dates, and recurrence
rule were parsed without ambiguity. `Special rule` or `Review needed` excludes a
row from confident now/soon results unless its exact supported component can be
evaluated. None of these labels makes a reception or on-air claim. “What am I
hearing?” is supported as a search-and-identification aid; received audio or an
operator observation remains the evidence of identity.

Station origin, transmitter site, and target region must never be conflated.
Target relevance may improve ordering but never hides all other results.

## EiBi Source Contract

### Audited local corpus

The initial candidate files are:

- `/Users/bill/RadioTools/Programs/shortwave/eibi_sked-a26.csv`
- `/Users/bill/RadioTools/Programs/shortwave/eibi_README.TXT`

The audited A26 CSV is ISO-8859-1, semicolon-delimited, and contains 9,442 data
rows across 1,999 unique frequencies from 16.3 through 27,184 kHz. The header has
a trailing empty field while every data row has eleven fields; the parser must
validate and tolerate this exact distinction. The README permits third-party use
and distribution but does not identify a formal license. FIO attribution must
identify EiBi/Eike Bierwirth, retain the upstream source statement, and present
the schedule-accuracy caveat.

The authoritative provider landing page is `https://www.eibispace.de/`. It lists
Summer 2026/A26 as current with an update date of 2026-08-31 and links the CSV,
README, and historical archive. The provider page, rather than a guessed download
path in UI code, is the discovery authority; an approved adapter configuration
owns the exact downloadable asset URLs and expected content types.

The local CSV SHA-256 is
`60830fc5db384d160c17d3144bd15966d4c3f5cb9b29f4600a6d7c11eacd4dd4`; the
local README SHA-256 is
`9d0227b6b1e2ce8310d4fd6f5019922f6b11062a1dc0deeaedbc54b2b8a19ffc`.

### Interpretation rules

- Decode ISO-8859-1 explicitly and report decode/substitution diagnostics.
- Parse frequency with decimal arithmetic and store exact integer Hz; never use
  binary float as the canonical value.
- Parse `HHMM-HHMM` UTC, including `2400` as next-day midnight and windows that
  cross midnight.
- Blank Days means daily. Preserve the raw expression for every row.
- Expand only deterministic day rules. Weekday lists/ranges, numeric masks,
  monthly rules, fixed dates, `irr`, `Test`, `spur`, `LSB`, and `USB` require
  explicit grammar or a visible `Special / not evaluated` state.
- A same-start/end time, malformed date, unknown day rule, or unsupported rule is
  a review diagnostic; it is never silently interpreted as currently scheduled.
- Start/stop dates use DDMM. Bracketed stop data is last-heard metadata, not a
  normal end date. Preserve malformed or extended provider values.
- Language can contain multiple codes. Codes beginning with `-` describe signal
  types, not spoken language.
- Preserve station ITU/home code, target code, and transmitter-site code
  separately. Resolve provider dictionaries per dataset version.
- Preserve exact duplicate source rows for audit while deduplicating the normal
  result projection. Show duplicate counts in import preview.
- Persistence `8` is inactive. Other persistence values and season boundaries
  follow the bundled README for that dataset; undocumented assumptions are not
  promoted to product semantics.
- Every provider manifest supplies `season_effective_from_utc` and
  `season_effective_to_utc`, including the correct year mapping for winter `Bxx`
  seasons that cross New Year. DDMM rules are evaluated only inside that bounded
  season. When authoritative season bounds are unavailable, date-bounded rows are
  labeled `Special / not evaluated` and excluded from confident now/soon results.

### Dataset lifecycle

EiBi is a provider adapter, not hard-coded UI behavior. Each import creates an
immutable staged dataset and preview. Apply transactionally promotes the dataset
pointer only after validation and confirmation. The previous current dataset is
retained for rollback and for reminders that accepted its snapshot. Reimporting
the same bytes is idempotent.

The initial update model is:

1. a reviewed bundled snapshot for offline first use; and
2. user-triggered import/update from the authoritative EiBi source, with preview.

Opening Shortwave performs no network request and no import. Automated provider
polling, HFCC, and community DX providers are future adapters and cannot be
combined invisibly with EiBi results.

The official-download adapter uses a fixed HTTPS provider allow-list, accepts no
credentials or cookies, bounds redirects and response bytes, and validates host,
status, content type, file signature/shape, and row limits before parsing. The UI
cannot supply or execute an arbitrary download URL. Download failure leaves the
current validated dataset untouched.

The update preview reports source/publisher, season, provider date, hashes in
technical details, counts for new/changed/unchanged/removed, exact duplicates,
invalid/ambiguous rows, unsupported recurrence rules, inactive rows, and sample
changes. It also reports whether saved Listening reminders reference changed or
missing listings. No source update silently mutates an accepted reminder.

## Persistence Model

All Shortwave persistence lives in the existing startup-owned
`freqinout_nets.db`; schema initialization remains Qt-free, additive,
transactional, and idempotent.

The existing `resource_catalog_sources` table remains the canonical registry for
provider identity, friendly source name, provenance, and read-only ownership.
Shortwave does not introduce a second source registry.

### `shortwave_datasets`

- `dataset_key` stable primary key
- `catalog_source_key` foreign key to `resource_catalog_sources`
- `provider_key`, `provider_label` immutable dataset snapshot fields
- `season_code`, `publisher_updated_utc`
- `season_effective_from_utc`, `season_effective_to_utc`
- `source_uri`, `source_filename`
- `csv_sha256`, `readme_sha256`, `parser_version`
- `encoding`, `record_count`, diagnostic counts
- `imported_utc`
- `state`: `staged`, `current`, `superseded`, or `rejected`
- provenance, conditions, and source metadata JSON

Only one dataset per provider is current. Promotion and pointer replacement occur
in one transaction. Old unreferenced datasets are removed only by an explicit,
bounded retention policy; referenced datasets cannot be hard-deleted.

### `shortwave_entries`

- dataset-specific stable entry key and provider identity hash
- dataset/source foreign key and raw source line number
- exact `frequency_hz`
- UTC start/end minute and `crosses_midnight`
- raw day expression, normalized weekday mask/rule JSON, parse state, special
  flags
- station name and station-home ITU code
- raw and decoded language/signal values
- raw and decoded target values
- raw and decoded transmitter host/site values
- raw persistence, inactive/utility/classification state
- raw and normalized start/stop/last-heard values
- raw source row, content hash, validation state, and diagnostics JSON

Indexes cover current dataset plus frequency, time candidates, station, language,
target, classification, and listing state. Text search may use FTS5 when available
with a bounded indexed fallback.

### Canonical frequency relationship

Shortwave entries may reference a canonical frequency identity, but FIO must not
create one editable Frequency Catalog row per transmission. Provider schedule
rows remain source-owned and immutable. The existing Amateur/GMRS transfer
service allow-list is not weakened as a side effect; Shortwave transfer and
classification receive explicit validation.

### `shortwave_listening_reminders`

This table is introduced only with the Listening package. It stores:

- station-owned reminder key;
- provider entry key and accepted immutable snapshot;
- reminder timing/lead and enabled state;
- optional configured receive device key;
- operator label/notes;
- source-update comparison state; and
- occurrence/dismissal state.

It contains no PTT instruction, scheduler command, application launch, or
automatic QSY flag.

## Listening And Device Integration

### Initial behavior

`Add to Listening` creates or opens a receive-only reminder from the selected
listing. The editor shows the accepted station, frequency, mode, UTC/local time,
days, source/season, and optional target device. A source update produces a
reviewable diff with `Apply listing update` and `Keep my reminder`.

Ops Center integration, when implemented, presents a distinct **Listening**
reminder, not an HF Net or operational schedule. It states that tuning is manual.
Shortwave reminders do not receive Operating Group or SOP semantics by default.

### Transceivers

A future `Tune selected radio now` action may reuse the existing manual QSY path
only after a separate safety package validates device capability, mode/bandwidth,
radio busy/PTT state, RF Guard, explicit confirmation, and readback. It remains a
manual action and does not enable timed automatic tuning. Existing manual QSY
places that radio off schedule until Resume or a later scheduler transition; the
confirmation must show the target radio, current plan impact, hold/recovery
behavior, and a visible `Resume schedule` path. A separate transient receive-tune
path is not implied and would require its own ownership specification.

### SDRs

Current FIO observer SDR profiles are receive-only identities/endpoints; FIO has
no generic SDR tuning backend. Initial Shortwave support may assign an SDR as a
reminder context but cannot claim it will tune. Actual tuning requires one named,
tested adapter/API and its own lifecycle, timeout, cancellation, and hardware
acceptance gate. Such an adapter introduces an explicit receiver-control
capability separate from the current observer profile. Configuring an
`sdr_host:sdr_port` endpoint alone must never enable `control_via`, scheduler
ownership, PTT, or generic observer auto-tune.

## Frequency Export Preview Contract

All Resources export entry points use one immutable preview service and one
aggregate transfer budget. Frequency Catalog gains multi-select controls while
ordinary row selection continues to drive details.

The flow is:

1. Select records in context.
2. Choose `Review export`.
3. Compute a bounded, non-mutating dependency closure.
4. Show the exact human-readable preview.
5. Confirm.
6. Revalidate source/catalog versions, choose the destination, and write once.

The preview shows:

- selected and dependency counts by type;
- label, service, decimal-MHz frequency/range, mode/kind;
- friendly source/listing and last-verified provenance;
- included net meetings, frequencies, group references, or Local Net
  usage and why each dependency is included;
- warnings for retired, changed, missing, ambiguous, or unsupported items; and
- projected item count and payload size against the transfer limits.

Opaque keys and hashes stay in technical details. Cancel writes nothing. A
catalog change between preview and confirmation invalidates the preview and asks
the operator to review again.

Export and import enforce the same aggregate item maximum and payload-byte limit.
An exported valid bundle must be accepted by its matching import preview. The
current independent per-collection export bounds must not produce a bundle that
the combined import limit rejects.

Transfer scope remains frequency resources, Net Directory entries, and their
published net meetings/group references. HF and Local Net schedules are
station-owned snapshots and are shown as `Used by — not included in export`;
they do not enter the transfer payload. Extending transfer to station schedules
would require a separately versioned schema, privacy review, conflict model, and
compatibility gate.

## Performance And Concurrency

- Shortwave is lazy and performs no database migration, file import, or network
  work merely because the tab opens.
- Import/parse/resolve/hash work is Qt-free and runs off the GUI thread with
  progress, cancellation, single-flight ownership, and bounded shutdown.
- The initial 9,442-row corpus is queried through indexed, bounded APIs. UI code
  never scans the corpus or creates thousands of row widgets.
- Result pages contain at most 200 rows. Search is debounced and stale results are
  discarded by request generation.
- Scheduled-now evaluation first obtains bounded indexed time/day candidates,
  then evaluates recurrence in the service layer. Unsupported rules cannot enter
  the confident Scheduled-now set.
- Warm filtered query target: below 100 ms p95 on the Linux production baseline.
- Navigation acknowledgement target: below 100 ms; first useful page target:
  below 250 ms warm and below 750 ms cold after schema initialization.
- Visible now/soon state refreshes at meaningful minute/boundary intervals and
  coalesces requests. Hidden tabs have no refresh loop.
- Provider dictionaries and dataset metadata are cached per immutable dataset,
  not decoded per row or paint.
- Imports enforce a source byte limit and row limit before allocation. Apply uses
  a shadow dataset transaction and atomic current-pointer switch.

## Safety, Provenance, And Failure Behavior

- A listing never authorizes transmission or proves reception.
- FIO shows stale age and source confidence; it does not silently present an old
  season as current.
- Parse failures leave the current dataset unchanged and export copyable
  diagnostics.
- Missing source files do not erase the last validated dataset.
- An interrupted import cannot leave a partial dataset current.
- Shortwave does not join the commandable HF scheduler, QSY, launcher, PTT, or
  automatic SOP activation paths.
- Bundle packaging explicitly includes any approved source files; development
  paths under `/Users/bill/...` never become runtime dependencies.
- Source paths, URLs, and dataset provenance are data, never executable commands.

## Responsive, Theme, And Accessibility Contract

Validate Resources navigation, export preview, and Shortwave at 1920x1080,
approximately 1000x700, and approximately 900x560 in Light/Dark and Normal/Large
Text.

- Wide result/detail splits stack at compact widths.
- The result surface receives remaining space; descriptive prose does not crush
  the data view.
- Chips wrap or use bounded internal scrolling without hiding selected state.
- Station names and primary frequency/time fields receive elastic width.
- No primary task depends on page-level horizontal scrolling.
- Icons supplement operator text and have accessible names, tooltips, keyboard
  focus, and visible focus state.
- Scheduled, stale, ambiguous, inactive, update-available, and selected states
  are conveyed by text/icon as well as color.
- Locale may affect separators in prose but never changes canonical frequency
  input or the shortwave decimal display into a grouped integer.

## Decision Gates

The following decisions are required before their named package, but they do not
block the Resources navigation or export-preview correction:

1. **Dataset distribution (before SW-1):** approve bundling the audited EiBi A26
   CSV/README as FIO's offline seed. Recommended: yes, with EiBi attribution,
   provenance, conditions text, and an authoritative source URL recorded.
2. **Provider update policy (before SW-1):** confirm bundled snapshot plus
   user-triggered official download/file import, with no background polling.
   Recommended: yes.
3. **Initial content (before SW-2):** confirm both broadcast and utility/DX rows,
   defaulting the Explore view to Broadcast while keeping Utility one chip away.
   Recommended: yes.
4. **Listening behavior (before SW-3):** confirm a receive-only Listening
   calendar/reminder rather than commandable HF schedule rows. Recommended: yes.
5. **Transceiver control (before SW-4):** separately authorize and hardware-test
   an explicit manual Tune Now action. It is not needed for Shortwave browsing.
6. **SDR control (before SW-5):** name the first supported SDR application/API.
   Until then, SDR association is descriptive/reminder-only.

## Acceptance Criteria

1. Resources is one master group with only Frequencies and Shortwave as main-nav
   children; existing catalog tabs remain internal and deep-linkable.
2. Frequency export always shows a human preview before any destination write,
   and cancel is non-mutating.
3. Matching export/import limits guarantee that a valid exported bundle can be
   previewed for import.
4. EiBi import is encoding-aware, preview-first, transactional, idempotent,
   versioned, cancellable, and preserves raw provenance and diagnostics.
5. Time, cross-midnight, `2400`, blank/deterministic/special day rules, DDMM,
   last-heard markers, inactive rows, duplicates, and malformed fixtures behave
   according to this contract.
6. Explore distinguishes home country, transmitter site, and target, and labels
   schedule claims as Scheduled/Listed rather than observed fact.
7. A26 and winter cross-year fixtures prove provider season bounds and DDMM
   evaluation; rows without authoritative bounds cannot enter confident now/soon.
8. A 10,000-row corpus meets the bounded query, tab acknowledgement, and idle-work
   budgets without UI-thread parsing or per-row widgets.
9. Saved reminders retain accepted snapshots; source updates never silently
   rewrite them.
10. Initial Shortwave support cannot QSY, transmit, launch software, enter the HF
   scheduler, or activate an SOP.
11. Responsive, Dark/Light, Normal/Large Text, keyboard, cancellation, shutdown,
    packaging, migration, production-clone, and Linux release gates pass.

## Definition Of Complete

Completion means an operator can find and understand a source-attributed
shortwave listing, distinguish schedule intelligence from actual reception,
review data provenance and ambiguity, save a receive-only reminder without
causing radio action, and export Resources only after seeing exactly what will be
written. Hardware tuning is complete only for adapters that pass their separate
gated acceptance packages.
