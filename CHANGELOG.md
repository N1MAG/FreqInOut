# Changelog

## [1.1.5]
- Added: ControlFreq refinements (activity window filter, intersections summary, frequency control status, and keyword search).
- Added: ControlFreq now includes net check-ins in activity totals to capture voice net traffic.
- Changed: ControlFreq timezone toggle defaults to Local and supports UTC/Local switching.
- Changed: ControlFreq layout polish (spacing, divider, label tweaks, and column sizing).
- Updated: FLDigi NCS label to Digi/SSB NCS with tab title aligned to FLDigi / SSB Net Control.
- Fixed: ControlFreq SOP rows, inbox column sizing/tooltip behavior, and theme refresh on live theme changes.

## [1.1.4]
- Added: Window icon now uses the FreqInOut desktop asset for consistent taskbar display.
- Added: ControlFreq tab (console-style summary of activity, inbox, schedules, and SOPs).
- Added: Propagation overlay with Actual/Blended/Modeled modes and best-band labels for regions/states.
- Added: Propagation window selector (1h/3h/6h/12h/24h/History) for observed-data weighting.
- Added: Propagation legend colors now follow FreqPlanner band color palette.
- Added: Background ingest for JS8 links, JS8 inbox, spotter traffic, and VarAC to improve tab responsiveness.
- Changed: Lazy-load heavy tabs (Messages, FreqPlanner, Map) to reduce startup time and tab-switch stutter.
- Changed: Pause tab-specific timers when their tab is hidden to reduce background work.
- Changed: Message file scanning now runs asynchronously to prevent UI freezes.
- Changed: Map JS8 ingest starts on first Map activation to reduce idle overhead.
- Changed: FreqPlanner rebuild checks are throttled to reduce unnecessary refresh work.
- Changed: Propagation model uses distance + day/night weighting for more realistic band guidance.
- Fixed: Single-instance lock cleanup now supports both PySide6 stale-lock API names.
- Updated: Linux installer icon refresh/overwrite to reliably apply the latest desktop icon asset.
- Updated: Help tab now opens external links in the system browser.

## [1.1.3]
- Added: SOP Builder tab with per-row band/frequency/software/action/interval/contact rules and manual completion reminders.
- Added: SOP import/export and hidden-row preservation for actions tied to software not yet configured.
- Added: SOP role and callsign contact targeting with primary+secondary group filtering.
- Added: SOP due-state styling (warning/success), overdue grace handling, and Schedule Status "SOP Action in" countdown support.
- Changed: SOP time display now supports UTC/Local toggle with matching start-time label behavior.
- Changed: SOP contact display now omits empty role groups and orders by role priority (HUB then HUB-ALT, NCS then ANCS).
- Updated: Linux installer/docs and DB tooling wrappers/documentation refinements.
- Updated: Release hygiene docs, security policy, and version/license metadata consistency.

## [1.1.2]
- Added: FLDigi log folder setting to detect RX activity from latest fldigi*.log.
- Added: FLDigi log-based busy detection to delay schedule frequency changes when recent RX traffic is present.
- Added: FLDigi gibberish detection with timeout to avoid indefinite holds; BUSY RX can show "FLDigi (gibberish)".
- Changed: Peer Schedule timezone toggle now matches Daily HF Schedule "Showing" style and behavior.
- Changed: FLDigi RX busy delay is skipped during scheduled and ad hoc nets; mode/offset auto-apply waits for FLDigi idle.
- Fixed: Daily nets now render on every day in the planner (not just the first day).
- Added: Schedule Status shows upcoming frequency change countdown within 15 minutes.

## [1.1.1]
- Fixed: Scheduled net handling now matches ad hoc net behavior (no forced corrections during active net).
- Fixed: Net end now clears control backoff/pending state before resuming schedule frequency/offsets.
- Changed: Resume Schedule and end-net resume enforce full schedule immediately when allowed.
- Updated: Guide to document net handling behavior and resume rules.

## [1.1.0]
- Added: VarAC database integration for messages, operator/map context, and delete support.
- Added: Messages tab bulk select/delete with confirmation summaries and flagging (red/green) for follow-up.
- Added: Operator History export by group (multi-select) and group editing in modal.
- Added: Schedule Status panel under tabs with Resume Schedule and off-schedule context.
- Added: Scheduler enforcement timers (Frequency / FLDigi Mode / JS8 Offset) with prompt intervals.
- Added: VarAC busy detection from VarAC_traffic.log to gate schedule changes.
- Added: Fldigi file senders and net check-ins tracking for map tooltip modes.
- Added: Overlap display for peer schedules with counts and details.
- Added: FreqPlanner band/frequency toggle and per-band color picker with persistent storage.
- Changed: Net schedule recurrence UI (periodic weeks of month, daily option, validation).
- Changed: Messages filters and search UX (searchable from/to, spotter message type).
- Changed: Scheduler prompts and labels (Resume/Skip/Pause wording) and prompt behavior.
- Fixed: Multiple UI alignment/visibility issues across tabs (headers, dropdowns, buttons).
- Updated: Guide documentation for new UI, scheduler logic, messages, operators, and overlaps.

## [1.0.7]
- Added: JS8 RX hub for live ingest fanout and map updates without queue starvation.
- Added: Map updates via JS API (no full reload) with payload deduplication.
- Added: Operator History export to CSV with selection-based export.
- Added: FLDigi NCS end-net adds group to operator history based on operating group frequency.
- Changed: JS8 log ingest is incremental with saved offsets (no table clear).
- Changed: Operator History backfill and JS8 log parsing stream line-by-line (lower memory use).
- Changed: Operator History UI: Import/Export menu and Manage Operators menu.
- Changed: CSV import merge rules keep longest grid, prefer CSV name/state, preserve first/last seen.
- Changed: Scheduler mode labels to Normal/Loose/Strict with legacy value mapping.
- Changed: JS8 NCS offset prefers RX.DIRECTED and DT ms column removed.
- Fixed: Map cleanup and JS8 socket shutdown to reduce exit errors on Linux.
- Fixed: js8net RX thread handles socket close cleanly.
- Fixed: Help tab export PDF default filename.

## [1.0.6]
- Unrecorded changes (not captured in this changelog).

## [1.0.5]
- Unrecorded changes (not captured in this changelog).

## [1.0.4]
- Unrecorded changes (not captured in this changelog).

## [1.0.3]
- Unrecorded changes (not captured in this changelog).

## [1.0.2]
- Unrecorded changes (not captured in this changelog).

## [1.0.1]
- Unrecorded changes (not captured in this changelog).

## [1.0.0] - Initial developer bundle
