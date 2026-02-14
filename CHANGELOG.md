# Changelog

## [1.1.6]
- Added: Shared offline propagation core service used by ControlFreq and Stations Map for consistent modeled/blended scoring paths.
- Added: Offline empirical propagation blending from local historical outcomes with confidence gating and recency decay.
- Added: Propagation regression coverage for blend runtime behavior (disabled passthrough and sufficient-history weighting).
- Added: Expanded propagation regression suite coverage for ingest idempotency, modeled fallback/parity, and overnight schedule window semantics.
- Added: Map propagation target controls now include `Region = ALL` for lower-48 national targeting.
- Added: Map Layers propagation target controls (Target Type/Value) now mirror ControlFreq target settings.
- Added: Map station pins now support JS8Spotter `MCF304` impact status coloring with persistent per-callsign state (`green/yellow/red`, default `blue` for unknown), including tooltip timestamp and legend entries.
- Added: JS8Spotter SitRep status ingestion expanded to `F!104` and `F!301` (in addition to `F!304`) with per-form mapping and persistent source metadata.
- Added: Latest-signal-wins SitRep resolution now uses UTC timestamp first with ingest-time tie-break for deterministic status updates.
- Added: Operators tab `SitRep` column with color-chip status display (`R/Y/G/?`) and manual per-callsign status update (`MANUAL` source) that can be superseded by newer reports.
- Added: ControlFreq `Message Summary` now includes a `SitRep` row with filter-aware totals and `R:x  Y:y  G:z` detail counts.
- Added: Map `SitRep Status` mode to show only known-status stations (`red/yellow/green`), hide links, and temporarily override standard map link filters.
- Changed: Map controls splitter now includes a clearer chevron icon affordance in drawer mode for show/hide discoverability.
- Updated: `docs/guide.html` and new `docs/tools-and-scripts.md` for comprehensive v1.1.6 tab/control and maintenance-script guidance.
- Added: ControlFreq Operating Status strip equivalent to Settings (JS8, FLRig, FLDigi, FLMsg, FLAmp, VarAC, JS8Spotter, CommStat).
- Added: Shared software status probe service for consistent process/API status behavior across Settings and ControlFreq.
- Added: Settings IA Phase 2 split of `Radio Software` into `Fast Light Settings` and `VarAC Settings` sections.
- Added: `JS8Call Settings` now include install/launch path fields for JS8Call, JS8Spotter, and CommStat.
- Added: VarAC BBS configuration fields (`BBS Directory`, `BBS Archive`, `Auto-Archive BBS Files`, archive days) with save-time safety validation.
- Added: Messages tab support for VarAC BBS inbox files (top-level scan of configured `BBS Directory`) with `MSG Type = BBS`.
- Added: BBS row actions in Messages tab: `View | Archive | Delete` (`Archive` moves files to configured `BBS Archive` with collision-safe timestamp suffixing).
- Added: `Launch Control` section with ordered app list, per-app `Enabled` and `Launch on Startup`, global `Launch All with FreqInOut`, manual `Launch Configured Now`, and `Stop Launch Sequence`.
- Added: Shared launch orchestration core service with serial startup launch, 30s per-app readiness timeout, and continue-on-failure behavior.
- Added: ControlFreq `BBS Files` summary section with total file count and `Aging Out` filenames (next 24h to archive threshold).
- Changed: ControlFreq now renders `Message Summary` and `BBS Files` as one sectioned table to improve space usage and scanning.
- Changed: ControlFreq layout rebalanced to give more room to `Today` and `7 Days` planning views.
- Changed: ControlFreq layout redesigned into a two-column top stack (`Activity` + `Schedule Intersections` + `Message Summary` on left, `Frequency Control` + `Schedule Outlook` on right) with full-width `Propagation Forecast` along the bottom.
- Changed: `Today` and `7 Days` are now unified into sectioned `Schedule Outlook` in ControlFreq.
- Changed: ControlFreq `Message Summary` now uses direct rows (no section separator rows) and the third column header is `Details / BBS Aging Out` to maximize visible rows.
- Fixed: Scheduler VarAC busy gating now reads both VarAC traffic and main logs (`VarAC_traffic.log`, `VarAC.log`/`varalog.log`) to detect active connections and file transfer windows more reliably.
- Changed: Net-sourced schedule changes now bypass VarAC wait/busy gating so scheduled nets can override active VarAC link hold behavior.
- Changed: Net-sourced schedule changes now apply consistent backend deferral logic across JS8Call, FLDigi, and VarAC (NET bypasses backend busy deferrals; non-NET defers when busy).
- Changed: Legacy `Launch Selected` / app selector controls removed from `Fast Light Settings` and replaced by `Launch Control`.
- Changed: Schedule-tab mode auto-launch is bypassed when Launch Control is enabled to avoid duplicate launch triggers.
- Changed: In Settings, `Operating Status` now appears above `FreqInOut Settings`.
- Changed: Settings sections are now collapsible and default to collapsed at startup (including Operating Groups and FreqInOut Settings) to maximize open-panel workspace.
- Changed: ControlFreq `Inbox Summary` renamed to `Message Summary`.
- Changed: Messages tab defaults BBS-only view to oldest-first when using default time sort.
- Changed: Peer Schedules Local time headers now use `(Local)` labels instead of long timezone names, and the table adds sortable columns plus a `Clear Filters` action for consistency.
- Changed: Daily HF Schedule and Net Schedule headers now explicitly label Day/Start/End columns with `(Local)` or `(UTC)` to match active time display mode.
- Changed: Added `Clear Filters` actions to ControlFreq and Operators for consistent filter reset behavior.
- Fixed: Local/UTC toggle buttons now initialize from saved display mode across Daily HF Schedule, Net Schedule, FreqPlanner, Messages, and Peer Schedules.
- Added: Messages tab `Mark All as Read` action (type-scoped to current MSG Type filter) with batch local DB updates and bulk JS8 inbox sync handling.
- Changed: Operator History table is now sortable by column header as part of list-table sorting consistency.
- Changed: Header-row select-all checkbox contrast updated for Messages and Operators to improve Linux visibility/highlight consistency.
- Added: Non-blocking launch progress indicator (popup + status bar updates) while Launch Control sequences run.
- Added: ControlFreq compact scheduler strip with `Now`, `Next change`, and `Suspend` status indicators.
- Added: ControlFreq Schedule Outlook quick actions per row (`QSY`, `Open Net`, `Open SOP`).
- Changed: App/documentation version references updated to 1.1.6.
- Changed: ControlFreq top panels now use resizable splitters with persisted layout and view/filter preferences.
- Changed: ControlFreq now includes a persisted Focus Mode toggle for reduced-distraction operations.
- Changed: ControlFreq table UX consistency pass (stable row heights, stronger empty states, urgency row highlighting, keyboard shortcuts, and richer operating-status LED tooltips).
- Changed: ControlFreq Operating Status now renders as a compact single-row strip near `Last updated`, and Frequency Control remains in the top tri-panel layout.
- Fixed: ControlFreq urgency highlighting now uses theme-aware contrast in dark mode for Schedule Outlook, Schedule Intersections, and Message Summary.
- Fixed: ControlFreq theme-switch styling now reapplies button styles consistently after light/dark changes.
- Changed: ControlFreq scheduler strip text simplified (`Status`, `Next`, `Suspend`) to reduce visual noise.
- Changed: ControlFreq top layout now uses three sections (`Frequency Control` left, `Activity` middle, `Message Summary` right) with `Operating Status` in the upper status strip and expanded lower-left `Schedule Intersections`.
- Changed: ControlFreq Operating Status LED order now matches requested sequence (`FLRig`, `FLDigi`, `FLMsg`, `FLAmp`, `JS8`, `VarAC`, `JS8Spotter`, `CommStat`).
- Changed: ControlFreq Status/Next/Suspend indicators moved into `Frequency Control`; `Operating Status` moved to a single-row strip near `Last updated`; `Message Summary` moved into the top tri-panel area; `Schedule Intersections` expanded to full left-column height.
- Changed: ControlFreq now keeps `Message Summary` visible in Focus Mode (shared top row with `Frequency Control`), keeps `Frequency Control` at a stable compact height in both Focus modes, and defaults Focus Mode to `On` when no prior preference exists.
- Changed: ControlFreq top-row panel heights now align to `Frequency Control` (Activity + Message Summary when Focus Off, Message Summary when Focus On), and the Focus Mode button now highlights with `info` styling when active.
- Changed: UI button role styling now aliases `info` to the same accent palette as `primary` for consistent action coloring across tabs.
- Changed: Logs access moved out of primary sidebar navigation and into a dedicated `Settings > Logging & Diagnostics` section via `Open Logs`.
- Changed: `Logging & Diagnostics` is now its own Settings section (separate from `FreqInOut Settings`) with responsive action-row wrapping on narrow widths.
- Changed: `FreqInOut Settings` now focuses on control/timer configuration without embedded logging controls.
- Changed: Settings now uses a two-pane section navigator (left section list + right active section) so heavy panels like `Operating Groups` can use full available space.
- Changed: Settings section navigator is now more compact (narrower/shorter) with explicit hover highlighting and concise section labels.
- Changed: Settings section order now places `Logging & Diagnostics` at the bottom of the section list.
- Changed: Active Settings sections now expand to use available vertical space in two-pane mode (instead of fit-to-content height clamping), reducing unnecessary scrolling.
- Fixed: Settings layout no longer reserves extra blank vertical space below the active section pane, allowing large sections (for example `Operating Groups`) to use full available height.
- Changed: `Fast Light Settings` layout was reorganized into a cleaner aligned single-column form (app paths + related file paths + check-in copy actions) for better scanability.
- Changed: `JS8Call Settings` layout was cleaned up with consistent label widths, aligned path/browse rows, and a clearer top control row.
- Changed: `VarAC Settings` layout was cleaned up with aligned install/path rows and a clearer auto-archive policy row (`Enable Auto-Archive`, `After <days> days`).
- Fixed: Settings form-row spacing consistency for `Fast Light Settings` and `JS8Call Settings` by removing extra per-row layout margins.
- Fixed: `VarAC Settings` auto-archive hint now remains directly associated with the auto-archive policy controls.
- Changed: SOP Builder profile selector now defaults to hint text (`Select existing or add new...`) while keeping `New SOP` as the first dropdown option followed by existing SOPs.
- Added: Global `Suspend Schedule` quick action in the sidebar `Schedule Status` panel (toggles to `Resume Schedule` while suspended).
- Added: Scheduler engine public suspend helper (`suspend_schedule(minutes)`) for consistent UI-triggered temporary schedule holds.
- Changed: ControlFreq `Schedule Outlook` action UX now uses clearer `Actions` buttons (for example `QSY Now`), plus a contextual row right-click action menu and inline usage hint.
- Added: Settings logging controls (`Logging Level`, `Enable DEBUG For` 15/30/60 min, `Open Log Folder`, `Export Diagnostics`) with caution tooltips.
- Added: Schedule Status panel now shows a warning-highlighted `Logs: LEVEL` quick-open indicator whenever logging is active.
- Added: Timed DEBUG auto-revert logic restores prior logging level when the selected timer expires.

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
