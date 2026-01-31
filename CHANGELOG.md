# Changelog

## [1.1.2]
- Added: FLDigi log folder setting to detect RX activity from latest fldigi*.log.
- Added: FLDigi log-based busy detection to delay schedule frequency changes when recent RX traffic is present.
- Changed: FLDigi RX busy delay is skipped during scheduled and ad hoc nets; mode/offset auto-apply waits for FLDigi idle.

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
