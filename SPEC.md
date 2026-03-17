# UI and Data Performance Acceleration Spec (v1)

Related archived spec: `SPEC_offline_hf_propagation_forecast_v1.md`.

## 1. Metadata

- Feature name: UI and Data Performance Acceleration
- Owner: FreqInOut maintainers
- Date: 2026-02-15
- Target release: phased across upcoming releases
- Status: Draft

### 1.1 Addendum (2026-02-18): Local NCS Lookup Enter-Key Stability

Problem:
- `Local NCS` lookup Enter handling diverged from `FLDigi NCS` and executed add/write paths directly from lookup completion events, causing unstable behavior and user-reported app exits.

Scope:
- Align `Local NCS` lookup keyboard behavior with `FLDigi NCS`:
  - `Enter` in lookup performs operator autofill only.
  - Add/write occurs only through `Add Check-in` action.
- Keep existing net-session gating, autosave, and table/session behavior unchanged.

Acceptance criteria:
- Typing a unique operator (for example `Wayne`) and pressing `Enter` only fills the lookup field with canonical `CALL / Name / State`.
- Pressing `Enter` or `Space` on `Add Check-in` still adds one row.
- No process exit/crash on repeated lookup Enter actions.

Rollback:
- Revert `Local NCS` lookup handler changes in `freqinout/gui/local_ncs_tab.py`.

### 1.2 Addendum (2026-02-18): Action Eligibility Highlighting Consistency

Problem:
- CTA buttons and toggle controls are inconsistent across tabs: some high-value next actions remain visually muted, while some non-context actions stay emphasized.
- Operators need low-noise, state-aware guidance for what to do next without changing workflow behavior.

Scope:
- Introduce a shared soft-highlight style family for eligible actions.
- Apply state-driven button highlighting across:
  - `ControlFreq`, `FreqPlanner`, `SOP`, `Messages`
  - `FLDigi/SSB NCS`, `JS8 NCS`, `Local NCS`
  - `HF Schedule`, `Net Schedule`, `Net Resources`
  - `HF Operators`, `Local Operators`, `Map`, main `Schedule Status` panel
- Add schedule dirty-state detection for Save-button highlighting in schedule tabs.

Constraints:
- No functional behavior changes to scheduler, ingestion, or NCS state machines.
- Keep visual treatment soft (guidance, not alarm).
- Preserve existing shortcuts and action availability rules.

Acceptance criteria:
- Eligible next actions are visibly highlighted only when user state makes them actionable.
- Non-scenario actions (for example `Refresh`) are not promoted as contextual CTAs.
- Save buttons on schedule tabs highlight only when unsaved edits exist.
- Net Resources `Add to Net Schedule` CTA highlights only when row selection makes direct add actionable.
- Theme parity: behavior is consistent in light and dark themes.

Rollback:
- Revert tab-level style-state hooks and fallback to existing static `button_style` assignments.

### 1.3 Addendum (2026-02-18): ControlFreq and FreqPlanner Highlight Reliability

Problem:
- User-reported highlight regressions remained in `ControlFreq` and `FreqPlanner` after the broad eligibility pass.
- `ControlFreq` QSY/Resume highlight state could become stale when selection and refresh paths ran in different order.
- `FreqPlanner` toggle highlight state needed explicit re-application across click/rebuild/clock updates.

Scope:
- `ControlFreq`: centralize frequency-action button style updates so `QSY Now` and `Resume Schedule` are recomputed together from the same current state on refresh and selection changes.
- `FreqPlanner`: re-apply toggle highlight styling after view toggles, table rebuilds, and periodic clock label updates.
- Keep behavior unchanged; this is styling-state synchronization only.

Acceptance criteria:
- In `ControlFreq`, selecting a frequency that differs from active highlights `QSY Now` and de-emphasizes `Resume Schedule`.
- In `ControlFreq`, when no pending QSY exists and scheduler/active are mismatched, `Resume Schedule` highlights.
- In `FreqPlanner`, `Showing: UTC` and `Showing Frequency` states consistently show highlight styling immediately on click and remain highlighted through table refresh and timer ticks.

Rollback:
- Revert style synchronization changes in `freqinout/gui/controlfreq_tab.py` and `freqinout/gui/freq_planner_tab.py`.

### 1.4 Addendum (2026-02-18): Button Style Selector Compatibility Fix

Problem:
- Button highlight states remained visually unchanged in runtime despite state logic firing.
- Root cause: `button_style()` emitted a combined selector (`QPushButton, QToolButton`) that was not reliably applying when styles were set directly on individual buttons in this environment.

Scope:
- Update shared style generation to emit separate rules for `QPushButton` and `QToolButton`.
- Keep role color semantics unchanged.
- Validate with runtime rendering checks and baseline preflight.

Acceptance criteria:
- Role changes (`muted` to `info`/`warning`, and eligible roles) produce visible button background changes at runtime.
- ControlFreq `QSY Now`/`Resume Schedule` and FreqPlanner toggle highlights visibly change after user actions in both Light and Dark themes.

Rollback:
- Revert selector generation in `freqinout/gui/theme.py` to prior implementation.

### 1.5 Addendum (2026-02-18): Active Net Reminder in Sidebar Navigation

Problem:
- During active NCS sessions, users can forget a net is still running unless the state is visible from the main menu.

Scope:
- Highlight sidebar menu buttons while nets are active for:
  - `FLDigi/SSB NCS`
  - `JS8 NCS`
  - `Local NCS`
- Keep in-tab behavior consistent with action eligibility:
  - Once a net is active, `Start Net`/`Ad Hoc Net`/`Join Net` actions remain muted/disabled as non-clickable.
  - `End Net` remains the primary active-session action.

Acceptance criteria:
- Starting any NCS net highlights the corresponding sidebar NCS menu button until the net ends.
- Ending a net clears sidebar highlight for that NCS menu button.
- Theme changes preserve accurate sidebar net-state highlighting.
- In-tab start actions are muted while active net sessions are in progress.

Rollback:
- Revert sidebar net-status highlight wiring in `freqinout/gui/main_window.py` and related NCS tab signal hooks.

### 1.6 Addendum (2026-02-18): Map First-Open Window Flash Stability

Problem:
- On first click of `Map`, users report the main window appears to close/reopen before the map appears.

Scope:
- Remove first-open native window/reparent flash risk in map initialization by constructing the embedded web view with an explicit parent from the start.
- Keep lazy loading and map rendering behavior unchanged.

Acceptance criteria:
- First `Map` open no longer produces a visible app-window close/reopen effect.
- Existing map load flow (`Loading map...` then rendered map) remains functional.

Rollback:
- Revert webview-parent initialization changes in `freqinout/gui/stations_map_tab.py`.

### 1.7 Addendum (2026-02-18): WebEngine Warmup Before First Map Click

Problem:
- Some environments still show a one-time visual flash when WebEngine initializes on first Map tab click.

Scope:
- Warm up Qt WebEngine shortly after app startup using a hidden, parented `QWebEngineView`.
- Prioritize Map in lazy prewarm ordering and start lazy prewarm earlier.
- Keep user-visible tab behavior unchanged.

Acceptance criteria:
- First Map click no longer triggers one-time close/reopen-style flash from initial WebEngine startup.
- Startup remains stable and Map lazy-load behavior still functions.

Rollback:
- Revert WebEngine warmup and lazy-prewarm timing/order updates in `freqinout/gui/main_window.py`.

### 1.8 Addendum (2026-02-18): Remove First-Click Map Widget Swap Path

Problem:
- User testing still reports a close/reopen-style flash on first `Map` activation in some environments after prior WebEngine warmup changes.
- Remaining risk is first-click lazy replacement and first visible WebEngine widget activation happening on the interactive path.

Scope:
- Construct the `Map` tab during startup (hidden) so first user click only changes stack index and does not create/replace tab widgets.
- Keep other heavy tabs (`Messages`, `FreqPlanner`) lazy-loaded.
- Retain and harden early WebEngine warmup behavior to run before first user interaction.

Acceptance criteria:
- First click of `Map` does not perform lazy widget replacement.
- First click of `Map` no longer presents close/reopen-style flash in user-visible flow.
- Existing map lifecycle behavior (`set_map_visible`, first render on activation, timers on visibility) remains intact.

Rollback:
- Restore `Map` to lazy placeholder/factory wiring in `freqinout/gui/main_window.py`.

### 1.9 Addendum (2026-02-18): Release 1.1.8 Version and Guide Alignment

Problem:
- Build artifacts and user docs still contain `1.1.7` references.
- `guide.html` does not fully reflect current shipped UI/workflows, especially `Local Operators`, `Local NCS`, new sidebar labels, and Net Resources import/resource-catalog behavior.

Scope:
- Update release/version metadata to `1.1.8` across application/versioning files.
- Update `docs/guide.html` for current tab labels and workflows:
  - `NCS-FLDigi/SSB`, `NCS-JS8`, `NCS-Local`
  - `HF Operators` + new `Local Operators` and `Local NCS` behavior
  - `HF Operating Groups` terminology
  - Net Schedule + Net Resources catalog/import JSON flow.
- Keep docs technically specific while remaining operator-friendly for both novice and advanced users.

Acceptance criteria:
- `release_preflight` reports no version mismatch errors.
- Guide shows `Current version: 1.1.8`.
- Guide contains explicit sections for `Local Operators` and `Local NCS`.
- Guide references current labels and actions users see in the shipped UI.

Rollback:
- Revert version bumps and guide updates in:
  - `freqinout/version.py`
  - `pyproject.toml`
  - `installer.iss`
  - `docs/guide.html`
  - `README.md`

### 1.10 Addendum (2026-02-18): Performance Hardening + Upsert Integrity Audit

Problem:
- Users report slow UI actions across multiple tabs under live workloads.
- Hot DB write paths still perform repeated schema checks/`CREATE TABLE` operations during runtime.
- Some operator upsert paths use legacy-style write patterns that can degrade performance and risk field clobbering.

Scope:
- Audit and harden hot operator/local-net DB upsert paths for:
  - schema correctness against unified tables
  - no legacy-only field dependency for runtime writes
  - reduced per-action DB overhead.
- Add lightweight caching for high-frequency schedule reads used by dashboard refresh paths.
- Preserve behavior and storage compatibility.

Implementation targets:
- `freqinout/core/checkins_db.py`
  - expose a shared operator-schema ensure helper for reuse by UI tabs.
- `freqinout/gui/js8call_net_control_tab.py`
  - remove repeated runtime `CREATE TABLE IF NOT EXISTS` from hot operator update paths.
  - normalize timestamps to UTC ISO format in check-in increment/update paths.
  - replace replace-style inserts in operator updates with conflict-safe upsert/update patterns that preserve existing fields.
- `freqinout/core/local_ops_store.py`
  - add process-level schema-ensure memoization to avoid repeated PRAGMA/DDL work per operation.
- `freqinout/gui/controlfreq_tab.py`
  - add short-lived schedule-row caches to reduce repeated full-table reads during refresh cycles.

Acceptance criteria:
- JS8/local operator write actions do not run repetitive table-creation logic per event in normal runtime.
- Operator upsert/update paths preserve existing non-target fields (no replace-clobber behavior).
- ControlFreq refresh performs fewer redundant schedule table reads while keeping displayed data correct.
- Verification baseline passes:
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`
  - `powershell -ExecutionPolicy Bypass -File .\tools\freqinout-db.ps1 status`

Rollback:
- Revert edits in:
  - `freqinout/core/checkins_db.py`
  - `freqinout/gui/js8call_net_control_tab.py`
  - `freqinout/core/local_ops_store.py`
  - `freqinout/gui/controlfreq_tab.py`

### 1.11 Addendum (2026-02-18): SOP Printable PDF Export Workflow

Problem:
- SOP export currently supports JSON only and is not optimized for printed operational use.
- Operators need a clear time-oriented SOP printout with optional roster attachment and explicit as-of provenance.

Scope:
- Add SOP PDF export flow with a robust criteria modal in the SOP tab.
- Support two PDF scopes:
  - selected SOP only (current selection)
  - unified export of all active SOPs
- Keep output as a single PDF file.
- Include configurable export options:
  - display times in `Local` or `UTC`
  - optional inclusion of operator callsign in header
  - optional roster appendix: `HF Operators`, `Local Operators`, or both
  - optional roster filter by `State` or FEMA `Region` (R01-R10)
- Render printable layout for 8.5in x 11in with readable sizing, wrapping descriptions/notes, and pagination.
- Keep existing SOP JSON import/export behavior intact.

Export content rules:
- SOP action rows are sorted by `Next Due Time`.
- SOP action descriptions are fully rendered with line wrapping (no truncation).
- Header includes `As Of` using local date/time.
- HF roster columns: `Callsign, Name, State, SitRep, Notes`.
- Local roster columns: `Callsign, First, Last, City, State, Category, SitRep, Notes`.

Acceptance criteria:
- User can open an SOP export modal and choose selected SOP vs unified active SOP export.
- Exported SOP PDF is generated as a single file and prints cleanly on letter size (8.5in x 11in).
- `As Of` appears in local time/date.
- Times in SOP tables render according to selected export mode (`Local` or `UTC`).
- If roster inclusion is enabled, selected roster table(s) are appended to the same PDF with configured filter(s).
- Long action descriptions and notes wrap and paginate without clipping.

Rollback:
- Revert edits in:
  - `freqinout/gui/sop_tab.py`
  - `freqinout/core/sop_manager.py`

### 1.12 Addendum (2026-02-18): SOP Operator Export Multi-Select Filters

Problem:
- SOP PDF operator appendix currently supports geo filtering (state/region) only.
- Operators need roster exports filtered by operational attributes:
  - HF Operators: `Groups` and `Trusted`
  - Local Operators: `Category`

Scope:
- Extend SOP PDF export modal with additional multi-select filters:
  - HF groups (multi-select, optional)
  - HF trusted status (multi-select: `Trusted`, `Untrusted`)
  - Local categories (multi-select, optional)
- Apply filter selections during PDF roster generation.
- Preserve existing scope/time/as-of behavior and single-PDF output.

Acceptance criteria:
- Export modal presents selectable HF groups and trusted states when HF roster is enabled.
- Export modal presents selectable local categories when local roster is enabled.
- HF roster output includes only rows matching selected group/trusted filters.
- Local roster output includes only rows matching selected category filters.
- If no specific values are deselected (defaults), behavior matches current "include all" semantics.

Rollback:
- Revert edits in:
  - `freqinout/gui/sop_tab.py`
  - `freqinout/core/sop_manager.py`

### 1.13 Addendum (2026-02-18): SOP JSON Action Consolidation + Roster Modal Stability

Problem:
- Separate `Export JSON` and `Import JSON` buttons add header clutter for infrequent actions.
- SOP PDF roster modal shows layout jump when toggling HF/Local sub-options.
- Operators requested that roster sub-options remain hidden until roster inclusion is enabled.

Scope:
- Consolidate SOP JSON actions into a single `Export/Import` dropdown control.
- Keep `Export PDF` as a separate primary action.
- In SOP PDF modal:
  - Rename roster master toggle to `Include Operator Rosters`.
  - Hide HF/Local roster selectors and all roster filter controls until master toggle is checked.
  - Prevent layout jump by avoiding per-subtoggle show/hide behavior; use enable/disable for sub-filters instead.

Acceptance criteria:
- SOP header shows one `Export/Import` dropdown for JSON actions.
- JSON export/import behavior remains unchanged.
- In SOP PDF modal, only `Include Operator Rosters` is visible by default for roster controls.
- Checking `Include Operator Rosters` reveals HF/Local selectors and roster filters.
- Toggling HF/Local no longer causes layout collapse/expand jumps.

Rollback:
- Revert edits in:
  - `freqinout/gui/sop_tab.py`

### 1.14 Addendum (2026-02-18): FLAMP `.k2s` GPG Authenticity Verification

Problem:
- Operators need authenticity verification for FLAMP transfer payloads without adding UI lag or workflow friction.
- Verification and trust setup are currently external-only and hard for low-tech users.

Scope:
- Apply signature verification only to file-backed `flamp` `.k2s` messages in Message Viewer.
- Add Settings support for GPG management:
  - enable/disable verification
  - configure/test `gpg` executable path
  - import public keys from file or pasted armored text
  - display public keys/fingerprints
  - mark trusted signer fingerprints (app trust list)
  - trigger local key-signing action (`gpg --quick-lsign-key`)
- Verify in background (non-blocking UI) and cache results for fast reload.
- Support detached signature pairing variants:
  - `<file>.sig` / `<file>.asc` / `<file>.gpg`
  - `<stem>.sig` / `<stem>.asc` / `<stem>.gpg`

UI behavior:
- Valid signature: confirmed icon.
- Invalid signature: warning icon.
- Missing signature: neutral (row remains normal).
- Verification details (signature/trust/fingerprint) shown in message info text when viewing a file.

Constraints:
- No blocking GPG operations on GUI thread.
- Keep existing message ingest, filtering, and row actions unchanged.
- If GPG is not installed or misconfigured, app remains functional and reports neutral/error states.

Acceptance criteria:
- Settings tab can import/list/manage keys and trusted signer fingerprints without manual CLI usage.
- Message Viewer verifies eligible `flamp` `.k2s` files asynchronously and updates row icons after completion.
- Verification results persist in cache and are reused on restart when file/signature metadata is unchanged.
- Invalid signatures produce warning icons; valid signatures produce confirmed icons.
- Missing signatures do not block display and remain neutral.
- No tab-freeze regression during scan/refresh/view flows.

Rollback:
- Revert edits in:
  - `freqinout/core/gpg_tools.py`
  - `freqinout/gui/settings_tab.py`
  - `freqinout/gui/message_viewer_tab.py`

### 1.15 Addendum (2026-02-18): FLAMP `.k2s` Checksum Hash Verification

Problem:
- Signature verification confirms signer authenticity but does not fully cover simple transfer corruption workflows where checksum sidecars are provided.
- Operators requested checksum-based tamper/corruption detection support in FreqInOut.

Scope:
- Add checksum hash verification for `flamp` `.k2s` files in Message Viewer.
- Verification runs in the same non-blocking background authenticity pipeline and is cached.
- Add local trusted-hash registry in Settings so operators can store received hash values
  (for example SHA-1) for automatic verification without requiring sidecar files.
- Support common sidecar formats and algorithms:
  - sidecar names: `.sha256`, `.sha512`, `.sha1`, `.md5` (including filename variants)
  - line formats: `HASH  filename`, `ALGO(filename)=HASH`, and single-hash line
- Add Settings toggle under Message Authenticity:
  - enable/disable checksum verification for FLAMP `.k2s`.
- Add Settings trusted-hash management:
  - add/paste/import/remove local hash values (MD5/SHA-1/SHA-256/SHA-512).
- Combine signature and checksum outcomes for row authenticity UI:
  - if any verification path succeeds (signature, sidecar hash, or local trusted hash), mark confirmed/trusted
  - if none succeed and any path is invalid/error, mark warning
  - no verification artifacts => neutral

Constraints:
- Keep UI thread non-blocking; hashing stays in worker thread.
- Preserve existing message import/filter/view behavior.
- Prefer SHA-256/SHA-512 when multiple checksum options exist.

Acceptance criteria:
- FLAMP `.k2s` rows with valid checksum sidecar show confirmed authenticity status.
- FLAMP `.k2s` rows with local trusted hash match show confirmed authenticity status.
- Mismatched checksum shows warning status and mismatch details.
- Missing checksum sidecar remains neutral unless signature check dictates otherwise.
- Authenticity cache persists hash results and avoids unnecessary recomputation when file + sidecar metadata is unchanged.
- Authenticity cache invalidates when trusted-hash registry changes.
- No regressions in Message Viewer responsiveness.

Rollback:
- Revert edits in:
  - `freqinout/core/hash_tools.py`
  - `freqinout/gui/settings_tab.py`
  - `freqinout/gui/message_viewer_tab.py`

## 2. Problem Statement

FreqInOut currently shows user-visible latency on tab activation and message viewing.

Observed issues:
- First open of heavy tabs (`Messages`, `Map`, `Operators`) can take 3 to 10 seconds.
- Warm switching between heavy tabs can still take about 3 seconds.
- Message `View` is slow across message types.

Who is affected:
- Operators running live HF workflows where fast context switching is operationally critical.

Why now:
- UI responsiveness is part of operational safety and reliability.
- Current delays reduce trust and slow live decision-making.

## 3. User Constraints and Decisions

Confirmed constraints for this effort:
- Optimize for both Windows and Linux.
- Real workload can reach 300 new messages/day.
- DB schema migrations are allowed.
- Background refresh with visible progress indicators is allowed and preferred.
- Priority order for improvement:
  1. First tab open latency
  2. Messages performance (especially `View`)
  3. Warm tab switching
- No strict memory cap is imposed for this effort.
- Virtualized/paged models are allowed.
- Feature-flag rollout is required for safe fallback.
- The current development machine is representative of common community hardware and is the baseline test profile.

## 4. Goals

- Make first-open and warm tab interactions feel near-instant on community baseline hardware.
- Keep GUI thread responsive under production-like data volume.
- Reduce message `View` latency by eliminating repeated heavy parse paths on click.
- Preserve behavior correctness and existing workflows.
- Provide measurable performance budgets and regression protection.

## 5. Non-Goals

- No visual redesign unrelated to performance.
- No cloud dependency introduction.
- No changes to UTC-as-truth data contracts.
- No risky behavior changes without feature-flag guard and rollback path.

## 6. Performance SLOs (Targets)

These are release gates for this program.

- App startup to first interactive frame: `p95 < 2.0 s` on baseline machine.
- First open of each heavy tab (`Messages`, `Map`, `Operators`): `p95 < 1.0 s`, `p99 < 1.5 s`.
- Warm tab switch between any loaded tabs: `p95 < 200 ms`, `p99 < 350 ms`.
- Messages `View` action (click to content painted): `p95 < 300 ms`, `p99 < 500 ms`.
- Background refresh UI blocking budget: no single UI-thread task > 20 ms on hot paths.

## 7. Current Hotspots (Code-Level Baseline)

Primary synchronous bottlenecks identified:

- `freqinout/gui/main_window.py`
  - `_ensure_lazy_tab_loaded(...)` instantiates heavy tab widgets synchronously on first click.
  - `_set_screen(...)` activates tabs and immediately triggers activation work.

- `freqinout/gui/message_viewer_tab.py`
  - `on_tab_activated(...)` calls multiple refresh paths immediately.
  - `_populate_messages_table(...)` rebuilds derived rows and filters synchronously.
  - `_load_content(...)` reads/parses files on UI thread during `View`.

- `freqinout/gui/stations_map_tab.py`
  - Constructor calls `_load_operator_history()` and `_render_map()` path setup during first creation.
  - `_render_map(...)` issues several independent DB reads and large payload assembly synchronously.
  - Repeated calls to `_load_js8_links`, `_load_varac_links`, `_load_varac_stats`, `_load_js8_presence`, `_load_fldigi_presence`, `_load_spotter_station_status`, `_load_recent_calls` in render path.

- `freqinout/gui/operator_history_tab.py`
  - `_load_data(...)` performs ingest/backfill/read on activation.
  - `_render_rows(...)` fully rebuilds `QTableWidget` rows on each refresh/filter path.

### 7.1 Measured Baseline Snapshot (2026-02-15)

Source: `docs/perf-baseline-latest.md` on baseline community hardware.

- `main_window.set_screen`: p50 `466 ms`, p95 `16,899 ms`, p99 `18,494 ms`, max `18,720 ms`
- `messages.file_scan_total`: p50 `651 ms`, p95 `3,332 ms`, p99 `17,003 ms`, max `22,510 ms`
- `messages.refresh_js8_messages`: p50 `775 ms`, p95 `1,747 ms`, p99 `2,057 ms`
- `messages.populate_table`: p50 `508 ms`, p95 `854 ms`, p99 `1,006 ms`
- `messages.apply_filters`: p50 `517 ms`, p95 `823 ms`, p99 `1,002 ms`
- `operators.load_data`: p50 `346 ms`, p95 `1,846 ms`
- `operators.render_rows`: p50 `319 ms`, p95 `1,835 ms`
- `messages.view_message`: p50 `437 ms`, p95 `495 ms`, p99 `506 ms`

Interpretation:
- Long-tail latency is dominated by repeated synchronous refresh/rebuild paths in Messages and tab activation orchestration.
- Map render spans are not the primary contributor in this run.

## 8. Architecture Direction

### 8.1 Core principles

- No heavy I/O or parsing on the GUI thread.
- First paint fast, then progressive data hydration.
- Incremental updates over full-table rebuilds.
- Cached snapshots with explicit TTL and invalidation.
- Instrument first, optimize second.

### 8.2 New core modules (planned)

- `freqinout/core/perf_metrics.py`
  - Timing probes, percentile aggregation, tagged spans.

- `freqinout/core/background_tasks.py`
  - Shared worker orchestration for tab refresh tasks.

- `freqinout/core/messages_repository.py`
  - Canonical read/write layer for parsed message store and query APIs.

## 9. Phased Delivery Plan

### Phase 0: Instrumentation and Benchmark Harness (No behavior change)

Scope:
- Add high-resolution timing spans for startup, tab activation, data load, model bind, and view render.
- Emit structured logs through `freqinout.core.logger`.
- Add benchmark harness scripts for reproducible cold/warm tests on Windows and Linux.

Impacted files:
- `freqinout/gui/main_window.py`
- `freqinout/gui/message_viewer_tab.py`
- `freqinout/gui/stations_map_tab.py`
- `freqinout/gui/operator_history_tab.py`
- `tools/release_preflight.py` (perf baseline checks)
- `docs/perf-baseline.md` (new)

Acceptance criteria:
- Baseline metrics captured for cold/warm flows on both OSes.
- Timing spans identify top 5 contributors per slow workflow.
- No user-visible behavior changes.

Rollback:
- Disable instrumentation via `perf_metrics_enabled=0`.

### Phase 1: First-Open and Warm-Switch Responsiveness

Scope:
- Convert first-open tab work into staged activation:
  - Immediate shell paint.
  - Async data kickoff.
  - Progressive bind with visible progress state.
- Add shared non-blocking "refresh in progress" indicator pattern for heavy tabs.
- Defer non-critical tab initialization until after first interactive paint.
- Add checkpointed file-scan cache for Message Viewer:
  - Persist last known file records (`origin`, `path`, `mtime`, `size`) after scan.
  - Restore cached records on tab startup when watch-path signature matches.
  - Defer full filesystem rescan until after first paint or refresh interval.

Impacted files:
- `freqinout/gui/main_window.py`
- `freqinout/gui/message_viewer_tab.py`
- `freqinout/gui/stations_map_tab.py`
- `freqinout/gui/operator_history_tab.py`

Acceptance criteria:
- First open of each heavy tab meets SLO targets.
- Warm switching no longer runs heavy synchronous work in tab-change handler.

Rollback:
- `perf_pipeline_v1_enabled=0` restores legacy activation path.

### Phase 2: Messages Fast Path (Highest functional priority)

Scope:
- Introduce pre-parsed message store in `freqinout_nets.db` for file-backed messages (FLMSG/FLAMP/Spotter/BBS metadata).
- Store normalized preview payload at ingest time to avoid parse-on-click where possible.
- Implement content cache for rendered previews (`msg_render_cache`) with invalidation on file mtime/size or source row update.
- Move expensive parsing and format transformation off UI thread.
- Keep `View` action non-blocking with immediate skeleton and async fill.
- Phase 2A (interim): stage row-build and table-filter pipeline asynchronously so `populate` and `apply` do not block tab activation.
  - Build unified message rows on a worker thread from immutable snapshots.
  - Precompute row search text once per rebuild to reduce repeated filter costs.
  - Coalesce overlapping rebuild requests into a single pending refresh.
- Phase 2B (interim): reduce table-bind overhead on filter/apply.
  - Replace `ResizeToContents` column sizing on Messages table with fixed/interactive widths.
  - Skip model resets when filtered row sequence is unchanged.

Schema additions (draft):
- `messages_index`
  - canonical message key, source, origin, type, from/to, timestamps, status, title, summary, storage refs.
- `message_content_cache`
  - normalized content format (`plain|html|json`), cached body, cache version, source fingerprint.
- Indexes aligned to active filters/sorts (`type`, `status`, `from_call`, `to_call`, `rcv_ts`).

Impacted files:
- `freqinout/gui/message_viewer_tab.py`
- `freqinout/core/message_ingest.py`
- `freqinout/core/db_initializer.py`
- `tools/db_schema.py`

Acceptance criteria:
- `View` action meets SLO with existing dataset.
- Message tab first-open and filter/sort interactions are non-blocking.
- Legacy parsing fallback still works for unsupported/unknown formats.

Rollback:
- `perf_messages_store_v1_enabled=0` bypasses parsed store and reverts to legacy read path.

### Phase 3: Map Pipeline Optimization

Scope:
- Split map render into staged pipeline with cached data snapshots.
- Replace repeated multi-query per-render reads with batched repository call(s).
- Apply TTL-based caches for station presence/status datasets.
- Coalesce rapid filter changes into debounced render commits.
- Preserve current map behavior and propagation modes.

Impacted files:
- `freqinout/gui/stations_map_tab.py`
- `freqinout/core/db_initializer.py` (indexes)
- optional new `freqinout/core/map_repository.py`

Acceptance criteria:
- First Map open and warm refresh meet tab SLO.
- Filter changes remain responsive with no full map rebuild unless required.

Rollback:
- `perf_map_pipeline_v1_enabled=0` restores current render path.

### Phase 4: Operators Table Virtualization and Incremental Updates

Scope:
- Migrate from full `QTableWidget` rebuilds to model-based table (`QTableView + QAbstractTableModel`).
- Implement incremental fetch/paging and model diff updates.
- Move load/filter preprocessing off UI thread when dataset grows.

Impacted files:
- `freqinout/gui/operator_history_tab.py`

Acceptance criteria:
- Operators first open meets SLO.
- Filter/search/sort remain responsive under production-like dataset.

Rollback:
- `perf_virtualized_tables_v1_enabled=0` restores legacy table implementation.

### Phase 5: Cross-Cutting DB and Runtime Tuning

Scope:
- Validate/optimize SQLite indexes using `EXPLAIN QUERY PLAN` for hot queries.
- Ensure WAL mode, busy timeouts, and batched writes are consistently configured.
- Add periodic maintenance hooks (safe vacuum/checkpoint windows).

Impacted files:
- `freqinout/core/db_initializer.py`
- relevant ingest/repository modules
- `tools/freqinout-db.ps1` status checks/docs

Acceptance criteria:
- Hot queries use intended indexes on both Windows and Linux.
- No DB lock regressions in concurrent read/write workflows.

Rollback:
- Index changes are additive and backward-compatible; feature flags gate behavioral path changes.

## 10. Progress Indicators and UX Contract

- Heavy tab activation must show immediate visual state within 100 ms:
  - status text + determinate/indeterminate progress bar/spinner.
- Progress indicator must not block user navigation.
- Progress text examples:
  - `Loading Messages: scanning files...`
  - `Loading Map: preparing overlays...`
  - `Loading Operators: syncing records...`
- Completion state should clear automatically and preserve current selection/filter context.

## 11. Data Model and Migration Safety

Migration rules:
- `CREATE TABLE IF NOT EXISTS` and additive indexes only.
- Idempotent migration functions in `db_initializer`.
- Keep legacy message sources intact during transition.
- Backfill parsed message cache incrementally in background.

Failure modes and mitigations:
- Cache corruption or parse failure: fallback to legacy parser and log warning.
- DB lock contention: bounded retry + busy timeout + smaller write batches.
- Progress worker failure: show degraded banner, keep tab functional.

## 12. Verification Plan

### Automated baseline (required after meaningful performance changes)

```powershell
python tools/release_preflight.py
python -m compileall freqinout
```

When DB logic changes:

```powershell
powershell -ExecutionPolicy Bypass -File .\tools\freqinout-db.ps1 status
```

### Performance tests (required)

Run cold and warm scenarios on Windows and Linux using production-like staged data:
- Launch app and record startup timing spans.
- First-open timings for `Messages`, `Map`, `Operators`.
- Warm tab-switch loop across heavy tabs (at least 30 switches).
- Message `View` sampling across JS8/Spotter/FLMSG/FLAMP/VarAC/BBS.
- Map filter/pan/update timing.

### Manual checks

- Launch app with no radio software running.
- Exercise changed tabs and verify no UI freezes.
- Confirm settings persistence after restart.
- Verify background progress indicators appear and clear correctly.

## 13. Regression Risks and Mitigations

- Risk: stale cached data shown too long.
  - Mitigation: explicit TTLs + manual refresh action + timestamp labels.

- Risk: paging/virtualization changes selection behavior.
  - Mitigation: preserve selection contract, add focused UI tests, keep fallback flag.

- Risk: background workers race with UI state.
  - Mitigation: request IDs, last-write-wins reconciliation, cancel stale tasks.

- Risk: Linux/Windows perf divergence.
  - Mitigation: collect per-OS perf baselines and gate by OS-specific thresholds if needed.

## 14. Rollout Strategy

- Ship phase-by-phase under feature flags.
- Default new paths on for internal testing first.
- Promote to default-on after two stable cycles with no operational regressions.
- Maintain one-release rollback window for each major phase.

Feature flags (initial set):
- `perf_metrics_enabled` (default on)
- `perf_pipeline_v1_enabled` (default on after Phase 1 sign-off)
- `perf_messages_store_v1_enabled` (default off until Phase 2 sign-off)
- `perf_map_pipeline_v1_enabled` (default off until Phase 3 sign-off)
- `perf_virtualized_tables_v1_enabled` (default off until Phase 4 sign-off)

## 15. PR / Change Summary Requirements for This Program

Every performance PR must include:
- Problem solved.
- What changed and why.
- Baseline vs after metrics (`p50/p95/p99`) for affected workflow.
- Verification commands run and key outcomes.
- Regression risks and mitigation.
- Rollback path and controlling feature flag(s).

## 16. Immediate Next Step (Phase 2b - Low-Risk Table/Activation Latency Cuts)

Implement only this low-risk subset first:
- Remove synchronous scheduler-status refresh from tab-switch hot path in `main_window._set_screen(...)`.
- Reduce Message table reflow/rebind cost:
  - avoid content-based auto-resize on every refresh
  - no-op model reset when filtered rows are unchanged.
- Remove synchronous `OperatorHistoryTab.showEvent(...)` refresh and route through deferred `on_tab_activated(...)`.
- Add unchanged-data fast path in `OperatorHistoryTab._load_data(...)` to skip table rebuild when row fingerprint is unchanged.
- Defer `ControlFreqTab.set_tab_active(True)` initial refresh calls (`_refresh_frequency_control`, `_refresh_status_widgets`) with `QTimer.singleShot(0, ...)` so tab switch path stays non-blocking.
- Replace widget-heavy Operators table cells (checkbox/button widgets per row) with item-based cells + click handlers to reduce render cost.
- Restore robust SitRep update UX on Operators tab:
  - support both explicit anchor positions and legacy widget anchors when opening the status menu
  - apply in-place SitRep cell updates (without full table rebuild) when no active search filter.
- Smooth map propagation overlay costs by caching recent calls per band via a single bulk DB query (`_load_recent_calls_by_band`) and using longer TTL on recent-call caches.
- Add idle prewarm in `StationsMapTab` for propagation climatology cache and by-band recent-calls cache to reduce first overlay-compute spikes.
- Keep behavior backward-compatible and feature-flag neutral.

Acceptance criteria:
- No workflow or data correctness regressions.
- `main_window.set_screen` p95 and `messages.apply_filters` p95/p99 improve from current baseline.
- Verification baseline passes (`release_preflight`, `compileall`).

Rollback:
- Revert the focused Phase 1a commit; no schema change involved.

## 17. Pre-Benchmark Correctness and UX Fixes (2026-02-15)

Scope:
- Correct ControlFreq upcoming schedule evaluation for monthly/periodic net-style entries so entries are shown by true occurrence date, not weekday-only matching.
- Add active-state affordance on Operators `Clear Filters` button when any search/group filter is applied.
- Add Messages file-scan unchanged fast path: skip post-scan table rebuild and full scan-cache rewrite when file records are unchanged, while still updating scan metadata/dir mtimes.
- Add Messages quick-skip preflight: when cached watched-directory mtimes are unchanged, skip launching the file-scan worker and record a `quick_skip` scan span.
- Remove blocking network asset downloads from Map render path; resolve to local assets when present and otherwise use remote URLs so render is non-blocking.
- Add ControlFreq perf instrumentation spans so benchmark summaries can include ControlFreq tab activation/refresh timing.
- Optimize ControlFreq tab activation path: perform immediate lightweight status/frequency refresh and defer full refresh (`controlfreq.activation_refresh`) off the tab-switch hot path with staleness gating.
- Decouple ControlFreq intersection refresh from high-frequency activation/timer frequency-control updates so intersection DB queries run on full refresh cadence, not every 2 seconds.
- Remove duplicate ControlFreq activation refresh work triggered by `set_tab_active(True)` + `on_tab_activated()` in the same tab switch path.
- Split ControlFreq refresh into light activation refresh and deferred heavy refresh (`controlfreq.heavy_refresh`) so tab-open path is fast while message/propgation summaries catch up asynchronously.
- Add staged ControlFreq deferred refresh phases (`controlfreq.secondary_refresh` then `controlfreq.heavy_refresh`) after fast activation/periodic refresh passes.
- Keep ControlFreq activation/periodic refresh non-blocking by excluding synchronous software-status probes from `_refresh_all(...)`; status continues on its dedicated timer/deferred path.
- Avoid duplicate first-open map renders by clearing `_map_dirty` before `visible_init` render; allow dirty to be re-set only by real updates during webview load.
- Reduce map first-reload payload overhead by bootstrapping webview HTML with empty marker/link arrays and sending real payload only via post-load `updateMapData(...)`.
- Fix map link persistence across layer/theme reloads by resetting per-page payload signature before/after webview reload so identical payloads still apply to the new page context.
- Fix `Peer Sched Now` legend sizing/reset behavior by updating legend content from live payload state (not page-init-only state) so toggle-off restores normal legend size.
- Reduce map layer/theme-change visual disruption by keeping the current map visible during webview reloads (show full loading placeholder only for first load/failure).
- Defer StationsMap initial operator-history load and first render from constructor to first visible-map callback to reduce lazy-tab creation latency.
- Include Digi/SSB NCS and JS8 NCS tabs in benchmark scenarios and summary filtering with explicit activation spans.
- Tune Operators tab activation refresh cadence (default 60s min interval, configurable) and remove eager startup load for hidden tab to reduce repeated heavy reload spikes during tab-switch benchmarks.

Impacted files:
- `freqinout/gui/controlfreq_tab.py`
- `freqinout/gui/operator_history_tab.py`
- `freqinout/gui/message_viewer_tab.py`
- `freqinout/gui/stations_map_tab.py`
- `freqinout/gui/controlfreq_tab.py`

Acceptance criteria:
- A net configured as first-Sunday monthly is not shown in ControlFreq `Today` unless today is the first Sunday.
- ControlFreq `7 Days` only shows rows whose next occurrence falls within the 7-day horizon.
- Operators `Clear Filters` is visually highlighted whenever search text is non-empty or group filter is not `All`; returns to normal style after clearing filters.
- For unchanged file scans, Messages skips redundant rebuild work and emits scan metrics with `unchanged=true`.
- For unchanged directory trees, Messages can skip the scan worker entirely (`mode=quick_skip`) without data regressions.
- Map first render does not perform synchronous Python-side network downloads for Leaflet/GeoJSON assets.
- ControlFreq emits `controlfreq.on_tab_activated` and `controlfreq.refresh_all` spans during benchmark workflows.
- ControlFreq activation no longer blocks on full refresh when recent data exists; heavy refresh runs deferred and rate-limited.
- ControlFreq frequency/status tick path avoids intersection-summary DB work; intersection updates continue on full refresh and explicit paths.
- ControlFreq `on_tab_activated` now gates/delegates deferred full refresh only; immediate frequency/status updates are handled by the existing deferred tick hooks from `set_tab_active(True)`.
- ControlFreq activation path avoids heavy summary/propagation work synchronously; heavy sections run deferred and rate-limited.
- ControlFreq activation/periodic refresh now execute a minimal pass first and queue secondary + heavy sections, reducing tab-switch blocking.
- ControlFreq activation refresh no longer synchronously probes process/API status in `_refresh_all(...)`; status still updates via timer/deferred refresh with no behavior loss.
- Map first-open no longer performs a redundant post-load render when no data changed during webview initialization.
- Map webview reload path no longer serializes marker/link payload twice (HTML bootstrap + immediate JS push), reducing first-render and force-reload overhead.
- Map links remain visible after layer/theme changes when payload content is unchanged (no false de-dup skip across page contexts).
- Map legend `Peer Sched Now` section appears only while enabled and reverts immediately when toggled off.
- Changing map layers/themes no longer forces a full blank `Loading map...` placeholder when a map is already visible.
- Map tab constructor no longer performs operator-history DB load and initial render synchronously; these now run on first visible activation.
- Benchmark workflows and focused summaries now include `digi_ncs.on_tab_activated` and `js8_ncs.on_tab_activated`.
- Operators tab now lazy-loads on activation instead of eager startup load, with a longer activation reload interval to reduce long-tail `operators.load_data/render_rows` spikes.

Rollback:
- Revert this focused patch set; no DB/schema migration involved.

## 18. VarAC Data Foundation (Phase 1a - Background Ingest Only)

Status: In Progress

Date: 2026-02-16

Implementation note (2026-02-16):
- Phase 1a schema and background ingest plumbing is implemented in `freqinout/core/varac_ingest.py` and mirrored in `tools/db_schema.py` for DB admin/status tooling.

### Scope

Add a read-only, incremental background ingest foundation for VarAC SQLite data so FreqInOut can leverage richer signals without blocking UI workflows.

This phase is data-plane only. It does not require immediate UI placement for every new dataset.

Primary ingest targets:
- Message semantics:
  - `vmail` + `vmail_folder` for true folder semantics (Inbox/Sent/Outbox/Parking).
  - `vmail_relay_notification` for relay inbox support.
  - `urgent` and `has_attachment` fields for message badges.
- Map signals:
  - `cqframe` (+ `cqframe_type`) for beacon/CQ density overlays.
  - `broadcast` including `via_callsign` for path context.
  - per-band recency inputs for activity heat.
- Propagation signals:
  - `qso_snr_report` as high-confidence, per-QSO SNR trend data.
  - source-tier metadata so downstream scoring can weight certainty.

### Data and Schema Plan

Extend local `freqinout_nets.db` VarAC mirror schema with:
- durable ingest checkpoints per source table (high-watermark by `id`)
- run health/status records (last run time, success/failure, error text, rows ingested)
- normalized local mirror tables for lookup and event streams required by planned UI/features

Preserve existing local tables and read paths:
- `varac_messages`
- `varac_links`
- `varac_callsign_stats`

No behavior-breaking schema removal in this phase.

### Reliability/Performance Guardrails

- Keep ingest read-only against `VarAC.db`.
- Keep ingestion idempotent via primary keys and upserts.
- Use incremental reads (`id > last_id`) with safe defaults when checkpoints are absent.
- Never fail app startup when VarAC path/db is missing or inaccessible.
- Continue logging through `freqinout.core.logger` with concise diagnostics.

### Failure Modes and Mitigations

- VarAC DB unavailable/locked:
  - Skip ingest run, record failed sync status, keep prior local data.
- Source table absent (version drift):
  - Skip that table only; do not fail full ingest.
- Partial ingest failure:
  - Commit successful table chunks; preserve per-table checkpoint integrity.
- Schema drift in local DB:
  - Additive migration only (create table/add columns/indexes).

### Acceptance Criteria

- Background ingest persists incremental checkpoints and run health metrics.
- Target tables (`vmail_folder`, `vmail_relay_notification`, `broadcast`, `cqframe`, `qso_snr_report`) are mirrored locally when present in VarAC DB.
- Existing consumers (`Messages`, `Map`, `ControlFreq`, propagation ingest) continue functioning with no regression.
- Verification baseline passes:
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`
  - `powershell -ExecutionPolicy Bypass -File .\tools\freqinout-db.ps1 status`

### Rollback

- Revert the VarAC ingest foundation commit.
- Existing runtime behavior remains intact because this phase is additive and background-only.

## 19. Memory Stability Hardening (Linux Long-Run)

Status: In Progress

Date: 2026-02-16

### Problem

On Linux, users report application slowdown over long run-times. Primary risk is unbounded growth in runtime caches and temporary map artifacts, plus query cost drift as traffic tables grow.

### Goals

- Bound in-memory growth for long-lived caches.
- Prevent accumulation of temporary map-render files.
- Keep map/query refresh performance stable as history tables grow.
- Preserve existing behavior and UX contracts.

### Phased Implementation

Phase A (low-risk cache bounds):
- Add bounded eviction to propagation empirical cache.
- Bound JS8 form-definition caches in message ingest/viewer paths.

Phase B (map temp artifact control):
- Replace repeated one-off map temp file creation with reusable path and cache-busting URL query.
- Add shutdown cleanup path for managed map temp file.

Phase C (query drift mitigation):
- Add `js8_links(ts)` index creation to runtime DB initializer and DB tooling schema metadata.

### Acceptance Criteria

- No unbounded memory growth from the identified caches in normal use.
- Map tab no longer leaves unbounded temp HTML files during layer/theme/map payload reloads.
- JS8 link queries used by map recency filters use indexed `ts` path after migration/init.
- Existing functionality remains unchanged for:
  - map rendering and layer switching
  - message decoding and display
  - propagation overlay calculations

### Verification

- `python -m compileall freqinout`
- `python tools/release_preflight.py`
- `powershell -ExecutionPolicy Bypass -File .\tools\freqinout-db.ps1 status`

### Rollback

- Revert memory-hardening commit set for Phase A/B/C.
- Changes are additive and do not remove existing schema columns/tables.

## 20. Unified SitRep Normalization (CommStat + JS8Spotter)

Status: Planned

Date: 2026-02-16

### Problem

SitRep-like data arrives from multiple sources with overlapping semantics:
- CommStat 2.3 and CommStat-Improved 3.x formats
- JS8Spotter MCForms (`F!104`, `F!301`, `F!304`) and `csstatrep`

Current behavior is partially source-specific and not normalized across:
- Messages tab (categorization and decode consistency)
- Operators tab (effective status and source conflict visibility)
- Map tab (consistent status rendering and source provenance)

### Objective

Create one canonical SitRep model and rendering contract so Messages, Operators, and Map present consistent, source-aware status while keeping UI fast.

### Canonical Message Category Model

Messages tab category for this domain:
- `SitRep`

Required source/subtype tags:
- `source`: `COMMSTAT23`, `COMMSTAT3`, `JS8SPOTTER`
- `subtype`: `COMMSTAT_12`, `COMMSTAT_FWD`, `SPOTTER_104`, `SPOTTER_301`, `SPOTTER_304`

### Canonical SitRep Fields

Normalized dimensions (per event and per latest projection):
- `scope`
- `overall_status`
- `power`
- `water`
- `medical`
- `communications`
- `internet`
- `travel`
- `food`
- `fuel`
- `crime`
- `civil_unrest`
- `political`

Normalized values:
- `green | yellow | red | unknown | not_reported`

### Mapping Contract (Source -> Canonical)

- CommStat 12-digit payload:
  - maps directly to all canonical dimensions.
- Spotter `F!104`:
  - maps to `overall_status`; all other dimensions `not_reported`.
- Spotter `F!301`:
  - maps `scope`, `communications`, `internet`, `water`, `power`;
  - remaining dimensions `not_reported`.
- Spotter `F!304`:
  - maps `communications`, `internet`, `water`, `power`;
  - remaining dimensions `not_reported`.

### Phased Delivery Plan

Phase 0: Spec lock and baseline
- Lock canonical schema, mapping rules, precedence/conflict policy.
- Capture before metrics for first-open, warm-switch, and SitRep-heavy filters.
- Acceptance:
  - mapping contract approved and traceable.
  - baseline metrics recorded on Windows and Linux.
- Rollback:
  - none (spec/measurement only).

Phase 1: Source adapters and incremental ingest
- Add background adapters for:
  - JS8Spotter DB (`forms`, `csstatrep`)
  - CommStat-Improved DB (`statrep`, `messages`, `alerts`)
  - CommStat 2.3 DB (`StatRep_Data` and related minimum tables)
- High-watermark ingest only; no full-table rescans in steady state.
- Implementation notes (v1.1.8):
  - Add one-time historical backfill from local `spotter_traffic` into unified SitRep staging so legacy Spotter forms can appear under normalized `SitRep` views.
  - Backfill is checkpointed and auto-completes with a done flag to avoid repeated full rescans.
- Acceptance:
  - incremental ingest checkpoints persisted.
  - ingestion survives missing/locked source DBs without startup failure.
- Rollback:
  - feature flag off for each source adapter; keep existing Spotter ingest path.

Phase 2: SitRep fusion model and projections
- Add canonical `sitrep_events` and `sitrep_latest_by_callsign` local tables.
- Add dedupe key strategy and source provenance columns.
- Compute `effective_status` and per-source status views.
- Acceptance:
  - same report from multiple sources collapses correctly.
  - per-callsign latest projection query returns in bounded time.
- Rollback:
  - disable fusion projection use; retain raw-source displays.

Phase 3: Messages tab normalization
- Add `SitRep` category and subtype filters.
- Render normalized fields + raw payload + source metadata in one view.
- Ensure decode behavior is identical regardless of source origin.
- Implementation notes (v1.1.8):
  - Add display-level dedupe to suppress duplicate raw Spotter rows when an equivalent normalized SitRep row exists.
  - Dedupe runs in row-build path only (no source-table deletion/mutation) and uses compact semantic report keys for O(n) behavior.
- Acceptance:
  - SitRep rows consistently categorized under `SitRep`.
  - decoded field panel matches canonical mapping contract.
- Rollback:
  - hide normalized SitRep category via feature flag and use legacy row rendering.

Phase 4: Operators tab normalization
- Show effective status plus source chips and recency.
- Add conflict indicators when sources disagree.
- Keep manual override supported and clearly tagged.
- Acceptance:
  - operators list shows one consistent effective status per callsign.
  - conflict and source provenance visible without opening detail dialogs.
- Rollback:
  - disable effective-status projection and use existing status source path.

Phase 5: Map tab normalization
- Use same effective status logic and source metadata as Operators.
- Popup includes effective status, source statuses, and age.
- Preserve existing layer toggles while removing source-specific inconsistencies.
- Implementation notes (v1.1.8):
  - Map SitRep status lookup now merges `spotter_station_status` (legacy/manual) with fused projection from `sitrep_latest_by_callsign` when `sitrep_unified_map_enabled` is on.
  - Marker/popup data now carries source chips and conflict hints so map provenance aligns with Operators.
- Acceptance:
  - map marker color and operator row color agree for same callsign/status window.
  - popup reflects source and timestamp provenance.
- Rollback:
  - disable normalized sitrep rendering path for map markers/popups.

Phase 6: Performance and soak hardening
- Index/tune new SitRep tables and hot queries.
- Add memory and long-run stability checks on Linux and Windows.
- Acceptance:
  - no regression to tab SLOs from SitRep normalization.
  - long-run soak shows no unbounded memory growth trend from new caches/projections.
- Rollback:
  - disable SitRep normalization flags and use legacy source-specific paths.

### Peer Schedule Overlap Accuracy Fix (v1.1.8)

- Problem:
  - Peer Schedules overlap indicator can miss valid overlaps because computation is constrained to current UTC day/time.
  - Existing overlap summary text is not optimized for quick operator reachability decisions.
- Change:
  - Evaluate overlaps against weekly schedule windows (day-aware), including overnight windows, instead of today/now-only clipping.
  - Keep exact frequency and mode matching behavior unchanged.
  - Update overlap summary display to prioritize actionable windows:
    - `NOW HH:MM-HH:MM` when overlap is active.
    - `Today HH:MM-HH:MM` for the next overlap later today.
    - `Day HH:MM-HH:MM` for the next upcoming weekly overlap on another day.
- Acceptance:
  - Overlap column reports expected overlaps for non-today rows and overnight schedule ranges.
  - Overlap column gives an immediately actionable next window (`NOW`, `Today`, or day+time).
  - Overlap detail tooltip/dialog remains responsive with multiple ranges.
- Rollback:
  - Revert to prior today/now overlap logic in `peer_sched_tab.py`.

### Map Peer Sched Now Accuracy Fix (v1.1.8)

- Problem:
  - Map `Peer Sched Now` can miss valid peers currently on the same frequency due to strict day matching, schedule-source lookup gaps, and marker visibility being tied to recent traffic.
  - Peer Sched legend text is too verbose/wide and slows visual interpretation during operation.
- Change:
  - Read active personal schedule frequencies from both schedule DBs (`freqinout.db` + `freqinout_nets.db`) for both daily/net table variants.
  - Include current scheduler frequency as an active-frequency source to match live schedule state.
  - Normalize day parsing for peer schedules (supports abbreviations) and use day-aware active-window evaluation, including overnight windows.
  - Use practical same-frequency matching tolerance (`<= 0.001 MHz`) and include matched peer callsigns in marker visibility even when no fresh link edge exists.
  - Replace Peer Sched legend prose with compact status swatches and matching marker-ring colors:
    - Green = `NOW`
    - Blue = `Later Today`
    - Purple = `QSY < 10m`
- Acceptance:
  - `Peer Sched Now` includes peers whose active peer schedule frequency matches an active local schedule frequency at current UTC time.
  - Matched peers remain visible as stations on the map even if they are not part of recent link lines.
  - Overnight windows and day abbreviations are handled correctly.
  - Legend remains compact and readable without long wrapped explanatory text.
- Rollback:
  - Revert map peer-schedule active-window logic in `stations_map_tab.py`.

### Peer Schedule Inference (v1.1.8)

- Problem:
  - Many operators do not publish/import peer schedule files, but repeated on-air activity can still indicate practical weekly reachability windows.
- Change:
  - Add background inferred peer schedule generation from observed traffic (`js8_links`, `varac_links`) using UTC day/time buckets and rolling-week recurrence scoring.
  - Persist inferred rows in a dedicated table (`peer_hf_schedule_inferred`) with confidence/sample metadata.
  - Add an effective peer schedule view (`peer_hf_schedule_effective`) where imported schedules always take precedence over inferred schedules for the same callsign.
  - When importing a peer schedule file for a callsign, remove stale inferred rows for that callsign.
- Acceptance:
  - Peer schedules appear even without imported files when recurrence confidence is sufficient.
  - Importing a schedule for a callsign replaces inferred schedule visibility for that callsign.
  - Map/ControlFreq/Peer Schedule reads use the effective schedule source without regressions.
- Rollback:
  - Disable background inference and revert read paths to `peer_hf_schedule` only.

### ControlFreq Intersection Reliability Fix (v1.1.8)

- Problem:
  - `ControlFreq > Schedule Intersections` can show empty peer overlaps when schedules are overnight, near UTC day boundary, or mirrored in alternate DB locations.
- Change:
  - Replace today-only overlap checks with next-2-hour weekly segment intersection logic (supports overnight/day-wrap windows).
  - Read schedule rows from both settings and nets DB table locations for overlap inputs.
  - Keep exact-frequency semantics while using robust frequency parsing tolerance.
- Acceptance:
  - Intersections populate for valid peer overlaps occurring now or in the next 2 hours, including cross-midnight windows.
- Rollback:
  - Revert to prior day-local overlap checks in `controlfreq_tab.py`.

### Callsign Canonicalization and Trust Policy Hardening (v1.1.8)

- Problem:
  - Portable/mobile suffix variants (example: `K0RPG` vs `K0RPG/Z`) can fragment inferred peer schedules into separate identities.
  - Some auto-ingested operator rows can end up `Trusted` by default, which conflicts with operational intent that trust should be explicit.
- Change:
  - Canonicalize callsigns for peer-schedule inference grouping by stripping common trailing portable/mobile suffixes, while keeping raw link records unchanged for provenance.
  - Update operator trust defaults for ingestion paths so auto-discovered operators default to `Untrusted` (`0`).
  - Keep CSV/manual workflows able to set `Trusted` explicitly.
  - Normalize JS8 auto-upsert paths to use base callsign for operator-table keys.
- Acceptance:
  - Inference produces one canonical schedule identity for suffix variants.
  - `operator_checkins` rows created by traffic/auto-ingest default to `trusted=0`.
  - Existing manual/CSV trust assignment remains available and explicit.
- Rollback:
  - Revert canonicalization helper usage in `peer_schedule_infer.py`.
  - Restore previous trust defaults in operator ingestion/migration paths.

### ControlFreq UX Refactor (v1.1.8+)

- Problem:
  - `ControlFreq` currently mixes high-priority operational controls with secondary context, creating visual clutter and slower operator comprehension.
  - Frequency control does not have a clear single-anchor "current frequency" presentation.
- Goals:
  - Make `Current Frequency` the visual and operational anchor.
  - Reduce cognitive load by default while preserving all existing capability.
  - Keep tab activation and refresh behavior non-blocking.

#### Phase 1 (this change): Frequency Control "Now Card"

- Change:
  - Introduce a hero frequency control in `Frequency Control` where the selected/current frequency is the primary dropdown display.
  - Keep selected frequency emphasis while reducing dropdown item font size to lower visual dominance.
  - Add a compact schedule-state badge (`On Schedule`, `Off Schedule`, `Blocked`) with color semantics.
  - Consolidate scheduled/active details into a clearer two-line summary:
    - `Scheduled: {Group} | {Band} - {Frequency}`
  - Remove redundant `Active` line when hero control itself represents current frequency.
  - On `Resume Schedule`, re-sync hero control selection to scheduler/current frequency.
  - Replace redundant status text with a single `Next Change: {freq} HH:MM` line.
  - Remove separate suspend/mode text from the strip to reduce visual noise; scheduling details remain available in main scheduler views.
  - Simplify frequency dropdown row labels to compact frequency-first labels for faster scanning.
  - Keep `Frequency Control` and `Message Summary` persistent in the top row.
  - Keep `Frequency Control` and `Message Summary` equal height for visual balance.
  - Constrain `Frequency Control` to content-sized width and allow `Message Summary` to consume remaining horizontal space.
  - Move `Activity` into the main card area with `Schedule Intersections` instead of the top row.
  - Remove the visible `Focus Mode` toggle from the header.
  - Ensure `Message Summary` shows 7 visible rows without partial clipping and sort rows alphabetically by message type.
- Non-goals (Phase 1):
  - No removal of existing schedule/intersections/messages/propagation tables yet.
  - No change to scheduler correctness logic or backend control behavior.
- Acceptance:
  - Operator can identify current frequency and schedule alignment within one glance.
  - Frequency control section has fewer prominent text rows while retaining existing actions.
  - No regression in QSY, resume, scheduler status refresh, or focus mode behavior.
- Rollback:
  - Revert `controlfreq_tab.py` UI readout additions and label compaction; restore prior frequency-control rendering.

#### Phase 2 (follow-up): Information hierarchy

- Planned:
  - Promote task-flow cards (`Now`, `Next`, `Actions`) and progressively disclose lower-priority context.
  - Keep detailed summaries available behind compact expanders or secondary views.

#### Phase 2a (this change): View Bar Chips + Presets

- Change:
  - Add a `View` control bar with a preset selector and chip toggles to control main cards:
    - `Activity`, `Intersections`, `Schedule`, `Propagation`.
  - Place the `View` bar directly below the persistent top row (`Frequency Control` + `Message Summary`) to reduce pointer travel.
  - Default preset is `All` so existing visibility behavior is preserved on upgrade.
  - Keep `Frequency Control` and `Message Summary` persistent and unaffected by View toggles.
  - When cards are sharing a splitter region, default to equal space distribution.
    - `Operations` defaults to equal width between `Activity/Intersections` and `Schedule`.
  - Persist selected preset/chip state and restore it on next launch.
  - Apply smooth card show/hide transitions (short expand/collapse animation) without blocking UI.
  - Do not reintroduce Focus Mode behavior.
  - Move propagation hint text into the top-right area of the propagation section header for cleaner association.
  - Keep `Propagation Forecast` in native group-box title styling for visual consistency with other cards.
  - Move propagation hint text to the right of the target selector row and keep it concise.
  - Enable type-to-search behavior for the propagation target value dropdown across Region/State/Operator modes.
- Acceptance:
  - Operator can quickly tailor visible cards without changing tabs.
  - Hidden cards still refresh in background so re-show is immediate.
  - No regression in scheduler status, QSY actions, or tab activation responsiveness.

#### Phase 2b (this change): ControlFreq Performance Hardening

- Change:
  - Debounce search/group filter-triggered refreshes to avoid full refresh work on every keystroke.
  - Gate secondary/heavy refresh work by visible cards from View state:
    - Skip `Activity`/`Intersections`/`Schedule`/`Propagation` refresh when hidden.
  - Cache frequency combo source rows and avoid rebuilding the dropdown unless operating-group content changes.
  - Cache expensive operator-group and local-schedule reads used by intersection/activity computations with short TTL + DB mtime invalidation.
  - Reduce status polling cadence to lower UI-thread pressure while preserving startup/status updates.
- Acceptance:
  - Typing in search feels responsive with fewer UI stalls.
  - Hidden cards no longer consume meaningful refresh time.
  - Frequency dropdown remains correct but no longer rebuilds every tick when unchanged.
  - No regression in schedule correctness, QSY actions, or status display behavior.

#### Phase 2c (this change): Resume/SitRep Interaction Latency Polish

- Problem:
  - Manual SitRep updates from `Operators` can feel slow because dependent-view refresh fanout triggers an unnecessary full Operators reload.
  - `Resume Schedule` can feel delayed before `ControlFreq` hero/status displays reflect resumed state, especially when triggered from the global main-panel button.
- Change:
  - Split operator-history refresh fanout paths in `MainWindow`:
    - Keep full `refresh_operator_history_views()` behavior for external ingests/imports.
    - Route local Operators-tab update signal to a lightweight dependent-only refresh path (Map/FLDigi), avoiding a redundant Operators full reload.
  - Add a `ControlFreq` resume-sync helper invoked by both local and global resume actions:
    - force immediate hero re-sync,
    - refresh status strip immediately,
    - run short delayed follow-up refresh pulses to absorb asynchronous scheduler/radio apply completion.
  - Trigger immediate + short delayed scheduler status panel refresh from global `Resume Schedule`.
- Acceptance:
  - Manual SitRep changes in Operators update visible cell quickly and no longer trigger a multi-second Operators table reload loop.
  - Resume from either `ControlFreq` or main-panel `Resume Schedule` updates hero/state readout noticeably faster and converges within short follow-up pulses.
  - No regression in scheduler correctness or cross-tab operator/map consistency.

#### Phase 2d (this change): Map Mode Coherence + Peer Callsign Normalization

- Problem:
  - `Map` allows `Peer Sched Now` and `SitRep Status` station-pin modes at the same time, creating ambiguous pin semantics.
  - `SitRep Status` currently suppresses link rendering, which removes useful path context.
  - Peer schedule visibility can diverge between `Map` and `Peer Schedules` when callsign suffix variants (`/P`, `/M`, `/Z`, etc.) are present.
- Change:
  - Make `Peer Sched Now` and `SitRep Status` pin modes mutually exclusive in `Map`.
  - Keep link rendering available when either pin mode is active (subject to normal link-mode selection).
  - Normalize peer schedule callsigns to base form for read/display/matching in both `Map` and `Peer Schedules`.
  - Normalize imported owner callsign and delete/import cleanup paths so suffix variants do not create fragmented peer schedule identity.
- Acceptance:
  - Enabling one pin mode automatically disables the other.
  - Links can still render while either pin mode is active.
  - Callsign base identity is consistent across `Map` and `Peer Schedules` for suffix variants.

#### Phase 2e (this change): Settings Tab Perf Visibility + Active-Only Status Polling

- Problem:
  - `Settings` is reported as heavy but currently lacks explicit perf spans in key paths, reducing benchmark visibility.
  - Process status polling in `Settings` runs continuously even when tab is inactive.
- Change:
  - Add perf spans for Settings hot paths:
    - `settings.load_settings`
    - `settings.on_tab_activated`
    - `settings.refresh_running_status`
    - `settings.refresh_launch_control_table`
    - `settings.refresh_operating_groups_table`
  - Add `set_tab_active(...)` and activation gating in `Settings`:
    - run status timer only when Settings tab is active,
    - stop status timer when tab is inactive,
    - perform immediate status refresh on activation.
- Acceptance:
  - Benchmark summaries include Settings spans.
  - Settings status polling does not run while tab is hidden.
  - No regression in Settings status display when tab is visible.

#### Phase 2f (this change): Settings Save-Path Latency Cuts

- Problem:
  - `Save Settings` feels heavy, especially when change propagation fanout runs.
  - Save-path signal fanout currently risks duplicate downstream refresh work.
- Change:
  - Remove duplicate `settings_saved` emission from Save button path.
  - Emit `settings_saved` once via deferred pulse after save, so Save UI is less blocking.
  - Add `settings.save_settings` perf span for save-path benchmarking.
  - Gate expensive operator-history fanout after save to only run when operator identity/grid fields changed.
  - Defer operator-history fanout call to next event-loop tick when needed.
- Acceptance:
  - Save action no longer triggers duplicate settings fanout.
  - Save-path p95 improves for routine setting edits.
  - Operator history/map refresh still occurs when operator identity/grid values actually change.

#### Phase 2g (this change): Benchmark Consistency Without App Log Noise

- Problem:
  - Benchmark runs depend on `freqinout.log` `PERF|...` lines, which disappear when log level is `DISABLED`.
  - Enabling INFO/DEBUG logging during benchmark can add extra non-perf I/O and make test conditions less representative.
- Change:
  - Add a dedicated perf sink file (`perf_metrics.log`) used as a fallback when normal logger output for perf spans is not available due to logger disable/level gating.
  - Keep existing `PERF|...` logger output behavior unchanged when logger output is enabled.
  - Extend `tools/perf_benchmark.py` auto log discovery/reset to include `perf_metrics.log` so benchmark commands remain consistent.
- Acceptance:
  - Perf summaries work with `log_level=DISABLED`.
  - Benchmark protocol no longer requires changing normal log level.
  - Existing benchmark summaries still work when spans are present in `freqinout.log`.

#### Phase 2h (this change): Net Resources Catalog + Active Schedule Promotion

- Problem:
  - `Net Schedule` currently mixes long-term schedule curation and active schedule operations in one table.
  - Importing JSON today replaces the active schedule set, which is high risk and not ideal for reusable net catalogs.
  - Operators need seasonal/shared net catalogs (Winter/Summer) with quick promotion into active schedule.
- Change:
  - Add a read-only `Net Resources` catalog table below active `Net Schedules`.
  - Ship built-in resource sets from:
    - `config/net_resources/sitrepnets-fall.json` (displayed as Winter set)
    - `config/net_resources/sitrepnets-summer.json`
  - Add resource metadata columns:
    - `Source` (`Built-in`, `Imported`, `Manual`, `Migrated`)
    - `Set` (`Winter`, `Summer`, `Custom`, etc.)
  - Add actions:
    - `Add Selected to Net Schedule`
    - `Add Filtered to Net Schedule`
    - `Move Selected to Resources`
  - Make resources sortable by header and filterable via global search.
  - Remember the last selected resource set between sessions.
  - Change JSON import behavior to import into `Net Resources` by default.
  - On upgrade, backfill existing active schedule rows into resources (`Migrated`) while preserving active rows.
  - Add duplicate guard when promoting resources into active schedule:
    - block duplicates by `(day + start + end + band + frequency + mode)`,
    - show detailed collision report for resolution.
  - Support round-trip edit flow:
    - rows promoted from resources to active schedule can be edited,
    - moving edited rows back to resources updates the corresponding resource entry.
  - Add citation text in Net Resources section:
    - `Visit SitRepNet.com for more information.`
- Acceptance:
  - Built-in Winter/Summer resources are visible without manual import.
  - Existing users retain active schedule behavior; no scheduler regression.
  - Active schedule can be curated from resources with duplicate protection and clear conflict details.
  - Imported/manually moved rows persist in resources and are available on subsequent launches.

#### Phase 2i (this change): Editable Net Resources + FLDigi Start Parameters

- Problem:
  - Users need to correct typos and fill missing FLDigi start parameters in seasonal resources.
  - Current Net Resources table is read-only and lacks FLDigi mode/offset fields.
  - Scheduler currently resolves FLDigi mode/offset from Operating Groups only.
- Change:
  - Extend `net_resources` and active net schedule persistence with optional:
    - `fldigi_mode`
    - `fldigi_offset`
  - Add Net Resources UI actions:
    - `Edit Selected Resource`
    - `Delete Selected Resources`
    - `Export Resource Set`
    - `Publish Set to Delivery File` (writes to `config/net_resources` with set-based filenames)
  - Expand Net Resources grid columns to display FLDigi mode/offset.
  - Allow resource-edit dialog updates for typos and FLDigi parameters.
  - Scheduler precedence update:
    - use entry-level `fldigi_mode` / `fldigi_offset` when provided,
    - otherwise fall back to Operating Groups behavior.
- Acceptance:
  - Operators can correct seasonal resource rows in-app and publish updated delivery JSON files.
  - Net starts honor explicit row-level FLDigi mode/offset values when present.
  - Existing behavior remains unchanged when row-level FLDigi values are absent.

#### Phase 2j (this change): FLDigi Mode Picker Consistency + Net Schedule Visibility

- Problem:
  - Net Resources edit currently allows free-text FLDigi mode, which can lead to typos and non-standard values.
  - Active `Net Schedule` rows persist FLDigi mode/offset but do not show those fields in the main table, reducing operator clarity.
- Change:
  - Reuse the same FLDigi mode pick-list behavior used in `Settings -> Operating Groups`:
    - editable combo box,
    - option list-backed completion,
    - no arbitrary insert into the base list.
  - Apply this FLDigi mode picker to Net Resources row editing.
  - Add visible `FLDigi Mode` and `FLDigi Offset` columns to active `Net Schedule`.
  - Wire row collection/saving/loading to read/write these columns directly, with fallback compatibility for legacy rows.
  - Continue to default FLDigi mode/offset from matching Operating Group when a row has no explicit values.
- Acceptance:
  - Users can select FLDigi mode from a constrained dropdown/completer in Net Resources edit.
  - Net Schedule table clearly displays FLDigi mode/offset per row.
  - Saving and reloading Net Schedule preserves FLDigi mode/offset values without regression.

#### Phase 2k (this change): Net Resources Bootstrap Idempotency

- Problem:
  - Built-in Net Resources can duplicate on app startup when upsert key matching misses mixed-case values.
- Change:
  - Make Net Resources upsert matching case-insensitive for text fields and numeric-tolerant for frequency comparisons.
  - Match on a full resource identity (set/day/recurrence/month-weeks/group/band/mode/frequency/start/end/net-name) to avoid false merges.
  - Add a startup dedupe pass for `net_resources` using the same normalized identity key, keeping the most recently updated row.
- Acceptance:
  - Re-opening the app does not create additional Net Resource duplicates.
  - Existing duplicated rows are collapsed safely on next startup/bootstrap.

#### Phase 2l (this change): Net Resources Workflow Simplification

- Problem:
  - Resource import actions are visually associated with Net Schedule instead of Net Resources.
  - Operators need explicit import semantics (`Merge/Update` vs `Replace`) and a safe export that never overwrites files.
  - Add-to-schedule flow needs clearer intent and confirmation.
- Change:
  - Add `Manage Net Resources` split-button in Net Resources section with:
    - `Import (Merge/Update)`
    - `Import (Replace Built-in Set)`
    - `Export New Resource File`
  - Remove schedule-row `Import Net Resource` button from the Net Schedule action row.
  - Startup built-in resource seeding runs only when `net_resources` is empty.
  - `Import (Merge/Update)`:
    - join key: `frequency + start_utc + end_utc + fldigi_mode`
    - numeric-tolerant frequency matching,
    - update all non-key fields when matched.
  - `Import (Replace Built-in Set)`:
    - same merge/update behavior for matched rows,
    - remove rows missing from incoming file only when `source_type='builtin'`,
    - never remove `source_type='manual'`.
  - Export writes a new timestamped resource JSON via the same chooser behavior used by `Export Net Schedule`.
    - If chosen filename exists, create a suffixed non-overwrite variant.
  - Replace separate `Add Selected` / `Add Filtered` buttons with `Add to Net Schedule` split-button.
    - default action: add selected if selection exists, otherwise add filtered view.
    - show duplicate conflicts before confirmation prompt.
    - confirmation prompt format:
      - `Add Selected {count} Nets for Automated Scheduling?`
- Acceptance:
  - Net Resources management controls are clearly grouped under Net Resources.
  - Import modes behave predictably with manual-vs-built-in retention rules.
  - Export never overwrites an existing file.
  - Add-to-schedule prompts and highlights candidate rows before add.

#### Phase 2m (this change): Net Resources Coverage Field + Seasonal CSV Refresh

- Problem:
  - New SitRep seasonal schedules include a `Coverage` field that is not persisted or shown in Net Resources.
  - Seasonal source schedules are provided as CSV and must be converted to scheduler-compatible Net Resource JSON.
  - Day-of-week and week-of-month formatting must remain compatible with Net Schedule import and scheduler recurrence logic.
- Change:
  - Extend `net_resources` schema with optional `coverage TEXT` across:
    - GUI table creation/migration logic (`net_schedule_tab`)
    - Core DB initializer (`db_initializer`)
    - DB schema tooling (`tools/db_schema.py`)
  - Extend Net Resources UI to display and search `Coverage`.
  - Extend Net Resources import/export/upsert/edit flows to preserve `coverage` values.
  - Convert:
    - `C:\Users\billd\RadioCode\Testing\SitRep-Winter-new.csv`
    - `C:\Users\billd\RadioCode\Testing\SitRep-Summer-new.csv`
    into built-in JSON payloads under `config/net_resources/`.
  - Normalize recurrence inputs for compatibility:
    - `Day of Week` -> canonical `day_utc` values (`Sunday`..`Saturday`).
    - `Week of Month`:
      - `ALL` for non-Periodic rows -> empty `month_weeks`
      - comma-separated numeric weeks (`1`..`5`) for `Periodic` rows.
- Acceptance:
  - Net Resources table persists and renders `Coverage` end-to-end.
  - Seasonal JSON files load with valid day/week formatting and can be imported into Net Resources without row rejection.
  - Scheduler continues to consume active Net Schedule rows with no recurrence regression.
- Rollback:
  - Revert `coverage` column/UI wiring and restore previous `config/net_resources` JSON files.
  - Existing scheduler behavior remains unchanged because active schedule schema is not altered by this phase.

#### Phase 2n (this change): Import Key Collision Fix for Periodic Week Variants

- Problem:
  - Net Resource import upsert currently matches rows by:
    - `frequency + start_utc + end_utc + fldigi_mode`
  - Multiple legitimate nets share that tuple but differ by:
    - `day_utc`
    - `recurrence`
    - `month_weeks`
    - `band` / `mode`
  - Result: imports can overwrite distinct Periodic rows, causing missing week lists such as `1,3,5` or `1,2,4,5`.
- Change:
  - Expand import identity key and upsert matching to include:
    - `day_utc`
    - `recurrence`
    - normalized `month_weeks`
    - `band`
    - `mode`
    - existing `frequency`, `start_utc`, `end_utc`, `fldigi_mode`
  - Keep numeric-tolerant frequency comparison.
  - Apply same expanded key in `Import (Replace Built-in Set)` removal logic.
- Acceptance:
  - Importing Winter/Summer resources preserves distinct Periodic rows that share time/frequency but differ by week sets.
  - Week sets remain visible in Net Resources and can be promoted to Net Schedule without data loss.

#### Phase 2o (this change): Seasonal Import Set Routing

- Problem:
  - Import target set currently defaults to the selected set when not `All`.
  - If `Winter` is selected and `sitrepnets-summer.json` is imported, rows are written into `Winter`, so `Summer` may never appear as a set.
- Change:
  - Route built-in seasonal files (`source_type='builtin'`, filename `sitrepnets-*.json`) to set inferred from filename (`Winter`/`Summer`) regardless of combo selection.
  - Keep existing selected-set behavior for non-built-in/custom imports.
- Acceptance:
  - Importing `sitrepnets-winter.json` and `sitrepnets-summer.json` always creates/updates `Winter` and `Summer` sets respectively.

#### Phase 2p (this change): Cross-Platform Net Resources Import Discoverability

- Problem:
  - Import controls are available primarily through a split-button menu, which is easy to miss.
  - Linux operators specifically need an obvious browse flow for local JSON imports.
- Change:
  - Add visible Net Resources controls:
    - `Import JSON...`
    - `Open Net Resources Folder`
  - `Import JSON...` flow:
    - open file picker (browse),
    - then prompt for import mode (`Merge/Update` vs `Replace Built-in Set`).
  - Remember the last import directory and default browse start directory in this order:
    - last used import directory,
    - `~/Downloads`,
    - bundled `config/net_resources`.
  - Keep existing Manage split-menu actions for advanced workflows (backward compatible).
- Acceptance:
  - Both Windows and Linux users can discover and execute JSON import without using the split-menu.
  - Import browse opens at a sensible location and remembers the last import path.

#### Phase 2q (this change): Consolidate Import Under Manage Menu

- Problem:
  - `Import JSON...` as a standalone button adds persistent UI weight for an infrequent action.
- Change:
  - Remove standalone `Import JSON...` button.
  - Add `Import JSON...` as an explicit action inside `Manage Net Resource` dropdown.
  - Set primary `Manage Net Resource` click behavior to the same import picker + mode prompt flow for quick access.
  - Keep advanced direct actions (`Import (Merge/Update)`, `Import (Replace Built-in Set)`) in the same menu.
- Acceptance:
  - No standalone Import button is shown.
  - Import remains discoverable and executable from Manage menu on both Windows and Linux.

#### Phase 2r (this change): Net Resources Action Row Simplification

- Problem:
  - Infrequent file-management controls (`Open Net Resources Folder`, direct mode-specific import actions, `Publish Set to Delivery File`) add UI noise and can confuse typical operators.
- Change:
  - Remove `Open Net Resources Folder` button.
  - Remove direct `Import (Merge/Update)` and `Import (Replace Built-in Set)` menu entries.
  - Keep `Import JSON...` under `Manage Net Resources`, with the existing post-browse mode chooser.
  - Remove `Publish Set to Delivery File` button/action.
  - Keep `Export New Resource File` in Manage menu as the explicit export path.
- Acceptance:
  - Net Resources UI exposes a simplified file workflow:
    - `Manage Net Resources -> Import JSON...`
    - `Manage Net Resources -> Export New Resource File`
  - No loss of import capability (merge vs replace remains available via Import mode prompt).

#### Phase 2s (this change): Theme Readability + Cross-Tab Theme Coherence

- Problem:
  - Light-theme `info` buttons can have poor foreground/background contrast.
  - Some controls retain dark-styled visuals after theme switching, especially on Map tab controls with hardcoded dark styles.
  - Theme apply fan-out is inconsistent for tabs with local per-widget styles.
- Change:
  - Improve button contrast logic in shared theme helpers for role-based buttons.
  - Ensure main-window theme application re-runs tab-local `apply_theme()` hooks for all relevant tabs.
  - Add/normalize `apply_theme()` hooks for tabs that own local button styles (`StationsMap`, `Peer Schedules`, `Help`).
  - Replace hardcoded Map tab visual colors (splitter handle/chevron and propagation badge) with theme-derived tokens.
  - Ensure Map tab action buttons (`Refresh Links`, `Peer Sched Now`, `SitRep Status`) are restyled when theme toggles.
  - Refresh Map tab settings cache before applying tab-local styles so theme changes from Settings immediately propagate to map action buttons.
- Acceptance:
  - In light theme, `info` and `primary` button text is readable with clear contrast.
  - Switching between dark and light themes updates Map/Peer Schedules/Help controls without stale dark styling.
  - Map propagation badge and splitter indicator use theme-consistent colors in both themes.
- Rollback:
  - Revert phase-specific updates in `theme.py`, `main_window.py`, `stations_map_tab.py`, `peer_sched_tab.py`, and `help_tab.py`.
  - Global theming behavior returns to pre-phase defaults.

#### Phase 2t (this change): SOP Local Net Profiles (Phased)

- Problem:
  - SOP reminders currently assume HF operating-group workflows and software-specific actions.
  - Local VHF/UHF/GMRS/FRS/MURS/Meshtastic net opening workflows need reminder support without coupling to the scheduler engine.
- Design Decision:
  - Introduce `Local Net Profiles` in Settings as a separate, non-scheduler data model.
  - Allow SOP action rows to reference a `Local Net Profile` via a dedicated local action template.
  - Keep scheduler engine and HF schedule truth tables unchanged.

##### Phase 1 (implemented in this change): Local Net Profile Data + SOP Action Template

- Change:
  - Add `local_net_profiles` settings key (list of dicts) managed in Settings UI:
    - fields: `name`, `service`, `mode`, `target`, `notes`
  - Add Settings section `Local Net Profiles` with:
    - table view,
    - `Add`, `Edit Selected`, `Delete Selected` actions,
    - immediate persistence pattern matching Operating Groups.
  - Extend SOP Builder:
    - add pseudo-software channel `Local Net` (always available),
    - add action `Open Local Net`,
    - add contact rule `Local Profile` for local-net actions,
    - bind `Contact Target` picker to configured Local Net Profiles.
  - Extend SOP upcoming rendering:
    - local actions display selected local profile in Contact column,
    - no scheduler coupling is introduced.
- Acceptance:
  - User can define/edit/delete Local Net Profiles in Settings.
  - SOP can save and load actions using `Local Net -> Open Local Net -> Local Profile`.
  - Upcoming SOP rows include local-net reminders even when JS8/VarAC/FLDigi are not configured.
  - Existing HF SOP workflows remain backward-compatible.

##### Phase 2 (follow-up): Local Service-Specific Metadata

- Change:
  - Add optional fields for repeater/access detail and protocol-specific hints.
  - Improve local-net summary text in SOP upcoming table/tooltips.
- Acceptance:
  - Operators can capture common repeater/simplex details without overloading SOP row schema.

##### Phase 3 (future, optional): Advanced Custom SOP Action Builder

- Change:
  - Add flexible custom SOP action schema for non-radio workflows/tools.
- Acceptance:
  - Power users can model complex local workflows while preserving a simple default path.

- Rollback:
  - Revert `local_net_profiles` UI/persistence and SOP local action wiring in:
    - `settings_tab.py`
    - `sop_tab.py`
    - `sop_manager.py`
  - Existing HF SOP actions remain intact since base SOP tables are unchanged for Phase 1.

#### Phase 2u (this change): Local Operators + Local NCS Workflow

- Problem:
  - Local net operations need a dedicated operator roster and check-in workflow that is independent from HF scheduler-driven operating groups.
  - Operators need a single local check-in log with persistent SitRep status and evolving notes during ad hoc local net cycles.
- Change:
  - Navigation/label updates:
    - Settings section label `Operating Groups` -> `HF Operating Groups`.
    - Sidebar label `Digi/SSB NCS` -> `FLDigi/SSB NCS`.
    - Sidebar label `Operators` -> `HF Operators`.
    - Add new sidebar tabs `Local Operators` and `Local NCS`.
  - Data model:
    - Add local roster table `local_operator_checkins` with fields:
      `callsign`, `name`, `city`, `state`, `category`, `first_seen_utc`, `last_seen_utc`, `checkin_count`, `notes`, `sitrep_status`, `updated_utc`.
    - Add local net log table `local_ncs_checkins` with fields:
      `checkin_utc`, `net_name`, `channels`, `callsign`, `name`, `city`, `state`, `category`, `sitrep_status`, `notes`, `updated_utc`.
    - Ensure startup schema initialization is idempotent for both tables.
  - Local Operators tab:
    - Columns: `Callsign`, `First Name`, `Last Name`, `City`, `State`, `Category`, `First Seen`, `Last Seen`, `Check-ins`, `SitRep`, `Notes`.
    - Support refresh, add/edit/delete, CSV import/export, search/filter.
    - CSV import/export supports `first_name` and `last_name` with backward-compatible `name` fallback.
    - Enable sortable headers for local-operator table columns.
    - Keep categories local-focused (`VHF/UHF/GMRS/MURS/FRS/Other` + free text).
  - Local NCS tab:
    - Role fixed to `NCS` (no role-switch compare workflow).
    - Add explicit session controls: `Start Net`, `Join Net`, `End Net`, with visible active-session status.
    - Treat each Local NCS net as a unique session; clear the check-in table on `End Net`.
    - Single check-in log table (no compare-NCS lists).
    - `Operator Lookup/Add` parser supports `CALL / Name / State` and whitespace variants.
    - Lookup source is `Local Operators` roster; check-ins auto-populate known operator profile fields.
    - Per-check-in SitRep status values: `GREEN` (all ok), `YELLOW` (risks), `RED` (priority issue).
    - Per-check-in notes remain editable and persistent across session/restart.
    - Autosave runs periodically and persists dirty status/notes edits.
    - Persist latest status/notes back to the matching local operator record.
- Acceptance:
  - Updated labels appear consistently in sidebar and settings UI.
  - `Local Operators` CRUD/import/export works and persists after restart.
  - `Local NCS` can add check-ins from lookup/parsing without scheduler coupling.
  - SitRep status and notes edits survive restart and update local operator records.
  - UI stays responsive while editing/adding rows; no heavy blocking path on tab activation.
- Rollback:
  - Revert local workflow additions in:
    - `freqinout/core/local_ops_store.py`
    - `freqinout/core/db_initializer.py`
    - `tools/db_schema.py`
    - `freqinout/gui/local_operator_tab.py`
    - `freqinout/gui/local_ncs_tab.py`
    - `freqinout/gui/main_window.py`
    - `freqinout/gui/settings_tab.py`

### 1.16 Addendum (2026-02-18): Extend FLAMP Authenticity Verification to `.b2s`

Problem:
- Message authenticity checks currently gate on FLAMP `.k2s` records only.
- Operators also exchange FLAMP `.b2s` payloads that need the same trust signals (GPG + checksum/local hash).

Scope:
- Treat FLAMP `.b2s` and `.k2s` equally in authenticity verification:
  - background signature/hash verification scheduling
  - cached authenticity state usage
  - message-row icon/tooltip rendering
  - info-panel signature detail rendering.
- Update Settings copy to reflect `.k2s/.b2s` coverage.
- Keep existing settings keys unchanged for backward compatibility.

Acceptance criteria:
- FLAMP `.b2s` rows show the same trust icon/status behavior as `.k2s` rows.
- Signature/hash validation cache and refresh paths include `.b2s`.
- Settings labels explicitly mention `.k2s/.b2s`.
- No regressions in non-FLAMP file handling.

Rollback:
- Revert `.b2s` auth-file predicate expansion in:
  - `freqinout/gui/message_viewer_tab.py`
  - `freqinout/gui/settings_tab.py`

### 1.17 Addendum (2026-02-18): Inline `-sig` FLAMP Signature Verification

Problem:
- Operational traffic includes one-file embedded clearsigned FLAMP files (for example `*-sig.k2s`), not only detached `.sig` sidecars.
- Detached-only verification misses valid embedded signatures and can mislead operators.

Scope:
- Add inline clearsigned verification fallback for FLAMP auth files when no detached signature sidecar exists.
- Gate inline verification by filename suffix patterns to avoid unnecessary GPG calls on all FLAMP files.
- Defaults:
  - `-sig.k2s`
  - `-sig.b2s`
- Keep suffix patterns easy to change via one setting key:
  - `gpg_inline_signed_filename_suffixes` (list or comma-separated string).

Constraints:
- Preserve detached signature behavior as first priority.
- Keep UI path non-blocking by continuing verification in background worker.
- Do not add broad per-file full-content scans; only perform lightweight header sniff for pattern-matched filenames.

Acceptance criteria:
- `*-sig.k2s` / `*-sig.b2s` with embedded PGP clearsign can verify as valid in Message Viewer without sidecar files.
- Non-matching filenames remain detached-only checks and avoid extra inline GPG work.
- Existing trust behavior and hash OR-logic remain unchanged.

Rollback:
- Revert inline verification fallback and suffix-pattern handling in:
  - `freqinout/core/gpg_tools.py`
  - `freqinout/gui/message_viewer_tab.py`

### 1.18 Addendum (2026-02-18): Messages Filter Header First-Open Alignment

Problem:
- On first open of `Messages`, filter controls can render with compressed/offset spacing.
- A minor window resize corrects alignment, indicating initial header geometry sync is happening too early.

Scope:
- Harden first-open filter header alignment by:
  - listening to header `geometriesChanged` in addition to `sectionResized`
  - adding startup deferred re-sync passes
  - applying deterministic fallback column widths when initial section sizes are unresolved.

Acceptance criteria:
- Filter controls appear correctly aligned on first `Messages` open without requiring window resize.
- Post-resize alignment remains correct and unchanged.

Rollback:
- Revert header sync hardening in `freqinout/gui/message_viewer_tab.py`.

### 1.19 Addendum (2026-02-18): Local-Time Toggle Default Highlight Consistency

Problem:
- Multiple tabs visually highlight `Showing: Local` even though Local is the default view state.
- Users need highlight to indicate an active override action (`Showing: UTC`), not default state.

Scope:
- Apply consistent toggle styling behavior across:
  - `Messages`
  - `HF Schedule`
  - `Net Schedule`
  - `Peer Schedules`
- Styling rule:
  - Local/default: muted
  - UTC override: info-highlighted

Acceptance criteria:
- On initial load in Local mode, time-toggle button is not highlighted on all four tabs.
- Switching to UTC highlights the toggle button.
- Switching back to Local returns to muted/default styling.

Rollback:
- Revert time-toggle style-role updates in:
  - `freqinout/gui/message_viewer_tab.py`
  - `freqinout/gui/daily_schedule_tab.py`
  - `freqinout/gui/net_schedule_tab.py`
  - `freqinout/gui/peer_sched_tab.py`

### 1.20 Addendum (2026-02-18): SOP Schedule Layer Override

Problem:
- SOP actions can legitimately require frequency/mode windows that do not align with the baseline HF schedule.
- Current SOP behavior only warns on misalignment; operators must manually infer when to QSY and what source is driving schedule state.

Scope:
- Add an explicit per-SOP `schedule layer` that can override HF schedule while that SOP profile is active.
- Preserve existing net priority semantics and scheduler safety controls.
- Keep scheduler/UI performance stable by using DB-backed cached reads and simple precedence logic.

Precedence:
- Effective scheduler source order becomes:
  - `NET` (highest)
  - `SOP Layer` (active SOP profiles only)
  - `HF` (fallback)

Impacted files (planned):
- `freqinout/core/db_initializer.py`
- `freqinout/core/sop_manager.py`
- `freqinout/core/scheduler_engine.py`
- `freqinout/gui/sop_tab.py`
- `freqinout/gui/main_window.py` (status wording only, if required)

Primary failure modes and mitigations:
- Failure mode: schema drift or missing SOP layer table on upgrade.
  - Mitigation: idempotent table/index creation in startup DB initializer and SOP manager.
- Failure mode: scheduler regression from new source precedence.
  - Mitigation: isolate precedence change to `_evaluate`, keep existing `NET` guard logic untouched, and maintain backward-compatible defaults when no SOP layer rows exist.
- Failure mode: UI ambiguity about active source.
  - Mitigation: surface `SOP Layer` source in existing schedule status summary path.
- Failure mode: performance regressions from extra DB reads.
  - Mitigation: reuse scheduler mtime cache and query only active/enabled SOP rows.

Phases:

#### Phase 1: Data Model + Manager APIs

- Change:
  - Add `sop_schedule_layer` table:
    - `id`, `profile_id`, `day_utc`, `recurrence`, `biweekly_offset_weeks`, `month_weeks`,
      `band`, `mode`, `vfo`, `frequency`, `start_utc`, `end_utc`, `enabled`, `sort_order`, `updated_utc`.
  - Extend SOP manager load/save/export/import paths to include schedule layer rows per profile.
- Acceptance:
  - Existing SOP profiles continue loading without migration prompts.
  - Saving a profile persists schedule-layer rows.
  - Export/import round-trips schedule-layer rows.

#### Phase 2: Scheduler Engine Integration

- Change:
  - Load active SOP layer rows (active profile + enabled row).
  - Add source resolution path for `SOP` and apply precedence `NET > SOP > HF`.
  - Include SOP layer entry in next-change computation.
  - Preserve all existing busy/suspend/manual-net safety controls.
- Acceptance:
  - With active SOP layer rows and no active net, scheduler source is `SOP`.
  - With active net row, scheduler source remains `NET`.
  - With no active SOP layer row, behavior matches current HF/NET logic.

#### Phase 3: SOP UI Editing + Clarity

- Change:
  - Add SOP tab section to define schedule-layer rows per SOP profile.
  - Ensure row validation (`day/start/end/frequency`) and UTC semantics.
  - Keep table lightweight and consistent with existing SOP tab styles.
- Acceptance:
  - User can create/edit/remove schedule-layer rows per SOP profile.
  - Profile save/load reflects layer rows correctly.
  - Scheduler reflects profile activation state without extra user steps.

#### Phase 4: Layer Bootstrap from Action Rows (Assisted)

- Change:
  - Add SOP UI action `Populate Layer from Actions`.
  - Derive candidate schedule-layer rows by matching non-local SOP action rows to existing HF/Net schedule windows using:
    - selected `Operating Group`
    - action `Band`
    - action `Frequency`
  - Apply action-driven defaults for generated layer rows:
    - `recurrence = Daily`
    - `day_utc = ALL`
  - Present review prompt with options:
    - `Append` candidates to existing layer rows
    - `Replace Existing Layer` with candidates
    - `Cancel`
  - Keep this explicitly user-triggered (no automatic mutation).
  - Remove `VFO` from SOP layer UI editing; scheduler/operating-group resolution remains authoritative for VFO behavior.
  - Layer row `Start`/`End` editing follows SOP tab `Showing: Local/UTC` and converts display values while preserving UTC storage.
  - Expand layer table horizontal layout for easier visual review.
- Acceptance:
  - User can prepopulate layer rows from action rows with one guided action.
  - Unmatched actions are reported without modifying layer rows unless user confirms.
  - Resulting rows default to `Daily` recurrence for SOP action workflows.

#### Phase 5: Layer Review Safety + Effective Source Visibility

- Change:
  - Add row-level SOP layer validation hints in the SOP UI for pre-save review, including:
    - invalid/missing time format
    - invalid/missing frequency
    - missing `Weeks` for `Periodic` recurrence
    - potential overlap with other layer rows
  - Add a lightweight `Rebuild Layer Preview` action that compares action-derived candidates against current layer rows and shows:
    - rows to add
    - rows to remove (if replacing)
    - unchanged rows
    before apply.
  - Add explicit effective-source visibility for SOP override scenarios in:
    - `ControlFreq` frequency control area
    - `HF Schedule` header/status area
  - Keep runtime cost low by:
    - only computing layer validation from in-memory table rows
    - only running candidate diff when user clicks preview/rebuild actions.
- Acceptance:
  - SOP layer table surfaces non-blocking row-level warning cues before save.
  - Rebuild flow clearly previews add/remove impact before mutation.
  - When SOP layer is currently overriding HF, ControlFreq and HF Schedule both visibly show that effective source.
  - No scheduler behavior changes beyond source visibility text; precedence remains `NET > SOP Layer > HF`.

Additional impacted files (Phase 5):
- `freqinout/gui/sop_tab.py`
- `freqinout/gui/controlfreq_tab.py`
- `freqinout/gui/daily_schedule_tab.py`

Additional failure modes and mitigations (Phase 5):
- Failure mode: warning styling conflicts with theme readability.
  - Mitigation: use subtle border/tooltips and existing theme palette; avoid hard-coded high-contrast fills.
- Failure mode: preview/rebuild confusion causing accidental row replacement.
  - Mitigation: explicit preview summary and clear destructive-action labeling.
- Failure mode: excessive UI refresh churn from new source labels.
  - Mitigation: reuse existing timer/status refresh paths and avoid extra polling.

#### Phase 6: Multi-SOP Arbitration + Contention Warning

- Change:
  - Add deterministic SOP-layer winner selection when multiple active SOP layer rows overlap the same runtime window.
  - Arbitration order for SOP-layer conflicts:
    - profile priority (lower numeric value wins)
    - profile `updated_utc` recency (newer wins) as fallback
    - stable row tie-breaker (start/sort/id) as final fallback.
  - Preserve source precedence exactly as-is:
    - `NET > SOP Layer > HF`.
  - Surface explicit contention metadata in scheduler status so UI can warn when 2+ active SOP profiles contend.
  - Add UI warning display in schedule-status surfaces (ControlFreq / HF Schedule / sidebar status path) when SOP contention exists.
- Acceptance:
  - For overlapping active SOP profiles, winner selection is deterministic and stable across refresh ticks/restarts.
  - UI shows a clear contention warning including selected profile and contenders.
  - No behavior regression for non-overlap SOP usage.

Additional impacted files (Phase 6):
- `freqinout/core/scheduler_engine.py`
- `freqinout/core/sop_manager.py`
- `freqinout/core/db_initializer.py`
- `freqinout/gui/sop_tab.py`
- `freqinout/gui/controlfreq_tab.py`
- `freqinout/gui/daily_schedule_tab.py`
- `freqinout/gui/main_window.py`

#### Phase 7: Arbitration Transparency + Next Source Transition Preview

- Change:
  - Add scheduler status transparency metadata so UI can explain:
    - why current source/profile won (`NET` precedence, `SOP` arbitration reason, or `HF` fallback)
    - what source is expected after the next schedule boundary.
  - Extend SOP overlap arbitration metadata with explicit winner reason code:
    - `priority`
    - `updated_utc`
    - `start_time`
    - `stable_tiebreak`
    - `single_active_profile`
  - Add low-cost next-transition preview by evaluating source state at `next_change_utc + 1 second` (read-only projection).
  - Surface new hints in:
    - `ControlFreq` effective source / next-change hinting
    - `HF Schedule` effective source tooltip
    - `SOP` tab runtime hint label for active scheduler source and upcoming source shift.
- Acceptance:
  - When active source is SOP, UI can explain why the current SOP profile won overlap arbitration.
  - Scheduler status includes next-source preview metadata without changing schedule enforcement behavior.
  - UI surfaces show concise next-source hints only when a meaningful transition exists.
  - No regressions to precedence (`NET > SOP Layer > HF`) or control-path behavior.

Additional impacted files (Phase 7):
- `freqinout/core/scheduler_engine.py`
- `freqinout/gui/controlfreq_tab.py`
- `freqinout/gui/daily_schedule_tab.py`
- `freqinout/gui/sop_tab.py`

Additional failure modes and mitigations (Phase 7):
- Failure mode: next-transition preview flickers at boundary times.
  - Mitigation: evaluate at `next_change_utc + 1s` and suppress preview text when source remains unchanged.
- Failure mode: tooltips become noisy or too verbose for operators.
  - Mitigation: keep source labels short; move detail into concise tooltip text.
- Failure mode: extra scheduler work affects UI responsiveness.
  - Mitigation: reuse in-memory schedule rows from current evaluation tick; avoid extra DB reads.

#### Phase 8: SOP Layer/Action Drift Visibility + Guided CTA

- Change:
  - Add an SOP Builder `Layer Sync` state hint that compares:
    - current SOP Layer rows
    - action-derived layer candidates for the selected operating group.
  - Add debounced + cached candidate evaluation in SOP UI to avoid repeated DB scans while typing.
  - Promote `Rebuild Layer Preview` as the primary CTA only when drift is detected (`+add`/`-remove` differences).
  - Keep scheduler runtime behavior unchanged (UI-only guidance).
- Acceptance:
  - SOP tab shows `Layer Sync` status (`In Sync`, `Out of Sync`, or `No matching windows`) with low-noise styling.
  - `Rebuild Layer Preview` gets contextual warning emphasis only when layer drift exists.
  - Editing non-layer fields does not cause heavy repeated candidate rebuilds (debounced + cached key path).
  - No changes to precedence, enforcement, or DB schema.

Additional impacted files (Phase 8):
- `freqinout/gui/sop_tab.py`
- `CHANGELOG.md`

Additional failure modes and mitigations (Phase 8):
- Failure mode: noisy drift warnings while a profile is incomplete.
  - Mitigation: show muted guidance for missing group/no eligible action rows and avoid warning role in that state.
- Failure mode: UI lag while editing action rows.
  - Mitigation: debounce sync recompute and cache by action/group signature.

### 1.21 Addendum (2026-02-18): Local Net SOP Actions + Fast Local Profile Persist

Problem:
- Operators need multiple Local Net SOP action intents (`NCS`, `Check-in`, `Message`) instead of a single local action type.
- Local Net Profile add/edit/delete in Settings can feel slow due to broad `settings_saved` fanout and unrelated tab refresh work.

Scope:
- Extend SOP Local Net action catalog to include:
  - `NCS`
  - `Check-in`
  - `Message`
- Keep legacy `local_open_net` action-key compatibility for existing SOP records.
- Optimize Local Net Profile save path:
  - persist `local_net_profiles` directly via settings KV write
  - emit a lightweight Local Net Profiles changed signal for SOP refresh
  - avoid full `settings_saved` fanout for Local Net Profile CRUD operations.

Acceptance criteria:
- SOP action row Software `Local Net` shows `NCS`, `Check-in`, and `Message`.
- Existing SOPs with legacy local action keys still load and remain editable.
- After Local Net Profile add/edit/delete, SOP `Local Profile` targets refresh quickly without full app-wide settings refresh latency.

Impacted files:
- `freqinout/gui/sop_tab.py`
- `freqinout/gui/settings_tab.py`
- `freqinout/gui/main_window.py`

Rollback:
- Revert SOP layer table and manager/scheduler/UI wiring in:
  - `freqinout/core/db_initializer.py`
  - `freqinout/core/sop_manager.py`
  - `freqinout/core/scheduler_engine.py`
  - `freqinout/gui/sop_tab.py`

### 1.22 Addendum (2026-02-19): SOP Interval Phase (Staggered Reminders)

Problem:
- SOP actions in the same profile currently share one anchor (`sop_start_utc`) and only support pure interval cadence.
- For local-net operations, users need predictable staggered reminders (for example `MURS every 3h` and `GMRS every 3h +30m`) that stay offset without editing profile start time.

Scope:
- Extend SOP action schema with `interval_phase_minutes` (default `0`), persisted per action.
- Keep existing interval behavior backward-compatible when phase is `0`.
- Update due-time computation to apply phase offset before interval stepping.
- Update SOP Builder Interval field parsing/formatting:
  - existing formats continue: `00:45`, `90m`, `1.5h`, `0130`
  - new stagger syntax: `HH:MM@MM` or `HH:MM@MMm` (for example `03:00@30m`)
- Save/export/import must carry `interval_phase_minutes`.

Acceptance criteria:
- Existing SOP profiles load/save with unchanged schedules when no phase suffix is used.
- Action `03:00@30m` evaluates as a 3-hour cadence with a 30-minute phase offset from profile start anchor.
- ControlFreq/SOP upcoming due times reflect phase-aware schedules.
- No UI regression for non-local SOP workflows.

Impacted files:
- `freqinout/core/sop_manager.py`
- `freqinout/gui/sop_tab.py`
- `CHANGELOG.md`

Rollback:
- Revert `interval_phase_minutes` reads/writes and parser changes in:
  - `freqinout/core/sop_manager.py`
  - `freqinout/gui/sop_tab.py`

### 1.23 Addendum (2026-02-19): Local Interval Migration + SOP->ControlFreq Refresh Latency

Problem:
- Existing Local Net SOP rows created before interval-phase support encode intended staggering as fractional-hour intervals (for example `3:30`), causing incorrect cadence display.
- SOP save/delete/complete changes can take too long to appear in `ControlFreq` due to refresh gating and repeated SOP manager initialization/query work.

Scope:
- Add an idempotent migration in SOP schema initialization for legacy Local Net rows:
  - convert common fractional-hour local intervals (`:15`, `:30`, `:45`) into:
    - `interval_minutes` rounded down to whole-hour cadence
    - `interval_phase_minutes` set to remainder
- Add explicit SOP data-changed signaling from SOP tab to main window and ControlFreq.
- On SOP changes:
  - invalidate cached next-SOP timer summary
  - force scheduler refresh
  - trigger immediate ControlFreq schedule-outlook refresh when active.
- Optimize ControlFreq SOP fetch path:
  - avoid constructing a new `SOPManager` on every schedule rebuild
  - use window-aligned SOP prefetches (`Today` from now, `Tomorrow` from tomorrow-start) per schedule-outlook refresh.

Acceptance criteria:
- Existing Local Net `3:30` style rows migrate to `3:00@30m` equivalent without manual edits.
- After SOP save/delete/import/complete, ControlFreq reflects updated SOP actions without waiting for stale activation/periodic windows.
- No regressions in non-local SOP reminder behavior.

Impacted files:
- `freqinout/core/sop_manager.py`
- `freqinout/gui/sop_tab.py`
- `freqinout/gui/controlfreq_tab.py`
- `freqinout/gui/main_window.py`
- `CHANGELOG.md`

Rollback:
- Revert migration SQL and SOP-change signal wiring in the files above.

### 1.24 Addendum (2026-02-19): ControlFreq Outlook Mutual-Exclusion Windows

Problem:
- `ControlFreq` `Today` and follow-on outlook sections can overlap when the second section starts at `now`, causing same-day rows to appear in both panes.

Scope:
- Partition `Schedule Outlook` into mutually-exclusive windows:
  - `Today`: `now` through end-of-day (in active time basis).
  - `Tomorrow`: start-of-tomorrow through end-of-tomorrow.
- Keep horizon complete across both sections and preserve local/UTC boundary behavior.

Acceptance criteria:
- No row with a timestamp inside `Today` appears in `Tomorrow`.
- `Tomorrow` starts exactly at tomorrow boundary in Local mode when `Showing: Local`, and UTC boundary in `Showing: UTC`.
- `Tomorrow` includes complete SOP reminder occurrences that fall within tomorrow, not only immediate next-due rows.
- Existing row rendering/actions remain unchanged.

Impacted files:
- `freqinout/gui/controlfreq_tab.py`
- `CHANGELOG.md`

Rollback:
- Revert `Schedule Outlook` window boundary changes in `freqinout/gui/controlfreq_tab.py`.

### 1.25 Addendum (2026-02-19): SOP PDF Daily Action Plan + Resource Labeling

Problem:
- SOP PDF export currently emphasizes status/next-due snapshots and can miss same-day repeated occurrences, making it less suitable as a printed action checklist.
- SOP tab still labels action source as `Software`, while operations terminology should use `Resource`.

Scope:
- Rename SOP action source labels from `Software` to `Resource` in SOP tab tables.
- Replace SOP PDF action rendering with a blended single-day action plan (across selected scope) that includes full same-day occurrences.
- Remove `Status` from printed action checklist.
- Add separate periodic section when periodic schedule-layer rows exist, with:
  - `Week(s) of Month`
  - `Day of Week`
  - `Resource`, `Action`, `Band/Freq`, `Contact`, `Description`

Acceptance criteria:
- SOP tab action tables show `Resource` column label.
- PDF daily action section includes complete occurrences in the chosen day window and is sorted by time.
- PDF action checklist excludes `Status`.
- When periodic rows exist, PDF includes a separate periodic action table with week/day metadata.

Impacted files:
- `freqinout/core/sop_manager.py`
- `freqinout/gui/sop_tab.py`
- `CHANGELOG.md`

Rollback:
- Revert SOP PDF rendering and label updates in the files above.

### 1.26 Addendum (2026-02-19): Long-Run UI Perf Hardening + Cache Busting

Problem:
- After same-day SOP/ControlFreq changes, repeated refresh/filter paths can recompute SOP windows too often.
- Settings-save fanout can trigger expensive ControlFreq refresh work even when the tab is inactive.
- Long-running sessions need explicit cache-bust mechanics so operators do not need app restart.

Scope:
- Add bounded SOP window caches in ControlFreq for `Today` and `Tomorrow` prefetches, with TTL and keying by:
  - active day-window bounds
  - display mode
  - minute bucket (for today)
  - SOP cache epoch
- Add explicit SOP cache invalidation on:
  - SOP data changed events
  - settings saved
- Keep inactive `ControlFreq` settings-save path lightweight (invalidate + defer heavy refresh until activation).
- Keep cache bounded/pruned for long runtime.

Acceptance criteria:
- Repeated `ControlFreq` filter/refresh cycles no longer force full SOP recomputation each pass.
- SOP edits/settings changes immediately invalidate cached SOP windows.
- Inactive ControlFreq no longer performs heavy refresh on settings save.
- No behavior regressions in `Today/Tomorrow` SOP display correctness.

Impacted files:
- `freqinout/gui/controlfreq_tab.py`
- `CHANGELOG.md`

Rollback:
- Revert ControlFreq SOP cache/invalidations and inactive settings-save gating.

### 1.27 Addendum (2026-02-19): Accessibility UI Text Size Presets (Normal/Medium/Large)

Problem:
- Some operators need larger in-app text for readability, but unbounded scaling can create clipping, layout shifts, and workflow friction in dense operational tabs.
- Current UI has theme controls but no user-facing text-size control.

Scope:
- Add bounded, user-selectable UI text-size presets in `Settings`:
  - `Normal` = 100% (default / current baseline)
  - `Medium` = 110%
  - `Large` = 125%
- Persist selection in settings and apply immediately without restart.
- Apply sizing through centralized application font scaling so behavior is consistent across tabs.
- Keep `ControlFreq` frequency hero display fixed-size (do not scale with the global text-size setting).

Constraints:
- No workflow/functionality changes to scheduler, ingestion, SOP, NCS logic, or DB schemas.
- Preserve existing light/dark theme behavior and CTA highlighting.
- Keep presets bounded to avoid unusable clipping at extreme scales.

Acceptance criteria:
- Settings shows a new `Text Size` selector with `Normal`, `Medium`, `Large`.
- Changing text size applies app-wide immediately and persists across restart.
- `Normal` visually matches current baseline behavior.
- `ControlFreq` hero frequency readout remains fixed to its current size.
- Verification baseline passes:
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Impacted files:
- `freqinout/gui/theme.py`
- `freqinout/gui/main_window.py`
- `freqinout/gui/settings_tab.py`
- `CHANGELOG.md`

Rollback:
- Revert text-size setting wiring and app-font scaling changes in the files above.

### 1.28 Addendum (2026-02-19): Large Text (125%) Follow-Up Clipping Pass

Problem:
- Initial text-size preset rollout can still leave isolated clipping/spacing issues in dense, mixed-control layouts where controls were fixed-width at build time.
- Highest-risk tabs for this pass: `Settings`, `Messages`, and `SOP`.

Scope:
- Add low-risk, font-metric-based width guards for fixed-width labels/buttons/filter controls in:
  - `Settings` tab sections/forms
  - `Messages` tab header/filter controls
  - `SOP` tab header/action controls and export filter dialog controls
- Keep behavior and data flow unchanged; layout-only adjustments.

Acceptance criteria:
- At `Text Size = Large (125%)`, key action buttons and filter controls in `Settings`, `Messages`, and `SOP` do not truncate visible labels.
- No action/state regressions in affected tabs.
- Verification baseline passes:
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`
  - `python -m pytest -q`

Impacted files:
- `freqinout/gui/settings_tab.py`
- `freqinout/gui/message_viewer_tab.py`
- `freqinout/gui/sop_tab.py`
- `CHANGELOG.md`

Rollback:
- Revert width-adjustment helper logic and related call sites in the files above.

### 1.29 Addendum (2026-02-19): Schedule Resources UX Unification (Low-Cognitive-Load)

Problem:
- Operators need a clearer way to understand and control schedule precedence (`Net > SOP > Daily`) without reading multiple tabs/tooltips.
- Current SOP/HF interactions are technically correct but still feel complex under live operations, especially when gaps/conflicts occur.
- Users need direct confirmation in HF Schedule of active SOPs and one-click activate/deactivate controls.

User expectation:
- "This sounds complicated. I don't want to think too hard to get it right."

Design principles (non-negotiable):
- One-glance clarity: user can tell in under 5 seconds what source is active and why.
- Progressive disclosure: basic guidance first, details only when requested.
- Actionable warnings: every conflict/gap warning includes at least one immediate correction action.
- Stable performance: no heavy DB recompute on frequent UI refresh loops.

Scope:
- Add a user-facing `Schedule Resources` layer in HF workflows that clarifies:
  - baseline `Daily HF Schedule`
  - `Active SOP Schedules`
  - effective runtime source and precedence.
- Add explicit conflict/gap guidance between `Net`, `SOP`, and `Daily`.
- Add direct SOP active-state controls inside HF Schedule context.
- Keep scheduler precedence unchanged: `NET > SOP > HF`.

Terminology model (for UI copy consistency):
- `SOP Category`: high-level SOP type (`HF` or `Local Net`).
- `SOP Group`: SOP profile-level grouping label (operator-facing organizational bucket).
- `Group Name`: existing `HF Operating Group` value used for band/frequency mapping.

Critical UX outcomes:
- HF tab always shows:
  - active source (`Net`, `SOP`, or `HF`)
  - active SOP profile(s) when source is `SOP`
  - contention summary if multiple SOPs overlap.
- SOP active-state can be toggled from HF tab without opening SOP Builder.
- Gaps/conflicts are listed in plain language with suggested fix actions.

Primary failure modes and mitigations:
- Failure mode: user confusion from similar labels (`SOP Group` vs `Group Name`).
  - Mitigation: enforced label taxonomy + inline helper text + consistent column names.
- Failure mode: warning fatigue from noisy conflict messages.
  - Mitigation: severity tiers (`Info`, `Needs Review`, `Conflict`) and deduped issue list.
- Failure mode: UI slowdown from recomputing conflict analysis on every timer tick.
  - Mitigation: cache conflict snapshots keyed by scheduler epoch + active profile hash; recompute on data-change events only.
- Failure mode: accidental SOP deactivation from HF tab.
  - Mitigation: optional confirm prompt for active SOP currently winning runtime source.

Phased implementation:

#### Phase 1: Information Architecture + Terminology Hardening

- Change:
  - Define and apply canonical labels:
    - `SOP Category`
    - `SOP Group`
    - `Group Name (HF Operating Group)`
  - Add concise inline help text where all three can appear together.
  - Normalize column/header labels across SOP/HF views to match taxonomy.
- Acceptance:
  - No ambiguous mixed labels remain in touched surfaces.
  - Operators can distinguish SOP category, SOP grouping, and HF group at first glance.

#### Phase 2: HF Tab Active SOP Visibility + Quick Toggle Controls

- Change:
  - Add an `Active SOPs` strip/panel in HF Schedule with:
    - active/inactive badge
    - SOP name
    - category
    - quick `Activate` / `Deactivate` action.
  - Add clear runtime summary text:
    - `Now: <source>`
    - `Next: <source transition>`
  - Wire toggles to SOP manager APIs with guarded refresh and scheduler force-refresh.
- Acceptance:
  - User can activate/deactivate SOPs directly from HF tab.
  - HF tab explicitly confirms which SOPs are active.
  - Source summary updates immediately after toggle.

#### Phase 3: Conflict/Gap Detection + Guided Resolution

- Change:
  - Add `Schedule Issues` list in HF tab for:
    - source contention (overlapping active SOPs)
    - coverage gaps (no active window where upcoming SOP action exists)
    - schedule mismatch (SOP action outside schedule window).
  - Each issue row includes one-click actions, for example:
    - `Open SOP`
    - `Deactivate SOP`
    - `Adjust Daily Row`
    - `Dismiss Until Change`.
  - Keep issue language non-technical and resolution-oriented.
- Acceptance:
  - Conflicts/gaps are visible without opening SOP Builder.
  - Each issue presents at least one direct correction action.
  - Dismissed issues reappear when underlying data changes.

#### Phase 4: Schedule Resources Workspace (Consolidated View)

- Change:
  - Add a consolidated `Schedule Resources` workspace in HF Schedule context showing:
    - Daily schedule resource rows
    - Active SOP layer rows
    - Net overrides (read-only priority indicator).
  - Keep precedence visual and explicit in one table/view.
  - Support quick filtering by source/category/group name.
- Acceptance:
  - Operator can review complete effective scheduling context from one workspace.
  - Precedence is visually obvious (`Net` first, then `SOP`, then `Daily`).
  - No scheduler behavior change introduced by this UI consolidation.

#### Phase 5: Assisted Resolution + Safe Defaults

- Change:
  - Add optional resolution assistant:
    - suggest winner SOP when contention exists based on current arbitration logic
    - suggest daily row adjustments for repeated mismatches
    - suggest deactivation of stale SOPs with no upcoming actions.
  - Keep assistant advisory first; no silent auto-mutations.
- Acceptance:
  - User can accept or ignore each suggestion explicitly.
  - No background auto-change occurs without user confirmation.

#### Phase 6: Performance Hardening + Regression Safety

- Change:
  - Cache schedule issue snapshots with strict invalidation on:
    - SOP save/toggle
    - HF schedule save
    - Net schedule save
    - scheduler source transition.
  - Keep UI refresh paths incremental and event-driven.
  - Add targeted perf timings for HF tab issue panel refresh.
- Acceptance:
  - No measurable UI lag increase when opening HF tab or toggling SOP active state.
  - Existing scheduler behavior remains stable (`NET > SOP > HF`).
  - Long-running session cache invalidation remains correct without restart.

Impacted files (planned):
- `freqinout/gui/daily_schedule_tab.py`
- `freqinout/gui/sop_tab.py`
- `freqinout/gui/controlfreq_tab.py`
- `freqinout/gui/freq_planner_tab.py`
- `freqinout/gui/main_window.py`
- `freqinout/core/sop_manager.py`
- `freqinout/core/scheduler_engine.py`
- `freqinout/core/db_initializer.py` (only if additional indexing/schema support is needed)
- `docs/guide.html`
- `CHANGELOG.md`

Verification focus for this addendum:
- UX/manual:
  - Operator can identify active source + active SOPs in HF tab in one view.
  - Operator can resolve a synthetic SOP contention case in <= 3 clicks.
  - Operator can toggle SOP active state from HF tab and see immediate confirmation.
- Technical:
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`
  - `powershell -ExecutionPolicy Bypass -File .\\tools\\freqinout-db.ps1 status` (if schema/index changes are introduced)

Rollback:
- Revert Schedule Resources UX additions in affected GUI files.
- Revert SOP manager toggle APIs and any scheduler status metadata additions tied to this addendum.

### Feature Flags (initial)

- `sitrep_unified_ingest_enabled`
- `sitrep_unified_fusion_enabled`
- `sitrep_unified_messages_enabled`
- `sitrep_unified_operators_enabled`
- `sitrep_unified_map_enabled`

### Verification Gates

After each implemented phase:
- `python tools/release_preflight.py`
- `python -m compileall freqinout`
- `powershell -ExecutionPolicy Bypass -File .\tools\freqinout-db.ps1 status` (for schema phases)

Manual checks:
- Launch with no radio software running.
- Verify Messages/Operators/Map remain responsive during background ingest.
- Verify same callsign/report displays consistently across all three tabs.

### 1.30 Addendum (2026-02-19): HF-Only SOP Status Grid + Conflict Summary Consolidation

Problem:
- HF Schedule currently shows separate SOP runtime and schedule-issue sections, which increases cognitive load.
- Users need one operational table that clearly shows SOP activation state and conflicts for HF operations only.
- Local-only SOPs should not appear in HF Schedule operational status.

Scope:
- Replace split `SOP Runtime` + `Schedule Issues` presentation in HF Schedule with one consolidated SOP status grid.
- Include only SOP profiles associated with HF operating workflows:
  - include `SOP-HF` and `SOP-Mixed`
  - exclude `SOP-Local Net` only profiles
  - require an HF operating group value for inclusion.
- Grid columns:
  - `Group Name`
  - `SOP Name`
  - `Status` (`Conflict` / `Active` / `Inactive`)
  - `Issue Summary` (`Schedule Conflict: <first span> (+N more)` when conflicts exist)
  - `Open SOP`
  - `Activate/Deactivate`
- Conflict horizon is next 24 hours and uses upcoming-action alignment checks.
- `Open SOP` must navigate to SOP tab and auto-select the matching SOP profile.
- Sort order: `Conflict` first, then `Active`, then `Inactive`.

Acceptance criteria:
- HF Schedule SOP status grid contains no local-only SOP profiles.
- Conflict summaries show first conflict time span plus additional-count format.
- Activating/deactivating from the grid updates scheduler/UI state immediately.
- Open SOP action brings user to SOP tab with correct profile selected.
- No scheduler precedence behavior change (`NET > SOP > HF`).

Rollback:
- Revert HF Schedule SOP status-grid refactor in:
  - `freqinout/gui/daily_schedule_tab.py`
  - `freqinout/gui/sop_tab.py`

### 1.31 Addendum (2026-02-19): HF Schedule Resources Workflow Parity + Settings Launch Control Visibility

Problem:
- HF Schedule `Schedule Resources` currently includes `NET` source data, which duplicates Net Schedule ownership.
- HF Schedule lacks move/add workflow parity with Net Schedule (`Active <-> Resources`).
- Some users cannot find `Launch Control` in Settings due section navigation clipping.

Scope:
- HF Schedule tab:
  - Remove `NET` source from Schedule Resources.
  - Add move/add workflow parity with Net Schedule:
    - `Move Selected to Resources` from Active Schedule.
    - `Add to Active Schedule` from Schedule Resources (selected/filtered actions).
  - Persist HF resource entries in a dedicated local table (`hf_schedule_resources`) so resources survive restart.
  - Add explicit section labels styled like Net Schedule (`<h3>` headers) for Active Schedule and Schedule Resources.
- Settings tab:
  - Ensure section navigation exposes all sections reliably, including Launch Control.

Acceptance criteria:
- HF Schedule resources list no longer shows `NET` source rows.
- Users can move checked active HF rows into resources and add resources back into Active Schedule.
- HF Schedule shows clear section labels consistent with Net Schedule styling.
- Launch Control is reachable in Settings without resize/workaround.

Rollback:
- Revert changes in:
  - `freqinout/gui/daily_schedule_tab.py`

### 1.66 Addendum (2026-02-21): Net Schedule vs Active SOP Conflict Policy + Automatic Net Restoration

Problem:
- Net rows can be added/edited while HF SOP is active, creating runtime conflicts that are not clearly resolved per conflict window.
- Current behavior surfaces warnings but does not provide deterministic per-conflict SOP-vs-Net resolution with persistence.
- Operators need conflict choices that are explicit, fast, and reversible, with guaranteed restoration of standard Net behavior when SOP deactivates.

Scope:
- Add Net-vs-active-SOP conflict detection and resolution flow for:
  - Net row add/edit/save/import on `HF Nets`.
  - HF SOP activation and reactivation.
- Conflict criteria (single-radio model):
  - time overlap exists,
  - conflicting rows are on different frequency (or different band+frequency),
  - and group differs.
- Introduce per-conflict policy choice:
  - `SOP Priority` (SOP wins conflict window),
  - `Net Priority` (Net wins conflict window).
- Keep baseline schedule rows immutable:
  - do not rewrite original Net rows,
  - do not rewrite SOP action rows for conflict decisions.
- Apply conflict policy only while SOP is active; when SOP deactivates, runtime automatically restores standard Net behavior.

Data model (new persistence):
- Add table `sop_net_conflict_policy` in `freqinout_nets.db` with idempotent migration:
  - `id INTEGER PRIMARY KEY`
  - `sop_profile_id INTEGER NOT NULL`
  - `sop_layer_id INTEGER` (nullable for profile-scoped cases)
  - `net_row_signature TEXT NOT NULL`
  - `sop_row_signature TEXT NOT NULL`
  - `policy TEXT NOT NULL` (`SOP_PRIORITY` or `NET_PRIORITY`)
  - `window_start_utc TEXT NOT NULL`
  - `window_end_utc TEXT NOT NULL`
  - `active INTEGER NOT NULL DEFAULT 1`
  - `resolution_note TEXT`
  - `updated_utc TEXT NOT NULL`
- Add indexes:
  - `(sop_profile_id, active)`
  - `(net_row_signature, sop_row_signature, window_start_utc, window_end_utc)`
- Staleness handling:
  - if Net/SOP signatures no longer match current rows, policy is ignored and row flagged stale for cleanup/re-prompt.

Phase 1: Service-Layer Conflict Engine
- Extend `SOPManager` with Net-vs-SOP conflict APIs that return:
  - consolidated conflict list,
  - day scope summary,
  - window-level signatures for stable persistence.
- Ensure checks run outside UI rendering loops and use cached schedule snapshots where safe.

Acceptance criteria:
- Service returns deterministic conflict rows for active HF SOP vs candidate Net rows.
- No UI blocking during conflict evaluation on routine table redraws.

Phase 2: Policy Persistence + Runtime Arbitration
- Load applicable `sop_net_conflict_policy` rows in scheduler evaluation path.
- Arbitration rules:
  - non-conflict windows keep existing precedence `NET > SOP > HF`.
  - conflict windows use saved per-window policy winner (`NET_PRIORITY` or `SOP_PRIORITY`).
- Keep source reasoning metadata explicit for UI (`net_policy_override`, `sop_policy_override`).

Acceptance criteria:
- With active SOP and a saved conflict policy, scheduler winner follows policy only for matching conflict windows.
- Outside conflict windows, precedence remains unchanged.

Phase 3: Net Schedule UX Conflict Flow
- On Net save/import when active HF SOP conflicts are detected:
  - show one consolidated prompt (not repeated loops),
  - allow per-conflict choice plus bulk apply defaults.
- Add compact Net-tab conflict summary with `Resolve Conflicts` entry point.
- Keep a fast path: no dialog when no conflicts.

Acceptance criteria:
- Operator receives exactly one actionable conflict dialog per changed conflict signature set.
- Choosing policy persists decisions and immediately refreshes scheduler/runtime views.

Phase 4: SOP Activation/Reactivation Guard
- During HF SOP activate/reactivate:
  - re-run Net-vs-SOP conflict check against current Net schedule,
  - prompt to resolve new/unresolved conflicts before final activation.
- Mirror status in SOP Builder and HF Schedule status controls.

Acceptance criteria:
- HF SOP activation cannot finish with unresolved Net conflicts.
- Resolved conflicts do not re-prompt unless source signatures changed.

Phase 5: Automatic Restoration on SOP Deactivate
- On HF SOP deactivation:
  - stop applying `sop_net_conflict_policy` at runtime immediately,
  - return to standard Net precedence behavior with original Net rows intact.
- Policy rows remain persisted but inactive for runtime until SOP reactivates.
- Add stale-policy cleanup utility (best-effort, non-blocking) on data-change boundaries.

Acceptance criteria:
- Deactivating HF SOP restores standard Net schedule behavior with no manual reset.
- Reactivating same SOP reuses valid policy rows; stale ones prompt for re-resolution.

Phase 6: Performance + Safety Hardening
- Add conflict-evaluation debounce for UI-triggered edits/imports.
- Cache signatures for unchanged Net/SOP snapshots to avoid repeated full-window expansion.
- Add targeted logging via `freqinout.core.logger` for:
  - conflict count,
  - prompt time,
  - policy apply/reuse.
- Keep startup safe when SOP/Net data or tables are missing.

Acceptance criteria:
- No observable UI freeze on Net save/import with typical schedule sizes.
- No scheduler regression when no SOP is active.

Verification:
- `python -m compileall freqinout/core/sop_manager.py freqinout/core/scheduler_engine.py freqinout/gui/net_schedule_tab.py freqinout/gui/daily_schedule_tab.py`
- `python tools/release_preflight.py`
- `powershell -ExecutionPolicy Bypass -File .\\tools\\freqinout-db.ps1 status`
- Manual:
  - activate HF SOP, add conflicting Net row, confirm one consolidated conflict prompt appears.
  - choose `SOP Priority` for one conflict and `Net Priority` for another; verify runtime source follows per-window choices.
  - deactivate SOP and verify standard Net behavior is restored automatically.
  - reactivate SOP and verify unchanged policies are reused while changed-signature conflicts are re-prompted.
  - confirm no conflict prompt appears when Net edits do not overlap active SOP windows.

Rollback:
- Revert changes in:
  - `freqinout/core/sop_manager.py`
  - `freqinout/core/scheduler_engine.py`
  - `freqinout/gui/net_schedule_tab.py`
  - `freqinout/gui/daily_schedule_tab.py`
  - any new conflict-policy helper module introduced by this addendum.

### 1.67 Addendum (2026-02-21): Net/SOP Policy Manager (Edit/Clear Without Net Save)

Problem:
- After initial Net-vs-SOP conflict resolution, operators need to adjust or clear saved policies later.
- Current flow only prompts during Net save / SOP activation; there is no direct review/edit UI for existing policy rows.

Scope:
- Add a dedicated `Manage Net/SOP Policies` action in `HF Nets`.
- Provide a modal table view of persisted Net/SOP policy decisions with:
  - overlap window (`Start UTC`, `End UTC`)
  - SOP identity summary
  - Net identity summary
  - current policy (`SOP Priority` / `Net Priority`)
  - state (`Current` vs `Stale` based on active conflict signatures).
- Add actions in the modal:
  - `Set Selected: SOP Priority`
  - `Set Selected: Net Priority`
  - `Clear Selected`
  - `Clear All`
  - `Refresh`

Constraints:
- Do not mutate Net schedule rows or SOP rows from this manager.
- Clearing policy means deactivating policy rows (history-safe), not deleting operational schedules.
- Keep UI thread responsive; only compute conflict-state snapshots on dialog open/refresh.

Acceptance criteria:
- User can open the policy manager without re-saving Net schedule.
- User can change selected policy rows between SOP/Net priority and save immediately.
- User can clear selected/all policy rows and scheduler falls back to baseline precedence behavior for those windows.
- Stale rows are visibly labeled and can be cleared.

Verification:
- `python -m compileall freqinout/core/sop_manager.py freqinout/gui/net_schedule_tab.py`
- `python tools/release_preflight.py`
- Manual:
  - create at least one saved Net/SOP policy via conflict prompt.
  - open policy manager and flip policy; confirm runtime source reason updates on overlap window.
  - clear selected policy and confirm no policy override applies.
  - clear all and confirm dialog returns empty/prompt-free state.

Rollback:
- Revert changes in:
  - `freqinout/core/sop_manager.py`
  - `freqinout/gui/net_schedule_tab.py`

### 1.70 Addendum (2026-02-21): FreqPlanner Net/SOP Policy-Aware Rendering

Problem:
- FreqPlanner still rendered static precedence (`Net > SOP > HF`) and ignored saved Net/SOP conflict policies.
- After changing a conflict policy (for example Net -> SOP), planner cells could still display Net labels, making verification difficult.

Scope:
- Load active `sop_net_conflict_policy` windows in FreqPlanner.
- Apply per-cell policy arbitration when both Net and SOP coverage exist in the same planner hour:
  - `SOP_PRIORITY` => display SOP label.
  - `NET_PRIORITY` (or no policy) => display Net label.
- Include policy windows in planner snapshot hash so policy edits trigger automatic table rebuild.

Acceptance criteria:
- Editing Net/SOP policy updates FreqPlanner display without restarting app.
- Cells with overlapping Net+SOP windows reflect the configured policy winner.
- Non-overlap cells and HF-only cells remain unchanged.

Verification:
- `python -m compileall freqinout/gui/freq_planner_tab.py`
- `python tools/release_preflight.py`
- Manual:
  - create overlapping Net/SOP window and set `NET_PRIORITY`; confirm planner shows Net label.
  - switch same window to `SOP_PRIORITY`; confirm planner shows SOP label.
  - verify unchanged behavior for non-overlap hours.

Rollback:
- Revert changes in:
  - `freqinout/gui/freq_planner_tab.py`

### 1.68 Addendum (2026-02-21): Net Resource Add Mapping Integrity + Immediate Conflict Prompt

Problem:
- Adding rows from `Net Resources` to `Net Schedules` could overwrite resource frequency/group intent with Operating Group defaults during row hydration.
- Conflict prompt timing was delayed to Net Save; operators expected immediate conflict resolution visibility after add.

Scope:
- Preserve explicit resource row frequency when creating Net Schedule rows from resource data.
- Preserve non-catalog group names by retaining row-provided group text in the group combo.
- Trigger Net-vs-active-SOP conflict prompt immediately after add-from-resources completes.

Acceptance criteria:
- A resource row added to Net Schedule keeps its source `group_name`, `band`, and `frequency` values.
- Unknown/non-settings group names are not silently replaced by first Operating Group.
- If active HF SOP conflicts are introduced by add-from-resources, conflict dialog appears immediately.

Verification:
- `python -m compileall freqinout/gui/net_schedule_tab.py`
- `python tools/release_preflight.py`
- Manual:
  - add a resource row with group/frequency not matching first Operating Group and verify values are preserved.
  - add a conflicting net while HF SOP is active and confirm immediate conflict prompt.

Rollback:
- Revert changes in:
  - `freqinout/gui/net_schedule_tab.py`

### 1.69 Addendum (2026-02-21): Unsaved Net-Row Conflict Detection in HF Nets

Problem:
- Net/SOP conflict dialog on `HF Nets` add-from-resources evaluated only DB-persisted Net rows.
- Newly added (unsaved) Net rows were not included, so operators could miss immediate conflict prompts.

Scope:
- Extend Net/SOP conflict detection service API to accept an optional in-memory Net row set.
- Use this override in `HF Nets` immediately after add-from-resources.
- Keep save-time DB-based conflict checks unchanged.
- If all detected conflicts already have saved policies, optionally show an informational note (no re-resolution prompt).

Acceptance criteria:
- Adding a conflicting Net resource row triggers conflict review before Save.
- Conflict detection uses current table state when invoked from add-from-resources.
- Save path still detects conflicts from persisted Net schedule.
- Existing policy-covered conflicts do not force redundant prompts.

Verification:
- `python -m compileall freqinout/core/sop_manager.py freqinout/gui/net_schedule_tab.py`
- `python tools/release_preflight.py`
- Manual:
  - add Net resource that conflicts with active SOP and confirm immediate prompt before Save.
  - confirm same conflict with existing policy shows informational resolved notice (no forced prompt).
  - save and verify no regression in save-time conflict behavior.

Rollback:
- Revert changes in:
  - `freqinout/core/sop_manager.py`
  - `freqinout/gui/net_schedule_tab.py`
  - `freqinout/gui/settings_tab.py`

### 1.32 Addendum (2026-02-19): Settings Open Responsiveness + Launch-Safety Hardening

Problem:
- Settings section navigation currently relies on scroll behavior that can hide available sections.
- Settings tab open can feel slow in environments where GPG key enumeration is expensive.
- User reports app-exit/restart-like behavior and repeated `already running` windows after launch-control edits, with elevated Python memory usage.

Scope:
- Settings:
  - Ensure section list shows all section names without requiring nav-list scroll in normal window size.
  - Defer expensive GPG key-list probing until the Message Auth section is opened or user explicitly refreshes.
- Launch safety:
  - Harden launch orchestration against accidental self-launch targets (for example `freqinout.main` / `FreqInOut.exe`).
  - Improve running-process token detection for command-style configured paths to reduce duplicate launches of Python-based tools.
- Single-instance guard:
  - Remove aggressive stale-lock forced removal path that can permit concurrent instance races.

Acceptance criteria:
- Settings sections are fully visible in the section list without scrolling in standard app layout.
- Settings open path no longer blocks on GPG key-list subprocess work.
- Launch orchestrator refuses self-launch targets and reports them as skipped/blocked.
- Duplicate instance dialog storms from self-launch misconfiguration are prevented.

Rollback:
- Revert changes in:
  - `freqinout/gui/settings_tab.py`
  - `freqinout/core/launch_orchestrator.py`
  - `freqinout/core/software_status_service.py`
  - `freqinout/main.py`

### 1.33 Addendum (2026-02-19): Settings Section-Switch Responsiveness

Problem:
- Users report latency and occasional `Not Responding` flashes while switching Settings sections.
- Current section-switch path can trigger synchronous expensive work (notably GPG key probing) and periodic status checks that are unnecessary when JS8 is not running.

Scope:
- Settings tab:
  - Keep section-switch handler lightweight and avoid repeated auto-probes while navigating.
  - Perform Message Auth key auto-load at most once per session unless user explicitly clicks `Refresh Keys`.
- Software status service:
  - Skip JS8 API socket probing when JS8 process is not running.
  - Add short JS8 API probe caching and lower per-attempt socket timeout for loopback checks.

Acceptance criteria:
- Switching between Settings sections is immediate under normal workloads and does not trigger visible `Not Responding` flashes.
- Entering Message Auth no longer repeatedly blocks UI when keys are unavailable or expensive to enumerate.
- Runtime status indicators remain accurate while reducing UI-thread blocking in Settings.

Rollback:
- Revert changes in:
  - `freqinout/gui/settings_tab.py`
  - `freqinout/core/software_status_service.py`

### 1.34 Addendum (2026-02-19): Shared App-Status Service Layer (Phase 1)

Problem:
- Multiple tabs still perform direct process checks (`psutil`) in UI code paths.
- Duplicate polling across tabs increases UI-thread work and can contribute to perceived latency.

Scope:
- Keep all app-running checks in `SoftwareStatusService` and remove remaining direct UI-layer process scans in:
  - `DailyScheduleTab`
  - `NetScheduleTab`
  - `JS8CallNetControlTab`
- Introduce shared cache behavior inside `SoftwareStatusService` so multiple tab instances reuse one process snapshot and JS8 API probe cache.
- Preserve existing behavior and status semantics.

Acceptance criteria:
- No direct `psutil.process_iter` usage remains in the above UI tabs for running checks.
- Existing auto-start safeguards still prevent relaunch of already-running apps.
- Status checks remain accurate while reducing redundant scans across tabs.

Rollback:
- Revert changes in:
  - `freqinout/core/software_status_service.py`
  - `freqinout/gui/daily_schedule_tab.py`
  - `freqinout/gui/net_schedule_tab.py`
  - `freqinout/gui/js8call_net_control_tab.py`

### 1.35 Addendum (2026-02-19): HF Schedule Resources SOP Layer Integration (Phase A)

Problem:
- HF Schedule `Schedule Resources` currently shows persisted manual resource rows but not SOP-layer schedule rows.
- Operators need quick promotion workflows from resources into either:
  - baseline HF Active Schedule, or
  - SOP Layer schedule rows, based on SOP operating group.

Scope:
- Include SOP HF/Mixed schedule-layer rows in HF `Schedule Resources` as virtual resource rows.
- Add an `Apply To` target selector (`HF Active Schedule` / `SOP Layer`) and keep one-click apply behavior via existing action button/menu.
- For SOP target apply:
  - resolve SOP profile by HF operating group.
  - when multiple SOP profiles share the same group, prompt once for profile selection and remember per-group choice for session.
  - upsert schedule-layer rows into `sop_schedule_layer` via `SOPManager` API.

Acceptance criteria:
- Schedule Resources table includes SOP-layer rows for HF-related SOP profiles.
- User can select target and apply selected/filtered rows in one click flow.
- SOP-layer apply updates scheduler/UI state immediately without restart.
- Ambiguous SOP group mapping is resolved interactively and does not block unrelated groups.

Rollback:
- Revert changes in:
  - `freqinout/gui/daily_schedule_tab.py`
  - `freqinout/core/sop_manager.py`

### 1.36 Addendum (2026-02-19): SOP Gap Candidates in HF Schedule Resources

Problem:
- Operators need to see SOP action gaps (actions not aligned to schedule windows) directly in HF `Schedule Resources` so they can promote fixes with minimal steps.

Scope:
- Add virtual `SOP Gap` resource rows to HF Schedule resources from upcoming active SOP actions where `aligned = false`.
- Default gap rows to daily cadence (`day_utc = ALL`) and generate start/end from due time plus action interval.
- Use HF operating-group defaults for mode/VFO and expose rows as `source = sop_gap`.
- Auto-select `Apply To = SOP Layer` when selected resource rows are SOP-sourced (`sop_layer`/`sop_gap`).

Acceptance criteria:
- HF Schedule Resources shows SOP Gap rows for uncovered active HF SOP actions.
- Selecting SOP-sourced rows auto-targets SOP apply path.
- Operators can apply gap rows to SOP layer in one-click flow.

Rollback:
- Revert changes in:
  - `freqinout/gui/daily_schedule_tab.py`

### 1.37 Addendum (2026-02-19): HF Resources Conflict Visibility and One-Click Resolve

Problem:
- Operators need immediate visibility of apply conflicts in HF Schedule resources and a low-friction way to resolve SOP group ambiguity.

Scope:
- Add `Conflict` column to HF Schedule resources table.
- Evaluate conflict state against current apply target:
  - `HF Active Schedule`: duplicate day/group/mode/band/frequency/time already in active schedule.
  - `SOP Layer`: missing SOP profile for group, or multiple SOP profiles requiring user choice.
- Add `Resolve Conflicts` action to resolve SOP group ambiguity in one flow.

Acceptance criteria:
- Resources table shows conflict state per row.
- `Resolve Conflicts` can assign SOP profiles for ambiguous groups and refresh conflict status.
- Apply actions remain behavior-compatible and continue to block invalid/duplicate writes.

Rollback:
- Revert changes in:
  - `freqinout/gui/daily_schedule_tab.py`

### 1.38 Addendum (2026-02-19): Active Schedule Composite View + FreqPlanner SOP Labeling

Problem:
- HF Schedule `Active Schedule` currently shows only baseline HF rows, even though runtime precedence can use SOP layer rows.
- `Effective Source` wording is ambiguous for operators and can be misread as a static configuration value.
- FreqPlanner does not currently expose active SOP layer entries with explicit source labeling.

Scope:
- HF Schedule tab:
  - Render a composite `Active Schedule` table containing:
    - editable baseline HF rows, plus
    - read-only SOP layer overlay rows for active HF SOP profiles.
  - Keep SOP overlay rows non-editable/non-deletable from HF Schedule to prevent accidental writes outside SOP workflows.
  - Clarify source label text from `Effective Source` to `Runtime Source`.
- FreqPlanner:
  - Load active SOP schedule-layer rows from DB.
  - Display SOP coverage entries as `SOP:<Group>` (net rows still take precedence in display).
  - Include SOP rows in planner snapshot/rebuild detection so updates appear without restart.

Acceptance criteria:
- HF `Active Schedule` visibly includes both baseline HF and active SOP layer rows.
- Saving HF schedule updates only baseline HF rows (no SOP layer mutation/regression).
- Runtime label reads `Runtime Source: ...` and remains precedence-aligned.
- FreqPlanner cells containing SOP-layer coverage show `SOP:<Group>`.

Rollback:
- Revert changes in:
  - `freqinout/gui/daily_schedule_tab.py`
  - `freqinout/gui/freq_planner_tab.py`

### 1.39 Addendum (2026-02-19): Windows-Safe Log Rotation Resilience

Problem:
- On Windows, `RotatingFileHandler` rollover can fail with `PermissionError [WinError 32]` when `freqinout.log` is temporarily locked by another process.
- Current behavior emits repeated logging traceback noise and can degrade runtime diagnostics.

Scope:
- Introduce a resilient rotating file handler in `freqinout.core.logger` that:
  - detects lock-related rollover errors (`PermissionError`, `WinError 32`, common busy/permission errnos),
  - suppresses rollover attempts for a short cooldown window,
  - continues writing to the current log file without surfacing traceback spam,
  - retries rollover after cooldown.
- Keep existing log path, format, max size, and backup count unchanged.

Acceptance criteria:
- During temporary log-file lock, app continues logging without repeated `--- Logging error ---` tracebacks.
- Once lock clears, rotation resumes normally.
- No behavior change to logger API usage across the app.

Rollback:
- Revert changes in:
  - `freqinout/core/logger.py`

### 1.40 Addendum (2026-02-19): HF Active Schedule SOP Row Select/Move/Delete UX

Problem:
- SOP overlay rows in HF `Active Schedule` are visible but not selectable for bulk workflows.
- Operators need to move SOP rows into `Schedule Resources`.
- Deleting SOP rows from `Active Schedule` should remove them from that table view only, without mutating underlying SOP profile entries.

Scope:
- In HF `Active Schedule`:
  - enable checkbox selection for SOP overlay rows.
  - allow SOP overlay rows to be included in `Move Selected to Resources`.
  - allow SOP overlay rows to be removed via `Delete Selected` as a table-view action only.
- Keep SOP overlay rows read-only for direct cell edits.
- Keep persistence behavior unchanged:
  - HF save continues to persist only baseline HF rows.
  - SOP profile/layer rows are never deleted by this action.

Acceptance criteria:
- SOP overlay rows can be selected with checkbox and moved to `Schedule Resources`.
- `Delete Selected` removes selected SOP rows from Active Schedule view and does not alter SOP profile data.
- Baseline HF save behavior remains unchanged.

Rollback:
- Revert changes in:
  - `freqinout/gui/daily_schedule_tab.py`

### 1.41 Addendum (2026-02-19): HF Active Schedule Source Column + SOP Edit Persistence + Conflict Highlight

Problem:
- Runtime source hint placement in HF Schedule is not aligned with where operators evaluate active rows.
- Active schedule conflicts are not visually obvious in Start/End fields.
- Operators need explicit row origin (`HF` vs `SOP`) in Active Schedule.
- SOP rows edited in Active Schedule must persist back to SOP-backed scheduling data.

Scope:
- HF Schedule UI:
  - Move `Runtime Source` hint to the right of `Active Schedule` heading.
  - Add `Source` column after `Day` in Active Schedule (`HF` / `SOP`).
  - Add Start/End time conflict highlighting for overlapping active rows.
- Data behavior:
  - Keep baseline HF save path unchanged for `HF` rows.
  - Allow SOP rows to be edited in Active Schedule (time/day edits) and on save write updates back to SOP schedule layer via `SOPManager.upsert_schedule_layer_rows`.
  - Refresh runtime/SOP resources after SOP row save so changes are reflected immediately.

Acceptance criteria:
- `Runtime Source` appears beside the `Active Schedule` section label.
- Active Schedule shows a `Source` column with `HF` or `SOP`.
- Overlapping active-row time windows are highlighted in Start/End cells.
- Editing and saving an SOP row updates SOP schedule-layer data and is visible in schedule resources/runtime behavior.

Rollback:
- Revert changes in:
  - `freqinout/gui/daily_schedule_tab.py`

### 1.42 Addendum (2026-02-19): Local Net Profiles Remodel to Group/Resource/Mode + SOP Group Mapping

Problem:
- Local Net Profiles are modeled as one profile name with one service/mode pair, which does not represent real local workflows where one local group operates multiple resources and modes.
- SOP local actions currently target a single local profile name instead of a reusable local group concept.
- SOP export/upcoming mapping must stay coherent with the updated local settings model.

Scope:
- Settings:
  - Remodel `local_net_profiles` rows to normalized entries:
    - `group`, `resource`, `mode`, `target`, `notes`
  - Rename Local Net Profiles table labels:
    - `Name` -> `Group`
    - `Service` -> `Resource`
  - Allow multiple entries per group (dedupe by group/resource/mode/target instead of group only).
  - Keep legacy load compatibility by mapping legacy fields (`name` -> `group`, `service` -> `resource`) on read.
- SOP Builder:
  - For local-net actions, use contact rule `local_group` (compat: still accepts legacy `local_profile`).
  - Contact target picker lists local groups (unique), matching HF-style "choose a group" workflow.
  - Default local action description references group (not profile).
  - Upcoming table local contact display resolves group -> resource/mode/target summary.
- SOP runtime/export services:
  - Treat `local_group` as equivalent to legacy local profile routing where applicable.
  - Keep alignment logic skipping local-net actions for HF schedule mismatch diagnostics.
  - Ensure export contact rendering supports `local_group`.

Acceptance criteria:
- Settings Local Net Profiles supports multiple rows for the same group with different resource/mode/target combinations.
- SOP local actions choose a Local Group as contact target.
- Saving SOP local actions writes `contact_rule=local_group` and persists correctly.
- SOP upcoming/export shows valid local group mapping and no regressions for existing legacy `local_profile` actions.
- ControlFreq/HF Schedule local-action handling continues to classify local rows correctly with `local_group`.

Rollback:
- Revert changes in:
  - `freqinout/gui/settings_tab.py`
  - `freqinout/gui/sop_tab.py`
  - `freqinout/core/sop_manager.py`
  - `freqinout/gui/controlfreq_tab.py`
  - `freqinout/gui/daily_schedule_tab.py`

### 1.43 Addendum (2026-02-19): FreqPlanner SOP Cell Labeling by Group Names

Problem:
- FreqPlanner SOP coverage labels should present SOP group names in the same operator-friendly way net names are presented, not as repeated prefixed fragments.
- For overlapping SOP groups in one hour cell, users need compact merged labels (example: `SOP:MAGNET/AMRRON`).

Scope:
- In FreqPlanner cell text composition for SOP coverage:
  - normalize SOP slice labels to group/profile names,
  - de-duplicate names per cell preserving first-seen order,
  - render one prefix with slash-joined names: `SOP:<Name1>/<Name2>/...`.
- Keep precedence and behavior unchanged (`Net > SOP > HF`).

Acceptance criteria:
- SOP-only cells show `SOP:<group name>`.
- SOP cells with multiple overlapping groups show one compact label such as `SOP:MAGNET/AMRRON`.
- Net precedence remains unchanged and continues to override SOP text in overlapping net windows.

Rollback:
- Revert changes in:
  - `freqinout/gui/freq_planner_tab.py`

### 1.44 Addendum (2026-02-20): FreqPlanner SOP Loader Mapping Hardening

Problem:
- Some SOP layer rows can be dropped from FreqPlanner labeling when profile group metadata is not populated in `operating_group` even though valid SOP schedule rows exist.
- This causes affected cells to fall back to HF band/frequency text instead of `SOP:<name>` labels.

Scope:
- Harden `FreqPlanner` SOP layer loader to:
  - resolve display group from `operating_group`, then `secondary_group`, then profile `name`,
  - treat HF-like SOP rows based on row band/frequency presence (not only `operating_group`),
  - keep active-first loading with fallback to enabled rows when no active rows are available.
- Keep cell precedence unchanged (`Net > SOP > HF`).

Acceptance criteria:
- SOP rows with blank `operating_group` but populated `secondary_group` or profile name still render as `SOP:<name>` in FreqPlanner.
- Existing active-profile behavior remains intact.
- No regression in net/HF precedence behavior.

Rollback:
- Revert changes in:
  - `freqinout/gui/freq_planner_tab.py`

### 1.45 Addendum (2026-02-20): ControlFreq Hero Sizing Stability Across Display Scaling

Problem:
- On some OS/display scaling configurations, ControlFreq `Frequency Control` hero rendering grows enough to expand the top row and force main-window resizing.
- `Scheduled` metadata wrapping can make the middle rows appear over-spaced and push action controls down.

Scope:
- Keep Frequency Control section visually stable by:
  - using deterministic pixel sizing for the hero frequency combo font,
  - constraining hero combo control height to a fixed UI envelope,
  - forcing scheduled metadata to a single line with ellipsis + full tooltip,
  - tightening Frequency Control internal vertical spacing/margins for consistent row rhythm.
- Preserve existing behavior and actions (`QSY Now`, `Resume Schedule`, schedule state logic).

Acceptance criteria:
- Frequency Control no longer inflates panel height under higher display scaling.
- `Scheduled...` metadata remains one line (elided if needed) and does not create extra row height.
- `Frequency Control` and `Message Summary` remain matched in top-row height behavior without window-growth regressions.

Rollback:
- Revert changes in:
  - `freqinout/gui/controlfreq_tab.py`

### 1.46 Addendum (2026-02-20): ControlFreq Schedule Badge Size Normalization

Problem:
- In ControlFreq `Frequency Control`, the schedule status badge (`On Schedule`/`Off Schedule`) can appear oversized on scaled displays because it inherits global app text scaling.

Scope:
- Normalize badge sizing to standard button-like dimensions:
  - fixed badge height,
  - explicit badge font size in stylesheet,
  - tighter padding/radius consistent with nearby action buttons.
- Keep existing status semantics/colors unchanged.

Acceptance criteria:
- `On Schedule` / `Off Schedule` badge no longer appears oversized relative to nearby buttons.
- Badge status text/colors still update correctly.

Rollback:
- Revert changes in:
  - `freqinout/gui/controlfreq_tab.py`

### 1.47 Addendum (2026-02-20): ControlFreq Frequency Panel Action/Row Simplification

Problem:
- Frequency Control still has extra visual weight and action ambiguity from separate `QSY Now` and `Resume Schedule` buttons plus top-row `Now` label.
- Operators requested a cleaner two-control row (`Status` + one conditional action button) and clearer row labeling.

Scope:
- Frequency Control UI:
  - remove `Now` label from hero row,
  - move schedule status badge into the action row,
  - replace dual action buttons with a single conditional button:
    - `QSY Now` when a QSY action is pending,
    - `Resume Schedule` when off-schedule without pending QSY,
  - rename `Effective Source` label copy to `Active Source`.
- Row structure and spacing:
  - three compact metadata rows in this order:
    - `Scheduled...`
    - `Active Source...`
    - `Next Change...`
  - keep a small spacing gap between `Next Change` and action row.

Acceptance criteria:
- Frequency Control shows one primary action button (QSY/Resume) plus status badge in the same row.
- `Now` label is removed.
- Source text reads `Active Source`.
- Metadata rows render with standard compact spacing and no widened gaps.

Rollback:
- Revert changes in:
  - `freqinout/gui/controlfreq_tab.py`

### 1.48 Addendum (2026-02-20): ControlFreq Metadata Row Spacing Normalization

Problem:
- After Frequency Control layout simplification, `Scheduled`, `Active Source`, and `Next Change` rows can appear unevenly spaced because row widgets are still vertically expandable under locked panel height.

Scope:
- Normalize metadata row rhythm by:
  - using fixed-height labels for `Active Source` and `Next Change`,
  - syncing metadata-row heights from font metrics after style updates,
  - keeping rows compact while leaving a controlled spacer only before the action row.

Acceptance criteria:
- `Scheduled`, `Active Source`, and `Next Change` rows render with consistent standard row spacing.
- Extra panel height is not distributed between metadata rows.
- Existing source/next-change warning styling behavior remains intact.

Rollback:
- Revert changes in:
  - `freqinout/gui/controlfreq_tab.py`

### 1.49 Addendum (2026-02-20): Collapsible Main Menu Groups + Status Dock + Condition Levels

Problem:
- Left-rail navigation can exceed available vertical space during window resize, causing status information clipping.
- `Schedule Status` must remain visible and readable at all times.
- Operators need a second status card (`Condition Level`) keyed by HF operating group with configurable level and color semantics.

Confirmed UX Decisions:
- Condition Level editing: both in Settings and from Status Dock quick-edit.
- Unconfigured groups: hidden.
- Level range: integer `1..5`.
- Collapsible nav groups: multi-open allowed (not accordion-exclusive).
- Visibility scope: global left rail (available across all tabs).
- Fixed level-color mapping:
  - `1 = Red`
  - `2 = Orange`
  - `3 = Yellow`
  - `4 = Blue`
  - `5 = Green`
- Operating Group configuration simplification:
  - no per-group custom color picker,
  - add per-group toggle `Use Condition Levels`.

Phased implementation:

Phase 1: Left Rail Layout Foundation + Status Dock
- Split left rail into:
  - Navigation zone (scrollable/collapsible content),
  - Status dock (always visible, non-collapsible).
- Move `Schedule Status` into status dock and eliminate clipping by using resize-safe card layout.
- Keep map-filter panel behavior intact.

Phase 2: Collapsible Nav Groups (multi-open)
- Group nav buttons into:
  - Core (always visible),
  - `NCS`,
  - `Schedule`,
  - `Operators`,
  - utilities (`SOP Builder`, `Settings`, `Help`).
- Add per-group collapse toggles with persisted state.
- On narrow height, allow scroll in navigation zone while keeping status dock visible.

Phase 3: HF Operating Group Data Model Extension
- Extend HF operating group records in settings with:
  - `use_condition_levels` (bool),
  - `condition_level` (int, `1..5`, only meaningful when `use_condition_levels=true`).
- Add backward-compatible defaults and migration-safe load/save path.
- Validate range and apply fixed color mapping by level.

Phase 4: Condition Level Status Card
- Add `Condition Level` card to status dock.
- Display one row/chip per configured HF group:
  - Group name,
  - Level number,
  - color badge/chip using fixed mapping (`1=Red, 2=Orange, 3=Yellow, 4=Blue, 5=Green`) with contrast-safe text.
- Hide groups without `use_condition_levels=true` or without valid level.

Phase 5: Condition Level Editing (both locations)
- Settings tab:
  - Add Level and Color controls in HF Operating Groups editor workflow.
- Status dock quick-edit:
  - Add lightweight edit action per group row (or card-level editor dialog) that writes through the same validated save path.

Phase 6: QA, Performance, Documentation
- Ensure left-rail resize remains responsive and no UI-thread blocking added.
- Validate no regressions in tab switching, map filter visibility, and scheduler status refresh.
- Update docs (`docs/guide.html`, `CHANGELOG.md`) with grouped nav and condition-level usage.

Acceptance criteria:
- During window resize, status dock remains fully visible and `Schedule Status` content is not truncated.
- Navigation groups can be independently collapsed/expanded and persist across restart.
- `Condition Level` card appears globally, showing only groups with `use_condition_levels=true` and valid level `1..5` with fixed mapped color.
- Condition levels are editable in both Settings and Status Dock and remain consistent after restart.

Rollback:
- Revert changes in:
  - `freqinout/gui/main_window.py`
  - `freqinout/gui/settings_tab.py`
  - `freqinout/core/settings_manager.py` (if schema helpers are added)
  - `docs/guide.html`
  - `CHANGELOG.md`

### 1.50 Addendum (2026-02-20): Condition-Level UX Finalization

Refinement:
- Keep fixed mapping only:
  - `1 = Red`
  - `2 = Orange`
  - `3 = Yellow`
  - `4 = Blue`
  - `5 = Green`
- HF Operating Groups table/editor should expose only `Use Condition Levels` for condition-level participation.
- Level value remains persisted in data model for each operating-group row and is edited from the global status-dock quick editor.
- Collapsible nav groups default to hidden/collapsed for first run, with persisted user overrides thereafter.

Scope:
- `freqinout/gui/settings_tab.py`
  - Remove visible level column/control from HF Operating Groups UI.
  - Preserve existing stored `condition_level` values and defaults.
- `freqinout/gui/main_window.py`
  - Set default nav group states collapsed (`NCS`, `Schedule`, `Operators`).

Acceptance criteria:
- New installs open with NCS/Schedule/Operators collapsed by default.
- Settings HF Operating Groups shows `Use Condition Levels` but no level/color controls.
- Existing condition levels continue to display and persist in Condition Level status card.
- Condition levels remain editable from status-dock quick editor and survive restart.

Rollback:
- Revert changes in:
  - `freqinout/gui/main_window.py`
  - `freqinout/gui/settings_tab.py`

### 1.51 Addendum (2026-02-20): Nav/Status Polish + Group-Scoped Condition Toggle

Problem:
- Left-rail `Schedule` group label should read `Schedules`.
- Condition-level edit affordance is visually too prominent.
- Status card title placement is inconsistent.
- HF Operating Group condition-level toggle appears per-row, which can imply per-frequency behavior.

Scope:
- `freqinout/gui/main_window.py`
  - Rename nav accordion header from `Schedule` to `Schedules`.
  - Change `Edit Levels` from prominent button styling to a text action.
  - Left-align titles for both `Schedule Status` and `Condition Level` cards.
- `freqinout/gui/settings_tab.py`
  - Clarify condition-level scope as group-based.
  - Make row toggles propagate to all rows sharing the same `Group`.
  - Preserve save semantics as group-scoped for `use_condition_levels`.

Acceptance criteria:
- Sidebar accordion shows `Schedules`.
- `Edit Levels` renders as a low-emphasis text action.
- `Schedule Status` and `Condition Level` card titles are aligned consistently (left).
- Toggling `Use Condition Levels` in any row applies to every row of that group and persists accordingly.

Rollback:
- Revert changes in:
  - `freqinout/gui/main_window.py`
  - `freqinout/gui/settings_tab.py`

### 1.52 Addendum (2026-02-20): Condition-Level Panel Visibility + Status Dock Clipping

Problem:
- Condition-level summary text is redundant and consumes vertical space.
- Condition-level card should not be shown when no groups are configured.
- In constrained window heights, status cards can appear clipped.

Scope:
- `freqinout/gui/main_window.py`
  - Remove summary-line presentation from Condition Level panel.
  - Hide `Condition Level` card entirely when no groups have `use_condition_levels=true`.
  - Wrap status dock in a scroll-safe container so status cards are never clipped.
  - Width-sync status cards using status-dock viewport metrics.

Acceptance criteria:
- No `{N} group(s) using condition levels` text is shown.
- If no groups use condition levels, `Condition Level` section is hidden.
- `Schedule Status` and `Condition Level` content remain accessible without clipping at smaller window heights.

Rollback:
- Revert changes in:
  - `freqinout/gui/main_window.py`

### 1.53 Addendum (2026-02-21): Full 1.1.8 Guide Coverage Refresh

Problem:
- `docs/guide.html` includes legacy labels/flows and does not reliably match the current 1.1.8 UI across all tabs.
- Operators need one authoritative in-app reference that explains every available action, including nested menus and row actions.

Scope:
- Perform a full documentation audit against current UI code and update `docs/guide.html` to:
  - match current left-rail/main-menu labels and groupings,
  - document each tab's purpose, controls, nested actions, and table columns,
  - include key modal/dialog workflows (for example SOP PDF export options, Net/SOP policy manager),
  - keep tone practical, supportive, and user-friendly for both new and advanced operators.
- Keep behavior unchanged (documentation-only update).

Acceptance criteria:
- `docs/guide.html` reflects current nav labels:
  - `ControlFreq`, `FreqPlanner`, `Messages`, `Map`
  - `FLDigi / SSB`, `JS8Call`, `VHF/UHF`
  - `HF Daily`, `HF Nets`, `HF Peers`
  - `HF Callsigns`, `Local Callsigns`
  - `SOP Builder`, `Settings`, `Help`
- Guide includes explicit action references for nested menu actions (toolbutton dropdowns/context actions) where present.
- Guide includes table column documentation for every primary tab table.
- In-app Help tab TOC and PDF export continue to work with updated guide.

Rollback:
- Revert changes in:
  - `docs/guide.html`

### 1.53 Addendum (2026-02-20): Left Rail Width Fit for Expanded Accordions + Status Cards

Problem:
- Main rail buttons and status cards can clip horizontally.
- Expanded accordion child buttons need width reservation for their left-indent offset.

Scope:
- `freqinout/gui/main_window.py`
  - Improve rail width calculation to include:
    - widest nav button/header,
    - accordion child indent width,
    - status-card size hints,
    - extra internal padding for scroll/container borders.
  - Increase rail clamp range to avoid truncation on longer labels.

Acceptance criteria:
- Sidebar buttons are fully readable without right-edge clipping.
- `Schedule Status` and `Condition Level` card outlines/content are fully visible horizontally.
- Expanding accordion groups does not truncate child buttons.

Rollback:
- Revert changes in:
  - `freqinout/gui/main_window.py`

### 1.54 Addendum (2026-02-20): Persistent Status Dock + Smart Accordion Collapse

Problem:
- `Schedule Status` and `Condition Level` sections must be persistently visible, not inside a scroll region.
- During vertical resize pressure, left-rail accordions should collapse intelligently to keep status sections visible.
- When `NCS` is collapsed while any NCS net is active, the collapsed accordion header must show an active reminder.

Scope:
- `freqinout/gui/main_window.py`
  - Remove status-dock scroll wrapper; keep status sections in a persistent non-scroll area.
  - Add smart vertical-fit logic:
    - detect rail height pressure,
    - auto-collapse only inactive expanded accordions first.
  - Add collapsed-header active reminder style for `NCS` when any NCS net is active.
  - Trigger smart-fit checks on resize and status refresh paths.

Acceptance criteria:
- `Schedule Status` and `Condition Level` are not contained in a dedicated scroll area.
- On resize pressure, inactive expanded accordions collapse automatically before status cards lose visibility.
- If any NCS net is active and `NCS` accordion is collapsed, the `NCS` accordion header is highlighted as active reminder.

Rollback:
- Revert changes in:
  - `freqinout/gui/main_window.py`

### 1.55 Addendum (2026-02-20): SOP Builder Remodel (HF + Local Comms) With Conflict-Gated Schedule Derivation

Problem:
- Current SOP model is profile-centric (`sop_start_utc`, profile priority) and requires users to infer conflicts late.
- Operators need a simpler action-row workflow that catches HF Daily/Net conflicts during SOP build and before activation.
- SOP runtime must remain scheduler-safe (`NET > SOP > HF`) without UI-thread blocking or regressions.

Scope:
- Replace SOP profile configuration emphasis with action-row planning emphasis.
- Keep scheduler source-of-truth in UTC and preserve existing precedence.
- Support exactly two operator-facing SOP categories:
  - `HF SOP` (single profile)
  - `Local Comms SOP` (single profile)
- Move heavy recurrence/conflict work into service/manager paths, not direct UI loops.

Data model updates:
- `sop_profiles`:
  - add `category TEXT` (`HF` / `LOCAL`)
  - keep legacy fields for backward compatibility but stop requiring `sop_start_utc`/`priority` in UI flow.
- `sop_actions`:
  - add per-row planning fields:
    - `group_name`
    - `condition_levels` (CSV or `ALL`)
    - `mode`
    - `daily_start_utc`
    - `daily_end_utc`
    - `duration_minutes`
    - `interval_minutes` (reuse existing)
    - `conflict_policy` (`SOP_ALL`, `NET_PRIORITY`, `DAILY_PRIORITY`)
    - `daily_conflict_summary`
    - `net_conflict_summary`
    - `schedule_applied` (0/1)
- Keep `sop_schedule_layer` as derived scheduler input; rows are generated from action rows.

Phased implementation:

Phase 1: Schema + Service Foundation
- Add idempotent schema migration for new SOP columns.
- Add `SOPManager` service APIs to:
  - resolve/create category profile (`HF`/`LOCAL`) with one-per-category guard in service layer.
  - validate action rows and normalize UTC storage.
  - compute daily occurrences from:
    - first start/end window,
    - interval,
    - duration,
    - per-row conflict policy.
  - detect and summarize conflicts against HF daily and HF net schedules.
  - generate/refresh derived `sop_schedule_layer` rows from eligible HF actions.
  - evaluate per-group condition-level filtering for upcoming/runtime rows.

Acceptance criteria (Phase 1):
- Existing SOP rows load without crash after migration.
- New rows persist per-action UTC start/end/duration/interval/conflict policy.
- Service can return conflict summaries and first non-conflict suggestion for row start time.
- Derived schedule-layer generation succeeds without blocking UI.

Phase 2: SOP Builder UI Simplification
- SOP tab top flow:
  - `New SOP` (choose category: HF or Local Comms)
  - `Manage SOP` (switch between the two category profiles)
- Remove UI dependence on:
  - `SOP Daily Start`
  - profile `priority`
  - profile-level primary contacts strip.
- Replace split Action Rows + Layer sections with one action planning table per category.
- HF table columns:
  - `Group | Condition Levels | Resource | Action | Band-Freq | Daily Start | Daily End | Action Duration | Interval | Contact Type | Contact Target | Description`
- Local table columns:
  - `Group | Resource | Mode | Action | Daily Start | Daily End | Action Duration | Interval | Contact Type | Contact Target | Description`
- Remove `Upcoming SOP Actions` section from SOP tab.

Acceptance criteria (Phase 2):
- SOP Builder supports only HF/Local category workflows with one profile each.
- User can add/edit/delete action rows without touching legacy profile fields.
- Times are stored in UTC and shown in Local/UTC per tab toggle.

Phase 3: Conflict-Gated Activation + Cross-Tab Sync
- Before activating HF SOP:
  - run conflict check over all generated occurrences.
  - require user to select per-row policy:
    - `SOP Priority For ALL`
    - `Net Priority on Conflicts; Apply SOP where possible`
    - `Daily Schedule Priority on Conflicts; Apply SOP where possible`
- If policy is `NET_PRIORITY`/`DAILY_PRIORITY` and first occurrence conflicts:
  - prompt for new start time with first non-conflicting suggestion.
- Activation state updates must mirror between SOP tab and HF Schedule tab.
- If HF Daily/Net schedules change while HF SOP is active:
  - prompt immediate resolve/deactivate flow.

Acceptance criteria (Phase 3):
- Activation is blocked until unresolved conflicts are handled.
- SOP active status is consistent in both tabs.
- Schedule edits during active SOP trigger immediate discrepancy prompt.

Phase 4: Runtime Rendering + Condition-Level Filtering
- ControlFreq and FreqPlanner display only actions matching current group condition level:
  - action `condition_levels=ALL` always eligible.
  - group without `use_condition_levels` treated as `ALL`.
- Local SOP reminder prompt:
  - if any HF group level is 1..4 and Local Comms SOP inactive:
    - show prompt `HF Condition Level raised. Activate Local Comms SOP?`
- Maintain scheduler precedence unchanged: `NET > SOP > HF`.

Acceptance criteria (Phase 4):
- ControlFreq/FreqPlanner SOP visibility tracks current condition levels.
- Local SOP activation prompt appears when HF condition rises and Local SOP is inactive.
- No regression in net precedence or automatic radio schedule application.

Migration strategy:
- Best-effort migration from legacy rows:
  - infer category from resource/action family.
  - map missing times to `00:00`/`23:59` defaults.
  - map missing duration to `60`.
  - map missing policy to `SOP_ALL`.
  - preserve legacy rows even if not fully mappable; mark `schedule_applied=0`.
- Existing `sop_schedule_layer` rows remain readable; service may rebuild from actions on save/activation.

Performance guardrails:
- No full DB scans on every keystroke in SOP UI.
- Debounce conflict previews and cache schedule snapshots.
- Batch upserts for action and layer writes.
- Avoid scheduler-thread/UI-thread coupling; use existing signals for refresh/invalidation.

Regression guardrails:
- Preserve `NET > SOP > HF`.
- Keep UTC as persisted truth.
- Do not break startup when no radio apps are running.
- Maintain backward compatibility for JSON import/export (legacy fields tolerated).

Verification:
- `python tools/release_preflight.py`
- `python -m compileall freqinout`
- `powershell -ExecutionPolicy Bypass -File .\tools\freqinout-db.ps1 status`
- Manual:
  - create/edit both SOP categories, save, restart, reload.
  - activate/deactivate HF SOP from SOP and HF Schedule tabs (state mirrored).
  - modify HF Daily/Net while HF SOP active and confirm immediate resolve prompt.
  - verify ControlFreq/FreqPlanner condition-level filtering.
  - launch without radio software and confirm no crashes/freezes.

Rollback:
- Revert SOP remodel edits in:
  - `freqinout/core/sop_manager.py`
  - `freqinout/gui/sop_tab.py`
  - `freqinout/gui/daily_schedule_tab.py`
  - `freqinout/gui/controlfreq_tab.py`
  - `freqinout/gui/freq_planner_tab.py`
  - any new SOP planning service module added in this change.

### 1.56 Addendum (2026-02-20): HF SOP Daily-Only Layer Normalization + Conflict DB Source Fix

Problem:
- HF SOP action cadence is now daily-by-design, but legacy SOP layer rows may still carry weekly/periodic day semantics.
- This can surface as per-day SOP overlays in HF Active Schedule and inconsistent conflict behavior.
- SOP conflict window loading still had a path reading `daily_schedule_tab` from the nets DB instead of settings DB.

Scope:
- Normalize HF SOP schedule-layer rows to daily semantics:
  - `day_utc = ALL`
  - `recurrence = Daily`
  - `biweekly_offset_weeks = 0`
  - `month_weeks = ''`
- Ensure HF Schedule SOP apply/save paths preserve this daily-only SOP contract.
- Fix SOP conflict schedule window loading to read:
  - HF daily rows from `freqinout.db`
  - net rows from `freqinout_nets.db`.

Acceptance criteria:
- HF Active Schedule SOP source rows render as `Source = SOP` and `Day = ALL`.
- Toggling SOP inactive removes only SOP overlays; baseline HF rows remain intact.
- Conflict detection includes daily schedule windows from the settings DB and net windows from the nets DB.
- No scheduler precedence change (`NET > SOP > HF`).

Verification:
- `python tools/release_preflight.py`
- `python -m compileall freqinout`
- Manual:
  - save HF SOP actions and confirm generated layer rows are daily/ALL.
  - activate/deactivate SOP and verify baseline HF rows are retained.
  - trigger conflict-gated save and confirm daily/net conflicts are detected.

Rollback:
- Revert changes in:
  - `freqinout/core/sop_manager.py`
  - `freqinout/gui/daily_schedule_tab.py`

### 1.57 Addendum (2026-02-20): HF Schedule UX Consolidation (Import/Export, Resource Delete, SOP Status Compact)

Problem:
- HF Schedule actions are fragmented (`Export` only, separate apply target modes, large SOP status grid), causing higher operator effort.
- Schedule Resources lacks direct deletion of stale HF resource rows.
- SOP management affordances in HF Schedule consume too much vertical space for active operations.

Scope:
- Replace `Export HF Schedule` button with `Import/Export` action menu placed beside `Save HF Schedule`.
- Add HF schedule import flow (JSON) that only accepts `source=HF` rows.
- Keep export scoped to HF rows and include `source=HF` metadata.
- Remove `Apply To (HF/SOP)` control from Schedule Resources and make action explicit:
  - `Add to Active Schedule`.
- Add `Delete Selected` action in Schedule Resources:
  - allow deletion of persisted HF resource rows.
  - block SOP-derived (`sop_layer`, `sop_gap`) rows with clear user feedback.
- Move `Resolve Conflicts` control into Active Schedule action row.
- Replace SOP status table with compact SOP indicator/toggle buttons (max two visible rows + overflow hint).
- Ensure Active Schedule `Delete` / `Move Selected to Resources` remain muted when no explicit selection.

Acceptance criteria:
- HF Schedule has an `Import/Export` control next to `Save HF Schedule`.
- Importing schedule ignores non-HF rows and reports skipped rows.
- Schedule Resources can delete selected HF resource rows without moving them into Active Schedule first.
- SOP-derived resource rows cannot be deleted from Schedule Resources.
- `Add to Active Schedule` has no HF/SOP target dropdown.
- SOP status area is compact and supports activate/deactivate directly from indicator buttons.
- Active Schedule Delete/Move buttons are muted when there is no selected row/checkbox.

Verification:
- `python tools/release_preflight.py`
- `python -m compileall freqinout`
- Manual:
  - Import/export HF schedule JSON from HF tab.
  - Delete selected rows in Schedule Resources (HF vs SOP-derived mix).
  - Confirm Add-to-active flow works without target dropdown.
  - Confirm SOP indicator buttons toggle active state and refresh scheduler/UI.

Rollback:
- Revert changes in:
  - `freqinout/gui/daily_schedule_tab.py`

### 1.58 Addendum (2026-02-21): SOP Conflict Prompt Restoration + HF Resource Action Gating

Problem:
- Saving HF SOP actions is not consistently prompting operators when conflicts exist with HF Daily, Net, or peer SOP actions.
- On HF Schedule, `Resolve Conflicts` can appear non-actionable because enablement is tied only to precomputed conflict flags.
- `Add to Active Schedule` is enabled even when no Schedule Resources row is selected.

Scope:
- Restore mandatory conflict resolution prompting during HF SOP save:
  - include Daily conflicts,
  - include Net conflicts (including conflicts outside the selected group for single-radio operation),
  - include SOP-vs-SOP peer action overlap.
- Keep prompt choices and policies:
  - `SOP Priority For ALL`
  - `Net Priority on Conflicts`
  - `Daily Schedule Priority on Conflicts`
- For Net/Daily priority when first occurrence conflicts, require adjusted start time and validate against the same conflict scope before continuing.
- Update HF Schedule resource action gating:
  - `Add to Active Schedule` enabled only when one or more Schedule Resources rows are selected.
  - `Resolve Conflicts` remains available whenever resource rows exist and reports either duplicate conflicts or clean state.

Acceptance criteria:
- Saving HF SOP with conflicting rows always surfaces a conflict dialog before save completes.
- Dialog includes Daily, Net, and SOP conflict summaries when present.
- Choosing Net/Daily priority with first-occurrence conflict requires a valid non-conflicting start time.
- On HF Schedule, `Add to Active Schedule` is muted until a row is selected.
- On HF Schedule, `Resolve Conflicts` is clickable when resources exist and produces informative output.

Verification:
- `python -m compileall freqinout/core/sop_manager.py freqinout/gui/sop_tab.py freqinout/gui/daily_schedule_tab.py`
- `python tools/release_preflight.py`
- Manual:
  - create HF SOP action overlapping Daily/Net and confirm save prompt appears.
  - create two overlapping HF SOP actions on different frequencies and confirm SOP conflict prompt appears.
  - verify `Add to Active Schedule` disabled without resource selection.
  - verify `Resolve Conflicts` runs with and without duplicate rows.

Rollback:
- Revert changes in:
  - `freqinout/core/sop_manager.py`
  - `freqinout/gui/sop_tab.py`
  - `freqinout/gui/daily_schedule_tab.py`

### 1.59 Addendum (2026-02-21): SOP Builder Real-Time Conflict Prompting (Debounced)

Problem:
- Conflict prompts existed on HF SOP save, but operators need earlier feedback while composing action rows.
- Net conflicts are not visible in-table, so waiting until save is late for planning workflow.

Scope:
- Add debounced real-time conflict checks for HF SOP action rows in `SOP Builder`.
- Trigger checks on action-row edits (group/resource/action/band-freq/start/duration/interval/contact/description).
- Reuse the same conflict policy prompt choices used at save-time:
  - `SOP Priority For ALL`
  - `Net Priority on Conflicts`
  - `Daily Schedule Priority on Conflicts`
- For Net/Daily policy first-occurrence conflicts (and SOP-vs-SOP overlaps), require adjusted start time with suggestion.
- Keep save-time conflict gating as final guardrail (no behavior removal).

Performance guardrails:
- Use a short single-shot debounce timer; avoid conflict scans on every keystroke event.
- Skip checks while UI is loading or when category is Local Comms.

Acceptance criteria:
- While editing HF action rows, conflict dialog appears after edit pause when a complete row conflicts with Daily/Net/SOP.
- Prompt includes Daily, Net, and SOP conflict summaries.
- Selecting a policy updates row conflict policy metadata and any adjusted start/end values in-table.
- No prompt loops while typing (debounce + duplicate-signature suppression).
- Save-time conflict checks still run and prevent unresolved activation conflicts.

Verification:
- `python -m compileall freqinout/gui/sop_tab.py`
- `python tools/release_preflight.py`
- Manual:
  - add/edit a conflicting HF action row and confirm prompt appears without pressing Save.
  - verify Net conflict summary appears in prompt.
  - verify start-time suggestion/validation loop for Net/Daily priority.
  - verify Local Comms category does not trigger HF conflict prompts.

Rollback:
- Revert changes in:
  - `freqinout/gui/sop_tab.py`

### 1.60 Addendum (2026-02-21): SOP Action Row Inline Conflict Badges

Problem:
- Operators needed immediate visual conflict state per action row before modal prompts.
- Conflict awareness should be visible at a glance while editing SOP actions.

Scope:
- Add an inline `Conflict` status badge column to HF SOP action rows.
- Badge states:
  - `Pending` for incomplete rows not yet eligible for conflict evaluation.
  - `OK` when no Daily/Net/SOP conflicts are detected.
  - `Conflict` when conflicts are detected, with tooltip detail summary.
- Reuse debounced real-time conflict analysis and keep checks off Local Comms rows.
- Preserve save-time conflict enforcement as final guard.

Acceptance criteria:
- Each HF action row shows a live conflict badge while editing.
- Hovering a `Conflict` badge displays Daily/Net/SOP conflict summaries.
- Badge styling updates with theme changes.
- No blocking regressions or typing lag introduced.

Verification:
- `python -m compileall freqinout/gui/sop_tab.py`
- `python tools/release_preflight.py`
- Manual:
  - create one clean HF action row and confirm `OK`.
  - create conflicting HF rows and confirm `Conflict` tooltip details.
  - leave an incomplete row and confirm `Pending`.
  - switch themes and verify badge legibility.

Rollback:
- Revert changes in:
  - `freqinout/gui/sop_tab.py`

### 1.61 Addendum (2026-02-21): SOP Prompt Debounce Stabilization + HF Schedule Time Sort Action

Problem:
- SOP Builder real-time conflict prompts could repeat for the same unresolved row even after one user decision.
- HF Schedule needed a quick way to sequence rows chronologically for operator review.

Scope:
- SOP Builder:
  - stabilize real-time prompt suppression by marking the current conflict signature as handled before opening the dialog.
  - prevent immediate re-prompt on unchanged conflict state until row data changes.
- HF Schedule:
  - add `Sort by Time` action in Active Schedule controls.
  - sort rows by day then start time (with stable tie-breakers) while preserving row metadata and SOP overlay tags.

Acceptance criteria:
- A single real-time conflict decision does not immediately re-open the same dialog on unchanged row values.
- HF Schedule users can click `Sort by Time` to quickly reorder Active Schedule into day/time sequence.
- Sorting does not drop SOP overlay metadata or HF row metadata.

Verification:
- `python -m compileall freqinout/gui/sop_tab.py freqinout/gui/daily_schedule_tab.py`
- `python tools/release_preflight.py`
- Manual:
  - create one persistent conflict row in SOP Builder, choose a policy once, confirm no immediate duplicate dialog loop.
  - add several HF rows out-of-order and confirm `Sort by Time` reorders by day/start.

Rollback:
- Revert changes in:
  - `freqinout/gui/sop_tab.py`
  - `freqinout/gui/daily_schedule_tab.py`

### 1.62 Addendum (2026-02-21): HF Resolve Conflicts Active-Row Scope Fix

Problem:
- `Resolve Conflicts` on HF Schedule could report "No HF schedule conflicts detected" even when highlighted active rows clearly overlapped in time.
- The handler was only checking duplicate resource-to-active keys, not active-row time overlaps.

Scope:
- Update `Resolve Conflicts` to prioritize selected Active Schedule rows (checkbox or row selection).
- Detect active-row time overlap conflicts using the same day/time overlap model used for row highlighting.
- Keep existing resource-duplicate conflict behavior when no active rows are selected.

Acceptance criteria:
- Selecting overlapping Active Schedule rows and clicking `Resolve Conflicts` reports those overlaps.
- Message includes actionable guidance (edit time, move to resources, or delete).
- Existing resource duplicate checks still work when resolving from Schedule Resources context.

Verification:
- `python -m compileall freqinout/gui/daily_schedule_tab.py`
- `python tools/release_preflight.py`
- Manual:
  - select two highlighted active rows with overlapping times and verify conflict message appears.
  - run resolve with only resource rows selected and verify duplicate resource check still functions.

Rollback:
- Revert changes in:
  - `freqinout/gui/daily_schedule_tab.py`

### 1.63 Addendum (2026-02-21): HF Conflict Summary Consolidation + Auto-Eligible Resolve

Problem:
- Active HF conflicts could require manual row selection before `Resolve Conflicts` looked actionable.
- Conflict output repeated the same pair for every day when rows applied to `ALL`, producing noisy dialogs.

Scope:
- Automatically make `Resolve Conflicts` eligible when Active Schedule contains time-overlap conflicts, even without selection.
- Prioritize Active Schedule conflict reporting before resource duplicate checks.
- Consolidate Active Schedule conflict output by row-pair:
  - show each conflicting pair once,
  - include day scope only when conflict is limited to specific days,
  - suppress repetitive day-by-day duplication for all-day conflicts.

Acceptance criteria:
- `Resolve Conflicts` is enabled and highlighted when active HF conflicts exist with no selected rows.
- Clicking `Resolve Conflicts` without selection shows consolidated Active Schedule conflicts if present.
- Day-scoped conflicts show concise day scope (e.g., `Mon, Wed`); all-day conflicts are not repeated per day.

Verification:
- `python -m compileall freqinout/gui/daily_schedule_tab.py`
- `python tools/release_preflight.py`
- Manual:
  - create two `ALL` rows with overlapping windows and verify one consolidated pair entry.
  - create day-limited overlap and verify day scope is shown.
  - verify resource duplicate resolution still works when no active conflicts exist.

Rollback:
- Revert changes in:
  - `freqinout/gui/daily_schedule_tab.py`

### 1.64 Addendum (2026-02-21): HF Conflict Dialog Auto-Adjust Option (SOP Priority Clip/Split)

Problem:
- Operators expected `Resolve Conflicts` to provide an actionable auto-resolution path, not only diagnostics.

Scope:
- Add `Auto-Adjust HF Around SOP` option in HF conflict dialog when eligible HF-vs-SOP overlaps are present.
- Auto-adjust behavior:
  - keep SOP rows as-is,
  - clip or split overlapping HF rows around SOP windows,
  - remove fully-covered HF rows.
- Keep unsupported/ambiguous overlap cases as manual resolution and report skipped counts.

Constraints:
- Generated adjustment applies to the current Active Schedule table (user still controls save).
- Preserve SOP overlay metadata and existing row conflict highlighting behavior.

Acceptance criteria:
- Conflict dialog shows `Auto-Adjust HF Around SOP` when applicable.
- Applying auto-adjust updates Active Schedule rows in place (clip/split/remove) and marks schedule dirty.
- Result summary reports changed/removed/split counts and skipped manual pairs.

Verification:
- `python -m compileall freqinout/gui/daily_schedule_tab.py`
- `python tools/release_preflight.py`
- Manual:
  - create HF row overlapping SOP row and confirm auto-adjust option appears.
  - run auto-adjust and verify HF row is clipped/split around SOP interval.
  - verify unsupported day-scope conflicts are reported as skipped/manual.

Rollback:
- Revert changes in:
  - `freqinout/gui/daily_schedule_tab.py`

### 1.65 Addendum (2026-02-21): HF Schedule Default Time-Sorted View + Quiet Resolve CTA

Problem:
- Dedicated `Sort by Time` control increased UI noise for a behavior that should be default.
- `Resolve Conflicts` remained visible/highlighted even in no-conflict states, which was misleading.

Scope:
- Remove explicit `Sort by Time` button from HF Schedule actions.
- Apply time-sort by default during schedule load/rebuild/overlay refresh paths.
- Hide `Resolve Conflicts` button unless actual conflicts exist:
  - active schedule time-overlap conflicts, or
  - schedule-resource duplicate conflicts.

Acceptance criteria:
- HF Active Schedule opens in time-sorted order by default.
- No sort button is shown in HF Schedule actions.
- `Resolve Conflicts` is hidden when there are no conflicts and appears only when conflicts exist.

Verification:
- `python -m compileall freqinout/gui/daily_schedule_tab.py`
- `python tools/release_preflight.py`
- Manual:
  - open HF tab and verify rows are sorted by day/time without clicking sort.
  - verify `Resolve Conflicts` hidden in clean schedule and visible after introducing conflict.

Rollback:
- Revert changes in:
  - `freqinout/gui/daily_schedule_tab.py`

### 1.71 Addendum (2026-02-21): Net Schedule Net/SOP Conflict Enforcement + FreqPlanner Policy Fidelity

Problem:
- Net rows that conflict with active SOP windows were not consistently highlighted in Net Schedule.
- Net Schedule allowed saves without enforcing explicit Net-priority decisions for conflicting overlaps.
- FreqPlanner could still render Net in Net/SOP overlap windows even after SOP-priority resolution due to incomplete signature wiring.

Scope:
- Net Schedule tab:
  - compute active Net-vs-SOP conflicts from current table rows (including unsaved edits),
  - highlight blocking conflict rows directly in the Net Schedule grid,
  - block `Save Net Schedule` while any conflicting overlap is unresolved or marked SOP-priority,
  - allow save only when each blocking overlap has `NET_PRIORITY`, or conflicts are removed.
- FreqPlanner tab:
  - propagate row signatures through Net/SOP hour slices,
  - use signature+window policy matching when both Net and SOP occupy a cell,
  - ensure policy result drives visible label (`SOP:...` when SOP-priority).
- Preserve scheduler-engine behavior and policy table schema; this change is UI/rendering + save-gating only.

Acceptance criteria:
- Net Schedule visually indicates rows that currently conflict with active SOP windows and are not Net-priority resolved.
- Attempting to save Net Schedule with unresolved/SOP-priority overlaps is blocked with actionable guidance.
- If conflicts are resolved to `NET_PRIORITY`, save proceeds normally.
- In overlapping Net/SOP windows, FreqPlanner displays SOP labels when policy is `SOP_PRIORITY` and Net labels when policy is `NET_PRIORITY`.

Verification:
- `python -m compileall freqinout/gui/net_schedule_tab.py`
- `python -m compileall freqinout/gui/freq_planner_tab.py`
- `python tools/release_preflight.py`
- Manual:
  - add overlapping Net row while HF SOP is active; confirm highlight and save blocking until Net priority is set.
  - set conflict to SOP priority and confirm FreqPlanner shows SOP label in overlap window.
  - switch same conflict to Net priority and confirm FreqPlanner returns to Net label.

Rollback:
- Revert changes in:
  - `freqinout/gui/net_schedule_tab.py`
  - `freqinout/gui/freq_planner_tab.py`

### 1.72 Addendum (2026-02-21): Net Add Gate + Built-In Move-Back Dedupe

Problem:
- Adding a single recurring Net row could show many conflict lines (one per occurrence window), which is noisy.
- In add-from-resources flow, closing the conflict dialog could still leave the conflicting row added.
- Moving an unchanged built-in Net row from Schedule back to Resources could create/convert to a manual entry instead of behaving like delete/restore.

Scope:
- Net add flow:
  - evaluate Net-vs-active-SOP conflicts against the prospective post-add schedule before inserting rows,
  - if user cancels/closes conflict resolution, abort add and do not insert rows,
  - consolidate conflict summaries/prompts by Net/SOP row pair and show `(+N more)` occurrences.
- Net/SOP review flow:
  - prompt once per consolidated conflict pair; apply decision to all matching overlap occurrences.
- Move Selected to Resources:
  - if selected schedule row originated from a built-in resource and matches the built-in record (unchanged), skip manual upsert and only remove it from active schedule.

Acceptance criteria:
- Closing/canceling conflict resolution while adding conflicting rows leaves Net Schedule unchanged.
- Conflict summaries for recurring rows are consolidated, not repeated per weekly occurrence line.
- Moving unchanged built-in row(s) back to resources does not create manual duplicates or convert built-in source type.

Verification:
- `python -m compileall freqinout/gui/net_schedule_tab.py`
- `python tools/release_preflight.py`
- Manual:
  - add one weekly Net conflicting with active SOP and confirm one consolidated line with occurrence count.
  - close/cancel dialog and confirm row is not added.
  - move unchanged built-in-origin schedule row back to resources and confirm no new manual duplicate.

Rollback:
- Revert changes in:
  - `freqinout/gui/net_schedule_tab.py`

### 1.73 Addendum (2026-02-21): Messages `+BBS` Action for FLMsg/FLAmp/VarAC File Rows

Problem:
- Operators need a fast in-table action to route selected message files into the configured VarAC BBS folder.
- Current Messages action column supports View/Flag/Delete (and Archive for BBS rows) but lacks direct BBS-copy flow.

Scope:
- Messages action column:
  - when VarAC BBS folder is configured, show `+BBS` action to the right of flag for eligible rows.
  - eligibility: message type `FLMSG`, `FLAMP`, or `VarAC` file row (`FileRecord`-backed row).
- `+BBS` click behavior:
  - copy file to configured VarAC BBS directory,
  - if destination filename exists, prompt `Overwrite existing file?` (Yes replaces, No cancels),
  - show success popup on completion,
  - show failure popup on copy error.

Constraints:
- Keep existing action-column behavior for non-eligible rows unchanged.
- Keep BBS-origin row actions (`Archive`, `Delete`) unchanged.

Acceptance criteria:
- `+BBS` appears only when VarAC BBS path is configured and row is eligible.
- Clicking `+BBS` copies the exact source file into VarAC BBS folder.
- Existing destination filename triggers overwrite confirmation.
- Copy success and failure both show user-facing feedback dialogs.

Verification:
- `python -m compileall freqinout/gui/message_viewer_tab.py`
- `python tools/release_preflight.py`
- Manual:
  - configure `varac_bbs_dir`, verify `+BBS` on FLMSG/FLAMP/VarAC file rows.
  - click `+BBS` with no existing destination file and verify successful copy + popup.
  - click `+BBS` with existing destination file and verify overwrite prompt behavior.

Rollback:
- Revert changes in:
  - `freqinout/gui/message_viewer_tab.py`

### 1.74 Addendum (2026-02-21): SOP Builder Dynamic-Option Hotpath Caching + Defect Audit

Problem:
- SOP action-row editing can become sluggish under repeated row refresh paths.
- `SOPTab` dynamic option refresh recomputes several lists per event and repeatedly calls DB-backed contact lookup functions.
- Spotter-form action catalog building performs repeated directory scans in hot UI paths.

Scope:
- Add lightweight, in-process memoization for SOP action-row dynamic option lookups in `freqinout/gui/sop_tab.py`:
  - HF/Local group name option lists
  - Local resource/mode options keyed by group/resource
  - HF band/frequency options keyed by group
  - Contact target lookup results keyed by `(group, contact_rule)` with short TTL
  - Spotter form/action catalog caching keyed by forms-path directory metadata
- Add explicit cache invalidation on settings/reference-data refresh paths.
- Preserve existing SOP behavior, validation rules, and UTC storage semantics.

Acceptance criteria:
- SOP row refresh operations avoid repeated identical DB lookup calls during rapid UI edits.
- Spotter forms are not re-scanned from disk on every row dynamic refresh.
- Settings refresh and local profile refresh invalidate caches so option lists remain accurate.
- No functional workflow regressions for HF/Local SOP row editing.

Verification:
- Benchmark (offscreen Qt micro-benchmark) before/after:
  - repeated `SOPTab._refresh_row_dynamic_options` calls on configured HF rows
  - verify reduced call counts for:
    - `_load_spotter_forms`
    - `SOPManager.resolve_primary_contacts`
- `python tools/release_preflight.py`
- `python -m compileall freqinout`

Rollback:
- Revert changes in:
  - `freqinout/gui/sop_tab.py`

### 1.75 Addendum (2026-02-21): Reusable GUI Smoke Tool for Full Tab Sweep

Problem:
- Existing automated tests focus on core propagation/scheduler logic and do not provide broad GUI tab smoke coverage.
- We need a repeatable, low-friction smoke command that exercises all registered tabs and basic subsection controls to catch startup/render/activation regressions early.

Scope:
- Add a reusable tool script under `tools/` that:
  - starts the app UI in offscreen mode,
  - isolates run data via configurable `FREQINOUT_CONFIG_DIR`,
  - instantiates `MainWindow`,
  - activates every registered screen/tab in `_screens`,
  - performs lightweight subsection sweeps (combo/index toggles, checkable tool/button toggles, nested stacked/tab index cycling),
  - reports per-tab pass/fail and failed action details,
  - exits non-zero on smoke failures.
- Keep the smoke tool non-destructive to real operator data by default.

Acceptance criteria:
- One command can execute a full tab smoke sweep and print a tab-by-tab report.
- Script runs against isolated config by default and does not require radio software to be present.
- Failures are surfaced with actionable context (tab label + operation + exception).

Verification:
- Run the new smoke tool and confirm all tabs are visited.
- `python tools/release_preflight.py`
- `python -m compileall freqinout`

Rollback:
- Revert changes in:
  - `tools/gui_smoke_tabs.py`

### 1.76 Addendum (2026-02-21): Scheduler Offline-Loop Guardrails + Voice Sideband Control

Problem:
- Scheduler control paths can enter rapid retry/reapply loops when control backends are unavailable or not converging.
- Manual/forced retry flow can reschedule every second while backoff is active, creating high-frequency control churn.
- Operating Groups still rely on `SSB` as a generic mode, which does not carry required USB/LSB sideband intent.
- FLRig voice transitions from digital schedules do not consistently switch rig mode to USB/LSB.

Scope:
- Scheduler safety hardening:
  - prevent forced-retry spin loops while control backoff is active or a control task is still running,
  - limit forced retry escalation to user-forced/control-critical paths (not normal periodic schedule ticks),
  - treat missing local controller process (`flrig`/`js8call`) as temporarily unavailable control backend.
- Voice mode normalization:
  - normalize legacy Operating Group `SSB` entries to explicit `USB`/`LSB` using band defaults,
  - update Operating Group add/edit UX to align voice selection with band sideband expectations.
- FLRig command reliability:
  - pass resolved voice mode through scheduler control actions,
  - issue verify-capable mode commands where available and verify post-set frequency readback.

Band sideband policy:
- `LSB`: `160M`, `80M`, `60M`, `40M`, `30M`
- `USB`: `20M`, `17M`, `15M`, `12M`, `10M`, `6M`, `2M`

Acceptance criteria:
- With `control_via=FLRig` and FLRig not running, scheduler does not enter high-frequency retry loops or UI-freeze-like command churn.
- With `control_via=JS8Call` and JS8Call not running, scheduler remains stable and does not spin forced retries every second.
- Operating Group voice entries are stored/used as explicit `USB`/`LSB` (legacy `SSB` upgraded safely by band).
- Voice schedule/QSY transitions to FLRig issue USB/LSB mode changes and no longer remain stuck in digital mode when moving to voice bands.

Verification:
- Reproduce control backend down cases:
  - stop FLRig, set `control_via=FLRig`, observe stable scheduler behavior.
  - stop JS8Call, set `control_via=JS8Call`, observe stable scheduler behavior.
- Reproduce voice transition:
  - start from digital schedule entry, switch to voice entry on low band (`40M`) and high band (`20M`), verify FLRig mode changes to `LSB`/`USB` respectively.
- `python tools/release_preflight.py`
- `python -m compileall freqinout`

Rollback:
- Revert changes in:
  - `freqinout/core/scheduler_engine.py`
  - `freqinout/radio_interface/rigctl_client.py`
  - `freqinout/gui/settings_tab.py`
  - `freqinout/gui/qsy_helper.py`

### 1.77 Addendum (2026-02-21): Operating-Group Frequency/VFO Authority + FLRig Set Sequencing

Problem:
- FLRig mode transitions now occur, but some QSY/schedule applies do not land on the expected frequency/VFO for the selected Operating Group.
- Scheduler currently resolves Operating Group metadata for VFO/FLDigi, but frequency can remain sourced from schedule row payloads even when an Operating Group is selected.
- Some rigs can alter tuned frequency when mode is changed after frequency is set.

Scope:
- Scheduler Operating Group application:
  - normalize mode matching (`SSB` legacy vs `USB/LSB`) when resolving Operating Group entries,
  - treat matched Operating Group rows as authoritative for frequency and VFO in control apply paths.
- FLRig command ordering:
  - apply VFO and mode before final frequency set,
  - prefer explicit VFO-targeted frequency calls where available,
  - keep readback verification to surface non-convergence.

Acceptance criteria:
- A schedule/QSY that targets Operating Group `PRA` (`20M`, `USB`, `14.225`, `VFO B`) issues FLRig control with `14.225 MHz` and `VFO B`.
- Scheduler no longer applies stale frequency values from schedule rows when a valid Operating Group match is present.
- FLRig transitions from digital to voice land on both the expected sideband and requested frequency after one apply cycle.

Verification:
- Reproduce with a known Operating Group row (e.g., `PRA`) from ControlFreq QSY and schedule-driven paths.
- Confirm log line includes applied `band/freq/vfo/mode` and matches Operating Group values.
- `python tools/release_preflight.py`
- `python -m compileall freqinout`

Rollback:
- Revert changes in:
  - `freqinout/core/scheduler_engine.py`
  - `freqinout/radio_interface/rigctl_client.py`

### 1.78 Addendum (2026-02-21): Exhaustive GUI Subsection Smoke Coverage

Problem:
- Current GUI smoke automation validates top-level tab activation and a minimal control flip pattern, but does not fully traverse subsection selectors.
- Settings and other multi-section tabs can hide defects when only a single alternate index is exercised.
- Performance review confidence is limited unless subsection paths are covered in one reproducible run.

Scope:
- Extend `tools/gui_smoke_tabs.py` with an explicit exhaustive sweep mode that:
  - cycles all indices for supported subsection selectors (combo boxes, stacked widgets, tab widgets, toolboxes, list-based section selectors),
  - exercises checkable exclusive-button groups in addition to non-exclusive toggles,
  - records per-tab coverage metrics for subsection switches.
- Preserve existing basic mode defaults for fast CI/local smoke runs.
- Add CLI flags to select sweep mode and control breadth safely.

Acceptance criteria:
- Smoke tool supports both `basic` and `exhaustive` sweep modes.
- Exhaustive mode visits all reachable subsection indices for supported selector widgets in each tab.
- Output report includes per-tab subsection coverage metrics and continues to return non-zero on failures.
- Existing basic mode behavior remains backward compatible for current users.

Verification:
- `python -m compileall tools/gui_smoke_tabs.py`
- `python tools/perf_benchmark.py reset-log`
- `python tools/gui_smoke_tabs.py --sweep-mode exhaustive --max-controls 0 --json-out .benchmarks/gui-smoke-exhaustive.json --keep-config`
- `python tools/perf_benchmark.py summarize --sort p95 --limit 120`

Rollback:
- Revert changes in:
  - `tools/gui_smoke_tabs.py`
  - `SPEC.md`

### 1.79 Addendum (2026-02-21): Conflict-Path UI Performance Hardening (ControlFreq + HF Schedule + Net Schedule)

Problem:
- Conflict detection and conflict-highlighting paths can perform duplicate full-table scans on edit/change events.
- Net/SOP conflict scans run on the UI thread and can be retriggered with unchanged inputs, increasing freeze risk on larger schedules.
- SOP-change fanout triggers heavy tab refresh work even when target tabs are not visible.

Scope:
- `freqinout/gui/net_schedule_tab.py`
  - reuse collected Net rows inside conflict-highlighting path to avoid duplicate `_collect_rows()` passes.
  - add lightweight memoization for Net/SOP conflict scan results keyed by row signature + SOP policy/source mtime + horizon.
  - defer conflict-refresh work when Net Schedule tab is not visible, and refresh on next tab activation.
- `freqinout/gui/daily_schedule_tab.py`
  - unify active conflict-pair and conflict-row computation into a shared helper.
  - avoid duplicate active-conflict recomputation in `_on_table_item_changed` by sharing precomputed results between highlight and action-state updates.
  - avoid duplicate local refresh after global SOP data dispatch when main window callback already handles refresh.
- `freqinout/gui/main_window.py`
  - no behavior changes required; existing tab activation callback path is used for deferred refresh execution.

Constraints:
- Preserve existing conflict policy semantics and save-blocking behavior.
- Preserve UTC storage and existing schedule validation workflows.
- Keep all heavy conflict scans on current thread for this phase (no thread-model change), but reduce redundant executions.

Acceptance criteria:
- Net conflict-highlighting path performs one row-collection pass per refresh cycle rather than duplicate full collections.
- Repeated Net conflict scans with unchanged inputs reuse cached results.
- SOP data changes received while Net Schedule is hidden do not immediately run heavy conflict scans; refresh runs on next tab activation.
- Daily schedule item-edit path performs one active-conflict computation and reuses it across highlight/action-state updates.
- Existing save-blocking behavior for unresolved Net/SOP conflicts remains intact.

Verification:
- `python tools/release_preflight.py`
- `python -m compileall freqinout`
- `powershell -ExecutionPolicy Bypass -File .\tools\freqinout-db.ps1 status`
- Manual:
  - edit Net Schedule rows rapidly and verify conflict highlighting remains responsive.
  - switch away from Net Schedule, trigger SOP change, return to Net Schedule, verify refresh occurs on activation.
  - edit HF Schedule row times and verify conflict highlight + Resolve button state remain accurate.

Rollback:
- Revert changes in:
  - `freqinout/gui/net_schedule_tab.py`
  - `freqinout/gui/daily_schedule_tab.py`
  - `SPEC.md`

### 1.80 Addendum (2026-02-21): Conflict-Path Perf Observability Spans

Problem:
- Benchmark summaries do not currently expose key conflict-checking paths in `HF Schedule` and `Net Schedule`.
- Without direct span coverage, regressions in conflict scan/highlight behavior are difficult to quantify.

Scope:
- Add targeted perf spans to:
  - `freqinout/gui/daily_schedule_tab.py`
    - active conflict-state computation
    - conflict-highlight repaint path
    - table-item conflict recompute pipeline
  - `freqinout/gui/net_schedule_tab.py`
    - Net/SOP conflict scan miss path
    - Net conflict-highlight refresh path
- Keep instrumentation lightweight and gated by existing perf-metrics settings.

Acceptance criteria:
- `tools/perf_benchmark.py summarize` shows named Daily/Net conflict spans after exercising those tabs.
- No functional behavior changes in conflict policies or save blocking.

Verification:
- `python tools/gui_smoke_tabs.py --sweep-mode exhaustive --max-controls 0 --json-out .benchmarks/gui-smoke-exhaustive-after-fix.json --keep-config`
- `python tools/perf_benchmark.py summarize --name \"^(daily_schedule|net_schedule)\" --sort p95 --limit 120`
- `python tools/release_preflight.py`
- `python -m compileall freqinout`
- `powershell -ExecutionPolicy Bypass -File .\\tools\\freqinout-db.ps1 status`

Rollback:
- Revert changes in:
  - `freqinout/gui/daily_schedule_tab.py`
  - `freqinout/gui/net_schedule_tab.py`
  - `SPEC.md`

### 1.81 Addendum (2026-02-21): Conflict Computation Throughput Optimization (HF + Net)

Problem:
- Conflict observability spans show `daily_schedule.active_conflict_state` as the dominant UI-side conflict cost under dense row sets.
- Net conflict scans can alternate between multiple row-signature shapes, reducing cache reuse when only one cache slot is retained.

Scope:
- `freqinout/gui/daily_schedule_tab.py`
  - optimize active conflict pair generation using a sweep-style active-window pass per day.
  - reduce selected-scope conflict work by comparing non-selected rows only against currently active selected rows.
  - avoid repeated day-index linear lookups in hot loops.
- `freqinout/gui/net_schedule_tab.py`
  - replace single-entry conflict cache with a small bounded multi-key cache to improve reuse across alternating scan call patterns.

Constraints:
- Preserve conflict semantics, including overnight handling, day scoping, and duplicate-pair suppression.
- Preserve save-blocking behavior for unresolved Net/SOP conflicts.

Acceptance criteria:
- `daily_schedule.active_conflict_state` span shows measurable runtime reduction on dense synthetic workloads.
- Repeated alternating Net conflict scans show cache reuse across recent distinct keys (no behavior change in returned conflict rows).

Verification:
- `python -m compileall freqinout/gui/daily_schedule_tab.py freqinout/gui/net_schedule_tab.py`
- Run targeted synthetic conflict workloads and compare `tools/perf_benchmark.py summarize --name "^(daily_schedule|net_schedule)"`.
- `python tools/release_preflight.py`
- `python -m compileall freqinout`
- `powershell -ExecutionPolicy Bypass -File .\\tools\\freqinout-db.ps1 status`

Rollback:
- Revert changes in:
  - `freqinout/gui/daily_schedule_tab.py`
  - `freqinout/gui/net_schedule_tab.py`
  - `SPEC.md`

### 1.82 Addendum (2026-02-21): Scheduler Status Poll Guarding and Offline Backoff

Problem:
- UI status refresh paths call `SchedulerEngine.get_status_summary()` every 2-5 seconds.
- `get_status_summary()` can synchronously poll FLRig XML-RPC even when control mode is `NONE`/`JS8CALL`, causing avoidable timeouts and UI stalls when FLRig is unavailable.
- A single status refresh can perform duplicate frequency polls through off-schedule checks.

Scope:
- `freqinout/core/scheduler_engine.py`
  - make rig-frequency reads control-mode aware so JS8 mode does not probe FLRig first.
  - gate FLRig status polling to FLRig control mode only.
  - add short TTL/backoff caching for FLRig status polls used by status-summary paths.
  - reuse pre-read frequency in off-schedule checks to avoid duplicate backend polling per status pass.
  - keep schedule-apply behavior unchanged for command paths.

Constraints:
- Preserve scheduler precedence and prompt/off-schedule semantics.
- Preserve behavior when radio software is absent (no startup failures).
- Prefer stale/empty status over blocking the UI thread.

Acceptance criteria:
- With `control_via=FLRig` and FLRig unavailable, periodic status refresh does not repeatedly block on XML-RPC on every tick.
- With `control_via=JS8Call`, status frequency polling uses JS8 backend directly and does not first query FLRig.
- `get_status_summary()` performs at most one frequency poll per refresh cycle for off-schedule frequency checks.

Verification:
- `python -m compileall freqinout/core/scheduler_engine.py`
- `python tools/gui_smoke_tabs.py --sweep-mode basic --json-out .benchmarks/gui-smoke-basic-after-status-guard.json --keep-config`
- `python tools/release_preflight.py`
- `python -m compileall freqinout`
- `powershell -ExecutionPolicy Bypass -File .\\tools\\freqinout-db.ps1 status`

Rollback:
- Revert changes in:
  - `freqinout/core/scheduler_engine.py`
  - `SPEC.md`

### 1.83 Addendum (2026-02-22): SOP Layer Per-Row Group Label Fidelity (FreqPlanner/Scheduler)

Problem:
- FreqPlanner SOP labels were derived from profile-level `operating_group`, not per-action/per-row group data.
- In mixed-group HF SOP profiles, multiple entries could all display as one group (for example, both rows shown as `SOP:AMRRON`).
- SOP layer persistence did not carry row-level group metadata, so scheduler/planner could not reliably differentiate per-row groups.

Scope:
- `freqinout/core/sop_manager.py`
  - add/maintain `group_name` column on `sop_schedule_layer`.
  - persist row-level `group_name` during profile save and layer upsert paths.
  - rebuild layer rows from actions with each action’s group and include group in dedupe identity.
  - add migration/backfill for existing layer rows from matching `sop_actions` (fallback to profile group).
  - include `group_name` in schedule-layer export/import normalization.
- `freqinout/gui/freq_planner_tab.py`
  - load SOP layer rows with row-level `group_name` (fallback to profile group if absent).
  - include `sop_profile_id` and `sop_layer_id` in loaded rows for signature correctness.
- `freqinout/core/scheduler_engine.py`
  - read SOP row group from `sop_schedule_layer.group_name` when present (fallback to profile group).
- `freqinout/core/db_initializer.py`
  - ensure `sop_schedule_layer.group_name` exists in baseline DB initialization.

Acceptance criteria:
- FreqPlanner displays SOP labels using row-level group values when available.
- Mixed-group SOP entries (same profile) can render distinct SOP group labels correctly.
- Scheduler SOP layer loader preserves per-row group identity for condition-level filtering.

Verification:
- `python -m compileall freqinout/core/sop_manager.py freqinout/gui/freq_planner_tab.py freqinout/core/scheduler_engine.py freqinout/core/db_initializer.py`
- `python tools/release_preflight.py`
- `python tools/gui_smoke_tabs.py --sweep-mode basic --json-out .benchmarks/gui-smoke-basic-after-sop-group-label-fix.json --keep-config`
- `python -m compileall freqinout`
- `powershell -ExecutionPolicy Bypass -File .\\tools\\freqinout-db.ps1 status`

Rollback:
- Revert changes in:
  - `freqinout/core/sop_manager.py`
  - `freqinout/gui/freq_planner_tab.py`
  - `freqinout/core/scheduler_engine.py`
  - `freqinout/core/db_initializer.py`
  - `SPEC.md`

### 1.84 Addendum (2026-02-22): SOP Builder Net-Priority Decisions Must Drive Runtime/Planner Arbitration

Problem:
- SOP Builder conflict prompts allow choosing `SOP Priority` vs `Net Priority`, but those decisions are stored only on `sop_actions.conflict_policy`.
- Runtime scheduler and FreqPlanner Net-vs-SOP arbitration read `sop_net_conflict_policy` rows, not `sop_actions.conflict_policy`.
- Result: users can select SOP priority in SOP Builder and still see Net win in FreqPlanner/runtime, causing misleading behavior and operator confusion.

Impacted files and failure modes:
- `freqinout/gui/sop_tab.py`
  - Failure mode: UI records conflict choice that is not persisted to active policy table.
- `freqinout/core/sop_manager.py`
  - Failure mode: no reconciliation from SOP action conflict choices to Net/SOP policy rows for overlapping windows.
- `freqinout/gui/freq_planner_tab.py` and `freqinout/core/scheduler_engine.py` (readers)
  - Existing behavior is correct for their source-of-truth table; risk is stale display if writer path is incomplete.

Scope:
- Add manager-level synchronization that maps saved SOP action conflict policies to concrete overlapping Net/SOP windows and upserts `sop_net_conflict_policy` rows.
- Invoke synchronization from SOP save flow after profile/actions/layer persistence.
- Policy mapping rules:
  - `SOP_ALL` -> `SOP_PRIORITY` for overlapping Net windows.
  - `NET_PRIORITY` -> `NET_PRIORITY` for overlapping Net windows.
  - `DAILY_PRIORITY` does not create Net/SOP policy rows.
- Maintain existing conflict-detection behavior and schedule-layer precedence semantics.

Constraints:
- Do not change scheduler/FreqPlanner arbitration logic or policy-table schema.
- Keep startup/radio-disconnected behavior unchanged.
- Keep writes idempotent (use existing unique key upsert semantics).

Acceptance criteria:
- Choosing SOP priority in SOP Builder for a row that overlaps a Net window produces active matching rows in `sop_net_conflict_policy`.
- FreqPlanner shows SOP label (not Net) for overlap windows with saved SOP-priority policy.
- Scheduler source selection chooses SOP over Net for those windows.
- Existing Net Schedule and Daily Schedule policy workflows remain functional.

Verification:
- `python -m compileall freqinout/gui/sop_tab.py freqinout/core/sop_manager.py`
- `python tools/release_preflight.py`
- `python -m compileall freqinout`
- `powershell -ExecutionPolicy Bypass -File .\\tools\\freqinout-db.ps1 status`
- Manual:
  - Create/modify an SOP action that overlaps a Net row, choose `SOP Priority`, save, then verify corresponding active `sop_net_conflict_policy` rows exist and FreqPlanner reflects SOP precedence.

Rollback:
- Revert changes in:
  - `freqinout/core/sop_manager.py`
  - `freqinout/gui/sop_tab.py`
  - `SPEC.md`

### 1.85 Addendum (2026-02-22): SOP Conflict Prompt UX Streamlining (Single Save-Time Decision Pass)

Problem:
- SOP Builder currently can show repeated modal prompts for the same conflict context:
  - realtime edit-path modal prompts while typing/changing row fields
  - additional per-row modal prompts during Save/import
- This increases operator interruption, can feel loop-like, and adds freeze risk perception on conflict-heavy SOP edits.

Impacted files and failure modes:
- `freqinout/gui/sop_tab.py`
  - Failure mode: repeated modal dialogs across realtime + save paths for one logical save action.
  - Failure mode: users must resolve row conflicts one-at-a-time with duplicated message flows.

Scope:
- Keep realtime conflict checks inline-only (conflict badge and tooltip refresh), without launching modal policy dialogs while editing.
- Replace save/import per-row conflict modal loop with one consolidated conflict-resolution dialog covering all conflicting HF action rows.
- Preserve existing conflict policy semantics:
  - `SOP Priority` can keep Net/Daily overlap.
  - `Net`/`Daily` priority cannot keep first-occurrence conflict.
  - SOP-vs-SOP overlap must still be resolved before save.
- Keep current save blocking behavior when unresolved blocking conflicts remain.

Constraints:
- No schema changes.
- No scheduler/FreqPlanner arbitration logic changes.
- Keep behavior stable when no conflicts are present.

Acceptance criteria:
- Editing HF SOP rows shows inline conflict status only; no realtime modal conflict prompt.
- Save/import presents a single consolidated conflict dialog when one or more HF rows conflict.
- User can choose per-row policy and start-time adjustments in one pass.
- Save/import is blocked until all blocking conflicts are resolved or user cancels.

Verification:
- `python -m compileall freqinout/gui/sop_tab.py`
- `python tools/release_preflight.py`
- `python -m compileall freqinout`
- Manual:
  - edit conflicting HF rows and confirm no realtime modal prompts appear.
  - click Save and confirm one consolidated conflict dialog appears.
  - confirm unresolved blocking conflicts are summarized in one warning and can be corrected without dialog stacking.

Rollback:
- Revert changes in:
  - `freqinout/gui/sop_tab.py`
  - `SPEC.md`

### 1.86 Addendum (2026-02-22): Inactive HF SOP Should Bypass Conflict Prompts and Planner SOP Fallback

Problem:
- Saving/importing an HF SOP profile as inactive can still trigger HF conflict-priority resolution prompts, even though inactive SOP actions are not scheduled for runtime use.
- FreqPlanner currently falls back to showing SOP layer rows even when no HF SOP profile is active, which can keep SOP entries visible after deactivation and conflict with operator expectation.

Impacted files and failure modes:
- `freqinout/gui/sop_tab.py`
  - Failure mode: unnecessary conflict-policy prompts during inactive HF SOP save/import.
- `freqinout/gui/freq_planner_tab.py`
  - Failure mode: SOP rows remain visible due to inactive-profile fallback query path.
- `freqinout/gui/main_window.py`
  - Risk: planner refresh path on SOP data change not explicitly invoked when loaded.

Scope:
- Skip HF conflict-resolution dialog flow when the target HF SOP profile is inactive on Save/import.
- Make FreqPlanner load SOP layer rows from active SOP profiles only (remove inactive fallback path).
- Trigger loaded FreqPlanner refresh on SOP data change dispatch in main window.

Constraints:
- No scheduler arbitration logic changes.
- No DB schema changes.
- Preserve existing behavior for active HF SOP save/import conflict handling.

Acceptance criteria:
- Saving or importing HF SOP with `Active` unchecked does not show conflict-priority prompts.
- After saving HF SOP inactive, FreqPlanner shows standard Daily/Net results without SOP layer overlays.
- Loaded FreqPlanner view refreshes promptly when SOP data changes.

Verification:
- `python -m compileall freqinout/gui/sop_tab.py freqinout/gui/freq_planner_tab.py freqinout/gui/main_window.py`
- `python tools/release_preflight.py`
- `python -m compileall freqinout`
- Manual:
  - Set HF SOP inactive and Save; confirm no conflict dialog.
  - Confirm FreqPlanner no longer displays SOP rows when all HF SOP profiles are inactive.

Rollback:
- Revert changes in:
  - `freqinout/gui/sop_tab.py`
  - `freqinout/gui/freq_planner_tab.py`
  - `freqinout/gui/main_window.py`
  - `SPEC.md`

### 1.87 Addendum (2026-02-22): Schedule Status Resume Button Highlight Parity with ControlFreq

Problem:
- In the main sidebar `Schedule Status` panel, `Resume Schedule` is visible during off-schedule state but styled as muted, making it appear inactive.
- In `ControlFreq`, the equivalent resume action is highlighted (`info`) when off-schedule mismatch is present.

Impacted files and failure modes:
- `freqinout/gui/main_window.py`
  - Failure mode: off-schedule primary corrective action is visually de-emphasized.

Scope:
- Align main-panel off-schedule `Resume Schedule` button styling with `ControlFreq` resume style by using the same highlighted role.
- Keep off-schedule detection logic and button visibility behavior unchanged.

Acceptance criteria:
- When scheduler status is off-schedule, sidebar `Resume Schedule` button is styled as highlighted (`info`) rather than muted.
- No behavior changes to suspend/resume actions or scheduler state transitions.

Verification:
- `python -m compileall freqinout/gui/main_window.py`
- Manual:
  - force off-schedule state and verify sidebar `Resume Schedule` is visibly highlighted consistent with `ControlFreq`.

Rollback:
- Revert changes in:
  - `freqinout/gui/main_window.py`
  - `SPEC.md`

### 1.88 Addendum (2026-02-22): ControlFreq Propagation View First-Open Refresh

Problem:
- Switching ControlFreq view preset/chips to show `Propagation` can display an empty propagation table until a target filter control is manually changed.
- Root cause: view-change handlers update visibility only; they do not trigger a propagation target/snapshot refresh when propagation becomes visible.

Impacted files and failure modes:
- `freqinout/gui/controlfreq_tab.py`
  - Failure mode: propagation card appears blank on first show after being hidden.

Scope:
- Detect view-card transitions and trigger immediate propagation refresh when `propagation` changes from hidden to visible.
- Keep existing periodic/deferred refresh scheduling unchanged.

Acceptance criteria:
- Switching to `Propagation` view populates target defaults and forecast rows without requiring manual target-type toggle.
- Existing propagation target change behavior remains unchanged.

Verification:
- `python -m compileall freqinout/gui/controlfreq_tab.py`
- Manual:
  - set view to non-propagation preset, then switch to `Propagation`, verify table populates on first show.

Rollback:
- Revert changes in:
  - `freqinout/gui/controlfreq_tab.py`
  - `SPEC.md`

### 1.89 Addendum (2026-02-22): Reduce Startup Flash From Qt WebEngine Warmup

Problem:
- Windows EXE launch can show several brief console-like/helper window flashes shortly after the main window opens.
- A likely contributor is startup WebEngine warmup creating and showing a hidden `QWebEngineView`, which intentionally forces early native surface/process initialization.

Impacted files and failure modes:
- `freqinout/gui/main_window.py`
  - Failure mode: visible helper-window flashes during startup on some Windows systems.
  - Regression risk if changed incorrectly: slower first `Map` activation or WebEngine warmup no longer completes.

Scope:
- Keep early WebEngine warmup, but make it less aggressive by warming WebEngine with a page-only preload (no `QWebEngineView.show()` call).
- Preserve existing lazy tab behavior and warmup cleanup/timeout safeguards.

Acceptance criteria:
- App startup no longer forces a hidden `QWebEngineView.show()` path during warmup.
- WebEngine warmup still marks complete and does not break `Map` tab loading.
- Startup remains stable when Qt WebEngine is unavailable.

Verification:
- `python -m compileall freqinout/gui/main_window.py`
- `python tools/release_preflight.py`
- Manual:
  - launch Windows EXE and confirm startup flashes are reduced/absent
  - open `Map` tab and confirm map still loads

Rollback:
- Revert changes in:
  - `freqinout/gui/main_window.py`
  - `SPEC.md`

### 1.90 Addendum (2026-02-22): Defer Map WebView Construction Until First Map Activation

Problem:
- Windows EXE startup still shows multiple brief helper-window flashes after reducing WebEngine warmup aggressiveness.
- `Map` tab shell is constructed eagerly at startup and `StationsMapTab` currently creates `QWebEngineView` during `__init__`, which can still trigger visible Qt WebEngine helper process/window startup.

Impacted files and failure modes:
- `freqinout/gui/stations_map_tab.py`
  - Failure mode: startup flashes caused by eager `QWebEngineView` construction.
  - Regression risk: first `Map` activation may fail to render if deferred webview creation is not correctly hooked into visible-init render flow.

Scope:
- Keep `Map` tab shell and controls constructed at startup.
- Defer `QWebEngineView` creation until `Map` becomes visible for the first time.
- Preserve existing loading placeholder behavior and render pipeline.

Acceptance criteria:
- App startup does not create `QWebEngineView` in `StationsMapTab.__init__`.
- First `Map` activation creates the webview and renders the map using existing loading state.
- Existing non-Map tabs remain unaffected.

Verification:
- `python -m compileall freqinout/gui/stations_map_tab.py`
- `python tools/release_preflight.py`
- Manual:
  - launch EXE and confirm startup flashing is further reduced
  - open `Map` tab once and confirm map loads

Rollback:
- Revert changes in:
  - `freqinout/gui/stations_map_tab.py`
  - `SPEC.md`

### 1.91 Addendum (2026-02-22): Disable Startup WebEngine Prewarm on Windows by Default

Problem:
- Users still observe multiple brief flashing helper windows immediately after launching the Windows EXE, before clicking any tabs.
- After deferring `Map` webview construction, the remaining startup-time WebEngine initialization path is `MainWindow._prewarm_webengine()` (startup warmup), which creates a `QWebEnginePage` and triggers Qt WebEngine helper process startup.

Impacted files and failure modes:
- `freqinout/gui/main_window.py`
  - Failure mode: visible startup flashes on Windows caused by early WebEngine process initialization.
  - Tradeoff: first `Map` activation may incur one-time WebEngine startup cost/flash when startup prewarm is disabled.

Scope:
- Disable startup WebEngine prewarm by default on Windows.
- Keep the warmup code available and allow hidden settings override for users who prefer first-Map-open smoothness over startup quietness.
- Leave non-Windows behavior unchanged by default.

Acceptance criteria:
- Windows startup path no longer calls `_prewarm_webengine()` unless explicitly enabled via setting.
- `Map` tab continues to load on first activation (using deferred webview creation path).
- Existing startup behavior on non-Windows platforms remains unchanged by default.

Verification:
- `python -m compileall freqinout/gui/main_window.py`
- `python tools/release_preflight.py`
- Manual:
  - launch Windows EXE and confirm startup flashes are absent/reduced before any tab click
  - open `Map` tab and confirm map loads

Rollback:
- Revert changes in:
  - `freqinout/gui/main_window.py`
  - `SPEC.md`

### 1.92 Addendum (2026-02-22): Suppress Startup GnuPG Console Flashes From Hidden Messages Prewarm

Problem:
- User still sees multiple brief flashing windows at EXE startup and observed a `GNU` header.
- Root cause is not the Settings page being opened; it is `Messages` tab lazy-prewarm at startup creating `MessageViewerTab`, which runs initial signature/hash verification for FLAMP auth files and invokes `gpg.exe`.
- `freqinout.core.gpg_tools._run_gpg()` uses plain `subprocess.run(...)` on Windows without hidden-window flags, allowing `gpg.exe` console windows to flash.

Impacted files and failure modes:
- `freqinout/gui/message_viewer_tab.py`
  - Failure mode: hidden startup prewarm triggers auth verification (GPG calls) before the Messages tab is shown.
- `freqinout/core/gpg_tools.py`
  - Failure mode: GPG subprocess console window can flash on Windows whenever GPG is invoked.

Scope:
- Defer Messages auth verification until the Messages tab becomes active (visible), while preserving hidden lazy-prewarm of file/message caches.
- Run GPG subprocesses on Windows with hidden/no-window startup flags.

Acceptance criteria:
- Startup lazy-prewarm of Messages does not launch GPG verification while the tab remains hidden.
- GPG operations (startup-adjacent or manual) do not show console flashes on Windows.
- Message auth verification still runs when Messages tab becomes active.

Verification:
- `python -m compileall freqinout/gui/message_viewer_tab.py freqinout/core/gpg_tools.py`
- `python tools/release_preflight.py`
- Manual:
  - launch EXE with auth verification enabled and FLAMP files present; confirm no GNU/GPG console flashes before any tab click
  - open `Messages` tab and confirm auth statuses still populate

Rollback:
- Revert changes in:
  - `freqinout/gui/message_viewer_tab.py`
  - `freqinout/core/gpg_tools.py`
  - `SPEC.md`

### 1.93 Addendum (2026-02-22): On-Demand Map WebEngine Prewarm Before First Map Switch

Problem:
- After disabling startup WebEngine prewarm on Windows (to reduce launch flashing), first click on `Map` can again appear like the main window closes/reopens due to one-time Qt WebEngine startup during visible tab switch.

Impacted files and failure modes:
- `freqinout/gui/main_window.py`
  - Failure mode: first Map click triggers visible one-time WebEngine startup on the interactive path.
- `freqinout/gui/stations_map_tab.py`
  - Failure mode: first visible Map render creates WebEngine view during tab-visible transition.

Scope:
- On first `Map` navigation request, if WebEngine warmup has not yet completed, keep the current tab visible and run page-only WebEngine prewarm on-demand.
- After warmup completes, optionally create the hidden Map webview before switching tabs, then proceed with the requested Map tab switch.
- Keep startup WebEngine prewarm disabled on Windows by default.

Acceptance criteria:
- First `Map` click no longer immediately switches tabs while one-time WebEngine startup is still pending.
- App remains on the current tab during one-time on-demand prewarm, then switches to `Map` once prewarm completes.
- Startup remains quiet (no restored startup WebEngine prewarm on Windows).

Verification:
- `python -m compileall freqinout/gui/main_window.py freqinout/gui/stations_map_tab.py`
- `python tools/release_preflight.py`
- Manual:
  - launch EXE and confirm startup flashing remains reduced
  - click `Map` once and confirm transition feels stable (current tab remains visible until map is ready to switch)
  - verify `Map` still loads normally

Rollback:
- Revert changes in:
  - `freqinout/gui/main_window.py`
  - `freqinout/gui/stations_map_tab.py`
  - `SPEC.md`

### 1.94 Addendum (2026-02-22): Favor Stable First Map Switch Over Windows Startup Warmup Minimalism

Problem:
- After moving WebEngine warmup off startup, first `Map` activation on Windows can again present a close/reopen-style main-window flash.
- User priority is stable main-window behavior during first `Map` click, even if the app shows `Loading map...` while the map becomes ready.

Impacted files and failure modes:
- `freqinout/gui/main_window.py`
  - Failure mode: one-time WebEngine native view initialization occurs on visible first `Map` activation path.

Scope:
- Restore hidden `QWebEngineView` startup warmup (release-style behavior) so Qt WebEngine native view/process startup occurs before first user `Map` click.
- Keep GPG popup suppression and deferred hidden Messages auth verification fixes in place (to avoid unrelated startup console flashes).
- Retain hidden settings override for startup WebEngine prewarm if troubleshooting is needed.

Acceptance criteria:
- First `Map` click no longer triggers close/reopen-style main-window flash in normal operation.
- App startup no longer shows multiple GnuPG console flashes (from prior fix).
- `Map` still uses loading placeholder and renders normally.

Verification:
- `python -m compileall freqinout/gui/main_window.py`
- `python tools/release_preflight.py`
- Manual:
  - launch EXE and confirm startup remains acceptable (no GNU/GPG console flashes)
  - click `Map` once and confirm main window remains visually stable

Rollback:
- Revert changes in:
  - `freqinout/gui/main_window.py`
  - `SPEC.md`

### 1.95 Addendum (2026-02-22): Messages `+BBS` Session-Disable After Successful Copy

Problem:
- In `Messages`, `+BBS` remains active for a file row after that exact source file has already been copied to the VarAC BBS folder.
- This makes it easy to repeat-copy the same source row accidentally, while users still need overwrite validation for a different duplicate file row with the same destination filename.

Impacted files and failure modes:
- `freqinout/gui/message_viewer_tab.py`
  - Failure mode: repeated clicks on the same source row trigger unnecessary copy/overwrite prompts.
  - Regression risk: disabling based on destination filename would block valid duplicate-file overwrite workflows.

Scope:
- Track successful `+BBS` copies in session memory only using exact source-row identity (`path`, `mtime`, `size`).
- Render `+BBS` as visible but muted/disabled for already-copied source rows, and suppress click action for that row.
- Preserve existing overwrite prompt when a different row (including same filename) is copied to an existing BBS destination.

Acceptance criteria:
- After successful copy, the same source row shows disabled/muted `+BBS` for the rest of the session.
- Clicking disabled `+BBS` does nothing and shows no extra popup.
- A different duplicate row with the same filename still shows active `+BBS` and can trigger the existing overwrite prompt.
- Disabled state resets on app restart.

Verification:
- `python -m compileall freqinout/gui/message_viewer_tab.py`
- `python tools/release_preflight.py`
- Manual:
  - copy a row to BBS, confirm `+BBS` becomes muted/disabled for that row
  - receive/select duplicate filename row, confirm `+BBS` remains active and overwrite prompt still appears

Rollback:
- Revert changes in:
  - `freqinout/gui/message_viewer_tab.py`
  - `SPEC.md`

### 1.96 Addendum (2026-02-22): Versioned Checksum Asset Name in Release Publish Helper

Problem:
- `tools/publish-release.ps1` writes a generic `SHA256SUMS.txt` file and uploads it as a release asset.
- Reusing the same checksum filename across releases can cause confusion when users compare repo files/assets and see stale checksum content from prior releases.

Impacted files and failure modes:
- `tools/publish-release.ps1`
  - Failure mode: ambiguous checksum asset naming across releases (`SHA256SUMS.txt` reused).

Scope:
- Generate checksum assets using the release tag in the filename (for example `SHA256SUMS-v1.1.8.txt`).
- Keep existing release helper flow and `gh release create/upload --clobber` behavior unchanged otherwise.

Acceptance criteria:
- Running `tools/publish-release.ps1 -Version 1.1.8` generates `SHA256SUMS-v1.1.8.txt`.
- The versioned checksum file is uploaded to the release instead of a generic `SHA256SUMS.txt`.
- Existing installer asset upload behavior remains unchanged.

Verification:
- PowerShell parse/syntax check for `tools/publish-release.ps1`
- `python tools/release_preflight.py`
- `python -m compileall freqinout`

Rollback:
- Revert changes in:
  - `tools/publish-release.ps1`
  - `SPEC.md`

### 1.97 Addendum (2026-02-23): Linux Station Benchmark Capture + Summary Tooling

Problem:
- Performance tuning decisions for FreqInOut (FIO) in a live Linux radio station environment are currently based on ad hoc observations (`top`/`htop`, screenshots, short notes), which makes runs hard to compare and can obscure whether contention comes from FIO or companion radio software.
- Operators need a single repeatable command that captures low-overhead system/process telemetry during real operating conditions and a summary tool that highlights where FIO competes for CPU/memory/disk with the station stack.

Impacted files and failure modes:
- `tools/linux_fio_bench_capture.sh`
  - Failure mode: collector overhead or missing-tool hard failures disrupt live ops or reduce capture reliability.
  - Failure mode: orphaned background collectors (`pidstat`/`sar`/`iostat`/`vmstat`) continue running after the session ends.
  - Failure mode: brittle process matching misses VarAC (Wine) or CommStatOne aliases and yields misleading data.
- `tools/linux_fio_bench_summary.py`
  - Failure mode: parser misreads `pidstat` output and reports incorrect FIO/peer-app utilization, leading to wrong optimization priorities.
- `tools/linux_fio_bench_process_patterns.tsv`
  - Failure mode: over-broad regex patterns incorrectly classify unrelated processes (false positives) or miss target apps (false negatives).
- `docs/tools-and-scripts.md`
  - Failure mode: incomplete usage guidance causes unsafe capture settings or inconsistent runs.

Scope:
- Add a Linux single-command benchmark capture script that:
  - records UTC start/end metadata and system snapshots,
  - samples low-overhead telemetry using available tools (`pidstat`, `sar`, `iostat`, `vmstat`),
  - stores start/end process snapshots and a copied pattern list used for process grouping,
  - stops collectors cleanly on normal exit or interrupt and avoids leaving orphan collectors,
  - optionally runs a local summary step after capture.
- Add a default process-pattern config covering:
  - `FIO`
  - `FLRig`, `FLDigi`, `FLAmp`, `FLMsg`
  - `VarAC` (Wine command line / `VarAC.exe` path matching)
  - `JS8Call`, `JS8Spotter`
  - `CommStatOne` via `commstat` or `littlegucci` process aliases
- Add a Python summary tool that reads the capture folder and outputs human-readable + machine-readable summaries of:
  - per-app aggregate CPU/memory/I/O/context-switch utilization from `pidstat`,
  - top commands within each app group,
  - key `vmstat` and `iostat` indicators for contention clues.
- Keep FIO runtime behavior unchanged (tooling only).

Acceptance criteria:
- Running `bash tools/linux_fio_bench_capture.sh --duration 10 --interval 1` on Linux creates a timestamped capture directory with:
  - manifest/metadata,
  - start/end process snapshots,
  - copied process pattern file,
  - collector logs for tools available on that host.
- If one or more collector commands are missing, the capture script completes and records warnings instead of hard-failing.
- Interrupting the capture (Ctrl+C) still writes end snapshots and stops collector subprocesses.
- `python tools/linux_fio_bench_summary.py <capture_dir>` produces text, JSON, and Markdown summaries.
- Default process grouping matches VarAC Wine command lines and CommStatOne aliases (`commstat`, `littlegucci`) when present in `pidstat` command lines.

Verification:
- `python tools/release_preflight.py`
- `python -m compileall freqinout tools`
- `python tools/linux_fio_bench_summary.py --help`
- Shell syntax check (on a Linux/bash-capable environment): `bash -n tools/linux_fio_bench_capture.sh`

Rollback:
- Revert changes in:
  - `tools/linux_fio_bench_capture.sh`
  - `tools/linux_fio_bench_summary.py`
  - `tools/linux_fio_bench_process_patterns.tsv`
  - `docs/tools-and-scripts.md`
  - `SPEC.md`

### 1.14 Addendum (2026-02-23): ControlFreq Busy Reason on QSY Button

Problem:
- In `ControlFreq` Frequency Control, the schedule-state badge (`On Schedule`) is currently repurposed to show `Blocked` when traffic/PTT activity is present.
- Operators need the schedule-state indicator to remain schedule-focused, while the QSY action itself should communicate why it is temporarily unavailable.

Impacted files and failure modes:
- `freqinout/gui/controlfreq_tab.py`
  - Failure mode: busy-state text can mask `Resume Schedule`/`QSY Now` and fail to restore when activity clears.
  - Failure mode: busy reason precedence is inconsistent when multiple sources are active simultaneously.
  - Failure mode: button state changes could regress existing QSY/Resume action availability logic.

Scope:
- Keep the frequency state badge focused on schedule state (`On Schedule`, `Off Schedule`, `Unknown`).
- Move busy indication to the primary Frequency Control action button:
  - show `Busy: {reason}` while activity blocks frequency changes.
  - use reasons: `JS8Call`, `VarAC`, `FLDigi`, `PTT active`.
- Disable the button while busy is active.
- Preserve existing QSY/Resume mode selection logic and restore it when busy clears.
- Use deterministic reason precedence when multiple busy flags are true:
  - `PTT active` > `JS8Call` > `VarAC` > `FLDigi`.

Acceptance criteria:
- During RX/TX/PTT busy conditions, the Frequency Control action button displays `Busy: {reason}` and is disabled.
- The schedule badge no longer changes to `Blocked`; it continues to reflect schedule state only.
- When the busy condition clears, the action button returns to the correct prior state (`QSY Now`, `Resume Schedule`, or disabled `QSY Now`).
- Existing QSY and Resume shortcuts/actions remain unchanged when not busy.

Verification:
- `python -m compileall freqinout`
- Manual UI check in `ControlFreq`:
  - Trigger busy condition and confirm button text/disable state.
  - Clear busy condition and confirm correct button state restoration.

Rollback:
- Revert `ControlFreq` busy-indicator UI changes in `freqinout/gui/controlfreq_tab.py`.

### 1.15 Addendum (2026-02-23): FLDigi Mode vs Offset Off-Schedule Enforcement Split

Problem:
- FLDigi offset drift is currently being classified as a FLDigi `mode` mismatch in scheduler off-schedule flags.
- This makes FLDigi offset drift use the stricter FLDigi mode prompt/enforcement path and hides the distinction between `FLDigi Mode` and `FLDigi Offset` in prompt notifications.
- Operators need offset drift to remain visible as off-schedule while preserving distinct notifications for mode vs offset conditions.

Impacted files and failure modes:
- `freqinout/core/scheduler_engine.py`
  - Failure mode: FLDigi offset drift incorrectly sets the `mode` off-schedule flag, causing prompt labels/handling to treat offset-only drift as mode mismatch.
  - Failure mode: prompt suppression/force-apply logic only checks FLDigi `mode`, so separated FLDigi offset flags could bypass prompt gating if not updated.
- `freqinout/gui/main_window.py`
  - Failure mode: scheduler status panel reason text may stop showing `FLDigi Offset` if it still keys only on the `mode` flag.

Scope:
- Keep FLDigi off-schedule detection split into distinct conditions:
  - `FLDigi Mode`
  - `FLDigi Offset`
- Preserve off-schedule notification for FLDigi offset drift.
- Route FLDigi mode/offset prompt behavior through the same FLDigi enforcement setting (`On Schedule Change` / `Prompt`) while presenting distinct prompt items.
- Keep JS8 offset (`Offset`) behavior unchanged.

Acceptance criteria:
- FLDigi offset-only drift sets off-schedule status without forcing FLDigi `mode` mismatch classification.
- Off-schedule prompt payloads can distinguish `FLDigi Mode` and `FLDigi Offset` when applicable.
- In `Prompt` mode, FLDigi mode/offset prompt gating applies to either FLDigi mismatch type.
- Sidebar Schedule Status reasons can show `FLDigi Mode` and `FLDigi Offset` distinctly.

Verification:
- `python -m compileall freqinout`
- `python tools/release_preflight.py`
- Manual check:
  - induce FLDigi offset-only drift and confirm off-schedule notification labels `FLDigi Offset` (not `FLDigi Mode`)
  - induce FLDigi mode-only drift and confirm `FLDigi Mode`
  - induce both and confirm distinct notification items

Rollback:
- Revert FLDigi off-schedule flag/prompt split changes in:
  - `freqinout/core/scheduler_engine.py`
  - `freqinout/gui/main_window.py`

### 1.16 Addendum (2026-02-23): Resume Schedule UI/Correction Responsiveness Hardening

Problem:
- Users report `Resume Schedule` feels slow in both:
  - `ControlFreq` Frequency Control (`Resume Schedule` action state)
  - left sidebar `Schedule Status` panel (`Resume Schedule` button/display)
- Symptoms include delayed visual state updates and slow convergence of correction display after resume.

Impacted files and failure modes:
- `freqinout/gui/controlfreq_tab.py`
  - Failure mode: duplicate post-resume refresh pulses and duplicate scheduler-status calls per pulse keep the UI thread busy and delay visual convergence.
- `freqinout/gui/main_window.py`
  - Failure mode: repeated resume pulses rebuild status-reason labels/layout even when content is unchanged, causing left-sidebar lag.
- `freqinout/core/scheduler_engine.py`
  - Failure mode: `get_status_summary()` performs duplicate FLDigi mode/offset backend reads per call, increasing latency for every status refresh during resume.

Scope:
- Preserve resume behavior and scheduler semantics.
- Reduce duplicate work on resume:
  - fewer post-resume UI pulses,
  - remove duplicate `ControlFreq` scheduler-strip refresh calls,
  - cache/skip sidebar reason-label rebuilds when text is unchanged.
- In `ControlFreq`, reuse one scheduler status snapshot across the frequency-control refresh path.
- In scheduler status summary, avoid duplicate FLDigi mode/offset reads when off-schedule flags already provide the same state.

Acceptance criteria:
- Resume UI state updates are visibly more responsive in `ControlFreq` and sidebar `Schedule Status`.
- No change to scheduler correctness or prompt behavior.
- ControlFreq and sidebar status still converge after asynchronous rig/apply completion.

Verification:
- `python -m compileall freqinout`
- `python tools/release_preflight.py`
- Manual checks:
  - click `Resume Schedule` from sidebar and confirm quick display update + correction convergence
  - click `Resume Schedule` in `ControlFreq` and confirm quick button/state update without UI lag spikes

Rollback:
- Revert performance hardening edits in:
  - `freqinout/gui/controlfreq_tab.py`
  - `freqinout/gui/main_window.py`
  - `freqinout/core/scheduler_engine.py`

### 1.17 Addendum (2026-02-23): FLDigi Offset Prompt Gating Regression After Mode/Offset Split

Problem:
- After splitting FLDigi off-schedule notifications into distinct `FLDigi Mode` and `FLDigi Offset`, FLDigi offset drift is again being corrected immediately in cases that should notify first (prompt path) instead of waiting for user action.
- Resume action responsiveness improvements should remain intact and not change scheduler correction semantics.

Impacted files and failure modes:
- `freqinout/core/scheduler_engine.py`
  - Failure mode: FLDigi prompt gating compares an entry key shape that does not match `_last_entry_key`, preventing prompt-mode force-apply state from clearing and allowing unintended immediate FLDigi apply.
  - Failure mode: `fldigi_offset` prompt state is not included in prompt-next-due calculations, making status timing hints inconsistent after the mode/offset split.

Scope:
- Preserve resume-performance hardening changes.
- Restore notify-first behavior for FLDigi offset drift in prompt-driven FLDigi enforcement cases.
- Keep explicit `Resume Schedule` action semantics unchanged.
- Ensure prompt timing/status calculations include `fldigi_offset` alongside other prompt-managed drift types.

Acceptance criteria:
- FLDigi offset drift under FLDigi `Prompt` mode raises notification/prompt instead of immediate correction.
- `Resume Schedule` remains responsive and still converges correctly.
- Prompt timing/status paths remain coherent when only `fldigi_offset` is off-schedule.

Verification:
- `python -m compileall freqinout`
- `python tools/release_preflight.py`
- Manual check:
  - With FLDigi enforcement set to `Prompt`, create FLDigi offset-only drift and confirm prompt notification appears before correction.
  - Confirm `Resume Schedule` still updates UI quickly and applies when explicitly requested.

Rollback:
- Revert scheduler FLDigi prompt-gating/timing changes in `freqinout/core/scheduler_engine.py`.

### 1.18 Addendum (2026-02-23): FLDigi Offset Resume/Re-Apply Regression (Prompt + On Schedule Change)

Problem:
- FLDigi offset drift can still be immediately re-applied after `Resume Schedule`, even when the current schedule entry has not changed and drift should remain notify-only.
- Prompt-mode FLDigi gating still fails in some cases because the FLDigi prompt-suppression compare does not match the scheduler's full `_last_entry_key` shape when JS8 offset is included.

Impacted files and failure modes:
- `freqinout/core/scheduler_engine.py`
  - Failure mode: `resume_schedule()` force-applies FLDigi for the already-current entry, re-arming FLDigi apply and immediately correcting offset drift.
  - Failure mode: `_maybe_apply_fldigi()` FLDigi prompt-gating compares an entry key that omits the JS8 offset tuple element, so `_fldigi_force_apply_once` may not clear after a same-entry apply.

Scope:
- Preserve resume responsiveness improvements.
- Keep frequency resume behavior intact.
- Prevent same-entry resume from immediately re-applying FLDigi mode/offset.
- Make FLDigi prompt-gating key comparisons match scheduler apply-key structure.

Acceptance criteria:
- With FLDigi offset-only drift on the current entry, `Resume Schedule` does not immediately force FLDigi offset correction.
- `Prompt` mode shows notify/prompt first for FLDigi offset drift after resume.
- `On Schedule Change` mode leaves same-entry offset drift as off-schedule and does not auto-correct until a real schedule entry change.

Verification:
- `python -m compileall freqinout/core/scheduler_engine.py`
- `python tools/release_preflight.py`

Rollback:
- Revert resume FLDigi-skip and FLDigi prompt-gating key updates in `freqinout/core/scheduler_engine.py`.

### 1.19 Addendum (2026-02-23): FLDigi Offset Drift Re-Queued On Same-Entry Retries

Problem:
- FLDigi offset drift can still be immediately forced back after manual/signal adjustment because same-entry scheduler reapply paths (resume/retry/frequency-only apply flows) re-queue FLDigi apply even when the active schedule row has not changed.

Impacted files and failure modes:
- `freqinout/core/scheduler_engine.py`
  - Failure mode: `_update_desired_fldigi_settings()` always sets `_fldigi_apply_pending=True`, causing same-entry forced reapply paths to re-enforce FLDigi mode/offset.
  - Failure mode: FLDigi same-entry checks rely on full `_last_entry_key` equality, which fails when the last action intentionally skipped FLDigi/JS8 offset fields (e.g., frequency-only apply).

Scope:
- Preserve on-schedule-change FLDigi enforcement for real schedule transitions.
- Allow FLDigi offset drift to persist (notify off-schedule) until prompt/apply or an actual schedule entry change.

Acceptance criteria:
- Manual or signal-driven FLDigi offset drift is not immediately forced back while remaining on the same active schedule row.
- FLDigi prompt/on-schedule-change resolution still works on real schedule transitions or explicit user apply.

Verification:
- `python -m compileall freqinout/core/scheduler_engine.py`
- `python tools/release_preflight.py`

Rollback:
- Revert FLDigi same-entry apply re-queue guards in `freqinout/core/scheduler_engine.py`.

### 1.20 Addendum (2026-02-24): FLDigi Offset Enforcement Should Key Off Real Scheduler Changes (Not Internal Reapply Keys)

Problem:
- FLDigi offset drift is correctly allowed during active net operation, but still gets immediately forced outside nets because FLDigi "on schedule change" force-apply logic is triggered by `_last_entry_key` mismatches that can occur on same-row resume/retry/frequency-only paths.
- In `Prompt` mode, a valid FLDigi offset prompt may still be followed by immediate correction after `Resume Schedule` because resume/frequency-only actions perturb the internal entry key and re-arm FLDigi force-apply.
- Prompt handling uses boolean off-schedule flags only, so a new manual FLDigi offset drift value while still off-schedule is not treated as a new prompt cycle.

Impacted files and failure modes:
- `freqinout/core/scheduler_engine.py`
  - Failure mode: `_fldigi_force_apply_once` is re-armed on same active schedule row due to internal entry-key field differences (`apply_fldigi=False`, JS8 tuple slot differences), causing unintended FLDigi offset correction.
  - Failure mode: FLDigi offset prompt cycle detection is boolean-only and does not recognize changed drift values as a new event.

Scope:
- Preserve active net/manual-net suppression behavior.
- Preserve real schedule-transition enforcement (`On Schedule Change`) and explicit prompt apply behavior.
- Stop same-row resume/retry/reapply paths from re-arming FLDigi offset correction.
- Treat changed FLDigi offset mismatch values as a new prompt cycle in `Prompt` mode.

Acceptance criteria:
- Active net behavior remains unchanged (FLDigi offset drift allowed while net/manual-net suppression is active).
- `On Schedule Change`: FLDigi offset is enforced on actual scheduler row changes, not same-row manual/signal drift.
- `Prompt`: FLDigi offset prompts notify first and do not auto-correct after `Resume Schedule` on the same row.
- `Prompt`: changing FLDigi offset to a different off-schedule value starts a new prompt cycle (without waiting for the previous offset-drift timer interval).

Verification:
- `python -m compileall freqinout`
- `python tools/release_preflight.py`
- Manual checks:
  - Active net: drift FLDigi offset and confirm no snap-back.
  - Same row (HF/SOP/NET) + `On Schedule Change`: drift FLDigi offset and confirm no snap-back until a real scheduler row transition.
  - `Prompt`: drift offset, confirm prompt, click ignore/resume, drift to a new offset, confirm new prompt cycle without immediate correction.

Rollback:
- Revert scheduler transition-keying and FLDigi offset prompt-cycle changes in `freqinout/core/scheduler_engine.py`.

### 1.21 Addendum (2026-02-24): ControlFreq Status Row Layout + Schedule-Card QSY Hero Sync

Problem:
- In `ControlFreq`, the `Operating Status` LED container does not expand cleanly to fit all status indicators/labels in the top row.
- Clicking `QSY Now` from the `Schedule Outlook` card can complete the QSY but leave the Frequency Control hero indicator/combo showing the previous selection because the refresh path restores the prior combo selection instead of forcing a hero resync to the active frequency.

Impacted files and failure modes:
- `freqinout/gui/controlfreq_tab.py`
  - Failure mode: top-row layout spacer allocation starves the status-group container width even when horizontal space is available.
  - Failure mode: schedule-card QSY refresh does not set `_force_hero_resync`, so `_refresh_frequency_control()` restores the previous combo selection and the hero display can remain stale.

Scope:
- Preserve existing top-row content and right-side clock layout.
- Preserve schedule-card QSY behavior and only fix post-click hero display synchronization.

Acceptance criteria:
- `Operating Status` group can use available width in the top row and no longer clips/truncates due to avoidable spacer allocation.
- Clicking `QSY Now` in `Schedule Outlook` updates the Frequency Control hero indicator to reflect the active frequency after the QSY completes.

Verification:
- `python -m compileall freqinout/gui/controlfreq_tab.py`
- `python tools/release_preflight.py`
- Manual checks:
  - Open `ControlFreq` at normal window width and confirm `Operating Status` row expands appropriately.
  - Click `QSY Now` in `Schedule Outlook` and confirm the hero indicator/combo reflects the active frequency.

Rollback:
- Revert `ControlFreq` top-row layout and schedule-card QSY hero resync edits in `freqinout/gui/controlfreq_tab.py`.

### 1.22 Addendum (2026-02-24): Configurable JS8Call TCP Hostname in Settings

Problem:
- JS8Call supports a configurable TCP server hostname (`TCPServer`) with default `127.0.0.1`, but FreqInOut settings only exposes the JS8 TCP port.
- Several FreqInOut JS8 integration paths still assume localhost, so users running JS8Call on a custom host/IP cannot reliably use JS8-backed features.

Impacted files and failure modes:
- `freqinout/gui/settings_tab.py`
  - Failure mode: no UI field to persist `js8_host`, preventing user override.
- `freqinout/gui/main_window.py`, `freqinout/core/software_status_service.py`, `freqinout/radio_interface/js8_status.py`, and JS8-consuming tabs
  - Failure mode: JS8 status/control connections use default localhost instead of configured host, causing false disconnected/busy status or failed JS8 control for remote/custom-IP setups.

Scope:
- Add a `JS8 Host` (TCP hostname/IP) field under `Settings -> JS8Call Settings`.
- Default to `127.0.0.1` for backward compatibility.
- Use configured `js8_host` across JS8 status/control and JS8 message/map/net-control readers.
- Preserve existing port behavior and legacy fallback behavior if settings are missing.

Acceptance criteria:
- New installs/users with no setting continue using `127.0.0.1`.
- Users can set a custom hostname/IP and JS8 status/control/data features use that host.
- Leaving the field blank in UI normalizes back to `127.0.0.1` (no empty-host breakage).
- Existing localhost workflows remain unchanged.

Verification:
- `python -m compileall freqinout`
- `python tools/release_preflight.py`
- Manual checks:
  - Set `JS8 Host` to `127.0.0.1` and confirm JS8 status/control behavior remains unchanged.
  - Set `JS8 Host` to a reachable custom IP/hostname and confirm status probe and JS8-backed tabs connect using the configured host.

Rollback:
- Revert JS8 host settings UI/persistence and JS8 client host wiring changes.

### 1.23 Addendum (2026-02-24): Settings `Load JS8 Traffic` In-Process Indicator

Problem:
- `Settings -> JS8Call Settings -> Load JS8 Traffic` performs a full JS8 log rebuild on the UI thread and can take noticeable time with larger logs.
- The action provides no immediate visual feedback until completion, which can look like the app is stalled.

Impacted files and failure modes:
- `freqinout/gui/settings_tab.py`
  - Failure mode: users click `Load JS8 Traffic` repeatedly or assume the app is frozen while synchronous ingest is running.

Scope:
- Add an in-process indicator near the `Load JS8 Traffic` button (indeterminate progress + status text).
- Disable the button while the rebuild is running and restore it afterward.
- Preserve the existing synchronous ingest workflow and result dialogs.

Acceptance criteria:
- Clicking `Load JS8 Traffic` immediately shows visible in-progress feedback before JS8 ingest work begins.
- The button is disabled during the operation and restored on success/failure.
- Existing success/error dialogs and ingest behavior remain unchanged.

Verification:
- `python -m compileall freqinout/gui/settings_tab.py`
- `python tools/release_preflight.py`
- Manual checks:
  - Trigger `Load JS8 Traffic` with a valid log path and confirm in-process indicator appears immediately and clears on completion.
  - Trigger error path (invalid/missing file) and confirm no stuck busy indicator.

Rollback:
- Revert `SettingsTab` JS8 load indicator UI and `_load_js8_logs()` busy-state wrapper changes.

### 1.24 Addendum (2026-02-24): Windows Taskbar Icon When Launching via `python -m`

Problem:
- When launching FreqInOut with `python -m freqinout.main` on Windows, the taskbar/button can show the default Python icon instead of the FreqInOut icon.
- This makes the app look like a generic Python process and can cause confusing taskbar grouping behavior.

Impacted files and failure modes:
- `freqinout/main.py`
  - Failure mode: Windows process uses Python's default AppUserModelID/icon identity before the Qt window is created.
- `freqinout/gui/main_window.py`
  - Failure mode: window icon uses a PNG-only path when a native `.ico` is available.

Scope:
- Set a Windows-specific explicit AppUserModelID during startup when running from Python.
- Apply the application icon on `QApplication` before creating windows/dialogs.
- Prefer `assets/FreqInOut.ico` on Windows with PNG fallback.

Acceptance criteria:
- Launching with `python -m freqinout.main` on Windows shows the FreqInOut icon in the taskbar/window button instead of the default Python icon.
- Existing packaged executable behavior remains unchanged or improves.
- Non-Windows platforms continue to run without startup errors.

Verification:
- `python -m compileall freqinout/main.py freqinout/gui/main_window.py`
- `python tools/release_preflight.py`
- Manual checks (Windows):
  - Launch via `python -m freqinout.main` and confirm taskbar icon is FreqInOut.
  - Confirm startup single-instance message dialog still appears with app icon (if triggered).

Rollback:
- Revert Windows AppUserModelID/app-icon startup initialization and icon-path preference changes.

### 1.25 Addendum (2026-02-24): HF Schedule Post-Save Auto-Sort + Strict Formatting Save Block

Problem:
- HF Schedule rows are sorted on load, but users can add/edit rows and save while the visible row order remains unsorted until a reload.
- The current HF save path allows partial saves when row time formatting is invalid, warning the user but still persisting other rows.

Impacted files and failure modes:
- `freqinout/gui/daily_schedule_tab.py`
  - Failure mode: users save successfully but the active HF table remains visually unsorted until reload.
  - Failure mode: invalid time formatting (`HH:MM`) causes rows to be skipped while other rows save, creating accidental partial saves.

Scope:
- Re-sort the HF Active Schedule table immediately after a successful save.
- Block the save entirely when formatting issues are present (instead of partial-saving).
- Preserve existing handling for non-format warnings (e.g., internal SOP overlay mapping issues) unless they prevent persistence.

Acceptance criteria:
- After successful HF Schedule save, the active table is immediately sorted by the existing time sort helper.
- Invalid row time formatting blocks the save and no HF rows are persisted.
- The tab remains in a clean (not dirty) state after a successful save + auto-sort.

Verification:
- `python -m compileall freqinout/gui/daily_schedule_tab.py`
- `python tools/release_preflight.py`
- Manual checks:
  - Add an out-of-order valid row, save, and confirm the table reorders immediately.
  - Enter an invalid time format and confirm save is blocked with no partial save.

Rollback:
- Revert HF Schedule save validation tightening and post-save auto-sort changes.

### 1.26 Addendum (2026-02-24): ControlFreq Hero Resync on Scheduler Frequency Change

Problem:
- `ControlFreq -> Frequency Control` can continue displaying a stale Hero frequency selection after an automatic scheduler-driven frequency change.
- The scheduler/radio/backend frequency may change correctly, but the Hero combo preserves the prior selection unless a manual flow sets `_force_hero_resync`.

Impacted files and failure modes:
- `freqinout/gui/controlfreq_tab.py`
  - Failure mode: `_refresh_frequency_control()` restores the previously selected combo item and never reselects the new scheduled frequency after an automatic scheduler transition, leaving the Hero display stale.

Scope:
- Detect scheduler frequency transitions during the normal ControlFreq refresh path.
- Force a Hero combo resync on those transitions.
- Prefer the new scheduled frequency for the transition refresh so stale cached "active frequency" readings do not keep the old Hero selection.

Acceptance criteria:
- After a scheduler-driven frequency change, the ControlFreq Hero frequency selection updates on the next refresh cycle without requiring manual QSY/Resume actions.
- Existing manual QSY/Resume Hero resync behavior remains unchanged.
- No false dirty state or scheduler behavior changes.

Verification:
- `python -m compileall freqinout/gui/controlfreq_tab.py`
- `python tools/release_preflight.py`
- Manual checks:
  - Let scheduler perform an automatic frequency change and confirm Hero selection updates to the new scheduled/current frequency.
  - Manual QSY and Resume Schedule flows still update Hero selection as before.

Rollback:
- Revert scheduler-frequency transition detection and Hero resync preference changes in `freqinout/gui/controlfreq_tab.py`.

### 1.27 Addendum (2026-02-24): ControlFreq Clock Label Width Guard

Problem:
- The `ControlFreq` top-row time display can overflow/clamp off the right edge of the window unless the main window is maximized.
- The clock label currently increases its minimum width every refresh based on the formatted time string, over-constraining the top row layout.

Impacted files and failure modes:
- `freqinout/gui/controlfreq_tab.py`
  - Failure mode: `_sync_header_time_label_widths()` expands `current_time_label` minimum width (>=320px), causing the `Operating Status` + clock row to exceed available width on narrower windows.

Scope:
- Stop dynamically increasing the clock label minimum width.
- Allow the clock label to shrink within the layout.
- Elide the displayed time text when horizontal space is constrained while keeping the full text available via tooltip.

Acceptance criteria:
- The `ControlFreq` clock label remains contained within the window width at non-maximized sizes.
- The time display remains readable and right-aligned when space is available.
- Full time text is still accessible (tooltip) when elided.

Verification:
- `python -m compileall freqinout/gui/controlfreq_tab.py`
- `python tools/release_preflight.py`
- Manual checks:
  - Resize the main window narrower and confirm the ControlFreq time label no longer spills off the right edge.
  - Confirm the displayed time updates each second and tooltip shows the full text when elided.

Rollback:
- Revert the clock label size-policy/elide handling changes in `freqinout/gui/controlfreq_tab.py`.

### 1.28 Addendum (2026-02-24): ControlFreq Operating Status Width Balance

Problem:
- The `ControlFreq` `Operating Status` group expands wider than its contents require, which can crowd the top-row clock label and partially push the time display off-screen on non-maximized windows.

Impacted files and failure modes:
- `freqinout/gui/controlfreq_tab.py`
  - Failure mode: the top-row layout gives extra width to the `Operating Status` group (expanding policy + row stretch), leaving insufficient width for the clock label even though the LED content itself does not need the extra space.

Scope:
- Keep the `Operating Status` group sized to its content width (with normal shrink behavior when needed).
- Route extra horizontal space into a spacer between the status group and the clock label.

Acceptance criteria:
- The `Operating Status` group no longer expands wider than needed when window width increases.
- The top-row clock label remains fully visible at typical non-maximized widths (subject to existing elide behavior under very narrow widths).
- Existing LED/status content remains visible and aligned.

Verification:
- `python -m compileall freqinout/gui/controlfreq_tab.py`
- `python tools/release_preflight.py`
- Manual checks:
  - Open `ControlFreq` at non-maximized width and confirm the status group does not consume excess width.
  - Confirm the clock remains visible and right-aligned while resizing.

Rollback:
- Revert top-row `Operating Status` size-policy and layout stretch changes in `freqinout/gui/controlfreq_tab.py`.

### 1.29 Addendum (2026-02-24): ControlFreq Clock Column Width Allocation

Problem:
- After constraining the `Operating Status` group width, the clock text can still be truncated because the top-row layout routes extra width into a spacer while the clock widget remains a right-aligned, fixed-size child of the right column.

Impacted files and failure modes:
- `freqinout/gui/controlfreq_tab.py`
  - Failure mode: `current_time_label` receives only its intrinsic width (often the already-elided text width), so it cannot expand to show the full clock text even when the row has available space.

Scope:
- Give the right-side clock column the remaining horizontal row width.
- Allow the clock label to expand within that column while keeping text right-aligned.

Acceptance criteria:
- At normal non-maximized widths, the clock uses available right-side space and shows a longer/full time string when space permits.
- Under tighter widths, the existing elide behavior still prevents overflow.

Verification:
- `python -m compileall freqinout/gui/controlfreq_tab.py`
- `python tools/release_preflight.py`
- Manual checks:
  - Resize the window and confirm the clock grows/shrinks with available width rather than staying fixed narrow.
  - Confirm right alignment is preserved.

Rollback:
- Revert right-column stretch and clock label size-policy/layout insertion changes in `freqinout/gui/controlfreq_tab.py`.

### 1.70 Addendum (2026-02-25): BBS Auto-Archive Background Trigger

Problem:
- `Settings` exposes VarAC BBS `Enable Auto-Archive` and an age policy, but no runtime background trigger currently executes the archive action automatically.
- Operators must archive BBS files manually from `Messages`, which can leave aging files in the live BBS folder and create a mismatch with the UI hint text.

Impacted files and failure modes:
- `freqinout/gui/message_viewer_tab.py`
  - Failure mode: running directory scans and moves on the UI thread can freeze `Messages`.
  - Failure mode: aggressive or repeated checks can interfere with live operating workflows.
  - Failure mode: moving unsupported/non-message files from the BBS folder could surprise operators.

Scope:
- Implement a low-frequency, reliability-first auto-archive trigger in `Messages` tab behavior.
- Trigger checks:
  - on first `Messages` tab activation after startup
  - periodically while the `Messages` tab remains active (daily cadence with cooldown gate)
- Execute file moves off the GUI thread.
- Preserve manual `Archive` action behavior and collision-safe destination naming.
- Keep BBS auto-archive limited to BBS-supported file types already recognized by the `Messages` BBS scanner.

Acceptance criteria:
- With auto-archive enabled and valid BBS/BBS Archive directories configured, opening `Messages` after startup triggers a background check and moves BBS files older than the selected day threshold.
- A file newer than the threshold remains in the BBS directory.
- Auto-archive does not freeze the `Messages` UI during checks/moves.
- If files are moved, `Messages` BBS rows refresh to reflect the new folder contents without user restart.
- Missing/invalid settings result in a logged skip, not a crash or blocking dialog.
- Manual `Archive` action in `Messages` continues to move a selected BBS file to the archive folder.

Verification:
- `python tools/release_preflight.py`
- `python -m compileall freqinout`
- Manual checks:
  - Launch app with no radio software running.
  - Enable auto-archive and configure BBS/BBS Archive directories.
  - Place one old eligible file and one fresh eligible file in BBS Directory.
  - Open `Messages`; confirm only the old file moves to BBS Archive and UI remains responsive.
  - Confirm settings persist after restart.

Rollback:
- Revert background auto-archive trigger/worker changes in `freqinout/gui/message_viewer_tab.py`.

### 1.71 Addendum (2026-02-25): SOP Action Condition-Level Eligibility and Display Filtering Consistency

Problem:
- SOP action rows allow freeform condition-level text entry, which can introduce entry errors.
- Invalid/freeform condition-level text is normalized to `ALL`, which can unintentionally broaden action applicability.
- SOP `Upcoming` action display filters by condition levels, but related SOP display/export plan builders do not consistently apply the same filtering, causing non-matching actions to appear in some displays.

Impacted files and failure modes:
- `freqinout/gui/sop_tab.py`
  - Failure mode: row condition entry errors broaden applicability or create ambiguous action intent.
  - Failure mode: HF action rows can be assigned to groups that do not use condition levels.
- `freqinout/core/sop_manager.py`
  - Failure mode: daily/periodic SOP plan rows include actions that do not match the operating group's configured condition level.

Scope:
- Replace SOP v2 action-row `Condition Levels` freeform entry with a selectable multi-select control (`ALL`, `1`, `2`, `3`, `4`, `5`) that normalizes selected items into the stored canonical value.
- Restrict HF action-row group eligibility to operating groups with `Use Condition Levels` enabled.
- Add save-time validation so HF SOP action rows cannot be saved for groups that do not use condition levels.
- Apply condition-level filtering consistently in related SOP plan/display builder paths (daily/periodic).

Constraints:
- Preserve existing stored `condition_levels` format (`ALL` or comma-separated canonical values).
- Keep runtime filtering semantics unchanged for existing scheduler/upcoming paths already applying condition-level checks.
- Avoid expensive repeated recomputation in row-refresh paths; use cached group metadata and generated selector values.

Acceptance criteria:
- In SOP action rows, `Condition Levels` is selectable (not freeform) via a multi-select control and supports `ALL`, single levels, and multi-level selections.
- HF groups without `Use Condition Levels` enabled are not offered as eligible new action-row group selections.
- Saving an HF SOP with an action row assigned to a non-condition-enabled group is blocked with a clear validation error.
- SOP `Upcoming` behavior remains correct.
- SOP daily/periodic plan builders (used by SOP export/display) exclude actions that do not match the current operating-group condition level.
- Verification baseline passes:
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Verification:
- Manual checks:
  - Configure an HF operating group with condition levels enabled and set a current level (e.g., `2`).
  - Create SOP action rows for `ALL`, `2`, `1,2`, and `3`; confirm non-matching rows are excluded from related SOP displays/exports.
  - Confirm condition-level values are selectable only (no manual typing required).
  - Confirm groups without `Use Condition Levels` enabled are not eligible for new HF action-row selection and cannot be saved if present in a row.

Rollback:
- Revert SOP condition-row UI and validation changes in `freqinout/gui/sop_tab.py`.
- Revert SOP condition-level display/export filtering changes in `freqinout/core/sop_manager.py`.

### 1.73 Addendum (2026-02-25): SOP Builder Row Conflict Details (HF vs Net vs SOP)

Problem:
- The SOP Builder row `Conflict` status is currently a non-clickable badge with tooltip-only summaries.
- Operators cannot quickly tell whether a conflict is caused by HF Daily Schedule, Net Schedule, or another SOP action without entering the save-time resolver flow.
- Conflict details are not explicit about overlap timing and different-frequency cause, reducing clarity for manual resolution.

Impacted files and failure modes:
- `freqinout/gui/sop_tab.py`
  - Failure mode: row-level conflicts are visible but not actionable until Save, causing confusion and extra steps.
  - Failure mode: tooltip summaries do not clearly explain what to change to resolve the conflict.
- `freqinout/core/sop_manager.py`
  - Failure mode: conflict diagnostics return summaries only, so UI cannot show precise overlap details and source-specific resolution guidance.

Scope:
- Replace the SOP Builder row `Conflict` display widget with a clickable status control.
- Add an on-demand row conflict details dialog that classifies conflicts into:
  - HF Schedule
  - Net Schedule
  - SOP Actions
- Extend `SOPManager.detect_action_conflicts()` with optional detailed conflict payloads for UI display (default off to preserve realtime performance characteristics).
- Show overlap windows and frequencies for actionable conflicts only.

Constraints:
- Preserve existing conflict semantics:
  - same-frequency time overlaps are not conflicts and must not be shown in the row conflict details dialog.
- Keep realtime row conflict refresh fast:
  - do not compute detailed conflict rows during debounced inline badge refresh.
- Preserve existing save-time conflict resolver behavior unless explicitly improved by this change.

Acceptance criteria:
- SOP Builder conflict column remains compact, but `Conflict` rows are clickable.
- Clicking a conflicting row opens a clear dialog showing whether the conflict is in HF Schedule, Net Schedule, and/or SOP Actions.
- Conflict details include overlap timing and the conflicting frequency/band when applicable.
- Same-frequency overlaps are not listed as conflicts in the dialog.
- Realtime conflict badges continue to refresh without computing detailed conflict payloads.
- Verification baseline passes:
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Verification:
- Manual checks:
  - Create an SOP row that overlaps HF Daily Schedule on a different frequency; click `Conflict` and confirm it appears under `HF Schedule`.
  - Create an SOP row that overlaps Net Schedule on a different frequency; click `Conflict` and confirm it appears under `Net Schedule`.
  - Create two SOP rows that overlap on different frequencies; click `Conflict` and confirm `SOP Actions` details appear.
  - Create two SOP rows that overlap on the same frequency; confirm no SOP conflict is shown for that overlap.
  - Confirm `OK`/`Pending`/`Local` rows are not treated as actionable conflict clicks.

Rollback:
- Revert row conflict detail dialog/button changes in `freqinout/gui/sop_tab.py`.
- Revert optional detailed conflict payload changes in `freqinout/core/sop_manager.py`.

### 1.74 Addendum (2026-02-25): Reversible SOP Session Return-to-Normal Flow

Problem:
- `Auto-Adjust HF Around SOP` rewrites HF Daily Schedule rows, but SOP deactivation only removes SOP overlays and does not restore the pre-adjust HF schedule.
- Net/SOP conflict policy decisions created during SOP activation/conflict resolution are persisted and can outlive the operator's intent for the current SOP use session.
- SOP activation/deactivation and conflict prompts are split across Daily, Net, and SOP Builder flows, making it hard for operators to understand how to "return to normal."

Impacted files and failure modes:
- `freqinout/gui/daily_schedule_tab.py`
  - Failure mode: operators assume auto-adjust is temporary and lose track of modified HF rows after SOP deactivation.
  - Failure mode: deactivation prompts do not offer to restore HF schedule or revert temporary Net/SOP decisions.
- `freqinout/gui/net_schedule_tab.py`
  - Failure mode: Net/SOP conflict decisions are saved without session tracking and cannot be cleanly reverted as part of SOP deactivation.
- `freqinout/gui/sop_tab.py`
  - Failure mode: deactivation via SOP Builder save does not offer a return-to-normal workflow.

Scope:
- Add a local persisted SOP session journal (JSON, no DB schema change) to track:
  - active SOP session metadata,
  - reversible pre-auto-adjust HF schedule snapshot,
  - temporary Net/SOP conflict policy decisions saved during the session.
- Add a unified "Deactivate SOP and Return to Normal" confirmation flow (Daily/SOP/Net entry points routed through Daily tab logic) that can:
  - deactivate HF SOP profiles,
  - restore the pre-auto-adjust HF schedule snapshot,
  - revert temporary Net/SOP conflict policy decisions from the current SOP session.
- Default new Net/SOP conflict decisions saved via Daily/Net conflict dialogs to temporary session-tracked decisions.
- Warn on HF restore when current HF rows differ from the recorded post-auto-adjust signature (overwrite prompt).

Constraints:
- Preserve UI responsiveness; no background polling.
- No DB schema migrations.
- Keep existing runtime precedence behavior unchanged in scheduler; this is UX/session-state orchestration.
- Same-frequency conflict semantics remain unchanged.

Acceptance criteria:
- Auto-adjust records a reversible pre-adjust HF snapshot when an HF SOP session is active.
- Deactivating SOP from Daily tab offers a single confirmation flow to return to normal (HF restore + temp Net/SOP rollback).
- Deactivating via SOP Builder save also triggers a return-to-normal prompt after deactivation.
- Net tab "Deactivate Active HF SOPs" routes through the same return-to-normal flow.
- Net/SOP decisions saved via Daily/Net conflict resolution dialogs are tracked as temporary session decisions by default.
- If HF schedule changed after auto-adjust, restore prompt explicitly warns before overwriting newer edits.
- Verification baseline passes:
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Verification:
- Manual checks:
  - Activate an HF SOP, trigger auto-adjust, then deactivate SOP from Daily tab and confirm one-click return-to-normal restores HF rows and reverts temp Net/SOP decisions.
  - Repeat deactivation from SOP Builder (save with Active unchecked) and confirm prompt appears.
  - Create Net/SOP conflict decisions in Net tab while SOP is active; confirm they are reverted by return-to-normal.
  - Modify HF rows after auto-adjust, then deactivate SOP and confirm overwrite warning appears before restore.

Rollback:
- Revert SOP session journal and deactivation restore flow changes in `freqinout/gui/daily_schedule_tab.py`.
- Revert session-aware Net/SOP policy save/deactivation routing changes in `freqinout/gui/net_schedule_tab.py`.
- Revert SOP Builder deactivation prompt integration in `freqinout/gui/sop_tab.py`.

### 1.75 Addendum (2026-02-26): SOP Builder Activation Conflict Defaults + Summary (Rollout Phase 1)

Problem:
- SOP conflict handling feels fragmented across SOP Builder, Daily Schedule, and Net Schedule, especially during HF SOP activation.
- SOP Builder requires users to infer what happens next for HF and Net conflicts, and users must often tab-hop to complete conflict handling.
- The existing save-time conflict dialog does not provide a lightweight summary of conflict types in the SOP Builder itself.

Impacted files and failure modes:
- `freqinout/gui/sop_tab.py`
  - Failure mode: users cannot set a preferred activation conflict-handling strategy from the SOP Builder UI.
  - Failure mode: users do not see clear conflict counts by type (`HF Schedule`, `Net Schedule`, `SOP Actions`) while editing.
  - Failure mode: save-time activation flow prompts even when defaults could safely handle non-blocking conflicts.

Scope (Phase 1 only):
- Add an `SOP Activation Conflict Defaults` panel to SOP Builder (HF category only) with locally persisted settings (no DB migration):
  - HF Schedule default:
    - `Auto-adjust HF Schedule around SOP (Reversible)`
    - `Add SOP, review HF conflicts in Daily Schedule`
  - Net Schedule default:
    - `Temporary SOP Priority for Net overlaps (this SOP session)`
    - `Add SOP, review conflicts in Net Schedule`
- Add an SOP Builder conflict summary label that reuses existing realtime row conflict diagnostics and shows counts by type (`HF`, `Net`, `SOP`) plus pending/incomplete rows.
- Use activation defaults in the existing save-time HF activation flow:
  - skip the row conflict dialog when defaults make all remaining conflicts non-blocking,
  - auto-apply temporary Net/SOP priority decisions after activation when configured,
  - attempt Daily-tab HF auto-adjust after activation when configured (reusing existing Daily tab auto-adjust logic),
  - otherwise provide clear post-save review guidance in the SOP saved message.

Constraints:
- No DB schema changes for this phase; defaults persist in `SettingsManager`.
- Preserve same-frequency overlap behavior (not treated as a conflict).
- Preserve realtime UI responsiveness by reusing existing debounced conflict scans; no new background polling.
- Keep the existing row conflict dialog for cases that still require manual timing adjustments.

Acceptance criteria:
- SOP Builder shows a visible HF-only `SOP Activation Conflict Defaults` section with the two default controls.
- Defaults persist across app restart.
- SOP Builder shows a clear conflict summary by type using existing realtime conflict diagnostics.
- Saving an active HF SOP uses the configured defaults to reduce manual conflict-resolution steps where safe.
- Net default `Temporary SOP Priority` writes temporary session-tracked Net/SOP decisions when possible.
- HF default `Auto-adjust` reuses Daily tab reversible auto-adjust when the Daily tab is available and eligible conflicts exist.
- Verification baseline passes:
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Verification:
- Manual checks:
  - In SOP Builder (HF), change activation defaults, restart app, and confirm defaults persist.
  - Create SOP rows with HF-only conflicts and confirm conflict summary counts update in realtime.
  - Activate an HF SOP with Net overlaps and `Temporary SOP Priority` default; confirm Net/SOP decisions are applied without extra Net-tab prompts when no manual timing conflicts remain.
  - Activate an HF SOP with eligible HF/SOP active schedule overlaps and `Auto-adjust` default; confirm Daily HF rows are auto-adjusted and the save message explains what happened.
  - Switch to Local Comms SOP and confirm the activation defaults panel is hidden (or clearly marked not applicable).

Rollback:
- Revert SOP Builder activation defaults + summary + activation flow changes in `freqinout/gui/sop_tab.py`.

### 1.76 Addendum (2026-02-26): SOP Builder Conflict Workbench (Rollout Phase 2)

Problem:
- Even with SOP Builder activation defaults and conflict summaries, operators still need to jump between tabs or wait until Save-time dialogs to apply per-row conflict policy decisions.
- Users want to resolve as many SOP activation conflicts as possible from one SOP Builder screen, especially Net-vs-SOP and Daily-vs-SOP policy choices.

Impacted files and failure modes:
- `freqinout/gui/sop_tab.py`
  - Failure mode: conflict resolution remains fragmented and users cannot batch-apply policy decisions from SOP Builder.
  - Failure mode: a dense always-on conflict table could degrade responsiveness if it triggers new scans or rebuilds too often.

Scope (Phase 2):
- Add an HF-only `Conflict Workbench` panel in the lower SOP Builder area.
- Populate the workbench from the existing debounced realtime conflict diagnostics (reuse `_refresh_inline_conflict_badges()` analyses; no extra conflict scan pass).
- Show one row per SOP action row with actionable conflict information:
  - row number, action, group, HF/Net/SOP summaries, policy selector, status/next action, details button.
- Add batch buttons for quick policy resolution from one screen:
  - `Set SOP Priority`
  - `Set Net Priority`
  - `Set Daily Priority`
  - `Apply Builder Defaults`
- Keep same-frequency overlaps excluded (existing conflict semantics).
- Keep time/timing conflict fixes in the existing Save-time dialog for rows that still require manual start-time changes.

Constraints:
- Preserve UI responsiveness; no new polling and no duplicate conflict scans.
- Rebuild/update the workbench only from debounced analyses and use a signature gate to avoid unnecessary table rebuilds.
- Do not change DB schema or conflict semantics.

Acceptance criteria:
- SOP Builder (HF) shows a visible `Conflict Workbench` panel under the action rows (or lower section) when applicable.
- Workbench rows reflect current debounced conflict diagnostics and update as rows are edited.
- Changing a workbench policy selector updates the underlying SOP action row conflict policy used on Save.
- Batch buttons update displayed conflict rows and reduce manual per-row edits.
- Rows that still require timing changes remain clearly marked and continue to be handled by the existing Save-time conflict dialog.
- Verification baseline passes:
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Verification:
- Manual checks:
  - Create multiple HF SOP rows with Daily/Net conflicts and confirm the workbench shows one row per conflicting SOP action.
  - Use batch `Set SOP Priority`, `Set Net Priority`, and `Set Daily Priority` and confirm row policies change and Save uses those choices.
  - Use `Apply Builder Defaults` and confirm policies reset to the current Activation Conflict Defaults behavior.
  - Confirm rows with SOP-vs-SOP overlaps are marked as needing timing changes and still require Save-time adjustment.
  - Confirm Local Comms SOP hides or disables the workbench.

Rollback:
- Revert SOP Builder conflict workbench UI and wiring in `freqinout/gui/sop_tab.py`.

### 1.77 Addendum (2026-02-26): SOP Builder Conflict Workbench Actionability Feedback

Problem:
- In the SOP Builder Conflict Workbench, batch action buttons could appear non-actionable because:
  - they were styled as muted regardless of enabled state, and/or
  - clicking a batch action that had nothing applicable resulted in a silent no-op.
- Timing-only SOP conflicts (for example SOP-vs-SOP overlaps) do not benefit from policy batch actions, but the UI did not explain that clearly.

Impacted files and failure modes:
- `freqinout/gui/sop_tab.py`
  - Failure mode: users cannot tell whether a batch action is available, already applied, or not applicable.
  - Failure mode: users perceive the workbench as broken when actions silently make no changes.

Scope:
- Improve Conflict Workbench batch action UX by:
  - enabling/disabling batch buttons based on actual applicability,
  - styling enabled buttons as actionable,
  - adding explanatory tooltips for disabled states,
  - updating the workbench status label when a batch click makes no changes,
  - explicitly calling out timing-only conflicts where batch priority actions do not apply.

Constraints:
- No changes to conflict semantics or save-time timing resolution logic.
- Keep responsiveness unchanged; reuse existing cached workbench diagnostics/state.

Acceptance criteria:
- Workbench batch buttons visibly indicate actionable vs non-actionable states.
- Clicking a batch action that cannot change anything provides clear status feedback.
- Timing-only conflicts are clearly identified as requiring timing changes rather than policy changes.
- Verification baseline passes:
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Rollback:
- Revert workbench actionability feedback updates in `freqinout/gui/sop_tab.py`.

### 1.78 Addendum (2026-02-26): SOP Builder Conflict Workbench Inline Timing Assistance

Problem:
- The Conflict Workbench improves policy resolution, but rows marked `Adjust time` still require users to wait for the Save-time conflict dialog to get suggested start times.
- This keeps SOP Builder flow cumbersome for timing-only conflicts (especially SOP-vs-SOP overlaps).

Impacted files and failure modes:
- `freqinout/gui/sop_tab.py`
  - Failure mode: operators cannot resolve timing conflicts from the workbench and must context-switch into a later modal dialog.
  - Failure mode: precomputing timing suggestions for all rows during realtime conflict updates could hurt responsiveness.

Scope:
- Add inline timing assistance to the SOP Builder Conflict Workbench:
  - `Suggested Start` control/column for rows that need timing changes
  - `Apply` action to write the suggested start into the SOP action row immediately
- Compute suggestions on demand (button click) using existing `SOPManager.suggest_non_conflicting_start(...)`.
- Update the action row start time display and conflict status/workbench after apply.
- Keep the existing Save-time conflict dialog as fallback/final validation.

Constraints:
- No extra scans in the debounced realtime conflict path.
- Suggestions must be generated lazily/on-demand to preserve UI responsiveness.
- No change to conflict semantics (same-frequency overlaps remain non-conflicts).

Acceptance criteria:
- Timing-conflict rows in the Conflict Workbench show an inline way to request a suggested start time and apply it.
- Applying a suggested time updates the underlying SOP action row and triggers conflict refresh.
- Non-timing rows do not show active timing controls.
- Realtime performance remains unchanged because suggestions are not precomputed in the background.
- Verification baseline passes:
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Rollback:
- Revert workbench inline timing suggestion/apply changes in `freqinout/gui/sop_tab.py`.

### 1.79 Addendum (2026-02-26): SOP Builder Conflict Workbench Filter Strip

Problem:
- As SOP action counts grow, the Conflict Workbench can become dense.
- Operators need a fast way to focus on one conflict type (`HF`, `Net`, `SOP`) or only rows that still need timing adjustments.

Impacted files and failure modes:
- `freqinout/gui/sop_tab.py`
  - Failure mode: users must scan all conflict rows manually, increasing cognitive load.
  - Failure mode: adding filter controls could create extra conflict scans or slow updates if not wired to existing cached diagnostics.

Scope:
- Add a single-select filter strip in the Conflict Workbench:
  - `All`, `HF`, `Net`, `SOP`, `Needs Time`
- Apply filtering using existing debounced/cached conflict analyses (no new conflict scan paths).
- Update workbench status text to indicate filtered vs total rows where applicable.
- Keep batch actions scoped to currently visible filtered rows.

Constraints:
- No change to conflict detection semantics.
- No extra background work; filtering must be in-memory over existing analysis data.
- Keep UI responsiveness at current levels.

Acceptance criteria:
- Filter strip is visible in the Conflict Workbench and switches row view immediately.
- `Needs Time` shows rows that still require timing adjustments.
- Status text reflects filtered view context (for example `Showing X of Y` when filtered).
- Batch actions only affect visible filtered rows.
- Verification baseline passes:
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Rollback:
- Revert Conflict Workbench filter strip changes in `freqinout/gui/sop_tab.py`.

### 1.80 Addendum (2026-02-26): SOP Builder Guided Workflow (Collapsible Sections + Readiness Strip)

Problem:
- SOP Builder remains visually crowded even after conflict workbench enhancements.
- Activation defaults and conflict controls are valuable but not always needed in view at the same time.
- Users need stronger in-UI guidance on what to do next without reading documentation.

Impacted files and failure modes:
- `freqinout/gui/sop_tab.py`
  - Failure mode: users feel overwhelmed by always-visible controls and miss next-step cues.
  - Failure mode: `Suggested Start` column can look like required input even when timing adjustments are not needed.

Scope:
- Add a lightweight guided workflow UX in SOP Builder:
  - top readiness/status strip summarizing current SOP edit state and next step,
  - collapsible section controls for `Activation Defaults` and `Conflict Workbench`,
  - auto-expanded workbench when conflicts exist; defaults collapsed by default with a compact summary.
- Improve `Suggested Start` presentation:
  - timing-needed rows: auto-populated or `Computing...` then concrete suggestion,
  - non-timing rows: explicit `Not needed` display (no implied user input).

Constraints:
- Preserve existing save/conflict semantics and reversible session behavior.
- No heavy compute on each keystroke; keep suggestion generation lazy/incremental.
- Keep UI thread responsive and avoid new blocking scans.

Acceptance criteria:
- SOP Builder presents a clear workflow status summary near the top.
- Activation defaults are collapsible and summarized when collapsed.
- Conflict workbench is collapsible but surfaces automatically when conflicts exist.
- `Suggested Start` clearly differentiates timing-needed rows from non-timing rows.
- Verification baseline passes:
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Rollback:
- Revert guided workflow/collapsible section/readiness-strip changes in `freqinout/gui/sop_tab.py`.

### 1.98 Addendum (2026-02-26): SOP Builder Condition-Level Selection Persistence + Interaction Stability

Problem:
- Users report condition-level edits in SOP action rows are not persisting after Save.
- Condition-level multi-select interaction is visually/behaviorally inconsistent (selection clicks can feel unreliable).

Impacted files and failure modes:
- `freqinout/gui/sop_tab.py`
  - Failure mode: action-row initialization refresh resets loaded condition-level values to `ALL`.
  - Failure mode: multi-select popup click handling can conflict with default item handling, causing inconsistent toggles.

Scope:
- Preserve existing condition-level values during row initialization and dynamic option refresh.
- Ensure condition-level selections from `_ConditionLevelsMultiCombo` are stable and saved as expected in SOP payloads.
- Harden multi-select click handling to avoid double-toggle/unstable UI behavior.

Constraints:
- No change to condition-level matching semantics (`ALL` and explicit `1..5` combinations remain unchanged).
- No DB schema changes.
- Keep UI responsiveness unchanged.

Acceptance criteria:
- Editing condition levels in SOP Builder, clicking Save, and reloading the same SOP preserves the edited values.
- Existing stored condition-level values display correctly when loading SOP rows.
- Multi-select row clicks reliably toggle intended options without jittery/double-toggle behavior.
- Verification baseline passes:
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Rollback:
- Revert condition-level widget interaction and row initialization updates in `freqinout/gui/sop_tab.py`.

### 1.99 Addendum (2026-02-27): ControlFreq SOP Condition-Level Refresh Parity

Problem:
- SOP action condition-level edits are reflected in `FreqPlanner` but can appear stale in `ControlFreq` until later refresh cycles.
- Operators expect `ControlFreq` schedule outlook to reflect current SOP eligibility immediately after SOP/condition-level changes.

Impacted files and failure modes:
- `freqinout/gui/controlfreq_tab.py`
  - Failure mode: stale SOP eligibility in schedule outlook due to long-lived SOP manager settings snapshot.
  - Failure mode: refresh gating while tab is inactive delays user-visible SOP row updates after returning to ControlFreq.

Scope:
- Ensure `ControlFreq` reloads SOP manager settings context when SOP/settings data changes.
- Add a lightweight pending-refresh handoff so SOP outlook is refreshed immediately on next `ControlFreq` activation.
- Keep existing non-blocking refresh behavior and cache strategy intact.

Constraints:
- No changes to SOP condition-level matching semantics.
- No DB schema changes.
- Preserve UI responsiveness and avoid aggressive background polling.

Acceptance criteria:
- After saving SOP condition-level edits, switching to `ControlFreq` reflects expected SOP rows in schedule outlook without requiring manual refresh/restart.
- After settings-level condition edits, `ControlFreq` reflects updated SOP eligibility on next active refresh.
- Verification baseline passes:
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Rollback:
- Revert ControlFreq SOP refresh wiring changes in `freqinout/gui/controlfreq_tab.py`.

### 1.100 Addendum (2026-02-27): Fast Condition-Level Update Fanout Path (Main Tab + Automation-Ready)

Problem:
- Editing condition levels from the main tab can feel slow because the save path emits global `settings_saved`, which fans out synchronously to many tabs.
- Future automated condition-level updates (for example JS8Call-driven notifications) require a lightweight, coalesced update path to avoid repeated expensive full-settings fanout.

Impacted files and failure modes:
- `freqinout/gui/main_window.py`
  - Failure mode: condition-level save is blocked by broad synchronous settings refresh handlers.
  - Failure mode: no dedicated debounce/coalesce path for frequent condition-level updates.
- `freqinout/gui/sop_tab.py`
  - Failure mode: only full `on_settings_saved()` path exists, which can be heavier than needed for condition-level-only changes.
- `freqinout/gui/controlfreq_tab.py`, `freqinout/gui/freq_planner_tab.py`
  - Failure mode: no explicit lightweight condition-level refresh entrypoint for targeted updates.

Scope:
- Add a dedicated, debounced condition-level change notification path in `MainWindow`.
- Route main-tab condition-level editor saves through this path instead of global `settings_saved` emit.
- Add targeted handlers in SOP/ControlFreq/FreqPlanner for condition-level-only refresh behavior.
- Preserve existing global settings save behavior in Settings tab.

Constraints:
- No change to condition-level semantics or persistence schema.
- Keep UI thread responsive; avoid blocking fanout in the save-button path.
- Keep future automation integration simple via a reusable queued notifier.

Acceptance criteria:
- Saving condition levels from the main tab closes promptly and applies visible updates without full-settings lag.
- SOP eligibility/rendering updates still propagate to affected tabs (`SOP`, `ControlFreq`, `FreqPlanner`) safely.
- New condition-level update path is debounced/coalesced for future automated updates.
- Verification baseline passes:
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Rollback:
- Revert condition-level fanout changes in:
  - `freqinout/gui/main_window.py`
  - `freqinout/gui/sop_tab.py`
  - `freqinout/gui/controlfreq_tab.py`
  - `freqinout/gui/freq_planner_tab.py`

### 1.101 Addendum (2026-02-27): SOP Builder Workbench Expansion UX (Manual-First)

Problem:
- In SOP Builder, the Conflict Workbench auto-expands on single-row edits, interrupting multi-row action editing flow.
- Users need to make several action-row edits before switching context to conflict resolution.

Impacted files and failure modes:
- `freqinout/gui/sop_tab.py`
  - Failure mode: realtime conflict updates force workbench expansion during row editing.
  - Failure mode: users lose editing focus due to layout jumps after each change.

Scope:
- Make workbench expansion manual-first while editing:
  - keep workbench collapsed unless the user explicitly opens it,
  - keep conflict counts visible in the header summary,
  - show an actionable collapsed-state CTA label (for example `Review Conflicts (N)`).
- Force-expand workbench only on Save-time conflict gating when unresolved manual conflicts remain.

Constraints:
- No change to conflict detection semantics.
- Keep existing realtime conflict computation and caching behavior.
- Preserve save-time conflict dialog behavior.

Acceptance criteria:
- Editing SOP action rows no longer auto-expands the workbench.
- Header still updates with conflict totals and timing-needs counts while collapsed.
- Save-time unresolved manual conflicts force workbench expansion before/followed by existing resolution flow.
- Verification baseline passes:
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Rollback:
- Revert manual-first workbench expansion changes in `freqinout/gui/sop_tab.py`.

### 1.102 Addendum (2026-02-27): SOP Profile Version Snapshots (Save/Load/Delete)

Problem:
- Operators can only keep one active draft per SOP category in the builder.
- Users need a low-friction way to save named SOP variants and restore them later without replacing the current profile until explicit Save.

Impacted files and failure modes:
- `freqinout/core/sop_manager.py`
  - Failure mode: missing persistent store for version snapshots.
  - Failure mode: non-idempotent schema setup could break existing installs.
- `freqinout/gui/sop_tab.py`
  - Failure mode: no UI affordance to save/load/delete versions from builder context.
  - Failure mode: loading a version could silently cross category boundaries or overwrite unsaved edits.

Scope:
- Add persistent SOP version storage table:
  - `sop_profile_versions` with `category`, `label`, `note`, `snapshot_json`, timestamps.
- Add manager APIs for version lifecycle:
  - default label generation,
  - save/list/get/delete operations.
- Add SOP Builder `Versions` menu with:
  - `Save Version`,
  - `Load Version`,
  - `Delete Version`.
- Load behavior:
  - only within current category (HF vs LOCAL),
  - prompt before discarding unsaved edits,
  - load into builder as draft and require explicit Save to apply runtime schedule effects.

Constraints:
- Preserve existing profile activation, conflict, and schedule-derivation semantics.
- Keep DB migration idempotent and startup-safe.
- Keep UI flow responsive; no background heavy scans.

Acceptance criteria:
- User can save a named version from SOP Builder and receive a persisted version id.
- User can list/select and load a saved version for the current category into the builder.
- User can delete saved versions via confirmation prompt.
- Loaded version does not auto-apply until user clicks `Save`.
- Verification baseline passes:
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`
  - `powershell -ExecutionPolicy Bypass -File .\tools\freqinout-db.ps1 status`

Rollback:
- Revert SOP version schema/API changes in `freqinout/core/sop_manager.py`.
- Revert SOP Builder version menu/actions in `freqinout/gui/sop_tab.py`.

### 1.103 Addendum (2026-02-27): Linux Launch Control VarAC via Wine Command

Problem:
- In Linux environments, VarAC is commonly launched via Wine command wrappers (for example desktop entry `Exec=env WINEPREFIX=... wine-stable .../VarAC.exe`).
- Launch Control currently derives VarAC launch only from `varac_path` and direct executable invocation, which can fail when Wine wrapping is required.

Impacted files and failure modes:
- `freqinout/core/launch_orchestrator.py`
  - Failure mode: VarAC startup/autostart fails on Linux because `.exe` is launched without Wine.
  - Failure mode: wrapped Wine command could infer incorrect working directory if not handled explicitly.
- `freqinout/gui/settings_tab.py`
  - Failure mode: no dedicated launch-command field for Linux Wine invocation.
  - Failure mode: Launch Control visibility gating may hide VarAC when only launch command (not install folder) is configured.
- `docs/guide.html`, `CHANGELOG.md`
  - Failure mode: operator docs do not explain Linux VarAC Launch Command behavior.

Scope:
- Add optional setting `varac_launch_cmd` in VarAC Settings UI for custom launch command (Linux-focused).
- Launch orchestration precedence for VarAC:
  1) `varac_launch_cmd` (freeform command),
  2) existing `varac_path`-derived path behavior,
  3) fallback command behavior.
- Add Linux fallback wrapping for VarAC path-derived `.exe` launch:
  - if resolved command points to `VarAC.exe` and command is not already wine-prefixed, prepend available wine binary (`wine-stable`, `wine`, `wine64`) when present.
- Keep `varac_path` semantics for DB discovery (`VarAC.db`) unchanged.
- Ensure Launch Control `configured` checks include `varac_launch_cmd` for VarAC row visibility.

Constraints:
- Preserve existing Windows VarAC launch behavior.
- No schema migration required (settings key is optional).
- Keep launch sequence/readiness polling non-blocking and deterministic.

Acceptance criteria:
- On Linux, with `VarAC Launch Command` configured to Wine invocation, Launch Control startup/manual sequence launches VarAC successfully.
- On Linux, with only `varac_path` folder containing `VarAC.exe`, Launch Control attempts Wine-wrapped launch when wine binary is available.
- VarAC remains visible in Launch Control when either `varac_path` or `varac_launch_cmd` is configured.
- Existing VarAC ingest path resolution via `varac_path`/`varac_db_path` remains unchanged.
- Verification baseline passes:
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Rollback:
- Revert launch-command and Wine-wrapper changes in `freqinout/core/launch_orchestrator.py`.
- Revert VarAC Launch Command UI/settings persistence wiring in `freqinout/gui/settings_tab.py`.
- Revert corresponding docs/changelog updates.

### 1.104 Addendum (2026-02-27): Launch Control Dependency-Aware Readiness (VarAC + JS8Call)

Problem:
- Operators report launch-order races in startup automation:
  - VarAC startup can appear ready late because it brings up VARA before stable operation.
  - JS8Spotter/CommStat may launch before JS8Call TCP API is ready, causing failed/partial connections.
- Existing launch progression uses process-running checks only and advances immediately once process presence is detected.

Impacted files and failure modes:
- `freqinout/core/launch_orchestrator.py`
  - Failure mode: `JS8Call` marked ready too early (process exists but API not yet reachable).
  - Failure mode: no settle delay between VarAC and subsequent apps when queue continues.
  - Failure mode: no targeted pacing before JS8-dependent apps (`JS8Spotter`, `CommStat`).
- `docs/guide.html`, `CHANGELOG.md`
  - Failure mode: operators do not understand new launch pacing/readiness behavior.

Scope:
- Add dependency-aware readiness in launch orchestration:
  - `JS8Call` readiness requires both process running and JS8 API reachable.
  - all other apps continue to use process-running readiness.
- Add targeted post-ready pacing:
  - after `VarAC`, apply a settle delay when more apps remain in queue,
  - after `JS8Call`, apply a settle delay only when pending queue includes `JS8Spotter` or `CommStat`.
- Keep behavior asynchronous and non-blocking (timer-based queue advancement).

Defaults:
- VarAC settle delay: 12 seconds (when not last in queue).
- JS8Call settle delay: 4 seconds (only when JS8Spotter/CommStat remain).

Constraints:
- Preserve existing startup/manual sequence controls and summary reporting.
- Avoid blocking the GUI thread.
- Keep existing launch timeout logic intact.

Acceptance criteria:
- When launch queue includes `VarAC` followed by other apps, queue progression pauses for VarAC settle delay before launching the next app.
- `JS8Call` is considered ready only after API reachability check passes (or timeout occurs).
- When queue contains `JS8Spotter`/`CommStat` after `JS8Call`, additional JS8 settle delay is applied before launching dependents.
- Verification baseline passes:
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Rollback:
- Revert dependency readiness and post-ready delay additions in `freqinout/core/launch_orchestrator.py`.
- Revert documentation/changelog notes for launch pacing behavior.

### 1.105 Addendum (2026-02-27): VarAC Launch Command Hardening + Clarity

Problem:
- Linux operators can observe different VarAC profile/config state when launching from desktop entry versus Launch Control.
- Main causes include command-mode launch context differences (working directory/prefix path expansion) and UI wording that suggests custom launch command is routinely required.

Impacted files and failure modes:
- `freqinout/core/launch_orchestrator.py`
  - Failure mode: VarAC custom command launches without stable CWD alignment to configured install folder.
  - Failure mode: `~`/`$HOME` style values in freeform launch command do not reliably resolve under `shell=False`.
- `freqinout/gui/settings_tab.py`
  - Failure mode: `VarAC Launch Command` UI copy does not clearly indicate default auto-launch path is preferred.
- `docs/guide.html`, `CHANGELOG.md`
  - Failure mode: docs do not explain that custom command is advanced/usually unnecessary.

Scope:
- Launch hardening:
  - normalize freeform command tokens for environment/user path expansions,
  - for VarAC custom command launches, prefer `varac_path` as launch CWD when valid.
- UI clarity:
  - reword label/placeholder/hint for VarAC Launch Command as advanced override,
  - explicitly recommend leaving it blank unless auto-launch fails.
- Documentation:
  - update guide/troubleshooting and changelog wording.

Constraints:
- Preserve existing default launch behavior when command override is blank.
- Keep launch orchestration non-blocking.
- No changes to VarAC DB path resolution semantics.

Acceptance criteria:
- Launch Control VarAC custom command launches with consistent CWD (when `varac_path` is configured and valid).
- `~`/`$HOME` style values in VarAC launch command resolve predictably.
- Settings UI text clearly communicates that VarAC Launch Command is optional advanced override and usually not needed.
- Verification baseline passes:
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Rollback:
- Revert VarAC launch-command normalization/CWD hardening in `freqinout/core/launch_orchestrator.py`.
- Revert VarAC UI wording/hint changes in `freqinout/gui/settings_tab.py`.
- Revert guide/changelog wording updates.

### 1.106 Addendum (2026-02-27): SOP Builder HF Start-Time Slot Guidance

Problem:
- In SOP Builder, operators configure HF action times without seeing the Daily HF schedule context in the same workflow.
- This increases cognitive load and causes avoidable trial-and-error when aligning SOP action start times to existing HF schedule rows.

Impacted files and failure modes:
- `freqinout/gui/sop_tab.py`
  - Failure mode: no in-row guidance for existing Daily HF schedule windows on the selected frequency.
  - Failure mode: introducing schedule lookups on each row edit can degrade UI responsiveness.
- `freqinout/gui/main_window.py`
  - Failure mode: SOP tab guidance cache may remain stale after HF schedule save unless explicitly invalidated.

Scope:
- Add lightweight row-level guidance in SOP Builder HF action rows:
  - keep manual `Start` entry editable,
  - add a `Slots` picker for the row that lists Daily HF schedule windows for the selected `Band-Freq`.
- Slot list content:
  - `day_utc`, `start_utc-end_utc`, and `group_name` from `daily_schedule_tab`,
  - filtered by selected `band/frequency`,
  - displayed in current SOP time mode (UTC/Local) for time values.
- Selection behavior:
  - selecting a slot sets the SOP row `Start` value only,
  - conflicting selections remain allowed (guidance-only behavior).
- Performance guardrails:
  - use cached in-memory index keyed by normalized `(band, frequency)`,
  - rebuild only when settings DB file token changes or explicit invalidation occurs.
- Cache invalidation:
  - clear SOP guidance cache when HF schedule emits `schedule_saved`.

Constraints:
- No changes to conflict semantics or save validation rules.
- Preserve existing manual entry workflow and real-time conflict checks.
- Avoid DB scans inside per-keystroke row refresh paths.

Acceptance criteria:
- In HF SOP Builder rows, when Group and Band-Freq are selected, user can open `Slots` and choose from matching Daily HF schedule windows.
- Each slot entry shows day + time span + group.
- Choosing a slot populates row `Start` while preserving manual edit capability.
- If no schedule windows exist for selected frequency, picker shows a clear empty-state message.
- UI remains responsive when editing multiple rows (no blocking DB work on every row edit).
- Verification baseline passes:
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Rollback:
- Revert SOP row slot-picker/caching changes in `freqinout/gui/sop_tab.py`.
- Revert HF schedule save-to-SOP cache invalidation signal wiring in `freqinout/gui/main_window.py`.

### 1.107 Addendum (2026-02-27): SOP Slot Refresh Smoothness + SOP Cross-Tab Update Coalescing

Problem:
- Editing SOP action `Group` values can trigger frequent dynamic row rebuilds, causing visible UI churn while typing.
- SOP data changes can fan out immediate refresh calls across HF Schedule, Net Schedule, ControlFreq, and scheduler status paths, creating redundant work bursts.

Impacted files and failure modes:
- `freqinout/gui/sop_tab.py`
  - Failure mode: per-keystroke group/resource edits trigger immediate full row dynamic refresh.
  - Failure mode: repeated SOP data-change emits trigger repeated cross-tab refreshes.
- `freqinout/gui/main_window.py`
  - Failure mode: `_on_sop_data_changed` executes full refresh cascade on every signal emission without coalescing.

Scope:
- Add short debounce for row dynamic option refresh on SOP row free-text edits:
  - keep immediate refresh for index selections,
  - debounce text-change refresh to smooth typing and reduce repeated UI repaints.
- Keep slot guidance responsive to group edits:
  - ensure slot-button state and slot ordering update from the edited group context.
- Coalesce SOP data-change fanout:
  - queue a single short-delay flush in `MainWindow` for SOP-driven cross-tab refresh paths.
- Add lightweight perf spans around key SOP paths for benchmark visibility.

Constraints:
- No behavioral changes to conflict rules, save validation, or schedule arbitration semantics.
- Keep UI thread responsive and avoid long blocking operations on edit events.

Acceptance criteria:
- Group text edits in SOP action rows no longer trigger visibly choppy per-keystroke row rebuilding.
- Slot guidance remains consistent with the current row group/frequency state.
- Multiple rapid SOP data-change events coalesce into a single refresh fanout cycle.
- Verification baseline passes:
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Rollback:
- Revert SOP row dynamic-refresh debounce and SOP emit coalescing in `freqinout/gui/sop_tab.py`.
- Revert main-window SOP fanout coalescing in `freqinout/gui/main_window.py`.

### 1.108 Addendum (2026-02-27): Active HF SOP Conflict Scan Caching

Problem:
- Active HF SOP conflict scans can take hundreds of milliseconds and are called from multiple UI flows.
- Re-running full scans back-to-back causes avoidable latency and UI churn while no underlying SOP data has changed.

Impacted files and failure modes:
- `freqinout/core/sop_manager.py`
  - Failure mode: repeated `collect_active_hf_conflicts()` calls recompute full profile conflict sets each time.
  - Failure mode: stale conflict results if cache invalidation is incomplete after SOP/profile/policy edits.
- `freqinout/gui/daily_schedule_tab.py`
  - Failure mode: prompt after HF schedule save could read a stale cached result unless explicitly refreshed.

Scope:
- Add a short-lived in-memory cache in `SOPManager.collect_active_hf_conflicts()`:
  - conservative TTL to collapse burst calls,
  - explicit `force_refresh` path for correctness-critical call sites.
- Add targeted cache invalidation after SOP/profile/layer/policy mutators that can change active HF conflict outcomes.
- Update HF schedule post-save conflict prompt path to use `force_refresh=True`.

Constraints:
- Preserve existing conflict semantics and ordering.
- Keep implementation low-risk and local to SOP conflict scan path.
- Do not add blocking operations to GUI paths.

Acceptance criteria:
- Back-to-back active HF conflict scans within cache TTL avoid full recomputation.
- SOP/profile/layer/policy edits invalidate cached active HF conflict results.
- HF schedule save prompt uses a fresh recomputed active HF conflict scan.
- Verification baseline passes:
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Rollback:
- Revert active HF conflict cache and invalidation hooks in `freqinout/core/sop_manager.py`.
- Revert forced refresh usage in `freqinout/gui/daily_schedule_tab.py`.

### 1.109 Addendum (2026-02-27): Daily Schedule SOP Conflict Indicator Consistency

Problem:
- Daily Schedule SOP status indicators can show `Conflict` while SOP Builder shows no conflicts for the same active SOP.
- Root cause: Daily status panel used upcoming-action schedule alignment (`aligned=False`) while SOP Builder uses frequency/time conflict detection (`detect_action_conflicts` via `collect_active_hf_conflicts`).

Impacted files and failure modes:
- `freqinout/gui/daily_schedule_tab.py`
  - Failure mode: top SOP status panel reports false-positive conflict states caused by alignment heuristics rather than SOP conflict rules.

Scope:
- Change Daily Schedule SOP status row conflict source to `SOPManager.collect_active_hf_conflicts`.
- Keep the panel’s active/inactive behavior and button workflow unchanged.
- Update issue-summary text to reflect conflict details from active conflict rows.

Constraints:
- Preserve existing conflict semantics (same-frequency overlaps are not conflicts).
- Keep panel refresh behavior non-blocking and reuse existing SOP conflict caching.

Acceptance criteria:
- If SOP Builder reports no active HF conflicts, Daily Schedule SOP status panel does not show `Conflict` for that profile.
- If active HF conflicts exist, Daily Schedule SOP status panel shows `Conflict` with a concise summary and conflict count.
- Verification baseline passes:
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Rollback:
- Revert Daily Schedule SOP status row conflict-source changes in `freqinout/gui/daily_schedule_tab.py`.

### 1.110 Addendum (2026-02-27): Daily Schedule SOP Overlay Eligibility and Same-Frequency Conflict Suppression

Problem:
- Daily Schedule can still show invalid SOP-driven conflicts after restart.
- Root causes:
  - SOP overlay rows were expanded from profile layer rows with flattened defaults (for example forcing `day_utc=ALL`, profile group), which can overstate active overlap windows.
  - Active conflict pair detection in Daily Schedule highlighted any time overlap, including same-band/frequency overlaps that are intentionally non-conflicting in SOP logic.

Impacted files and failure modes:
- `freqinout/gui/daily_schedule_tab.py`
  - Failure mode: active overlay rows include layer entries not eligible for current operating condition level.
  - Failure mode: same-frequency overlaps are shown as conflicts.

Scope:
- Overlay row loading:
  - preserve layer `day_utc`, `recurrence`, `biweekly_offset_weeks`, `month_weeks`, and row-level `group_name`,
  - apply condition-level eligibility filtering for active SOP overlay rows.
- Conflict pair logic:
  - skip overlap pair creation when both rows share normalized `(band, frequency)`.

Constraints:
- Keep existing UI flow, row editing behavior, and conflict-highlighting UX intact.
- Avoid heavy recomputation in per-refresh conflict path.

Acceptance criteria:
- Daily Schedule no longer reports conflicts for SOP layer rows that are not eligible at current group condition level.
- Same-band/frequency time overlaps are not highlighted as conflicts in Daily Schedule active conflict detection.
- Verification baseline passes:
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Rollback:
- Revert SOP overlay eligibility/shape and same-frequency conflict suppression changes in `freqinout/gui/daily_schedule_tab.py`.

### 1.111 Addendum (2026-02-27): Keep SOP Out of Daily Grid While Preserving HF Auto-Adjust

Problem:
- Rendering active SOP rows directly in the Daily Schedule table adds UI and scheduler-adjacent overhead, and can create operator confusion when no actual frequency transition is needed.
- Desired behavior is simpler: Daily grid should represent HF schedule rows only; SOP should influence adjustment decisions without becoming schedule rows.

Impacted files and failure modes:
- `freqinout/gui/daily_schedule_tab.py`
  - Failure mode: SOP overlays appear as synthetic Daily rows and are interpreted as first-class schedule entries.
  - Failure mode: if overlays are removed without replacement diagnostics, `Auto-Adjust HF Around SOP` becomes unavailable.

Scope:
- Stop appending SOP overlays into the Daily Schedule table.
- Keep conflict resolution simple:
  - retain existing HF-vs-HF overlap detection for table rows,
  - add a direct HF-vs-active-SOP conflict scan (without injecting SOP rows),
  - keep `Auto-Adjust HF Around SOP (Reversible)` available when HF rows overlap active SOP windows on different frequencies.
- Keep adjusted HF rows under operating-group schedule semantics (no SOP synthetic group rows).

Constraints:
- No change to scheduler core frequency-transition rules.
- Keep same-frequency overlap behavior as non-conflict.
- Preserve reversible snapshot behavior for auto-adjust.

Acceptance criteria:
- Daily Schedule table contains HF schedule rows only after load/refresh.
- When no HF-vs-HF overlaps exist but HF-vs-SOP overlaps exist, Resolve Conflicts still offers SOP auto-adjust.
- Auto-adjust mutates only HF rows and continues to save reversible pre-adjust snapshots.
- Verification baseline passes:
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Rollback:
- Revert no-overlay + direct HF-vs-SOP conflict path changes in `freqinout/gui/daily_schedule_tab.py`.

### 1.112 Addendum (2026-02-27): HF/SOP Auto-Adjust Eligibility Fix + Daily Tab Transition Performance

Problem:
- `HF/SOP Conflicts` dialog could list clear overlaps but return `No eligible HF/SOP overlap pairs were available for auto-adjust`.
- Transition from SOP tab to Daily tab could feel slow due repeated heavyweight active-SOP conflict checks in routine UI state updates.

Impacted files and failure modes:
- `freqinout/gui/daily_schedule_tab.py`
  - Failure mode: auto-adjust eligibility derived from fragile precomputed fields that can drift from actual row/day blocker context.
  - Failure mode: routine button-state refresh invokes conflict checks that should be sourced from cached panel state.

Scope:
- Rebuild HF/SOP conflict collection from expanded HF and active SOP windows over a bounded horizon using shared SOPManager expansion semantics.
- Build auto-adjust blockers from concrete overlap day-segments and apply day-aware adjustment:
  - support `ALL`-day HF rows by splitting to day-specific rows when needed.
- Reduce Daily tab transition overhead:
  - avoid forcing heavy conflict refreshes on every panel refresh call,
  - use cached SOP conflict presence state for routine action-button eligibility.

Constraints:
- Preserve reversible auto-adjust snapshot behavior.
- Keep same-frequency overlap suppression.
- Keep Daily grid HF-only (no SOP row injection).

Acceptance criteria:
- When HF/SOP conflict dialog lists overlaps, Auto-Adjust no longer reports “no eligible pairs” for valid overlaps.
- `ALL`-day HF rows can auto-adjust around day-scoped SOP blockers by converting to day-specific rows as needed.
- Daily tab transition avoids repeated heavyweight SOP conflict scans in normal UI state updates.
- Verification baseline passes:
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Rollback:
- Revert HF/SOP conflict-entry and auto-adjust blocker reconstruction changes in `freqinout/gui/daily_schedule_tab.py`.
- Revert Daily panel/button-state SOP conflict caching adjustments in `freqinout/gui/daily_schedule_tab.py`.

### 1.113 Addendum (2026-02-27): Authoritative Daily HF/SOP Conflict Parity with SOP Builder

Problem:
- A conflict can appear in SOP Builder/Workbench but not in Daily Schedule conflict resolution.
- Root cause: Daily conflict collection relied on SOP-layer overlap heuristics instead of the same authoritative conflict details used by SOP Builder (`detect_action_conflicts`).

Impacted files and failure modes:
- `freqinout/core/sop_manager.py`
  - Failure mode: active HF conflict collector lacked optional detail payload for downstream parity consumers.
- `freqinout/gui/daily_schedule_tab.py`
  - Failure mode: Daily HF/SOP conflict collector could miss builder-detected conflicts due source/expansion mismatch.

Scope:
- Add optional detail-enabled active conflict collection path in `SOPManager`:
  - `collect_profile_conflicts(..., include_details=True)`
  - `collect_active_hf_conflicts(..., include_details=True)`
  - keep existing cache path for non-detailed calls only.
- Update Daily HF/SOP conflict collection to build blockers from authoritative active conflict `daily_details`, then map to current HF grid rows.

Constraints:
- Preserve non-detailed conflict collector caching behavior.
- Keep detailed conflict scan off hot UI paths (invoke during explicit conflict resolution flow only).
- Maintain same-frequency suppression and reversible auto-adjust behavior.

Acceptance criteria:
- Any active conflict shown in SOP Builder daily-conflict details is discoverable in Daily Schedule HF/SOP conflict resolution.
- Daily no longer misses builder-detected daily conflicts due alternate overlap heuristics.
- Verification baseline passes:
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Rollback:
- Revert optional detail collection in `freqinout/core/sop_manager.py`.
- Revert authoritative-details mapping logic in `freqinout/gui/daily_schedule_tab.py`.

### 1.114 Addendum (2026-02-27): Preserve HF Row Index `0` in Daily HF/SOP Conflict Mapping

Problem:
- Daily HF/SOP conflict detection can miss valid conflicts when the affected HF row is table row `0`.
- Root cause: `int(value or -1)` treats `0` as falsey and rewrites it to `-1`, causing row `0` to be dropped from conflict mapping, summary, and auto-adjust pairing.

Impacted files and failure modes:
- `freqinout/gui/daily_schedule_tab.py`
  - Failure mode: `_collect_hf_sop_conflict_entries` skips valid overlaps tied to HF row `0`.
  - Failure mode: `_auto_adjust_hf_around_sop_conflicts` cannot find eligible pair(s) for row `0`.
  - Failure mode: conflict summary text may display incorrect row mapping for row `0`.

Scope:
- Add safe row-index parsing helper that preserves valid integer `0`.
- Replace `int(... or -1)` row-index conversions in Daily HF/SOP conflict collection, summary, and auto-adjust code paths with the safe parser.

Constraints:
- Keep all existing conflict semantics unchanged (same-frequency suppression, authoritative detail source, reversible auto-adjust snapshots).
- Keep performance characteristics unchanged (constant-time parsing only).

Acceptance criteria:
- Daily conflict resolution detects SOP overlaps affecting HF row `0`.
- Auto-adjust no longer reports “No eligible HF/SOP overlap pairs” when the overlap is on HF row `0`.
- Verification baseline passes:
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Rollback:
- Revert row-index parsing helper and call-site replacements in `freqinout/gui/daily_schedule_tab.py`.

### 1.115 Addendum (2026-02-27): Keep HF/SOP Auto-Adjust Arithmetic in UTC (Local View Safe)

Problem:
- In Local time display mode, Daily HF/SOP auto-adjust can report no row changes even with valid conflict blockers.
- Root cause: blocker windows are computed in UTC while row segment subtraction was using display-local row times.

Impacted files and failure modes:
- `freqinout/gui/daily_schedule_tab.py`
  - Failure mode: `_auto_adjust_hf_around_sop_conflicts` computes valid blockers but subtracts against local-time segments, resulting in false no-op outcomes.

Scope:
- Normalize row-map entries used by auto-adjust to UTC values via `_active_row_to_utc`.
- Keep UI display behavior unchanged by converting adjusted UTC rows back through `_entry_for_display` at render time.

Constraints:
- Do not change conflict detection criteria.
- Preserve reversible snapshot behavior and existing table metadata wiring.

Acceptance criteria:
- In Local display mode, auto-adjust mutates eligible HF rows when valid UTC blockers exist.
- In UTC display mode, behavior remains unchanged.
- Verification baseline passes:
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Rollback:
- Revert UTC normalization and display re-conversion changes in `_auto_adjust_hf_around_sop_conflicts` in `freqinout/gui/daily_schedule_tab.py`.

### 1.116 Addendum (2026-02-27): Daily Tab SOP Visibility Toggle + Low-Risk Load Performance Pass

Problem:
- Operators want clearer confirmation of active SOP windows from the Daily Schedule tab after HF rows are auto-adjusted around SOP.
- Daily tab load/edit feels slower than necessary because table `itemChanged` conflict work still runs during bulk rebuilds and startup does duplicate refresh passes.

Impacted files and failure modes:
- `freqinout/gui/daily_schedule_tab.py`
  - Failure mode: users cannot quickly confirm which active SOP windows are in effect from the Daily tab.
  - Failure mode: bulk row rebuilds (load/sort/auto-adjust) trigger repeated conflict recomputation through `itemChanged`, causing avoidable UI latency.
  - Failure mode: startup performs redundant SOP/resource refreshes after `_load_schedule()` already populated them.

Scope:
- Add a read-only Daily-tab SOP visibility control for active HF SOP windows:
  - visual only,
  - does not participate in scheduler persistence,
  - does not change scheduler source selection logic.
- Reduce Daily tab overhead by:
  - bypassing `itemChanged` conflict recompute while dirty tracking is suspended,
  - coalescing normal item change conflict refreshes through a short single-shot timer,
  - removing duplicate startup refresh calls already handled by `_load_schedule()`.

Constraints:
- Preserve existing scheduler correctness: runtime continues to read HF from `daily_schedule_tab` and SOP from `sop_schedule_layer`.
- Keep SOP visibility read-only and clearly non-editable.
- Avoid introducing heavy recurring work when SOP visibility is off.

Acceptance criteria:
- Daily tab can show active SOP windows on demand for operator clarity without affecting scheduler behavior.
- Bulk load/sort/auto-adjust no longer triggers repeated per-row `itemChanged` conflict recomputes.
- Initial Daily tab load performs fewer redundant refresh passes and remains behaviorally identical.
- Verification baseline passes:
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Rollback:
- Revert SOP visibility toggle and overlay rendering changes in `freqinout/gui/daily_schedule_tab.py`.
- Revert deferred/guarded item-change conflict refresh logic in `freqinout/gui/daily_schedule_tab.py`.

### 1.117 Addendum (2026-02-27): Inline Read-Only SOP Rows in Daily Schedule

Problem:
- Operators want active SOP windows visible in the Daily schedule timeline itself, not in a separate panel.
- These rows must improve clarity without becoming part of the editable/persisted HF schedule or affecting scheduler truth.

Impacted files and failure modes:
- `freqinout/gui/daily_schedule_tab.py`
  - Failure mode: synthetic SOP rows are mistaken for real HF rows and get saved to `daily_schedule_tab`.
  - Failure mode: synthetic SOP rows participate in delete/resource/conflict workflows, creating false actions or false conflicts.
  - Failure mode: toggling SOP visibility marks the HF schedule dirty even though no persisted HF rows changed.

Scope:
- Change the SOP visibility toggle to inject synthetic read-only SOP rows directly into the Daily schedule table.
- Use visible row markers:
  - `Source = SOP`
  - `Group Name = SOP:<group>`
- Keep synthetic rows display-only:
  - non-editable,
  - excluded from save/dirty-state persistence,
  - excluded from delete/move/resource actions,
  - excluded from HF-vs-HF conflict detection.
- Keep scheduler runtime unchanged (still reads HF from `daily_schedule_tab` and SOP from `sop_schedule_layer`).

Constraints:
- Preserve the existing authoritative HF-vs-SOP conflict path for Resolve Conflicts.
- Avoid restoring the prior behavior where Daily inline SOP rows looked editable or updateable.
- Keep row insertion/removal low-risk and compatible with existing sort/rebuild paths.

Acceptance criteria:
- Enabling the SOP visibility toggle inserts non-editable SOP rows inline in the Daily schedule table.
- Disabling the toggle removes only the synthetic SOP rows.
- Toggling SOP row visibility does not dirty the HF schedule and does not change what gets saved.
- Synthetic SOP rows do not create HF/HF conflicts and cannot be deleted or moved to resources.
- Verification baseline passes:
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Rollback:
- Revert inline SOP row insertion/removal and related guardrails in `freqinout/gui/daily_schedule_tab.py`.

### 1.118 Addendum (2026-02-27): Correct Bundled SitRep Net Resources from Verified Seasonal CSVs

Problem:
- The bundled SitRep net resource files delivered with FreqInOut contain incorrect schedule data.
- Verified replacement sources have been provided for Winter and Summer as CSV exports.

Impacted files and failure modes:
- `config/net_resources/sitrepnets-winter.json`
  - Failure mode: shipped Winter rows have incorrect day-of-week assignments versus verified source CSV.
- `config/net_resources/sitrepnets-summer.json`
  - Failure mode: shipped Summer rows have incorrect day-of-week assignments and multiple incorrect band assignments versus verified source CSV.
- `config/net_resources/sitrepnets-fall.json`
  - Failure mode: fallback file can continue serving stale Winter-equivalent data if not kept aligned with corrected Winter content.

Scope:
- Convert the provided verified CSVs into the bundled JSON resource format used by Net Resources bootstrap.
- Replace the shipped Winter and Summer bundled JSON rows with the verified data.
- Keep Fall aligned with corrected Winter as the current Winter fallback file.

Constraints:
- Preserve bundled JSON schema and row ordering.
- Do not change Net Resources import/bootstrap logic.
- Keep values normalized for shipped JSON compatibility (times, month-week formatting, frequency formatting).

Acceptance criteria:
- Bundled Winter and Summer JSON files match the verified CSV content.
- Bundled Fall JSON matches corrected Winter data for fallback consistency.
- Row counts remain stable and the JSONs remain importable by existing Net Resources bootstrap.
- Verification baseline passes:
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Rollback:
- Restore previous bundled resource JSON files under `config/net_resources/`.

### 1.119 Addendum (2026-02-27): Auto-Refresh Builtin Net Resource Sets on Existing Installs

Problem:
- Correcting the bundled Winter/Summer net resource JSON files fixes new builds and fresh bootstraps, but existing installs with populated `net_resources` tables will keep stale builtin rows unless the app refreshes them.

Impacted files and failure modes:
- `freqinout/gui/net_schedule_tab.py`
  - Failure mode: `net_resources` bootstrap only loads bundled JSONs when the table is empty, so corrected builtin resources never reach existing local installs after pull/build.

Scope:
- Add a one-time versioned builtin resource sync during Net Resources bootstrap.
- On version change:
  - remove existing builtin-tagged rows for each builtin resource set,
  - repopulate from the bundled JSON files,
  - persist a settings sync version marker.

Constraints:
- Preserve user-added/imported rows (only delete rows tagged `source_type='builtin'`).
- Keep bootstrap behavior unchanged for custom/migrated rows.
- Keep sync idempotent after the new version marker is written.

Acceptance criteria:
- After updating to the new code, existing installs refresh the bundled Winter/Summer builtin resource rows once during Net Resources bootstrap.
- New/fresh installs still receive the bundled builtin rows correctly.
- Repeated app launches do not reimport builtin sets once the sync version matches.
- Verification baseline passes:
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Rollback:
- Revert builtin sync versioning and refresh logic in `freqinout/gui/net_schedule_tab.py`.

### 1.120 Addendum (2026-02-27): Daily Effective HF/SOP Projection for Clear Runtime Fallbacks

Problem:
- The Daily tab currently shows the editable baseline HF rows and, optionally, inline SOP overlay rows. When an SOP window temporarily overrides an HF row and then ends, the operator must infer the resumed HF fallback segment instead of seeing the actual runtime timeline explicitly.

Impacted files and failure modes:
- `freqinout/gui/daily_schedule_tab.py`
  - Failure mode: operators can misread the active timeline and doubt which HF frequency resumes after an SOP override ends.
  - Failure mode: inline synthetic SOP rows add clutter to the editable table while still not explicitly showing resumed HF fallback spans.

Scope:
- Replace the prior inline "Show Active SOP Rows" behavior with a read-only "Effective Schedule" projection in the Daily tab.
- Build a derived HF/SOP runtime timeline for the next 7 days using the current HF rows, active HF SOP rows, and operating-group-backed runtime values.
- Show explicit projected segments in a read-only panel so resumed HF windows are visible without editing the baseline table.

Constraints:
- Do not persist projection rows into `daily_schedule_tab`.
- Do not change scheduler runtime logic; this is a visibility-only UX improvement.
- Keep the main HF table editable and responsive.
- Keep projection generation bounded and cache-friendly by limiting it to a short rolling horizon.

Acceptance criteria:
- When the projection is enabled, the Daily tab shows explicit read-only runtime segments for HF and active HF SOP windows.
- If an SOP window ends inside a longer HF row, the resumed HF segment is shown explicitly.
- Projection uses operating-group-backed frequency/mode values for display so it matches runtime behavior.
- The editable HF table is not polluted with synthetic rows.
- Verification baseline passes:
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Rollback:
- Restore the prior inline SOP overlay behavior in `freqinout/gui/daily_schedule_tab.py`.

### 1.121 Addendum (2026-02-27): Auto-Save After HF Auto-Adjust and Distinguish HF vs Other SOP Issues

Problem:
- `Auto-Adjust HF Around SOP` currently modifies only the in-memory Daily table. Until the user manually saves, the SOP conflict status still evaluates the persisted HF schedule and may continue to show a conflict even though the visible Daily rows are already refactored.
- The Daily HF SOP status also conflates HF/Daily overlaps with Net and SOP-only issues, so a resolved HF conflict can still appear as a generic red conflict.

Impacted files and failure modes:
- `freqinout/gui/daily_schedule_tab.py`
  - Failure mode: after successful auto-adjust, users still see a conflict state and may not realize a separate save is required.
  - Failure mode: remaining Net/SOP issues are not clearly distinguished from unresolved HF conflicts.

Scope:
- Auto-save the HF schedule immediately after a successful `Auto-Adjust HF Around SOP`.
- Refresh the Daily SOP status panel from a fresh conflict scan after the auto-save.
- Update Daily SOP status classification so:
  - `Conflict` means unresolved HF/Daily conflicts remain,
  - `Attention` means HF is clear but Net/SOP issues remain,
  - `Active` means no Daily/Net/SOP issues remain.

Constraints:
- Keep ordinary manual row edits on the existing explicit save workflow.
- Limit auto-save to the explicit auto-adjust conflict-resolution path.
- Preserve reversibility using the existing pre-adjust snapshot/journal flow.

Acceptance criteria:
- After successful auto-adjust, the adjusted HF rows are persisted automatically.
- The Daily SOP status updates immediately without requiring a manual save click.
- If HF conflicts are resolved and no other issues remain, the status shows clear/active.
- If HF conflicts are resolved but Net/SOP issues remain, the status no longer shows a generic HF conflict; it shows a review/attention state instead.
- Verification baseline passes:
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Rollback:
- Remove the auto-save step from the auto-adjust path and restore the prior generic conflict classification.

### 1.122 Addendum (2026-02-27): Persist HF Restore on Return to Normal

Problem:
- `Return to Normal` currently restores the pre-adjust HF snapshot only in the Daily tab UI. The adjusted/split HF rows remain persisted in `daily_schedule_tab`, so reload/restart can still show the post-SOP split schedule even after the journal says restore is complete.

Impacted files and failure modes:
- `freqinout/gui/daily_schedule_tab.py`
  - Failure mode: auto-adjust persists split HF rows, but return-to-normal only changes the in-memory table.
  - Failure mode: the session journal can mark the HF restore snapshot consumed even though the saved HF schedule was not actually restored.

Scope:
- Make `_restore_hf_from_session_snapshot()` persist the restored pre-adjust HF rows using the same HF save path used by normal save and auto-adjust.
- Only mark the journal restore snapshot as consumed after persistence succeeds.

Constraints:
- Preserve the overwrite-protection check against `post_adjust_signature`.
- Keep the existing reversible session-journal model.
- If persistence fails, leave the journal snapshot available so the restore can be retried.

Acceptance criteria:
- After `Return to Normal`, the saved Daily HF schedule matches the pre-adjust snapshot.
- Restart/reload does not bring back the split post-SOP rows.
- If restore persistence fails, the journal still retains the pending HF restore snapshot.
- Verification baseline passes:
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Rollback:
- Revert restore persistence changes in `freqinout/gui/daily_schedule_tab.py`.

### 1.123 Addendum (2026-02-28): Preserve `ALL` During HF Auto-Adjust When Result Is Uniform

Problem:
- `Auto-Adjust HF Around SOP` currently expands any changed `ALL`-day HF row into seven day-specific rows, even when the same subtraction applies on every UTC day.
- This adds unnecessary row clutter, makes the saved HF schedule harder to review, and makes the adjusted schedule look more complex than the scheduler behavior actually is.

Impacted files and failure modes:
- `freqinout/gui/daily_schedule_tab.py`
  - Failure mode: a daily SOP conflict against an `ALL` HF row creates seven weekday rows when two `ALL` rows would correctly express the same adjusted schedule.
  - Failure mode: users may infer the schedule logic changed by day even when the effective runtime pattern is identical every day.

Scope:
- Update the HF auto-adjust path so it only explodes an `ALL` HF row into weekday rows when the post-adjust segments differ by day.
- If the adjusted remaining segments are identical on all seven UTC days, persist the adjusted rows as `ALL` instead.

Constraints:
- Preserve the existing conservative weekday split when the remaining segments are not identical across all days.
- Keep all adjustment math in UTC.
- Do not change scheduler engine semantics; this is a normalization of the saved HF rows only.

Acceptance criteria:
- When an `ALL` HF row is adjusted by a daily SOP pattern that produces the same remaining segments every day, the saved adjusted HF rows remain `ALL`.
- When different days require different remaining segments, auto-adjust still creates day-specific rows.
- The auto-adjust summary reflects the actual number of additional split rows created.
- Verification baseline passes:
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Rollback:
- Restore the prior `ALL`-row auto-adjust branch that always expands changed rows into day-specific entries.

### 1.124 Addendum (2026-02-28): Enable Sorting in the Effective Schedule Panel

Problem:
- The Daily tab's `Effective Schedule (Read-Only)` panel currently renders a fixed projection table with sorting disabled.
- Users cannot click column headers to inspect the projected runtime by day, start time, source, or frequency, which makes the view less useful once more than a few segments are shown.

Impacted files and failure modes:
- `freqinout/gui/daily_schedule_tab.py`
  - Failure mode: operators must visually scan an unsortable projection table, which slows review of longer HF/SOP windows.
  - Failure mode: enabling sorting naively can cause unstable repaints or unwanted reordering while the table is being repopulated.

Scope:
- Enable interactive sorting on the Effective Schedule table.
- Use stable sort keys for displayed values so day/time/frequency sorting is useful.
- Preserve the active sort column/order when the projection refreshes.

Constraints:
- Keep the projection panel read-only.
- Avoid expensive per-row widget work; continue using plain table items.
- Do not change the projection contents or scheduler behavior.

Acceptance criteria:
- Users can sort the Effective Schedule table by clicking column headers.
- Sorting remains stable after the projection refreshes.
- Day, Start, End, and Freq columns sort in a user-meaningful order rather than raw string order.
- Verification baseline passes:
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Rollback:
- Disable sorting on `self.sop_overlay_table` and remove the custom sort-key item logic in `freqinout/gui/daily_schedule_tab.py`.

### 1.125 Addendum (2026-02-28): Disable Effective Schedule When No HF SOP Is Active

Problem:
- The Daily tab currently leaves the `Show Effective Schedule` control available even when no HF SOP profile is active.
- This creates an unnecessary control state because the projection is only relevant while an HF SOP is active.

Impacted files and failure modes:
- `freqinout/gui/daily_schedule_tab.py`
  - Failure mode: users can try to open a projection that is not meaningful after all HF SOP profiles have been deactivated.
  - Failure mode: the Effective Schedule panel can remain conceptually available after the last active HF SOP has been turned off, which adds doubt about whether the projection still matters.

Scope:
- Disable the `Show Effective Schedule` checkbox when no HF SOP profile is active.
- Automatically turn the checkbox off and hide the Effective Schedule panel when the last active HF SOP is deactivated.
- Re-enable the checkbox when at least one HF SOP profile becomes active again.

Constraints:
- Keep the control visible in the layout to avoid header reflow.
- Do not change the projection logic itself when an HF SOP is active.
- Keep the behavior driven by the existing HF SOP status refresh path.

Acceptance criteria:
- With no active HF SOP profile, `Show Effective Schedule` is disabled and the Effective Schedule panel is hidden.
- If the checkbox was on and the last active HF SOP is deactivated, it is turned off automatically.
- When an HF SOP profile becomes active again, the checkbox is enabled again.
- Verification baseline passes:
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Rollback:
- Remove the control-state gating and restore the prior always-available `Show Effective Schedule` checkbox behavior.

### 1.126 Addendum (2026-02-28): Reduce Daily HF Tab Refresh Overhead

Problem:
- The Daily HF tab is functionally correct, but several refresh paths do more work than necessary.
- The main overhead comes from repeated whole-table conflict scans and unnecessary Schedule Resources table rebuilds even when nothing relevant changed.

Impacted files and failure modes:
- `freqinout/gui/daily_schedule_tab.py`
  - Failure mode: the same active-conflict state is recomputed multiple times during adjacent UI updates (`highlight`, `resource action state`, sort/rebuild paths).
  - Failure mode: the 30-second refresh timer rebuilds the Schedule Resources table even when the resource data, filters, and active HF schedule have not changed.
  - Failure mode: some load/save/rebuild paths perform redundant post-sort refresh work, which makes the Daily tab feel slower than necessary.

Scope:
- Cache whole-table active HF conflict state and reuse it across adjacent UI refresh calls until the active schedule changes.
- Skip Schedule Resources table rebuilds when the resource source data, filters, timezone view, and active schedule have not changed.
- Remove redundant refresh work in high-traffic Daily tab rebuild/sort paths while keeping the visible behavior unchanged.

Constraints:
- Preserve all existing Daily HF tab behavior and visual results.
- Keep conflict scans fresh whenever the active schedule content or row ordering changes.
- Do not change scheduler logic, SOP logic, or resource data semantics.

Acceptance criteria:
- Daily tab interactions use cached whole-table conflict state where safe instead of recomputing it repeatedly.
- The 30-second timer does not rebuild the Schedule Resources table when no relevant inputs changed.
- Load/save/rebuild paths avoid redundant post-sort refresh work while leaving the resulting UI state unchanged.
- Verification baseline passes:
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`
  - `powershell -ExecutionPolicy Bypass -File .\tools\freqinout-db.ps1 status`

Rollback:
- Remove the Daily tab conflict/resource refresh caching changes in `freqinout/gui/daily_schedule_tab.py`.

### 1.127 Addendum (2026-02-28): Flatten Repeated Daily HF/SOP Conflict Rows in the Resolve Dialog

Problem:
- The Daily Schedule `Resolve Conflicts` dialog currently lists HF/SOP overlaps as one row per day occurrence.
- When an HF row and SOP window are effectively daily, the same conflict is repeated for each weekday, which makes the dialog noisy and harder to review.

Impacted files and failure modes:
- `freqinout/gui/daily_schedule_tab.py`
  - Failure mode: operators see seven nearly identical HF/SOP conflict entries for a daily overlap and must mentally collapse them.
  - Failure mode: the conflict list feels larger and more intimidating than the actual set of unique decisions the operator needs to make.

Scope:
- Flatten the HF/SOP conflict dialog display so repeated daily instances are grouped into one summarized entry.
- Reuse the existing day-scope formatter so grouped entries can show `All days` or a compact weekday list.

Constraints:
- Keep the underlying conflict detection and `Auto-Adjust HF Around SOP` logic unchanged.
- Limit the change to display formatting in the Daily tab conflict summary dialog.
- Preserve the current button actions and conflict resolution workflow.

Acceptance criteria:
- Daily HF/SOP conflict summaries no longer repeat the same visible HF/SOP conflict once per day when the overlap pattern is effectively daily.
- Grouped entries show a compact day scope, using `All days` when all seven weekdays are represented.
- Auto-adjust still operates on the full underlying conflict set.
- Verification baseline passes:
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Rollback:
- Remove the HF/SOP display grouping logic and restore the prior one-row-per-day summary rendering in `freqinout/gui/daily_schedule_tab.py`.

### 1.128 Addendum (2026-02-28): Remove Unused Suggested-Start Work From Daily Tab SOP Status Refresh

Problem:
- The Daily HF tab's SOP status panel calls the active HF conflict collector during tab initialization and refresh.
- That collector currently computes `suggested_start_utc` for each action, which is expensive and not used by the Daily tab status panel.
- This makes the Daily tab constructor substantially slower than necessary.

Impacted files and failure modes:
- `freqinout/core/sop_manager.py`
  - Failure mode: `collect_profile_conflicts()` always computes suggested starts even for callers that only need conflict summaries.
- `freqinout/gui/daily_schedule_tab.py`
  - Failure mode: Daily tab pays the full suggestion-computation cost during SOP status refresh even though it only needs counts/summaries.

Scope:
- Add an opt-out flag so SOP conflict collection can skip `suggest_non_conflicting_start()` when the caller does not need it.
- Use that lighter path in the Daily tab SOP status panel refresh.
- Trim the redundant dynamic SOP/resource refresh during the initial UI build so the tab does not do the same work twice on first construction.

Constraints:
- Preserve the existing default behavior for callers that do need suggested starts.
- Do not change conflict semantics, scheduler behavior, or the builder/workbench suggestion UX.
- Keep the change backward compatible by defaulting the new flag to the current behavior.

Acceptance criteria:
- Daily tab SOP status refresh no longer computes suggested starts it does not display.
- Other existing callers continue to receive `suggested_start_utc` unless they explicitly opt out.
- Daily tab constructor/open time improves materially on the same local data set.
- Verification baseline passes:
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Rollback:
- Remove the `include_suggestions` opt-out path and restore the prior always-compute suggested-start behavior.

### 1.129 Addendum (2026-02-28): Enrich SOP PDF Export With Configurable Intro/Outro Text and Full SOP Context

Problem:
- The current SOP PDF export only includes the derived daily and periodic action plans (plus optional operator rosters).
- Operators need the export to stand on its own as a field document, including optional narrative context and the core SOP configuration that drives the plan.
- There is currently no settings-backed place to define reusable export preamble/postamble text.

Impacted files and failure modes:
- `freqinout/gui/settings_tab.py`
  - Failure mode: no UI exists to author reusable SOP export preamble/postamble text, so operators must add narrative context outside the app.
  - Failure mode: if new fields are not persisted with the existing settings batch save flow, export text will appear to save but be lost on restart.
- `freqinout/gui/sop_tab.py`
  - Failure mode: PDF export omits critical context such as operating groups, SOP schedule rows, and SOP actions, forcing operators to cross-reference the app.
  - Failure mode: if free-form export text is injected unsafely, malformed content could break the generated HTML/PDF.

Scope:
- Add a dedicated `SOP Export` settings section with settings-backed `Preamble` and `Postamble` text fields.
- Extend the existing SOP PDF export HTML builder to include:
  - optional preamble text
  - SOP profile summary
  - operating groups table (including mode and starting offset)
  - SOP schedule layer table
  - SOP actions table
  - existing Daily Action Plan and Periodic Actions sections
  - optional postamble text
- Keep the existing PDF export workflow and output format (HTML rendered through `QTextDocument` and `QPrinter`).

Constraints:
- Reuse the current export pipeline; do not add a second export subsystem.
- Treat preamble/postamble as plain text and HTML-escape the content before rendering.
- Preserve existing export options and roster export behavior.
- Keep the change low-risk by reading existing settings/profile data only; do not introduce DB schema changes.

Acceptance criteria:
- Settings includes a visible `SOP Export` section where operators can edit and save preamble/postamble text.
- Preamble/postamble persist across restart through the normal settings save flow.
- SOP PDF export includes the new narrative and configuration sections without removing the existing daily/periodic action sections.
- Operating groups in the PDF include mode and FLDigi starting offset when configured.
- The PDF export remains valid when preamble/postamble are blank.
- Verification baseline passes:
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Rollback:
- Remove the `SOP Export` settings fields and revert `freqinout/gui/sop_tab.py` to the prior daily/periodic-only PDF export layout.

### 1.130 Addendum (2026-02-28): Scope SOP PDF Export Support Data to the Exported SOP Profiles

Problem:
- The enriched SOP PDF export now includes the required sections, but some supporting data is still broader than necessary.
- In particular, the `Operating Groups` table currently includes all configured operating groups, which makes the export less focused on the SOP being exported.
- The current section ordering also places supporting configuration ahead of the most directly SOP-specific sections.

Impacted files and failure modes:
- `freqinout/gui/sop_tab.py`
  - Failure mode: operators exporting one SOP must scan unrelated operating-group rows that are not used by the exported profile(s).
  - Failure mode: the document front-loads supporting context before the core SOP schedule/action data, reducing readability in the field.

Scope:
- Filter the `Operating Groups` section to only rows referenced by the exported SOP profile(s), using group and, when available, band/frequency cues from the exported profile data.
- Rename the section to make it clear that it is a scoped support section.
- Reorder the export so directly SOP-oriented sections (profile summary, schedule layer, actions) appear before supporting configuration and derived action views.

Constraints:
- Do not add new DB reads or schema changes.
- Reuse the existing settings/profile data already loaded for the export.
- Keep the export fast and deterministic by using in-memory filtering only.
- Preserve the existing PDF export workflow and the already-added preamble/postamble support.

Acceptance criteria:
- The support-data table no longer includes unrelated operating groups when exporting a narrow SOP scope.
- The document presents the most directly SOP-specific sections before supporting/derived sections.
- Export remains valid when no matching operating-group rows are found.
- Verification baseline passes:
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Rollback:
- Revert the export filtering/order changes in `freqinout/gui/sop_tab.py` and restore the broader `Operating Groups` section.

### 1.131 Addendum (2026-02-28): Remove Windows Printer Dependency From SOP PDF Export

Problem:
- The SOP PDF export currently uses `QPrinter` configured for PDF output.
- On Windows, `QPrinter` can still attempt to initialize the Win32 print engine (`CreateDC`) before the PDF output path is fully configured.
- This causes export failures on systems where the print subsystem is unavailable, aborted, or otherwise unstable, even though the user is only exporting to PDF.

Impacted files and failure modes:
- `freqinout/gui/sop_tab.py`
  - Failure mode: SOP PDF export triggers `QWin32PrintEngine::initialize: CreateDC failed` and aborts the export.
  - Failure mode: PDF export becomes dependent on printer availability, which is unnecessary for file-only PDF output.

Scope:
- Replace the SOP PDF export backend with a printer-independent PDF writer.
- Keep the existing file selection flow and HTML rendering through `QTextDocument`.
- Preserve the same page size and margin intent used by the current export.

Constraints:
- Do not change the export document content or options.
- Limit the change to the SOP PDF export path.
- Prefer a Qt-native PDF file writer that does not require a printer connection.

Acceptance criteria:
- SOP PDF export no longer initializes a Win32 printer device during PDF generation.
- Export continues to write a PDF file with the same document content and page sizing intent.
- Verification baseline passes:
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Rollback:
- Restore the prior `QPrinter`-based SOP PDF export path in `freqinout/gui/sop_tab.py`.

### 1.132 Addendum (2026-02-28): Include a FreqPlanner Snapshot in SOP PDF Export

Problem:
- Operators use the FreqPlanner view to understand the holistic weekly schedule at a glance.
- The current SOP PDF export includes configuration and derived action tables, but it does not include the planner-style weekly grid that shows UTC-to-local alignment and overall schedule precedence.
- This forces operators to mentally reconstruct the live schedule from multiple sections.

Impacted files and failure modes:
- `freqinout/gui/sop_tab.py`
  - Failure mode: exported SOP documents omit the most recognizable “big picture” schedule view, reducing field usability.
- `freqinout/gui/freq_planner_tab.py`
  - Failure mode: if export uses a separate planner algorithm, the PDF can drift from the visible FreqPlanner tab.

Scope:
- Add a `FreqPlanner Snapshot` section to the SOP PDF export.
- Reuse the existing FreqPlanner table rendering logic for the export snapshot.
- Scope the SOP overlay in that snapshot to the SOP profile(s) being exported while still using the current HF and Net schedules for the holistic view.
- Present the snapshot in a compact PDF-friendly table with UTC and Local time columns.

Constraints:
- Keep the existing export workflow and document generation path.
- Prefer reuse of the current FreqPlanner logic over reimplementing planner precedence a second time.
- Limit extra work to user-invoked export time; do not change tab-open performance.

Acceptance criteria:
- SOP PDF export includes a weekly FreqPlanner-style table with UTC and Local time columns plus Sunday-Saturday columns.
- The planner snapshot reflects the current HF and Net schedules and overlays the exported SOP profile(s).
- The section renders safely even if the planner snapshot cannot be built, using a clear empty/error message instead of aborting the export.
- Verification baseline passes:
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Rollback:
- Remove the `FreqPlanner Snapshot` section and related export helper code from `freqinout/gui/sop_tab.py`.

### 1.133 Addendum (2026-02-28): Keep Archived VarAC BBS Files Visible and Actionable in Messages

Problem:
- VarAC BBS files that are moved into the configured BBS Archive folder are no longer represented correctly in the Messages tab.
- Operators can still have stale awareness of those files, but the viewer path no longer resolves once the file has moved, which makes `View` fail.
- Operators also need a clear archived indicator so they can identify older BBS files and delete them from the archive when desired.

Impacted files and failure modes:
- `freqinout/gui/message_viewer_tab.py`
  - Failure mode: archived BBS files are not scanned from the archive folder, so the row set does not track the file’s real location after archive.
  - Failure mode: the UI does not clearly distinguish live BBS files from archived BBS files.
  - Failure mode: archived BBS rows can incorrectly still present an `Archive` action even though the file is already in the archive.

Scope:
- Include the configured VarAC BBS Archive directory as an additional BBS scan root in the Messages tab.
- Mark archived BBS rows clearly in the table and file info display.
- Remove the `Archive` action from already-archived BBS rows while keeping `View` and `Delete`.
- Preserve file delete eligibility for archived BBS files.
- Preserve the file read/flag state when a BBS file is manually archived.

Constraints:
- Do not add a new DB schema or message type.
- Keep archived files in the existing `BBS` message grouping/filter model.
- Use the actual archive file path for display and delete, rather than stale references.

Acceptance criteria:
- Archived BBS files appear in the Messages tab from the configured archive directory.
- Archived BBS rows are clearly labeled as archived.
- Archived BBS rows do not show the `Archive` action.
- `View` opens the archived file from its archive path.
- `Delete` remains available for archived BBS rows (single-row and bulk delete paths).
- Manual archive preserves the row’s read/flag state when it reappears from the archive path.
- Verification baseline passes:
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Rollback:
- Remove archive-directory scanning and archived-row tagging from `freqinout/gui/message_viewer_tab.py`.

### 1.134 Addendum (2026-02-28): v1.2.0 Release Readiness for SOP Guidance, Perf Observability, and Build Prep

Problem:
- `v1.2.0` includes substantial SOP workflow changes across `SOP Builder`, `HF Daily`, `HF Nets`, and the SOP PDF export path.
- The current `docs/guide.html` references those surfaces, but the SOP operating flow is still too terse for new users who need explicit step-by-step guidance.
- Recent low-risk performance work improved SOP-related paths, but the remaining release tooling and perf instrumentation still leave gaps:
  - `HF Daily`, `HF Nets`, `SOP Builder`, and `FreqPlanner` tab activation/rebuild paths are not fully observable in perf spans.
  - `tools/perf_benchmark.py` auto mode can mix `freqinout.log` with `perf_metrics.log`, which can produce misleading summaries and can clear normal app logs when `reset-log` is used.
  - `release_builder.py` does not run the repository verification baseline end-to-end because it omits `python -m compileall freqinout`.

Impacted files and failure modes:
- `docs/guide.html`
  - Failure mode: operators do not understand the intended SOP flow across builder, conflict review, auto-adjust, effective schedule review, and return-to-normal restore.
- `docs/perf-baseline.md`
  - Failure mode: the documented perf workflow under-tests the newly critical SOP-related UI paths.
- `freqinout/gui/daily_schedule_tab.py`
  - Failure mode: re-entering `HF Daily` can do avoidable refresh work even when no schedule-resource data changed.
- `freqinout/gui/net_schedule_tab.py`
  - Failure mode: missing activation spans make Net/SOP conflict-path regressions harder to validate.
- `freqinout/gui/sop_tab.py`
  - Failure mode: missing activation spans make SOP Builder open-path regressions harder to validate.
- `freqinout/gui/freq_planner_tab.py`
  - Failure mode: hidden FreqPlanner rebuilds can add avoidable SOP-change fanout cost, and the tab lacks direct perf visibility.
- `freqinout/gui/main_window.py`
  - Failure mode: SOP data fanout can trigger unnecessary hidden FreqPlanner rebuilds.
- `tools/perf_benchmark.py`
  - Failure mode: default summaries can overstate or misstate hotspots by combining stale normal logs with dedicated perf logs.
  - Failure mode: `reset-log` can clear ordinary app logs unintentionally.
- `release_builder.py`
  - Failure mode: release-helper execution can pass preflight while still missing the required compile baseline.
- `docs/release-checklist.md`
  - Failure mode: release verification steps do not explicitly require compile and SOP-focused perf smoke checks.
- `CHANGELOG.md`
  - Failure mode: release notes omit the release-readiness and operator-guidance improvements shipped in `1.2.0`.

Scope:
- Expand `docs/guide.html` SOP workflow guidance with explicit step-by-step operator instructions covering:
  - building an HF SOP,
  - reviewing builder conflicts,
  - handling HF Daily and HF Net conflicts,
  - using `Show Effective Schedule`,
  - understanding auto-adjust autosave and return-to-normal restore.
- Extend perf documentation to cover the critical `v1.2.0` SOP-related UI paths.
- Add low-risk perf observability for `HF Daily`, `HF Nets`, `SOP Builder`, and `FreqPlanner`.
- Avoid unnecessary hidden `FreqPlanner` rebuilds during SOP data fanout.
- Avoid unnecessary forced `HF Daily` schedule-resource refresh work on simple tab activation when data is unchanged.
- Fix `tools/perf_benchmark.py` auto-log handling so default summaries and resets prefer dedicated perf logs.
- Update release-helper documentation/scripts to include the compile baseline and SOP-focused perf smoke steps.

Constraints:
- Preserve current functional behavior; this pass is release hardening, documentation, and low-risk performance work only.
- Keep scheduler logic and SOP conflict semantics unchanged.
- Avoid broad refactors or any DB schema/data changes.

Acceptance criteria:
- `docs/guide.html` provides a clear, step-by-step SOP workflow that a new operator can follow without inferring cross-tab behavior.
- `HF Daily`, `HF Nets`, `SOP Builder`, and `FreqPlanner` emit useful perf spans for tab activation or rebuild paths.
- Hidden `FreqPlanner` tabs are marked dirty and refreshed on activation instead of rebuilding immediately on every SOP-change fanout.
- `HF Daily` tab activation avoids forced schedule-resource repopulation when the cached resource snapshot is still valid.
- `tools/perf_benchmark.py` default `summarize` and `reset-log` behavior use dedicated perf logs by default and do not unintentionally aggregate/clear standard app logs.
- `release_builder.py` runs the compile baseline unless explicitly skipped.
- Release docs/checklists mention SOP-focused perf validation for `v1.2.0`.
- Verification baseline passes:
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Rollback:
- Revert the guide/checklist/perf-baseline wording changes and remove the new perf/logging helper behavior from the touched files.

### 1.135 Addendum (2026-03-03): Shared Temporary Schedule Hold UX for QSY and Suspend Actions

Problem:
- Temporary scheduler holds are currently inconsistent across UI surfaces:
  - the main sidebar `Suspend Schedule` action is hardcoded to 30 minutes and does not show a live countdown,
  - `HF Daily`, `FLDigi Net Control`, and `JS8Call Net Control` each maintain their own QSY pause wording/state,
  - `ControlFreq` manual `QSY Now` does not follow the same timed-hold model,
  - off-schedule and VarAC wait prompts still hardcode `Pause Sched. 30 Min`.
- The current hold UI also leaves room for operator doubt because the time remaining and impending automatic resume are not presented consistently.

Impacted files and failure modes:
- `freqinout/gui/qsy_helper.py`
  - Failure mode: hold duration presets, persistence, and countdown state continue to diverge across tabs.
- `freqinout/core/scheduler_engine.py`
  - Failure mode: expired temporary holds can leave stale manual-QSY state until an explicit manual resume, defeating automatic resume expectations.
- `freqinout/gui/main_window.py`
  - Failure mode: the sidebar status panel can continue to show stale suspended state and does not expose consistent hold-duration selection.
- `freqinout/gui/daily_schedule_tab.py`
  - Failure mode: `QSY` hold controls remain fixed-duration and do not visually indicate imminent automatic resume.
- `freqinout/gui/fldigi_net_control_tab.py`
  - Failure mode: this tab retains a bespoke 5-minute modal extension prompt that does not match the rest of the app.
- `freqinout/gui/js8call_net_control_tab.py`
  - Failure mode: this tab remains fixed-duration and inconsistent with the shared hold UX.
- `freqinout/gui/controlfreq_tab.py`
  - Failure mode: `QSY Now` remains a separate indefinite manual-QSY path instead of participating in the same timed-hold workflow.

Scope:
- Introduce a shared temporary-hold duration model with presets: `30`, `60`, `90`, `120` minutes.
- Persist the last-selected hold duration for reuse across sessions.
- Apply the shared hold duration model to:
  - sidebar `Suspend Schedule`,
  - `HF Daily` QSY,
  - `FLDigi Net Control` QSY,
  - `JS8Call Net Control` QSY,
  - `ControlFreq` manual QSY actions,
  - off-schedule and VarAC wait pause actions.
- Show a live countdown anywhere an active hold is surfaced.
- Add a non-modal visual warning state as automatic resume approaches (warning and critical thresholds), without introducing new blocking dialogs.
- Keep scheduler storage/truth as the existing `schedule_suspend_until` UTC timestamp.

Constraints:
- Preserve the existing scheduler persistence model (`schedule_suspend_until`) and avoid DB schema changes.
- Keep the implementation low-risk by centralizing hold helpers rather than duplicating timing logic per tab.
- Do not add new blocking modal prompts for near-expiry warnings.
- Ensure expired holds clear stale manual-QSY state so automatic resume behaves as users expect.

Acceptance criteria:
- Users can choose `30`, `60`, `90`, or `120` minutes anywhere the app offers temporary QSY/Suspend control.
- The selected hold duration persists across sessions and becomes the default reused by other QSY/Suspend entry points.
- The main sidebar, `HF Daily`, `FLDigi Net Control`, `JS8Call Net Control`, and `ControlFreq` all show a live hold countdown while active.
- As resume approaches, active hold controls visibly shift into warning/critical styling without a modal interruption.
- Off-schedule and VarAC wait pause actions use the same shared duration model instead of a hardcoded 30-minute pause.
- When a timed hold expires, the scheduler returns to normal enforcement without requiring a separate manual `Resume Schedule`.
- Verification baseline passes:
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Rollback:
- Remove the shared hold helpers, revert the touched UI surfaces to their prior fixed-duration controls, and restore the previous scheduler suspend-expiry behavior.

### 1.136 Addendum (2026-03-03): Shared Near-Real-Time Hold State Broadcast

Problem:
- Temporary schedule hold countdowns are currently derived from the same stored state, but each surface updates on its own timer or local refresh path.
- `ControlFreq` feels more responsive because it does immediate local action refreshes, while `HF Daily`, `FLDigi NCS`, `JS8 NCS`, and the left-rail status can lag until their next polling tick.
- Repeated per-surface hold polling risks UI drift and unnecessary repeated settings/cache reads.

Impacted files and failure modes:
- `freqinout/gui/qsy_helper.py`
  - Failure mode: hold state remains purely pull-based, requiring each surface to resample independently.
- `freqinout/gui/main_window.py`
  - Failure mode: the left-rail status remains on a slower independent timer and cannot drive immediate synchronized hold updates.
- `freqinout/gui/controlfreq_tab.py`
  - Failure mode: `ControlFreq` continues to poll/read hold state during unrelated frequency refreshes instead of using a pushed snapshot.
- `freqinout/gui/daily_schedule_tab.py`
  - Failure mode: its 1-second clock timer continues to recompute hold state locally.
- `freqinout/gui/fldigi_net_control_tab.py`
  - Failure mode: its 1-second timer continues to recompute hold state locally.
- `freqinout/gui/js8call_net_control_tab.py`
  - Failure mode: its 1-second timer continues to recompute hold state locally.

Scope:
- Introduce a single shared GUI-layer hold state broadcaster owned by `MainWindow`.
- Emit immediate push updates on hold start, resume, and hold-duration default changes.
- Run a single 1-second shared hold tick only while a hold is active.
- Update the left-rail status and all hold-aware tabs from the pushed snapshot instead of each tab polling the setting every second.
- Keep existing clock timers for time labels, but remove per-tick hold-state polling from tab-local timers.

Constraints:
- Preserve the existing `schedule_suspend_until` persistence model.
- Keep per-tick work limited to lightweight label/button updates.
- Avoid adding heavy cross-tab refreshes, table rebuilds, or full status recomputation on every second.

Acceptance criteria:
- A hold started from any hold-aware surface is reflected across all other hold-aware surfaces immediately, without waiting for their next local polling interval.
- While a hold is active, all displayed countdowns stay in sync to the same minute bucket.
- Tab-local clock timers no longer recompute hold state every second.
- Hold-duration preset changes made in one hold-aware surface propagate quickly to the others without requiring a full tab refresh.
- Focused hot-path benchmark remains low-risk after the change:
  - no material regression in the per-call cost of hold-related UI update methods.
- Verification baseline passes:
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Rollback:
- Remove the shared broadcaster and restore the prior per-surface polling model.

### 1.137 Addendum (2026-03-04): Persistent Messages Type Exclusion Filter

Problem:
- The Messages tab currently supports inclusive filters (`MSG Type`, `Status`, `From`, `To`, search) but does not let users keep noisy message types out of the default view.
- Operators want to hide multiple message types by default, while still being able to temporarily view a hidden type by selecting it in the existing `MSG Type...` filter.

Impacted files and failure modes:
- `freqinout/gui/message_viewer_tab.py`
  - Failure mode: hidden message types are not persisted, or the exclusion logic conflicts with the existing inclusive type filter and unexpectedly hides explicitly selected rows.

Scope:
- Add a persistent Messages-tab control for excluding multiple message types from the default view.
- Store the hidden type selections inside the existing `message_viewer` settings blob.
- Apply exclusions only when the `MSG Type...` filter is not explicitly selecting a specific type.
- If the user selects a specific message type in `MSG Type...`, that selection overrides the exclusion list for that view only.
- Keep `Clear Filters` limited to the temporary inclusive filters/search and do not clear the persistent hidden-type list.
- Match the exclusion labels to the existing visible message-type labels, including aliases such as `Spotter` and `SitRep/<subtype>`.

Constraints:
- Preserve existing message filtering, sorting, and row rendering behavior aside from the new exclusion path.
- Keep the implementation in-memory and low-cost; do not add DB schema changes.
- Avoid introducing a separate modal configuration flow for this feature.

Acceptance criteria:
- Users can hide multiple message types from the default Messages view.
- Hidden message types persist across restart.
- Explicitly selecting a message type in `MSG Type...` shows that type even if it is hidden by default.
- `Clear Filters` resets only the temporary filters/search and leaves hidden message types intact.
- Verification baseline passes:
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Rollback:
- Remove the persistent hidden-type control and revert Messages filtering to the prior inclusive-only behavior.

### 1.138 Addendum (2026-03-05): ControlFreq Hero/Next-Change Accuracy

Problem:
- Operators observed `ControlFreq` showing stale hero frequency after a successful manual `QSY + Hold`.
- `Next Change` text could display the current scheduled frequency instead of the upcoming frequency at the next transition time.

Impacted files and failure modes:
- `freqinout/core/scheduler_engine.py`
  - Failure mode: status summary lacks next-transition frequency preview, forcing UI to guess from current entry.
- `freqinout/gui/controlfreq_tab.py`
  - Failure mode: next-change row uses current scheduled frequency for display; hero resync path can miss immediate post-QSY visual updates.

Scope:
- Add a scheduler status field for next-transition frequency preview (with operating-group override semantics).
- Update `ControlFreq` next-change rendering to use scheduler-provided next frequency preview.
- Tighten `ControlFreq` hero resync behavior after manual `QSY + Hold` so the displayed hero frequency follows the applied frequency promptly.

Constraints:
- Preserve scheduler behavior and frequency-control side effects; this is display/preview correctness only.
- Keep refresh-path cost low and avoid introducing new blocking UI work.

Acceptance criteria:
- After manual `QSY + Hold`, `ControlFreq` hero frequency display updates promptly to the applied frequency.
- `Next Change` shows the frequency expected at the upcoming transition time, not the current schedule frequency.
- Verification baseline passes:
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Rollback:
- Remove scheduler next-frequency preview field and revert `ControlFreq` next-change/hero logic to prior behavior.

### 1.139 Addendum (2026-03-05): Theme-Aware Popup Contrast Hardening

Problem:
- Modal popups (`QMessageBox` and other dialogs) can visually blend into the main app window, especially in dark theme.
- Operators report popups are hard to see quickly during active workflows.

Impacted files and failure modes:
- `freqinout/gui/theme.py`
  - Failure mode: dialog and message-box surfaces inherit near-identical background to the main window, reducing visual separation and action clarity.

Scope:
- Improve popup contrast at the global theme layer only.
- Introduce a distinct dialog/message-box background and stronger border/elevation cues for both light and dark themes.
- Keep text and button contrast accessible and consistent with existing palette semantics.
- Do not change popup behavior, flow, or per-tab logic.

Constraints:
- Preserve existing application themes and button role colors.
- Keep the change low-risk and centralized (`theme.py` only).
- Avoid introducing modal behavior changes or new dialog classes in this pass.

Acceptance criteria:
- Popups are visually distinct from the main window in both light and dark themes.
- Default/confirm actions inside popups remain easy to identify.
- Existing popup logic and workflows remain functionally unchanged.
- Verification baseline passes:
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Rollback:
- Revert popup-specific stylesheet rules in `freqinout/gui/theme.py`.

### 1.140 Addendum (2026-03-14): Reliability Baseline Phase 1 (FLRig Port + Runtime Path Consistency)

Problem:
- Review and roadmap analysis identified several confirmed reliability defects in the current runtime:
  - `FLRig XMLRPC Port` is shown in Settings but is not persisted on save and is not applied at next startup.
  - Runtime data uses `get_config_dir()`, while logging and DB utility scripts resolve different roots, causing misleading status/backup behavior and support confusion.
- These defects create avoidable operator risk during live HF workflows because the app can appear configured correctly while controlling or inspecting the wrong endpoint/profile.

Impacted files and failure modes:
- `freqinout/gui/settings_tab.py`
  - Failure mode: user changes `FLRig XMLRPC Port`, clicks save, restarts, and the UI/runtime silently fall back to `12345`.
- `freqinout/gui/main_window.py`
  - Failure mode: startup always creates `FLRigClient()` with default endpoint instead of the saved endpoint.
- `freqinout/core/logger.py`
  - Failure mode: logs go to a different profile root than the app databases, complicating troubleshooting and profile isolation.
- `tools/db_schema.py`
  - Failure mode: DB admin helpers inspect repo-local `config/` instead of the actual runtime AppData profile.
- `tools/freqinout_db.py`
  - Failure mode: backup/vacuum/status commands operate on the wrong DB files.

Scope:
- Implement Phase 1 only:
  - persist the `FLRig XMLRPC Port` setting on save
  - use the saved FLRig port during startup client construction
  - align log path resolution with the same runtime config root used by the app
  - align DB wrapper/tool runtime DB paths with the same runtime config root used by the app.
- Add targeted regression coverage for the persistence/path changes.

Out of scope:
- Multi-instance profile isolation and lock scoping
- Background-ingest worker/thread refactor
- New endpoint UI for FLRig/FLDigi/VarAC host fields
- Scheduler behavior changes

Phased plan:
- Phase 1: settings/path consistency and regression coverage.
- Phase 2: explicit profile identity for config/log/cache/lock separation and safe parallel instances.
- Phase 3: instance-bound health checks and richer radio endpoint configuration.
- Phase 4: move ingest/scans off UI hot paths and reduce polling/logging churn.

Constraints:
- Preserve current single-profile behavior for users who do not opt into custom profile paths.
- Do not change scheduler enforcement rules, NCS workflows, or DB schemas in this phase.
- Keep rollback simple by isolating edits to settings persistence, startup wiring, and tool/log path helpers.

Acceptance criteria:
- Changing `FLRig XMLRPC Port` in Settings persists the new value and reloads it on restart.
- Main-window startup constructs `FLRigClient` using the saved port value.
- `freqinout.core.logger` resolves its log directory from the same runtime config root as `get_config_dir()`, including `FREQINOUT_CONFIG_DIR`.
- `python tools/freqinout_db.py status|backup|vacuum` and PowerShell wrapper equivalents operate on runtime profile DB files instead of repo-local `config/`.
- No regression in existing verification baseline:
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`
  - `powershell -ExecutionPolicy Bypass -File .\tools\freqinout-db.ps1 status`

Rollback:
- Revert:
  - `freqinout/gui/settings_tab.py`
  - `freqinout/gui/main_window.py`
  - `freqinout/core/logger.py`
  - `tools/db_schema.py`
  - `tools/freqinout_db.py`
  - related targeted tests

### 1.141 Addendum (2026-03-14): Reliability Baseline Phase 2 (Background Ingest Workerization)

Problem:
- `BackgroundIngestController` currently uses `QTimer`, but each timeout runs real ingest work inline on the GUI thread.
- That violates the non-blocking UI guardrail and is a likely cause of startup and tab-latency issues already captured in the roadmap review.
- The current controller also keeps a shared `SettingsManager` / `MessageIngestor`, which cannot be safely reused across threads because settings persistence is backed by a thread-affine SQLite connection.

Impacted files and failure modes:
- `freqinout/core/background_ingest.py`
  - Failure mode: timer callbacks block the UI event loop while scanning logs, reading SQLite DBs, and writing checkpoints.
  - Failure mode: naive workerization could reuse the GUI-thread settings connection on a background thread and trigger SQLite cross-thread errors.
- `freqinout/gui/main_window.py`
  - Failure mode: shutdown could become sticky if the ingest worker requires a blocking wait.

Scope:
- Keep the existing timer cadence and startup staggering unchanged.
- Change timer callbacks so they submit ingest jobs to a serialized background worker instead of executing inline on the GUI thread.
- Use fresh worker-local settings/ingest helper instances inside background jobs.
- Suppress duplicate submissions for a job that is already queued or running.
- Add targeted regression coverage for worker-thread execution and duplicate-trigger suppression.

Out of scope:
- Stations Map ingest refactor
- Operator History / Message Viewer ingest refactor
- Schema cleanup
- New ingest features

Constraints:
- Preserve current ingest behavior and data outputs.
- Minimize DB contention by serializing worker jobs.
- Do not add blocking waits on the GUI thread during normal operation or shutdown.

Acceptance criteria:
- Triggering a background ingest path does not run the heavy ingest body on the GUI thread.
- Background ingest jobs execute one-at-a-time in a serialized worker.
- Existing timer intervals and staggered startup triggers remain intact.
- Re-triggering a job while it is already queued/running is ignored rather than queued again.
- Verification baseline passes:
  - `python -m pytest tests/test_background_ingest_phase2.py`
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Rollback:
- Revert the `BackgroundIngestController` workerization as a single unit if any ingest cadence or shutdown regressions appear.

### 1.142 Addendum (2026-03-14): Reliability Baseline Phase 3 (Operator Checkins Schema Ownership)

Problem:
- `operator_checkins` schema ownership is split across runtime DB init, `checkins_db`, the Operator History UI, and tool-side DDL metadata.
- The Operator History tab currently contains destructive rebuild logic (`DROP TABLE` / rename) on a UI path, while the tool schema still advertises an older `date_added` layout.
- That makes schema migration timing unpredictable and raises regression risk for live station databases.

Impacted files and failure modes:
- `freqinout/core/checkins_db.py`
  - Failure mode: schema repair logic is incomplete or inconsistent with other owners.
- `freqinout/core/db_initializer.py`
  - Failure mode: startup does not fully normalize legacy `operator_checkins` tables before UI access.
- `freqinout/gui/operator_history_tab.py`
  - Failure mode: opening the tab triggers table rebuilds on a GUI path.
- `tools/db_schema.py`
  - Failure mode: DB admin tooling recreates a stale `operator_checkins` schema.

Scope:
- Make `freqinout.core.checkins_db.ensure_operator_checkins_schema()` the authoritative schema owner for `operator_checkins`.
- Have startup DB initialization call that helper with one-time data repair enabled.
- Remove destructive schema ownership from the Operator History tab by delegating to the shared helper.
- Update tool-side `operator_checkins` DDL to the same unified schema.
- Add regression coverage for core migration, startup migration, and tool-created schema.

Out of scope:
- Broader DB schema deduplication beyond `operator_checkins`
- Operator History feature changes
- New schema versioning framework

Constraints:
- Preserve existing `operator_checkins` data during legacy migration.
- Keep rollback straightforward by changing only schema ownership paths, not downstream feature behavior.
- Avoid adding new blocking UI work.

Acceptance criteria:
- Startup migration upgrades legacy `operator_checkins` tables before UI access.
- Opening Operator History no longer owns destructive table rebuild logic.
- DB tooling creates the same unified `operator_checkins` schema the runtime expects.
- Verification baseline passes:
  - `python -m pytest tests/test_operator_checkins_schema_phase3.py`
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Rollback:
- Revert the schema-ownership consolidation together so runtime, UI, and tools return to the previous behavior consistently.

### 1.143 Addendum (2026-03-14): Reliability Baseline Phase 4 (Instance-Aware JS8/FLRig Status Badges)

Problem:
- Status badges currently treat any matching process name as proof that the configured software instance is available.
- In a multi-radio station, that can produce a false green badge when a different JS8Call or FLRig instance is running on another port/profile than the one FreqInOut is configured to use.

Impacted files and failure modes:
- `freqinout/core/software_status_service.py`
  - Failure mode: `status_snapshot()` reports `ok` based only on generic process detection rather than the configured endpoint.
- `freqinout/gui/settings_tab.py`
  - Failure mode: the running-status UI does not reflect an unsaved FLRig port value while the operator is editing settings.

Scope:
- Keep broad process detection behavior unchanged for launch sequencing and generic "is something running" checks.
- Make `status_snapshot()` endpoint-aware for JS8Call API and FLRig by combining process detection with configured host/port reachability.
- Report `warn` when a matching process exists but the configured endpoint is unreachable, indicating a likely instance/port mismatch.
- Allow the settings UI to probe FLRig status against the currently entered port value before save.
- Add targeted regression coverage for the new status logic.

Out of scope:
- Reworking launch orchestration readiness rules
- Full multi-instance process command-line matching
- FLDigi/VarAC status redesign

Constraints:
- Preserve existing launch behavior that depends on broad process-name checks.
- Keep status probing lightweight with caching.
- Do not require the UI to save settings before status badges can validate the typed JS8/FLRig endpoint values.

Acceptance criteria:
- JS8 status badges show `warn` when JS8Call is running but the configured API endpoint is unreachable.
- FLRig status badges show `warn` when FLRig is running but the configured XML-RPC endpoint is unreachable.
- JS8/FLRig badges show `ok` when the configured endpoint is reachable, even if generic local process detection is inconclusive.
- Settings status refresh probes FLRig against the current port field value.
- Verification baseline passes:
  - `python -m pytest tests/test_software_status_phase4.py`
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Rollback:
- Revert the endpoint-aware status logic and settings-tab override wiring together so status badges return to the previous process-only behavior consistently.

### 1.144 Addendum (2026-03-14): Reliability Baseline Phase 5 (Cold-Start VarAC Schema Baseline)

Problem:
- `StationsMap` reads `varac_callsign_stats` during startup, but `db_initializer.ensure_all_tables()` does not currently create the VarAC local tables.
- On a fresh profile or a profile that has never run VarAC ingest, startup logs avoidable `no such table: varac_callsign_stats` errors even though this is a normal cold-start state.

Impacted files and failure modes:
- `freqinout/core/db_initializer.py`
  - Failure mode: core DB initialization leaves required VarAC-derived local tables absent on cold start.
- `freqinout/core/varac_ingest.py`
  - Failure mode: VarAC schema creation is only reachable through ingest code instead of being available as a reusable initialization helper.

Scope:
- Expose a reusable helper to create the local VarAC tables in `freqinout_nets.db`.
- Call that helper from startup DB initialization so fresh profiles have the expected baseline schema before UI activation.
- Add regression coverage for the cold-start DB schema path.

Out of scope:
- Changing VarAC ingest behavior
- Changing Stations Map query logic
- New VarAC features or indexes beyond the existing local schema

Constraints:
- Preserve existing VarAC table definitions and index creation.
- Keep startup initialization idempotent and low-risk.

Acceptance criteria:
- `db_initializer.ensure_all_tables()` creates `varac_callsign_stats` and the rest of the VarAC local tables in a fresh runtime DB.
- Offscreen app startup no longer logs `no such table: varac_callsign_stats` on a cold profile.
- Verification baseline passes:
  - `python -m pytest tests/test_varac_schema_phase5.py`
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Rollback:
- Revert the reusable VarAC schema helper and the startup initializer call together so cold-start behavior returns to the previous lazy-creation model.

### 1.145 Addendum (2026-03-14): Reliability Baseline Phase 6 (Tracked Reliability Artifacts + FLDigi Endpoint Parity)

Problem:
- The completed reliability-phase spec and regression tests were still ignored locally, so the verification history for Phases 1-5 was not actually trackable in git.
- `SettingsTab` still uses deprecated `datetime.utcnow()` when seeding a default JS8 offset.
- FLDigi runtime control already supports configurable XML-RPC host/port, but Settings does not expose those fields and the software-status service still reports FLDigi from generic process detection only.
- In a multi-radio station, that leaves FLDigi with the same false-green risk Phase 4 fixed for JS8/FLRig, and operators cannot confirm or persist the intended FLDigi endpoint from the UI.

Impacted files and failure modes:
- `.gitignore`
  - Failure mode: completed reliability artifacts remain local-only and are easy to lose or omit from review.
- `freqinout/gui/settings_tab.py`
  - Failure mode: the deprecated UTC call starts warning/failing under newer Python releases.
  - Failure mode: operators cannot save or verify the intended FLDigi XML-RPC host/port from Settings.
- `freqinout/core/software_status_service.py`
  - Failure mode: `FLDigi` shows `ok` for the wrong running instance because endpoint reachability is not considered.
- `CHANGELOG.md`
  - Failure mode: the new reliability follow-up is undocumented relative to the prior phase entries.

Scope:
- Unignore `SPEC.md` and the existing Phase 1-5 targeted regression tests so they are trackable.
- Replace the remaining deprecated UTC call in Settings with timezone-aware UTC.
- Add Settings UI/load/save handling for `fldigi_host` and `fldigi_port`, preserving existing effective defaults for profiles that only configured `flrig_host`.
- Make `FLDigi` status endpoint-aware using the configured XML-RPC host/port, including unsaved Settings overrides during status refresh.
- Add targeted regression coverage for FLDigi endpoint persistence/status behavior and the UTC cleanup path.

Out of scope:
- New FLRig host UI
- Launch-orchestration readiness redesign
- Full multi-instance process command-line matching
- FLDigi control behavior changes beyond endpoint configuration/status probing

Constraints:
- Preserve existing runtime behavior for profiles that already rely on FLRig host fallback for FLDigi.
- Keep generic process-name detection available for other callers and launch orchestration.
- Do not require saving Settings before the FLDigi status badge can validate the typed endpoint.

Acceptance criteria:
- `SPEC.md` and the Phase 1-5 targeted tests are no longer ignored by git.
- Loading Settings on Python 3.13+ no longer uses `datetime.utcnow()` for JS8 default offset seeding.
- Settings persists `fldigi_host` and `fldigi_port`, reloads them on startup, and keeps prior effective host behavior for legacy profiles.
- `FLDigi` status shows `warn` when a matching process is running but the configured XML-RPC endpoint is unreachable.
- `FLDigi` status shows `ok` when the configured XML-RPC endpoint is reachable, even if generic local process detection is inconclusive.
- Settings status refresh probes FLDigi against the currently entered host/port values.
- Verification baseline passes:
  - `python -m pytest tests/test_reliability_phase1.py tests/test_software_status_phase4.py`
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Rollback:
- Revert the `.gitignore`, Settings, status-service, targeted test, and changelog updates together so artifact tracking and FLDigi endpoint behavior return to the prior baseline consistently.

### 1.146 Addendum (2026-03-14): Multi-Radio Single-Instance Architecture (Design Only)

Problem:
- FreqInOut is currently structured around one active station context: one settings store, one scheduler, one launch/readiness model, and one set of radio endpoints.
- That is workable for a single HF station, but it does not scale cleanly to multiple simultaneous radios, multiple JS8Call/VarAC/FLRig instances, remote rig endpoints, or operator workflows that temporarily move operating roles between radios.
- Treating "profile" as one flat object is not sufficient. A multi-radio station needs a clear separation between:
  - the physical/logical device being controlled
  - the schedule/role currently assigned to that device
  - the station-wide coordination rules between devices
  - the runtime feature level required for a given deployment.

Goals:
- Support multiple radios from one FreqInOut process and one operator-facing portal.
- Preserve current single-radio behavior by migrating existing installs into a default profile layout.
- Allow multiple external application instances per station where supported:
  - `FLRig`
  - `FLDigi`
  - `JS8Call`
  - `VarAC`
- Support remote control endpoints (`rigctld`, remote `FLRig`) as first-class device options.
- Support a resource-light deployment mode for field/temporary computers where only basic scheduler control is needed.
- Keep automation conservative by default: prompt/warn before auto-reassigning or auto-QSY unless the operator explicitly enables auto-resolution rules.

Non-goals for the first implementation:
- Distributed multi-user collaboration across multiple operators
- Automatic message deduplication/merging across every external application on day one
- Full remote-device fleet management outside the local station use case
- Replacing existing single-radio tabs with a brand new UI all at once

Design principles:
- One process, many radio runtimes.
- Explicit profile ownership beats implied global settings.
- Defaults must remain safe for single-radio operators.
- All auto-coordination must be policy-driven and operator-auditable.
- Shared RF resources matter more than exact frequency alone.
- Remote endpoint loss must fail safe and avoid surprise QSY/transmit behavior.

Terms:
- `Device Profile`: a physical or logical radio/control endpoint. Examples:
  - local HF transceiver with local `FLRig`
  - remote `rigctld` endpoint
  - `VarAC` node with its own instance number
  - receive-only SDR / observer device
- `Operating Profile`: the schedule/role/automation intent assigned to a device.
  Examples:
  - "HF Daily Primary"
  - "Night Nets"
  - "Field Lite"
- `Station Coordinator`: the rule engine that resolves cross-device interactions such as:
  - RF conflicts
  - temporary profile swaps
  - SDR follow/parking rules
  - shared antenna/PTT/amplifier protection
- `Deployment Mode`: runtime feature level. Initial values:
  - `full`
  - `minimal`

#### Proposed Data Model

Entity model:
- `device_profiles`
  - one row per radio/control endpoint
  - owns endpoint, launch, and hardware/resource metadata
- `operating_profiles`
  - one row per operator intent/schedule role
  - owns schedule policy and automation behavior
- `operating_profile_assignments`
  - current and scheduled mapping of operating profiles to device profiles
  - supports temporary swaps without rewriting device endpoint settings
- `station_coordination_policies`
  - cross-device rules and interlocks
- `varac_clusters`
  - shared VarAC cluster definitions
- `varac_cluster_members`
  - per-device VarAC node membership and instance numbering

Suggested `device_profiles` fields:
- `id`
- `name`
- `enabled`
- `display_order`
- `device_class`
  - `tx_rx`
  - `observer`
  - `gateway`
- `deployment_mode`
  - `full`
  - `minimal`
- `control_backend`
  - `flrig`
  - `rigctld`
  - `js8call`
  - `manual`
- `rig_host`
- `rig_port`
- `flrig_host`
- `flrig_port`
- `fldigi_host`
- `fldigi_port`
- `fldigi_log_path`
- `js8_host`
- `js8_port`
- `js8_profile_path`
- `varac_install_path`
- `varac_db_path`
- `varac_ini_path`
- `varac_cluster_member_enabled`
- `sdr_host`
- `sdr_port`
- `launch_enabled`
- `launch_path`
- `launch_cmd`
- `working_dir`
- `ptt_group`
- `antenna_group`
- `frontend_group`
- `amplifier_group`
- `notes`

Suggested `operating_profiles` fields:
- `id`
- `name`
- `enabled`
- `description`
- `scheduler_enabled`
- `scheduler_mode`
  - `full`
  - `simple`
- `preferred_antenna_group`
- `preferred_band_set`
- `preferred_mode_set`
- `allow_auto_qsy`
- `allow_auto_band_change`
- `allow_profile_swap`
- `prompt_only`
- `use_messages`
- `use_map`
- `use_background_ingest`
- `use_launch_control`
- `use_net_control_tabs`

Suggested `operating_profile_assignments` fields:
- `id`
- `device_profile_id`
- `operating_profile_id`
- `assignment_state`
  - `active`
  - `scheduled`
  - `temporary_override`
- `starts_utc`
- `ends_utc`
- `reason`
- `created_by`

Suggested `station_coordination_policies` fields:
- `id`
- `name`
- `enabled`
- `policy_type`
  - `rf_conflict`
  - `shared_ptt`
  - `profile_swap`
  - `sdr_follow`
  - `observer_park`
  - `gateway_exclusive`
- `source_device_id`
- `target_device_id`
- `priority`
- `trigger_json`
- `action_json`
- `safety_mode`
  - `warn`
  - `prompt`
  - `auto`

Suggested `varac_clusters` fields:
- `id`
- `name`
- `cluster_id`
- `shared_db_path`
- `counters_refresh_sec`
- `ptt_lock_enabled`
- `gateway_handler_device_id`

Suggested `varac_cluster_members` fields:
- `cluster_id`
- `device_profile_id`
- `instance_number`
- `enabled`

Storage approach:
- Keep existing global `SettingsManager` keys for station-wide defaults and migration compatibility.
- Add structured tables for multi-radio entities instead of proliferating flat keys such as `radio2_js8_port`.
- Existing schedule tables/JSON payloads should gain a target scope over time:
  - `station`
  - `operating_profile`
  - `device_profile`

Migration rule:
- Existing installs migrate to:
  - one default `device_profile`
  - one default `operating_profile`
  - one active assignment between them
- No behavior change for users who never enable additional devices/profiles.

#### Runtime Architecture

Proposed runtime objects:
- `DeviceRuntime`
  - one per `device_profile`
  - owns radio/status/control clients for that device
- `OperatingRuntime`
  - one per active assignment
  - owns the scheduler view and role-specific automation state
- `StationRuntimeManager`
  - owns all device and operating runtimes
  - starts/stops them and provides snapshots to the UI
- `StationCoordinator`
  - evaluates cross-device policies and enforces safe actions

Expected responsibilities:
- `DeviceRuntime`
  - `FLRig` / `rigctld` control
  - `FLDigi` XML-RPC status/control
  - `JS8Call` host/port integration
  - `VarAC` node status and launch metadata
  - per-device launch/readiness
- `OperatingRuntime`
  - scheduler decisions for the assigned operating profile
  - prompt state and temporary overrides
  - "simple scheduler" behavior for `minimal` deployments
- `StationCoordinator`
  - RF conflict detection
  - profile swap execution
  - SDR follow rules
  - shared-PTT / shared-antenna / shared-frontend protection

Important design note:
- `JS8RxHub` cannot remain a singleton tied to one host/port in a multi-radio design.
  The implementation must move to host/port-keyed hubs or per-device receive hubs.
- `VarAC` must be modeled as both:
  - a per-device node/runtime
  - an optional shared cluster data source

#### Status and Monitoring

Monitoring goals:
- Track health per `device_profile`, not only per program name.
- Distinguish specific configured instances from "some matching process is running."
- Provide enough detail for operators to understand:
  - whether a device is usable
  - which backend/service is degraded
  - whether a service is local or remote
  - whether FreqInOut launched it or merely detected it
- Feed scheduler/coordinator decisions from device-local runtime health instead of direct global process checks.

Primary status model:
- `DeviceHealthSnapshot`
  - one aggregated snapshot per `device_profile`
  - answers: "Is this device ready/safe/blocked/busy?"
- `ServiceHealthSnapshot`
  - one snapshot per service instance within a device
  - examples:
    - rig backend (`flrig` or `rigctld`)
    - `JS8Call`
    - `FLDigi`
    - `VarAC`
    - `FLMsg`
    - `FLAmp`
    - observer/SDR endpoint

Suggested `ServiceHealthSnapshot` fields:
- `device_profile_id`
- `service_kind`
  - `rig_backend`
  - `js8call`
  - `fldigi`
  - `varac`
  - `flmsg`
  - `flamp`
  - `observer`
- `service_label`
- `configured`
- `enabled`
- `status_state`
  - `off`
  - `configured`
  - `launching`
  - `reachable`
  - `degraded`
  - `busy`
  - `blocked`
  - `unknown`
- `status_summary`
- `endpoint_uri`
- `host`
- `port`
- `is_remote`
- `launched_by_fio`
- `pid`
- `exe_path`
- `working_dir`
- `profile_path`
- `instance_number`
- `cluster_id`
- `last_probe_utc`
- `last_ok_utc`
- `latency_ms`
- `busy_reason`
- `warning_text`
- `error_text`
- `confidence`
  - `endpoint`
  - `launch_metadata`
  - `process_only`
  - `derived`

Suggested `DeviceHealthSnapshot` fields:
- `device_profile_id`
- `overall_state`
- `control_ready`
- `scheduler_ready`
- `busy`
- `busy_reason`
- `blocked_by_policy`
- `blocked_reason`
- `assigned_operating_profile_id`
- `next_change_utc`
- `actual_freq_hz`
- `target_freq_hz`
- `actual_band`
- `target_band`
- `service_states_json`

Instance identity rules:
- Endpoint identity is primary for active service monitoring:
  - host
  - port
  - protocol/backend
- Launch/process metadata is secondary:
  - PID
  - exe path
  - working directory
  - launch arguments
- Profile identity is required where the external app supports multiple profiles:
  - `JS8Call` profile/config path when discoverable
  - `VarAC` `.ini` path
  - `VarAC` DB path
  - `VarAC` cluster member instance number
- Cluster identity is required for shared-cluster software:
  - `VarAC` cluster ID

Monitoring policy:
- Endpoint-first, not process-name-first.
- A matching process name alone must never produce a strong `reachable/ok` state for a multi-radio device.
- Process-only discovery may be used as a weak signal:
  - to show `configured` or `degraded`
  - to provide troubleshooting hints
  - not to authorize scheduler/coordinator actions that require a working control path

Per-service monitoring expectations:
- `FLRig`
  - probe configured XML-RPC endpoint
  - if locally launched, correlate PID/exe/working-dir when available
- `rigctld`
  - probe configured TCP endpoint
  - treat endpoint reachability as the primary readiness signal
- `JS8Call`
  - probe configured API endpoint
  - where possible, correlate with configured profile path or launch metadata
  - receive hubs must be keyed per device endpoint, not globally
- `FLDigi`
  - probe configured XML-RPC endpoint
  - optionally correlate with configured log path for higher-confidence diagnostics
- `VarAC`
  - combine node endpoint/process/log evidence with:
    - configured `VarAC.ini`
    - configured DB path
    - cluster ID
    - instance number
  - cluster-level status must remain distinct from node-local status
- `FLMsg` / `FLAmp`
  - treat as companion services, not primary rig-control truth
  - their degraded state should not by itself imply the device is unavailable for scheduler control

UI presentation requirements:
- Replace the current one-LED-per-program global assumption with two levels:
  - station-level device summary
  - per-device service detail
- `Station Overview`
  - one device card per `device_profile`
  - one summary LED/state for the device overall
  - compact service badges inside the card
- `Device Detail`
  - service rows for:
    - rig backend
    - `JS8Call`
    - `FLDigi`
    - `VarAC`
    - `FLMsg`
    - `FLAmp`
    - observer/SDR
  - each row should show:
    - status state
    - endpoint
    - local/remote
    - last good contact
    - last error/warning
- `Settings`
  - device editing should show live status for that device's configured endpoints, including unsaved host/port overrides when practical
- If a station-global summary strip remains, it should show aggregate counts or alerts, not pretend there is only one instance of each program.

Scheduler/coordinator integration:
- `OperatingRuntime` and `StationCoordinator` must read `DeviceHealthSnapshot` rather than performing raw process-name checks.
- Scheduler readiness questions should be device-local:
  - "Is this device's selected control backend reachable?"
  - "Is this device busy?"
  - "Is this device blocked by policy?"
  - "Is this device's assigned operating profile active?"
- Coordinator actions must use device-local service state and must not infer cross-device readiness from a global process list.

Safety and failure rules:
- Remote endpoint loss degrades the affected service/device only; it must not imply unrelated devices are unhealthy.
- Ambiguous identity should degrade confidence and prefer `degraded`/`prompt` over `reachable`/`auto`.
- Shared-cluster software must avoid duplicate monitoring work:
  - monitor node-local runtime per device
  - monitor shared-cluster state once per cluster where possible

Migration/compatibility note:
- During the early phases, the existing single-radio LED strip may remain as a compatibility view for the migrated default device only.
- Once multi-device runtime is active, the canonical monitoring model is per-device/per-service, and the old single-instance display must no longer be treated as authoritative.

#### UI Surfaces

New or expanded UI surfaces:
- `Station Overview`
  - top-level dashboard with one card per device profile
  - shows band, target frequency, actual frequency, busy state, next change, launch/readiness, assigned operating profile
- `Devices`
  - create/edit device profiles
  - local or remote endpoints
  - launch configuration
  - shared resource groups
- `Operating Profiles`
  - create/edit operating roles
  - scheduler mode
  - allowed automations
  - lite/full feature flags
- `Assignments`
  - attach operating profiles to devices
  - temporary swap dialog
  - start/end times for temporary overrides
- `Coordination Rules`
  - define RF conflict policies
  - define SDR follow rules
  - define shared-PTT / shared-antenna protections
- `VarAC Cluster`
  - cluster definition
  - shared DB path
  - member list
  - unique instance number validation
  - one gateway handler selection

Existing surface changes:
- `Settings`
  - becomes station-global settings plus entry points to `Devices`, `Operating Profiles`, and `Coordination Rules`
- `HF Schedule`
  - add `Target` column/filter
  - allow schedule rows to target station-wide, operating-profile, or device-specific scopes
- `Net Schedule`
  - add `Target` column/filter
- `Launch Control`
  - move from one global app list to per-device launch plans with optional station startup groups
- `ControlFreq`
  - add device selector or multi-device summary strip
- `Status badges`
  - show device-local endpoint state, not station-global process presence

Minimal deployment UI behavior:
- `minimal` device profiles should suppress or disable heavy features for that profile:
  - map
  - message ingest
  - background reconciliation not needed for scheduler-only use
  - web-heavy surfaces
- UI should make it obvious that a device is in `minimal` mode and intentionally not running the full feature set.

#### Coordination Behavior

Initial coordination rule families:
- `RF conflict`
  - do not key only on exact same frequency
  - support rules based on:
    - same frequency
    - same band
    - shared antenna
    - shared amplifier
    - shared front end / receive overload concern
- `Profile swap`
  - temporary reassignment of an operating profile to another device
  - should not rewrite the device endpoint definition
- `SDR follow`
  - example: if active TX radio is on `20m`, park SDR on `40m`
- `Shared PTT`
  - only one device in a group may transmit
- `Gateway exclusivity`
  - example: only one VarAC device in a cluster may act as gateway handler

Safety defaults:
- New policies default to `warn` or `prompt`.
- `auto` mode must be opt-in and logged.
- Any remote endpoint failure forces coordinator actions back to safe/no-op or prompt-only state.

#### Phased Rollout

Phase A: data-model foundation
- Add structured multi-radio entities and migration for the default single-radio install.
- No concurrent multi-device runtime yet.
- Goal: make existing single-radio behavior run through the new model without UI/behavior regression.

Phase B: device endpoints and deployment modes
- Introduce device profile editing.
- Add remote `rigctld` and remote `FLRig` support.
- Add `minimal` deployment mode.
- Keep one active device in runtime while the architecture is proven.

Phase C: multi-runtime manager
- Add `StationRuntimeManager` with multiple `DeviceRuntime` instances.
- Refactor status/control clients to accept device context instead of reading one global settings object.
- Remove singleton assumptions from JS8 receive/state handling.

Phase D: operating profiles and assignments
- Add operating profile editing and assignment UI.
- Support temporary swap / temporary assignment overrides.
- Add target scope to schedules.

Phase E: station coordination
- Add policy engine and safe prompt-first conflict handling.
- Start with:
  - shared PTT
  - RF conflict warnings
  - temporary profile swap workflow
- Hold back full auto-resolution until prompt flows and logging are proven.

Phase F: observer/SDR and VarAC cluster specialization
- Add SDR/observer profile type and follow rules.
- Add VarAC cluster surfaces and shared-cluster handling.
- Enforce one gateway handler and unique instance numbers per cluster.

Phase G: UX refinement and performance hardening
- Add operator-facing dashboards, summaries, and low-clutter workflows.
- Trim polling and ensure multi-device runtime remains responsive on modest hardware.

#### Verification Strategy

Design-time acceptance criteria:
- The architecture preserves current single-radio behavior via default migration.
- Device endpoints are cleanly separable from operating roles.
- Remote endpoints and `minimal` deployment mode fit without creating special-case hacks.
- Temporary operating-profile reassignment is possible without rewriting device endpoints.
- Station coordination policies can represent the user-requested workflows:
  - temporary swaps between radios
  - same-frequency / same-resource conflict handling
  - SDR alternate-band following
  - remote `rigctld`
  - remote `FLRig`
  - scheduler-only field deployment

Implementation-time verification expectations:
- Each phase must be backward compatible with the default migrated single-radio profile.
- Each phase must include targeted tests for migration, runtime isolation, and policy safety.
- Multi-device runtime must be validated with:
  - absent software
  - local endpoints
  - remote endpoints
  - temporary assignment overrides
  - minimal-mode devices

#### Risks and Mitigations

Primary risks:
- Profile scope confusion if device and operating roles are not clearly separated.
- Hidden singleton assumptions in current clients and tabs.
- Resource growth if every device loads every heavy surface.
- Unsafe auto-actions when remote endpoint state is stale.
- Message/cluster duplication if shared data sources are ingested per-device instead of per-cluster.

Mitigations:
- Keep the object model explicit and simple: device, operating profile, assignment, coordinator.
- Default to prompt-first coordination.
- Use lazy loading and capability flags for heavy surfaces.
- Treat cluster/shared data as first-class shared resources, not just another device-local feed.
- Land the architecture in phases with rollback at every stage.

Rollback:
- Each implementation phase should be independently revertible while preserving the migrated default single-radio profile layout.
- The single-radio default runtime must remain available as the fallback operating mode until multi-device runtime is proven stable.

### 1.147 Addendum (2026-03-14): Multi-Radio Phase A Implementation (Settings-DB Foundation)

Problem:
- The design addendum defines the multi-radio model, but there is no persistent schema or migration path yet.
- Current runtime code still assumes one flat settings store and one implicit station context.
- Before any multi-runtime UI or scheduler work can happen safely, the settings DB needs a backward-compatible foundation for:
  - `device_profiles`
  - `operating_profiles`
  - current assignment state
  - future coordination/cluster metadata

Impacted files and failure modes:
- `SPEC.md`
  - Failure mode: implementation proceeds without a constrained Phase A scope and accidentally drifts into UI/runtime behavior changes.
- `freqinout/core/settings_manager.py`
  - Failure mode: schema is not created or default migration is not seeded consistently when the settings DB is first opened.
- `freqinout/core/db_initializer.py`
  - Failure mode: startup DB initialization creates only the legacy `kv` table and tooling/runtime drift reappears.
- `tools/db_schema.py`
  - Failure mode: DB tools do not know about the new settings-side multi-radio tables.
- new multi-radio store/helper module
  - Failure mode: future code keeps reading/writing ad-hoc flat keys instead of using the structured Phase A model.

Scope:
- Implement Phase A only:
  - add settings-DB tables for multi-radio entities
  - seed one default `device_profile`, one default `operating_profile`, and one active assignment from existing single-radio settings
  - expose a small core helper/store API for listing/loading the default migrated records
  - align settings DB tooling metadata with the new tables
  - add targeted tests for schema creation and default migration

Out of scope:
- Multi-device runtime manager
- Scheduler changes
- UI changes for device/operating profile editing
- Status badge redesign
- Coordination-rule execution

Constraints:
- No user-visible behavior change for current single-radio operation.
- Existing flat `kv` settings remain the runtime source of truth for current features during Phase A.
- The migration must be idempotent and safe to run on every startup.
- The seeded default profile names/fields should be stable and deterministic.

Acceptance criteria:
- Opening/initializing `freqinout.db` creates the Phase A multi-radio tables alongside `kv`.
- Existing installs are seeded with:
  - one default `device_profile`
  - one default `operating_profile`
  - one active assignment
- Seeded records reflect current single-radio control settings closely enough for later phases to migrate behavior safely.
- Running the migration multiple times does not create duplicate default records.
- DB tooling can inspect the new settings-side tables.
- Verification baseline passes:
  - `python -m pytest tests/test_multi_radio_phase_a.py`
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`
  - `powershell -ExecutionPolicy Bypass -File .\tools\freqinout-db.ps1 status`

Rollback:
- Revert the Phase A schema/helper changes together so the app returns to the previous flat-settings-only behavior without partial structured-schema leftovers in active code paths.

### 1.148 Addendum (2026-03-14): Multi-Radio Phase B Slice 1 (Runtime-Active Device Projection)

Problem:
- Phase A created the structured multi-radio tables, but there is still no safe runtime concept of "which device profile is the current single-radio compatibility device."
- Current runtime code still reads legacy flat settings, so structured device rows can drift out of sync as soon as operators edit Settings.
- Remote endpoint and `minimal` deployment metadata can be stored already, but there is no controlled path to switch the active runtime device without partial/unsafe behavior.

Impacted files and failure modes:
- `SPEC.md`
  - Failure mode: Phase B work drifts into unsupported multi-runtime or `rigctld` control behavior without a constrained compatibility slice.
- `freqinout/core/multi_radio_store.py`
  - Failure mode: device profiles have no deterministic runtime-active state, no safe active-device switching path, and no projection into legacy settings for current runtime code.
- `freqinout/core/settings_manager.py`
  - Failure mode: edits to flat settings continue to drift away from the active structured device profile, making later phases unreliable.
- `tests/test_multi_radio_phase_a.py`
  - Failure mode: regressions in active-device seeding/projection are not covered while the app still depends on flat settings.

Scope:
- Resume Phase B with the smallest backward-compatible compatibility slice only:
  - add a structured `runtime_active` state for `device_profiles`
  - expose core store APIs to load/save device profiles and set the one runtime-active device
  - project the runtime-active device profile into the existing legacy flat settings keys used by the current runtime
  - mirror legacy flat-settings edits back into the runtime-active device profile so Phase B data does not drift
  - allow persisted remote endpoint and `deployment_mode` metadata on device profiles
  - block activation of unsupported runtime backends (currently `rigctld`) until scheduler/control support exists

Out of scope:
- Multi-device runtime manager
- Settings-tab device profile CRUD UI
- Scheduler/control implementation for `rigctld`
- Minimal-mode feature suppression across tabs
- Operating-profile assignment UI or policy execution

Constraints:
- Preserve current single-radio runtime behavior and flat-settings compatibility.
- Keep one and only one runtime-active device profile in normal operation.
- Do not allow a profile to become runtime-active if the current runtime cannot safely honor its control backend.
- Keep migration/idempotency safe on every startup.

Acceptance criteria:
- Existing installs keep one default device profile marked runtime-active.
- Store APIs can create/update device profiles and deterministically switch the runtime-active profile for supported backends.
- Switching the runtime-active device updates the legacy settings keys used by the current runtime (`control_via`, host/port fields, and related endpoint settings).
- Editing legacy flat settings updates the runtime-active device profile so structured Phase B data stays aligned with current behavior.
- Attempting to activate a `rigctld` device profile fails safely without partially switching the runtime-active device.
- Verification passes:
  - `python -m pytest tests/test_multi_radio_phase_a.py`
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Rollback:
- Revert the runtime-active device projection/mirroring changes together so the app returns to the prior Phase A foundation without mixed active-device semantics.

### 1.149 Addendum (2026-03-14): Multi-Radio Phase B Slice 2 (Settings Device Profile Management)

Problem:
- Phase B Slice 1 established runtime-active device projection in core code, but operators still have no Settings UI to inspect, create, edit, activate, or delete device profiles.
- Without a dedicated UI, remote endpoint metadata and `minimal` deployment intent remain effectively hidden, and Phase B cannot be exercised safely end-to-end.
- Reusing the legacy flat-settings fields alone is not sufficient because multi-device editing needs explicit structured CRUD without breaking the current single-runtime compatibility path.

Impacted files and failure modes:
- `SPEC.md`
  - Failure mode: UI work lands without a constrained compatibility scope and accidentally drifts into unsupported multi-runtime or `rigctld` runtime-control behavior.
- `freqinout/core/multi_radio_store.py`
  - Failure mode: Settings UI cannot safely persist/delete/switch device profiles, or unsupported activation leaves partial runtime-active state behind.
- `freqinout/gui/settings_tab.py`
  - Failure mode: operators cannot manage device profiles from the app, or activating a profile fails to refresh the legacy-compatible settings fields shown elsewhere in Settings.
  - Failure mode: device-profile CRUD is blocked by unrelated full-settings validation instead of persisting independently.
- `tests/test_multi_radio_phase_a.py`
  - Failure mode: core CRUD/activation regressions are not covered after adding UI-driven workflows.
- new targeted Settings UI regression test file
  - Failure mode: panel wiring/activation flow regresses without automated coverage.

Scope:
- Add a new `Device Profiles` Settings section that:
  - lists current structured device profiles
  - shows which profile is runtime-active
  - supports add/edit/delete for device profiles
  - supports explicit `Set Active` for supported runtime backends
- Persist device-profile CRUD directly through `MultiRadioStore` so this workflow is not blocked by unrelated full-settings validation.
- Refresh the existing legacy-compatible Settings controls after active-device changes so operators immediately see the active profile’s projected control/backend endpoint values.
- Allow persisted editing of:
  - `name`
  - `control_backend`
  - `deployment_mode`
  - `rig_host` / `rig_port`
  - `flrig_host` / `flrig_port`
  - `fldigi_host` / `fldigi_port`
  - `js8_host` / `js8_port`
  - `launch_enabled` / `launch_path`
  - `notes`
- Allow storing `rigctld` and `minimal` metadata, but keep activation/runtime behavior constrained to the currently supported backends from Slice 1.

Out of scope:
- Full multi-device runtime manager
- Per-tab feature suppression for `minimal` devices
- Scheduler/control implementation for `rigctld`
- Operating-profile assignment UI
- Sidebar/device dashboard redesign

Constraints:
- Preserve current single-runtime behavior; one profile remains runtime-active at a time.
- Device-profile CRUD must not require saving unrelated Settings sections first.
- Unsupported runtime activation attempts must fail clearly and leave the current runtime-active device unchanged.
- Prevent destructive deletes of the currently runtime-active profile.

Acceptance criteria:
- Settings shows a `Device Profiles` section with a table and actions to add, edit, delete, and activate profiles.
- Creating/editing device profiles persists through app restart.
- Activating a supported profile refreshes the visible legacy Settings controls (`control_via`, endpoint fields currently shown in Settings, launch flags) to the selected profile.
- Attempting to activate an unsupported backend (currently `rigctld`) shows a clear warning and does not switch the runtime-active profile.
- Attempting to delete the runtime-active profile is blocked with a clear warning.
- Verification passes:
  - `python -m pytest tests/test_multi_radio_phase_a.py tests/test_multi_radio_phase_b_slice2.py`
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`
  - `powershell -ExecutionPolicy Bypass -File .\tools\freqinout-db.ps1 status`

Rollback:
- Revert the Settings device-profile UI and supporting store/test updates together so Phase B returns to the Slice 1 core-only compatibility state.

### 1.150 Addendum (2026-03-14): Multi-Radio Phase B Slice 3 (rigctld Runtime Support)

Problem:
- Phase B Slice 2 made `rigctld` device profiles visible and editable, but activation is still blocked because the current runtime only knows how to drive FLRig and JS8Call.
- That leaves the multi-radio device model incomplete for the most important remote-control backend in Phase B: operators can describe a remote rig endpoint, but they still cannot run it as the active compatibility device.
- The current startup/runtime wiring is also still FLRig-specific: the main window always builds an FLRig client, scheduler status paths assume FLRig-only rig polling, and software status surfaces have no `rigctld` endpoint awareness.

Impacted files and failure modes:
- `SPEC.md`
  - Failure mode: `rigctld` support lands partially and operators can activate profiles that the runtime only half honors.
- `freqinout/radio_interface/rigctl_client.py`
  - Failure mode: there is no real `rigctld` client/factory, so active-device projection can switch `control_via` without a usable backend implementation.
- `freqinout/core/scheduler_engine.py`
  - Failure mode: scheduler/readback/PTT logic remains FLRig-specific and either refuses `rigctld` outright or treats it as a broken FLRig runtime.
- `freqinout/core/software_status_service.py`
  - Failure mode: status LEDs continue to imply only FLRig/JS8/FLDigi endpoint truth, leaving `rigctld` operators without endpoint-aware diagnostics.
- `freqinout/gui/main_window.py`
  - Failure mode: runtime client construction remains pinned to FLRig even after Settings activates a `rigctld` profile.
- `freqinout/gui/settings_tab.py`
  - Failure mode: Settings still labels `rigctld` as stored-only and continues to coerce/erase a projected `RIGCTLD` active backend in the legacy controls.
- targeted tests
  - Failure mode: activation/runtime/status regressions for `rigctld` are not covered.

Scope:
- Add real single-runtime compatibility support for `rigctld`:
  - implement a lightweight TCP `rigctld` client with frequency read/set, PTT read, availability probe, and best-effort mode/VFO handling
  - add a runtime rig-backend factory that selects FLRig or `rigctld` from current settings
  - allow `rigctld` device profiles to become runtime-active
  - extend scheduler control/readback/PTT paths so `rigctld` behaves like a supported rig backend in the existing compatibility model
  - rebuild the active rig backend in the main window when Settings changes relevant runtime endpoint/backend values
  - extend software status probing with endpoint-aware `rigctld` status
  - update current Settings/ControlFreq UI surfaces so active `RIGCTLD` state is visible and no longer rewritten back to `FLRig`

Out of scope:
- Phase C multi-device runtime manager
- Per-device concurrent schedulers
- Per-device JS8 receive hub refactor
- Full device-card status/dashboard redesign
- Per-device launch-plan separation

Constraints:
- Preserve current single-runtime behavior for FLRig, JS8Call, and Manual operators.
- `rigctld` support must fail safe on endpoint loss and must not cause surprise QSY/transmit behavior.
- Keep endpoint probes/control calls short and cached enough to avoid UI-freeze-like behavior.
- Do not require a local `rigctld` process for remote endpoint support; endpoint reachability is the truth signal.

Acceptance criteria:
- A `rigctld` device profile can be activated from Settings and projects `control_via=RIGCTLD` plus the configured host/port into legacy settings.
- Main-window runtime construction switches to a real `rigctld` backend when the active profile/backend requires it.
- Scheduler control/readback/PTT paths treat `RIGCTLD` as a supported rig backend and remain stable when the endpoint is absent.
- Software status exposes endpoint-aware `rigctld` state on the current status surfaces.
- Settings no longer rewrites an active `RIGCTLD` backend back to `FLRig` on load/save.
- Verification passes:
  - `python -m pytest tests/test_multi_radio_phase_a.py tests/test_multi_radio_phase_b_slice2.py tests/test_multi_radio_phase_b_slice3.py tests/test_scheduler_deferral.py tests/test_software_status_phase4.py`
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Rollback:
- Revert the `rigctld` client/factory, scheduler/status/main-window wiring, Settings/runtime-activation changes, and related tests together so Phase B returns to the Slice 2 state cleanly.

### 1.151 Addendum (2026-03-14): Multi-Radio Phase B Slice 4 (Minimal Deployment Mode Enforcement)

Problem:
- Phase B Slice 2 added `deployment_mode` editing and Slice 3 makes remote backends operational, but `minimal` devices still behave like full desktop installs.
- That breaks the intent of Phase B for field and temporary systems: operators can mark a device as `minimal`, but the app still starts heavy background ingest, prewarms web-heavy surfaces, exposes irrelevant views, and behaves like a full station console.
- Without explicit UI/runtime enforcement, `minimal` mode is metadata only and does not improve performance or operational clarity.

Impacted files and failure modes:
- `SPEC.md`
  - Failure mode: `minimal` mode remains declarative only and operators cannot trust its behavior.
- `freqinout/gui/main_window.py`
  - Failure mode: heavy views remain navigable, background tasks keep running, and startup still performs full-mode warmup/launch behavior for `minimal` devices.
- `freqinout/core/background_ingest.py`
  - Failure mode: there is no clean observable runtime contract for start/stop state when deployment mode changes.
- `freqinout/gui/settings_tab.py`
  - Failure mode: operators do not get clear feedback about what `minimal` mode suppresses once active.
- targeted tests
  - Failure mode: deployment-mode enforcement regresses silently.

Scope:
- Enforce active-device `deployment_mode` in the current single-runtime app shell:
  - treat the runtime-active device profile as authoritative for current deployment mode
  - when `deployment_mode=minimal`, suppress heavy/non-essential surfaces in the current UI:
    - `Map`
    - `Messages`
    - `FreqPlanner`
  - redirect attempts to open suppressed views back to a safe operational surface
  - stop background ingest while `minimal` mode is active
  - skip startup lazy prewarm, WebEngine prewarm, and launch-control startup sequence while `minimal` mode is active
  - show a persistent runtime banner/hint so operators can see that the active device is intentionally running in `minimal` mode
  - restore the suppressed behavior when switching back to a `full` active device

Out of scope:
- Phase C operating-profile feature matrices
- True per-device UI multiplexing
- Re-architecting Map from eager construction to a brand-new lazy/runtime-managed surface
- Removing non-essential tabs from the codebase entirely

Constraints:
- Preserve current full-mode behavior exactly when the active device is not `minimal`.
- Switching deployment mode must not freeze the UI or require app restart.
- If the operator is currently on a now-suppressed screen, redirect predictably instead of leaving a dead view active.
- Minimal mode is intentionally conservative: prefer disabling/suppressing heavy work over partial best-effort execution.

Acceptance criteria:
- Activating a `minimal` device profile suppresses `Map`, `Messages`, and `FreqPlanner` from the primary navigation and blocks programmatic navigation to those screens.
- Background ingest is not running while a `minimal` device is active, and resumes when switching back to `full`.
- Startup does not perform launch-control startup or UI prewarm work while a `minimal` device is active.
- The main window shows a persistent indicator that the active device is running in `minimal` mode.
- Settings device-profile messaging reflects the actual runtime effects of `minimal` mode instead of describing it as future metadata only.
- Verification passes:
  - `python -m pytest tests/test_multi_radio_phase_b_slice4.py tests/test_background_ingest_phase2.py`
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`
  - `powershell -ExecutionPolicy Bypass -File .\tools\freqinout-db.ps1 status`

Rollback:
- Revert the main-window deployment-mode enforcement, Settings messaging updates, background-ingest runtime-state helper, and related tests together so `minimal` mode returns to metadata-only behavior.

### 1.152 Addendum (2026-03-14): Multi-Radio Phase C Slice 1 (Multiple Runtime-Active Devices with One Primary Compatibility Device)

Problem:
- Phase B still hard-limits the station to one runtime-active device profile even though the settings DB and Settings UI can already describe multiple devices.
- That mismatch is now visible to operators during testing: additional device profiles can be stored, but only one can actually be marked active.
- The current runtime still depends on one projected flat-settings view, so Phase C needs a safe bridge from "many active devices" to "one primary compatibility device" instead of a breaking all-at-once rewrite.

Impacted files and failure modes:
- `SPEC.md`
  - Failure mode: runtime work drifts into schedule-targeting or per-tab role redesign before the compatibility boundary is explicit.
- `freqinout/core/multi_radio_store.py`
  - Failure mode: multiple active profiles are not normalized safely, no single primary compatibility device exists, or legacy flat settings drift away from the selected primary device.
- `freqinout/core/settings_manager.py`
  - Failure mode: flat-settings writes mirror into the wrong device row once multiple devices can be active.
- targeted tests
  - Failure mode: multi-active semantics regress and the app silently falls back to last-write-wins primary selection.

Scope:
- Complete the Phase C store transition in the smallest safe compatibility slice:
  - allow multiple `device_profiles` to be `runtime_active=1`
  - add and normalize one `runtime_primary=1` device among the active set
  - keep legacy flat-settings projection and mirror behavior bound to the primary device only
  - expose store APIs to:
    - list runtime-active profiles
    - set/unset runtime-active state per profile
    - set the runtime-primary profile
  - preserve automatic seeding/normalization so existing installs still land on one deterministic primary device

Out of scope:
- Per-device concurrent schedulers
- Operating-profile assignment editing
- Station overview/dashboard UI
- Per-device launch plans

Constraints:
- There must always be exactly one primary device among the active set whenever at least one active device exists.
- At least one runtime-active device must remain enabled.
- Existing callers that ask for the "runtime-active device" remain backward-compatible and receive the primary compatibility device.
- Legacy flat settings remain the truth source for current single-runtime tabs and scheduler behavior until later Phase C slices finish.

Acceptance criteria:
- Operators can persist more than one `runtime_active` device profile.
- One and only one active device is marked `runtime_primary`.
- Making a device primary also projects its endpoint/backend values into the legacy flat settings used by current runtime code.
- Deactivating the primary device is blocked until another device is made primary first.
- Legacy settings writes continue to mirror into the primary device row only.
- Verification passes:
  - `python -m pytest tests/test_multi_radio_phase_a.py`
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Rollback:
- Revert the multi-active store/schema changes together so the app returns to the Phase B single-active compatibility model without mixed semantics.

### 1.153 Addendum (2026-03-14): Multi-Radio Phase C Slice 2 (StationRuntimeManager and Per-Device Runtime Snapshots)

Problem:
- After Slice 1, the store can represent multiple active devices, but the running app still has no object that owns or reports multi-device runtime state.
- `main_window.py` still assumes one active profile, one runtime backend selection, and one set of status/control objects.
- Advanced operators need a station-level runtime model that can track every active device without forcing a premature rewrite of schedule-targeting and tab ownership.

Impacted files and failure modes:
- `SPEC.md`
  - Failure mode: Phase C runtime work over-promises full per-device scheduler/tab multiplexing before the current UI can safely support it.
- new station runtime manager module
  - Failure mode: there is no durable place to own device-scoped runtime/status objects or provide a stable snapshot API to the UI.
- `freqinout/gui/main_window.py`
  - Failure mode: the app keeps rebuilding only one backend directly from flat settings and ignores the rest of the active station.
- `freqinout/radio_interface/js8_rx_hub.py`
  - Failure mode: JS8 receive state remains pinned to one global endpoint, so primary-device changes or station-level monitoring become stale/brittle.
- `freqinout/radio_interface/js8_status.py`
  - Failure mode: JS8 status/control clients cannot be constructed against explicit device endpoints.
- `freqinout/core/software_status_service.py`
  - Failure mode: per-device snapshots still fall back to the wrong endpoint when probing JS8.
- targeted tests
  - Failure mode: runtime-manager refresh, primary rebinding, and endpoint-keyed JS8 behavior regress silently.

Scope:
- Add a `StationRuntimeManager` with one `DeviceRuntime` per runtime-active device profile.
- Add per-device settings proxies so status/control clients can be created from device rows instead of the global `SettingsManager`.
- Expose a device snapshot model for the UI with, at minimum:
  - device identity and backend
  - active/primary flags
  - deployment mode
  - assigned operating-profile summary
  - endpoint summary
  - control readiness / overall state
  - per-service status snapshot payload
- Keep the current scheduler, launch orchestrator, and existing control tabs primary-device scoped in Phase C:
  - the primary runtime remains the compatibility owner for current tabs
  - secondary active devices are runtime-managed and monitored, but not yet schedule-targeted
- Finish the JS8 endpoint-keying work needed for Phase C safety:
  - `JS8RxHub` instances keyed by host/port
  - JS8 clients/status checks accept explicit host/port settings
  - primary-device endpoint changes fully tear down stale JS8 hubs

Out of scope:
- Operating-profile-targeted schedulers
- Multiple concurrent `JS8Call Net Control` or `Map` tab contexts
- Per-device launch orchestration and startup sequencing
- Full cluster-aware VarAC runtime management

Constraints:
- Existing tabs remain bound to the primary compatibility device until a later phase adds role/device selectors.
- The runtime manager must fail safe if a configured backend is absent or unreachable.
- JS8 monitoring/control remains primary-tab scoped where the current UI depends on global workflow assumptions; Phase C should expose and manage endpoint identity cleanly without pretending the existing JS8 tabs are already device-multiplexed.
- The app must still start cleanly with no radio software running.

Acceptance criteria:
- The app constructs a station runtime manager from all runtime-active device profiles.
- The main window rebinds its current scheduler/runtime clients to the primary device after profile changes.
- Station-level runtime snapshots are available for every active device and include endpoint-aware service state.
- JS8 hub/client teardown occurs when the primary JS8 endpoint changes so stale receive state is not reused across devices.
- Verification passes:
  - `python -m pytest tests/test_multi_radio_phase_c.py`
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Rollback:
- Revert the station runtime manager, primary rebinding, JS8 endpoint-keying adjustments, and related tests together so the app returns to the Phase B primary-only runtime wiring.

### 1.154 Addendum (2026-03-14): Multi-Radio Phase C Slice 3 (Settings Multi-Active Controls and Station Overview UI)

Problem:
- Even with Slice 1 and Slice 2 in place, operators still need an in-app way to activate multiple devices, choose the primary device explicitly, and see the current station runtime at a glance.
- The current `Device Profiles` panel still presents one-active-device behavior, and there is no dedicated station-level runtime view for advanced operators.
- Without a first-class overview, multi-device runtime exists only in the store/core and is too opaque for reliable live operating use.

Impacted files and failure modes:
- `SPEC.md`
  - Failure mode: UI work lands without a clear boundary and starts implying schedule-targeted per-device workflows that do not exist yet.
- `freqinout/gui/settings_tab.py`
  - Failure mode: operators cannot activate/deactivate multiple devices or choose the primary device safely.
  - Failure mode: primary-device projection into current Settings controls happens for the wrong device or not at all.
- new `Station Overview` UI module
  - Failure mode: active-device state is hidden or too low-signal for operators managing a multi-rig station.
- `freqinout/gui/main_window.py`
  - Failure mode: the app has no station-level view and does not refresh UI state after runtime-profile changes.
- targeted tests
  - Failure mode: Settings and overview wiring regress without coverage.

Scope:
- Update the existing Settings `Device Profiles` section to support true Phase C semantics:
  - show both `Active` and `Primary`
  - allow activating one or more selected profiles
  - allow deactivating selected non-primary active profiles
  - allow choosing exactly one primary profile
  - keep add/edit/delete behavior
- Ensure primary changes still refresh the visible legacy Settings controls so current single-runtime tabs stay aligned.
- Add a new `Station Overview` surface using the current app design language:
  - one runtime card per active device
  - clear primary badge/state
  - control backend and endpoint summary
  - deployment mode and assigned operating profile summary
  - compact service-state indicators derived from the runtime manager snapshots
  - explicit note when a device is monitored but not the current primary compatibility owner
- Refresh the overview and runtime state live after Settings profile changes and normal periodic status refresh.

Out of scope:
- Per-device schedule editing or targeting
- Per-device duplicate control tabs
- Device-specific launch/start-stop buttons in the overview
- A separate station-coordination policy UI

Constraints:
- The UI must preserve the current look and interaction style; this is an extension of the existing desktop app, not a redesign.
- Existing primary-device workflows must continue to work exactly as before once a primary is selected.
- Multi-select activation/deactivation must fail clearly and leave the store/runtime in a normalized state.
- The overview should stay lightweight and readable on both large and modest desktop window sizes.

Acceptance criteria:
- Settings allows multiple device profiles to be active at once and lets the operator select one primary device explicitly.
- The current flat Settings controls reload from the chosen primary device after a primary change.
- The app shows a `Station Overview` view with one card per active device and live runtime state.
- The main window updates the overview and primary compatibility runtime after device-profile changes without restart.
- Verification passes:
  - `python -m pytest tests/test_multi_radio_phase_b_slice2.py tests/test_multi_radio_phase_c.py`
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`
  - `powershell -ExecutionPolicy Bypass -File .\\tools\\freqinout-db.ps1 status`

Rollback:
- Revert the Settings multi-active/primary UI, station overview UI, main-window integration, and related tests together so the app returns to the Phase B device-profile panel and primary-only shell.

### 1.155 Addendum (2026-03-14): Multi-Radio Phase D Slice 1 (Operating Profile CRUD and Effective Assignment APIs)

Problem:
- Phase C made multiple devices runtime-active and operator-visible, but operating profiles still exist only as seeded rows with no supported CRUD or reassignment path.
- `StationRuntimeManager` can read assignment rows, but there is no store API to manage one effective operating profile per device safely.
- Advanced operators need explicit operating-role state without mutating device endpoint definitions just to change station behavior.

Impacted files and failure modes:
- `SPEC.md`
  - Failure mode: Phase D lands as ad-hoc UI writes directly against tables without a clear runtime contract.
- `freqinout/core/multi_radio_store.py`
  - Failure mode: multiple effective assignments can exist for one device, deletes disable live profiles unsafely, or temporary overrides overwrite history without normalization.
- `freqinout/core/station_runtime_manager.py`
  - Failure mode: runtime snapshots continue to consume stale or ambiguous assignment rows.
- targeted tests
  - Failure mode: assignment normalization and operating-profile persistence regress silently.

Scope:
- Add store support for Phase D operating-role management:
  - operating-profile CRUD APIs
  - one effective assignment per device
  - assignment change APIs that preserve superseded history rows instead of rewriting in place
  - support explicit `temporary_override` effective assignments as operator-applied metadata/state
  - expose helpers for listing the current effective assignments cleanly
- Keep scheduled start/end activation out of runtime execution for this slice:
  - `starts_utc` / `ends_utc` remain stored metadata
  - no automatic timed activation/expiry yet

Out of scope:
- Schedule table target scopes
- Per-device concurrent schedulers
- Coordination policy execution
- Automatic timed assignment transitions

Constraints:
- Each device must have at most one effective assignment at a time.
- The default operating profile must remain available as the fallback compatibility profile.
- Disabling or deleting an operating profile that is currently assigned to any device must fail clearly.
- Existing Phase C runtime behavior must remain unchanged when all devices use the seeded default operating profile.

Acceptance criteria:
- Operators can create, edit, and delete non-active operating profiles through supported store APIs.
- Reassigning a device to a different operating profile creates a new effective assignment row and retires the previous effective row.
- `temporary_override` can be stored as the current effective assignment state for a device.
- `StationRuntimeManager` can obtain one normalized effective assignment per device without ambiguous last-row wins behavior.
- Verification passes:
  - `python -m pytest tests/test_multi_radio_phase_d.py`
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Rollback:
- Revert the operating-profile CRUD and assignment store/runtime changes together so the app returns to seeded-profile, read-only Phase C behavior.

### 1.156 Addendum (2026-03-14): Multi-Radio Phase D Slice 2 (Settings Operating Profiles and Assignment UI)

Problem:
- Even with store support, operators still have no UI to edit operating profiles or apply them to device profiles.
- Station Overview currently shows only the assigned operating-profile name and does not surface the effective assignment state or capability matrix clearly.
- Without an explicit assignment workflow, advanced station configuration remains too opaque for live use.

Impacted files and failure modes:
- `SPEC.md`
  - Failure mode: UI work implies schedule-targeting or automatic override expiry that does not exist yet.
- `freqinout/gui/settings_tab.py`
  - Failure mode: operators cannot manage operating profiles safely, or assignment changes drift away from runtime state.
- `freqinout/gui/station_overview_tab.py`
  - Failure mode: active-device runtime cards omit the assignment state and effective operating-policy summary operators need.
- targeted tests
  - Failure mode: Settings CRUD/assignment workflows regress without coverage.

Scope:
- Extend Settings using the current app design language:
  - add an `Operating Profiles` section with CRUD
  - add a `Device Assignments` section that shows the effective operating profile for each device
  - allow operators to assign or temporarily override one or more selected devices from Settings
  - allow restoring selected devices to the default operating profile
- Enrich Station Overview cards with:
  - effective assignment state
  - scheduler on/off summary
  - suppressed feature summary from the assigned operating profile

Out of scope:
- Separate schedule-target editing UI
- Automatic time-based assignment transitions
- Per-device duplicate tabs
- Coordination prompts or swap wizards

Constraints:
- The UI must remain lightweight and consistent with the existing Settings/overview patterns.
- Assignment actions must be explicit and reversible.
- Temporary override messaging must state clearly that expiry is manual in this slice.
- Existing device-profile editing and Phase C station overview behavior must remain intact.

Acceptance criteria:
- Settings supports operating-profile CRUD and current-assignment management without direct DB editing.
- Operators can assign an operating profile or temporary override to one or more devices from Settings.
- Station Overview shows assignment state and an operator-readable operating-policy summary per active device.
- Verification passes:
  - `python -m pytest tests/test_multi_radio_phase_d.py`
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Rollback:
- Revert the Settings operating-profile/assignment UI and overview-summary updates together so the app returns to the Phase C device-only workflow.

### 1.157 Addendum (2026-03-14): Multi-Radio Phase D Slice 3 (Primary Runtime Operating-Profile Enforcement)

Problem:
- Phase D operating profiles are not useful enough if the primary compatibility runtime ignores them.
- The current shell only enforces device `deployment_mode`; it does not react to the assigned operating profile's scheduler or feature-matrix flags.
- The architecture still uses one compatibility scheduler and one app shell, so the highest-value Phase D step is to make the primary assigned operating profile drive that shell safely.

Impacted files and failure modes:
- `SPEC.md`
  - Failure mode: enforcement work overreaches into full schedule-target scope instead of the current compatibility shell.
- `freqinout/core/station_runtime_manager.py`
  - Failure mode: the main window has no normalized policy view for the primary assigned operating profile.
- `freqinout/core/scheduler_engine.py`
  - Failure mode: scheduler automation keeps reading only the flat settings default and ignores the active operating profile.
- `freqinout/core/launch_orchestrator.py`
  - Failure mode: launch automation still runs when the primary operating profile disallows it.
- `freqinout/gui/main_window.py`
  - Failure mode: suppressed tabs/background work do not follow the primary operating profile, or changes require restart.
- `freqinout/gui/settings_tab.py`
  - Failure mode: Launch Control actions fail opaquely when the primary operating profile blocks them.
- targeted tests
  - Failure mode: runtime policy enforcement regresses without coverage.

Scope:
- Add a normalized primary-runtime operating-policy view derived from the assigned operating profile.
- Enforce that policy in the current compatibility shell only:
  - `scheduler_enabled` disables current scheduler automation/status ownership
  - `use_map`, `use_messages`, and `use_net_control_tabs` suppress those primary-shell views
  - `use_background_ingest` controls background ingest
  - `use_launch_control` suppresses startup launch automation and manual launch-control actions
- Combine operating-profile policy with device `deployment_mode` conservatively:
  - `minimal` remains the strongest suppression mode
  - operating-profile flags can suppress additional surfaces/work while the device remains `full`

Out of scope:
- Schedule table `target_scope`
- One scheduler per device or per operating profile
- Automatic swap orchestration or conflict policy handling
- Full per-device launch orchestration

Constraints:
- Existing single-runtime behavior must remain unchanged when the primary device uses the default operating profile.
- Enforcement must switch live when the primary assignment changes; no restart required.
- The app must remain safe when software is absent or when multiple devices are active but only one is primary.
- This slice must not pretend that secondary active devices already have independent schedulers.

Acceptance criteria:
- The primary assigned operating profile can disable scheduler automation in the current compatibility runtime.
- The app shell suppresses the relevant views/background work/launch automation when the primary operating profile disables them.
- Temporary override assignment changes applied to the primary device take effect immediately in the current shell.
- Settings explains when Launch Control is blocked by the primary operating profile.
- Verification passes:
  - `python -m pytest tests/test_multi_radio_phase_d.py tests/test_multi_radio_phase_b_slice4.py`
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`
  - `powershell -ExecutionPolicy Bypass -File .\\tools\\freqinout-db.ps1 status`

Deferred Phase D boundary:
- Schedule `target_scope` for HF/Net/SOP tables remains a follow-on step after these slices.
- That work requires schedule schema, precedence, and editor changes that are broader than the current compatibility-shell enforcement slice.

Rollback:
- Revert the primary-runtime operating-policy enforcement, launch/scheduler overrides, and related UI/runtime tests together so the app returns to Phase C device-only shell behavior plus Phase D persisted metadata.

### 1.158 Addendum (2026-03-14): Multi-Radio Phase D Slice 4 (HF/Net Schedule Target Scope for the Primary Compatibility Runtime)

Problem:
- Phase D operating-profile enforcement gives the primary compatibility runtime a richer policy model, but HF and Net schedule rows are still global.
- Advanced operators need to stage schedules for different devices or operating profiles without cloning databases or hand-editing rows before each role swap.
- The runtime is still a single primary compatibility scheduler, so schedule targeting must constrain eligibility cleanly instead of pretending that secondary devices already own independent schedulers.

Impacted files and failure modes:
- `SPEC.md`
  - Failure mode: the implementation implies full per-device schedulers or SOP retargeting that do not exist yet.
- `freqinout/core/scheduler_engine.py`
  - Failure mode: target-specific rows are still evaluated globally, or primary-device / assignment changes do not invalidate cached schedule views.
- `freqinout/gui/daily_schedule_tab.py`
  - Failure mode: HF rows cannot persist target metadata safely, or conflict highlighting keeps treating mutually exclusive device-targeted rows as collisions.
- `freqinout/gui/net_schedule_tab.py`
  - Failure mode: Net rows lose target metadata in save/export paths, or duplicate/conflict policies collide across different targets.
- `freqinout/core/sop_manager.py`
  - Failure mode: Net/SOP policy signatures are ambiguous when two otherwise identical Net rows differ only by target scope.
- `freqinout/core/db_initializer.py`
- `tools/db_schema.py`
  - Failure mode: database creation or drift repair omits the new target columns, causing runtime/UI mismatch.
- targeted tests
  - Failure mode: primary-target filtering and row persistence regress silently.

Scope:
- Extend HF and Net schedule rows with persisted target metadata:
  - `target_scope` in `{station, device_profile, operating_profile}`
  - `target_device_profile_id`
  - `target_operating_profile_id`
- Default all legacy rows to `station`.
- Extend the HF and Net editors using the current table-driven UI:
  - append compact `Target Scope` and `Target` controls to each editable row
  - allow targeting a device profile or operating profile by persisted row id
  - preserve stale/missing ids visibly instead of dropping them silently
- Make the primary compatibility scheduler honor only rows whose target matches the current runtime context:
  - `station` rows always match
  - `device_profile` rows match the current primary device profile id
  - `operating_profile` rows match the current effective operating-profile assignment on the current primary device
- Keep schedule precedence unchanged after filtering:
  - Net still wins over HF unless an existing Net/SOP policy override selects SOP
- Fold target metadata into Net/SOP policy row signatures so otherwise identical rows for different targets do not collide.
- Reduce obvious false-positive HF/Net duplicate/conflict checks:
  - rows targeted to different device profiles do not conflict with each other
  - rows targeted to different operating profiles do not conflict with each other
  - mixed device-profile vs operating-profile rows remain conservatively overlapping because assignments are dynamic

Out of scope:
- Separate per-device scheduler instances
- Secondary-device runtime schedule ownership
- SOP schedule-layer target scope UI or schema
- Automatic retargeting when device or operating profiles are deleted
- Timed operating-profile assignment activation/expiry

Constraints:
- Existing single-station rows must continue to behave exactly as before with no manual migration.
- Invalid or stale target references must not crash load, save, export, or scheduler evaluation.
- The runtime must react to primary-device or primary-assignment changes without restart.
- The UI must preserve the current app design language; this is an extension of the existing schedule tables, not a redesign.

Acceptance criteria:
- HF and Net schedule rows can be saved, loaded, and exported with `target_scope` metadata.
- The primary compatibility scheduler ignores rows whose target does not match the current primary device or its effective operating profile.
- Switching the primary device or the primary device's operating-profile assignment changes schedule eligibility live.
- Identical Net rows that differ only by target metadata no longer share the same Net/SOP policy signature.
- Verification passes:
  - `python -m pytest tests/test_multi_radio_phase_d_target_scope.py`
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`
  - `powershell -ExecutionPolicy Bypass -File .\\tools\\freqinout-db.ps1 status`

Deferred boundary after this slice:
- SOP schedule-layer target scopes remain deferred because SOP already has its own profile/layer arbitration model and requires a separate UI/runtime design.
- The scheduler remains primary-runtime only; target scope limits which rows the compatibility scheduler sees, it does not create concurrent schedulers.

Rollback:
- Revert the HF/Net schedule-target schema, editor, scheduler-filtering, and signature/test changes together so schedule behavior returns to Phase D Slice 3 global-row semantics.

### 1.159 Addendum (2026-03-15): Multi-Radio Phase E Slice 1 (Shared-PTT Coordination Interlock)

Problem:
- Phase C and Phase D allow multiple devices to be runtime-active, but the app still has no enforced notion of which devices share a transmit/PTT domain.
- Advanced operators can already model multiple rigs and assignments, yet a primary-device QSY or scheduler action can still proceed while another active device that shares the same PTT chain is transmitting.
- The branch already stores `ptt_group` and `station_coordination_policies`, but that data is not operator-visible enough and does not currently influence runtime behavior.

Impacted files and failure modes:
- `SPEC.md`
  - Failure mode: the slice over-promises full RF-conflict orchestration or prompt flows that belong to later Phase E work.
- `freqinout/core/multi_radio_store.py`
  - Failure mode: `ptt_group` remains effectively hidden, or shared-PTT policy rows drift from device-profile state.
- `freqinout/core/station_runtime_manager.py`
  - Failure mode: runtime snapshots cannot report which active device currently owns a shared PTT domain.
- `freqinout/core/scheduler_engine.py`
  - Failure mode: scheduled or manual QSY actions ignore another active device already transmitting on the same PTT group.
- `freqinout/gui/settings_tab.py`
  - Failure mode: operators cannot configure or review `ptt_group` safely from the existing device-profile UI.
- `freqinout/gui/station_overview_tab.py`
  - Failure mode: active shared-PTT contention is invisible in the current station-level view.
- targeted tests
  - Failure mode: shared-PTT lock enforcement regresses silently or only works for one action path.

Scope:
- Extend the existing `Device Profiles` workflow to expose `ptt_group` directly:
  - editable in the existing Add/Edit Device Profile dialog
  - visible in the existing Device Profiles table using the current table-driven design
- Add store support for shared-PTT coordination state:
  - list persisted station-coordination policies
  - derive/synchronize `shared_ptt` policy rows from non-empty matching `ptt_group` values
  - keep policy generation idempotent and automatic; no separate policy editor in this slice
- Extend the station runtime manager to compute shared-PTT lock state for active devices:
  - poll rig-backed active devices for live PTT state with lightweight caching
  - identify when another runtime-active device in the same `ptt_group` currently owns the shared PTT domain
  - expose that state in per-device runtime snapshots and a primary-device lock summary
- Enforce the shared-PTT interlock in the primary compatibility scheduler:
  - scheduled HF/Net/SOP applications must not queue a frequency/control action when another active device in the same `ptt_group` is keyed
  - manual QSY must respect the same interlock
  - the interlock should appear alongside the existing busy/PTT safety reasons instead of replacing them
- Surface shared-PTT status in the current UI:
  - `Station Overview` cards show `PTT Group` and whether the runtime is clear, keyed, or blocked by another active device
  - current scheduler/frequency status surfaces expose a concise shared-PTT busy reason using the existing design language

Out of scope:
- A separate station-coordination policy management UI
- Prompt-first operator resolution flows
- Automatic profile swap or reassignment workflows
- Full RF conflict logic across antenna/front-end/amplifier groups
- Secondary-device duplicate control tabs or per-device schedulers

Constraints:
- Existing single-device installs with blank `ptt_group` values must behave exactly as before.
- Shared-PTT evaluation must stay lightweight and must not add long blocking work to status refresh paths.
- The first slice should be safety-first: block/hold unsafe scheduler/manual-QSY changes and surface the reason clearly, without adding prompt workflows yet.
- Current primary-device control behavior must remain unchanged when no active shared-PTT conflict exists.

Acceptance criteria:
- Operators can configure `ptt_group` for a device profile from the existing Settings UI and see that value after save/reload.
- The store derives stable `shared_ptt` coordination rows from device `ptt_group` membership without creating duplicate policy rows.
- When another runtime-active device in the same `ptt_group` reports PTT active, the primary compatibility scheduler refuses the frequency/control action and reports a shared-PTT reason.
- Manual QSY uses the same shared-PTT interlock path as scheduled actions.
- `Station Overview` and the current frequency/scheduler status surfaces show which shared PTT group is in use and which device currently owns it.
- Verification passes:
  - `python -m pytest tests/test_multi_radio_phase_e.py tests/test_scheduler_deferral.py`
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`
  - `powershell -ExecutionPolicy Bypass -File .\\tools\\freqinout-db.ps1 status`

Deferred boundary after this slice:
- Prompt-first conflict handling remains Phase E Slice 2.
- Antenna/front-end/amplifier conflict families remain deferred until after shared-PTT interlocks are proven.
- The scheduler remains primary-runtime only; this slice blocks unsafe actions, it does not create concurrent transmitter arbitration.

Rollback:
- Revert the shared-PTT spec addendum, store/runtime coordination support, scheduler/status/UI changes, and targeted tests together so the branch returns to the Phase D target-scope state cleanly.

### 1.160 Addendum (2026-03-15): Multi-Radio Phase E Slice 2 (Prompt-First RF Conflict Warnings)

Problem:
- Phase E Slice 1 added a hard shared-PTT interlock, but the station still lacks an operator-guided path for softer RF/resource conflicts.
- Advanced operators need warning prompts before the primary compatibility runtime moves onto a band/frequency that overlaps another active device on a shared antenna, amplifier, or front-end path.
- The current runtime either proceeds silently or blocks only on hard safety conditions; there is not yet a prompt-first coordination layer for likely-but-operator-manageable conflicts.

Impacted files and failure modes:
- `SPEC.md`
  - Failure mode: Slice 2 overreaches into full auto-resolution or concurrent per-device scheduling.
- `freqinout/core/multi_radio_store.py`
  - Failure mode: `rf_conflict` policies are implied only in code and cannot be audited from persisted coordination rows.
- `freqinout/core/station_runtime_manager.py`
  - Failure mode: the runtime cannot evaluate active peer band/frequency/resource state for primary-device conflict prompts.
- `freqinout/core/scheduler_engine.py`
  - Failure mode: schedule-driven actions do not warn before entering a likely RF conflict, or prompt decisions do not apply cleanly.
- `freqinout/gui/main_window.py`
  - Failure mode: the operator is warned inconsistently, or the prompt flow diverges from the app’s existing scheduler prompt design.
- `freqinout/gui/qsy_helper.py`
  - Failure mode: manual QSY bypasses the conflict prompt path or double-prompts against the scheduler path.
- `freqinout/gui/station_overview_tab.py`
  - Failure mode: current peer tuning/resource context is still too opaque for advanced operators to reason about prompts.
- targeted tests
  - Failure mode: warning prompts, proceed-once behavior, and skip/pause decisions regress silently.

Scope:
- Add derived `rf_conflict` coordination policies from shared resource groups on enabled non-observer device profiles:
  - `antenna_group`
  - `amplifier_group`
  - `frontend_group`
- Persist those policies automatically and idempotently using the existing `station_coordination_policies` table:
  - one derived `rf_conflict` row per device pair
  - `trigger_json` records the shared resource groups
  - `safety_mode='prompt'`
- Extend active runtime snapshots with lightweight current tuning state for rig-backed devices:
  - current frequency in Hz
  - derived current band label
  - normalized shared resource groups
- Evaluate prompt-worthy RF conflicts for the current primary compatibility action against other active devices:
  - compare the primary target band/frequency to each active peer’s current band/frequency
  - prompt only when there is both:
    - a shared resource relationship from derived `rf_conflict` policy state, and
    - an overlap signal (`same frequency` or `same band`)
- Keep this slice warning-based, not block-based:
  - shared PTT remains a hard interlock from Slice 1
  - RF conflict warnings prompt the operator with explicit choices
- Add a scheduler-driven prompt flow using the existing `MainWindow` modal pattern:
  - `Proceed Once`
  - `Skip Once`
  - `Pause Schedule`
- Add the same prompt-first warning path for manual QSY:
  - `Proceed QSY`
  - `Cancel`
- Surface concise RF conflict summary text in the existing station/scheduler UI so operators can see why a prompt appeared.

Out of scope:
- Auto-resolution or automatic retuning of other devices
- A separate station-coordination policy editor
- Prompt flows for every possible conflict family beyond shared antenna/amplifier/front-end overlaps
- Full temporary profile swap workflow
- Secondary-device schedulers or duplicate per-device control tabs

Constraints:
- Existing single-device and no-shared-resource installs must behave exactly as before.
- Slice 2 must reuse the current scheduler prompt style instead of inventing a new coordination UI framework.
- Warning prompts must default safe:
  - no automatic proceed
  - timeout or close behavior must not force an action
- Frequency/band polling for secondary devices must stay lightweight and cached.

Acceptance criteria:
- The store derives stable `rf_conflict` coordination rows from shared resource groups without duplicating rows.
- When the primary scheduler would move onto the same band or frequency as another active device that shares antenna/amplifier/front-end resources, the app prompts instead of silently proceeding.
- `Proceed Once` applies the current schedule action without immediately re-prompting for the same conflict signature.
- `Skip Once` suppresses the current conflict signature until the target/context changes.
- Manual QSY uses the same conflict-evaluation logic and prompts before proceeding.
- Existing shared-PTT hard-block behavior remains intact and takes precedence over warning-only RF conflict prompts.
- Verification passes:
  - `python -m pytest tests/test_multi_radio_phase_e.py tests/test_scheduler_deferral.py`
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`
  - `powershell -ExecutionPolicy Bypass -File .\\tools\\freqinout-db.ps1 status`

Deferred boundary after this slice:
- Temporary profile swap remains a later Phase E slice.
- Auto-resolution and logged policy history remain deferred until prompt flows are proven in operator testing.
- Conflict families that require more than shared resource group + current tuning context remain deferred.

Rollback:
- Revert the Slice 2 spec addendum, derived `rf_conflict` policy support, runtime conflict evaluation, prompt wiring, and targeted tests together so the branch returns to Phase E Slice 1 behavior cleanly.

### 1.161 Addendum (2026-03-15): Multi-Radio Phase E Slice 3 (Temporary Profile Swap Workflow)

Problem:
- Phase E Slice 1 and Slice 2 added hard shared-PTT interlocks and prompt-first RF conflict warnings, but operators still lack a first-class workflow for temporarily shifting the primary compatibility runtime to another active device and then restoring it cleanly.
- The current branch can manually make another device primary and can separately assign `temporary_override` operating profiles, but that sequence is not captured as one reversible station-coordination action.
- Advanced operators need a safe “run from the other rig for now” workflow that does not rewrite endpoint definitions, does not lose the previous primary/profile context, and is explicit about what will be restored.

Impacted files and failure modes:
- `SPEC.md`
  - Failure mode: Slice 3 overreaches into automatic coordination or concurrent per-device scheduler ownership.
- `freqinout/core/multi_radio_store.py`
  - Failure mode: swap state is transient/UI-only, cannot be restored reliably after restart, or loses prior primary/assignment context.
- `freqinout/core/station_runtime_manager.py`
  - Failure mode: runtime snapshots and primary-policy state do not explain when a temporary swap is active or which device/profile is temporary.
- `freqinout/gui/settings_tab.py`
  - Failure mode: operators must still compose primary changes and assignment overrides manually, or cannot restore a swap deterministically.
- `freqinout/gui/main_window.py`
  - Failure mode: the live shell does not make it obvious that the primary runtime is operating under a temporary swap state.
- `freqinout/gui/station_overview_tab.py`
  - Failure mode: station cards do not indicate which device is the swap target/source.
- targeted tests
  - Failure mode: swap apply/restore semantics regress silently, especially across primary-device changes and target assignment restore.

Scope:
- Add a persisted temporary swap workflow for the current primary compatibility runtime:
  - one active swap at a time
  - operator-applied only
  - explicit restore path
- Persist active swap state in `station_coordination_policies` using `policy_type='profile_swap'`:
  - `enabled=1` means the swap is currently active
  - `source_device_id` is the original primary device
  - `target_device_id` is the temporary primary device
  - `trigger_json` stores operator-facing swap metadata (`mode`, `reason`, optional `ends_utc`)
  - `action_json` stores restore metadata for the target device assignment and original primary device
- Add store APIs to:
  - inspect the active temporary swap
  - start a temporary swap
  - restore the active temporary swap
- Support two operator-safe swap modes:
  - `use_target_profile`
    - make another active device primary temporarily, using that device’s current effective operating profile
  - `carry_primary_profile`
    - make another active device primary temporarily and copy the current primary device’s effective operating profile onto the target as a `temporary_override`
- Guard `carry_primary_profile` with the existing operating-profile coordination field:
  - the carried effective operating profile must have `allow_profile_swap=1`
- Restore semantics must be explicit and deterministic:
  - restore the original primary device
  - if the swap changed the target device’s effective operating-profile assignment, restore the target’s prior effective assignment
  - do not rewrite endpoint/device settings
- Add a Settings UI entry point in the existing `Device Assignments` section:
  - `Temporary Swap...`
  - `Restore Swap`
- Surface the active swap in current UI/state:
  - Device Assignments hint text
  - main runtime banner/status text
  - Station Overview device cards

Out of scope:
- Automatic timed expiry of swaps from `ends_utc`
- Multi-step swap orchestration involving more than one target device
- Auto-resolving RF conflicts as part of the swap
- Full coordination policy CRUD/history UI
- Phase F observer/SDR or VarAC cluster specialization

Constraints:
- Existing single-radio and no-swap installs must behave exactly as before.
- Slice 3 must reuse the current effective-assignment/runtime-primary model; it must not introduce a second primary-runtime concept.
- There can be at most one active temporary swap at a time in this compatibility architecture.
- Restore must fail clearly rather than silently dropping previous primary/profile context.

Acceptance criteria:
- Operators can initiate a temporary swap from Settings for one selected active non-primary device.
- `use_target_profile` temporarily makes the selected device primary and can be restored in one action.
- `carry_primary_profile` temporarily makes the selected device primary and applies the prior primary operating profile to that target as a `temporary_override`, then restores the target’s previous effective assignment on restore.
- Active swap state survives restart because it is persisted in store-backed coordination data.
- MainWindow and Station Overview indicate when the primary runtime is operating under a temporary swap.
- Verification passes:
  - `python -m pytest tests/test_multi_radio_phase_e.py tests/test_scheduler_deferral.py`
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`
  - `powershell -ExecutionPolicy Bypass -File .\\tools\\freqinout-db.ps1 status`

Deferred boundary after this slice:
- Automatic expiry and scheduled swap windows remain future work.
- Prompt-first coordination is not expanded into auto-resolution here.
- Phase F specialization remains separate.

Rollback:
- Revert the Slice 3 spec addendum, store/runtime swap support, Settings/MainWindow/Station Overview swap UI, and targeted tests together so the branch returns to Phase E Slice 2 behavior cleanly.

### 1.162 Addendum (2026-03-15): Multi-Radio Phase F Slice 1 (Observer/SDR Runtime and Follow Guidance)

Problem:
- The data model already persists `device_class`, `sdr_host`, and `sdr_port`, but `observer` profiles are not yet a real runtime class.
- Current multi-radio behavior still treats every active device as if it were a possible primary compatibility radio, which is incorrect for receive-only observer/SDR roles.
- There is also no visible follow/park guidance for active observer devices, even though the architecture explicitly calls for SDR follow rules.

Goals:
- Make `observer` a first-class active device role in the current compatibility architecture.
- Keep observer devices out of primary-runtime ownership and swap flows.
- Surface observer endpoint health and follow/park guidance in current UI without introducing a premature SDR tuning-control subsystem.

Scope:
- Settings `Device Profiles`:
  - add explicit `Device Class` editing with operator-facing support for:
    - `Transceiver`
    - `Observer / SDR`
    - `Gateway`
  - add persisted `SDR Host` / `SDR Port` fields to the existing device-profile dialog.
  - show observer identity clearly in the device table and guidance text.
- Store/runtime rules:
  - observer devices may be enabled and `runtime_active`.
  - observer devices must not become the primary compatibility device.
  - observer devices must not be valid temporary-swap targets.
  - derive persisted `sdr_follow` coordination rows between enabled non-observer devices and enabled observer devices for auditability.
- Runtime state:
  - probe observer endpoint reachability from configured `sdr_host` / `sdr_port`.
  - include observer service state in per-device snapshots.
  - derive observer follow/park guidance from:
    - the current primary runtime band/frequency when available
    - the observer device’s effective operating profile preferred-band list when configured
  - if an alternate preferred band exists, recommend parking the observer there; otherwise recommend following/monitoring the primary band.
- UI/state surfaces:
  - Station Overview cards show observer class, endpoint health, and current follow/park guidance.
  - Settings hints explain that observer devices are active station members but never own the primary compatibility shell in this slice.

Out of scope:
- Direct SDR tuning/control commands
- Waterfall or panadapter integration
- A dedicated coordination-rules CRUD editor
- Automatic observer retune when the primary radio changes band
- VarAC cluster membership and gateway enforcement

Constraints:
- Existing single-radio and transceiver-only workflows must behave exactly as before.
- The current scheduler, ControlFreq, and manual QSY flows remain primary-device workflows only.
- Slice 1 must be advisory-first for observers: health and follow guidance are allowed; silent auto-retune is not.
- `gateway` class may remain persisted-only in this slice; only `observer` gains new runtime behavior.

Acceptance criteria:
- Operators can create/edit device profiles with `Observer / SDR` class and SDR endpoint fields in Settings.
- Observer profiles can be runtime-active but cannot be made primary and cannot be selected for Temporary Swap.
- Station store derives stable `sdr_follow` coordination rows for enabled observer/non-observer pairs without duplicate rows.
- Station runtime snapshots expose observer endpoint health and a readable follow/park summary.
- Station Overview makes it obvious which active devices are observers and what they are advised to monitor.
- Verification passes:
  - `python -m pytest tests/test_multi_radio_phase_f_slice1.py`
  - `python -m pytest tests/test_multi_radio_phase_d.py tests/test_multi_radio_phase_e.py`
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`
  - `powershell -ExecutionPolicy Bypass -File .\\tools\\freqinout-db.ps1 status`

Deferred boundary after this slice:
- Observer retune remains operator-guided only.
- `observer_park` explicit policy rows remain future work; Slice 1 uses derived `sdr_follow` policy rows plus runtime advisory text.
- VarAC cluster specialization remains Phase F follow-on work.

Rollback:
- Revert the Slice 1 spec addendum, observer store/runtime/status/UI changes, and targeted tests together so observer devices return to persisted-only behavior.

### 1.163 Addendum (2026-03-15): Multi-Radio Phase F Slice 2 (VarAC Cluster CRUD, Membership, and Device-Scoped Runtime Visibility)

Problem:
- The structured settings schema already includes `varac_clusters` and `varac_cluster_members`, but there is no operator-facing way to create or manage them.
- Current multi-radio runtime cards still treat `VarAC` largely as a global side signal instead of a per-device node plus optional shared-cluster resource.
- Device profiles persist `varac_install_path`, `varac_db_path`, `varac_ini_path`, and `launch_cmd`, but the current multi-radio device editor does not expose those fields.

Goals:
- Make VarAC cluster definition and membership a first-class Settings workflow.
- Surface per-device VarAC node identity and shared-cluster metadata in the current multi-radio runtime model.
- Keep current single-radio behavior backward compatible by continuing to project the primary compatibility device into the existing flat VarAC settings.

Scope:
- Settings `Device Profiles`:
  - expose device-local VarAC fields in the existing device-profile dialog:
    - `VarAC Install Path`
    - `VarAC DB Path`
    - `VarAC INI Path`
    - `VarAC Launch Command`
  - keep these fields optional so non-VarAC devices remain simple.
- Settings `VarAC Clusters`:
  - add CRUD for cluster definitions with:
    - name
    - `cluster_id`
    - shared DB path
    - counters refresh interval
    - PTT lock flag
    - designated gateway handler device
  - show member counts and current gateway handler in the table summary.
- Settings `VarAC Memberships`:
  - add create/update/remove workflows for per-device cluster membership.
  - show:
    - cluster
    - device
    - runtime state
    - device class
    - instance number
    - enabled state
    - gateway role
- Store/runtime behavior:
  - add joined CRUD APIs for `varac_clusters` and `varac_cluster_members`.
  - keep `device_profiles.varac_cluster_member_enabled` synchronized with effective enabled membership state for compatibility/reporting.
  - limit this phase to one enabled VarAC cluster membership per device profile.
  - enrich active device runtime profiles with:
    - cluster name
    - cluster ID
    - shared DB path
    - instance number
    - gateway-handler identity
- Runtime/status surfaces:
  - per-device VarAC node status must use that device profile’s own VarAC settings instead of relying only on the station-global process strip.
  - add a distinct shared-cluster status surface per member device:
    - shared DB configured/missing
    - gateway handler identity
    - member instance number
  - Station Overview cards should show both node-local `VarAC` state and shared-cluster context when a device is a member.

Out of scope:
- Duplicate-process disambiguation beyond configured device-local VarAC paths/settings
- VarAC message/cluster ingest deduplication changes
- Gateway exclusivity enforcement beyond data capture and display
- Automatic instance-number assignment

Constraints:
- Existing single-radio/global VarAC flows must remain unchanged for users who do not create clusters.
- The primary compatibility runtime remains the only source projected into flat settings and current scheduler/control tabs.
- Cluster metadata is additive; it must not break startup when VarAC is absent or unconfigured.
- Slice 2 must preserve the current Settings UI design language and table/dialog patterns already used in multi-radio sections.

Acceptance criteria:
- Operators can create/edit/delete VarAC clusters in Settings.
- Operators can assign a device profile to a VarAC cluster with an instance number and enabled state.
- Device-profile editing exposes device-local VarAC path/DB/INI/launch fields and persists them through `MultiRadioStore`.
- Runtime snapshots expose VarAC cluster membership metadata and a shared-cluster status summary per active member device.
- Station Overview and Settings make cluster membership and gateway identity visible without requiring manual DB inspection.
- Verification passes:
  - `python -m pytest tests/test_multi_radio_phase_f_slice2.py`
  - `python -m pytest tests/test_multi_radio_phase_f_slice1.py tests/test_multi_radio_phase_e.py`
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`
  - `powershell -ExecutionPolicy Bypass -File .\\tools\\freqinout-db.ps1 status`

Deferred boundary after this slice:
- Instance-number and gateway-handler hard enforcement remain Slice 3.
- Shared-cluster data-source deduplication remains future work.
- Current scheduler/control tabs still act only on the primary compatibility device.

Rollback:
- Revert the Slice 2 spec addendum, VarAC cluster store/runtime/UI changes, and targeted tests together so VarAC cluster tables return to schema-only status.

### 1.164 Addendum (2026-03-15): Multi-Radio Phase F Slice 3 (VarAC Cluster Enforcement and Gateway Exclusivity)

Problem:
- Slice 2 makes VarAC clusters visible and editable, but operators still need hard guardrails for cluster membership integrity.
- The architecture explicitly calls for unique instance numbers and one gateway handler per cluster, yet the current branch has no first-class enforcement or runtime annotation for those constraints.

Goals:
- Enforce cluster membership integrity in the store layer with explicit operator-facing validation errors.
- Make gateway-handler responsibility visible in runtime and Settings surfaces.
- Keep enforcement prompt-safe and additive without rewriting the primary compatibility control model.

Scope:
- Store validation:
  - require enabled cluster memberships to reference an existing cluster and device.
  - require positive `instance_number` values.
  - enforce unique instance numbers per cluster with clear validation messages.
  - enforce one enabled cluster membership per device in this phase.
  - require `gateway_handler_device_id`, when set, to refer to an enabled member of that cluster.
  - block deletion or disablement of the current gateway-handler membership until the handler is cleared or reassigned.
- Derived coordination metadata:
  - derive persisted `gateway_exclusive` coordination rows from cluster definitions for auditability and future coordinator work.
- Runtime/UI annotations:
  - Station Overview cards show whether a cluster member is:
    - gateway handler
    - non-handler member
    - cluster member awaiting handler selection
  - cluster-level warnings appear when:
    - shared DB path is missing
    - cluster membership is incomplete for the current gateway selection
  - Settings hints and table summaries explain the enforcement state clearly.

Out of scope:
- Automatic gateway failover
- Automatic instance renumbering
- Full coordinator actions driven by `gateway_exclusive` policies
- Multi-cluster membership per device

Constraints:
- Store enforcement must fail cleanly with actionable messages; raw `sqlite3.IntegrityError` should not leak to the operator.
- Existing non-cluster VarAC workflows must remain unaffected.
- Gateway exclusivity remains a configuration/runtime visibility rule in this phase, not a hidden background auto-action.

Acceptance criteria:
- Duplicate instance numbers in the same cluster are rejected with a clear validation message.
- A device cannot hold more than one enabled VarAC cluster membership.
- Gateway handler selection is limited to enabled members of the selected cluster.
- Removing or disabling the gateway-handler membership is blocked until the cluster gateway handler is cleared or reassigned.
- Runtime and Settings surfaces clearly show gateway role and missing-handler warnings.
- Derived `gateway_exclusive` policies remain stable and duplicate-free.
- Verification passes:
  - `python -m pytest tests/test_multi_radio_phase_f_slice3.py`
  - `python -m pytest tests/test_multi_radio_phase_f_slice2.py tests/test_multi_radio_phase_f_slice1.py`
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`
  - `powershell -ExecutionPolicy Bypass -File .\\tools\\freqinout-db.ps1 status`

Deferred boundary after this slice:
- Shared-cluster ingest deduplication and cluster-aware message routing remain future work.
- Full coordinator/runtime actions driven by `gateway_exclusive` policies remain future work.
- Control tabs and scheduler continue to act on the primary compatibility device only.

Rollback:
- Revert the Slice 3 spec addendum, cluster-enforcement store/runtime/UI changes, derived gateway policy support, and targeted tests together so the branch returns to Slice 2 behavior.

### 1.165 Addendum (2026-03-15): Multi-Radio Phase G Slice 1 (Station Summary UX and Refresh-Path Hardening)

Problem:
- Phase A-F land the core multi-radio architecture, but the operator surfaces still make advanced station state harder to scan than it needs to be.
- `Station Overview` currently rebuilds every device card on every timer refresh, even when the rendered runtime state has not changed.
- `Settings` currently refreshes the multi-radio tables through chained reload paths, which causes duplicate table rebuilds and repeated store queries during load and common edit workflows.
- `Station Overview` also reloads settings on each refresh tick just to resolve theme values, which is unnecessary after the app-wide theme has already been applied.

Goals:
- Give operators a compact station-level summary that highlights primary state, active device count, observer presence, VarAC cluster state, and pending operator attention items without forcing table-by-table inspection.
- Keep the existing visual design language and table workflows while reducing unnecessary UI churn on periodic refresh.
- Preserve the current compatibility runtime model and all current operator actions.

Scope:
- `Station Overview` UX:
  - add a compact station alerts strip above the device cards.
  - summarize high-value station state in plain language:
    - active device count
    - primary compatibility device
    - observer count
    - VarAC member count
    - live attention items such as:
      - primary `minimal` mode
      - active temporary swap
      - shared-PTT blocks
      - runtime warnings
  - keep the existing per-device card structure, but avoid rebuilding cards when the rendered snapshot data is unchanged.
- `Station Overview` refresh hardening:
  - stop reloading `SettingsManager` on every overview refresh tick.
  - avoid polling/re-rendering the overview while the tab is inactive.
  - cache the last rendered overview signature so repeated identical snapshots do not trigger card destruction/recreation.
- `Settings` UX:
  - add a compact `Station Summary` section above the structured multi-radio tables.
  - surface the current station configuration in one read-only summary, including:
    - active device count
    - primary device identity
    - operating-profile assignment coverage
    - VarAC cluster/member counts
    - pending configuration attention items
  - keep the current table/dialog patterns; no new wizard-style workflow is introduced in this slice.
- `Settings` refresh hardening:
  - batch the structured multi-radio table refresh path so:
    - `Device Profiles`
    - `Operating Profiles`
    - `Device Assignments`
    - `VarAC Clusters`
    - `VarAC Memberships`
    refresh in a predictable order without duplicate cascaded reloads.
  - reuse that batched path from Settings load and common multi-radio mutations.

Out of scope:
- New scheduler or coordinator behavior
- Concurrent non-primary control-tab ownership
- Cluster ingest deduplication or message-routing changes
- Large-scale table virtualization or a custom model/view rewrite
- Manual smoke-only UX polish that is not safely automatable in this slice

Constraints:
- Existing device/profile/assignment/cluster CRUD behavior must remain unchanged.
- Refresh hardening must not hide real state changes; force refresh remains available for explicit reload paths.
- The overview and Settings summaries must stay advisory-first and must not become another source of runtime truth separate from the existing store/runtime manager.
- Multi-radio UI changes must preserve the current desktop design language and not introduce dense operator clutter.

Acceptance criteria:
- `Station Overview` shows both a compact station summary and a separate attention strip without removing existing per-device detail.
- Repeated overview refreshes with unchanged runtime data do not rebuild all device cards.
- Inactive overview tabs no longer poll/re-render on the normal timer path.
- Settings shows a `Station Summary` section that makes primary state, assignment coverage, and VarAC cluster health easier to scan.
- Multi-radio Settings refreshes run through a single ordered batch path instead of chained duplicate refresh cascades.
- Verification passes:
  - `python -m pytest tests/test_multi_radio_phase_g.py`
  - `python -m pytest tests/test_multi_radio_phase_f_slice3.py tests/test_multi_radio_phase_f_slice2.py tests/test_multi_radio_phase_f_slice1.py tests/test_multi_radio_phase_e.py`
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`
  - `powershell -ExecutionPolicy Bypass -File .\\tools\\freqinout-db.ps1 status`

Deferred boundary after this slice:
- Shared-cluster ingest deduplication and cluster-aware data routing remain future follow-on work.
- Runtime summaries remain card-based; this slice does not add a new dock, matrix view, or heatmap-style dashboard.
- Manual desktop validation of the new summary text and spacing is still required after the automated pass.

Rollback:
- Revert the Phase G addendum, Station Overview summary/refresh changes, Settings summary/refresh batching changes, and targeted tests together so the branch returns to the prior multi-radio UI behavior.

### 1.166 Addendum (2026-03-15): Multi-Radio Phase G Slice 2 (Cluster-Aware VarAC Ingest and Operator-Surface Deduplication)

Problem:
- Phase F made VarAC clusters visible and enforceable, but the actual ingest path still behaves like one station-global VarAC source.
- `background_ingest.py` and all direct `ingest_varac(...)` callers still resolve only one legacy `varac_db_path` / install path, so:
  - shared-cluster data is not treated as a first-class ingest source
  - multiple active VarAC devices cannot ingest independently without collisions
  - a shared cluster DB can be multiply represented downstream if multiple devices point at the same feed
- `varac_messages` currently keys rows by `(source, id)` where `source` is only the VarAC table family (`qso`, `vmail`, `broadcast`), which is insufficient once more than one VarAC source exists.
- Operator surfaces still cannot clearly tell whether VarAC traffic came from a shared cluster or a standalone device/runtime.

Goals:
- Make VarAC ingest source-aware across the current station model.
- Deduplicate shared-cluster ingest so one shared VarAC DB/feed is ingested once, not once per member device.
- Preserve existing single-instance VarAC behavior for operators who do not use clusters.
- Surface source identity and shared-cluster ingest status in current operator views without rewriting the broader message architecture.

Scope:
- `freqinout/core/varac_ingest.py`
  - add station-aware VarAC ingest source resolution using:
    - active device profiles
    - enabled VarAC cluster memberships
    - shared cluster DB path when configured
    - deterministic fallback to one device-local DB path when a cluster lacks a shared DB path
  - treat each logical VarAC ingest source as:
    - `legacy`
    - standalone device
    - shared cluster
  - deduplicate ingest sources by resolved DB/feed path so shared-cluster data is not ingested multiple times.
  - preserve backward compatibility by keeping single-instance legacy ingest available when no structured sources exist.
  - add persisted source metadata to mirrored VarAC rows and sync-status rows so later consumers can distinguish:
    - logical row type (`qso`, `vmail`, `broadcast`)
    - ingest source identity
    - source label
    - cluster name / cluster public ID
    - source DB path
  - add per-source checkpointing so incremental watermarks do not collide across multiple VarAC sources.
- `freqinout/core/background_ingest.py`
  - run the cluster-aware/station-aware VarAC ingest path instead of assuming one global source.
- `freqinout/gui/message_viewer_tab.py`
  - read the new VarAC source metadata from local mirrored rows.
  - keep message operations keyed to the local mirrored row identity while preserving the underlying VarAC table name needed for delete/update actions.
  - show cluster/source identity cleanly in VarAC row titles and detail panes.
  - avoid duplicate VarAC rows in operator views by relying on deduplicated ingest sources.
- `freqinout/core/station_runtime_manager.py`
  - load latest per-source VarAC ingest status from the local nets DB.
  - annotate cluster-member snapshots with cluster ingest health/recency in addition to current membership metadata.
  - reflect ingest failures or stale shared-cluster status in the existing `VarAC Cluster` service surface when warranted.
- `freqinout/gui/controlfreq_tab.py`
  - make the VarAC summary row source-aware so shared-cluster operation is represented as shared station state, not implied duplicate per-device state.
  - keep counts accurate after source-aware ingest.
- schema/tooling
  - extend local VarAC mirror table definitions and DB-tool schema descriptions so tooling does not drift from runtime.

Out of scope:
- Full cluster-aware VarAC message-routing or send-path ownership
- Automatic gateway failover or shared-cluster repair
- Cross-device JS8 multi-instance traffic ingest
- A new dedicated VarAC cluster dashboard
- Per-device ControlFreq or Message Viewer contexts

Constraints:
- Existing single-instance/non-cluster VarAC workflows must remain unchanged unless the operator enables structured multi-radio VarAC sources.
- Shared-cluster dedup must be deterministic and operator-auditable.
- Row identity for local mirrored VarAC content must remain stable enough for read/flag/delete actions after the schema expansion.
- Slice 2 must remain additive: existing local DBs should migrate in place without destructive resets.

Acceptance criteria:
- Cluster-aware source resolution ingests one shared VarAC cluster DB/feed once even when multiple active devices are members.
- Standalone non-cluster VarAC devices still ingest normally alongside clustered devices.
- Mirrored VarAC rows retain the underlying VarAC table family needed for delete/update actions while also exposing source/cluster identity.
- Message Viewer shows cluster/source identity on VarAC rows and detail views without duplicate rows from shared clusters.
- Station runtime snapshots include cluster-ingest status annotations beyond raw membership metadata alone.
- ControlFreq VarAC summary counts remain accurate and show shared-cluster/source context when present.
- Verification passes:
  - `python -m pytest tests/test_multi_radio_phase_g_slice2.py`
  - `python -m pytest tests/test_multi_radio_phase_g.py tests/test_multi_radio_phase_f_slice3.py tests/test_multi_radio_phase_f_slice2.py`
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`
  - `powershell -ExecutionPolicy Bypass -File .\\tools\\freqinout-db.ps1 status`

Deferred boundary after this slice:
- Cluster-aware VarAC send/ownership arbitration remains future work.
- Shared-cluster ingest dedup lands here, but broader shared-data routing across all tabs still remains a later follow-on.
- JS8 multi-instance traffic ingest still needs its own explicit addendum.

Rollback:
- Revert the Phase G Slice 2 addendum, VarAC source-aware ingest/schema changes, runtime/viewer/control summary updates, and targeted tests together so the branch returns to the prior single-source VarAC ingest behavior.

### 1.167 Addendum (2026-03-15): Multi-Radio Phase G Slice 3 (JS8 Multi-Instance Ingest and Source-Aware Messages)

Problem:
- The current JS8 ingest path still assumes one station-global JS8Call instance.
- `freqinout/core/message_ingest.py` ingests only one inbox DB and one `DIRECTED.TXT`, keyed by one legacy `js8_directed_path`.
- `freqinout/gui/message_viewer_tab.py` duplicates that same single-source JS8 ingest/cache logic, so source assumptions are split across two code paths.
- `freqinout/gui/stations_map_tab.py` and the background `js8_links` ingest path also assume one `DIRECTED.TXT` and one `ALL.TXT`.
- In a real multi-rig station, each JS8Call instance has its own instance directory, `DIRECTED.TXT`, `ALL.TXT`, and inbox DB, and the operator needs to know which rig received a message in the Messages tab.

Goals:
- Make JS8 ingest source-aware across multiple active JS8Call device profiles without regressing single-instance behavior.
- Preserve operator visibility by showing rig/source identity on JS8 and JS8 Spotter rows in Messages.
- Keep performance high by using per-source checkpoints, deduplicated source discovery, and batched local DB writes instead of repeated full rescans.
- Keep current primary-device compatibility behavior for JS8 control tabs; this slice is about ingest and operator visibility, not multi-context JS8 control.

Scope:
- Device/profile configuration:
  - extend device profiles with explicit per-instance JS8 ingest paths:
    - `js8_directed_path`
    - optional `js8_inbox_path`
  - keep `ALL.TXT` derived from the resolved `DIRECTED.TXT` directory unless a later slice needs its own explicit override.
  - project the primary device's `js8_directed_path` back into the legacy flat settings key so current primary-only JS8 tabs keep working.
- Shared JS8 source resolution:
  - add one shared resolver for JS8 instance sources that:
    - loads runtime-active device profiles
    - resolves each device's JS8 inbox / `DIRECTED.TXT` / `ALL.TXT`
    - falls back to legacy flat settings when structured device paths are absent
    - deduplicates identical instance directories / files so the same source is not ingested twice
    - emits a stable logical source key plus operator-facing source label
- Local JS8 mirror/schema:
  - replace the current single-source local mirror assumptions with source-aware v2 tables:
    - `js8_messages_v2`
    - `js8_inbox_state_v2`
    - `js8_inbox_ingest_state_v2`
    - `js8_spotter_ingest_state_v2`
    - `js8_links_ingest_state_v2`
  - preserve existing local data by migrating legacy single-source rows into the v2 tables in place when safe.
  - store source metadata on mirrored JS8 rows, including:
    - source key
    - source scope (`legacy` or `device_profile`)
    - source label
    - device profile id when known
    - resolved inbox / `DIRECTED.TXT` / `ALL.TXT` paths
    - upstream JS8 inbox row id (`remote_id`)
- `freqinout/core/message_ingest.py`
  - ingest JS8 inbox rows from all resolved sources using per-source checkpoints instead of one global `MAX(id)`.
  - ingest Spotter traffic from all resolved `DIRECTED.TXT` sources using dedicated per-source Spotter offsets instead of one global `spotter_directed_offset`.
  - batch local inserts/updates per source instead of opening the local DB once per row.
  - keep duplicate Spotter suppression source-aware so the same traffic can still be represented separately when it was received on different rigs.
- `freqinout/core/background_ingest.py`
  - continue to use `MessageIngestor`, which now becomes multi-source automatically.
- `freqinout/gui/stations_map_tab.py`
  - make `JS8LogLinkIndexer` use the same source resolver for multiple `DIRECTED.TXT` / `ALL.TXT` pairs.
  - keep `js8_links` aggregated as a station-level view for now, but use dedicated per-source link offsets/checkpoints so multiple JS8 instance logs do not collide and do not interfere with Spotter ingest.
- `freqinout/gui/message_viewer_tab.py`
  - stop relying on the duplicated single-source JS8 ingest path for steady-state operation; ingest should route through the shared JS8 ingest implementation.
  - load JS8 and Spotter rows from source-aware local mirrors.
  - show source/rig identity on JS8 and Spotter rows in the current table/detail design rather than adding a new column-heavy layout.
  - keep read, flag, and delete actions keyed by logical source plus remote JS8 row id so two instances can carry the same upstream row id safely.
- Downstream consumers:
  - update propagation-outcome ingest and any other local JS8 readers to consume the source-aware mirror rather than the old single-source mirror.
- Schema/tooling/tests:
  - extend DB init/tooling definitions for the new JS8 v2 tables and any new source-aware Spotter columns.
  - add targeted automated coverage for:
    - source resolution and deduplication
    - multi-source JS8 inbox ingest
    - multi-source Spotter ingest
    - Messages provenance/read-delete behavior
    - multi-source JS8 links ingest

Out of scope:
- Multiple concurrent JS8 net-control tabs or per-device JS8 control contexts
- A new JS8 station dashboard
- Per-device Map or Message Viewer instances
- Automatic conflict handling driven by JS8 message provenance
- Full operator-history or net-control refactors beyond preserving primary-device compatibility

Constraints:
- Existing single-instance JS8 behavior must continue to work with only legacy flat settings configured.
- The Messages tab must not block on repeated full rescans of all configured JS8 sources.
- Source-aware local mirrors must tolerate device renames and path updates without destructive resets.
- Current primary-device legacy tabs that still read `js8_directed_path` must continue to follow the primary compatibility device.
- Secondary-device JS8 ingest must remain additive and advisory; this slice does not transfer JS8 control ownership away from the primary device.

Acceptance criteria:
- Multiple active JS8 device profiles can each contribute inbox and Spotter traffic from their own JS8 instance directory.
- Mirrored JS8 rows carry stable source identity so read/delete operations remain correct even when different sources reuse the same upstream inbox row id.
- Messages clearly shows which rig/source received each JS8 and Spotter message without adding a new dense table layout.
- Background ingest and manual/tab refresh paths use per-source checkpoints and do not rescan the full history of every configured JS8 instance on each poll.
- JS8 link ingestion (`DIRECTED.TXT` / `ALL.TXT`) can ingest from multiple resolved JS8 sources without offset collisions.
- Verification passes:
  - `python -m pytest tests/test_js8_multi_instance_ingest.py`
  - `python -m pytest tests/test_message_viewer_js8_multi_instance.py`
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`
  - `powershell -ExecutionPolicy Bypass -File .\\tools\\freqinout-db.ps1 status`

Deferred boundary after this slice:
- JS8 control/net tabs remain primary-device scoped even though ingest becomes multi-source.
- Station-level JS8 link views remain aggregated; this slice does not yet add per-rig link visualization.
- Operator-history and net-control backfill paths can adopt the shared JS8 source resolver in later follow-on work.

Rollback:
- Revert the Phase G Slice 3 addendum, device-profile JS8 path additions, shared JS8 source-resolution helpers, source-aware JS8/Spotter schema and ingest changes, JS8 links multi-source updates, Message Viewer provenance changes, and targeted tests together so the branch returns to the prior single-source JS8 behavior.

### 1.168 Addendum (2026-03-15): Device Profile Editor Progressive Disclosure and Monitor Fit

Problem:
- The `Settings -> Device Profiles -> Add/Edit Device Profile` dialog has grown into one long flat form.
- On smaller displays and common laptop monitor heights, the dialog can exceed the visible screen and push the Save / Cancel controls out of reach.
- The current all-at-once layout also overwhelms operators because advanced JS8, VarAC, SDR, launch, and RF-coordination fields are mixed together with the basic profile identity and backend fields.

Goals:
- Make the device-profile editor fit reliably on common monitors so Save / Cancel actions remain reachable.
- Reduce operator overload with progressive disclosure while preserving all current device-profile fields and save semantics.
- Reuse the app's existing collapsible-section design language instead of introducing a disconnected one-off editor style.

Scope:
- `freqinout/gui/settings_tab.py`
  - Refactor the device-profile editor from one flat `QFormLayout` into a scrollable dialog with fixed footer actions.
  - Group fields into manageable collapsible sections, with defaults optimized for the common editing path:
    - `Profile Basics`
    - `Endpoints`
    - `JS8 Instance Files`
    - `VarAC & Launch`
    - `Coordination & Notes`
  - Keep `Profile Basics` and `Endpoints` expanded by default.
  - Collapse advanced sections by default unless the existing profile already contains meaningful values in that section.
  - Keep Save / Cancel outside the scroll area so they remain reachable even when the content is taller than the viewport.
  - Reduce vertical sprawl where practical, including pairing host/port fields within the same endpoint row.
  - Preserve current backend-hint messaging, but make the hint work within the new grouped layout.
- Tests:
  - Add focused UI coverage for:
    - presence of the scrollable editor shell
    - existence of collapsible device-profile sections
    - ability to expand a collapsed section and retain access to the Save action

Out of scope:
- Changing the device-profile data model
- Removing any currently supported fields
- Adding a wizard flow or multi-page modal
- Reworking non-device dialogs in Settings

Constraints:
- All existing device-profile save/load behavior must remain backward compatible.
- The redesign must not hide required core fields behind an initially collapsed advanced section.
- The dialog must remain functional in offscreen Qt tests and on lower-height displays.

Acceptance criteria:
- The device-profile editor opens inside a scrollable container with Save / Cancel anchored in a fixed footer.
- Operators can access Save / Cancel without needing the full dialog height to fit on screen.
- Basic identity/runtime fields are immediately visible on open.
- Advanced JS8, VarAC, and RF-coordination fields are organized into clearly labeled collapsible sections and are not all expanded by default for a new profile.
- Existing profiles with JS8, VarAC, or coordination settings pre-populate and auto-expand the relevant sections so important configured values are not hidden.
- Verification passes:
  - `python -m pytest tests/test_multi_radio_phase_b_slice2.py`
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Rollback:
- Revert the device-profile editor restructuring, the new scrollable/collapsible dialog helpers, and the targeted UI tests together so the previous flat editor returns unchanged.

### 1.169 Addendum (2026-03-16): Settings Status Refresh De-Janking and Device Profile Path Browsers

Problem:
- The Settings surface currently refreshes radio-software status by probing endpoints directly from the UI thread.
- When configured device endpoints are unreachable, the 5-second Settings status timer and activation refresh can stall the entire Settings experience, including the Device Profiles area and the device editor dialog.
- The device-profile editor now uses progressive disclosure, but several file and directory fields still require manual path entry even when the operator is selecting a local file or folder.

Impacted files and failure modes:
- `SPEC.md`
  - Failure mode: this slice drifts into broader runtime-health architecture instead of fixing the Settings-specific UI stall and browse affordances.
- `freqinout/gui/settings_tab.py`
  - Failure mode: endpoint polling still blocks the GUI thread, refresh requests overlap and pile up, or browse controls add clutter without reducing operator effort.
- `freqinout/core/software_status_service.py`
  - Failure mode: existing probe logic is duplicated instead of being reused through a background helper.
- `tests/test_software_status_phase4.py`
  - Failure mode: unsaved endpoint overrides are no longer respected when a status refresh is requested from Settings.
- `tests/test_multi_radio_phase_b_slice2.py`
  - Failure mode: the device editor loses its monitor-fit behavior or browse affordances regress silently.

Scope:
- Keep the existing `SoftwareStatusService` probe logic, but move the Settings-tab `status_snapshot(...)` execution off the UI thread:
  - use one background worker dedicated to Settings status refresh
  - coalesce overlapping refresh requests so unreachable endpoints do not create a backlog
  - continue using unsaved host/port values from the Settings form when building the request
- Preserve current UI behavior for the status LEDs and tooltips:
  - apply results back on the UI thread only
  - keep activation refresh and 5-second timer behavior, but make those triggers lightweight
- Add browse affordances in the device-profile editor for local file and directory fields:
  - `JS8 Profile Dir`
  - `JS8 DIRECTED.TXT`
  - `JS8 Inbox DB`
  - `Launch Path`
  - `VarAC Install`
  - `VarAC DB Path`
  - `VarAC INI Path`
- Keep the device editor compact:
  - path rows stay inside the existing collapsible sections
  - do not add extra modal steps for common file and folder selection

Out of scope:
- A full rewrite of status polling for other tabs
- New station-health dashboards or richer probe detail
- Automatic path discovery for every field
- Browse affordances for free-form command strings such as `VarAC Launch Cmd`

Constraints:
- UI responsiveness is the priority: unreachable endpoints must no longer freeze Settings interactions.
- The background refresh path must remain single-purpose and low-overhead; no overlapping probe storms.
- The device editor must keep its fixed footer and scrollable body behavior from Addendum `1.168`.

Acceptance criteria:
- Opening or editing device profiles remains responsive while configured device endpoints are unreachable.
- `_refresh_running_status()` no longer performs the blocking `status_snapshot(...)` call directly on the UI thread.
- Unsaved FLRig, FLDigi, and JS8 endpoint edits are still reflected in the next Settings status refresh request.
- Device-profile path fields listed in scope have working `Browse` controls for the appropriate file or directory type.
- Verification passes:
  - `python -m pytest tests/test_software_status_phase4.py tests/test_multi_radio_phase_b_slice2.py`
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Deferred boundary after this slice:
- Status polling for other tabs remains on their existing architecture.
- No automatic path validation is introduced beyond existing save-time and runtime behavior.

Rollback:
- Revert the Settings status-refresh worker changes, the device-profile browse affordances, the targeted tests, and this spec addendum together so Settings returns to the previous synchronous-refresh behavior.

### 1.170 Addendum (2026-03-16): Runtime Primary Device Persistence Must Survive Restart

Problem:
- Operators can make a different device profile primary during a session, but on restart the seeded legacy/default device may reclaim `runtime_primary`.
- That makes the Device Profiles UI misleading and breaks the expected persistence contract for multi-radio primary selection.
- The likely failure boundary is startup normalization, where default-record seeding and legacy-to-structured mirroring run again on every `SettingsManager` initialization.

Impacted files and failure modes:
- `SPEC.md`
  - Failure mode: the fix drifts into broader runtime-ownership redesign instead of a persistence regression correction.
- `freqinout/core/multi_radio_store.py`
  - Failure mode: startup normalization still treats the default seeded device as preferred even when a valid non-default primary already exists.
- `freqinout/core/settings_manager.py`
  - Failure mode: initialization order continues to reassert the default primary indirectly through store helpers.
- `tests/test_multi_radio_phase_c.py`
  - Failure mode: restart persistence for non-default primary devices regresses silently.

Scope:
- Preserve the operator-selected `runtime_primary` device across restart.
- Keep default-device seeding as a first-run fallback only:
  - if no valid primary exists, normalization may still choose the default seeded device
  - if a valid primary already exists, startup must not override it just because the default device also exists
- Add regression coverage that simulates:
  - initial seed
  - creation/activation of a non-default device
  - setting that device primary
  - restart via fresh `SettingsManager` initialization
  - verifying the chosen non-default device remains primary

Out of scope:
- Changing the single-primary compatibility architecture
- Changing UI affordances for selecting the primary device
- Adding new persistence tables or migration logic

Constraints:
- Existing first-run default-device creation behavior must remain intact.
- Legacy projection must still follow the current runtime primary device.
- The fix must be minimal and safe: no destructive migrations, no resets of operator data.

Acceptance criteria:
- After a non-default device is made primary and settings are reloaded through a fresh `SettingsManager`, that same device remains `runtime_primary`.
- The default seeded device is used only when no valid primary exists.
- Verification passes:
  - `python -m pytest tests/test_multi_radio_phase_c.py`
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Rollback:
- Revert the startup-normalization change, the regression test, and this spec addendum together so persistence behavior returns to the prior state.

### 1.171 Addendum (2026-03-16): Structured JS8, Fast Light, and VarAC Instance Records with Device-Profile Association

Problem:
- The app currently stores JS8Call, Fast Light, and VarAC configuration in two competing places:
  - singleton base-tab settings under legacy flat keys
  - per-device raw endpoint/path fields inside `device_profiles`
- That creates configuration drift and an unclear ownership model. Operators can update a device profile and the base tab separately, with no single authoritative record for a reusable software instance.
- In a multi-rig station, the right unit is not “one global JS8/FL/VarAC config” and not “copy-paste every endpoint/path into every device profile.” It is a reusable software-instance/config record that a device profile can reference.

Impacted files and failure modes:
- `SPEC.md`
  - Failure mode: the slice overreaches into full removal of legacy settings instead of establishing structured records with safe fallback.
- `freqinout/core/multi_radio_store.py`
  - Failure mode: there is no normalized store for reusable JS8/Fast Light/VarAC instance records, or device profiles cannot associate to them safely.
- `freqinout/gui/settings_tab.py`
  - Failure mode: operators still have only singleton forms or duplicated raw per-device fields instead of reusable configs plus association.
- `freqinout/core/station_runtime_manager.py`
  - Failure mode: runtime still ignores associated instance records and continues to rely only on raw device-profile fields.
- `freqinout/core/launch_orchestrator.py`
  - Failure mode: launch resolution still depends exclusively on legacy singleton paths instead of device-associated instance data.
- `freqinout/core/js8_multi_source.py`
  - Failure mode: multi-instance JS8 ingest ignores associated JS8 instance records.
- `freqinout/core/varac_ingest.py`
  - Failure mode: VarAC ingest ignores associated VarAC node records.
- targeted tests
  - Failure mode: the new association layer regresses silently and operators end up back in split-authority configuration.

Scope:
- Add structured reusable records for software instances/configs:
  - `js8_instances`
  - `fast_light_configs`
  - `varac_nodes`
- Add optional associations on `device_profiles`:
  - `js8_instance_id`
  - `fast_light_config_id`
  - `varac_node_id`
- Seed one default record for each new table from current legacy singleton settings so existing stations remain functional.
- Update the base Settings areas to manage multiple structured records:
  - `JS8Call Settings`
    - keep station-wide JS8 preferences in place
    - add CRUD for JS8 instance records
  - `Fast Light Settings`
    - keep station-wide FLMsg/FLAmp and message-path behavior in place for now
    - add CRUD for reusable FLRig/FLDigi config records
  - `VarAC Settings`
    - keep station-wide BBS/archive policy in place for now
    - add CRUD for reusable VarAC node records
- Update the device-profile editor to select structured records:
  - choose one JS8 instance
  - choose one Fast Light config
  - choose one VarAC node
  - retain existing raw per-device fields only as legacy fallback when no associated record is selected
- Update runtime/settings projection to prefer associated records over raw per-device fields when present.

Out of scope:
- Full removal of legacy singleton settings keys
- Per-instance FLMsg or FLAmp management in this slice
- Automatic conversion of every existing raw device profile into a structured record
- Deep redesign of scheduler/control ownership

Constraints:
- Existing stations must keep working after upgrade with no manual migration required.
- Legacy singleton settings remain as fallback/default compatibility in this slice.
- New multi-instance work should prefer structured records and device-profile association over new raw duplication.
- UI changes must stay incremental and recognizable inside the current Settings design system.

Acceptance criteria:
- Operators can create multiple JS8 instance records, Fast Light configs, and VarAC node records from the corresponding base Settings areas.
- Device profiles can associate to those structured records instead of re-entering duplicate endpoint/path data.
- Runtime projection and settings compatibility use the associated structured record when present.
- Existing legacy-only installs continue to function through default seeded records and fallback behavior.
- Verification passes:
  - `python -m pytest tests/test_multi_radio_phase_a.py tests/test_multi_radio_phase_b_slice2.py tests/test_multi_radio_phase_c.py`
  - `python tools/release_preflight.py`
  - `python -m compileall freqinout`

Deferred boundary after this slice:
- FLMsg/FLAmp remain station-global.
- Legacy singleton editors remain present as compatibility/default surfaces until a later cleanup slice removes or demotes them further.
- Automatic one-click conversion of legacy raw per-device overrides into structured shared records remains future work.

Rollback:
- Revert the structured instance-record schema/store changes, Settings UI CRUD/association changes, runtime-resolution updates, targeted tests, and this spec addendum together so the app returns to the prior singleton-plus-raw-device-field model.

### 1.172 Addendum (2026-03-16): JS8 Settings Accordion Layout for Default and Additional Instances

Problem:
- The JS8 Settings area currently presents the default compatibility fields inline, but additional JS8 instances are managed through a separate table and popup editor.
- That makes the default instance feel structurally different from the others and adds friction for operators managing several JS8Call-improved instances.

Scope:
- Keep the existing JS8 compatibility behavior and settings keys unchanged.
- Rework the JS8 Settings UI so the default compatibility record is presented as a collapsible `Default` instance card.
- Present each additional managed JS8 instance as an inline collapsible card in the same section, using the instance name in the header.
- Make the JS8 instance accordion single-open: expanding one instance collapses the others.
- Replace popup-first editing for JS8 instances with inline editing and save/delete actions on the expanded card.
- Keep `Load JS8 Traffic` and related JS8 operator tooling in the section.

Out of scope:
- No equivalent accordion rewrite for Fast Light or VarAC in this slice.
- No changes to JS8 ingest/runtime semantics.
- No migration of default JS8 save behavior away from the current singleton compatibility fields.

Acceptance criteria:
- In Settings, the JS8 section shows one default card and one card per additional JS8 instance.
- Expanding one JS8 card collapses the other JS8 cards.
- Adding a JS8 instance creates an inline card without opening a popup editor.
- Editing and saving an inline JS8 instance updates the managed store and refreshes any primary-device compatibility projection when needed.
- Automated tests cover the accordion rendering and single-open behavior.

Rollback:
- Revert the JS8 Settings layout changes and related tests together, leaving the structured instance store intact.

### 1.173 Addendum (2026-03-16): JS8 Settings Responsiveness Hardening and Inline Accordion Geometry Fix

Problem:
- Operators report the JS8 Settings area becoming barely responsive when the configured JS8 endpoint is unavailable.
- The Settings status worker is already asynchronous, but other JS8 control paths can still attempt a `js8net` attach from active application flows, which is unnecessary and potentially expensive when the configured TCP API is refusing connections.
- The new inline JS8 accordion cards hide their content, but the enclosing card can retain its expanded height, leaving large dead space and making the section unusable on smaller displays.

Scope:
- Keep the Settings status polling path asynchronous.
- Make JS8 API status probes prefer fast socket checks and avoid implicit `js8net` fallback unless a caller explicitly asks for it.
- Harden `JS8ControlClient` so it preflights the configured JS8 endpoint before attempting `js8net.start_net(...)` and backs off briefly after a failed attach.
- Update the inline/dialog collapsible helper so collapsed cards shrink to header height and release layout space immediately.
- Cover both the JS8 attach hardening and accordion collapse behavior with targeted regression tests.

Out of scope:
- No redesign of scheduler ownership or JS8 control semantics.
- No new background worker architecture beyond the existing Settings status worker.
- No broader Fast Light or VarAC accordion rewrite in this slice.

Acceptance criteria:
- Visiting JS8 Settings does not synchronously invoke `js8net` through the Settings status path.
- A refused JS8 TCP endpoint does not repeatedly drive slow `js8net.start_net(...)` attempts in active UI/control flows.
- Collapsing a JS8 instance card reduces the card to header height so adjacent configuration remains visible.
- Targeted automated tests cover the fast-fail JS8 attach behavior and the card-height collapse behavior.

Rollback:
- Revert the JS8 status hardening, collapsible-layout helper changes, targeted tests, and this spec addendum together.

### 1.174 Addendum (2026-03-16): UI Responsiveness Hardening for Settings, ControlFreq, and Station Overview

Problem:
- Operators report that the UI becomes barely usable while configuring multi-radio settings, especially when endpoints are unavailable or JS8 data files are large.
- The remaining performance issues are no longer centered on the Settings status worker itself. The main regressions are:
  - JS8 geo backfill work still running from the UI path
  - direct synchronous status/runtime snapshot work in `ControlFreq` and `Station Overview`
  - broad `_load_settings()` reloads after local managed-record and primary-device changes
  - duplicated JS8 labeling in `Station Overview` cards

Impacted files and failure modes:
- `SPEC.md`
  - Failure mode: the slice drifts into large architecture changes instead of focused UI responsiveness hardening.
- `freqinout/gui/settings_tab.py`
  - Failure mode: JS8 geo backfill still blocks the GUI thread, and local device/JS8 edits still trigger full Settings reloads.
- `freqinout/gui/controlfreq_tab.py`
  - Failure mode: status refresh still calls `status_snapshot()` directly on the GUI thread.
- `freqinout/gui/station_overview_tab.py`
  - Failure mode: runtime snapshots still probe status synchronously on refresh, and cards repeat JS8 labeling in confusing ways.
- `freqinout/gui/main_window.py`
  - Failure mode: tabs do not share the new async status path and continue to refresh hidden runtime surfaces unnecessarily.
- new shared GUI status helper
  - Failure mode: each tab continues to solve async status separately, increasing duplication and regressions.
- targeted tests
  - Failure mode: UI-thread regressions return silently during later multi-radio work.

Scope:
- Move JS8 geo backfill scheduling in Settings to a one-shot background worker.
- Introduce one shared async GUI status broker used by `ControlFreq` and `Station Overview`.
- Keep `ControlFreq` and `Station Overview` functionally the same, but remove direct synchronous status/runtime probes from their normal refresh paths when the broker is available.
- Replace full `_load_settings()` calls after local JS8 managed-record saves and primary-device projection changes with targeted widget/model refreshes.
- Clean up `Station Overview` card wording so JS8 devices do not show duplicated JS8 identity indicators.

Out of scope:
- No redesign of scheduler behavior or station-runtime data model.
- No broader refactor of all tab refresh paths.
- No change to the underlying status semantics or device-profile store model.

Constraints:
- Existing visible behavior should remain stable aside from improved responsiveness and clearer station-card wording.
- Runtime/device-profile changes must still propagate correctly to legacy compatibility widgets and other tabs.
- Hidden tabs should avoid expensive refresh work where practical.

Acceptance criteria:
- Opening Settings or saving local device/JS8 changes does not run JS8 geo backfill on the GUI thread.
- `ControlFreq` and `Station Overview` no longer perform direct synchronous status/runtime snapshot probes on their main refresh paths when the async broker is present.
- Primary-device and managed-record changes update the relevant Settings widgets without forcing a full `_load_settings()` rebuild.
- `Station Overview` no longer shows duplicated JS8 identification text for JS8-backed devices.
- Automated tests cover:
  - async broker dispatch usage in `ControlFreq` and `Station Overview`
  - background JS8 geo backfill scheduling
  - targeted Settings projection refresh without `_load_settings()` in the local save path
  - cleaned-up station overview JS8 labeling

Rollback:
- Revert the shared async status helper, Settings backfill/save-path changes, `ControlFreq` and `Station Overview` wiring, UI wording/test updates, and this spec addendum together.

### 1.175 Addendum (2026-03-16): Multi-Radio Operating UX Simplification and Screen-by-Screen Implementation Plan

Problem:
- The current branch successfully adds multiple active device profiles, per-device software associations, station coordination, VarAC clusters, and multi-instance ingest, but the operator experience is still too close to the original single-radio mental model.
- The result is avoidable ambiguity:
  - `ControlFreq` still reads like a station-global control surface even though operators need a clear per-rig control target.
  - `FreqPlanner` remains primary-device oriented and does not explain which rig or schedule target it is showing.
  - `Messages` now ingest multiple sources, but the receive rig/source identity must be obvious at a glance.
  - NCS actions are device-sensitive, but the UI does not consistently expose which rig a given NCS action will run on.
  - Daily and Net schedules now need practical assignment at schedule level, not only row-by-row targeting.
- The branch therefore needs a consistent multi-radio UX model that remains intuitive for:
  - new users who are effectively operating one radio
  - experienced operators running two or three radios plus an observer / SDR
  - operators moving between simple and advanced stations without relearning the app

Product direction:
- FreqInOut should present a stable `station` model with two clear scopes:
  - `station-wide` surfaces for shared awareness, unified traffic, conflicts, and readiness
  - `device-scoped` surfaces for frequency control, planner focus, and NCS execution
- The UI should be adaptive by active station configuration, but not structurally inconsistent:
  - one active TX/RX radio should feel very close to `origin/main`
  - two or more active TX/RX radios should reveal explicit device context selectors and station-wide summaries
  - observer / SDR devices should be visible but should not pollute TX-control workflows by default

UX principles:
- Preserve the clean single-radio baseline whenever a station only has one active TX/RX device.
- Make device scope explicit everywhere an action can affect only one rig.
- Prefer `Selected Radio`, `Station Default`, `Runs On`, and `Inherited` over operator-facing use of `Primary`.
- Use one persistent station-level context model instead of per-tab ad hoc selectors.
- Keep heavy status and refresh work asynchronous and shared across visible surfaces.
- Prefer progressive disclosure over large always-visible control walls.
- Make per-rig identity persistent and easy to scan:
  - operator-assigned device name
  - optional short label / callsign suffix
  - stable accent/badge color
  - backend + role summary

Impacted files and failure modes:
- `freqinout/gui/main_window.py`
  - Failure mode: the app shell lacks a single source of truth for selected-radio context, so each screen invents its own behavior.
- `freqinout/gui/station_overview_tab.py`
  - Failure mode: station-wide status remains useful, but it is not yet the canonical multi-rig at-a-glance surface.
- `freqinout/gui/controlfreq_tab.py`
  - Failure mode: operators cannot tell which rig is being controlled, which schedule is active for that rig, or whether displayed status is station-wide or device-specific.
- `freqinout/gui/freq_planner_tab.py`
  - Failure mode: planner data remains effectively bound to the compatibility device and gives no clear device/scope selection path.
- `freqinout/gui/message_viewer_tab.py`
  - Failure mode: unified traffic is useful, but receive-source identity is not prominent enough for fast operator judgment.
- `freqinout/gui/fldigi_net_control_tab.py`
  - Failure mode: FLDigi NCS actions can be executed without a clear device target model.
- `freqinout/gui/js8call_net_control_tab.py`
  - Failure mode: JS8 NCS actions remain implicitly tied to the compatibility device, which becomes ambiguous in multi-rig stations.
- `freqinout/gui/local_ncs_tab.py`
  - Failure mode: local net actions do not clearly express target device context.
- `freqinout/gui/daily_schedule_tab.py`
  - Failure mode: row-level targeting alone is too granular for common practice and makes schedule assignment noisy.
- `freqinout/gui/net_schedule_tab.py`
  - Failure mode: net schedules cannot be assigned cleanly to a default device/profile with selective overrides.
- `freqinout/core/scheduler_engine.py`
  - Failure mode: UI clarity improvements drift away from the actual runtime target-resolution logic.
- `freqinout/core/station_runtime_manager.py`
  - Failure mode: station/device summaries remain too implementation-oriented and do not expose the operator context needed by the new shell.
- `freqinout/core/multi_radio_store.py`
  - Failure mode: schedule/device association data grows in ad hoc ways instead of a consistent inheritance model.

Terminology standard:
- `Selected Radio`
  - The currently focused TX/RX device for device-scoped operating surfaces.
- `Station Default`
  - The compatibility/runtime-default device currently projected into legacy singleton settings.
- `Runs On`
  - The effective device or operating-profile target for a schedule, net session, planner view, or action.
- `Inherited`
  - A row or action uses the target defined by the parent schedule/session rather than an explicit row override.
- `Observer`
  - A non-primary, non-default SDR/observer device that contributes visibility but is not selected by default for TX-control actions.

Global UX model:

1. App shell and station header
- Add a persistent station header above the tab body and below the app title/navigation.
- The station header should contain:
  - compact chips/cards for each active TX/RX device
  - optional observer / SDR chip(s) in a visually secondary treatment
  - selected-radio highlight
  - quick station alerts such as:
    - shared PTT blocked
    - RF conflict warning
    - temporary profile swap active
    - minimal mode active on station default
  - a compact station-wide health summary
- The station header becomes the canonical way to switch the selected radio.
- Switching the selected radio must be fast, non-modal, and visible across tabs that honor device scope.

2. Dynamic display rules
- One active TX/RX device:
  - hide or downplay selected-radio controls
  - preserve the current single-radio flow wherever possible
  - keep station header compact
- Two or three active TX/RX devices:
  - show persistent station header device chips
  - show device-scoped context on affected tabs
  - enable compare/multi-lane views where helpful, but keep one device selected by default
- Observer / SDR active:
  - show observer state in Station Overview and station header
  - do not place observer devices into frequency-control or NCS selectors by default

3. Shared UI rules for device-scoped surfaces
- Any tab that acts on one rig must clearly show:
  - `Selected Radio`
  - `Runs On`
  - backend / endpoint summary
  - current schedule/profile context when relevant
- If a tab supports station-wide aggregated content, the aggregation must be clearly labeled as such.

Screen-by-screen UX specification:

1. Station Overview
- Purpose:
  - canonical at-a-glance station dashboard
  - replaces the old idea of global `Operating Status`
- Required behavior:
  - one card per active device
  - compact station alert strip at top
  - clear labels for:
    - station default
    - selected radio
    - observer / gateway roles
    - current schedule/profile assignment
    - current band/frequency when available
    - blocked / warning conditions
- Interaction:
  - clicking a TX/RX device card sets `Selected Radio`
  - clicking an observer card should optionally focus observer detail, but should not silently change TX-control context
- Notes:
  - station summary stays station-wide
  - service chips must remain deduplicated and readable

2. ControlFreq
- Purpose:
  - primary workbench for live tuning/control on one radio at a time
- Required behavior:
  - top section should become `Selected Radio Control`
  - explicit selected-radio switcher if more than one TX/RX device is active
  - the old `Operating Status` block should be reduced to selected-radio status only
  - a compact line must show:
    - `Selected Radio`
    - `Runs On`
    - backend
    - current band/frequency
    - effective schedule source for that device
- Message summary:
  - remains station-wide
  - label should make this explicit, for example `Station Message Summary`
- Decision:
  - `Station Overview` supersedes station-wide operating status
  - `ControlFreq` remains useful, but only as a device-scoped control surface

3. FreqPlanner
- Purpose:
  - planner and schedule-awareness view for frequencies/modes over time
- Required behavior:
  - default view follows `Selected Radio`
  - view mode selector:
    - `Selected Radio`
    - `All Active Radios`
    - `Compare`
  - `Selected Radio` mode should preserve current planner simplicity
  - `All Active Radios` should provide a compact station overview of planner/schedule occupancy
  - `Compare` should use parallel lanes/cards only when two or more TX/RX devices are active
- Notes:
  - do not create separate permanent planner tabs per radio
  - planner must always show which device/scope is being visualized

4. Messages
- Purpose:
  - unified station traffic and operator message handling
- Required behavior:
  - continue using a unified inbox/history view
  - every row must show receive/source identity clearly enough for rapid scanning:
    - device profile name
    - software source / instance
    - optional band/frequency when useful and cheap to compute
  - filter bar must support:
    - all radios
    - one selected radio
    - one source/app
  - detail pane/header must make the receive rig explicit
- Notes:
  - the operator should never need to infer receive rig from hidden metadata
  - source labels should be compact badges, not oversized verbose columns

5. NCS tabs (`FLDigi`, `JS8Call`, `Local`)
- Purpose:
  - conduct device-specific net-control actions safely
- Required behavior:
  - each NCS tab must include a required `Run on:` or `Selected Radio` target control
  - when the active net/session matches a schedule target, the device target should default from that schedule
  - once a session is active, the chosen radio should remain pinned and visible in the tab header
  - the tab must show when a session/device choice differs from the schedule default
- Safety rules:
  - if an operator attempts an NCS action on a conflicting or unsupported device, the UI must explain why
  - station coordination warnings must remain visible without overwhelming the operating surface

6. Daily Schedule and Net Schedule
- Purpose:
  - schedule definition and operating-target assignment
- Required behavior:
  - each schedule should have a `Default Target`
  - rows inherit that default target unless explicitly overridden
  - row-level override remains available, but secondary to header-level assignment
- Target options:
  - `Station`
  - `Operating Profile`
  - `Device Profile`
- UI behavior:
  - inherited rows should visibly indicate `Inherited`
  - overridden rows should visibly indicate the override target
  - bulk editing should allow changing the schedule default without rewriting every row
- Notes:
  - the common use case is assigning the whole schedule to a rig/profile
  - row-level targeting is an exception path, not the main workflow

7. Settings
- Purpose:
  - define device identity cleanly enough that operating surfaces stay readable
- Required behavior:
  - device profiles should emphasize:
    - operator-facing device name
    - short label / optional shorthand
    - class and role
    - optional accent/badge identity
  - software associations should stay in Settings, not leak raw technical fields into operating tabs
- Notes:
  - Settings remains the configuration authority
  - operating tabs should select/target configured devices, not edit them

Implementation plan:

Phase H Slice 1: Selected-radio station shell
- Files:
  - `freqinout/gui/main_window.py`
  - new shared selected-radio context helper in `freqinout/gui/` or `freqinout/core/`
  - `freqinout/gui/station_overview_tab.py`
  - `freqinout/core/station_runtime_manager.py`
- Deliverables:
  - persistent station header
  - selected-radio context service
  - device-chip switching
  - clear station default vs selected-radio labeling
- Acceptance:
  - one-radio stations remain visually simple
  - multi-radio stations can switch selected radio from the shell

Phase H Slice 2: ControlFreq selected-radio redesign
- Files:
  - `freqinout/gui/controlfreq_tab.py`
  - `freqinout/core/station_runtime_manager.py`
  - `freqinout/gui/main_window.py`
- Deliverables:
  - selected-radio header in ControlFreq
  - selected-radio operating status
  - explicit station-wide label on message summary
  - schedule/source context line for the selected radio
- Acceptance:
  - operators can always tell which rig ControlFreq is controlling
  - ControlFreq no longer reads as a station-global operating-status page

Phase H Slice 3: FreqPlanner multi-view model
- Files:
  - `freqinout/gui/freq_planner_tab.py`
  - `freqinout/core/scheduler_engine.py`
  - `freqinout/core/station_runtime_manager.py`
- Deliverables:
  - `Selected Radio`, `All Active Radios`, and `Compare` modes
  - planner header showing effective target and current device scope
- Acceptance:
  - one-radio behavior remains simple
  - multi-radio stations can inspect planner state by device without extra tabs

Phase H Slice 4: Unified source-aware Messages and NCS targeting
- Files:
  - `freqinout/gui/message_viewer_tab.py`
  - `freqinout/gui/fldigi_net_control_tab.py`
  - `freqinout/gui/js8call_net_control_tab.py`
  - `freqinout/gui/local_ncs_tab.py`
  - supporting status/runtime files as needed
- Deliverables:
  - stronger per-row receive-rig/source identity in Messages
  - per-tab `Run on:` / selected-radio controls in NCS tabs
  - pinned device identity for active NCS sessions
- Acceptance:
  - operators can tell which rig received a message
  - operators can tell which rig will execute each NCS action

Phase H Slice 5: Schedule-level target inheritance and overrides
- Files:
  - `freqinout/gui/daily_schedule_tab.py`
  - `freqinout/gui/net_schedule_tab.py`
  - `freqinout/core/multi_radio_store.py`
  - `freqinout/core/scheduler_engine.py`
- Deliverables:
  - schedule-level default target
  - row inheritance indicators
  - row override controls
  - scheduler alignment with the new schedule target model
- Acceptance:
  - a whole schedule can be assigned to a device/profile
  - row overrides remain available but are clearly exceptional

Phase H Slice 6: Visual consistency, performance, and polish
- Files:
  - all touched UI surfaces above
  - shared async/broker/context helpers
- Deliverables:
  - consistent badges, labels, and device identity treatment
  - hidden-tab refresh discipline
  - no blocking device/schedule context changes
  - final terminology cleanup away from operator-facing `primary` language
- Acceptance:
  - multi-radio UX feels cohesive rather than bolted-on
  - single-radio UX remains fast and recognizable

Performance requirements:
- No synchronous network/device probes on tab open, tab switch, or context selector change.
- Selected-radio changes must reuse cached runtime state where possible and update only affected surfaces.
- Device-scoped tabs should subscribe to selected-radio context changes rather than re-querying the world independently.
- Hidden tabs must not perform heavy planner/status rebuilds simply because station context changed.
- Compare and all-radios views must degrade gracefully with three TX/RX radios plus an observer.

Out of scope for this addendum:
- No redesign of the underlying radio-control backends beyond what is needed to support clear device targeting.
- No attempt to expose every backend-specific configuration detail on operating tabs.
- No new concurrent scheduler architecture beyond the existing station runtime/scheduler capabilities.
- No change to the underlying ingest schemas beyond surfacing already-ingested source identity more clearly.

Manual verification plan once implementation starts:
- Start with one active TX/RX radio and verify the app still feels close to `origin/main`.
- Activate a second TX/RX radio and verify selected-radio switching in the station header, ControlFreq, FreqPlanner, and NCS tabs.
- Activate a third radio and verify layout density remains usable without overflowing the shell.
- Add an observer / SDR device and verify it appears in station-wide views but not as the default TX-control target.
- Confirm Messages clearly identify receive rig/source across JS8 and VarAC traffic.
- Confirm schedule-level target inheritance and row overrides are visible and understandable.

Acceptance criteria:
- The app presents a stable station header with selected-radio context when more than one active TX/RX device exists.
- ControlFreq, FreqPlanner, and NCS tabs all make device scope explicit and consistent.
- Messages clearly identify which rig/source received each message while preserving a unified station view.
- Daily and Net schedules support schedule-level target assignment with row inheritance and explicit overrides.
- One-radio stations remain simple and close to the existing baseline.
- Multi-radio stations become easier to understand rather than merely more configurable.

Rollback:
- Revert this addendum if the implementation direction changes materially before code work begins.

### 1.176 Addendum (2026-03-16): Phase H Completion and GUI Performance Benchmark Harness

Implemented:
- `main_window.py`
  - persistent station header with selected-radio chips and station summary
  - shared `SelectedRadioContext` for device-scoped operating surfaces
- `station_overview_tab.py`
  - operator-facing `Station Default` terminology
  - `Selected Radio` badge and direct per-card select action
- `controlfreq_tab.py`
  - selected-radio header and device-scoped status treatment
- `freq_planner_tab.py`
  - selected-radio context selector and multi-view header model
- `message_viewer_tab.py`
  - radio/source filters on the unified inbox
  - selected-radio-aware scope summary
  - optional `reportlab` dependency handling so the Messages tab still loads when PDF export support is absent
- `js8call_net_control_tab.py`
  - `Run on:` selector tied to selected-radio context
  - active-session radio pinning
- `fldigi_net_control_tab.py`
  - `Run on:` selector tied to selected-radio context
  - active-session radio pinning
- `local_ncs_tab.py`
  - `Run on:` selector tied to selected-radio context
  - active-session radio pinning
- `daily_schedule_tab.py`
  - schedule-level default target editor
  - row-level `Inherited` targeting
  - persisted default-target state aligned with scheduler target resolution
- `net_schedule_tab.py`
  - schedule-level default target editor
  - row-level `Inherited` targeting
  - persisted default-target state aligned with scheduler target resolution

Benchmark harness:
- Added GUI benchmark-style tests for:
  - selected-radio context sync cost
  - unified Messages filter refresh/apply cost
  - Net Schedule target-widget refresh cost
- The benchmark tests are intentionally regression-oriented rather than micro-optimized:
  - they run in normal `pytest`
  - they assert broad upper bounds suitable for CI and local operator workstations
  - they target responsiveness regressions rather than machine-specific absolute speed contests

Benchmark-driven hardening:
- Avoided reintroducing blocking imports in `Messages` by making PDF export support optional.
- Fixed schedule-target widget refresh so changing the schedule default does not get reset by target-catalog refresh.
- Preserved targeted widget refresh patterns instead of returning to full-tab rebuilds for these new UX paths.

Verification focus:
- Functional UI tests for:
  - station overview selected-radio state
  - message-viewer radio/source filtering
  - local NCS radio pinning
  - schedule default-target persistence and inherited rows
- GUI performance regression tests for:
  - selected-radio context sync
  - Messages filtering
  - schedule-target widget refresh

Residual boundary:
- `FreqPlanner` still uses one table view with labeled selected/all/compare modes rather than a fully parallel compare-lane renderer.
- NCS schedule-derived auto-selection remains conservative: the `Run on:` model is explicit and pinned, but not yet a full schedule-to-session auto-routing engine.
- The benchmark harness is CI-safe and useful for regressions, but it is not a replacement for occasional manual profiling on real operator stations.

Rollback:
- Revert the Phase H UI/context/schedule-editor changes together with the new benchmark tests if a different multi-radio operating model is chosen.
