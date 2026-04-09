# Changelog

## [1.2.2]
- Fixed: `ControlFreq` `Next Change` now shows the upcoming target frequency instead of repeating the currently active scheduled frequency.
- Fixed: `ControlFreq` frequency hero now re-syncs to the actual active radio frequency more reliably after automatic schedule-driven QSY and other runtime frequency changes, while still preserving an intentional pending user selection.
- Changed: When a schedule hold is already active, changing the hold-duration preset from `ControlFreq` or the main sidebar now immediately adjusts the active hold window instead of only changing the default for the next hold.
- Fixed: `HF Operator History` CSV import now accepts UTF-8-with-BOM files such as Excel `CSV UTF-8` exports.
- Fixed: `Messages` now treats `.k2s` fallback payloads like `.b2s` transport forms so received NBEMS transport files decode through the existing form-friendly renderer instead of generic unknown-form output.
- Fixed: `Map` station markers now honor the selected group/region filters in station-display mode, and marker filtering now uses merged operator group membership from both `group1/2/3` and stored group lists.
- Fixed: `Map` tooltip activity now distinguishes overall `Last Seen` from direct inbound `Last Contact`; overall last-seen data is shared with `HF Operator History`, and outbound-only attempts no longer appear as contact.
- Changed: FLDigi log-based RX busy detection now uses a short shared status cache, short-lived resolved-log-path cache, bounded token/timestamp memoization, and targeted perf spans to reduce repeated CPU load during active readable-text receive bursts without changing busy-detection rules.
- Fixed: Reliability Baseline Phase 1 now persists `FLRig XMLRPC Port`, applies the saved FLRig endpoint at startup, and aligns logger/DB-tool paths with the active runtime profile root so diagnostics and DB admin actions operate on the same profile the app is using.
- Changed: Reliability Baseline Phase 2 moved background ingest work off the GUI thread into a serialized worker with duplicate-trigger suppression and worker-local settings objects, reducing startup/UI stall risk without changing ingest cadence.
- Fixed: Reliability Baseline Phase 3 centralizes `operator_checkins` schema ownership in core DB code, upgrades legacy layouts during startup, removes destructive schema rebuild ownership from `Operator History`, and aligns DB tool schema metadata with the unified runtime schema.
- Fixed: Reliability Baseline Phase 4 makes `JS8` and `FLRig` status badges endpoint-aware so the configured instance shows `ok`, while mismatched process-running / endpoint-unreachable cases show `warn` instead of false green status; Settings now probes FLRig status against the currently entered port value.
- Fixed: Reliability Baseline Phase 5 ensures VarAC local tables are created during cold-start DB initialization, eliminating fresh-profile startup errors when `Stations Map` reads `varac_callsign_stats` before the first VarAC ingest run.
- Fixed: Reliability Baseline Phase 6 tracks `SPEC.md` and the Phase 1-5 targeted regression tests in git, replaces the remaining deprecated `datetime.utcnow()` settings path with timezone-aware UTC, and adds FLDigi XML-RPC host/port persistence plus endpoint-aware FLDigi status validation in Settings.
- Fixed: Updater archive extraction now validates ZIP entry paths before extraction, rejecting unsafe absolute or traversal paths and storing downloads under the active runtime config root.
- Fixed: Startup single-instance lock acquisition now uses a single definitive lock attempt instead of redundant checks that could report an already-running state inconsistently.
- Fixed: Scheduler shutdown now stops and tears down the serialized control executor cleanly, best-effort cancels any in-flight control future, and ignores stale control callbacks during app exit to reduce shutdown hangs and orphaned worker-thread risk.
- Fixed: `SettingsManager` now enforces thread affinity at runtime so cross-thread reuse fails fast with a clear SQLite-style programming error instead of surfacing as intermittent thread-bound connection failures later.
- Changed: Main-window teardown now emits targeted debug logging when scheduler/background-ingest/JS8/widget shutdown steps fail, and adds focused regression coverage for scheduler-stop cleanup and settings thread-affinity guardrails.
- Changed: WebEngine startup prewarm now defaults to enabled on Windows and disabled on macOS/Linux unless `map_webengine_startup_prewarm` explicitly overrides the platform default.
- Changed: App/documentation/installer version references updated to `1.2.2`.

## [1.2.1]
- Added: Temporary schedule hold controls now use shared `30` / `60` / `90` / `120` minute presets across sidebar `Schedule Status`, `ControlFreq`, `HF Daily`, `FLDigi / SSB`, `JS8Call`, and prompt-based pause actions, with live countdowns and synchronized near-real-time updates across tabs.
- Fixed: Changing the hold duration in one QSY/Suspend surface now updates the other hold selectors immediately in-process instead of waiting for a delayed settings reload.
- Added: `Messages` now includes a persistent `Hide Types` filter so multiple message types can be hidden from the default view while explicit `MSG Type...` selections still show that type for the current view.
- Changed: `Clear Filters` in `Messages` now resets only the temporary filter row/search controls and intentionally leaves the persistent hidden-type list in place.
- Changed: App/documentation version references updated to `1.2.1`.

## [1.2.0]
- Fixed: Bundled `Net Resources` SitRep seasonal sets were corrected from verified source data; shipped `Winter` and `Summer` resource files now contain the updated schedules, and `Fall` stays aligned with corrected `Winter` as the current fallback set.
- Changed: Existing installs now perform a one-time builtin `Net Resources` refresh during Net Schedule bootstrap so corrected bundled `Winter`/`Summer` rows replace stale builtin rows while preserving user-added/manual resource entries.
- Added: `Settings -> JS8Call Settings` now includes configurable `TCP Host` (`js8_host`) with default `127.0.0.1`, and JS8 status/control/net/map integrations now use the configured hostname/IP instead of assuming localhost.
- Changed: `Settings -> JS8Call Settings -> Load JS8 Traffic` now shows an in-process indicator (button busy state + progress/status text) during manual JS8 traffic rebuilds so long reloads provide immediate UI feedback.
- Fixed: On Windows, launching with `python -m freqinout.main` now sets a FreqInOut-specific app identity/icon early so the taskbar button shows the FreqInOut icon instead of the default Python icon.
- Changed: `HF Schedule` now re-sorts the Active Schedule table immediately after a successful save (using the existing time sort) so newly added rows do not remain visually out of order until reload.
- Fixed: `HF Schedule` save now blocks on row time-format errors (`HH:MM`) instead of performing a partial save that silently skips malformed rows.
- Fixed: `ControlFreq` top-row `Operating Status` LED container now expands correctly to fit status indicators/labels without avoidable clipping from the outer layout spacer.
- Fixed: Clicking `QSY Now` from `ControlFreq` `Schedule Outlook` now forces a hero-frequency resync so the Frequency Control hero indicator reflects the active frequency after QSY.
- Fixed: `ControlFreq` Frequency Control hero indicator now re-syncs when the scheduler changes to a new scheduled frequency, preventing stale hero frequency display after automatic schedule-driven QSY changes.
- Fixed: `ControlFreq` top-row clock/time display now stays contained on narrower window widths by allowing shrink/eliding instead of forcing a large minimum width that could overflow the right edge.
- Added: `SOP Builder` now includes a `Versions` menu (`Save Version`, `Load Version`, `Delete Version`) so operators can keep named HF/Local SOP snapshots and load them back into the builder as drafts.
- Added: `SOP Builder` now provides a guided conflict workflow with collapsible `Activation Defaults` and `Conflict Workbench`, workbench filters, and clearer next-step/readiness guidance.
- Changed: `SOP Builder` `Suggested Start` behavior now auto-computes timing suggestions for timing conflicts and explicitly shows `Not needed` for rows that do not require a timing adjustment.
- Fixed: SOP action-row `Condition Levels` multi-select interaction and persistence were stabilized so saved condition-level edits reliably reload and render across tabs.
- Changed: Main-tab condition-level edits now fan out through a lightweight debounced refresh path so SOP/FreqPlanner/ControlFreq updates are faster and automation-ready.
- Changed: VarAC BBS archive behavior is now explicit and consistent: manual `Archive` moves files from `BBS Directory` to `BBS Archive`, and auto-archive runs on first Messages activation after startup and then periodically (daily).
- Fixed: Linux Launch Control can now start VarAC under Wine using a new optional `VarAC Launch Command` setting; when omitted, path-derived `VarAC.exe` launches are Wine-wrapped automatically when Wine is available.
- Changed: Launch Control readiness is now dependency-aware: `JS8Call` must be API-reachable (not only process-running) before it is marked ready, and launch pacing adds targeted settle delays after `VarAC` and before `JS8Spotter`/`CommStat` when they depend on `JS8Call`.
- Fixed: VarAC custom launch-command mode now hardens launch context by normalizing user/env path tokens and preferring `VarAC Install Folder` as working directory, reducing Linux profile/config drift between desktop-launch and Launch Control.
- Changed: `VarAC Launch Command` UI wording now marks it as an advanced override and recommends leaving it blank unless default auto-launch fails.
- Changed: In-app guide wording now clarifies `Resume Schedule`, FLDigi offset expectations (including Operating Group fallback), and `Prompt` vs `On Schedule Change` behavior.
- Changed: `docs/guide.html` now provides a fuller step-by-step SOP workflow covering `SOP Builder`, `HF Daily` conflict handling, `HF Nets` policy review, `Show Effective Schedule`, and `Return to Normal`.
- Changed: Release perf guidance now explicitly covers the `FreqPlanner`, `HF Daily`, `HF Nets`, and `SOP Builder` paths, and the perf benchmark helper now defaults to dedicated `perf_metrics.log` files instead of mixing in normal app logs.
- Changed: `release_builder.py` now runs `python -m compileall freqinout` by default so the release helper matches the required verification baseline.
- Changed: App/documentation version references updated to `1.2.0`.

## [1.1.9]
- Changed: ControlFreq `Frequency Control` now keeps the schedule-state badge focused on `On Schedule` / `Off Schedule` / `Unknown`, and shows traffic/PTT gating on the QSY action button as `Busy: {reason}` (`PTT active`, `JS8Call`, `VarAC`, `FLDigi`) while the action is temporarily disabled.
- Fixed: Scheduler FLDigi off-schedule detection now distinguishes `FLDigi Mode` vs `FLDigi Offset` (offset drift no longer masquerades as mode mismatch), preserving off-schedule notification while using the existing FLDigi enforcement mode (`On Schedule Change` / `Prompt`) for resolution handling.
- Fixed: FLDigi `Prompt` enforcement no longer immediately re-applies offset drift due to mismatched FLDigi prompt-gating entry-key comparisons; offset-only drift now notifies first as expected.
- Fixed: FLDigi offset drift is no longer re-queued for immediate enforcement by same-entry resume/retry/reapply paths; offset drift can remain off-schedule until prompt/apply or an actual schedule entry change.
- Fixed: FLDigi `On Schedule Change` enforcement now keys off real scheduler row transitions (not internal reapply-key differences), and `Prompt` mode treats changed FLDigi offset drift values as a new prompt cycle.
- Changed: `Resume Schedule` responsiveness improved in ControlFreq and sidebar `Schedule Status` by reducing duplicate resume refresh pulses and repeated scheduler/FLDigi status polling in hot UI refresh paths.

## [1.1.8]
- Added: Accessibility `Text Size` setting in `Settings` with bounded presets (`Normal` 100%, `Medium` 110%, `Large` 125%) applied app-wide without restart.
- Changed: UI text-size presets now persist across restarts via `ui_text_size` and are applied centrally through app-theme refresh.
- Changed: ControlFreq frequency hero display remains fixed-size across text-size presets so dashboard frequency readability stays stable.
- Changed: Sidebar navigation and Schedule Status action controls now auto-size from text metrics to reduce clipping risk at larger UI text sizes.
- Changed: Settings section navigator width now auto-sizes to section labels, improving readability at larger UI text sizes.
- Changed: Settings forms now apply width guards for fixed-width labels/buttons/combos so `Large` text size avoids clipped control text.
- Changed: Messages tab header/filter controls now auto-fit minimum widths from font metrics to improve `Large` readability.
- Fixed: Messages filter combo popups (`MSG Type`, `Status`, `From`, `To`) now expand to fit filter values, and `From/To` dropdown arrow affordance is fully visible.
- Changed: SOP header/action controls now auto-fit minimum widths, and SOP PDF export filter controls now use expandable minimum widths for `Large` text size.
- Changed: SOP Action Rows `Resource` popup width is now capped for a more comfortable dropdown reading width.
- Added: HF Schedule now includes an `SOP Runtime` panel with `Now/Next` source summary, active SOP count, SOP category visibility, and one-click `Activate`/`Deactivate` controls.
- Added: SOP manager now supports lightweight profile summaries with inferred SOP category (`HF`, `Local Net`, `HF + Local Net`) and direct active-state toggles for low-latency UI controls.
- Changed: SOP terminology is now clearer in UI/docs (`Group Name (HF)`, `SOP Group`, `SOP Category`) to reduce user interpretation friction.
- Changed: HF Schedule SOP category labels now render as `SOP-HF`, `SOP-Local Net`, or `SOP-Mixed` to make clear that toggle actions apply to SOP sets only (not the baseline HF schedule).
- Added: HF Schedule now includes a `Schedule Issues` panel with severity tiers (`Conflict`, `Needs Review`, `Info`), guided action buttons, and `Dismiss Until Change` behavior.
- Added: HF Schedule now includes a consolidated `Schedule Resources` workspace showing NET/SOP/HF rows with source/category/text filters and explicit priority ordering context.
- Changed: HF Schedule `Schedule Resources` now excludes NET source rows and supports Active-Schedule parity workflows (`Move Selected to Resources`, `Add Selected/Filtered to Active Schedule`) backed by persisted `hf_schedule_resources`.
- Changed: HF Schedule now uses explicit section labels (`Active Schedule`, `Schedule Resources`) in the same visual style as Net Schedule section headers.
- Fixed: Settings section navigator now shows a vertical scrollbar as needed so lower sections, including `Launch Control`, remain reachable without window resize workarounds.
- Changed: ControlFreq now uses bounded SOP window caches (`Today`/`Tomorrow`) with TTL and explicit cache-bust on SOP/settings changes for long-run responsiveness without restart.
- Changed: ControlFreq settings-save handling is now lightweight when tab is inactive (deferred heavy refresh until activation).
- Changed: SOP tab action-source labels now use `Resource` (replacing `Software`) in Action Rows and Upcoming Actions tables.
- Changed: SOP PDF export now renders a blended single-day action checklist (Time, Resource, Action, Band/Freq, Contact, Description) without Status, and includes complete same-day action occurrences.
- Added: SOP PDF export now includes a separate `Periodic Actions` section (Week(s) of Month, Day of Week, Resource, Action, Band/Freq, Contact, Description) when periodic schedule-layer rows exist.
- Fixed: ControlFreq `Tomorrow` outlook now expands SOP reminder occurrences across the full tomorrow window (not only each action's immediate next due), restoring complete SOP visibility alongside net rows.
- Changed: ControlFreq `Schedule Outlook` second section now shows `Tomorrow` only (not a rolling 7-day window) and uses strict mutually-exclusive day boundaries with `Today`.
- Added: SOP legacy Local Net interval migration now auto-converts common fractional-hour cadence rows (`:15/:30/:45`) into interval + phase (`interval_phase_minutes`) so existing stagger intent is preserved.
- Fixed: SOP save/delete/import/complete now emits a data-change signal that invalidates SOP status caches, forces scheduler refresh, and prompts immediate ControlFreq SOP outlook update.
- Changed: ControlFreq SOP outlook performance improved by reusing a tab-scoped `SOPManager` and using window-aligned SOP prefetches (`Today` from now, `Tomorrow` from tomorrow-start) instead of rebuilding manager/query path per section.
- Added: SOP Interval now supports optional phase offset syntax (`HH:MM@MMm`, for example `03:00@30m`) for staggered reminders; due-time evaluation, JSON import/export, and DB persistence now carry per-action `interval_phase_minutes`.
- Fixed: ControlFreq SOP `Schedule Outlook` now excludes disabled SOP action rows; only active-profile + enabled-action reminders are shown.
- Fixed: ControlFreq `Schedule Outlook` now buckets “Today” using local-day boundaries when `Showing: Local`, preventing tonight-local items from being pushed only into `7 Days`.
- Added: SOP `Local Net` action catalog now includes `NCS`, `Check-in`, and `Message` options (with legacy local action-key compatibility for existing SOP rows).
- Changed: Settings `Local Net Profiles` add/edit/delete now emits a lightweight `local_net_profiles_changed` path to refresh SOP targets without full app-wide settings refresh fanout.
- Fixed: Local Net Profile add/edit/delete now persists `local_net_profiles` directly, so SOP Local Net action targets are available even when unrelated Settings validation would block a full save.
- Fixed: SOP `Local Net` actions remain fully supported for reminders; Layer Sync messaging now explicitly states it applies only to HF/Net schedule actions.
- Changed: SOP `Populate/Rebuild Layer` controls are now disabled when no eligible non-local action rows exist, preventing false impression that local reminders were removed.
- Added: SOP Builder now shows a `Layer Sync` hint (In Sync / Out of Sync / No matching windows) comparing current SOP Layer rows to action-derived candidates.
- Changed: `Rebuild Layer Preview` now gets contextual warning emphasis when SOP Layer drift is detected.
- Changed: SOP layer-sync checks are now debounced and candidate-cached in UI paths to keep editing responsive while still updating guidance quickly.
- Added: Scheduler status now includes source-resolution rationale (`source_reason*`, `sop_selected_reason*`) and next-source transition preview metadata (`next_source*`) for UI transparency.
- Changed: ControlFreq and HF Schedule `Effective Source` / `Next Change` hints now include low-noise rationale and upcoming source-transition guidance when applicable.
- Added: SOP Builder now shows a runtime source hint line with contention context and next-source transition guidance.
- Added: SOP profiles now include a persisted `Priority` value (lower wins) used for runtime SOP-layer conflict arbitration.
- Changed: Scheduler SOP-layer overlap arbitration is now deterministic: `priority` -> profile `updated_utc` recency -> stable row tie-breakers.
- Added: Scheduler status now reports SOP contention metadata (selected profile and contenders) for UI warning surfaces.
- Changed: ControlFreq, HF Schedule, and sidebar Schedule Status now surface explicit SOP contention warnings when multiple active SOP profiles overlap.
- Changed: ControlFreq `Message Summary` row height now uses the shared default table row height for visual consistency with `Schedule Outlook`.
- Added: SOP Schedule Layer row-level pre-save warnings for invalid/missing time or frequency, missing `Periodic` weeks, and potential row overlaps.
- Added: SOP Builder `Rebuild Layer Preview` action with add/remove/unchanged diff summary before applying action-derived layer updates.
- Changed: `Populate Layer from Actions` now uses the same diff-style preview flow so users can safely append missing rows or rebuild with clear impact.
- Added: ControlFreq Frequency Control now shows `Effective Source` so operators can see when `SOP Layer` is overriding baseline HF schedule.
- Added: HF Schedule header now shows `Effective Source` with the same SOP/Net/HF precedence visibility.
- Changed: SOP Schedule Layer defaults new/bootstrap rows to `Daily` recurrence for action-driven workflows.
- Changed: SOP Schedule Layer time columns now follow `Showing: Local/UTC` and convert display/edit values while preserving UTC storage.
- Changed: Removed `VFO` from SOP Schedule Layer UI (VFO resolution now follows Operating Group data/scheduler behavior).
- Changed: SOP Schedule Layer table now uses stretch sizing to better fill available width for easier review.
- Added: SOP Builder now includes `Populate Layer from Actions` for assisted schedule-layer bootstrap from existing SOP action rows.
- Added: Guided SOP layer bootstrap review flow with `Append`, `Replace Existing Layer`, and `Cancel`, including unmatched-action reporting.
- Added: SOP Builder now supports an optional per-profile `SOP Schedule Layer` table (day/recurrence/time/band/frequency/mode) to define schedule windows that override HF schedule when that SOP profile is active.
- Changed: Scheduler source precedence is now `NET > SOP Layer > HF`, with SOP layer rows evaluated only for active SOP profiles and enabled layer rows.
- Changed: SOP alignment checks now evaluate against Daily/Net/SOP Layer windows so reminder mismatch warnings reflect effective schedule behavior.
- Added: New `Local Operators` tab with roster CRUD/search and CSV import/export for local VHF/UHF/GMRS/MURS/FRS operator tracking, now using `first_name` and `last_name` fields in UI and DB.
- Changed: Local Operators table now includes visible `SitRep` status column and supports sortable column headers.
- Added: New `Local NCS` tab with `Start Net`/`Join Net`/`End Net` session controls, local-operator lookup, single check-in log table, SitRep status (`Green/Yellow/Red`), persistent notes, and periodic autosave.
- Fixed: Local NCS lookup/add keyboard flow now mirrors FLDigi NCS expectations (`Enter` on lookup/completion autofills selected operator, while `Enter/Space` on `Add Check-in` performs the add) with guarded exception handling for stability.
- Fixed: Local NCS lookup `Enter` path no longer triggers direct add/check-in writes from completion handlers, eliminating duplicate re-entrant handling that could terminate the app during lookup.
- Changed: Local NCS check-in table is now session-scoped and clears on `End Net` so each net run starts with an empty session list.
- Added: Shared soft CTA button styling (`eligible_*`) for low-noise, state-aware action highlighting across tabs.
- Changed: Contextual action highlighting pass across ControlFreq, FreqPlanner, SOP, Messages, NCS tabs, Operators tabs, Map toggles, and scheduler status panel to better surface next eligible actions.
- Fixed: ControlFreq and FreqPlanner eligibility highlights now stay in sync through selection/click, refresh/rebuild, and timer update paths.
- Changed: ControlFreq and FreqPlanner highlight contrast increased for clearer active-state visibility in both Light and Dark themes.
- Fixed: Shared button stylesheet generation now emits separate `QPushButton` and `QToolButton` rules, restoring runtime action-highlight rendering where combined selectors failed to apply.
- Changed: Active-net reminder moved to sidebar NCS menu buttons (`FLDigi/SSB NCS`, `JS8 NCS`, `Local NCS`), while in-tab start actions remain muted when a net is active.
- Changed: Sidebar NCS labels/order updated to `NCS-FLDigi/SSB`, `NCS-JS8`, `NCS-Local`, with `NCS-Local` positioned directly under `NCS-JS8`.
- Changed: Main sidebar tab-button labels now use left-aligned text for faster scanability across mixed-length tab names.
- Fixed: Main sidebar tab-button typography/style is now normalized across all tabs for matching active/inactive states, with NCS warning highlight only when a net is active.
- Fixed: Map first-open stability by constructing the embedded `QWebEngineView` with an explicit parent, reducing transient close/reopen-style window flash on initial Map tab load.
- Fixed: Additional Map first-click flash hardening by creating the `Map` tab at startup (hidden) and using early offscreen WebEngine warmup so first user activation no longer triggers widget-swap/first-surface startup paths.
- Changed: HF Schedule and Net Schedule now highlight `Save` only when unsaved edits exist (dirty-state tracking), while row-dependent actions (`Delete`, `Move`, resource actions) highlight only when eligible.
- Changed: Settings label `Operating Groups` is now `HF Operating Groups` for clearer separation from local-net workflows.
- Changed: Sidebar labels updated to `FLDigi/SSB NCS` and `HF Operators`, with `Local Operators` and `Local NCS` added to navigation.
- Added: Net Schedule tab now includes a new read-only `Net Resources` catalog with sortable headers, global search, manual set selector, and persistent last-set selection.
- Added: Built-in seasonal net resource files are now shipped in `config/net_resources` (`sitrepnets-fall.json`, `sitrepnets-summer.json`) with in-app citation text: `Visit SitRep.net for more info.`.
- Added: Net Resources actions to `Add Selected to Net Schedule`, `Add Filtered to Net Schedule`, and `Move Selected to Resources` for active-schedule curation workflows.
- Changed: `Import Net Schedule` now imports JSON rows into `Net Resources` by default (resource catalog flow) instead of replacing active schedule rows.
- Added: One-time migration that backfills existing active Net Schedule rows into `Net Resources` (`Migrated` source) without removing active schedule entries.
- Fixed: Duplicate promotion guard now blocks net-resource adds when an active row already matches `(day + start/end + band + frequency + mode)` and shows conflict details for resolution.
- Added: Active Net Schedule rows now persist `resource_id` linkage so edited rows moved back to resources update the corresponding resource entry.
- Added: VarAC background data foundation in local `freqinout_nets.db` with mirrored event/lookup tables for `vmail_folder`, `vmail_relay_notification`, `broadcast` (including `via_callsign`), `cqframe`/`cqframe_type`, and `qso_snr_report`.
- Added: VarAC ingest run telemetry (`varac_sync_status`, `varac_sync_table_counts`) with per-run success/failure, scanned/written row counts, and per-table watermark visibility for diagnostics.
- Added: VarAC callsign trait tracking (`varac_callsign_traits`) for downstream UI/use-cases such as EmComm/BBS/alert context.
- Added: Operators tab unified SitRep projection support (`sitrep_latest_by_callsign`) with effective status display, source summary chips, recency age, and conflict indicators when sources disagree.
- Added: Map tab unified SitRep projection support (`sitrep_latest_by_callsign`) with source chips/conflict metadata in station detail popups for parity with Operators.
- Added: Messages tab fast display-level SitRep dedupe that suppresses raw Spotter duplicates when equivalent normalized SitRep rows are present (`sitrep_messages_dedupe_enabled`, with optional raw override via `sitrep_messages_show_raw_duplicates`).
- Added: One-time checkpointed local backfill from `spotter_traffic` into unified SitRep staging so pre-existing Spotter forms (`F!104/301/304`) are represented under normalized `SitRep` views.
- Fixed: Peer Schedules overlap computation now evaluates day-aware weekly windows (including overnight ranges) instead of only current-day/current-time clipping, improving overlap accuracy.
- Changed: Peer Schedules overlap column now prioritizes actionable reachability windows (`NOW`, `Today`, or next `Day HH:MM-HH:MM`) for faster operator decisions.
- Added: Background inferred peer schedules from recurring observed traffic (`js8_links`/`varac_links`) with confidence metadata and effective-source precedence so imported schedules override inferred rows per callsign.
- Changed: Peer schedule inference scoring now normalizes recurrence by each callsign's observed active weeks (instead of a fixed global window) so valid recurring peers populate in sparse/partial datasets.
- Changed: Peer schedule inference now canonicalizes portable/mobile callsign suffix variants (for example `/P`, `/M`, `/Z`) into base callsigns for grouping, while leaving raw link records intact for provenance.
- Fixed: Inferred schedule identity split for suffix variants (for example `K0RPG` and `K0RPG/Z`) by merging recurrence scoring to a single base callsign.
- Fixed: ControlFreq `Schedule Intersections` overlap detection now evaluates next-2-hour weekly segments (including overnight/day-wrap) and reads schedule rows from both settings/nets DB locations, restoring peer overlap population.
- Fixed: Map `Peer Sched Now` now computes active schedule-frequency matches using both local schedule sources plus current scheduler frequency, with normalized day/overnight evaluation and practical same-frequency tolerance; matched peers are kept visible on the map even without fresh link traffic.
- Changed: Map `Peer Sched Now` legend is now a compact color-key (Green `NOW`, Blue `Later Today`, Purple `QSY <10m`) with matching marker-ring states, replacing the wide descriptive text block.
- Changed: VarAC ingest pipeline now includes concurrency protection and short cadence throttling to avoid overlapping ingest runs and reduce background churn.
- Changed: VarAC message mirror schema now stores richer metadata (`folder_label`, `urgent`, `has_attachment`, `via_callsign`) to support true folder semantics and badge-ready UI paths.
- Changed: Operators manual SitRep override behavior now coexists with unified status projection and remains active until a newer fused report supersedes it.
- Changed: Map SitRep status resolution now preserves manual overrides from `spotter_station_status` until newer unified fused status is available.
- Changed: DB admin/schema tooling now includes the expanded VarAC mirror/sync tables so status/init commands report them consistently.
- Fixed: Long-run memory stability risks by bounding previously unbounded runtime caches (propagation empirical blend cache and JS8 form-definition caches).
- Fixed: Map HTML reload path now reuses a managed cache file with cleanup on shutdown (instead of creating unbounded temporary HTML files over time).
- Fixed: Added/ensured `js8_links(ts)` indexing for map recency query paths to reduce progressive slowdown as traffic history grows.
- Changed: Operator auto-ingest trust policy hardened so traffic-discovered operators default to `Untrusted` unless explicitly promoted (manual edit or trusted import workflow).
- Changed: Operator auto-upsert paths now normalize callsigns to base form before DB writes to prevent suffix variants creating separate operator identities.
- Changed: ControlFreq `Frequency Control` now uses a primary digital-style `Now` frequency readout with compact schedule-state badge (`On Schedule`/`Off Schedule`/`Blocked`) and condensed scheduled-vs-active metadata.
- Changed: ControlFreq quick-frequency selector labels are now compact frequency-first entries to reduce visual clutter and speed QSY scanning.

## [1.1.7]
- Added: Expanded benchmark/perf instrumentation for `controlfreq`, `digi_ncs`, and `js8_ncs` activation flows.
- Changed: Performance pipeline updates across `Messages`, `Map`, `Operators`, and `ControlFreq` to reduce first-open and warm-switch latency.
- Changed: ControlFreq activation path now defers heavy refresh phases and keeps status probing off the tab-switch hot path.
- Changed: Map first-load/reload rendering now avoids duplicate work and reduces payload/bootstrap overhead.
- Fixed: Map links no longer disappear after layer/theme changes when payload data is unchanged.
- Fixed: Map grid-layer render regression (`grid_color` unbound local) during map visibility refresh.
- Fixed: FLDigi check-in history writes now use unified `operator_checkins` schema fields (`first_seen_utc`/`last_seen_utc`) instead of legacy `date_added`.
- Fixed: JS8 `DIRECTED.TXT`/`ALL.TXT` incremental read offset handling for large production logs (`telling position disabled by next() call`).
- Fixed: ControlFreq monthly/periodic schedule evaluation now honors day-of-month week semantics (for example first-Sunday nets).
- Fixed: Operators tab `Clear Filters` affordance now highlights when filters are active.
- Changed: App/documentation version references updated to 1.1.7.

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
