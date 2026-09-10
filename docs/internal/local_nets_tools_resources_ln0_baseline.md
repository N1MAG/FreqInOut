# Local Nets / Tools & Resources LN-0 Baseline

Status: audit baseline; no production schema, configuration, or UI feature
changes are included. Date: 2026-09-09.

## Scope

This baseline records the current repository ownership and executable test
characterization required before LN-1. Structural migration cases are in
[`tests/fixtures/local_nets_tools_resources_baseline.json`](../../tests/fixtures/local_nets_tools_resources_baseline.json).
The fixture uses synthetic Group Alpha/Bravo/Charlie/Delta and net names and
contains no callsigns.

## Current call-site and ownership map

| Surface | Current owner / evidence | Current behavior |
| --- | --- | --- |
| Legacy `net_resources` schema | `freqinout/core/db_initializer.py:1600-1648`; duplicate assurance in `freqinout/gui/net_schedule_tab.py:3890-4050` | Combined resource/net/session row; startup owner and UI owner both assure schema today. |
| Legacy resource bootstrap and bundled sync | `freqinout/gui/net_schedule_tab.py:4655-4808` | UI bootstraps, deduplicates, migrates settings rows, and syncs bundled JSON. |
| HF Nets resource reads/writes | `freqinout/gui/net_schedule_tab.py:4168-4267`, `4482-4654`, `5559-5622`, `5848-5943`, `6053-6125` | Directly reads, upserts, updates, deletes, imports, and links `resource_id`; this is the main legacy writer. |
| FreqPlanner resource reader/writer | `freqinout/gui/freq_planner_tab.py:3423-3460`, `4234-4255`, `4300-4428` | Reads the same table and can update a linked master row during plan editing; second direct writer. |
| Known Operating Groups | `freqinout/core/known_operating_groups.py:122-127`, `259-319`, `419-433` | Reads `net_resources` first, then bundled JSON fallback; group identity is normalized display name, not a stable key. |
| HF schedule projection | `freqinout/core/schedule_projection.py:368-387`; `freqinout/core/operational_projection.py:336-390` | Carries legacy resource IDs/source identity into projection refs and deduplicates resource rows against HF Nets. |
| Daily Schedule resource library | `freqinout/gui/daily_schedule_tab.py:2851-3167`, `4794-4852` | Uses separate `hf_schedule_resources`; its integer IDs overlap `net_resources` namespace and are not globally stable. |
| Named HF source schedules | `freqinout/core/schedule_source_sets.py:599-815`; `freqinout/gui/daily_schedule_tab.py` and `net_schedule_tab.py` save/rename/delete seams | Settings legacy lists plus `MultiRadioStore` frequency plans (`hf_daily_schedule` / `hf_net_schedule`); duplicate-name IDs receive timestamp suffixes. |
| Scheduler boundary | `freqinout/core/scheduler_engine.py:4746-5435`, `5437-5495` | Reads daily/net commandable tables, SOP layer/policies, and settings fallback only; never queries `net_resources` or any Local Net table. |
| SOP/group references | `freqinout/core/sop_manager.py:400-780`, `819-850`, `1192-1325`; `freqinout/gui/sop_tab.py` | SOP profile/action/layer rows use `operating_group`/`group_name`; legacy `local_net_profiles` is settings-shaped metadata, not a recurring schedule or stable resource reference. |
| Ops Center | `freqinout/gui/controlfreq_tab.py:1233-1274`, `7051-7242`, `7392-7466` | Existing Schedule Outlook is HF/SOP-oriented; current SOP handoff carries labels only, not typed draft/identity context. |
| Startup/lifecycle | `freqinout/core/db_initializer.py`; `tests/test_startup_shell_deferred_screens.py`, `tests/test_gui_slice0_soak.py`, `tests/test_scheduler_shutdown.py` | Deferred screen construction and bounded scheduler/soak shutdown are characterized; no Local Nets worker exists. |
| Geometry | `docs/internal/ln0_ui_geometry_contract.md`; existing phase7/responsive/font tests | Wireframes and target matrix exist. Current app minimum height 600 blocks literal 900x560 validation (`main_window.py:905-907`). |

Bundled compatibility inputs are `config/net_resources/sitrepnets-fall.json`,
`sitrepnets-summer.json`, and `sitrepnets-winter.json` (31 rows each). They are
legacy seasonal HF resources, not the proposed cited Amateur/GMRS reference
manifest; `sitrepnets-fall.json` currently carries a `Winter` payload label and
should be reviewed during LN-1 classification.

## Structural fixture coverage

The baseline fixture covers empty installation, bundled-like read-only rows,
station-created rows, imported read-only rows, normalized duplicate variants,
malformed rows, linked/unlinked HF schedule rows, and two named HF Net source
schedules. The fixture shape is asserted by
`test_ln0_structural_fixture_covers_migration_and_schedule_relationships`.

## Focused executable evidence

Commands below were run from `FreqInOut-multi-rig` with the repository `.venv`:

| Command | Result | Runtime |
| --- | ---: | ---: |
| `.venv/bin/pytest -q tests/test_ln0_contract_baseline.py` | 2 passed | 0.19 s |
| `.venv/bin/pytest -q tests/test_multi_rig_wave3_slice4.py` | 12 passed | 0.66 s |
| `.venv/bin/pytest -q tests/test_operational_projection.py` | 9 passed | 0.06 s |
| `.venv/bin/pytest -q tests/test_scheduler_runtime_command_routing.py tests/test_scheduler_shutdown.py tests/test_scheduler_executor_thread_leak_1_2_7.py` | 26 passed, 1 skipped | 0.29 s |
| `.venv/bin/pytest -q tests/test_startup_shell_deferred_screens.py tests/test_gui_slice0_soak.py` | 11 passed | 0.42 s |
| `.venv/bin/pytest -q tests/test_plan_builder_slice5_responsive.py tests/test_slice5_responsive_builders.py tests/test_ui_font_rendering_consistency_1_2_3.py tests/test_ui_responsiveness_contract.py` | 45 passed | 4.18 s |
| `.venv/bin/pytest -q tests/test_phase7_main_shell_ux.py -k 'phase7_hf_nets_uses_compact_default or phase7_hf_nets_schedule_name_is_inline_editable or phase7_hf_nets_resources_use_age or phase7_hf_nets_view_edit_toggles or phase7_hf_nets_action_rows_reflow or phase7_hf_daily_uses_compact_default or phase7_hf_daily_schedule_name_is_inline_editable or phase7_hf_daily_action_rows_reflow or phase7_schedule_time_conversion_handles_local_day_boundaries or phase7_controlfreq_has_responsive_card_layout_breakpoint or phase7_controlfreq_restores_wide_splitter_sizes_after_compact or phase7_controlfreq_compact_mode_scrolls_without_clipping_frequency_card or phase7_settings_sections_use_bounded_fit_content_layouts'` | 13 passed, 114 deselected | 0.62 s |
| `.venv/bin/pytest -q tests/test_freqplanner_blended_projection.py -k 'named_source or source_schedule or hf_daily_tab_saves_named or hf_net_tab_saves_named or rename_existing_schedule or new_schedule_action or delete_source_schedule or save_selected_as_resources'` | 12 passed, 81 deselected | 0.51 s |
| Combined LN-0 core/lifecycle characterization command | 60 passed, 1 skipped | 0.87 s |

The repository’s retained macOS soak telemetry provides adjacent lifecycle
baselines (four runs under `.benchmarks/gui-soak/20260906-*/perf_metrics.log`):
HF Nets construction was 33.3–34.8 ms, Ops Center `refresh_all` was
18.1–21.9 ms, and Ops Center construction was 55.5–60.1 ms. Startup manager
construction was 3.9–4.7 ms. Shutdown completion was 7.0–2,010.2 ms across
the runs (the 2,010.2 ms outlier is below the 3,000 ms lifecycle budget but
requires follow-up). No existing telemetry isolates resource-filter query,
Plan reprojection, or idle cost; those are explicit LN-1/LN-3 baseline gaps,
not passing claims.

The SOP compact acceptance matrix has an environment-specific hang on this
macOS Qt run: its `invalid-light-1.0-size1` case passes, then the process stops
emitting progress. Individual non-matrix SOP tests pass; the matrix was not
counted as a gate pass. A two-file SOP invocation was terminated after the same
stall. This is a test/lifecycle blocker, not a Local Nets behavior failure.

The added test `test_scheduler_load_schedules_ignores_local_net_settings_and_has_no_local_reader`
exercises the current settings fallback with a synthetic `local_net_schedules`
row and verifies only HF/net rows are returned. It does not create a schema or
invoke hardware/QSY.

## LN-0 findings carried into implementation

- No canonical resource catalog, migration classifier, backup/rollback,
  version-diff, usage, import/export, recurrence, Local Net store, occurrence
  state, or Local Net projection exists yet; these are correctly deferred to
  LN-1/LN-4/LN-5.
- No runtime test can yet prove active/next/later Local Net Outlook behavior,
  dismiss semantics, or SOP handoff because those surfaces do not exist.
- No stable Operating Group key exists in feature code yet. The key generation,
  rename preservation, and snapshot behavior are locked in
  `local_nets_resources_ln0_architecture_decisions.md` for LN-1.
- Existing `net_schedule_tab.py` and `db_initializer.py` duplicate legacy
  schema assurance; FreqPlanner is an independent legacy writer. The shadow
  migration and LN-2 cutover sequence prevent either from silently diverging
  from an exposed canonical catalog.
- The 900x560 minimum-height correction, Plans/Resources navigation changes,
  typed navigation intent, and SOP context payload have locked dispositions in
  the architecture and UI contracts. Their implementation belongs to the first
  package that owns the affected surface; they are not waived.
- The macOS SOP matrix stall predates this package and remains a release-gate
  lifecycle investigation. It did not fail an LN-0 characterization assertion
  and no SOP runtime code changed here.

## LN-0 gate result

**Passed 2026-09-09.** Every known reader, writer, duplicate schema owner, and
scheduler boundary is mapped; the anonymized fixtures cover all required
shapes; responsive wireframes cover the required target sizes; identity,
compatibility, navigation, viewport, bundle, SOP payload, and cutover decisions
are locked; and the independently rerun focused gate produced 60 passed / 1
skipped plus 45 responsive, 13 geometry, and 12 HF source/projection passes.
No production schema, configuration, data, or feature navigation changed.
