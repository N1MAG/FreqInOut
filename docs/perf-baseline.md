# Performance Baseline Workflow

Use this baseline on both Windows and Linux with production-like staged data.

## 1. Preconditions

- Enable perf spans (default enabled):
  - setting: `perf_metrics_enabled = 1`
  - optional env override: `FREQINOUT_PERF_METRICS=1`
- Close other heavy applications.
- Use the same dataset and machine profile for each comparison run.

## 2. Reset Log

```powershell
python tools/perf_benchmark.py reset-log
```

## 3. Scenario Runs

### Cold run

1. Start FreqInOut.
2. Wait for initial UI idle.
3. Open tabs in order: `Messages`, `Map`, `Operators`, `ControlFreq`, `FreqPlanner`, `HF Daily`, `HF Nets`, `SOP Builder`, `Settings`, `Digi/SSB NCS`, `JS8 NCS`.
4. In `Messages`, click `View` on at least 20 mixed message types.
5. In `SOP Builder`, select `HF SOP`, edit at least 3 action rows, expand the conflict workbench once, and save one change that triggers SOP data fanout.
6. In `HF Daily`, if an HF SOP is active, toggle `Show Effective Schedule`, open `Resolve Conflicts`, and close it without applying changes.
7. In `HF Nets`, open `Manage Net/SOP Policies`, review the table, and close it.
8. Close app.

### Warm run

1. Start FreqInOut again (same dataset).
2. Switch between `Messages`, `Map`, `Operators`, `ControlFreq`, `FreqPlanner`, `HF Daily`, `HF Nets`, `SOP Builder`, `Settings`, `Digi/SSB NCS`, `JS8 NCS` for at least 30 switches.
3. Repeat 20 `View` actions in `Messages`.
4. Repeat one `SOP Builder -> Save` path that refreshes Daily/Net/FreqPlanner data.
5. Close app.

## 4. Summarize Perf Spans

All spans:

```powershell
python tools/perf_benchmark.py summarize --sort p95 --limit 80
```

Focused summaries:

```powershell
python tools/perf_benchmark.py summarize --name "^(main_window|messages|map|operators|controlfreq|freqplanner|daily_schedule|net_schedule|sop\\.|settings|digi_ncs|js8_ncs)" --sort p95 --limit 80
```

Write markdown table:

```powershell
python tools/perf_benchmark.py summarize --name "^(main_window|messages|map|operators|controlfreq|freqplanner|daily_schedule|net_schedule|sop\\.|settings|digi_ncs|js8_ncs)" --markdown docs/perf-baseline-latest.md
```

## 5. Record Results

Capture these in PR/change notes:

- `p50/p95/p99` for:
  - `main_window.set_screen`
  - `main_window.ensure_lazy_tab_loaded`
  - `messages.on_tab_activated`
  - `messages.view_message`
  - `map.render_call`
  - `operators.on_tab_activated`
  - `controlfreq.on_tab_activated`
  - `freqplanner.rebuild_table`
  - `daily_schedule.on_tab_activated`
  - `daily_schedule.active_conflict_state`
  - `net_schedule.on_tab_activated`
  - `net_schedule.scan_net_sop_conflicts`
  - `net_schedule.refresh_conflict_highlighting`
  - `sop.on_tab_activated`
  - `sop.realtime_conflict_check`
  - `digi_ncs.on_tab_activated`
  - `js8_ncs.on_tab_activated`
- hardware, OS, dataset date/size
- before/after delta and regression risk notes
