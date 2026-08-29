# UI Text Size Accessibility Assessment

Status: planned after multi-rig compose recovery point `9d0e3d3`.

## Goal

FIO must stay readable and usable at `Settings > Main > Text Size = Large`
without requiring a tab-by-tab guessing exercise. The fix should be broad
enough to prevent routine clipping, but conservative enough that it does not
reshape every screen at once.

The desired result is simple: users with poor vision can choose Large text and
still operate the app without clipped controls, hidden buttons, or confusing
half-visible fields.

## Current Assessment

What already works:

- App font scaling is centralized in `freqinout/gui/theme.py` through
  `apply_app_theme(...)` and `resolve_ui_text_scale(...)`, and startup applies
  that scale from `freqinout/main.py`.
- Some important surfaces already use font metrics instead of fixed guesses.
  Examples include ControlFreq frequency controls, some Settings value labels,
  message filter widths, SOP controls, and map label scaling.
- Shared combo helpers exist in `theme.py`: `fit_combo_box_to_contents(...)`
  and `fit_child_combo_boxes(...)`.
- Settings, Messages, and SOP already contain local accessibility width guards.
  These are proof that the approach works, but they should not remain isolated
  per-screen patterns.
- Compose now uses short radio labels and scrollable setup panels in several
  places, which is the right model for constrained views.

Risk areas found in the source scan:

- Fixed-height or capped-height widgets are concentrated in:
  `settings_tab.py`, `message_viewer_tab.py`, and `controlfreq_tab.py`.
- The most suspicious values are small text-bearing caps such as 24, 26, 30,
  32, 36, and 40 pixels. These are risky at Large text because Qt grows the
  font but the row height may remain frozen.
- Compose still has several fixed/capped rows and chips. These should become
  font-metric sized rather than visually tuned for Normal text.
- ControlFreq intentionally uses dense radio cards. It already has some
  metric-aware code, so it should be treated carefully: preserve the card model
  and only fix controls whose text clips.
- Settings has the most fixed-height usage, but it also has the most existing
  accessibility guard behavior. It is a good donor for shared helper behavior.
- Some maps, tables, and canvases use intentional fixed dimensions. These should
  not be blindly resized by a global pass.

What should not change globally:

- Do not shrink or override Large text inside crowded controls.
- Do not force every panel to become taller if it is already scrollable and
  readable.
- Do not expand tables or map canvases merely because they have fixed heights.
- Do not convert every fixed-width control to unlimited width; that can make
  dense workspaces worse. Prefer elision, short labels, tooltips, and scrolling.

## Design Rule

Text-bearing widgets must size from the active font. A control may be compact,
but it must not be shorter than its text plus reasonable vertical padding at
Normal, Medium, or Large text size.

Use shared helpers for:

- button and tool-button minimum heights
- chip minimum heights
- combo-box and line-edit minimum heights
- one-line status/guidance labels
- fixed-width label/button guards
- scroll-area promotion when a panel cannot fit

Local pixel constants are acceptable only when they are derived from
`QFontMetrics` or when the widget is explicitly non-textual.

## Proposed Shared Helpers

Add these to `freqinout/gui/theme.py` or a small adjacent layout helper module:

- `control_height_for_font(widget, vertical_padding=10, floor=28) -> int`
- `button_height_for_font(widget, vertical_padding=12, floor=30) -> int`
- `single_line_label_height(widget, vertical_padding=6, floor=24) -> int`
- `apply_text_size_accessibility_guards(root, *, include_widths=True) -> None`
- `mark_text_size_guard_opt_out(widget) -> None`

The global guard should be conservative:

- Only inspect `QPushButton`, `QToolButton`, `QComboBox`, `QLineEdit`,
  `QPlainTextEdit`, `QTextEdit`, and simple `QLabel` controls.
- If a text-bearing widget has a maximum height below its font-derived minimum,
  raise that maximum height.
- If a widget has a fixed height below its font-derived minimum, raise both min
  and max height to the computed value.
- If a label is one-line and too narrow, do not blindly widen it beyond the
  layout. Prefer elision or tooltip handling in the owning screen.
- Skip widgets with an opt-out property such as `fio_text_size_guard_opt_out`.
- Skip table views, tree views, graphics views, web views, map canvases, image
  previews, progress bars, and deliberate non-text indicators.

## Step-by-Step Implementation Plan

### Step 1: Baseline Inventory

- Add a small developer script or test helper that lists fixed/capped
  text-bearing controls by file and line.
- Capture before/after screenshots at Normal and Large text for the high-use
  screens.
- Use the existing source scan as the first target list:
  `settings_tab.py`, `message_viewer_tab.py`, `controlfreq_tab.py`,
  `main_window.py`, `sop_tab.py`, and net-control tabs.

Acceptance:

- Inventory is repeatable and does not require manual `rg` interpretation.
- The highest-risk fixed-height controls are known before code changes begin.

### Step 2: Shared Metric Helpers

- Implement the font-metric sizing helpers in `theme.py`.
- Add unit/source tests proving the helpers exist and are called from the app
  theme path.
- Do not yet alter individual tabs except to import/use the shared helper where
  the app already has a natural theme-refresh hook.

Acceptance:

- Helpers compute heights from active widget font metrics.
- Helpers are opt-out capable.
- Existing tests pass without visible UI redesign.

### Step 3: App-Level Conservative Guard

- Call `apply_text_size_accessibility_guards(...)` after `apply_app_theme(...)`
  in the main window theme refresh path.
- Apply the guard to the main window root and open dialogs/workbenches after
  their layout is built.
- Limit the first pass to raising undersized maximum/fixed heights for common
  text controls.

Acceptance:

- Large text no longer clips common buttons, chips, combos, line edits, and
  one-line labels.
- Normal text screenshots remain visually close to current layout.
- Dense screens may take more vertical space, but controls remain reachable via
  existing or added scroll areas.

### Step 4: Replace Local Guards

- Move duplicated local width/height guard logic from Settings, Messages, and
  SOP into shared helpers.
- Keep tab-specific semantic decisions local, such as which labels elide,
  which panels scroll, and which workbench button is promoted.

Acceptance:

- Local guard behavior is preserved.
- Shared helper coverage increases without changing workflow behavior.

### Step 5: Compose-Specific Large Text Cleanup

- Replace remaining hard-coded compose row/chip heights with shared
  font-derived helpers.
- Confirm the embedded Compose screen promotes the full workbench when Large
  text plus reduced height makes the embedded surface unsuitable.
- Confirm the full Compose Workbench remains clean after close/reopen and after
  switching FLMsg/FLAmp, JS8Call, FIOSpotter, and CommStat RF modes.

Acceptance:

- FLMsg/FLAmp routing chips, VarAC copy chips, signing, folder, BBS, Spotter,
  and CommStat controls are readable at Large text.
- No controls overlap after returning from the full workbench.
- The operator can finish a message without reducing text size.

### Step 6: High-Use Screen Sweep

Review in this order:

1. Station command bar and ControlFreq dashboard.
2. Messages Inbox and Compose.
3. Settings radio/profile/setup panes.
4. Map controls and detail panels.
5. SOP Builder.
6. JS8/FLDigi net-control tabs.
7. Lower-frequency dialogs.

Acceptance:

- Every reviewed screen has a Large-text screenshot or explicit manual note.
- Fixes are small and local only when the shared guard cannot know the correct
  semantic layout.

### Step 7: Regression Coverage

- Add source tests that prevent new hard-coded small maximum heights on
  text-bearing controls unless they use a font-metric helper or opt-out marker.
- Add targeted tests for Compose, Settings, and station command bar because
  those are the most operator-visible.
- Keep screenshot/manual QA in the release checklist because automated Qt layout
  tests cannot catch every visual clipping failure.

Acceptance:

- Future UI slices have a failing test or checklist item when they introduce
  obvious Large-text clipping risk.
- Review burden moves from repeated manual discovery to a predictable rule.

## Manual QA Matrix

For each high-use screen, verify:

- Normal, Medium, and Large text.
- Main laptop window.
- Reduced window around `1000x700`.
- Tight/minimized-like height around `900x560`.
- Open/close any pop-out workbench or modal and return to the tab.
- Switch tabs away and back.
- Confirm no text-bearing control is clipped, overlapped, or unreachable.

## Rollout Notes

This should not be a single huge visual refactor. The safest rollout is:

1. Build shared helpers.
2. Apply the conservative global guard.
3. Fix only the layouts that still fail at Large text.
4. Promote the rule into tests and release QA.

That sequence protects users with poor vision while reducing the amount of
tedious tab-by-tab review needed for every future UI change.
