# Operational View Code Issue Log

This log captures code issues found while implementing the operational view
framework. Items here should be resolved deliberately before this area is treated
as world-class production UI.

## Open Issues

### UI-001: Operational views still rely on large legacy Qt classes

Observed while wiring ControlFreq, Messages, and Map handoffs.

Impact:

- View contracts now exist, but several screens still render from large tab
  classes. This increases the risk that future source additions will bypass the
  contract layer.

Recommended fix:

- Continue extracting Qt-free projection builders and focused presenters before
  adding MeshCore, Mesh MQTT, APRS, or Reticulum/LXMF UI.

### UI-002: Full visual QA is still manual

Observed during this implementation pass.

Impact:

- Focused tests verify contracts and wiring, but they do not prove that every
  Large text, minimized-window, and multi-radio layout renders cleanly.

Recommended fix:

- Add screenshot or widget-geometry checks for the highest-risk views:
  Station Command Bar, ControlFreq Operational Awareness, Messages Compose, Map
  context handoffs, and setup banners.

### TEST-001: Full pytest suite did not complete within interactive window

Observed during verification after the operational view contract/UI updates.

Impact:

- Focused operational, ControlFreq, Messages, Map, Compose, and station-command
  tests pass, but the full suite did not finish before the interactive run was
  stopped. This leaves residual risk in unrelated or slower integration tests.

Recommended fix:

- Run `.venv/bin/pytest -q` in a longer unattended environment before release.
  If it stalls again, bisect by test directory and mark slow/hanging GUI tests
  with clearer timeout behavior.

Latest verification:

- A bounded `.venv/bin/pytest -q` run timed out after 180 seconds in the same
  early progress range. Focused operational view and shell tests pass; the
  full-suite hang still needs a separate bisection pass.

## Recently Addressed

### VIEW-001: Selectable operational views were not registered in code

Status: Addressed.

- `freqinout/core/operational_view_registry.py` now defines reusable view keys,
  labels, templates, allowed source families, gate requirements, action kinds,
  and default row limits.
- ControlFreq view chips and presets now consume that registry instead of a
  standalone local list.

### CTRL-001: ControlFreq row actions were not source-contract aware

Status: Addressed.

- Row action chips now check `SourceViewContract.actions`.
- Global ControlFreq action buttons now sync against the selected context and
  disable invalid actions with a specific tooltip.

### CTRL-002: Station Command Bar could size one card too wide

Status: Addressed.

- Multi-radio card sizing now uses scroll viewport width, then scroll width,
  then bar width.
- Multi-radio cards cap at `520` px and shrink to `280` px before horizontal
  scroll is needed.
- Card mode now gives the radio-card scroller the full station command bar row,
  so the legacy single-radio grid cannot squeeze two configured radios into a
  clipped strip.

### MAP-001: Map tab emitted invalid escape sequence warning

Status: Addressed.

- The embedded JavaScript regex in `freqinout/gui/stations_map_tab.py` now
  escapes the Python string backslash, preserving the generated regex while
  removing the Python `SyntaxWarning`.
