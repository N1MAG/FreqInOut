# FIO UI Layout Standards

## Minimized Window Usability

Every UI change must be reviewed at reduced window sizes, not only at the
developer's normal desktop size. A minimized or narrow view may become more
compact, but it must remain usable.

Required behavior:

- Primary navigation remains visible or scrollable.
- Action and filter panels use vertical scrolling when their content cannot fit.
- Buttons, combo boxes, text fields, and labels keep a readable minimum height.
- Tables may scroll internally, but surrounding controls must not collapse into
  clipped slivers.
- Horizontal scrolling is avoided for control panels unless the content is an
  intentionally wide data grid.
- Dense inbox/table workspaces may preserve a wider designed body width and use
  horizontal scrolling when shrinking would clip filters, focus buttons, or
  operator-critical table columns.
- Responsive layouts should prefer stacked/vertical control groups over shrinking
  controls below their usable size.
- Dense workspaces with many categories should use a stable left navigation rail
  with scrollable category content instead of long horizontal button rows.
- Settings-style views should keep the left rail bounded and scrollable, with the
  selected content pane owning the remaining width.

Acceptance check for future UI slices:

- Test or manually verify the touched screen at a reduced window size around
  `1000x700` and at a tighter minimized-like height around `900x560`.
- If controls do not fit, add a scroll area or alternate compact layout rather
  than allowing Qt to compress fields.
- Add focused regression coverage for scroll areas or compact layout behavior
  when the changed screen contains dense controls.

## Station Command Bar

The station command bar is the primary always-visible radio control surface. It
is organized as three readable zones:

- `Radio`: selected command radio.
- `Now`: current or commanded operating target.
- `Action`: manual QSY and scheduler control actions.

The `Now` hero should prefer the operator-facing operating group and band, such
as `MAGNET 40M` or `S2/GHOSTNET 20M`, rather than the raw frequency. Exact
frequency, mode, and mismatch detail belong in the tooltip. If no operating
group can be inferred, fall back to frequency plus band.

Long operating group names must not expand the command bar. The hero label
elides when needed and keeps the full value in its tooltip. `S2 UNDERGROUND`
may be presented as `S2/GHOSTNET` in this control surface because that is the
more operator-recognizable label; the underlying stored group name remains
unchanged.

Action semantics:

- `QSY Now`: immediately commands the selected manual QSY target and places the
  scheduler into manual QSY state. Scheduled changes are suspended until the
  operator uses `Resume Schedule` or the scheduler explicitly transitions to a
  new active schedule entry.
- `QSY Suspend`: immediately commands the selected manual QSY target and applies
  a timed scheduler suspension for the selected duration.
- `Suspend Scheduler`: indefinitely suspends scheduled frequency changes without
  changing the radio frequency. This supports manual control from FLRig, another
  app, or the radio itself. It is cleared only by `Resume Schedule`.
- `Resume Schedule`: clears manual QSY or timed scheduler suspension and returns
  control to the active schedule.

When manual QSY is active, `Resume Schedule` must be enabled and visually
highlighted. The state label should show `Manual QSY` so the operator knows the
station is intentionally off the configured schedule. If the radio is not fully
configured or has not yet reported the new frequency, the hero should continue
to show the commanded QSY target; the tooltip should disclose the radio-reported
frequency if it differs.

When timed scheduler suspension from `QSY Suspend` is active, the `QSY Suspend`
button label should carry the countdown so the timed state is visible where the
operator acted. Use compact minute labels while there is ample time left, such
as `QSY Suspend 30m`. Under 10 minutes, switch to `MM:SS`, such as
`QSY Suspend 09:42`. Restore the default button label when the timed suspension
expires or the user resumes the schedule. Button tooltips should include the
local resume time. `Suspend Scheduler` is not timed and should not display a
countdown.

In compact layouts, QSY actions should stack as paired controls:

- `QSY Now` above `QSY Suspend`.
- `Suspend Scheduler` above `Resume Schedule`.

The hold-duration selector belongs on the suspend row because it affects
`QSY Suspend`, not the immediate `QSY Now` command or indefinite
`Suspend Scheduler` action.
