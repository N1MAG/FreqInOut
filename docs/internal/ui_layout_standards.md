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
is organized as one stable card per active radio. Do not rely on a `primary
radio` concept in the operator-facing control model; a radio is either active
and managed, or inactive and out of the command surface.

The card model is used for one or more active radios. A single active radio may
use the available width, but it must still be the same card interaction model
used when a second radio is activated. Activating or deactivating a radio should
not switch to a different legacy control-strip layout.

The `Now` hero should prefer the operator-facing operating group and band, such
as `MAGNET 40M` or `S2/GHOSTNET 20M`, rather than the raw frequency. Exact
frequency, mode, and mismatch detail belong in the tooltip. If no operating
group can be inferred, fall back to frequency plus band.

Long operating group names must not expand the command bar. The hero label
elides when needed and keeps the full value in its tooltip. `S2 UNDERGROUND`
may be presented as `S2/GHOSTNET` in this control surface because that is the
more operator-recognizable label; the underlying stored group name remains
unchanged.

Each radio card uses that radio's saved frequency-plan assignment as the
authoritative source for the displayed target, next target, plan name, and QSY
option list. Runtime scheduler lanes, radio snapshots, and app-reported
frequencies are fallback or mismatch signals; they must not override another
radio's assigned plan. This prevents one FLRig, RigCtl, JS8Call, or SDR path from
leaking into another radio's card.

Every command emitted from a radio card must carry the selected
`device_profile_id` through the scheduler. The scheduler must resolve that
target to the matching runtime client or the matching configured radio endpoint
before transmitting. If no target-specific client can be resolved in a
multi-active-radio configuration, the command is skipped and surfaced as a
health/routing issue; it must not fall back to a singleton FLRig, RigCtl, or
JS8Call client. Singleton fallback is only acceptable for an unambiguous
single-active-radio compatibility path.

Saving a radio profile must preserve endpoint fields for linked app instances
when those endpoint fields were not part of the user edit. A profile-name,
activation, schedule-assignment, or health-setting save must not default a linked
FLRig/FLDigi/JS8Call row back to standard ports.

Action semantics:

- `QSY`: immediately commands the selected manual QSY target and places only
  that radio into manual QSY state. Scheduled changes for that radio are
  suspended until the operator uses `Resume` or the scheduler explicitly
  transitions to a new active schedule entry.
- `Timed QSY`: commands the selected manual QSY target and applies a timed
  scheduler suspension for the selected duration. Include `Indefinite` as an
  option for operator-controlled manual duration.
- `Timed Suspend`: suspends scheduled frequency changes for the selected
  duration without changing the radio frequency. This supports manual control
  from FLRig, another app, or the radio itself. Include `Indefinite` as an
  option for manual control until the operator resumes.
- `Resume`: clears manual QSY or timed scheduler suspension and returns control
  to the active schedule.

When manual QSY is active, `Resume` must be enabled and visually highlighted.
The `QSY` button text remains stable so it does not resize or clip; button color
and the highlighted `Resume` action carry the state. If the radio is not fully
configured or has not yet reported the new frequency, the target field should
continue to show the commanded QSY target; the tooltip should disclose the
radio-reported frequency if it differs.

When timed QSY or timed suspend is active, only the button that initiated the
state should be highlighted. Its label should carry the countdown so the timed
state is visible where the operator acted. Use compact minute labels while there
is ample time left, such as `28m | Extend`. Under 10 minutes, switch to
`MM:SS | Extend`, such as `09:42 | Extend`. Restore the default button label
when the timed state expires or the user resumes the schedule. Button tooltips
should include the local resume time.

In compact layouts, QSY actions should stack as paired controls:

- `QSY Now` above `QSY Suspend`.
- `Suspend Scheduler` above `Resume Schedule`.

The hold-duration selector belongs on the suspend row because it affects
`QSY Suspend`, not the immediate `QSY Now` command or indefinite
`Suspend Scheduler` action.
