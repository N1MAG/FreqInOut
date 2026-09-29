# Windows Startup Helper Flash Hotfix Specification

Status: `Awaiting maintainer pass approval`

Private implementation commit:
`7ab70df07f263b28c1f7ea253376626c761c202d` on
`wip/private-testing-multi-rig-1.2.3-not-ready`.

Follow-up startup-surface commit:
`e0aebbfa91bf91e0442a4d993b6cd4961ca88486` on the same branch.

Governing contracts:

- `project_delivery_rules.md`
- `cross_platform_release_packaging_spec.md`
- `public_2_0_runtime_release_plan.md`

## Operator evidence

The supplied Windows recording showed a short blank window before the FIO
splash and additional blank frames behind the splash while FIO was loading.
The FIO splash itself remained stable until the main window opened. The
packaged executable already uses PyInstaller's windowed mode (`console=False`)
and deferred screen warming is disabled by default.

The first private correction found two noninteractive child commands without
Windows no-window policy: `tzutil /g` for system-timezone detection and
`rigctl -l` for the radio-model catalog. Either command could create a
transient console host when launched from the packaged GUI application.

Maintainer retest at private commit `7c6eca2` reported that the desktop no
longer redrew, but the transient windows remained. Disabling both monitoring
and automatic startup for the only configured Launch Control application did
not change the behavior. That evidence rejects Launch Control and companion
applications as the source of the remaining startup defect.

Frame-by-frame review localized the remaining surfaces to two FIO-owned
boundaries: an unpainted native surface immediately before the first splash
frame, and an untitled FIO-sized surface behind the splash while the status was
`Loading application settings...`. The latter changes geometry while
`MainWindow` is still under construction and before `main.py` deliberately
calls `show()`.

## Bounded correction

`freqinout.core.subprocess_utils.noninteractive_subprocess_kwargs()` is an
opt-in policy for internal commands that never require operator interaction.
On Windows it requests both `CREATE_NO_WINDOW` and a hidden `STARTUPINFO`; on
other platforms it returns no subprocess keyword arguments.

Only the timezone and radio-catalog probes use the policy. The Launch
Orchestrator, Settings launch controls, and every user-visible FLRig, FLDigi,
JS8Call, VarAC, and other companion application launch path remain unchanged.
The hotfix does not change catalog caching, radio identity, upgrade routing,
database state, or the first-launch guidance contract.

The follow-up surface correction prepares the splash status and pixmap before
mapping its native window. On Windows only, `MainWindow` applies
`WA_DontShowOnScreen` immediately after its base constructor and removes that
shield only after construction is complete, immediately before the one
intentional `win.show()` in `main.py`. Removing the shield first normalizes any
logical visibility transition with `hide()`. Linux and macOS retain their
existing main-window lifecycle.

This correction does not hide, minimize, change flags for, or otherwise alter
any user-launched companion application. The existing-station upgrade dialog
runs before `MainWindow` construction and is therefore outside the shield.

## First-launch boundary

This hotfix preserves the state-driven first-launch design:

- a fresh station will receive the separate guided `Set Up First Radio` work;
- an existing single-radio station continues to require `Upgrade Existing
  Station` before runtime services start;
- a current configured station opens normally; and
- an unreadable or unsafe station must stop with recovery guidance.

That guidance is a successor slice and is not implemented by this flashing
hotfix.

## Ownership and review

- Primary `gpt-6-astra` (high reasoning) owned scope, implementation,
  integration, tests, documentation, and exit-gate review.
- Independent `gpt-5.6-terra` (low reasoning) performed the initial read-only
  Windows subprocess-policy audit and the follow-up startup process/surface
  boundary audit. It made no file changes. The first audit confirmed that
  companion-application launch paths must remain outside the hidden-process
  policy; the follow-up confirmed that disabling Launch Control is the correct
  isolation test before changing native startup visibility.

## Acceptance evidence and gate

Current automated evidence:

- splash first-frame ordering, Windows main-shell shield ordering/release,
  subprocess policy, deferred startup surfaces, deferred screens, and runtime
  partition: **32 passed, 3 platform skips**;
- isolated offscreen startup smoke: passed, reaching the first usable shell and
  clean worker shutdown;
- changed Python files compile successfully; and
- `git diff --check` passes.

Maintainer pass gate: install the next Windows candidate, close any existing
FIO process, and record one cold launch from the FreqInOut shortcut. The pass
condition is one fully painted splash followed by one fully constructed main
window, with no untitled or blank FIO window before or behind the splash. For a
legacy profile, `Upgrade Existing Station` must still appear and retain its
existing behavior. A configured companion application must still open normally
when its Launch Control startup choice is restored.

Approval queues this hotfix for the next point release. It does not authorize
an immediate public push.
