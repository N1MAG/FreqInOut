# Windows Startup Helper Flash Hotfix Specification

Status: `Awaiting maintainer pass approval`

Private implementation commit:
`7ab70df07f263b28c1f7ea253376626c761c202d` on
`wip/private-testing-multi-rig-1.2.3-not-ready`.

Governing contracts:

- `project_delivery_rules.md`
- `cross_platform_release_packaging_spec.md`
- `public_2_0_runtime_release_plan.md`

## Operator evidence

The supplied Windows recording showed a short blank process window before the
FIO splash and additional blank process frames behind the splash while FIO was
loading. The FIO splash itself remained stable until the main window opened.
The packaged executable already uses PyInstaller's windowed mode
(`console=False`), so the blank frames were not FIO's own console and were not
deferred screen warming.

The startup path contained two noninteractive child commands without Windows
no-window policy: `tzutil /g` for system-timezone detection and `rigctl -l` for
the radio-model catalog. Either command could create a transient console host
when launched from the packaged GUI application.

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
- Independent `gpt-5.6-terra` (low reasoning) performed a read-only Windows
  subprocess-policy and regression-boundary audit. It made no file changes and
  confirmed that companion-application launch paths must remain outside this
  policy.

## Acceptance evidence and gate

Automated evidence:

- subprocess-policy, radio-catalog, runtime, splash, startup-deferral, and
  public-export partition: **27 passed, 3 platform skips**;
- changed Python files compile successfully; and
- `git diff --check` passes.

Maintainer pass gate: install the next Windows candidate, close any existing
FIO process, and record one cold launch from the FreqInOut shortcut. The pass
condition is no blank helper-process window before or behind the splash, with
the normal splash-to-main-window sequence preserved. For a legacy profile,
`Upgrade Existing Station` must still appear and retain its existing behavior.

Approval queues this hotfix for the next point release. It does not authorize
an immediate public push.
