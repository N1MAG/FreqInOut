# FreqInOut First-Run Onboarding Specification

Status: `Awaiting maintainer pass approval`

The maintainer approved this bounded design on 2026-09-29. The private WIP
implementation is complete and has not been authorized for public release.

## Purpose

Give a non-technical user a consistent first-launch experience without making a
new installation carry the complexity of a legacy-station upgrade.

The same saved station state must produce the same onboarding behavior whether
FIO was started from a Windows package, a Linux package, a future macOS package,
or a source checkout.

## Confirmed Current Behavior

FIO currently has two different startup paths:

1. A legacy single-rig station enters the mandatory, pre-shell **Upgrade Existing
   Station** gate. The user must complete or decline the upgrade before the main
   window and runtime services exist.
2. A fresh installation is initialized as a valid multi-rig blank slate with no
   radio profiles. It opens directly to Ops Center. The only guidance is the
   general setup-readiness banner, whose first action currently points to the
   missing callsign rather than **Guided Add Radio**.

This difference is based on the station data FIO finds, not on the operating
system. The package installers do not currently implement onboarding.

## Product Decisions

### Keep upgrade and new-install setup separate

**Upgrade Existing Station** remains a mandatory pre-shell workflow for:

- `existing_unmigrated`
- `deferred`
- `migration_error`

It retains its verified-backup and fail-closed behavior. Fresh-install
onboarding must not reuse, weaken, or add pages to this migration workflow.

### Add one small fresh-install welcome surface

After the usable main window has been painted, a genuinely fresh station with
no radio should see a single owned dialog:

**Title:** Welcome to FreqInOut

**Body:**

> No existing FreqInOut station was found. Start by setting up the first radio.
> Nothing is changed until you review and save it.

**Primary action:** Set Up First Radio…

**Secondary action:** Set Up Later

The primary action opens **Settings > Radios** and starts the existing **Guided
Add Radio** workflow. FIO must not create a placeholder or default radio.

The secondary action opens the normal Ops Center. The setup-readiness surface
remains available so the user cannot lose the path back to radio setup.

This is intentionally a launcher into existing configuration work, not a new
all-in-one setup wizard.

### Do not repeat the welcome dialog after an explicit choice

The dialog is shown until the user chooses either action. Closing the dialog by
its window control is treated as **Set Up Later**.

A small versioned settings key records that the welcome choice was made. The
key must be added to `FIO_EXISTING_USE_IGNORED_KEYS` so onboarding metadata can
never make a blank station look like legacy FIO usage.

Creating a radio also makes the dialog ineligible. If Guided Add Radio is
canceled, no radio row is written; the persistent setup guidance remains.

The acknowledgement key prevents a user who intentionally runs without an HF
radio, including a mesh-focused user, from being prompted on every launch.

### Make the persistent fallback point to the missing radio

When no device profile exists, the Ops Center setup surface should have an
explicit **Set Up First Radio…** action. It must route to the same Guided Add
Radio workflow.

While there are zero saved radio profiles, this action is persistent: the
generic **Dismiss** and **Do Not Remind Again For This Version** actions are not
shown. After any radio profile is saved, the existing dismissible readiness
behavior returns.

The existing readiness review continues to guide later tasks, including:

- station callsign and grid;
- Operating Group and Frequency Plan review;
- radio-specific readiness and safety items.

This change should not reorder the readiness engine globally. It adds a
specific no-radio action so **Review Now** cannot send a new user to callsign
settings while leaving the first-radio path unclear.

## Authoritative Eligibility Policy

The fresh welcome dialog is eligible only when all of these are true:

1. authoritative runtime status is `fresh_default_ready`;
2. the current migration summary contains `fresh_install_blank_slate`;
3. no device profile exists;
4. the versioned welcome acknowledgement key is absent; and
5. the legacy upgrade gate has already returned success or was not required.

The UI must not infer this state from platform, install path, file timestamps,
or whether the executable was built by GitHub Actions.

### State matrix

| Station state | Startup experience |
|---|---|
| Fresh profile, no radio, welcome not acknowledged | Paint shell, then show **Welcome to FreqInOut** |
| Fresh profile, user chose Set Up Later | Open Ops Center; keep the no-radio setup action visible |
| Fresh profile, first radio saved | Normal startup; readiness guides remaining work |
| Legacy single-rig profile | Mandatory pre-shell **Upgrade Existing Station** only |
| Successful migrated station | Normal startup; no fresh welcome dialog |
| Deferred or failed migration | Mandatory upgrade/recovery gate; no fresh welcome dialog |
| Current station whose radios were later removed | No first-run dialog if it was previously acknowledged; show no-radio readiness action |
| Observer-only radio saved | Onboarding is complete; normal readiness rules apply |
| Mesh-focused user chooses Set Up Later | No repeated modal; normal Ops Center and settings remain available |

## Interaction and Lifecycle Contract

This is a guided-workflow entry point with the following sequence:

1. **Context:** FIO states that no existing station was found.
2. **Choice:** Set up the first radio or defer setup.
3. **Work:** Existing Guided Add Radio owns radio identity, software,
   connections, operating behavior, safety, and review.
4. **Commit:** Nothing is persisted as a radio until Guided Add Radio's existing
   final save succeeds.
5. **Feedback:** Settings and Ops Center refresh from committed state; the
   readiness surface shows the next incomplete task.

The fresh dialog must be non-blocking to the Qt event loop and owned by the main
window. It is scheduled only after:

- the main window has been shown;
- the first usable shell has been recorded; and
- the startup splash has finished.

Only one instance may exist. Opening it must not start external applications,
radio connections, Bluetooth discovery, scheduler work, or additional console
processes. It must not cause any pre-shell surface transition or Windows desktop
flash.

Packaged `--smoke-test` runs do not show or acknowledge the dialog. They retain
their existing unattended one-second verification and clean-shutdown behavior.

## Task-Oriented Design Brief

- **Primary operator task:** Set up the first radio or deliberately defer that
  task without losing the in-application route back to it.
- **Starting context:** FIO has authoritative fresh-blank-slate state and no
  saved radio profiles.
- **Completion outcome:** Guided Add Radio saves the first profile, or the user
  returns to Ops Center with a persistent setup action.
- **Task sequence:** Fresh context → setup/defer choice → existing Guided Add
  Radio → existing review and save → refreshed readiness.
- **Primary action:** **Set Up First Radio…**.
- **Essential state and Why:** The welcome explains that no station was found
  and that nothing changes before final save; Ops Center states that no radio
  is configured.
- **Secondary and advanced work:** **Set Up Later** is secondary. Callsign,
  grid, Operating Groups, Frequency Plans, and advanced radio details remain
  in their existing owner workspaces.
- **Workspace archetype:** Guided-workflow entry point, because it establishes
  context and hands off to the existing staged radio workflow.
- **Responsive behavior:** Standard QMessageBox layout wraps naturally at
  compact widths; the existing single-scroll-owner Ops Center layout owns the
  persistent card. No new horizontal scrolling or fixed content height is
  introduced.
- **Shared-theme and component reuse:** Standard Qt message-box controls,
  application theme resolution, shared button roles, and the existing readiness
  card are reused.
- **Performance boundary:** One post-shell status/settings/profile eligibility
  read is allowed. Repaint, resize, and dialog interaction perform no endpoint,
  process, Bluetooth, or companion-application work.

## Minimal Implementation Boundary

The approved implementation should be limited to:

1. a pure policy function that decides whether fresh onboarding is eligible;
2. one ignored, versioned KV acknowledgement key;
3. one small main-window-owned welcome dialog;
4. a public Settings entry point that opens **Guided Add Radio** without calling
   a private method across object boundaries;
5. one explicit no-radio action in the existing Ops Center setup surface; and
6. focused automated tests and operator evidence.

Out of scope:

- installer-specific onboarding;
- a replacement configuration wizard;
- automatic creation of a radio, software instance, Operating Group, or
  Frequency Plan;
- changes to legacy migration or backup behavior;
- automatic launch of companion applications;
- changes to radio readiness or scheduler safety policy;
- onboarding based on Windows, Linux, macOS, `.exe`, `.deb`, or source mode.

## Failure and Recovery Behavior

- If eligibility cannot be read, log the error and leave the normal readiness
  surface available. Do not block launch and do not assume the station is fresh.
- If the welcome dialog cannot open, log the error and continue to Ops Center.
- If Guided Add Radio cannot open, retain the blank station and show its existing
  actionable error. The welcome acknowledgement does not hide the Ops Center
  no-radio action.
- If the acknowledgement write fails, continue safely. A repeated welcome on a
  later launch is preferable to changing station configuration incorrectly.
- Migration failures continue to use the existing fail-closed upgrade path and
  never fall through to fresh onboarding.

## Accessibility and Presentation

- Reuse the application theme and standard dialog/button components.
- The primary action is the default keyboard action; Escape and window close
  behave as **Set Up Later**.
- Button and explanatory text must remain usable with supported large-text
  scaling and at the supported minimum desktop size.
- The dialog must not depend on color to communicate the required next action.
- Wording must use operator language: **radio**, **Set Up First Radio**, and
  **Guided Add Radio**. Do not expose migration versions, database terms, or
  internal profile identifiers.

## Automated Acceptance Criteria

1. Fresh blank state plus no acknowledgement schedules exactly one welcome
   dialog after the shell is visible.
2. Windows, Linux, macOS, and source execution produce the same policy result
   for the same station data.
3. Existing-unmigrated, deferred, and migration-error states run only the
   upgrade gate.
4. Migrated/current stations never receive fresh onboarding.
5. **Set Up First Radio…** opens Settings > Radios and Guided Add Radio exactly
   once.
6. **Set Up Later**, window close, and Escape persist acknowledgement but create
   no radio or software rows.
7. Canceling Guided Add Radio creates no partial rows and leaves the no-radio
   action visible.
8. Saving the first radio removes the fresh welcome eligibility and refreshes
   readiness to the next actionable task.
9. The acknowledgement key is ignored by legacy-use detection.
10. Startup surface tracing confirms the welcome surface is created only after
    `startup_complete` and does not add pre-shell windows.
11. Existing packaged smoke tests remain successful without needing to interact
    with the welcome dialog.
12. Existing upgrade-gate and Guided Add Radio tests remain green.

## Operator Validation Matrix

Before release, validate at minimum:

1. clean Windows executable installation;
2. clean Linux Debian-package installation;
3. Windows installation over an existing 1.2.8 station;
4. source launch against a clean temporary configuration directory;
5. Set Up Later followed by restart;
6. Guided Add Radio cancel followed by restart;
7. successful first-radio save followed by restart; and
8. large-text and minimum-window-size presentation.

For each run, preserve the application log and startup surface trace. Confirm
that package updates retain user data and do not cause a completed or deferred
welcome dialog to reappear unexpectedly.

## Recommendation for Approval

Approve this as a small consistency hotfix: one post-shell welcome entry point,
reuse of Guided Add Radio, and one persistent no-radio action. Keep station
identity and Operating Group work in the existing readiness flow rather than
expanding first launch into a new multi-page wizard.

## Private Implementation Evidence

Implementation state: `Awaiting maintainer pass approval`.

- The acknowledgement is a versioned ignored KV key and cannot make a blank
  profile count as legacy usage.
- Eligibility is a pure policy over authoritative runtime status, the fresh
  migration marker, saved-profile presence, and acknowledgement.
- The welcome is queued after `startup_complete`; unattended smoke mode is
  excluded.
- Both welcome and Ops Center route through one public Settings seam into the
  existing Guided Add Radio workflow.
- The no-radio Ops Center action remains visible regardless of generic
  readiness dismissal or version suppression.
- Focused onboarding, upgrade, runtime-status, readiness, startup-surface,
  deferred-screen, projection-lifecycle, performance, and shell UX partition:
  **267 passed, 4 platform skips**.
- The larger Guided Add Radio/settings partition produced **216 passes** and
  one unrelated baseline source-contract mismatch: the test expects
  `settings_section_nav_scroll.setVisible(True)` in a construction block where
  the current branch does not contain it. Onboarding did not touch that block.
- Fresh-profile offscreen visual review confirmed the welcome hierarchy and the
  persistent Ops Center action at 1280×800.
- Fresh packaged startup smoke completed without showing or acknowledging the
  interactive welcome dialog.
- Changed Python compilation and `git diff --check` pass.
