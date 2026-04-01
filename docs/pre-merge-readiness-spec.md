# Pre-Merge Readiness Spec

## Objective

Define the minimum set of fixes and regression coverage required before merging the stabilization/security work into `main`.

This spec is intentionally narrow. It focuses on concrete correctness, security, and stability issues already identified during review. It does not attempt a broad architectural cleanup.

## Merge Decision Context

The current review surfaced two groups of concerns:

1. Issues already present on `main` (`v1.2.1`) that should be corrected before or as part of the merge.
2. A remaining defect in the stabilization branch around status probing for unsaved FLRig/FLDigi endpoint changes.

The goal is to merge with a branch that is measurably safer than `main`, not to bundle every desirable refactor.

## In Scope

### 1. Single-instance lock cleanup

Problem:
- `freqinout/main.py` contains redundant `QLockFile` state checks after `tryLock(0)`.
- The current flow is harder to reason about than necessary and invites future mistakes around startup behavior.

Required change:
- Replace the duplicate `isLocked()` / `tryLock()` sequence with a single `tryLock(0)` gate.
- Preserve current user-visible behavior: if another instance is active, show the existing message and exit cleanly.

Acceptance criteria:
- Startup code has one authoritative lock acquisition check.
- Existing lockfile lifetime behavior remains unchanged.
- A targeted regression test covers successful acquisition and already-running behavior if practical for the current test harness.

### 2. Updater archive hardening

Problem:
- `freqinout/core/updater.py` downloads an archive and calls `ZipFile.extractall(...)` on untrusted content.
- This permits path traversal style archive entries and is the highest-priority security concern found in review.

Required change:
- Replace raw `extractall(...)` usage with validated extraction.
- Reject archive members that escape the intended extraction directory.
- Reject absolute paths and suspicious path segments.
- Fail closed: if validation fails, abort update application and log the reason.

Preferred implementation notes:
- Validate each member path before extraction.
- Normalize against the temp extraction root and confirm each target stays within that root.
- Keep backup/rollback behavior intact.

Optional but recommended:
- Add support for release metadata integrity checks if the update feed already provides a hash.
- If hash validation is added, make it explicit and mandatory only when metadata is present.

Acceptance criteria:
- A malicious zip entry like `../outside.txt` is rejected.
- A malicious absolute-path entry is rejected.
- A normal archive still applies successfully.
- Regression tests cover both safe and unsafe archive shapes.

### 3. Runtime path consistency

Problem:
- `main` still contains multiple path-resolution implementations instead of treating `freqinout/core/config_paths.py` as the source of truth.
- The most visible divergence is in logging and ancillary tooling.
- The stabilization branch already moves some of this in the right direction, and the merge should complete that direction rather than reintroduce divergence.

Required change:
- Standardize runtime config/log/profile resolution on `freqinout/core/config_paths.py`.
- Ensure logger, DB tooling, and any merge-touched modules use the same runtime profile root.
- Avoid adding new parallel path helpers.

Acceptance criteria:
- Log output, settings DB, nets DB, and merge-touched tools resolve under the same effective runtime root.
- `FREQINOUT_CONFIG_DIR` remains honored everywhere in scope.
- A regression test confirms logger and DB/tooling align to the same runtime profile.

### 4. Timezone helper consolidation

Problem:
- Timezone resolution is duplicated between `freqinout/__init__.py`, `freqinout/utils/__init__.py`, and `freqinout/utils/timezones.py`.
- The duplicated top-level package helpers are inconsistent with the newer shared utility behavior and still use older UTC patterns.

Required change:
- Establish one shared timezone-resolution path for package helpers.
- Remove or delegate duplicated logic in `freqinout/__init__.py`.
- Use timezone-aware UTC calls in any touched code paths.

Acceptance criteria:
- Top-level time helpers and UI-facing helpers resolve timezone names through the same implementation.
- No newly touched code uses naive UTC APIs where aware UTC equivalents are available.
- Regression coverage exists for the specific helper behavior we preserve.

### 5. Settings status probe correctness for unsaved FLRig/FLDigi edits

Problem:
- In the stabilization branch, `SoftwareStatusService.status_snapshot(...)` accepts FLRig override arguments, but FLDigi probing still reads persisted FLRig host/port instead of the unsaved values currently entered in Settings.
- This can produce misleading status badges while editing endpoints.
- The shared cache key for FLDigi reachability also ignores the FLRig endpoint that the probe depends on.

Required change:
- Thread unsaved FLRig host/port overrides through the FLDigi reachability path.
- Update any related cache key so FLDigi probe results are invalidated when the effective FLRig endpoint changes.
- Preserve the endpoint-aware status behavior already added by the stabilization branch.

Acceptance criteria:
- If the user edits FLRig port or host in Settings without saving, both FLRig and FLDigi status reflect those unsaved values consistently.
- FLDigi cached reachability does not remain stale across FLRig endpoint edits within the cache TTL window.
- Regression tests cover both override propagation and cache behavior.

### 6. Targeted regression coverage

Problem:
- `main` currently has almost no committed automated test coverage.
- The merge adds behavior in startup, background ingest, DB schema repair, and status probing that is too easy to regress silently.

Required change:
- Commit focused regression tests for the items above.
- Prefer small, behavior-anchored tests over broad GUI smoke coverage.

Minimum required test areas:
- Single-instance lock startup behavior if testable with current Qt harness.
- Updater archive validation.
- Runtime config-path consistency.
- Timezone helper delegation and aware UTC usage where touched.
- FLRig/FLDigi unsaved endpoint probe correctness.
- Existing stabilization tests for background ingest, DB schema migration, and VarAC cold-start table creation should remain green.

Acceptance criteria:
- The new merge path does not reduce the test coverage introduced by the stabilization branch.
- All new tests are checked into `tests/` and not silently excluded by ignore rules.

## Explicit Non-Goals

These are valid follow-up efforts, but they should not block this merge unless new evidence shows they are directly causing defects:

- Splitting `sop_tab.py` and `message_viewer_tab.py` into many modules.
- Broad GUI lazy-loading work beyond already reviewed stabilization changes.
- Replacing `SettingsManager` with connection pooling.
- Large-scale async conversion of database or UI code.
- Generalized JSON schema validation across the entire application.

## Post-Merge Follow-Up Track

The large GUI files should be addressed as a separate refactor track after merge:

- Extract pure parsing/query/business logic out of the tabs into `core/` services.
- Split each large tab into a package with `view`, `dialogs`, `models`, and `actions` modules.
- Add characterization tests before moving behavior-heavy code.
- Defer lazy loading and UI composition improvements until after behavior is covered.

This work is important, but it is higher-risk and lower urgency than the merge-readiness items above.

## Suggested Implementation Order

1. Fix updater archive hardening and add tests.
2. Fix the stabilization branch FLRig/FLDigi unsaved override defect and cache invalidation.
3. Clean up single-instance lock startup logic.
4. Finish path consistency cleanup for logger/tooling.
5. Consolidate timezone helpers and add regression tests.
6. Ensure all new tests are tracked by git and execute in the normal test workflow.

## Merge Gate

The branch is ready to merge when:

- No known high-severity security issue remains in the update/install path.
- Settings status badges do not present misleading endpoint state during unsaved edits.
- Startup lock behavior is simplified and verified.
- Runtime profile paths are consistent across logging and DB/tooling in merge-touched code.
- Targeted regression tests for these fixes are committed and passing in the project test environment.
