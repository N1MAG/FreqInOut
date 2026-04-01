# Release 1.2.2 Hardening Spec

## Objective

Define the additional changes that should still be made before treating `1.2.2` as final based on the latest code review and follow-up verification against the current `release/1.2.2` branch.

This spec is intentionally narrow. It focuses on concrete release-scope issues that are either:

- security-sensitive,
- clearly actionable with low implementation risk, or
- important for release confidence and regression prevention.

It does not attempt to fold in broad architectural cleanup.

## Review Outcome Summary

After checking the review claims against the current branch, the following conclusions apply:

- The updater integrity concern is real and remains the highest-priority unresolved issue.
- `tmp_gpg_guidance.txt` is present in the repo root and should be removed.
- The `datetime.datetime.utcnow()` concern is still valid; there are remaining calls in the scheduler and several GUI tabs.
- CI currently runs only propagation tests and should be broadened to include the new committed reliability/security tests.
- `updater.py` still has a few small quality issues worth fixing while that module is already being touched.
- The `SettingsManager` thread-safety concern is partly outdated because runtime thread-affinity guards now exist; this is now a contract/documentation issue, not a silent corruption issue.

## Release Decision Context

The branch is already materially safer than `main` because it contains:

- startup lock cleanup,
- updater archive path hardening,
- runtime path consistency fixes,
- status probe improvements,
- scheduler shutdown cleanup,
- `SettingsManager` thread-affinity guardrails,
- targeted regression coverage.

The purpose of this addendum is to finish the remaining high-signal items without turning `1.2.2` into a broad refactor.

## In Scope

### 1. Remove temporary repo artifact

Problem:
- `tmp_gpg_guidance.txt` exists at repo root and appears to be a temporary working artifact, not shipped product documentation.

Required change:
- Remove `tmp_gpg_guidance.txt` from the repository.
- Add an ignore rule for this specific artifact or a narrowly-scoped temporary-text pattern that does not risk hiding real docs.

Acceptance criteria:
- `tmp_gpg_guidance.txt` is no longer tracked.
- The ignore rules prevent accidental recommit of the same temporary artifact class.

### 2. Add updater download integrity verification

Problem:
- `freqinout/core/updater.py` currently trusts any successfully downloaded archive and applies it without validating authenticity or integrity.
- Existing ZIP path hardening prevents traversal, but it does not prove the archive is the intended release.

Required change:
- Extend update metadata validation to require integrity information for downloadable releases.
- Verify the downloaded archive before returning it for installation.
- Fail closed: if verification metadata is missing or the archive does not validate, abort the update and log the reason.

Preferred implementation:
- Add a required `sha256` field to the update JSON.
- Compute SHA-256 locally after download and compare it to the declared value.

Optional future extension:
- GPG signature verification may be added later, but a required SHA-256 check is sufficient for `1.2.2`.

Acceptance criteria:
- A downloaded archive with the expected hash installs normally.
- A downloaded archive with a mismatched hash is rejected before extraction/application.
- Missing or malformed integrity metadata causes the update flow to fail clearly.
- Regression tests cover success, mismatch, and missing-metadata cases.

### 3. Finish UTC modernization in release-touched runtime paths

Problem:
- The branch still contains `datetime.datetime.utcnow()` calls, including one in the scheduler and multiple GUI tabs.
- These calls return naive datetimes and are deprecated in modern Python.

Required change:
- Replace all remaining `datetime.datetime.utcnow()` usage in `freqinout/` with timezone-aware UTC calls:
  - `datetime.datetime.now(datetime.timezone.utc)`

Constraints:
- Do not change user-visible time formatting semantics.
- Keep the change mechanical and low-risk.

Acceptance criteria:
- No remaining `datetime.datetime.utcnow()` calls exist under `freqinout/`.
- Existing tests that guard against `utcnow()` use continue to pass.

### 4. Broaden CI to include committed reliability/security regression tests

Problem:
- The current GitHub Actions workflow only runs propagation tests.
- The branch now contains committed tests for updater security, scheduler shutdown, settings thread-affinity, background ingest, schema readiness, and status behavior that are not exercised in CI.

Required change:
- Expand CI beyond `test_propagation*.py`.
- Include at least the non-macOS-safe committed regression files that are already intended to protect the stabilization work.

Preferred implementation:
- Move from a narrow propagation-only glob to full test discovery, if the Linux CI environment supports it cleanly.
- If full discovery is too broad for this release, explicitly include the committed release-hardening tests in addition to propagation tests.

Acceptance criteria:
- CI executes the updater security test coverage.
- CI executes the scheduler/settings/background-ingest reliability tests that are compatible with the Linux runner.
- The workflow fails on regressions in these areas instead of silently passing.

### 5. Clean up updater implementation while touching the module

Problem:
- `updater.py` still diverges from the logging style used elsewhere in the codebase.
- `parse_version()` collapses malformed/pre-release versions to `(0, 0, 0)`, which can misclassify a newer version as not newer.

Required change:
- Convert eager f-string logger calls in `updater.py` to lazy `%s` style logging.
- Harden `parse_version()` so common suffixes like `1.2.2-beta` do not degrade to `(0, 0, 0)`.

Suggested implementation:
- Parse only the numeric release prefix (`major.minor.patch`) and ignore a trailing suffix for comparison.
- If parsing still fails, log at debug level and continue using a safe fallback.

Acceptance criteria:
- `updater.py` logging style matches project convention.
- Version strings with common suffixes compare sensibly instead of always becoming `(0, 0, 0)`.
- Tests cover at least one pre-release-style version string.

### 6. Clarify scheduler/settings thread contract

Problem:
- `SettingsManager` now correctly fails fast on cross-thread use, but the scheduler’s runtime assumption is still implicit.
- Future refactors could accidentally construct or drive scheduler/settings objects from the wrong thread and only discover the problem late.

Required change:
- Add a lightweight, explicit scheduler thread-contract note or assertion in `SchedulerEngine` initialization/startup code.

Constraints:
- Keep this minimal and non-invasive.
- Do not redesign scheduler threading in `1.2.2`.

Acceptance criteria:
- The scheduler’s main-thread/settings-thread assumption is explicit in code.
- Misuse fails clearly rather than ambiguously.

## Explicit Non-Goals

These are valid follow-up topics, but they should not block `1.2.2` unless new evidence shows they are causing active defects:

- Large-scale reduction of all `except Exception` usage across the codebase.
- Enabling SQLite WAL mode across every DB path.
- Refactoring dynamic SOP SQL construction for readability only.
- Implementing `_maybe_resync_js8()` as a new scheduler behavior feature.
- Splitting large GUI modules into smaller packages.
- Broader updater trust redesign beyond SHA-256 validation.
- Cosmetic-only style cleanup such as the blank line before `main()` unless touched incidentally.

## Suggested Implementation Order

1. Remove `tmp_gpg_guidance.txt` and add the ignore rule.
2. Add updater SHA-256 verification and updater regression tests.
3. Convert remaining `utcnow()` calls to aware UTC.
4. Broaden CI to exercise the committed release-hardening tests.
5. Finish updater logging/version parsing cleanup.
6. Add the lightweight scheduler thread-contract assertion/documentation.

## Release Gate

`1.2.2` is ready for final merge/tag when:

- no unverified updater archive can be applied,
- the temporary repo artifact is removed,
- runtime code under `freqinout/` no longer uses `datetime.datetime.utcnow()`,
- CI covers the committed reliability/security tests relevant to the stabilization branch,
- updater regression tests pass for both safe and unsafe update metadata/archive cases,
- the branch remains clean and testable on Linux and Windows.
