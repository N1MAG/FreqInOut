# Internal Security Process

This document describes maintainer handling steps after receiving a private security report.

## Intake

1. Confirm report receipt with the reporter.
2. Create a private tracking item (local notes or private issue tracker).
3. Record affected version(s), platform(s), and reproduction steps.

## Triage

1. Reproduce the issue.
2. Assess impact:
   - confidentiality
   - integrity
   - availability
3. Assign severity (low/medium/high/critical).

## Remediation

1. Implement the fix on a working branch.
2. Add regression checks where feasible.
3. Validate on impacted platforms (Windows/Linux).
4. Prepare release notes with concise user guidance.

## Release

1. Publish patched release.
2. Update `CHANGELOG.md` with a security note.
3. Notify reporter and request validation if appropriate.

## Post-release

1. Review root cause and preventive controls.
2. Update secure coding and review checklist items as needed.
