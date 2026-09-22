# FreqInOut 2.0 Public Runtime Release Plan

Status: recorded for future execution; do not begin public promotion until the
maintainer explicitly declares the release candidate ready.

## Decision And Outcome

FreqInOut 2.0 multi-rig becomes the public base application. Single-radio use
remains a fully supported configuration of that application, not a separate
public product branch.

The public `N1MAG/FreqInOut` repository is a user-facing runtime distribution.
Users must be able to clone or pull it and receive the application, installers,
runtime assets, licenses, release notes, and user documentation without tests,
engineering specifications, internal worklogs, diagnostic evidence, or other
development-oriented material.

Promotion uses two separate boundaries:

1. Reconcile public v1.2.8 and multi-rig 2.0, run tests, and retain engineering
   evidence in the private `FreqInOut-internal-testing` repository.
2. Export an explicit allowlisted runtime snapshot into a clean public release
   branch based on public `main`. Never merge or publish the private WIP history.

The publishing implementation must use an allowlist. Deleting forbidden paths
after a broad copy is not sufficient because an omitted deletion could publish
private material and a deleted file would remain in public history.

## Public Runtime Boundary

The final allowlist is reviewed before the first export. It normally includes:

- the `freqinout` application package;
- required runtime assets, packaged configuration/reference data, and vendored
  runtime dependencies;
- public README, changelog, license, credits, security contact, and user help;
- Python/runtime dependency metadata;
- user-facing install, update, uninstall, launcher, Windows packaging, and
  source-package metadata required to obtain or run FIO.

It excludes at minimum:

- `tests/`, engineering `tools/`, `.github/` development workflows, `.windsurf/`,
  `AGENTS.md`, specifications, internal worklogs, and `docs/internal/`;
- benchmark, audit, fixture-generation, and developer-only scripts;
- operator databases, logs, screenshots, captures, rendered working files,
  machine-specific paths, credentials, or private repository instructions;
- local caches, build trees, coverage data, virtual environments, and release
  rehearsal output.

Private verification must scan both the public candidate tree and its new
public commits. An excluded file must never enter public Git history, even
temporarily.

## Gate PR-1 — Reconcile The Product Baseline Privately

- Freeze and identify one immutable multi-rig 2.0 release-candidate commit.
- Reconcile the candidate with every public v1.2.3 through v1.2.8 runtime fix.
  Preserve the intent of scheduler/thread lifecycle, shared process/status
  polling, FLDigi demand-driven checks, JS8 connection sharing, Managed BBS
  cadence, Station Health, packaged-runtime, installer, and Python compatibility
  corrections within the multi-rig architecture.
- Resolve conflicts semantically; selecting either side wholesale is not
  acceptable for persistence, scheduling, launch, status, or migration code.
- Perform all integration and acceptance testing in the private repository.
- Record the candidate commit, public base commit, reconciliation decisions,
  acceptance results, remaining platform gates, and runtime export fingerprint.

## Gate PR-2 — Smooth Single-Rig Upgrade

Upgrading the current public single-rig release is a first-class release path,
not an exceptional test route.

- Start with a clean installation of the then-current public release and with a
  production-shaped copy containing schedules, plans, messages, operators,
  SOPs, Spotter/Expect state, software paths, launch preferences, VarAC/BBS
  state, keyring references, and window/settings state.
- Close FIO and affected companion applications before backup or migration.
- Create and verify a complete, restorable pre-upgrade backup before changing
  the checkout, environment, database, profile, launcher, or desktop entry.
- Resolve the current contract contradiction: the upgrade guide says migration
  waits for informed operator confirmation, while the Linux installer currently
  invokes multi-rig finalization. Public 2.0 must have one documented behavior.
  The required default is explicit informed confirmation after backup review.
  Cancel or Defer must cause zero production-profile migration writes.
- Preserve all existing operator data and application configuration. Convert
  the existing station into a clearly identified default radio and carry its
  software identity, schedule assignment, launch/monitor/startup choices, and
  message paths forward without duplicate application instances or invented
  paths.
- Make migration idempotent. A second launch or rerun performs no duplicate
  conversion and no unexplained rewrite.
- Show a concise post-upgrade review identifying derived values, warnings, and
  optional follow-up without blocking ordinary use for harmless uncertainty.
- Prove rollback from the verified backup, including checkout/launcher state
  and both FIO databases. Document which OS keyring and external application
  files are intentionally outside the backup.
- Test fresh install, in-place update, interrupted/failed update, Cancel/Defer,
  retry, restart, and rollback on supported Linux and Windows paths. macOS
  source update remains supported and documented even when no signed bundle is
  published.
- Compare pre/post integrity checks and durable row counts; investigate every
  decrease or unassigned record rather than treating application startup as a
  successful migration.

## Gate PR-3 — README, In-App Help, And User Documentation

The README is the primary public landing page and must be rewritten for the
public 2.0 product rather than lightly editing the private-testing preview.
It must explain:

- what FreqInOut 2.0 does and that multi-rig is the base while one radio remains
  a normal supported setup;
- supported operating systems and Python versions;
- fresh installation and update commands using the public repository and
  public `main`/release tag;
- the single-rig upgrade flow, backup/consent/rollback guarantees, and where to
  get detailed help;
- first-run expectations, configuration/profile locations, external companion
  software ownership, privacy/offline behavior, and support-safe diagnostics;
- Windows package/source choices, Linux installer behavior, macOS limitations,
  known external qualifications, and the exact 2.0 version.

Review all shipped user documentation against the actual release, including
`docs/guide.html`, `docs/Installation.md`, Linux installer documentation, the
single-rig upgrade guide, troubleshooting, and every contextual Help action.

Help acceptance requires:

- every registered help context resolves to a real guide anchor;
- every static UI help key has a specific registered context rather than
  silently falling back to generic Help;
- each major workspace and setup flow explains purpose, safe normal workflow,
  radio/software ownership, saved effects, recovery, and platform differences;
- Add Radio, Software Administration, Launch Control, schedules/plans, FIO
  Spotter, Messages/Compose, BBS, MeshCore/Meshtastic, VarAC/VARA, Fast Light,
  JS8Call, upgrade/migration, backup, and rollback match shipped behavior;
- screenshots or wording do not expose private station data or private-testing
  instructions.

Help implementation audit on 2026-09-22: the guide's stale left-rail map was
aligned to the current routes and dedicated sections were added for FIO
Spotter, Station Control Center, Local Reports, Radios, Guided Add/Edit Radio,
Local Mesh, and Condition Alerts. The current 39 registered contexts each
resolve to one unique guide anchor; every literal contextual-help caller has a
registered context, including the previously generic Local Mesh and Condition
Alerts actions. Regression coverage now checks unique registered anchors, all
internal guide links, static caller registration, and coverage of the current
primary navigation labels. Major workspaces without a screen-local Help button
remain reachable from the guide table of contents; adding new UI buttons is a
separate reviewed UI change rather than an implied part of documentation
coverage.

## Gate PR-4 — Runtime Dependencies

- Treat `pyproject.toml` runtime dependencies as the canonical dependency set.
- Make public `requirements.txt` complete for a clean user installation and
  semantically equivalent to that runtime set, including platform markers and
  supported Python constraints.
- Do not publish internal-tool dependencies in the runtime requirements.
  `PyYAML` is currently documented as internal tooling only and must not be
  required by the public runtime unless application code gains a reviewed need.
- Verify a clean environment on each supported platform can install without an
  undeclared import or an unnecessary compiler/toolchain dependency.
- Verify packaged Qt/QML, Map assets, spotter forms, timezone data, BLE/Mesh
  transports, keyring behavior, vendored JS8 support, and report generation.
- Record resolved dependency versions and licenses. Test the oldest and primary
  supported Python versions and reject unsupported Python before modifying an
  existing installation.

## Gate PR-5 — Install, Update, Uninstall, And Packaging

- Change every installer default and example from the private WIP repository
  and opaque WIP branch to the canonical public repository and release channel.
- Test anonymous public clone, first install, normal update, repair, dirty-tree
  handling, running-process refusal, offline behavior, interrupted update,
  rollback, and side-by-side ownership where supported.
- Back up before checkout replacement, dependency changes, launcher changes,
  or database migration. Validate the backup before proceeding.
- Ensure update preserves the selected install/config roots and never silently
  switches an operator between isolated and production profiles.
- Ensure launcher, menu, icon, and uninstall actions prove ownership before
  replacing/removing shared paths. Clearly disclose retained profile data.
- Upgrade installer self-test beyond import/database creation: confirm displayed
  version/build provenance, application launch, packaged Qt/QML and offline Map
  assets, and required resource discovery.
- Build and smoke the Windows executable/installer on Windows. Record signing
  status, SHA-256, build command, and install/uninstall behavior.
- State macOS source-install support and any unsigned/unnotarized limitation
  plainly; do not imply a published bundle where none exists.

## Gate PR-6 — Version 2.0 And Release Metadata

- `freqinout/version.py`, `pyproject.toml`, installer metadata,
  `docs/guide.html`, README, and the top changelog release agree on `2.0.0`.
- Release notes distinguish operator-visible changes, upgrade effects, backup
  and rollback, requirements, supported platforms, known limitations, and
  external companion-software ownership.
- Verify license, credits/third-party notices, security contact, bundled data
  attribution, application icon/title, and package/resource manifests.
- Record immutable source commit, release tag, build provenance, artifact
  SHA-256 values, signing/notarization status, and the private acceptance record.
- Scan for private repo/branch names, preview language, placeholders, mojibake,
  absolute machine paths, callsigns, access codes, database/log names, and
  developer-only navigation.

## Gate PR-7 — Final Qualification And Public Export

- Pass release preflight, compilation, full private automated suite, focused
  migration/installer suites, and all specification-required acceptance gates.
- Complete supported-platform fresh-install and current-public-version upgrade
  runs using clean hosts or clean virtual machines.
- Complete the required multi-radio companion-app, scheduler/RF Guard,
  JS8/Fast Light/VarAC, FIO Spotter/CommStat, Mesh, BBS, shutdown/restart, and
  long-soak operator matrix. An unavailable hardware check remains a visible
  release blocker unless the governing specification explicitly classifies it
  as a post-release limitation.
- Export the allowlisted runtime snapshot into a clean public release branch.
- Clone that public candidate as an ordinary user, install it, launch it, update
  an existing public installation, and verify its tree contains no forbidden
  development/private material.
- Run the private test harness against the exported public checkout so the
  publication boundary cannot conceal a missing runtime file.
- Review the exact public diff and file inventory before pushing. Public `main`,
  the final tag, and release artifacts remain unchanged until this gate passes.

## Promotion And Rollback

After every gate closes, push the curated runtime branch to the public
repository, review it, merge it into public `main`, tag `v2.0.0`, publish the
approved artifacts/checksums, and perform one post-release install/update smoke.
Change public documentation and installer channels only as part of that reviewed
promotion.

Retain the immutable private candidate, reconciliation record, export manifest,
backup/rollback evidence, and prior public tag. If public smoke fails, stop the
release channel, preserve operator data, publish clear recovery instructions,
and correct forward from the private integration branch; never rewrite an
operator's configuration to disguise a failed release.

## Execution Trigger

Small patches and review findings may continue on the private WIP branch. They
do not start this plan. Execution begins only after the maintainer explicitly
says the 2.0 release candidate is ready for promotion.
