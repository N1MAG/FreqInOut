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
- the root `start-freqinout.sh` and `start-freqinout.cmd` operator launchers,
  plus the prior `start-multi-rig` names as compatibility wrappers. Both neutral
  launchers use FIO's standard OS profile by default, preserve an explicit
  `FREQINOUT_CONFIG_DIR`, and accept either the `.venv` source layout or the
  Linux installer's `venv` layout without inventing a parallel multi-rig root.

It excludes at minimum:

- `tests/`, engineering `tools/`, `.github/` development workflows, `.windsurf/`,
  `AGENTS.md`, specifications, internal worklogs, and `docs/internal/`;
- benchmark, audit, fixture-generation, and developer-only scripts;
- all emulator, GUI-lab, demo-profile, capture, migration-rehearsal, and test
  helpers, including `tools/start_multirig_gui_lab.sh`,
  `tools/ts2000_cat_emulator.py`, `tools/create_readme_demo_profile.py`, and
  the remaining engineering `tools/` tree;
- operator databases, logs, screenshots, captures, rendered working files,
  machine-specific paths, credentials, or private repository instructions;
- local caches, build trees, coverage data, virtual environments, and release
  rehearsal output.

Private verification must scan both the public candidate tree and its new
public commits. An excluded file must never enter public Git history, even
temporarily.

The public export must also prove that no shipped runtime import, installer,
launcher, help link, or README link depends on an excluded tool or developer
file. If application operation requires such a file, the required runtime
logic must first be moved into the reviewed runtime package rather than
allowlisting an engineering directory wholesale.

`docs/tools-and-scripts.md` currently catalogs private engineering utilities.
It must either remain private or be replaced in the public export by a short
operator-only appendix covering only the launch, install, update, uninstall,
log, and supported repair commands that are actually shipped.

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

### Reconciliation evidence (2026-09-23)

The semantic history audit is recorded in
`docs/internal/public_1_2_8_to_2_0_reconciliation.md`. It compares public
`2c3ba1a` with audited multi-rig `630c45b`, accounts for the 34 public-only
range-diff commits, verifies the adapted managed-BBS behavior, and identifies
Windows packaged-runtime hardening as the only code gap found. The gap is
implemented privately; native Windows qualification and the final immutable
candidate/export fingerprint remain open.

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
- The installer/migration contract is now resolved: the common source installer
  validates the release, repairs an incomplete environment recoverably, checks
  and backs up an existing closed profile, installs dependencies, and writes a
  launch receipt only after an isolated smoke check. It does not finalize
  multi-rig configuration. Migration waits for explicit informed confirmation
  in **Upgrade Existing Station**. Exit or window-close causes zero
  production-profile migration writes and ends FIO; the upgrade is required on
  the next launch.
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
- Test fresh install, in-place update, interrupted/failed update, Exit/close,
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

### Private release-media capture profile

README screenshots and videos must be made from a disposable profile derived
from a matched settings/operational database pair, never from the operator's
live profile. The private `tools/create_readme_demo_profile.py` utility owns the
repeatable preparation workflow and remains outside the public runtime export.

- Source databases are opened read-only and copied through SQLite's backup API.
- Recognized amateur-radio callsign bases are obfuscated by rotating ASCII
  letters forward three positions and digits forward one position, with wrap;
  digit-prefix international callsigns are supported, Maidenhead grids are
  excluded, and portable/mobile suffixes are retained. `N1MAG` is intentionally
  public and is preserved, including its suffix variants.
- Personal names, grids, schedules, timestamps, and non-callsign message text
  remain unchanged. This is presentation obfuscation, not irreversible
  anonymization, and release captions must not claim otherwise.
- A transform that would collide with a preserved callsign fails closed.
  Database triggers are transactionally suspended and restored while the copied
  rows are rewritten, preventing the masking operation from manufacturing
  derived dirty-queue work.
- Verification compares every source text cell with its expected transformed
  copy before runtime overlays, then runs SQLite integrity checks on both copied
  databases.
- The capture profile disables launch-at-startup and unattended transmission,
  clears saved Mesh connections and live ingest paths, and points radio
  endpoints and companion launch paths at the selected RadioTools or GUI-lab
  suites. GUI-lab radios map by display order to `fio-a`, `fio-b`, and later
  suites, including the lab's intentional JS8 TCP `2242` series and save roots.
- Rebuilding an existing destination requires `--replace-output`; the previous
  profile is renamed to a timestamped sibling and retained. A failed rebuild
  removes the incomplete result and restores the previous profile. Process
  open-file inspection causes replacement to fail closed when a running process
  owns either database, without mistaking closed residual WAL files for a live
  FIO instance.

The current private capture command is:

```bash
python tools/create_readme_demo_profile.py \
  --settings-db /path/to/matched/freqinout.db \
  --nets-db /path/to/matched/freqinout_nets.db \
  --output-root /Users/Shared/FreqInOut-README-Demo-Current \
  --radio-tools /Users/Shared/RadioTools-Demo \
  --gui-lab-root /Users/bill/RadioCode/WORK/MultiRig/TestLab \
  --replace-output
```

The first build omits `--replace-output`. Additional public callsigns may be
retained with repeated `--preserve-callsign` arguments. Release review must
still inspect every captured frame for private message content, names, paths,
access codes, and secrets before publication.

The maintainer supplied a separate production screenshot set for the public
README on 2026-09-23. The reviewed frames cover Ops Center, two-radio profile
readiness, per-radio Launch Control, Plan Builder, multi-transport Compose, and
the offline operational map. The public asset copies use neutral stable names
below `docs/images/readme-2.0/`. No non-public callsigns, local filesystem
paths, access codes, or message bodies are visible; the public project identity
`N1MAG` remains intentionally visible where present. Final release review must
repeat that visual check against the exact exported image hashes.

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
- FIO 2.0 supports Python 3.10 through 3.13. Python 3.9 compatibility work from
  the single-rig history is intentionally superseded, allowing the same
  official MeshCore client on every supported interpreter.

## Gate PR-5 — Install, Update, Uninstall, And Packaging

- Change every installer default and example from the private WIP repository
  and opaque WIP branch to the canonical public repository and release channel.
- The Linux installer now names the canonical promotion target explicitly:
  `https://github.com/N1MAG/FreqInOut.git` on `main`. Private-preview testing
  must pass its private repository and branch explicitly.
- The source launchers are now cross-platform (`.sh` and `.cmd`) and share the
  same standard-profile/explicit-override contract. The Linux uninstaller
  removes every icon size and pixmap installed by its paired installer.
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
- The private PyInstaller candidate must sanitize inherited Python/Qt runtime
  paths, use bundled QML/plugins, disable UPX, default Windows Qt/Chromium to
  software rendering, tolerate a windowed process without stdout, expose the
  hidden bounded `--smoke-test`, and retain `startup-error.log` after fatal
  startup failure. Focused cross-platform tests do not replace the native
  Windows package gate.
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

### Private readiness evidence (2026-09-23)

The private candidate now has a repeatable, fail-closed public projection tool:
`tools/build_public_runtime_export.py`. It copies only individually allowlisted
runtime files, substitutes the reviewed public 2.0 README draft, excludes the
entire engineering tools/tests/internal-docs/lab surface, narrows bundled
JS8Net content to its runtime module and license, and rejects private repository
markers or named-user filesystem paths. The current projection contains **408
files** and validates successfully. From that projected tree, a fresh temporary
profile completed the packaged `--smoke-test` startup and clean shutdown on
macOS.

The release audit also closed the six stale private-test modules previously
listed as blockers. The corrected modules pass **42 tests** together. An
isolated sweep accounted for all **378 private test modules**: 374 module files
pass, two are intentional skip-only modules, and two real-widget modules expose
the documented PySide 6.8/macOS cross-test teardown fault. All **32 individual
test nodes** in those two modules pass and exit normally in fresh processes,
so no unsupported production lifecycle change was made.

Focused final checks pass: release preflight; Linux installer, launcher, and
uninstaller shell syntax; changed Python compilation; diff whitespace; and 55
help, packaging, export, portability, and managed-BBS tests. The public
installation documents now target `N1MAG/FreqInOut`, describe the explicit
in-app upgrade/backup review, and contain no private-preview path. Runtime
fallback discovery uses only the current operator's home and saved application
roots; named local accounts are absent from the projected tree.

Work-package ownership for this readiness slice:

- Primary `gpt-6-astra` owned release architecture, persistence and portability
  safety, integration, delegated-diff review, public projection validation, and
  exit-gate decisions. The runtime did not expose a trustworthy reasoning-effort
  label, so none is invented.
- `release_projection_contract`: `gpt-5.6-luna`, low reasoning, bounded
  projection-contract test review.
- `release_stale_fixtures`: `gpt-5.6-terra`, medium reasoning, bounded stale
  fixture review and correction.
- `release_public_audit`: `gpt-5.6-luna`, medium reasoning, read-only public
  runtime/documentation boundary audit.

At the time of this evidence, the gate still included native packaging,
clean-host interpreter, operator-upgrade, and soak qualifications. The
maintainer's subsequent bounded risk decisions are recorded below. The exact
immutable candidate, export fingerprint, and final public diff/inventory review
remain mandatory. No public push, tag, or release is authorized by this evidence
alone.

### Maintainer-accepted release qualifications (2026-09-24)

The maintainer accepted the following bounded residual risks so they do not
block the source-only 2.0 promotion:

- No Windows executable or signed application bundle will be published for this
  release. Native executable/installer qualification is deferred until such an
  artifact is actually offered.
- Python 3.11 is the tested and recommended release interpreter. The declared
  3.10 through 3.13 range remains accepted by metadata, but 3.10 clean-host
  qualification is deferred and is not represented as completed evidence.
- Stable local macOS operation is accepted as the maintainer's source-runtime
  qualification; the macOS/PySide shared-process test-harness limitation remains
  documented separately.
- The production-shaped 1.2.8 operator migration rehearsal is waived as a
  release prerequisite. Automated migration/backup/idempotency/rollback coverage
  remains required, the `v1.2.8` tag remains recoverable, and public instructions
  require a verified backup plus review or reconstruction of affected companion
  settings when needed.

These are explicit scope/risk decisions, not silently passed tests. They do not
waive final source projection, smoke, privacy, inventory, version, or public-diff
review.

### Frozen local candidates (2026-09-24)

- Immutable private source candidate:
  `43b869f563333a89304f69634c5f9a4c4d4114b2`
- Local public promotion candidate based directly on public `v1.2.8`/`main`:
  `2f03e5f72c8d5dc27d0666b8444849d282079809`
- Public candidate Git tree:
  `dda03e10aa5d728a93ade49e91f6fecafabc4e7d`
- Allowlisted runtime inventory: 408 files, recorded in
  `docs/internal/public_2_0_runtime_manifest.sha256`
- Inventory-manifest SHA-256:
  `c67241bd2c0367c8dfa869fd0f883221f29ba8f2d863b862d5b7833415ff1b19`

The local branch is `release/public-2.0-candidate` and is deliberately not
pushed. Both candidate worktrees were clean after their commits. The projected
tree contains no tests, engineering tools, internal documentation, packaging
helpers, emulator/lab code, caches, or private repository markers. Its only
non-third-party removals relative to public 1.2.8 are `CODE_OF_CONDUCT.md`,
`CONTRIBUTING.md`, the propagation developer README, generated Linux installer
HTML, the obsolete executable-installer definition, and `start-single-rig.sh`;
the latter is replaced by the reviewed POSIX and Windows multi-rig launchers.

The committed public candidate exactly matches the recorded inventory
fingerprint. From that tree, shell syntax and diff hygiene pass, local Markdown
links have zero missing targets, a fresh temporary profile starts and shuts
down cleanly with `--smoke-test`, and 38 private help/runtime/upgrade tests pass
against a qualification copy containing the exact projected application code.
No `v2.0.0` tag, remote promotion branch, merge, or public push exists yet.

### Public promotion completion (2026-09-24)

After explicit maintainer review and approval, the exact public candidate was
promoted atomically. Remote `main` and
`release/public-2.0-candidate` now both resolve to
`2f03e5f72c8d5dc27d0666b8444849d282079809`. The annotated `v2.0.0` tag
dereferences to that same commit. Public `v1.2.8` remains preserved at
`2c3ba1af81d7d1ac319637a8eb77dae21254564c` for recovery and archived
single-radio installations.

No executable, installer binary, or signed application bundle was published.
The promotion branch remains as an audit trail. Remote-ref verification and the
local candidate worktree cleanliness check passed immediately after promotion.

### FreqInOut 2.0.1 hotfix inclusion audit (2026-09-28)

The public `release/public-2.0.1-candidate` branch currently resolves to
`47f030839899d8afd6e24abf5ff6eac3eed37c53`, directly on public 2.0.0
`2f03e5f72c8d5dc27d0666b8444849d282079809`. Its tree exactly matches the
411-file allowlisted runtime export from private commit
`f9ab0e6d3d6a34684b9d3d6bc8bcd273a1300bd1`; no unexplained candidate drift
was found. Both trees declare version `2.0.1`.

The hotfix inclusion matrix is:

| Hotfix | Private implementation | Current 2.0.1 candidate | Approval state |
| --- | --- | --- | --- |
| BBS automation restoration, Radio Service initialization, and 1.2.8 JS8 schema ordering | `8ea27181f42a24eb5433bfebf7d2c77f58bf688b` | Included | Awaiting maintainer pass approval |
| Canonical Mesh Inbox projection | `9f022118a6f38cd9abc83db0bbc5d7d0f85a6de5` | Missing | Approved—queued for next point release |
| Guarded Local Mesh outbound Compose | `3501e021b1d03d8b2f637cf0591a1e27ddd4f22b` | Missing | Awaiting maintainer pass approval |
| Verified common installer and mandatory existing-station upgrade gate | `865bd61c8be46f6eb89943bc7e22df0a636769a0` | Missing | Awaiting maintainer pass approval |
| Visible saved-device `Allow Send` permission | `fd2b543b92d2948bc23f9beee2097a3119f7895d` | Missing | Awaiting maintainer pass approval |
| MeshCore BLE outbound, saved-device/schema repair, and dependency receipt enforcement | `53238486741d467abfec5c7691b4baae2810d00c` | Missing | Awaiting maintainer pass approval |

Private tracking-only commits such as the Inbox approval record and installer
tracking update remain excluded by the public runtime allowlist. The current
private WIP export contains 413 allowlisted files; the two additional files are
the neutral `start-freqinout.sh` and `start-freqinout.cmd` launchers introduced
by the installer hotfix. The remaining candidate differences are the reviewed
runtime, public documentation, manifest, and compatibility-launcher changes
owned by the five post-candidate hotfixes above.

The intended 2.0.1 release scope includes all six rows. This scope decision is
not maintainer pass approval. Before rebuilding the public candidate, every
row still marked awaiting approval must pass its work-log operator gate and be
changed to `Approved—queued for next point release`. The next candidate must be
regenerated from one reviewed private WIP commit through the allowlisted export,
replace the existing candidate tree in one auditable commit, retain version
`2.0.1`, and pass public-export, install/upgrade, Mesh Inbox/send, BBS,
documentation, smoke, and clean-tree checks. Public `main`, tags, and release
artifacts remain unchanged until the maintainer separately authorizes final
promotion.

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
