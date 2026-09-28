# Cross-Platform Release Packaging Specification

Status: `Slice RP-1 candidate builders in progress`

Governing contracts:

- `project_delivery_rules.md`
- `public_2_0_runtime_release_plan.md`
- `release-checklist.md`

## Objective

FreqInOut releases must be trustworthy, repeatable, and simple for a
non-technical operator to install or update. Windows and Linux x86-64 are the
first production package targets. Raspberry Pi ARM64 and macOS follow only
after the first two targets use the same proven release contract.

The immutable source tag is the authority for every package. A public release
is published only after all required platform jobs build, install, and launch
their completed package successfully. Candidate builds never create a GitHub
Release and therefore do not increase the user-visible release count.

## Release artifacts

- Windows x86-64: `FreqInOut-<version>-windows-x86_64-setup.exe`
- Linux x86-64: `FreqInOut-<version>-linux-amd64.deb`
- Per-artifact SHA-256 files during candidate qualification.
- One combined `SHA256SUMS.txt` for a public release.
- Resolved dependency inventories retained with candidate evidence.
- GitHub build-provenance attestations for public binary releases.

## RP-1 — Internal candidate builders

The internal WIP repository owns a non-publishing candidate workflow. It runs
manually and when packaging inputs change on the WIP branch.

For both Windows and Linux it must:

1. check out one exact commit;
2. use Python 3.11 and explicitly pinned build tools;
3. validate version metadata and required packaging inputs;
4. compile the runtime package;
5. build a PyInstaller one-folder application;
6. run that unpacked application with `--smoke-test` and a fresh profile;
7. build the native installer/package;
8. install the completed artifact into the runner;
9. run the installed application with a separate fresh profile;
10. uninstall it while proving that operator profile data remains; and
11. retain the installer, checksum, and dependency inventory as Actions
    artifacts without publishing a GitHub Release.

Exit gate: both GitHub-hosted jobs pass, their artifacts can be downloaded, and
one real Windows and one supported Linux system complete fresh-install checks.

## RP-2 — Packaged upgrade safety

The source installer currently creates and verifies the mandatory station
backup, while a frozen executable bypasses that source installer. Before a
binary package may be public, the packaged runtime must reuse the same
read-only database validation, verified backup, running-instance refusal, and
Windows backup-name fallback on the first launch of each installed version.

The gate must cover a fresh profile, an existing 2.0 profile, corrupt database
refusal, insufficient-space refusal, retry after interruption, idempotent
relaunch, and preservation of the actual verified-backup path. Package removal
must never remove the station profile.

## RP-3 — Reproducible dependency contract

Candidate jobs record their complete resolved dependency sets. After the first
successful Windows and Linux candidates, reviewed platform constraint files
pin the complete tested build resolution. Subsequent candidates and releases
install through those constraints. Updating a dependency is a reviewed change
that reruns both package jobs; normal application releases do not silently
float to newly published Python packages.

The PyInstaller version and hooks package are pinned from the first candidate.
GitHub-maintained actions are referenced by immutable commit SHA with the
human-readable release version recorded in a comment.

## RP-4 — Public release workflow

The reviewed public runtime allowlist includes only the build inputs and public
release workflow needed to reproduce a package. It continues to exclude private
tests, specifications, work logs, lab tools, evidence, and private history.

An annotated `v<version>` tag starts parallel Windows and Linux builds. A
single publication job waits for all platform gates, creates a draft release,
uploads every artifact and checksum, generates provenance attestations, and
publishes the draft only after the upload is complete. The job uses a protected
`production-release` environment and must confirm that the tag version matches
all application metadata and that the tagged commit belongs to public `main`.

The release job never overwrites an asset on rerun. An unexpected existing
release or asset is a failure requiring maintainer review, not permission to
replace an artifact built from an immutable tag.

## RP-5 — Signing

Unsigned artifacts are permitted only for internal candidate testing. A
production Windows installer requires Authenticode signing of both the
application executable and final installer, with timestamp verification. The
signing credential is stored only in the protected release environment.

Linux direct-download packages initially use SHA-256 and GitHub provenance. A
separately signed APT repository is out of scope unless FIO later operates a
package feed.

macOS production packages require Developer ID signing and notarization; an
unsigned DMG is not a supported non-technical-user release.

## RP-6 — Later platforms

Raspberry Pi uses an ARM64 Linux build on a GitHub-hosted ARM runner followed by
qualification on physical Raspberry Pi OS Bookworm hardware. Passing on an
Ubuntu ARM runner is necessary but not sufficient.

macOS builds separately on Apple Silicon and Intel runners because PyInstaller
outputs are operating-system and architecture specific. Both architectures
must pass signed-installed-app launch checks before the DMGs are public.

## Release cadence and rollback

- Package-candidate runs are not releases and may be repeated freely.
- A public version is created only after its runtime fixes are approved and all
  package gates pass against the same immutable commit.
- A failed package job creates no public release.
- A failed field qualification blocks publication; it does not trigger a
  corrective public version.
- Published tags and assets are immutable. Corrections use the next version.
- The previous public release and its checksums remain available for rollback.

## Work-package ownership

Primary `gpt-6-astra` (high reasoning) owns release architecture, packaged
upgrade safety, build scripts, workflow security, integration, specification,
and exit-gate review. No sub-agent was used under the active agent constraint.
