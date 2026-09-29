# Cross-Platform Release Packaging Specification

Status: `Transitional unsigned release mode approved; hosted requalification pending`

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

- Windows x86-64 before signing is configured:
  `FreqInOut-<version>-windows-x86_64-setup-unsigned.exe`
- Windows x86-64 after signing is configured:
  `FreqInOut-<version>-windows-x86_64-setup.exe`
- Linux x86-64: `FreqInOut-<version>-linux-amd64.deb`
- macOS Intel before signing is configured:
  `FreqInOut-<version>-macos-x86_64-unsigned.dmg`
- macOS Apple Silicon before signing is configured:
  `FreqInOut-<version>-macos-arm64-unsigned.dmg`
- Signed/notarized macOS packages use the same names without `-unsigned`.
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
    artifacts for three days without publishing a GitHub Release.

Hosted qualification evidence: internal candidate run `Package Candidates #1`
at commit `0b22e8c` passed input verification, the Windows x86-64 installer job,
and the Linux amd64 Debian package job. The run produced both downloadable
candidate artifacts. Candidate retention is capped at three days because these
large, repeatable files are temporary test inputs rather than public releases.

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

An annotated `v<version>` tag starts parallel Windows, Linux, macOS Intel, and
macOS Apple Silicon builds. A
single publication job waits for all platform gates, creates a draft release,
uploads every artifact and checksum, generates provenance attestations, and
publishes the draft only after the upload is complete. The job uses a protected
`production-release` environment and must confirm that the tag version matches
all application metadata and that the tagged commit belongs to public `main`.

The release job never overwrites an asset on rerun. An unexpected existing
release or asset is a failure requiring maintainer review, not permission to
replace an artifact built from an immutable tag.

### Public workflow implementation (2026-09-29)

The allowlisted `.github/workflows/public-release.yml` is the single public
entry point. A push to `release/public-*-candidate` or a manual dispatch builds
unsigned, version-named candidate artifacts and retains them for three days;
it cannot create a GitHub Release. An immutable `v<version>` tag switches to
the production path, confirms that the tag version matches all application
metadata and that its commit belongs to public `main`, and requires the
protected `production-release` environment.

Production Windows jobs require an Authenticode PFX, sign and timestamp both
the frozen executable and installer, verify both signatures, and then install,
launch, and remove the completed installer. Production macOS jobs build native
Intel and Apple Silicon bundles, require a Developer ID certificate plus App
Store Connect notary credentials, sign the app and DMG, notarize and staple the
DMG, and install and launch it from the completed image. Linux uses the same
install/launch/remove gate without a platform signing secret. Each production
package receives a GitHub provenance attestation. Publication downloads only
the production artifacts, rechecks every per-file SHA-256, creates one
`SHA256SUMS.txt`, refuses to overwrite an existing release, and promotes a
draft only after all four packages upload successfully.

The macOS bundle/DMG implementation passed a native Apple Silicon PyInstaller
build, ad-hoc code-signature verification, fresh-profile smoke test, DMG
verification, mounted-image copy/install smoke test, and profile-preservation
check. Focused packaging/export tests pass. Public GitHub Actions run
`36641946095` at public commit `4732ea13299cd4a06a6b9a139edffc4ce45bbc53`
then passed the Windows x86-64, Linux amd64, macOS Intel, and macOS Apple Silicon
candidate package jobs and retained all four version-named artifacts. That exact
commit was fast-forwarded to public `main`, where **Build and Publish Release**
is active. Credential-backed signing/notarization remains the production exit
gate; no production tag may be created until the protected environment and its
secrets are configured and those jobs pass.

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

### Maintainer-approved transitional exception (2026-09-29)

The maintainer does not yet have a Windows signing certificate, Azure
subscription, or Apple Developer Program membership and explicitly approved a
temporary unsigned-package path so automated native packages do not block the
2.0.4 release. This exception supersedes the preceding production-signing
requirement only while the signing accounts are unavailable.

Windows and macOS packages must carry `-unsigned` in the downloadable filename;
the installed product name and title remain **FreqInOut**. Release notes must
state that Windows can show **Unknown publisher** or SmartScreen and that macOS
can require **Open Anyway**. Every package still passes its completed-package
install/launch/remove or preservation checks, ships with SHA-256 verification,
and receives GitHub build-provenance attestation. Linux remains an ordinary
production package.

The workflow defaults to unsigned packages and fails closed at the protected
publication environment. Existing opt-in variables retain the credential-backed
PFX and Developer ID/notarization paths, but they remain disabled unless the
maintainer explicitly configures them. A future SignPath integration is a
separate reviewed signing adapter. Previously published unsigned assets remain
immutable; signed naming begins with a later release rather than replacing
historical files.

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
and exit-gate review. The initial RP-1 implementation used no sub-agent under
the active constraint; the later retention-policy reconciliation received an
independent read-only audit from `gpt-6-astra` (high reasoning).

The 2026-09-29 public workflow, macOS package builder, allowlist update, tests,
native macOS qualification, and integration review were all performed by the
primary `gpt-6-astra` model at high reasoning under the active no-delegation
constraint.
