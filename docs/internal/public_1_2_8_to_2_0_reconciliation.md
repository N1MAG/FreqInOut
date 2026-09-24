# Public FIO 1.2.8 To Multi-Rig 2.0 Reconciliation

Date: 2026-09-23  
Status: private semantic audit complete; native Windows and supported-platform
release qualification remain open.

## Compared Baselines

- Common ancestor: `e91deaa8f116c3ea56d9e25f581632b199074a44`
- Public single-rig 1.2.8: `2c3ba1af81d7d1ac319637a8eb77dae21254564c`
- Audited multi-rig head: `630c45bfa73dc603b5c8630a944f9798fa551afe`
- Multi-rig 1.2.8 alignment checkpoint: `b8a71c6`

Public history contains 98 commits after the common ancestor and the audited
multi-rig history contains 577. `git cherry` found 15 patch-equivalent public
commits. A high-creation-factor `git range-diff` paired 64 public commits (13
exact and 51 semantically adapted) and left 34 public-only commits requiring
explicit disposition. No public runtime module is absent from multi-rig: all
62 shared runtime paths remain, and multi-rig adds 250 runtime modules.

## Public-Only Disposition

The 34 public-only commits from `9ce4522` through `2c3ba1a` are accounted for as
follows:

| Public intent | Current disposition |
|---|---|
| Qt-thread scheduler callback marshaling (`9ce4522`) | Present through `6aa16e9` and current scheduler/UI callback boundaries. |
| UI hang watchdog (`fa1693f`) | Present through `2161843` and current diagnostics. |
| Python 3.9 compatibility (`d1c55fa`, `a0e2fb5`) | Superseded intentionally: FIO 2.0 requires Python 3.10-3.13. |
| Demand-driven FLDigi polling (`3c288ec`) | Present through `563c775`. |
| Shared process/status inventory (`b1d9807`, `af08aca`, `f676669`) | Superseded by current radio-scoped process inventory and shared polling services, including `c5d26da`. |
| Bounded JS8/vault work (`fe48366`) | Present through `45cb83f` and current bounded source jobs. |
| Scheduler thread lifecycle (`c48dd80`) | Superseded by current scheduler worker lifecycle and callback marshaling. |
| Startup splash (`3e5489b`) | Present through `09d581a`. |
| Station-health cleanup (`2c3ba1a`) | Present in current station-health work, including `f040f18`. |
| ControlFreq net-mode/summary polish (`676bf52`, `489440d`) | Present in later UI work including `8d0a45a` and `95fd5c4`. |
| JS8Spotter fields, JS8 offset status, CommStat delete (`60efed6`, `c11c748`, `db698d5`) | Present in the current radio-scoped ingest, scheduler status, and CommStat deletion implementations and focused tests. |
| `urllib3<2` Python 3.9/LibreSSL workaround (`52256a1`) | Superseded by the Python 3.10 floor and current resolved dependency set. |
| Release-only, help/layout, and public test-removal commits | Reconciled into the 2.0 public README/help/export plan; development tests remain private by design. |

The 13 managed-BBS/VarAC/FLAMP behavior changes that were structurally adapted
rather than safely cherry-picked were separately exercised against the current
station-managed catalog, message helper filtering, signing, vault event parser,
BLR/FLAMP command state machine, and radio-scoped BBS configuration.

## Corrections Applied By This Audit

- The supported interpreter contract is now Python 3.10-3.13 in project
  metadata, both installers, requirements, user documentation, Mesh
  specification, and the lock file.
- The PyInstaller runtime hook removes inherited host Python and Qt variables,
  then points QML/plugin discovery at bundled resources. Windows defaults to
  software Qt/Chromium rendering unless the operator explicitly overrides the
  frozen-runtime isolation contract.
- UPX is disabled for the packaged executable and collected application.
- Windowed startup no longer assumes `sys.stdout` exists. The packaged app has
  a bounded `--smoke-test` path and persists fatal startup tracebacks to
  `startup-error.log`.
- A repeatable test now rehearses a public-1.2.8-shaped profile through preview,
  backup, explicit migration, idempotent rerun, and rollback.

## Acceptance Evidence

- BBS/VarAC/FLAMP parity: 156 passed, 1 expected skip, 0 failures.
- Packaging, interpreter, installer, and migration focus: 240 passed, 0
  failures. Release preflight, compilation, shell syntax, lock
  consistency, and whitespace validation also pass.
- The isolated public-profile rehearsal proves legacy endpoints and linked app
  identities are retained, launch is disabled for safe review, repeat apply is
  a no-op, and rollback restores the pre-migration marker/state.
- The six stale private assertion modules identified by the first audit are
  reconciled and pass 42 tests together. An isolated sweep accounted for all
  378 private test modules: 374 pass, two are intentional skip-only modules,
  and two macOS/PySide real-widget modules have a file-level teardown crash.
  Every one of their 32 test nodes passes in a fresh process.
- The allowlisted public projection contains 408 validated runtime files. A
  fresh temporary profile launched and shut down cleanly from that projected
  tree with `--smoke-test`; 55 focused help, packaging, export, portability,
  and BBS checks also pass.

## External Qualification Review

- Native Windows executable/installer qualification was reviewed and deferred
  because no such 2.0 artifact will be published.
- Clean-host Python 3.10 qualification was reviewed and deferred; Python 3.11
  is the tested and recommended source-release interpreter.
- The final production-shaped public-1.2.8 operator migration was reviewed and
  waived in favor of the automated rehearsal plus mandatory backup and rollback
  guidance.
- Stable local macOS use is accepted as source-runtime qualification. The two
  real-widget Qt modules already pass all 32 nodes under the approved
  fresh-process policy; no production lifecycle change is justified by the
  separate PySide 6.8 harness-only teardown evidence.
- Record the immutable candidate commit and export fingerprint, run the private
  harness against that exact public projection, and approve its final public
  diff and inventory before any push or tag.

## Maintainer Risk Acceptance (2026-09-24)

The native executable gate is deferred because 2.0 will be published as a
source release only. Local macOS operation is accepted as the source-runtime
qualification, and Python 3.11 is the tested/recommended interpreter; local
Python 3.10 installation is not required for this promotion. The final
production-shaped 1.2.8 operator migration is also waived as a prerequisite in
exchange for prominent backup, explicit confirmation, rollback, and possible
companion-configuration reconstruction guidance. The automated upgrade
rehearsal and preserved public `v1.2.8` tag remain required safeguards.

The remaining promotion work is therefore the immutable private candidate,
exact allowlisted source projection, projected-tree smoke and private harness,
and maintainer review of the final public diff and inventory.

Those local artifacts were subsequently frozen as private candidate
`43b869f563333a89304f69634c5f9a4c4d4114b2` and public candidate
`2f03e5f72c8d5dc27d0666b8444849d282079809`, with public Git tree
`dda03e10aa5d728a93ade49e91f6fecafabc4e7d`. The 408-file inventory manifest
hash is `c67241bd2c0367c8dfa869fd0f883221f29ba8f2d863b862d5b7833415ff1b19`.
The exact projected source passed clean startup/shutdown and 38 focused private
harness tests. Final maintainer approval and all remote push/merge/tag actions
remain outstanding.

The maintainer subsequently approved the exact candidate. On 2026-09-24,
public `main`, `release/public-2.0-candidate`, and the dereferenced annotated
`v2.0.0` tag were verified at
`2f03e5f72c8d5dc27d0666b8444849d282079809`. The prior `v1.2.8` tag remains
unchanged at `2c3ba1af81d7d1ac319637a8eb77dae21254564c`.
