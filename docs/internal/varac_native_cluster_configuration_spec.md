# VarAC Native Cluster Configuration Specification

Status: authoritative implementation specification; approved direction
2026-09-17. Automated implementation gate passed; live Windows and Linux/Wine
operator qualification remains open.

Governing contracts:

- `project_delivery_rules.md`
- `multirig_product_ui_contract.md`
- `task_oriented_workspace_design_guideline.md`
- `ui_layout_standards.md`
- `guided_radio_software_configuration_spec.md`
- `multi_instance_software_administration_spec.md`
- `multi_endpoint_scheduler_concurrency_spec.md`

Primary external contract reviewed 2026-09-17:

- VarAC, **VarAC Cluster (Multiple instances) - Quick configuration guide**,
  https://www.varac-hamradio.com/post/varac-cluster-multiple-instances-quick-configuration-guide
- VarAC release history, including custom configuration-file launch support,
  https://www.varac-hamradio.com/varac-releases

This specification owns creation of native VarAC cluster-instance
configuration by FIO. It replaces older statements that all VarAC cluster
membership is read-only. The read/import-only boundary remains authoritative
for an unknown VarAC version, unsupported layout, ambiguous source, running
application that cannot be stopped safely, or any plan that fails preview,
backup, apply, or readback qualification.

## Observable Operator Outcome

From `Settings -> Radios -> Add Radio`, an operator can add a transceiver,
select a new VarAC instance, explicitly choose a cluster arrangement, prepare
the software, review what will change, and save a usable FIO launch identity
plus native VarAC instance configuration without manually constructing an INI
file.

For the production-shaped route with one existing standalone node and no
cluster, the observable reproduction statement is:

`Settings -> Radios -> Add Radio -> Transceiver -> VarAC -> Create a new
FIO-managed instance -> Create a cluster with <existing radio> and <new radio>
-> Prepare selected software automatically -> Review & Save`

Before final confirmation, neither the existing INI, new INI, shared database,
VARA profile, FIO cluster record, nor radio assignment changes. After a
successful confirmation, both native member configurations read back exactly as
reviewed, the new launch command selects its own INI, FIO records the same
membership, and recovery evidence is retained. Cancel or any failure restores
the exact prior external and FIO state.

## Required Workspace Redesign Brief

- **Primary operator task:** Create and verify a native VarAC cluster member for
  the selected transceiver.
- **Starting context:** The operator has named a transceiver, selected VarAC,
  chosen `Create a new FIO-managed instance`, and can see a bounded snapshot of
  existing usable VarAC nodes and clusters.
- **Completion outcome:** FIO has a verified launch identity and native INI for
  the new node, a reviewed shared-database arrangement, distinct VARA resources,
  synchronized native/FIO membership, and a recoverable backup manifest.
- **Task sequence:** Choose arrangement -> Prepare automatically -> review
  concise node/cluster cards -> resolve only unsupported or ambiguous items ->
  preview changes -> confirm Save -> staged apply/readback -> publish result.
- **Primary action:** `Review & Save`; while native work is active it is disabled
  and labeled by stable progress, never repeatedly reflowed.
- **Essential state and Why:** affected existing/new radios, shared database,
  member numbers, PTT-lock policy, email-gateway sender, readiness, backup
  coverage, and any reason automatic application is unavailable remain visible.
- **Secondary and advanced work:** exact paths, ports, commands, fingerprints,
  native keys, and textual diff are collapsed under `Show details`; custom
  values are available only after automatic preparation.
- **Workspace archetype:** Guided workflow, because this is a staged external
  configuration change with review and recovery.
- **Responsive behavior:** one vertical scroll owner; wide views may pair the
  new-node and shared-cluster summaries, medium/compact views stack them in task
  order; the review/commit footer remains reachable at 900x560 and Large Text;
  no page-level horizontal scroll.
- **Shared theme/components:** existing Settings step chips, semantic status
  banners, button roles, combo sizing, font-derived control sizing, scroll-area
  helpers, and focus/disabled treatments from `freqinout/gui/theme.py`; no local
  palette or fixed text-bearing height.
- **Performance boundary:** rendering and editing use one immutable prepared
  snapshot. Discovery, hashing, filesystem inspection, backup, copy, SQLite
  backup, write, process checks, and readback run only after explicit actions on
  bounded workers and are generation-fenced from stale navigation.

The surface supports **What** (create this native instance and cluster
membership) and **Why** (topology recommendation, safety policy, and readiness).
The Station Control Bar remains the owner of current Where/When context.

## Native VarAC Contract

### One installation, distinct instance identities

One VarAC executable installation may serve multiple processes. Each local
member requires a separate INI and an effective command equivalent to:

`VarAC.exe <member-configuration.ini>`

On Wine, the executable, selected INI argument, Wine prefix/environment, and
working directory together form the launch identity. FIO never assumes that an
INI outside the VarAC-visible filesystem can be opened. The prepared plan must
prove the exact command on the selected platform or remain `Needs attention`.
Host paths under a Wine `drive_<letter>` are written using that Windows drive;
other managed host paths use Wine's `Z:` root mapping. FIO retains the host path
separately for its own file and database access. The native VarAC INI argument,
`DBCustomFilePath`, and VARA executable paths therefore never receive an
untranslated Linux path.

### Controlled native keys

FIO writes only a version-qualified allowlist. For cluster membership the
required `[VARAC_CLUSTER]` projection is:

- `ClusterEnabled=ON`
- `InstanceNumber=<unique positive member number>`
- `CountersRefreshRateSec=<5..600>`
- `PTTLock=ON|OFF`
- `ClusterEmailGatewaySenderNode=ON|OFF`

The last field means **Email gateway sender node**. It is not a general gateway
or routing leader. Exactly one enabled member may be selected when the station
uses VarAC email gateway relay; zero is valid only when email gateway relay is
not enabled for the cluster.

FIO also controls, when present in the qualified template:

- `[OTHER] DBCustomFilePath` for the one effective shared `VarAC.db`;
- the real `[VARAHF_CONFIG]` fields `VarahfMainPath`, `VarahfMainPort`,
  `VarahfMainHost`, `VarahfEnableKissInterface`, `VarahfMainKissPort`,
  `VarahfMonitorPath`, `VarahfMonitorPort`, and
  `VarahfLaunchOnModemConnect` when each field is present in the qualified
  source layout;
- the radio-control fields already resolved from the selected FIO transceiver;
- instance-owned incoming and custom-file paths that must not collide; and
- explicit receive/transmit safety fields included in the reviewed capability
  plan.

Unknown sections and keys, comments, ordering, newline convention, and encoding
are preserved byte-semantically except for the reviewed keys. FIO never emits a
complete INI from an undocumented blank schema.

### FIO-only identity

FIO's human cluster name and normalized cluster ID are durable FIO metadata.
Native VarAC does not expose an equivalent cluster-ID key in the qualified
contract. Native membership is represented by enabled cluster settings, the
shared database, unique member numbers, and compatible per-node resources.

### Shared database semantics

A native VarAC cluster uses one effective shared `VarAC.db`. It is not an
optional second database beside a member-local operational VarAC database.

FIO therefore records:

- `varac_clusters.shared_db_path` as required for a native-managed cluster;
- each managed cluster member's effective database reference as that same
  normalized path; and
- one non-exclusive resource claim owned by the cluster, rather than duplicate
  exclusive node-local database claims.

Standalone and operator-managed nodes retain their current `db_path` behavior.
An additive migration may add explicit native-management/apply-state columns,
but it does not rewrite any existing row, infer membership, relocate a database,
or change an existing path.

When converting an existing standalone node:

- default to its current healthy database as the proposed shared database when
  that preserves existing data and passes path/readability checks;
- never copy an open SQLite database with a raw filesystem copy;
- if relocation is explicitly reviewed, require affected VarAC processes to be
  stopped and use SQLite's backup API with integrity verification; and
- retain the original database and INI backups until operator-confirmed
  recovery cleanup exists as a separate operation.

## VARA Instance Contract

Every simultaneously running member requires its own VARA HF folder/profile and
non-conflicting command, data, KISS, and monitor ports. FIO may reuse an
installed VARA binary as source evidence, but it must create or select a
distinct runtime configuration for the new member. In the qualified VARA HF
layout, `VARA.ini` stores `[Setup] TCP Command Port`, `Enable KISS`, and
`KISS Port`; the data port is the command port plus one. `[Monitor] Monitor
Mode` is preserved or changed only when the reviewed plan includes a qualified
monitor action. FIO must not invent a `VARADataPort` field in `VarAC.ini`.

Automatic preparation allocates ports from the immutable station inventory and
checks configured claims plus an explicit bounded availability probe during
Prepare/Save. A port becoming occupied after Review is a stale-plan failure,
not permission to renumber silently. Existing member ports and folders remain
unchanged unless their exact update appears in Review.

If the installed VARA version/layout is not qualified for safe cloning, the
plan states one operator action and does not claim the native cluster is ready.

## Operator Decisions And Derived Values

The normal operator decides only:

- the cluster arrangement and, when multiple candidates exist, which existing
  standalone node participates;
- whether cluster PTT lock is enabled;
- which enabled member is the email-gateway sender, or `No email gateway`;
- whether FIO launches the new instance automatically; and
- whether to accept FIO's proposed existing database or deliberately choose a
  new shared location.

FIO derives and presents as read-only facts unless Advanced is opened:

- cluster display name and internal ID;
- existing/new member numbers;
- new INI filename and target;
- effective shared database path;
- new VARA runtime folder and ports;
- launch command, working directory, environment, and dependencies;
- incoming/outbox/custom paths; and
- backup, validation, and readback targets.

For one usable existing standalone node, default member numbers are existing
`1` and new `2` unless either conflicts with reviewed evidence. With multiple
standalone nodes the operator must choose one before preparation. A mutating
cluster arrangement is never selected merely because it is recommended.

## Version Qualification

Native writes require an exact writer capability keyed by:

- VarAC family and detected version;
- platform (`windows`, `linux-wine`, or qualified macOS/Wine route);
- operation (`create-member`, `convert-standalone`, or `update-member`);
- source INI layout fingerprint/required-key contract; and
- VARA layout/version capability where FIO creates its runtime profile.

The registry must not use a wildcard version. Unknown or newer versions remain
read/import/operator-managed until fixtures and readback tests qualify them.
The review card states the detected version and writer status without exposing
raw fingerprints by default.

The initial production capability is VarAC `13.2.7`, qualified against the
operator's exact executable-version evidence and adjacent real `VarAC.ini` and
`VARA.ini` layouts. A public release announcement can identify a newer version,
but it does not qualify that writer by itself. VarAC `15.0.18` therefore remains
read/import/operator-managed until its exact configuration fixtures pass this
contract suite.

## Prepared Plan

The immutable plan includes:

- generation and complete source fingerprints;
- affected existing and new member identities;
- every source, target, backup, temporary, and recovery path;
- expected pre-write digest/existence for every external target;
- allowed native key changes and expected post-write semantic values;
- shared database strategy (`reuse-existing` or explicitly reviewed
  `sqlite-backup-relocate`);
- VARA copy/profile and unique port allocation;
- launch components and exact readiness checks;
- FIO schema/store mutations;
- PTT lock and email-gateway sender policy;
- stop requirements and running-process evidence; and
- a bounded human preview plus a detailed machine/audit representation.

Changing the radio, source, arrangement, shared database, member numbers,
gateway choice, PTT policy, application version/path, or relevant inventory
invalidates the plan and requires Prepare again.

The plan fingerprint covers all of those decisions, every controlled native
value, launch argv/environment, pre-write target state, and the complete
bounded VARA runtime snapshot. A presentation whose draft fingerprint no longer
matches the visible choices is immediately `Needs attention`; final apply also
rechecks the generation and fingerprint before starting a worker.

## External Apply And Recovery Transaction

SQLite cannot atomically commit third-party files and FIO database rows in one
primitive. FIO uses a durable staged transaction with idempotent recovery:

1. **Preflight:** revalidate generation, source digests, paths, permissions,
   ports, member numbers, process state, and free space. No writes occur.
2. **Journal:** persist a pending native-apply record containing the plan
   fingerprint, state, targets, and recovery manifest before touching external
   files.
3. **Backup:** back up every existing target. Missing targets are recorded so
   rollback removes only files/directories created by this plan.
4. **Stage:** create directories and temporary files on the target filesystem;
   use SQLite backup for a reviewed database relocation.
5. **Validate staged state:** parse/read back every staged INI, verify controlled
   keys, uniqueness, shared database, launch identity, and VARA claims.
6. **Promote:** atomically replace individual files with `os.replace` on the
   same filesystem. Directory promotion uses a versioned managed directory and
   a final manifest pointer; it never replaces an arbitrary operator folder.
7. **Validate promoted state:** hash and semantically reread all targets before
   any FIO radio/cluster assignment becomes active.
8. **Persist FIO state:** in one FIO transaction save cluster metadata,
   memberships, node/manifest, launch bundle, radio link, and verified apply
   evidence.
9. **Complete journal:** record success only after both native readback and FIO
   commit succeed.

If stages 3-8 fail, restore exact backups, remove exact newly-created targets,
verify restoration, and leave FIO membership inactive/uncommitted. If automatic
restoration is incomplete, mark the journal `recovery_required`, block launch of
affected instances, retain backup paths, and route the operator to Health.
Closing Add/Edit Radio, removing the prepared VarAC draft, a stale outer review,
or failure of another reviewed software-family apply also compensates an
already-completed native external phase immediately; startup recovery is a
crash fallback, not the normal Cancel implementation.

At startup, bounded recovery scans only unfinished native-apply journal rows.
It performs no automatic forward mutation. A fully promoted-and-verified plan
whose FIO commit is demonstrably complete may be marked complete; every other
state is restored or presented for explicit recovery according to recorded
evidence. The UI thread never performs this work.

## Process And RF Safety

- FIO never edits an INI belonging to a running VarAC instance.
- The normal route asks the operator to close affected VarAC/VARA processes;
  automatic process termination is a separate explicit confirmation and is not
  required for this slice.
- Configuration verification does not key PTT, tune, QSY, beacon, broadcast,
  or connect.
- Native `PTTLock` is cluster coordination, not proof of RF safety and not a
  substitute for FIO RF Guard.
- A new cluster member is saved inactive when native readiness is unknown.
- Launching the member remains distinct from transmit authorization.

## UI Presentation

The concise prepared VarAC card shows:

- `New native VarAC instance`;
- arrangement and affected radio names;
- `Shared VarAC database`;
- member numbers;
- `PTT lock`;
- `Email gateway sender`;
- launch policy; and
- `Ready`, `Needs attention`, `Stop VarAC to continue`, `Manual setup
  required`, or `Recovery required`.

`Show details` reveals paths, commands, ports, key changes, fingerprints,
backup coverage, and readback evidence. Required warnings and the reason Save is
disabled remain visible without opening details. The old generic label
`gateway handler` is not used for this native setting.

Prepare and apply work use bounded workers. Results publish only when the
navigation and plan generations remain current. Cancel/Back remains responsive
before apply; during promotion the modal progress state prevents duplicate
commit but does not pump nested event loops or rebuild the workspace.
The native preparation card appears only for a new FIO-managed Create/Join
cluster route. If a process check requires VarAC/VARA to close, the card retains
an explicit `Retry after closing VarAC` action.

## Schema And Compatibility

All schema changes are additive. Existing databases open without rewriting
cluster topology or external files.

Required durable concepts may be stored as explicit columns or a dedicated
table, but must include:

- native-management state and exact writer capability;
- plan/desired/observed fingerprints;
- email-gateway sender device identity with compatibility read of the legacy
  `gateway_handler_device_id` column;
- effective shared database ownership;
- source/target INI and VARA identities;
- pending apply/recovery journal and backup manifest; and
- last verified timestamp and summary.

If `gateway_handler_device_id` is retained for compatibility, new code exposes
it only as `email_gateway_sender_device_id` and migrates no values
automatically. Existing non-null values are treated as legacy evidence requiring
review until the operator confirms their email-gateway meaning.

## Acceptance And Exit Gates

### VNC-1 — Specification and architecture

- Native/FIO field ownership and the shared-database correction are explicit.
- Redesign brief, performance boundary, staged transaction, recovery, and
  version registry are defined.
- No destructive migration or automatic topology inference is authorized.

Exit gate: specification consistency review passes; contract-test matrix is
recorded. No implementation begins before this gate passes.

### VNC-2 — Pure planner and native writer

- Parse a realistic VarAC INI while preserving unknown data and formatting.
- Build create/convert plans without I/O side effects.
- Write only the controlled allowlist to staged targets.
- Back up existing and record missing targets.
- Atomically promote and semantically read back.
- Roll back byte-for-byte after injected failure at every apply phase.
- Reject stale digest, symlink escape, path overlap, duplicate member number,
  unsupported version/layout, running target, and conflicting VARA ports.

Exit gate: focused pure/writer/fault-injection tests, compile, and diff checks
pass.

### VNC-3 — Store migration and durable transaction

- Additive migration opens legacy, production-shaped, and fresh databases.
- Legacy gateway evidence is not silently reinterpreted.
- Shared-database claims are cluster-owned and non-exclusive among members.
- Pending/completed/recovery journal transitions are durable and idempotent.
- FIO commit failure restores external state and leaves assignments unchanged.
- Restart recovery blocks launch until resolved.

Exit gate: migration, atomic-store, stale-plan, crash-state, rollback, and
read-only production-shaped tests pass.

### VNC-4 — Guided UI

- Add Radio is the binding route.
- Arrangement precedes preparation and mutating choices remain explicit.
- Automatic preparation precedes technical correction.
- Gateway copy uses `Email gateway sender` and offers existing member, new
  member, or no gateway when email relay is off.
- Shared database and distinct VARA resources are correctly summarized.
- Preview, stop requirement, unsupported writer, recovery, and Why remain clear.
- One scroll owner; no clipping/overlap/horizontal page scroll at 1920x1080,
  1000x700, and 900x560 in Light/Dark and Normal/Large Text.
- Render/resize/typing paths perform no I/O and stale workers cannot navigate or
  overwrite a newer draft.

Exit gate: focused route, widget, geometry/lifecycle, accessibility, and
performance tests pass; human visual review limitations are recorded.

### VNC-5 — Integration and release qualification

- Windows and Linux/Wine command generation is fixture-tested.
- Existing standalone conversion, first cluster, and join-existing routes pass.
- Cancellation and every failure preserve existing native/FIO state.
- Existing JS8Call, Fast Light, standalone VarAC, BBS sync, launch, Settings,
  and multi-radio regression suites pass.
- Specification and `ui_regression_work_log.md` record commands, model owners,
  results, skips, and external qualification.

Automated implementation gate: all available automated checks pass.

Release gate remains open until an operator validates, on disposable copies and
then a controlled station test, one Windows and one Linux/Wine two-member
cluster: preview, stop, backup, native apply, readback, launch, shared mailbox,
unique VARA ports, email-gateway sender, PTT lock behavior, Cancel, and recovery.
No automated result substitutes for this live RF/application qualification.

## Implementation Evidence — 2026-09-17

Implemented behavior includes the exact VarAC 13.2.7 Windows and Linux/Wine
writer, controlled-key parser/renderer, distinct bounded VARA runtime clones,
host-to-Wine native path projection, immutable preparation, complete plan
fingerprints, additive store fields/journal, split external/FIO transaction,
startup recovery, launch blocking, explicit email-gateway sender ownership,
shared-database semantics, generation-fenced workers, immediate Cancel/stale
compensation, and the concise Add Radio/Software Administration presentation.
VarAC 15.0.18 remains operator-managed and unsupported for native writes.

Automated evidence uses temporary fixtures and an isolated copy of the supplied
production-shaped database. It covers Windows/Linux-Wine preparation, exact
writer allowlists, unexpected-fault rollback, partial promotion, split
transaction commit/cancel, crash recovery, stale generation rejection,
create-first-cluster and join-existing-cluster persistence, legacy gateway
separation, launch blocking, responsive UI, adjacent JS8Call/Fast Light/VarAC
BBS and launch behavior, compilation, and diff hygiene. The original production
database and unrelated operator documents are not modified.

## Work-Package Ownership

- VNC-1 architecture/specification: `gpt-5.6-sol`, high reasoning.
- VNC-2 bounded native planner/writer implementation: `gpt-5.6-terra`, high
  reasoning; primary review required.
- VNC-3 migrations, persistence, external/FIO transaction, and recovery:
  `gpt-5.6-sol`, high reasoning.
- VNC-4 bounded UI implementation: `gpt-5.6-terra`, high reasoning; primary
  review required.
- VNC-5 focused acceptance extensions: `gpt-5.6-luna`, high reasoning; primary
  review required.
- Final integration, corrections, specification/work-log reconciliation, and
  exit-gate decision: `gpt-5.6-sol`, high reasoning.
