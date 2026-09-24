# VarAC Native Cluster Configuration Specification

Status: authoritative implementation specification; approved direction
2026-09-17. Operator testing on 2026-09-18 reopened the automated gate under
VNC-6: the native plan was not projected into Add Radio, intentional shared
database use blocked draft completion, and durable launch identity was not
platform-safe. The VNC-6 automated correction gate passed on 2026-09-18. Live
Windows and Linux/Wine operator qualification remains open.

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
operator-selected data outside the prefix may use Wine's `Z:` root mapping.
FIO-created VARA runtimes may not: when the qualified VarAC source is inside a
Wine drive, each generated runtime is placed inside that same drive and stored
in VarAC as a native drive-letter path such as
`C:\VARA-ft-710\VARA.exe`. FIO retains the host path separately for its own
file access. The native VarAC INI argument, `DBCustomFilePath`, and VARA
executable paths therefore never receive an untranslated Linux path.

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

## VNC-6 Correction — Canonical Draft, Native Layout, And Launch Identity

Status: specified and automated implementation gate passed 2026-09-18. Live
Windows and Linux/Wine qualification remains open. This correction is
governed by GRS-10 and supersedes any earlier allowance for a managed-root
VarAC INI, eager native apply from Software Administration, flattened launch
text, or a generic duplicate-storage check against a cluster-owned database.

### Native layout and platform mapping

A qualified VarAC cluster recipe uses one reviewed VarAC installation and one
distinct INI per instance in that installation, as described by the official
VarAC cluster guide. The INI filename is derived from the stable sanitized
instance identity. A new member receives a separate VARA runtime/configuration,
incoming/outbox identity, command/KISS ports, and launch identity. Members may
share only the reviewed cluster database and explicitly selected cluster
policy.

On native Windows, launch stores an argument vector containing the exact
`VarAC.exe` and Windows INI path and uses the VarAC installation as its working
directory. On Linux/Wine, launch stores `wine`, the host `VarAC.exe`, and the
Wine-visible installation-adjacent INI path as three distinct arguments, plus
the discovered Wine prefix environment and installation working directory.
The durable vector is never flattened and reparsed by a shell. macOS is best-
effort and may remain launch-pending when no qualified Wine mapping exists.

A `Z:` managed-root INI is not the default for a qualified native writer. An
unwritable or unmappable installation is a visible concern with a reviewed
location choice or launch-pending outcome; it is not permission to mutate an
existing file or silently invent a different native contract.

### Canonical draft and collision semantics

Native preparation publishes one complete VarAC bundle to the Add Radio draft
session as soon as the current-generation plan is ready. Software
Administration and the parent Connections/Review pages render that same bundle
without waiting for the nested assistant to complete every page. Application,
INI, database, incoming, outbox, working directory, VARA runtime/INI, ports,
cluster identity, exact argument vectors, environment, and mutation plan may
not exist solely in a technical presentation or loose widget.

The database claim is classified before collision validation. An intentional
cluster-owned database shared by members of that same reviewed cluster is
valid. Private storage reuse, a database owned by another cluster, or any
unapproved overwrite remains a safety blocker. INI, incoming/outbox, working
identity, VARA runtime, and endpoints must remain distinct.

Managed VARA runtime naming is resolved before the writer plan is built. On
Linux/Wine, when the selected VarAC source proves a `drive_<letter>`, the
preferred target is a readable, space-free sibling at that drive root:
`VARA-<radio-slug>`. This gives each process a distinct `VARA.ini` while keeping
the path native to the Windows application. A Linux/Wine layout without
concrete `drive_<letter>` evidence remains launch-pending or needs attention;
FIO must not substitute a radio-scoped host path that VarAC would see through
Wine as an unsafe `Z:` executable path.
When any filesystem object already occupies the preferred name, preparation
selects the first absent numbered sibling. Broken symlinks and paths reserved
by another member in the same plan count as occupied. This allocator is read-
only and deterministic for unchanged evidence. It never deletes or adopts an
occupied target. The transactional writer still rejects any target that exists
at validation or apply time, so a race or stale plan cannot overwrite an
arbitrary runtime. Every native path and launch projection must use the exact
selected sibling; consumers may not reconstruct the preferred path.

### Persisted cluster visibility

Saved cluster topology is authoritative application state, not a view
preference. If one or more cluster records exist, Settings and Software
Administration must load and show those clusters and memberships even when a
stale `varac_cluster_mode_enabled` preference is false. The cluster control is
forced on and cannot be turned off until the saved topology is deleted. A
display preference may hide an empty cluster editor; it may never suppress a
persisted cluster or make a successful Add Radio save appear to have failed.

### Draft and final transaction boundary

`Save as draft` performs no external mutation. It validates and returns the
complete immutable plan to Add Radio for Ready, warning-ready, and launch-
pending results. It neither invokes the native writer nor requires VarAC/VARA
to be running. Cancel or Back discards editor changes without creating files.

Only final `Save Radio and Software` may revalidate and execute the accepted
native transaction. It stages, backs up, atomically promotes, semantically
reads back, and commits the same canonical bundle to FIO. Failure in native or
database commit restores prior bytes and assignments. The recovery journal is
durable and idempotent, and only an unresolved affected transaction blocks its
launch.

### VNC-6 exit gate

- First-cluster and join-cluster fixtures enable non-mutating draft save while
  sharing only the reviewed cluster database.
- Windows and Linux/Wine fixtures assert install-adjacent unique INIs,
  structured argument vectors, working directory, environment, distinct VARA
  resources, and exact readback.
- The parent and nested UI assert identical bundle IDs/fingerprints and all
  populated facts before nested completion.
- Store and launch tests round-trip spaces, backslashes, drive letters, and
  Wine paths without shell reparsing or directory-as-command substitution.
- Cancel, draft save, stale generation, failure injection, and FIO commit
  failure preserve existing external bytes and database state.
- A copied production-shaped database passes additive migration and reload;
  live Windows and Linux/Wine qualification remains a separate release gate.

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

## VNC-7 — Cluster-Shared BBS Projection And Freshness Correction

Status: automated implementation gate passed 2026-09-19; live Windows and
Linux/Wine operator qualification remains open.

### Canonical cluster resources

The prepared VarAC bundle has two cluster-shared facts: `bbs_path` and
`bbs_archive_path`. For a native-managed cluster they persist exactly as
`varac_clusters.shared_bbs_path` and
`varac_clusters.shared_bbs_archive_path`; a member does not own a competing
private copy of either fact. Incoming and outbox remain member-local and
continue to use their distinct canonical bundle fields.

For a create-cluster route, FIO inherits a nonblank BBS/archive pair from the
reviewed standalone profile. For either missing inherited value, FIO derives
the corresponding path from the qualified VarAC installation as
`<VarAC install>/BBS` or `<VarAC install>/BBS/Archive`. For a join-cluster
route, both facts come from the selected cluster's durable shared paths; the
new member must not substitute its profile, working-directory, incoming, or
outbox path. These decisions are part of the frozen reviewed payload and its
fingerprint.

Final accepted Save may create a reviewed missing BBS or archive directory. It
never replaces, clears, or deletes existing directory content. A successful
external apply records the exact directories it created; same-session
compensation removes only a directory from that set and only when it is empty.
A nonempty or pre-existing directory is retained exactly as found. If a crash
occurs before created-directory evidence is durable, recovery leaves an empty
directory in place rather than guessing that it is safe to delete.

### Readback, freshness, and recovery

Post-native readback may enrich the prepared bundle with observed paths,
digests, and verification evidence. That enrichment is an expected result of
the accepted plan and must not invalidate the already rendered Review or force
the operator to re-prepare. Before mutation, FIO compares the current durable
cluster/member inventory with the original frozen reviewed payload, not with a
post-readback-enriched UI copy. A material durable-inventory difference is a
stale-plan failure; readback-only enrichment is not.

Rollback terminal journal states are explicit and idempotent. Repeating cleanup
for `complete` or `fio_committed` never restores committed native files;
repeating cleanup for `rolled_back` performs no mutation. `recovery_required`
is deliberately nonterminal: it retains backup evidence, blocks launch, and
requires explicit operator recovery rather than an unbounded automatic retry.
Startup recovery never performs automatic forward apply.

### Required presentation and ownership

Connections, Review, and Software Administration render BBS and archive as
prepared read-only cluster facts from the same canonical bundle as the shared
database. They must never show blank editable BBS fields after a qualified
cluster plan is prepared.

Work packages for this correction:

- Primary `gpt-5.6-sol`, high reasoning: schema/migration, transaction,
  concurrency, frozen-payload comparison, integration review, and final gate.
- `gpt-5.6-terra`, medium reasoning: bounded Settings projection, focused UI
  tests, and this documentation package.
- `gpt-5.6-luna`, medium reasoning: focused regression tests.

Automated final-tree evidence: **139 passed** across the guided-radio,
transaction, cluster persistence, native writer/preparation, production-shaped
audit, and real-widget partitions. Changed Python compilation,
production-copy migration/reload and integrity checks, and diff hygiene passed.
This is an automated implementation correction only. A controlled operator
walkthrough on supported Windows and Linux/Wine VarAC/VARA installations
remains an open release gate.

## VNC-8 — Member Mailbox Placement From Reviewed Station Evidence

Status: automated implementation gate passed 2026-09-19; live Windows and
Linux/Wine qualification remains open.

Incoming and outbox are member-local resources, but local does not mean that
FIO should disregard the operator's established VarAC data root. For Create
Cluster, the reviewed standalone member is the placement authority. For Join
Cluster, the reviewed seed member from the selected durable cluster is the
placement authority. FIO uses the existing member's incoming and outbox paths
as location evidence while never reusing those exact directories.

Automatic placement follows this exact policy:

- if both existing member paths are known, each new path uses its corresponding
  existing parent;
- if only one is known, its parent supplies the established station data root
  for both new paths;
- the generated children are filesystem-safe `<radio-name>_In` and
  `<radio-name>_Out` names;
- all durable profile mailbox claims are checked, with deterministic numeric
  suffixes used to avoid a collision; and
- without reviewed mailbox evidence, FIO retains the per-radio managed-root
  fallback rather than guessing from the VarAC installation or shared BBS.

An operator correction made through Advanced remains authoritative. A value
copied from the preceding prepared-plan presentation is not a correction; it
is generated state and must be re-derived on the next preparation. This
prevents an old `.freqinout` default from becoming sticky after FIO discovers
better durable station evidence.

The exact incoming and outbox targets join the immutable plan's managed
directories and reviewed lexical/resolved roots. Stable Wine directory aliases
retain the VNC-7 destination-fingerprint checks. Existing target content is
never replaced or cleared. Final apply may create only the reviewed missing
directories; rollback may remove only directories created by that apply and
only while empty. BBS/archive ownership and shared-database semantics are
unchanged.

Required regression evidence covers create and join routes, same-parent and
single-known-parent derivation, collisions, generated-value re-preparation,
explicit correction, managed-root fallback, Linux/Wine stable aliases,
retarget/broken-alias rejection, and existing-content preservation. No schema
or migration is introduced.

Automated final-tree evidence: **18 focused tests** and **288 tests** in the
full applicable guided-radio, planner, Software Administration, native
preparation/writer/transaction, and VNC partition passed. Changed Python
compilation and diff hygiene passed. A controlled operator walkthrough remains
required before release qualification.

## VNC-9 — Shared Installation Claims And Atomic Cluster Conversion

Status: automated implementation gate passed 2026-09-20; live Windows and
Linux/Wine qualification remains open.

The native plan and the FIO persistence model use the same ownership boundary.
VarAC cluster members may intentionally share the qualified VarAC executable,
installation working directory, effective cluster database, and cluster
BBS/archive. Those paths are nonexclusive member-manifest references to
application- or cluster-owned resources. A launch working directory is not a
member identity merely because each process starts there.

Member identity remains strict and exclusive: each member has its own VarAC
INI, cloned VARA runtime, VARA INI, incoming folder, outbox, endpoint ports,
and positive cluster instance number. The writer continues to reject any
existing VARA target and any unsafe or ambiguous member-local collision.

For `Create cluster` from a standalone node, final Save is one indivisible
operation. The existing node's verified native paths and launch command, its
manifest ownership, its cluster membership, the new node/application/manifest,
both radio projections, and both canonical Software Administration identities
commit together. Native files are applied under the existing split external
transaction, and any later FIO rejection rolls the database transaction back
and compensates the external session. Join-cluster applies the same shared
claim normalization to the new member without changing unrelated members.

Final validation reports authoritative member-number conflicts before generic
manifest wording. Other member-local path or endpoint collisions remain hard
stops. A persistence rejection must not claim that the radio was saved; its UI
and log state that nothing was saved, existing assignments were retained, and
whether native compensation completed or needs recovery.

Required regression evidence begins with a legacy standalone manifest whose
common executable, install working directory, database, BBS, and archive are
exclusive. Conversion must reload both manifests with only those named shared
claims nonexclusive, while preserving member-local exclusivity and rejecting a
duplicate member. No schema or destructive data migration is introduced.

## VNC-10 — Legacy Launch And Managed Wine Runtime Repair

Status: specified and automated implementation gate passed 2026-09-21; live
Windows and Linux/Wine qualification remains open.

Legacy VarAC launch text is compatibility evidence, not a shell command to
reparse. On Linux/Wine, FIO projects it to a structured vector containing
`wine`, the host `VarAC.exe`, and the exact Wine-native member INI path as
separate arguments, with the verified Wine prefix and installation working
directory. On native Windows, the vector contains the native `VarAC.exe` and
native member INI path and uses the native installation directory. A saved
canonical VarAC identity always wins; compatibility recovery may fill a missing
canonical identity but must never overwrite one.

For an exact FIO-managed VarAC 13.2.7 Linux/Wine node whose persisted VARA
runtime is outside the proven Wine drive, startup recovery may repair it
without an operator dialog only when all evidence is unambiguous and VarAC and
VARA are stopped. The normal native transaction copies the readable,
non-symlink source to the first absent drive-local `VARA-<radio-slug>` sibling,
rewrites only the qualified VarAC key, backs up and reads back the result, and
then updates the node, manifest, canonical Software Administration identity,
and Launch Control projection in the same persistence boundary. Existing
startup and monitoring choices are preserved. The old runtime remains in
place; repair never deletes, adopts, or replaces an occupied folder.

If the native INI already identifies a valid drive-local VARA executable, FIO
reconciles stale database projections without changing the native file. A
running process defers repair. Missing, conflicting, unsupported, symlinked,
or otherwise ambiguous evidence produces one actionable needs-attention result
and no mutation. Native Windows never runs the Wine-layout migration; its
qualified preparation, persistence, and structured launch remain native.

Regression evidence covers Linux/Wine and Windows structured launch, the
canonical-over-legacy precedence guard, transactional copy/rewrite/readback,
reconcile-only recovery, running-process deferral, old-runtime retention,
four-projection persistence, and native-Windows migration no-op behavior.

## VNC-11 — Cluster Launch Authority And Station-Wide Process Attribution

Status: implemented and covered by the focused automated gate on 2026-09-24;
live Linux/Wine operator qualification remains open.

### Observable defect

With the FTDX-10 VarAC/VARA node already running, an explicit row `Start` for
the FT-710 VarAC node reports a duplicate-risk failure and launches nothing.
The launch-owned inventory sees the existing family process, but the
selected-radio queue contains only the FT-710 candidate. Counting only that
queue makes the valid FTDX-10 process appear unattributed and incorrectly
blocks the distinct FT-710 member.

Separately, the managed FT-710 cluster INI was produced with
`[VARAHF_CONFIG] VarahfLaunchOnModemConnect=OFF`. In managed cluster mode,
VarAC is the launch authority for its node-local VARA modem. The value must be
`ON` so starting that VarAC member also starts the VARA runtime named by that
member's `VarahfMainPath`.

### Surgical correction

The selected-radio launch plan remains selected-radio scoped: it may start only
the requested radio's rows. Process attribution, however, is station-wide. The
preflight constructs one immutable attribution catalog from every persisted,
configured VarAC and VARA identity before evaluating the selected target. Each
observed process is matched against the exact executable, VarAC INI argument,
Wine prefix/working directory where applicable, or distinct VARA runtime path.

An existing FTDX-10 process that matches the FTDX-10 identity is therefore
attributed and does not block an absent FT-710 identity. The requested FT-710
VarAC process launches with its own INI. A process matching the requested
FT-710 identity is `already running`. Any same-family process that cannot be
attributed to the complete persisted station catalog remains a fail-closed
duplicate-risk result. The correction must not relax exact matching or permit
the selected-radio action to launch another radio's application.

For every FIO-managed **cluster member**, qualified native preparation and
targeted repair write and semantically read back:

`[VARAHF_CONFIG] VarahfLaunchOnModemConnect=ON`

The node's `VarahfMainPath`, command port, KISS configuration/port, monitor
path/port, VarAC INI, and VARA runtime remain member-distinct. Existing
standalone or operator-managed nodes retain their reviewed policy; this rule
does not broadly rewrite their INIs.

When this key is `ON`, the VarAC row owns starting VARA. The hidden canonical
VARA component remains in the radio bundle as runtime identity, readiness,
port, and recovery evidence, but FIO does not independently spawn it as a
second startup row for that managed member. This prevents FIO and VarAC from
racing to start the same modem. Status and duplicate prevention still
attribute an already-running VARA process to the exact member runtime.

An existing managed cluster member whose key is missing or `OFF` is repairable
without replacing its Fast Light, JS8Call, message, schedule, launch-choice, or
cluster configuration. The repair is limited to this qualified allowlisted key,
uses the existing native backup/atomic-write/semantic-readback transaction,
and preserves comments, unknown keys, encoding, newline convention, and every
unrelated byte-semantic value. If the affected VarAC or VARA process is
running, FIO performs no native write and reports the exact stop-and-retry
action. Unsupported or ambiguous layouts remain read-only.

### Acceptance and regression boundary

1. With exact FTDX-10 VarAC and VARA processes running, manual FT-710 VarAC
   `Start` launches only the FT-710 VarAC command and is not blocked by the
   known FTDX-10 processes.
2. The same station-wide attribution behavior governs automatic startup,
   `Start Startup Apps`, and row `Start` without broadening their launch scope.
3. An exact already-running FT-710 VarAC process is credited and never
   duplicated; an unknown VarAC or VARA family process still fails closed.
4. Managed create-cluster and join-cluster fixtures write
   `VarahfLaunchOnModemConnect=ON`, read it back, and retain distinct runtime
   paths and ports on Linux/Wine and native Windows.
5. The resulting launch plan starts VarAC once and does not independently spawn
   that member's parent-managed VARA component.
6. A targeted legacy managed-member repair changes only the launch-on-connect
   key and the corresponding canonical projection/audit evidence. Cancel,
   write failure, readback failure, and persistence failure restore the prior
   INI and FIO state.
7. Standalone and operator-managed VarAC policies remain unchanged, and all
   existing cluster database, BBS, mailbox, launch-choice, and recovery tests
   remain green.

No destructive migration is authorized. This slice is complete only after the
focused automated gate passes and a Linux/Wine operator confirms that starting
the second VarAC member opens its distinct VARA modem and connects through the
configured member ports without duplicating the first node.

Implementation evidence: managed preparation and the qualified writer require
launch-on-connect `ON`; startup/manual planning keeps the parent-managed VARA
row out of the execution queue; saved legacy recipes receive the same in-memory
authority correction; launch preflight attributes family processes against the
complete saved station catalog; and automatic recovery performs a one-key
backup/stage/semantic-readback repair only for an idle, exact managed member.
Unknown family process evidence remains fail-closed. The focused VarAC,
launch-planner, identity, transaction, and Launch Control gate completed with
197 passing tests on 2026-09-24.
